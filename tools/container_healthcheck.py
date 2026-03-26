#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


def run_cmd(args: list[str], *, check: bool = True) -> str:
    proc = subprocess.run(args, capture_output=True, text=True)
    if check and proc.returncode != 0:
        stderr = proc.stderr.strip()
        stdout = proc.stdout.strip()
        detail = stderr or stdout or f"exit code {proc.returncode}"
        raise RuntimeError(f"command failed: {' '.join(args)}: {detail}")
    return proc.stdout.strip()


def docker_exec(container: str, command: list[str], *, check: bool = True) -> str:
    return run_cmd(["docker", "exec", container, *command], check=check)


def fetch_json(url: str, timeout_s: float) -> object:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return json.loads(response.read().decode("utf-8"))


@dataclass
class CheckResult:
    ok: bool
    label: str
    detail: str


def check_container_exists(container: str) -> CheckResult:
    output = run_cmd(["docker", "inspect", container], check=False)
    if not output:
        return CheckResult(False, "container", f"{container} not found")
    return CheckResult(True, "container", f"{container} exists")


def check_image(container: str, expected_image: str) -> CheckResult:
    image = run_cmd(["docker", "inspect", container, "--format", "{{.Config.Image}}"])
    ok = image == expected_image
    detail = f"image={image}"
    if not ok:
        detail += f" expected={expected_image}"
    return CheckResult(ok, "image", detail)


def check_health(container: str) -> CheckResult:
    status = run_cmd(
        [
            "docker",
            "inspect",
            container,
            "--format",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}",
        ]
    )
    return CheckResult(status == "healthy", "health", f"status={status}")


def check_log_config(container: str, max_size: str, max_file: str) -> CheckResult:
    raw = run_cmd(["docker", "inspect", container, "--format", "{{json .HostConfig.LogConfig}}"])
    config = json.loads(raw)
    actual = config.get("Config", {})
    ok = config.get("Type") == "json-file" and actual.get("max-size") == max_size and actual.get("max-file") == max_file
    detail = json.dumps(config, sort_keys=True)
    return CheckResult(ok, "logging", detail)


def check_version(url: str, timeout_s: float) -> CheckResult:
    try:
        payload = fetch_json(url, timeout_s)
    except urllib.error.URLError as exc:
        return CheckResult(False, "version", f"request failed: {exc}")
    version = payload.get("version") if isinstance(payload, dict) else None
    ok = isinstance(payload, dict) and bool(version)
    return CheckResult(ok, "version", json.dumps(payload, sort_keys=True))


def check_pipeline(container: str, min_receivers: int) -> CheckResult:
    output = docker_exec(container, ["ps", "-ef"])
    lines = output.splitlines()
    kiwirecorder_count = sum("kiwirecorder.py" in line for line in lines)
    af2udp_count = sum("/usr/local/bin/af2udp" in line for line in lines)
    ft8modem_count = sum("/usr/local/bin/ft8modem" in line for line in lines)
    ok = all(count >= min_receivers for count in (kiwirecorder_count, af2udp_count, ft8modem_count))
    detail = (
        f"kiwirecorder={kiwirecorder_count} "
        f"af2udp={af2udp_count} "
        f"ft8modem={ft8modem_count}"
    )
    return CheckResult(ok, "pipeline", detail)


def collect_decode_summary(container: str) -> dict[str, dict[str, object]]:
    script = "\n".join(
        [
            "from pathlib import Path",
            "import json",
            "from datetime import datetime",
            "base = Path('/tmp/ft8modem')",
            "out = {}",
            "for path in sorted(base.glob('udp-*/decoded.txt')):",
            "    stat = path.stat()",
            "    lines = [line for line in path.read_text(errors='replace').splitlines() if line.strip()]",
            "    out[path.parent.name] = {",
            "        'size': stat.st_size,",
            "        'mtime_epoch': stat.st_mtime,",
            "        'mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(sep=' ', timespec='seconds'),",
            "        'tail': lines[-3:],",
            "    }",
            "print(json.dumps(out, sort_keys=True))",
        ]
    )
    raw = docker_exec(container, ["python", "-c", script])
    return json.loads(raw) if raw else {}


def check_decodes(container: str, min_nonempty: int) -> CheckResult:
    summary = collect_decode_summary(container)
    nonempty = sorted(name for name, info in summary.items() if int(info.get("size", 0)) > 0)
    ok = len(nonempty) >= min_nonempty
    detail = f"nonempty={len(nonempty)}/{len(summary)} active={','.join(nonempty) if nonempty else 'none'}"
    return CheckResult(ok, "decodes", detail)


def check_fresh_decodes(
    container: str,
    fresh_within_s: float | None,
    min_fresh_decodes: int,
    required_slots: list[str],
) -> CheckResult | None:
    if fresh_within_s is None and not required_slots:
        return None

    summary = collect_decode_summary(container)
    now = time.time()
    fresh_slots: list[str] = []
    stale_slots: list[str] = []
    empty_slots: list[str] = []

    for name, info in sorted(summary.items()):
        size = int(info.get("size", 0))
        age_s = now - float(info.get("mtime_epoch", 0.0))
        if size <= 0:
            empty_slots.append(name)
            continue
        if fresh_within_s is None or age_s <= fresh_within_s:
            fresh_slots.append(name)
        else:
            stale_slots.append(name)

    missing_required: list[str] = []
    for slot in required_slots:
        info = summary.get(slot)
        if not info or int(info.get("size", 0)) <= 0:
            missing_required.append(slot)
            continue
        if fresh_within_s is not None:
            age_s = now - float(info.get("mtime_epoch", 0.0))
            if age_s > fresh_within_s:
                missing_required.append(slot)

    ok = len(fresh_slots) >= min_fresh_decodes and not missing_required
    detail_parts = [f"fresh={len(fresh_slots)}/{len(summary)}"]
    if fresh_slots:
        detail_parts.append(f"active={','.join(fresh_slots)}")
    if stale_slots:
        detail_parts.append(f"stale={','.join(stale_slots)}")
    if empty_slots:
        detail_parts.append(f"empty={','.join(empty_slots)}")
    if fresh_within_s is not None:
        detail_parts.append(f"window={fresh_within_s:g}s")
    if required_slots:
        detail_parts.append(f"required={','.join(required_slots)}")
    if missing_required:
        detail_parts.append(f"missing_required={','.join(missing_required)}")
    return CheckResult(ok, "decode-freshness", " ".join(detail_parts))


def print_decode_details(container: str) -> None:
    summary = collect_decode_summary(container)
    now = time.time()
    print("decode-details:")
    for name in sorted(summary):
        info = summary[name]
        tail = info.get("tail", [])
        tail_text = " | ".join(tail) if tail else "<empty>"
        age_s = now - float(info.get("mtime_epoch", 0.0))
        print(f"  {name}: size={info['size']} mtime={info['mtime']} age_s={age_s:.1f} tail={tail_text}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the KiwiScan Docker container.")
    parser.add_argument("--container", default="kiwiscan")
    parser.add_argument("--expected-image", default="n4ldr/kiwiscan:0.1.6")
    parser.add_argument("--version-url", default="http://127.0.0.1:4020/version")
    parser.add_argument("--http-timeout", type=float, default=10.0)
    parser.add_argument("--expected-max-size", default="10m")
    parser.add_argument("--expected-max-file", default="5")
    parser.add_argument("--min-receivers", type=int, default=8)
    parser.add_argument("--min-nonempty-decodes", type=int, default=1)
    parser.add_argument("--fresh-within-s", type=float)
    parser.add_argument("--min-fresh-decodes", type=int, default=0)
    parser.add_argument("--require-slots", default="")
    parser.add_argument("--show-decode-details", action="store_true")
    args = parser.parse_args()

    required_slots = [slot.strip() for slot in args.require_slots.split(",") if slot.strip()]

    checks = [check_container_exists(args.container)]
    if not checks[0].ok:
        for check in checks:
            prefix = "OK" if check.ok else "FAIL"
            print(f"[{prefix}] {check.label}: {check.detail}")
        return 1

    checks.extend(
        [
            check_image(args.container, args.expected_image),
            check_health(args.container),
            check_log_config(args.container, args.expected_max_size, args.expected_max_file),
            check_version(args.version_url, args.http_timeout),
            check_pipeline(args.container, args.min_receivers),
            check_decodes(args.container, args.min_nonempty_decodes),
        ]
    )

    fresh_check = check_fresh_decodes(
        args.container,
        args.fresh_within_s,
        args.min_fresh_decodes,
        required_slots,
    )
    if fresh_check is not None:
        checks.append(fresh_check)

    failed = False
    for check in checks:
        prefix = "OK" if check.ok else "FAIL"
        print(f"[{prefix}] {check.label}: {check.detail}")
        failed = failed or not check.ok

    if args.show_decode_details:
        print_decode_details(args.container)

    if failed:
        print("Summary: FAIL", file=sys.stderr)
        return 1

    print("Summary: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())