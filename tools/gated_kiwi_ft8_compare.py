from __future__ import annotations

import argparse
import signal
import json
import os
import select
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_API_BASE = "http://127.0.0.1:4020"
DEFAULT_CONTAINER = "kiwiscan"
DEFAULT_OURS_HOST = "10.13.73.235"
DEFAULT_OURS_PORT = 8073
DEFAULT_CONTROL_HOST = "10.13.73.236"
DEFAULT_CONTROL_PORT = 8074
DEFAULT_GAIN = 0.25
_DOCKER_CANDIDATES = (
    "/usr/local/bin/docker",
    "/opt/homebrew/bin/docker",
    "/Applications/Docker.app/Contents/Resources/bin/docker",
)
_CONTROL_USERS_FREQ_TOLERANCE_HZ = 500.0
FT8_FREQS_HZ = {
    "17m": 18_100_000,
    "20m": 14_074_000,
}


@dataclass(frozen=True)
class ProbeTarget:
    label: str
    host: str
    port: int


class TerminatedBySignal(Exception):
    def __init__(self, signum: int):
        super().__init__(f"terminated by signal {signum}")
        self.signum = int(signum)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gate on live FT8 activity from the control Kiwi, then optionally pause "
            "KiwiScan automation and run a paired direct .235 vs .236 FT8 comparison."
        )
    )
    parser.add_argument("--band", choices=sorted(FT8_FREQS_HZ.keys()), required=True)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--ours-host", default=DEFAULT_OURS_HOST)
    parser.add_argument("--ours-port", type=int, default=DEFAULT_OURS_PORT)
    parser.add_argument("--control-host", default=DEFAULT_CONTROL_HOST)
    parser.add_argument("--control-port", type=int, default=DEFAULT_CONTROL_PORT)
    parser.add_argument(
        "--gate-source",
        choices=("probe", "users", "either", "both"),
        default="probe",
        help=(
            "How to decide whether the control Kiwi is active: raw decoder probe, "
            "visible FT8 /users session, either signal, or both."
        ),
    )
    parser.add_argument(
        "--gate-seconds",
        type=int,
        default=65,
        help="Seconds to listen on the control Kiwi before deciding whether the band is active.",
    )
    parser.add_argument(
        "--gate-min-decodes",
        type=int,
        default=1,
        help="Minimum control-side FT8 decodes required to treat the band as active.",
    )
    parser.add_argument(
        "--compare-seconds",
        type=int,
        default=65,
        help="Seconds to run the paired direct comparison after gating succeeds.",
    )
    parser.add_argument(
        "--post-pause-grace-seconds",
        type=int,
        default=10,
        help="Extra quiet period after fixed receivers stop before launching an external compare probe.",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        default=0,
        help=(
            "Optional total time budget for waiting on an active control window. "
            "0 means run the gate once and stop."
        ),
    )
    parser.add_argument(
        "--gate-interval-seconds",
        type=int,
        default=30,
        help="Delay between control gate attempts when waiting for a live window.",
    )
    parser.add_argument(
        "--gate-only",
        action="store_true",
        help="Only run the control-side gate and report activity; do not pause automation or compare both Kiwis.",
    )
    parser.add_argument(
        "--skip-gate",
        action="store_true",
        help="Bypass control-side gating and run the paired comparison immediately.",
    )
    parser.add_argument(
        "--keep-paused",
        action="store_true",
        help="Do not restore fixedModeEnabled after the paired comparison.",
    )
    parser.add_argument(
        "--quiet-progress",
        action="store_true",
        help="Suppress intermediate gate/comparison progress lines.",
    )
    return parser.parse_args()


def _log(event: str, **fields: Any) -> None:
    payload = {"event": event}
    payload.update(fields)
    print(json.dumps(payload, sort_keys=True), flush=True)


def _http_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def _load_settings(api_base: str) -> dict[str, Any]:
    payload = _http_json(f"{api_base.rstrip('/')}/automation/settings")
    if not isinstance(payload, dict):
        raise RuntimeError("automation/settings did not return a JSON object")
    return payload


def _save_settings(api_base: str, payload: dict[str, Any]) -> None:
    _http_json(f"{api_base.rstrip('/')}/automation/settings", method="POST", payload=payload)


def _confirm_fixed_mode(api_base: str, *, expected: bool, timeout_s: int = 30) -> dict[str, Any]:
    deadline = time.time() + max(1, int(timeout_s))
    last_settings: dict[str, Any] = {}
    while time.time() < deadline:
        settings = _load_settings(api_base)
        if isinstance(settings, dict):
            last_settings = settings
            if bool(settings.get("fixedModeEnabled", True)) == bool(expected):
                return settings
        time.sleep(1.0)
    raise RuntimeError(
        f"Timed out waiting for fixedModeEnabled={expected}; last settings={last_settings}"
    )


def _wait_for_fixed_state(api_base: str, *, expect_active: bool, timeout_s: int = 90) -> dict[str, Any]:
    deadline = time.time() + max(1, int(timeout_s))
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        payload = _http_json(f"{api_base.rstrip('/')}/health/rx")
        if not isinstance(payload, dict):
            time.sleep(1.0)
            continue
        last_payload = payload
        channels = payload.get("channels") if isinstance(payload.get("channels"), dict) else {}
        fixed = [channels.get(str(rx)) or {} for rx in range(2, 8)]
        if expect_active:
            if fixed and all(bool(ch.get("active")) for ch in fixed):
                return payload
        else:
            active_receivers = int(payload.get("active_receivers", 0) or 0)
            if active_receivers == 0:
                return payload
        time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for fixed receivers expect_active={expect_active}")


def _container_has_matching_processes(container: str, pattern: str) -> bool:
    proc = subprocess.run(
        [_docker_path(), "exec", container, "pgrep", "-f", pattern],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    return proc.returncode == 0 and bool((proc.stdout or "").strip())


def _wait_for_container_orphan_cleanup(container: str, *, timeout_s: int = 12) -> None:
    deadline = time.time() + max(1, int(timeout_s))
    while time.time() < deadline:
        try:
            active_auto = _container_has_matching_processes(container, "kiwirecorder.py.*(AUTO_|FIXED_|ROAM)")
            active_ft8modem = _container_has_matching_processes(container, "ft8modem.*udp:")
        except Exception:
            return
        if not active_auto and not active_ft8modem:
            return
        time.sleep(0.2)
    raise RuntimeError("Timed out waiting for in-container orphan cleanup")


def _restore_fixed_receivers(api_base: str, *, band: str, expected_fixed_mode: bool) -> None:
    _log("restore_fixed_receivers", band=band)
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            _save_settings(api_base, {"fixedModeEnabled": expected_fixed_mode})
            _confirm_fixed_mode(api_base, expected=expected_fixed_mode, timeout_s=20)
            _wait_for_fixed_state(api_base, expect_active=expected_fixed_mode)
            _log("restored_fixed_receivers", band=band, attempts=attempt)
            return
        except Exception as exc:
            last_error = exc
            _log("restore_retry", band=band, attempt=attempt, error=str(exc))
            time.sleep(1.0)
    raise RuntimeError(f"Failed to restore fixed receivers: {last_error}")


def _install_signal_handlers() -> tuple[dict[int, Any], dict[str, Any]]:
    handled = (signal.SIGINT, signal.SIGTERM)
    cleanup_state: dict[str, Any] = {"in_cleanup": False, "pending_signal": None}
    previous: dict[int, Any] = {}

    def _handler(signum: int, _frame: Any) -> None:
        if cleanup_state["in_cleanup"]:
            cleanup_state["pending_signal"] = int(signum)
            return
        raise TerminatedBySignal(signum)

    for signum in handled:
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, _handler)
    return previous, cleanup_state


def _restore_signal_handlers(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _docker_path() -> str:
    resolved = shutil.which("docker")
    if resolved:
        return resolved
    for candidate in _DOCKER_CANDIDATES:
        if os.path.exists(candidate) and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError("docker executable not found in PATH or known install locations")


def _docker_exec_python(
    container: str,
    script: str,
    *,
    heartbeat: dict[str, Any] | None = None,
) -> tuple[int, str]:
    docker = _docker_path()
    token = f"kiwiscan_probe_{os.getpid()}_{int(time.time() * 1000)}"
    output_path = f"/tmp/{token}.out"
    rc_path = f"/tmp/{token}.rc"
    launch = subprocess.run(
        [
            docker,
            "exec",
            container,
            "sh",
            "-lc",
            "rm -f \"$2\" \"$3\"; nohup sh -lc 'python3 -c \"$1\" > \"$2\" 2>&1; printf \"%s\" $? > \"$3\"' worker \"$1\" \"$2\" \"$3\" >/dev/null 2>&1 < /dev/null &",
            "probe",
            script,
            output_path,
            rc_path,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if launch.returncode != 0:
        raise RuntimeError(
            f"docker exec launch failed with exit code {launch.returncode}: {(launch.stderr or launch.stdout).strip()}"
        )

    start = time.time()
    last_heartbeat_tick = -1
    exit_code: int | None = None
    output = ""
    try:
        while True:
            rc_proc = subprocess.run(
                [
                    docker,
                    "exec",
                    container,
                    "sh",
                    "-lc",
                    'if [ -f "$1" ]; then cat "$1"; fi',
                    "probe",
                    rc_path,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            rc_text = rc_proc.stdout.strip() if rc_proc.returncode == 0 else ""
            if rc_text:
                exit_code = int(rc_text)
                output_proc = subprocess.run(
                    [
                        docker,
                        "exec",
                        container,
                        "sh",
                        "-lc",
                        'if [ -f "$1" ]; then cat "$1"; fi',
                        "probe",
                        output_path,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                output = output_proc.stdout if output_proc.returncode == 0 else ""
                break
            output_proc = subprocess.run(
                [
                    docker,
                    "exec",
                    container,
                    "sh",
                    "-lc",
                    'if [ -f "$1" ]; then cat "$1"; fi',
                    "probe",
                    output_path,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            output = output_proc.stdout if output_proc.returncode == 0 else ""
            if output.strip():
                last_line = output.strip().splitlines()[-1].strip()
                try:
                    payload = json.loads(last_line)
                except Exception:
                    payload = None
                if isinstance(payload, dict):
                    if payload.get("event") == "probe_signal":
                        exit_code = 128 + int(payload.get("signal", 15) or 15)
                        break
                    if payload.get("event") == "result":
                        exit_code = 0
                        break
            if heartbeat:
                elapsed = int(time.time() - start)
                tick = elapsed // 15
                if elapsed > 0 and tick != last_heartbeat_tick:
                    last_heartbeat_tick = tick
                    _log("probe_wait", elapsed_s=elapsed, **heartbeat)
            if time.time() - start > 300:
                raise subprocess.TimeoutExpired([docker, "exec", container, "python3", "-c", script], 300)
            time.sleep(1.0)
    except Exception:
        subprocess.run(
            [
                docker,
                "exec",
                container,
                "sh",
                "-lc",
                'rm -f "$1" "$2" >/dev/null 2>&1 || true',
                "probe",
                output_path,
                rc_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        raise
    subprocess.run(
        [
            docker,
            "exec",
            container,
            "sh",
            "-lc",
            'rm -f "$1" "$2" >/dev/null 2>&1 || true',
            "probe",
            output_path,
            rc_path,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert exit_code is not None
    return exit_code, output


def _probe_script(targets: list[ProbeTarget], *, freq_hz: int, duration_s: int, gain: float, progress: bool) -> str:
    targets_payload = [
        {"label": target.label, "host": target.host, "port": int(target.port)}
        for target in targets
    ]
    return f"""
import json
import os
import select
import signal
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path('/opt/kiwiscan')
with (ROOT / 'config' / 'kiwi_secrets.json').open('r', encoding='utf-8') as fp:
    PASSWORD = str(json.load(fp).get('kiwi_password') or '')
KIWIRECORDER = ROOT / 'vendor' / 'kiwiclient-jks' / 'kiwirecorder.py'
AF2UDP = ROOT / 'vendor' / 'ft8modem-sm' / 'af2udp'
FT8MODEM = ROOT / 'vendor' / 'ft8modem-sm' / 'ft8modem'
TARGETS = json.loads({json.dumps(json.dumps(targets_payload))})
FREQ_HZ = {int(freq_hz)}
DURATION_S = {int(duration_s)}
GAIN = {float(gain)!r}
SHOW_PROGRESS = {bool(progress)!r}
LAST_SIGNAL = None

def handle_signal(signum, _frame):
    global LAST_SIGNAL
    LAST_SIGNAL = int(signum)
    print(json.dumps({{
        'event': 'probe_signal',
        'signal': int(signum),
    }}, sort_keys=True), flush=True)
    raise SystemExit(128 + int(signum))

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

def record_line(line, sample_decodes, sample_inputs):
    if not line:
        return 0
    if line.startswith('D:'):
        if len(sample_decodes) < 6:
            sample_decodes.append(line)
        return 1
    if line.startswith('INPUT:') and len(sample_inputs) < 4:
        sample_inputs.append(line)
    return 0

def stop(proc):
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass

results = []
for index, target in enumerate(TARGETS):
    udp_port = 39520 + index
    temp = Path(f"/tmp/{{target['label']}}_ft8_compare")
    shutil.rmtree(temp, ignore_errors=True)
    temp.mkdir(parents=True, exist_ok=True)
    rec = sox = af = dec = None
    decode_lines = 0
    sample_decodes = []
    sample_inputs = []
    try:
        rec = subprocess.Popen([
            'python3',
            str(KIWIRECORDER),
            '-s', str(target['host']),
            '-p', str(target['port']),
            '--password', PASSWORD,
            '-f', str(FREQ_HZ),
            '-m', 'usb',
            '-L', '0',
            '-H', '3100',
            '--user', str(target['label']),
            '--nc',
            '--quiet',
        ], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        sox = subprocess.Popen([
            'sox', '-v', str(GAIN),
            '-t', 'raw', '-r', '12000', '-e', 'signed', '-b', '16', '-c', '1', '-',
            '-t', 'raw', '-r', '48000', '-e', 'signed', '-b', '16', '-c', '1', '-',
        ], stdin=rec.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        rec.stdout.close()
        af = subprocess.Popen([
            str(AF2UDP), str(udp_port), '256', '48000'
        ], stdin=sox.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sox.stdout.close()
        dec = subprocess.Popen([
            str(FT8MODEM), '-t', str(temp), '-r', '48000', 'FT8', f'udp:{{udp_port}}'
        ], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
        os.set_blocking(dec.stdout.fileno(), False)

        start = time.time()
        end = start + DURATION_S
        last_tick = -1
        line_buffer = bytearray()
        while time.time() < end:
            elapsed = int(time.time() - start)
            tick = elapsed // 15
            if SHOW_PROGRESS and tick != last_tick:
                last_tick = tick
                print(json.dumps({{
                    'event': 'progress',
                    'label': target['label'],
                    'elapsed_s': elapsed,
                    'decode_lines': decode_lines,
                    'sample_inputs': sample_inputs[:2],
                }}, sort_keys=True), flush=True)
            rlist, _, _ = select.select([dec.stdout], [], [], 1.0)
            if not rlist:
                if dec.poll() is not None:
                    break
                continue
            try:
                chunk = os.read(dec.stdout.fileno(), 4096)
            except BlockingIOError:
                continue
            if not chunk:
                if dec.poll() is not None:
                    break
                continue
            line_buffer.extend(chunk)
            while True:
                newline_index = line_buffer.find(b'\\n')
                if newline_index < 0:
                    break
                raw_line = bytes(line_buffer[:newline_index])
                del line_buffer[: newline_index + 1]
                line = raw_line.decode('utf-8', errors='ignore').strip()
                decode_lines += record_line(line, sample_decodes, sample_inputs)
        if line_buffer:
            line = bytes(line_buffer).decode('utf-8', errors='ignore').strip()
            decode_lines += record_line(line, sample_decodes, sample_inputs)
    finally:
        for proc in (dec, af, sox, rec):
            stop(proc)

    results.append({{
        'label': target['label'],
        'host': target['host'],
        'port': target['port'],
        'freq_hz': FREQ_HZ,
        'decode_lines': decode_lines,
        'sample_decodes': sample_decodes,
        'sample_inputs': sample_inputs,
    }})

print(json.dumps({{'event': 'result', 'results': results}}, sort_keys=True), flush=True)
"""


def _run_probe(container: str, targets: list[ProbeTarget], *, freq_hz: int, duration_s: int, gain: float, progress: bool) -> list[dict[str, Any]]:
    effective_progress = False
    if progress:
        _log(
            "progress_disabled",
            container=container,
            freq_hz=freq_hz,
            targets=[target.label for target in targets],
        )
    exit_code, raw = _docker_exec_python(
        container,
        _probe_script(
            targets,
            freq_hz=freq_hz,
            duration_s=duration_s,
            gain=gain,
            progress=effective_progress,
        ),
        heartbeat={
            "container": container,
            "freq_hz": freq_hz,
            "targets": [target.label for target in targets],
        }
        if progress
        else None,
    )
    result_payload: dict[str, Any] | None = None
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("event") == "progress":
            progress_event = str(payload.get("event") or "probe_progress")
            progress_fields = {key: value for key, value in payload.items() if key != "event"}
            _log(progress_event, **progress_fields)
            continue
        if payload.get("event") == "result":
            result_payload = payload
    if exit_code != 0:
        raise RuntimeError(f"docker exec failed with exit code {exit_code}: {raw.strip()}")
    if not isinstance(result_payload, dict):
        raise RuntimeError(f"Probe did not produce a result payload: {raw.strip()}")
    results = result_payload.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"Probe returned malformed results: {result_payload}")
    return [result for result in results if isinstance(result, dict)]


def _control_users_gate(args: argparse.Namespace, *, band: str) -> dict[str, Any]:
    url = f"http://{args.control_host}:{int(args.control_port)}/users"
    try:
        payload = _http_json(url)
    except Exception as exc:
        return {
            "band": band,
            "source": "users",
            "active": False,
            "error": str(exc),
            "matches": [],
        }

    matches: list[dict[str, Any]] = []
    target_freq_hz = float(FT8_FREQS_HZ[band])
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, dict):
            continue
        ext = str(row.get("e") or "").strip().upper()
        mode = str(row.get("m") or "").strip().lower()
        try:
            freq_hz = float(row.get("f") or 0.0)
        except Exception:
            freq_hz = 0.0
        if ext != "FT8":
            continue
        if mode != "usb":
            continue
        if abs(freq_hz - target_freq_hz) > _CONTROL_USERS_FREQ_TOLERANCE_HZ:
            continue
        matches.append(
            {
                "slot": row.get("i"),
                "identity": row.get("n"),
                "freq_hz": freq_hz,
                "mode": row.get("m"),
                "ext": row.get("e"),
                "age": row.get("t"),
            }
        )

    return {
        "band": band,
        "source": "users",
        "active": bool(matches),
        "matches": matches,
    }


def _control_gate(args: argparse.Namespace, *, band: str) -> dict[str, Any]:
    probe_gate: dict[str, Any] | None = None
    users_gate: dict[str, Any] | None = None
    gate_source = str(args.gate_source or "probe")

    if gate_source in {"probe", "either", "both"}:
        target = ProbeTarget(label=f"gate_control_{band}", host=args.control_host, port=args.control_port)
        results = _run_probe(
            args.container,
            [target],
            freq_hz=FT8_FREQS_HZ[band],
            duration_s=args.gate_seconds,
            gain=DEFAULT_GAIN,
            progress=not args.quiet_progress,
        )
        if len(results) != 1:
            raise RuntimeError(f"Expected one gate result, got {results}")
        probe_gate = dict(results[0])
        probe_gate["band"] = band
        probe_gate["source"] = "probe"
        probe_gate["active"] = int(probe_gate.get("decode_lines", 0) or 0) >= int(args.gate_min_decodes)

    if gate_source in {"users", "either", "both"}:
        users_gate = _control_users_gate(args, band=band)

    if gate_source == "probe":
        assert probe_gate is not None
        return probe_gate
    if gate_source == "users":
        assert users_gate is not None
        return users_gate

    probe_active = bool(probe_gate and probe_gate.get("active"))
    users_active = bool(users_gate and users_gate.get("active"))
    combined = {
        "band": band,
        "source": gate_source,
        "probe": probe_gate,
        "users": users_gate,
        "active": (probe_active or users_active) if gate_source == "either" else (probe_active and users_active),
    }
    return combined


def _paired_compare(args: argparse.Namespace, *, band: str) -> dict[str, Any]:
    ours = ProbeTarget(label=f"ours_{band}", host=args.ours_host, port=args.ours_port)
    control = ProbeTarget(label=f"control_{band}", host=args.control_host, port=args.control_port)
    results = _run_probe(
        args.container,
        [ours, control],
        freq_hz=FT8_FREQS_HZ[band],
        duration_s=args.compare_seconds,
        gain=DEFAULT_GAIN,
        progress=not args.quiet_progress,
    )
    by_label = {str(item.get('label')): item for item in results}
    ours_result = by_label.get(ours.label)
    control_result = by_label.get(control.label)
    if not isinstance(ours_result, dict) or not isinstance(control_result, dict):
        raise RuntimeError(f"Paired compare returned incomplete results: {results}")
    return {
        "band": band,
        "freq_hz": FT8_FREQS_HZ[band],
        "ours": ours_result,
        "control": control_result,
        "delta_decode_lines": int(ours_result.get("decode_lines", 0) or 0)
        - int(control_result.get("decode_lines", 0) or 0),
    }


def main() -> int:
    args = _parse_args()
    band = str(args.band)
    previous_handlers, cleanup_state = _install_signal_handlers()
    wait_deadline = 0.0
    if int(args.wait_timeout_seconds) > 0:
        wait_deadline = time.time() + int(args.wait_timeout_seconds)

    gate: dict[str, Any] | None = None
    try:
        if args.skip_gate:
            _log("skip_gate", band=band)
        else:
            attempt = 0
            while True:
                attempt += 1
                gate = _control_gate(args, band=band)
                _log("control_gate", attempt=attempt, **gate)
                if gate.get("active"):
                    break
                if wait_deadline <= 0.0 or time.time() >= wait_deadline:
                    _log("no_active_window", band=band, attempts=attempt)
                    return 2
                time.sleep(max(1, int(args.gate_interval_seconds)))

        if args.gate_only:
            return 0

        original_settings = _load_settings(args.api_base)
        original_fixed_mode = bool(original_settings.get("fixedModeEnabled", True))
        paused = False
        try:
            if original_fixed_mode:
                _log("pause_fixed_receivers", band=band)
                _save_settings(args.api_base, {"fixedModeEnabled": False})
                _confirm_fixed_mode(args.api_base, expected=False, timeout_s=20)
                _wait_for_fixed_state(args.api_base, expect_active=False)
                _wait_for_container_orphan_cleanup(args.container)
                post_pause_grace = max(0, int(args.post_pause_grace_seconds))
                if post_pause_grace > 0:
                    _log("post_pause_grace", band=band, seconds=post_pause_grace)
                    time.sleep(float(post_pause_grace))
                paused = True
            comparison = _paired_compare(args, band=band)
            _log("paired_compare", **comparison)
        finally:
            if paused and not args.keep_paused:
                cleanup_state["in_cleanup"] = True
                try:
                    _restore_fixed_receivers(
                        args.api_base,
                        band=band,
                        expected_fixed_mode=original_fixed_mode,
                    )
                finally:
                    cleanup_state["in_cleanup"] = False
    except TerminatedBySignal as exc:
        _log("terminated", band=band, signal=exc.signum)
        return 128 + int(exc.signum)
    finally:
        pending_signal = cleanup_state.get("pending_signal")
        _restore_signal_handlers(previous_handlers)
        if pending_signal is not None:
            return 128 + int(pending_signal)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)