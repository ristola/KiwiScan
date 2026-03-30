from __future__ import annotations

import json
import os
import plistlib
import re
import subprocess
import urllib.request
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from ..ws4010_server import restart_ws4010


def _launchd_candidates() -> list[dict[str, str]]:
    roots = [
        Path.home() / "Library" / "LaunchAgents",
        Path("/Library/LaunchAgents"),
        Path("/Library/LaunchDaemons"),
    ]
    out: dict[str, dict[str, str]] = {}
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for path in root.glob("*.plist"):
            try:
                payload = plistlib.loads(path.read_bytes())
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            label = str(payload.get("Label") or "").strip()
            program = str(payload.get("Program") or "").strip()
            program_args = payload.get("ProgramArguments")
            arg_text = ""
            if isinstance(program_args, list):
                try:
                    arg_text = " ".join(str(v) for v in program_args)
                except Exception:
                    arg_text = ""
            hay = f"{label} {program} {arg_text} {path}".lower()
            if (
                ("kiwi_scan" not in hay)
                and ("run_server.sh" not in hay)
                and ("kiwi-scan-web" not in hay)
            ):
                continue
            if label:
                out[label] = {
                    "label": label,
                    "plist": str(path),
                }
    return sorted(out.values(), key=lambda x: x.get("label") or "")


def _launchctl_disabled_labels() -> set[str]:
    disabled: set[str] = set()
    uid = os.getuid()
    domains = [f"gui/{uid}", "system"]
    pat = re.compile(r'"([^"]+)"\s*=>\s*(true|false|disabled|enabled)', re.IGNORECASE)
    for domain in domains:
        try:
            proc = subprocess.run(
                ["launchctl", "print-disabled", domain],
                capture_output=True,
                text=True,
                timeout=2.5,
                check=False,
            )
        except Exception:
            continue
        if proc.returncode != 0:
            continue
        for m in pat.finditer(proc.stdout or ""):
            label = str(m.group(1) or "").strip()
            val = str(m.group(2) or "").strip().lower()
            if label and val in {"true", "disabled"}:
                disabled.add(label)
    return disabled


def _launchd_status() -> dict[str, object]:
    candidates = _launchd_candidates()
    disabled_labels = _launchctl_disabled_labels()
    if not candidates:
        return {
            "launchd_enabled": False,
            "launchd_running": False,
            "launchd_labels": [],
        }

    enabled = False
    running = False
    labels: list[str] = []
    uid = os.getuid()

    for item in candidates:
        label = str(item.get("label") or "").strip()
        if not label:
            continue
        labels.append(label)
        is_disabled = label in disabled_labels
        if not is_disabled:
            enabled = True

        is_running = False
        for target in (f"gui/{uid}/{label}", f"system/{label}"):
            try:
                proc = subprocess.run(
                    ["launchctl", "print", target],
                    capture_output=True,
                    text=True,
                    timeout=2.5,
                    check=False,
                )
            except Exception:
                continue
            if proc.returncode == 0:
                txt = (proc.stdout or "").lower()
                if ("state = running" in txt) or ("pid =" in txt):
                    is_running = True
                    break
        if is_running:
            running = True

    return {
        "launchd_enabled": bool(enabled),
        "launchd_running": bool(running),
        "launchd_labels": labels,
    }


def _set_launchd_enabled(enabled: bool) -> dict[str, object]:
    uid = os.getuid()
    candidates = _launchd_candidates()
    labels = [str(item.get("label") or "").strip() for item in candidates if str(item.get("label") or "").strip()]
    if not labels:
        return {
            "ok": True,
            "changed": False,
            "enabled": bool(enabled),
            "labels": [],
            "updated": 0,
            "failed": [],
        }

    action = "enable" if enabled else "disable"
    updated = 0
    failed: list[dict[str, str]] = []

    def _scoped_targets(label: str, plist_path: str | None) -> list[tuple[str, str]]:
        gui_domain = f"gui/{uid}"
        system_domain = "system"
        plist = str(plist_path or "")
        if plist.startswith(str(Path.home() / "Library" / "LaunchAgents")):
            return [(gui_domain, f"{gui_domain}/{label}")]
        if plist.startswith("/Library/LaunchDaemons"):
            return [(system_domain, f"{system_domain}/{label}")]
        return [
            (gui_domain, f"{gui_domain}/{label}"),
            (system_domain, f"{system_domain}/{label}"),
        ]

    for item in candidates:
        label = str(item.get("label") or "").strip()
        plist = str(item.get("plist") or "").strip()
        if not label:
            continue
        scoped_targets = _scoped_targets(label, plist)
        target_updated = False
        target_failed = 0
        for _, scoped in scoped_targets:
            try:
                proc = subprocess.run(
                    ["launchctl", action, scoped],
                    capture_output=True,
                    text=True,
                    timeout=3.0,
                    check=False,
                )
            except Exception as e:
                target_failed += 1
                failed.append({"label": label, "scope": scoped, "error": str(e)})
                continue

            if proc.returncode == 0:
                target_updated = True
            else:
                target_failed += 1
                err = (proc.stderr or proc.stdout or "launchctl failed").strip()
                failed.append({"label": label, "scope": scoped, "error": err})

        if target_updated:
            updated += 1

    started = 0
    if enabled:
        for item in candidates:
            label = str(item.get("label") or "").strip()
            plist = str(item.get("plist") or "").strip()
            if not label:
                continue
            scoped_targets = _scoped_targets(label, plist)
            start_ok = False
            for domain, scoped in scoped_targets:
                try:
                    loaded = subprocess.run(
                        ["launchctl", "print", scoped],
                        capture_output=True,
                        text=True,
                        timeout=3.0,
                        check=False,
                    )
                except Exception:
                    loaded = None

                needs_bootstrap = (loaded is None) or (loaded.returncode != 0)
                if needs_bootstrap and plist:
                    try:
                        subprocess.run(
                            ["launchctl", "bootstrap", domain, plist],
                            capture_output=True,
                            text=True,
                            timeout=4.0,
                            check=False,
                        )
                    except Exception:
                        pass

                try:
                    kick = subprocess.run(
                        ["launchctl", "kickstart", "-k", scoped],
                        capture_output=True,
                        text=True,
                        timeout=4.0,
                        check=False,
                    )
                except Exception as e:
                    failed.append({"label": label, "scope": scoped, "error": str(e)})
                    continue

                if kick.returncode == 0:
                    start_ok = True
                    break
                err = (kick.stderr or kick.stdout or "launchctl kickstart failed").strip()
                failed.append({"label": label, "scope": scoped, "error": err})

            if start_ok:
                started += 1

    return {
        "ok": True,
        "changed": updated > 0,
        "enabled": bool(enabled),
        "labels": labels,
        "updated": int(updated),
        "started": int(started),
        "failed": failed,
    }


def make_router(*, auto_set_loop: object | None = None) -> APIRouter:
    """Create router for admin endpoints."""

    router = APIRouter()

    @router.post("/admin/ws4010/restart")
    def restart_ws4010_endpoint() -> dict:
        restart_ws4010()
        return {"status": "ok"}

    @router.get("/admin/ws4010/status")
    def ws4010_status_endpoint() -> dict:
        try:
            req = urllib.request.Request("http://127.0.0.1:4010/ws_status")
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8", "ignore"))
            return {
                "ok": True,
                "ws4010_clients": int(data.get("ws4010_clients", 0) or 0),
                "ws4010_total_clients": int(data.get("ws4010_total_clients", 0) or 0),
                "source": "ws4010",
            }
        except Exception:
            return {
                "ok": False,
                "ws4010_clients": 0,
                "ws4010_total_clients": 0,
                "source": "ws4010",
            }

    @router.get("/admin/headless/status")
    def headless_status_endpoint() -> dict:
        if auto_set_loop is None:
            return {
                "ok": False,
                "error": "auto_set_loop_unavailable",
            }
        try:
            status = auto_set_loop.status()  # type: ignore[attr-defined]
        except Exception:
            return {
                "ok": False,
                "error": "auto_set_loop_status_failed",
            }
        if not isinstance(status, dict):
            status = {}
        out = {"ok": True}
        out.update(status)
        out.update(_launchd_status())
        return out

    @router.post("/admin/launchd/set")
    async def launchd_set_endpoint(request: Request) -> dict:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be an object")
        enabled = bool(payload.get("enabled", False))
        return _set_launchd_enabled(enabled)

    @router.post("/admin/force-reassign")
    def force_reassign_endpoint() -> dict:
        """Re-apply the current schedule assignments immediately.

        Reads the saved automation settings and calls /auto_set_receivers with
        ``force=True``, bypassing the endpoint's dedup cache.  This kicks any
        receivers running on bands that are no longer selected and corrects Kiwi
        slot drift detected by the assignment reconcile logic.

        The reassign runs in a background thread so the response is instant.
        """
        if auto_set_loop is None:
            raise HTTPException(status_code=503, detail="auto_set_loop unavailable")
        import threading

        def _bg() -> None:
            try:
                auto_set_loop.force_reassign()  # type: ignore[attr-defined]
            except Exception:
                pass

        threading.Thread(target=_bg, daemon=True, name="force-reassign-bg").start()
        return {"ok": True, "status": "reassigning"}

    return router
