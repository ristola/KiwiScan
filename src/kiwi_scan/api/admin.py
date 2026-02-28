from __future__ import annotations

import json
import os
import plistlib
import re
import subprocess
import urllib.request
from pathlib import Path

from fastapi import APIRouter

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
            if ("kiwi_scan" not in hay) and ("run_server.sh" not in hay) and ("shackmate" not in hay):
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
    pat = re.compile(r'"([^"]+)"\s*=>\s*(true|false)', re.IGNORECASE)
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
            if label and val == "true":
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

    return router
