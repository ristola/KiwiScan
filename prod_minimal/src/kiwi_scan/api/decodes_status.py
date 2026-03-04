from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict

from fastapi import APIRouter


def make_router(*, receiver_mgr: object, af2udp_path: Path, ft8modem_path: Path) -> APIRouter:
    """Create router for decode process status.

    This is a small extraction to keep server.py slimmer without changing behavior.
    """

    router = APIRouter()
    _last_live_assignments: Dict[int, Dict[str, object]] = {}
    _last_live_ok_unix: float = 0.0

    @router.get("/decodes/status")
    def get_decode_status():
        nonlocal _last_live_assignments, _last_live_ok_unix
        rx_status: Dict[int, Dict[str, object]] = {}
        # receiver_mgr is a ReceiverManager instance; access its assignments under lock.
        with receiver_mgr._lock:  # type: ignore[attr-defined]
            assignments = dict(receiver_mgr._assignments)  # type: ignore[attr-defined]
        for rx, a in assignments.items():
            rx_status[int(rx)] = {
                "band": a.band,
                "mode": a.mode_label,
                "freq_hz": a.freq_hz,
            }

        def _live_users_assignments() -> tuple[bool, Dict[int, Dict[str, object]]]:
            host = str(getattr(receiver_mgr, "_active_host", "") or "").strip()
            port = int(getattr(receiver_mgr, "_active_port", 0) or 0)
            if (not host or port <= 0):
                host = str(getattr(receiver_mgr, "_host", "") or "").strip()
                port = int(getattr(receiver_mgr, "_port", 0) or 0)
            if (not host or port <= 0):
                try:
                    cfg_path = Path(__file__).resolve().parents[3] / "outputs" / "config.json"
                    cfg = json.loads(cfg_path.read_text(encoding="utf-8", errors="ignore"))
                    if isinstance(cfg, dict):
                        host = str(cfg.get("host") or "").strip() or host
                        port = int(cfg.get("port") or 0) or port
                except Exception:
                    pass
            if not host or port <= 0:
                return (False, {})
            payload = None
            for path in ("/users?json=1", "/users?admin=1", "/users"):
                try:
                    req = urllib.request.Request(f"http://{host}:{port}{path}", method="GET")
                    with urllib.request.urlopen(req, timeout=2.0) as resp:
                        maybe = json.loads(resp.read().decode("utf-8", errors="ignore"))
                    if isinstance(maybe, list):
                        payload = maybe
                        has_details = any(
                            isinstance(row, dict) and (row.get("n") or row.get("f") or row.get("m"))
                            for row in maybe
                        )
                        if has_details:
                            break
                except Exception:
                    continue

            if not isinstance(payload, list):
                return (False, {})

            out: Dict[int, Dict[str, object]] = {}
            for row in payload:
                if not isinstance(row, dict):
                    continue
                try:
                    rx = int(row.get("i"))
                except Exception:
                    continue

                name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                if not name:
                    continue
                m = re.match(r"^AUTO_([^_]+)_(.+)$", name, flags=re.IGNORECASE)
                if not m:
                    continue
                band = str(m.group(1)).strip().lower()
                mode = str(m.group(2)).strip().upper().replace("_", " ")
                try:
                    freq_hz = float(row.get("f"))
                except Exception:
                    freq_hz = None

                out[int(rx)] = {
                    "band": band,
                    "mode": mode,
                    "freq_hz": freq_hz,
                }
            return (True, out)

        users_available, live_status = _live_users_assignments()
        now = __import__("time").time()
        if users_available:
            _last_live_assignments = dict(live_status)
            _last_live_ok_unix = float(now)

        cache_fresh = (_last_live_ok_unix > 0.0) and ((now - _last_live_ok_unix) <= 30.0)
        if users_available:
            assignments_out = live_status
            source = "kiwi_users"
        elif cache_fresh:
            assignments_out = dict(_last_live_assignments)
            source = "kiwi_users_cached"
        else:
            assignments_out = rx_status
            source = "receiver_manager"

        def _tail_lines(path: Path, max_lines: int = 10) -> list[str]:
            if not path.exists():
                return []
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                return []
            lines = [ln for ln in text.splitlines() if ln.strip()]
            return lines[-max_lines:]

        logs = []
        for rx in range(2, 8):
            dec_log = Path("/tmp") / f"ft8modem_rx{rx}.log"
            dec_log_ft4 = Path("/tmp") / f"ft8modem_rx{rx}_ft4.log"
            dec_log_ft8 = Path("/tmp") / f"ft8modem_rx{rx}_ft8.log"
            pipe_log = Path("/tmp") / f"kiwi_rx{rx}_pipeline.log"
            logs.append(
                {
                    "rx": rx,
                    "decoder_log": str(dec_log),
                    "decoder_exists": dec_log.exists(),
                    "decoder_size": dec_log.stat().st_size if dec_log.exists() else 0,
                    "decoder_tail": _tail_lines(dec_log),
                    "decoder_ft4_log": str(dec_log_ft4),
                    "decoder_ft4_exists": dec_log_ft4.exists(),
                    "decoder_ft4_size": dec_log_ft4.stat().st_size if dec_log_ft4.exists() else 0,
                    "decoder_ft4_tail": _tail_lines(dec_log_ft4),
                    "decoder_ft8_log": str(dec_log_ft8),
                    "decoder_ft8_exists": dec_log_ft8.exists(),
                    "decoder_ft8_size": dec_log_ft8.stat().st_size if dec_log_ft8.exists() else 0,
                    "decoder_ft8_tail": _tail_lines(dec_log_ft8),
                    "pipeline_log": str(pipe_log),
                    "pipeline_exists": pipe_log.exists(),
                    "pipeline_size": pipe_log.stat().st_size if pipe_log.exists() else 0,
                    "pipeline_tail": _tail_lines(pipe_log),
                }
            )

        return {
            "assignments": assignments_out,
            "assignments_source": source,
            "logs": logs,
            "af2udp": str(af2udp_path),
            "ft8modem": str(ft8modem_path),
        }

    return router
