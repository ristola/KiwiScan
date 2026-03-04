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

        def _normalize_mode_label(value: object) -> str:
            raw = str(value or "").strip().upper().replace("_", " ").replace("-", " ")
            if not raw:
                return ""
            if "WSPR" in raw:
                return "WSPR"
            has_ft4 = "FT4" in raw
            has_ft8 = "FT8" in raw
            if has_ft4 and has_ft8:
                return "FT4 / FT8"
            if has_ft4:
                return "FT4"
            if has_ft8:
                return "FT8"
            if raw in {"SSB", "PHONE", "USB", "LSB", "AM"} or "SSB" in raw:
                return "SSB"
            return raw

        def _fetch_live_users_assignments(host: str, port: int) -> tuple[bool, Dict[int, Dict[str, object]]]:
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

                name = urllib.parse.unquote(str(row.get("n") or "")).strip().strip('"\'')
                if not name:
                    continue
                m = re.search(r"AUTO_([^_]+)_(.+)", name, flags=re.IGNORECASE)
                if not m:
                    continue
                band = str(m.group(1)).strip().lower()
                mode = _normalize_mode_label(m.group(2))
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

        def _live_users_assignments() -> tuple[bool, Dict[int, Dict[str, object]], str, int]:
            candidates: list[tuple[str, int]] = []

            def _add_candidate(host_value: object, port_value: object) -> None:
                host = str(host_value or "").strip()
                try:
                    port = int(port_value or 0)
                except Exception:
                    port = 0
                if not host or port <= 0:
                    return
                candidate = (host, port)
                if candidate not in candidates:
                    candidates.append(candidate)

            _add_candidate(getattr(receiver_mgr, "_active_host", ""), getattr(receiver_mgr, "_active_port", 0))
            _add_candidate(getattr(receiver_mgr, "_host", ""), getattr(receiver_mgr, "_port", 0))

            try:
                cfg_path = Path(__file__).resolve().parents[3] / "outputs" / "config.json"
                cfg = json.loads(cfg_path.read_text(encoding="utf-8", errors="ignore"))
                if isinstance(cfg, dict):
                    _add_candidate(cfg.get("host"), cfg.get("port"))
            except Exception:
                pass

            if not candidates:
                return (False, {}, "", 0)

            best_assignments: Dict[int, Dict[str, object]] = {}
            best_host = ""
            best_port = 0
            best_score = -1
            best_count = -1

            for host, port in candidates:
                ok, rows = _fetch_live_users_assignments(host, port)
                if not ok:
                    continue
                score = 0
                for rx, row in rows.items():
                    expected = rx_status.get(int(rx)) if isinstance(rx_status, dict) else None
                    if not isinstance(expected, dict):
                        continue
                    exp_band = str(expected.get("band") or "").strip().lower()
                    exp_mode = _normalize_mode_label(expected.get("mode"))
                    got_band = str(row.get("band") or "").strip().lower()
                    got_mode = _normalize_mode_label(row.get("mode"))
                    if got_band and got_band == exp_band:
                        score += 1
                    if got_mode and got_mode == exp_mode:
                        score += 1
                count = len(rows)
                if score > best_score or (score == best_score and count > best_count):
                    best_score = score
                    best_count = count
                    best_assignments = rows
                    best_host = host
                    best_port = port

            if best_count < 0:
                return (False, {}, "", 0)
            return (True, best_assignments, best_host, best_port)

        users_available, live_status, live_host, live_port = _live_users_assignments()
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
            live_host = ""
            live_port = 0
        else:
            assignments_out = rx_status
            source = "receiver_manager"
            live_host = ""
            live_port = 0

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
            "assignments_host": live_host,
            "assignments_port": live_port,
            "logs": logs,
            "af2udp": str(af2udp_path),
            "ft8modem": str(ft8modem_path),
        }

    return router
