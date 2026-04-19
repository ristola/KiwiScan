from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict

from fastapi import APIRouter

from .decodes import get_published_decode_stats_by_rx


def make_router(*, receiver_mgr: object, af2udp_path: Path, ft8modem_path: Path) -> APIRouter:
    """Create router for decode process status.

    This is a small extraction to keep server.py slimmer without changing behavior.
    """

    router = APIRouter()
    _last_live_assignments: Dict[int, Dict[str, object]] = {}
    _last_live_ok_unix: float = 0.0
    _last_live_host: str = ""
    _last_live_port: int = 0
    _last_status_payload: Dict[str, object] | None = None

    @router.get("/decodes/status")
    def get_decode_status():
        nonlocal _last_live_assignments, _last_live_ok_unix, _last_live_host, _last_live_port, _last_status_payload
        rx_status: Dict[int, Dict[str, object]] = {}
        workers: Dict[int, object] = {}
        # Use a timeout so this endpoint never blocks indefinitely while
        # apply_assignments() holds the lock during startup/eviction (can be minutes).
        _lock = receiver_mgr._lock  # type: ignore[attr-defined]
        if not _lock.acquire(timeout=0.5):
            if isinstance(_last_status_payload, dict):
                cached = dict(_last_status_payload)
                cached["_from_cache"] = True
                return cached
            return {
                "assignments": {},
                "assignments_source": "busy",
                "assignments_host": "",
                "assignments_port": 0,
                "assignments_mismatch_rxs": [],
                "logs": [],
                "af2udp": str(af2udp_path),
                "ft8modem": str(ft8modem_path),
                "_from_cache": True,
            }
        try:
            assignments = dict(receiver_mgr._assignments)  # type: ignore[attr-defined]
            workers = dict(getattr(receiver_mgr, "_workers", {}))  # type: ignore[attr-defined]
        finally:
            _lock.release()
        for rx, a in assignments.items():
            rx_status[int(rx)] = {
                "band": a.band,
                "mode": a.mode_label,
                "freq_hz": a.freq_hz,
            }

        def _mode_token_set(value: object) -> set[str]:
            raw = str(value or "").strip().upper().replace("_", " ").replace("-", " ")
            if not raw:
                return set()
            if raw == "ALL" or raw == "MIX":
                return {"FT4", "FT8", "WSPR"}
            tokens: set[str] = set()
            if "FT4" in raw:
                tokens.add("FT4")
            if "FT8" in raw:
                tokens.add("FT8")
            if "WSPR" in raw or re.search(r"\bWS\b", raw):
                tokens.add("WSPR")
            if raw in {"SSB", "PHONE", "USB", "LSB", "AM"} or "SSB" in raw:
                tokens.add("SSB")
            if tokens:
                return tokens
            return {raw}

        def _normalize_mode_label(value: object) -> str:
            raw = str(value or "").strip().upper()
            if raw in {"ALL", "MIX"}:
                return raw
            tokens = _mode_token_set(value)
            if not tokens:
                return ""
            order = ["FT4", "FT8", "WSPR", "SSB"]
            ordered = [token for token in order if token in tokens]
            ordered.extend(sorted(token for token in tokens if token not in order))
            return " / ".join(ordered)

        def _fetch_live_users_assignments(host: str, port: int) -> tuple[bool, Dict[int, Dict[str, object]]]:
            return _fetch_live_users_assignments_with_details(host, port)[0:2]  # pragma: no cover

        def _parse_elapsed_seconds(value: object) -> int:
            raw = str(value or "").strip()
            if not raw:
                return 0
            parts = raw.split(":")
            try:
                numbers = [int(part) for part in parts]
            except Exception:
                return 0
            if len(numbers) == 3:
                hours, minutes, seconds = numbers
                return max(0, (hours * 3600) + (minutes * 60) + seconds)
            if len(numbers) == 2:
                minutes, seconds = numbers
                return max(0, (minutes * 60) + seconds)
            return 0

        def _assignment_from_auto_label(label: object, *, fallback_freq_hz: object = None) -> Dict[str, object] | None:
            name = str(label or "").strip().strip('"\'')
            if not name:
                return None
            match = re.search(r"^(?:AUTO|FIXED|ROAM(?:\d+)?)_([^_]+)_(.+)$", name, flags=re.IGNORECASE)
            if not match:
                match = re.search(r"^(?:AUTO|FIXED|ROAM\d)(\d+[mM])([A-Z0-9/\-]+)$", name, flags=re.IGNORECASE)
            if not match:
                return None
            freq_hz = None
            try:
                if fallback_freq_hz is not None:
                    freq_hz = float(fallback_freq_hz)
            except Exception:
                freq_hz = None
            return {
                "band": str(match.group(1)).strip().lower(),
                "mode": _normalize_mode_label(match.group(2)),
                "freq_hz": freq_hz,
            }

        def _assignment_matches(left: Dict[str, object], right: Dict[str, object]) -> bool:
            left_band = str(left.get("band") or "").strip().lower()
            right_band = str(right.get("band") or "").strip().lower()
            left_mode = _mode_token_set(left.get("mode"))
            right_mode = _mode_token_set(right.get("mode"))
            if not left_band or not right_band or left_band != right_band:
                return False
            if not left_mode or not right_mode:
                return False
            if not (left_mode.issubset(right_mode) or right_mode.issubset(left_mode)):
                return False
            try:
                left_freq_hz = float(left.get("freq_hz"))
                right_freq_hz = float(right.get("freq_hz"))
            except Exception:
                return True
            return abs(left_freq_hz - right_freq_hz) <= 500.0

        worker_status: Dict[int, Dict[str, object]] = {}
        for rx, worker in workers.items():
            fallback = assignments.get(int(rx))
            worker_entry = _assignment_from_auto_label(
                getattr(worker, "_active_user_label", ""),
                fallback_freq_hz=getattr(fallback, "freq_hz", None),
            )
            if worker_entry is not None:
                worker_status[int(rx)] = worker_entry

        reference_status: Dict[int, Dict[str, object]] = dict(rx_status)
        reference_status.update(worker_status)

        def _fetch_live_users_assignments_with_details(host: str, port: int) -> tuple[bool, Dict[int, Dict[str, object]], Dict[int, Dict[str, object]]]:
            if not host or port <= 0:
                return (False, {}, {})
            payload = None
            for path in ("/users?json=1", "/users?admin=1", "/users"):
                try:
                    req = urllib.request.Request(f"http://{host}:{port}{path}", method="GET")
                    with urllib.request.urlopen(req, timeout=0.5) as resp:
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
                return (False, {}, {})

            out: Dict[int, Dict[str, object]] = {}
            details: Dict[int, Dict[str, object]] = {}
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
                parsed = _assignment_from_auto_label(name, fallback_freq_hz=row.get("f"))
                if parsed is None:
                    continue
                out[int(rx)] = parsed
                details[int(rx)] = {
                    "user_label": name,
                    "age_seconds": _parse_elapsed_seconds(row.get("t")),
                }
            return (True, out, details)

        def _live_users_assignments() -> tuple[bool, Dict[int, Dict[str, object]], Dict[int, Dict[str, object]], str, int]:
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
                return (False, {}, {}, "", 0)

            # Short-circuit: if the last fetch was very recent, reuse it rather than
            # making blocking Kiwi HTTP calls on every poll tick.
            if _last_live_ok_unix > 0.0 and (time.time() - _last_live_ok_unix) <= 5.0 and _last_live_assignments:
                return (True, dict(_last_live_assignments), {}, _last_live_host, _last_live_port)

            best_assignments: Dict[int, Dict[str, object]] = {}
            best_details: Dict[int, Dict[str, object]] = {}
            best_host = ""
            best_port = 0
            best_score = -1
            best_count = -1

            for host, port in candidates:
                ok, rows, details = _fetch_live_users_assignments_with_details(host, port)
                if not ok:
                    continue
                score = 0
                for rx, row in rows.items():
                    expected = reference_status.get(int(rx)) if isinstance(reference_status, dict) else None
                    if not isinstance(expected, dict):
                        continue
                    if _assignment_matches(expected, row):
                        score += 3
                count = len(rows)
                if score > best_score or (score == best_score and count > best_count):
                    best_score = score
                    best_count = count
                    best_assignments = rows
                    best_details = details
                    best_host = host
                    best_port = port

            if best_count < 0:
                return (False, {}, {}, "", 0)
            return (True, best_assignments, best_details, best_host, best_port)

        def _should_prefer_receiver_manager(
            expected_assignments: Dict[int, Dict[str, object]],
            live_assignments: Dict[int, Dict[str, object]],
            live_details: Dict[int, Dict[str, object]],
        ) -> tuple[bool, list[int]]:
            unmatched_expected = list(expected_assignments.keys())
            unmatched_live = list(live_assignments.keys())
            for rx in list(unmatched_expected):
                expected = expected_assignments[rx]
                for lr in list(unmatched_live):
                    live_row = live_assignments.get(lr)
                    if not isinstance(live_row, dict):
                        continue
                    if _assignment_matches(expected, live_row):
                        unmatched_expected.remove(rx)
                        unmatched_live.remove(lr)
                        break
            if len(unmatched_expected) > 0:
                return (True, unmatched_expected)
            return (False, [])

        def _health_summary_prefers_receiver_manager(
            expected_assignments: Dict[int, Dict[str, object]],
        ) -> tuple[bool, list[int]]:
            try:
                summary = receiver_mgr.health_summary()  # type: ignore[attr-defined]
            except Exception:
                return (False, [])
            if not isinstance(summary, dict):
                return (False, [])
            channels = summary.get("channels")
            if not isinstance(channels, dict):
                return (False, [])

            mismatch_rxs: list[int] = []
            for rx_key, channel in channels.items():
                if not isinstance(channel, dict):
                    continue
                try:
                    rx = int(rx_key)
                except Exception:
                    continue
                if rx not in expected_assignments:
                    continue
                reason = str(channel.get("last_reason") or "").strip().lower()
                state = str(channel.get("health_state") or "").strip().lower()
                # Reason may be "kiwi_assignment_mismatch" or "kiwi_assignment_mismatch_observed"
                if state == "stalled":
                    mismatch_rxs.append(rx)

            mismatch_rxs = sorted(set(mismatch_rxs))
            # Only prefer internal state when >75% of channels show mismatch,
            # so partial transition states still show live KiwiSDR data.
            threshold = max(2, int(len(expected_assignments) * 0.75) + 1)
            if len(mismatch_rxs) < threshold:
                return (False, [])
            return (True, mismatch_rxs)

        users_available, live_status, live_details, live_host, live_port = _live_users_assignments()
        now = time.time()
        prefer_receiver_manager, mismatch_rxs = _should_prefer_receiver_manager(reference_status, live_status, live_details)
        if not prefer_receiver_manager:
            prefer_receiver_manager, mismatch_rxs = _health_summary_prefers_receiver_manager(reference_status)
        if users_available and not prefer_receiver_manager:
            _last_live_assignments = dict(live_status)
            _last_live_ok_unix = float(now)
            _last_live_host = str(live_host)
            _last_live_port = int(live_port)

        cache_fresh = (_last_live_ok_unix > 0.0) and ((now - _last_live_ok_unix) <= 30.0)
        if users_available and not prefer_receiver_manager:
            assignments_out = live_status
            source = "kiwi_users"
        elif prefer_receiver_manager:
            assignments_out = reference_status
            source = "receiver_manager_drift"
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

        payload = {
            "assignments": assignments_out,
            "assignments_source": source,
            "assignments_host": live_host,
            "assignments_port": live_port,
            "assignments_mismatch_rxs": mismatch_rxs,
            "published_decode_stats_by_rx": get_published_decode_stats_by_rx(),
            "logs": logs,
            "af2udp": str(af2udp_path),
            "ft8modem": str(ft8modem_path),
        }
        _last_status_payload = dict(payload)
        return payload

    return router
