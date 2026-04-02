from __future__ import annotations

import ast
import asyncio
import json
import logging
import re
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import APIRouter, WebSocket
from fastapi.websockets import WebSocketDisconnect

from ..scheduler import block_for_hour, expected_schedule_by_season, season_for_date
from ..udp4010_server import publish_udp4010

logger = logging.getLogger(__name__)

router = APIRouter()

_decode_ws_lock = threading.Lock()
_decode_ws_clients: set[WebSocket] = set()

_decode_lock = threading.Lock()
_decode_seq = 0
_decode_buffer: deque[Dict] = deque(maxlen=500)
_decode_times: deque[float] = deque(maxlen=5000)

# Server-side band activity chart buckets: fixed 15-second wall-clock intervals so the
# data is accurate regardless of browser tab visibility or polling throttling.
_CHART_BUCKET_S = 15.0
_CHART_MAX_BUCKETS = 60  # 15 minutes of history
_chart_lock = threading.Lock()
_chart_buckets: deque = deque(maxlen=_CHART_MAX_BUCKETS)
_chart_running: Dict[str, Any] = {}  # {"ts": float, "bands": {band: {"RXn|MODE": count}}}

_loop: asyncio.AbstractEventLoop | None = None
_loop_4010: asyncio.AbstractEventLoop | None = None

_decode_ws4010_lock = threading.Lock()
_decode_ws4010_clients: set[WebSocket] = set()
_decode_ws4010_dashboard_clients: set[WebSocket] = set()
_ws4010_debug_events: deque[Dict[str, Any]] = deque(maxlen=200)

_automation_lock = threading.Lock()
_valid_bands = ("10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m")
_valid_band_modes = {"FT4", "FT4 / FT8", "FT8", "WSPR", "SSB"}


def set_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop
    _loop = loop


def set_ws4010_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop_4010
    _loop_4010 = loop


def _record_ws4010_debug(event: str, **fields: Any) -> None:
    row: Dict[str, Any] = {
        "ts": datetime.now().astimezone().isoformat(timespec="seconds"),
        "event": event,
    }
    row.update(fields)
    with _decode_ws4010_lock:
        _ws4010_debug_events.append(row)


def _automation_settings_path() -> Path:
    return Path(__file__).resolve().parents[3] / "outputs" / "automation_settings.json"


def _load_automation_settings() -> Dict[str, Any]:
    path = _automation_settings_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_automation_settings(payload: Dict[str, Any]) -> None:
    path = _automation_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_band_mode(value: object) -> str:
    raw = str(value or "FT8").strip().upper().replace("_", " ")
    if raw in {"FT4/FT8", "FT4 + FT8", "FT4-FT8"}:
        return "FT4 / FT8"
    if raw == "WSRP":
        return "WSPR"
    if raw in {"PHONE", "LSB", "USB"}:
        return "SSB"
    if raw in _valid_band_modes:
        return raw
    return "FT8"


def _blocks_for_mode(mode: str, block: str) -> List[str]:
    _ = str(block or "").strip()
    now = datetime.now().astimezone()
    current = block_for_hour(now.hour, mode=mode)
    blocks: List[str] = [current]
    m = re.match(r"^(\d{2})-(\d{2})$", current)
    if m:
        start, end = m.groups()
        if end == "24":
            blocks.append(f"{start}-00")
        if end == "00":
            blocks.append(f"{start}-24")
    dedup: List[str] = []
    for b in blocks:
        if b not in dedup:
            dedup.append(b)
    return dedup


def _block_sort_key(block_key: object) -> tuple[int, int] | None:
    raw = str(block_key or "").strip()
    m = re.match(r"^(\d{2})-(\d{2})$", raw)
    if not m:
        return None
    try:
        start = int(m.group(1))
        end = int(m.group(2))
    except Exception:
        return None
    if not (0 <= start <= 24 and 0 <= end <= 24):
        return None
    return start, end


def _fallback_profile_entry(by_mode: dict, block_key: str) -> tuple[str, dict] | tuple[None, None]:
    exact = by_mode.get(str(block_key))
    if isinstance(exact, dict):
        return str(block_key), exact

    target_key = _block_sort_key(block_key)
    if target_key is None:
        return None, None

    ordered: list[tuple[int, str, dict]] = []
    for candidate_key, candidate_entry in by_mode.items():
        if not isinstance(candidate_entry, dict):
            continue
        sort_key = _block_sort_key(candidate_key)
        if sort_key is None:
            continue
        ordered.append((sort_key[0], str(candidate_key), candidate_entry))
    if not ordered:
        return None, None

    ordered.sort(key=lambda item: item[0])
    target_start = target_key[0]
    prior = [(candidate_key, entry) for start, candidate_key, entry in ordered if start <= target_start]
    if prior:
        return prior[-1]
    _, candidate_key, entry = ordered[-1]
    return candidate_key, entry


def _trigger_auto_set_apply(settings: Dict[str, Any], mode: str) -> Dict[str, Any]:
    try:
        now = datetime.now().astimezone()
        block = block_for_hour(now.hour, mode=mode)
        candidate_blocks = _blocks_for_mode(mode, block)
        schedule_profiles = settings.get("scheduleProfiles") if isinstance(settings, dict) else {}
        by_mode = schedule_profiles.get(mode) if isinstance(schedule_profiles, dict) else {}
        selected_block = block
        entry = {}
        if isinstance(by_mode, dict):
            for candidate in candidate_blocks:
                candidate_entry = by_mode.get(candidate)
                if isinstance(candidate_entry, dict):
                    selected_block = candidate
                    entry = candidate_entry
                    break
            if not entry:
                fallback_block, fallback_entry = _fallback_profile_entry(by_mode, block)
                if isinstance(fallback_entry, dict) and fallback_block:
                    selected_block = str(fallback_block)
                    entry = fallback_entry

        selected_bands = entry.get("selectedBands") if isinstance(entry, dict) else []
        band_modes = entry.get("bandModes") if isinstance(entry, dict) else {}
        payload = {
            "enabled": True,
            "mode": mode,
            "block": selected_block,
            "selected_bands": selected_bands if isinstance(selected_bands, list) else [],
            "band_modes": band_modes if isinstance(band_modes, dict) else {},
            "wspr_scan_enabled": bool(settings.get("autoScanWspr", False)),
            "band_hop_seconds": int(settings.get("bandHopSeconds", 105) or 105),
            "wspr_start_band": str(settings.get("wsprStartBand") or "10m"),
        }

        req = urllib_request.Request(
            "http://127.0.0.1:4020/auto_set_receivers",
            data=json.dumps(payload).encode("utf-8"),
            headers={"content-type": "application/json"},
            method="POST",
        )
        with urllib_request.urlopen(req, timeout=3.0) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="ignore"))
        return {"ok": True, "block": selected_block, "response": body}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _apply_ws4010_band_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    mode_raw = str(payload.get("mode") or "").strip()
    profile_raw = str(payload.get("profile") or "").strip().lower()

    band_raw = str(payload.get("band") or payload.get("target_band") or "ALL").strip()
    band_upper = band_raw.upper()
    if band_upper == "ALL":
        band = "ALL"
    else:
        band = next((b for b in _valid_bands if b.lower() == band_raw.lower()), "")
        if not band:
            raise ValueError("band must be ALL or one of 10m..160m")

    if "enabled" not in payload:
        raise ValueError("enabled is required")
    enabled = bool(payload.get("enabled"))

    band_mode_raw = payload.get("band_mode")
    if band_mode_raw is None:
        band_mode_raw = payload.get("mode_value")
    if band_mode_raw is None:
        band_mode_raw = payload.get("bandMode")

    if band_mode_raw is None and mode_raw:
        mode_as_band_mode = _normalize_band_mode(mode_raw)
        if mode_as_band_mode in _valid_band_modes:
            band_mode_raw = mode_raw

    if profile_raw:
        mode = profile_raw
    elif band_mode_raw is not None and str(band_mode_raw).strip() != "":
        mode = "ft8"
    elif mode_raw.lower() in {"ft8", "phone"}:
        mode = mode_raw.lower()
    else:
        mode = "ft8"

    if mode not in {"ft8", "phone"}:
        raise ValueError("profile must be 'ft8' or 'phone'")

    has_band_mode = band_mode_raw is not None and str(band_mode_raw).strip() != ""
    band_mode = _normalize_band_mode(band_mode_raw) if has_band_mode else None
    block = str(payload.get("block") or "all").strip()
    target_blocks = _blocks_for_mode(mode, block)
    if not target_blocks:
        raise ValueError("No matching schedule block")

    with _automation_lock:
        settings = _load_automation_settings()
        schedule_profiles = settings.get("scheduleProfiles")
        if not isinstance(schedule_profiles, dict):
            schedule_profiles = {}
        by_mode = schedule_profiles.get(mode)
        if not isinstance(by_mode, dict):
            by_mode = {}

        for block_key in target_blocks:
            entry = by_mode.get(block_key)
            if not isinstance(entry, dict):
                entry = {}

            selected_raw = entry.get("selectedBands")
            if isinstance(selected_raw, list):
                selected = [b for b in _valid_bands if b in selected_raw]
            else:
                selected = [b for b in _valid_bands]

            if band == "ALL":
                selected = [b for b in _valid_bands] if enabled else []
            else:
                if enabled and band not in selected:
                    selected.append(band)
                if (not enabled) and band in selected:
                    selected = [b for b in selected if b != band]

            band_modes = entry.get("bandModes")
            if not isinstance(band_modes, dict):
                band_modes = {}

            if has_band_mode:
                if band == "ALL":
                    for b in _valid_bands:
                        band_modes[b] = band_mode
                else:
                    band_modes[band] = band_mode

            entry["selectedBands"] = selected
            entry["bandModes"] = band_modes
            by_mode[block_key] = entry

        schedule_profiles[mode] = by_mode
        settings["scheduleProfiles"] = schedule_profiles
        _save_automation_settings(settings)

    apply_result = _trigger_auto_set_apply(settings, mode)
    return {
        "ok": True,
        "action": "set_band",
        "mode": mode,
        "band": band,
        "enabled": enabled,
        "band_mode": band_mode if has_band_mode else "UNCHANGED",
        "blocks": target_blocks,
        "apply": apply_result,
    }


def _coerce_bool(value: object, *, field_name: str = "enabled") -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "enable", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disable", "disabled"}:
        return False
    raise ValueError(f"{field_name} must be true/false")


def _apply_ws4010_wspr_scan_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("enabled")
    if raw is None:
        raw = payload.get("auto_scan_wspr")
    if raw is None:
        raw = payload.get("value")
    if raw is None:
        raise ValueError("enabled is required")

    enabled = _coerce_bool(raw, field_name="enabled")
    mode_raw = str(payload.get("mode") or payload.get("profile") or "ft8").strip().lower()
    mode = mode_raw if mode_raw in {"ft8", "phone"} else "ft8"

    with _automation_lock:
        settings = _load_automation_settings()
        settings["autoScanWspr"] = enabled
        _save_automation_settings(settings)

    apply_result = _trigger_auto_set_apply(settings, mode)
    return {
        "ok": True,
        "action": "set_wspr_scan",
        "wspr_scan_enabled": enabled,
        "mode": mode,
        "apply": apply_result,
    }


def _apply_ws4010_discover_kiwi_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    port = int(payload.get("port", 8073) or 8073)
    timeout_s = float(payload.get("timeout_s", 0.20) or 0.20)
    max_hosts = int(payload.get("max_hosts", 32) or 32)
    url = f"http://127.0.0.1:4020/config/discover?port={port}&timeout_s={timeout_s}&max_hosts={max_hosts}"
    req = urllib_request.Request(url, method="GET")
    with urllib_request.urlopen(req, timeout=max(3.0, timeout_s * max_hosts + 1.0)) as resp:
        body = json.loads(resp.read().decode("utf-8", errors="ignore"))
    found = body.get("found") if isinstance(body, dict) else []
    count = len(found) if isinstance(found, list) else 0
    return {
        "ok": True,
        "action": "discover_kiwi",
        "count": count,
        "result": body,
    }


def _load_runtime_assignments() -> Dict[str, Any]:
    try:
        req = urllib_request.Request("http://127.0.0.1:4020/decodes/status", method="GET")
        with urllib_request.urlopen(req, timeout=2.0) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="ignore"))
        if isinstance(body, dict):
            assignments = body.get("assignments")
            return assignments if isinstance(assignments, dict) else {}
    except Exception:
        pass
    return {}


def _apply_ws4010_status_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    verbose_raw = payload.get("verbose")
    if isinstance(verbose_raw, bool):
        verbose = verbose_raw
    elif isinstance(verbose_raw, (int, float)):
        verbose = bool(verbose_raw)
    else:
        verbose = str(verbose_raw or "").strip().lower() in {"1", "true", "yes", "on", "full", "verbose"}

    mode_raw = str(payload.get("mode") or payload.get("profile") or "").strip().lower()

    with _automation_lock:
        settings = _load_automation_settings()

    if mode_raw not in {"ft8", "phone"}:
        auto_mode = str(settings.get("autoScanMode") or "ft8").strip().lower()
        mode = auto_mode if auto_mode in {"ft8", "phone"} else "ft8"
    else:
        mode = mode_raw

    now = datetime.now().astimezone()
    season = season_for_date(now)
    block = block_for_hour(now.hour, mode=mode)
    candidate_blocks = _blocks_for_mode(mode, block)

    schedule_profiles = settings.get("scheduleProfiles") if isinstance(settings, dict) else {}
    by_mode = schedule_profiles.get(mode) if isinstance(schedule_profiles, dict) else {}
    selected_block = block
    entry: Dict[str, Any] = {}
    if isinstance(by_mode, dict):
        for candidate in candidate_blocks:
            candidate_entry = by_mode.get(candidate)
            if isinstance(candidate_entry, dict):
                selected_block = candidate
                entry = candidate_entry
                break

    selected_raw = entry.get("selectedBands") if isinstance(entry, dict) else []
    has_explicit_selected_bands = isinstance(selected_raw, list)
    if isinstance(selected_raw, list):
        if len(selected_raw) == 0:
            selected_set = set(_valid_bands)
        else:
            selected_set = {b for b in selected_raw if b in _valid_bands}
    else:
        selected_set = set(_valid_bands)

    band_modes_raw = entry.get("bandModes") if isinstance(entry, dict) else {}
    band_modes: Dict[str, str] = {}
    if isinstance(band_modes_raw, dict):
        for b in _valid_bands:
            band_modes[b] = _normalize_band_mode(band_modes_raw.get(b) or "FT8")
    else:
        for b in _valid_bands:
            band_modes[b] = "FT8"

    seasonal_tables = expected_schedule_by_season(mode=mode)
    season_blocks = seasonal_tables.get(season) if isinstance(seasonal_tables, dict) else {}
    block_conditions = season_blocks.get(selected_block) if isinstance(season_blocks, dict) else {}
    if not isinstance(block_conditions, dict):
        block_conditions = {}

    runtime_assignments = _load_runtime_assignments()
    assignments_by_band: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(runtime_assignments, dict):
        for rx_key, row in runtime_assignments.items():
            if not isinstance(row, dict):
                continue
            band = str(row.get("band") or "").strip()
            if band not in _valid_bands:
                continue
            try:
                rx = int(rx_key)
            except Exception:
                try:
                    rx = int(row.get("rx"))
                except Exception:
                    continue
            assignments_by_band.setdefault(band, []).append(
                {
                    "rx": rx,
                    "mode": str(row.get("mode") or "").upper(),
                    "freq_hz": row.get("freq_hz"),
                }
            )

    if not has_explicit_selected_bands:
        assigned_set = {band for band, rows in assignments_by_band.items() if rows}
        if assigned_set:
            selected_set = assigned_set

    bands: List[Dict[str, Any]] = []
    active_conditions: List[str] = []
    total_assignments = 0
    for band in _valid_bands:
        condition = str(block_conditions.get(band) or "CLOSED").upper()
        assignments = assignments_by_band.get(band, [])
        if condition == "OPEN":
            active_conditions.append(band)
        total_assignments += len(assignments)
        bands.append(
            {
                "band": band,
                "selected": band in selected_set,
                "mode": band_modes.get(band, "FT8"),
                "condition": condition,
                "assignments": assignments,
            }
        )

    response: Dict[str, Any] = {
        "ok": True,
        "action": "settings",
        "message": "Compact status snapshot.",
        "mode": mode,
        "season": season,
        "block": selected_block,
        "verbose": verbose,
        "auto_scan_mode": str(settings.get("autoScanMode") or mode),
        "wspr_scan_enabled": bool(settings.get("autoScanWspr", False)),
        "wspr_start_band": str(settings.get("wsprStartBand") or "10m"),
        "band_hop_seconds": int(settings.get("bandHopSeconds", 105) or 105),
        "selected_bands": [b for b in _valid_bands if b in selected_set],
        "open_bands": active_conditions,
        "assignment_count": total_assignments,
        "band_settings": [
            {
                "band": band,
                "enabled": band in selected_set,
                "band_mode": band_modes.get(band, "FT8"),
            }
            for band in _valid_bands
        ],
        "status_lines": [
            f"band:{band} enabled:{str(band in selected_set).lower()} band_mode:{band_modes.get(band, 'FT8')}"
            for band in _valid_bands
        ],
    }
    if verbose:
        response["bands"] = bands
    return response


def _handle_ws4010_command(raw: str) -> Optional[Dict[str, Any]]:
    def _bool_from_text(text_value: str) -> Optional[bool]:
        m = re.search(r"\b(true|false|on|off|enable|enabled|disable|disabled)\b", str(text_value or "").lower())
        if not m:
            return None
        token = m.group(1)
        return token in {"true", "on", "enable", "enabled"}

    def _help_response() -> Dict[str, Any]:
        commands = ["set_band", "set_wspr_scan", "discover_kiwi", "status"]
        examples = [
            {"command": "set_band", "enabled": True, "band": "20m", "mode": "FT8"},
            {"command": "set_wspr_scan", "enabled": True},
            {"command": "discover_kiwi"},
            {"command": "status"},
            {"command": "status", "verbose": True},
        ]
        return {
            "ok": True,
            "type": "command_ack",
            "action": "help",
            "message": "Compact command reference.",
            "commands": commands,
            "examples": examples,
        }

    text = str(raw or "").strip()
    if not text:
        return None

    if text.lower() == "help":
        return _help_response()

    if text.lower() in {"ping", "{\"type\":\"ping\"}"}:
        return {"ok": True, "type": "pong"}

    lowered = text.lower()
    if not text.startswith("{"):
        if lowered in {"status", "get status", "get_status", "band status", "band_status", "settings", "show status"} or lowered.startswith("status "):
            try:
                verbose_flag = any(token in lowered for token in {"verbose", "full", "detail", "detailed"})
                result = _apply_ws4010_status_command({"verbose": True} if verbose_flag else {})
                result["type"] = "command_ack"
                return result
            except Exception as exc:
                logger.exception("ws4010 status text command failed")
                return {"ok": False, "type": "command_ack", "error": str(exc)}
        if ("discover" in lowered) and ("kiwi" in lowered):
            try:
                result = _apply_ws4010_discover_kiwi_command({})
                result["type"] = "command_ack"
                return result
            except Exception as exc:
                logger.exception("ws4010 discover text command failed")
                return {"ok": False, "type": "command_ack", "error": str(exc)}
        if ("wspr" in lowered) and ("scan" in lowered):
            enabled = _bool_from_text(lowered)
            if enabled is None:
                return {
                    "ok": False,
                    "type": "command_ack",
                    "error": "wspr scan command requires true/false",
                }
            try:
                result = _apply_ws4010_wspr_scan_command({"enabled": enabled})
                result["type"] = "command_ack"
                return result
            except Exception as exc:
                logger.exception("ws4010 wspr text command failed")
                return {"ok": False, "type": "command_ack", "error": str(exc)}

    if not text.startswith("{"):
        return None

    try:
        payload = json.loads(text)
    except Exception as json_exc:
        try:
            normalized = re.sub(r"\btrue\b", "True", text, flags=re.IGNORECASE)
            normalized = re.sub(r"\bfalse\b", "False", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"\bnull\b", "None", normalized, flags=re.IGNORECASE)
            payload = ast.literal_eval(normalized)
        except Exception:
            return {"ok": False, "type": "command_ack", "error": f"invalid_json: {json_exc}"}

    if not isinstance(payload, dict):
        return {"ok": False, "type": "command_ack", "error": "command must be a JSON object"}

    action = str(
        payload.get("action")
        or payload.get("command")
        or payload.get("type")
        or payload.get("message")
        or payload.get("text")
        or ""
    ).strip().lower()

    if action.startswith("{") and action.endswith("}"):
        try:
            nested = json.loads(action)
            if isinstance(nested, dict):
                merged: Dict[str, Any] = dict(payload)
                merged.update(nested)
                payload = merged
                action = str(
                    payload.get("action")
                    or payload.get("command")
                    or payload.get("type")
                    or payload.get("message")
                    or payload.get("text")
                    or ""
                ).strip().lower()
        except Exception:
            pass
    if action == "help":
        return _help_response()

    if ("discover" in action) and ("kiwi" in action):
        try:
            result = _apply_ws4010_discover_kiwi_command(payload)
            result["type"] = "command_ack"
            return result
        except ValueError as exc:
            return {"ok": False, "type": "command_ack", "error": str(exc)}
        except urllib_error.URLError as exc:
            return {"ok": False, "type": "command_ack", "error": f"discover_failed: {exc}"}
        except Exception as exc:
            logger.exception("ws4010 discover command failed")
            return {"ok": False, "type": "command_ack", "error": str(exc)}

    if ("wspr" in action) and ("scan" in action):
        enabled: Optional[bool]
        if "enabled" in payload or "auto_scan_wspr" in payload or "value" in payload:
            raw_enabled = payload.get("enabled")
            if raw_enabled is None:
                raw_enabled = payload.get("auto_scan_wspr")
            if raw_enabled is None:
                raw_enabled = payload.get("value")
            try:
                enabled = _coerce_bool(raw_enabled, field_name="enabled")
            except Exception:
                enabled = None
        else:
            enabled = _bool_from_text(action)
        if enabled is None:
            return {
                "ok": False,
                "type": "command_ack",
                "error": "wspr scan command requires true/false",
            }
        try:
            result = _apply_ws4010_wspr_scan_command({"enabled": enabled, **payload})
            result["type"] = "command_ack"
            return result
        except ValueError as exc:
            return {"ok": False, "type": "command_ack", "error": str(exc)}
        except urllib_error.URLError as exc:
            return {"ok": False, "type": "command_ack", "error": f"apply_failed: {exc}"}
        except Exception as exc:
            logger.exception("ws4010 wspr-scan command failed")
            return {"ok": False, "type": "command_ack", "error": str(exc)}

    if action in {"set_wspr_scan", "wspr_scan", "auto_scan_wspr", "run wspr band scan", "run_wspr_band_scan"}:
        try:
            result = _apply_ws4010_wspr_scan_command(payload)
            result["type"] = "command_ack"
            return result
        except ValueError as exc:
            return {"ok": False, "type": "command_ack", "error": str(exc)}
        except urllib_error.URLError as exc:
            return {"ok": False, "type": "command_ack", "error": f"apply_failed: {exc}"}
        except Exception as exc:
            logger.exception("ws4010 wspr-scan command failed")
            return {"ok": False, "type": "command_ack", "error": str(exc)}

    if action in {"discover_kiwi", "discover-kiwi", "config_discover", "discover kiwi", "discover"}:
        try:
            result = _apply_ws4010_discover_kiwi_command(payload)
            result["type"] = "command_ack"
            return result
        except ValueError as exc:
            return {"ok": False, "type": "command_ack", "error": str(exc)}
        except urllib_error.URLError as exc:
            return {"ok": False, "type": "command_ack", "error": f"discover_failed: {exc}"}
        except Exception as exc:
            logger.exception("ws4010 discover command failed")
            return {"ok": False, "type": "command_ack", "error": str(exc)}

    if action in {
        "status",
        "get_status",
        "get status",
        "band_status",
        "band status",
        "show_status",
        "show status",
        "settings",
        "settings_status",
        "settings status",
    }:
        try:
            result = _apply_ws4010_status_command(payload)
            result["type"] = "command_ack"
            return result
        except Exception as exc:
            logger.exception("ws4010 status command failed")
            return {"ok": False, "type": "command_ack", "error": str(exc)}

    is_band_command = (
        action in {"set_band", "band_set", "setband", "set_band_mode"}
        or ("enabled" in payload and ("band" in payload or "target_band" in payload))
    )
    if not is_band_command:
        return None

    try:
        result = _apply_ws4010_band_command(payload)
        result["type"] = "command_ack"
        return result
    except ValueError as exc:
        return {"ok": False, "type": "command_ack", "error": str(exc)}
    except urllib_error.URLError as exc:
        return {"ok": False, "type": "command_ack", "error": f"apply_failed: {exc}"}
    except Exception as exc:
        logger.exception("ws4010 command failed")
        return {"ok": False, "type": "command_ack", "error": str(exc)}


def prune_decode_buffer(allowed_bands: Optional[set[str]]) -> None:
    with _decode_lock:
        if not allowed_bands:
            _decode_buffer.clear()
            return
        filtered = [
            d
            for d in _decode_buffer
            if d.get("band") in allowed_bands and d.get("grid")
        ]
        _decode_buffer.clear()
        _decode_buffer.extend(filtered)


def _parse_decode_line(line: str) -> Dict[str, Optional[str]]:
    raw = (line or "").strip()
    ts = None
    mode_symbol = None
    mode = None
    message = raw
    snr = None
    dt = None
    hz = None
    power = None
    if raw.startswith("D:"):
        parts = raw.split()
        # Expected formats:
        # - FT8/FT4: D: FT8 <epoch> <snr> <dt> <hz> ~ <message...>
        # - WSPR:    D: WSPR <epoch> <wsprd columns... including CALL and GRID>
        if len(parts) >= 3:
            mode = parts[1].upper()
            try:
                ts = int(float(parts[2]))
            except Exception:
                ts = None

            if mode in {"FT8", "FT4", "JT9", "JT65"}:
                # Keep only the decoded message portion after the mode code.
                # For FT8/FT4 the mode code is usually at index 6.
                if len(parts) >= 8:
                    try:
                        snr = float(parts[3])
                    except Exception:
                        snr = None
                    try:
                        dt = float(parts[4])
                    except Exception:
                        dt = None
                    try:
                        hz = float(parts[5])
                    except Exception:
                        hz = None
                    mode_symbol = parts[6]
                    message = " ".join(parts[7:])
                else:
                    message = " ".join(parts[3:]) if len(parts) > 3 else ""
            elif mode == "WSPR":
                # D: WSPR <epoch> <date> <time> <sync> <snr> <dt> <freq_audio_mhz> <call> <grid> <power> [...]
                # indices:    0     1      2      3     4      5      6       7         8      9    10    11
                if len(parts) >= 12:
                    try:
                        snr = float(parts[6])
                    except Exception:
                        snr = None
                    try:
                        dt = float(parts[7])
                    except Exception:
                        dt = None
                    try:
                        hz = float(parts[8]) * 1e6  # audio freq MHz -> Hz
                    except Exception:
                        hz = None
                    try:
                        power = int(float(parts[11]))
                    except Exception:
                        power = None
                    # callsign=parts[9], grid=parts[10]; let the regex below extract them
                    message = " ".join(parts[9:12])
                else:
                    message = " ".join(parts[3:]) if len(parts) > 3 else ""
            else:
                # For other modes, keep the rest of the decoder line.
                message = " ".join(parts[3:]) if len(parts) > 3 else ""

    callsign = None
    grid = None
    grid_pattern = re.compile(r"^[A-R]{2}\d{2}(?:[A-X]{2})?$", re.IGNORECASE)
    grid_match = re.search(r"\b[A-R]{2}\d{2}([A-X]{2})?\b", message, re.IGNORECASE)
    if grid_match:
        candidate = grid_match.group(0).upper()
        if candidate not in {"RR73", "R73"}:
            grid = candidate

    for token in re.findall(r"\b[A-Z0-9/]+\b", message.upper()):
        if token in {"CQ", "QRZ", "RR73", "73", "R73"}:
            continue
        if grid_pattern.match(token):
            continue
        if re.match(r"^[A-Z]{1,2}\d[A-Z0-9]{1,4}$", token):
            callsign = token
            break

    if callsign and grid and callsign.upper() == grid.upper():
        callsign = None

    return {
        "callsign": callsign,
        "grid": grid,
        "timestamp": ts,
        "mode_symbol": mode_symbol,
        "mode": mode,
        "message": message,
        "snr": snr,
        "dt": dt,
        "hz": hz,
        "power": power,
    }


async def _broadcast_decodes(payload: Dict) -> None:
    dead: List[WebSocket] = []
    text = json.dumps(payload)
    with _decode_ws_lock:
        ws_list = list(_decode_ws_clients)
    for ws in ws_list:
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    if dead:
        with _decode_ws_lock:
            for d in dead:
                _decode_ws_clients.discard(d)


async def _broadcast_decodes_4010(payload: Dict, exclude: WebSocket | None = None) -> None:
    dead: List[WebSocket] = []
    text = json.dumps(payload, default=str)
    with _decode_ws4010_lock:
        ws_list = [ws for ws in _decode_ws4010_clients if ws is not exclude]
    for ws in ws_list:
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    if dead:
        with _decode_ws4010_lock:
            for d in dead:
                _decode_ws4010_clients.discard(d)


async def _broadcast_ws4010_dashboard(payload: Dict, exclude: WebSocket | None = None) -> None:
    dead: List[WebSocket] = []
    text = json.dumps(payload, default=str)
    with _decode_ws4010_lock:
        ws_list = [ws for ws in _decode_ws4010_dashboard_clients if ws is not exclude]
    for ws in ws_list:
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    if dead:
        with _decode_ws4010_lock:
            for d in dead:
                _decode_ws4010_dashboard_clients.discard(d)
                _decode_ws4010_clients.discard(d)


def _ws4010_command_compat_frame(response: Dict[str, Any]) -> Dict[str, Any]:
    now_str = datetime.now().astimezone().strftime("%H:%M:%S")
    action = str(response.get("action") or "command")
    ok = bool(response.get("ok", False))
    summary = {
        "type": str(response.get("type") or "command_ack"),
        "action": action,
        "ok": ok,
    }
    if action == "status":
        summary["block"] = response.get("block")
        summary["mode"] = response.get("mode")
        summary["bands"] = len(response.get("bands", [])) if isinstance(response.get("bands"), list) else 0
    elif action == "help":
        summary["commands"] = len(response.get("commands", [])) if isinstance(response.get("commands"), list) else 0
    return {
        "timestamp": now_str,
        "frequency_mhz": None,
        "mode": "WS4010",
        "callsign": "SERVER",
        "grid": "WS40",
        "message": json.dumps(summary, separators=(",", ":")),
        "snr": None,
        "dt": None,
        "hz": None,
        "band": "control",
        "rx": -1,
        "type": "command_notice",
        "action": action,
        "ok": ok,
    }


def _ws4010_settings_decode_frames(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    action = str(response.get("action") or "")
    if action not in {"status", "settings"}:
        return []
    band_rows = response.get("band_settings")
    if not isinstance(band_rows, list):
        return []
    now_str = datetime.now().astimezone().strftime("%H:%M:%S")
    out: List[Dict[str, Any]] = []
    for row in band_rows:
        if not isinstance(row, dict):
            continue
        band = str(row.get("band") or "")
        enabled = bool(row.get("enabled", False))
        band_mode = str(row.get("band_mode") or "FT8")
        out.append(
            {
                "timestamp": now_str,
                "frequency_mhz": None,
                "mode": "FT8",
                "callsign": "SETTINGS",
                "grid": "AA00",
                "message": f"band:{band} enabled:{str(enabled).lower()} band_mode:{band_mode}",
                "snr": 0,
                "dt": 0.0,
                "hz": 0,
                "band": "control",
                "rx": -1,
            }
        )
    return out


def publish_decode(payload: Dict) -> None:
    """Append payload to the decode buffer and broadcast to WS clients."""

    global _decode_seq
    with _decode_lock:
        now = time.time()
        _decode_seq += 1
        payload["id"] = _decode_seq
        _decode_buffer.append(payload)
        _decode_times.append(now)
        cutoff = now - 300.0
        while _decode_times and _decode_times[0] < cutoff:
            _decode_times.popleft()

    _chart_ingest(payload, now)

    loop = _loop
    loop_4010 = _loop_4010
    if loop is None and loop_4010 is None:
        return

    if loop is not None:
        try:
            asyncio.run_coroutine_threadsafe(_broadcast_decodes(payload), loop)
        except Exception:
            # Best effort: don't let background decode threads crash.
            logger.debug("decode broadcast failed", exc_info=True)

    if loop_4010 is not None:
        try:
            asyncio.run_coroutine_threadsafe(_broadcast_decodes_4010(payload), loop_4010)
        except Exception:
            logger.debug("decode WS4010 broadcast failed", exc_info=True)

    try:
        publish_udp4010(payload)
    except Exception:
        logger.debug("decode UDP4010 broadcast failed", exc_info=True)


def decode_callback(event: Dict) -> None:
    """ReceiverManager callback: parse ft8modem lines into a UI decode payload."""

    try:
        mode_label = str(event.get("mode_label") or "").strip()
        freq_hz = float(event.get("freq_hz") or 0.0)
        parsed = _parse_decode_line(str(event.get("message") or ""))
        if not parsed.get("grid"):
            return

        ts = parsed.get("timestamp")
        if ts is not None:
            ts_str = datetime.fromtimestamp(int(ts)).astimezone().strftime("%H:%M:%S")
        else:
            ts_str = datetime.now().astimezone().strftime("%H:%M:%S")

        parsed_mode = str(parsed.get("mode") or "").strip().upper()
        mode_symbol = parsed.get("mode_symbol")
        if parsed_mode:
            mode_label = parsed_mode
        elif mode_symbol == "+":
            mode_label = "FT4"
        elif mode_symbol == "~":
            mode_label = "FT8"

        precision = 4 if mode_label.upper() == "WSPR" else 3

        payload = {
            "timestamp": ts_str,
            "frequency_mhz": round(freq_hz / 1e6, precision) if freq_hz else None,
            "mode": mode_label,
            "callsign": parsed.get("callsign"),
            "grid": parsed.get("grid"),
            "message": parsed.get("message"),
            "snr": parsed.get("snr"),
            "dt": parsed.get("dt"),
            "hz": parsed.get("hz"),
            "power": parsed.get("power"),
            "band": event.get("band"),
            "rx": event.get("rx"),
        }
        publish_decode(payload)
    except Exception:
        logger.debug("decode_callback failed", exc_info=True)


def get_decode_metrics() -> Dict[str, float | int]:
    with _decode_lock:
        now = time.time()
        count_60 = sum(1 for ts in _decode_times if (now - ts) <= 60.0)
        count_300 = sum(1 for ts in _decode_times if (now - ts) <= 300.0)
        total = int(_decode_seq)
    return {
        "total_decodes": total,
        "decodes_last_60s": int(count_60),
        "decodes_last_300s": int(count_300),
        "decode_rate_per_sec_60s": float(count_60) / 60.0,
        "decode_rate_per_sec_300s": float(count_300) / 300.0,
    }


def reset_decode_metrics() -> Dict[str, int]:
    global _decode_seq
    with _decode_lock:
        _decode_seq = 0
        _decode_buffer.clear()
        _decode_times.clear()
    return {
        "total_decodes": 0,
        "buffer_size": 0,
    }


@router.get("/decodes")
def get_decodes(since: int = 0):
    with _decode_lock:
        items = [
            d for d in list(_decode_buffer)
            if int(d.get("id", 0)) > int(since or 0)
            and d.get("grid")
        ]
        latest = _decode_seq
    return {"latest": latest, "items": items}


def _chart_ingest(payload: Dict, now: float) -> None:
    """Accumulate one decode event into the current wall-clock bucket."""
    global _chart_running
    band = str(payload.get("band") or "").strip()
    if not band or band.lower() == "control":
        return
    rx_raw = payload.get("rx")
    if rx_raw is None or rx_raw == -1:
        rx_label = "RX?"
    else:
        try:
            rx_label = f"RX{int(rx_raw)}"
        except Exception:
            rx_label = "RX?"
    mode = str(payload.get("mode") or "?").strip().upper() or "?"
    key = f"{rx_label}|{mode}"
    bucket_ts = (now // _CHART_BUCKET_S) * _CHART_BUCKET_S
    with _chart_lock:
        if not _chart_running or _chart_running.get("ts") != bucket_ts:
            if _chart_running:
                _chart_buckets.append(_chart_running)
            _chart_running = {"ts": bucket_ts, "bands": {}}
        bands = _chart_running["bands"]
        if band not in bands:
            bands[band] = {}
        bands[band][key] = bands[band].get(key, 0) + 1


@router.get("/decodes/chart")
def get_decodes_chart():
    """Return server-side band activity time series (fixed 15s buckets)."""
    with _chart_lock:
        completed = list(_chart_buckets)
        running = dict(_chart_running) if _chart_running else None
        running_bands = {}
        if running:
            running_bands = {k: dict(v) for k, v in running.get("bands", {}).items()}

    result: List[Dict] = []
    for b in completed:
        result.append({
            "ts": b["ts"],
            "bands": {
                band: {
                    "total": sum(breakdown.values()),
                    "breakdown": dict(breakdown),
                }
                for band, breakdown in b.get("bands", {}).items()
            },
        })
    if running:
        result.append({
            "ts": running["ts"],
            "bands": {
                band: {
                    "total": sum(breakdown.values()),
                    "breakdown": dict(breakdown),
                }
                for band, breakdown in running_bands.items()
            },
        })
    return {"bucket_s": _CHART_BUCKET_S, "buckets": result}


def get_decode_ws_counts() -> Dict[str, int]:
    with _decode_ws_lock:
        ws_clients = len(_decode_ws_clients)
    with _decode_ws4010_lock:
        ws4010_total_clients = len(_decode_ws4010_clients)
        ws4010_dashboard_clients = len(_decode_ws4010_dashboard_clients)
    ws4010_external_clients = max(0, ws4010_total_clients - ws4010_dashboard_clients)
    return {
        "ws_clients": int(ws_clients),
        "ws4010_clients": int(ws4010_external_clients),
        "ws4010_total_clients": int(ws4010_total_clients),
        "ws4010_dashboard_clients": int(ws4010_dashboard_clients),
    }


@router.get("/decodes/ws_status")
def get_decode_ws_status() -> Dict[str, object]:
    counts = get_decode_ws_counts()
    return {
        "ws_clients": int(counts.get("ws_clients", 0)),
        "ws4010_clients": int(counts.get("ws4010_clients", 0)),
        "ws4010_total_clients": int(counts.get("ws4010_total_clients", 0)),
        "ws_loop_active": _loop is not None,
        "ws4010_loop_active": _loop_4010 is not None,
    }


@router.get("/decodes/ws4010/debug")
def get_ws4010_debug(limit: int = 50) -> Dict[str, Any]:
    n = max(1, min(int(limit or 50), 200))
    with _decode_ws4010_lock:
        rows = list(_ws4010_debug_events)[-n:]
    return {
        "count": len(rows),
        "items": rows,
    }


@router.websocket("/ws/decodes")
async def websocket_decodes(websocket: WebSocket):
    await websocket.accept()
    with _decode_ws_lock:
        _decode_ws_clients.add(websocket)
    try:
        while True:
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        with _decode_ws_lock:
            _decode_ws_clients.discard(websocket)


async def websocket_decodes_4010(websocket: WebSocket) -> None:
    """WebSocket handler for the dedicated WS:4010 server.

    Uses a separate client set because it runs on a different uvicorn server.
    """

    is_dashboard = str(websocket.query_params.get("role") or "").strip().lower() == "dashboard"

    await websocket.accept()
    with _decode_ws4010_lock:
        _decode_ws4010_clients.add(websocket)
        if is_dashboard:
            _decode_ws4010_dashboard_clients.add(websocket)
    try:
        while True:
            try:
                raw = await websocket.receive_text()
                try:
                    peer = getattr(websocket, "client", None)
                    peer_text = f"{peer.host}:{peer.port}" if peer is not None else "unknown"
                except Exception:
                    peer_text = "unknown"
                logger.info("ws4010 recv peer=%s raw=%s", peer_text, (raw[:400] + "...") if len(raw) > 400 else raw)
                _record_ws4010_debug("recv", peer=peer_text, raw=(raw[:400] + "...") if len(raw) > 400 else raw)
                response = _handle_ws4010_command(raw)
                if response is not None:
                    logger.info(
                        "ws4010 ack action=%s type=%s ok=%s verbose=%s",
                        response.get("action"),
                        response.get("type"),
                        response.get("ok"),
                        response.get("verbose"),
                    )
                    _record_ws4010_debug(
                        "ack",
                        action=response.get("action"),
                        type=response.get("type"),
                        ok=response.get("ok"),
                        verbose=response.get("verbose"),
                    )
                    try:
                        await websocket.send_text(json.dumps(response, default=str))
                    except Exception:
                        pass
                    await _broadcast_decodes_4010(response, exclude=websocket)
                    if str(response.get("type") or "") == "command_ack":
                        await _broadcast_decodes_4010(_ws4010_command_compat_frame(response), exclude=websocket)
                        for settings_frame in _ws4010_settings_decode_frames(response):
                            await _broadcast_decodes_4010(settings_frame, exclude=websocket)
            except WebSocketDisconnect:
                break
            except Exception:
                _record_ws4010_debug("error")
                break
    finally:
        with _decode_ws4010_lock:
            _decode_ws4010_dashboard_clients.discard(websocket)
            _decode_ws4010_clients.discard(websocket)
