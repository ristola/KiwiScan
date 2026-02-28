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

from ..scheduler import block_for_hour, expected_schedule_by_season

logger = logging.getLogger(__name__)

router = APIRouter()

_decode_ws_lock = threading.Lock()
_decode_ws_clients: set[WebSocket] = set()

_decode_lock = threading.Lock()
_decode_seq = 0
_decode_buffer: deque[Dict] = deque(maxlen=500)
_decode_times: deque[float] = deque(maxlen=5000)

_loop: asyncio.AbstractEventLoop | None = None
_loop_4010: asyncio.AbstractEventLoop | None = None

_decode_ws4010_lock = threading.Lock()
_decode_ws4010_clients: set[WebSocket] = set()
_decode_ws4010_dashboard_clients: set[WebSocket] = set()

_automation_lock = threading.Lock()
_valid_bands = ("10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m")
_valid_band_modes = {"FT4", "FT4 / FT8", "FT8", "WSPR", "SSB"}


def set_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop
    _loop = loop


def set_ws4010_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop_4010
    _loop_4010 = loop


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
    if raw in {"PHONE", "LSB", "USB"}:
        return "SSB"
    if raw in _valid_band_modes:
        return raw
    return "FT8"


def _blocks_for_mode(mode: str, block: str) -> List[str]:
    _ = str(block or "").strip()
    now = datetime.now().astimezone()
    return [block_for_hour(now.hour, mode=mode)]


def _trigger_auto_set_apply(settings: Dict[str, Any], mode: str) -> Dict[str, Any]:
    try:
        now = datetime.now().astimezone()
        block = block_for_hour(now.hour, mode=mode)
        schedule_profiles = settings.get("scheduleProfiles") if isinstance(settings, dict) else {}
        by_mode = schedule_profiles.get(mode) if isinstance(schedule_profiles, dict) else {}
        entry = by_mode.get(block) if isinstance(by_mode, dict) else {}

        selected_bands = entry.get("selectedBands") if isinstance(entry, dict) else []
        band_modes = entry.get("bandModes") if isinstance(entry, dict) else {}
        payload = {
            "enabled": True,
            "mode": mode,
            "block": block,
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
        return {"ok": True, "block": block, "response": body}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _apply_ws4010_band_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(payload.get("mode") or "ft8").strip().lower()
    if mode not in {"ft8", "phone"}:
        raise ValueError("mode must be 'ft8' or 'phone'")

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

    band_mode = _normalize_band_mode(payload.get("band_mode") or payload.get("mode_value") or payload.get("bandMode"))
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
        "band_mode": band_mode,
        "blocks": target_blocks,
        "apply": apply_result,
    }


def _handle_ws4010_command(raw: str) -> Optional[Dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return None

    if text.lower() in {"ping", "{\"type\":\"ping\"}"}:
        return {"ok": True, "type": "pong"}

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

    action = str(payload.get("action") or payload.get("command") or "").strip().lower()
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
            else:
                # For WSPR and other modes, keep the rest of the decoder line.
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


async def _broadcast_decodes_4010(payload: Dict) -> None:
    dead: List[WebSocket] = []
    text = json.dumps(payload)
    with _decode_ws4010_lock:
        ws_list = list(_decode_ws4010_clients)
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
    text = json.dumps(payload)
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
                response = _handle_ws4010_command(raw)
                if response is not None:
                    await websocket.send_text(json.dumps(response))
                    if str(response.get("type") or "") == "command_ack":
                        await _broadcast_ws4010_dashboard(response, exclude=websocket)
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        with _decode_ws4010_lock:
            _decode_ws4010_dashboard_clients.discard(websocket)
            _decode_ws4010_clients.discard(websocket)
