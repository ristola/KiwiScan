from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import APIRouter, WebSocket
from fastapi.websockets import WebSocketDisconnect

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


def set_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop
    _loop = loop


def set_ws4010_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    global _loop_4010
    _loop_4010 = loop


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
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        with _decode_ws4010_lock:
            _decode_ws4010_dashboard_clients.discard(websocket)
            _decode_ws4010_clients.discard(websocket)
