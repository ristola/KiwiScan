from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Callable, Dict, List

from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect


_ws_lock = threading.Lock()
_ws_clients: set[WebSocket] = set()


async def broadcast_status(payload: Dict) -> None:
    dead: List[WebSocket] = []
    text = json.dumps(payload)
    with _ws_lock:
        ws_list = list(_ws_clients)
    for ws in ws_list:
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    with _ws_lock:
        for d in dead:
            try:
                _ws_clients.remove(d)
            except KeyError:
                pass


def make_router(
    *,
    mgr: object,
    waterholes: Dict[str, float],
    compute_s_metrics: Callable[[Dict[str, Dict], float], Dict[str, Dict]],
) -> APIRouter:
    """Create router for the status websocket (/ws)."""

    router = APIRouter()

    @router.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        with _ws_lock:
            _ws_clients.add(websocket)
        try:
            # send initial full status
            with mgr.lock:  # type: ignore[attr-defined]
                payload = {
                    "results": compute_s_metrics(mgr.results, float(mgr.s_meter_offset_db)),
                    "current_band": mgr.current_band,
                    "calibrating_band": mgr.calibrating_band,
                    "last_updated": mgr.last_updated,
                    "rx_chan": mgr.rx_chan,
                    "camp_status": mgr.camp_status,
                    "waterholes": waterholes,
                    "threshold_db": mgr.threshold_db,
                    "threshold_db_by_band": mgr.threshold_db_by_band,
                    "s_meter_offset_db": mgr.s_meter_offset_db,
                    "status_seq": mgr.status_seq,
                    "status_time": time.time(),
                }
            await websocket.send_text(json.dumps(payload))

            # keep the connection open; receive loop to detect disconnects
            while True:
                try:
                    await websocket.receive_text()
                except WebSocketDisconnect:
                    break
                except Exception:
                    await asyncio.sleep(0.1)
        finally:
            with _ws_lock:
                try:
                    _ws_clients.remove(websocket)
                except KeyError:
                    pass

    return router
