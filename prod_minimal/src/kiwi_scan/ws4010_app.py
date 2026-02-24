from __future__ import annotations

import asyncio

from fastapi import FastAPI, WebSocket

from .api import decodes as decodes_api


app = FastAPI(title="KiwiSDR Scanner WS4010")


@app.on_event("startup")
async def _startup() -> None:
    # Capture the loop that uvicorn creates for this dedicated server.
    decodes_api.set_ws4010_loop(asyncio.get_event_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    decodes_api.set_ws4010_loop(None)


@app.get("/")
def _root() -> dict:
    return {
        "ok": True,
        "service": "kiwi_scan ws4010",
        "websocket": ["/", "/ws/decodes"],
    }


@app.get("/ws_status")
def _ws_status() -> dict:
    counts = decodes_api.get_decode_ws_counts()
    return {
        "ws4010_clients": int(counts.get("ws4010_clients", 0)),
        "ws4010_total_clients": int(counts.get("ws4010_total_clients", 0)),
        "ws4010_loop_active": True,
    }


@app.websocket("/")
async def websocket_root(websocket: WebSocket):
    await decodes_api.websocket_decodes_4010(websocket)


@app.websocket("/ws/decodes")
async def websocket_decodes(websocket: WebSocket):
    await decodes_api.websocket_decodes_4010(websocket)
