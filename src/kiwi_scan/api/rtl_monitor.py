"""
api/rtl_monitor.py — REST + WebSocket endpoints for the RTL-SDR IQ monitor.

POST /api/rtl-monitor/start   — launch a new monitor session
GET  /api/rtl-monitor/status  — current per-channel status JSON
POST /api/rtl-monitor/stop    — stop the active session
GET  /api/rtl-monitor/presets — list built-in channel group presets
WS   /api/rtl-monitor/ws      — push status every second while running
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..rtl_monitor import (
    RtlRepeaterMonitor,
    SHENANDOAH_REPEATERS,
    SHENANDOAH_CENTER_HZ,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/rtl-monitor", tags=["rtl-monitor"])

# Single active monitor (one SDR at a time per KiwiScan instance)
_monitor: Optional[RtlRepeaterMonitor] = None

PRESETS = {
    "shenandoah-vhf": {
        "label": "Shenandoah Valley VHF",
        "center_hz": SHENANDOAH_CENTER_HZ,
        "sample_rate": 2_400_000,
        "channels": SHENANDOAH_REPEATERS,
    },
}


# ── Request / response models ─────────────────────────────────────────────────

class ChannelDef(BaseModel):
    freq_hz: int
    label: str


class StartRequest(BaseModel):
    device: str                         # serial number or integer index
    center_hz: int
    sample_rate: int = 2_400_000
    channels: list[ChannelDef]
    enable_stt: bool = True
    rtl_gain: str = "0"
    preset: Optional[str] = None        # if set, overrides channels/center_hz


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/presets")
async def get_presets():
    return {"presets": {k: {**v, "channels": len(v["channels"])} for k, v in PRESETS.items()}}


@router.post("/start")
async def start_monitor(req: StartRequest):
    global _monitor

    if _monitor and _monitor._running:
        raise HTTPException(409, "A monitor session is already running — stop it first")

    # Resolve preset if given
    channels = [c.model_dump() for c in req.channels]
    center_hz = req.center_hz

    if req.preset and req.preset in PRESETS:
        p = PRESETS[req.preset]
        channels = p["channels"]
        center_hz = p["center_hz"]

    if not channels:
        raise HTTPException(400, "No channels defined")

    _monitor = RtlRepeaterMonitor(
        device=req.device,
        center_hz=center_hz,
        sample_rate=req.sample_rate,
        repeaters=channels,
        enable_stt=req.enable_stt,
        rtl_gain=req.rtl_gain,
    )
    await _monitor.start()

    if _monitor.error:
        raise HTTPException(500, _monitor.error)

    return {"ok": True, "center_hz": center_hz, "channels": len(channels)}


@router.post("/stop")
async def stop_monitor():
    global _monitor
    if not _monitor:
        return {"ok": True, "message": "No active session"}
    await _monitor.stop()
    _monitor = None
    return {"ok": True}


@router.get("/status")
async def get_status():
    if not _monitor:
        return {"running": False}
    return _monitor.status()


def _wav_header(sample_rate: int = 48_000, channels: int = 1, bits: int = 16) -> bytes:
    """44-byte WAV header for a streaming (unknown-length) PCM file."""
    byte_rate   = sample_rate * channels * bits // 8
    block_align = channels * bits // 8
    data_size   = 0x7FFF_FFFF
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", data_size + 36,
        b"WAVE",
        b"fmt ", 16,
        1, channels, sample_rate, byte_rate, block_align, bits,
        b"data", data_size,
    )


@router.get("/audio/{freq_hz}")
async def rtl_audio_stream(freq_hz: int) -> StreamingResponse:
    """Stream live FM audio for a single repeater channel as WAV PCM."""
    if not _monitor or not _monitor._running:
        raise HTTPException(503, "Monitor not running")
    try:
        q = _monitor.subscribe_audio(freq_hz)
    except ValueError as exc:
        raise HTTPException(404, str(exc))

    async def _generate():
        yield _wav_header()
        try:
            while True:
                pcm = await asyncio.wait_for(q.get(), timeout=10.0)
                yield pcm
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass
        finally:
            if _monitor:
                _monitor.unsubscribe_audio(freq_hz, q)

    return StreamingResponse(
        _generate(),
        media_type="audio/wav",
        headers={"Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff"},
    )


@router.websocket("/ws")
async def ws_status(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            payload = _monitor.status() if _monitor else {"running": False}
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        logger.debug("rtl-monitor ws error: %s", exc)
