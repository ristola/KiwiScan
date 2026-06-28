"""
api/rtl_monitor.py — REST + WebSocket endpoints for the RTL-SDR IQ monitor.

POST /api/rtl-monitor/start              — launch a new monitor session
GET  /api/rtl-monitor/status             — current per-channel status JSON
POST /api/rtl-monitor/stop               — stop the active session
GET  /api/rtl-monitor/presets            — list built-in channel group presets
WS   /api/rtl-monitor/ws                — push status every second while running
GET  /api/rtl-monitor/recordings         — list recordings with metadata
GET  /api/rtl-monitor/recordings/{name} — download a WAV recording
DELETE /api/rtl-monitor/recordings/{name} — delete one recording
POST /api/rtl-monitor/recordings/cleanup — delete all recordings older than retention
GET  /api/rtl-monitor/settings           — get retention_days
POST /api/rtl-monitor/settings           — set retention_days
"""

from __future__ import annotations

import asyncio
import json
import logging
import re as _re
import struct
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from ..rtl_monitor import (
    RtlRepeaterMonitor,
    RECORDINGS_DIR,
    SHENANDOAH_REPEATERS,
    SHENANDOAH_CENTER_HZ,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/rtl-monitor", tags=["rtl-monitor"])

# Single active monitor (one SDR at a time per KiwiScan instance)
_monitor: Optional[RtlRepeaterMonitor] = None

# ── Recording config ──────────────────────────────────────────────────────────

_REC_CONFIG_PATH = Path("outputs/repeater_monitor_config.json")
_REC_NAME_RE     = _re.compile(r"^(\d{8}_\d{6})_(.+)_(\d+)Hz\.wav$")


def _load_rec_config() -> dict:
    try:
        return json.loads(_REC_CONFIG_PATH.read_text())
    except Exception:
        return {"retention_days": 10}


def _save_rec_config(cfg: dict) -> None:
    _REC_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _REC_CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def _cleanup_recordings() -> int:
    days = _load_rec_config().get("retention_days", 10)
    if not RECORDINGS_DIR.exists():
        return 0
    cutoff  = time.time() - days * 86400
    deleted = 0
    for f in RECORDINGS_DIR.glob("*.wav"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        except Exception:
            pass
    if deleted:
        logger.info("Pruned %d recordings older than %d days", deleted, days)
    return deleted


def _parse_rec_info(f: Path) -> dict:
    stat = f.stat()
    m    = _REC_NAME_RE.match(f.name)
    dur  = max(0.0, (stat.st_size - 44) / (48_000 * 2))   # 48 kHz mono 16-bit
    info: dict = {
        "filename":     f.name,
        "label":        m.group(2).replace("_", " ") if m else f.stem,
        "freq_hz":      int(m.group(3)) if m else 0,
        "created_at":   stat.st_mtime,
        "size_bytes":   stat.st_size,
        "duration_sec": round(dur, 1),
    }
    return info

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

    asyncio.ensure_future(asyncio.get_event_loop().run_in_executor(None, _cleanup_recordings))

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


# ── Recordings ────────────────────────────────────────────────────────────────

@router.get("/recordings")
async def list_recordings():
    if not RECORDINGS_DIR.exists():
        return {"recordings": []}
    files = sorted(
        RECORDINGS_DIR.glob("*.wav"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return {"recordings": [_parse_rec_info(f) for f in files]}


@router.get("/recordings/{filename}")
async def get_recording(filename: str):
    path = RECORDINGS_DIR / Path(filename).name   # strip any path traversal
    if not path.exists() or path.suffix != ".wav":
        raise HTTPException(404, "Recording not found")
    return FileResponse(str(path), media_type="audio/wav", filename=path.name)


@router.delete("/recordings/{filename}")
async def delete_recording(filename: str):
    path = RECORDINGS_DIR / Path(filename).name
    if not path.exists():
        raise HTTPException(404, "Recording not found")
    path.unlink()
    return {"deleted": filename}


@router.post("/recordings/cleanup")
async def cleanup_recordings(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    # Optional override: retention_days=0 means delete everything
    override_days = body.get("retention_days")

    def _do():
        days   = override_days if override_days is not None else _load_rec_config().get("retention_days", 10)
        cutoff = time.time() - days * 86400
        if not RECORDINGS_DIR.exists():
            return 0
        deleted = 0
        for f in RECORDINGS_DIR.glob("*.wav"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except Exception:
                pass
        return deleted

    deleted = await asyncio.get_event_loop().run_in_executor(None, _do)
    return {"deleted": deleted}


# ── Settings ──────────────────────────────────────────────────────────────────

@router.get("/settings")
async def get_settings():
    return _load_rec_config()


@router.post("/settings")
async def save_settings(request: Request):
    body = await request.json()
    cfg  = _load_rec_config()
    if "retention_days" in body:
        cfg["retention_days"] = max(1, int(body["retention_days"]))
    _save_rec_config(cfg)
    return cfg
