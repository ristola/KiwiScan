"""API endpoints for voice scanning feature.

Handles HTTP requests from UI for voice scan control and status.
Should be integrated into FastAPI app (server.py).
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .voice_scan_manager import get_voice_scan_manager
from .voice_settings import get_voice_settings_manager
from .voice_bands import list_voice_bands

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/voice", tags=["voice_scanning"])


# Settings endpoints


@router.get("/settings")
async def get_voice_settings():
    """Get current voice scan settings.
    
    Returns:
        Dict with STT engine, audio capture settings, memory behavior, etc.
    """
    settings_mgr = get_voice_settings_manager()
    return {
        "settings": settings_mgr.to_json_dict(),
    }


@router.post("/settings")
async def update_voice_settings(
    stt_engine: Optional[str] = Query(None),
    stt_enabled: Optional[bool] = Query(None),
    whisper_model: Optional[str] = Query(None),
    capture_enabled: Optional[bool] = Query(None),
    auto_fine_tune: Optional[bool] = Query(None),
    min_confidence_threshold: Optional[float] = Query(None),
):
    """Update voice scan settings.
    
    Query parameters (all optional):
        stt_engine: "none", "whisper", "google_cloud", "vosk"
        stt_enabled: Enable speech-to-text decoding
        whisper_model: "tiny", "base", "small", "medium", "large"
        capture_enabled: Enable audio clip capture to /tmp
        auto_fine_tune: Auto-tune to carrier peak when detected
        min_confidence_threshold: 0.0–1.0, minimum confidence to store channel
    
    Returns:
        Updated settings dict
    """
    settings_mgr = get_voice_settings_manager()
    
    # Build update dict with only provided values
    updates = {}
    if stt_engine is not None:
        updates["stt_engine"] = stt_engine
    if stt_enabled is not None:
        updates["stt_enabled"] = stt_enabled
    if whisper_model is not None:
        updates["whisper_model"] = whisper_model
    if capture_enabled is not None:
        updates["capture_enabled"] = capture_enabled
    if auto_fine_tune is not None:
        updates["auto_fine_tune"] = auto_fine_tune
    if min_confidence_threshold is not None:
        updates["min_confidence_threshold"] = min_confidence_threshold
    
    if updates:
        settings_mgr.update_settings(**updates)
    
    return {
        "settings": settings_mgr.to_json_dict(),
        "message": "Settings updated" if updates else "No changes",
    }


# Voice bands endpoints


@router.get("/bands")
async def list_available_voice_bands():
    """List available voice bands for scanning.
    
    Returns:
        List of band definitions with frequency ranges
    """
    bands = list_voice_bands()
    return {
        "bands": [
            {
                "name": b.name,
                "center_freq_hz": b.center_freq_hz,
                "center_freq_khz": b.center_freq_hz / 1e3,
                "ssb_start_hz": b.ssb_start_hz,
                "ssb_start_khz": b.ssb_start_khz(),
                "ssb_end_hz": b.ssb_end_hz,
                "ssb_end_khz": b.ssb_end_khz(),
                "bandwidth_hz": b.bandwidth_hz(),
                "bandwidth_khz": b.bandwidth_hz() / 1e3,
            }
            for b in bands
        ]
    }


# Scan control endpoints


@router.post("/scan/start")
async def start_voice_scan(
    rx_index: int = Query(..., ge=0, le=7),
    band_name: str = Query(...),
    sideband: str = Query("LSB", pattern="^(LSB|USB)$"),
    freq_step_hz: int = Query(1000, ge=100, le=10000),
):
    """Start voice scan on receiver slot.
    
    Query parameters:
        rx_index: Receiver slot (0–7) *required*
        band_name: Band identifier ("40m", "20m", etc.) *required*
        sideband: "LSB" or "USB" (default: LSB)
        freq_step_hz: Frequency step in Hz (default: 1000)
    
    Returns:
        Scan session info (band, frequency range, progress)
    """
    mgr = get_voice_scan_manager()
    
    try:
        session = await mgr.start_scan_for_receiver(
            rx_index=rx_index,
            band_name=band_name,
            sideband=sideband,
            freq_step_hz=freq_step_hz,
        )
        
        if not session:
            raise HTTPException(status_code=400, detail="Failed to start scan")
        
        return {
            "success": True,
            "message": f"Voice scan started on RX{rx_index}",
            "scan_status": mgr.get_scan_status_for_receiver(rx_index),
        }
    
    except Exception as err:
        logger.error(f"Failed to start voice scan: {err}")
        raise HTTPException(status_code=400, detail=str(err))


@router.post("/scan/stop")
async def stop_voice_scan(rx_index: int = Query(..., ge=0, le=7)):
    """Stop voice scan on receiver slot.
    
    Query parameters:
        rx_index: Receiver slot (0–7) *required*
    
    Returns:
        Final scan results (channels found, duration, etc.)
    """
    mgr = get_voice_scan_manager()
    
    session = mgr.stop_scan_for_receiver(rx_index)
    
    if not session:
        return {
            "success": True,
            "message": f"No active scan on RX{rx_index}",
        }
    
    return {
        "success": True,
        "message": f"Voice scan stopped on RX{rx_index}",
        "final_results": {
            "band": session.band_name,
            "sideband": session.sideband,
            "duration_s": session.duration_s(),
            "frequencies_scanned": session.frequencies_scanned,
            "detections_found": len(session.memory_channels),
            "memory_channels": [
                {
                    "index": ch.memory_index,
                    "frequency_khz": ch.frequency_hz / 1e3,
                    "sideband": ch.sideband,
                    "snr_db": ch.snr_db,
                    "confidence": ch.confidence,
                }
                for ch in session.memory_channels
            ],
        },
    }


@router.get("/scan/status")
async def get_voice_scan_status(rx_index: Optional[int] = Query(None, ge=0, le=7)):
    """Get voice scan status.
    
    Query parameters:
        rx_index: Receiver slot (0–7). If not provided, returns status for all.
    
    Returns:
        Scan status with progress, memory channels, statistics
    """
    mgr = get_voice_scan_manager()
    
    if rx_index is not None:
        return {
            f"rx{rx_index}": mgr.get_scan_status_for_receiver(rx_index),
        }
    else:
        return mgr.get_all_scan_status()


# Memory channel endpoints


@router.get("/memory")
async def get_memory_channels(rx_index: int = Query(..., ge=0, le=7)):
    """Get memory channels from scan on receiver slot.
    
    Query parameters:
        rx_index: Receiver slot (0–7) *required*
    
    Returns:
        List of memory channels with frequency, SNR, confidence, audio clip path
    """
    mgr = get_voice_scan_manager()
    rx_state = mgr.get_rx_state(rx_index)
    
    if not rx_state or not rx_state.session:
        return {"memory_channels": []}
    
    return {
        "memory_channels": [
            {
                "index": ch.memory_index,
                "frequency_hz": ch.frequency_hz,
                "frequency_khz": ch.frequency_hz / 1e3,
                "sideband": ch.sideband,
                "energy_rms": ch.voice_energy_rms,
                "carrier_offset_hz": ch.carrier_offset_hz,
                "snr_db": ch.snr_db,
                "confidence": ch.confidence,
                "audio_clip_path": str(ch.audio_clip_path) if ch.audio_clip_path else None,
                "audio_duration_s": ch.audio_duration_s,
            }
            for ch in rx_state.session.memory_channels
        ]
    }


@router.delete("/memory")
async def clear_memory_channels(rx_index: int = Query(..., ge=0, le=7)):
    """Clear memory channels on receiver slot.
    
    Query parameters:
        rx_index: Receiver slot (0–7) *required*
    
    Returns:
        Confirmation
    """
    mgr = get_voice_scan_manager()
    mgr.clear_memory_channels_for_receiver(rx_index)
    
    return {
        "success": True,
        "message": f"Memory channels cleared for RX{rx_index}",
    }
