"""Voice scan integration with receiver manager.

Manages voice scanning state and audio routing for each receiver slot.
Coordinates between kiwirecorder audio stream and VAD engine.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

import numpy as np

from .voice_scanner import VoiceScanEngine, VoiceScanSession, MemoryChannel
from .voice_bands import get_voice_band
from .voice_settings import get_voice_settings

logger = logging.getLogger(__name__)


@dataclass
class RxVoiceScanState:
    """Voice scanning state for a single receiver slot."""
    
    rx_index: int
    
    # Active session (if scanning)
    session: Optional[VoiceScanSession] = None
    scan_engine: Optional[VoiceScanEngine] = field(default_factory=VoiceScanEngine)
    
    # Audio buffering for detection window
    audio_buffer: dict[float, list] = field(default_factory=dict)  # freq_hz -> [chunks]
    buffer_duration_s: float = 0.5  # Keep 0.5s of audio per frequency
    
    # Timing
    last_frequency_update_unix: float = field(default_factory=time.time)
    last_audio_chunk_unix: float = field(default_factory=time.time)
    
    # Statistics
    total_chunks_processed: int = 0
    total_audio_seconds: float = 0.0
    
    def is_scanning(self) -> bool:
        return self.session is not None and self.session.is_scanning
    
    def get_memory_channels(self) -> list[MemoryChannel]:
        if not self.session:
            return []
        return self.session.memory_channels


class VoiceScanManager:
    """Manages voice scanning across all receiver slots."""
    
    def __init__(self, num_receivers: int = 8):
        self.num_receivers = num_receivers
        self._rx_states: list[RxVoiceScanState] = [
            RxVoiceScanState(rx_index=i) for i in range(num_receivers)
        ]
    
    def get_rx_state(self, rx_index: int) -> Optional[RxVoiceScanState]:
        """Get voice scan state for receiver slot."""
        if 0 <= rx_index < len(self._rx_states):
            return self._rx_states[rx_index]
        return None
    
    async def start_scan_for_receiver(
        self,
        rx_index: int,
        band_name: str,
        sideband: str,
        freq_step_hz: int = 1000,
    ) -> Optional[VoiceScanSession]:
        """Start voice scan on receiver slot.
        
        Args:
            rx_index: Receiver slot (0–7)
            band_name: Band identifier ("40m", "20m", etc.)
            sideband: "LSB" or "USB"
            freq_step_hz: Frequency step size (default 1 kHz)
        
        Returns:
            VoiceScanSession if started successfully, None on error
        """
        if not (0 <= rx_index < len(self._rx_states)):
            logger.error(f"Invalid receiver index: {rx_index}")
            return None
        
        # Get band definition
        voice_band = get_voice_band(band_name)
        if not voice_band:
            logger.error(f"Unknown voice band: {band_name}")
            return None
        
        # Stop any existing scan on this receiver
        self.stop_scan_for_receiver(rx_index)
        
        rx_state = self._rx_states[rx_index]
        
        # Start new scan session
        session = await rx_state.scan_engine.start_scan(
            band_name=band_name,
            sideband=sideband,
            start_freq_hz=voice_band.ssb_start_hz,
            end_freq_hz=voice_band.ssb_end_hz,
            receiver_index=rx_index,
            freq_step_hz=freq_step_hz,
        )
        
        rx_state.session = session
        logger.info(f"Started voice scan on RX{rx_index}: {session}")
        
        return session
    
    def stop_scan_for_receiver(self, rx_index: int) -> Optional[VoiceScanSession]:
        """Stop voice scan on receiver slot.
        
        Returns:
            Completed VoiceScanSession if one was active, None otherwise
        """
        if not (0 <= rx_index < len(self._rx_states)):
            return None
        
        rx_state = self._rx_states[rx_index]
        if not rx_state.session:
            return None
        
        session = rx_state.scan_engine.stop_scan()
        rx_state.session = None
        
        if session:
            logger.info(
                f"Stopped voice scan on RX{rx_index}: "
                f"found {session.detections_found} voices in {session.duration_s():.1f}s"
            )
        
        return session
    
    def process_audio_for_receiver(
        self,
        rx_index: int,
        audio_chunk: np.ndarray,
        current_freq_hz: float,
        sideband: str = "LSB",
        chunk_timestamp_s: float = 0.0,
    ) -> None:
        """Process audio chunk for receiver during scanning.
        
        Called from receiver_manager when audio is available.
        Accumulates audio and triggers detection analysis.
        
        Args:
            rx_index: Receiver slot
            audio_chunk: Audio data (16-bit samples or float -1.0–1.0)
            current_freq_hz: Current receiver frequency
            sideband: "LSB" or "USB"
            chunk_timestamp_s: Absolute timestamp for stability tracking
        """
        if not (0 <= rx_index < len(self._rx_states)):
            return
        
        rx_state = self._rx_states[rx_index]
        
        # Only process if scanning
        if not rx_state.is_scanning():
            return
        
        # Convert audio to float if needed
        if audio_chunk.dtype != np.float32 and audio_chunk.dtype != np.float64:
            audio_float = np.array(audio_chunk, dtype=np.float32) / 32768.0
        else:
            audio_float = np.array(audio_chunk, dtype=np.float32)
        
        # Process chunk through VAD
        detection = rx_state.scan_engine.process_audio_chunk(
            audio_float,
            current_freq_hz=current_freq_hz,
            chunk_timestamp_s=chunk_timestamp_s,
        )
        
        # Track statistics
        rx_state.total_chunks_processed += 1
        rx_state.total_audio_seconds += len(audio_float) / 8000.0
        rx_state.last_audio_chunk_unix = time.time()
        
        if detection and rx_state.session:
            # Log detection for debugging
            settings = get_voice_settings()
            if detection.detection_confidence >= settings.min_confidence_threshold:
                logger.debug(f"RX{rx_index} voice detection: {detection}")
                
                # If stable, finalize detection
                if detection.is_stable:
                    # Save audio clip from buffer
                    audio_clip = None
                    if settings.capture_enabled:
                        # Use the current chunk as the audio clip
                        audio_clip = audio_float
                    
                    memory_channel = rx_state.scan_engine.finalize_detection(
                        current_freq_hz,
                        audio_clip=audio_clip,
                    )
                    
                    if memory_channel:
                        logger.info(
                            f"RX{rx_index} saved memory channel: {memory_channel}"
                        )
    
    def get_scan_status_for_receiver(self, rx_index: int) -> dict:
        """Get scan status for API response.
        
        Returns:
            Dict with scan status (is_scanning, progress, memory_channels, etc.)
        """
        rx_state = self.get_rx_state(rx_index)
        if not rx_state:
            return {}
        
        if not rx_state.session:
            return {
                "is_scanning": False,
                "memory_channels": [],
            }
        
        session = rx_state.session
        
        return {
            "is_scanning": session.is_scanning,
            "band": session.band_name,
            "sideband": session.sideband,
            "current_frequency_hz": session.current_freq_hz or 0,
            "start_frequency_hz": session.start_freq_hz,
            "end_frequency_hz": session.end_freq_hz,
            "progress_percent": session.progress_percent(),
            "duration_s": session.duration_s(),
            "frequencies_scanned": session.frequencies_scanned,
            "detections_found": session.detections_found,
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
                    "detected_at_unix": ch.detected_at_unix,
                }
                for ch in session.memory_channels
            ],
            "statistics": {
                "total_chunks_processed": rx_state.total_chunks_processed,
                "total_audio_seconds": rx_state.total_audio_seconds,
            },
        }
    
    def clear_memory_channels_for_receiver(self, rx_index: int) -> None:
        """Clear memory channels for receiver."""
        rx_state = self.get_rx_state(rx_index)
        if rx_state:
            rx_state.scan_engine.clear_memory_channels()
    
    def get_all_scan_status(self) -> dict:
        """Get scan status for all receivers.
        
        Returns:
            Dict mapping rx_index -> scan_status
        """
        return {
            i: self.get_scan_status_for_receiver(i)
            for i in range(self.num_receivers)
        }


# Global instance
_voice_scan_manager: Optional[VoiceScanManager] = None


def get_voice_scan_manager() -> VoiceScanManager:
    """Get or create global voice scan manager."""
    global _voice_scan_manager
    if _voice_scan_manager is None:
        _voice_scan_manager = VoiceScanManager(num_receivers=8)
    return _voice_scan_manager
