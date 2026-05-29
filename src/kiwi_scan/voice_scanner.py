"""Voice scanner for SSB band scanning and memory channel management.

Handles:
- Frequency sweeping across SSB range (1 kHz steps)
- Voice detection with 10-second wait for stability
- Memory channel storage (up to 5 channels)
- Audio clip capture to /tmp
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from datetime import datetime
import wave
import struct

import numpy as np

from .voice_activity_detector import VADEngine, VoiceDetection

logger = logging.getLogger(__name__)

# Memory channel limits
MAX_MEMORY_CHANNELS = 5
DETECTION_WAIT_TIMEOUT_S = 10.0  # Wait up to 10s for signal to stabilize

# Audio capture
AUDIO_CAPTURE_DIR = Path("/tmp/kiwiscan_voice")
AUDIO_CAPTURE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class MemoryChannel:
    """Stored memory of detected voice channel."""
    
    memory_index: int  # 1-5
    frequency_hz: float
    sideband: str  # "LSB" or "USB"
    
    # Detection metrics
    voice_energy_rms: float
    carrier_offset_hz: float
    snr_db: float
    confidence: float
    
    # Audio storage
    audio_clip_path: Optional[Path] = None
    audio_duration_s: float = 0.0
    
    # Metadata
    detected_at_unix: float = field(default_factory=time.time)
    
    def frequency_mhz(self) -> float:
        return self.frequency_hz / 1e6
    
    def __str__(self) -> str:
        return (
            f"[Memory {self.memory_index}] {self.frequency_mhz():.4f} MHz {self.sideband} | "
            f"energy={self.voice_energy_rms:.4f} snr={self.snr_db:.1f}dB conf={self.confidence:.2f}"
        )


@dataclass
class VoiceScanSession:
    """Active voice scanning session."""
    
    band_name: str  # "40m", "20m", etc.
    sideband: str  # "LSB" or "USB"
    start_freq_hz: float
    end_freq_hz: float
    receiver_index: int  # RX slot number
    
    # Scanning state
    is_scanning: bool = False
    current_freq_hz: Optional[float] = None
    start_time_unix: float = field(default_factory=time.time)
    
    # Memory channels found
    memory_channels: list[MemoryChannel] = field(default_factory=list)
    
    # Scan progress
    frequencies_scanned: int = 0
    detections_found: int = 0
    
    def frequency_mhz_display(self) -> str:
        if self.current_freq_hz:
            return f"{self.current_freq_hz/1e3:.2f}"
        return "--"
    
    def progress_percent(self) -> int:
        if self.end_freq_hz <= self.start_freq_hz:
            return 100
        range_hz = self.end_freq_hz - self.start_freq_hz
        if self.current_freq_hz is None:
            return 0
        progress = (self.current_freq_hz - self.start_freq_hz) / range_hz
        return int(max(0, min(100, progress * 100)))
    
    def duration_s(self) -> float:
        return time.time() - self.start_time_unix
    
    def __str__(self) -> str:
        return (
            f"VoiceScan {self.band_name} {self.sideband} "
            f"({self.current_freq_hz/1e3:.2f} kHz if scanning) | "
            f"Found {self.detections_found} | "
            f"Scanned {self.frequencies_scanned} freqs | "
            f"{self.progress_percent()}% | {self.duration_s():.0f}s"
        )


class VoiceScanEngine:
    """Orchestrates voice scanning across frequency range."""
    
    def __init__(self):
        self.vad_engine = VADEngine(sample_rate_hz=8000)
        self.active_session: Optional[VoiceScanSession] = None
        self._detection_buffer: dict[float, list] = {}  # freq_hz -> [audio chunks]
    
    async def start_scan(
        self,
        band_name: str,
        sideband: str,
        start_freq_hz: float,
        end_freq_hz: float,
        receiver_index: int = 0,
        freq_step_hz: int = 1000,  # 1 kHz steps
    ) -> VoiceScanSession:
        """Start frequency sweep scan for voice activity.
        
        Args:
            band_name: Band identifier ("40m", "20m", etc.)
            sideband: "LSB" or "USB"
            start_freq_hz: Scan start frequency
            end_freq_hz: Scan end frequency
            receiver_index: Receiver slot number
            freq_step_hz: Frequency step size (default 1 kHz)
        
        Returns:
            VoiceScanSession for monitoring progress
        """
        logger.info(
            f"Starting voice scan {band_name} {sideband} "
            f"{start_freq_hz/1e3:.2f}–{end_freq_hz/1e3:.2f} kHz"
        )
        
        self.active_session = VoiceScanSession(
            band_name=band_name,
            sideband=sideband,
            start_freq_hz=start_freq_hz,
            end_freq_hz=end_freq_hz,
            receiver_index=receiver_index,
            is_scanning=True,
        )
        
        # Generate frequency list
        frequencies = list(
            np.arange(start_freq_hz, end_freq_hz + 1, freq_step_hz, dtype=np.float64)
        )
        logger.info(f"Will scan {len(frequencies)} frequencies")
        
        # For now, return session (actual scanning happens via external audio stream callback)
        return self.active_session
    
    def stop_scan(self) -> Optional[VoiceScanSession]:
        """Stop active scan session."""
        if self.active_session:
            self.active_session.is_scanning = False
            logger.info(f"Stopped scan: {self.active_session}")
            session = self.active_session
            self.active_session = None
            self._detection_buffer.clear()
            return session
        return None
    
    def process_audio_chunk(
        self,
        audio_chunk: np.ndarray,
        current_freq_hz: float,
        chunk_timestamp_s: float = 0.0,
    ) -> Optional[VoiceDetection]:
        """Process audio chunk during scanning.
        
        Called by receiver manager when audio is available at current frequency.
        Returns VoiceDetection if voice detected, None otherwise.
        """
        if not self.active_session or not self.active_session.is_scanning:
            return None
        
        sideband = self.active_session.sideband
        
        # Analyze chunk
        detection = self.vad_engine.analyze_chunk(
            audio_chunk,
            frequency_hz=current_freq_hz,
            sideband=sideband,
            chunk_timestamp_s=chunk_timestamp_s,
        )
        
        # Track for averaging
        if current_freq_hz not in self._detection_buffer:
            self._detection_buffer[current_freq_hz] = []
        self._detection_buffer[current_freq_hz].append(detection)
        
        return detection
    
    def finalize_detection(
        self,
        frequency_hz: float,
        audio_clip: Optional[np.ndarray] = None,
    ) -> Optional[MemoryChannel]:
        """Finalize detection and store as memory channel.
        
        Called when signal is stable or detection window expires.
        """
        if not self.active_session:
            return None
        
        # Get averaged metrics from buffer
        detections = self._detection_buffer.get(frequency_hz, [])
        if not detections:
            return None
        
        # Use latest detection as representative
        latest = detections[-1]
        
        # Average confidence from all detections at this frequency
        avg_confidence = np.mean([d.detection_confidence for d in detections])
        
        # Skip if low confidence
        if avg_confidence < 0.3:
            logger.debug(f"Skipping low-confidence detection at {frequency_hz/1e3:.2f} kHz")
            return None
        
        # Check memory limit
        if len(self.active_session.memory_channels) >= MAX_MEMORY_CHANNELS:
            logger.info("Memory channel limit reached")
            return None
        
        # Save audio clip if provided
        audio_clip_path = None
        audio_duration_s = 0.0
        if audio_clip is not None and len(audio_clip) > 0:
            audio_clip_path, audio_duration_s = self._save_audio_clip(
                frequency_hz,
                self.active_session.sideband,
                audio_clip,
            )
        
        # Create memory channel
        channel_index = len(self.active_session.memory_channels) + 1
        memory_channel = MemoryChannel(
            memory_index=channel_index,
            frequency_hz=frequency_hz,
            sideband=self.active_session.sideband,
            voice_energy_rms=latest.voice_energy_rms,
            carrier_offset_hz=latest.carrier_offset_hz,
            snr_db=latest.snr_db,
            confidence=avg_confidence,
            audio_clip_path=audio_clip_path,
            audio_duration_s=audio_duration_s,
        )
        
        self.active_session.memory_channels.append(memory_channel)
        self.active_session.detections_found += 1
        
        logger.info(f"Created {memory_channel}")
        
        # Clear buffer for this frequency
        self._detection_buffer.pop(frequency_hz, None)
        
        return memory_channel
    
    def _save_audio_clip(
        self,
        frequency_hz: float,
        sideband: str,
        audio_chunk: np.ndarray,
        sample_rate_hz: int = 8000,
    ) -> tuple[Path, float]:
        """Save audio clip to /tmp as WAV file.
        
        Returns (filepath, duration_seconds)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        freq_label = f"{frequency_hz/1e3:.0f}kHz"
        filename = f"voice_{freq_label}_{sideband}_{timestamp}.wav"
        filepath = AUDIO_CAPTURE_DIR / filename
        
        try:
            # Ensure audio is int16
            if audio_chunk.dtype != np.int16:
                # Clip to [-1, 1] and convert
                audio_clipped = np.clip(audio_chunk, -1.0, 1.0)
                audio_int16 = (audio_clipped * 32767).astype(np.int16)
            else:
                audio_int16 = audio_chunk
            
            # Write WAV
            with wave.open(str(filepath), "wb") as wav_file:
                wav_file.setnchannels(1)  # Mono
                wav_file.setsampwidth(2)  # 16-bit
                wav_file.setframerate(sample_rate_hz)
                wav_file.writeframes(audio_int16.tobytes())
            
            duration_s = len(audio_int16) / sample_rate_hz
            logger.info(f"Saved audio clip: {filepath} ({duration_s:.1f}s)")
            
            return filepath, duration_s
        
        except Exception as err:
            logger.error(f"Failed to save audio clip: {err}")
            return None, 0.0
    
    def get_memory_channels(self) -> list[MemoryChannel]:
        """Get all memory channels from current session."""
        if not self.active_session:
            return []
        return self.active_session.memory_channels
    
    def clear_memory_channels(self) -> None:
        """Clear all memory channels (manual button press)."""
        if self.active_session:
            self.active_session.memory_channels.clear()
            self._detection_buffer.clear()
            self.vad_engine.reset()
            logger.info("Cleared memory channels")
