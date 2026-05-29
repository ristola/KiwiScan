"""Voice Activity Detection (VAD) for SSB voice scanning.

Detects and analyzes voice activity in audio stream with metrics:
- voice_energy: RMS energy in 300–2700 Hz band
- carrier_offset: Peak frequency position (center of speech)
- snr: Signal-to-noise ratio in dB above floor
- stability: Duration signal persists (seconds)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from collections import deque
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# SSB voice bandwidth (ham radio standard)
SSB_LOW_HZ = 300
SSB_HIGH_HZ = 2700
SSB_BANDWIDTH_HZ = SSB_HIGH_HZ - SSB_LOW_HZ

# VAD thresholds
NOISE_FLOOR_RMS = 0.01  # RMS below which is noise
VOICE_ENERGY_THRESHOLD_RMS = 0.05  # Minimum voice energy to trigger detection
STABILITY_THRESHOLD_S = 2.0  # Signal must persist this long to be "stable"
CARRIER_DETECTION_THRESHOLD_RMS = 0.03  # Minimum energy to detect carrier peak


@dataclass(frozen=True)
class VoiceDetection:
    """Result of voice detection analysis on audio chunk."""
    
    frequency_hz: float  # Center frequency of Kiwi receiver (not modulation carrier)
    sideband: str  # "LSB" or "USB"
    
    voice_energy_rms: float  # RMS in SSB band (300–2700 Hz)
    carrier_offset_hz: float  # Peak freq relative to audio baseband (0–3000 Hz)
    snr_db: float  # Signal above noise floor
    noise_floor_rms: float  # Baseline noise level
    
    duration_s: float  # How long signal has been present
    is_stable: bool  # True if duration >= STABILITY_THRESHOLD_S
    detection_confidence: float  # 0.0–1.0, composite score
    
    def __str__(self) -> str:
        return (
            f"VoiceDetection @ {self.frequency_hz/1e3:.2f} kHz {self.sideband} | "
            f"voice_energy={self.voice_energy_rms:.4f} | "
            f"carrier_offset={self.carrier_offset_hz:.0f} Hz | "
            f"snr={self.snr_db:.1f} dB | "
            f"stable={'✓' if self.is_stable else '✗'} ({self.duration_s:.1f}s) | "
            f"confidence={self.detection_confidence:.2f}"
        )


class VADEngine:
    """Voice Activity Detector for SSB audio stream.
    
    Processes 16-bit audio chunks at 8 kHz sample rate (standard kiwirecorder LSB/USB output).
    Maintains rolling statistics for noise floor estimation.
    """
    
    def __init__(
        self,
        sample_rate_hz: int = 8000,
        noise_floor_window_s: float = 5.0,
        carrier_detect_bins: int = 10,
    ):
        """Initialize VAD engine.
        
        Args:
            sample_rate_hz: Audio sample rate (8 kHz for LSB/USB)
            noise_floor_window_s: Duration for rolling noise floor estimate
            carrier_detect_bins: FFT bins around each frequency to detect carrier
        """
        self.sample_rate_hz = int(sample_rate_hz)
        self.noise_floor_window_s = float(noise_floor_window_s)
        self.carrier_detect_bins = int(carrier_detect_bins)
        
        # Rolling buffer for noise floor estimation (low-energy frames)
        self._noise_floor_samples = deque(maxlen=int(sample_rate_hz * noise_floor_window_s))
        self._noise_floor_rms = NOISE_FLOOR_RMS
        
        # Track detection history for stability measurement
        self._detection_start_time: Optional[float] = None
        self._last_detection_time: Optional[float] = None
        
    def analyze_chunk(
        self,
        audio_chunk: np.ndarray,
        frequency_hz: float,
        sideband: str = "LSB",
        chunk_timestamp_s: float = 0.0,
    ) -> VoiceDetection:
        """Analyze audio chunk for voice activity.
        
        Args:
            audio_chunk: Numpy array of 16-bit audio samples (mono)
            frequency_hz: Receiver frequency (for logging)
            sideband: "LSB" or "USB"
            chunk_timestamp_s: Absolute timestamp of this chunk (for stability)
        
        Returns:
            VoiceDetection with analysis results
        """
        # Normalize to [-1.0, 1.0]
        audio_float = np.array(audio_chunk, dtype=np.float32) / 32768.0
        
        # Compute RMS and extract SSB band energy
        voice_energy_rms = np.sqrt(np.mean(audio_float ** 2))
        
        # Extract 300–2700 Hz band and compute energy
        ssb_band_rms = self._extract_ssb_band_energy(audio_float)
        
        # Detect carrier peak position within SSB band
        carrier_offset_hz = self._detect_carrier_offset(audio_float)
        
        # Update noise floor estimate
        self._update_noise_floor(voice_energy_rms)
        
        # Compute SNR
        snr_db = self._compute_snr_db(voice_energy_rms)
        
        # Determine stability and duration
        is_voice_present = voice_energy_rms > VOICE_ENERGY_THRESHOLD_RMS
        duration_s, is_stable = self._update_stability(
            is_voice_present, chunk_timestamp_s
        )
        
        # Composite confidence score
        confidence = self._compute_confidence(
            ssb_band_rms, snr_db, is_stable
        )
        
        return VoiceDetection(
            frequency_hz=frequency_hz,
            sideband=sideband,
            voice_energy_rms=ssb_band_rms,
            carrier_offset_hz=carrier_offset_hz,
            snr_db=snr_db,
            noise_floor_rms=self._noise_floor_rms,
            duration_s=duration_s,
            is_stable=is_stable,
            detection_confidence=confidence,
        )
    
    def _extract_ssb_band_energy(self, audio_float: np.ndarray) -> float:
        """Extract energy in 300–2700 Hz band using FFT."""
        if len(audio_float) == 0:
            return 0.0
        
        # Compute FFT
        fft = np.fft.rfft(audio_float)
        freqs = np.fft.rfftfreq(len(audio_float), d=1.0 / self.sample_rate_hz)
        
        # Find bins in SSB band
        mask = (freqs >= SSB_LOW_HZ) & (freqs <= SSB_HIGH_HZ)
        ssb_bins = np.abs(fft[mask])
        
        if len(ssb_bins) == 0:
            return 0.0
        
        # RMS of SSB band (normalized by bin count for fair comparison)
        ssb_energy = np.sqrt(np.mean(ssb_bins ** 2)) / len(audio_float)
        return float(ssb_energy)
    
    def _detect_carrier_offset(self, audio_float: np.ndarray) -> float:
        """Detect peak frequency position within 300–2700 Hz band.
        
        Returns carrier offset in Hz (0–3000 representing the audio band).
        """
        if len(audio_float) < 128:
            return 1500.0  # Default to center
        
        # Compute FFT
        fft = np.fft.rfft(audio_float)
        freqs = np.fft.rfftfreq(len(audio_float), d=1.0 / self.sample_rate_hz)
        magnitudes = np.abs(fft)
        
        # Find bins in SSB band
        mask = (freqs >= SSB_LOW_HZ) & (freqs <= SSB_HIGH_HZ)
        if not np.any(mask):
            return 1500.0
        
        # Find peak frequency in band
        band_freqs = freqs[mask]
        band_mags = magnitudes[mask]
        peak_idx = np.argmax(band_mags)
        carrier_offset = float(band_freqs[peak_idx])
        
        return max(SSB_LOW_HZ, min(SSB_HIGH_HZ, carrier_offset))
    
    def _update_noise_floor(self, chunk_rms: float) -> None:
        """Update rolling noise floor estimate."""
        # Only use low-energy frames for noise estimate
        if chunk_rms < NOISE_FLOOR_RMS:
            self._noise_floor_samples.append(chunk_rms)
        
        # Update noise floor as median of low-energy frames
        if len(self._noise_floor_samples) > 0:
            self._noise_floor_rms = float(np.median(list(self._noise_floor_samples)))
    
    def _compute_snr_db(self, voice_energy_rms: float) -> float:
        """Compute SNR in dB relative to noise floor."""
        if self._noise_floor_rms <= 0:
            return 0.0
        
        ratio = voice_energy_rms / max(self._noise_floor_rms, 1e-6)
        snr_db = 20.0 * math.log10(max(ratio, 1e-6))
        return float(snr_db)
    
    def _update_stability(
        self, is_voice_present: bool, chunk_timestamp_s: float
    ) -> tuple[float, bool]:
        """Update stability tracking and return duration and stability flag."""
        if is_voice_present:
            if self._detection_start_time is None:
                self._detection_start_time = chunk_timestamp_s
            self._last_detection_time = chunk_timestamp_s
        else:
            # If no voice for > 0.5s, reset detection window
            if (self._last_detection_time is not None and
                chunk_timestamp_s - self._last_detection_time > 0.5):
                self._detection_start_time = None
                self._last_detection_time = None
        
        if self._detection_start_time is None:
            return 0.0, False
        
        duration_s = chunk_timestamp_s - self._detection_start_time
        is_stable = duration_s >= STABILITY_THRESHOLD_S
        
        return duration_s, is_stable
    
    def _compute_confidence(
        self, voice_energy_rms: float, snr_db: float, is_stable: bool
    ) -> float:
        """Compute composite confidence score (0.0–1.0)."""
        # Normalize energy (assume max voice energy ~0.5 RMS)
        energy_score = min(voice_energy_rms / 0.2, 1.0)
        
        # Normalize SNR (assume good SNR > 15 dB)
        snr_score = min(max(snr_db / 15.0, 0.0), 1.0)
        
        # Stability bonus
        stability_bonus = 0.2 if is_stable else 0.0
        
        # Composite score
        confidence = 0.5 * energy_score + 0.3 * snr_score + 0.2 * stability_bonus
        return max(0.0, min(confidence, 1.0))
    
    def reset(self) -> None:
        """Reset detection state (for scanning new frequency)."""
        self._detection_start_time = None
        self._last_detection_time = None
        self._noise_floor_samples.clear()
        self._noise_floor_rms = NOISE_FLOOR_RMS
