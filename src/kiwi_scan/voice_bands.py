"""Voice band definitions and SSB frequency ranges.

Maps band names to SSB phone ranges for scanning.
Region 2 (Americas) frequencies by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .bandplan import BANDPLAN, BandPlanSegment


@dataclass(frozen=True)
class VoiceBand:
    """Voice band definition with SSB frequency range."""
    
    name: str
    center_freq_hz: float
    ssb_start_hz: float
    ssb_end_hz: float
    region: str = "region2"
    
    def ssb_start_khz(self) -> float:
        return self.ssb_start_hz / 1e3
    
    def ssb_end_khz(self) -> float:
        return self.ssb_end_hz / 1e3
    
    def bandwidth_hz(self) -> float:
        return self.ssb_end_hz - self.ssb_start_hz
    
    def __str__(self) -> str:
        return (
            f"{self.name} ({self.ssb_start_khz():.0f}–{self.ssb_end_khz():.0f} kHz, "
            f"{self.bandwidth_hz()/1e3:.1f} kHz wide)"
        )


# Region 2 (Americas) SSB phone bands
# Based on common convention and ARRL band plan
VOICE_BANDS_REGION2: dict[str, VoiceBand] = {
    "160m": VoiceBand(
        name="160m Phone",
        center_freq_hz=1900e3,
        ssb_start_hz=1843e3,  # 1843 kHz (Region 2)
        ssb_end_hz=2000e3,
    ),
    "80m": VoiceBand(
        name="80m Phone",
        center_freq_hz=3750e3,
        ssb_start_hz=3600e3,
        ssb_end_hz=4000e3,
    ),
    "40m": VoiceBand(
        name="40m Phone",
        center_freq_hz=7200e3,
        ssb_start_hz=7125e3,  # Region 2 convention
        ssb_end_hz=7300e3,
    ),
    "30m": VoiceBand(
        name="30m CW/Digital",  # No phone on 30m
        center_freq_hz=10136e3,
        ssb_start_hz=10100e3,
        ssb_end_hz=10150e3,
    ),
    "20m": VoiceBand(
        name="20m Phone",
        center_freq_hz=14250e3,
        ssb_start_hz=14150e3,
        ssb_end_hz=14350e3,
    ),
    "17m": VoiceBand(
        name="17m Phone",
        center_freq_hz=18130e3,
        ssb_start_hz=18110e3,
        ssb_end_hz=18168e3,
    ),
    "15m": VoiceBand(
        name="15m Phone",
        center_freq_hz=21300e3,
        ssb_start_hz=21200e3,
        ssb_end_hz=21450e3,
    ),
    "12m": VoiceBand(
        name="12m Phone",
        center_freq_hz=24960e3,
        ssb_start_hz=24950e3,
        ssb_end_hz=24990e3,
    ),
    "10m": VoiceBand(
        name="10m Phone",
        center_freq_hz=28500e3,
        ssb_start_hz=28500e3,
        ssb_end_hz=29700e3,
    ),
}

# Non-Region 2 variants (phone starts at 7.175 MHz on 40m)
VOICE_BANDS_NON_REGION2: dict[str, VoiceBand] = {
    **VOICE_BANDS_REGION2,
    "40m": VoiceBand(
        name="40m Phone",
        center_freq_hz=7225e3,
        ssb_start_hz=7175e3,  # Non-Region 2
        ssb_end_hz=7300e3,
        region="non_region2",
    ),
}

# Select default region
VOICE_BANDS = VOICE_BANDS_REGION2


def get_voice_band(band_name: str, region: str = "region2") -> Optional[VoiceBand]:
    """Get VoiceBand definition by name.
    
    Args:
        band_name: Band identifier ("40m", "20m", etc.)
        region: "region2" (default) or "non_region2"
    
    Returns:
        VoiceBand if found, None otherwise
    """
    bands = VOICE_BANDS_NON_REGION2 if region != "region2" else VOICE_BANDS_REGION2
    return bands.get(band_name)


def list_voice_bands(region: str = "region2") -> list[VoiceBand]:
    """List available voice bands for region.
    
    Args:
        region: "region2" (default) or "non_region2"
    
    Returns:
        List of VoiceBand objects, sorted by frequency
    """
    bands = VOICE_BANDS_NON_REGION2 if region != "region2" else VOICE_BANDS_REGION2
    return sorted(bands.values(), key=lambda b: b.center_freq_hz)


def voice_band_from_freq(freq_hz: float, region: str = "region2") -> Optional[VoiceBand]:
    """Find voice band containing frequency.
    
    Args:
        freq_hz: Frequency in Hz
        region: Region for band plan
    
    Returns:
        VoiceBand if frequency is in a phone range, None otherwise
    """
    bands = VOICE_BANDS_NON_REGION2 if region != "region2" else VOICE_BANDS_REGION2
    for band in bands.values():
        if band.ssb_start_hz <= freq_hz <= band.ssb_end_hz:
            return band
    return None


# UI-friendly descriptions
VOICE_BAND_DESCRIPTIONS: dict[str, str] = {
    "160m": "160m Phone (1.843–2.0 MHz)",
    "80m": "80m Phone (3.6–4.0 MHz)",
    "40m": "40m Phone (7.125–7.3 MHz, Region 2)",
    "30m": "30m CW/Digital (10.1–10.15 MHz, no phone)",
    "20m": "20m Phone (14.15–14.35 MHz)",
    "17m": "17m Phone (18.11–18.168 MHz)",
    "15m": "15m Phone (21.2–21.45 MHz)",
    "12m": "12m Phone (24.95–24.99 MHz)",
    "10m": "10m Phone (28.5–29.7 MHz)",
}
