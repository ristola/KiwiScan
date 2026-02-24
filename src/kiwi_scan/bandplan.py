from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class BandPlanSegment:
    start_hz: float
    end_hz: float
    label: str
    modes: tuple[str, ...] = ()

    def contains(self, freq_hz: float) -> bool:
        f = float(freq_hz)
        return float(self.start_hz) <= f < float(self.end_hz)


# Minimal, opinionated bandplan hints.
# Not regulatory; intended only as a heuristic layer for classifying detections.
#
# Notes:
# - Segment boundaries vary by region (IARU/ARRL/etc) and license class.
# - For 40m phone, Region 2 common convention starts at 7.125 MHz.
#   Many other regions have phone starting at 7.175 MHz.
BANDPLAN: dict[str, tuple[BandPlanSegment, ...]] = {
    "160m": (
        BandPlanSegment(1800000, 1838000, "CW", ("cw",)),
        BandPlanSegment(1838000, 1843000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(1843000, 2000000, "Phone", ("phone", "ssb")),
    ),
    "80m": (
        BandPlanSegment(3500000, 3575000, "CW", ("cw",)),
        BandPlanSegment(3575000, 3600000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(3600000, 4000000, "Phone", ("phone", "ssb")),
    ),
    "60m": (
        # 60m is channelized in the US; chart shows "Dig" windows. Treat as heuristic.
        BandPlanSegment(5330500, 5333500, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(5346500, 5349500, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(5358500, 5361500, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(5371500, 5374500, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(5403500, 5406500, "RTTY", ("digital", "rtty", "data")),
    ),
    "40m": (
        # Broad "all modes" region, with more-specific overlays below.
        BandPlanSegment(7000000, 7125000, "All modes", ("cw", "digital", "phone")),

        # Common narrow digital activity (RTTY/data/various) and WSPR.
        BandPlanSegment(7038400, 7038800, "WSPR", ("digital", "wspr")),

        # Very narrow windows for popular calling frequencies (heuristics).
        BandPlanSegment(7047400, 7048100, "FT4", ("digital", "ft4")),
        BandPlanSegment(7073500, 7074500, "FT8", ("digital", "ft8")),

        # Phone/SSB region (Region 2 convention).
        BandPlanSegment(7125000, 7300000, "Phone", ("phone", "ssb")),
    ),
    "30m": (
        BandPlanSegment(10100000, 10130000, "CW", ("cw",)),
        BandPlanSegment(10130000, 10150000, "RTTY", ("digital", "rtty", "data")),
    ),
    "20m": (
        BandPlanSegment(14000000, 14025000, "CW", ("cw",)),
        BandPlanSegment(14025000, 14150000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(14150000, 14350000, "Phone", ("phone", "ssb")),
    ),
    "17m": (
        BandPlanSegment(18068000, 18110000, "CW", ("cw",)),
        BandPlanSegment(18110000, 18168000, "Phone", ("phone", "ssb")),
    ),
    "15m": (
        BandPlanSegment(21000000, 21025000, "CW", ("cw",)),
        BandPlanSegment(21025000, 21200000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(21200000, 21450000, "Phone", ("phone", "ssb")),
    ),
    "12m": (
        BandPlanSegment(24890000, 24930000, "CW", ("cw",)),
        BandPlanSegment(24930000, 24950000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(24950000, 24990000, "Phone", ("phone", "ssb")),
    ),
    "10m": (
        BandPlanSegment(28000000, 28300000, "CW", ("cw",)),
        BandPlanSegment(28300000, 28500000, "RTTY", ("digital", "rtty", "data")),
        BandPlanSegment(28500000, 29700000, "Phone", ("phone", "ssb")),
    ),
}


REGIONAL_OVERRIDES: dict[tuple[str, str], tuple[BandPlanSegment, ...]] = {
    # "outside region 2" shown in the provided chart: phone begins at 7.175 MHz.
    ("40m", "non_region2"): (
        BandPlanSegment(7000000, 7175000, "All modes", ("cw", "digital", "phone")),
        BandPlanSegment(7038400, 7038800, "WSPR", ("digital", "wspr")),
        BandPlanSegment(7047400, 7048100, "FT4", ("digital", "ft4")),
        BandPlanSegment(7073500, 7074500, "FT8", ("digital", "ft8")),
        BandPlanSegment(7175000, 7300000, "Phone", ("phone", "ssb")),
    ),
}


def band_from_freq(freq_hz: float) -> str | None:
    f = float(freq_hz)
    if 1800000 <= f <= 2000000:
        return "160m"
    if 3500000 <= f <= 4000000:
        return "80m"
    if 5330000 <= f <= 5406500:
        return "60m"
    if 7000000 <= f <= 7300000:
        return "40m"
    if 10100000 <= f <= 10150000:
        return "30m"
    if 14000000 <= f <= 14350000:
        return "20m"
    if 18068000 <= f <= 18168000:
        return "17m"
    if 21000000 <= f <= 21450000:
        return "15m"
    if 24890000 <= f <= 24990000:
        return "12m"
    if 28000000 <= f <= 29700000:
        return "10m"
    return None


def bandplan_segments_for_freq(
    freq_hz: float, *, region: str = "region2"
) -> tuple[str | None, tuple[BandPlanSegment, ...]]:
    band = band_from_freq(freq_hz)
    if band is None:
        return None, ()
    if region != "region2":
        override = REGIONAL_OVERRIDES.get((band, region))
        if override is not None:
            return band, override
    return band, BANDPLAN.get(band, ())


def bandplan_label(freq_hz: float, *, region: str = "region2") -> str | None:
    band, segs = bandplan_segments_for_freq(freq_hz, region=region)
    if band is None:
        return None
    matches = [seg for seg in segs if seg.contains(freq_hz)]
    if not matches:
        return None
    # Prefer the narrowest (most specific) match when segments overlap.
    best = min(matches, key=lambda s: float(s.end_hz) - float(s.start_hz))
    return best.label


def bandplan_modes(freq_hz: float, *, region: str = "region2") -> tuple[str, ...] | None:
    band, segs = bandplan_segments_for_freq(freq_hz, region=region)
    if band is None:
        return None
    matches = [seg for seg in segs if seg.contains(freq_hz)]
    if not matches:
        return None
    best = min(matches, key=lambda s: float(s.end_hz) - float(s.start_hz))
    return best.modes


def bandplan_ranges_for_label(
    label: str, *, band: str, region: str = "region2"
) -> list[tuple[float, float]]:
    """Return all (start_hz, end_hz) ranges for a given label within a band.

    This is primarily for user-facing display of active filter ranges.
    """

    if band not in BANDPLAN:
        return []
    segs = REGIONAL_OVERRIDES.get((band, region)) if region != "region2" else None
    if segs is None:
        segs = BANDPLAN.get(band, ())
    out: list[tuple[float, float]] = []
    for s in segs:
        if s.label == label:
            out.append((float(s.start_hz), float(s.end_hz)))
    return out


def combine_type_hints(*, width_guess: str, bandplan_label: str | None) -> str:
    """Combine occupied-bandwidth guess with a bandplan label.

    The goal is a practical hint string, not a definitive mode.
    """

    if not bandplan_label:
        return width_guess

    # If we're in a phone segment but the width looks narrow, keep both.
    if bandplan_label.lower() == "phone" and width_guess in {"very_narrow", "narrow"}:
        return f"{width_guess}+phone_segment"

    # If we're in CW or narrow-digital segments but the width looks wide, keep both.
    if bandplan_label.lower() in {"cw", "narrow digital"} and width_guess in {"medium", "wide"}:
        return f"{width_guess}+{bandplan_label.lower().replace(' ', '_')}_segment"

    return f"{width_guess}+{bandplan_label.lower().replace(' ', '_')}"
