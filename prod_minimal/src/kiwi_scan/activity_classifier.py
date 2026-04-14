from __future__ import annotations

from typing import Any


_MODE_WIDTH_PATTERNS: tuple[tuple[str, str, float, float], ...] = (
    ("PSK31", "digital", 20.0, 60.0),
    ("FT8", "digital", 40.0, 80.0),
    ("FT4", "digital", 70.0, 140.0),
    ("CW", "cw", 80.0, 220.0),
    ("RTTY", "digital", 170.0, 360.0),
    ("SSB Phone", "phone", 1400.0, 3500.0),
    ("AM", "am", 4500.0, 12000.0),
)


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0.0:
        return None
    return number


def classify_activity_width(
    width_hz: object,
    *,
    type_guess: object | None = None,
    bandplan: object | None = None,
) -> dict[str, Any]:
    width = _finite_float(width_hz)
    mode_hint = "Unknown"
    activity_kind = "unknown"
    bandwidth_bucket = "unknown"
    matched_width_pattern = False

    if width is not None:
        matches: list[tuple[float, str, str, float, float]] = []
        for label, kind, low_hz, high_hz in _MODE_WIDTH_PATTERNS:
            if low_hz <= width <= high_hz:
                midpoint_hz = (low_hz + high_hz) / 2.0
                matches.append((abs(width - midpoint_hz), label, kind, low_hz, high_hz))
        if matches:
            _, mode_hint, activity_kind, low_hz, high_hz = min(matches, key=lambda item: item[0])
            bandwidth_bucket = f"{int(low_hz)}-{int(high_hz)} Hz"
            matched_width_pattern = True
        elif width < 20.0:
            mode_hint = "Tone-like"
            activity_kind = "tone"
            bandwidth_bucket = "<20 Hz"
        elif width < 500.0:
            mode_hint = "Narrow Digital"
            activity_kind = "digital"
            bandwidth_bucket = "20-500 Hz"
        elif width < 1400.0:
            mode_hint = "Medium Digital"
            activity_kind = "digital"
            bandwidth_bucket = "500-1400 Hz"
        elif width < 4500.0:
            mode_hint = "SSB Phone"
            activity_kind = "phone"
            bandwidth_bucket = "1400-4500 Hz"
        elif width < 15000.0:
            mode_hint = "AM"
            activity_kind = "am"
            bandwidth_bucket = "4500-15000 Hz"
        else:
            mode_hint = "Wideband"
            activity_kind = "wide"
            bandwidth_bucket = ">=15000 Hz"

    type_hint = str(type_guess or "").strip().lower()
    bandplan_hint = str(bandplan or "").strip().lower()
    if activity_kind in {"unknown", "tone"}:
        if "phone" in type_hint or bandplan_hint == "phone":
            if width is None or width >= 1200.0:
                mode_hint = "SSB Phone"
                activity_kind = "phone"
        elif "cw" in type_hint or bandplan_hint == "cw":
            mode_hint = "CW"
            activity_kind = "cw"
        elif "digital" in type_hint:
            mode_hint = "Digital"
            activity_kind = "digital"

    if bandplan_hint == "phone" and activity_kind == "wide":
        mode_hint = "SSB Phone"
        activity_kind = "phone"

    return {
        "mode_hint": mode_hint,
        "activity_kind": activity_kind,
        "phone_like": activity_kind == "phone",
        "voice_like": activity_kind in {"phone", "am"},
        "narrow_like": activity_kind in {"tone", "cw", "digital"},
        "bandwidth_hz": width,
        "bandwidth_bucket": bandwidth_bucket,
        "matched_width_pattern": matched_width_pattern,
    }