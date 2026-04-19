from __future__ import annotations


THIRTY_M_BAND = (10100.0, 10150.0)
SIXTY_M_BAND = (5250.0, 5450.0)


def resolve_voice_sideband(freq_khz: float, sideband: str | None = None) -> str:
    freq_khz = float(freq_khz)
    if freq_khz <= 0.0:
        raise ValueError("freq_khz must be > 0")

    if THIRTY_M_BAND[0] <= freq_khz < THIRTY_M_BAND[1]:
        raise ValueError("30m has no phone operation")

    requested_sideband = str(sideband or "").strip().upper()
    if requested_sideband and requested_sideband not in {"LSB", "USB"}:
        raise ValueError("sideband must be LSB or USB")

    if SIXTY_M_BAND[0] <= freq_khz < SIXTY_M_BAND[1]:
        return "USB"

    expected_sideband = "LSB" if freq_khz < 10000.0 else "USB"
    if requested_sideband and requested_sideband == expected_sideband:
        return requested_sideband
    return expected_sideband