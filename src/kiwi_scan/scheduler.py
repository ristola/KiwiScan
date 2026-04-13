from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

BandStatus = str  # "OPEN" | "MARGINAL" | "CLOSED"


@dataclass(frozen=True)
class UsabilityTable:
    """Seasonal HF usability table for a mode.

    Hours are local time in 24h blocks.
    """

    season: str  # "summer" | "spring_fall" | "winter"
    mode: str  # "ft8"
    blocks: Dict[str, Dict[str, BandStatus]]


# Seasonal charts based on mid-latitude North America guidance.
# Keys in each block are ham bands (meters).
SUMMER_FT8 = UsabilityTable(
    season="summer",
    mode="ft8",
    blocks={
        "00-04": {"10m": "CLOSED", "12m": "CLOSED", "15m": "CLOSED", "17m": "MARGINAL", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "MARGINAL"},
        "04-08": {"10m": "CLOSED", "12m": "CLOSED", "15m": "MARGINAL", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "CLOSED"},
        "08-10": {"10m": "MARGINAL", "12m": "MARGINAL", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "MARGINAL", "160m": "CLOSED"},
        "10-16": {"10m": "OPEN", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "CLOSED", "160m": "CLOSED"},
        "16-20": {"10m": "MARGINAL", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "MARGINAL", "160m": "CLOSED"},
        "20-24": {"10m": "CLOSED", "12m": "CLOSED", "15m": "MARGINAL", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "MARGINAL"},
    },
)

SPRING_FALL_FT8 = UsabilityTable(
    season="spring_fall",
    mode="ft8",
    blocks={
        "00-04": {"10m": "CLOSED", "12m": "CLOSED", "15m": "MARGINAL", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "OPEN"},
        "04-08": {"10m": "CLOSED", "12m": "MARGINAL", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "MARGINAL"},
        "08-10": {"10m": "MARGINAL", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "MARGINAL", "160m": "CLOSED"},
        "10-16": {"10m": "OPEN", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "CLOSED", "160m": "CLOSED"},
        "16-20": {"10m": "MARGINAL", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "MARGINAL", "160m": "CLOSED"},
        "20-24": {"10m": "CLOSED", "12m": "MARGINAL", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "OPEN"},
    },
)

WINTER_FT8 = UsabilityTable(
    season="winter",
    mode="ft8",
    blocks={
        "00-04": {"10m": "CLOSED", "12m": "CLOSED", "15m": "CLOSED", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "OPEN"},
        "04-08": {"10m": "CLOSED", "12m": "CLOSED", "15m": "MARGINAL", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "OPEN"},
        "08-10": {"10m": "CLOSED", "12m": "MARGINAL", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "MARGINAL", "160m": "MARGINAL"},
        "10-16": {"10m": "MARGINAL", "12m": "OPEN", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "CLOSED", "160m": "CLOSED"},
        "16-20": {"10m": "CLOSED", "12m": "MARGINAL", "15m": "OPEN", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "MARGINAL"},
        "20-24": {"10m": "CLOSED", "12m": "CLOSED", "15m": "MARGINAL", "17m": "OPEN", "20m": "OPEN", "30m": "OPEN", "40m": "OPEN", "60m": "OPEN", "80m": "OPEN", "160m": "OPEN"},
    },
)

TABLES: List[UsabilityTable] = [
    SUMMER_FT8,
    SPRING_FALL_FT8,
    WINTER_FT8,
]


def season_for_date(dt: datetime) -> str:
    """Return season label for a local datetime (northern hemisphere)."""
    m = int(dt.month)
    if m in (12, 1, 2):
        return "winter"
    if m in (3, 4, 9, 10):
        return "spring_fall"
    return "summer"


def block_for_hour(hour: int, *, mode: str) -> str:
    """Map hour to chart block key for a given mode."""
    h = int(hour) % 24
    _ = mode

    # FT8 blocks
    if 0 <= h < 4:
        return "00-04"
    if 4 <= h < 8:
        return "04-08"
    if 8 <= h < 10:
        return "08-10"
    if 10 <= h < 16:
        return "10-16"
    if 16 <= h < 20:
        return "16-20"
    return "20-24"


def get_table(season: str, mode: str) -> UsabilityTable:
    season = season.lower().strip()
    _ = mode.lower().strip()
    for t in TABLES:
        if t.season == season:
            return t
    raise KeyError(f"No table for season={season} mode=ft8")


def expected_status(
    *,
    band: str,
    mode: str,
    local_dt: Optional[datetime] = None,
    latitude: float = 38.6,
    region: str = "NA",
) -> BandStatus:
    """Return expected OPEN/MARGINAL/CLOSED for the given band and time.

    This assumes mid-latitude North America with normal solar conditions.
    """

    if local_dt is None:
        local_dt = datetime.now().astimezone()

    _ = float(latitude)
    _ = str(region)

    season = season_for_date(local_dt)
    block = block_for_hour(local_dt.hour, mode=mode)
    table = get_table(season, mode)
    return table.blocks.get(block, {}).get(str(band), "CLOSED")


def expected_schedule(
    *,
    mode: str,
    local_dt: Optional[datetime] = None,
    latitude: float = 38.6,
    region: str = "NA",
) -> Dict[str, BandStatus]:
    """Return expected band statuses for the current time block."""

    if local_dt is None:
        local_dt = datetime.now().astimezone()

    _ = float(latitude)
    _ = str(region)

    season = season_for_date(local_dt)
    block = block_for_hour(local_dt.hour, mode=mode)
    table = get_table(season, mode)
    return dict(table.blocks.get(block, {}))


def expected_schedule_by_season(*, mode: str) -> Dict[str, Dict[str, Dict[str, BandStatus]]]:
    """Return the entire seasonal table for a mode."""
    _ = mode
    out: Dict[str, Dict[str, Dict[str, BandStatus]]] = {}
    for t in TABLES:
        out[t.season] = dict(t.blocks)
    return out
