"""Maidenhead grid-square ↔ latitude/longitude and haversine distance.

Supports 4-character (field+square) and 6-character (field+square+subsquare)
Maidenhead locators.  All functions are pure Python with no external deps.

Exported API
------------
grid_to_latlon(grid: str) -> tuple[float, float]
    Return the (lat, lon) centre of a Maidenhead locator string.

latlon_to_grid(lat: float, lon: float, precision: int = 6) -> str
    Encode a lat/lon pair as a 4- or 6-character grid square.

haversine_km(lat1, lon1, lat2, lon2) -> float
    Great-circle distance in kilometres.

haversine_mi(lat1, lon1, lat2, lon2) -> float
    Great-circle distance in statute miles.

distance_from_grid(our_lat, our_lon, their_grid) -> tuple[float, float] | None
    Return (dist_km, dist_mi) from our location to the centre of *their_grid*.
    Returns None if *their_grid* is not a valid grid square.
"""

from __future__ import annotations

import math
import re
from typing import Optional

__all__ = [
    "grid_to_latlon",
    "latlon_to_grid",
    "haversine_km",
    "haversine_mi",
    "distance_from_grid",
]

_EARTH_RADIUS_KM = 6371.0
_KM_TO_MI = 0.621371

# Accept 4-char (AA00) or 6-char (AA00AA) locators, case-insensitive.
_GRID_RE = re.compile(r"^[A-R]{2}\d{2}([A-X]{2})?$", re.IGNORECASE)


def _validate(grid: str) -> str:
    """Return upper-cased grid if valid, raise ValueError otherwise."""
    g = (grid or "").strip().upper()
    if not _GRID_RE.match(g):
        raise ValueError(f"Invalid Maidenhead locator: {grid!r}")
    return g


def grid_to_latlon(grid: str) -> tuple[float, float]:
    """Return (latitude, longitude) for the **centre** of *grid*.

    Supports 4-char and 6-char locators.
    """
    g = _validate(grid)

    # Field (chars 0-1): A-R encodes 20° longitude / 10° latitude bands
    lon = (ord(g[0]) - ord("A")) * 20.0 - 180.0
    lat = (ord(g[1]) - ord("A")) * 10.0 - 90.0

    # Square (chars 2-3): digits 0-9 encode 2° lon / 1° lat
    lon += int(g[2]) * 2.0
    lat += int(g[3]) * 1.0

    if len(g) >= 6:
        # Subsquare (chars 4-5): A-X encode 5'/lon, 2.5'/lat
        lon += (ord(g[4]) - ord("A")) * (5.0 / 60.0)
        lat += (ord(g[5]) - ord("A")) * (2.5 / 60.0)
        # Centre of subsquare
        lon += 2.5 / 60.0
        lat += 1.25 / 60.0
    else:
        # Centre of 4-char square
        lon += 1.0
        lat += 0.5

    return (lat, lon)


def latlon_to_grid(lat: float, lon: float, precision: int = 6) -> str:
    """Encode *(lat, lon)* as a Maidenhead locator.

    *precision* must be 4 or 6.
    """
    if precision not in (4, 6):
        raise ValueError("precision must be 4 or 6")

    lon_adj = lon + 180.0
    lat_adj = lat + 90.0

    field_lon = int(lon_adj / 20.0)
    field_lat = int(lat_adj / 10.0)

    sq_lon = int((lon_adj % 20.0) / 2.0)
    sq_lat = int(lat_adj % 10.0)

    result = (
        chr(ord("A") + field_lon)
        + chr(ord("A") + field_lat)
        + str(sq_lon)
        + str(sq_lat)
    )

    if precision == 6:
        sub_lon = int(((lon_adj % 20.0) % 2.0) / (5.0 / 60.0))
        sub_lat = int((lat_adj % 1.0) / (2.5 / 60.0))
        sub_lon = min(sub_lon, 23)
        sub_lat = min(sub_lat, 23)
        result += chr(ord("A") + sub_lon) + chr(ord("A") + sub_lat)

    return result


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres using the haversine formula."""
    r = _EARTH_RADIUS_KM
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles."""
    return haversine_km(lat1, lon1, lat2, lon2) * _KM_TO_MI


def distance_from_grid(
    our_lat: float,
    our_lon: float,
    their_grid: str,
) -> Optional[tuple[float, float]]:
    """Return *(dist_km, dist_mi)* from *(our_lat, our_lon)* to the centre of
    *their_grid*.  Returns ``None`` if *their_grid* is not a valid locator.
    """
    try:
        their_lat, their_lon = grid_to_latlon(their_grid)
    except ValueError:
        return None
    km = haversine_km(our_lat, our_lon, their_lat, their_lon)
    return (round(km, 1), round(km * _KM_TO_MI, 1))
