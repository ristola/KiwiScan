"""Tests for src/kiwi_scan/maidenhead.py"""

import math
import pytest
from kiwi_scan.maidenhead import (
    distance_from_grid,
    grid_to_latlon,
    haversine_km,
    haversine_mi,
    latlon_to_grid,
)

# ---------------------------------------------------------------------------
# grid_to_latlon
# ---------------------------------------------------------------------------


class TestGridToLatlon:
    def test_fn20_four_char(self):
        """FN20 covers eastern Pennsylvania; centre at (40.5, -75.0)."""
        lat, lon = grid_to_latlon("FN20")
        assert abs(lat - 40.5) < 0.01
        assert abs(lon - (-75.0)) < 0.01

    def test_fm18_four_char(self):
        """FM18 covers the DC/Northern-VA area."""
        lat, lon = grid_to_latlon("FM18")
        assert abs(lat - 38.5) < 0.01
        assert abs(lon - (-77.0)) < 0.01

    def test_io91_london(self):
        """IO91 is central England / London area."""
        lat, lon = grid_to_latlon("IO91")
        assert abs(lat - 51.5) < 0.01
        assert abs(lon - (-1.0)) < 0.01

    def test_six_char_more_precise(self):
        """Six-char locator should produce a tighter result than 4-char."""
        lat4, lon4 = grid_to_latlon("FM18")
        lat6, lon6 = grid_to_latlon("FM18lv")
        # Both should be in the FM18 square
        assert 38.0 <= lat6 <= 39.0
        assert -78.0 <= lon6 <= -76.0
        # Six-char centre ≠ four-char centre (subsquare offset)
        assert (lat6, lon6) != (lat4, lon4)

    def test_case_insensitive(self):
        lat1, lon1 = grid_to_latlon("FN20")
        lat2, lon2 = grid_to_latlon("fn20")
        assert lat1 == lat2 and lon1 == lon2

    def test_invalid_grid_raises(self):
        with pytest.raises(ValueError):
            grid_to_latlon("ZZ99")  # Z is outside A-R

    def test_too_short_raises(self):
        with pytest.raises(ValueError):
            grid_to_latlon("FM")

    def test_aa00_corner(self):
        """AA00 is the southernmost-westernmost grid; centre at (-89.5, -179.0)."""
        lat, lon = grid_to_latlon("AA00")
        assert abs(lat - (-89.5)) < 0.01
        assert abs(lon - (-179.0)) < 0.01

    def test_rr99_corner(self):
        """RR99 is the northernmost-easternmost 4-char grid."""
        lat, lon = grid_to_latlon("RR99")
        assert 89.0 <= lat <= 90.0
        assert 179.0 <= lon <= 180.0


# ---------------------------------------------------------------------------
# latlon_to_grid round-trip
# ---------------------------------------------------------------------------


class TestLatlonToGrid:
    def test_roundtrip_4(self):
        lat, lon = grid_to_latlon("FN20")
        assert latlon_to_grid(lat, lon, precision=4) == "FN20"

    def test_roundtrip_6(self):
        lat, lon = grid_to_latlon("FM18lv")
        assert latlon_to_grid(lat, lon, precision=6) == "FM18LV"

    def test_invalid_precision(self):
        with pytest.raises(ValueError):
            latlon_to_grid(38.0, -78.0, precision=5)


# ---------------------------------------------------------------------------
# haversine_km / haversine_mi
# ---------------------------------------------------------------------------


class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine_km(38.0, -78.0, 38.0, -78.0) == pytest.approx(0.0)

    def test_equator_one_degree_lon(self):
        """One degree of longitude on the equator ≈ 111.32 km."""
        d = haversine_km(0.0, 0.0, 0.0, 1.0)
        assert d == pytest.approx(111.32, abs=0.5)

    def test_one_degree_lat(self):
        """One degree of latitude ≈ 111.19 km."""
        d = haversine_km(0.0, 0.0, 1.0, 0.0)
        assert d == pytest.approx(111.19, abs=0.5)

    def test_km_to_mi_conversion(self):
        d_km = haversine_km(38.0, -78.0, 51.5, 0.0)
        d_mi = haversine_mi(38.0, -78.0, 51.5, 0.0)
        assert d_mi == pytest.approx(d_km * 0.621371, rel=1e-5)

    def test_transatlantic_plausible(self):
        """VA to London: ~6 000 km / ~3 700 mi."""
        d = haversine_km(38.594841, -78.431839, 51.5, -0.1)
        assert 5800 < d < 6500


# ---------------------------------------------------------------------------
# distance_from_grid
# ---------------------------------------------------------------------------

# Station location used in tests (Shenandoah Valley, VA)
STATION_LAT = 38.594841
STATION_LON = -78.431839


class TestDistanceFromGrid:
    def test_nearby_grid(self):
        """FM18 (DC/VA area) should be a short distance away."""
        result = distance_from_grid(STATION_LAT, STATION_LON, "FM18")
        assert result is not None
        dist_km, dist_mi = result
        assert 0 < dist_km < 200
        assert dist_mi == pytest.approx(dist_km * 0.621371, abs=1.0)

    def test_london_grid(self):
        """IO91 (London area) should be transatlantic ~5 800–6 500 km away."""
        result = distance_from_grid(STATION_LAT, STATION_LON, "IO91")
        assert result is not None
        dist_km, dist_mi = result
        assert 5800 < dist_km < 6500

    def test_six_char_grid(self):
        result = distance_from_grid(STATION_LAT, STATION_LON, "FM18lv")
        assert result is not None
        dist_km, dist_mi = result
        assert 0 < dist_km < 200

    def test_invalid_grid_returns_none(self):
        assert distance_from_grid(STATION_LAT, STATION_LON, "ZZ99") is None

    def test_empty_grid_returns_none(self):
        assert distance_from_grid(STATION_LAT, STATION_LON, "") is None

    def test_same_grid_as_station(self):
        """Station grid to itself should be a very small distance."""
        from kiwi_scan.maidenhead import latlon_to_grid
        same = latlon_to_grid(STATION_LAT, STATION_LON, precision=4)
        result = distance_from_grid(STATION_LAT, STATION_LON, same)
        assert result is not None
        assert result[0] < 200  # within the same square

    def test_returns_rounded_floats(self):
        result = distance_from_grid(STATION_LAT, STATION_LON, "FN20")
        assert result is not None
        km, mi = result
        # Should be rounded to 1 decimal place
        assert km == round(km, 1)
        assert mi == round(mi, 1)

    def test_japan_grid(self):
        """PM85 (Tokyo area): roughly 11 000–12 000 km from east-coast US."""
        result = distance_from_grid(STATION_LAT, STATION_LON, "PM85")
        assert result is not None
        dist_km, _ = result
        assert 10000 < dist_km < 13000
