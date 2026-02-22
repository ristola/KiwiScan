import pytest

from kiwi_scan.bandplan import bandplan_label


@pytest.mark.parametrize(
    "freq_hz,expected",
    [
        # 160m
        (1_810_000, "CW"),
        (1_840_000, "RTTY"),
        (1_900_000, "Phone"),
        # 80m
        (3_560_000, "CW"),
        (3_585_000, "RTTY"),
        (3_700_000, "Phone"),
        # 60m (channelized; we only check a couple representative points)
        (5_331_500, "RTTY"),
        (5_403_500, "RTTY"),
        # 40m
        (7_010_000, "All modes"),
        (7_200_000, "Phone"),
        # 30m (no phone)
        (10_140_000, "RTTY"),
        # 20m
        (14_020_000, "CW"),
        (14_050_000, "RTTY"),
        (14_200_000, "Phone"),
        # 17m
        (18_080_000, "CW"),
        (18_120_000, "Phone"),
        # 15m
        (21_050_000, "RTTY"),
        (21_300_000, "Phone"),
        # 12m
        (24_940_000, "RTTY"),
        (24_960_000, "Phone"),
        # 10m
        (28_050_000, "CW"),
        (28_400_000, "RTTY"),
        (28_600_000, "Phone"),
    ],
)
def test_bandplan_label_extra_segments(freq_hz: float, expected: str) -> None:
    assert bandplan_label(freq_hz) == expected
