from datetime import datetime

from kiwi_scan.api.auto_set import _sort_other_tasks_by_activity
from kiwi_scan.scheduler import get_table


def test_evening_ft8_pushes_weaker_high_band_to_the_back():
    table = get_table("spring_fall", "ft8")
    tasks = [
        {"band": "15m", "mode": "FT8"},
        {"band": "17m", "mode": "FT8"},
        {"band": "160m", "mode": "FT8"},
    ]

    ranked = _sort_other_tasks_by_activity(
        tasks=tasks,
        band_order=["15m", "17m", "160m"],
        blocks=table.blocks,
        block_key="20-24",
        local_dt=datetime(2026, 3, 23, 23, 30),
    )

    assert [task["band"] for task in ranked] == ["17m", "160m", "15m"]


def test_sort_keeps_dual_mode_pair_order_within_same_band():
    table = get_table("spring_fall", "ft8")
    tasks = [
        {"band": "20m", "mode": "FT4"},
        {"band": "20m", "mode": "FT8"},
        {"band": "15m", "mode": "FT8"},
    ]

    ranked = _sort_other_tasks_by_activity(
        tasks=tasks,
        band_order=["15m", "20m"],
        blocks=table.blocks,
        block_key="20-24",
        local_dt=datetime(2026, 3, 23, 21, 0),
    )

    assert [task["mode"] for task in ranked[:2]] == ["FT4", "FT8"]


def test_wspr_task_stays_ahead_of_regular_band_when_slots_are_tight():
    table = get_table("spring_fall", "ft8")
    tasks = [
        {"band": "20m", "mode": "FT8"},
        {"band": "15m", "mode": "WSPR"},
    ]

    ranked = _sort_other_tasks_by_activity(
        tasks=tasks,
        band_order=["15m", "20m"],
        blocks=table.blocks,
        block_key="20-24",
        local_dt=datetime(2026, 3, 23, 21, 0),
    )

    assert ranked[0]["mode"] == "WSPR"