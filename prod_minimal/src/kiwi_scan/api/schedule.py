from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from ..scheduler import (
    block_for_hour,
    expected_schedule,
    expected_schedule_by_season,
    get_table,
    season_for_date,
)


def make_router() -> APIRouter:
    """Create router for GET /schedule.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()

    @router.get("/schedule")
    def get_schedule(mode: str = "ft8"):
        requested_mode = str(mode or "ft8").strip().lower()
        if requested_mode != "ft8":
            raise HTTPException(status_code=400, detail="mode must be 'ft8'")
        mode = "ft8"

        local_dt = datetime.now().astimezone()
        season = season_for_date(local_dt)
        block = block_for_hour(local_dt.hour, mode="ft8")
        try:
            table = get_table(season, "ft8")
        except KeyError as e:
            raise HTTPException(status_code=400, detail=str(e))

        return {
            "mode": mode,
            "season": season,
            "block": block,
            "local_time": local_dt.isoformat(),
            "current": expected_schedule(mode="ft8", local_dt=local_dt),
            "table": table.blocks,
            "tables_by_season": expected_schedule_by_season(mode="ft8"),
        }

    return router
