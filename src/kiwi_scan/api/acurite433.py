"""
api/acurite433.py — Proxy for the Acurite 433 MHz sensor feed on sigmon.

rtl_433 runs on sigmon as a systemd service writing newline-delimited JSON to
/var/log/433/Accurite.json.  A thin PHP endpoint on the sigmon web server
(http://10.13.73.185/433_api.php) exposes the last N events as JSON.

GET /api/acurite433/events          — recent detections (default 200)
GET /api/acurite433/status          — is the feed reachable and recent?
"""

from __future__ import annotations

import logging
import os

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/acurite433", tags=["acurite433"])

_SIGMON_API = os.getenv("ACURITE433_URL", "http://10.13.73.185/433_api.php")
_TIMEOUT    = httpx.Timeout(6.0)


async def _fetch(limit: int) -> dict:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.get(_SIGMON_API, params={"limit": limit})
        r.raise_for_status()
        return r.json()
    except httpx.ConnectError:
        raise HTTPException(503, f"Acurite 433 feed unreachable at {_SIGMON_API}")
    except Exception as exc:
        raise HTTPException(502, str(exc))


@router.get("/events")
async def acurite_events(limit: int = Query(200, ge=1, le=1000)):
    """Return the most recent Acurite 433 MHz detections."""
    return JSONResponse(content=await _fetch(limit))


@router.get("/status")
async def acurite_status():
    """Return feed reachability and last-event timestamp."""
    try:
        data = await _fetch(1)
        last = data["events"][0] if data.get("events") else None
        return {"reachable": True, "count": data.get("count", 0), "last_event": last}
    except HTTPException as exc:
        return JSONResponse(
            status_code=200,
            content={"reachable": False, "error": exc.detail, "count": 0, "last_event": None},
        )
