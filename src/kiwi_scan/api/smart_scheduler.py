"""API router for GET /smart_scheduler/status and band-condition override endpoints."""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request

from ..smart_scheduler import SmartScheduler


def make_router(*, smart_scheduler: SmartScheduler) -> APIRouter:
    router = APIRouter()

    @router.get("/smart_scheduler/status")
    async def get_status(mode: str = "ft8") -> Dict[str, Any]:
        """Return current band conditions and SmartScheduler health.

        Optional ``?mode=ft8`` (default) or ``?mode=phone``.
        """
        return smart_scheduler.get_status(mode=mode)

    @router.get("/smart_scheduler/scan_config")
    async def get_scan_config() -> Dict[str, Any]:
        """Return the current band allowlist configuration."""
        return smart_scheduler.get_scan_config()

    @router.put("/smart_scheduler/scan_config")
    async def put_scan_config(request: Request) -> Dict[str, Any]:
        """Update the band allowlist.

        Body: {"allowed_bands": ["10m", "20m", ...]}
        """
        body = await request.json()
        allowed_bands = body.get("allowed_bands")
        if not isinstance(allowed_bands, list):
            raise HTTPException(status_code=400, detail="'allowed_bands' must be a list")
        smart_scheduler.set_scan_config(allowed_bands)
        return {"ok": True, "allowed_bands": smart_scheduler.get_scan_config()["allowed_bands"]}

    @router.post("/smart_scheduler/band_override")
    async def set_band_override(request: Request) -> Dict[str, Any]:
        """Pin a band to a specific condition.

        Body: {"band": "20m", "condition": "CLOSED"} — "CLOSED" prevents the
        band from receiving a receiver slot until the override is cleared.
        """
        body = await request.json()
        band = str(body.get("band") or "").strip()
        condition = str(body.get("condition") or "").strip().upper()
        if not band:
            raise HTTPException(status_code=400, detail="'band' is required")
        if condition not in {"OPEN", "MARGINAL", "CLOSED"}:
            raise HTTPException(
                status_code=400,
                detail="'condition' must be OPEN, MARGINAL, or CLOSED",
            )
        smart_scheduler.set_override(band, condition)
        return {"ok": True, "band": band, "condition": condition}

    @router.delete("/smart_scheduler/band_override/{band}")
    async def clear_band_override(band: str) -> Dict[str, Any]:
        """Remove a user-pinned condition so the band reverts to empirical / seasonal."""
        band = str(band or "").strip()
        if not band:
            raise HTTPException(status_code=400, detail="'band' path parameter is required")
        smart_scheduler.clear_override(band)
        return {"ok": True, "band": band}

    @router.post("/smart_scheduler/force_check")
    async def force_check() -> Dict[str, Any]:
        """Trigger an immediate condition check outside the normal schedule."""
        smart_scheduler.force_check()
        return {"ok": True}

    return router
