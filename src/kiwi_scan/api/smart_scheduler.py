"""API router for GET /smart_scheduler/status and band-condition override endpoints."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from ..smart_scheduler import SmartScheduler
from ..targeted_service_registry import resolve_targeted_service


def make_router(*, mgr: object, smart_scheduler: object) -> APIRouter:
    router = APIRouter()

    def _resolved_runtime_target(*, kiwi_key: str | None = None) -> dict[str, object]:
        resolved_target = None
        resolve_runtime_target = getattr(mgr, "resolve_runtime_target", None)
        if callable(resolve_runtime_target):
            try:
                resolved_target = resolve_runtime_target(kiwi_key=kiwi_key)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        with mgr.lock:  # type: ignore[attr-defined]
            if resolved_target is None:
                host = str(mgr.host)  # type: ignore[attr-defined]
                port = int(mgr.port)  # type: ignore[attr-defined]
                return {
                    "host": host,
                    "port": port,
                    "kiwi_index": None,
                    "kiwi_key": f"{host}:{port}" if host else "",
                }
            return dict(resolved_target)

    def _request_kiwi_key(request: Request, body: dict[str, object] | None = None) -> str | None:
        query_key = str(request.query_params.get("kiwi_key") or "").strip()
        if query_key:
            return query_key
        if isinstance(body, dict):
            payload_key = body.get("kiwi_key")
            if payload_key is None:
                payload_key = body.get("kiwiKey")
            payload_text = str(payload_key).strip() if payload_key is not None else ""
            if payload_text:
                return payload_text
        return None

    def _scheduler_for_request(request: Request, body: dict[str, object] | None = None) -> SmartScheduler:
        target = _resolved_runtime_target(kiwi_key=_request_kiwi_key(request, body))
        return resolve_targeted_service(smart_scheduler, target=target)  # type: ignore[return-value]

    @router.get("/smart_scheduler/status")
    async def get_status(request: Request) -> Dict[str, Any]:
        """Return current FT8 band conditions and SmartScheduler health."""
        scheduler = _scheduler_for_request(request)
        return scheduler.get_status()

    @router.get("/smart_scheduler/scan_config")
    async def get_scan_config(request: Request) -> Dict[str, Any]:
        """Return the current band allowlist configuration."""
        scheduler = _scheduler_for_request(request)
        return scheduler.get_scan_config()

    @router.put("/smart_scheduler/scan_config")
    async def put_scan_config(request: Request) -> Dict[str, Any]:
        """Update the band allowlist.

        Body: {"allowed_bands": ["10m", "20m", ...]}
        """
        body = await request.json()
        allowed_bands = body.get("allowed_bands")
        if not isinstance(allowed_bands, list):
            raise HTTPException(status_code=400, detail="'allowed_bands' must be a list")
        scheduler = _scheduler_for_request(request, body)
        scheduler.set_scan_config(allowed_bands)
        return {"ok": True, "allowed_bands": scheduler.get_scan_config()["allowed_bands"]}

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
        scheduler = _scheduler_for_request(request, body)
        scheduler.set_override(band, condition)
        return {"ok": True, "band": band, "condition": condition}

    @router.delete("/smart_scheduler/band_override/{band}")
    async def clear_band_override(band: str, request: Request) -> Dict[str, Any]:
        """Remove a user-pinned condition so the band reverts to empirical / seasonal."""
        band = str(band or "").strip()
        if not band:
            raise HTTPException(status_code=400, detail="'band' path parameter is required")
        scheduler = _scheduler_for_request(request)
        scheduler.clear_override(band)
        return {"ok": True, "band": band}

    @router.post("/smart_scheduler/force_check")
    async def force_check(request: Request) -> Dict[str, Any]:
        """Trigger an immediate condition check outside the normal schedule."""
        scheduler = _scheduler_for_request(request)
        scheduler.force_check()
        return {"ok": True}

    return router
