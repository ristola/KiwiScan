from __future__ import annotations

from fastapi import APIRouter


def make_router(*, receiver_mgr: object) -> APIRouter:
    """Create router for receiver health summary endpoints."""

    router = APIRouter()

    @router.get("/health/rx")
    def get_receiver_health() -> dict:
        if hasattr(receiver_mgr, "health_summary"):
            return receiver_mgr.health_summary()
        return {
            "overall": "unknown",
            "active_receivers": 0,
            "unstable_receivers": 0,
            "stalled_receivers": 0,
            "silent_receivers": 0,
            "restart_total": 0,
            "channels": {},
        }

    @router.get("/health/rx/truth")
    def get_receiver_truth() -> dict:
        if hasattr(receiver_mgr, "truth_snapshot"):
            return receiver_mgr.truth_snapshot()
        return {
            "overall": "unknown",
            "host": "",
            "port": 0,
            "channels": {},
            "_from_cache": False,
        }

    return router
