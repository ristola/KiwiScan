from __future__ import annotations

from fastapi import APIRouter

from ..ws4010_server import restart_ws4010


def make_router() -> APIRouter:
    """Create router for admin endpoints."""

    router = APIRouter()

    @router.post("/admin/ws4010/restart")
    def restart_ws4010_endpoint() -> dict:
        restart_ws4010()
        return {"status": "ok"}

    return router
