from __future__ import annotations

import json
import urllib.request

from fastapi import APIRouter

from ..ws4010_server import restart_ws4010


def make_router() -> APIRouter:
    """Create router for admin endpoints."""

    router = APIRouter()

    @router.post("/admin/ws4010/restart")
    def restart_ws4010_endpoint() -> dict:
        restart_ws4010()
        return {"status": "ok"}

    @router.get("/admin/ws4010/status")
    def ws4010_status_endpoint() -> dict:
        try:
            req = urllib.request.Request("http://127.0.0.1:4010/ws_status")
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8", "ignore"))
            return {
                "ok": True,
                "ws4010_clients": int(data.get("ws4010_clients", 0) or 0),
                "ws4010_total_clients": int(data.get("ws4010_total_clients", 0) or 0),
                "source": "ws4010",
            }
        except Exception:
            return {
                "ok": False,
                "ws4010_clients": 0,
                "ws4010_total_clients": 0,
                "source": "ws4010",
            }

    return router
