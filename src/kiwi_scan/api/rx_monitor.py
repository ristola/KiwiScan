from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from ..voice_mode import resolve_voice_sideband


def make_router(*, monitor: object) -> APIRouter:
    router = APIRouter()

    @router.get("/rx_monitor/status")
    def rx_monitor_status():
        return monitor.status()  # type: ignore[attr-defined]

    @router.post("/rx_monitor/start")
    async def rx_monitor_start(request: Request):
        payload = await request.json()
        try:
            freq_khz = float(payload.get("freq_khz"))
        except Exception:
            raise HTTPException(status_code=400, detail="freq_khz required")
        if freq_khz <= 0:
            raise HTTPException(status_code=400, detail="freq_khz must be > 0")
        rx_chan = payload.get("rx_chan", payload.get("rx", 0))
        try:
            rx_chan = int(rx_chan)
        except Exception:
            rx_chan = 0

        try:
            sideband = resolve_voice_sideband(freq_khz, payload.get("sideband"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        monitor.start(freq_khz=freq_khz, sideband=sideband, rx_chan=rx_chan)  # type: ignore[attr-defined]
        return {"ok": True}

    @router.post("/rx_monitor/stop")
    def rx_monitor_stop():
        monitor.stop()  # type: ignore[attr-defined]
        return {"ok": True}

    return router
