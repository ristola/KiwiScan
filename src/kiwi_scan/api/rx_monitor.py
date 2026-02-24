from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request


THIRTY_M_BAND = (10100.0, 10150.0)
SIXTY_M_BAND = (5250.0, 5450.0)


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

        if THIRTY_M_BAND[0] <= freq_khz < THIRTY_M_BAND[1]:
            raise HTTPException(status_code=400, detail="30m has no phone operation")

        sideband = str(payload.get("sideband") or "").strip().upper()
        if sideband not in {"LSB", "USB"}:
            if sideband:
                raise HTTPException(status_code=400, detail="sideband must be LSB or USB")

        if SIXTY_M_BAND[0] <= freq_khz < SIXTY_M_BAND[1]:
            sideband = "USB"
        else:
            expected_sideband = "LSB" if freq_khz < 10000 else "USB"
            sideband = sideband or expected_sideband
            if sideband != expected_sideband:
                sideband = expected_sideband

        monitor.start(freq_khz=freq_khz, sideband=sideband, rx_chan=rx_chan)  # type: ignore[attr-defined]
        return {"ok": True}

    @router.post("/rx_monitor/stop")
    def rx_monitor_stop():
        monitor.stop()  # type: ignore[attr-defined]
        return {"ok": True}

    return router
