from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from ..voice_mode import resolve_voice_sideband


def make_router(
    *,
    mgr: object,
    caption_monitor: object,
    receiver_scan: object | None = None,
    net_monitor: object | None = None,
    rx_monitor: object | None = None,
) -> APIRouter:
    router = APIRouter()

    @router.post("/caption/start")
    def start_caption_monitor(request: dict[str, object] | None = Body(default=None)):
        payload = request if isinstance(request, dict) else {}

        try:
            freq_khz = float(payload.get("freq_khz"))
        except Exception:
            raise HTTPException(status_code=400, detail="freq_khz required")
        if freq_khz <= 0.0:
            raise HTTPException(status_code=400, detail="freq_khz must be > 0")

        sideband_raw = payload.get("sideband")
        try:
            sideband = resolve_voice_sideband(freq_khz, str(sideband_raw) if sideband_raw is not None else None)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        rx_chan = payload.get("rx_chan", payload.get("rx", 0))
        chunk_duration_s = payload.get("chunk_duration_s", payload.get("duration_s"))
        max_chunks = payload.get("max_chunks")

        try:
            rx_chan = int(rx_chan)
        except Exception:
            rx_chan = 0

        if chunk_duration_s is not None:
            try:
                chunk_duration_s = int(chunk_duration_s)
            except Exception:
                raise HTTPException(status_code=400, detail="chunk_duration_s must be an integer")
            if chunk_duration_s <= 0:
                raise HTTPException(status_code=400, detail="chunk_duration_s must be > 0")

        if max_chunks is not None:
            try:
                max_chunks = int(max_chunks)
            except Exception:
                raise HTTPException(status_code=400, detail="max_chunks must be an integer")
            if max_chunks < 0:
                raise HTTPException(status_code=400, detail="max_chunks must be >= 0")

        for service, method_name in (
            (receiver_scan, "deactivate"),
            (net_monitor, "deactivate"),
            (rx_monitor, "stop"),
        ):
            if service is not None and hasattr(service, method_name):
                try:
                    getattr(service, method_name)()  # type: ignore[misc]
                except Exception:
                    pass

        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)  # type: ignore[attr-defined]
            port = int(mgr.port)  # type: ignore[attr-defined]
            password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]

        start_kwargs = {
            "host": host,
            "port": port,
            "password": password,
            "freq_khz": freq_khz,
            "sideband": sideband,
            "rx_chan": rx_chan,
        }
        if chunk_duration_s is not None:
            start_kwargs["chunk_duration_s"] = chunk_duration_s
        if max_chunks is not None:
            start_kwargs["max_chunks"] = max_chunks
        return caption_monitor.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.get("/caption/status")
    def caption_monitor_status():
        return caption_monitor.status()  # type: ignore[attr-defined]

    @router.post("/caption/stop")
    def stop_caption_monitor():
        return caption_monitor.stop()  # type: ignore[attr-defined]

    @router.post("/caption/deactivate")
    def deactivate_caption_monitor():
        return caption_monitor.deactivate()  # type: ignore[attr-defined]

    return router