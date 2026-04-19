from __future__ import annotations

from fastapi import APIRouter, Body


def make_router(
    *,
    mgr: object,
    receiver_scan: object,
    net_monitor: object | None = None,
    caption_monitor: object | None = None,
) -> APIRouter:
    router = APIRouter()

    def _manager_state(*, scan_band: str | None = None) -> tuple[str, int, str | None, float]:
        current_band = str(getattr(receiver_scan, "band", getattr(receiver_scan, "BAND", "40m")))
        normalize_band = getattr(receiver_scan, "normalize_band", None)
        if callable(normalize_band):
            resolved_band = normalize_band(scan_band, fallback=current_band) or current_band
        else:
            resolved_band = str(scan_band or current_band)
        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)  # type: ignore[attr-defined]
            port = int(mgr.port)  # type: ignore[attr-defined]
            password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]
            threshold_db = float(mgr.threshold_db_by_band.get(resolved_band, mgr.threshold_db))  # type: ignore[attr-defined]
        return host, port, password, threshold_db

    @router.post("/receiver_scan/start")
    def start_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_band = None
        requested_mode = None
        if isinstance(request, dict) and request.get("band") is not None:
            requested_band = str(request.get("band"))
        if isinstance(request, dict) and request.get("mode") is not None:
            requested_mode = str(request.get("mode"))
        for service in (net_monitor, caption_monitor):
            if service is not None and hasattr(service, "deactivate"):
                try:
                    service.deactivate()  # type: ignore[attr-defined]
                except Exception:
                    pass
        host, port, password, threshold_db = _manager_state(scan_band=requested_band)
        start_kwargs = {
            "host": host,
            "port": port,
            "password": password,
            "threshold_db": threshold_db,
        }
        if requested_band is not None:
            start_kwargs["band"] = requested_band
        if requested_mode is not None:
            start_kwargs["mode"] = requested_mode
        return receiver_scan.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.post("/receiver_scan/prepare")
    def prepare_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_band = None
        requested_mode = None
        if isinstance(request, dict) and request.get("band") is not None:
            requested_band = str(request.get("band"))
        if isinstance(request, dict) and request.get("mode") is not None:
            requested_mode = str(request.get("mode"))
        for service in (net_monitor, caption_monitor):
            if service is not None and hasattr(service, "deactivate"):
                try:
                    service.deactivate()  # type: ignore[attr-defined]
                except Exception:
                    pass
        host, port, _password, _threshold_db = _manager_state(scan_band=requested_band)
        prepare_kwargs = {
            "host": host,
            "port": port,
        }
        if requested_band is not None:
            prepare_kwargs["band"] = requested_band
        if requested_mode is not None:
            prepare_kwargs["mode"] = requested_mode
        return receiver_scan.prepare(**prepare_kwargs)  # type: ignore[attr-defined]

    @router.get("/receiver_scan/status")
    def receiver_scan_status():
        return receiver_scan.status()  # type: ignore[attr-defined]

    @router.post("/receiver_scan/stop")
    def stop_receiver_scan():
        return receiver_scan.stop()  # type: ignore[attr-defined]

    @router.post("/receiver_scan/deactivate")
    def deactivate_receiver_scan():
        return receiver_scan.deactivate()  # type: ignore[attr-defined]

    return router