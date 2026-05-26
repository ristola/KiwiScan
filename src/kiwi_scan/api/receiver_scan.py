from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from ..targeted_service_registry import resolve_targeted_service


def make_router(
    *,
    mgr: object,
    receiver_scan: object,
    net_monitor: object | None = None,
    caption_monitor: object | None = None,
) -> APIRouter:
    router = APIRouter()

    def _resolved_runtime_target(*, kiwi_key: str | None = None, kiwi_index: object | None = None) -> dict[str, object]:
        resolved_target = None
        resolve_runtime_target = getattr(mgr, "resolve_runtime_target", None)
        if callable(resolve_runtime_target):
            try:
                resolved_target = resolve_runtime_target(kiwi_key=kiwi_key, kiwi_index=kiwi_index)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        with mgr.lock:  # type: ignore[attr-defined]
            if resolved_target is None:
                host = str(mgr.host)  # type: ignore[attr-defined]
                port = int(mgr.port)  # type: ignore[attr-defined]
                password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]
                return {
                    "host": host,
                    "port": port,
                    "password": password,
                    "kiwi_index": None,
                    "kiwi_key": f"{host}:{port}" if host else "",
                }
            return dict(resolved_target)

    def _request_kiwi_selector(request: dict[str, object] | None) -> tuple[str | None, object | None]:
        if not isinstance(request, dict):
            return None, None
        kiwi_key = request.get("kiwi_key")
        if kiwi_key is None:
            kiwi_key = request.get("kiwiKey")
        kiwi_index = request.get("kiwi_index")
        if kiwi_index is None:
            kiwi_index = request.get("kiwiIndex")
        kiwi_key_text = str(kiwi_key).strip() if kiwi_key is not None else ""
        return kiwi_key_text or None, kiwi_index

    def _manager_state(
        *,
        service: object,
        target: dict[str, object],
        scan_band: str | None = None,
    ) -> tuple[str, int, str | None, float]:
        current_band = str(getattr(service, "band", getattr(service, "BAND", "40m")))
        normalize_band = getattr(service, "normalize_band", None)
        if callable(normalize_band):
            resolved_band = normalize_band(scan_band, fallback=current_band) or current_band
        else:
            resolved_band = str(scan_band or current_band)
        with mgr.lock:  # type: ignore[attr-defined]
            threshold_db = float(mgr.threshold_db_by_band.get(resolved_band, mgr.threshold_db))  # type: ignore[attr-defined]
        host = str(target.get("host") or "")
        port = int(target.get("port") or 0)
        password = target.get("password")
        return host, port, password, threshold_db

    @router.post("/receiver_scan/start")
    def start_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_band = None
        requested_mode = None
        if isinstance(request, dict) and request.get("band") is not None:
            requested_band = str(request.get("band"))
        if isinstance(request, dict) and request.get("mode") is not None:
            requested_mode = str(request.get("mode"))
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        scan_service = resolve_targeted_service(receiver_scan, target=target)
        for service in (net_monitor, caption_monitor):
            resolved_service = resolve_targeted_service(service, target=target) if service is not None else None
            if resolved_service is not None and hasattr(resolved_service, "deactivate"):
                try:
                    resolved_service.deactivate()  # type: ignore[attr-defined]
                except Exception:
                    pass
        host, port, password, threshold_db = _manager_state(
            service=scan_service,
            target=target,
            scan_band=requested_band,
        )
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
        return scan_service.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.post("/receiver_scan/prepare")
    def prepare_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_band = None
        requested_mode = None
        if isinstance(request, dict) and request.get("band") is not None:
            requested_band = str(request.get("band"))
        if isinstance(request, dict) and request.get("mode") is not None:
            requested_mode = str(request.get("mode"))
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        scan_service = resolve_targeted_service(receiver_scan, target=target)
        for service in (net_monitor, caption_monitor):
            resolved_service = resolve_targeted_service(service, target=target) if service is not None else None
            if resolved_service is not None and hasattr(resolved_service, "deactivate"):
                try:
                    resolved_service.deactivate()  # type: ignore[attr-defined]
                except Exception:
                    pass
        host, port, _password, _threshold_db = _manager_state(
            service=scan_service,
            target=target,
            scan_band=requested_band,
        )
        prepare_kwargs = {
            "host": host,
            "port": port,
        }
        if requested_band is not None:
            prepare_kwargs["band"] = requested_band
        if requested_mode is not None:
            prepare_kwargs["mode"] = requested_mode
        return scan_service.prepare(**prepare_kwargs)  # type: ignore[attr-defined]

    @router.get("/receiver_scan/status")
    def receiver_scan_status(kiwi_key: str | None = None, kiwi_index: int | None = None):
        target = _resolved_runtime_target(kiwi_key=kiwi_key, kiwi_index=kiwi_index)
        scan_service = resolve_targeted_service(receiver_scan, target=target)
        return scan_service.status()  # type: ignore[attr-defined]

    @router.post("/receiver_scan/stop")
    def stop_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        scan_service = resolve_targeted_service(receiver_scan, target=target)
        return scan_service.stop()  # type: ignore[attr-defined]

    @router.post("/receiver_scan/deactivate")
    def deactivate_receiver_scan(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        scan_service = resolve_targeted_service(receiver_scan, target=target)
        return scan_service.deactivate()  # type: ignore[attr-defined]

    return router