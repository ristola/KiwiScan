from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from ..targeted_service_registry import resolve_targeted_service


def make_router(
    *,
    mgr: object,
    net_monitor: object,
    receiver_scan: object | None = None,
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

    def _manager_target(*, target: dict[str, object]) -> tuple[str, int, str | None]:
        with mgr.lock:  # type: ignore[attr-defined]
            host = str(target.get("host") or "")
            port = int(target.get("port") or 0)
            password = target.get("password")
        return host, port, password

    @router.post("/net_monitor/start")
    def start_net_monitor(request: dict[str, object] | None = Body(default=None)):
        profile_name = "20m-net"
        threshold_db = None
        cycle_sleep_s = None
        max_cycles = None
        requested_kiwi_key = None
        requested_kiwi_index = None
        if isinstance(request, dict):
            if request.get("profile") is not None:
                profile_name = str(request.get("profile"))
            if request.get("threshold_db") is not None:
                threshold_db = float(request.get("threshold_db"))
            if request.get("cycle_sleep_s") is not None:
                cycle_sleep_s = float(request.get("cycle_sleep_s"))
            if request.get("max_cycles") is not None:
                max_cycles = int(request.get("max_cycles"))
            requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)

        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        monitor_service = resolve_targeted_service(net_monitor, target=target)

        for service in (receiver_scan, caption_monitor):
            resolved_service = resolve_targeted_service(service, target=target) if service is not None else None
            if resolved_service is not None and hasattr(resolved_service, "deactivate"):
                try:
                    resolved_service.deactivate()  # type: ignore[attr-defined]
                except Exception:
                    pass

        host, port, password = _manager_target(target=target)

        start_kwargs = {
            "host": host,
            "port": port,
            "password": password,
            "profile_name": profile_name,
        }
        if threshold_db is not None:
            start_kwargs["threshold_db"] = threshold_db
        if cycle_sleep_s is not None:
            start_kwargs["cycle_sleep_s"] = cycle_sleep_s
        if max_cycles is not None:
            start_kwargs["max_cycles"] = max_cycles
        return monitor_service.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.post("/net_monitor/capture")
    def capture_net_monitor(request: dict[str, object] | None = Body(default=None)):
        duration_s = None
        freq_mhz = None
        requested_kiwi_key = None
        requested_kiwi_index = None
        if isinstance(request, dict):
            if request.get("duration_s") is not None:
                duration_s = int(request.get("duration_s"))
            if request.get("freq_mhz") is not None:
                freq_mhz = float(request.get("freq_mhz"))
            requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)

        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        monitor_service = resolve_targeted_service(net_monitor, target=target)

        host, port, password = _manager_target(target=target)

        capture_kwargs = {
            "host": host,
            "port": port,
            "password": password,
        }
        if duration_s is not None:
            capture_kwargs["duration_s"] = duration_s
        if freq_mhz is not None:
            capture_kwargs["freq_mhz"] = freq_mhz
        return monitor_service.capture(**capture_kwargs)  # type: ignore[attr-defined]

    @router.get("/net_monitor/status")
    def net_monitor_status(kiwi_key: str | None = None, kiwi_index: int | None = None):
        target = _resolved_runtime_target(kiwi_key=kiwi_key, kiwi_index=kiwi_index)
        monitor_service = resolve_targeted_service(net_monitor, target=target)
        return monitor_service.status()  # type: ignore[attr-defined]

    @router.post("/net_monitor/stop")
    def stop_net_monitor(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        monitor_service = resolve_targeted_service(net_monitor, target=target)
        return monitor_service.stop()  # type: ignore[attr-defined]

    @router.post("/net_monitor/deactivate")
    def deactivate_net_monitor(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        monitor_service = resolve_targeted_service(net_monitor, target=target)
        return monitor_service.deactivate()  # type: ignore[attr-defined]

    return router