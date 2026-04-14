from __future__ import annotations

from fastapi import APIRouter, Body


def make_router(*, mgr: object, net_monitor: object, receiver_scan: object | None = None) -> APIRouter:
    router = APIRouter()

    @router.post("/net_monitor/start")
    def start_net_monitor(request: dict[str, object] | None = Body(default=None)):
        profile_name = "20m-net"
        threshold_db = None
        cycle_sleep_s = None
        max_cycles = None
        if isinstance(request, dict):
            if request.get("profile") is not None:
                profile_name = str(request.get("profile"))
            if request.get("threshold_db") is not None:
                threshold_db = float(request.get("threshold_db"))
            if request.get("cycle_sleep_s") is not None:
                cycle_sleep_s = float(request.get("cycle_sleep_s"))
            if request.get("max_cycles") is not None:
                max_cycles = int(request.get("max_cycles"))

        if receiver_scan is not None and hasattr(receiver_scan, "deactivate"):
            try:
                receiver_scan.deactivate()  # type: ignore[attr-defined]
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
            "profile_name": profile_name,
        }
        if threshold_db is not None:
            start_kwargs["threshold_db"] = threshold_db
        if cycle_sleep_s is not None:
            start_kwargs["cycle_sleep_s"] = cycle_sleep_s
        if max_cycles is not None:
            start_kwargs["max_cycles"] = max_cycles
        return net_monitor.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.post("/net_monitor/capture")
    def capture_net_monitor(request: dict[str, object] | None = Body(default=None)):
        duration_s = None
        freq_mhz = None
        if isinstance(request, dict):
            if request.get("duration_s") is not None:
                duration_s = int(request.get("duration_s"))
            if request.get("freq_mhz") is not None:
                freq_mhz = float(request.get("freq_mhz"))

        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)  # type: ignore[attr-defined]
            port = int(mgr.port)  # type: ignore[attr-defined]
            password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]

        capture_kwargs = {
            "host": host,
            "port": port,
            "password": password,
        }
        if duration_s is not None:
            capture_kwargs["duration_s"] = duration_s
        if freq_mhz is not None:
            capture_kwargs["freq_mhz"] = freq_mhz
        return net_monitor.capture(**capture_kwargs)  # type: ignore[attr-defined]

    @router.get("/net_monitor/status")
    def net_monitor_status():
        return net_monitor.status()  # type: ignore[attr-defined]

    @router.post("/net_monitor/stop")
    def stop_net_monitor():
        return net_monitor.stop()  # type: ignore[attr-defined]

    @router.post("/net_monitor/deactivate")
    def deactivate_net_monitor():
        return net_monitor.deactivate()  # type: ignore[attr-defined]

    return router