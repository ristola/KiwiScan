from __future__ import annotations

from fastapi import APIRouter


def make_router(*, mgr: object, receiver_scan: object) -> APIRouter:
    router = APIRouter()

    def _manager_state() -> tuple[str, int, str | None, float]:
        scan_band = str(getattr(receiver_scan, "BAND", "20m"))
        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)  # type: ignore[attr-defined]
            port = int(mgr.port)  # type: ignore[attr-defined]
            password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]
            threshold_db = float(mgr.threshold_db_by_band.get(scan_band, mgr.threshold_db))  # type: ignore[attr-defined]
        return host, port, password, threshold_db

    @router.post("/receiver_scan/start")
    def start_receiver_scan():
        host, port, password, threshold_db = _manager_state()
        return receiver_scan.start(  # type: ignore[attr-defined]
            host=host,
            port=port,
            password=password,
            threshold_db=threshold_db,
        )

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