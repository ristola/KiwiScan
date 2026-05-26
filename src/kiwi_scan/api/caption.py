from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from ..targeted_service_registry import resolve_targeted_service
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

        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(payload)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        caption_service = resolve_targeted_service(caption_monitor, target=target)

        for service, method_name in (
            (receiver_scan, "deactivate"),
            (net_monitor, "deactivate"),
            (rx_monitor, "stop"),
        ):
            resolved_service = resolve_targeted_service(service, target=target) if service is not None and service is not rx_monitor else service
            if resolved_service is not None and hasattr(resolved_service, method_name):
                try:
                    getattr(resolved_service, method_name)()  # type: ignore[misc]
                except Exception:
                    pass

        host, port, password = _manager_target(target=target)

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
        return caption_service.start(**start_kwargs)  # type: ignore[attr-defined]

    @router.get("/caption/status")
    def caption_monitor_status(kiwi_key: str | None = None, kiwi_index: int | None = None):
        target = _resolved_runtime_target(kiwi_key=kiwi_key, kiwi_index=kiwi_index)
        caption_service = resolve_targeted_service(caption_monitor, target=target)
        return caption_service.status()  # type: ignore[attr-defined]

    @router.post("/caption/stop")
    def stop_caption_monitor(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        caption_service = resolve_targeted_service(caption_monitor, target=target)
        return caption_service.stop()  # type: ignore[attr-defined]

    @router.post("/caption/deactivate")
    def deactivate_caption_monitor(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        caption_service = resolve_targeted_service(caption_monitor, target=target)
        return caption_service.deactivate()  # type: ignore[attr-defined]

    return router