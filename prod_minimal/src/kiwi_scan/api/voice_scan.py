from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from ..targeted_service_registry import resolve_targeted_service


def make_router(
    *,
    mgr: object,
    voice_scan: object,
    caption_monitor: object | None = None,
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

    @router.post("/voice_scan/start")
    def start_voice_scan(request: dict[str, object] | None = Body(default=None)):
        payload = request if isinstance(request, dict) else {}
        band = str(payload.get("band") or "").strip()
        sideband = str(payload.get("sideband") or "LSB").strip().upper()
        if not band:
            raise HTTPException(status_code=400, detail="band required")
        if sideband not in {"LSB", "USB"}:
            raise HTTPException(status_code=400, detail="sideband must be LSB or USB")

        rx_chan = payload.get("rx_chan", payload.get("rx", 0))
        step_hz = payload.get("step_hz", 1000)
        chunk_duration_s = payload.get("chunk_duration_s", 2)
        stability_wait_s = payload.get("stability_wait_s", 10)
        max_memories = payload.get("max_memories", 5)

        try:
            rx_chan = int(rx_chan)
            step_hz = int(step_hz)
            chunk_duration_s = int(chunk_duration_s)
            stability_wait_s = int(stability_wait_s)
            max_memories = int(max_memories)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid numeric field: {exc}") from exc

        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(payload)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)

        for service, method_name in (
            (receiver_scan, "deactivate"),
            (net_monitor, "deactivate"),
            (caption_monitor, "deactivate"),
            (rx_monitor, "stop"),
        ):
            resolved_service = resolve_targeted_service(service, target=target) if service is not None and service is not rx_monitor else service
            if resolved_service is not None and hasattr(resolved_service, method_name):
                try:
                    getattr(resolved_service, method_name)()  # type: ignore[misc]
                except Exception:
                    pass

        voice_scan_service = resolve_targeted_service(voice_scan, target=target)
        host, port, password = _manager_target(target=target)

        return voice_scan_service.start(  # type: ignore[attr-defined]
            host=host,
            port=port,
            password=password,
            band=band,
            sideband=sideband,
            rx_chan=rx_chan,
            step_hz=step_hz,
            chunk_duration_s=chunk_duration_s,
            stability_wait_s=stability_wait_s,
            max_memories=max_memories,
        )

    @router.get("/voice_scan/status")
    def voice_scan_status(kiwi_key: str | None = None, kiwi_index: int | None = None):
        target = _resolved_runtime_target(kiwi_key=kiwi_key, kiwi_index=kiwi_index)
        voice_scan_service = resolve_targeted_service(voice_scan, target=target)
        return voice_scan_service.status()  # type: ignore[attr-defined]

    @router.post("/voice_scan/stop")
    def stop_voice_scan(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        voice_scan_service = resolve_targeted_service(voice_scan, target=target)
        return voice_scan_service.stop()  # type: ignore[attr-defined]

    @router.post("/voice_scan/clear")
    def clear_voice_scan_memories(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        voice_scan_service = resolve_targeted_service(voice_scan, target=target)
        return voice_scan_service.clear()  # type: ignore[attr-defined]

    @router.post("/voice_scan/deactivate")
    def deactivate_voice_scan(request: dict[str, object] | None = Body(default=None)):
        requested_kiwi_key, requested_kiwi_index = _request_kiwi_selector(request)
        target = _resolved_runtime_target(kiwi_key=requested_kiwi_key, kiwi_index=requested_kiwi_index)
        voice_scan_service = resolve_targeted_service(voice_scan, target=target)
        return voice_scan_service.deactivate()  # type: ignore[attr-defined]

    return router
