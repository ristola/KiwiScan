from __future__ import annotations

import logging
import inspect
from typing import Dict

from fastapi import APIRouter, HTTPException, Request

from ..kiwi_discovery import discover_kiwis, extract_gps_lat_lon, read_kiwi_status


logger = logging.getLogger(__name__)


def make_router(*, mgr: object, waterholes: Dict[str, float], receiver_mgr: object | None = None) -> APIRouter:
    """Create router for GET/POST /config.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()

    @router.get("/config/discover")
    def discover_kiwi(request: Request, port: int = 8073, timeout_s: float = 0.20, max_hosts: int = 32):
        """Best-effort LAN discovery for KiwiSDR.

        Tries my.kiwisdr.com first, then scans likely private /24 subnets inferred
        from caller and server interface IPs for the given TCP port, returning hosts
        whose HTTP root page looks like a KiwiSDR.

        This is intentionally conservative (small timeouts, bounded results) so it
        can't hang the server.
        """

        try:
            client_ip = (request.client.host if request.client else "") or ""
            return discover_kiwis(client_ip=client_ip, port=port, timeout_s=timeout_s, max_hosts=max_hosts)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/config")
    def get_config():
        kiwi_lat = None
        kiwi_lon = None
        kiwi_grid = None
        kiwi_gps_good = None
        try:
            with mgr.lock:  # type: ignore[attr-defined]
                host = str(mgr.host)
                port = int(mgr.port)
            st = read_kiwi_status(host, port, timeout_s=0.75)
            if st:
                kiwi_lat, kiwi_lon = extract_gps_lat_lon(st)
                kiwi_grid = st.get("grid")
                kiwi_gps_good = st.get("gps_good")
        except Exception:
            pass

        with mgr.lock:  # type: ignore[attr-defined]
            runtime_deps = {}
            try:
                runtime_deps = dict(getattr(mgr, "runtime_dependencies", {}) or {})
            except Exception:
                runtime_deps = {}
            return {
                "dwell_s": mgr.dwell_s,
                "span_hz": mgr.span_hz,
                "threshold_db": mgr.threshold_db,
                "threshold_db_by_band": mgr.threshold_db_by_band,
                "fps": mgr.fps,
                "s_meter_offset_db": mgr.s_meter_offset_db,
                "latitude": mgr.latitude,
                "longitude": mgr.longitude,
                "fast_scan_enabled": mgr.fast_scan_enabled,
                "fast_scan_s_threshold": mgr.fast_scan_s_threshold,
                "fast_scan_min_frames": mgr.fast_scan_min_frames,
                "fast_scan_min_duration_s": mgr.fast_scan_min_duration_s,
                "retune_pause_s": mgr.retune_pause_s,
                "rx_chan": mgr.rx_chan,
                "host": mgr.host,
                "port": mgr.port,
                "kiwi_latitude": kiwi_lat,
                "kiwi_longitude": kiwi_lon,
                "kiwi_grid": kiwi_grid,
                "kiwi_gps_good": kiwi_gps_good,
                "runtime_dependencies": runtime_deps,
            }

    @router.post("/config/runtime-deps/refresh")
    def refresh_runtime_dependencies():
        if receiver_mgr is None:
            raise HTTPException(status_code=503, detail="receiver manager unavailable")
        try:
            report = receiver_mgr.dependency_report()  # type: ignore[attr-defined]
        except Exception as exc:
            logger.exception("Runtime dependency refresh failed")
            raise HTTPException(status_code=500, detail=f"dependency refresh failed: {exc}") from exc

        try:
            mgr.set_runtime_dependencies(report, save=True)  # type: ignore[attr-defined]
        except Exception:
            pass

        return {"ok": True, "runtime_dependencies": report}

    @router.post("/config/runtime-deps/test-path-failures")
    def test_runtime_dependency_path_failures():
        if receiver_mgr is None:
            raise HTTPException(status_code=503, detail="receiver manager unavailable")
        try:
            dep_report = receiver_mgr.dependency_report  # type: ignore[attr-defined]
            kwargs: Dict[str, object] = {}
            try:
                sig = inspect.signature(dep_report)
                params = sig.parameters
                if "test_injected_failures" in params:
                    kwargs["test_injected_failures"] = True
                if "apply_corrections" in params:
                    kwargs["apply_corrections"] = False
            except Exception:
                kwargs = {}

            report = dep_report(**kwargs)
        except Exception as exc:
            logger.exception("Runtime dependency path failure test failed")
            raise HTTPException(status_code=500, detail=f"path failure test failed: {exc}") from exc

        try:
            mgr.set_runtime_dependencies(report, save=True)  # type: ignore[attr-defined]
        except Exception:
            pass

        path_checks = report.get("path_checks") if isinstance(report, dict) else {}
        path_checks = path_checks if isinstance(path_checks, dict) else {}
        corrected = sum(1 for item in path_checks.values() if isinstance(item, dict) and bool(item.get("corrected")))
        unresolved = sum(1 for item in path_checks.values() if isinstance(item, dict) and not bool(item.get("ok")))

        return {
            "ok": True,
            "corrected": int(corrected),
            "unresolved": int(unresolved),
            "runtime_dependencies": report,
        }

    @router.post("/config")
    async def set_config(request: Request):
        data = await request.json()
        # rx_chan is intentionally not user-configurable: let Kiwi choose.
        allowed = {
            "dwell_s",
            "span_hz",
            "threshold_db",
            "threshold_db_by_band",
            "fps",
            "host",
            "port",
            "debug",
            "s_meter_offset_db",
            "latitude",
            "longitude",
            "fast_scan_enabled",
            "fast_scan_s_threshold",
            "fast_scan_min_frames",
            "fast_scan_min_duration_s",
            "retune_pause_s",
        }
        with mgr.lock:  # type: ignore[attr-defined]
            for k, v in data.items():
                if k not in allowed:
                    continue
                try:
                    if k == "dwell_s":
                        val = float(v)
                        if val <= 0 or val > 600:
                            raise ValueError("dwell_s must be > 0 and <= 600 seconds")
                        mgr.dwell_s = val
                    elif k == "span_hz":
                        val = float(v)
                        if val <= 0 or val > 30000:
                            raise ValueError("span_hz must be > 0 and <= 30000 Hz")
                        mgr.span_hz = val
                    elif k == "threshold_db":
                        val = float(v)
                        if val < 0 or val > 60:
                            raise ValueError("threshold_db must be between 0 and 60 dB")
                        mgr.threshold_db = val
                        mgr._save_thresholds()  # type: ignore[attr-defined]
                    elif k == "threshold_db_by_band":
                        if v is None:
                            mgr.threshold_db_by_band = {}
                            mgr._save_thresholds()  # type: ignore[attr-defined]
                        elif not isinstance(v, dict):
                            raise ValueError("threshold_db_by_band must be an object mapping band->dB")
                        else:
                            new_map: Dict[str, float] = dict(mgr.threshold_db_by_band)
                            for bk, bv in v.items():
                                band = str(bk)
                                if band not in waterholes:
                                    raise ValueError(f"unknown band in threshold_db_by_band: {band}")
                                val = float(bv)
                                if val < 0 or val > 60:
                                    raise ValueError(f"threshold_db_by_band[{band}] must be between 0 and 60 dB")
                                new_map[band] = val
                            mgr.threshold_db_by_band = new_map
                            mgr._save_thresholds()  # type: ignore[attr-defined]
                    elif k == "fps":
                        val = float(v)
                        if val <= 0 or val > 10:
                            raise ValueError("fps must be > 0 and <= 10 frames/sec")
                        mgr.fps = val
                    elif k == "port":
                        val = int(v)
                        if val < 1 or val > 65535:
                            raise ValueError("port must be between 1 and 65535")
                        mgr.port = val
                    elif k == "host":
                        mgr.host = str(v)
                    elif k == "debug":
                        mgr.debug = bool(v)
                    elif k == "s_meter_offset_db":
                        val = float(v)
                        if val < -60 or val > 60:
                            raise ValueError("s_meter_offset_db must be between -60 and +60 dB")
                        mgr.s_meter_offset_db = float(val)
                    elif k == "latitude":
                        val = float(v)
                        if val < -90 or val > 90:
                            raise ValueError("latitude must be between -90 and 90")
                        mgr.latitude = float(val)
                    elif k == "longitude":
                        val = float(v)
                        if val < -180 or val > 180:
                            raise ValueError("longitude must be between -180 and 180")
                        mgr.longitude = float(val)
                    elif k == "fast_scan_enabled":
                        mgr.fast_scan_enabled = bool(v)
                    elif k == "fast_scan_s_threshold":
                        val = float(v)
                        if val < 0 or val > 25:
                            raise ValueError("fast_scan_s_threshold must be between 0 and 25")
                        mgr.fast_scan_s_threshold = float(val)
                    elif k == "fast_scan_min_frames":
                        val = int(v)
                        if val < 1 or val > 20:
                            raise ValueError("fast_scan_min_frames must be between 1 and 20")
                        mgr.fast_scan_min_frames = int(val)
                    elif k == "fast_scan_min_duration_s":
                        val = float(v)
                        if val < 0.5 or val > 10:
                            raise ValueError("fast_scan_min_duration_s must be between 0.5 and 10 seconds")
                        mgr.fast_scan_min_duration_s = float(val)
                    elif k == "retune_pause_s":
                        val = float(v)
                        if val < 0 or val > 10:
                            raise ValueError("retune_pause_s must be between 0 and 10 seconds")
                        mgr.retune_pause_s = float(val)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"invalid value for {k}: {e}")

            # Do not force an RX channel; allow the server to choose.
            mgr.rx_chan = None
            mgr._save_config()  # type: ignore[attr-defined]

        return {"ok": True}

    return router
