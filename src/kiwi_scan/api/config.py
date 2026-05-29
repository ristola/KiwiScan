from __future__ import annotations

import logging
import inspect
from typing import Dict

from fastapi import APIRouter, HTTPException, Request

from ..kiwi_discovery import discover_kiwis, extract_gps_lat_lon, is_unconfigured_kiwi_host, normalize_kiwi_host, read_kiwi_status


logger = logging.getLogger(__name__)


def _discovery_ports_for_request(mgr: object, requested_port: int) -> list[int]:
    ports: list[int] = []
    for candidate in (8073, requested_port, getattr(mgr, "port", requested_port)):
        try:
            value = int(candidate)
        except Exception:
            continue
        if value < 1 or value > 65535 or value in ports:
            continue
        ports.append(value)
    return ports or [8073]


def _get_cached_discovery(mgr: object) -> dict:
    if hasattr(mgr, "get_discovered_kiwis"):
        try:
            data = mgr.get_discovered_kiwis()  # type: ignore[attr-defined]
            if isinstance(data, dict):
                found = data.get("found")
                return {
                    "found": list(found) if isinstance(found, list) else [],
                    "source": str(data.get("source") or "").strip(),
                    "updated_unix": data.get("updated_unix"),
                }
        except Exception:
            logger.debug("Failed reading cached Kiwi discovery results", exc_info=True)
    found = getattr(mgr, "discovered_kiwis", [])
    return {
        "found": list(found) if isinstance(found, list) else [],
        "source": str(getattr(mgr, "discovery_source", "") or "").strip(),
        "updated_unix": getattr(mgr, "discovery_updated_unix", None),
    }


def _get_configured_kiwis(mgr: object) -> list[dict]:
    if hasattr(mgr, "get_configured_kiwis"):
        try:
            data = mgr.get_configured_kiwis()  # type: ignore[attr-defined]
            if isinstance(data, list):
                return [dict(item) for item in data if isinstance(item, dict)]
        except Exception:
            logger.debug("Failed reading configured Kiwi entries", exc_info=True)
    data = getattr(mgr, "configured_kiwis", [])
    return [dict(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []


def _get_active_kiwi_index(mgr: object) -> int:
    if hasattr(mgr, "get_active_kiwi_index"):
        try:
            return max(0, int(mgr.get_active_kiwi_index()))  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Failed reading active Kiwi index", exc_info=True)
    try:
        return max(0, int(getattr(mgr, "active_kiwi_index", 0) or 0))
    except Exception:
        return 0


def _has_kiwi_admin_password(mgr: object) -> bool:
    if hasattr(mgr, "has_kiwi_admin_password"):
        try:
            return bool(mgr.has_kiwi_admin_password())  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Failed reading Kiwi admin password state", exc_info=True)
    return bool(str(getattr(mgr, "admin_password", "") or "").strip())


def _has_kiwi_password(mgr: object) -> bool:
    if hasattr(mgr, "has_kiwi_password"):
        try:
            return bool(mgr.has_kiwi_password())  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Failed reading Kiwi password state", exc_info=True)
    return bool(str(getattr(mgr, "password", "") or "").strip())


def _configured_kiwi_keys(entries: object) -> set[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    if not isinstance(entries, list):
        return keys
    for item in entries:
        if not isinstance(item, dict):
            continue
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        keys.add((host, port))
    return keys


def _normalize_configured_kiwi_entry(entry: object) -> dict[str, object] | None:
    if not isinstance(entry, dict):
        return None
    host = normalize_kiwi_host(entry.get("host"))
    try:
        port = int(entry.get("port") or 0)
    except Exception:
        port = 0
    if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
        return None
    normalized: dict[str, object] = {
        "host": host,
        "port": port,
    }
    for key in ("grid", "name", "loc", "sdr_hw", "sw_version"):
        value = str(entry.get(key) or "").strip()
        if value:
            normalized[key] = value
    for key, lower, upper in (("latitude", -90.0, 90.0), ("longitude", -180.0, 180.0)):
        try:
            value = float(entry.get(key))
        except Exception:
            continue
        if lower <= value <= upper:
            normalized[key] = value
    return normalized


def _prioritize_my_kiwisdr_primary_slots(
    ordered_entries: list[dict[str, object]],
    discovered: object,
) -> list[dict[str, object]]:
    if not ordered_entries or not isinstance(discovered, list):
        return ordered_entries

    entry_by_key = {
        (str(entry["host"]), int(entry["port"])): entry
        for entry in ordered_entries
    }

    my_candidates: list[tuple[int, tuple[str, int]]] = []
    seen: set[tuple[str, int]] = set()
    for item in discovered:
        if not isinstance(item, dict):
            continue
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        source = str(item.get("known_source") or "").strip().lower()
        if "my.kiwisdr.com" not in source:
            continue
        try:
            known_order = int(item.get("known_order"))
        except Exception:
            continue
        key = (host, port)
        if key in seen:
            continue
        seen.add(key)
        my_candidates.append((known_order, key))

    if not my_candidates:
        return ordered_entries

    my_candidates.sort(key=lambda item: item[0])
    primary_keys: list[tuple[str, int]] = []
    for _, key in my_candidates:
        if key in entry_by_key and key not in primary_keys:
            primary_keys.append(key)
        if len(primary_keys) >= 2:
            break

    if not primary_keys:
        return ordered_entries

    prioritized = [dict(entry_by_key[key]) for key in primary_keys]
    for entry in ordered_entries:
        key = (str(entry["host"]), int(entry["port"]))
        if key in primary_keys:
            continue
        prioritized.append(dict(entry))
    return prioritized


def _preserve_configured_kiwi_order(
    requested: object,
    existing: object,
    *,
    discovered: object = None,
) -> list[dict[str, object]] | object:
    if not isinstance(requested, list):
        return requested
    requested_entries = [
        normalized
        for normalized in (_normalize_configured_kiwi_entry(item) for item in requested)
        if normalized is not None
    ]
    if not isinstance(existing, list) or not existing:
        return _prioritize_my_kiwisdr_primary_slots(requested_entries, discovered)
    existing_entries = [
        normalized
        for normalized in (_normalize_configured_kiwi_entry(item) for item in existing)
        if normalized is not None
    ]
    if len(requested_entries) != len(existing_entries):
        return requested_entries
    requested_by_key = {
        (str(entry["host"]), int(entry["port"])): entry
        for entry in requested_entries
    }
    existing_keys = [
        (str(entry["host"]), int(entry["port"]))
        for entry in existing_entries
    ]
    if len(requested_by_key) != len(requested_entries) or set(requested_by_key) != set(existing_keys):
        return _prioritize_my_kiwisdr_primary_slots(requested_entries, discovered)
    preserved = [dict(requested_by_key[key]) for key in existing_keys]
    return _prioritize_my_kiwisdr_primary_slots(preserved, discovered)


def _filter_discovered_kiwis_to_configured(discovered: object, configured: object) -> list[dict]:
    configured_keys = _configured_kiwi_keys(configured)
    if not configured_keys or not isinstance(discovered, list):
        return []
    filtered: list[dict] = []
    for item in discovered:
        if not isinstance(item, dict):
            continue
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if (host, port) not in configured_keys:
            continue
        filtered.append(dict(item))
    return filtered


def _augment_discovered_kiwis_with_status(discovered: object, configured: object, *, timeout_s: float = 0.75) -> list[dict]:
    configured_items = [dict(item) for item in configured if isinstance(item, dict)] if isinstance(configured, list) else []
    out = [dict(item) for item in discovered if isinstance(item, dict)] if isinstance(discovered, list) else []
    by_key: dict[tuple[str, int], int] = {}
    for index, item in enumerate(out):
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        by_key[(host, port)] = index

    for index, item in enumerate(list(out)):
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        merged = dict(item)
        status = read_kiwi_status(host, port, timeout_s=timeout_s)
        if not status:
            continue
        latitude, longitude = extract_gps_lat_lon(status)
        if latitude is not None:
            merged["latitude"] = latitude
        if longitude is not None:
            merged["longitude"] = longitude
        for key in ("grid", "gps_good", "name", "sdr_hw", "sw_version", "loc"):
            value = str(status.get(key) or "").strip()
            if value:
                merged[key] = value
        out[index] = merged

    for item in configured_items:
        host = normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        existing_index = by_key.get((host, port))
        merged = dict(out[existing_index]) if existing_index is not None else {}
        for key, value in item.items():
            if key in {"host", "port"}:
                continue
            merged.setdefault(str(key), value)
        merged["host"] = host
        merged["port"] = port
        known_source = str(merged.get("known_source") or "").strip()
        if existing_index is None and not known_source:
            merged["known_source"] = "configured"
        if existing_index is None:
            status = read_kiwi_status(host, port, timeout_s=timeout_s)
            if status:
                latitude, longitude = extract_gps_lat_lon(status)
                if latitude is not None:
                    merged["latitude"] = latitude
                if longitude is not None:
                    merged["longitude"] = longitude
                for key in ("grid", "gps_good", "name", "sdr_hw", "sw_version", "loc"):
                    value = str(status.get(key) or "").strip()
                    if value:
                        merged[key] = value
        if existing_index is not None:
            out[existing_index] = merged
        elif len(merged) > 2:
            by_key[(host, port)] = len(out)
            out.append(merged)
    return out


def make_router(
    *,
    mgr: object,
    waterholes: Dict[str, float],
    receiver_mgr: object | None = None,
    auto_set_loop: object | None = None,
) -> APIRouter:
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
            result = discover_kiwis(
                client_ip=client_ip,
                port=port,
                ports=_discovery_ports_for_request(mgr, port),
                timeout_s=timeout_s,
                max_hosts=max_hosts,
            )
            if hasattr(mgr, "set_discovered_kiwis"):
                try:
                    mgr.set_discovered_kiwis(result, save=True)  # type: ignore[attr-defined]
                except Exception:
                    logger.debug("Failed caching Kiwi discovery results", exc_info=True)
            return result
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/config/verify")
    def verify_kiwi(host: str, port: int = 8073, timeout_s: float = 1.2):
        host_text = str(host or "").strip()
        if not host_text:
            raise HTTPException(status_code=400, detail="host is required")
        if port < 1 or port > 65535:
            raise HTTPException(status_code=400, detail="port must be 1..65535")
        if timeout_s <= 0 or timeout_s > 5:
            raise HTTPException(status_code=400, detail="timeout_s must be > 0 and <= 5")

        status = read_kiwi_status(host_text, port, timeout_s=timeout_s)
        latitude = None
        longitude = None
        grid = None
        gps_good = None
        name = None
        sdr_hw = None
        sw_version = None
        if status:
            latitude, longitude = extract_gps_lat_lon(status)
            grid = status.get("grid")
            gps_good = status.get("gps_good")
            name = status.get("name")
            sdr_hw = status.get("sdr_hw")
            sw_version = status.get("sw_version")

        return {
            "ok": True,
            "reachable": bool(status),
            "host": host_text,
            "port": port,
            "latitude": latitude,
            "longitude": longitude,
            "grid": grid,
            "gps_good": gps_good,
            "name": name,
            "sdr_hw": sdr_hw,
            "sw_version": sw_version,
        }

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

        cached_discovery = _get_cached_discovery(mgr)
        configured_kiwis = _get_configured_kiwis(mgr)
        enriched_discovery = _augment_discovered_kiwis_with_status(
            cached_discovery.get("found"),
            configured_kiwis,
        )
        discovery_source = str(cached_discovery.get("source") or "").strip()
        if enriched_discovery and not discovery_source:
            discovery_source = "status"
        active_kiwi_index = _get_active_kiwi_index(mgr)
        kiwi_password_set = _has_kiwi_password(mgr)
        kiwi_admin_password_set = _has_kiwi_admin_password(mgr)
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
                "kiwisdrs": configured_kiwis,
                "active_kiwi_index": active_kiwi_index,
                "kiwi_password_set": kiwi_password_set,
                "kiwi_admin_password_set": kiwi_admin_password_set,
                "kiwi_latitude": kiwi_lat,
                "kiwi_longitude": kiwi_lon,
                "kiwi_grid": kiwi_grid,
                "kiwi_gps_good": kiwi_gps_good,
                "discovered_kiwis": enriched_discovery,
                "discovery_source": discovery_source,
                "discovery_updated_unix": cached_discovery.get("updated_unix"),
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
        prior_host = ""
        prior_port = 0
        prior_active_kiwi_index = _get_active_kiwi_index(mgr)
        prior_configured_keys = _configured_kiwi_keys(_get_configured_kiwis(mgr))
        try:
            with mgr.lock:  # type: ignore[attr-defined]
                prior_host = normalize_kiwi_host(getattr(mgr, "host", ""))
                prior_port = int(getattr(mgr, "port", 0) or 0)
        except Exception:
            prior_host = normalize_kiwi_host(getattr(mgr, "host", ""))
            try:
                prior_port = int(getattr(mgr, "port", 0) or 0)
            except Exception:
                prior_port = 0
        configured_kiwis = data.get("kiwisdrs") if isinstance(data, dict) else None
        active_kiwi_index = data.get("active_kiwi_index") if isinstance(data, dict) else None
        sync_discovered_to_configured = bool(data.get("sync_discovered_to_configured") is True) if isinstance(data, dict) else False
        kiwi_password_changed = False
        kiwi_admin_password_changed = False
        existing_configured = None
        if "kiwisdrs" in data and hasattr(mgr, "set_configured_kiwis"):
            existing_configured = _get_configured_kiwis(mgr)
            configured_kiwis = _preserve_configured_kiwi_order(
                configured_kiwis,
                existing_configured,
                discovered=_get_cached_discovery(mgr).get("found"),
            )
            try:
                mgr.set_configured_kiwis(configured_kiwis, save=False)  # type: ignore[attr-defined]
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for kiwisdrs: {exc}") from exc
        if "active_kiwi_index" in data and hasattr(mgr, "set_active_kiwi_index"):
            try:
                mgr.set_active_kiwi_index(active_kiwi_index, save=False)  # type: ignore[attr-defined]
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for active_kiwi_index: {exc}") from exc
        if "kiwi_password" in data and hasattr(mgr, "set_kiwi_password"):
            try:
                mgr.set_kiwi_password(data.get("kiwi_password"), save=False)  # type: ignore[attr-defined]
                kiwi_password_changed = True
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for kiwi_password: {exc}") from exc
        elif bool(data.get("clear_kiwi_password")) and hasattr(mgr, "set_kiwi_password"):
            try:
                mgr.set_kiwi_password("", save=False)  # type: ignore[attr-defined]
                kiwi_password_changed = True
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for clear_kiwi_password: {exc}") from exc
        if "kiwi_admin_password" in data and hasattr(mgr, "set_kiwi_admin_password"):
            try:
                mgr.set_kiwi_admin_password(data.get("kiwi_admin_password"), save=False)  # type: ignore[attr-defined]
                kiwi_admin_password_changed = True
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for kiwi_admin_password: {exc}") from exc
        elif bool(data.get("clear_kiwi_admin_password")) and hasattr(mgr, "set_kiwi_admin_password"):
            try:
                mgr.set_kiwi_admin_password("", save=False)  # type: ignore[attr-defined]
                kiwi_admin_password_changed = True
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for clear_kiwi_admin_password: {exc}") from exc
        if ("discovered_kiwis" in data or "discovery_source" in data) and hasattr(mgr, "set_discovered_kiwis"):
            try:
                discovered_payload = data.get("discovered_kiwis")
                if sync_discovered_to_configured and "kiwisdrs" in data:
                    discovered_payload = _filter_discovered_kiwis_to_configured(
                        data.get("discovered_kiwis"),
                        configured_kiwis,
                    )
                mgr.set_discovered_kiwis(  # type: ignore[attr-defined]
                    {
                        "found": discovered_payload,
                        "source": data.get("discovery_source"),
                    },
                    save=False,
                )
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for discovered_kiwis: {exc}") from exc
        if "kiwisdrs" not in data and hasattr(mgr, "set_configured_kiwis"):
            existing_configured = _get_configured_kiwis(mgr)
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
        merged_configured = None
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
                        mgr.host = normalize_kiwi_host(v)
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
            if "kiwisdrs" not in data and hasattr(mgr, "set_configured_kiwis"):
                primary: dict[str, object] = {
                    "host": normalize_kiwi_host(mgr.host),
                    "port": int(mgr.port),
                    "latitude": float(mgr.latitude),
                    "longitude": float(mgr.longitude),
                }
                if existing_configured:
                    merge_index = 0
                    grid = str(existing_configured[merge_index].get("grid") or "").strip()
                    if grid:
                        primary["grid"] = grid
                    merged_configured = list(existing_configured)
                    if is_unconfigured_kiwi_host(primary["host"]):
                        del merged_configured[merge_index]
                    else:
                        merged_configured[merge_index] = primary
                else:
                    merged_configured = [] if is_unconfigured_kiwi_host(primary["host"]) else [primary]
        if merged_configured is not None and hasattr(mgr, "set_configured_kiwis"):
            mgr.set_configured_kiwis(merged_configured, save=False)  # type: ignore[attr-defined]
        configured_snapshot = _get_configured_kiwis(mgr)
        if configured_snapshot:
            primary_entry = configured_snapshot[0]
            if isinstance(primary_entry, dict):
                active_host = normalize_kiwi_host(primary_entry.get("host"))
                try:
                    active_port = int(primary_entry.get("port") or 0)
                except Exception:
                    active_port = 0
                if not is_unconfigured_kiwi_host(active_host) and 1 <= active_port <= 65535:
                    with mgr.lock:  # type: ignore[attr-defined]
                        mgr.host = active_host
                        mgr.port = active_port
                        if primary_entry.get("latitude") is not None:
                            mgr.latitude = float(primary_entry.get("latitude"))
                        if primary_entry.get("longitude") is not None:
                            mgr.longitude = float(primary_entry.get("longitude"))
        cached_discovery = _get_cached_discovery(mgr)
        enriched_discovery = _augment_discovered_kiwis_with_status(
            cached_discovery.get("found"),
            configured_snapshot,
        )
        discovery_source = str(cached_discovery.get("source") or "").strip()
        if enriched_discovery and not discovery_source:
            discovery_source = "status"
        if hasattr(mgr, "set_discovered_kiwis"):
            mgr.set_discovered_kiwis(  # type: ignore[attr-defined]
                {
                    "found": enriched_discovery,
                    "source": discovery_source,
                },
                save=False,
            )
        mgr._save_config()  # type: ignore[attr-defined]
        if (kiwi_password_changed or kiwi_admin_password_changed) and hasattr(mgr, "_save_secrets"):
            try:
                mgr._save_secrets()  # type: ignore[attr-defined]
            except Exception:
                logger.debug("Failed saving Kiwi secret", exc_info=True)

        try:
            current_host = normalize_kiwi_host(getattr(mgr, "host", ""))
            current_port = int(getattr(mgr, "port", 0) or 0)
        except Exception:
            current_host = prior_host
            current_port = prior_port
        current_active_kiwi_index = _get_active_kiwi_index(mgr)
        current_configured_keys = _configured_kiwi_keys(configured_snapshot)
        endpoint_changed = (current_host != prior_host) or (current_port != prior_port)
        reapply_required = (
            endpoint_changed
            or current_configured_keys != prior_configured_keys
        )
        if reapply_required and auto_set_loop is not None:
            applied = False
            if hasattr(auto_set_loop, "apply_current_settings"):
                try:
                    applied = bool(auto_set_loop.apply_current_settings(force=True, sync_state=True))  # type: ignore[attr-defined]
                except Exception:
                    logger.warning(
                        "Config update changed Kiwi endpoint to %s:%s but immediate receiver reapply failed",
                        current_host,
                        current_port,
                        exc_info=True,
                    )
            if not applied and hasattr(auto_set_loop, "notify_settings_changed"):
                try:
                    auto_set_loop.notify_settings_changed()  # type: ignore[attr-defined]
                except Exception:
                    logger.debug("Failed waking auto-set loop after Kiwi endpoint change", exc_info=True)

        cached_discovery = _get_cached_discovery(mgr)
        return {
            "ok": True,
            "discovered_kiwis": cached_discovery.get("found", []),
            "discovery_source": cached_discovery.get("source", ""),
        }

    return router
