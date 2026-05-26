from __future__ import annotations

import concurrent.futures
from html import unescape
import logging
import re
import socket
import time
from urllib.error import URLError
from urllib.request import urlopen
from typing import Dict

from fastapi import APIRouter, HTTPException, Request


logger = logging.getLogger(__name__)


DEFAULT_KIWI_HOST = "0.0.0.0"
LEGACY_DEFAULT_KIWI_HOST = "192.168.1.93"
PLACEHOLDER_KIWI_HOSTS = frozenset({"1.2.3.4"})


def _normalize_kiwi_host(host: object) -> str:
    value = str(host or "").strip()
    if value.lower() in {
        "",
        DEFAULT_KIWI_HOST,
        "127.0.0.1",
        "localhost",
        *PLACEHOLDER_KIWI_HOSTS,
    }:
        return DEFAULT_KIWI_HOST
    return value


def _is_unconfigured_kiwi_host(host: object) -> bool:
    return _normalize_kiwi_host(host) == DEFAULT_KIWI_HOST


def _configured_kiwi_keys(entries: object) -> set[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    if not isinstance(entries, list):
        return keys
    for item in entries:
        if not isinstance(item, dict):
            continue
        host = _normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if _is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        keys.add((host, port))
    return keys


def _normalize_configured_kiwi_entry(entry: object) -> dict[str, object] | None:
    if not isinstance(entry, dict):
        return None
    host = _normalize_kiwi_host(entry.get("host"))
    try:
        port = int(entry.get("port") or 0)
    except Exception:
        port = 0
    if _is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
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


def _preserve_configured_kiwi_order(requested: object, existing: object) -> list[dict[str, object]] | object:
    if not isinstance(requested, list):
        return requested
    requested_entries = [
        normalized
        for normalized in (_normalize_configured_kiwi_entry(item) for item in requested)
        if normalized is not None
    ]
    if not isinstance(existing, list) or not existing:
        return requested_entries
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
        return requested_entries
    return [dict(requested_by_key[key]) for key in existing_keys]


def _filter_discovered_kiwis_to_configured(discovered: object, configured: object) -> list[dict[str, object]]:
    configured_keys = _configured_kiwi_keys(configured)
    if not configured_keys or not isinstance(discovered, list):
        return []
    filtered: list[dict[str, object]] = []
    for item in discovered:
        if not isinstance(item, dict):
            continue
        host = _normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if (host, port) not in configured_keys:
            continue
        filtered.append(dict(item))
    return filtered


def _augment_discovered_kiwis_with_status(discovered: object, configured: object, *, timeout_s: float = 0.75) -> list[dict[str, object]]:
    configured_items = [dict(item) for item in configured if isinstance(item, dict)] if isinstance(configured, list) else []
    out = [dict(item) for item in discovered if isinstance(item, dict)] if isinstance(discovered, list) else []
    by_key: dict[tuple[str, int], int] = {}
    for index, item in enumerate(out):
        host = _normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if _is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        by_key[(host, port)] = index

    for item in configured_items:
        host = _normalize_kiwi_host(item.get("host"))
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if _is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
            continue
        existing_index = by_key.get((host, port))
        merged = dict(out[existing_index]) if existing_index is not None else {}
        merged["host"] = host
        merged["port"] = port
        known_source = str(merged.get("known_source") or "").strip()
        if existing_index is None and not known_source:
            merged["known_source"] = "configured"
        status = _read_kiwi_status(host, port, timeout_s=timeout_s)
        if status:
            latitude, longitude = _extract_gps_lat_lon(status)
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


def _get_configured_kiwis(mgr: object) -> list[dict[str, object]]:
    if hasattr(mgr, "get_configured_kiwis"):
        try:
            data = mgr.get_configured_kiwis()  # type: ignore[attr-defined]
            if isinstance(data, list):
                return [dict(item) for item in data if isinstance(item, dict)]
        except Exception:
            pass
    raw = getattr(mgr, "configured_kiwis", [])
    return [dict(item) for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


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


def _has_kiwi_password(mgr: object) -> bool:
    if hasattr(mgr, "has_kiwi_password"):
        try:
            return bool(mgr.has_kiwi_password())  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Failed reading Kiwi password state", exc_info=True)
    return bool(str(getattr(mgr, "password", "") or "").strip())


def _has_kiwi_admin_password(mgr: object) -> bool:
    if hasattr(mgr, "has_kiwi_admin_password"):
        try:
            return bool(mgr.has_kiwi_admin_password())  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Failed reading Kiwi admin password state", exc_info=True)
    return bool(str(getattr(mgr, "admin_password", "") or "").strip())


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

    def _parse_kiwi_status(text: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in (text or "").splitlines():
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
        return out

    def _extract_gps_lat_lon(status: dict[str, str]) -> tuple[float | None, float | None]:
        gps = status.get("gps")
        if not gps:
            return None, None
        # gps=(38.594989, -78.431794)
        m = re.search(r"\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*\)", gps)
        if not m:
            return None, None
        try:
            return float(m.group(1)), float(m.group(2))
        except Exception:
            return None, None

    def _looks_like_kiwi_http(ip: str, port: int, *, timeout_s: float) -> bool:
        try:
            with urlopen(f"http://{ip}:{port}/", timeout=max(timeout_s, 0.5)) as resp:
                data = resp.read(8192)
            txt = data.decode("utf-8", errors="ignore").lower()
            return "kiwisdr" in txt or "kiwi sdr" in txt
        except Exception:
            return False

    def _read_kiwi_status(ip: str, port: int, *, timeout_s: float) -> dict[str, str] | None:
        try:
            with urlopen(f"http://{ip}:{port}/status", timeout=max(timeout_s, 0.5)) as resp:
                data = resp.read(65536)
            txt = data.decode("utf-8", errors="ignore")
            if "status=" not in txt:
                return None
            return _parse_kiwi_status(txt)
        except Exception:
            return None

    def _parse_my_kiwisdr_for_lan_hosts(html: str) -> list[tuple[str, int]]:
        # Extract RFC1918 host:port strings shown as links, e.g. 192.168.1.93:8073
        pat = re.compile(
            r"\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}"
            r"|192\.168\.(?:\d{1,3}\.)\d{1,3}"
            r"|172\.(?:1[6-9]|2\d|3[0-1])\.(?:\d{1,3}\.)\d{1,3})"
            r":(\d{1,5})\b"
        )
        out: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()
        for m in pat.finditer(html or ""):
            hostport = m.group(0)
            host, port_s = hostport.split(":", 1)
            try:
                port = int(port_s)
            except Exception:
                continue
            if port < 1 or port > 65535:
                continue
            t = (host, port)
            if t in seen:
                continue
            seen.add(t)
            out.append(t)
        return out

    def _collapse_html_text(value: str) -> str:
        return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", value or ""))).strip()

    def _parse_my_kiwisdr_entries(html: str) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        seen: set[tuple[str, int]] = set()
        for known_order, match in enumerate(re.finditer(r"<tr\b[^>]*>(.*?)</tr>", html or "", re.IGNORECASE | re.DOTALL)):
            row_html = match.group(1) or ""
            endpoint: tuple[str, int] | None = None
            for candidate in re.finditer(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{1,5})\b", row_html):
                host = str(candidate.group(1) or "").strip()
                try:
                    port = int(candidate.group(2) or 0)
                except Exception:
                    continue
                if port < 1 or port > 65535:
                    continue
                endpoint = (host, port)
                break
            if not endpoint or endpoint in seen:
                continue
            seen.add(endpoint)
            row_text = _collapse_html_text(row_html)
            entry: dict[str, object] = {
                "host": endpoint[0],
                "port": endpoint[1],
                "known_source": "my.kiwisdr.com",
                "known_order": known_order,
            }
            sw_version_match = re.search(r"Kiwi\s+v[^,<>]*,\s*Debian\s+[^\s<>]+", row_text, re.IGNORECASE)
            if sw_version_match:
                entry["sw_version"] = sw_version_match.group(0).strip()
            name_match = re.search(r"\((kiwisdr-[^)]+)\)", row_text, re.IGNORECASE)
            if name_match:
                entry["name"] = name_match.group(1).strip()
            hardware_match = re.search(r"(KiwiSDR\s+\d+\s+\([^)]+\))", row_text)
            if hardware_match:
                entry["sdr_hw"] = hardware_match.group(1).strip()
            out.append(entry)
        return out

    @router.get("/config/discover")
    def discover_kiwi(request: Request, port: int = 8073, timeout_s: float = 0.20, max_hosts: int = 32):
        """Best-effort LAN discovery for KiwiSDR.

        Scans the caller's /24 (based on request.client.host) for the given TCP port
        and returns any hosts whose HTTP root page looks like a KiwiSDR.

        This is intentionally conservative (small timeouts, bounded results) so it
        can't hang the server.
        """

        if port < 1 or port > 65535:
            raise HTTPException(status_code=400, detail="port must be 1..65535")
        if timeout_s <= 0 or timeout_s > 2:
            raise HTTPException(status_code=400, detail="timeout_s must be > 0 and <= 2")
        if max_hosts < 1 or max_hosts > 256:
            raise HTTPException(status_code=400, detail="max_hosts must be 1..256")

        started = time.time()

        # Preferred: use my.kiwisdr.com to obtain the LAN address/port shown for this Kiwi.
        candidates: list[tuple[str, int]] = []
        discovered_meta: dict[tuple[str, int], dict[str, object]] = {}
        source = ""
        try:
            with urlopen("http://my.kiwisdr.com/", timeout=2.0) as resp:
                html = resp.read(1024 * 1024).decode("utf-8", errors="ignore")
            my_kiwi_entries = _parse_my_kiwisdr_entries(html)
            candidates = [(str(entry.get("host") or ""), int(entry.get("port") or 0)) for entry in my_kiwi_entries]
            discovered_meta = {
                (str(entry.get("host") or ""), int(entry.get("port") or 0)): dict(entry)
                for entry in my_kiwi_entries
                if str(entry.get("host") or "") and 1 <= int(entry.get("port") or 0) <= 65535
            }
            if not candidates:
                candidates = _parse_my_kiwisdr_for_lan_hosts(html)
            if candidates:
                source = "my.kiwisdr.com"
        except Exception:
            candidates = []
            discovered_meta = {}

        # Fallback: scan the caller's /24 based on request.client.host.
        if not candidates:
            client_ip = (request.client.host if request.client else "") or ""
            m = re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", client_ip)
            if not m:
                prefixes = ["192.168.1", "192.168.0", "10.0.0"]
            else:
                prefixes = [f"{m.group(1)}.{m.group(2)}.{m.group(3)}"]
            source = "lan_scan"

            def has_port_open(ip: str) -> bool:
                try:
                    with socket.create_connection((ip, port), timeout=timeout_s):
                        return True
                except OSError:
                    return False

            for prefix in prefixes:
                ips = [f"{prefix}.{i}" for i in range(1, 255)]
                with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ex:
                    for ip, ok in zip(ips, ex.map(has_port_open, ips)):
                        if ok:
                            candidates.append((ip, port))
                            if len(candidates) >= max_hosts:
                                break
                if candidates:
                    break

        found: list[dict[str, object]] = []
        for host, hp_port in candidates:
            if len(found) >= max_hosts:
                break
            if not _looks_like_kiwi_http(host, hp_port, timeout_s=timeout_s):
                continue
            row_meta = discovered_meta.get((host, hp_port), {})
            st = _read_kiwi_status(host, hp_port, timeout_s=timeout_s)
            lat, lon = (None, None)
            name = str(row_meta.get("name") or "").strip() or None
            grid = None
            gps_good = None
            sdr_hw = str(row_meta.get("sdr_hw") or "").strip() or None
            sw_version = str(row_meta.get("sw_version") or "").strip() or None
            if st:
                lat, lon = _extract_gps_lat_lon(st)
                name = name or st.get("name")
                grid = st.get("grid")
                gps_good = st.get("gps_good")
                sdr_hw = sdr_hw or st.get("sdr_hw")
                sw_version = sw_version or st.get("sw_version")
            found.append(
                {
                    "host": host,
                    "port": hp_port,
                    "known_source": str(row_meta.get("known_source") or source or "").strip(),
                    "latitude": lat,
                    "longitude": lon,
                    "grid": grid,
                    "gps_good": gps_good,
                    "name": name,
                    "sdr_hw": sdr_hw,
                    "sw_version": sw_version,
                    "known_order": row_meta.get("known_order"),
                }
            )

        return {
            "ok": True,
            "source": source,
            "found": found,
            "elapsed_s": round(time.time() - started, 3),
        }

    @router.get("/config/verify")
    def verify_kiwi(host: str, port: int = 8073, timeout_s: float = 1.2):
        host_text = str(host or "").strip()
        if not host_text:
            raise HTTPException(status_code=400, detail="host is required")
        if port < 1 or port > 65535:
            raise HTTPException(status_code=400, detail="port must be 1..65535")
        if timeout_s <= 0 or timeout_s > 5:
            raise HTTPException(status_code=400, detail="timeout_s must be > 0 and <= 5")

        status = _read_kiwi_status(host_text, port, timeout_s=timeout_s)
        latitude = None
        longitude = None
        grid = None
        gps_good = None
        name = None
        sdr_hw = None
        sw_version = None
        if status:
            latitude, longitude = _extract_gps_lat_lon(status)
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
            st = _read_kiwi_status(host, port, timeout_s=0.75)
            if st:
                kiwi_lat, kiwi_lon = _extract_gps_lat_lon(st)
                kiwi_grid = st.get("grid")
                kiwi_gps_good = st.get("gps_good")
        except Exception:
            pass

        kiwi_password_set = _has_kiwi_password(mgr)
        kiwi_admin_password_set = _has_kiwi_admin_password(mgr)
        with mgr.lock:  # type: ignore[attr-defined]
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
                "kiwisdrs": _get_configured_kiwis(mgr),
                "active_kiwi_index": _get_active_kiwi_index(mgr),
                "discovered_kiwis": getattr(mgr, "discovered_kiwis", []),
                "discovery_source": str(getattr(mgr, "discovery_source", "") or ""),
                "kiwi_password_set": kiwi_password_set,
                "kiwi_admin_password_set": kiwi_admin_password_set,
                "kiwi_latitude": kiwi_lat,
                "kiwi_longitude": kiwi_lon,
                "kiwi_grid": kiwi_grid,
                "kiwi_gps_good": kiwi_gps_good,
            }

    @router.post("/config")
    async def set_config(request: Request):
        data = await request.json()
        prior_host = ""
        prior_port = 0
        prior_active_kiwi_index = _get_active_kiwi_index(mgr)
        prior_configured_keys = _configured_kiwi_keys(_get_configured_kiwis(mgr))
        active_kiwi_index = data.get("active_kiwi_index") if isinstance(data, dict) else None
        try:
            with mgr.lock:  # type: ignore[attr-defined]
                prior_host = _normalize_kiwi_host(getattr(mgr, "host", ""))
                prior_port = int(getattr(mgr, "port", 0) or 0)
        except Exception:
            prior_host = _normalize_kiwi_host(getattr(mgr, "host", ""))
            try:
                prior_port = int(getattr(mgr, "port", 0) or 0)
            except Exception:
                prior_port = 0
        configured_kiwis = data.get("kiwisdrs") if isinstance(data, dict) else None
        kiwi_password_changed = False
        kiwi_admin_password_changed = False
        if "kiwisdrs" in data and hasattr(mgr, "set_configured_kiwis"):
            existing_configured = _get_configured_kiwis(mgr)
            configured_kiwis = _preserve_configured_kiwi_order(configured_kiwis, existing_configured)
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
                mgr.set_discovered_kiwis({
                    "found": data.get("discovered_kiwis"),
                    "source": data.get("discovery_source"),
                }, save=False)  # type: ignore[attr-defined]
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"invalid value for discovered_kiwis: {exc}") from exc
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
                        mgr.host = _normalize_kiwi_host(v)
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
                existing = _get_configured_kiwis(mgr)
                primary: dict[str, object] = {
                    "host": _normalize_kiwi_host(mgr.host),
                    "port": int(mgr.port),
                    "latitude": float(mgr.latitude),
                    "longitude": float(mgr.longitude),
                }
                if existing:
                    merge_index = 0
                    grid = str(existing[merge_index].get("grid") or "").strip()
                    if grid:
                        primary["grid"] = grid
                    merged = list(existing)
                    if _is_unconfigured_kiwi_host(primary["host"]):
                        del merged[merge_index]
                    else:
                        merged[merge_index] = primary
                else:
                    merged = [] if _is_unconfigured_kiwi_host(primary["host"]) else [primary]
                mgr.set_configured_kiwis(merged, save=False)  # type: ignore[attr-defined]
        existing_discovered = list(getattr(mgr, "discovered_kiwis", []) or [])
        configured_snapshot = _get_configured_kiwis(mgr)
        if configured_snapshot:
            primary_entry = configured_snapshot[0]
            if isinstance(primary_entry, dict):
                active_host = _normalize_kiwi_host(primary_entry.get("host"))
                try:
                    active_port = int(primary_entry.get("port") or 0)
                except Exception:
                    active_port = 0
                if not _is_unconfigured_kiwi_host(active_host) and 1 <= active_port <= 65535:
                    with mgr.lock:  # type: ignore[attr-defined]
                        mgr.host = active_host
                        mgr.port = active_port
                        if primary_entry.get("latitude") is not None:
                            mgr.latitude = float(primary_entry.get("latitude"))
                        if primary_entry.get("longitude") is not None:
                            mgr.longitude = float(primary_entry.get("longitude"))
        enriched_discovery = _augment_discovered_kiwis_with_status(
            existing_discovered,
            configured_snapshot,
        )
        discovery_source = str(getattr(mgr, "discovery_source", "") or "").strip()
        if enriched_discovery and not discovery_source:
            discovery_source = "status"
        if hasattr(mgr, "set_discovered_kiwis"):
            mgr.set_discovered_kiwis({  # type: ignore[attr-defined]
                "found": enriched_discovery,
                "source": discovery_source,
            }, save=False)
        with mgr.lock:  # type: ignore[attr-defined]
            mgr._save_config()  # type: ignore[attr-defined]
        if (kiwi_password_changed or kiwi_admin_password_changed) and hasattr(mgr, "_save_secrets"):
            try:
                mgr._save_secrets()  # type: ignore[attr-defined]
            except Exception:
                logger.debug("Failed saving Kiwi secret", exc_info=True)

        try:
            current_host = _normalize_kiwi_host(getattr(mgr, "host", ""))
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

        return {
            "ok": True,
            "discovered_kiwis": list(getattr(mgr, "discovered_kiwis", []) or []),
            "discovery_source": str(getattr(mgr, "discovery_source", "") or ""),
        }

    return router
