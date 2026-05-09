from __future__ import annotations

import json
import os
import platform
import shutil
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter

from ..kiwi_discovery import read_kiwi_status

_SYSTEM_INFO_CACHE_LOCK: threading.Lock = threading.Lock()
_SYSTEM_INFO_CACHE: dict[str, Any] = {"payload": None, "timestamp": 0.0, "future": None}


def _get_cached_discovery(mgr: object) -> dict[str, object]:
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
            pass
    found = getattr(mgr, "discovered_kiwis", [])
    return {
        "found": list(found) if isinstance(found, list) else [],
        "source": str(getattr(mgr, "discovery_source", "") or "").strip(),
        "updated_unix": getattr(mgr, "discovery_updated_unix", None),
    }


def _read_proc_uptime_seconds() -> float | None:
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as handle:
            return float((handle.read().split() or [""])[0])
    except Exception:
        return None


def _read_meminfo_kib() -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                fields = value.strip().split()
                if not fields:
                    continue
                try:
                    out[key.strip()] = int(fields[0])
                except Exception:
                    continue
    except Exception:
        return {}
    return out


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return None


def _safe_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except Exception:
        return None


def _parse_elapsed_seconds(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parts = [int(part) for part in text.split(":") if str(part).strip()]
    except Exception:
        return None
    if len(parts) == 3:
        return max(0, (parts[0] * 3600) + (parts[1] * 60) + parts[2])
    if len(parts) == 2:
        return max(0, (parts[0] * 60) + parts[1])
    if len(parts) == 1:
        return max(0, parts[0])
    return None


def _resolve_managed_label(label: str, label_to_rx: dict[str, Any]) -> tuple[str | None, int | None]:
    clean = str(label or "").strip()
    if not clean:
        return (None, None)
    
    if clean in label_to_rx:
        val = label_to_rx[clean]
        if isinstance(val, list) and val:
            return (clean, val.pop(0))
        elif isinstance(val, int):
            return (clean, val)
        
    # Fallback substring checks for label truncation
    for managed_label, rx_val in list(label_to_rx.items()):
        if clean.startswith(managed_label) or managed_label.startswith(clean):
            if isinstance(rx_val, list) and rx_val:
                return (managed_label, rx_val.pop(0))
            elif isinstance(rx_val, int):
                return (managed_label, rx_val)
            
    return (None, None)


def _fetch_kiwi_users(host: str, port: int) -> list[dict[str, Any]]:
    for path in ("/users?json=1", "/users?admin=1", "/users"):
        try:
            req = urllib.request.Request(f"http://{host}:{int(port)}{path}", method="GET")
            with urllib.request.urlopen(req, timeout=0.5) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if isinstance(payload, list):
                return [row for row in payload if isinstance(row, dict)]
        except Exception:
            continue
    return []


def _count_active_kiwi_users(rows: list[dict[str, Any]]) -> int:
    active = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        fields = (
            row.get("n"),
            row.get("m"),
            row.get("f"),
            row.get("g"),
            row.get("a"),
            row.get("t"),
        )
        if any(value not in (None, "") for value in fields):
            active += 1
    return active


def _build_kiwi_status_snapshot(status: dict[str, Any]) -> dict[str, object]:
    return {
        "name": status.get("name"),
        "sdr_hw": status.get("sdr_hw"),
        "sw_version": status.get("sw_version"),
        "bands": status.get("bands"),
        "users": _safe_int(status.get("users")),
        "users_max": _safe_int(status.get("users_max")),
        "preempt": _safe_int(status.get("preempt")),
        "gps": status.get("gps"),
        "grid": status.get("grid"),
        "gps_good": _safe_int(status.get("gps_good")),
        "fixes": _safe_int(status.get("fixes")),
        "loc": status.get("loc"),
        "antenna": status.get("antenna"),
        "snr": status.get("snr"),
        "adc_ov": _safe_int(status.get("adc_ov")),
        "uptime_seconds": _safe_int(status.get("uptime")),
        "date": status.get("date"),
        "offline": status.get("offline"),
    }


def _normalize_discovered_entry(entry: object) -> dict[str, object] | None:
    if not isinstance(entry, dict):
        return None
    host = str(entry.get("host") or "").strip()
    port = _safe_int(entry.get("port")) or 0
    if not host or port <= 0:
        return None
    return dict(entry)


def _build_discovered_kiwi_entries(
    entries: list[object],
    *,
    active_host: str,
    active_port: int,
    active_status: dict[str, Any],
    active_users: list[dict[str, Any]],
) -> list[dict[str, object]]:
    normalized_entries = [entry for item in entries if (entry := _normalize_discovered_entry(item)) is not None]
    if not normalized_entries:
        return []

    def _enrich(entry: dict[str, object]) -> dict[str, object]:
        host = str(entry.get("host") or "").strip()
        port = _safe_int(entry.get("port")) or 0
        if host == active_host and port == active_port:
            status = dict(active_status)
            users = list(active_users)
        else:
            status = read_kiwi_status(host, port, timeout_s=0.5) or {}
            users = _fetch_kiwi_users(host, port)
        enriched = dict(entry)
        enriched["reachable"] = bool(status or users)
        enriched["status"] = _build_kiwi_status_snapshot(status)
        enriched["raw_user_count"] = len(users)
        enriched["active_user_count"] = _count_active_kiwi_users(users)
        return enriched

    max_workers = min(4, len(normalized_entries)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(_enrich, normalized_entries))


def _build_container_payload() -> dict[str, object]:
    disk_total, disk_used, disk_free = shutil.disk_usage("/")
    meminfo = _read_meminfo_kib()
    mem_total_kib = _safe_int(meminfo.get("MemTotal"))
    mem_available_kib = _safe_int(meminfo.get("MemAvailable"))
    mem_used_kib = None
    mem_used_percent = None
    if mem_total_kib is not None and mem_available_kib is not None and mem_total_kib > 0:
        mem_used_kib = max(0, mem_total_kib - mem_available_kib)
        mem_used_percent = (float(mem_used_kib) / float(mem_total_kib)) * 100.0

    load_avg = None
    try:
        load_avg = os.getloadavg()
    except Exception:
        load_avg = None

    cpu_count = os.cpu_count() or 0
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_count": int(cpu_count),
        "uptime_seconds": _read_proc_uptime_seconds(),
        "load_1m": load_avg[0] if load_avg else None,
        "load_5m": load_avg[1] if load_avg else None,
        "load_15m": load_avg[2] if load_avg else None,
        "memory_total_bytes": (mem_total_kib * 1024) if mem_total_kib is not None else None,
        "memory_used_bytes": (mem_used_kib * 1024) if mem_used_kib is not None else None,
        "memory_available_bytes": (mem_available_kib * 1024) if mem_available_kib is not None else None,
        "memory_used_percent": mem_used_percent,
        "disk_total_bytes": int(disk_total),
        "disk_used_bytes": int(disk_used),
        "disk_free_bytes": int(disk_free),
        "disk_used_percent": (float(disk_used) / float(disk_total) * 100.0) if disk_total > 0 else None,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


def _build_kiwi_payload(mgr: object, receiver_mgr: object | None = None) -> dict[str, object]:
    with mgr.lock:  # type: ignore[attr-defined]
        host = str(getattr(mgr, "host", "") or "").strip()
        port = int(getattr(mgr, "port", 0) or 0)

    out: dict[str, object] = {
        "host": host,
        "port": port,
        "reachable": False,
        "status": {},
        "active_users": [],
        "raw_users": [],
        "discovered_kiwis": [],
        "discovery_source": "",
        "discovery_updated_unix": None,
    }
    cached_discovery = _get_cached_discovery(mgr)
    out["discovered_kiwis"] = cached_discovery.get("found", [])
    out["discovery_source"] = cached_discovery.get("source", "")
    out["discovery_updated_unix"] = cached_discovery.get("updated_unix")
    if not host or port <= 0:
        return out

    status = read_kiwi_status(host, port, timeout_s=0.5) or {}
    users = _fetch_kiwi_users(host, port)
    out["reachable"] = bool(status or users)
    out["status"] = _build_kiwi_status_snapshot(status)
    discovered_found = cached_discovery.get("found", [])
    out["discovered_kiwis"] = _build_discovered_kiwi_entries(
        list(discovered_found) if isinstance(discovered_found, list) else [],
        active_host=host,
        active_port=port,
        active_status=status,
        active_users=users,
    )

    # Build label → internal-rx mapping so the display uses KiwiScan's own rx
    # numbering rather than the raw Kiwi channel index.  This matters when the Kiwi
    # does not honour our --rx-chan hint and assigns a different slot (e.g. WSPR lands
    # on Kiwi ch4 while our internal rx=3, or FT8 lands on ch3 while internal rx=4).
    # active_label_to_rx() uses its own short-timeout lock internally; call it directly.
    label_to_rx: dict[str, Any] = {}
    label_source = receiver_mgr if receiver_mgr is not None else mgr
    if hasattr(label_source, "active_label_to_rx"):
        try:
            label_to_rx = label_source.active_label_to_rx()  # type: ignore[union-attr]
        except Exception:
            pass

    active_users: list[dict[str, object]] = []
    raw_users: list[dict[str, object]] = []
    for row in users:
        label = urllib.parse.unquote(str(row.get("n") or "")).strip()
        kiwi_rx = _safe_int(row.get("i"))
        row_payload = {
            "rx": kiwi_rx,
            "name": label or None,
            "location": urllib.parse.unquote(str(row.get("g") or "")).strip(),
            "freq_khz": round(float(row.get("f")) / 1000.0, 3) if _safe_float(row.get("f")) is not None else None,
            "mode": str(row.get("m") or "").strip().upper() or None,
            "ip": str(row.get("a") or "").strip() or None,
            "connected_seconds": _parse_elapsed_seconds(row.get("t")),
        }
        raw_users.append(dict(row_payload))
        resolved_label, resolved_rx = _resolve_managed_label(label, label_to_rx)
        if not resolved_label:
            continue
        rx_display = resolved_rx if resolved_rx is not None else kiwi_rx
        active_users.append(
            {
                "rx": rx_display,
                "name": resolved_label,
                "location": urllib.parse.unquote(str(row.get("g") or "")).strip(),
                "freq_khz": round(float(row.get("f")) / 1000.0, 3) if _safe_float(row.get("f")) is not None else None,
                "mode": str(row.get("m") or "").strip().upper() or None,
                "ip": str(row.get("a") or "").strip() or None,
                "connected_seconds": _parse_elapsed_seconds(row.get("t")),
            }
        )
    # Sort by our internal rx number so the table always appears in assignment order.
    active_users.sort(key=lambda u: u["rx"] if isinstance(u["rx"], int) else 999)
    raw_users.sort(key=lambda u: u["rx"] if isinstance(u["rx"], int) else 999)
    out["active_users"] = active_users
    out["raw_users"] = raw_users
    return out


def make_router(*, mgr: object, receiver_mgr: object | None = None) -> APIRouter:
    router = APIRouter()

    @router.get("/system/info")
    def get_system_info() -> Dict[str, object]:
        return {
            "container": _build_container_payload(),
            "kiwi": _build_kiwi_payload(mgr, receiver_mgr=receiver_mgr),
        }

    return router