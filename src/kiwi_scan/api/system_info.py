from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..audio_stream import stop_kiwi_audio_stream, stream_kiwi_audio_wav
from ..kiwi_discovery import read_kiwi_status

_SYSTEM_INFO_CACHE_LOCK: threading.Lock = threading.Lock()
_SYSTEM_INFO_CACHE: dict[str, Any] = {"payload": None, "timestamp": 0.0, "future": None}
logger = logging.getLogger(__name__)


def _get_configured_kiwis(mgr: object) -> list[dict[str, object]]:
    if hasattr(mgr, "get_configured_kiwis"):
        try:
            data = mgr.get_configured_kiwis()  # type: ignore[attr-defined]
            if isinstance(data, list):
                return [dict(entry) for entry in data if isinstance(entry, dict)]
        except Exception:
            pass
    data = getattr(mgr, "configured_kiwis", [])
    return [dict(entry) for entry in data if isinstance(entry, dict)] if isinstance(data, list) else []


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


def _resolve_runtime_target(mgr: object, kiwi_key: str | None = None) -> tuple[str, int]:
    requested_key = str(kiwi_key or "").strip()
    if hasattr(mgr, "resolve_runtime_target"):
        try:
            resolved = mgr.resolve_runtime_target(kiwi_key=requested_key or None)  # type: ignore[attr-defined]
            host = str((resolved or {}).get("host") or "").strip()
            port = int((resolved or {}).get("port") or 0)
            if host and port > 0:
                return host, port
        except Exception:
            pass
    with mgr.lock:  # type: ignore[attr-defined]
        return (
            str(getattr(mgr, "host", "") or "").strip(),
            int(getattr(mgr, "port", 0) or 0),
        )


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
            with urllib.request.urlopen(req, timeout=2.0) as resp:
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
        "cpu_pct": _safe_int(status.get("cpu")),
    }


def _normalize_known_kiwi_entry(entry: object) -> dict[str, object] | None:
    if not isinstance(entry, dict):
        return None
    host = str(entry.get("host") or "").strip()
    port = _safe_int(entry.get("port")) or 0
    if not host or port <= 0:
        return None
    normalized = dict(entry)
    normalized["host"] = host
    normalized["port"] = port
    return normalized


def _merge_known_kiwi_source(existing: object, incoming: object) -> str:
    values = []
    for candidate in (existing, incoming):
        text = str(candidate or "").strip().lower()
        if not text:
            continue
        for part in text.split("_"):
            if part and part not in values:
                values.append(part)
    if not values:
        return ""
    if values == ["configured", "discovered"]:
        return "configured_discovered"
    if len(values) == 1:
        return values[0]
    return "_".join(values)


def _build_known_kiwi_seed_entries(
    *,
    configured_entries: list[dict[str, object]],
    discovered_entries: list[object],
    active_host: str,
    active_port: int,
) -> list[dict[str, object]]:
    merged: dict[tuple[str, int], dict[str, object]] = {}

    for index, raw_entry in enumerate(configured_entries):
        entry = _normalize_known_kiwi_entry(raw_entry)
        if entry is None:
            continue
        entry["known_source"] = "configured"
        entry["known_order"] = index
        merged[(str(entry["host"]), int(entry["port"]))] = entry

    for raw_entry in discovered_entries:
        entry = _normalize_known_kiwi_entry(raw_entry)
        if entry is None:
            continue
        key = (str(entry["host"]), int(entry["port"]))
        existing = merged.get(key)
        if existing is None:
            merged[key] = entry
            continue
        known_order = existing.get("known_order")
        merged_entry = dict(existing)
        merged_entry.update(entry)
        merged_entry["known_source"] = _merge_known_kiwi_source(existing.get("known_source"), "discovered")
        if known_order is not None:
            merged_entry["known_order"] = known_order
        merged[key] = merged_entry

    return list(merged.values())


def _build_known_kiwi_entries(
    entries: list[object],
    *,
    active_host: str,
    active_port: int,
    active_status: dict[str, Any],
    active_users: list[dict[str, Any]],
) -> list[dict[str, object]]:
    normalized_entries = [entry for item in entries if (entry := _normalize_known_kiwi_entry(item)) is not None]
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


def _resolve_audio_stream_endpoint(
    *,
    mgr: object,
    requested_host: str | None,
    requested_port: int | None,
) -> tuple[str, int]:
    lock = getattr(mgr, "lock", None)
    if hasattr(lock, "acquire") and hasattr(lock, "release"):
        lock.acquire()
        try:
            active_host = str(getattr(mgr, "host", "") or "").strip()
            active_port = int(getattr(mgr, "port", 0) or 0)
        finally:
            lock.release()
    else:
        active_host = str(getattr(mgr, "host", "") or "").strip()
        active_port = int(getattr(mgr, "port", 0) or 0)

    if not requested_host and (requested_port is None or int(requested_port or 0) <= 0):
        return active_host, active_port

    candidate_host = str(requested_host or "").strip()
    candidate_port = int(requested_port or 0)
    if not candidate_host or candidate_port <= 0:
        raise HTTPException(status_code=400, detail="Audio stream host and port must both be provided")

    configured_kiwis = _get_configured_kiwis(mgr)
    cached_discovery = _get_cached_discovery(mgr)
    seed_entries = _build_known_kiwi_seed_entries(
        configured_entries=configured_kiwis,
        discovered_entries=list(cached_discovery.get("found", [])) if isinstance(cached_discovery.get("found"), list) else [],
        active_host=active_host,
        active_port=active_port,
    )
    allowed_endpoints = {
        (str(entry.get("host") or "").strip(), int(entry.get("port") or 0))
        for entry in seed_entries
        if str(entry.get("host") or "").strip() and int(entry.get("port") or 0) > 0
    }
    if active_host and active_port > 0:
        allowed_endpoints.add((active_host, active_port))
    if (candidate_host, candidate_port) not in allowed_endpoints:
        raise HTTPException(status_code=400, detail="Audio stream endpoint is not a configured KiwiSDR")
    return candidate_host, candidate_port


def _resolve_audio_stream_receiver_target(
    *,
    receiver_mgr: object | None,
    requested_rx: int | None,
    requested_kiwi_rx: int | None,
    requested_host: str | None,
    requested_port: int | None,
) -> tuple[int | None, str | None, int | None]:
    resolved_kiwi_rx = int(requested_kiwi_rx) if requested_kiwi_rx is not None else None
    resolved_host = str(requested_host or "").strip() or None
    resolved_port = int(requested_port) if requested_port is not None else None
    if requested_rx is None or receiver_mgr is None or not hasattr(receiver_mgr, "health_summary"):
        return resolved_kiwi_rx, resolved_host, resolved_port

    summary: dict[str, object] | None = None
    if resolved_host and resolved_port is not None and resolved_port > 0 and hasattr(receiver_mgr, "health_summary_for_target"):
        try:
            summary = receiver_mgr.health_summary_for_target(resolved_host, resolved_port)  # type: ignore[attr-defined]
        except Exception:
            logger.exception(
                "Failed reading targeted receiver health summary for audio stream resolution host=%s port=%s",
                resolved_host,
                resolved_port,
            )
    try:
        if summary is None:
            summary = receiver_mgr.health_summary()  # type: ignore[attr-defined]
    except Exception:
        logger.exception("Failed reading receiver health summary for audio stream resolution")
        return resolved_kiwi_rx, resolved_host, resolved_port

    channels = summary.get("channels") if isinstance(summary, dict) else None
    channel = channels.get(str(int(requested_rx))) if isinstance(channels, dict) else None
    if not isinstance(channel, dict):
        return resolved_kiwi_rx, resolved_host, resolved_port

    channel_kiwi_rx = _safe_int(channel.get("kiwi_actual_rx"))
    if channel_kiwi_rx is None:
        channel_kiwi_rx = _safe_int(channel.get("kiwi_rx"))
    channel_host = str(channel.get("host") or "").strip() or None
    channel_port = _safe_int(channel.get("port"))

    if channel_kiwi_rx is not None and channel_kiwi_rx >= 0:
        resolved_kiwi_rx = channel_kiwi_rx
    if (not resolved_host or resolved_port is None or resolved_port <= 0) and channel_host and channel_port is not None and channel_port > 0:
        resolved_host = channel_host
        resolved_port = channel_port
    return resolved_kiwi_rx, resolved_host, resolved_port


def _resolve_audio_stream_source_freq_hz(
    *,
    receiver_mgr: object | None,
    requested_rx: int | None,
    requested_source_freq_hz: float | None,
    requested_host: str | None,
    requested_port: int | None,
) -> float | None:
    resolved_source_freq_hz = float(requested_source_freq_hz) if requested_source_freq_hz is not None else None
    if requested_rx is None or receiver_mgr is None:
        return resolved_source_freq_hz

    summary: dict[str, object] | None = None
    normalized_host = str(requested_host or "").strip() or None
    normalized_port = int(requested_port) if requested_port is not None else None
    if normalized_host and normalized_port is not None and normalized_port > 0 and hasattr(receiver_mgr, "health_summary_for_target"):
        try:
            summary = receiver_mgr.health_summary_for_target(normalized_host, normalized_port)  # type: ignore[attr-defined]
        except Exception:
            logger.exception(
                "Failed reading targeted receiver health summary for audio source frequency host=%s port=%s",
                normalized_host,
                normalized_port,
            )
    try:
        if summary is None:
            summary = receiver_mgr.health_summary()  # type: ignore[attr-defined]
    except Exception:
        logger.exception("Failed reading receiver health summary for audio source frequency resolution")
    else:
        channels = summary.get("channels") if isinstance(summary, dict) else None
        channel = channels.get(str(int(requested_rx))) if isinstance(channels, dict) else None
        if isinstance(channel, dict):
            channel_freq_hz = _safe_float(channel.get("freq_hz"))
            if channel_freq_hz is not None and channel_freq_hz > 0:
                return float(channel_freq_hz)

    lock = getattr(receiver_mgr, "_lock", None)
    lock_acquired = False
    if hasattr(lock, "acquire") and hasattr(lock, "release"):
        try:
            lock_acquired = bool(lock.acquire(timeout=0.05))
        except Exception:
            lock_acquired = False
        if not lock_acquired:
            return resolved_source_freq_hz
    try:
        assignments = getattr(receiver_mgr, "_assignments", None)
        assignment = assignments.get(int(requested_rx)) if isinstance(assignments, dict) else None
        assignment_freq_hz = _safe_float(getattr(assignment, "freq_hz", None))
        if assignment_freq_hz is not None and assignment_freq_hz > 0:
            return float(assignment_freq_hz)
    except Exception:
        logger.exception("Failed reading receiver assignment source frequency for rx=%s", requested_rx)
    finally:
        if lock_acquired:
            lock.release()
    return resolved_source_freq_hz


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


def _build_kiwi_payload(mgr: object, receiver_mgr: object | None = None, *, kiwi_key: str | None = None) -> dict[str, object]:
    host, port = _resolve_runtime_target(mgr, kiwi_key)

    out: dict[str, object] = {
        "host": host,
        "port": port,
        "reachable": False,
        "status": {},
        "active_users": [],
        "raw_users": [],
        "configured_kiwis": [],
        "discovered_kiwis": [],
        "discovery_source": "",
        "discovery_updated_unix": None,
    }
    cached_discovery = _get_cached_discovery(mgr)
    configured_kiwis = _get_configured_kiwis(mgr)
    out["configured_kiwis"] = configured_kiwis
    seed_entries = _build_known_kiwi_seed_entries(
        configured_entries=configured_kiwis,
        discovered_entries=list(cached_discovery.get("found", [])) if isinstance(cached_discovery.get("found"), list) else [],
        active_host=host,
        active_port=port,
    )
    out["discovered_kiwis"] = seed_entries
    out["discovery_source"] = cached_discovery.get("source", "")
    out["discovery_updated_unix"] = cached_discovery.get("updated_unix")
    if not host or port <= 0:
        return out

    status = read_kiwi_status(host, port, timeout_s=0.5) or {}
    users = _fetch_kiwi_users(host, port)
    out["reachable"] = bool(status or users)
    out["status"] = _build_kiwi_status_snapshot(status)
    out["discovered_kiwis"] = _build_known_kiwi_entries(
        seed_entries,
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
    if hasattr(label_source, "active_label_to_rx_for_target"):
        try:
            label_to_rx = label_source.active_label_to_rx_for_target(host, port)  # type: ignore[union-attr]
        except Exception:
            pass
    elif hasattr(label_source, "active_label_to_rx"):
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
            "host": host,
            "port": port,
        }
        raw_users.append(dict(row_payload))
        resolved_label, resolved_rx = _resolve_managed_label(label, label_to_rx)
        if not resolved_label:
            continue
        rx_display = resolved_rx if resolved_rx is not None else kiwi_rx
        active_users.append(
            {
                "rx": rx_display,
                "kiwi_rx": kiwi_rx,
                "name": resolved_label,
                "location": urllib.parse.unquote(str(row.get("g") or "")).strip(),
                "freq_khz": round(float(row.get("f")) / 1000.0, 3) if _safe_float(row.get("f")) is not None else None,
                "mode": str(row.get("m") or "").strip().upper() or None,
                "ip": str(row.get("a") or "").strip() or None,
                "connected_seconds": _parse_elapsed_seconds(row.get("t")),
                "host": host,
                "port": port,
            }
        )
    # Sort by our internal rx number so the table always appears in assignment order.
    active_users.sort(key=lambda u: u["rx"] if isinstance(u["rx"], int) else 999)
    raw_users.sort(key=lambda u: u["rx"] if isinstance(u["rx"], int) else 999)
    out["active_users"] = active_users
    out["raw_users"] = raw_users
    return out


def _find_kick_script() -> Path | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "tools" / "kiwi_admin_kick.py"
        if candidate.exists():
            return candidate
    return None


def _read_admin_password() -> str:
    search_roots = [Path("/opt/kiwiscan")]
    here = Path(__file__).resolve()
    for parent in here.parents:
        search_roots.append(parent)
        if (parent / "pyproject.toml").exists():
            break
    for root in search_roots:
        candidate = root / "outputs" / "automation_settings.json"
        try:
            data = json.loads(candidate.read_text())
            val = str(data.get("kiwi_admin_password") or "").strip()
            if val:
                return val
        except Exception:
            continue
    return ""


def make_router(*, mgr: object, receiver_mgr: object | None = None) -> APIRouter:
    router = APIRouter()

    @router.get("/system/info")
    def get_system_info(kiwi_key: str | None = Query(default=None)) -> Dict[str, object]:
        return {
            "container": _build_container_payload(),
            "kiwi": _build_kiwi_payload(mgr, receiver_mgr=receiver_mgr, kiwi_key=kiwi_key),
        }

    @router.get("/system/kiwi_status")
    def get_kiwi_status(kiwi_key: str = Query(...)) -> Dict[str, object]:
        """Fetch per-kiwi status (cpu_pct, snr, users, etc.) by parsing kiwi_key as host:port directly."""
        try:
            host_part, port_part = kiwi_key.rsplit(":", 1)
            host_part = host_part.strip()
            port_val = int(port_part.strip())
        except Exception:
            return {"error": "invalid kiwi_key format, expected host:port"}
        status = read_kiwi_status(host_part, port_val, timeout_s=3.0) or {}
        return _build_kiwi_status_snapshot(status)

    @router.get("/system/kiwi_users")
    def get_kiwi_users(kiwi_key: str = Query(...)) -> Dict[str, object]:
        """Return live users on a KiWi SDR, flagging managed (KiwiScan) vs public connections."""
        try:
            host_part, port_part = kiwi_key.rsplit(":", 1)
            host_part = host_part.strip()
            port_val = int(port_part.strip())
        except Exception:
            return {"error": "invalid kiwi_key format, expected host:port", "users": []}
        rows = _fetch_kiwi_users(host_part, port_val)
        users = []
        for row in rows:
            label = urllib.parse.unquote(str(row.get("n") or "")).strip()
            freq_raw = _safe_float(row.get("f"))
            conn_s = _parse_elapsed_seconds(row.get("t"))
            # Require a non-empty name or a non-zero frequency — empty KiwiSDR slots
            # have n="" and f=0, and f=0 is not in (None, "") so the old any() check
            # incorrectly treated those as active.
            is_occupied = bool(label) or (freq_raw is not None and freq_raw > 0)
            if not is_occupied:
                continue
            label_upper = label.upper()
            is_managed = bool(label and (
                label_upper.startswith("AUTO_")
                or label_upper.startswith("FIXED_")
                or label_upper.startswith("ROAM")
                or label_upper.startswith("MANUAL_")
                or label_upper.startswith("MAN_")
                or label_upper.startswith("HOLD_")
                or label_upper.startswith("SKIP_")
            ))
            users.append({
                "rx": _safe_int(row.get("i")),
                "name": label or None,
                "freq_khz": round(freq_raw / 1000.0, 3) if freq_raw is not None else None,
                "mode": str(row.get("m") or "").strip().upper() or None,
                "location": urllib.parse.unquote(str(row.get("g") or "")).strip() or None,
                "ip": str(row.get("a") or "").strip() or None,
                "connected_seconds": conn_s,
                "is_managed": is_managed,
            })
        return {"users": users}

    @router.post("/system/kiwi_kick")
    def post_kiwi_kick(
        kiwi_key: str = Query(...),
        rx: int = Query(..., ge=0, le=7),
    ) -> Dict[str, object]:
        """Kick a specific rx channel on a KiWi SDR using the admin WebSocket."""
        try:
            host_part, port_part = kiwi_key.rsplit(":", 1)
            host_part = host_part.strip()
            port_val = int(port_part.strip())
        except Exception:
            return {"ok": False, "error": "invalid kiwi_key format"}
        script = _find_kick_script()
        if script is None:
            return {"ok": False, "error": "kick script not found"}
        password = _read_admin_password()
        cmd = [
            sys.executable, str(script),
            "--host", host_part,
            "--port", str(port_val),
            "--kick", str(rx),
            "--user", "KiwiScanNOCKick",
            "--take-admin",
        ]
        if password:
            cmd.extend(["--password", password])
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=12.0, check=False)
            return {
                "ok": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": (result.stdout or "").strip()[-500:],
                "stderr": (result.stderr or "").strip()[-500:],
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "kick command timed out"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @router.get("/system/audio_stream")
    def get_system_audio_stream(
        freq_khz: float = Query(..., gt=0),
        mode: str = Query("USB"),
        source_freq_khz: float | None = Query(default=None, gt=0),
        rx: int | None = Query(default=None),
        kiwi_rx: int | None = Query(default=None),
        camp: bool = Query(default=False),
        host: str | None = Query(default=None),
        port: int | None = Query(default=None),
        stream_id: str | None = Query(default=None),
    ) -> StreamingResponse:
        requested_host = str(host or "").strip() or None
        requested_port = int(port) if port is not None else None
        lock = getattr(mgr, "lock", None)
        if hasattr(lock, "acquire") and hasattr(lock, "release"):
            lock.acquire()
            try:
                password = getattr(mgr, "password", None)
            finally:
                lock.release()
        else:
            password = getattr(mgr, "password", None)
        resolved_kiwi_rx, requested_host, requested_port = _resolve_audio_stream_receiver_target(
            receiver_mgr=receiver_mgr,
            requested_rx=rx,
            requested_kiwi_rx=kiwi_rx,
            requested_host=requested_host,
            requested_port=requested_port,
        )
        resolved_source_freq_hz = _resolve_audio_stream_source_freq_hz(
            receiver_mgr=receiver_mgr,
            requested_rx=rx,
            requested_source_freq_hz=float(source_freq_khz) * 1000.0 if source_freq_khz is not None else None,
            requested_host=requested_host,
            requested_port=requested_port,
        )
        stream_host, stream_port = _resolve_audio_stream_endpoint(
            mgr=mgr,
            requested_host=requested_host,
            requested_port=requested_port,
        )
        if not stream_host or stream_port <= 0:
            raise HTTPException(status_code=503, detail="KiwiSDR host is not configured")

        user_suffix = f" RX{int(rx)}" if rx is not None else ""
        stream = stream_kiwi_audio_wav(
            host=stream_host,
            port=stream_port,
            password=password if isinstance(password, str) else None,
            freq_hz=float(freq_khz) * 1000.0,
            mode=mode,
            user=f"KiwiScan Web Audio{user_suffix}",
            source_freq_hz=resolved_source_freq_hz,
            required_rx=None if camp else (int(resolved_kiwi_rx) if resolved_kiwi_rx is not None else None),
            camp_rx=int(resolved_kiwi_rx) if camp and resolved_kiwi_rx is not None else None,
            stream_id=stream_id,
        )
        return StreamingResponse(
            stream,
            media_type="audio/wav",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @router.post("/system/audio_stream/stop")
    def stop_system_audio_stream(stream_id: str = Query(..., min_length=1)) -> Dict[str, object]:
        return {"stopped": bool(stop_kiwi_audio_stream(stream_id))}

    @router.get("/system/adsb_aircraft")
    def get_adsb_aircraft(url: str = Query(...)) -> Dict[str, object]:
        """Proxy an aircraft.json URL to avoid browser CORS restrictions."""
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=4.0) as resp:
                return json.loads(resp.read().decode("utf-8", errors="ignore"))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc))

    return router