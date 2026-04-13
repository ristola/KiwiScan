from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter


def _merge_receiver_scan_health(summary: dict[str, Any], receiver_scan: object | None) -> dict[str, Any]:
    if receiver_scan is None or not hasattr(receiver_scan, "health_channels"):
        return summary
    try:
        scan_channels = receiver_scan.health_channels()  # type: ignore[attr-defined]
    except Exception:
        return summary
    if not isinstance(scan_channels, dict) or not scan_channels:
        return summary

    result = dict(summary)
    channels_raw = result.get("channels")
    channels = {
        str(key): dict(value)
        for key, value in (channels_raw.items() if isinstance(channels_raw, dict) else [])
        if isinstance(value, dict)
    }
    reason_counts = dict(result.get("reason_counts") or {}) if isinstance(result.get("reason_counts"), dict) else {}

    scan_active = 0
    scan_unstable = 0
    scan_stalled = 0
    scan_silent = 0
    scan_warn = 0
    latest_scan_update = None
    for rx, channel in scan_channels.items():
        if not isinstance(channel, dict):
            continue
        normalized = dict(channel)
        channels[str(rx)] = normalized
        if bool(normalized.get("active")):
            scan_active += 1
        if bool(normalized.get("is_unstable")):
            scan_unstable += 1
        if bool(normalized.get("is_stalled")):
            scan_stalled += 1
        if bool(normalized.get("is_silent")):
            scan_silent += 1
        if bool(normalized.get("is_no_decode_warning")):
            scan_warn += 1
        reason = str(normalized.get("last_reason") or "").strip()
        if reason and str(normalized.get("status_level") or "") in {"fault", "warning"}:
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
        try:
            ts = float(normalized.get("last_updated_unix"))
        except Exception:
            ts = None
        if ts is not None:
            latest_scan_update = ts if latest_scan_update is None else max(latest_scan_update, ts)

    active_total = int(result.get("active_receivers", 0) or 0) + scan_active
    unstable_total = int(result.get("unstable_receivers", 0) or 0) + scan_unstable
    stalled_total = int(result.get("stalled_receivers", 0) or 0) + scan_stalled
    silent_total = int(result.get("silent_receivers", 0) or 0) + scan_silent
    warn_total = int(result.get("no_decode_warning_receivers", 0) or 0) + scan_warn

    overall = "healthy"
    if unstable_total > 0:
        overall = "degraded"
    elif silent_total > 0 or warn_total > 0:
        overall = "quiet"
    if active_total == 0:
        overall = "idle"

    stale_seconds = result.get("health_stale_seconds")
    if active_total > 0 and unstable_total <= 0 and silent_total <= 0 and warn_total <= 0:
        stale_seconds = 0.0
    elif latest_scan_update is not None:
        scan_stale = max(0.0, time.time() - latest_scan_update)
        try:
            base_stale = float(stale_seconds)
        except Exception:
            base_stale = None
        stale_seconds = scan_stale if base_stale is None else max(base_stale, scan_stale)

    result["channels"] = channels
    result["reason_counts"] = reason_counts
    result["active_receivers"] = active_total
    result["unstable_receivers"] = unstable_total
    result["stalled_receivers"] = stalled_total
    result["silent_receivers"] = silent_total
    result["no_decode_warning_receivers"] = warn_total
    result["overall"] = overall
    result["health_stale_seconds"] = stale_seconds
    return result


def make_router(*, receiver_mgr: object, receiver_scan: object | None = None) -> APIRouter:
    """Create router for receiver health summary endpoints."""

    router = APIRouter()

    @router.get("/health/rx")
    def get_receiver_health() -> dict:
        if hasattr(receiver_mgr, "health_summary"):
            return _merge_receiver_scan_health(receiver_mgr.health_summary(), receiver_scan)
        return _merge_receiver_scan_health({
            "overall": "unknown",
            "active_receivers": 0,
            "unstable_receivers": 0,
            "stalled_receivers": 0,
            "silent_receivers": 0,
            "no_decode_warning_receivers": 0,
            "restart_total": 0,
            "reason_counts": {},
            "channels": {},
        }, receiver_scan)

    @router.get("/health/rx/truth")
    def get_receiver_truth() -> dict:
        if hasattr(receiver_mgr, "truth_snapshot"):
            return receiver_mgr.truth_snapshot()
        return {
            "overall": "unknown",
            "host": "",
            "port": 0,
            "channels": {},
            "_from_cache": False,
        }

    return router
