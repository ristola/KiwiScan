from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

_lock = threading.Lock()

_DEPRECATED_SETTINGS_KEYS = {
    "autoScanMode",
    "autoScanWspr",
    "alertsEnabled",
    "alertThreshold",
    "bandHopMinutes",
    "bandHopSeconds",
    "quietEnd",
    "quietStart",
    "ssbAdaptiveThreshold",
    "ssbAdaptiveThresholdByBand",
    "ssbDwellS",
    "ssbEnabled",
    "ssbSideband",
    "ssbStepKHz",
    "ssbStepStrategy",
    "ssbTailS",
    "ssbThresholdDb",
    "ssbUseKiwiSnr",
    "ssbWaitS",
    "wsprHopState",
    "wsprStartBand",
}

_DEFAULT_SETTINGS: Dict[str, Any] = {
    "activeKiwiKey": "",
    "autoScanOnBlock": False,
    "autoScanOnStartup": False,
    "autoRefreshSchedule": True,
    "fixedModeEnabled": True,
    "headlessEnabled": True,
    "kiwiModes": {},
    "kiwiProfiles": {},
    "receiversMode": "auto",
    "useLaunchd": False,
    "uiThemeMode": "auto",
    "uiThemeNightHour": 21,
    "uiThemeHourOffset": 0,
    "uiDensity": "normal",
    "scheduleProfiles": {},
}

_OVERVIEW_TOPIC_KEYS = (
    "status",
    "assignments",
    "faults",
    "messages",
    "map",
    "active-receivers",
    "receiver-scan",
)

_RECEIVERS_MODES = {"auto", "semi", "manual", "scan"}


def _settings_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "outputs" / "automation_settings.json"


def _config_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "outputs" / "config.json"


def _configured_kiwi_keys_from_config() -> set[str] | None:
    path = _config_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw_kiwis = data.get("kiwisdrs") if isinstance(data, dict) else None
    if raw_kiwis is None:
        return None
    if not isinstance(raw_kiwis, list):
        return set()
    keys: set[str] = set()
    for entry in raw_kiwis:
        if not isinstance(entry, dict):
            continue
        host = str(entry.get("host") or "").strip()
        if not host:
            continue
        try:
            port = int(entry.get("port") or 8073)
        except Exception:
            port = 8073
        keys.add(f"{host}:{port}")
    return keys


def _load_settings() -> Dict[str, Any]:
    path = _settings_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return _sanitize_settings(data)
    except Exception:
        return {}
    return {}


def _save_settings(payload: Dict[str, Any]) -> None:
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sanitize_schedule_profiles(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    ft8 = payload.get("ft8")
    if not isinstance(ft8, dict):
        return {}
    return {"ft8": ft8}


def _sanitize_overview_topics(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean: Dict[str, Any] = {}
    for key in _OVERVIEW_TOPIC_KEYS:
        if key in payload:
            clean[key] = bool(payload.get(key))
    order = payload.get("order")
    if isinstance(order, list):
        seen: set[str] = set()
        normalized_order: list[str] = []
        for item in order:
            key = str(item or "").strip()
            if not key or key not in _OVERVIEW_TOPIC_KEYS or key in seen:
                continue
            seen.add(key)
            normalized_order.append(key)
        if normalized_order:
            clean["order"] = normalized_order
    return clean


def _sanitize_kiwi_profiles(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean: Dict[str, Any] = {}
    for raw_key, raw_profile in payload.items():
        key = str(raw_key or "").strip()
        if not key or not isinstance(raw_profile, dict):
            continue
        profile: Dict[str, Any] = {}
        receivers_mode = str(raw_profile.get("receiversMode") or "").strip().lower()
        if receivers_mode in _RECEIVERS_MODES:
            profile["receiversMode"] = receivers_mode
        overview_topics = _sanitize_overview_topics(raw_profile.get("overviewTopics"))
        if overview_topics:
            profile["overviewTopics"] = overview_topics
        if profile:
            clean[key] = profile
    return clean


def _sanitize_kiwi_modes(payload: object) -> Dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    clean: Dict[str, str] = {}
    for raw_key, raw_mode in payload.items():
        key = str(raw_key or "").strip()
        mode = str(raw_mode or "").strip().lower()
        if key and mode in _RECEIVERS_MODES:
            clean[key] = mode
    return clean


def _sanitize_settings(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean = {key: value for key, value in payload.items() if key not in _DEPRECATED_SETTINGS_KEYS}
    receivers_mode = str(clean.get("receiversMode") or "").strip().lower()
    if receivers_mode in _RECEIVERS_MODES:
        clean["receiversMode"] = receivers_mode
    else:
        clean.pop("receiversMode", None)
    if "activeKiwiKey" in clean:
        clean["activeKiwiKey"] = str(clean.get("activeKiwiKey") or "").strip()
    if "overviewTopics" in clean:
        clean["overviewTopics"] = _sanitize_overview_topics(clean.get("overviewTopics"))
    if "kiwiModes" in clean:
        clean["kiwiModes"] = _sanitize_kiwi_modes(clean.get("kiwiModes"))
    if "kiwiProfiles" in clean:
        clean["kiwiProfiles"] = _sanitize_kiwi_profiles(clean.get("kiwiProfiles"))
    if "scheduleProfiles" in clean:
        clean["scheduleProfiles"] = _sanitize_schedule_profiles(clean.get("scheduleProfiles"))
    configured_kiwi_keys = _configured_kiwi_keys_from_config()
    if configured_kiwi_keys is not None:
        if "activeKiwiKey" in clean and clean.get("activeKiwiKey") not in configured_kiwi_keys:
            clean["activeKiwiKey"] = ""
        if "kiwiModes" in clean and isinstance(clean.get("kiwiModes"), dict):
            clean["kiwiModes"] = {
                key: value
                for key, value in clean["kiwiModes"].items()
                if key in configured_kiwi_keys
            }
        if "kiwiProfiles" in clean and isinstance(clean.get("kiwiProfiles"), dict):
            clean["kiwiProfiles"] = {
                key: value
                for key, value in clean["kiwiProfiles"].items()
                if key in configured_kiwi_keys
            }
    return clean


def _with_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(_DEFAULT_SETTINGS)
    merged.update(_sanitize_settings(payload))
    return merged


def make_router(*, auto_set_loop: object | None = None) -> APIRouter:
    """Create router for automation settings endpoints."""

    router = APIRouter()

    @router.get("/automation/settings")
    def get_settings() -> Dict[str, Any]:
        with _lock:
            current = _load_settings()
            merged = _with_defaults(current)
            if merged != current:
                _save_settings(merged)
            return merged

    @router.post("/automation/settings")
    async def put_settings(request: Request, notify_auto_set: bool = True) -> Dict[str, str]:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Settings must be a JSON object")
        clean_payload = _sanitize_settings(payload)
        with _lock:
            current = _load_settings()
            merged = _with_defaults(current)
            merged.update(clean_payload)
            _save_settings(_with_defaults(merged))
        if auto_set_loop is not None and notify_auto_set:
            try:
                auto_set_loop.notify_settings_changed()  # type: ignore[attr-defined]
            except Exception:
                pass
        return {"status": "ok"}

    return router
