from __future__ import annotations

import json
import re
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
_MAX_ASSIGNMENT_MANAGED_KIWIS = 2
_MANUAL_ASSIGNMENT_BANDS = {"10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"}
_MANUAL_ASSIGNMENT_MODE_ALIASES = {
    "FT8": "FT8",
    "FT4": "FT4",
    "WSPR": "WSPR",
    "USB": "USB",
    "LSB": "LSB",
    "AM": "AM",
    "AMN": "AM",
    "SAM": "AM",
    "FM": "FM",
    "NBFM": "FM",
    "CW": "CW",
    "CWN": "CW",
}


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
        if len(keys) >= _MAX_ASSIGNMENT_MANAGED_KIWIS:
            break
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


def _sanitize_manual_assignment_entry(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean: Dict[str, Any] = {}

    if "enabled" in payload:
        clean["enabled"] = bool(payload.get("enabled"))

    raw_band = str(payload.get("band") or "").strip().lower()
    if raw_band:
        match = re.fullmatch(r"(\d+)\s*[mM]?$", raw_band)
        if match:
            band = f"{match.group(1)}m"
            if band in _MANUAL_ASSIGNMENT_BANDS:
                clean["band"] = band

    freq_khz: float | None = None
    try:
        freq_khz = float(payload.get("freq_khz"))
    except Exception:
        freq_khz = None
    if freq_khz is not None and 0.0 < freq_khz < 1_000_000.0:
        clean["freq_khz"] = round(freq_khz, 3)

    raw_mode = str(payload.get("mode") or payload.get("mode_label") or "").strip().upper().replace("-", "")
    normalized_mode = ""
    if raw_mode in {"SSB", "PHONE"}:
        normalized_mode = "LSB" if freq_khz is not None and freq_khz < 10_000 else "USB"
    else:
        normalized_mode = _MANUAL_ASSIGNMENT_MODE_ALIASES.get(raw_mode, "")
    if normalized_mode:
        clean["mode"] = normalized_mode

    if not any(key in clean for key in ("band", "mode", "freq_khz")) and "enabled" not in clean:
        return {}
    return clean


def _sanitize_manual_assignments(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean: Dict[str, Any] = {}
    for raw_key, raw_entry in payload.items():
        try:
            rx = int(str(raw_key or "").strip())
        except Exception:
            continue
        if rx < 0 or rx > 7:
            continue
        entry = _sanitize_manual_assignment_entry(raw_entry)
        if entry:
            clean[str(rx)] = entry
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
        manual_assignments = _sanitize_manual_assignments(raw_profile.get("manualAssignments"))
        if manual_assignments:
            profile["manualAssignments"] = manual_assignments
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


def _sync_active_kiwi_mode(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep activeKiwi mode maps aligned with the selected receiversMode.

    UI saves include both a global receiversMode and per-Kiwi mode maps.
    If those drift out of sync for the active Kiwi, runtime mode resolution
    can remain stuck in manual and no receivers are assigned in auto/semi.
    """
    if not isinstance(payload, dict):
        return payload

    active_key = str(payload.get("activeKiwiKey") or "").strip()
    receivers_mode = str(payload.get("receiversMode") or "").strip().lower()
    if not active_key or receivers_mode not in _RECEIVERS_MODES:
        return payload
    assignment_managed_keys = _configured_kiwi_keys_from_config()
    if assignment_managed_keys is not None and active_key not in assignment_managed_keys:
        return payload

    kiwi_modes = payload.get("kiwiModes")
    if not isinstance(kiwi_modes, dict):
        kiwi_modes = {}
        payload["kiwiModes"] = kiwi_modes
    existing_mode = str(kiwi_modes.get(active_key) or "").strip().lower()
    if existing_mode not in _RECEIVERS_MODES:
        kiwi_modes[active_key] = receivers_mode

    kiwi_profiles = payload.get("kiwiProfiles")
    if not isinstance(kiwi_profiles, dict):
        return payload
    profile = kiwi_profiles.get(active_key)
    if not isinstance(profile, dict):
        profile = {}
        kiwi_profiles[active_key] = profile
    profile_mode = str(profile.get("receiversMode") or "").strip().lower()
    if profile_mode not in _RECEIVERS_MODES:
        profile["receiversMode"] = str(kiwi_modes.get(active_key) or receivers_mode)
    return payload


def _effective_receivers_mode(payload: Dict[str, Any]) -> str:
    active_key = str(payload.get("activeKiwiKey") or "").strip()
    if active_key:
        kiwi_modes = payload.get("kiwiModes")
        if isinstance(kiwi_modes, dict):
            mode = str(kiwi_modes.get(active_key) or "").strip().lower()
            if mode in _RECEIVERS_MODES:
                return mode
    mode = str(payload.get("receiversMode") or "").strip().lower()
    return mode if mode in _RECEIVERS_MODES else "auto"


def make_router(*, auto_set_loop: object | None = None) -> APIRouter:
    """Create router for automation settings endpoints."""

    router = APIRouter()

    @router.get("/automation/settings")
    def get_settings() -> Dict[str, Any]:
        with _lock:
            current = _load_settings()
            merged = _sync_active_kiwi_mode(_with_defaults(current))
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
        manual_button_press = False
        manual_target_key = ""
        merged_for_notify: Dict[str, Any] = {}
        with _lock:
            current = _load_settings()
            current_with_defaults = _sync_active_kiwi_mode(_with_defaults(current))
            merged = _with_defaults(current)
            merged.update(clean_payload)
            merged_final = _sync_active_kiwi_mode(_with_defaults(merged))
            previous_mode = _effective_receivers_mode(current_with_defaults)
            new_mode = _effective_receivers_mode(merged_final)
            manual_button_press = previous_mode != "manual" and new_mode == "manual"
            manual_target_key = str(merged_final.get("activeKiwiKey") or "").strip()
            _save_settings(merged_final)
            merged_for_notify = merged_final
        if auto_set_loop is not None and notify_auto_set:
            try:
                if manual_button_press:
                    clear_manual = getattr(auto_set_loop, "clear_receivers_for_manual_mode", None)
                    if callable(clear_manual):
                        clear_manual(merged_for_notify, kiwi_key=manual_target_key)
                auto_set_loop.notify_settings_changed()  # type: ignore[attr-defined]
            except Exception:
                pass
        return {"status": "ok"}

    return router
