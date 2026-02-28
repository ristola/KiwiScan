from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

_lock = threading.Lock()

_DEFAULT_SETTINGS: Dict[str, Any] = {
    "autoScanOnBlock": False,
    "autoScanWspr": False,
    "autoScanOnStartup": False,
    "autoRefreshSchedule": True,
    "bandHopSeconds": 105,
    "wsprStartBand": "10m",
    "quietStart": "22:00",
    "quietEnd": "06:00",
    "alertsEnabled": True,
    "alertThreshold": 12,
    "ssbEnabled": True,
    "ssbSideband": "USB",
    "ssbThresholdDb": 20,
    "ssbAdaptiveThreshold": True,
    "ssbUseKiwiSnr": True,
    "ssbWaitS": 1.0,
    "ssbDwellS": 6.0,
    "ssbTailS": 1.0,
    "ssbStepStrategy": "adaptive",
    "ssbStepKHz": 10.0,
    "headlessEnabled": True,
    "useLaunchd": False,
    "uiThemeMode": "auto",
    "uiThemeNightHour": 21,
    "uiDensity": "normal",
    "scheduleProfiles": {},
}


def _settings_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "outputs" / "automation_settings.json"


def _load_settings() -> Dict[str, Any]:
    path = _settings_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def _save_settings(payload: Dict[str, Any]) -> None:
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _with_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(_DEFAULT_SETTINGS)
    merged.update(payload or {})
    return merged


def make_router() -> APIRouter:
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
    async def put_settings(request: Request) -> Dict[str, str]:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Settings must be a JSON object")
        with _lock:
            _save_settings(_with_defaults(payload))
        return {"status": "ok"}

    return router
