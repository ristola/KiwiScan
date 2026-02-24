from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

_lock = threading.Lock()


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


def make_router() -> APIRouter:
    """Create router for automation settings endpoints."""

    router = APIRouter()

    @router.get("/automation/settings")
    def get_settings() -> Dict[str, Any]:
        with _lock:
            return _load_settings()

    @router.post("/automation/settings")
    async def put_settings(request: Request) -> Dict[str, str]:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Settings must be a JSON object")
        with _lock:
            _save_settings(payload)
        return {"status": "ok"}

    return router
