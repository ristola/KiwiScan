"""
api/noc.py — NOC dashboard persistent SDR assignments.

Saves which SDR is assigned to which function so assignments survive
page reloads and server restarts without relying on browser localStorage.

Config file: config/noc_assignments.json

Endpoints:
  GET  /api/noc/assignments        — return all saved assignments
  POST /api/noc/assignments        — merge-update assignments (null value clears a key)
"""

from __future__ import annotations

import json
import logging
import pathlib

from fastapi import APIRouter, Body

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/noc", tags=["noc"])

_CONFIG_PATH = pathlib.Path("config/noc_assignments.json")
_state: dict = {}


def _load() -> None:
    global _state
    try:
        _state = json.loads(_CONFIG_PATH.read_text())
        logger.info("NOC: loaded SDR assignments from %s", _CONFIG_PATH)
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("NOC: could not load assignments: %s", exc)


def _save() -> None:
    try:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(_state, indent=2))
    except Exception as exc:
        logger.warning("NOC: could not save assignments: %s", exc)


_load()


@router.get("/assignments")
def get_noc_assignments():
    """Return current NOC SDR function assignments."""
    return dict(_state)


@router.post("/assignments")
def set_noc_assignments(body: dict = Body(...)):
    """Merge-update NOC SDR function assignments. Pass null for a key to clear it."""
    global _state
    for key, val in body.items():
        if val is None:
            _state.pop(key, None)
        else:
            _state[key] = val
    _save()
    return {"ok": True}
