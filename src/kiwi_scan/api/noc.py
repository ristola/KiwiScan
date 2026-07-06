"""
api/noc.py — NOC dashboard persistent SDR assignments.

Saves which SDR is assigned to which function so assignments survive
page reloads and server restarts without relying on browser localStorage.

Config file: config/noc_assignments.json

Endpoints:
  GET  /api/noc/assignments        — return all saved assignments
  POST /api/noc/assignments        — merge-update assignments (null value clears a key)

Assignment change hooks
───────────────────────
Services that need to react to SDR re-assignments register an async callable
via ``register_assignment_hook(fn)``.  ``fn`` receives
``(changed_keys: set[str], new_state: dict)`` and is awaited as an asyncio task
whenever the POST endpoint saves a change.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib

from fastapi import APIRouter, Body

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/noc", tags=["noc"])

_CONFIG_PATH = pathlib.Path("config/noc_assignments.json")
_state: dict = {}

# Hooks: async callables(changed_keys: set[str], state: dict)
_hooks: list = []


def register_assignment_hook(fn) -> None:
    """Register an async callable invoked after each assignment change."""
    _hooks.append(fn)


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
async def set_noc_assignments(body: dict = Body(...)):
    """Merge-update NOC SDR function assignments. Pass null for a key to clear it."""
    global _state
    changed: set[str] = set()
    for key, val in body.items():
        if val is None:
            if key in _state:
                _state.pop(key)
                changed.add(key)
        else:
            if _state.get(key) != val:
                _state[key] = val
                changed.add(key)
    _save()
    if changed:
        snapshot = dict(_state)
        for hook in _hooks:
            try:
                asyncio.create_task(hook(changed, snapshot))
            except Exception as exc:
                logger.warning("NOC hook error: %s", exc)
    return {"ok": True}
