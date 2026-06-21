"""
api/sdr_assignments.py — Unified SDR assignment registry.

Maps sdr_id → mode + rtl_tcp params in config/sdr_assignments.json and delegates
start/stop to the per-mode proxy endpoints.

GET    /api/sdr/assignments              — all assignments with live running status
POST   /api/sdr/assign                   — assign a mode to an SDR slot and start it
DELETE /api/sdr/assignments/{sdr_id}     — stop mode and clear the assignment
POST   /api/sdr/assignments/restore      — re-start all persisted assignments
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Any

import httpx
from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sdr", tags=["sdr-assignments"])

_CONFIG_PATH    = pathlib.Path("config/sdr_assignments.json")
_TIMEOUT        = httpx.Timeout(10.0)
_STATUS_TIMEOUT = httpx.Timeout(4.0)

# KiwiScan base for calling its own proxy endpoints during restore
_KIWISCAN_BASE = os.getenv("KIWISCAN_BASE_URL", "http://127.0.0.1:4020")

# Mode → proxy path prefix on this KiwiScan instance
_MODE_PROXY: dict[str, str] = {
    "tpms":        "/api/tpms",
    "vhf-digital": "/api/vhf-digital",
    "acurite433":  "/api/acurite433",
}


# ── Persistence ───────────────────────────────────────────────────────────────

def _load() -> dict[str, Any]:
    try:
        return json.loads(_CONFIG_PATH.read_text())
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("SDR assignments: load failed: %s", exc)
        return {}


def _save(assignments: dict[str, Any]) -> None:
    try:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(assignments, indent=2))
    except Exception as exc:
        logger.warning("SDR assignments: save failed: %s", exc)


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _proxy_start(mode: str, sdr_id: str, rtl_host: str, rtl_port: int,
                       params: dict | None = None) -> dict:
    prefix = _MODE_PROXY.get(mode)
    if not prefix:
        raise HTTPException(400, f"Unknown mode '{mode}'. Known: {list(_MODE_PROXY)}")
    body: dict = {"sdr_id": sdr_id, "rtl_host": rtl_host, "rtl_port": rtl_port}
    if params:
        body.update(params)
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.post(_KIWISCAN_BASE + prefix + "/start", json=body)
        return r.json()
    except httpx.ConnectError:
        raise HTTPException(503, f"Cannot reach {mode} proxy at {_KIWISCAN_BASE}")
    except Exception as exc:
        raise HTTPException(502, str(exc))


async def _proxy_stop(mode: str) -> dict:
    prefix = _MODE_PROXY.get(mode)
    if not prefix:
        return {"ok": False, "error": f"unknown mode {mode}"}
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.post(_KIWISCAN_BASE + prefix + "/stop")
        return r.json()
    except Exception as exc:
        logger.warning("SDR assignments: stop %s failed: %s", mode, exc)
        return {"ok": False, "error": str(exc)}


async def _proxy_status(mode: str) -> dict | None:
    prefix = _MODE_PROXY.get(mode)
    if not prefix:
        return None
    try:
        async with httpx.AsyncClient(timeout=_STATUS_TIMEOUT) as c:
            r = await c.get(_KIWISCAN_BASE + prefix + "/status")
        return r.json()
    except Exception:
        return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/assignments")
async def get_assignments():
    """Return all persisted assignments enriched with live running status."""
    assignments = _load()
    result: dict[str, Any] = {}
    for sdr_id, rec in assignments.items():
        mode   = rec.get("mode", "")
        status = await _proxy_status(mode)
        result[sdr_id] = {
            **rec,
            "running": bool(status and status.get("running")),
        }
    return {"assignments": result}


@router.put("/assignments")
async def register_assignment(body: dict = Body(...)):
    """Persist an already-running assignment without starting the mode.

    Call this from the dashboard immediately after a successful mode start so the
    assignment survives page refresh and KiwiScan restarts.
    """
    sdr_id   = (body.get("sdr_id") or "").lower()
    mode     = (body.get("mode") or "").lower()
    if not sdr_id or not mode:
        raise HTTPException(400, "sdr_id and mode required")
    assignments = _load()
    assignments[sdr_id] = {
        "mode":     mode,
        "sdr_id":   sdr_id,
        "rtl_host": body.get("rtl_host") or "",
        "rtl_port": int(body.get("rtl_port") or 7373),
        "params":   body.get("params") or {},
    }
    _save(assignments)
    return {"ok": True, "sdr_id": sdr_id, "mode": mode}


@router.post("/assign")
async def assign_sdr(body: dict = Body(...)):
    """Assign a mode to an SDR slot and start the container."""
    sdr_id   = (body.get("sdr_id") or "").lower()
    mode     = (body.get("mode") or "").lower()
    rtl_host = body.get("rtl_host") or ""
    rtl_port = int(body.get("rtl_port") or 7373)
    params   = {k: v for k, v in body.items()
                if k not in ("sdr_id", "mode", "rtl_host", "rtl_port")}

    if not sdr_id:
        raise HTTPException(400, "sdr_id is required")
    if not mode:
        raise HTTPException(400, "mode is required")
    if mode not in _MODE_PROXY:
        raise HTTPException(400, f"Unknown mode '{mode}'. Known: {list(_MODE_PROXY)}")

    # Stop any previous mode on this SDR slot if it changed
    assignments = _load()
    existing = assignments.get(sdr_id)
    if existing and existing.get("mode") and existing["mode"] != mode:
        await _proxy_stop(existing["mode"])

    # Start the requested mode
    await _proxy_start(mode, sdr_id, rtl_host, rtl_port, params)

    assignments[sdr_id] = {
        "mode":     mode,
        "sdr_id":   sdr_id,
        "rtl_host": rtl_host,
        "rtl_port": rtl_port,
        "params":   params,
    }
    _save(assignments)
    return {"ok": True, "sdr_id": sdr_id, "mode": mode}


@router.delete("/assignments/{sdr_id}")
async def remove_assignment(sdr_id: str):
    """Stop the assigned mode and remove the assignment record.

    Returns 200 even if no record exists (idempotent deregister).
    """
    sdr_id = sdr_id.lower()
    assignments = _load()
    rec = assignments.pop(sdr_id, None)
    if rec is not None:
        await _proxy_stop(rec.get("mode", ""))
        _save(assignments)
    return {"ok": True, "sdr_id": sdr_id}


@router.post("/assignments/restore")
async def restore_assignments_endpoint():
    """Re-start all persisted assignments. Also called on KiwiScan startup."""
    results = await restore_all_assignments()
    return {"restored": results}


# ── Startup restore (called from app_lifecycle.py) ────────────────────────────

async def restore_all_assignments() -> dict[str, Any]:
    """Re-start all persisted SDR assignments. Safe to call after server is ready."""
    assignments = _load()
    results: dict[str, Any] = {}
    for sdr_id, rec in assignments.items():
        mode     = rec.get("mode", "")
        rtl_host = rec.get("rtl_host", "")
        rtl_port = int(rec.get("rtl_port") or 7373)
        params   = rec.get("params") or {}
        try:
            resp = await _proxy_start(mode, sdr_id, rtl_host, rtl_port, params)
            results[sdr_id] = {"ok": True, "mode": mode, "response": resp}
            logger.info("SDR assignment restored: %s → %s@%s:%s", sdr_id, mode, rtl_host, rtl_port)
        except Exception as exc:
            results[sdr_id] = {"ok": False, "mode": mode, "error": str(exc)}
            logger.warning("SDR assignment restore failed: %s/%s: %s", sdr_id, mode, exc)
    return results
