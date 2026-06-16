"""
api/vhf_digital.py — Proxy router for the VHF Digital Monitor container.

The browser calls /api/vhf-digital/* (same-origin, no CORS).
This module forwards each request to the container's HTTP API.

The container URL is resolved in order:
  1. Last URL set via POST /start body field "container_url"
  2. VHF_DIGITAL_URL environment variable
  3. Hard-coded default (http://10.13.73.192:4025)

GET    /api/vhf-digital/status
POST   /api/vhf-digital/start
POST   /api/vhf-digital/stop
GET    /api/vhf-digital/decodes
DELETE /api/vhf-digital/decodes
GET    /api/vhf-digital/defaults
GET    /api/vhf-digital/config
"""

from __future__ import annotations

import json
import logging
import os
import pathlib

import httpx
from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/vhf-digital", tags=["vhf-digital"])

_DEFAULT_URL  = "http://10.13.73.192:4025"
_TIMEOUT      = httpx.Timeout(10.0)
_CONFIG_PATH  = pathlib.Path("config/vhf_digital.json")

_container_url: str | None = None
_sdr_id: str | None = None


def _load_persisted() -> None:
    global _container_url, _sdr_id
    try:
        data = json.loads(_CONFIG_PATH.read_text())
        _container_url = data.get("container_url") or None
        _sdr_id        = data.get("sdr_id") or None
        if _container_url:
            logger.info("VHF Digital: restored container_url=%s sdr_id=%s", _container_url, _sdr_id)
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("VHF Digital: could not load config: %s", exc)


def _persist() -> None:
    try:
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps({
            "container_url": _container_url or _DEFAULT_URL,
            "sdr_id":        _sdr_id,
        }, indent=2))
    except Exception as exc:
        logger.warning("VHF Digital: could not save config: %s", exc)


_load_persisted()


def _base() -> str:
    return (_container_url or os.getenv("VHF_DIGITAL_URL", _DEFAULT_URL)).rstrip("/")


async def _get(path: str, params: dict | None = None) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.get(_base() + path, params=params)
        return JSONResponse(content=r.json(), status_code=r.status_code)
    except httpx.ConnectError:
        raise HTTPException(503, f"VHF Digital container unreachable at {_base()}")
    except Exception as exc:
        raise HTTPException(502, str(exc))


async def _post(path: str, body: dict | None = None) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.post(_base() + path, json=body or {})
        return JSONResponse(content=r.json(), status_code=r.status_code)
    except httpx.ConnectError:
        raise HTTPException(503, f"VHF Digital container unreachable at {_base()}")
    except Exception as exc:
        raise HTTPException(502, str(exc))


async def _delete(path: str) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.delete(_base() + path)
        return JSONResponse(content=r.json(), status_code=r.status_code)
    except httpx.ConnectError:
        raise HTTPException(503, f"VHF Digital container unreachable at {_base()}")
    except Exception as exc:
        raise HTTPException(502, str(exc))


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/config")
async def vhf_get_config():
    """Return persisted VHF Digital assignment (container URL + assigned SDR id)."""
    return {"container_url": _base(), "sdr_id": _sdr_id}


@router.get("/status")
async def vhf_status():
    return await _get("/status")


@router.get("/defaults")
async def vhf_defaults():
    return await _get("/defaults")


@router.post("/start")
async def vhf_start(body: dict = Body(...)):
    global _container_url, _sdr_id
    if url := body.pop("container_url", None):
        _container_url = url.rstrip("/")
        logger.info("VHF Digital container URL set to %s", _container_url)
    if sid := body.pop("sdr_id", None):
        _sdr_id = sid.lower()
    _persist()
    return await _post("/start", body)


@router.post("/stop")
async def vhf_stop():
    global _sdr_id
    _sdr_id = None
    _persist()
    return await _post("/stop")


@router.get("/decodes")
async def vhf_decodes(
    limit: int = Query(200, le=2000),
    mode:  str | None = Query(None),
):
    params: dict = {"limit": limit}
    if mode:
        params["mode"] = mode
    return await _get("/decodes", params)


@router.delete("/decodes")
async def vhf_clear_decodes():
    return await _delete("/decodes")
