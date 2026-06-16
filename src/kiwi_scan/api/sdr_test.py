"""
api/sdr_test.py — SDR Hardware Test Suite router.

GET  /api/sdr-test/run     Server-Sent Events stream of test results.
                           Runs network/TCP probe tests locally, then
                           proxies the hardware test stream from the
                           SDR Test Agent running on sigmon.

GET  /api/sdr-test/status  JSON: {running: bool, agent_url: str}

Event types (JSON payloads on `data:` lines):
  {"type": "phase",        "id": str,    "label": str}
  {"type": "sys_result",   "test": str,  "label": str, "pass": bool|null, "detail": str}
  {"type": "net_result",   "port_num": int, "serial": str, "ip": str,
                            "tcp_port": int, "label": str,
                            "pass": bool, "ms": float|null, "detail": str}
  {"type": "discovery",    "count": int, "devices": [...]}
  {"type": "device_start", "serial": str, "index": int, "n": int, "of": int}
  {"type": "test_start",   "serial": str, "test": str}
  {"type": "test_result",  "serial": str, "test": str, "pass": bool|null, "detail": str}
  {"type": "device_done",  "serial": str, "pass": bool|null}
  {"type": "done",         "passed": int, "failed": int, "skipped": int}
  {"type": "error",        "msg": str}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sdr-test", tags=["sdr-test"])

_AGENT_URL = os.environ.get("SDR_TEST_AGENT_URL", "http://10.13.73.195:7380")

# Physical port → IP/service mapping (mirrors PORT_LABELS in hubPPPS on sigmon).
# ADS-B dongle uses dump1090 (Beast TCP on 30005); all others use rtl_tcp on 7373.
_NETWORK_TARGETS: list[dict] = [
    {"port_num":  1, "serial": "00001090", "ip": "10.13.73.190", "tcp_port": 30005, "proto": "beast",  "label": "ADS-B 1090"},
    {"port_num":  2, "serial": "00144390", "ip": "10.13.73.202", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
    {"port_num":  3, "serial": "20000897", "ip": "10.13.73.192", "tcp_port": 7373,  "proto": "rtltcp", "label": "Repeater Monitoring"},
    {"port_num":  4, "serial": "00000978", "ip": "10.13.73.193", "tcp_port": 7373,  "proto": "rtltcp", "label": "NOAA Weather"},
    {"port_num":  5, "serial": "00000433", "ip": "10.13.73.194", "tcp_port": 7373,  "proto": "rtltcp", "label": "Acurite 433"},
    {"port_num":  6, "serial": "00162400", "ip": "10.13.73.195", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open (broken PLL)"},
    {"port_num":  7, "serial": "00000315", "ip": "10.13.73.196", "tcp_port": 7373,  "proto": "rtltcp", "label": "TPMS"},
    {"port_num":  8, "serial": "20000145", "ip": "10.13.73.197", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
    {"port_num":  9, "serial": "10000004", "ip": "10.13.73.201", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
    {"port_num": 10, "serial": "10000003", "ip": "10.13.73.200", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
    {"port_num": 11, "serial": "10000002", "ip": "10.13.73.199", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
    {"port_num": 12, "serial": "10000001", "ip": "10.13.73.198", "tcp_port": 7373,  "proto": "rtltcp", "label": "Open"},
]

# RTL-TCP sends a 12-byte dongle_info struct on connect: magic "RTL0" + tuner type + gain count.
_RTL0_MAGIC = b"RTL0"


def _sse(data: dict) -> str:
    return "data: {}\n\n".format(json.dumps(data))


async def _probe_target(target: dict) -> dict:
    """TCP-probe a single SDR service endpoint. Returns pass/ms/detail dict."""
    ip = target["ip"]
    port = target["tcp_port"]
    proto = target["proto"]
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=2.0
        )
        ms = round((time.monotonic() - t0) * 1000, 1)

        if proto == "rtltcp":
            # rtl_tcp sends 12-byte header; first 4 bytes must be "RTL0"
            data = await asyncio.wait_for(reader.read(12), timeout=2.0)
            ok = len(data) >= 4 and data[:4] == _RTL0_MAGIC
            detail = "" if ok else "bad magic {}".format(data[:4].hex())
        else:
            # Beast protocol — just verify TCP connectivity
            ok = True
            detail = "TCP OK"

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        return {"pass": ok, "ms": ms, "detail": detail}

    except asyncio.TimeoutError:
        return {"pass": False, "ms": None, "detail": "timeout"}
    except ConnectionRefusedError:
        return {"pass": False, "ms": None, "detail": "connection refused"}
    except OSError as e:
        return {"pass": False, "ms": None, "detail": str(e)[:60]}


@router.get("/run")
async def sdr_test_run(request: Request):
    """Stream the full SDR test suite as Server-Sent Events."""

    async def generate():
        # ── Phase 1: Network / Service (runs locally, in parallel) ───────────
        yield _sse({"type": "phase", "id": "network", "label": "Network / Service"})

        results = await asyncio.gather(*[_probe_target(t) for t in _NETWORK_TARGETS])
        for target, result in zip(_NETWORK_TARGETS, results):
            yield _sse({"type": "net_result", **target, **result})

        # ── Check client still connected ──────────────────────────────────────
        if await request.is_disconnected():
            return

        # ── Phase 2-N: Hardware tests streamed from sigmon agent ──────────────
        try:
            timeout = httpx.Timeout(connect=5.0, read=300.0, write=10.0, pool=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", f"{_AGENT_URL}/test") as resp:
                    if resp.status_code != 200:
                        yield _sse({"type": "error",
                                    "msg": f"Agent returned HTTP {resp.status_code}"})
                        yield _sse({"type": "done", "passed": 0, "failed": 1, "skipped": 0})
                        return

                    async for line in resp.aiter_lines():
                        if await request.is_disconnected():
                            return
                        if line.startswith("data: "):
                            yield line + "\n\n"

        except httpx.ConnectError:
            yield _sse({"type": "error",
                        "msg": f"SDR Test Agent unreachable at {_AGENT_URL} — "
                               "is sdr_test_agent.py running on sigmon?"})
            yield _sse({"type": "done", "passed": 0, "failed": 1, "skipped": 0})
        except Exception as exc:
            logger.exception("sdr_test proxy error")
            yield _sse({"type": "error", "msg": str(exc)})
            yield _sse({"type": "done", "passed": 0, "failed": 1, "skipped": 0})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/status")
async def sdr_test_status():
    """Check whether a test is currently running on the agent."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
            r = await client.get(f"{_AGENT_URL}/status")
            return JSONResponse({"running": r.json().get("running", False),
                                 "agent_url": _AGENT_URL, "agent_ok": True})
    except Exception:
        return JSONResponse({"running": False, "agent_url": _AGENT_URL, "agent_ok": False})
