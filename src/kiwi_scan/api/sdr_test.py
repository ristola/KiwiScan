"""
api/sdr_test.py — SDR Hardware Test Suite router.

GET  /api/sdr-test/run     Server-Sent Events stream of test results.
                           Phase 1: parallel TCP/network probe of all 12 endpoints.
                           Phase 2-N: hardware tests streamed from the SDR Test Agent
                                      on sigmon (USB-level: PLL, overflow, IQ, EEPROM).
                           Phase N+1: signal-strength measurement via rtl_tcp
                                      (162.4 MHz NOAA WX, excl. ADS-B dongle).

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
  {"type": "test_result",  "serial": str, "test": str, "pass": bool|null, "detail": str,
                            "meta": {"dbfs": float|null, "freq_hz": int}}
  {"type": "device_done",  "serial": str, "pass": bool|null}
  {"type": "done",         "passed": int, "failed": int, "skipped": int}
  {"type": "error",        "msg": str}
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import struct
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sdr-test", tags=["sdr-test"])

_AGENT_URL = os.environ.get("SDR_TEST_AGENT_URL", "http://10.13.73.195:7380")

# NOAA WX Channel 2 — always-on, nationwide, strong reference signal.
SIGNAL_FREQ_HZ = 162_400_000

# Physical port → IP/service mapping (mirrors PORT_LABELS on sigmon).
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
            data = await asyncio.wait_for(reader.read(12), timeout=2.0)
            ok = len(data) >= 4 and data[:4] == _RTL0_MAGIC
            detail = "" if ok else "bad magic {}".format(data[:4].hex())
        else:
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


async def _measure_signal(target: dict, freq_hz: int = SIGNAL_FREQ_HZ,
                           n_samples: int = 32768) -> dict:
    """
    Connect to rtl_tcp, tune to freq_hz, read IQ samples, return dBFS power level.

    dBFS is computed as:
        10 * log10(mean(I² + Q²) / 2) - 10 * log10(128²)
    where 128 is the full-scale amplitude for uint8-centered IQ.
    """
    if target.get("proto") != "rtltcp":
        return {"pass": None, "dbfs": None, "detail": "not rtl_tcp — skipped"}

    ip = target["ip"]
    port = target["tcp_port"]
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=3.0
        )

        # 12-byte rtl_tcp banner: magic(4) + tuner_type(4) + tuner_gain_count(4)
        header = await asyncio.wait_for(reader.read(12), timeout=2.0)
        if len(header) < 4 or header[:4] != _RTL0_MAGIC:
            writer.close()
            return {"pass": False, "dbfs": None, "detail": "bad rtl_tcp magic"}

        # 5-byte commands: cmd(1) + value big-endian uint32(4)
        writer.write(struct.pack(">BI", 0x02, 2_048_000))  # sample rate: 2.048 MSPS
        writer.write(struct.pack(">BI", 0x03, 0))           # gain mode: auto
        writer.write(struct.pack(">BI", 0x01, freq_hz))     # center frequency
        await writer.drain()

        # Settle time for PLL to lock and AGC to stabilize
        await asyncio.sleep(0.20)

        # Read n_samples IQ pairs (2 bytes per sample: uint8 I, uint8 Q)
        n_bytes = n_samples * 2
        buf = bytearray()
        deadline = time.monotonic() + 4.0
        while len(buf) < n_bytes:
            left = deadline - time.monotonic()
            if left <= 0:
                break
            chunk = await asyncio.wait_for(
                reader.read(min(8192, n_bytes - len(buf))),
                timeout=min(left, 2.0),
            )
            if not chunk:
                break
            buf.extend(chunk)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        n_got = len(buf) // 2
        if n_got < 256:
            return {"pass": False, "dbfs": None, "detail": f"too few samples ({n_got})"}

        # Compute mean IQ power and convert to dBFS
        raw = bytes(buf[: n_got * 2])
        if _HAS_NUMPY:
            arr = np.frombuffer(raw, dtype=np.uint8).astype(np.float32) - 128.0
            mean_pwr = float(np.mean(arr ** 2))
        else:
            mean_pwr = sum((b - 128) ** 2 for b in raw) / len(raw)

        # Full-scale single-component power = 128² = 16384
        dbfs = 10.0 * math.log10(max(mean_pwr, 1e-10)) - 10.0 * math.log10(16384.0)
        dbfs = round(dbfs, 2)
        return {
            "pass": True,
            "dbfs": dbfs,
            "detail": f"{dbfs:.1f} dBFS @ {freq_hz / 1e6:.3f} MHz",
        }

    except asyncio.TimeoutError:
        return {"pass": False, "dbfs": None, "detail": "timeout"}
    except ConnectionRefusedError:
        return {"pass": False, "dbfs": None, "detail": "connection refused"}
    except OSError as exc:
        return {"pass": False, "dbfs": None, "detail": str(exc)[:80]}
    except Exception as exc:
        logger.debug("_measure_signal %s:%s — %s", target["ip"], target["tcp_port"], exc)
        return {"pass": False, "dbfs": None, "detail": str(exc)[:80]}


@router.get("/run")
async def sdr_test_run(request: Request):
    """Stream the full SDR test suite as Server-Sent Events."""

    async def generate():
        # ── Phase 1: Network / Service (parallel TCP probes) ─────────────
        yield _sse({"type": "phase", "id": "network", "label": "Network / Service"})

        net_results = await asyncio.gather(*[_probe_target(t) for t in _NETWORK_TARGETS])
        for target, result in zip(_NETWORK_TARGETS, net_results):
            yield _sse({"type": "net_result", **target, **result})

        if await request.is_disconnected():
            return

        # ── Phase 2-N: Hardware tests from sigmon agent ───────────────────
        # Hold the agent's "done" event so we can append the signal phase first.
        agent_done: dict = {}
        agent_ok = True
        try:
            timeout = httpx.Timeout(connect=5.0, read=300.0, write=10.0, pool=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", f"{_AGENT_URL}/test") as resp:
                    if resp.status_code != 200:
                        yield _sse({"type": "error",
                                    "msg": f"Agent returned HTTP {resp.status_code}"})
                        agent_ok = False
                    else:
                        async for line in resp.aiter_lines():
                            if await request.is_disconnected():
                                return
                            if not line.startswith("data: "):
                                continue
                            try:
                                ev = json.loads(line[6:])
                                if ev.get("type") == "done":
                                    agent_done = ev   # intercept — emit after signal phase
                                    continue
                            except Exception:
                                pass
                            yield line + "\n\n"

        except httpx.ConnectError:
            yield _sse({"type": "error",
                        "msg": f"SDR Test Agent unreachable at {_AGENT_URL} — "
                               "is sdr_test_agent.py running on sigmon?"})
            agent_ok = False
        except Exception as exc:
            logger.exception("sdr_test proxy error")
            yield _sse({"type": "error", "msg": str(exc)})
            agent_ok = False

        if await request.is_disconnected():
            return

        # ── Phase N+1: Signal strength (local, parallel via rtl_tcp) ─────
        yield _sse({"type": "phase", "id": "signal", "label": "Signal Strength"})

        sig_targets = [t for t in _NETWORK_TARGETS if t["proto"] == "rtltcp"]
        sig_results = await asyncio.gather(*[_measure_signal(t) for t in sig_targets])

        a_pass = int(agent_done.get("passed", 0))
        a_fail = int(agent_done.get("failed", 0)) + (0 if agent_ok else 1)
        a_skip = int(agent_done.get("skipped", 0))

        for target, sig in zip(sig_targets, sig_results):
            yield _sse({"type": "test_start", "serial": target["serial"], "test": "signal"})
            p = sig["pass"]
            if p is True:
                a_pass += 1
            elif p is False:
                a_fail += 1
            else:
                a_skip += 1
            yield _sse({
                "type": "test_result",
                "serial": target["serial"],
                "test": "signal",
                "pass": p,
                "detail": sig["detail"],
                "meta": {"dbfs": sig.get("dbfs"), "freq_hz": SIGNAL_FREQ_HZ},
            })

        yield _sse({"type": "done", "passed": a_pass, "failed": a_fail, "skipped": a_skip})

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
