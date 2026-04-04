from __future__ import annotations

import json
import logging
import time
import asyncio
import threading
import os
import shutil
from collections import deque
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from pathlib import Path

from .discovery import FT8_WATERHOLES
from .receiver_manager import ReceiverManager
from .band_scanner import BandScanner
from .discovery_manager import DiscoveryManager
from .api.backup import router as backup_router
from .api.ui import mount_static, router as ui_router
from .api.decodes import (
    decode_callback as decodes_callback,
    prune_decode_buffer as prune_decodes,
    publish_decode,
    set_loop as set_decodes_loop,
    router as decodes_router,
)
from .api.decodes_status import make_router as make_decodes_status_router
from .api.band_scan import make_router as make_band_scan_router
from .api.config import make_router as make_config_router
from .api.status import compute_s_metrics, make_router as make_status_router
from .api.schedule import make_router as make_schedule_router
from .api.auto_set import make_router as make_auto_set_router
from .api.ws_status import broadcast_status, make_router as make_ws_status_router
from .api.calibrate import make_router as make_calibrate_router
from .api.ssb_scan_hits import router as ssb_scan_hits_router
from .api.rx_monitor import make_router as make_rx_monitor_router
from .api.admin import make_router as make_admin_router
from .api.automation import make_router as make_automation_router
from .api.metrics import make_router as make_metrics_router
from .api.health import make_router as make_health_router
from .api.system_info import make_router as make_system_info_router
from .rx_monitor import RxMonitor
from .auto_set_loop import AutoSetLoop
from .smart_scheduler import SmartScheduler
from .api.smart_scheduler import make_router as make_smart_scheduler_router
from .app_lifecycle import register_lifecycle

# Configure logging to output to console (stderr) with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="KiwiSDR Scanner")

_api_metrics_lock = threading.Lock()
_api_latency_ms: deque[float] = deque(maxlen=2000)
_api_request_total = 0
_api_error_total = 0


def _get_api_metrics() -> Dict[str, float | int]:
    with _api_metrics_lock:
        samples = list(_api_latency_ms)
        req_total = int(_api_request_total)
        err_total = int(_api_error_total)
    if not samples:
        return {
            "request_total": req_total,
            "error_total": err_total,
            "latency_avg_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_samples": 0,
        }
    sorted_samples = sorted(samples)
    idx = max(0, min(len(sorted_samples) - 1, int(0.95 * (len(sorted_samples) - 1))))
    avg = sum(samples) / float(len(samples))
    return {
        "request_total": req_total,
        "error_total": err_total,
        "latency_avg_ms": float(avg),
        "latency_p95_ms": float(sorted_samples[idx]),
        "latency_samples": len(samples),
    }


@app.middleware("http")
async def _api_latency_middleware(request: Request, call_next):
    global _api_request_total, _api_error_total
    start = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = int(getattr(response, "status_code", 200) or 200)
        return response
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with _api_metrics_lock:
            _api_request_total += 1
            if status >= 500:
                _api_error_total += 1
            _api_latency_ms.append(float(elapsed_ms))

mount_static(app)
app.include_router(ui_router)
app.include_router(backup_router)
app.include_router(decodes_router)
app.include_router(ssb_scan_hits_router)

BAND_ORDER: List[str] = ["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"]
BAND_FREQS_HZ: Dict[str, float] = {
    "10m": 28.074e6,
    "12m": 24.915e6,
    "15m": 21.074e6,
    "17m": 18.100e6,
    "20m": 14.074e6,
    "30m": 10.136e6,
    "40m": 7.074e6,
    "60m": 5.357e6,
    "80m": 3.573e6,
    "160m": 1.840e6,
}
BAND_FT4_FREQS_HZ: Dict[str, float] = {
    "10m": 28.180e6,
    "12m": 24.919e6,
    "15m": 21.140e6,
    "17m": 18.104e6,
    "20m": 14.080e6,
    "30m": 10.140e6,
    "40m": 7.0475e6,
    "60m": 5.357e6,
    "80m": 3.575e6,
    "160m": 1.843e6,
}
BAND_SSB_FREQS_HZ: Dict[str, float] = {
    "10m": 28.500e6,
    "12m": 24.950e6,
    "15m": 21.200e6,
    "17m": 18.110e6,
    "20m": 14.150e6,
    "40m": 7.125e6,
    "80m": 3.600e6,
    "160m": 1.843e6,
}
BAND_WSPR_FREQS_HZ: Dict[str, float] = {
    "10m": 28.1246e6,
    "12m": 24.9246e6,
    "15m": 21.0946e6,
    "17m": 18.1046e6,
    "20m": 14.0956e6,
    "30m": 10.1387e6,
    "40m": 7.0386e6,
    "60m": 5.2887e6,
    "80m": 3.5686e6,
    "160m": 1.8366e6,
}
AUTO_SET_RX: List[int] = [0, 1, 2, 3, 4, 5, 6, 7]
_KIWIRECORDER_PATH = Path(__file__).resolve().parents[2] / "vendor" / "kiwiclient-jks" / "kiwirecorder.py"


def _first_existing_path(paths: List[Path]) -> Path:
    for path in paths:
        try:
            if path.exists() and path.is_file() and os.access(str(path), os.X_OK):
                return path
        except Exception:
            continue
    return paths[0]


def _resolve_binary_path(binary_name: str, candidates: List[Path]) -> Path:
    resolved = shutil.which(binary_name)
    if resolved and not resolved.startswith("/opt/local/"):
        return Path(resolved)
    return _first_existing_path(candidates)


_FT8MODEM_PATH = _resolve_binary_path(
    "ft8modem",
    [
        Path("/usr/local/bin/ft8modem"),
        Path("/opt/homebrew/bin/ft8modem"),
        Path(__file__).resolve().parents[3] / "ft8modem" / "ft8modem",
        Path(__file__).resolve().parents[2] / "ft8modem" / "ft8modem",
    ],
)
_AF2UDP_PATH = _resolve_binary_path(
    "af2udp",
    [
        Path("/usr/local/bin/af2udp"),
        Path("/opt/homebrew/bin/af2udp"),
        Path(__file__).resolve().parents[3] / "ft8modem" / "af2udp",
        Path(__file__).resolve().parents[2] / "ft8modem" / "af2udp",
    ],
)


receiver_mgr = ReceiverManager(
    kiwirecorder_path=_KIWIRECORDER_PATH,
    ft8modem_path=_FT8MODEM_PATH,
    af2udp_path=_AF2UDP_PATH,
    decode_callback=decodes_callback,
)

app.include_router(
    make_decodes_status_router(
        receiver_mgr=receiver_mgr,
        af2udp_path=_AF2UDP_PATH,
        ft8modem_path=_FT8MODEM_PATH,
    )
)
band_scanner = BandScanner()


def _compute_s_metrics_with_offset(results, offset):
    return compute_s_metrics(results, s_meter_offset_db=offset)


mgr = DiscoveryManager(
    get_loop=lambda: loop,
    broadcast_status=broadcast_status,
    compute_s_metrics=_compute_s_metrics_with_offset,
    waterholes=FT8_WATERHOLES,
)
rx_monitor = RxMonitor(kiwirecorder_path=_KIWIRECORDER_PATH, mgr=mgr)

# Asyncio event loop for scheduling broadcasts from the discovery thread
loop: Optional[asyncio.AbstractEventLoop] = None

app.include_router(make_band_scan_router(mgr=mgr, band_scanner=band_scanner))
app.include_router(make_config_router(mgr=mgr, waterholes=FT8_WATERHOLES))
app.include_router(make_status_router(mgr=mgr, waterholes=FT8_WATERHOLES))
app.include_router(make_schedule_router())
app.include_router(
    make_auto_set_router(
        mgr=mgr,
        receiver_mgr=receiver_mgr,
        band_order=BAND_ORDER,
        band_freqs_hz=BAND_FREQS_HZ,
        band_ft4_freqs_hz=BAND_FT4_FREQS_HZ,
        band_ssb_freqs_hz=BAND_SSB_FREQS_HZ,
        band_wspr_freqs_hz=BAND_WSPR_FREQS_HZ,
    )
)
app.include_router(make_rx_monitor_router(monitor=rx_monitor))
auto_set_loop = AutoSetLoop()

# SmartScheduler: merges seasonal tables + live propagation evidence + user pins
# into a band-condition map.  Fires force_reassign() when conditions change so
# receivers are reallocated away from dead bands without waiting for the next
# 30-second AutoSetLoop tick.
smart_scheduler = SmartScheduler(
    receiver_mgr=receiver_mgr,
    on_condition_change=auto_set_loop.force_reassign,
)
auto_set_loop.set_smart_scheduler(smart_scheduler)

app.include_router(make_admin_router(auto_set_loop=auto_set_loop))
app.include_router(make_automation_router())
app.include_router(make_metrics_router(receiver_mgr=receiver_mgr, get_api_metrics=_get_api_metrics))
app.include_router(make_health_router(receiver_mgr=receiver_mgr))
app.include_router(make_system_info_router(mgr=mgr, receiver_mgr=receiver_mgr))
app.include_router(make_smart_scheduler_router(smart_scheduler=smart_scheduler))
app.include_router(
    make_ws_status_router(
        mgr=mgr,
        waterholes=FT8_WATERHOLES,
        compute_s_metrics=_compute_s_metrics_with_offset,
    )
)
app.include_router(
    make_calibrate_router(
        mgr=mgr,
        waterholes=FT8_WATERHOLES,
        broadcast_status=broadcast_status,
        get_loop=lambda: loop,
    )
)
register_lifecycle(
    app,
    mgr=mgr,
    receiver_mgr=receiver_mgr,
    rx_monitor=rx_monitor,
    set_decodes_loop=set_decodes_loop,
    set_loop=lambda v: globals().__setitem__("loop", v),
    auto_set_loop=auto_set_loop,
    smart_scheduler=smart_scheduler,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("kiwi_scan.server:app", host="0.0.0.0", port=4020, reload=False)
