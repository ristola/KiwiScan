from __future__ import annotations

import json
import logging
import time
import asyncio
import threading
import subprocess
import os
import re
import shlex
from collections import deque
from importlib.metadata import PackageNotFoundError, version as package_version
from typing import Dict, List, Optional
from urllib.error import URLError
from urllib.request import urlopen

try:
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - Python < 3.11
    tomllib = None  # type: ignore[assignment]

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
from .rx_monitor import RxMonitor
from .app_lifecycle import register_lifecycle

# Configure logging to output to console (stderr) with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="KiwiSDR Scanner")


def _resolve_app_version() -> str:
    try:
        return str(package_version("kiwi-scan"))
    except PackageNotFoundError:
        pass
    except Exception:
        pass

    try:
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        raw = pyproject.read_text(encoding="utf-8")
        if tomllib is not None:
            data = tomllib.loads(raw)
            project = data.get("project") if isinstance(data, dict) else None
            version = project.get("version") if isinstance(project, dict) else None
            if isinstance(version, str) and version.strip():
                return version.strip()
        else:
            m = re.search(r'^version\s*=\s*"([^"]+)"\s*$', raw, flags=re.MULTILINE)
            if m:
                return m.group(1).strip()
    except Exception:
        pass

    return "unknown"


def _resolve_git_commit() -> str | None:
    try:
        root = Path(__file__).resolve().parents[2]
        git_dir = root / ".git"
        if not git_dir.exists():
            return None
        out = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=1.5,
        )
        value = out.decode("utf-8", errors="ignore").strip()
        return value or None
    except Exception:
        return None


APP_VERSION = _resolve_app_version()
APP_COMMIT = _resolve_git_commit()

_update_lock = threading.Lock()
_update_in_progress = False

_api_metrics_lock = threading.Lock()
_api_latency_ms: deque[float] = deque(maxlen=2000)
_api_request_total = 0
_api_error_total = 0


@app.get("/version")
def get_version() -> Dict[str, str | None]:
    return {"version": APP_VERSION, "commit": APP_COMMIT}


def _safe_update_target() -> tuple[str, str]:
    repo = str(os.environ.get("KIWISCAN_UPDATE_REPO", "ristola/KiwiScan") or "ristola/KiwiScan").strip()
    branch = str(os.environ.get("KIWISCAN_UPDATE_BRANCH", "main") or "main").strip()
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
        repo = "ristola/KiwiScan"
    if not re.match(r"^[A-Za-z0-9_.\-/]+$", branch):
        branch = "main"
    return repo, branch


def _fetch_latest_commit(repo: str, branch: str) -> str:
    url = f"https://api.github.com/repos/{repo}/commits/{branch}"
    req_headers = {"User-Agent": "kiwi-scan-updater"}
    with urlopen(url, timeout=4.0) as resp:  # nosec - trusted GitHub API endpoint
        raw = resp.read(512 * 1024).decode("utf-8", errors="ignore")
    data = json.loads(raw)
    sha = str(data.get("sha") or "").strip()
    if not sha:
        return ""
    return sha[:7]


def _fetch_latest_version(repo: str, branch: str) -> str:
    url = f"https://raw.githubusercontent.com/{repo}/{branch}/pyproject.toml"
    with urlopen(url, timeout=4.0) as resp:  # nosec - trusted GitHub raw endpoint
        raw = resp.read(512 * 1024).decode("utf-8", errors="ignore")
    if tomllib is not None:
        try:
            data = tomllib.loads(raw)
            project = data.get("project") if isinstance(data, dict) else None
            version = project.get("version") if isinstance(project, dict) else None
            if isinstance(version, str) and version.strip():
                return version.strip()
        except Exception:
            pass
    m = re.search(r'^version\s*=\s*"([^"]+)"\s*$', raw, flags=re.MULTILINE)
    return m.group(1).strip() if m else ""


def _background_apply_update(repo: str, branch: str) -> None:
    global _update_in_progress
    try:
        root = Path(__file__).resolve().parents[2]
        install_url = f"https://raw.githubusercontent.com/{repo}/{branch}/tools/install_latest.sh"
        cmd = (
            "set -euo pipefail; "
            f"curl -fsSL {shlex.quote(install_url)} | bash -s -- {shlex.quote(str(root))}; "
            f"cd {shlex.quote(str(root))}; "
            "nohup ./run_server.sh >/tmp/kiwi_run_server.out 2>&1 &"
        )
        subprocess.Popen(
            ["/bin/bash", "-lc", cmd],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        logger.exception("Self-update failed to start")
        with _update_lock:
            _update_in_progress = False


@app.get("/update/check")
def check_update() -> Dict[str, object]:
    repo, branch = _safe_update_target()
    latest_commit = ""
    latest_version = ""
    latest_error = ""
    try:
        latest_commit = _fetch_latest_commit(repo, branch)
        latest_version = _fetch_latest_version(repo, branch)
    except (URLError, TimeoutError) as exc:
        latest_error = f"network: {exc}"
    except Exception as exc:
        latest_error = str(exc)

    current_commit = str(APP_COMMIT or "")
    by_commit = bool(latest_commit and current_commit and latest_commit != current_commit)
    by_version = bool(latest_version and APP_VERSION and latest_version != APP_VERSION)
    by_unknown_commit = bool(latest_commit and not current_commit)
    update_available = bool(by_commit or by_version or by_unknown_commit)
    return {
        "repo": repo,
        "branch": branch,
        "current_version": APP_VERSION,
        "latest_version": latest_version,
        "current_commit": current_commit,
        "latest_commit": latest_commit,
        "update_by_commit": by_commit,
        "update_by_version": by_version,
        "update_by_unknown_commit": by_unknown_commit,
        "update_available": update_available,
        "update_in_progress": bool(_update_in_progress),
        "error": latest_error,
    }


@app.post("/update/apply")
def apply_update() -> Dict[str, object]:
    global _update_in_progress
    repo, branch = _safe_update_target()
    with _update_lock:
        if _update_in_progress:
            return {
                "ok": True,
                "status": "already_in_progress",
                "repo": repo,
                "branch": branch,
            }
        _update_in_progress = True

    thread = threading.Thread(target=_background_apply_update, args=(repo, branch), daemon=True)
    thread.start()
    return {
        "ok": True,
        "status": "started",
        "repo": repo,
        "branch": branch,
    }


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
_FT8MODEM_PATH = Path(__file__).resolve().parents[3] / "ft8modem" / "ft8modem"
_AF2UDP_PATH = Path("/usr/local/bin/af2udp")
if not _AF2UDP_PATH.exists():
    alt_af2udp = Path(__file__).resolve().parents[2] / "ft8modem" / "af2udp"
    if alt_af2udp.exists():
        _AF2UDP_PATH = alt_af2udp


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
app.include_router(make_admin_router())
app.include_router(make_automation_router())
app.include_router(make_metrics_router(receiver_mgr=receiver_mgr, get_api_metrics=_get_api_metrics))
app.include_router(make_health_router(receiver_mgr=receiver_mgr))
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
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("kiwi_scan.server:app", host="0.0.0.0", port=4020, reload=False)
