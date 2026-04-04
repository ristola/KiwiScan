from __future__ import annotations

import json
import logging
import time
import asyncio
import threading
import subprocess
import os
import signal
import re
import shlex
import shutil
from collections import deque
from importlib.metadata import PackageNotFoundError, version as package_version
from typing import Dict, List, Optional
from urllib.error import URLError
from urllib.request import Request as UrlRequest, urlopen

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
from .api.system_info import make_router as make_system_info_router
from .rx_monitor import RxMonitor
from .app_lifecycle import register_lifecycle
from .auto_set_loop import AutoSetLoop
from .smart_scheduler import SmartScheduler
from .api.smart_scheduler import make_router as make_smart_scheduler_router

# Configure logging to output to console (stderr) with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="KiwiSDR Scanner")


def _resolve_app_version() -> str:
    try:
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        if pyproject.exists():
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

    try:
        return str(package_version("kiwi-scan"))
    except PackageNotFoundError:
        pass
    except Exception:
        pass

    return "unknown"


def _resolve_git_commit() -> str | None:
    root = Path(__file__).resolve().parents[2]

    try:
        value = str(os.environ.get("KIWISCAN_BUILD_COMMIT", "") or "").strip()
        if re.match(r"^[0-9a-fA-F]{7,40}$", value):
            return value[:7].lower()
    except Exception:
        pass

    try:
        marker = root / "outputs" / "installed_commit.txt"
        if marker.exists():
            value = marker.read_text(encoding="utf-8", errors="ignore").strip()
            if re.match(r"^[0-9a-fA-F]{7,40}$", value):
                return value[:7].lower()
    except Exception:
        pass

    try:
        git_dir = root / ".git"
        if not git_dir.exists():
            raise FileNotFoundError(".git not present")
        out = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=1.5,
        )
        value = out.decode("utf-8", errors="ignore").strip()
        if value:
            return value
    except Exception:
        pass

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
    return {
        "version": _resolve_app_version(),
        "commit": _resolve_git_commit(),
    }


def _safe_update_target() -> tuple[str, str]:
    repo = str(os.environ.get("KIWISCAN_UPDATE_REPO", "ristola/KiwiScan") or "ristola/KiwiScan").strip()
    branch = str(os.environ.get("KIWISCAN_UPDATE_BRANCH", "main") or "main").strip()
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
        repo = "ristola/KiwiScan"
    if not re.match(r"^[A-Za-z0-9_.\-/]+$", branch):
        branch = "main"
    return repo, branch


def _safe_docker_update_repo() -> str:
    repo = str(os.environ.get("KIWISCAN_DOCKER_REPO", "n4ldr/kiwiscan") or "n4ldr/kiwiscan").strip()
    if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
        repo = "n4ldr/kiwiscan"
    return repo


def _update_mode() -> str:
    requested = str(os.environ.get("KIWISCAN_UPDATE_MODE", "") or "").strip().lower()
    if requested in {"host", "container"}:
        return requested
    try:
        if Path("/.dockerenv").exists():
            return "container"
    except Exception:
        pass
    try:
        cgroup = Path("/proc/1/cgroup")
        if cgroup.exists():
            raw = cgroup.read_text(encoding="utf-8", errors="ignore").lower()
            if any(token in raw for token in ("docker", "containerd", "kubepods", "podman")):
                return "container"
    except Exception:
        pass
    return "host"


def _fetch_latest_commit(repo: str, branch: str) -> str:
    url = f"https://api.github.com/repos/{repo}/commits/{branch}"
    req = UrlRequest(url, headers={"User-Agent": "kiwi-scan-updater"})
    with urlopen(req, timeout=4.0) as resp:  # nosec - trusted GitHub API endpoint
        raw = resp.read(512 * 1024).decode("utf-8", errors="ignore")
    data = json.loads(raw)
    sha = str(data.get("sha") or "").strip()
    if not sha:
        return ""
    return sha[:7]


def _fetch_latest_version(repo: str, ref: str) -> str:
    url = f"https://raw.githubusercontent.com/{repo}/{ref}/pyproject.toml"
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


def _normalize_version(value: str | None) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("v"):
        text = text[1:].strip()
    return text


def _compare_versions(left: str | None, right: str | None) -> int:
    left_norm = _normalize_version(left)
    right_norm = _normalize_version(right)
    if not left_norm and not right_norm:
        return 0
    if not left_norm:
        return -1
    if not right_norm:
        return 1

    token_re = re.compile(r"[.-]")
    left_tokens = token_re.split(left_norm)
    right_tokens = token_re.split(right_norm)

    def to_key(token: str) -> tuple[int, int | str]:
        text = str(token or "").strip()
        if text.isdigit():
            return (1, int(text))
        return (0, text.lower())

    max_len = max(len(left_tokens), len(right_tokens))
    for idx in range(max_len):
        left_key = to_key(left_tokens[idx] if idx < len(left_tokens) else "0")
        right_key = to_key(right_tokens[idx] if idx < len(right_tokens) else "0")
        if left_key == right_key:
            continue
        return 1 if left_key > right_key else -1
    return 0


def _is_version_newer(latest: str | None, current: str | None) -> bool:
    current_norm = _normalize_version(current)
    if not current_norm or current_norm == "unknown":
        return False
    return _compare_versions(latest, current) > 0


def _fetch_latest_container_version(repo: str) -> str:
    url = f"https://hub.docker.com/v2/repositories/{repo}/tags?page_size=100"
    best = ""
    seen: set[str] = set()
    while url:
        req = UrlRequest(url, headers={"User-Agent": "kiwi-scan-updater"})
        with urlopen(req, timeout=4.0) as resp:  # nosec - trusted Docker Hub API endpoint
            raw = resp.read(512 * 1024).decode("utf-8", errors="ignore")
        data = json.loads(raw)
        results = data.get("results") if isinstance(data, dict) else []
        if not isinstance(results, list):
            results = []
        for row in results:
            if not isinstance(row, dict):
                continue
            name = _normalize_version(str(row.get("name") or ""))
            if not name or name == "latest" or name in seen:
                continue
            seen.add(name)
            if not re.match(r"^\d+\.\d+\.\d+(?:[.-][0-9A-Za-z]+)*$", name):
                continue
            if not best or _compare_versions(name, best) > 0:
                best = name
        next_url = data.get("next") if isinstance(data, dict) else None
        url = str(next_url).strip() if next_url else ""
        if len(seen) >= 500:
            break
    return best


def _manual_container_update_command(repo: str, version: str) -> str:
    next_ref = f"{repo}:{version}" if version else f"{repo}:<version>"
    return (
        f"Edit docker-compose.yml to use image: {next_ref}, "
        "then run: docker compose pull && docker compose up -d"
    )


def _background_apply_update(repo: str, branch: str) -> None:
    global _update_in_progress
    try:
        root = Path(__file__).resolve().parents[2]
        current_pid = os.getpid()
        install_url = f"https://raw.githubusercontent.com/{repo}/{branch}/tools/install_latest.sh"
        cmd = (
            "set -euo pipefail; "
            f"curl -fsSL {shlex.quote(install_url)} | bash -s -- {shlex.quote(str(root))}; "
            "true"
        )
        subprocess.run(
            ["/bin/bash", "-lc", cmd],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
        try:
            installed_commit = _fetch_latest_commit(repo, branch)
            if installed_commit:
                marker = root / "outputs" / "installed_commit.txt"
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.write_text(f"{installed_commit}\n", encoding="utf-8")
        except Exception:
            logger.debug("Could not refresh installed_commit marker after update", exc_info=True)
        try:
            os.kill(current_pid, signal.SIGTERM)
        except Exception:
            logger.warning("Self-update installed but failed to terminate current process; restart manually")
    except Exception:
        logger.exception("Self-update failed to start")
    finally:
        with _update_lock:
            _update_in_progress = False


@app.get("/update/check")
def check_update() -> Dict[str, object]:
    mode = _update_mode()
    current_version = _resolve_app_version()
    current_commit = str(_resolve_git_commit() or "")

    if mode == "container":
        docker_repo = _safe_docker_update_repo()
        latest_version = ""
        latest_error = ""
        try:
            latest_version = _fetch_latest_container_version(docker_repo)
        except (URLError, TimeoutError) as exc:
            latest_error = f"network: {exc}"
        except Exception as exc:
            latest_error = str(exc)

        update_available = bool(latest_version and _is_version_newer(latest_version, current_version))
        return {
            "deployment_mode": mode,
            "update_source": "docker_hub",
            "docker_repo": docker_repo,
            "current_version": current_version,
            "latest_version": latest_version,
            "current_commit": current_commit,
            "latest_commit": "",
            "update_by_commit": False,
            "update_by_version": bool(update_available),
            "update_by_unknown_commit": False,
            "update_available": bool(update_available),
            "update_in_progress": False,
            "apply_supported": False,
            "manual_update_required": True,
            "manual_update_command": _manual_container_update_command(docker_repo, latest_version),
            "error": latest_error,
        }

    repo, branch = _safe_update_target()
    latest_commit = ""
    latest_version = ""
    latest_error = ""
    try:
        latest_commit = _fetch_latest_commit(repo, branch)
        latest_version = _fetch_latest_version(repo, latest_commit or branch)
    except (URLError, TimeoutError) as exc:
        latest_error = f"network: {exc}"
    except Exception as exc:
        latest_error = str(exc)

    current_version_norm = _normalize_version(current_version)
    latest_version_norm = _normalize_version(latest_version)
    latest_is_newer = _is_version_newer(latest_version_norm, current_version_norm)
    by_commit = bool(latest_commit and current_commit and latest_commit != current_commit)
    by_version = bool(latest_is_newer)
    by_unknown_commit = bool(
        latest_commit
        and not current_commit
        and (
            not latest_version_norm
            or not current_version_norm
            or current_version_norm == "unknown"
            or latest_is_newer
        )
    )
    update_available = bool(by_commit or by_version or by_unknown_commit)
    return {
        "deployment_mode": mode,
        "update_source": "github",
        "repo": repo,
        "branch": branch,
        "current_version": current_version,
        "latest_version": latest_version,
        "current_commit": current_commit,
        "latest_commit": latest_commit,
        "update_by_commit": by_commit,
        "update_by_version": by_version,
        "update_by_unknown_commit": by_unknown_commit,
        "update_available": update_available,
        "update_in_progress": bool(_update_in_progress),
        "apply_supported": True,
        "manual_update_required": False,
        "manual_update_command": "",
        "error": latest_error,
    }


@app.post("/update/apply")
def apply_update() -> Dict[str, object]:
    global _update_in_progress
    mode = _update_mode()
    if mode == "container":
        docker_repo = _safe_docker_update_repo()
        latest_version = ""
        try:
            latest_version = _fetch_latest_container_version(docker_repo)
        except Exception:
            latest_version = ""
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Container deployments require a manual image-tag update and redeploy.",
                "deployment_mode": mode,
                "docker_repo": docker_repo,
                "latest_version": latest_version,
                "manual_update_command": _manual_container_update_command(docker_repo, latest_version),
            },
        )

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


def _reset_api_metrics() -> Dict[str, float | int]:
    global _api_request_total, _api_error_total
    with _api_metrics_lock:
        _api_request_total = 0
        _api_error_total = 0
        _api_latency_ms.clear()
    return _get_api_metrics()


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
        Path(__file__).resolve().parents[2] / "vendor" / "ft8modem-sm" / "ft8modem",
        Path(__file__).resolve().parents[3] / "ft8modem" / "ft8modem",
        Path(__file__).resolve().parents[2] / "ft8modem" / "ft8modem",
    ]
)

_AF2UDP_PATH = _resolve_binary_path(
    "af2udp",
    [
        Path("/usr/local/bin/af2udp"),
        Path("/opt/homebrew/bin/af2udp"),
        Path(__file__).resolve().parents[2] / "vendor" / "ft8modem-sm" / "af2udp",
        Path(__file__).resolve().parents[3] / "ft8modem" / "af2udp",
        Path(__file__).resolve().parents[2] / "ft8modem" / "af2udp",
    ]
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

# Asyncio event loop for scheduling broadcasts from the discovery thread
loop: Optional[asyncio.AbstractEventLoop] = None

app.include_router(make_band_scan_router(mgr=mgr, band_scanner=band_scanner))
try:
    app.include_router(make_config_router(mgr=mgr, waterholes=FT8_WATERHOLES, receiver_mgr=receiver_mgr))
except TypeError:
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
app.include_router(make_admin_router(auto_set_loop=auto_set_loop))
app.include_router(make_automation_router())
app.include_router(
    make_metrics_router(
        receiver_mgr=receiver_mgr,
        get_api_metrics=_get_api_metrics,
        reset_api_metrics=_reset_api_metrics,
    )
)
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
    auto_set_loop=auto_set_loop,
    smart_scheduler=smart_scheduler,
    set_decodes_loop=set_decodes_loop,
    set_loop=lambda v: globals().__setitem__("loop", v),
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("kiwi_scan.server:app", host="0.0.0.0", port=4020, reload=False)
