from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import uvicorn

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_server: Optional[uvicorn.Server] = None


def _is_enabled() -> bool:
    raw = os.environ.get("KIWI_SCAN_WS4010", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def ensure_ws4010_started() -> None:
    """Start a dedicated WebSocket server on port 4010 in a background thread.

    This keeps the main FastAPI/HTTP server on port 4020 while continuing to
    export legacy WebSocket decode data on port 4010.
    """

    if not _is_enabled():
        return

    global _thread, _server
    with _lock:
        if _thread and _thread.is_alive():
            return
        if _thread is not None and not _thread.is_alive():
            _thread = None
            _server = None

        src_dir = str(Path(__file__).resolve().parents[1])
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)

        config = uvicorn.Config(
            "kiwi_scan.ws4010_app:app",
            host="0.0.0.0",
            port=4010,
            reload=False,
            log_level=os.environ.get("KIWI_SCAN_WS4010_LOG_LEVEL", "info"),
            access_log=False,
            ws_ping_interval=20.0,
            ws_ping_timeout=120.0,
        )
        server = uvicorn.Server(config)
        _server = server

        def _run() -> None:
            try:
                server.run()
            except Exception:
                logger.warning("WS4010 server exited unexpectedly", exc_info=True)

        _thread = threading.Thread(target=_run, name="kiwi-scan-ws4010", daemon=True)
        _thread.start()

    # Best-effort: give uvicorn a moment to bind so users see immediate errors.
    time.sleep(0.05)


def stop_ws4010() -> None:
    global _server, _thread
    with _lock:
        if _server is None:
            return
        _server.should_exit = True
        if hasattr(_server, "force_exit"):
            _server.force_exit = True

    if _thread is not None:
        _thread.join(timeout=2.0)
        if not _thread.is_alive():
            with _lock:
                _thread = None
                _server = None


def restart_ws4010() -> None:
    stop_ws4010()
    ensure_ws4010_started()
