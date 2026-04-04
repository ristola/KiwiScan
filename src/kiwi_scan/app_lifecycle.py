from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Callable, Optional

try:
    import anyio
except ImportError:  # anyio may not be on the path before startup
    anyio = None  # type: ignore[assignment]

from fastapi import FastAPI

from .kiwi_discovery import discover_kiwis, is_unconfigured_kiwi_host

logger = logging.getLogger(__name__)


def _is_container_runtime() -> bool:
    raw_mode = str(os.environ.get("KIWISCAN_UPDATE_MODE", "") or "").strip().lower()
    if raw_mode == "container":
        return True
    if raw_mode in {"host", "native"}:
        return False
    if Path("/.dockerenv").exists():
        return True
    try:
        cgroup = Path("/proc/1/cgroup")
        if cgroup.exists():
            text = cgroup.read_text(encoding="utf-8", errors="ignore").lower()
            if "docker" in text or "containerd" in text or "kubepods" in text:
                return True
    except Exception:
        pass
    return False


def _bootstrap_container_kiwi(mgr: object) -> dict[str, object] | None:
    with mgr.lock:  # type: ignore[attr-defined]
        current_host = str(getattr(mgr, "host", "") or "").strip()
        current_port = int(getattr(mgr, "port", 8073) or 8073)

    if not is_unconfigured_kiwi_host(current_host):
        return None

    discovery = discover_kiwis(client_ip="", port=current_port, timeout_s=0.20, max_hosts=8)
    found = discovery.get("found")
    if not isinstance(found, list) or not found:
        logger.info("Container startup Kiwi auto-discovery found no hosts")
        return None

    candidate = found[0]
    if not isinstance(candidate, dict):
        return None

    host = str(candidate.get("host") or "").strip()
    try:
        port = int(candidate.get("port") or current_port)
    except Exception:
        port = current_port
    if not host or not (1 <= port <= 65535):
        return None

    with mgr.lock:  # type: ignore[attr-defined]
        mgr.host = host
        mgr.port = port
        mgr.rx_chan = None
        mgr._save_config()  # type: ignore[attr-defined]

    logger.info("Container startup Kiwi auto-discovery selected %s:%s", host, port)
    return {"host": host, "port": port, "source": discovery.get("source")}


def register_lifecycle(
    app: FastAPI,
    *,
    mgr: object,
    receiver_mgr: object,
    rx_monitor: object | None = None,
    auto_set_loop: object | None = None,
    smart_scheduler: object | None = None,
    set_decodes_loop: Callable[[Optional[asyncio.AbstractEventLoop]], None],
    set_loop: Callable[[Optional[asyncio.AbstractEventLoop]], None],
) -> None:
    """Register FastAPI startup/shutdown handlers.

    Keeps server.py focused on wiring.
    """

    async def _startup() -> None:
        loop = asyncio.get_event_loop()
        set_loop(loop)
        set_decodes_loop(loop)

        # Increase the anyio thread-pool capacity so that sync route handlers
        # (health, config, system/info, …) are never starved by a handful of
        # long-running Kiwi HTTP calls occupying the default ~4-thread pool.
        try:
            if anyio is not None:
                limiter = anyio.to_thread.current_default_thread_limiter()
                limiter.total_tokens = max(limiter.total_tokens, 40)
                logger.info("anyio thread pool capacity set to %d", limiter.total_tokens)
        except Exception:
            logger.debug("Could not adjust anyio thread pool size", exc_info=True)

        discovered_kiwi: dict[str, object] | None = None

        if _is_container_runtime():
            try:
                discovered_kiwi = await asyncio.to_thread(_bootstrap_container_kiwi, mgr)
            except Exception:
                logger.exception("Container startup Kiwi auto-discovery failed")

        try:
            if hasattr(receiver_mgr, "dependency_report"):
                report = receiver_mgr.dependency_report()  # type: ignore[attr-defined]
                if hasattr(mgr, "set_runtime_dependencies"):
                    mgr.set_runtime_dependencies(report, save=True)  # type: ignore[attr-defined]
                missing = report.get("missing") if isinstance(report, dict) else []
                if isinstance(missing, list) and missing:
                    logger.error("Receiver runtime dependencies missing at startup: %s", ", ".join(str(m) for m in missing))
        except Exception:
            logger.exception("Runtime dependency detection failed during startup")

        # Optional: dedicated WebSocket server for legacy clients (ws://<host>:4010/)
        try:
            from .ws4010_server import ensure_ws4010_started

            ensure_ws4010_started()
        except Exception:
            # Best effort: don't block startup if WS4010 cannot bind.
            pass

        try:
            from .udp4010_server import ensure_udp4010_started

            ensure_udp4010_started()
        except Exception:
            logger.exception("UDP4010 server failed to start")

        if auto_set_loop is not None:
            try:
                auto_set_loop.start()  # type: ignore[attr-defined]
                if discovered_kiwi is not None:
                    logger.info("Auto-set loop started after startup Kiwi discovery refresh")
            except Exception:
                logger.exception("Auto-set loop failed to start")

        if smart_scheduler is not None:
            try:
                smart_scheduler.start()  # type: ignore[attr-defined]
            except Exception:
                logger.exception("SmartScheduler failed to start")

    async def _shutdown() -> None:
        try:
            mgr.stop()  # type: ignore[attr-defined]
        finally:
            if rx_monitor is not None:
                try:
                    rx_monitor.stop()  # type: ignore[attr-defined]
                except Exception:
                    pass
            receiver_mgr.stop_all()  # type: ignore[attr-defined]
            set_decodes_loop(None)
            set_loop(None)

            try:
                from .ws4010_server import stop_ws4010

                stop_ws4010()
            except Exception:
                pass

            try:
                from .udp4010_server import stop_udp4010

                stop_udp4010()
            except Exception:
                pass

            if auto_set_loop is not None:
                try:
                    auto_set_loop.stop()  # type: ignore[attr-defined]
                except Exception:
                    pass

            if smart_scheduler is not None:
                try:
                    smart_scheduler.stop()  # type: ignore[attr-defined]
                except Exception:
                    pass

    if hasattr(app, "add_event_handler"):
        app.add_event_handler("startup", _startup)
        app.add_event_handler("shutdown", _shutdown)
        return

    router = getattr(app, "router", None)
    if router is None:
        raise AttributeError("FastAPI application has no lifecycle registration API")

    startup_handlers = getattr(router, "on_startup", None)
    shutdown_handlers = getattr(router, "on_shutdown", None)
    if startup_handlers is None or shutdown_handlers is None:
        raise AttributeError("FastAPI router does not expose startup/shutdown handler lists")

    startup_handlers.append(_startup)
    shutdown_handlers.append(_shutdown)
