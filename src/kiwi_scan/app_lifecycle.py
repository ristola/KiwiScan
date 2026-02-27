from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from fastapi import FastAPI

logger = logging.getLogger(__name__)


def register_lifecycle(
    app: FastAPI,
    *,
    mgr: object,
    receiver_mgr: object,
    rx_monitor: object | None = None,
    auto_set_loop: object | None = None,
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

        if auto_set_loop is not None:
            try:
                auto_set_loop.start()  # type: ignore[attr-defined]
            except Exception:
                logger.exception("Auto-set loop failed to start")

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

            if auto_set_loop is not None:
                try:
                    auto_set_loop.stop()  # type: ignore[attr-defined]
                except Exception:
                    pass

    app.add_event_handler("startup", _startup)
    app.add_event_handler("shutdown", _shutdown)
