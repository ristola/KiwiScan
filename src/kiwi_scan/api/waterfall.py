"""
api/waterfall.py — KiWi waterfall WebSocket proxy.

WS /api/waterfall/ws?host=HOST&port=PORT&center_hz=HZ&span_hz=HZ
   Connects to a KiWi SDR waterfall in a background thread and streams
   each FFT frame to the browser as JSON:
       {"bins":[...dBm values...],"center_hz":float,"span_hz":float}
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..kiwi_waterfall import KiwiClientUnavailable, WaterfallFrame, subscribe_waterfall

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/waterfall", tags=["waterfall"])


@router.websocket("/ws")
async def waterfall_ws(ws: WebSocket) -> None:
    await ws.accept()

    # Parse query params after accept to avoid Starlette 1.x injection 403
    p = ws.query_params
    host      = p.get("host", "127.0.0.1")
    port      = int(p.get("port", "8073"))
    center_hz = float(p.get("center_hz", "15000000"))
    span_hz   = float(p.get("span_hz",   "30000000"))

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=60)
    stop_event = threading.Event()

    def _on_frame(frame: WaterfallFrame) -> None:
        if stop_event.is_set():
            return
        payload = json.dumps(
            {
                "bins": list(frame.power_bins),
                "center_hz": frame.center_freq_hz,
                "span_hz": frame.span_hz,
            }
        )
        try:
            loop.call_soon_threadsafe(queue.put_nowait, payload)
        except asyncio.QueueFull:
            pass  # drop frame rather than block the kiwi thread

    def _run() -> None:
        err_msg: str | None = None
        try:
            subscribe_waterfall(
                host=host,
                port=port,
                center_freq_hz=center_hz,
                span_hz=span_hz,
                on_frame=_on_frame,
                should_stop=stop_event.is_set,
            )
        except (KiwiClientUnavailable, Exception) as exc:
            err_msg = str(exc)
            logger.warning("Waterfall %s:%s — %s", host, port, exc)
        finally:
            try:
                if err_msg:
                    import json as _json
                    loop.call_soon_threadsafe(
                        queue.put_nowait,
                        _json.dumps({"error": err_msg}),
                    )
                loop.call_soon_threadsafe(queue.put_nowait, None)  # sentinel
            except Exception:
                pass

    thread = threading.Thread(target=_run, daemon=True, name=f"wf-{host}:{port}")
    thread.start()

    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=20.0)
            if msg is None:
                break  # waterfall thread finished or errored
            await ws.send_text(msg)
    except (WebSocketDisconnect, asyncio.CancelledError, asyncio.TimeoutError):
        pass
    except Exception as exc:
        logger.debug("Waterfall ws error: %s", exc)
    finally:
        stop_event.set()
