from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Optional

from fastapi import APIRouter, HTTPException, Request

from ..discovery import DiscoveryWorker
from ..kiwi_waterfall import KiwiClientUnavailable

logger = logging.getLogger(__name__)


def make_router(
    *,
    mgr: object,
    waterholes: Dict[str, float],
    broadcast_status: Callable[[Dict], Awaitable[None]],
    get_loop: Callable[[], Optional[asyncio.AbstractEventLoop]],
) -> APIRouter:
    """Create router for /calibrate and /calibrate_all.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()

    @router.post("/calibrate")
    async def calibrate(request: Request):
        """Pause scanning, sample waterfall frames, and suggest a threshold_db.
            Body JSON:
                - band: str (required)
                - seconds: float (optional; default mgr.dwell_s)
                - target_score: float (optional; default 0.05)
                - margin_db: float (optional; default 1.0)
        """

        data = await request.json()
        band = str(data.get("band", "")).strip()
        if band not in waterholes:
            raise HTTPException(status_code=400, detail=f"unknown band: {band}")

        seconds = data.get("seconds", None)
        target_score = data.get("target_score", 0.05)
        margin_db = data.get("margin_db", 1.0)
        try:
            seconds_f = float(seconds) if seconds is not None else None
            if seconds_f is not None and (seconds_f <= 0 or seconds_f > 120):
                raise ValueError("seconds must be > 0 and <= 120")
            target_score_f = float(target_score)
            if target_score_f < 0 or target_score_f > 1:
                raise ValueError("target_score must be between 0 and 1")
            margin_db_f = float(margin_db)
            if margin_db_f < 0 or margin_db_f > 10:
                raise ValueError("margin_db must be between 0 and 10")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid calibrate parameters: {e}")

        # Snapshot config
        with mgr.lock:  # type: ignore[attr-defined]
            host = mgr.host
            port = mgr.port
            rx_chan = mgr.rx_chan
            span_hz = mgr.span_hz
            debug = mgr.debug
            dwell_s = mgr.dwell_s

        if seconds_f is None:
            seconds_f = float(min(60.0, max(5.0, dwell_s)))

        freq = float(waterholes[band])

        mgr.pause()  # type: ignore[attr-defined]
        # Wait for scan loop to reach a safe pause point (end of current band).
        paused = mgr.wait_until_paused(timeout_s=min(10.0, float(dwell_s) + 1.0))  # type: ignore[attr-defined]
        if not paused:
            logger.warning("calibrate: scan did not pause quickly; continuing")

        with mgr.lock:  # type: ignore[attr-defined]
            mgr.calibrating_band = band
            mgr.current_band = band

        def _do_calibrate():
            worker = DiscoveryWorker(
                host=host,
                port=port,
                debug=debug,
                rx_chan=rx_chan,
                dwell_s=float(seconds_f),
                span_hz=float(span_hz),
                threshold_db=15.0,
                frames_per_second=mgr.fps,  # type: ignore[attr-defined]
            )
            try:
                return worker.calibrate_threshold(
                    freq_hz=freq,
                    duration_s=float(seconds_f),
                    target_score=float(target_score_f),
                    threshold_min=0.0,
                    threshold_max=120.0,
                    step_db=1.0,
                )
            except Exception as e:
                return {
                    "ok": False,
                    "reason": "exception",
                    "error": f"{type(e).__name__}: {e}",
                    "frames": 0,
                    "suggested_threshold_db": None,
                }

        try:
            loop_ = asyncio.get_event_loop()
            result = await loop_.run_in_executor(None, _do_calibrate)
            suggested = None
            try:
                suggested = result.get("suggested_threshold_db", None) if isinstance(result, dict) else None
            except Exception:
                suggested = None

            applied = None
            with mgr.lock:  # type: ignore[attr-defined]
                if suggested is not None:
                    applied = float(min(120.0, max(0.0, float(suggested) + float(margin_db_f))))
                    mgr.threshold_db_by_band[str(band)] = float(applied)
                    mgr._save_thresholds()  # type: ignore[attr-defined]
            return {"ok": True, "band": band, "freq_hz": freq, "result": result, "applied_threshold_db": applied}
        except KiwiClientUnavailable as e:
            raise HTTPException(status_code=503, detail=f"Kiwi unavailable: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"calibration failed: {type(e).__name__}: {e}")
        finally:
            with mgr.lock:  # type: ignore[attr-defined]
                mgr.calibrating_band = None
            mgr.resume()  # type: ignore[attr-defined]

    @router.post("/calibrate_all")
    async def calibrate_all(request: Request):
        """Calibrate and store thresholds for all bands.

        Body JSON:
          - seconds_per_band: float (optional; default min(15, mgr.dwell_s))
          - target_score: float (optional; default 0.05)
          - margin_db: float (optional; default 1.0)
          - bands: list[str] (optional; default all FT8_WATERHOLES)
        """

        data = await request.json()
        seconds_per_band = data.get("seconds_per_band", None)
        target_score = data.get("target_score", 0.05)
        margin_db = data.get("margin_db", 1.0)
        bands_in = data.get("bands", None)

        try:
            spb = float(seconds_per_band) if seconds_per_band is not None else None
            ts = float(target_score)
            if ts < 0 or ts > 1:
                raise ValueError("target_score must be between 0 and 1")
            md = float(margin_db)
            if md < 0 or md > 10:
                raise ValueError("margin_db must be between 0 and 10")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid calibrate_all parameters: {e}")

        with mgr.lock:  # type: ignore[attr-defined]
            host = mgr.host
            port = mgr.port
            rx_chan = mgr.rx_chan
            span_hz = mgr.span_hz
            debug = mgr.debug
            dwell_s = mgr.dwell_s
            fps = mgr.fps

        if spb is None:
            # Rapid default calibration so users don't wait a full dwell per band.
            spb = 4.0
        if spb <= 0 or spb > 120:
            raise HTTPException(status_code=400, detail="seconds_per_band must be > 0 and <= 120")

        if bands_in is None:
            bands = list(waterholes.keys())
        else:
            if not isinstance(bands_in, list):
                raise HTTPException(status_code=400, detail="bands must be a list")
            bands = [str(b).strip() for b in bands_in]
            for b in bands:
                if b not in waterholes:
                    raise HTTPException(status_code=400, detail=f"unknown band in bands: {b}")

        mgr.pause()  # type: ignore[attr-defined]
        paused = mgr.wait_until_paused(timeout_s=min(10.0, float(dwell_s) + 1.0))  # type: ignore[attr-defined]
        if not paused:
            logger.warning("calibrate_all: scan did not pause quickly; continuing")

        results: Dict[str, Dict[str, object]] = {}
        applied: Dict[str, float] = {}

        try:
            loop_ = asyncio.get_event_loop()

            for band in bands:
                freq = float(waterholes[band])
                with mgr.lock:  # type: ignore[attr-defined]
                    mgr.calibrating_band = band
                    mgr.current_band = band
                try:
                    loop_for_status = get_loop()
                    if loop_for_status is not None:
                        payload = {
                            "results": mgr.results,
                            "current_band": mgr.current_band,
                            "calibrating_band": mgr.calibrating_band,
                            "last_updated": mgr.last_updated,
                            "rx_chan": mgr.rx_chan,
                            "camp_status": mgr.camp_status,
                            "waterholes": waterholes,
                            "threshold_db": mgr.threshold_db,
                            "threshold_db_by_band": mgr.threshold_db_by_band,
                        }
                        asyncio.run_coroutine_threadsafe(broadcast_status(payload), loop_for_status)
                except Exception:
                    pass

                def _do_one():
                    worker = DiscoveryWorker(
                        host=host,
                        port=port,
                        debug=debug,
                        rx_chan=rx_chan,
                        dwell_s=float(spb),
                        span_hz=float(span_hz),
                        threshold_db=15.0,
                        frames_per_second=float(fps),
                    )
                    try:
                        return worker.calibrate_threshold(
                            freq_hz=freq,
                            duration_s=float(spb),
                            target_score=float(ts),
                            threshold_min=0.0,
                            threshold_max=120.0,
                            step_db=1.0,
                        )
                    except Exception as e:
                        return {
                            "ok": False,
                            "reason": "exception",
                            "error": f"{type(e).__name__}: {e}",
                            "frames": 0,
                            "suggested_threshold_db": None,
                        }

                r = await loop_.run_in_executor(None, _do_one)
                results[band] = r
                suggested = None
                try:
                    suggested = r.get("suggested_threshold_db", None) if isinstance(r, dict) else None
                except Exception:
                    suggested = None
                if suggested is not None:
                    val = float(min(120.0, max(0.0, float(suggested) + float(md))))
                    applied[band] = val
                    with mgr.lock:  # type: ignore[attr-defined]
                        mgr.threshold_db_by_band[str(band)] = float(val)
                        mgr._save_thresholds()  # type: ignore[attr-defined]

            with mgr.lock:  # type: ignore[attr-defined]
                mgr._save_thresholds()  # type: ignore[attr-defined]
            return {
                "ok": True,
                "seconds_per_band": float(spb),
                "target_score": float(ts),
                "margin_db": float(md),
                "applied_threshold_db_by_band": applied,
                "results": results,
            }
        except KiwiClientUnavailable as e:
            raise HTTPException(status_code=503, detail=f"Kiwi unavailable: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"calibrate_all failed: {type(e).__name__}: {e}")
        finally:
            with mgr.lock:  # type: ignore[attr-defined]
                mgr.calibrating_band = None
            mgr.resume()  # type: ignore[attr-defined]

    return router
