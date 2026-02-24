from __future__ import annotations

import time
from typing import Dict

from fastapi import APIRouter

from ..discovery import dbm_to_s_units


def compute_s_metrics(results: Dict[str, Dict], *, s_meter_offset_db: float) -> Dict[str, Dict]:
    """Return a copy of results with derived S metrics computed from the current offset.

    This keeps S-meter display responsive to config changes without waiting for a
    fresh scan of every band.
    """

    out: Dict[str, Dict] = {}
    for band, r in (results or {}).items():
        rr = dict(r or {})
        try:
            n = rr.get("avg_noise_floor_dbm")
            if n is not None:
                rr["avg_noise_s"] = dbm_to_s_units(float(n) + float(s_meter_offset_db))
        except Exception:
            pass

        # Derived “signal S” proxy: noise floor + p95_rel_db.
        # This usually tracks activity better than noise alone.
        try:
            n = rr.get("avg_noise_floor_dbm")
            rel = rr.get("p95_rel_db")
            if n is not None and rel is not None:
                rr["signal_s"] = dbm_to_s_units(float(n) + float(rel) + float(s_meter_offset_db))
        except Exception:
            pass

        # Preserve any additional derived values already present.
        try:
            p = rr.get("max_peak_dbm")
            if p is not None:
                rr["max_peak_s"] = dbm_to_s_units(float(p) + float(s_meter_offset_db))
        except Exception:
            pass
        try:
            p95 = rr.get("p95_dbm")
            if p95 is not None:
                rr["p95_s"] = dbm_to_s_units(float(p95) + float(s_meter_offset_db))
        except Exception:
            pass

        out[str(band)] = rr
    return out


def make_router(*, mgr: object, waterholes: Dict[str, float]) -> APIRouter:
    """Create router for GET /status.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()

    @router.get("/status")
    def get_status():
        with mgr.lock:  # type: ignore[attr-defined]
            return {
                "results": compute_s_metrics(mgr.results, s_meter_offset_db=float(mgr.s_meter_offset_db)),
                "current_band": mgr.current_band,
                "calibrating_band": mgr.calibrating_band,
                "last_updated": mgr.last_updated,
                "rx_chan": mgr.rx_chan,
                "camp_status": mgr.camp_status,
                "waterholes": waterholes,
                "threshold_db": mgr.threshold_db,
                "threshold_db_by_band": mgr.threshold_db_by_band,
                "s_meter_offset_db": mgr.s_meter_offset_db,
                "status_seq": mgr.status_seq,
                "status_time": time.time(),
            }

    return router
