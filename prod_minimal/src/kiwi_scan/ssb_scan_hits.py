from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, List

_lock = threading.Lock()
_seq = 0
_hits: Deque[Dict] = deque(maxlen=500)
_status: Dict[str, Dict] = {}
_last_logged: Dict[str, tuple] = {}
_last_logged_rssi: Dict[str, float | None] = {}
_last_logged_freq: Dict[str, float] = {}
_pending_status: Dict[str, Dict] = {}


def log_ssb_scan_hit(
    *,
    band: str,
    rx: int | None,
    freq_khz: float | None,
    step_khz: float | None,
    sideband: str | None = None,
    threshold_db: float | None = None,
) -> None:
    global _seq
    payload = {
        "id": None,
        "ts": time.time(),
        "kind": "hit",
        "band": str(band) if band is not None else None,
        "rx": int(rx) if rx is not None else None,
        "freq_khz": float(freq_khz) if freq_khz is not None else None,
        "step_khz": float(step_khz) if step_khz is not None else None,
        "sideband": str(sideband).upper() if sideband else None,
        "threshold_db": float(threshold_db) if threshold_db is not None else None,
    }
    with _lock:
        _seq += 1
        payload["id"] = _seq
        _hits.append(payload)


def get_ssb_scan_hits(*, since: int = 0) -> Dict[str, object]:
    with _lock:
        items: List[Dict] = [h for h in list(_hits) if int(h.get("id", 0)) > int(since or 0)]
        latest = _seq
    return {"latest": latest, "items": items}


def update_ssb_scan_status(
    *,
    band: str,
    rx: int | None,
    freq_khz: float | None,
    rssi_db: float | None,
    step_khz: float | None,
    sideband: str | None = None,
    threshold_db: float | None = None,
) -> None:
    payload = {
        "ts": time.time(),
        "kind": "status",
        "band": str(band) if band is not None else None,
        "rx": int(rx) if rx is not None else None,
        "freq_khz": float(freq_khz) if freq_khz is not None else None,
        "rssi_db": float(rssi_db) if rssi_db is not None else None,
        "step_khz": float(step_khz) if step_khz is not None else None,
        "sideband": str(sideband).upper() if sideband else None,
        "threshold_db": float(threshold_db) if threshold_db is not None else None,
    }
    with _lock:
        if not band:
            return
        band_key = str(band)
        _status[band_key] = payload
        freq_value = payload.get("freq_khz")
        rssi_value = payload.get("rssi_db")

        if band_key in _pending_status:
            pending = _pending_status[band_key]
            pending_freq = pending.get("freq_khz")
            if freq_value is not None and pending_freq is not None:
                if round(float(freq_value), 1) != round(float(pending_freq), 1):
                    sig = (
                        pending.get("rx"),
                        round(float(pending_freq), 1),
                        round(pending.get("step_khz"), 2) if pending.get("step_khz") is not None else None,
                        pending.get("sideband"),
                        round(pending.get("threshold_db"), 1) if pending.get("threshold_db") is not None else None,
                    )
                    if _last_logged.get(band_key) != sig:
                        _last_logged[band_key] = sig
                        _last_logged_freq[band_key] = round(float(pending_freq), 1)
                        _last_logged_rssi[band_key] = pending.get("rssi_db")
                        global _seq
                        _seq += 1
                        pending["id"] = _seq
                        _hits.append(dict(pending))
                    _pending_status.pop(band_key, None)

        current = _pending_status.get(band_key)
        if current is None:
            if freq_value is None:
                return
            _pending_status[band_key] = dict(payload)
            return

        if freq_value is not None:
            current["freq_khz"] = freq_value
        if payload.get("step_khz") is not None:
            current["step_khz"] = payload.get("step_khz")
        if payload.get("sideband"):
            current["sideband"] = payload.get("sideband")
        if payload.get("threshold_db") is not None:
            current["threshold_db"] = payload.get("threshold_db")
        if rssi_value is not None:
            prev_rssi = current.get("rssi_db")
            if prev_rssi is None or float(rssi_value) > float(prev_rssi):
                current["rssi_db"] = rssi_value
        current["ts"] = payload.get("ts")


def clear_ssb_scan_hits() -> None:
    global _seq
    with _lock:
        _seq = 0
        _hits.clear()
        _status.clear()
        _last_logged.clear()
        _last_logged_rssi.clear()
        _last_logged_freq.clear()
        _pending_status.clear()
