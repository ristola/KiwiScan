"""Smart band-condition scheduler.

Merges three layers of evidence to decide which bands are "actively usable":

  1. Static seasonal tables (scheduler.py) — baseline
  2. Live propagation observations (ReceiverManager.health_summary()) — empirical
  3. User manual pin overrides (outputs/band_condition_overrides.json) — authoritative

Background thread re-evaluates every KIWISCAN_SMART_SCHED_INTERVAL_S seconds
(default 60).  When a band's merged condition changes the on_condition_change
callback fires so the AutoSetLoop can trigger an immediate force-reassign.

The FT8Modem / audio-pipe health is handled by ReceiverManager's built-in
stale-recovery and worker-watchdog loops; this module surfaces that data
through get_status() for the Pro UI.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
import math
from kiwi_scan.api.decodes import get_recent_decodes

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

from .scheduler import block_for_hour, expected_schedule, get_table, season_for_date

logger = logging.getLogger(__name__)

# Map ReceiverManager propagation_state → OPEN / MARGINAL / CLOSED.
# "unknown" is intentionally absent — we fall back to the seasonal table.
# "poor" maps to MARGINAL (not CLOSED) so that a band with low recent
# activity is shown as degraded in the UI but is never hard-excluded from
# automatic assignment — the seasonal schedule determines open/closed;
# empirical data only upgrades/downgrades within that framework.
_PROP_TO_CONDITION: Dict[str, str] = {
    "good": "OPEN",
    "fair": "OPEN",
    "marginal": "MARGINAL",
    "poor": "MARGINAL",
}
_CONDITION_RANK: Dict[str, int] = {"OPEN": 2, "MARGINAL": 1, "CLOSED": 0}

# Digital modes that supply propagation evidence.
_DIGITAL_MODES: frozenset[str] = frozenset({"FT8", "FT4", "WSPR"})

# All HF bands the scheduler knows about (ordered low→high on the dial).
_ALL_BANDS: tuple[str, ...] = (
    "10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"
)

# FT8 block start-hours (must match scheduler.py block_for_hour logic).
_FT8_BLOCK_STARTS: tuple[int, ...] = (0, 4, 8, 10, 16, 20)


# ---------------------------------------------------------------------------
# Band scoring
# ---------------------------------------------------------------------------

_SCORE_WEIGHT: Dict[str, float] = {"OPEN": 3.0, "MARGINAL": 1.0, "CLOSED": 0.0}


def _compute_band_score(
    band: str,
    merged_val: str,
    season: str,
    mode: str,
    local_dt: datetime,
) -> int:
    """Return a 0-100 integer band score for display.

    Uses *merged_val* (empirical > seasonal) for the current weight and
    the seasonal schedule for the adjacent-block carry factor so the score
    reflects both live conditions and near-future trajectory.

    Scale reference:  OPEN=91-100  MARGINAL=30-39  CLOSED=0-9
    """
    curr_w = _SCORE_WEIGHT.get(str(merged_val or "").upper(), 0.0)
    try:
        tbl = get_table(season, mode)
        block_key = block_for_hour(local_dt.hour, mode=mode)
        ordered = sorted(
            tbl.blocks.keys(),
            key=lambda k: int(k.split("-")[0]) if "-" in k else 0,
        )
        if not ordered:
            raise ValueError("empty blocks")
        idx = ordered.index(block_key) if block_key in ordered else 0
        prev_key = ordered[idx - 1]
        next_key = ordered[(idx + 1) % len(ordered)]
        prev_w = _SCORE_WEIGHT.get(
            str((tbl.blocks.get(prev_key) or {}).get(band, "")).upper(), 0.0
        )
        next_w = _SCORE_WEIGHT.get(
            str((tbl.blocks.get(next_key) or {}).get(band, "")).upper(), 0.0
        )
        if "-" in block_key:
            s, e = (float(x) for x in block_key.split("-", 1))
            dur = max(1.0, e - s)
            cur_h = float(local_dt.hour) + float(local_dt.minute) / 60.0
            if cur_h < s:
                cur_h += 24.0
            progress = max(0.0, min(1.0, (cur_h - s) / dur))
        else:
            progress = 0.5
        carry = (1.0 - progress) * prev_w + progress * next_w
        raw = curr_w * 100.0 + carry * 10.0
        return min(100, max(0, round(raw / 3.3)))
    except Exception:
        return {"OPEN": 91, "MARGINAL": 30, "CLOSED": 0}.get(
            str(merged_val or "").upper(), 0
        )


def _next_seasonal_change_for_band(
    band: str,
    current_seasonal: str,
    local_dt: datetime,
    mode: str = "ft8",
) -> "Optional[tuple[float, str]]":
    """Return (seconds_from_now, new_condition) for the next block boundary where
    the seasonal-table condition for *band* differs from *current_seasonal*.
    Looks up to 26 hours ahead.  Returns None if no change is found.
    """
    from datetime import timedelta  # noqa: PLC0415 — avoid circular at module level

    _ = mode
    now_ts = local_dt.timestamp()
    for day_offset in range(2):
        for start_h in _FT8_BLOCK_STARTS:
            future_dt = (
                local_dt.replace(hour=start_h, minute=0, second=0, microsecond=0)
                + timedelta(days=day_offset)
            )
            secs = future_dt.timestamp() - now_ts
            if secs <= 0 or secs > 26 * 3600:
                continue
            season = season_for_date(future_dt)
            block = block_for_hour(start_h, mode="ft8")
            try:
                table = get_table(season, "ft8")
            except KeyError:
                continue
            new_cond = str(table.blocks.get(block, {}).get(band, "")).upper()
            if new_cond and new_cond != current_seasonal:
                return (secs, new_cond)
    return None


def _empirical_from_health(health: Dict[str, Any]) -> Dict[str, str]:
    """Derive per-band empirical condition from a health_summary() snapshot.

    Only digital-mode channels contribute.  When multiple channels cover the
    same band the most-open observation wins (e.g. 20m FT8 OPEN beats 20m FT4
    MARGINAL → the band is reported OPEN overall).
    """
    channels = health.get("channels") if isinstance(health, dict) else {}
    if not isinstance(channels, dict):
        return {}

    result: Dict[str, str] = {}
    for _rx, ch in channels.items():
        if not isinstance(ch, dict):
            continue
        band = str(ch.get("band") or "").strip()
        if not band:
            continue
        mode = str(ch.get("mode") or "").strip().upper()
        if mode not in _DIGITAL_MODES:
            continue
        # Skip stalled / inactive channels — they don't have valid SNR data.
        health_state = str(ch.get("health_state") or "").lower()
        if health_state in {"inactive", "stalled"}:
            continue
        prop = str(ch.get("propagation_state") or "unknown").strip().lower()
        condition = _PROP_TO_CONDITION.get(prop)
        if condition is None:
            continue  # "unknown" → don't contradict seasonal table
        existing = result.get(band)
        if existing is None or _CONDITION_RANK[condition] > _CONDITION_RANK[existing]:
            result[band] = condition
    return result


class SmartScheduler:
    """Background band-condition monitor.

    Wires seasonal prediction, live decode evidence, and user pins into a
    single merged band-condition map.  When conditions change the optional
    on_condition_change callback fires so AutoSetLoop can force a receiver
    re-assignment.

    Typical lifecycle (from server.py):
        smart_scheduler = SmartScheduler(
            receiver_mgr=receiver_mgr,
            on_condition_change=auto_set_loop.force_reassign,
        )
        auto_set_loop.set_smart_scheduler(smart_scheduler)
        # started inside register_lifecycle startup handler
    """

    def __init__(
        self,
        *,
        receiver_mgr: Any,
        on_condition_change: Optional[Callable[[], None]] = None,
    ) -> None:
        self._receiver_mgr = receiver_mgr
        self._on_condition_change = on_condition_change
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._empirical: Dict[str, str] = {}
        self._last_check_ts: Optional[float] = None
        self._last_merged: Dict[str, str] = {}
        self._last_health_overall: str = "unknown"

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _overrides_path() -> Path:
        return Path(__file__).resolve().parents[2] / "outputs" / "band_condition_overrides.json"

    def _load_overrides(self) -> Dict[str, str]:
        try:
            path = self._overrides_path()
            if not path.exists():
                return {}
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {}
            return {
                str(k): str(v).upper()
                for k, v in data.items()
                if str(v).upper() in {"OPEN", "MARGINAL", "CLOSED"}
            }
        except Exception:
            return {}

    def _save_overrides(self, overrides: Dict[str, str]) -> None:
        try:
            path = self._overrides_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(overrides, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except Exception:
            logger.warning("SmartScheduler: failed to save band overrides", exc_info=True)

    # ------------------------------------------------------------------
    # Scan-config (band allowlist) persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _scan_config_path() -> Path:
        return Path(__file__).resolve().parents[2] / "outputs" / "band_scan_config.json"

    def _load_scan_config(self) -> Dict[str, Any]:
        try:
            path = self._scan_config_path()
            if not path.exists():
                return {}
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    def _save_scan_config(self, cfg: Dict[str, Any]) -> None:
        try:
            path = self._scan_config_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(cfg, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except Exception:
            logger.warning("SmartScheduler: failed to save scan config", exc_info=True)

    def _allowed_bands(self) -> "frozenset[str]":
        """Return the set of bands eligible for assignment.

        Defaults to all known bands if no allowlist has been saved yet.
        """
        cfg = self._load_scan_config()
        allowed = cfg.get("allowed_bands")
        if isinstance(allowed, list) and allowed:
            return frozenset(str(b).strip() for b in allowed if str(b).strip())
        return frozenset(_ALL_BANDS)

    # ------------------------------------------------------------------
    # Scan-config public API
    # ------------------------------------------------------------------

    def get_scan_config(self) -> Dict[str, Any]:
        """Return the current scan config (band allowlist)."""
        cfg = self._load_scan_config()
        allowed = cfg.get("allowed_bands")
        if not isinstance(allowed, list):
            allowed = list(_ALL_BANDS)
        return {"allowed_bands": allowed, "all_bands": list(_ALL_BANDS)}

    def set_scan_config(self, allowed_bands: list) -> None:
        """Persist a new band allowlist and fire the condition-change callback."""
        valid = [b for b in allowed_bands if str(b).strip() in _ALL_BANDS]
        self._save_scan_config({"allowed_bands": valid})
        logger.info("SmartScheduler: updated band allowlist → %s", valid)
        self._maybe_fire_change_callback()

    # ------------------------------------------------------------------
    # User override API
    # ------------------------------------------------------------------

    def set_override(self, band: str, condition: str) -> None:
        """Pin *band* to *condition* ("OPEN", "MARGINAL", or "CLOSED").

        Persists across restarts.  Fires the condition-change callback so an
        immediate force-reassign is triggered.
        """
        band = str(band or "").strip()
        condition = str(condition or "").strip().upper()
        if not band or condition not in {"OPEN", "MARGINAL", "CLOSED"}:
            return
        overrides = self._load_overrides()
        if overrides.get(band) == condition:
            return
        overrides[band] = condition
        self._save_overrides(overrides)
        logger.info("SmartScheduler: user pinned %s → %s", band, condition)
        self._maybe_fire_change_callback()

    def clear_override(self, band: str) -> None:
        """Remove a user-pinned condition for *band*."""
        band = str(band or "").strip()
        if not band:
            return
        overrides = self._load_overrides()
        if band not in overrides:
            return
        del overrides[band]
        self._save_overrides(overrides)
        logger.info("SmartScheduler: cleared user pin for %s", band)
        self._maybe_fire_change_callback()

    # ------------------------------------------------------------------
    # Condition assessment
    # ------------------------------------------------------------------

    def _check_once(self) -> None:
        """One assessment cycle: read health snapshot, update empirical state."""
        try:
            health = self._receiver_mgr.health_summary()
        except Exception:
            logger.debug("SmartScheduler: health_summary() failed", exc_info=True)
            return

        new_empirical = _empirical_from_health(health)
        health_overall = str(health.get("overall") or "unknown")

        with self._lock:
            old_empirical = dict(self._empirical)
            self._empirical = new_empirical
            self._last_check_ts = time.time()
            self._last_health_overall = health_overall

        local_dt = datetime.now().astimezone()
        old_merged = self._compute_merged(old_empirical, "ft8", local_dt)
        new_merged = self._compute_merged(new_empirical, "ft8", local_dt)
        with self._lock:
            self._last_merged = dict(new_merged)
        if new_merged != old_merged:
            logger.info("SmartScheduler: FT8 band conditions changed — %s", new_merged)
            self._maybe_fire_change_callback()

    def _compute_merged(
        self,
        empirical: Dict[str, str],
        mode: str,
        local_dt: datetime,
    ) -> Dict[str, str]:
        """Merge seasonal baseline + empirical + user overrides.

        Priority: user_override > empirical > seasonal.
        """
        try:
            seasonal = expected_schedule(mode=mode, local_dt=local_dt)
        except Exception:
            seasonal = {}
        overrides = self._load_overrides()
        merged: Dict[str, str] = {}
        for band in set(seasonal) | set(empirical) | set(overrides):
            if band in overrides:
                merged[band] = overrides[band]
            elif band in empirical:
                merged[band] = empirical[band]
            else:
                merged[band] = str(seasonal.get(band, "UNKNOWN")).upper()
        return merged

    # ------------------------------------------------------------------
    # Public accessors (called by AutoSetLoop every cycle)
    # ------------------------------------------------------------------

    def merged_conditions(self, mode: str = "ft8") -> Dict[str, str]:
        """Return the current merged band-condition map for *mode*."""
        _ = mode
        with self._lock:
            empirical = dict(self._empirical)
        return self._compute_merged(empirical, "ft8", datetime.now().astimezone())

    def get_closed_bands(self, mode: str = "ft8") -> Set[str]:
        """Return the set of bands that should not be assigned a decoder.

        Includes bands whose merged condition is CLOSED *and* any band that
        is not in the user's configured allowlist (treated as permanently
        closed if the user has excluded it).
        Called each AutoSetLoop cycle to build the ``closed_bands`` payload
        field passed to /auto_set_receivers.
        """
        _ = mode
        allowed = self._allowed_bands()
        result: Set[str] = set()
        for band, condition in self.merged_conditions("ft8").items():
            if band not in allowed or condition == "CLOSED":
                result.add(band)
        # Also close bands that appear in the allowlist but have no condition entry
        for band in _ALL_BANDS:
            if band not in allowed:
                result.add(band)
        return result

    # ------------------------------------------------------------------
    # Callback
    # ------------------------------------------------------------------

    def _maybe_fire_change_callback(self) -> None:
        if self._on_condition_change is not None:
            try:
                self._on_condition_change()
            except Exception:
                logger.debug("SmartScheduler: on_condition_change callback raised", exc_info=True)

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    @staticmethod
    def _interval_s() -> float:
        raw = str(os.environ.get("KIWISCAN_SMART_SCHED_INTERVAL_S", "60") or "60").strip()
        try:
            return max(15.0, min(600.0, float(raw)))
        except Exception:
            return 60.0

    def _run(self) -> None:
        logger.info("SmartScheduler started (interval=%ss)", self._interval_s())
        # 30-second warm-up so receivers have time to produce initial decodes.
        self._stop.wait(timeout=30.0)
        if not self._stop.is_set():
            self._check_once()
        while not self._stop.is_set():
            self._stop.wait(timeout=self._interval_s())
            if not self._stop.is_set():
                self._check_once()
        logger.info("SmartScheduler stopped")

    def force_check(self) -> None:
        """Trigger an immediate condition check outside the normal schedule."""
        threading.Thread(
            target=self._check_once,
            name="kiwi-scan-smart-scheduler-force",
            daemon=True,
        ).start()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="kiwi-scan-smart-scheduler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None

    # ------------------------------------------------------------------
    # Status snapshot (consumed by GET /smart_scheduler/status)
    # ------------------------------------------------------------------

    def get_status(self, mode: str = "ft8") -> Dict[str, Any]:
        _ = mode
        with self._lock:
            empirical = dict(self._empirical)
            last_check_ts = self._last_check_ts
            health_overall = str(self._last_health_overall)

        overrides = self._load_overrides()
        allowed = self._allowed_bands()
        local_dt = datetime.now().astimezone()
        season = season_for_date(local_dt)

        merged = self._compute_merged(empirical, "ft8", local_dt)
        try:
            seasonal = expected_schedule(mode="ft8", local_dt=local_dt)
        except Exception:
            seasonal = {}

        conditions: Dict[str, Any] = {}
        for band in sorted(set(seasonal) | set(empirical) | set(overrides)):
            if band not in allowed:
                continue
            merged_val = merged.get(band, "UNKNOWN")
            source: str
            if band in overrides:
                source = "user_override"
            elif band in empirical:
                source = "empirical"
            else:
                source = "seasonal"
            seasonal_val = str(seasonal.get(band, "UNKNOWN")).upper()
            next_chg = _next_seasonal_change_for_band(band, seasonal_val, local_dt, mode="ft8")
            conditions[band] = {
                "merged": merged_val,
                "seasonal": seasonal_val,
                "empirical": empirical.get(band),
                "user_override": overrides.get(band),
                "source": source,
                "next_seasonal_change_in_s": int(next_chg[0]) if next_chg else None,
                "next_seasonal_condition": next_chg[1] if next_chg else None,
                "score": _compute_band_score(band, merged_val, season, "ft8", local_dt),
            }

        return {
            "running": bool(self._thread is not None and self._thread.is_alive()),
            "receiver_health_overall": health_overall,
            "last_check_ts": last_check_ts,
            "interval_s": self._interval_s(),
            "mode": "ft8",
            "conditions": conditions,
            "closed_bands": sorted(b for b, c in merged.items() if c == "CLOSED" and b in allowed),
            "open_bands": sorted(b for b, c in merged.items() if c == "OPEN" and b in allowed),
            "marginal_bands": sorted(b for b, c in merged.items() if c == "MARGINAL" and b in allowed),
            "allowed_bands": sorted(allowed),
        }
    def _compute_smart_score(self, band: str, recent_decodes: list[Dict[str, Any]], current_roaming: list[str]) -> float:
        import math
        from datetime import datetime, timezone
        
        band_decodes = [d for d in recent_decodes if d.get("band") == band]
        
        # 1. live_activity_score
        count = len(band_decodes)
        live_activity_score = min(count * 2.0, 50.0)
        
        # 2. unique_station_score
        callsigns = {d.get("callsign") for d in band_decodes if d.get("callsign")}
        unique_station_score = min(len(callsigns) * 1.5, 30.0)
        
        # 3. snr_score
        snrs = [float(d["snr"]) for d in band_decodes if d.get("snr") is not None]
        if snrs:
            avg_snr = sum(snrs) / len(snrs)
            # Map -24 to +10 roughly to 0-10
            snr_score = max(0.0, min((avg_snr + 24) * (10.0 / 34.0), 10.0))
        else:
            snr_score = 0.0
            
        # 4. distance_score
        dists = [float(d["dist_km"]) for d in band_decodes if d.get("dist_km") is not None]
        if dists:
            max_dist = max(dists)
            # 1 point per 1000km, up to 10 points
            distance_score = min(max_dist / 1000.0, 10.0)
        else:
            distance_score = 0.0
            
        # 5. persistence_score
        if band_decodes:
            times = [d.get("epoch_ts", 0) for d in band_decodes]
            time_spread = max(times) - min(times)
            # Max 5 points for a full 15-minute spread
            persistence_score = min((time_spread / 900.0) * 5.0, 5.0)
        else:
            persistence_score = 0.0
            
        # 6. mode_diversity_score
        modes = {str(d.get("mode") or "").upper() for d in band_decodes}
        if len(modes) > 1:
            mode_diversity_score = 5.0
        elif len(modes) == 1:
            mode_diversity_score = 2.0
        else:
            mode_diversity_score = 0.0

        # 7. time_of_day_bonus (baseline logic)
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        is_day = 6 <= hour < 18
        time_of_day_bonus = 0.0
        day_bands = {"10m", "12m", "15m", "17m", "20m"}
        night_bands = {"30m", "40m", "60m", "80m", "160m"}
        if is_day and band in day_bands:
            time_of_day_bonus = 10.0
        elif not is_day and band in night_bands:
            time_of_day_bonus = 10.0
            
        # 8. solar_bonus
        # Placeholder - maybe we can query an API later
        solar_bonus = 0.0
        
        # 9. grayline_bonus
        # Normally sunrise is ~06:00 UTC and sunset is ~18:00 UTC in a generic model, add a +/- 1 hr window.
        grayline_bonus = 0.0
        if band in {"30m", "40m"}:
            if (5 <= hour <= 7) or (17 <= hour <= 19):
                grayline_bonus = 5.0

        smart_score = (
            live_activity_score +
            unique_station_score +
            snr_score +
            distance_score +
            persistence_score +
            mode_diversity_score +
            time_of_day_bonus +
            solar_bonus +
            grayline_bonus
        )
        
        # Avoid flapping: add hysteresis memory if already roaming here
        if band in current_roaming:
            smart_score += 15.0
            
        return max(0.0, smart_score)

    def rank_roaming_bands(self, available_bands: list[str], current_roaming: list[str]) -> list[str]:
        """Rank available bands using the smart score.
        Calculates all variables: live_activity, unique calls, snr, distance, setup, etc.
        """
        # Fetch decodes from the last 15 minutes
        try:
            from kiwi_scan.api.decodes import get_recent_decodes
            recent_decodes = get_recent_decodes(900)
        except Exception:
            recent_decodes = []
            
        ranked = []
        for band in available_bands:
            score = self._compute_smart_score(band, recent_decodes, current_roaming)
            # Add a tiny tie-breaker
            ranked.append((score, band))
            
        # Sort descending by score
        ranked.sort(key=lambda x: x[0], reverse=True)
        return [band for score, band in ranked]