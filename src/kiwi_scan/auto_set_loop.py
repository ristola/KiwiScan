from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict

from typing import Any
from .scheduler import block_for_hour
from .targeted_service_registry import resolve_targeted_service

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fixed receiver assignments (RX2-RX7) — always active when Auto Mode is ON.
# freq_hz is the IQ centre for multi-mode slots, dial frequency for single-mode.
# ---------------------------------------------------------------------------
_FIXED_ASSIGNMENTS = [
    # RX2: 20m FT4+FT8 IQ dual — centre of 14.074 MHz (FT8) and 14.080 MHz (FT4)
    {"rx": 2, "band": "20m", "mode": "FT4 / FT8",        "freq_hz": 14_077_000.0},
    # RX3: 20m WSPR
    {"rx": 3, "band": "20m", "mode": "WSPR",              "freq_hz": 14_095_600.0},
    # RX4: 40m FT8
    {"rx": 4, "band": "40m", "mode": "FT8",               "freq_hz":  7_074_000.0},
    # RX5: 40m FT4+WSPR IQ dual — centre of 7.0475 MHz (FT4) and 7.0386 MHz (WSPR)
    {"rx": 5, "band": "40m", "mode": "FT4 / WSPR",        "freq_hz":  7_043_050.0},
    # RX6: 30m ALL (FT4+FT8+WSPR triple IQ) — centre of 10.136–10.140 MHz span
    {"rx": 6, "band": "30m", "mode": "FT4 / FT8 / WSPR",  "freq_hz": 10_138_000.0},
    # RX7: 17m ALL (FT4+FT8+WSPR triple IQ) — centre of 18.100–18.104 MHz span
    {"rx": 7, "band": "17m", "mode": "FT4 / FT8 / WSPR",  "freq_hz": 18_102_000.0},
]

# Roaming schedule for RX0-RX1 (day = 07:00–20:59 local, night otherwise)
_ROAMING_DAY = [
    {"band": "10m", "mode": "FT8"},
    {"band": "12m", "mode": "FT4 / FT8"},
    {"band": "15m", "mode": "FT8"},
]
_ROAMING_NIGHT = [
    {"band": "60m", "mode": "FT8"},
    {"band": "80m", "mode": "FT4 / FT8 / WSPR"},
    {"band": "160m", "mode": "FT4 / FT8 / WSPR"},
]


class AutoSetLoop:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._did_startup_apply = False
        self._last_schedule_key: tuple[str, str] | None = None
        self._last_apply_signature: str | None = None
        self._manual_mode_cleared = False
        self._last_active_kiwi_key: str = ""
        self._state_lock = threading.Lock()
        self._per_kiwi_state: Dict[str, Dict[str, Any]] = {}
        self._last_run_ts: float | None = None
        self._last_success_ts: float | None = None
        self._last_error: str | None = None
        self._smart_scheduler: Any | None = None
        self._last_applied_band_config: str | None = None
        # After a force_health_recovery re-apply, back off for this many seconds
        # before checking health again.  This prevents thundering-herd restarts while
        # workers are still settling into their correct Kiwi slots (eviction loop can
        # take up to ~3 min for 8 receivers over VPN).
        self._recovery_backoff_until_ts: float = 0.0
        self._RECOVERY_BACKOFF_S: float = 180.0
        self._ROAMING_RECOVERY_BACKOFF_S: float = 30.0
        self._EMPTY_ROAMING_RECHECK_BACKOFF_S: float = 30.0
        self._FIXED_INVISIBLE_GRACE_S: float = 30.0
        self._external_hold_reason: str | None = None
        self._external_hold_by_kiwi: Dict[str, str] = {}

    def set_smart_scheduler(self, smart_scheduler: Any) -> None:
        """Bind a SmartScheduler instance so closed bands are filtered each cycle."""
        self._smart_scheduler = smart_scheduler

    def _smart_scheduler_for_kiwi(self, kiwi_key: str | None = None) -> Any | None:
        if self._smart_scheduler is None:
            return None
        target_key = self._normalize_kiwi_key(kiwi_key)
        target = {"kiwi_key": target_key} if target_key else None
        try:
            return resolve_targeted_service(self._smart_scheduler, target=target)
        except Exception:
            return self._smart_scheduler

    @staticmethod
    def _settings_path() -> Path:
        root = Path(__file__).resolve().parents[2]
        return root / "outputs" / "automation_settings.json"

    @staticmethod
    def _configured_kiwi_keys_from_config() -> set[str] | None:
        root = Path(__file__).resolve().parents[2]
        path = root / "outputs" / "config.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        raw_kiwis = data.get("kiwisdrs") if isinstance(data, dict) else None
        if raw_kiwis is None:
            return None
        if not isinstance(raw_kiwis, list):
            return set()
        keys: set[str] = set()
        for entry in raw_kiwis:
            if not isinstance(entry, dict):
                continue
            host = self_host = str(entry.get("host") or "").strip()
            if not host:
                continue
            try:
                port = int(entry.get("port") or 8073)
            except Exception:
                port = 8073
            keys.add(f"{self_host}:{port}")
        return keys

    @staticmethod
    def _safe_bool(value: object, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(default)

    @staticmethod
    def _safe_num(value: object, default: float, min_v: float, max_v: float) -> float:
        try:
            numeric_value = value if isinstance(value, (int, float, str)) else default
            v = float(numeric_value)
        except Exception:
            v = float(default)
        v = max(min_v, min(max_v, v))
        return v

    def _load_settings(self) -> Dict[str, Any]:
        path = self._settings_path()
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
        return {}

    @staticmethod
    def _normalize_kiwi_key(value: object | None) -> str:
        return str(value or "").strip()

    def _kiwi_state_locked(self, kiwi_key: str) -> Dict[str, Any]:
        key = self._normalize_kiwi_key(kiwi_key)
        state = self._per_kiwi_state.get(key)
        if state is None:
            state = {
                "did_startup_apply": False,
                "last_schedule_key": None,
                "last_apply_signature": None,
                "manual_mode_cleared": False,
                "last_applied_band_config": None,
                "recovery_backoff_until_ts": 0.0,
                "last_success_ts": None,
                "last_error": None,
            }
            self._per_kiwi_state[key] = state
        return state

    def _reset_kiwi_state_locked(self, kiwi_key: str, *, manual_mode_cleared: bool = False) -> Dict[str, Any]:
        state = self._kiwi_state_locked(kiwi_key)
        state["did_startup_apply"] = False
        state["last_schedule_key"] = None
        state["last_apply_signature"] = None
        state["manual_mode_cleared"] = bool(manual_mode_cleared)
        state["last_applied_band_config"] = None
        state["recovery_backoff_until_ts"] = 0.0
        state["last_error"] = None
        return state

    def _sync_selected_kiwi_state_locked(self, settings: Dict[str, Any]) -> None:
        active_key = self._normalize_kiwi_key(settings.get("activeKiwiKey"))
        state = self._kiwi_state_locked(active_key)
        self._last_active_kiwi_key = active_key
        self._did_startup_apply = bool(state.get("did_startup_apply"))
        self._last_schedule_key = state.get("last_schedule_key")
        self._last_apply_signature = state.get("last_apply_signature")
        self._manual_mode_cleared = bool(state.get("manual_mode_cleared"))
        self._last_applied_band_config = state.get("last_applied_band_config")
        self._recovery_backoff_until_ts = float(state.get("recovery_backoff_until_ts") or 0.0)
        self._last_success_ts = state.get("last_success_ts")
        self._last_error = state.get("last_error")

    def _clear_all_kiwi_state_locked(self, *, manual_mode_cleared: bool = False) -> None:
        for kiwi_key in list(self._per_kiwi_state.keys()):
            self._reset_kiwi_state_locked(kiwi_key, manual_mode_cleared=manual_mode_cleared)
        self._did_startup_apply = False
        self._last_schedule_key = None
        self._last_apply_signature = None
        self._manual_mode_cleared = bool(manual_mode_cleared)
        self._last_applied_band_config = None
        self._recovery_backoff_until_ts = 0.0
        self._last_error = None

    def _target_kiwi_keys(self, settings: Dict[str, Any]) -> list[str]:
        all_keys: list[str] = []
        all_seen: set[str] = set()
        keys: list[str] = []
        seen: set[str] = set()
        configured_keys = self._configured_kiwi_keys_from_config()

        def _append_all(key: str) -> None:
            if key and key not in all_seen:
                all_keys.append(key)
                all_seen.add(key)

        kiwi_modes = settings.get("kiwiModes")
        if isinstance(kiwi_modes, dict):
            for raw_key in kiwi_modes.keys():
                key = self._normalize_kiwi_key(raw_key)
                if not key:
                    continue
                _append_all(key)
                if key in seen:
                    continue
                if configured_keys is not None and key not in configured_keys:
                    continue
                keys.append(key)
                seen.add(key)
        active_key = self._normalize_kiwi_key(settings.get("activeKiwiKey"))
        if active_key:
            _append_all(active_key)
            if active_key not in seen and (configured_keys is None or active_key in configured_keys):
                keys.append(active_key)
                seen.add(active_key)
        if not keys and all_keys:
            return all_keys
        if not keys:
            keys.append("")
        return keys

    def notify_settings_changed(self) -> None:
        self._wake.set()

    def pause_for_external(self, reason: str = "external", kiwi_key: str | None = None) -> None:
        with self._state_lock:
            hold_reason = str(reason or "external")
            key = str(kiwi_key or "").strip()
            if key:
                self._external_hold_by_kiwi[key] = hold_reason
            else:
                self._external_hold_reason = hold_reason
            self._manual_mode_cleared = True
            self._did_startup_apply = False
            self._last_schedule_key = None
            self._last_apply_signature = None
            self._last_applied_band_config = None
        self.notify_settings_changed()

    def resume_from_external(self, reason: str | None = None, kiwi_key: str | None = None) -> None:
        with self._state_lock:
            changed = False
            key = str(kiwi_key or "").strip()
            if key:
                current_reason = self._external_hold_by_kiwi.get(key)
                if current_reason is not None and (reason is None or str(reason) == str(current_reason)):
                    self._external_hold_by_kiwi.pop(key, None)
                    changed = True
            else:
                current_reason = self._external_hold_reason
                if current_reason is not None and (reason is None or str(reason) == str(current_reason)):
                    self._external_hold_reason = None
                    changed = True
                if reason is None:
                    if self._external_hold_by_kiwi:
                        self._external_hold_by_kiwi.clear()
                        changed = True
                else:
                    to_clear = [k for k, r in self._external_hold_by_kiwi.items() if str(r) == str(reason)]
                    if to_clear:
                        for k in to_clear:
                            self._external_hold_by_kiwi.pop(k, None)
                        changed = True
            if not changed:
                return
            self._manual_mode_cleared = False
            self._did_startup_apply = False
            self._last_schedule_key = None
            self._last_apply_signature = None
            self._last_applied_band_config = None
        self.notify_settings_changed()

    def _external_hold_reason_for_kiwi_locked(self, kiwi_key: str) -> str | None:
        key = self._normalize_kiwi_key(kiwi_key)
        if key and key in self._external_hold_by_kiwi:
            return self._external_hold_by_kiwi[key]
        return None

    def _wait_for_notification(self, timeout_s: float | None = None) -> None:
        self._wake.wait(timeout=timeout_s)
        self._wake.clear()

    @staticmethod
    def _auto_set_url() -> str:
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        return f"http://127.0.0.1:{port}/auto_set_receivers"

    @staticmethod
    def _loop_interval_s() -> float:
        raw = str(os.environ.get("KIWISCAN_AUTOSET_LOOP_S", "30") or "30").strip()
        try:
            value = float(raw)
        except Exception:
            value = 30.0
        return max(5.0, min(600.0, value))

    @staticmethod
    def _enabled_by_env() -> bool:
        raw = str(os.environ.get("KIWISCAN_AUTOSET_LOOP", "1") or "1").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def _profile_selection_for_block(
        settings: Dict[str, Any],
        *,
        mode: str,
        block: str,
    ) -> tuple[list[str] | None, Dict[str, str] | None]:
        raw_profiles = settings.get("scheduleProfiles")
        if not isinstance(raw_profiles, dict):
            return None, None
        by_mode = raw_profiles.get(str(mode).lower())
        if not isinstance(by_mode, dict):
            return None, None
        entry = by_mode.get(str(block))
        if not isinstance(entry, dict):
            # Fall back to the nearest prior block (by start hour).
            try:
                target_start = int(str(block).split("-")[0])
            except Exception:
                return None, None
            candidates: list[tuple[int, dict]] = []
            for k, v in by_mode.items():
                if not isinstance(v, dict):
                    continue
                try:
                    cand_start = int(str(k).split("-")[0])
                except Exception:
                    continue
                candidates.append((cand_start, v))
            candidates.sort(key=lambda x: x[0])
            prior = [(s, e) for s, e in candidates if s <= target_start]
            if prior:
                entry = prior[-1][1]
            elif candidates:
                entry = candidates[-1][1]
            else:
                return None, None

        selected: list[str] | None = None
        selected_raw = entry.get("selectedBands")
        if isinstance(selected_raw, list):
            selected = []
            seen: set[str] = set()
            for item in selected_raw:
                band = str(item or "").strip()
                if not band or band in seen:
                    continue
                selected.append(band)
                seen.add(band)

        band_modes: Dict[str, str] | None = None
        band_modes_raw = entry.get("bandModes")
        if isinstance(band_modes_raw, dict):
            band_modes = {}
            for k, v in band_modes_raw.items():
                band = str(k or "").strip()
                mode_text = str(v or "").strip().upper()
                if not band or not mode_text:
                    continue
                band_modes[band] = mode_text

        return selected, band_modes

    def _build_payload(
        self,
        settings: Dict[str, Any],
        schedule_key: tuple[str, str] | None = None,
        *,
        kiwi_key: str | None = None,
    ) -> Dict[str, Any]:
        smart_scheduler = self._smart_scheduler_for_kiwi(kiwi_key)
        if schedule_key is None:
            schedule_key = self._current_schedule_key(settings, kiwi_key=kiwi_key)

        if schedule_key[0] == "fixed":
            return self._build_fixed_roaming_payload(settings, schedule_key[1], kiwi_key=kiwi_key)

        mode = "ft8"

        # Always use the current time's block for band selection regardless of the
        # passed schedule_key.  The loop passes the current key for mode-routing, but
        # callers should not be able to select bands from a different time window.
        current_block = block_for_hour(datetime.now().astimezone().hour, mode=mode)
        selected_bands, band_modes = self._profile_selection_for_block(
            settings,
            mode=mode,
            block=current_block,
        )

        payload: Dict[str, Any] = {
            "enabled": True,
            "mode": mode,
            "block": str(current_block),
        }
        if isinstance(selected_bands, list):
            # Filter against the user's band allowlist ("On" checkboxes in Band Schedule).
            if smart_scheduler is not None:
                try:
                    allowed = smart_scheduler._allowed_bands()
                    selected_bands = [b for b in selected_bands if b in allowed]
                except Exception:
                    pass
            payload["selected_bands"] = list(selected_bands)
        if isinstance(band_modes, dict):
            payload["band_modes"] = dict(band_modes)

        # Ask SmartScheduler which bands are empirically/seasonally closed and
        # pass them to /auto_set_receivers so receivers aren't wasted on dead bands.
        if smart_scheduler is not None:
            try:
                closed = list(smart_scheduler.get_closed_bands(mode))
                if closed:
                    payload["closed_bands"] = closed
            except Exception:
                pass

        return payload

    def _build_fixed_roaming_payload(self, settings: Dict[str, Any], day_night: str, *, kiwi_key: str | None = None) -> Dict[str, Any]:
        """Build a payload with fixed RX2-RX7 plus scored RX0/RX1 roaming.

        RX0/RX1 stay empty until all 6 mandatory fixed receivers are healthy. Once
        healthy, SmartScheduler ranks only the configured day/night roaming pool and
        fills the 2 spare receivers with the top-scored bands.
        """
        if not self._roaming_enabled(settings, kiwi_key=kiwi_key):
            return {
                "enabled": True,
                "mode": "ft8",
                "block": day_night,
                "fixed_assignments": list(_FIXED_ASSIGNMENTS),
                "selected_bands": [],
            }

        roaming = _ROAMING_DAY if day_night == "day" else _ROAMING_NIGHT
        smart_scheduler = self._smart_scheduler_for_kiwi(kiwi_key)
        _fixed_bands = {str(a["band"]).strip().lower() for a in _FIXED_ASSIGNMENTS}
        base_roaming_pool = [str(r["band"]) for r in roaming if str(r["band"]).strip().lower() not in _fixed_bands]
        base_band_modes: Dict[str, str] = {
            str(r["band"]): str(r["mode"])
            for r in roaming
            if str(r["band"]).strip().lower() not in _fixed_bands
        }
        roaming_pool = list(base_roaming_pool)
        band_modes: Dict[str, str] = dict(base_band_modes)
        closed_bands: list[str] = []
        if smart_scheduler is not None:
            try:
                closed_lookup = {
                    str(band).strip()
                    for band in smart_scheduler.get_closed_bands("ft8")
                    if str(band).strip()
                }
                roaming_pool = [band for band in roaming_pool if band not in closed_lookup]
                band_modes = {band: mode for band, mode in band_modes.items() if band not in closed_lookup}
                closed_bands = sorted(closed_lookup)
            except Exception:
                closed_bands = []
        selected_bands: list[str] = []
        num_roaming_slots = 2
        current_roaming = [
            band
            for band in self._current_roaming_bands(kiwi_key=kiwi_key)
            if band in base_band_modes
        ]

        fixed_health_state, _sick_fixed = self._fixed_health_state(kiwi_key=kiwi_key)
        if smart_scheduler is not None:
            try:
                ranked_bands = smart_scheduler.rank_roaming_bands(
                    available_bands=list(roaming_pool),
                    current_roaming=None,
                )
                selected_bands = [b for b in ranked_bands if b in band_modes][:num_roaming_slots]
            except Exception:
                selected_bands = []

        if len(selected_bands) < num_roaming_slots:
            fallback = [b for b in roaming_pool if b not in selected_bands]
            selected_bands.extend(fallback[:(num_roaming_slots - len(selected_bands))])

        if len(selected_bands) < num_roaming_slots:
            fallback_closed = [b for b in base_roaming_pool if b not in selected_bands]
            if fallback_closed:
                logger.info(
                    "Only %d roaming band(s) remained after closed-band filtering; "
                    "using fallback bands to keep RX0/RX1 populated: %s",
                    len(selected_bands),
                    fallback_closed,
                )
                selected_bands.extend(fallback_closed[:(num_roaming_slots - len(selected_bands))])

        band_modes = dict(band_modes)
        for band in selected_bands:
            if band in base_band_modes:
                band_modes.setdefault(band, base_band_modes[band])

        if fixed_health_state != "healthy":
            logger.info(
                "Fixed receiver health=%s; keeping RX0/RX1 empty until fixed slots recover",
                fixed_health_state,
            )
            selected_bands = []
            band_modes = {}

        payload: Dict[str, Any] = {
            "enabled": True,
            "mode": "ft8",
            "block": day_night,
            "fixed_assignments": list(_FIXED_ASSIGNMENTS),
            "selected_bands": selected_bands,
            "band_modes": band_modes,
        }
        if closed_bands:
            payload["closed_bands"] = closed_bands
        return payload

    def _current_roaming_bands(self, kiwi_key: str | None = None) -> list[str]:
        """Return the currently active RX0/RX1 roaming bands from /health/rx."""
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        try:
            url = self._service_url("/health/rx", kiwi_key=kiwi_key)
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return []
        if not isinstance(data, dict) or bool(data.get("_from_cache")):
            return []
        channels = data.get("channels")
        if not isinstance(channels, dict):
            return []
        current: list[str] = []
        seen: set[str] = set()
        for rx in (0, 1):
            ch = channels.get(str(rx))
            if not isinstance(ch, dict):
                continue
            band = str(ch.get("band") or "").strip()
            if not band or band in seen:
                continue
            current.append(band)
            seen.add(band)
        return current

    @staticmethod
    def _receivers_mode(settings: Dict[str, Any], kiwi_key: str | None = None) -> str:
        kiwi_key = str(kiwi_key if kiwi_key is not None else settings.get("activeKiwiKey") or "").strip()
        kiwi_modes = settings.get("kiwiModes")
        if kiwi_key and isinstance(kiwi_modes, dict) and kiwi_key in kiwi_modes:
            per_kiwi = str(kiwi_modes[kiwi_key] or "").strip().lower()
            if per_kiwi in {"auto", "semi", "manual", "scan"}:
                return per_kiwi
        raw = str(settings.get("receiversMode") or "").strip().lower()
        if raw in {"auto", "semi", "manual", "scan"}:
            return raw
        if not AutoSetLoop._safe_bool(settings.get("fixedModeEnabled"), default=True):
            return "manual"
        return "auto"

    @staticmethod
    def _roaming_enabled(settings: Dict[str, Any], kiwi_key: str | None = None) -> bool:
        return AutoSetLoop._receivers_mode(settings, kiwi_key=kiwi_key) == "auto"

    @staticmethod
    def _current_schedule_key(settings: Dict[str, Any], kiwi_key: str | None = None) -> tuple[str, str]:
        receivers_mode = AutoSetLoop._receivers_mode(settings, kiwi_key=kiwi_key)
        if receivers_mode == "scan":
            return ("scan", "parked")
        if receivers_mode in {"auto", "semi"}:
            local_hour = datetime.now().astimezone().hour
            day_night = "day" if 7 <= local_hour < 21 else "night"
            return ("fixed", day_night)
        local_dt = datetime.now().astimezone()
        return ("ft8", block_for_hour(local_dt.hour, mode="ft8"))

    @staticmethod
    def _apply_signature(settings: Dict[str, Any], schedule_key: tuple[str, str], *, kiwi_key: str | None = None) -> str:
        relevant = {
            "kiwi_key": str(kiwi_key or ""),
            "schedule_key": [str(schedule_key[0]), str(schedule_key[1])],
            "scheduleProfiles": settings.get("scheduleProfiles"),
            "fixedModeEnabled": settings.get("fixedModeEnabled"),
            "receiversMode": AutoSetLoop._receivers_mode(settings, kiwi_key=kiwi_key),
        }
        return json.dumps(relevant, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _service_url(path: str, *, kiwi_key: str | None = None) -> str:
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        url = f"http://127.0.0.1:{port}{path}"
        target_key = str(kiwi_key or "").strip()
        if not target_key:
            return url
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}kiwi_key={urllib.parse.quote(target_key, safe='')}"

    @staticmethod
    def _band_config_signature(payload: Dict[str, Any]) -> str:
        """Stable signature of just the band/mode config in a payload.

        For Auto blocks (no explicit selected_bands) the desired bands are
        derived from the static schedule table which varies per block, so the
        block name is included in the signature.  This ensures block transitions
        always trigger a POST for Auto mode.

        For explicit-selection blocks the selected_bands list fully determines
        the outcome regardless of the block, so the block name is omitted and
        the optimization (skip when bands+modes+closed are unchanged) applies.
        """
        selected = payload.get("selected_bands")
        bands: list | None = sorted(selected) if isinstance(selected, list) else None
        modes = dict(sorted((payload.get("band_modes") or {}).items()))
        closed = sorted(payload.get("closed_bands") or [])
        if bands is None:
            # Auto: different blocks use different static open-bands tables;
            # treat each block as its own identity.
            block = str(payload.get("block") or "")
            return json.dumps({"block": block, "modes": modes, "closed": closed}, separators=(",", ":"))
        return json.dumps({"bands": bands, "modes": modes, "closed": closed}, separators=(",", ":"))

    def _recovery_backoff_seconds_for_payload(self, payload: Dict[str, Any]) -> float:
        """Return the health-check backoff to use after applying a payload.

        Fixed-mode startup can intentionally post an empty roaming set while RX2-RX7
        settle. Rechecking sooner shortens the blank RX0/RX1 window without removing
        the normal anti-thrash backoff for broader reassignments.
        """
        selected = payload.get("selected_bands")
        fixed_assignments = payload.get("fixed_assignments")
        if isinstance(selected, list) and not selected and isinstance(fixed_assignments, list) and fixed_assignments:
            return float(min(self._RECOVERY_BACKOFF_S, max(self._loop_interval_s(), self._EMPTY_ROAMING_RECHECK_BACKOFF_S)))
        return float(self._RECOVERY_BACKOFF_S)

    @staticmethod
    def _target_auto_set_payload(settings: Dict[str, Any], payload: Dict[str, Any], *, kiwi_key: str | None = None) -> Dict[str, Any]:
        targeted = dict(payload)
        target_key = str(kiwi_key if kiwi_key is not None else settings.get("activeKiwiKey") or "").strip()
        if target_key:
            targeted["kiwi_key"] = target_key
        return targeted

    def _post_auto_set(self, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._auto_set_url(),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            # We intentionally use a 6.0s timeout. The endpoint runs apply_assignments
            # synchronously in a thread, which can take minutes if eviction is needed.
            # We don't want to block the auto-set loop for minutes.
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                _ = resp.read(1024 * 1024)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # If the timeout fires, the server is still processing the task in the background.
            # Do NOT bubble up the error, because completing without exception allows
            # _run() to save the new signature and avoid a thundering herd next 30s.
            import socket
            if isinstance(e, socket.timeout) or getattr(e, "reason", None) is not None:
                logger.debug("POST /auto_set_receivers returned or timed out: %s", getattr(e, "reason", e))
            else:
                logger.debug("POST /auto_set_receivers error (processing in background): %s", e)

    def clear_receivers_for_manual_mode(self, settings: Dict[str, Any], *, kiwi_key: str | None = None) -> None:
        """One-shot clear when user explicitly switches to Manual Mode."""
        target_key = self._normalize_kiwi_key(kiwi_key if kiwi_key is not None else settings.get("activeKiwiKey"))
        payload = self._target_auto_set_payload(
            settings,
            {"enabled": False, "force": True},
            kiwi_key=target_key,
        )
        logger.info(
            "Manual mode button pressed for %s: clearing current non-manual receiver set once",
            target_key or "<default>",
        )
        try:
            self._post_auto_set(payload)
        except Exception:
            logger.debug("Manual-mode one-shot clear failed", exc_info=True)

    def _fixed_health_state(self, kiwi_key: str | None = None) -> tuple[str, list[dict]]:
        """Return fixed receiver health as (healthy|sick|unknown, sick_entries)."""
        try:
            url = self._service_url("/health/rx", kiwi_key=kiwi_key)
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", list(_FIXED_ASSIGNMENTS)
        if isinstance(data, dict) and (str(data.get("overall") or "").lower() in {"busy", "starting"} or bool(data.get("_from_cache"))):
            return "unknown", list(_FIXED_ASSIGNMENTS)
        channels = data.get("channels") if isinstance(data, dict) else None
        if not isinstance(channels, dict):
            return "sick", list(_FIXED_ASSIGNMENTS)
        with self._state_lock:
            last_success_ts = self._kiwi_state_locked(self._normalize_kiwi_key(kiwi_key)).get("last_success_ts")
            if last_success_ts is None:
                last_success_ts = self._last_success_ts
        now = time.time()
        sick: list[dict] = []
        for entry in _FIXED_ASSIGNMENTS:
            rx_key = str(entry["rx"])
            ch = channels.get(rx_key)
            if not isinstance(ch, dict):
                logger.info("Fixed receiver RX%s missing from health channels", rx_key)
                sick.append({**entry, "_reason": "missing"})
                continue
            elif not ch.get("active"):
                last_reason = str(ch.get("last_reason") or "").strip().lower()
                try:
                    decoder_output_age_s = (
                        float(ch.get("decoder_output_age_s"))
                        if ch.get("decoder_output_age_s") is not None
                        else None
                    )
                except Exception:
                    decoder_output_age_s = None
                recent_apply_grace = bool(
                    last_success_ts is not None
                    and (now - float(last_success_ts)) <= self._FIXED_INVISIBLE_GRACE_S
                )
                if (
                    last_reason == "kiwi_not_visible"
                    and decoder_output_age_s is not None
                    and decoder_output_age_s <= 15.0
                    and recent_apply_grace
                ):
                    logger.debug(
                        "Fixed receiver RX%s has fresh decoder output (age=%.1fs) during post-apply kiwi_not_visible grace; suppressing recovery",
                        rx_key,
                        decoder_output_age_s,
                    )
                    continue
                logger.info("Fixed receiver RX%s is inactive", rx_key)
                sick.append({**entry, "_reason": "inactive"})
                continue
            else:
                kiwi_rx_raw = ch.get("kiwi_rx")
                try:
                    kiwi_rx = int(kiwi_rx_raw) if isinstance(kiwi_rx_raw, (int, float, str)) else None
                except Exception:
                    kiwi_rx = None
                if kiwi_rx is not None and kiwi_rx != int(entry["rx"]):
                    logger.info(
                        "Fixed receiver RX%s drifted to Kiwi slot %s",
                        rx_key,
                        kiwi_rx,
                    )
                    sick.append({**entry, "_reason": "drift"})
                    continue
            if ch.get("status_level") == "fault":
                logger.info("Fixed receiver RX%s is faulted (%s)", rx_key, ch.get("last_reason"))
                sick.append({**entry, "_reason": "fault"})
        if sick:
            return "sick", sick
        return "healthy", []

    def _sick_fixed_receivers(self, kiwi_key: str | None = None) -> list[dict]:
        """Return only authoritative sick fixed receivers."""
        state, sick = self._fixed_health_state(kiwi_key=kiwi_key)
        if state == "unknown":
            return []
        return sick

    def _fixed_receivers_healthy(self, kiwi_key: str | None = None) -> bool:
        """Return True only if all fixed receivers are at their expected Kiwi slots
        and the correct band is occupying each slot.

        Stricter than _fixed_health_state(): also verifies exact Kiwi slot alignment
        and Kiwi occupant band labels to confirm roaming can be safely activated.
        """
        try:
            url = self._service_url("/health/rx", kiwi_key=kiwi_key)
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                health_data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return False
        if isinstance(health_data, dict) and bool(health_data.get("_from_cache")):
            return False
        channels = health_data.get("channels") if isinstance(health_data, dict) else None
        if not isinstance(channels, dict):
            return False
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            ch = channels.get(str(rx))
            if not isinstance(ch, dict) or not ch.get("active"):
                return False
            if ch.get("kiwi_rx") != rx:
                return False
        try:
            url = self._service_url("/system/info", kiwi_key=kiwi_key)
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                sysinfo = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return False
        raw_users: dict[int, str] = {}
        if isinstance(sysinfo, dict):
            kiwi_data = sysinfo.get("kiwi") or {}
            for user in (kiwi_data.get("raw_users") or []):
                if isinstance(user, dict) and user.get("rx") is not None:
                    try:
                        raw_users[int(user["rx"])] = str(user.get("name") or "")
                    except Exception:
                        pass
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            band = str(entry["band"])
            if band.lower() not in raw_users.get(rx, "").lower():
                return False
        return True

    def _roaming_health_state(self, kiwi_key: str | None = None) -> tuple[str, list[int]]:
        """Return roaming RX0/RX1 health as (healthy|sick|unknown, sick_rxs)."""
        try:
            url = self._service_url("/health/rx", kiwi_key=kiwi_key)
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", [0, 1]

        if not isinstance(data, dict):
            return "unknown", [0, 1]
        if str(data.get("overall") or "").lower() in {"busy", "starting"} or bool(data.get("_from_cache")):
            return "unknown", [0, 1]

        channels = data.get("channels")
        if not isinstance(channels, dict):
            return "unknown", [0, 1]

        sick: list[int] = []
        for rx in (0, 1):
            ch = channels.get(str(rx))
            if not isinstance(ch, dict):
                sick.append(int(rx))
                continue
            if not bool(ch.get("active")) or not bool(ch.get("visible_on_kiwi")):
                sick.append(int(rx))
                continue
            if str(ch.get("status_level") or "").strip().lower() == "fault":
                sick.append(int(rx))

        return ("healthy", []) if not sick else ("sick", sick)

    def _restart_sick_receivers(self, sick: list[dict]) -> None:
        """Restart one stuck fixed receiver via the targeted admin endpoint.

        Fixed-slot recovery must reclaim one home slot at a time. Restarting more
        than one fixed receiver in the same cycle reopens multiple slots and lets
        Kiwi remap the displaced FIXED_* workers into the wrong free channel.
        """
        if not sick:
            return
        target = min((int(e["rx"]) for e in sick), default=None)
        if target is None:
            return
        rx_list = [target]
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        url = f"http://127.0.0.1:{port}/admin/restart-receivers"
        body = json.dumps({"rx_list": rx_list, "reason": "kiwi_assignment_mismatch"}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                _ = resp.read(1024 * 1024)
        except Exception:
            logger.debug("Failed to POST /admin/restart-receivers", exc_info=True)

    def _run_for_kiwi(self, settings: Dict[str, Any], kiwi_key: str) -> None:
        target_key = self._normalize_kiwi_key(kiwi_key)
        target_label = target_key or "<default>"
        receivers_mode = self._receivers_mode(settings, kiwi_key=target_key)
        schedule_key = self._current_schedule_key(settings, kiwi_key=target_key)
        apply_signature = self._apply_signature(settings, schedule_key, kiwi_key=target_key)

        if receivers_mode == "scan":
            with self._state_lock:
                self._reset_kiwi_state_locked(target_key, manual_mode_cleared=False)
                self._sync_selected_kiwi_state_locked(settings)
            return

        with self._state_lock:
            state = self._kiwi_state_locked(target_key)
            manual_mode_cleared = bool(state.get("manual_mode_cleared"))

        if receivers_mode == "manual":
            if not manual_mode_cleared:
                logger.info("Auto-set loop: Manual Mode detected for %s — clearing current non-manual receivers once, then parking loop", target_label)
                self.clear_receivers_for_manual_mode(settings, kiwi_key=target_key)
            with self._state_lock:
                self._reset_kiwi_state_locked(target_key, manual_mode_cleared=True)
                self._sync_selected_kiwi_state_locked(settings)
            return

        with self._state_lock:
            state = self._kiwi_state_locked(target_key)
            state["manual_mode_cleared"] = False
            did_startup_apply = bool(state.get("did_startup_apply"))
            last_schedule_key = state.get("last_schedule_key")
            last_apply_signature = state.get("last_apply_signature")
            last_applied_band_config = state.get("last_applied_band_config")
            recovery_backoff_until_ts = float(state.get("recovery_backoff_until_ts") or 0.0)

        should_apply = bool(
            (not did_startup_apply)
            or last_schedule_key != schedule_key
            or last_apply_signature != apply_signature
        )
        payload: Dict[str, Any] | None = None
        new_band_config: str | None = None
        force_health_recovery = False
        in_recovery_backoff = time.time() < recovery_backoff_until_ts

        if not should_apply and did_startup_apply and not in_recovery_backoff:
            fixed_health_state, sick = self._fixed_health_state(kiwi_key=target_key)
            if fixed_health_state == "unknown":
                logger.debug("Auto-set loop: fixed receiver health unknown for %s — skipping recovery check", target_label)
            elif sick:
                rx_nums = [int(e["rx"]) for e in sick]
                with self._state_lock:
                    state = self._kiwi_state_locked(target_key)
                    state["recovery_backoff_until_ts"] = time.time() + self._RECOVERY_BACKOFF_S
                    self._sync_selected_kiwi_state_locked(settings)
                if len(sick) == len(_FIXED_ASSIGNMENTS):
                    logger.info(
                        "Auto-set loop: all fixed receivers missing for %s — triggering full re-apply "
                        "(next health check suppressed for %.0fs)",
                        target_label,
                        self._RECOVERY_BACKOFF_S,
                    )
                    force_health_recovery = True
                    should_apply = True
                else:
                    logger.info(
                        "Auto-set loop: fixed receiver(s) RX%s unhealthy for %s — restarting targeted "
                        "(next health check suppressed for %.0fs)",
                        "/".join(str(r) for r in rx_nums),
                        target_label,
                        self._RECOVERY_BACKOFF_S,
                    )
                    self._restart_sick_receivers(sick)
            else:
                if self._roaming_enabled(settings, kiwi_key=target_key) and last_applied_band_config is not None and '"bands":[]' in str(last_applied_band_config):
                    logger.info(
                        "Auto-set loop: fixed receivers now healthy for %s — re-applying to fill scored roaming slots",
                        target_label,
                    )
                    force_health_recovery = True
                    should_apply = True
            if not should_apply and self._roaming_enabled(settings, kiwi_key=target_key):
                roaming_state, sick_roaming = self._roaming_health_state(kiwi_key=target_key)
                if roaming_state == "sick" and sick_roaming:
                    roaming_backoff_s = float(max(self._loop_interval_s(), self._ROAMING_RECOVERY_BACKOFF_S))
                    with self._state_lock:
                        state = self._kiwi_state_locked(target_key)
                        state["recovery_backoff_until_ts"] = time.time() + roaming_backoff_s
                        self._sync_selected_kiwi_state_locked(settings)
                    logger.info(
                        "Auto-set loop: roaming receiver(s) RX%s unhealthy for %s — forcing re-apply "
                        "(next health check suppressed for %.0fs)",
                        "/".join(str(r) for r in sick_roaming),
                        target_label,
                        roaming_backoff_s,
                    )
                    force_health_recovery = True
                    should_apply = True
            if not should_apply:
                payload = self._build_payload(settings, schedule_key=schedule_key, kiwi_key=target_key)
                new_band_config = self._band_config_signature(payload)
                if new_band_config != last_applied_band_config:
                    logger.info(
                        "Auto-set loop: scored band config changed for %s on %s — applying updated assignments",
                        schedule_key,
                        target_label,
                    )
                    should_apply = True
        elif in_recovery_backoff:
            logger.debug(
                "Auto-set loop: skipping health check for %s — recovery backoff active for %.0fs more",
                target_label,
                max(0.0, recovery_backoff_until_ts - time.time()),
            )

        if not should_apply:
            return

        with self._state_lock:
            self._last_run_ts = time.time()
            state = self._kiwi_state_locked(target_key)
            last_applied_band_config = state.get("last_applied_band_config")

        if payload is None:
            payload = self._build_payload(settings, schedule_key=schedule_key, kiwi_key=target_key)
        payload = self._target_auto_set_payload(settings, payload, kiwi_key=target_key)
        if force_health_recovery:
            payload["force"] = True
        if new_band_config is None:
            new_band_config = self._band_config_signature(payload)

        if (
            not force_health_recovery
            and did_startup_apply
            and last_applied_band_config is not None
            and new_band_config == last_applied_band_config
        ):
            logger.info(
                "Auto-set loop: block changed to %s for %s but band/mode config unchanged — skipping reassign",
                schedule_key,
                target_label,
            )
            with self._state_lock:
                state = self._kiwi_state_locked(target_key)
                state["last_schedule_key"] = schedule_key
                state["last_apply_signature"] = apply_signature
                self._sync_selected_kiwi_state_locked(settings)
            return

        try:
            self._post_auto_set(payload)
            now = time.time()
            with self._state_lock:
                state = self._kiwi_state_locked(target_key)
                state["did_startup_apply"] = True
                state["last_schedule_key"] = schedule_key
                state["last_apply_signature"] = apply_signature
                state["last_applied_band_config"] = new_band_config
                state["last_success_ts"] = now
                state["last_error"] = None
                state["recovery_backoff_until_ts"] = now + self._recovery_backoff_seconds_for_payload(payload)
                self._last_success_ts = now
                self._last_error = None
                self._sync_selected_kiwi_state_locked(settings)
        except urllib.error.HTTPError as e:
            with self._state_lock:
                state = self._kiwi_state_locked(target_key)
                state["last_error"] = f"HTTP {getattr(e, 'code', '?')}"
                self._last_error = state["last_error"]
                self._sync_selected_kiwi_state_locked(settings)
            logger.warning("Auto-set loop request failed for %s: HTTP %s", target_label, getattr(e, "code", "?"))
        except Exception:
            with self._state_lock:
                state = self._kiwi_state_locked(target_key)
                state["last_error"] = "request failed"
                self._last_error = str(state["last_error"])
                self._sync_selected_kiwi_state_locked(settings)
            logger.debug("Auto-set loop request failed for %s", target_label, exc_info=True)

    def _run(self) -> None:
        interval_s = self._loop_interval_s()
        logger.info("Auto-set loop started (interval=%ss)", interval_s)
        while not self._stop.is_set():
            settings = self._load_settings()
            headless_enabled = self._safe_bool(settings.get("headlessEnabled"), default=True)

            if not headless_enabled:
                with self._state_lock:
                    self._clear_all_kiwi_state_locked(manual_mode_cleared=False)
                    self._sync_selected_kiwi_state_locked(settings)
                self._wait_for_notification(timeout_s=interval_s)
                continue

            with self._state_lock:
                external_hold_reason = self._external_hold_reason
            if external_hold_reason:
                logger.info("Auto-set loop: external hold active (%s) — parking loop", external_hold_reason)
                with self._state_lock:
                    self._clear_all_kiwi_state_locked(manual_mode_cleared=True)
                    self._sync_selected_kiwi_state_locked(settings)
                self._wait_for_notification()
                continue

            for kiwi_key in self._target_kiwi_keys(settings):
                with self._state_lock:
                    kiwi_hold_reason = self._external_hold_reason_for_kiwi_locked(kiwi_key)
                    if kiwi_hold_reason:
                        self._reset_kiwi_state_locked(kiwi_key, manual_mode_cleared=True)
                if kiwi_hold_reason:
                    logger.info(
                        "Auto-set loop: external hold active for %s (%s) — skipping target",
                        kiwi_key or "<default>",
                        kiwi_hold_reason,
                    )
                    continue
                self._run_for_kiwi(settings, kiwi_key)

            with self._state_lock:
                self._sync_selected_kiwi_state_locked(settings)

            self._wait_for_notification(timeout_s=interval_s)

    def start(self) -> None:
        if not self._enabled_by_env():
            logger.info("Auto-set loop disabled by KIWISCAN_AUTOSET_LOOP")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._wake.clear()
        self._thread = threading.Thread(target=self._run, name="kiwi-scan-auto-set-loop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            if not self._thread.is_alive():
                self._thread = None

    def status(self) -> Dict[str, Any]:
        settings = self._load_settings()
        with self._state_lock:
            last_run_ts = self._last_run_ts
            last_success_ts = self._last_success_ts
            last_error = self._last_error
            external_hold_reason = self._external_hold_reason
            external_holds_by_kiwi = dict(self._external_hold_by_kiwi)
        return {
            "enabled_by_env": bool(self._enabled_by_env()),
            "thread_running": bool(self._thread is not None and self._thread.is_alive()),
            "interval_s": float(self._loop_interval_s()),
            "did_startup_apply": bool(self._did_startup_apply),
            "headless_enabled": bool(self._safe_bool(settings.get("headlessEnabled"), default=True)),
            "receivers_mode": self._receivers_mode(settings),
            "fixed_mode_enabled": self._receivers_mode(settings) != "manual",
            "manual_mode_parked": bool(self._manual_mode_cleared and self._receivers_mode(settings) == "manual"),
            "launchd_preferred": bool(self._safe_bool(settings.get("useLaunchd"), default=False)),
            "auto_scan_on_block": bool(self._safe_bool(settings.get("autoScanOnBlock"), default=False)),
            "auto_scan_on_startup": bool(self._safe_bool(settings.get("autoScanOnStartup"), default=False)),
            "last_run_ts": last_run_ts,
            "last_success_ts": last_success_ts,
            "last_error": last_error,
            "external_hold_reason": external_hold_reason,
            "external_holds_by_kiwi": external_holds_by_kiwi,
            "fixed_rx_count": len(_FIXED_ASSIGNMENTS),
            "fixed_rxs": [{"rx": e["rx"], "band": e["band"], "mode": e["mode"]} for e in _FIXED_ASSIGNMENTS],
        }

    def force_reassign(self, kiwi_key: str | None = None) -> None:
        """Force an immediate re-apply of current automation settings, bypassing all caches.

        Reads the saved settings, builds the same payload the auto-set loop would use,
        and posts it to /auto_set_receivers with ``force=True`` so the endpoint skips its
        15-second deduplication window.  The loop's own signature cache is also cleared so
        the next scheduled cycle re-evaluates from a clean state.

        No-ops when Auto Mode (fixedModeEnabled) is OFF or an external hold is
        active so SmartScheduler condition-change callbacks don't override
        receiver-scan / net-monitor receiver reservations.
        """
        with self._state_lock:
            external_hold_reason = self._external_hold_reason
        if external_hold_reason:
            logger.debug(
                "force_reassign skipped — external hold active (%s)",
                external_hold_reason,
            )
            return

        settings = self._load_settings()
        applied = False
        target_kiwi_keys = [self._normalize_kiwi_key(kiwi_key)] if kiwi_key is not None else self._target_kiwi_keys(settings)
        for target_kiwi_key in target_kiwi_keys:
            with self._state_lock:
                kiwi_hold_reason = self._external_hold_reason_for_kiwi_locked(target_kiwi_key)
            if kiwi_hold_reason:
                logger.debug(
                    "force_reassign skipped for %s — external hold active (%s)",
                    target_kiwi_key or "<default>",
                    kiwi_hold_reason,
                )
                continue
            receivers_mode = self._receivers_mode(settings, kiwi_key=target_kiwi_key)
            if receivers_mode == "scan":
                logger.debug("force_reassign skipped for %s — Scan Mode is active", target_kiwi_key or "<default>")
                continue
            if receivers_mode == "manual":
                logger.debug("force_reassign skipped for %s — Auto Mode is OFF", target_kiwi_key or "<default>")
                continue
            payload = self._build_payload(settings, schedule_key=self._current_schedule_key(settings, kiwi_key=target_kiwi_key), kiwi_key=target_kiwi_key)
            payload = self._target_auto_set_payload(settings, payload, kiwi_key=target_kiwi_key)
            payload["force"] = True
            self._post_auto_set(payload)
            applied = True
            with self._state_lock:
                self._reset_kiwi_state_locked(target_kiwi_key, manual_mode_cleared=False)
                self._sync_selected_kiwi_state_locked(settings)
        if not applied:
            return

    def apply_current_settings(self, *, force: bool = False, sync_state: bool = True) -> bool:
        """Apply the current automation payload immediately.

        Used by explicit mode-transition endpoints so the caller can request a
        single immediate apply without relying on the loop wakeup path.
        """
        with self._state_lock:
            external_hold_reason = self._external_hold_reason
        if external_hold_reason:
            logger.debug(
                "apply_current_settings skipped — external hold active (%s)",
                external_hold_reason,
            )
            return False

        settings = self._load_settings()
        applied = False
        for kiwi_key in self._target_kiwi_keys(settings):
            with self._state_lock:
                kiwi_hold_reason = self._external_hold_reason_for_kiwi_locked(kiwi_key)
            if kiwi_hold_reason:
                logger.debug(
                    "apply_current_settings skipped for %s — external hold active (%s)",
                    kiwi_key or "<default>",
                    kiwi_hold_reason,
                )
                continue
            receivers_mode = self._receivers_mode(settings, kiwi_key=kiwi_key)
            if receivers_mode == "scan":
                logger.debug("apply_current_settings skipped for %s — Scan Mode is active", kiwi_key or "<default>")
                continue
            if receivers_mode == "manual":
                logger.debug("apply_current_settings skipped for %s — Auto Mode is OFF", kiwi_key or "<default>")
                continue

            schedule_key = self._current_schedule_key(settings, kiwi_key=kiwi_key)
            payload = self._build_payload(settings, schedule_key=schedule_key, kiwi_key=kiwi_key)
            payload = self._target_auto_set_payload(settings, payload, kiwi_key=kiwi_key)
            if force:
                payload["force"] = True
            apply_signature = self._apply_signature(settings, schedule_key, kiwi_key=kiwi_key)
            band_config = self._band_config_signature(payload)

            self._post_auto_set(payload)
            applied = True

            now = time.time()
            with self._state_lock:
                state = self._kiwi_state_locked(kiwi_key)
                state["last_success_ts"] = now
                state["last_error"] = None
                state["manual_mode_cleared"] = False
                if sync_state:
                    state["did_startup_apply"] = True
                    state["last_schedule_key"] = schedule_key
                    state["last_apply_signature"] = apply_signature
                    state["last_applied_band_config"] = band_config
                state["recovery_backoff_until_ts"] = now + self._recovery_backoff_seconds_for_payload(payload)
                self._last_success_ts = now
                self._last_error = None
                self._sync_selected_kiwi_state_locked(settings)
        return applied