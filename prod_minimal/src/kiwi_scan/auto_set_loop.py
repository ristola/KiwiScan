from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

from typing import Any
from .scheduler import block_for_hour

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
    {"band": "12m", "mode": "FT8"},
    {"band": "15m", "mode": "FT8"},
]
_ROAMING_NIGHT = [
    {"band": "60m", "mode": "FT8"},
    {"band": "80m", "mode": "FT4 / FT8"},
    {"band": "160m", "mode": "WSPR"},
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
        self._state_lock = threading.Lock()
        self._last_run_ts: float | None = None
        self._last_success_ts: float | None = None
        self._last_error: str | None = None
        self._smart_scheduler: Any | None = None
        self._last_applied_band_config: str | None = None
        # After a force_health_recovery re-apply, back off for this many seconds
        # before checking health again.  This prevents thundering-herd restarts while
        # workers are still settling into their correct Kiwi slots (eviction loop can
        # take up to ~2 min for 8 receivers over VPN).
        self._recovery_backoff_until_ts: float = 0.0
        self._RECOVERY_BACKOFF_S: float = 60.0

    def set_smart_scheduler(self, smart_scheduler: Any) -> None:
        """Bind a SmartScheduler instance so closed bands are filtered each cycle."""
        self._smart_scheduler = smart_scheduler

    @staticmethod
    def _settings_path() -> Path:
        root = Path(__file__).resolve().parents[2]
        return root / "outputs" / "automation_settings.json"

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

    def notify_settings_changed(self) -> None:
        self._wake.set()

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
    def _manual_enforce_interval_s() -> float:
        raw = str(os.environ.get("KIWISCAN_MANUAL_ENFORCE_S", "5") or "5").strip()
        try:
            value = float(raw)
        except Exception:
            value = 5.0
        return max(0.1, min(60.0, value))

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

    def _build_payload(self, settings: Dict[str, Any], schedule_key: tuple[str, str] | None = None) -> Dict[str, Any]:
        if schedule_key is None:
            schedule_key = self._current_schedule_key(settings)

        if schedule_key[0] == "fixed":
            return self._build_fixed_roaming_payload(settings, schedule_key[1])

        mode = str(settings.get("autoScanMode") or "ft8").strip().lower()
        if mode not in {"ft8", "phone"}:
            mode = "ft8"

        active_mode, active_block = schedule_key
        if str(active_mode).strip().lower() in {"ft8", "phone"}:
            mode = str(active_mode).strip().lower()

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
            "block": str(active_block),
            "ssb_scan": {
                "enabled": self._safe_bool(settings.get("ssbEnabled"), default=True),
                "threshold_db": self._safe_num(settings.get("ssbThresholdDb"), 20.0, 1.0, 60.0),
                "wait_s": self._safe_num(settings.get("ssbWaitS"), 1.0, 0.1, 10.0),
                "dwell_s": self._safe_num(settings.get("ssbDwellS"), 6.0, 1.0, 60.0),
                "tail_s": self._safe_num(settings.get("ssbTailS"), 1.0, 0.1, 10.0),
                "step_strategy": str(settings.get("ssbStepStrategy") or "adaptive").strip().lower(),
                "step_khz": self._safe_num(settings.get("ssbStepKHz"), 10.0, 0.1, 20.0),
                "sideband": str(settings.get("ssbSideband") or "USB").strip().upper(),
                "adaptive_threshold": self._safe_bool(settings.get("ssbAdaptiveThreshold"), default=True),
                "use_kiwi_snr": self._safe_bool(settings.get("ssbUseKiwiSnr"), default=True),
            },
        }
        if isinstance(selected_bands, list):
            # Filter against the user's band allowlist ("On" checkboxes in Band Schedule).
            if self._smart_scheduler is not None:
                try:
                    allowed = self._smart_scheduler._allowed_bands()
                    selected_bands = [b for b in selected_bands if b in allowed]
                except Exception:
                    pass
            payload["selected_bands"] = list(selected_bands)
        if isinstance(band_modes, dict):
            payload["band_modes"] = dict(band_modes)

        # Ask SmartScheduler which bands are empirically/seasonally closed and
        # pass them to /auto_set_receivers so receivers aren't wasted on dead bands.
        if self._smart_scheduler is not None:
            try:
                closed = list(self._smart_scheduler.get_closed_bands(mode))
                if closed:
                    payload["closed_bands"] = closed
            except Exception:
                pass

        return payload

    def _build_fixed_roaming_payload(self, settings: Dict[str, Any], day_night: str) -> Dict[str, Any]:
        """Build a payload with fixed RX2-RX7 plus scored RX0/RX1 roaming.

        RX0/RX1 stay empty until all 6 mandatory fixed receivers are healthy. Once
        healthy, SmartScheduler ranks only the configured day/night roaming pool and
        fills the 2 spare receivers with the top-scored bands.
        """
        roaming = _ROAMING_DAY if day_night == "day" else _ROAMING_NIGHT
        _fixed_bands = {str(a["band"]).strip().lower() for a in _FIXED_ASSIGNMENTS}
        roaming_pool = [str(r["band"]) for r in roaming if str(r["band"]).strip().lower() not in _fixed_bands]
        band_modes: Dict[str, str] = {str(r["band"]): str(r["mode"]) for r in roaming if str(r["band"]).strip().lower() not in _fixed_bands}
        selected_bands: list[str] = []
        num_roaming_slots = 2

        fixed_health_state, _sick_fixed = self._fixed_health_state()
        if self._smart_scheduler is not None:
            try:
                ranked_bands = self._smart_scheduler.rank_roaming_bands(
                    available_bands=list(roaming_pool),
                    current_roaming=[],
                )
                selected_bands = [b for b in ranked_bands if b in band_modes][:num_roaming_slots]
            except Exception:
                selected_bands = []

        if len(selected_bands) < num_roaming_slots:
            fallback = [b for b in roaming_pool if b not in selected_bands]
            selected_bands.extend(fallback[:(num_roaming_slots - len(selected_bands))])

        if fixed_health_state != "healthy":
            logger.info(
                "Fixed receiver health=%s; keeping fallback roaming bands active: %s",
                fixed_health_state,
                selected_bands,
            )

        payload: Dict[str, Any] = {
            "enabled": True,
            "mode": "ft8",
            "block": day_night,
            "ssb_scan": {
                "enabled": self._safe_bool(settings.get("ssbEnabled"), default=False),
                "threshold_db": self._safe_num(settings.get("ssbThresholdDb"), 20.0, 1.0, 60.0),
                "wait_s": self._safe_num(settings.get("ssbWaitS"), 1.0, 0.1, 10.0),
                "dwell_s": self._safe_num(settings.get("ssbDwellS"), 6.0, 1.0, 60.0),
                "tail_s": self._safe_num(settings.get("ssbTailS"), 1.0, 0.1, 10.0),
                "step_strategy": str(settings.get("ssbStepStrategy") or "adaptive").strip().lower(),
                "step_khz": self._safe_num(settings.get("ssbStepKHz"), 10.0, 0.1, 20.0),
                "sideband": str(settings.get("ssbSideband") or "USB").strip().upper(),
                "adaptive_threshold": self._safe_bool(settings.get("ssbAdaptiveThreshold"), default=True),
                "use_kiwi_snr": self._safe_bool(settings.get("ssbUseKiwiSnr"), default=True),
            },
            "fixed_assignments": list(_FIXED_ASSIGNMENTS),
            "selected_bands": selected_bands,
            "band_modes": band_modes,
        }
        return payload

    @staticmethod
    def _current_schedule_key(settings: Dict[str, Any]) -> tuple[str, str]:
        if AutoSetLoop._safe_bool(settings.get("fixedModeEnabled"), default=True):
            local_hour = datetime.now().astimezone().hour
            day_night = "day" if 7 <= local_hour < 21 else "night"
            return ("fixed", day_night)
        mode = str(settings.get("autoScanMode") or "ft8").strip().lower()
        if mode not in {"ft8", "phone"}:
            mode = "ft8"
        local_dt = datetime.now().astimezone()
        return mode, block_for_hour(local_dt.hour, mode=mode)

    @staticmethod
    def _apply_signature(settings: Dict[str, Any], schedule_key: tuple[str, str]) -> str:
        relevant = {
            "schedule_key": [str(schedule_key[0]), str(schedule_key[1])],
            "autoScanMode": settings.get("autoScanMode"),
            "ssbEnabled": settings.get("ssbEnabled"),
            "ssbThresholdDb": settings.get("ssbThresholdDb"),
            "ssbAdaptiveThreshold": settings.get("ssbAdaptiveThreshold"),
            "ssbUseKiwiSnr": settings.get("ssbUseKiwiSnr"),
            "ssbWaitS": settings.get("ssbWaitS"),
            "ssbDwellS": settings.get("ssbDwellS"),
            "ssbTailS": settings.get("ssbTailS"),
            "ssbStepStrategy": settings.get("ssbStepStrategy"),
            "ssbStepKHz": settings.get("ssbStepKHz"),
            "ssbSideband": settings.get("ssbSideband"),
            "scheduleProfiles": settings.get("scheduleProfiles"),
            "fixedModeEnabled": settings.get("fixedModeEnabled"),
        }
        return json.dumps(relevant, sort_keys=True, separators=(",", ":"))

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

    def _fixed_health_state(self) -> tuple[str, list[dict]]:
        """Return fixed receiver health as (healthy|sick|unknown, sick_entries)."""
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        try:
            url = f"http://127.0.0.1:{port}/health/rx"
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", list(_FIXED_ASSIGNMENTS)
        if isinstance(data, dict) and (data.get("overall") == "busy" or bool(data.get("_from_cache"))):
            return "unknown", list(_FIXED_ASSIGNMENTS)
        channels = data.get("channels") if isinstance(data, dict) else None
        if not isinstance(channels, dict):
            return "sick", list(_FIXED_ASSIGNMENTS)
        sick: list[dict] = []
        for entry in _FIXED_ASSIGNMENTS:
            rx_key = str(entry["rx"])
            ch = channels.get(rx_key)
            if not isinstance(ch, dict):
                logger.info("Fixed receiver RX%s missing from health channels", rx_key)
                sick.append(entry)
            elif not ch.get("active"):
                logger.info("Fixed receiver RX%s is inactive", rx_key)
                sick.append(entry)
            elif ch.get("status_level") == "fault":
                logger.info("Fixed receiver RX%s is faulted (%s)", rx_key, ch.get("last_reason"))
                sick.append(entry)
        if sick:
            return "sick", sick
        return "healthy", []

    def _sick_fixed_receivers(self) -> list[dict]:
        """Return only authoritative sick fixed receivers."""
        state, sick = self._fixed_health_state()
        if state == "unknown":
            return []
        return sick

    def _fixed_receivers_healthy(self) -> bool:
        """Return True only if all fixed receivers are at their expected Kiwi slots
        and the correct band is occupying each slot.

        Stricter than _fixed_health_state(): also verifies exact Kiwi slot alignment
        and Kiwi occupant band labels to confirm roaming can be safely activated.
        """
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        try:
            url = f"http://127.0.0.1:{port}/health/rx"
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
            url = f"http://127.0.0.1:{port}/system/info"
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

    def _roaming_health_state(self) -> tuple[str, list[int]]:
        """Return roaming RX0/RX1 health as (healthy|sick|unknown, sick_rxs)."""
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        try:
            url = f"http://127.0.0.1:{port}/health/rx"
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return "unknown", [0, 1]

        if not isinstance(data, dict):
            return "unknown", [0, 1]
        if data.get("overall") == "busy" or bool(data.get("_from_cache")):
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
        """Restart only the listed stuck fixed receivers via the targeted admin endpoint."""
        rx_list = [int(e["rx"]) for e in sick]
        port_raw = str(os.environ.get("PORT", "4020") or "4020").strip()
        try:
            port = int(port_raw)
        except Exception:
            port = 4020
        url = f"http://127.0.0.1:{port}/admin/restart-receivers"
        body = json.dumps({"rx_list": rx_list}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                _ = resp.read(1024 * 1024)
        except Exception:
            logger.debug("Failed to POST /admin/restart-receivers", exc_info=True)

    def _run(self) -> None:
        interval_s = self._loop_interval_s()
        logger.info("Auto-set loop started (interval=%ss)", interval_s)
        while not self._stop.is_set():
            settings = self._load_settings()
            headless_enabled = self._safe_bool(settings.get("headlessEnabled"), default=True)
            schedule_key = self._current_schedule_key(settings)
            apply_signature = self._apply_signature(settings, schedule_key)

            if not headless_enabled:
                self._manual_mode_cleared = False
                self._did_startup_apply = False
                self._last_schedule_key = None
                self._last_apply_signature = None
                self._last_applied_band_config = None
                self._wait_for_notification(timeout_s=interval_s)
                continue

            fixed_mode_enabled = self._safe_bool(settings.get("fixedModeEnabled"), default=True)
            if not fixed_mode_enabled:
                if not self._manual_mode_cleared:
                    logger.info("Auto-set loop: Manual Mode detected — clearing receivers and parking loop")
                    try:
                        self._post_auto_set({"enabled": False, "force": True})
                    except Exception:
                        pass
                    self._manual_mode_cleared = True
                self._did_startup_apply = False
                self._last_schedule_key = None
                self._last_apply_signature = None
                self._last_applied_band_config = None
                self._wait_for_notification()
                continue

            self._manual_mode_cleared = False

            should_apply = bool(
                (not self._did_startup_apply)
                or self._last_schedule_key != schedule_key
                or self._last_apply_signature != apply_signature
            )

            # Even when nothing logically changed, verify fixed receivers are still live.
            # This recovers from unexpected restarts or external kicks without waiting for
            # a schedule change to trigger the normal should_apply path.
            # Backoff guard: after a recovery re-apply fires, skip health checks for
            # _RECOVERY_BACKOFF_S seconds so workers have time to settle into correct
            # Kiwi slots before we re-trigger.  Without this, the 30s loop interval fires
            # again before apply_assignments even releases its lock, causing a thundering
            # herd of re-applies that fight the eviction loop and make things worse.
            force_health_recovery = False
            _in_recovery_backoff = time.time() < self._recovery_backoff_until_ts
            if not should_apply and self._did_startup_apply and not _in_recovery_backoff:
                fixed_health_state, sick = self._fixed_health_state()
                if fixed_health_state == "unknown":
                    logger.debug("Auto-set loop: fixed receiver health unknown — skipping recovery check")
                elif sick:
                    rx_nums = [int(e["rx"]) for e in sick]
                    self._recovery_backoff_until_ts = time.time() + self._RECOVERY_BACKOFF_S
                    if len(sick) == len(_FIXED_ASSIGNMENTS):
                        # All fixed receivers missing — targeted restart fails silently when
                        # assignments have been cleared (e.g. by a duplicate-label auto-kick).
                        # Skip the targeted restart and force a full re-apply instead.
                        logger.info(
                            "Auto-set loop: all fixed receivers missing — triggering full re-apply "
                            "(next health check suppressed for %.0fs)",
                            self._RECOVERY_BACKOFF_S,
                        )
                        force_health_recovery = True
                        should_apply = True
                    else:
                        logger.info(
                            "Auto-set loop: fixed receiver(s) RX%s unhealthy — restarting targeted "
                            "(next health check suppressed for %.0fs)",
                            "/".join(str(r) for r in rx_nums),
                            self._RECOVERY_BACKOFF_S,
                        )
                        self._restart_sick_receivers(sick)
                else:
                    with self._state_lock:
                        last_band_config = self._last_applied_band_config
                    if last_band_config is not None and '"bands":[]' in last_band_config:
                        logger.info(
                            "Auto-set loop: fixed receivers now healthy — re-applying to fill scored roaming slots"
                        )
                        force_health_recovery = True
                        should_apply = True
                if not should_apply:
                    roaming_state, sick_roaming = self._roaming_health_state()
                    if roaming_state == "sick" and sick_roaming:
                        self._recovery_backoff_until_ts = time.time() + self._RECOVERY_BACKOFF_S
                        logger.info(
                            "Auto-set loop: roaming receiver(s) RX%s unhealthy — forcing re-apply "
                            "(next health check suppressed for %.0fs)",
                            "/".join(str(r) for r in sick_roaming),
                            self._RECOVERY_BACKOFF_S,
                        )
                        force_health_recovery = True
                        should_apply = True
            elif _in_recovery_backoff:
                logger.debug(
                    "Auto-set loop: skipping health check — recovery backoff active for %.0fs more",
                    max(0.0, self._recovery_backoff_until_ts - time.time()),
                )

            if should_apply:
                with self._state_lock:
                    self._last_run_ts = time.time()
                    last_applied_band_config = self._last_applied_band_config
                payload = self._build_payload(settings, schedule_key=schedule_key)
                # Force-flag health-recovery applies so the endpoint's dedup cache
                # doesn't suppress the re-kick when an identical payload was recently
                # used but failed to connect all workers.
                if force_health_recovery:
                    payload["force"] = True
                new_band_config = self._band_config_signature(payload)
                # If only the time block changed but the resulting band/mode config is
                # identical to what was last applied, skip the reassign entirely.
                if (
                    not force_health_recovery
                    and self._did_startup_apply
                    and last_applied_band_config is not None
                    and new_band_config == last_applied_band_config
                ):
                    logger.info(
                        "Auto-set loop: block changed to %s but band/mode config unchanged — skipping reassign",
                        schedule_key,
                    )
                    self._last_schedule_key = schedule_key
                    self._last_apply_signature = apply_signature
                else:
                    try:
                        self._post_auto_set(payload)
                        self._last_schedule_key = schedule_key
                        self._last_apply_signature = apply_signature
                        self._last_applied_band_config = new_band_config
                        with self._state_lock:
                            self._last_success_ts = time.time()
                            self._last_error = None
                            
                        # Set backoff after applying new assignments to prevent targeted restarts 
                        # from fighting the eviction loop while receivers are still booting.
                        self._recovery_backoff_until_ts = time.time() + self._RECOVERY_BACKOFF_S
                        
                        if not self._did_startup_apply:
                            self._did_startup_apply = True
                    except urllib.error.HTTPError as e:
                        with self._state_lock:
                            self._last_error = f"HTTP {getattr(e, 'code', '?')}"
                        logger.warning("Auto-set loop request failed: HTTP %s", getattr(e, "code", "?"))
                    except Exception:
                        with self._state_lock:
                            self._last_error = "request failed"
                        logger.debug("Auto-set loop request failed", exc_info=True)

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
        return {
            "enabled_by_env": bool(self._enabled_by_env()),
            "thread_running": bool(self._thread is not None and self._thread.is_alive()),
            "interval_s": float(self._loop_interval_s()),
            "did_startup_apply": bool(self._did_startup_apply),
            "headless_enabled": bool(self._safe_bool(settings.get("headlessEnabled"), default=True)),
            "fixed_mode_enabled": bool(self._safe_bool(settings.get("fixedModeEnabled"), default=True)),
            "manual_mode_parked": bool(self._manual_mode_cleared and not self._safe_bool(settings.get("fixedModeEnabled"), default=True)),
            "launchd_preferred": bool(self._safe_bool(settings.get("useLaunchd"), default=False)),
            "auto_scan_on_block": bool(self._safe_bool(settings.get("autoScanOnBlock"), default=False)),
            "auto_scan_on_startup": bool(self._safe_bool(settings.get("autoScanOnStartup"), default=False)),
            "last_run_ts": last_run_ts,
            "last_success_ts": last_success_ts,
            "last_error": last_error,
            "fixed_rx_count": len(_FIXED_ASSIGNMENTS),
            "fixed_rxs": [{"rx": e["rx"], "band": e["band"], "mode": e["mode"]} for e in _FIXED_ASSIGNMENTS],
        }

    def force_reassign(self) -> None:
        """Force an immediate re-apply of current automation settings, bypassing all caches.

        Reads the saved settings, builds the same payload the auto-set loop would use,
        and posts it to /auto_set_receivers with ``force=True`` so the endpoint skips its
        15-second deduplication window.  The loop's own signature cache is also cleared so
        the next scheduled cycle re-evaluates from a clean state.

        No-ops when Auto Mode (fixedModeEnabled) is OFF so that SmartScheduler
        condition-change callbacks don't undo the user's Manual selection.
        """
        settings = self._load_settings()
        if not self._safe_bool(settings.get("fixedModeEnabled"), default=True):
            logger.debug("force_reassign skipped — Auto Mode is OFF")
            return
        payload = self._build_payload(settings, schedule_key=self._current_schedule_key(settings))
        payload["force"] = True
        self._post_auto_set(payload)
        # Reset so the next loop cycle re-evaluates even if settings haven't changed.
        # All three fields are cleared atomically under the state lock so that _run()
        # cannot observe a partially-reset state (e.g. _last_schedule_key=None but
        # _last_applied_band_config still set) which causes a spurious skip.
        with self._state_lock:
            self._last_apply_signature = None
            self._last_schedule_key = None
            self._last_applied_band_config = None