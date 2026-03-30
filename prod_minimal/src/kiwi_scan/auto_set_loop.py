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

from .scheduler import block_for_hour

logger = logging.getLogger(__name__)


class AutoSetLoop:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._did_startup_apply = False
        self._last_schedule_key: tuple[str, str] | None = None
        self._last_apply_signature: str | None = None
        self._state_lock = threading.Lock()
        self._last_run_ts: float | None = None
        self._last_success_ts: float | None = None
        self._last_error: str | None = None

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
            v = float(value)
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
        mode = str(settings.get("autoScanMode") or "ft8").strip().lower()
        if mode not in {"ft8", "phone"}:
            mode = "ft8"

        if schedule_key is None:
            schedule_key = self._current_schedule_key(settings)
        active_mode, active_block = schedule_key
        if str(active_mode).strip().lower() in {"ft8", "phone"}:
            mode = str(active_mode).strip().lower()

        selected_bands, band_modes = self._profile_selection_for_block(
            settings,
            mode=mode,
            block=str(active_block),
        )

        payload: Dict[str, Any] = {
            "enabled": True,
            "mode": mode,
            "block": str(active_block),
            "wspr_scan_enabled": self._safe_bool(settings.get("autoScanWspr"), default=False),
            "band_hop_seconds": self._safe_num(settings.get("bandHopSeconds"), 105.0, 10.0, 600.0),
            "wspr_start_band": str(settings.get("wsprStartBand") or "10m"),
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
            payload["selected_bands"] = list(selected_bands)
        if isinstance(band_modes, dict):
            payload["band_modes"] = dict(band_modes)
        return payload

    @staticmethod
    def _current_schedule_key(settings: Dict[str, Any]) -> tuple[str, str]:
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
            "autoScanWspr": settings.get("autoScanWspr"),
            "bandHopSeconds": settings.get("bandHopSeconds"),
            "wsprStartBand": settings.get("wsprStartBand"),
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
        }
        return json.dumps(relevant, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _wspr_hop_due(settings: Dict[str, Any]) -> bool:
        if not AutoSetLoop._safe_bool(settings.get("autoScanWspr"), default=False):
            return False
        state = settings.get("wsprHopState")
        if not isinstance(state, dict):
            return False
        try:
            next_hop_unix = float(state.get("next_hop_unix") or 0.0)
        except Exception:
            return False
        return next_hop_unix > 0.0 and time.time() >= next_hop_unix

    def _post_auto_set(self, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._auto_set_url(),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=6.0) as resp:
            _ = resp.read(1024 * 1024)

    def _run(self) -> None:
        interval_s = self._loop_interval_s()
        logger.info("Auto-set loop started (interval=%ss)", interval_s)
        while not self._stop.is_set():
            settings = self._load_settings()
            headless_enabled = self._safe_bool(settings.get("headlessEnabled"), default=True)
            schedule_key = self._current_schedule_key(settings)
            apply_signature = self._apply_signature(settings, schedule_key)

            if not headless_enabled:
                self._did_startup_apply = False
                self._last_schedule_key = None
                self._last_apply_signature = None
                self._stop.wait(interval_s)
                continue

            should_apply = bool(
                (not self._did_startup_apply)
                or self._last_schedule_key != schedule_key
                or self._last_apply_signature != apply_signature
                or self._wspr_hop_due(settings)
            )

            if should_apply:
                with self._state_lock:
                    self._last_run_ts = time.time()
                try:
                    self._post_auto_set(self._build_payload(settings, schedule_key=schedule_key))
                    self._last_schedule_key = schedule_key
                    self._last_apply_signature = apply_signature
                    with self._state_lock:
                        self._last_success_ts = time.time()
                        self._last_error = None
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

            self._stop.wait(interval_s)
        logger.info("Auto-set loop stopped")

    def start(self) -> None:
        if not self._enabled_by_env():
            logger.info("Auto-set loop disabled by KIWISCAN_AUTOSET_LOOP")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="kiwi-scan-auto-set-loop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
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
            "launchd_preferred": bool(self._safe_bool(settings.get("useLaunchd"), default=False)),
            "auto_scan_on_block": bool(self._safe_bool(settings.get("autoScanOnBlock"), default=False)),
            "auto_scan_on_startup": bool(self._safe_bool(settings.get("autoScanOnStartup"), default=False)),
            "auto_scan_wspr": bool(self._safe_bool(settings.get("autoScanWspr"), default=False)),
            "last_run_ts": last_run_ts,
            "last_success_ts": last_success_ts,
            "last_error": last_error,
        }

    def force_reassign(self) -> None:
        """Force an immediate re-apply of current automation settings, bypassing all caches.

        Reads the saved settings, builds the same payload the auto-set loop would use,
        and posts it to /auto_set_receivers with ``force=True`` so the endpoint skips its
        15-second deduplication window.  The loop's own signature cache is also cleared so
        the next scheduled cycle re-evaluates from a clean state.
        """
        settings = self._load_settings()
        payload = self._build_payload(settings, schedule_key=self._current_schedule_key(settings))
        payload["force"] = True
        self._post_auto_set(payload)
        # Reset so the next loop cycle re-evaluates even if settings haven't changed.
        self._last_apply_signature = None