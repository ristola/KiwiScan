from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


class AutoSetLoop:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._did_startup_apply = False
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

    def _build_payload(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        mode = str(settings.get("autoScanMode") or "ft8").strip().lower()
        if mode not in {"ft8", "phone"}:
            mode = "ft8"

        return {
            "enabled": True,
            "mode": mode,
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
            startup_enabled = self._safe_bool(settings.get("autoScanOnStartup"), default=False)
            block_enabled = self._safe_bool(settings.get("autoScanOnBlock"), default=False)
            wspr_enabled = self._safe_bool(settings.get("autoScanWspr"), default=False)

            should_apply = bool(headless_enabled and (block_enabled or wspr_enabled))
            if startup_enabled and not self._did_startup_apply:
                should_apply = bool(headless_enabled)

            if should_apply:
                with self._state_lock:
                    self._last_run_ts = time.time()
                try:
                    self._post_auto_set(self._build_payload(settings))
                    with self._state_lock:
                        self._last_success_ts = time.time()
                        self._last_error = None
                    if startup_enabled and not self._did_startup_apply:
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