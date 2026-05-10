from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Awaitable, Callable, Dict, Optional

from .discovery import DiscoveryWorker, FT8_WATERHOLES
from .kiwi_discovery import DEFAULT_KIWI_HOST, is_unconfigured_kiwi_host, normalize_kiwi_host
from .kiwi_waterfall import KiwiClientUnavailable

logger = logging.getLogger(__name__)


class DiscoveryManager:
    def __init__(
        self,
        *,
        get_loop: Callable[[], Optional[asyncio.AbstractEventLoop]],
        broadcast_status: Callable[[Dict], Awaitable[None]],
        compute_s_metrics: Callable[[Dict[str, Dict], float], Dict[str, Dict]],
        waterholes: Optional[Dict[str, float]] = None,
    ) -> None:
        self._get_loop = get_loop
        self._broadcast_status = broadcast_status
        self._compute_s_metrics = compute_s_metrics
        self._waterholes: Dict[str, float] = dict(waterholes or FT8_WATERHOLES)

        self.lock = threading.Lock()
        # default config
        # Default dwell. Closed bands can stop early via fast-scan.
        self.dwell_s = 6.0
        self.span_hz = 3000.0
        # Global/default detection threshold (used when no per-band override exists)
        self.threshold_db = 15.0
        # Per-band detection thresholds. If a band key is present here, it overrides
        # the global `threshold_db` during scanning.
        self.threshold_db_by_band: Dict[str, float] = {}
        self.fps = 2.0
        # Calibration offset applied when converting derived dBm -> S units.
        # This is a UI/diagnostic aid; it does not change the peak detector.
        self.s_meter_offset_db = 0.0

        # Location (used for schedule heuristics; stored for UI config).
        self.latitude = 38.6
        self.longitude = -78.4

        # Fast scan: stop early on “closed” bands based on a quick S proxy.
        self.fast_scan_enabled = True
        self.fast_scan_s_threshold = 3.0
        self.fast_scan_min_frames = 2
        self.fast_scan_min_duration_s = 1.5

        # Small pause between retunes (seconds). Lower = faster sweeps.
        self.retune_pause_s = 1.0
        # default Kiwi host (override via /config or env var)
        self.host = DEFAULT_KIWI_HOST
        self.port = 8073
        self.password = str(os.environ.get("KIWISCAN_KIWI_PASSWORD", "") or "").strip() or None
        # Disable debug logging by default; enable via /config if needed
        self.debug = False
        # Let the kiwi server choose an available receiver for discovery.
        # This avoids reserving RX0/RX1 so web users can always use them.
        self.rx_chan = None

        # runtime state
        # Pre-populate results so the UI can render a full table immediately.
        self.results: Dict[str, Dict] = {b: {"score": None, "frames": 0, "hits": 0} for b in self._waterholes}
        self.current_band: Optional[str] = None
        self.calibrating_band: Optional[str] = None
        self.last_updated: float = 0.0
        # Monotonic sequence number for status payloads (helps UI ignore out-of-order WS frames)
        self.status_seq: int = 0
        # last camp status seen from Kiwi: dict with keys ok(bool|None), rx(int|None), last_time(float|None)
        self.camp_status: Dict[str, Optional[object]] = {"ok": None, "rx": None, "last_time": None}
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._paused = threading.Event()
        self.runtime_dependencies: Dict[str, object] = {}
        self.configured_kiwis: list[dict[str, object]] = []
        self.discovered_kiwis: list[dict[str, object]] = []
        self.discovery_source: str = ""
        self.discovery_updated_unix: float = 0.0

        # Persist thresholds between restarts
        root = Path(__file__).resolve().parents[2]
        self._config_path = root / "outputs" / "config.json"
        self._thresholds_path = root / "outputs" / "thresholds_by_band.json"
        self._load_config()
        try:
            if self._thresholds_path.exists():
                data = json.loads(self._thresholds_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    tb = data.get("threshold_db_by_band", {})
                    if isinstance(tb, dict):
                        for k, v in tb.items():
                            if k in self._waterholes:
                                self.threshold_db_by_band[str(k)] = float(v)
        except Exception:
            # best-effort: do not prevent server start
            pass

    def _save_thresholds(self) -> None:
        try:
            self._thresholds_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "threshold_db": float(self.threshold_db),
                "threshold_db_by_band": dict(self.threshold_db_by_band),
                "saved_unix": time.time(),
            }
            self._thresholds_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            self._save_config()
        except Exception:
            pass

    def _load_config(self) -> None:
        try:
            if not self._config_path.exists():
                return
            data = json.loads(self._config_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return
        except Exception:
            return

        def _read_float(key: str) -> Optional[float]:
            if key not in data:
                return None
            try:
                return float(data[key])
            except Exception:
                return None

        def _read_int(key: str) -> Optional[int]:
            if key not in data:
                return None
            try:
                return int(data[key])
            except Exception:
                return None

        def _read_bool(key: str) -> Optional[bool]:
            if key not in data:
                return None
            try:
                return bool(data[key])
            except Exception:
                return None

        def _read_str(key: str) -> Optional[str]:
            if key not in data:
                return None
            try:
                return str(data[key])
            except Exception:
                return None

        val = _read_float("dwell_s")
        if val is not None and 0 < val <= 600:
            self.dwell_s = val
        val = _read_float("span_hz")
        if val is not None and 0 < val <= 30000:
            self.span_hz = val
        val = _read_float("threshold_db")
        if val is not None and 0 <= val <= 60:
            self.threshold_db = val
        tb = data.get("threshold_db_by_band")
        if isinstance(tb, dict):
            self.threshold_db_by_band = {str(k): float(v) for k, v in tb.items()}
        val = _read_float("fps")
        if val is not None and 0 < val <= 10:
            self.fps = val
        val = _read_float("s_meter_offset_db")
        if val is not None and -60 <= val <= 60:
            self.s_meter_offset_db = val
        val = _read_float("latitude")
        if val is not None and -90 <= val <= 90:
            self.latitude = val
        val = _read_float("longitude")
        if val is not None and -180 <= val <= 180:
            self.longitude = val
        val = _read_bool("fast_scan_enabled")
        if val is not None:
            self.fast_scan_enabled = val
        val = _read_float("fast_scan_s_threshold")
        if val is not None and 0 <= val <= 25:
            self.fast_scan_s_threshold = val
        val = _read_int("fast_scan_min_frames")
        if val is not None and 1 <= val <= 20:
            self.fast_scan_min_frames = val
        val = _read_float("fast_scan_min_duration_s")
        if val is not None and 0.5 <= val <= 10:
            self.fast_scan_min_duration_s = val
        val = _read_float("retune_pause_s")
        if val is not None and 0 <= val <= 10:
            self.retune_pause_s = val
        val = _read_str("host")
        if val:
            self.host = normalize_kiwi_host(val)
        val = _read_int("port")
        if val is not None and 1 <= val <= 65535:
            self.port = val
        val = _read_bool("debug")
        if val is not None:
            self.debug = val
        configured = self._sanitize_configured_kiwis(data.get("kiwisdrs"))
        if configured:
            self.configured_kiwis = configured
        else:
            legacy_configured = self._legacy_configured_kiwis_from_payload(data)
            if legacy_configured:
                self.configured_kiwis = legacy_configured
        if self.configured_kiwis:
            self._apply_primary_configured_kiwi()
        rd = data.get("runtime_dependencies")
        if isinstance(rd, dict):
            self.runtime_dependencies = dict(rd)
        dk = data.get("discovered_kiwis")
        if isinstance(dk, list):
            self.discovered_kiwis = self._sanitize_discovered_kiwis(dk)
        ds = _read_str("discovery_source")
        if ds is not None:
            self.discovery_source = ds
        val = _read_float("discovery_updated_unix")
        if val is not None and val >= 0:
            self.discovery_updated_unix = val

    @staticmethod
    def _sanitize_discovered_kiwis(raw: object) -> list[dict[str, object]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, object]] = []
        seen: set[tuple[str, int]] = set()
        for item in raw:
            if not isinstance(item, dict):
                continue
            host = str(item.get("host") or "").strip()
            try:
                port = int(item.get("port") or 0)
            except Exception:
                port = 0
            if not host or not (1 <= port <= 65535):
                continue
            key = (host, port)
            if key in seen:
                continue
            seen.add(key)
            entry: dict[str, object] = {
                "host": host,
                "port": port,
            }
            for text_key in ("name", "grid", "sdr_hw", "sw_version", "loc"):
                value = str(item.get(text_key) or "").strip()
                if value:
                    entry[text_key] = value
            for float_key in ("latitude", "longitude"):
                try:
                    value = float(item.get(float_key))
                except Exception:
                    continue
                entry[float_key] = value
            try:
                gps_good = int(item.get("gps_good"))
            except Exception:
                gps_good = None
            if gps_good is not None:
                entry["gps_good"] = gps_good
            out.append(entry)
        return out

    @staticmethod
    def _sanitize_configured_kiwis(raw: object) -> list[dict[str, object]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, object]] = []
        seen: set[tuple[str, int]] = set()
        for item in raw:
            if not isinstance(item, dict):
                continue
            host = normalize_kiwi_host(item.get("host"))
            try:
                port = int(item.get("port") or 0)
            except Exception:
                port = 0
            if is_unconfigured_kiwi_host(host) or not (1 <= port <= 65535):
                continue
            key = (host, port)
            if key in seen:
                continue
            seen.add(key)
            entry: dict[str, object] = {
                "host": host,
                "port": port,
            }
            for text_key in ("grid", "name", "loc", "sdr_hw", "sw_version"):
                value = str(item.get(text_key) or "").strip()
                if value:
                    entry[text_key] = value
            for float_key, lower, upper in (("latitude", -90.0, 90.0), ("longitude", -180.0, 180.0)):
                try:
                    value = float(item.get(float_key))
                except Exception:
                    continue
                if lower <= value <= upper:
                    entry[float_key] = value
            out.append(entry)
        return out

    @staticmethod
    def _legacy_configured_kiwis_from_payload(data: dict[str, object]) -> list[dict[str, object]]:
        host = normalize_kiwi_host(data.get("host"))
        if is_unconfigured_kiwi_host(host):
            return []
        try:
            port = int(data.get("port") or 8073)
        except Exception:
            port = 8073
        if not (1 <= port <= 65535):
            port = 8073
        entry: dict[str, object] = {
            "host": host,
            "port": port,
        }
        for float_key, lower, upper in (("latitude", -90.0, 90.0), ("longitude", -180.0, 180.0)):
            try:
                value = float(data.get(float_key))
            except Exception:
                continue
            if lower <= value <= upper:
                entry[float_key] = value
        grid = str(data.get("kiwi_grid") or "").strip()
        if grid:
            entry["grid"] = grid
        return [entry]

    def _apply_primary_configured_kiwi(self) -> None:
        if not self.configured_kiwis:
            return
        primary = self.configured_kiwis[0]
        host = normalize_kiwi_host(primary.get("host"))
        try:
            port = int(primary.get("port") or self.port)
        except Exception:
            port = self.port
        if not is_unconfigured_kiwi_host(host):
            self.host = host
        if 1 <= port <= 65535:
            self.port = port
        for attr, lower, upper in (("latitude", -90.0, 90.0), ("longitude", -180.0, 180.0)):
            try:
                value = float(primary.get(attr))
            except Exception:
                continue
            if lower <= value <= upper:
                setattr(self, attr, value)

    def _configured_kiwi_payload(self) -> list[dict[str, object]]:
        entries = self._sanitize_configured_kiwis(self.configured_kiwis)
        host = normalize_kiwi_host(self.host)
        if entries:
            if not is_unconfigured_kiwi_host(host):
                entries[0]["host"] = host
                entries[0]["port"] = int(self.port)
                entries[0]["latitude"] = float(self.latitude)
                entries[0]["longitude"] = float(self.longitude)
            self.configured_kiwis = entries
            return entries
        if is_unconfigured_kiwi_host(host):
            self.configured_kiwis = []
            return []
        entry = {
            "host": host,
            "port": int(self.port),
            "latitude": float(self.latitude),
            "longitude": float(self.longitude),
        }
        self.configured_kiwis = [entry]
        return [dict(entry)]

    def _save_config(self) -> None:
        try:
            self._config_path.parent.mkdir(parents=True, exist_ok=True)
            configured_kiwis = self._configured_kiwi_payload()
            payload = {
                "dwell_s": float(self.dwell_s),
                "span_hz": float(self.span_hz),
                "threshold_db": float(self.threshold_db),
                "threshold_db_by_band": dict(self.threshold_db_by_band),
                "fps": float(self.fps),
                "s_meter_offset_db": float(self.s_meter_offset_db),
                "latitude": float(self.latitude),
                "longitude": float(self.longitude),
                "fast_scan_enabled": bool(self.fast_scan_enabled),
                "fast_scan_s_threshold": float(self.fast_scan_s_threshold),
                "fast_scan_min_frames": int(self.fast_scan_min_frames),
                "fast_scan_min_duration_s": float(self.fast_scan_min_duration_s),
                "retune_pause_s": float(self.retune_pause_s),
                "host": normalize_kiwi_host(self.host),
                "port": int(self.port),
                "kiwisdrs": configured_kiwis,
                "debug": bool(self.debug),
                "runtime_dependencies": dict(self.runtime_dependencies),
                "discovered_kiwis": list(self.discovered_kiwis),
                "discovery_source": str(self.discovery_source),
                "discovery_updated_unix": float(self.discovery_updated_unix),
                "saved_unix": time.time(),
            }
            self._config_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

    def set_configured_kiwis(self, kiwis: object, *, save: bool = True) -> None:
        entries = self._sanitize_configured_kiwis(kiwis)
        with self.lock:
            self.configured_kiwis = entries
            if entries:
                self._apply_primary_configured_kiwi()
                self.rx_chan = None
            if save:
                self._save_config()

    def get_configured_kiwis(self) -> list[dict[str, object]]:
        with self.lock:
            return list(self._configured_kiwi_payload())

    def set_discovered_kiwis(self, discovery: Dict[str, object], *, save: bool = True) -> None:
        found = self._sanitize_discovered_kiwis(discovery.get("found"))
        source = str(discovery.get("source") or "").strip()
        updated_unix = time.time()
        with self.lock:
            self.discovered_kiwis = found
            self.discovery_source = source
            self.discovery_updated_unix = updated_unix
            if save:
                self._save_config()

    def get_discovered_kiwis(self) -> Dict[str, object]:
        with self.lock:
            return {
                "found": list(self.discovered_kiwis),
                "source": str(self.discovery_source),
                "updated_unix": float(self.discovery_updated_unix),
            }

    def set_runtime_dependencies(self, report: Dict[str, object], *, save: bool = True) -> None:
        with self.lock:
            payload = dict(report or {})
            payload["updated_unix"] = time.time()
            self.runtime_dependencies = payload
            if save:
                self._save_config()

    def get_runtime_dependencies(self) -> Dict[str, object]:
        with self.lock:
            return dict(self.runtime_dependencies)

    def pause(self) -> None:
        self._pause.set()

    def resume(self) -> None:
        self._pause.clear()
        self._paused.clear()

    def is_paused(self) -> bool:
        return self._paused.is_set()

    def wait_until_paused(self, timeout_s: float) -> bool:
        return self._paused.wait(timeout=timeout_s)

    def _wait_if_paused(self) -> None:
        if not self._pause.is_set():
            return
        self._paused.set()
        while self._pause.is_set() and not self._stop.is_set():
            time.sleep(0.1)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _status_payload(self) -> Dict:
        return {
            "results": self._compute_s_metrics(self.results, float(self.s_meter_offset_db)),
            "current_band": self.current_band,
            "calibrating_band": self.calibrating_band,
            "last_updated": self.last_updated,
            "rx_chan": self.rx_chan,
            "camp_status": self.camp_status,
            "waterholes": self._waterholes,
            "threshold_db": self.threshold_db,
            "threshold_db_by_band": self.threshold_db_by_band,
            "s_meter_offset_db": self.s_meter_offset_db,
            "status_seq": self.status_seq,
            "status_time": time.time(),
        }

    def _maybe_broadcast(self) -> None:
        loop = self._get_loop()
        if loop is None:
            return
        with self.lock:
            self.status_seq += 1
            payload = self._status_payload()
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast_status(payload), loop)
        except Exception:
            pass

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            for band, freq in self._waterholes.items():
                self._wait_if_paused()
                if self._stop.is_set():
                    break

                # Snapshot the latest config so updates from /config take effect
                # on the next band change (no need to wait for a full sweep).
                with self.lock:
                    host = self.host
                    port = self.port
                    debug = self.debug
                    rx_chan = self.rx_chan
                    dwell_s = self.dwell_s
                    span_hz = self.span_hz
                    threshold_db = float(self.threshold_db_by_band.get(band, self.threshold_db))
                    fps = self.fps
                    fast_scan_enabled = self.fast_scan_enabled
                    fast_scan_s_threshold = self.fast_scan_s_threshold
                    fast_scan_min_frames = self.fast_scan_min_frames
                    fast_scan_min_duration_s = self.fast_scan_min_duration_s

                worker = DiscoveryWorker(
                    host=host,
                    port=port,
                    debug=debug,
                    rx_chan=rx_chan,
                    stop_event=self._pause,
                    dwell_s=dwell_s,
                    span_hz=span_hz,
                    threshold_db=threshold_db,
                    frames_per_second=fps,
                    s_meter_offset_db=float(self.s_meter_offset_db),
                    fast_scan_enabled=bool(fast_scan_enabled),
                    fast_scan_s_threshold=float(fast_scan_s_threshold),
                    fast_scan_min_frames=int(fast_scan_min_frames),
                    fast_scan_min_duration_s=float(fast_scan_min_duration_s),
                )

                with self.lock:
                    self.current_band = band

                # Broadcast immediately on band change so the UI highlight stays in sync.
                self._maybe_broadcast()

                try:
                    logger.info(
                        "DISCOVERY: tuning rx_chan=%s band=%s freq=%.6f MHz dwell=%ss",
                        rx_chan,
                        band,
                        float(freq) / 1e6,
                        dwell_s,
                    )
                except Exception:
                    pass

                try:
                    res = worker.measure_freq(band, freq)
                except KiwiClientUnavailable:
                    logger.warning("DISCOVERY: Kiwi client unavailable connecting to %s:%s; will retry later", host, port)
                    with self.lock:
                        self.results = {b: {"score": None, "frames": 0, "hits": 0} for b in self._waterholes}
                        self.last_updated = time.time()
                    time.sleep(5.0)
                    break
                except Exception:
                    logger.exception("DISCOVERY: unexpected error measuring %s", band)
                    res = None

                with self.lock:
                    if res is None:
                        self.results[band] = {"score": 0.0, "frames": 0, "hits": 0}
                    else:
                        self.results[band] = {
                            "score": res.score,
                            "frames": res.frames_sampled,
                            "hits": res.hits,
                            "avg_noise_floor_dbm": getattr(res, "avg_noise_floor_dbm", None),
                            "avg_noise_s": getattr(res, "avg_noise_s", None),
                            "p95_dbm": getattr(res, "p95_dbm", None),
                            "p95_s": getattr(res, "p95_s", None),
                            "max_peak_dbm": getattr(res, "max_peak_dbm", None),
                            "max_peak_s": getattr(res, "max_peak_s", None),
                            "max_peak_rel_db": getattr(res, "max_peak_rel_db", None),
                            "avg_peaks_per_frame": getattr(res, "avg_peaks_per_frame", None),
                            "avg_persistent_per_frame": getattr(res, "avg_persistent_per_frame", None),
                            "hit_persistent_span_hz_avg": getattr(res, "hit_persistent_span_hz_avg", None),
                            "hit_persistent_span_hz_max": getattr(res, "hit_persistent_span_hz_max", None),
                            "hit_persistent_offsets_hz_sample": getattr(res, "hit_persistent_offsets_hz_sample", None),
                        }
                        self.last_updated = time.time()
                        try:
                            if getattr(res, "camp_ok", None) is not None:
                                self.camp_status["ok"] = bool(res.camp_ok)
                                self.camp_status["rx"] = int(res.camp_rx) if res.camp_rx is not None else None
                                self.camp_status["last_time"] = self.last_updated
                        except Exception:
                            pass

                self._maybe_broadcast()

                # Pause between band frequency changes to let the Kiwi settle.
                self._wait_if_paused()
                time.sleep(float(self.retune_pause_s))

            # one cycle completed
            with self.lock:
                self.current_band = None

            # Broadcast the idle/cleared band state so the UI un-highlights.
            self._maybe_broadcast()
            time.sleep(1.0)
