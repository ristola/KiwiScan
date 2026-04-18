from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import math
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .activity_classifier import classify_activity_width
from .auto_set_loop import AutoSetLoop, _FIXED_ASSIGNMENTS
from .bandplan import BANDPLAN, bandplan_label
from .cw_decode import try_decode_cw_wav, validate_cw_message
from .record import RecordRequest, RecorderUnavailable, run_record
from .receiver_manager import ReceiverAssignment
from .scan import classify_candidate_type, run_scan


logger = logging.getLogger(__name__)


def _build_stepwise_freqs_mhz(*, start_mhz: float, end_mhz: float, step_hz: float) -> list[float]:
    start_hz = int(round(float(start_mhz) * 1_000_000.0))
    end_hz = int(round(float(end_mhz) * 1_000_000.0))
    step_hz_int = max(1, int(round(float(step_hz))))
    freqs: list[float] = []
    current_hz = start_hz
    while current_hz <= end_hz:
        freqs.append(round(current_hz / 1_000_000.0, 3))
        current_hz += step_hz_int
    end_freq_mhz = round(end_hz / 1_000_000.0, 3)
    if not freqs or freqs[-1] != end_freq_mhz:
        freqs.append(end_freq_mhz)
    return freqs


def _first_finite_float(*values: object) -> float | None:
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _coerce_bool_flag(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if math.isfinite(number):
            return number != 0.0
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    return None


def _mode_token_set(value: object) -> tuple[str, ...]:
    raw = str(value or "").strip().upper().replace("_", " ").replace("-", " ")
    if not raw:
        return ()
    if raw in {"ALL", "MIX"}:
        return ("FT4", "FT8", "WSPR")
    tokens: list[str] = []
    if "FT4" in raw:
        tokens.append("FT4")
    if "FT8" in raw:
        tokens.append("FT8")
    if "WSPR" in raw or " WS " in f" {raw} ":
        tokens.append("WSPR")
    return tuple(tokens)


def _band_limits_hz(band: str) -> tuple[float, float] | None:
    segments = BANDPLAN.get(str(band))
    if not segments:
        return None
    return (
        min(float(segment.start_hz) for segment in segments),
        max(float(segment.end_hz) for segment in segments),
    )


def _build_scan_centers_hz(*, start_hz: float, end_hz: float, span_hz: float, step_hz: float) -> list[float]:
    centers: list[float] = []
    center = float(start_hz) + (float(span_hz) / 2.0)
    while center <= float(end_hz) - (float(span_hz) / 2.0):
        centers.append(float(center))
        center += float(step_hz)
    if not centers:
        centers = [(float(start_hz) + float(end_hz)) / 2.0]
    return sorted({float(item) for item in centers if item is not None and float(item) > 0.0})


_SMART_SIGNAL_ORDER: tuple[str, ...] = (
    "CW",
    "PHONE",
    "FT8",
    "FT4",
    "WSPR",
    "DIGITAL",
    "SSTV",
    "CARRIER",
    "BIRDIE",
    "WIDEBAND_UNKNOWN",
    "UNKNOWN",
)

_SMART_CANDIDATE_TYPES: tuple[str, ...] = (
    "NARROW_SINGLE",
    "NARROW_MULTI",
    "MEDIUM_DIGITAL",
    "DIGITAL_CLUSTER",
    "WIDEBAND_VOICE",
    "WIDEBAND_IMAGE",
    "UNKNOWN",
)

_SMART_DIGITAL_CANDIDATE_TYPES: frozenset[str] = frozenset({"NARROW_MULTI", "MEDIUM_DIGITAL", "DIGITAL_CLUSTER"})
_SMART_DIGITAL_BANDPLANS: frozenset[str] = frozenset({"CW", "RTTY", "FT8", "FT4", "WSPR", "ALL MODES"})

_SMART_KNOWN_FREQUENCIES_HZ: dict[str, tuple[int, ...]] = {
    "FT8": (
        1_840_000,
        3_573_000,
        5_357_000,
        7_074_000,
        10_136_000,
        14_074_000,
        18_100_000,
        21_074_000,
        24_915_000,
        28_074_000,
        50_313_000,
    ),
    "FT4": (
        3_575_000,
        7_047_500,
        10_140_000,
        14_080_000,
        18_104_000,
        21_140_000,
        24_919_000,
        28_180_000,
        50_318_000,
    ),
    "WSPR": (
        1_836_600,
        3_568_600,
        5_287_200,
        7_038_600,
        10_138_700,
        14_095_600,
        18_104_600,
        21_094_600,
        24_924_600,
        28_124_600,
        50_293_000,
    ),
    "SSTV": (
        3_845_000,
        7_171_000,
        14_230_000,
        21_340_000,
        28_680_000,
    ),
}


@dataclass(frozen=True)
class ReceiverScanBandPlan:
    band: str
    mode_label: str
    cw_freqs_mhz: tuple[float, ...]
    phone_scan_start_mhz: float
    phone_scan_end_mhz: float
    phone_priority_freqs_mhz: tuple[float, ...] = ()


class ReceiverScanService:
    DEFAULT_BAND = "40m"
    DEFAULT_SCAN_MODE = "smart"
    SCAN_MODE_LABELS: dict[str, str] = {
        "smart": "Smart Scan",
        "cw": "CW Scan",
        "phone": "PHONE Scan",
    }
    BAND_PLANS: dict[str, ReceiverScanBandPlan] = {
        "20m": ReceiverScanBandPlan(
            band="20m",
            mode_label="20m IQ",
            cw_freqs_mhz=(14.025, 14.035, 14.045, 14.055),
            phone_scan_start_mhz=14.150,
            phone_scan_end_mhz=14.350,
            phone_priority_freqs_mhz=(14.295, 14.300, 14.305, 14.310),
        ),
        "40m": ReceiverScanBandPlan(
            band="40m",
            mode_label="40m IQ",
            cw_freqs_mhz=(7.025, 7.035, 7.045, 7.055),
            phone_scan_start_mhz=7.125,
            phone_scan_end_mhz=7.300,
        ),
    }
    HOLD_REASON = "receiver_scan"
    RESERVED_RECEIVERS = (0, 1)
    LISTEN_SECONDS = 2.5
    CW_FOLLOWUP_SECONDS = 60
    PHONE_STEP_HZ = 5_000.0
    PHONE_SPAN_HZ = 12_000.0
    PHONE_MIN_WIDTH_HZ = 1_000.0
    PHONE_CLUSTER_MIN_HZ = 1_800.0
    PHONE_CLUSTER_MAX_HZ = 3_200.0
    PHONE_VOICE_MIN_SCORE = 0.18
    PHONE_MAX_FRAMES = 12
    PHONE_EARLY_STOP_FRAMES = 0
    PHONE_ACTIVITY_MIN_SCORE = 45
    RECEIVER_MANAGER_SETTLE_TIMEOUT_S = 30.0
    RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S = 0.1
    RECEIVER_MANAGER_LOCK_TIMEOUT_S = 1.0
    SMART_SCAN_RX_CHAN = 0
    SMART_SCAN_SPAN_HZ = 12_000.0
    SMART_SCAN_STEP_HZ = 8_000.0
    SMART_SCAN_MAX_FRAMES = 10
    SMART_SCAN_MERGE_WINDOW_HZ = 2_500.0
    SMART_SCAN_RESULT_LIMIT = 48
    SMART_SCAN_POLL_INTERVAL_S = 0.15

    @classmethod
    def normalize_band(
        cls,
        band: object,
        *,
        fallback: str | None = None,
        supported_bands: list[str] | tuple[str, ...] | None = None,
    ) -> str | None:
        candidates = tuple(supported_bands or cls.BAND_PLANS.keys())
        band_text = str(band or "").strip().lower()
        for candidate in candidates:
            if candidate.lower() == band_text:
                return candidate
        fallback_text = str(fallback or "").strip().lower()
        for candidate in candidates:
            if candidate.lower() == fallback_text:
                return candidate
        return None

    @classmethod
    def supported_smart_bands(cls) -> list[str]:
        return list(BANDPLAN.keys())

    @classmethod
    def supported_dedicated_bands(cls) -> list[str]:
        return list(cls.BAND_PLANS.keys())

    @classmethod
    def normalize_scan_mode(cls, mode: object, *, fallback: str | None = None) -> str | None:
        mode_text = str(mode or "").strip().lower()
        if mode_text in cls.SCAN_MODE_LABELS:
            return mode_text
        fallback_text = str(fallback or "").strip().lower()
        if fallback_text in cls.SCAN_MODE_LABELS:
            return fallback_text
        return None

    def _current_scan_mode(self) -> str:
        return self.normalize_scan_mode(
            getattr(self, "_scan_mode", self.DEFAULT_SCAN_MODE),
            fallback=self.DEFAULT_SCAN_MODE,
        ) or self.DEFAULT_SCAN_MODE

    def _smart_band_scan_available(self) -> bool:
        return getattr(self, "_band_scanner", None) is not None

    def _supported_bands_for_mode(self, scan_mode: str | None = None) -> list[str]:
        resolved_mode = self.normalize_scan_mode(
            scan_mode,
            fallback=getattr(self, "_scan_mode", self.DEFAULT_SCAN_MODE),
        ) or self.DEFAULT_SCAN_MODE
        if resolved_mode == "smart":
            return self.supported_smart_bands()
        return self.supported_dedicated_bands()

    def _current_band(self) -> str:
        fallback_band = getattr(self, "_band", self.DEFAULT_BAND)
        supported_bands = self._supported_bands_for_mode(getattr(self, "_scan_mode", self.DEFAULT_SCAN_MODE))
        band_key = self.normalize_band(
            getattr(self, "_band", fallback_band),
            fallback=str(fallback_band),
            supported_bands=supported_bands,
        )
        if band_key is not None:
            return band_key
        if self.DEFAULT_BAND in supported_bands:
            return self.DEFAULT_BAND
        return supported_bands[0] if supported_bands else self.DEFAULT_BAND

    def _current_band_plan(self) -> ReceiverScanBandPlan:
        return self.BAND_PLANS.get(self.band) or self.BAND_PLANS[self.DEFAULT_BAND]

    @property
    def band(self) -> str:
        return self._current_band()

    @property
    def BAND(self) -> str:
        return self.band

    @property
    def scan_mode(self) -> str:
        return self._current_scan_mode()

    @property
    def scan_mode_label(self) -> str:
        return self.SCAN_MODE_LABELS[self.scan_mode]

    @property
    def MODE_LABEL(self) -> str:
        plan = self.BAND_PLANS.get(self.band)
        if plan is not None:
            return plan.mode_label
        return f"{self.band} IQ"

    @property
    def CW_FREQS_MHZ(self) -> list[float]:
        plan = self.BAND_PLANS.get(self.band)
        return list(plan.cw_freqs_mhz) if plan is not None else []

    @property
    def PHONE_SCAN_START_MHZ(self) -> float:
        plan = self.BAND_PLANS.get(self.band)
        return float(plan.phone_scan_start_mhz) if plan is not None else 0.0

    @property
    def PHONE_SCAN_END_MHZ(self) -> float:
        plan = self.BAND_PLANS.get(self.band)
        return float(plan.phone_scan_end_mhz) if plan is not None else 0.0

    @property
    def PHONE_PRIORITY_FREQS_MHZ(self) -> list[float]:
        plan = self.BAND_PLANS.get(self.band)
        if plan is None:
            return []
        return [round(float(freq_mhz), 3) for freq_mhz in plan.phone_priority_freqs_mhz]

    @property
    def PHONE_FREQS_MHZ(self) -> list[float]:
        base_freqs = _build_stepwise_freqs_mhz(
            start_mhz=self.PHONE_SCAN_START_MHZ,
            end_mhz=self.PHONE_SCAN_END_MHZ,
            step_hz=self.PHONE_STEP_HZ,
        )
        ordered_freqs: list[float] = []
        seen_freqs: set[float] = set()
        for freq_mhz in [*self.PHONE_PRIORITY_FREQS_MHZ, *base_freqs]:
            rounded = round(float(freq_mhz), 3)
            if rounded < round(self.PHONE_SCAN_START_MHZ, 3) or rounded > round(self.PHONE_SCAN_END_MHZ, 3):
                continue
            if rounded in seen_freqs:
                continue
            seen_freqs.add(rounded)
            ordered_freqs.append(rounded)
        return ordered_freqs

    def _enabled_lanes(self, scan_mode: str | None = None) -> tuple[str, ...]:
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        if resolved_mode == "smart":
            return ("smart",) if self._smart_band_scan_available() else ()
        if resolved_mode == "cw":
            return ("cw",)
        return ("phone",)

    def _lane_enabled(self, lane_key: str, scan_mode: str | None = None) -> bool:
        return str(lane_key or "").strip().lower() in self._enabled_lanes(scan_mode)

    def _cw_followup_enabled(self, scan_mode: str | None = None) -> bool:
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        if resolved_mode == "smart":
            return False
        return self._lane_enabled("cw", resolved_mode)

    def _reserved_receivers_for_mode(self, scan_mode: str | None = None) -> list[int]:
        enabled_lanes = self._enabled_lanes(scan_mode)
        if "smart" in enabled_lanes:
            return [int(rx_chan) for rx_chan in self.RESERVED_RECEIVERS]
        lane_to_rx = {"smart": self.SMART_SCAN_RX_CHAN, "cw": 0, "phone": 1}
        return [lane_to_rx[lane_key] for lane_key in enabled_lanes if lane_key in lane_to_rx]

    def _scan_order_for_mode(self, scan_mode: str | None = None) -> list[str]:
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        if resolved_mode == "smart":
            return ["smart"] if self._smart_band_scan_available() else []
        if resolved_mode == "cw":
            return ["cw", "cw_followup"]
        return ["phone"]

    def _inactive_lane_summary(self, lane_key: str, scan_mode: str | None = None) -> str:
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        if lane_key == "smart":
            if resolved_mode == "smart":
                return "Waiting for SMART scan" if self._smart_band_scan_available() else "SMART band scan unavailable"
            return "Waiting for SMART scan"
        if lane_key == "cw" and resolved_mode == "smart":
            return "CW lane inactive during SMART scan"
        if lane_key == "phone" and resolved_mode == "smart":
            return "PHONE lane inactive during SMART scan"
        if lane_key == "cw" and resolved_mode == "phone":
            return "CW lane inactive for PHONE-only scan"
        if lane_key == "phone" and resolved_mode == "cw":
            return "PHONE lane inactive for CW-only scan"
        return "Waiting for scan"

    def __init__(
        self,
        *,
        receiver_mgr: object,
        auto_set_loop: AutoSetLoop | None = None,
        band_scanner: object | None = None,
        output_root: Path | None = None,
    ) -> None:
        self._receiver_mgr = receiver_mgr
        self._auto_set_loop = auto_set_loop
        self._band_scanner = band_scanner
        self._output_root = output_root or (Path(__file__).resolve().parents[2] / "outputs" / "receiver_scans")
        self._lock = threading.Lock()
        self._band = self.DEFAULT_BAND
        self._scan_mode = self.DEFAULT_SCAN_MODE
        self._thread: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._activating = False
        self._mode_active = False
        self._release_requested = False
        self._running = False
        self._last_error: str | None = None
        self._last_started_ts: float | None = None
        self._last_finished_ts: float | None = None
        self._session_id: str | None = None
        self._results: dict[str, list[dict[str, Any]]] = {"smart": [], "cw": [], "phone": []}
        self._smart_report_path: str | None = None
        self._lanes: dict[str, dict[str, Any]] = self._initial_lanes(scan_mode=self.scan_mode)
        self._cw_followup: dict[str, Any] = self._initial_cw_followup(scan_mode=self.scan_mode)

    def _initial_lanes(self, *, scan_mode: str | None = None) -> dict[str, dict[str, Any]]:
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        smart_enabled = self._lane_enabled("smart", resolved_mode)
        cw_enabled = self._lane_enabled("cw", resolved_mode)
        phone_enabled = self._lane_enabled("phone", resolved_mode)
        return {
            "smart": {
                "lane": "smart",
                "label": "Hybrid IQ Band Scan",
                "rx_chan": int(self.SMART_SCAN_RX_CHAN),
                "status": "idle" if smart_enabled else "inactive",
                "completed": 0,
                "total": self._smart_window_total(self.band) if smart_enabled else 0,
                "current_freq_mhz": None,
                "last_score": None,
                "last_summary": "Waiting for SMART band scan" if smart_enabled else self._inactive_lane_summary("smart", resolved_mode),
            },
            "cw": {
                "lane": "cw",
                "label": "CW Anchors",
                "rx_chan": 0,
                "status": "idle" if cw_enabled else "inactive",
                "completed": 0,
                "total": len(self.CW_FREQS_MHZ) if cw_enabled else 0,
                "current_freq_mhz": None,
                "last_score": None,
                "last_summary": self._inactive_lane_summary("cw", resolved_mode),
            },
            "phone": {
                "lane": "phone",
                "label": "Phone Anchors",
                "rx_chan": 1,
                "status": "idle" if phone_enabled else "inactive",
                "completed": 0,
                "total": len(self.PHONE_FREQS_MHZ) if phone_enabled else 0,
                "current_freq_mhz": None,
                "last_score": None,
                "last_summary": self._inactive_lane_summary("phone", resolved_mode),
            },
        }

    def _initial_cw_followup(self, *, scan_mode: str | None = None) -> dict[str, Any]:
        followup_enabled = self._cw_followup_enabled(scan_mode)
        resolved_mode = self.normalize_scan_mode(scan_mode, fallback=self.scan_mode) or self.DEFAULT_SCAN_MODE
        if resolved_mode == "smart":
            default_summary = "CW follow-up disabled for SMART scan"
        elif followup_enabled:
            default_summary = "Waiting for CW scan"
        else:
            default_summary = "CW follow-up inactive for PHONE-only scan"
        return {
            "status": "idle" if followup_enabled else "inactive",
            "rx_chan": int(self.RESERVED_RECEIVERS[0]),
            "duration_s": int(self.CW_FOLLOWUP_SECONDS),
            "selected_freq_mhz": None,
            "signal_count": 0,
            "score": None,
            "recording_path": None,
            "wav_path": None,
            "decoded_text": "",
            "validated_text": "",
            "message_valid": False,
            "validation_reason": "",
            "validation_summary": "Waiting for CW scan",
            "confidence": 0.0,
            "tone_hz": None,
            "dot_ms": None,
            "wpm_est": None,
            "completed": 0,
            "total": 0,
            "validated_count": 0,
            "items": [],
            "summary": default_summary,
        }

    def _spawn_thread(self, *, name: str, target: Callable[[], None]) -> threading.Thread:
        return threading.Thread(name=name, target=target, daemon=True)

    def _smart_band_limits(self, band: str | None = None) -> tuple[float, float] | None:
        resolved_band = self.normalize_band(
            band or self.band,
            fallback=self.band,
            supported_bands=self.supported_smart_bands(),
        )
        if resolved_band is None:
            return None
        return _band_limits_hz(resolved_band)

    def _smart_window_total(self, band: str | None = None) -> int:
        limits = self._smart_band_limits(band)
        if limits is None:
            return 0
        centers = _build_scan_centers_hz(
            start_hz=limits[0],
            end_hz=limits[1],
            span_hz=float(self.SMART_SCAN_SPAN_HZ),
            step_hz=float(self.SMART_SCAN_STEP_HZ),
        )
        return len(centers)

    @staticmethod
    def _near_known_frequency(abs_freq_hz: float, known_list: tuple[int, ...], tol_hz: float) -> bool:
        return any(abs(float(abs_freq_hz) - float(freq_hz)) <= float(tol_hz) for freq_hz in known_list)

    @staticmethod
    def _frequency_hint_score(abs_freq_hz: float, known_list: tuple[int, ...], tol_hz: float) -> float:
        if float(tol_hz) <= 0.0 or not known_list:
            return 0.0
        best_delta_hz = min(abs(float(abs_freq_hz) - float(freq_hz)) for freq_hz in known_list)
        if best_delta_hz > float(tol_hz):
            return 0.0
        return max(0.0, min(1.0, 1.0 - (best_delta_hz / float(tol_hz))))

    @staticmethod
    def _frequency_window_hint_score(
        abs_freq_hz: float,
        known_list: tuple[int, ...],
        *,
        lower_offset_hz: float,
        upper_offset_hz: float,
        edge_slop_hz: float,
    ) -> float:
        if not known_list or float(upper_offset_hz) < float(lower_offset_hz) or float(edge_slop_hz) <= 0.0:
            return 0.0
        best = 0.0
        for base_freq_hz in known_list:
            low_hz = float(base_freq_hz) + float(lower_offset_hz)
            high_hz = float(base_freq_hz) + float(upper_offset_hz)
            if low_hz <= float(abs_freq_hz) <= high_hz:
                return 1.0
            edge_distance_hz = min(abs(float(abs_freq_hz) - low_hz), abs(float(abs_freq_hz) - high_hz))
            if edge_distance_hz <= float(edge_slop_hz):
                best = max(best, max(0.0, min(1.0, 1.0 - (edge_distance_hz / float(edge_slop_hz)))))
        return best

    @staticmethod
    def _frequency_window_range_hint_score(
        low_freq_hz: float,
        high_freq_hz: float,
        known_list: tuple[int, ...],
        *,
        lower_offset_hz: float,
        upper_offset_hz: float,
        edge_slop_hz: float,
    ) -> float:
        if not known_list or float(upper_offset_hz) < float(lower_offset_hz) or float(edge_slop_hz) <= 0.0:
            return 0.0

        range_low_hz = min(float(low_freq_hz), float(high_freq_hz))
        range_high_hz = max(float(low_freq_hz), float(high_freq_hz))
        best = 0.0
        for base_freq_hz in known_list:
            window_low_hz = float(base_freq_hz) + float(lower_offset_hz)
            window_high_hz = float(base_freq_hz) + float(upper_offset_hz)
            if max(range_low_hz, window_low_hz) <= min(range_high_hz, window_high_hz):
                return 1.0
            edge_distance_hz = min(
                abs(range_high_hz - window_low_hz),
                abs(range_low_hz - window_high_hz),
            )
            if edge_distance_hz <= float(edge_slop_hz):
                best = max(best, max(0.0, min(1.0, 1.0 - (edge_distance_hz / float(edge_slop_hz)))))
        return best

    @classmethod
    def _smart_signal_counts(cls, items: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            signal_type = str(item.get("signal_type") or "UNKNOWN").strip().upper() or "UNKNOWN"
            counts[signal_type] = counts.get(signal_type, 0) + 1
        return {
            signal_type: counts[signal_type]
            for signal_type in _SMART_SIGNAL_ORDER
            if counts.get(signal_type)
        }

    @classmethod
    def _smart_counts_summary_text(cls, items: list[dict[str, Any]]) -> str:
        counts = cls._smart_signal_counts(items)
        if not counts:
            return "No merged SMART detections yet"
        parts = [f"{counts[signal_type]} {signal_type}" for signal_type in _SMART_SIGNAL_ORDER if counts.get(signal_type)]
        return ", ".join(parts)

    @staticmethod
    def _known_band_mode_frequency_hz(band: str, mode: str) -> float | None:
        known_list = _SMART_KNOWN_FREQUENCIES_HZ.get(str(mode or "").strip().upper())
        band_limits = _band_limits_hz(band)
        if not known_list or band_limits is None:
            return None
        low_hz, high_hz = band_limits
        for freq_hz in known_list:
            if float(low_hz) <= float(freq_hz) <= float(high_hz):
                return float(freq_hz)
        return None

    def _decoder_backed_smart_results(self, current_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not hasattr(self._receiver_mgr, "health_summary"):
            return []

        try:
            summary = self._receiver_mgr.health_summary()  # type: ignore[attr-defined]
        except Exception:
            logger.exception("Receiver Scan failed reading receiver manager health summary")
            return []
        if not isinstance(summary, dict):
            return []
        channels = summary.get("channels")
        if not isinstance(channels, dict):
            return []

        current_band = str(self.BAND or "").strip().lower()
        existing_modes: set[str] = set()
        for item in current_items:
            if not isinstance(item, dict):
                continue
            item_band = str(item.get("band") or self.BAND or "").strip().lower()
            if item_band != current_band:
                continue
            for value in (item.get("signal_type"), item.get("mode_hint")):
                mode = str(value or "").strip().upper()
                if mode in {"FT4", "FT8", "WSPR"}:
                    existing_modes.add(mode)

        observed_modes: dict[str, dict[str, Any]] = {}
        for channel in channels.values():
            if not isinstance(channel, dict):
                continue
            if not bool(channel.get("active")):
                continue
            channel_band = str(channel.get("band") or "").strip().lower()
            if channel_band != current_band:
                continue
            if str(channel.get("health_state") or "").strip().lower() == "stalled":
                continue

            decode_rates_by_mode = channel.get("decode_rates_by_mode")
            mode_stats_map = decode_rates_by_mode if isinstance(decode_rates_by_mode, dict) else {}
            candidate_modes = set(_mode_token_set(channel.get("mode")))
            candidate_modes.update(
                str(mode or "").strip().upper()
                for mode in mode_stats_map.keys()
                if str(mode or "").strip().upper() in {"FT4", "FT8", "WSPR"}
            )

            for mode in sorted(candidate_modes):
                if mode in existing_modes or mode not in {"FT4", "FT8", "WSPR"}:
                    continue
                stats = mode_stats_map.get(mode) if isinstance(mode_stats_map.get(mode), dict) else {}
                decode_total = max(
                    0,
                    int(round(_first_finite_float(stats.get("decode_total"), 0.0) or 0.0)),
                )
                decode_rate_per_min = max(
                    0,
                    int(round(_first_finite_float(stats.get("decode_rate_per_min"), 0.0) or 0.0)),
                )
                decode_rate_per_hour = max(
                    0,
                    int(round(_first_finite_float(stats.get("decode_rate_per_hour"), 0.0) or 0.0)),
                )
                if not stats and mode in _mode_token_set(channel.get("mode")):
                    decode_total = max(0, int(round(_first_finite_float(channel.get("decode_total"), 0.0) or 0.0)))
                    decode_rate_per_min = max(
                        0,
                        int(round(_first_finite_float(channel.get("decode_rate_per_min"), 0.0) or 0.0)),
                    )
                    decode_rate_per_hour = max(
                        0,
                        int(round(_first_finite_float(channel.get("decode_rate_per_hour"), 0.0) or 0.0)),
                    )
                if decode_total <= 0 and decode_rate_per_min <= 0 and decode_rate_per_hour <= 0:
                    continue

                freq_hz = self._known_band_mode_frequency_hz(self.BAND, mode)
                if freq_hz is None:
                    continue
                rx_value = max(0, int(round(_first_finite_float(channel.get("rx"), 0.0) or 0.0)))
                entry = observed_modes.setdefault(
                    mode,
                    {
                        "band": self.BAND,
                        "freq_hz": freq_hz,
                        "freq_mhz": round(float(freq_hz) / 1_000_000.0, 6),
                        "signal_type": mode,
                        "mode_hint": mode,
                        "candidate_type": "DIGITAL_CLUSTER",
                        "confidence": 0.99,
                        "score": 0,
                        "hit_count": 1,
                        "event_count": 1,
                        "merged_count": 1,
                        "decoder_backed": True,
                        "source": "decoder_activity",
                        "decode_total": 0,
                        "decode_rate_per_min": 0,
                        "decode_rate_per_hour": 0,
                        "rx_channels": [],
                    },
                )
                entry["decode_total"] += decode_total
                entry["decode_rate_per_min"] += decode_rate_per_min
                entry["decode_rate_per_hour"] += decode_rate_per_hour
                if rx_value not in entry["rx_channels"]:
                    entry["rx_channels"].append(rx_value)

        supplemental_items: list[dict[str, Any]] = []
        for mode, entry in observed_modes.items():
            total = int(entry.get("decode_total") or 0)
            per_hour = int(entry.get("decode_rate_per_hour") or 0)
            per_min = int(entry.get("decode_rate_per_min") or 0)
            channels_used = len(entry.get("rx_channels") or [])
            entry["score"] = int(min(999, 120 + total + (per_hour * 2) + (per_min * 6) + (channels_used * 5)))
            entry["hit_count"] = max(1, per_min, per_hour, total)
            entry["event_count"] = max(1, total)
            entry["summary"] = (
                f"{mode} decode activity observed on {self.BAND}: "
                f"{total} total, {per_hour}/hr, {per_min}/min"
            )
            entry["rx_channels"] = sorted(int(rx) for rx in entry.get("rx_channels") or [])
            supplemental_items.append(entry)
        return supplemental_items

    def _smart_results_with_decoder_activity(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        base_items = [dict(item) for item in items if isinstance(item, dict)]
        supplemental_items = self._decoder_backed_smart_results(base_items)
        if not supplemental_items:
            return base_items
        combined = [*base_items, *supplemental_items]
        return sorted(combined, key=self._smart_item_strength, reverse=True)

    @staticmethod
    def _smart_item_freq_hz(item: dict[str, Any]) -> float | None:
        freq_hz = _first_finite_float(item.get("freq_hz"), None)
        if freq_hz is not None and freq_hz > 0.0:
            return float(freq_hz)
        freq_mhz = _first_finite_float(item.get("freq_mhz"), None)
        if freq_mhz is not None and freq_mhz > 0.0:
            return float(freq_mhz) * 1_000_000.0
        return None

    @staticmethod
    def _smart_item_strength(item: dict[str, Any]) -> float:
        return (
            max(0.0, _first_finite_float(item.get("score"), 0.0) or 0.0)
            + (max(0.0, _first_finite_float(item.get("confidence"), 0.0) or 0.0) * 100.0)
            + max(0.0, _first_finite_float(item.get("max_rel_db"), item.get("rel_db"), 0.0) or 0.0)
        )

    def _aggregate_smart_group(self, items: list[dict[str, Any]]) -> dict[str, Any] | None:
        group_items = [dict(item) for item in items if isinstance(item, dict)]
        if not group_items:
            return None

        representative = max(group_items, key=self._smart_item_strength)
        weighted_freq_sum = 0.0
        weighted_center_sum = 0.0
        weight_total = 0.0
        freq_values_hz: list[float] = []
        freq_low_values_hz: list[float] = []
        freq_high_values_hz: list[float] = []
        width_values_hz: list[float] = []
        rel_values: list[float] = []
        voice_values: list[float] = []
        occ_frac_values: list[float] = []
        speech_values: list[float] = []
        sweep_values: list[float] = []
        envelope_values: list[float] = []
        centroid_values: list[float] = []
        keying_values: list[float] = []
        steady_values: list[float] = []
        stability_values: list[float] = []
        observed_values: list[int] = []
        active_values: list[float] = []
        cadence_values: list[float] = []
        narrow_count_values: list[int] = []
        narrow_span_values: list[float] = []
        keying_edge_values: list[int] = []
        amplitude_span_values: list[float] = []
        bandplan_counts: dict[str, int] = {}
        candidate_counts: dict[str, int] = {}
        hit_count = 0
        event_count = 0
        raw_event_count = 0
        has_on_off_keying: bool | None = None

        for item in group_items:
            freq_hz = self._smart_item_freq_hz(item)
            if freq_hz is None:
                continue
            weight = max(1.0, self._smart_item_strength(item))
            weighted_freq_hz = _first_finite_float(item.get("weighted_freq_hz"), freq_hz) or freq_hz
            freq_low_hz = _first_finite_float(item.get("freq_low_hz"), freq_hz) or freq_hz
            freq_high_hz = _first_finite_float(item.get("freq_high_hz"), freq_hz) or freq_hz
            if freq_high_hz < freq_low_hz:
                freq_low_hz, freq_high_hz = freq_high_hz, freq_low_hz
            center_freq_hz = _first_finite_float(
                item.get("center_freq_hz"),
                (_first_finite_float(item.get("center_freq_mhz"), None) or 0.0) * 1_000_000.0,
                freq_hz,
            ) or freq_hz
            weighted_freq_sum += float(weighted_freq_hz) * weight
            weighted_center_sum += float(center_freq_hz) * weight
            weight_total += weight
            freq_values_hz.append(float(freq_hz))
            freq_low_values_hz.append(float(freq_low_hz))
            freq_high_values_hz.append(float(freq_high_hz))
            width_values_hz.append(_first_finite_float(item.get("occupied_bw_hz"), item.get("occ_bw_hz"), item.get("width_hz"), 0.0) or 0.0)
            rel_values.append(_first_finite_float(item.get("max_rel_db"), item.get("rel_db"), 0.0) or 0.0)
            voice_values.append(_first_finite_float(item.get("voice_score"), 0.0) or 0.0)
            occ_frac_values.append(_first_finite_float(item.get("occ_frac"), 0.0) or 0.0)
            speech_values.append(_first_finite_float(item.get("speech_envelope_score"), 0.0) or 0.0)
            sweep_values.append(_first_finite_float(item.get("sweep_score"), 0.0) or 0.0)
            envelope_values.append(_first_finite_float(item.get("envelope_variance"), 0.0) or 0.0)
            centroid_values.append(_first_finite_float(item.get("centroid_drift_hz"), 0.0) or 0.0)
            keying_values.append(_first_finite_float(item.get("keying_score"), 0.0) or 0.0)
            steady_values.append(_first_finite_float(item.get("steady_tone_score"), 0.0) or 0.0)
            stability_values.append(_first_finite_float(item.get("freq_stability_hz"), 0.0) or 0.0)
            observed_values.append(max(0, int(round(_first_finite_float(item.get("observed_frames"), 0.0) or 0.0))))
            active_values.append(max(0.0, min(1.0, _first_finite_float(item.get("active_fraction"), 0.0) or 0.0)))
            cadence_values.append(max(0.0, min(1.0, _first_finite_float(item.get("cadence_score"), 0.0) or 0.0)))
            narrow_count_values.append(max(0, int(round(_first_finite_float(item.get("narrow_peak_count"), 0.0) or 0.0))))
            narrow_span_values.append(_first_finite_float(item.get("narrow_peak_span_hz"), 0.0) or 0.0)
            keying_edge_values.append(max(0, int(round(_first_finite_float(item.get("keying_edge_count"), 0.0) or 0.0))))
            amplitude_span_values.append(max(0.0, _first_finite_float(item.get("amplitude_span_db"), 0.0) or 0.0))
            hit_count += max(1, int(round(_first_finite_float(item.get("hit_count"), 1.0) or 1.0)))
            event_count += max(1, int(round(_first_finite_float(item.get("event_count"), 1.0) or 1.0)))
            raw_event_count += max(1, int(round(_first_finite_float(item.get("raw_event_count"), 1.0) or 1.0)))
            raw_keying_flag = _coerce_bool_flag(item.get("has_on_off_keying"))
            if raw_keying_flag is True:
                has_on_off_keying = True
            elif raw_keying_flag is False and has_on_off_keying is None:
                has_on_off_keying = False

            bandplan_hint = str(item.get("bandplan_label") or item.get("bandplan") or "").strip().upper()
            if bandplan_hint:
                bandplan_counts[bandplan_hint] = bandplan_counts.get(bandplan_hint, 0) + 1
            merged_candidate_counts = item.get("merged_candidate_counts")
            merged_candidate_counts_present = False
            if isinstance(merged_candidate_counts, dict):
                for raw_candidate_type, raw_count in merged_candidate_counts.items():
                    candidate_key = str(raw_candidate_type or "").strip().upper()
                    if not candidate_key:
                        continue
                    try:
                        candidate_count = int(raw_count)
                    except (TypeError, ValueError):
                        continue
                    if candidate_count <= 0:
                        continue
                    candidate_counts[candidate_key] = candidate_counts.get(candidate_key, 0) + candidate_count
                    merged_candidate_counts_present = True
            if not merged_candidate_counts_present:
                candidate_type = str(item.get("candidate_type") or "UNKNOWN").strip().upper() or "UNKNOWN"
                candidate_counts[candidate_type] = candidate_counts.get(candidate_type, 0) + 1

        if not freq_values_hz:
            return None

        representative_freq_hz = self._smart_item_freq_hz(representative)
        weighted_freq_hz = weighted_freq_sum / weight_total if weight_total > 0.0 else float(freq_values_hz[0])
        freq_hz = representative_freq_hz if representative_freq_hz is not None else (weighted_freq_sum / weight_total if weight_total > 0.0 else float(freq_values_hz[0]))
        center_freq_hz = weighted_center_sum / weight_total if weight_total > 0.0 else freq_hz
        freq_low_hz = min(freq_low_values_hz, default=min(freq_values_hz))
        freq_high_hz = max(freq_high_values_hz, default=max(freq_values_hz))
        freq_span_hz = max(0.0, float(freq_high_hz) - float(freq_low_hz))
        width_hz = max(max(width_values_hz, default=0.0), freq_span_hz)
        voice_score = max(voice_values, default=0.0)
        occ_frac = max(occ_frac_values, default=0.0)
        speech_envelope_score = max(speech_values, default=0.0)
        sweep_score = max(sweep_values, default=0.0)
        envelope_variance = max(envelope_values, default=0.0)
        centroid_drift_hz = max(centroid_values, default=0.0)
        keying_score = max(keying_values, default=0.0)
        steady_tone_score = max(steady_values, default=0.0)
        freq_stability_hz = min(stability_values) if stability_values else 0.0
        observed_frames = max(observed_values, default=0)
        active_fraction = sum(active_values) / len(active_values) if active_values else 0.0
        cadence_score = max(cadence_values, default=0.0)
        narrow_peak_count = max(narrow_count_values, default=0)
        narrow_peak_span_hz = max(max(narrow_span_values, default=0.0), freq_span_hz)
        keying_edge_count = max(keying_edge_values, default=0)
        amplitude_span_db = max(amplitude_span_values, default=0.0)
        bandplan_hint = max(bandplan_counts.items(), key=lambda item: (item[1], item[0]))[0] if bandplan_counts else str(bandplan_label(freq_hz) or "").strip().upper()
        bandplan_key = bandplan_hint.strip().upper()
        voice_envelope_detected = envelope_variance >= 0.12 or speech_envelope_score >= 0.42
        digital_bandplan = bandplan_key in _SMART_DIGITAL_BANDPLANS
        digital_cluster = (
            narrow_peak_count > 4
            and width_hz <= 1_800.0
            and voice_score < 0.40
            and speech_envelope_score < 0.34
            and not voice_envelope_detected
        ) or (
            narrow_peak_count > 6
            and width_hz <= 2_400.0
            and digital_bandplan
            and voice_score < 0.35
            and speech_envelope_score < 0.30
            and not voice_envelope_detected
        )
        image_candidate = (
            width_hz >= 1_800.0
            and sweep_score >= 0.50
            and sweep_score >= speech_envelope_score
            and not digital_cluster
        )
        voice_candidate = (
            width_hz >= 1_500.0
            and voice_score >= 0.40
            and speech_envelope_score >= 0.24
            and voice_envelope_detected
            and narrow_peak_count <= 4
            and not digital_cluster
        )
        representative_candidate = str(representative.get("candidate_type") or "UNKNOWN").strip().upper() or "UNKNOWN"
        if width_hz < 100.0 and (has_on_off_keying or keying_edge_count >= 4 or keying_score >= 0.32):
            candidate_type = "NARROW_SINGLE"
        elif image_candidate and not digital_bandplan:
            candidate_type = "WIDEBAND_IMAGE"
        elif voice_candidate and not digital_bandplan:
            candidate_type = "WIDEBAND_VOICE"
        elif digital_cluster:
            candidate_type = "DIGITAL_CLUSTER"
        else:
            candidate_type = classify_candidate_type(
                width_hz=width_hz,
                type_guess=representative.get("type_guess"),
                bandplan_label=bandplan_hint or None,
                voice_score=voice_score,
                occ_frac=occ_frac,
                speech_score=speech_envelope_score,
                sweep_score=sweep_score,
                keying_score=keying_score,
                cadence_score=cadence_score,
                narrow_peak_count=narrow_peak_count,
                envelope_variance=envelope_variance,
                has_on_off_keying=has_on_off_keying,
            )
            if candidate_type == "UNKNOWN" and representative_candidate in _SMART_CANDIDATE_TYPES:
                candidate_type = representative_candidate

        merged = dict(representative)
        digital_candidate_count = sum(candidate_counts.get(candidate_key, 0) for candidate_key in _SMART_DIGITAL_CANDIDATE_TYPES)
        wideband_candidate_count = candidate_counts.get("WIDEBAND_VOICE", 0) + candidate_counts.get("WIDEBAND_IMAGE", 0)
        unknown_candidate_count = candidate_counts.get("UNKNOWN", 0)
        candidate_total_count = sum(candidate_counts.values())
        merged.update(
            {
                "freq_hz": float(freq_hz),
                "freq_mhz": round(float(freq_hz) / 1_000_000.0, 6),
                "freq_low_hz": float(freq_low_hz),
                "freq_high_hz": float(freq_high_hz),
                "weighted_freq_hz": float(weighted_freq_hz),
                "center_freq_hz": float(center_freq_hz),
                "center_freq_mhz": round(float(center_freq_hz) / 1_000_000.0, 6),
                "occ_bw_hz": float(width_hz),
                "occupied_bw_hz": float(width_hz),
                "width_hz": float(width_hz),
                "rel_db": max(rel_values, default=0.0),
                "max_rel_db": max(rel_values, default=0.0),
                "voice_score": float(voice_score),
                "occ_frac": float(occ_frac),
                "speech_envelope_score": float(speech_envelope_score),
                "sweep_score": float(sweep_score),
                "envelope_variance": float(envelope_variance),
                "centroid_drift_hz": float(centroid_drift_hz),
                "keying_score": float(keying_score),
                "steady_tone_score": float(steady_tone_score),
                "freq_stability_hz": float(freq_stability_hz),
                "observed_frames": int(observed_frames),
                "active_fraction": float(active_fraction),
                "cadence_score": float(cadence_score),
                "narrow_peak_count": int(narrow_peak_count),
                "narrow_peak_span_hz": float(narrow_peak_span_hz),
                "keying_edge_count": int(keying_edge_count),
                "has_on_off_keying": has_on_off_keying,
                "amplitude_span_db": float(amplitude_span_db),
                "hit_count": max(1, int(hit_count)),
                "event_count": max(1, int(event_count)),
                "raw_event_count": max(1, int(raw_event_count)),
                "bandplan": bandplan_hint or representative.get("bandplan"),
                "bandplan_label": bandplan_hint or None,
                "candidate_type": candidate_type,
                "candidate_total_count": int(candidate_total_count),
                "digital_candidate_count": int(digital_candidate_count),
                "wideband_candidate_count": int(wideband_candidate_count),
                "unknown_candidate_count": int(unknown_candidate_count),
                "merged_candidates": sorted(candidate_counts),
                "merged_candidate_counts": dict(sorted(candidate_counts.items())),
            }
        )
        return merged

    def _classify_smart_hit(self, hit: dict[str, Any]) -> dict[str, Any] | None:
        freq_hz = _first_finite_float(hit.get("freq_hz"), None)
        if freq_hz is None or freq_hz <= 0.0:
            freq_mhz = _first_finite_float(hit.get("freq_mhz"), None)
            if freq_mhz is not None and freq_mhz > 0.0:
                freq_hz = float(freq_mhz) * 1_000_000.0
        if freq_hz is None or freq_hz <= 0.0:
            return None

        band = str(hit.get("band") or self.BAND or "").strip() or (self.BAND or "")
        bandplan_hint = str(hit.get("bandplan") or bandplan_label(freq_hz) or "").strip()
        occ_bw_hz = _first_finite_float(hit.get("occ_bw_hz"), hit.get("occupied_bw_hz"), hit.get("width_hz"))
        width_hz = occ_bw_hz
        occ_frac = _first_finite_float(hit.get("occ_frac"), None)
        rel_db = _first_finite_float(hit.get("rel_db"), hit.get("max_rel_db"), 0.0) or 0.0
        voice_score = _first_finite_float(hit.get("voice_score"), 0.0) or 0.0
        center_freq_hz = _first_finite_float(
            hit.get("center_freq_hz"),
            (_first_finite_float(hit.get("center_freq_mhz"), None) or 0.0) * 1_000_000.0,
            freq_hz,
        ) or freq_hz
        weighted_freq_hz = _first_finite_float(hit.get("weighted_freq_hz"), freq_hz) or freq_hz
        freq_low_hz = _first_finite_float(hit.get("freq_low_hz"), min(freq_hz, weighted_freq_hz)) or min(freq_hz, weighted_freq_hz)
        freq_high_hz = _first_finite_float(hit.get("freq_high_hz"), max(freq_hz, weighted_freq_hz)) or max(freq_hz, weighted_freq_hz)
        if freq_high_hz < freq_low_hz:
            freq_low_hz, freq_high_hz = freq_high_hz, freq_low_hz
        narrow_peak_count = max(0, int(round(_first_finite_float(hit.get("narrow_peak_count"), 0.0) or 0.0)))
        narrow_peak_span_hz = _first_finite_float(hit.get("narrow_peak_span_hz"), 0.0) or 0.0
        keying_score = _first_finite_float(hit.get("keying_score"), 0.0) or 0.0
        steady_tone_score = _first_finite_float(hit.get("steady_tone_score"), 0.0) or 0.0
        freq_stability_hz = _first_finite_float(hit.get("freq_stability_hz"), 0.0) or 0.0
        envelope_variance = _first_finite_float(hit.get("envelope_variance"), 0.0) or 0.0
        speech_envelope_score = _first_finite_float(hit.get("speech_envelope_score"), 0.0) or 0.0
        sweep_score = _first_finite_float(hit.get("sweep_score"), 0.0) or 0.0
        centroid_drift_hz = _first_finite_float(hit.get("centroid_drift_hz"), 0.0) or 0.0
        observed_frames = max(0, int(round(_first_finite_float(hit.get("observed_frames"), 0.0) or 0.0)))
        active_fraction = max(0.0, min(1.0, _first_finite_float(hit.get("active_fraction"), 0.0) or 0.0))
        cadence_score = max(0.0, min(1.0, _first_finite_float(hit.get("cadence_score"), 0.0) or 0.0))
        amplitude_span_db = max(0.0, _first_finite_float(hit.get("amplitude_span_db"), 0.0) or 0.0)
        keying_edge_count = max(0, int(round(_first_finite_float(hit.get("keying_edge_count"), 0.0) or 0.0)))
        raw_keying_flag = _coerce_bool_flag(hit.get("has_on_off_keying"))
        pulse_metrics_present = raw_keying_flag is not None or keying_edge_count > 0 or amplitude_span_db > 0.0
        if raw_keying_flag is None:
            has_on_off_keying = keying_edge_count > 3 if pulse_metrics_present else (
                keying_edge_count > 3 or (keying_score >= 0.38 and cadence_score >= 0.20 and active_fraction <= 0.85)
            )
        else:
            has_on_off_keying = raw_keying_flag
        hit_count = max(1, int(round(_first_finite_float(hit.get("hit_count"), 1.0) or 1.0)))
        activity_hint = classify_activity_width(
            width_hz,
            type_guess=hit.get("type_guess"),
            bandplan=bandplan_hint or None,
        )
        candidate_type = str(hit.get("candidate_type") or "").strip().upper()
        if candidate_type not in _SMART_CANDIDATE_TYPES:
            candidate_type = classify_candidate_type(
                width_hz=width_hz,
                type_guess=hit.get("type_guess"),
                bandplan_label=bandplan_hint or None,
                voice_score=voice_score,
                occ_frac=occ_frac,
                speech_score=speech_envelope_score,
                sweep_score=sweep_score,
                keying_score=keying_score,
                cadence_score=cadence_score,
                narrow_peak_count=narrow_peak_count,
                envelope_variance=envelope_variance,
                has_on_off_keying=has_on_off_keying,
            )

        bandplan_key = bandplan_hint.strip().upper()
        digital_bandplan = bandplan_key in _SMART_DIGITAL_BANDPLANS
        offset_from_center_hz = abs(float(freq_hz) - float(center_freq_hz))
        ft8_direct_hint = max(
            self._frequency_hint_score(freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["FT8"], 450.0),
            self._frequency_window_hint_score(
                freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["FT8"],
                lower_offset_hz=-120.0,
                upper_offset_hz=3_100.0,
                edge_slop_hz=350.0,
            ),
        )
        ft8_weighted_hint = max(
            self._frequency_hint_score(weighted_freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["FT8"], 450.0),
            self._frequency_window_hint_score(
                weighted_freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["FT8"],
                lower_offset_hz=-120.0,
                upper_offset_hz=3_100.0,
                edge_slop_hz=350.0,
            ),
        )
        ft8_range_hint = self._frequency_window_range_hint_score(
            freq_low_hz,
            freq_high_hz,
            _SMART_KNOWN_FREQUENCIES_HZ["FT8"],
            lower_offset_hz=-120.0,
            upper_offset_hz=3_100.0,
            edge_slop_hz=350.0,
        )
        ft8_hint = max(ft8_direct_hint, ft8_weighted_hint, ft8_range_hint)
        ft4_direct_hint = max(
            self._frequency_hint_score(freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["FT4"], 450.0),
            self._frequency_window_hint_score(
                freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["FT4"],
                lower_offset_hz=-120.0,
                upper_offset_hz=2_800.0,
                edge_slop_hz=350.0,
            ),
        )
        ft4_weighted_hint = max(
            self._frequency_hint_score(weighted_freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["FT4"], 450.0),
            self._frequency_window_hint_score(
                weighted_freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["FT4"],
                lower_offset_hz=-120.0,
                upper_offset_hz=2_800.0,
                edge_slop_hz=350.0,
            ),
        )
        ft4_range_hint = self._frequency_window_range_hint_score(
            freq_low_hz,
            freq_high_hz,
            _SMART_KNOWN_FREQUENCIES_HZ["FT4"],
            lower_offset_hz=-120.0,
            upper_offset_hz=2_800.0,
            edge_slop_hz=350.0,
        )
        ft4_hint = max(ft4_direct_hint, ft4_weighted_hint, ft4_range_hint)
        wspr_direct_hint = max(
            self._frequency_hint_score(freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["WSPR"], 220.0),
            self._frequency_window_hint_score(
                freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["WSPR"],
                lower_offset_hz=1_300.0,
                upper_offset_hz=1_700.0,
                edge_slop_hz=180.0,
            ),
        )
        wspr_weighted_hint = max(
            self._frequency_hint_score(weighted_freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["WSPR"], 220.0),
            self._frequency_window_hint_score(
                weighted_freq_hz,
                _SMART_KNOWN_FREQUENCIES_HZ["WSPR"],
                lower_offset_hz=1_300.0,
                upper_offset_hz=1_700.0,
                edge_slop_hz=180.0,
            ),
        )
        wspr_range_hint = self._frequency_window_range_hint_score(
            freq_low_hz,
            freq_high_hz,
            _SMART_KNOWN_FREQUENCIES_HZ["WSPR"],
            lower_offset_hz=1_300.0,
            upper_offset_hz=1_700.0,
            edge_slop_hz=180.0,
        )
        wspr_hint = max(wspr_direct_hint, wspr_weighted_hint, wspr_range_hint)
        sstv_hint = self._frequency_hint_score(freq_hz, _SMART_KNOWN_FREQUENCIES_HZ["SSTV"], 1_500.0)
        digital_cluster = narrow_peak_count >= 5 and narrow_peak_span_hz >= 120.0
        wspr_cluster = narrow_peak_count >= 2 and 6.0 <= narrow_peak_span_hz <= 220.0
        very_narrow_single = width_hz is not None and width_hz < 50.0 and narrow_peak_count <= 1 and narrow_peak_span_hz <= 25.0
        keyed_cw = has_on_off_keying and (keying_edge_count > 3 or keying_score >= 0.38) and steady_tone_score <= 0.88 and freq_stability_hz <= 40.0 and active_fraction < 0.96
        voice_envelope_detected = envelope_variance >= 0.12 or speech_envelope_score >= 0.42
        speech_like = speech_envelope_score >= 0.22 and sweep_score <= 0.62 and voice_envelope_detected
        sweep_like = (
            sweep_score >= 0.25
            and centroid_drift_hz >= 120.0
            and sweep_score >= (speech_envelope_score * 0.95)
        )
        cluster_score = max(0.0, min(1.0, (float(narrow_peak_count) - 2.0) / 4.0))
        span_score = max(0.0, min(1.0, float(narrow_peak_span_hz) / 2_500.0))
        observed_fit = max(0.0, min(1.0, float(observed_frames) / 8.0))
        cross_window_fit = max(0.0, min(1.0, float(hit_count - 1) / 2.0))
        stability_fit = max(0.0, min(1.0, 1.0 - (float(freq_stability_hz) / 35.0)))
        no_keying_fit = max(0.0, min(1.0, 1.0 - float(keying_score)))
        low_speech_fit = max(0.0, min(1.0, 1.0 - float(speech_envelope_score)))
        burst_fit = max(0.0, min(1.0, 1.0 - (abs(float(active_fraction) - 0.55) / 0.55))) if active_fraction > 0.0 else 0.0
        anti_steady_fit = max(0.0, min(1.0, 1.0 - float(steady_tone_score)))
        side_energy_low = narrow_peak_count <= 1 and narrow_peak_span_hz <= 25.0 and (occ_frac is None or occ_frac <= 0.18)
        persistent_always_on = observed_frames >= 6 and active_fraction >= 0.90
        very_narrow_unkeyed = very_narrow_single and not has_on_off_keying and keying_edge_count <= 3
        digital_repeat = cadence_score >= 0.18 or hit_count >= 2
        strong_medium_digital_cluster = (
            candidate_type == "MEDIUM_DIGITAL"
            and narrow_peak_count >= 4
            and narrow_peak_span_hz >= 300.0
            and observed_frames >= 6
            and 0.35 <= active_fraction <= 0.88
            and cadence_score >= 0.50
            and keying_edge_count >= 4
            and occ_bw_hz is not None
            and 350.0 <= float(occ_bw_hz) <= 1_400.0
            and voice_score <= 0.32
            and steady_tone_score <= 0.76
            and sweep_score <= 0.45
        )
        weak_cw_bandplan_bonus = 0.06 if bandplan_key == "CW" else 0.0
        weak_phone_bandplan_bonus = 0.05 if bandplan_key == "PHONE" else 0.0
        weak_digital_bandplan_bonus = 0.05 if bandplan_key in {"CW", "PHONE", "RTTY", "FT8", "FT4", "WSPR"} else 0.0
        merged_candidate_counts_raw = hit.get("merged_candidate_counts")
        merged_candidate_counts: dict[str, int] = {}
        if isinstance(merged_candidate_counts_raw, dict):
            for raw_key, raw_value in merged_candidate_counts_raw.items():
                candidate_key = str(raw_key or "").strip().upper()
                if not candidate_key:
                    continue
                try:
                    candidate_count = int(raw_value)
                except (TypeError, ValueError):
                    continue
                if candidate_count > 0:
                    merged_candidate_counts[candidate_key] = candidate_count
        if not merged_candidate_counts and candidate_type in _SMART_CANDIDATE_TYPES:
            merged_candidate_counts = {candidate_type: 1}
        candidate_total_count = max(
            0,
            int(round(_first_finite_float(hit.get("candidate_total_count"), float(sum(merged_candidate_counts.values()))) or 0.0)),
        )
        if candidate_total_count <= 0 and merged_candidate_counts:
            candidate_total_count = int(sum(merged_candidate_counts.values()))
        digital_candidate_count = max(
            0,
            int(
                round(
                    _first_finite_float(
                        hit.get("digital_candidate_count"),
                        float(sum(merged_candidate_counts.get(candidate_key, 0) for candidate_key in _SMART_DIGITAL_CANDIDATE_TYPES)),
                    )
                    or 0.0
                )
            ),
        )
        wideband_candidate_count = max(
            0,
            int(
                round(
                    _first_finite_float(
                        hit.get("wideband_candidate_count"),
                        float(merged_candidate_counts.get("WIDEBAND_VOICE", 0) + merged_candidate_counts.get("WIDEBAND_IMAGE", 0)),
                    )
                    or 0.0
                )
            ),
        )
        unknown_candidate_count = max(
            0,
            int(round(_first_finite_float(hit.get("unknown_candidate_count"), float(merged_candidate_counts.get("UNKNOWN", 0))) or 0.0)),
        )
        provided_candidate_mix = (
            isinstance(merged_candidate_counts_raw, dict)
            or hit.get("candidate_total_count") is not None
            or hit.get("digital_candidate_count") is not None
            or hit.get("wideband_candidate_count") is not None
            or hit.get("unknown_candidate_count") is not None
        )
        digital_candidate_evidence_required = provided_candidate_mix and bandplan_key in {"PHONE", "RTTY", "FT8", "FT4", "WSPR", "ALL MODES"}
        digital_candidate_evidence = (not digital_candidate_evidence_required) or digital_candidate_count > 0
        digital_candidate_ratio = (float(digital_candidate_count) / float(candidate_total_count)) if candidate_total_count > 0 else 0.0
        unresolved_candidate_ratio = (
            float(digital_candidate_count + unknown_candidate_count) / float(candidate_total_count)
            if candidate_total_count > 0
            else 0.0
        )
        wideband_candidate_ratio = (float(wideband_candidate_count) / float(candidate_total_count)) if candidate_total_count > 0 else 0.0

        width_fit_ft8 = 0.0
        width_fit_ft4 = 0.0
        width_fit_wspr = 0.0
        width_fit_phone = 0.0
        width_fit_sstv = 0.0
        if width_hz is not None and width_hz > 0.0:
            width_fit_ft8 = max(0.0, min(1.0, 1.0 - (abs(float(width_hz) - 180.0) / 500.0)))
            width_fit_ft4 = max(0.0, min(1.0, 1.0 - (abs(float(width_hz) - 300.0) / 700.0)))
            width_fit_wspr = max(0.0, min(1.0, 1.0 - (abs(float(width_hz) - 50.0) / 150.0)))
            width_fit_phone = max(0.0, min(1.0, (float(width_hz) - 1_400.0) / 1_400.0))
            width_fit_sstv = max(0.0, min(1.0, 1.0 - (abs(float(width_hz) - 2_400.0) / 1_600.0)))

        carrier_score = (
            (0.22 * max(0.0, min(1.0, steady_tone_score)))
            + (0.16 * max(0.0, min(1.0, 1.0 - cadence_score)))
            + (0.16 * no_keying_fit)
            + (0.14 * stability_fit)
            + (0.14 * max(0.0, min(1.0, active_fraction)))
            + (0.10 * observed_fit)
            + (0.08 * low_speech_fit)
        )
        carrier_like = (
            width_hz is not None
            and width_hz <= 90.0
            and persistent_always_on
            and side_energy_low
            and steady_tone_score >= 0.82
            and keying_score <= 0.12
            and cadence_score <= 0.12
            and amplitude_span_db <= 6.0
            and speech_envelope_score <= 0.10
            and sweep_score <= 0.10
            and freq_stability_hz <= 10.0
            and envelope_variance <= 0.14
        )
        birdie_like = (carrier_like or very_narrow_unkeyed) and (offset_from_center_hz <= 120.0 or bandplan_key not in {"CW", "PHONE", "RTTY"})

        digital_behavior = (
            (0.24 * cluster_score)
            + (0.10 * span_score)
            + (0.18 * cadence_score)
            + (0.16 * burst_fit)
            + (0.12 * anti_steady_fit)
            + (0.10 * cross_window_fit)
            + (0.10 * low_speech_fit)
        )
        ft8_score = digital_behavior + (0.16 * ft8_hint) + (0.04 * width_fit_ft8) + weak_digital_bandplan_bonus
        ft4_score = digital_behavior + (0.16 * ft4_hint) + (0.04 * width_fit_ft4) + weak_digital_bandplan_bonus
        ft8_medium_ready = strong_medium_digital_cluster and ft8_hint >= 0.94 and ft8_score >= 0.72
        ft4_medium_ready = strong_medium_digital_cluster and ft4_hint >= 0.94 and ft4_score >= 0.72
        wspr_behavior = (
            (0.32 * (1.0 if wspr_cluster else 0.0))
            + (0.18 * max(0.0, min(1.0, steady_tone_score)))
            + (0.15 * stability_fit)
            + (0.12 * max(0.0, min(1.0, 1.0 - cadence_score)))
            + (0.07 * observed_fit)
            + (0.15 * active_fraction)
            + (0.01 * cross_window_fit)
        )
        wspr_score = wspr_behavior + (0.16 * wspr_hint) + (0.04 * width_fit_wspr) + weak_digital_bandplan_bonus
        cw_score = (
            (0.42 * max(0.0, min(1.0, keying_score)))
            + (0.20 * cadence_score)
            + (0.14 * stability_fit)
            + (0.12 * max(0.0, min(1.0, 1.0 - steady_tone_score)))
            + (0.12 * observed_fit)
        ) + weak_cw_bandplan_bonus
        phone_score = (
            (0.36 * max(0.0, min(1.0, speech_envelope_score)))
            + (0.18 * max(0.0, min(1.0, voice_score)))
            + (0.18 * width_fit_phone)
            + (0.14 * observed_fit)
            + (0.14 * max(0.0, min(1.0, 1.0 - sweep_score)))
        ) + weak_phone_bandplan_bonus
        sstv_behavior = (
            (0.42 * max(0.0, min(1.0, sweep_score)))
            + (0.18 * max(0.0, min(1.0, float(centroid_drift_hz) / 900.0)))
            + (0.14 * width_fit_sstv)
            + (0.10 * low_speech_fit)
            + (0.16 * observed_fit)
        )
        sstv_score = sstv_behavior + (0.16 * sstv_hint) + weak_phone_bandplan_bonus

        digital_subband_hint = max(ft8_hint, ft4_hint, wspr_hint)
        strong_digital_subband_cluster = (
            digital_bandplan
            and width_hz is not None
            and 800.0 <= float(width_hz) <= 3_200.0
            and narrow_peak_count >= 8
            and narrow_peak_span_hz >= 600.0
            and observed_frames >= 6
            and 0.35 <= active_fraction <= 0.98
            and (cadence_score >= 0.45 or keying_edge_count >= 4 or hit_count >= 2)
        )
        known_digital_window_cluster = (
            width_hz is not None
            and 600.0 <= float(width_hz) <= 3_200.0
            and digital_subband_hint >= 0.75
            and narrow_peak_count >= 6
            and observed_frames >= 6
            and (cadence_score >= 0.30 or keying_edge_count >= 2 or hit_count >= 2)
        )
        candidate_mix_digital_window_cluster = (
            width_hz is not None
            and 1_200.0 <= float(width_hz) <= 3_400.0
            and digital_subband_hint >= 0.94
            and observed_frames >= 6
            and hit_count >= 6
            and narrow_peak_count >= 3
            and (cadence_score >= 0.30 or keying_edge_count >= 3 or digital_candidate_count >= 4)
            and digital_candidate_count >= 3
            and digital_candidate_ratio >= 0.25
            and unresolved_candidate_ratio >= 0.68
            and wideband_candidate_ratio <= 0.35
        )
        hint_locked_digital_window_cluster = (
            width_hz is not None
            and 2_000.0 <= float(width_hz) <= 3_800.0
            and digital_subband_hint >= 0.99
            and observed_frames >= 8
            and hit_count >= 20
            and narrow_peak_count >= 3
            and steady_tone_score >= 0.95
            and keying_score >= 0.45
            and (cadence_score >= 0.55 or keying_edge_count >= 4)
            and digital_candidate_count >= 10
            and digital_candidate_ratio >= 0.30
            and voice_score <= 0.32
            and sweep_score <= 0.50
        )
        ft8_hint_locked_window_cluster = (
            hint_locked_digital_window_cluster
            and ft8_hint >= 0.99
            and unresolved_candidate_ratio >= 0.45
            and wideband_candidate_ratio <= 0.55
        )
        wspr_hint_locked_window_cluster = (
            hint_locked_digital_window_cluster
            and wspr_hint >= 0.99
            and unresolved_candidate_ratio >= 0.60
            and wideband_candidate_ratio <= 0.40
        )
        ft8_range_locked_window_cluster = (
            width_hz is not None
            and 4_000.0 <= float(width_hz) <= 5_600.0
            and ft8_range_hint >= 0.99
            and observed_frames >= 10
            and hit_count >= 40
            and narrow_peak_count >= 4
            and steady_tone_score >= 0.98
            and keying_score >= 0.70
            and keying_edge_count >= 4
            and digital_candidate_count >= 14
            and digital_candidate_ratio >= 0.25
            and unresolved_candidate_ratio >= 0.50
            and wideband_candidate_ratio <= 0.50
            and voice_score <= 0.30
            and speech_envelope_score <= 0.68
            and sweep_score <= 0.65
        )
        wspr_range_locked_window_cluster = (
            width_hz is not None
            and 3_500.0 <= float(width_hz) <= 5_600.0
            and wspr_range_hint >= 0.99
            and observed_frames >= 10
            and hit_count >= 40
            and narrow_peak_count >= 5
            and steady_tone_score >= 0.98
            and keying_score >= 0.70
            and keying_edge_count >= 4
            and digital_candidate_count >= 14
            and digital_candidate_ratio >= 0.25
            and unresolved_candidate_ratio >= 0.44
            and wideband_candidate_ratio <= 0.56
            and voice_score <= 0.28
            and speech_envelope_score <= 0.66
            and sweep_score <= 0.55
        )
        wspr_heavy_mix_range_locked_window_cluster = (
            width_hz is not None
            and 3_500.0 <= float(width_hz) <= 5_600.0
            and wspr_range_hint >= 0.99
            and observed_frames >= 10
            and hit_count >= 40
            and narrow_peak_count >= 10
            and steady_tone_score >= 0.98
            and keying_score >= 0.80
            and cadence_score >= 0.75
            and keying_edge_count >= 4
            and digital_candidate_count >= 32
            and digital_candidate_ratio >= 0.50
            and unresolved_candidate_ratio >= 0.60
            and wideband_candidate_ratio <= 0.40
            and voice_score <= 0.40
            and speech_envelope_score <= 0.66
            and sweep_score <= 0.62
        )
        if candidate_type not in _SMART_DIGITAL_CANDIDATE_TYPES and digital_candidate_evidence and (
            strong_digital_subband_cluster
            or known_digital_window_cluster
            or candidate_mix_digital_window_cluster
            or ft8_hint_locked_window_cluster
            or wspr_hint_locked_window_cluster
            or ft8_range_locked_window_cluster
            or wspr_range_locked_window_cluster
            or wspr_heavy_mix_range_locked_window_cluster
        ):
            candidate_type = "DIGITAL_CLUSTER"

        ft8_candidate_mix_ready = candidate_mix_digital_window_cluster and ft8_hint >= 0.94 and ft8_score >= 0.68
        ft4_candidate_mix_ready = candidate_mix_digital_window_cluster and ft4_hint >= 0.94 and ft4_score >= 0.68
        wspr_candidate_mix_ready = candidate_mix_digital_window_cluster and wspr_hint >= 0.94 and wspr_score >= 0.76
        ft8_hint_locked_ready = ft8_hint_locked_window_cluster and ft8_score >= 0.74
        wspr_hint_locked_ready = wspr_hint_locked_window_cluster and wspr_score >= 0.80
        ft8_range_locked_ready = ft8_range_locked_window_cluster and ft8_score >= 0.74
        wspr_range_locked_ready = (wspr_range_locked_window_cluster or wspr_heavy_mix_range_locked_window_cluster) and wspr_score >= 0.71
        wspr_locked_ready = wspr_hint_locked_ready or wspr_range_locked_ready

        signal_type = "UNKNOWN"
        confidence = 0.4
        if very_narrow_single and keyed_cw:
            signal_type = "CW"
            confidence = min(0.95, 0.40 + (cw_score * 0.68) + (max(rel_db, 0.0) / 160.0))
        elif carrier_like or very_narrow_unkeyed:
            signal_type = "BIRDIE" if birdie_like else "CARRIER"
            confidence = min(0.97, 0.46 + (carrier_score * 0.44) + (max(rel_db, 0.0) / 180.0))
        elif candidate_type == "NARROW_SINGLE":
            if keyed_cw:
                signal_type = "CW"
                confidence = min(0.95, 0.38 + (cw_score * 0.72) + (max(rel_db, 0.0) / 160.0))
            else:
                confidence = min(0.62, 0.30 + (cw_score * 0.35) + (max(rel_db, 0.0) / 170.0))
        elif candidate_type in _SMART_DIGITAL_CANDIDATE_TYPES and width_hz is not None and width_hz > 0.0:
            if (
                digital_candidate_evidence
                and wspr_hint >= 0.70
                and (wspr_score >= 0.80 or wspr_candidate_mix_ready or wspr_locked_ready)
                and (hit_count >= 2 or (observed_frames >= 8 and active_fraction >= 0.85))
                and (wspr_locked_ready or (wspr_score >= ft8_score and wspr_score >= ft4_score))
            ):
                signal_type = "WSPR"
                confidence = min(0.95, 0.42 + (wspr_score * 0.54) + (max(rel_db, 0.0) / 180.0))
            elif digital_candidate_evidence and ft8_hint >= 0.75 and digital_repeat and (((digital_cluster and ft8_score >= 0.78) or ft8_medium_ready or ft8_candidate_mix_ready or ft8_hint_locked_ready or ft8_range_locked_ready) and ft8_score >= ft4_score):
                signal_type = "FT8"
                confidence = min(0.96, 0.40 + (ft8_score * 0.56) + (max(rel_db, 0.0) / 180.0))
            elif digital_candidate_evidence and ft4_hint >= 0.75 and digital_repeat and ((digital_cluster and ft4_score >= 0.78) or ft4_medium_ready or ft4_candidate_mix_ready):
                signal_type = "FT4"
                confidence = min(0.96, 0.40 + (ft4_score * 0.56) + (max(rel_db, 0.0) / 180.0))
            elif candidate_type == "DIGITAL_CLUSTER":
                signal_type = "DIGITAL"
                confidence = min(0.90, 0.40 + (digital_behavior * 0.48) + (max(rel_db, 0.0) / 190.0))
            else:
                confidence = min(0.74, 0.34 + (digital_behavior * 0.44) + (max(rel_db, 0.0) / 190.0))
        elif candidate_type == "WIDEBAND_IMAGE" and width_hz is not None and width_hz > 0.0:
            if not digital_bandplan and sweep_like and sstv_score >= 0.60:
                signal_type = "SSTV"
                confidence = min(0.9, 0.38 + (sstv_score * 0.62) + (max(rel_db, 0.0) / 190.0))
            elif width_hz >= 1_800.0:
                signal_type = "WIDEBAND_UNKNOWN"
                confidence = min(0.74, 0.36 + (sstv_behavior * 0.42) + (max(rel_db, 0.0) / 210.0))
            else:
                confidence = min(0.68, 0.32 + (sstv_behavior * 0.40) + (max(rel_db, 0.0) / 210.0))
        elif candidate_type == "WIDEBAND_VOICE":
            if not digital_bandplan and width_hz is not None and width_hz >= 1_500.0 and speech_like and voice_score >= 0.08 and phone_score >= 0.48:
                signal_type = "PHONE"
                confidence = min(
                    0.95,
                    0.38 + (phone_score * 0.60) + (max(rel_db, 0.0) / 180.0),
                )
            elif width_hz is not None and width_hz >= 1_800.0:
                signal_type = "WIDEBAND_UNKNOWN"
                confidence = min(0.76, 0.36 + (phone_score * 0.40) + (max(rel_db, 0.0) / 210.0))
            else:
                confidence = min(0.7, 0.32 + (phone_score * 0.38) + (max(rel_db, 0.0) / 210.0))
        elif candidate_type == "MEDIUM_DIGITAL":
            confidence = min(0.74, 0.32 + (digital_behavior * 0.34) + (max(rel_db, 0.0) / 200.0))
        elif width_hz is not None and width_hz > 0.0 and activity_hint.get("activity_kind") == "cw" and keyed_cw and cw_score >= 0.50:
            signal_type = "CW"
            confidence = min(0.9, 0.36 + (cw_score * 0.62) + (max(rel_db, 0.0) / 180.0))

        display_freq_hz = float(freq_hz)
        if signal_type == "FT8" and ft8_hint > ft8_direct_hint:
            display_freq_hz = float(weighted_freq_hz)
        elif signal_type == "FT4" and ft4_hint > ft4_direct_hint:
            display_freq_hz = float(weighted_freq_hz)
        elif signal_type == "WSPR" and wspr_hint > wspr_direct_hint:
            display_freq_hz = float(weighted_freq_hz)

        mode_hint = signal_type
        if signal_type == "UNKNOWN":
            if candidate_type in _SMART_DIGITAL_CANDIDATE_TYPES:
                mode_hint = "DIGITAL_CANDIDATE"
            else:
                mode_hint = candidate_type

        score = self._clamp_score(
            (max(rel_db, 0.0) * 4.0)
            + (confidence * 55.0)
            + (max(voice_score, 0.0) * 12.0)
            + (speech_envelope_score * 14.0)
            + (keying_score * 10.0)
            + (cadence_score * 8.0)
            + (sweep_score * 8.0)
        )
        if signal_type in {"CARRIER", "BIRDIE"}:
            score = min(score, 45)
        status = "activity" if score >= 50 else "watch" if score >= 25 else "quiet"
        summary_prefix = f"{signal_type} detection"
        if signal_type == "UNKNOWN" and mode_hint != "UNKNOWN":
            summary_prefix = mode_hint if mode_hint.endswith("_CANDIDATE") else f"{mode_hint} candidate"
        summary = (
            f"{summary_prefix} at {display_freq_hz / 1_000_000.0:.6f} MHz"
            f" | bw {width_hz:.0f} Hz" if width_hz is not None
            else f"{summary_prefix} at {display_freq_hz / 1_000_000.0:.6f} MHz"
        )
        if width_hz is not None:
            summary = (
                f"{summary_prefix} at {display_freq_hz / 1_000_000.0:.6f} MHz"
                f" | bw {width_hz:.0f} Hz | {rel_db:+.1f} dB"
            )
        elif rel_db:
            summary = f"{summary_prefix} at {display_freq_hz / 1_000_000.0:.6f} MHz | {rel_db:+.1f} dB"

        return {
            "lane": "smart",
            "rx_chan": int(self.SMART_SCAN_RX_CHAN),
            "band": band,
            "freq_hz": float(display_freq_hz),
            "freq_mhz": round(float(display_freq_hz) / 1_000_000.0, 6),
            "center_freq_mhz": round(float(center_freq_hz) / 1_000_000.0, 6),
            "freq_low_hz": float(freq_low_hz),
            "freq_high_hz": float(freq_high_hz),
            "weighted_freq_hz": float(weighted_freq_hz),
            "status": status,
            "score": score,
            "summary": summary,
            "signal_count": max(1, hit_count),
            "event_count": max(1, int(round(_first_finite_float(hit.get("event_count"), float(hit_count)) or float(hit_count)))),
            "raw_event_count": max(1, int(round(_first_finite_float(hit.get("raw_event_count"), float(hit_count)) or float(hit_count)))),
            "max_rel_db": rel_db,
            "best_s_est": None,
            "voice_score": voice_score if voice_score > 0.0 else None,
            "occupied_bw_hz": width_hz,
            "mode_hint": mode_hint,
            "candidate_type": candidate_type,
            "candidate_total_count": candidate_total_count,
            "digital_candidate_count": digital_candidate_count,
            "wideband_candidate_count": wideband_candidate_count,
            "unknown_candidate_count": unknown_candidate_count,
            "merged_candidate_counts": dict(sorted(merged_candidate_counts.items())) if merged_candidate_counts else None,
            "activity_kind": activity_hint.get("activity_kind"),
            "bandwidth_bucket": activity_hint.get("bandwidth_bucket"),
            "phone_like": signal_type == "PHONE",
            "signal_type": signal_type,
            "confidence": round(float(confidence), 2),
            "narrow_peak_count": narrow_peak_count,
            "narrow_peak_span_hz": round(float(narrow_peak_span_hz), 1),
            "keying_score": round(float(keying_score), 3),
            "steady_tone_score": round(float(steady_tone_score), 3),
            "freq_stability_hz": round(float(freq_stability_hz), 1),
            "envelope_variance": round(float(envelope_variance), 3),
            "speech_envelope_score": round(float(speech_envelope_score), 3),
            "sweep_score": round(float(sweep_score), 3),
            "centroid_drift_hz": round(float(centroid_drift_hz), 1),
            "observed_frames": observed_frames,
            "active_fraction": round(float(active_fraction), 3),
            "cadence_score": round(float(cadence_score), 3),
            "keying_edge_count": keying_edge_count,
            "has_on_off_keying": bool(has_on_off_keying),
            "amplitude_span_db": round(float(amplitude_span_db), 2),
            "bandplan_label": bandplan_hint or None,
            "detector": hit.get("detector"),
            "hit_count": hit_count,
        }

    def _merge_smart_results(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[list[dict[str, Any]]] = []
        sorted_items = sorted(
            [dict(item) for item in items if isinstance(item, dict)],
            key=lambda item: float(self._smart_item_freq_hz(item) or 0.0),
        )
        for item in sorted_items:
            freq_hz = self._smart_item_freq_hz(item)
            if freq_hz is None:
                continue
            if not merged:
                merged.append([dict(item)])
                continue
            last_group = merged[-1]
            last_freq_hz = self._smart_item_freq_hz(last_group[-1])
            group_freqs_hz = [self._smart_item_freq_hz(existing) for existing in last_group]
            normalized_group_freqs_hz = [float(value) for value in group_freqs_hz if value is not None]
            group_low_hz = min(normalized_group_freqs_hz, default=float(freq_hz))
            group_high_hz = max(normalized_group_freqs_hz, default=float(freq_hz))
            group_span_hz = max(group_high_hz, float(freq_hz)) - min(group_low_hz, float(freq_hz))
            if (
                last_freq_hz is not None
                and abs(float(last_freq_hz) - float(freq_hz)) <= float(self.SMART_SCAN_MERGE_WINDOW_HZ)
                and group_span_hz <= float(self.SMART_SCAN_MERGE_WINDOW_HZ)
            ):
                last_group.append(dict(item))
                continue
            merged.append([dict(item)])
        reclassified: list[dict[str, Any]] = []
        for group in merged:
            grouped_item = self._aggregate_smart_group(group)
            if grouped_item is None:
                continue
            refreshed = self._classify_smart_hit(grouped_item)
            if refreshed is None:
                continue
            refreshed["merged_count"] = len(group)
            reclassified.append(refreshed)
        merged = sorted(
            reclassified,
            key=lambda item: (
                -int(item.get("score") or 0),
                float(item.get("freq_mhz") or 0.0),
            ),
        )
        return merged[: int(self.SMART_SCAN_RESULT_LIMIT)]

    def _ingest_smart_hit(self, hit: dict[str, Any]) -> None:
        with self._lock:
            current_items = [dict(item) for item in self._results.get("smart", [])]
            current_items.append(dict(hit))
            merged_items = self._merge_smart_results(current_items)
            self._results["smart"] = merged_items
            smart_lane = self._lanes.get("smart")
            if isinstance(smart_lane, dict) and merged_items:
                strongest_item = max(merged_items, key=self._smart_item_strength)
                smart_lane["last_score"] = strongest_item.get("score")
                smart_lane["last_summary"] = strongest_item.get("summary")

    def _smart_completed_window_count(self, report: dict[str, Any], *, default_total: int) -> int:
        probe_summaries = report.get("probe_summaries") if isinstance(report, dict) else None
        if isinstance(probe_summaries, list):
            completed = 0
            for summary in probe_summaries:
                if not isinstance(summary, dict):
                    continue
                frames_seen = int(summary.get("frames_seen") or 0)
                ssb_frames_seen = int(summary.get("ssb_frames_seen") or 0)
                if frames_seen > 0 or ssb_frames_seen > 0:
                    completed += 1
            return completed
        return int(report.get("windows") or default_total)

    def _finalize_smart_results(
        self,
        report: dict[str, Any],
        *,
        live_items: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if live_items:
            merged_live = [dict(item) for item in live_items if isinstance(item, dict)]
            if merged_live:
                return merged_live

        finalized_items = report.get("finalized_results") if isinstance(report, dict) else None
        if isinstance(finalized_items, list):
            persisted = [dict(item) for item in finalized_items if isinstance(item, dict)]
            if persisted:
                return persisted

        smart_items: list[dict[str, Any]] = []
        for hit in report.get("hits", []) if isinstance(report, dict) else []:
            if not isinstance(hit, dict) or hit.get("error"):
                continue
            smart_items.append(dict(hit))
        return self._merge_smart_results(smart_items)

    def _run_smart_band_scan(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> None:
        if self._band_scanner is None:
            raise RuntimeError("SMART band scan engine is not available")

        session_id = self._session_id or time.strftime("receiver_scan_%Y%m%d_%H%M%S")
        smart_dir = self._output_root / session_id / "smart"
        smart_record_dir = self._output_root / session_id / "smart_recordings"
        smart_dir.mkdir(parents=True, exist_ok=True)
        smart_record_dir.mkdir(parents=True, exist_ok=True)
        total_windows = self._smart_window_total(self.BAND)

        with self._lock:
            self._results["smart"] = []
            self._smart_report_path = None
            smart_lane = self._lanes["smart"]
            smart_lane["status"] = "scanning"
            smart_lane["completed"] = 0
            smart_lane["total"] = total_windows
            smart_lane["current_freq_mhz"] = None
            smart_lane["last_summary"] = f"Scanning {self.BAND} with {total_windows} IQ windows"

        self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(self.SMART_SCAN_RX_CHAN))
        start_result = self._band_scanner.start(  # type: ignore[attr-defined]
            band=self.BAND,
            host=host,
            port=int(port),
            password=password,
            user=f"Receiver Scan SMART {self.BAND}",
            threshold_db=float(threshold_db),
            rx_chan=int(self.SMART_SCAN_RX_CHAN),
            wf_rx_chan=int(self.SMART_SCAN_RX_CHAN),
            span_hz=float(self.SMART_SCAN_SPAN_HZ),
            step_hz=float(self.SMART_SCAN_STEP_HZ),
            max_frames=int(self.SMART_SCAN_MAX_FRAMES),
            record_seconds=int(self.LISTEN_SECONDS),
            record_hits=False,
            output_dir=smart_dir,
            record_dir=smart_record_dir,
            detector="waterfall",
            ssb_probe_only=False,
            required_hits=1,
            allow_rx_fallback=False,
            acceptable_rx_chans=tuple(self._reserved_receivers_for_mode()),
            before_window_attempt=lambda window_index, center_freq_hz, attempt: self._prepare_smart_window_attempt(
                int(window_index),
                float(center_freq_hz),
                int(attempt),
                host=host,
                port=int(port),
            ),
            on_hit=self._ingest_smart_hit,
            session_id=session_id,
        )
        if not bool(start_result.get("ok")):
            raise RuntimeError(str(start_result.get("error") or start_result.get("status") or "SMART scan failed"))

        report_path = Path(str(start_result.get("report_path"))) if start_result.get("report_path") else None
        while True:
            if self._stop_requested.is_set():
                try:
                    self._band_scanner.stop()  # type: ignore[attr-defined]
                except Exception:
                    logger.exception("Receiver Scan SMART stop request failed")
            band_status = self._band_scanner.status()  # type: ignore[attr-defined]
            progress = band_status.get("progress") if isinstance(band_status, dict) else {}
            last_progress = band_status.get("last_progress") if isinstance(band_status, dict) else {}
            progress_data = progress if isinstance(progress, dict) and progress else last_progress if isinstance(last_progress, dict) else {}
            with self._lock:
                if isinstance(progress_data, dict) and progress_data:
                    smart_lane = self._lanes["smart"]
                    window_total = int(progress_data.get("window_total") or total_windows)
                    window_index = int(progress_data.get("window_index") or 0)
                    center_freq_hz = _first_finite_float(progress_data.get("center_freq_hz"), None)
                    smart_lane["total"] = window_total
                    smart_lane["completed"] = max(0, window_index - (1 if band_status.get("running") else 0))
                    smart_lane["current_freq_mhz"] = (
                        round(float(center_freq_hz) / 1_000_000.0, 3) if center_freq_hz is not None else None
                    )
                    if center_freq_hz is not None:
                        smart_lane["last_summary"] = (
                            f"Scanning window {max(window_index, 1)} of {max(window_total, 1)}"
                            f" around {float(center_freq_hz) / 1_000_000.0:.3f} MHz"
                        )
                if band_status.get("last_error"):
                    self._last_error = str(band_status.get("last_error"))
            if not bool(band_status.get("running")):
                if report_path is None and band_status.get("last_report"):
                    report_path = Path(str(band_status.get("last_report")))
                break
            time.sleep(float(self.SMART_SCAN_POLL_INTERVAL_S))

        report = self._read_json(report_path) if report_path is not None else {}
        with self._lock:
            live_results = [dict(item) for item in self._results.get("smart", []) if isinstance(item, dict)]
        finalized_results = self._finalize_smart_results(report, live_items=live_results)
        counts_summary = self._smart_counts_summary_text(finalized_results)
        if report_path is not None:
            try:
                persisted_report = dict(report) if isinstance(report, dict) else {}
                persisted_report["finalized_results"] = [dict(item) for item in finalized_results]
                persisted_report["smart_summary"] = {
                    "counts": self._smart_signal_counts(finalized_results),
                    "total": len(finalized_results),
                }
                report_path.write_text(json.dumps(persisted_report, indent=2), encoding="utf-8")
            except Exception:
                logger.exception("Receiver Scan failed persisting finalized SMART results")
        with self._lock:
            self._results["smart"] = finalized_results
            self._smart_report_path = str(report_path) if report_path is not None else None
            smart_lane = self._lanes["smart"]
            planned_total = int(report.get("windows") or smart_lane.get("total") or total_windows)
            smart_lane["completed"] = self._smart_completed_window_count(report, default_total=planned_total)
            smart_lane["total"] = planned_total
            smart_lane["last_score"] = max((int(item.get("score") or 0) for item in finalized_results), default=None)
            smart_lane["last_summary"] = counts_summary
            if not self._stop_requested.is_set():
                smart_lane["status"] = "complete"

    def _build_fixed_assignments(self) -> dict[int, ReceiverAssignment]:
        assignments: dict[int, ReceiverAssignment] = {}
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            assignments[rx] = ReceiverAssignment(
                rx=rx,
                band=str(entry["band"]),
                freq_hz=float(entry["freq_hz"]),
                mode_label=str(entry["mode"]),
                ignore_slot_check=True,
            )
        return assignments

    def _clear_reserved_slot(self, *, host: str, port: int, rx_chan: int) -> None:
        kick = getattr(self._receiver_mgr, "_run_admin_kick_all", None)
        wait_clear = getattr(self._receiver_mgr, "_wait_for_kiwi_slots_clear", None)

        if callable(kick):
            try:
                kick(
                    host=host,
                    port=int(port),
                    kick_only_slots=[int(rx_chan)],
                    allow_fallback_kick_all=False,
                )
            except Exception:
                logger.exception("Receiver Scan failed clearing reserved RX%s", int(rx_chan))

        if callable(wait_clear):
            try:
                wait_clear(
                    host=host,
                    port=int(port),
                    slots={int(rx_chan)},
                    stable_secs=0.75,
                    timeout_s=4.0,
                )
            except Exception:
                logger.exception("Receiver Scan failed waiting for reserved RX%s clear", int(rx_chan))

    def _clear_reserved_slots(self, *, host: str, port: int) -> None:
        for rx_chan in self._reserved_receivers_for_mode():
            self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(rx_chan))

    def _prepare_smart_window_attempt(self, window_index: int, center_freq_hz: float, attempt: int, *, host: str, port: int) -> None:
        del window_index, center_freq_hz, attempt
        self._clear_reserved_slots(host=host, port=int(port))

    def _wait_for_receiver_manager_settle(self) -> bool:
        startup_event = getattr(self._receiver_mgr, "_startup_eviction_active", None)
        if startup_event is None or not hasattr(startup_event, "is_set"):
            return True

        started_wait_s = time.time()
        timeout_s = max(0.0, float(self.RECEIVER_MANAGER_SETTLE_TIMEOUT_S))
        deadline = started_wait_s + timeout_s
        waited = False
        while bool(startup_event.is_set()):
            waited = True
            if self._stop_requested.is_set():
                return False
            if timeout_s > 0.0 and time.time() >= deadline:
                logger.warning(
                    "Receiver Scan aborting because receiver manager startup is still active after %.1fs",
                    timeout_s,
                )
                return False
            time.sleep(max(0.0, float(self.RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S)))

        if waited:
            logger.info(
                "Receiver Scan waited %.1fs for receiver manager startup to settle",
                max(0.0, time.time() - started_wait_s),
            )
        return True

    def _wait_for_receiver_manager_lock(self) -> bool:
        manager_lock = getattr(self._receiver_mgr, "_lock", None)
        if manager_lock is None or not hasattr(manager_lock, "acquire") or not hasattr(manager_lock, "release"):
            return True

        timeout_s = max(0.0, float(self.RECEIVER_MANAGER_LOCK_TIMEOUT_S))
        deadline = time.time() + timeout_s
        while True:
            acquired = False
            try:
                acquired = bool(manager_lock.acquire(blocking=False))
            except TypeError:
                acquired = bool(manager_lock.acquire(False))
            except Exception:
                return True
            if acquired:
                manager_lock.release()
                return True
            if self._stop_requested.is_set():
                return False
            if timeout_s > 0.0 and time.time() >= deadline:
                logger.warning(
                    "Receiver Scan aborting because receiver manager lock stayed busy for %.1fs",
                    timeout_s,
                )
                return False
            time.sleep(max(0.0, float(self.RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S)))

    def _enter_mode(self, *, host: str, port: int) -> None:
        paused_external = False
        if self._auto_set_loop is not None:
            self._auto_set_loop.pause_for_external(self.HOLD_REASON)
            paused_external = True
        try:
            settled = self._wait_for_receiver_manager_settle()
            if not settled:
                if self._stop_requested.is_set():
                    raise RuntimeError("Receiver Scan activation was cancelled before receiver manager startup settled")
                raise RuntimeError(
                    f"Receiver manager startup is still active after {float(self.RECEIVER_MANAGER_SETTLE_TIMEOUT_S):.1f}s"
                )
            if not self._wait_for_receiver_manager_lock():
                if self._stop_requested.is_set():
                    raise RuntimeError("Receiver Scan activation was cancelled while receiver manager was busy")
                raise RuntimeError(
                    f"Receiver manager is busy applying assignments after {float(self.RECEIVER_MANAGER_LOCK_TIMEOUT_S):.1f}s"
                )
            self._clear_reserved_slots(host=host, port=int(port))
            assignments = self._build_fixed_assignments()
            self._receiver_mgr.apply_assignments(  # type: ignore[attr-defined]
                host,
                int(port),
                assignments,
                allow_starting_from_empty_full_reset=False,
            )
            with self._lock:
                self._mode_active = True
                self._release_requested = False
        except Exception:
            if paused_external and self._auto_set_loop is not None:
                try:
                    self._auto_set_loop.resume_from_external(self.HOLD_REASON)
                except Exception:
                    logger.exception("Receiver Scan failed resuming auto-set loop after activation failure")
            raise

    def _leave_mode(self) -> None:
        should_resume = False
        with self._lock:
            if self._mode_active:
                should_resume = True
            self._mode_active = False
            self._release_requested = False
        if should_resume and self._auto_set_loop is not None:
            self._auto_set_loop.resume_from_external(self.HOLD_REASON)

    def _state_label_locked(self) -> str:
        if self._activating:
            return "stopping" if self._stop_requested.is_set() else "starting"
        if self._running and self._stop_requested.is_set():
            return "stopping"
        if self._running:
            return "running"
        if self._mode_active:
            return "ready"
        return "idle"

    def status(self) -> dict[str, Any]:
        with self._lock:
            scan_mode = self.scan_mode
            cw_followup = dict(self._cw_followup)
            if isinstance(cw_followup.get("items"), list):
                cw_followup["items"] = [
                    dict(item) for item in cw_followup["items"] if isinstance(item, dict)
                ]
            results = {key: [dict(item) for item in value] for key, value in self._results.items()}
            results = self._annotate_results(results=results, cw_followup=cw_followup)
            smart_results = self._smart_results_with_decoder_activity(results.get("smart", []))
            results["smart"] = [dict(item) for item in smart_results]
            smart_counts = self._smart_signal_counts(smart_results)
            supported_bands = self._supported_bands_for_mode(scan_mode)
            smart_limits = self._smart_band_limits(self.BAND)
            return {
                "ok": True,
                "status": self._state_label_locked(),
                "activating": bool(self._activating),
                "mode_active": bool(self._mode_active),
                "running": bool(self._running),
                "stop_requested": bool(self._stop_requested.is_set()),
                "band": self.BAND,
                "supported_bands": supported_bands,
                "supported_smart_bands": self.supported_smart_bands(),
                "supported_dedicated_bands": self.supported_dedicated_bands(),
                "scan_mode": scan_mode,
                "scan_mode_label": self.SCAN_MODE_LABELS[scan_mode],
                "supported_scan_modes": list(self.SCAN_MODE_LABELS.keys()),
                "mode_label": self.MODE_LABEL,
                "listen_seconds": float(self.LISTEN_SECONDS),
                "session_id": self._session_id,
                "reserved_receivers": self._reserved_receivers_for_mode(scan_mode),
                "fixed_receivers": [int(entry["rx"]) for entry in _FIXED_ASSIGNMENTS],
                "plan": {
                    "scan_order": self._scan_order_for_mode(scan_mode),
                    "parallel_lanes": len(self._enabled_lanes(scan_mode)) > 1,
                    "active_lanes": list(self._enabled_lanes(scan_mode)),
                    "smart_range_mhz": {
                        "start": round(float(smart_limits[0]) / 1_000_000.0, 6),
                        "end": round(float(smart_limits[1]) / 1_000_000.0, 6),
                    } if smart_limits is not None else None,
                    "smart_chunk_bw_hz": float(self.SMART_SCAN_SPAN_HZ),
                    "smart_step_hz": float(self.SMART_SCAN_STEP_HZ),
                    "smart_window_total": int(self._smart_window_total(self.BAND)) if smart_limits is not None else 0,
                    "cw_freqs_mhz": list(self.CW_FREQS_MHZ),
                    "cw_followup_seconds": int(self.CW_FOLLOWUP_SECONDS),
                    "phone_range_mhz": {
                        "start": float(self.PHONE_SCAN_START_MHZ),
                        "end": float(self.PHONE_SCAN_END_MHZ),
                    },
                    "phone_priority_freqs_mhz": list(self.PHONE_PRIORITY_FREQS_MHZ),
                    "phone_freqs_mhz": list(self.PHONE_FREQS_MHZ),
                },
                "lanes": {key: dict(value) for key, value in self._lanes.items()},
                "cw_followup": cw_followup,
                "results": results,
                "smart_summary": {
                    "counts": smart_counts,
                    "total": len(smart_results),
                    "report_path": self._smart_report_path,
                },
                "last_error": self._last_error,
                "last_started_ts": self._last_started_ts,
                "last_finished_ts": self._last_finished_ts,
            }

    @staticmethod
    def _annotate_results(
        *,
        results: dict[str, list[dict[str, Any]]],
        cw_followup: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        followup_items = [
            dict(item) for item in cw_followup.get("items", []) if isinstance(item, dict)
        ]
        target_freq = cw_followup.get("selected_freq_mhz")
        current_status = str(cw_followup.get("status") or "idle").strip().lower()
        if target_freq is not None and current_status not in {"idle", "skipped"}:
            try:
                current_freq = float(target_freq)
            except Exception:
                current_freq = None
            if current_freq is not None and not any(
                abs(float(item.get("selected_freq_mhz") or 0.0) - current_freq) <= 1e-6
                for item in followup_items
                if item.get("selected_freq_mhz") is not None
            ):
                followup_items.append(
                    {
                        "status": cw_followup.get("status"),
                        "rx_chan": cw_followup.get("rx_chan"),
                        "duration_s": cw_followup.get("duration_s"),
                        "selected_freq_mhz": current_freq,
                        "signal_count": cw_followup.get("signal_count"),
                        "score": cw_followup.get("score"),
                        "recording_path": cw_followup.get("recording_path"),
                        "wav_path": cw_followup.get("wav_path"),
                        "decoded_text": cw_followup.get("decoded_text"),
                        "validated_text": cw_followup.get("validated_text"),
                        "message_valid": cw_followup.get("message_valid"),
                        "validation_reason": cw_followup.get("validation_reason"),
                        "validation_summary": cw_followup.get("validation_summary"),
                        "confidence": cw_followup.get("confidence"),
                        "tone_hz": cw_followup.get("tone_hz"),
                        "dot_ms": cw_followup.get("dot_ms"),
                        "wpm_est": cw_followup.get("wpm_est"),
                        "summary": cw_followup.get("summary"),
                    }
                )

        followup_by_freq: dict[float, dict[str, Any]] = {}
        for followup_item in followup_items:
            try:
                freq_key = round(float(followup_item.get("selected_freq_mhz") or 0.0), 6)
            except Exception:
                continue
            followup_by_freq[freq_key] = followup_item

        for item in results.get("cw", []):
            try:
                freq_mhz = float(item.get("freq_mhz") or 0.0)
            except Exception:
                continue
            followup_item = followup_by_freq.get(round(freq_mhz, 6))
            if followup_item is None:
                continue
            item["followup_selected"] = True
            item["followup_status"] = followup_item.get("status")
            item["followup_summary"] = followup_item.get("summary")
            item["followup_decoded_text"] = followup_item.get("decoded_text")
            item["followup_validated_text"] = followup_item.get("validated_text")
            item["followup_message_valid"] = bool(followup_item.get("message_valid"))
            item["followup_validation_reason"] = followup_item.get("validation_reason")
            item["followup_validation_summary"] = followup_item.get("validation_summary")
            item["followup_confidence"] = followup_item.get("confidence")
            item["followup_tone_hz"] = followup_item.get("tone_hz")
            item["followup_wpm_est"] = followup_item.get("wpm_est")

        for item in results.get("smart", []):
            if str(item.get("signal_type") or "").strip().upper() != "CW":
                continue
            try:
                freq_mhz = float(item.get("freq_mhz") or 0.0)
            except Exception:
                continue
            followup_item = followup_by_freq.get(round(freq_mhz, 6))
            if followup_item is None:
                continue
            item["followup_selected"] = True
            item["followup_status"] = followup_item.get("status")
            item["followup_summary"] = followup_item.get("summary")
            item["followup_decoded_text"] = followup_item.get("decoded_text")
            item["followup_validated_text"] = followup_item.get("validated_text")
            item["followup_message_valid"] = bool(followup_item.get("message_valid"))
            item["followup_validation_reason"] = followup_item.get("validation_reason")
            item["followup_validation_summary"] = followup_item.get("validation_summary")
            item["followup_confidence"] = followup_item.get("confidence")
            item["followup_tone_hz"] = followup_item.get("tone_hz")
            item["followup_wpm_est"] = followup_item.get("wpm_est")
        return results

    def health_channels(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            if not self._mode_active:
                return {}
            running = bool(self._running)
            enabled_lanes = set(self._enabled_lanes())
            lanes = {key: dict(value) for key, value in self._lanes.items()}
            cw_followup = dict(self._cw_followup)
            started_ts = float(self._last_started_ts) if self._last_started_ts is not None else None
            last_error = self._last_error

        now = time.time()
        connected_seconds = max(0, int(now - started_ts)) if started_ts is not None else None
        followup_status = str(cw_followup.get("status") or "idle").strip().lower()
        followup_freq_mhz = cw_followup.get("selected_freq_mhz")
        followup_summary = str(cw_followup.get("summary") or "").strip()

        channels: dict[str, dict[str, Any]] = {}
        for lane_key, lane in lanes.items():
            if lane_key not in enabled_lanes:
                continue
            rx_chan = int(lane.get("rx_chan") or 0)
            lane_status = str(lane.get("status") or "idle").strip().lower()
            lane_summary = str(lane.get("last_summary") or "").strip()
            current_freq_mhz = lane.get("current_freq_mhz")
            if lane_key == "smart":
                display_name = "Receiver Scan SMART"
                mode = "SMART"
            elif lane_key == "cw":
                display_name = "Receiver Scan CW"
                mode = "CW"
            else:
                display_name = "Receiver Scan Phone"
                mode = "PHONE"

            if lane_key in {"cw", "smart"} and followup_status in {"recording", "decoding"}:
                display_name = "Receiver Scan CW Follow-up" if lane_key == "cw" else "Receiver Scan SMART CW Follow-up"
                current_freq_mhz = followup_freq_mhz if followup_freq_mhz is not None else current_freq_mhz
                lane_summary = followup_summary or lane_summary
                mode = "CW"
            elif lane_key in {"cw", "smart"} and followup_status in {"complete", "error", "stopped"} and followup_freq_mhz is not None:
                current_freq_mhz = followup_freq_mhz

            status_level = "healthy"
            if lane_status == "error" or (lane_key in {"cw", "smart"} and followup_status == "error"):
                status_level = "fault"
            elif lane_status in {"stopped", "waiting"}:
                status_level = "warning"

            state_text = lane_summary or lane_status.replace("_", " ").title() or "Waiting for scan"
            active = bool(running or lane_status not in {"idle"})
            channels[str(rx_chan)] = {
                "rx": int(rx_chan),
                "kiwi_rx": int(rx_chan),
                "freq_hz": (float(current_freq_mhz) * 1e6) if current_freq_mhz is not None else None,
                "band": self.BAND,
                "mode": mode,
                "active": active,
                "visible_on_kiwi": active,
                "kiwi_user_age_s": connected_seconds,
                "kiwi_actual_rx": int(rx_chan),
                "restart_count": 0,
                "consecutive_failures": 0,
                "backoff_s": 0.0,
                "cooling_down": False,
                "cooldown_remaining_s": 0.0,
                "last_reason": last_error if status_level == "fault" and last_error else (state_text if status_level != "healthy" else None),
                "last_updated_unix": now,
                "last_decoder_output_unix": None,
                "last_decode_unix": None,
                "decoder_output_age_s": None,
                "decode_age_s": None,
                "snr_last_db": None,
                "snr_avg_db": None,
                "snr_samples": 0,
                "snr_age_s": None,
                "decode_total": 0,
                "decode_rate_per_min": 0,
                "decode_rate_per_hour": 0,
                "decode_rates_by_mode": {},
                "propagation_state": "unknown",
                "health_state": state_text,
                "status_level": status_level,
                "is_no_decode_warning": False,
                "is_silent": False,
                "is_stalled": status_level == "fault",
                "is_unstable": status_level == "fault",
                "display_name": display_name,
                "is_scan_channel": True,
            }
        return channels

    def start(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
        band: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        selected_mode = self.normalize_scan_mode(mode, fallback=self.scan_mode if mode is None else None)
        if selected_mode is None:
            payload = self.status()
            payload["ok"] = False
            payload["status"] = "error"
            payload["last_error"] = f"Unsupported receiver scan mode: {mode}"
            return payload
        supported_bands = self._supported_bands_for_mode(selected_mode)
        selected_band = self.normalize_band(
            band,
            fallback=self.band if band is None else None,
            supported_bands=supported_bands,
        )
        if selected_band is None:
            default_band = self.normalize_band(self.DEFAULT_BAND, supported_bands=supported_bands)
            selected_band = default_band or (supported_bands[0] if supported_bands else None)
        if selected_band is None:
            payload = self.status()
            payload["ok"] = False
            payload["status"] = "error"
            payload["last_error"] = f"Unsupported receiver scan band: {band}"
            return payload
        if selected_mode == "smart" and not self._smart_band_scan_available():
            payload = self.status()
            payload["ok"] = False
            payload["status"] = "error"
            payload["last_error"] = "SMART band scan engine is not available"
            return payload

        already_active = False
        with self._lock:
            if self._running or self._activating:
                already_active = True
            else:
                self._band = selected_band
                self._scan_mode = selected_mode
                self._activating = True
                self._stop_requested.clear()
                self._release_requested = False
                self._last_error = None
                self._last_started_ts = time.time()
                self._session_id = time.strftime("receiver_scan_%Y%m%d_%H%M%S")
                self._results = {"smart": [], "cw": [], "phone": []}
                self._smart_report_path = None
                self._lanes = self._initial_lanes(scan_mode=selected_mode)
                self._cw_followup = self._initial_cw_followup(scan_mode=selected_mode)
                for lane in self._lanes.values():
                    if str(lane.get("status") or "") == "inactive":
                        continue
                    lane["status"] = "starting"
                    lane["last_summary"] = "Activating receivers"

        if already_active:
            payload = self.status()
            payload["ok"] = False
            return payload

        thread = self._spawn_thread(
            name="receiver-scan",
            target=lambda: self._activate_and_run_session(
                host=host,
                port=int(port),
                password=password,
                threshold_db=float(threshold_db),
            ),
        )
        with self._lock:
            self._thread = thread
        thread.start()
        payload = self.status()
        return payload

    def stop(self) -> dict[str, Any]:
        with self._lock:
            running = bool(self._running)
            activating = bool(self._activating)
        if not running and not activating:
            return self.status()
        self._stop_requested.set()
        payload = self.status()
        payload["status"] = "stopping"
        return payload

    def deactivate(self, *, wait_timeout_s: float = 8.0) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            active = bool(self._running or self._activating)
            if active:
                self._release_requested = True
                self._stop_requested.set()

        if active and thread is not None:
            thread.join(timeout=max(0.0, float(wait_timeout_s)))

        with self._lock:
            still_running = bool(self._running or self._activating)
        if not still_running:
            self._leave_mode()
        payload = self.status()
        payload["status"] = "stopping" if still_running else "idle"
        return payload

    def _activate_and_run_session(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> None:
        try:
            logger.info("Receiver Scan activation starting on %s:%s", host, port)
            self._enter_mode(host=host, port=port)
        except Exception as exc:
            logger.exception("Receiver Scan activation failed")
            self._leave_mode()
            with self._lock:
                self._activating = False
                self._running = False
                self._thread = None
                self._last_error = f"Receiver Scan activation failed: {exc}"
                self._last_finished_ts = time.time()
                for lane in self._lanes.values():
                    lane["status"] = "error"
                    lane["current_freq_mhz"] = None
                    lane["last_summary"] = self._last_error
            self._stop_requested.clear()
            return

        with self._lock:
            stop_requested = bool(self._stop_requested.is_set())
            release_requested = bool(self._release_requested)
            scan_mode = self.scan_mode
            self._activating = False
            if stop_requested:
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                if self._cw_followup_enabled(scan_mode):
                    self._cw_followup["status"] = "stopped"
                    self._cw_followup["summary"] = "Scan stopped before CW follow-up began"
                for lane in self._lanes.values():
                    if str(lane.get("status") or "") == "inactive":
                        continue
                    lane["status"] = "stopped"
                    lane["current_freq_mhz"] = None
                    lane["last_summary"] = "Scan stopped before probes began"
            else:
                self._running = True
                if self._lane_enabled("smart", scan_mode):
                    self._lanes["smart"]["status"] = "ready"
                    self._lanes["smart"]["current_freq_mhz"] = None
                    self._lanes["smart"]["last_summary"] = "Starting SMART band scan"
                if self._lane_enabled("cw", scan_mode):
                    self._lanes["cw"]["status"] = "ready"
                    self._lanes["cw"]["current_freq_mhz"] = None
                    self._lanes["cw"]["last_summary"] = "Starting CW scan"
                if self._lane_enabled("phone", scan_mode):
                    self._lanes["phone"]["status"] = "ready"
                    self._lanes["phone"]["current_freq_mhz"] = None
                    self._lanes["phone"]["last_summary"] = "Starting Phone scan"
                self._cw_followup = self._initial_cw_followup(scan_mode=scan_mode)

        if stop_requested:
            logger.info("Receiver Scan activation completed after stop request; session will not start")
            self._stop_requested.clear()
            if release_requested:
                self._leave_mode()
            return

        logger.info("Receiver Scan activation complete; starting probes")
        self._run_session(
            host=host,
            port=port,
            password=password,
            threshold_db=threshold_db,
        )

    def _run_session(self, *, host: str, port: int, password: str | None, threshold_db: float) -> None:
        scan_mode = self.scan_mode
        enabled_lanes = set(self._enabled_lanes(scan_mode))
        try:
            if scan_mode == "smart":
                if not self._smart_band_scan_available():
                    raise RuntimeError("SMART band scan engine is not available")
                self._run_smart_band_scan(
                    host=host,
                    port=port,
                    password=password,
                    threshold_db=threshold_db,
                )
            elif scan_mode == "cw":
                self._run_lane(
                    lane_key="cw",
                    rx_chan=0,
                    freqs_mhz=list(self.CW_FREQS_MHZ),
                    host=host,
                    port=port,
                    password=password,
                    threshold_db=threshold_db,
                )
                if not self._stop_requested.is_set() and self._cw_followup_enabled(scan_mode):
                    self._run_cw_followup(host=host, port=port, password=password)
            else:
                self._run_lane(
                    lane_key="phone",
                    rx_chan=1,
                    freqs_mhz=list(self.PHONE_FREQS_MHZ),
                    host=host,
                    port=port,
                    password=password,
                    threshold_db=threshold_db,
                )
        except Exception as exc:
            with self._lock:
                self._last_error = f"Receiver Scan failed: {exc}"
        finally:
            release_requested = False
            session_id = None
            with self._lock:
                for lane_key, lane in self._lanes.items():
                    if lane_key not in enabled_lanes:
                        continue
                    if lane["status"] not in {"error", "stopped", "inactive"}:
                        lane["status"] = "complete"
                        if lane_key != "smart":
                            lane["current_freq_mhz"] = None
                if self._stop_requested.is_set():
                    for lane_key, lane in self._lanes.items():
                        if lane_key not in enabled_lanes:
                            continue
                        if lane["status"] != "error":
                            lane["status"] = "stopped"
                    if self._cw_followup["status"] not in {"complete", "error", "skipped", "stopped", "inactive"}:
                        self._cw_followup["status"] = "stopped"
                        self._cw_followup["summary"] = "Scan stopped during CW follow-up"
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                release_requested = bool(self._release_requested)
                session_id = self._session_id
                self._release_requested = False
            self._write_session_summary(session_id)
            self._stop_requested.clear()
            if release_requested:
                self._leave_mode()

    def _run_lane(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freqs_mhz: list[float],
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> None:
        total = len(freqs_mhz)
        for index, freq_mhz in enumerate(freqs_mhz, start=1):
            if self._stop_requested.is_set():
                break
            with self._lock:
                lane = self._lanes[lane_key]
                lane["status"] = "scanning"
                lane["current_freq_mhz"] = float(freq_mhz)
                lane["completed"] = int(index - 1)
            try:
                result = self._scan_frequency(
                    lane_key=lane_key,
                    rx_chan=rx_chan,
                    freq_mhz=float(freq_mhz),
                    probe_index=index,
                    probe_total=total,
                    host=host,
                    port=port,
                    password=password,
                    threshold_db=threshold_db,
                )
            except Exception as exc:
                result = {
                    "lane": lane_key,
                    "rx_chan": int(rx_chan),
                    "freq_mhz": float(freq_mhz),
                    "status": "error",
                    "score": 0,
                    "summary": f"Probe failed: {exc}",
                    "signal_count": 0,
                    "event_count": 0,
                    "max_rel_db": None,
                    "best_s_est": None,
                    "voice_score": None,
                    "occupied_bw_hz": None,
                    "probe_index": int(index),
                    "probe_total": int(total),
                }
                with self._lock:
                    self._last_error = str(result["summary"])
            with self._lock:
                self._results[lane_key].append(dict(result))
                lane = self._lanes[lane_key]
                lane["completed"] = int(index)
                lane["last_score"] = result.get("score")
                lane["last_summary"] = result.get("summary")
                lane["current_freq_mhz"] = None
                lane["status"] = "stopped" if self._stop_requested.is_set() else "ready"
        with self._lock:
            lane = self._lanes[lane_key]
            if self._stop_requested.is_set():
                lane["status"] = "stopped"
            elif lane_key == "cw" and self._cw_followup_enabled():
                lane["status"] = "followup"
                lane["last_summary"] = "Preparing CW follow-up decode queue"
            elif lane["status"] != "error":
                lane["status"] = "complete"

    def _run_cw_followup(self, *, host: str, port: int, password: str | None) -> None:
        followup_lane_key = "smart" if self._lane_enabled("smart", self.scan_mode) else "cw"
        selected_results = self._select_cw_followup_results()
        if not selected_results:
            with self._lock:
                self._cw_followup["status"] = "skipped"
                self._cw_followup["summary"] = "No CW hits found; follow-up skipped"
                self._lanes[followup_lane_key]["status"] = "complete"
                self._lanes[followup_lane_key]["current_freq_mhz"] = None
                self._lanes[followup_lane_key]["last_summary"] = "No CW hits found for follow-up"
            return

        if self._stop_requested.is_set():
            with self._lock:
                self._cw_followup["status"] = "stopped"
                self._cw_followup["summary"] = "Stop requested before CW follow-up started"
            return

        rx_chan = int(self.RESERVED_RECEIVERS[0])
        session_id = self._session_id or time.strftime("receiver_scan_%Y%m%d_%H%M%S")
        followup_root = self._output_root / session_id / "cw_followup"
        followup_root.mkdir(parents=True, exist_ok=True)
        total = len(selected_results)
        completed_items: list[dict[str, Any]] = []
        validated_count = 0

        with self._lock:
            self._cw_followup = {
                **self._initial_cw_followup(),
                "status": "queued",
                "rx_chan": rx_chan,
                "duration_s": int(self.CW_FOLLOWUP_SECONDS),
                "total": total,
                "summary": f"Preparing {total} CW follow-up decode{'s' if total != 1 else ''}",
            }
            self._lanes[followup_lane_key]["status"] = "followup"
            self._lanes[followup_lane_key]["current_freq_mhz"] = None
            self._lanes[followup_lane_key]["last_summary"] = self._cw_followup["summary"]

        for followup_index, selected in enumerate(selected_results, start=1):
            if self._stop_requested.is_set():
                break

            freq_mhz = float(selected["freq_mhz"])
            followup_dir = followup_root / f"{followup_index:02d}_{str(f'{freq_mhz:.3f}').replace('.', '_')}"
            followup_dir.mkdir(parents=True, exist_ok=True)
            initial_summary = (
                f"Recording {int(self.CW_FOLLOWUP_SECONDS)}s CW follow-up "
                f"{followup_index}/{total} on {freq_mhz:.3f} MHz"
            )
            followup_state = {
                "status": "recording",
                "rx_chan": rx_chan,
                "duration_s": int(self.CW_FOLLOWUP_SECONDS),
                "selected_freq_mhz": freq_mhz,
                "signal_count": int(selected.get("signal_count") or 0),
                "score": selected.get("score"),
                "recording_path": str(followup_dir),
                "wav_path": None,
                "decoded_text": "",
                "validated_text": "",
                "message_valid": False,
                "validation_reason": "",
                "validation_summary": f"Recording CW follow-up {followup_index}/{total}",
                "confidence": 0.0,
                "tone_hz": None,
                "dot_ms": None,
                "wpm_est": None,
                "summary": initial_summary,
            }

            with self._lock:
                self._cw_followup = {
                    **self._cw_followup,
                    **followup_state,
                    "items": [dict(item) for item in completed_items],
                    "completed": len(completed_items),
                    "total": total,
                    "validated_count": validated_count,
                }
                self._lanes[followup_lane_key]["status"] = "followup"
                self._lanes[followup_lane_key]["current_freq_mhz"] = freq_mhz
                self._lanes[followup_lane_key]["last_summary"] = initial_summary

            wav_path: Path | None = None
            try:
                self._clear_reserved_slot(host=host, port=int(port), rx_chan=rx_chan)
                run_record(
                    RecordRequest(
                        host=host,
                        port=int(port),
                        password=password,
                        user="Receiver Scan CW Follow-up",
                        freq_hz=freq_mhz * 1e6,
                        rx_chan=rx_chan,
                        duration_s=int(self.CW_FOLLOWUP_SECONDS),
                        mode="cw",
                        out_dir=followup_dir,
                    )
                )
                wav_path = self._latest_wav_path(followup_dir)
                if wav_path is None:
                    raise FileNotFoundError("CW follow-up recording completed but no WAV file was found")
                if self._stop_requested.is_set():
                    followup_state["status"] = "stopped"
                    followup_state["wav_path"] = str(wav_path)
                    followup_state["summary"] = "Stop requested after CW recording finished"
                    followup_state["validation_summary"] = "CW follow-up stopped before decode"
                else:
                    with self._lock:
                        self._cw_followup = {
                            **self._cw_followup,
                            **followup_state,
                            "status": "decoding",
                            "wav_path": str(wav_path),
                            "summary": f"Decoding CW follow-up {followup_index}/{total} from {freq_mhz:.3f} MHz",
                            "items": [dict(item) for item in completed_items],
                            "completed": len(completed_items),
                            "total": total,
                            "validated_count": validated_count,
                        }
                        self._lanes[followup_lane_key]["last_summary"] = self._cw_followup["summary"]

                    decode = try_decode_cw_wav(wav_path)
                    decoded_text = str(decode.get("decoded_text") or "").strip()
                    validation = validate_cw_message(decoded_text, confidence=float(decode.get("confidence") or 0.0))
                    validated_text = str(validation.get("normalized_text") or "").strip()
                    message_valid = bool(validation.get("valid"))
                    validation_reason = str(validation.get("reason") or "").strip()
                    validation_summary = str(validation.get("summary") or "CW decode did not validate").strip()
                    summary = validation_summary if validated_text else str(decode.get("summary") or "CW follow-up complete")

                    followup_state.update(
                        {
                            "status": "complete",
                            "wav_path": str(wav_path),
                            "decoded_text": decoded_text,
                            "validated_text": validated_text,
                            "message_valid": message_valid,
                            "validation_reason": validation_reason,
                            "validation_summary": validation_summary,
                            "confidence": float(decode.get("confidence") or 0.0),
                            "tone_hz": decode.get("tone_hz"),
                            "dot_ms": decode.get("dot_ms"),
                            "wpm_est": decode.get("wpm_est"),
                            "summary": summary,
                        }
                    )
            except RecorderUnavailable as exc:
                summary = f"CW follow-up recording unavailable: {exc}"
                with self._lock:
                    self._last_error = summary
                followup_state.update(
                    {
                        "status": "error",
                        "wav_path": str(wav_path) if wav_path is not None else None,
                        "summary": summary,
                        "validation_summary": summary,
                    }
                )
            except Exception as exc:
                summary = f"CW follow-up failed: {type(exc).__name__}: {exc}"
                with self._lock:
                    self._last_error = summary
                followup_state.update(
                    {
                        "status": "error",
                        "wav_path": str(wav_path) if wav_path is not None else None,
                        "summary": summary,
                        "validation_summary": summary,
                    }
                )

            if bool(followup_state.get("message_valid")):
                validated_count += 1
            completed_items.append(dict(followup_state))

            with self._lock:
                self._cw_followup = {
                    **self._cw_followup,
                    **followup_state,
                    "items": [dict(item) for item in completed_items],
                    "completed": len(completed_items),
                    "total": total,
                    "validated_count": validated_count,
                }
                self._lanes[followup_lane_key]["current_freq_mhz"] = None
                self._lanes[followup_lane_key]["last_summary"] = str(
                    followup_state.get("validation_summary")
                    or followup_state.get("summary")
                    or self._lanes[followup_lane_key]["last_summary"]
                )

        overall_summary = (
            f"Completed {len(completed_items)} CW follow-up decode{'s' if len(completed_items) != 1 else ''}; "
            f"validated {validated_count}"
        )
        with self._lock:
            if self._stop_requested.is_set() and len(completed_items) < total:
                self._cw_followup["status"] = "stopped"
                self._cw_followup["summary"] = (
                    f"Stopped after {len(completed_items)} of {total} CW follow-up decode"
                    f"{'s' if total != 1 else ''}"
                )
            else:
                self._cw_followup["status"] = "complete"
                self._cw_followup["summary"] = overall_summary
            self._cw_followup["items"] = [dict(item) for item in completed_items]
            self._cw_followup["completed"] = len(completed_items)
            self._cw_followup["total"] = total
            self._cw_followup["validated_count"] = validated_count
            self._lanes[followup_lane_key]["status"] = "complete"
            self._lanes[followup_lane_key]["current_freq_mhz"] = None
            self._lanes[followup_lane_key]["last_summary"] = self._cw_followup["summary"]

    def _select_cw_followup_results(self) -> list[dict[str, Any]]:
        with self._lock:
            if self._lane_enabled("smart", self.scan_mode):
                results = [
                    {
                        **dict(item),
                        "signal_count": max(1, int(item.get("hit_count") or 1)),
                    }
                    for item in self._results.get("smart", [])
                    if str(item.get("signal_type") or "").strip().upper() == "CW"
                ]
            else:
                results = [dict(item) for item in self._results.get("cw", [])]
        ranked = [
            item
            for item in results
            if str(item.get("status") or "") not in {"error", "unavailable"}
        ]
        ranked = [item for item in ranked if int(item.get("signal_count") or 0) > 0]
        return sorted(
            ranked,
            key=lambda item: (
                int(item.get("signal_count") or 0),
                int(item.get("score") or 0),
                float(item.get("max_rel_db") or float("-inf")),
                -float(item.get("freq_mhz") or 0.0),
            ),
            reverse=True,
        )

    def _select_best_cw_result(self) -> dict[str, Any] | None:
        ranked = self._select_cw_followup_results()
        return ranked[0] if ranked else None

    @staticmethod
    def _latest_wav_path(root: Path) -> Path | None:
        wavs = [path for path in root.glob("*.wav") if path.is_file()]
        if not wavs:
            return None
        return max(wavs, key=lambda path: path.stat().st_mtime)

    def _scan_frequency(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freq_mhz: float,
        probe_index: int,
        probe_total: int,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> dict[str, Any]:
        session_id = self._session_id or time.strftime("receiver_scan_%Y%m%d_%H%M%S")
        lane_dir = self._output_root / session_id / lane_key
        lane_dir.mkdir(parents=True, exist_ok=True)
        freq_tag = f"{freq_mhz:.3f}".replace(".", "_")
        report_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}.json"
        hits_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}_hits.jsonl"
        events_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}_events.jsonl"

        is_phone = lane_key == "phone"
        self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(rx_chan))
        rc = run_scan(
            host=host,
            port=int(port),
            password=password,
            user=f"Receiver Scan {lane_key.upper()}",
            rx_chan=int(rx_chan),
            band=self.BAND,
            center_freq_hz=float(freq_mhz) * 1e6,
            span_hz=float(self.PHONE_SPAN_HZ) if is_phone else 2400.0,
            threshold_db=float(threshold_db),
            min_width_bins=2,
            min_width_hz=float(self.PHONE_MIN_WIDTH_HZ) if is_phone else 20.0,
            ssb_detect=bool(is_phone),
            ssb_only=bool(is_phone),
            required_hits=1 if is_phone else 2,
            tolerance_bins=2.5,
            expiry_frames=6,
            max_frames=int(self.PHONE_MAX_FRAMES) if is_phone else 10,
            jsonl_path=hits_path,
            jsonl_events_path=events_path,
            json_report_path=report_path,
            min_s=1.0,
            status_hold_s=float(self.LISTEN_SECONDS),
            max_runtime_s=4.0,
            rx_wait_timeout_s=20.0,
            rx_wait_interval_s=1.0,
            rx_wait_max_retries=0,
            phone_only=bool(is_phone),
            status_modulation="iq",
            status_pre_tune=False,
            status_parallel_snd=True,
            ssb_occ_thresh_db=5.0,
            ssb_voice_min_score=float(self.PHONE_VOICE_MIN_SCORE) if is_phone else 0.45,
            ssb_early_stop_frames=int(self.PHONE_EARLY_STOP_FRAMES) if is_phone else 0,
            ssb_warmup_frames=2 if is_phone else 1,
            ssb_adaptive_threshold=bool(is_phone),
            ssb_adaptive_min_db=8.0,
            ssb_adaptive_max_db=22.0,
            ssb_adaptive_spread_gain=0.18,
            ssb_adaptive_spread_offset_db=0.0,
            ssb_adaptive_spread_target_db=55.0,
            show=False,
        )
        return self._summarize_probe(
            lane_key=lane_key,
            rx_chan=rx_chan,
            freq_mhz=freq_mhz,
            probe_index=probe_index,
            probe_total=probe_total,
            rc=rc,
            report_path=report_path,
            events_path=events_path,
        )

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        entries: list[dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                entries.append(item)
        return entries

    @staticmethod
    def _clamp_score(value: float) -> int:
        return int(max(0.0, min(100.0, round(float(value)))))

    def _summarize_probe(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freq_mhz: float,
        probe_index: int,
        probe_total: int,
        rc: int,
        report_path: Path,
        events_path: Path,
    ) -> dict[str, Any]:
        report = self._read_json(report_path)
        events = self._read_jsonl(events_path)
        peak = report.get("peak") if isinstance(report.get("peak"), dict) else {}
        raw_event_count = len(events)
        if rc == 3:
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": "unavailable",
                "score": 0,
                "summary": f"RX{int(rx_chan)} unavailable for this probe",
                "signal_count": 0,
                "event_count": 0 if lane_key == "phone" else raw_event_count,
                "raw_event_count": raw_event_count,
                "max_rel_db": None,
                "best_s_est": None,
                "voice_score": None,
                "occupied_bw_hz": None,
                "mode_hint": None,
                "activity_kind": "unavailable",
                "bandwidth_bucket": None,
                "phone_like": False,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }
        if rc != 0:
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": "error",
                "score": 0,
                "summary": f"Probe failed with rc={int(rc)}",
                "signal_count": 0,
                "event_count": 0 if lane_key == "phone" else raw_event_count,
                "raw_event_count": raw_event_count,
                "max_rel_db": None,
                "best_s_est": None,
                "voice_score": None,
                "occupied_bw_hz": None,
                "mode_hint": None,
                "activity_kind": "error",
                "bandwidth_bucket": None,
                "phone_like": False,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }

        rel_values = [float(item.get("rel_db")) for item in events if item.get("rel_db") is not None]
        s_values = [float(item.get("s_est")) for item in events if item.get("s_est") is not None]
        max_rel_db = max(rel_values) if rel_values else (
            float(peak.get("rel_db")) if isinstance(peak, dict) and peak.get("rel_db") is not None else None
        )
        best_s_est = max(s_values) if s_values else (
            float(peak.get("s_est")) if isinstance(peak, dict) and peak.get("s_est") is not None else None
        )

        if lane_key == "cw":
            distinct_hits: dict[float, float] = {}
            for item in events:
                try:
                    key = round(float(item.get("freq_mhz", freq_mhz)), 4)
                    rel_db = float(item.get("rel_db", 0.0))
                except Exception:
                    continue
                distinct_hits[key] = max(rel_db, distinct_hits.get(key, rel_db))
            signal_count = len(distinct_hits)
            score = self._clamp_score((float(max_rel_db or 0.0) * 4.0) + (signal_count * 18.0))
            status = "activity" if score >= 60 else "watch" if score >= 30 else "quiet"
            summary = (
                f"{signal_count} persistent narrow signal{'s' if signal_count != 1 else ''}"
                if signal_count
                else "No persistent CW-like tones"
            )
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": status,
                "score": score,
                "summary": summary,
                "signal_count": signal_count,
                "event_count": raw_event_count,
                "raw_event_count": raw_event_count,
                "max_rel_db": max_rel_db,
                "best_s_est": best_s_est,
                "voice_score": None,
                "occupied_bw_hz": None,
                "mode_hint": "CW" if signal_count else None,
                "activity_kind": "cw",
                "bandwidth_bucket": "80-220 Hz" if signal_count else None,
                "phone_like": False,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }

        voice_values = [float(item.get("voice_score")) for item in events if item.get("voice_score") is not None]
        occ_values = [float(item.get("occ_bw_hz")) for item in events if item.get("occ_bw_hz") is not None]
        voice_score = max(voice_values) if voice_values else (
            float(peak.get("voice_score")) if isinstance(peak, dict) and peak.get("voice_score") is not None else None
        )
        occupied_bw_hz = max(occ_values) if occ_values else (
            float(peak.get("occ_bw_hz")) if isinstance(peak, dict) and peak.get("occ_bw_hz") is not None else None
        )
        activity_hint = classify_activity_width(
            occupied_bw_hz,
            type_guess=peak.get("type_guess") if isinstance(peak, dict) else None,
            bandplan=peak.get("bandplan") if isinstance(peak, dict) else "Phone",
        )
        representative_voice_score = voice_score
        representative_bw_hz = occupied_bw_hz
        confirmed_phone_events: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for event in events:
            event_bw_hz = event.get("occ_bw_hz")
            if event_bw_hz is None:
                event_bw_hz = event.get("width_hz")
            event_hint = classify_activity_width(
                event_bw_hz,
                type_guess=event.get("type_guess"),
                bandplan=event.get("bandplan") or "Phone",
            )
            width_value = event_hint.get("bandwidth_hz")
            if width_value is None:
                continue
            if not event_hint.get("phone_like"):
                continue
            if not (float(self.PHONE_CLUSTER_MIN_HZ) <= float(width_value) <= float(self.PHONE_CLUSTER_MAX_HZ)):
                continue
            confirmed_phone_events.append((event, event_hint))

        if confirmed_phone_events:
            best_event, best_event_hint = max(
                confirmed_phone_events,
                key=lambda item: (
                    float(item[0].get("voice_score") or 0.0),
                    float(item[0].get("rel_db") or float("-inf")),
                    float(item[1].get("bandwidth_hz") or 0.0),
                ),
            )
            activity_hint = best_event_hint
            if best_event.get("voice_score") is not None:
                representative_voice_score = float(best_event.get("voice_score") or 0.0)
            if best_event_hint.get("bandwidth_hz") is not None:
                representative_bw_hz = float(best_event_hint.get("bandwidth_hz") or 0.0)

        score = self._clamp_score(
            (float(max_rel_db or 0.0) * 4.0)
            + (float(representative_voice_score or 0.0) * 45.0)
            + (min(float(representative_bw_hz or 0.0), 3200.0) / 90.0)
        )
        confirmed_event_count = len(confirmed_phone_events)
        if confirmed_event_count > 0 and score >= int(self.PHONE_ACTIVITY_MIN_SCORE):
            status = "activity"
            summary = (
                f"Confirmed {activity_hint.get('mode_hint') or 'phone'} IQ cluster "
                f"voice={float(representative_voice_score or 0.0):.2f}, "
                f"bw={float(representative_bw_hz or 0.0):.0f} Hz"
            )
        elif score >= 30:
            status = "watch"
            if raw_event_count == 0 and activity_hint.get("phone_like"):
                summary = (
                    f"Unconfirmed {activity_hint.get('mode_hint') or 'phone'} IQ cluster "
                    f"voice={float(representative_voice_score or 0.0):.2f}, "
                    f"bw={float(representative_bw_hz or 0.0):.0f} Hz"
                )
            else:
                summary = (
                    f"{activity_hint.get('mode_hint') or 'Band activity'} candidate "
                    f"voice={float(representative_voice_score or 0.0):.2f}, "
                    f"bw={float(representative_bw_hz or 0.0):.0f} Hz"
                )
        else:
            status = "quiet"
            summary = "No confirmed phone-like IQ cluster"
        return {
            "lane": lane_key,
            "rx_chan": int(rx_chan),
            "freq_mhz": float(freq_mhz),
            "status": status,
            "score": score,
            "summary": summary,
            "signal_count": 1 if confirmed_event_count > 0 else 0,
            "event_count": confirmed_event_count,
            "raw_event_count": raw_event_count,
            "max_rel_db": max_rel_db,
            "best_s_est": best_s_est,
            "voice_score": representative_voice_score,
            "occupied_bw_hz": representative_bw_hz,
            "mode_hint": activity_hint.get("mode_hint"),
            "activity_kind": activity_hint.get("activity_kind"),
            "bandwidth_bucket": activity_hint.get("bandwidth_bucket"),
            "phone_like": bool(confirmed_event_count > 0),
            "probe_index": int(probe_index),
            "probe_total": int(probe_total),
        }

    def _write_session_summary(self, session_id: str | None) -> None:
        if not session_id:
            return
        session_dir = self._output_root / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        summary_path = session_dir / "receiver_scan_session.json"
        summary_path.write_text(json.dumps(self.status(), sort_keys=True) + "\n", encoding="utf-8")