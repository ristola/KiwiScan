from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from .cache import TTLCache
from .bandplan import bandplan_label, bandplan_ranges_for_label, combine_type_hints
from .detect import PersistenceTracker, detect_peaks_with_noise_floor
from .display import sparkline, span_bar, top_peaks
from .kiwi_waterfall import (
    allocate_ws_timestamp,
    KiwiCampRejected,
    KiwiClientUnavailable,
    WaterfallFrame,
    set_receiver_frequency,
    subscribe_waterfall,
)
from .record import RecordRequest, RecorderUnavailable, run_record


@dataclass(frozen=True)
class Detection:
    t_unix: float
    frame_index: int
    noise_floor: float
    threshold_db: float
    bin_center: float
    width_bins: int
    width_hz: float
    type_guess: str
    bandplan: str | None
    peak_power: float
    freq_mhz: float
    candidate_type: str = "UNKNOWN"
    ssb_detect: bool = False
    # Optional SSB/voice heuristics (present when ssb_detect/ssb_only is enabled)
    occ_bw_hz: float | None = None
    occ_frac: float | None = None
    voice_score: float | None = None
    narrow_peak_count: int | None = None
    narrow_peak_span_hz: float | None = None
    keying_score: float | None = None
    steady_tone_score: float | None = None
    freq_stability_hz: float | None = None
    envelope_variance: float | None = None
    speech_envelope_score: float | None = None
    sweep_score: float | None = None
    centroid_drift_hz: float | None = None
    observed_frames: int | None = None
    active_fraction: float | None = None
    cadence_score: float | None = None
    keying_edge_count: int | None = None
    has_on_off_keying: bool | None = None
    amplitude_span_db: float | None = None


def guess_signal_type(*, width_hz: float) -> str:
    """Very rough classifier based on occupied bandwidth.

    This is intentionally conservative: width alone cannot distinguish CW from
    very narrow digital (e.g. FT8/PSK). Treat this as a hint.
    """

    w = float(abs(width_hz))
    if w <= 150.0:
        return "very_narrow"  # CW / FT8-ish / PSK-ish
    if w <= 500.0:
        return "narrow"  # RTTY-ish / other narrow digital
    if w <= 1500.0:
        return "medium"  # many utility/digital signals
    return "wide"  # voice-ish (SSB/AM) or wide modes


def _finite_metric(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def classify_candidate_type(
    *,
    width_hz: object | None,
    type_guess: object | None = None,
    bandplan_label: object | None = None,
    voice_score: object | None = None,
    occ_frac: object | None = None,
    speech_score: object | None = None,
    sweep_score: object | None = None,
    keying_score: object | None = None,
    cadence_score: object | None = None,
    narrow_peak_count: object | None = None,
    envelope_variance: object | None = None,
    has_on_off_keying: object | None = None,
) -> str:
    """Classify a detection into a generic candidate layer before final mode mapping."""

    width = _finite_metric(width_hz)
    voice = _finite_metric(voice_score)
    occupancy = _finite_metric(occ_frac)
    speech = _finite_metric(speech_score)
    sweep = _finite_metric(sweep_score)
    keying = _finite_metric(keying_score)
    cadence = _finite_metric(cadence_score)
    narrow_peaks = max(0, int(round(_finite_metric(narrow_peak_count) or 0.0)))
    envelope_var = _finite_metric(envelope_variance)
    on_off_keying = bool(has_on_off_keying) if has_on_off_keying is not None else False
    type_hint = str(type_guess or "").strip().lower()
    bandplan_hint = str(bandplan_label or "").strip().lower()
    voice_envelope_detected = envelope_var is not None and envelope_var >= 0.12
    strong_voice = (
        width is not None
        and width >= 1_500.0
        and (
            (speech is not None and speech >= 0.42)
            or (
                voice is not None
                and voice >= 0.40
                and voice_envelope_detected
            )
            or (
                speech is not None
                and speech >= 0.28
                and voice_envelope_detected
                and (voice is None or voice >= 0.18)
            )
            or (
                occupancy is not None
                and occupancy >= 0.55
                and voice_envelope_detected
            )
        )
    )
    digital_cluster = (
        width is not None
        and narrow_peaks > 4
        and width <= 1_800.0
        and (speech is None or speech < 0.34)
        and (voice is None or voice < 0.40)
        and not voice_envelope_detected
    )
    wide_digital_cluster = (
        width is not None
        and narrow_peaks > 6
        and width <= 2_400.0
        and bandplan_hint in {"cw", "rtty", "ft8", "ft4", "wspr", "all modes"}
        and (speech is None or speech < 0.30)
        and (voice is None or voice < 0.35)
        and not voice_envelope_detected
    )

    if any(token in type_hint for token in ("sstv", "image", "fax")) or (
        sweep is not None and sweep >= 0.55 and (speech is None or speech < 0.45)
    ):
        return "WIDEBAND_IMAGE"

    if strong_voice:
        return "WIDEBAND_VOICE"

    if any(token in type_hint for token in ("phone", "ssb")):
        if strong_voice or (
            width is not None
            and width >= 1_500.0
            and voice is not None
            and voice >= 0.28
            and voice_envelope_detected
            and (
                (speech is not None and speech >= 0.20)
                or (occupancy is not None and occupancy >= 0.35)
            )
        ):
            return "WIDEBAND_VOICE"

    if digital_cluster or wide_digital_cluster:
        return "DIGITAL_CLUSTER"

    if width is None or width <= 0.0:
        return "UNKNOWN"

    width_guess = guess_signal_type(width_hz=float(width))
    keying_level = float(keying or 0.0)
    cadence_level = float(cadence or 0.0)

    if width_guess == "very_narrow":
        if on_off_keying or keying_level >= 0.28:
            return "NARROW_SINGLE"
        return "NARROW_MULTI"

    if width_guess == "narrow":
        if (on_off_keying or (keying_level >= 0.32 and cadence_level >= 0.18)) and (speech is None or speech < 0.16) and (sweep is None or sweep < 0.16):
            return "NARROW_SINGLE"
        return "NARROW_MULTI"

    if width_guess == "medium":
        if strong_voice and width >= 1_200.0:
            return "WIDEBAND_VOICE"
        return "MEDIUM_DIGITAL"

    if strong_voice:
        return "WIDEBAND_VOICE"
    if speech is not None and speech >= 0.32 and width >= 1_500.0 and voice_envelope_detected:
        return "WIDEBAND_VOICE"
    if occupancy is not None and occupancy >= 0.45 and width >= 1_800.0 and voice_envelope_detected:
        return "WIDEBAND_VOICE"
    if 1_800.0 <= width <= 3_600.0:
        if sweep is not None and sweep >= 0.55 and (speech is None or speech < 0.45):
            return "WIDEBAND_IMAGE"
        if strong_voice:
            return "WIDEBAND_VOICE"
        return "WIDEBAND_IMAGE"
    if bandplan_hint == "phone" and width >= 2_000.0 and strong_voice:
        return "WIDEBAND_VOICE"
    return "UNKNOWN"


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = _mean(values)
    variance = sum((float(value) - avg) ** 2 for value in values) / float(len(values))
    return math.sqrt(max(0.0, variance))


def _weighted_centroid_hz(items: list[dict[str, float]]) -> float:
    if not items:
        return 0.0
    weights = [max(0.1, float(item.get("rel_db") or 0.0) + 0.1) for item in items]
    total = sum(weights)
    if total <= 0.0:
        return _mean([float(item.get("freq_hz") or 0.0) for item in items])
    return sum(float(item.get("freq_hz") or 0.0) * weight for item, weight in zip(items, weights)) / total


def _describe_peaks(*, frame: WaterfallFrame, noise_floor: float, peaks: list) -> list[dict[str, float]]:
    described: list[dict[str, float]] = []
    if not peaks:
        return described
    for peak in peaks:
        width_hz = float(peak_width_hz(frame=frame, bin_low=peak.bin_low, bin_high=peak.bin_high))
        freq_hz = float(
            bin_to_hz(
                center_freq_hz=frame.center_freq_hz,
                span_hz=frame.span_hz,
                n_bins=len(frame.power_bins),
                bin_center=float(peak.bin_center),
            )
        )
        described.append(
            {
                "bin_center": float(peak.bin_center),
                "freq_hz": freq_hz,
                "width_hz": width_hz,
                "rel_db": float(peak.peak_power) - float(noise_floor),
            }
        )
    return described


def _peak_cluster_context(
    peak_details: list[dict[str, float]],
    *,
    center_hz: float,
    cluster_window_hz: float = 3_200.0,
    narrow_width_hz: float = 120.0,
) -> dict[str, float]:
    if not peak_details:
        return {
            "narrow_peak_count": 0.0,
            "narrow_peak_span_hz": 0.0,
            "centroid_hz": float(center_hz),
        }
    half = float(cluster_window_hz) / 2.0
    local = [
        item
        for item in peak_details
        if abs(float(item.get("freq_hz") or 0.0) - float(center_hz)) <= half
    ]
    if not local:
        return {
            "narrow_peak_count": 0.0,
            "narrow_peak_span_hz": 0.0,
            "centroid_hz": float(center_hz),
        }
    narrow = [item for item in local if float(item.get("width_hz") or 0.0) <= float(narrow_width_hz)]
    narrow_freqs = [float(item.get("freq_hz") or 0.0) for item in narrow]
    return {
        "narrow_peak_count": float(len(narrow)),
        "narrow_peak_span_hz": float(max(narrow_freqs) - min(narrow_freqs)) if len(narrow_freqs) >= 2 else 0.0,
        "centroid_hz": float(_weighted_centroid_hz(local)),
    }


@dataclass
class _TemporalPeakTrack:
    bin_center: float
    first_seen_frame: int
    last_frame: int
    last_seen_frame: int
    last_active: bool = False
    match_count: int = 0
    observed_frames: int = 0
    active_frames: int = 0
    transition_count: int = 0
    rel_db_samples: list[float] = field(default_factory=list)
    center_hz_samples: list[float] = field(default_factory=list)
    width_hz_samples: list[float] = field(default_factory=list)
    narrow_peak_counts: list[float] = field(default_factory=list)
    narrow_peak_spans_hz: list[float] = field(default_factory=list)
    centroid_hz_samples: list[float] = field(default_factory=list)


class _TemporalFeatureTracker:
    def __init__(self, *, tolerance_bins: float = 2.5, expiry_frames: int = 8) -> None:
        self.tolerance_bins = float(tolerance_bins)
        self.expiry_frames = int(expiry_frames)
        self._tracks: list[_TemporalPeakTrack] = []

    def _match_track(self, *, bin_center: float, frame_index: int) -> _TemporalPeakTrack:
        candidates = [
            track
            for track in self._tracks
            if abs(float(track.bin_center) - float(bin_center)) <= self.tolerance_bins
            and (int(frame_index) - int(track.last_seen_frame)) <= self.expiry_frames
        ]
        if candidates:
            return min(candidates, key=lambda track: abs(float(track.bin_center) - float(bin_center)))
        track = _TemporalPeakTrack(
            bin_center=float(bin_center),
            first_seen_frame=int(frame_index),
            last_frame=int(frame_index) - 1,
            last_seen_frame=int(frame_index) - 1,
        )
        self._tracks.append(track)
        return track

    def _record_inactive(self, track: _TemporalPeakTrack, *, frame_index: int) -> None:
        if int(track.last_frame) >= int(frame_index):
            return
        if bool(track.last_active):
            track.transition_count += 1
        track.observed_frames += 1
        track.rel_db_samples.append(0.0)
        track.last_active = False
        track.last_frame = int(frame_index)

    def _record_active(
        self,
        track: _TemporalPeakTrack,
        *,
        frame_index: int,
        peak_detail: dict[str, float],
        cluster_context: dict[str, float],
    ) -> None:
        if int(track.last_frame) < int(frame_index) - 1:
            for missed_frame in range(int(track.last_frame) + 1, int(frame_index)):
                self._record_inactive(track, frame_index=int(missed_frame))
        if track.observed_frames > 0 and not bool(track.last_active):
            track.transition_count += 1
        track.observed_frames += 1
        track.active_frames += 1
        track.match_count += 1
        track.last_active = True
        track.last_frame = int(frame_index)
        track.last_seen_frame = int(frame_index)
        track.bin_center = (float(track.bin_center) + float(peak_detail.get("bin_center") or 0.0)) / 2.0
        track.rel_db_samples.append(float(peak_detail.get("rel_db") or 0.0))
        track.center_hz_samples.append(float(peak_detail.get("freq_hz") or 0.0))
        track.width_hz_samples.append(float(peak_detail.get("width_hz") or 0.0))
        track.narrow_peak_counts.append(float(cluster_context.get("narrow_peak_count") or 0.0))
        track.narrow_peak_spans_hz.append(float(cluster_context.get("narrow_peak_span_hz") or 0.0))
        track.centroid_hz_samples.append(float(cluster_context.get("centroid_hz") or peak_detail.get("freq_hz") or 0.0))

    def update(self, *, frame_index: int, peak_details: list[dict[str, float]]) -> None:
        matched_ids: set[int] = set()
        for peak_detail in peak_details:
            track = self._match_track(bin_center=float(peak_detail.get("bin_center") or 0.0), frame_index=int(frame_index))
            cluster_context = _peak_cluster_context(peak_details, center_hz=float(peak_detail.get("freq_hz") or 0.0))
            self._record_active(
                track,
                frame_index=int(frame_index),
                peak_detail=peak_detail,
                cluster_context=cluster_context,
            )
            matched_ids.add(id(track))

        for track in list(self._tracks):
            if id(track) in matched_ids:
                continue
            if int(frame_index) > int(track.last_frame):
                self._record_inactive(track, frame_index=int(frame_index))

        self._tracks = [
            track
            for track in self._tracks
            if (int(frame_index) - int(track.last_seen_frame)) <= self.expiry_frames
        ]

    def summarize_for_bin(self, *, bin_center: float) -> dict[str, float | int | bool]:
        candidates = [
            track for track in self._tracks if abs(float(track.bin_center) - float(bin_center)) <= self.tolerance_bins
        ]
        if not candidates:
            return {}
        track = min(candidates, key=lambda item: abs(float(item.bin_center) - float(bin_center)))
        if track.observed_frames <= 0:
            return {}

        amplitude_over_time = [max(0.0, float(value)) for value in track.rel_db_samples]
        state_threshold_db = 6.0
        signal_states = [float(value) > state_threshold_db for value in amplitude_over_time]
        keying_edge_count = sum(
            1
            for index in range(1, len(signal_states))
            if bool(signal_states[index]) != bool(signal_states[index - 1])
        )
        has_on_off_keying = keying_edge_count > 3
        active_fraction = (
            float(sum(1 for state in signal_states if state)) / float(len(signal_states))
            if signal_states
            else 0.0
        )
        rel_active = [
            float(value)
            for value, state in zip(amplitude_over_time, signal_states)
            if state and float(value) > 0.0
        ]
        amplitude_span_db = (
            max(0.0, _percentile(rel_active, 90.0) - _percentile(rel_active, 10.0))
            if len(rel_active) >= 2
            else 0.0
        )
        envelope_variance = _stddev(rel_active) / max(1.0, _mean(rel_active)) if rel_active else 0.0
        freq_stability_hz = _stddev([float(value) for value in track.center_hz_samples])
        narrow_peak_count = max((int(round(value)) for value in track.narrow_peak_counts), default=0)
        narrow_peak_span_hz = max((float(value) for value in track.narrow_peak_spans_hz), default=0.0)
        centroid_series = [float(value) for value in track.centroid_hz_samples]
        centroid_drift_hz = (max(centroid_series) - min(centroid_series)) if len(centroid_series) >= 2 else 0.0
        centroid_diffs = [
            float(centroid_series[index + 1]) - float(centroid_series[index])
            for index in range(len(centroid_series) - 1)
        ]
        total_motion = sum(abs(value) for value in centroid_diffs)
        monotonicity = abs(sum(centroid_diffs)) / total_motion if total_motion > 0.0 else 0.0
        frame_support = _clamp01(float(track.observed_frames) / 5.0)
        transition_score = _clamp01(float(keying_edge_count) / 6.0)
        balance_score = _clamp01(1.0 - (abs(active_fraction - 0.5) / 0.5))
        stability_score = _clamp01(1.0 - (freq_stability_hz / 40.0))
        envelope_score = _clamp01(envelope_variance / 0.35)
        amplitude_variation_score = _clamp01(float(amplitude_span_db) / 10.0)
        cadence_score = _clamp01((0.65 * transition_score) + (0.35 * balance_score)) * frame_support
        sweep_span_score = _clamp01(float(centroid_drift_hz) / 1_500.0)
        sweep_score = _clamp01((0.55 * sweep_span_score) + (0.45 * monotonicity)) * frame_support
        speech_envelope_score = _clamp01(
            (0.45 * envelope_score)
            + (0.25 * _clamp01(float(centroid_drift_hz) / 900.0))
            + (0.30 * (1.0 - monotonicity))
        ) * frame_support
        keying_score = _clamp01(
            (0.50 * transition_score)
            + (0.20 * balance_score)
            + (0.20 * stability_score)
            + (0.10 * amplitude_variation_score)
        ) * frame_support
        steady_tone_score = _clamp01(
            (0.50 * (1.0 - transition_score))
            + (0.30 * _clamp01(active_fraction / 0.95))
            + (0.20 * stability_score)
        ) * frame_support

        return {
            "observed_frames": float(track.observed_frames),
            "active_fraction": float(active_fraction),
            "narrow_peak_count": float(narrow_peak_count),
            "narrow_peak_span_hz": float(narrow_peak_span_hz),
            "freq_stability_hz": float(freq_stability_hz),
            "envelope_variance": float(envelope_variance),
            "speech_envelope_score": float(speech_envelope_score),
            "sweep_score": float(sweep_score),
            "keying_score": float(keying_score),
            "steady_tone_score": float(steady_tone_score),
            "keying_edge_count": int(keying_edge_count),
            "has_on_off_keying": bool(has_on_off_keying),
            "amplitude_span_db": float(amplitude_span_db),
            "centroid_drift_hz": float(centroid_drift_hz),
            "cadence_score": float(cadence_score),
        }


def peak_width_hz(*, frame: WaterfallFrame, bin_low: int, bin_high: int) -> float:
    if not frame.power_bins:
        return 0.0
    n = len(frame.power_bins)
    lo = max(0, min(int(bin_low), n - 1))
    hi = max(0, min(int(bin_high), n - 1))
    f_lo = bin_to_hz(
        center_freq_hz=frame.center_freq_hz,
        span_hz=frame.span_hz,
        n_bins=n,
        bin_center=float(lo),
    )
    f_hi = bin_to_hz(
        center_freq_hz=frame.center_freq_hz,
        span_hz=frame.span_hz,
        n_bins=n,
        bin_center=float(hi),
    )
    return float(abs(f_hi - f_lo))


def _hz_to_bin(*, center_freq_hz: float, span_hz: float, n_bins: int, freq_hz: float) -> float:
    if n_bins <= 1 or span_hz <= 0:
        return 0.0
    offset = float(freq_hz) - float(center_freq_hz)
    frac = (offset / float(span_hz)) + 0.5
    return float(frac * float(n_bins - 1))


def _bandpower_rel_db(
    *,
    frame: WaterfallFrame,
    noise_floor: float,
    center_hz: float,
    width_hz: float,
) -> tuple[float, int, float]:
    """Estimate wideband energy around center_hz.

    Returns (rel_db, width_bins, effective_width_hz).
    """

    if not frame.power_bins:
        return 0.0, 0, 0.0
    n = len(frame.power_bins)
    half = float(width_hz) / 2.0
    b0 = _hz_to_bin(center_freq_hz=frame.center_freq_hz, span_hz=frame.span_hz, n_bins=n, freq_hz=float(center_hz) - half)
    b1 = _hz_to_bin(center_freq_hz=frame.center_freq_hz, span_hz=frame.span_hz, n_bins=n, freq_hz=float(center_hz) + half)
    lo = max(0, min(int(round(min(b0, b1))), n - 1))
    hi = max(0, min(int(round(max(b0, b1))), n - 1))
    if hi < lo:
        lo, hi = hi, lo
    bins = frame.power_bins[lo : hi + 1]
    if not bins:
        return 0.0, 0, 0.0
    avg = float(sum(float(x) for x in bins)) / float(len(bins))
    rel = avg - float(noise_floor)
    eff_w = peak_width_hz(frame=frame, bin_low=lo, bin_high=hi)
    return float(rel), int(hi - lo + 1), float(eff_w)


def _ssb_voice_metrics(
    *,
    frame: WaterfallFrame,
    noise_floor: float,
    center_hz: float,
    window_hz: float = 2400.0,
    occ_thresh_db: float = 6.0,
) -> tuple[float, int, float, float]:
    """Compute crude SSB/voice-ish metrics inside a fixed window.

    Returns (occupied_bw_hz, occupied_bins, occupied_fraction, voice_score).

    - occupied_bw_hz: bandwidth between first/last bins above noise+occ_thresh_db
    - occupied_fraction: fraction of window bins above noise+occ_thresh_db
    - voice_score: 0..1 heuristic; higher tends to mean energy is spread, not spiky
    """

    if not frame.power_bins:
        return 0.0, 0, 0.0, 0.0
    n = len(frame.power_bins)
    if n <= 1 or float(frame.span_hz) <= 0:
        return 0.0, 0, 0.0, 0.0

    half = float(window_hz) / 2.0
    b0 = _hz_to_bin(center_freq_hz=frame.center_freq_hz, span_hz=frame.span_hz, n_bins=n, freq_hz=float(center_hz) - half)
    b1 = _hz_to_bin(center_freq_hz=frame.center_freq_hz, span_hz=frame.span_hz, n_bins=n, freq_hz=float(center_hz) + half)
    lo = max(0, min(int(round(min(b0, b1))), n - 1))
    hi = max(0, min(int(round(max(b0, b1))), n - 1))
    if hi < lo:
        lo, hi = hi, lo

    window = [float(x) for x in frame.power_bins[lo : hi + 1]]
    if not window:
        return 0.0, 0, 0.0, 0.0

    mx = max(window)
    avg = float(sum(window)) / float(len(window))
    rel_peak = float(mx) - float(noise_floor)
    peak_minus_avg = float(mx) - float(avg)
    denom = max(1e-6, float(rel_peak))
    spread = 1.0 - (float(peak_minus_avg) / denom)
    spread = max(0.0, min(1.0, float(spread)))

    thr = float(noise_floor) + float(occ_thresh_db)
    above_idx = [i for i, v in enumerate(window) if float(v) >= thr]
    if not above_idx:
        occ_bw_hz = 0.0
        occ_bins = 0
        occ_frac = 0.0
    else:
        occ_lo = lo + int(min(above_idx))
        occ_hi = lo + int(max(above_idx))
        occ_bins = int(occ_hi - occ_lo + 1)
        occ_bw_hz = float(peak_width_hz(frame=frame, bin_low=int(occ_lo), bin_high=int(occ_hi)))
        occ_frac = float(len(above_idx)) / float(len(window))

    voice_score = (0.6 * float(spread)) + (0.4 * float(occ_frac))
    voice_score = max(0.0, min(1.0, float(voice_score)))
    return float(occ_bw_hz), int(occ_bins), float(occ_frac), float(voice_score)


def estimate_s_units(*, rel_db: float, s1_db: float = 12.0, db_per_s: float = 6.0) -> float:
    """Estimate S-units from a relative dB-above-noise value.

    This is an approximation for display/filtering, not a calibrated S-meter.
    By convention S-units are often treated as ~6 dB per S-unit.
    """

    if db_per_s <= 0:
        db_per_s = 6.0
    # S1 occurs at rel_db ~= s1_db. Each +db_per_s => +1 S-unit.
    return 1.0 + (float(rel_db) - float(s1_db)) / float(db_per_s)


def _ansi_strength_bar(*, rel_db: float, width: int = 18) -> str:
    """Render a simple ANSI-colored strength bar.

    This is purely a visual indicator; it does not affect JSON outputs.
    """

    w = int(width)
    if w <= 0:
        return ""

    # Map rel_db into a 0..1 fraction. 0 dB => empty, ~30 dB => full.
    frac = max(0.0, min(1.0, float(rel_db) / 30.0))
    filled = int(round(frac * w))
    filled = max(0, min(w, filled))

    def color_for_index(i: int) -> str:
        # gradient: green -> yellow -> red
        t = float(i) / max(1.0, float(w - 1))
        if t < 0.55:
            return "\x1b[32m"  # green
        if t < 0.80:
            return "\x1b[33m"  # yellow
        return "\x1b[31m"  # red

    reset = "\x1b[0m"
    if filled <= 0:
        return f"[{(' ' * w)}]"

    chunks: list[str] = ["["]
    for i in range(w):
        if i < filled:
            chunks.append(color_for_index(i))
            chunks.append("█")
            chunks.append(reset)
        else:
            chunks.append(" ")
    chunks.append("]")
    return "".join(chunks)


def _round_activity_entry(entry: dict) -> dict:
    """Round numeric fields for JSON output readability."""

    out = dict(entry)
    if "freq_mhz" in out and out["freq_mhz"] is not None:
        out["freq_mhz"] = round(float(out["freq_mhz"]), 4)
    if "s_est" in out and out["s_est"] is not None:
        out["s_est"] = round(float(out["s_est"]), 1)
    if "width_hz" in out and out["width_hz"] is not None:
        out["width_hz"] = round(float(out["width_hz"]), 1)
    if "rel_db" in out and out["rel_db"] is not None:
        out["rel_db"] = round(float(out["rel_db"]), 1)
    return out


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(v) for v in values)
    pos = (max(0.0, min(100.0, float(pct))) / 100.0) * float(len(ordered) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    frac = pos - float(lo)
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac)


def _adaptive_ssb_threshold_db(
    *,
    power_bins: list[float],
    base_threshold_db: float,
    min_threshold_db: float,
    max_threshold_db: float,
    spread_gain: float,
    spread_offset_db: float,
    spread_target_db: float,
) -> float:
    if not power_bins:
        return float(base_threshold_db)
    p50 = _percentile(power_bins, 50.0)
    p95 = _percentile(power_bins, 95.0)
    spread = max(0.0, float(p95) - float(p50))
    spread_delta = float(spread) - float(spread_target_db)
    derived = float(base_threshold_db) + float(spread_offset_db) + float(spread_gain) * float(spread_delta)
    bounded = max(float(min_threshold_db), min(float(max_threshold_db), float(derived)))
    # Blend base and derived so adaptive mode can move in either direction
    # while staying stable for brief frame-to-frame variance.
    blended = (0.5 * float(base_threshold_db)) + (0.5 * float(bounded))
    return float(max(float(min_threshold_db), min(float(max_threshold_db), float(blended))))


def bin_to_hz(*, center_freq_hz: float, span_hz: float, n_bins: int, bin_center: float) -> float:
    # Assume bins evenly cover the span with DC at center.
    # Map bin index [0..n_bins-1] to offset [-span/2 .. +span/2].
    if n_bins <= 1:
        return float(center_freq_hz)
    frac = float(bin_center) / float(n_bins - 1)
    offset = (frac - 0.5) * float(span_hz)
    return float(center_freq_hz + offset)


def run_scan(
    *,
    host: str,
    port: int,
    password: str | None,
    user: str,
    rx_chan: int | None = None,
    band: str | None = None,
    center_freq_hz: float,
    span_hz: float,
    threshold_db: float,
    min_width_bins: int,
    min_width_hz: float = 0.0,
    ssb_detect: bool = False,
    ssb_only: bool = False,
    required_hits: int,
    tolerance_bins: float,
    expiry_frames: int,
    max_frames: Optional[int],
    jsonl_path: Path | None,
    jsonl_events_path: Path | None = None,
    json_report_path: Path | None = None,
    min_s: float = 1.0,
    s1_db: float = 12.0,
    db_per_s: float = 6.0,
    record: bool = False,
    record_seconds: int = 30,
    record_mode: str = "usb",
    record_out: Path = Path("recordings"),
    show: bool = False,
    show_top: int = 5,
    spanbar: bool = False,
    spanbar_width: int = 80,
    spanbar_scale: str = "frame",
    spanbar_color: bool = False,
    spark: bool = False,
    spark_width: int = 80,
    spark_clip_db: float = 25.0,
    spark_charset: str = "block",
    spark_color: bool = False,
    spark_bucket: str = "p90",
    spark_auto_clip: bool = False,
    debug: bool = False,
    debug_messages: bool = False,
    phone_only: bool = False,
    bandplan_region: str = "region2",
    signalbar: bool = False,
    signalbar_width: int = 18,
    rx_wait_timeout_s: float = 0.0,
    rx_wait_interval_s: float = 2.0,
    rx_wait_max_retries: int = 0,
    status_hold_s: float = 0.0,
    max_runtime_s: float = 0.0,
    status_modulation: str = "usb",
    status_pre_tune: bool = True,
    status_parallel_snd: bool = False,
    ssb_occ_thresh_db: float = 6.0,
    ssb_voice_min_score: float = 0.0,
    ssb_early_stop_frames: int = 0,
    ssb_warmup_frames: int = 1,
    ssb_adaptive_threshold: bool = False,
    ssb_adaptive_min_db: float = 5.0,
    ssb_adaptive_max_db: float = 20.0,
    ssb_adaptive_spread_gain: float = 0.35,
    ssb_adaptive_spread_offset_db: float = 2.5,
    ssb_adaptive_spread_target_db: float = 55.0,
    acceptable_rx_chans: tuple[int, ...] | list[int] | None = None,
) -> int:
    if phone_only:
        # Display bandplan-derived range so the user knows what is being kept.
        # We compute this from the bandplan and then clamp it to the current
        # receiver window.
        scan_lo = float(center_freq_hz) - float(span_hz) / 2.0
        scan_hi = float(center_freq_hz) + float(span_hz) / 2.0
        band_for_display = str(band).strip() if band else "40m"
        phone_ranges = bandplan_ranges_for_label("Phone", band=band_for_display, region=bandplan_region)
        clipped: list[tuple[float, float]] = []
        for lo, hi in phone_ranges:
            lo2 = max(float(lo), scan_lo)
            hi2 = min(float(hi), scan_hi)
            if lo2 < hi2:
                clipped.append((lo2, hi2))
        if clipped:
            parts = ", ".join(f"{lo/1e6:.4f}-{hi/1e6:.4f} MHz" for lo, hi in clipped)
            print(f"FILTER phone-only region={bandplan_region} keep={parts}")
        else:
            parts = ", ".join(f"{lo/1e6:.4f}-{hi/1e6:.4f} MHz" for lo, hi in phone_ranges) or "(none)"
            print(f"FILTER phone-only region={bandplan_region} keep={parts} (no overlap with current span)")
    tracker = PersistenceTracker(
        tolerance_bins=tolerance_bins,
        required_hits=required_hits,
        expiry_frames=expiry_frames,
    )
    temporal_tracker = _TemporalFeatureTracker(
        tolerance_bins=tolerance_bins,
        expiry_frames=expiry_frames,
    )

    out_f = None
    if jsonl_path is not None:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        out_f = jsonl_path.open("a", encoding="utf-8")

    evt_f = None
    if jsonl_events_path is not None:
        jsonl_events_path.parent.mkdir(parents=True, exist_ok=True)
        evt_f = jsonl_events_path.open("a", encoding="utf-8")

    last_emit: dict[float, int] = {}
    did_record = False
    best: dict[str, float] | None = None
    ssb_frames_seen = 0
    ssb_seen_good = False
    ssb_stop = False
    last_ssb_threshold_db = float(threshold_db)
    min_ssb_threshold_db = float(threshold_db)
    max_ssb_threshold_db = float(threshold_db)
    last_ssb_spread_db = 0.0
    min_ssb_spread_db = 0.0
    max_ssb_spread_db = 0.0
    frames_seen = 0
    stop_reason: str | None = None

    def on_frame(frame: WaterfallFrame) -> None:
        nonlocal did_record
        nonlocal ssb_frames_seen, ssb_seen_good, ssb_stop
        nonlocal last_ssb_threshold_db
        nonlocal min_ssb_threshold_db, max_ssb_threshold_db
        nonlocal last_ssb_spread_db, min_ssb_spread_db, max_ssb_spread_db
        nonlocal best
        nonlocal frames_seen
        frames_seen += 1
        noise, peaks = detect_peaks_with_noise_floor(
            frame.power_bins,
            threshold_db=threshold_db,
            min_width_bins=min_width_bins,
        )
        peak_details = _describe_peaks(frame=frame, noise_floor=float(noise), peaks=peaks)
        temporal_tracker.update(frame_index=frame.frame_index, peak_details=peak_details)

        if bool(ssb_only):
            if not frame.power_bins:
                return
            ssb_frames_seen += 1
            if int(ssb_warmup_frames) > 0 and ssb_frames_seen <= int(ssb_warmup_frames):
                return
            threshold_for_ssb = float(threshold_db)
            if bool(ssb_adaptive_threshold):
                p50 = _percentile(frame.power_bins, 50.0)
                p95 = _percentile(frame.power_bins, 95.0)
                spread_db = max(0.0, float(p95) - float(p50))
                last_ssb_spread_db = float(spread_db)
                if int(ssb_frames_seen) <= int(ssb_warmup_frames) + 1:
                    min_ssb_spread_db = float(spread_db)
                    max_ssb_spread_db = float(spread_db)
                else:
                    min_ssb_spread_db = min(float(min_ssb_spread_db), float(spread_db))
                    max_ssb_spread_db = max(float(max_ssb_spread_db), float(spread_db))
                threshold_for_ssb = _adaptive_ssb_threshold_db(
                    power_bins=frame.power_bins,
                    base_threshold_db=float(threshold_db),
                    min_threshold_db=float(ssb_adaptive_min_db),
                    max_threshold_db=float(ssb_adaptive_max_db),
                    spread_gain=float(ssb_adaptive_spread_gain),
                    spread_offset_db=float(ssb_adaptive_spread_offset_db),
                    spread_target_db=float(ssb_adaptive_spread_target_db),
                )
            last_ssb_threshold_db = float(threshold_for_ssb)
            min_ssb_threshold_db = min(float(min_ssb_threshold_db), float(threshold_for_ssb))
            max_ssb_threshold_db = max(float(max_ssb_threshold_db), float(threshold_for_ssb))
            peak_i = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
            freq0 = bin_to_hz(
                center_freq_hz=frame.center_freq_hz,
                span_hz=frame.span_hz,
                n_bins=len(frame.power_bins),
                bin_center=float(peak_i),
            )
            rel_bp, width_bins_bp, width_hz_bp = _bandpower_rel_db(
                frame=frame,
                noise_floor=float(noise),
                center_hz=float(freq0),
                width_hz=2400.0,
            )
            occ_bw_hz, occ_bins, occ_frac, voice_score = _ssb_voice_metrics(
                frame=frame,
                noise_floor=float(noise),
                center_hz=float(freq0),
                window_hz=2400.0,
                occ_thresh_db=float(ssb_occ_thresh_db),
            )
            if float(min_width_hz) > 0 and float(occ_bw_hz) < float(min_width_hz):
                rel_bp = 0.0
            if float(ssb_voice_min_score) > 0:
                if float(voice_score) >= float(ssb_voice_min_score):
                    ssb_seen_good = True
                elif int(ssb_early_stop_frames) > 0 and (not ssb_seen_good) and ssb_frames_seen >= int(ssb_early_stop_frames):
                    ssb_stop = True

            # Track a "best" candidate even if it doesn't pass the voice_score gate.
            try:
                rel_db0 = float(rel_bp)
                s_est0 = estimate_s_units(rel_db=rel_db0, s1_db=s1_db, db_per_s=db_per_s)
                if best is None or rel_db0 > float(best.get("rel_db", -1e9)):
                    best = {
                        "freq_mhz": float(freq0 / 1e6),
                        "s_est": float(s_est0),
                        "rel_db": float(rel_db0),
                        "voice_score": float(voice_score),
                        "occ_bw_hz": float(occ_bw_hz),
                        "occ_frac": float(occ_frac),
                    }
            except Exception:
                pass
            if rel_bp >= float(threshold_for_ssb) and (float(ssb_voice_min_score) <= 0 or float(voice_score) >= float(ssb_voice_min_score)):
                bp = bandplan_label(float(freq0), region=bandplan_region)
                if (not phone_only) or bp == "Phone":
                    width_guess = str(guess_signal_type(width_hz=float(occ_bw_hz)))
                    det = Detection(
                        t_unix=time.time(),
                        frame_index=frame.frame_index,
                        noise_floor=float(noise),
                        threshold_db=float(threshold_for_ssb),
                        bin_center=float(peak_i),
                        width_bins=int(occ_bins) if int(occ_bins) > 0 else int(width_bins_bp),
                        width_hz=float(occ_bw_hz) if float(occ_bw_hz) > 0 else float(width_hz_bp),
                        type_guess=combine_type_hints(width_guess=width_guess, bandplan_label=bp),
                        bandplan=bp,
                        peak_power=float(noise) + float(rel_bp),
                        freq_mhz=float(freq0 / 1e6),
                        candidate_type=classify_candidate_type(
                            width_hz=float(occ_bw_hz) if float(occ_bw_hz) > 0 else float(width_hz_bp),
                            type_guess=combine_type_hints(width_guess=width_guess, bandplan_label=bp),
                            bandplan_label=bp,
                            voice_score=float(voice_score),
                            occ_frac=float(occ_frac),
                        ),
                        ssb_detect=True,
                        occ_bw_hz=float(occ_bw_hz),
                        occ_frac=float(occ_frac),
                        voice_score=float(voice_score),
                    )
                    if out_f is not None:
                        out_f.write(json.dumps(asdict(det), sort_keys=True) + "\n")
                        out_f.flush()
                    if evt_f is not None:
                        rel_db = float(det.peak_power) - float(det.noise_floor)
                        s_est = estimate_s_units(rel_db=rel_db, s1_db=s1_db, db_per_s=db_per_s)
                        evt = {
                            "t_unix": det.t_unix,
                            "frame_index": det.frame_index,
                            "freq_mhz": det.freq_mhz,
                            "center_freq_hz": frame.center_freq_hz,
                            "span_hz": frame.span_hz,
                            "width_bins": det.width_bins,
                            "width_hz": det.width_hz,
                            "type_guess": det.type_guess,
                            "candidate_type": det.candidate_type,
                            "bandplan": det.bandplan,
                            "peak_power": det.peak_power,
                            "noise_floor": det.noise_floor,
                            "rel_db": rel_db,
                            "s_est": s_est,
                            "occ_bw_hz": det.occ_bw_hz,
                            "occ_frac": det.occ_frac,
                            "voice_score": det.voice_score,
                        }
                        evt_f.write(json.dumps(evt, sort_keys=True) + "\n")
                        evt_f.flush()
                    if record and not did_record:
                        did_record = True
                        try:
                            print(f"RECORD starting {record_seconds}s @ {det.freq_mhz:.4f} MHz")
                            run_record(
                                RecordRequest(
                                    host=host,
                                    port=int(port),
                                    password=password,
                                    user=user,
                                    freq_hz=float(det.freq_mhz) * 1e6,
                                    duration_s=int(record_seconds),
                                    mode=str(record_mode),
                                    out_dir=record_out,
                                )
                            )
                        except RecorderUnavailable as e:
                            print(f"RECORD failed: {e}")
                        except Exception as e:
                            print(f"RECORD failed: {type(e).__name__}: {e}")
            return

        # Track the strongest instantaneous bin for reporting, even if it doesn't
        # meet persistence/dedup criteria.
        if frame.power_bins:
            peak_i = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
            peak_power = float(frame.power_bins[peak_i])
            rel_db0 = peak_power - float(noise)
            s_est0 = estimate_s_units(rel_db=rel_db0, s1_db=s1_db, db_per_s=db_per_s)
            if s_est0 >= float(min_s):
                if best is None or rel_db0 > float(best["rel_db"]):
                    best = {
                        "freq_mhz": float(
                            bin_to_hz(
                                center_freq_hz=frame.center_freq_hz,
                                span_hz=frame.span_hz,
                                n_bins=len(frame.power_bins),
                                bin_center=float(peak_i),
                            )
                            / 1e6
                        ),
                        "s_est": float(s_est0),
                        "rel_db": float(rel_db0),
                    }

        if show or spark or spanbar:
            def _b2hz(b: float) -> float:
                return bin_to_hz(
                    center_freq_hz=frame.center_freq_hz,
                    span_hz=frame.span_hz,
                    n_bins=len(frame.power_bins),
                    bin_center=b,
                )

            if show:
                tps = top_peaks(
                    power_bins=frame.power_bins,
                    bin_to_hz_fn=_b2hz,
                    noise=float(noise),
                    n=int(show_top),
                )
                peaks_str = " ".join(
                    f"{p.freq_hz/1e6:.6f}MHz+{p.above_noise:.1f}dB" for p in tps
                )
                print(
                    f"FRAME f0={frame.center_freq_hz/1e6:.6f}MHz noise={float(noise):.1f} {peaks_str}"
                )
            if spark:
                print(
                    sparkline(
                        frame.power_bins,
                        noise=float(noise),
                        width=int(spark_width),
                        clip_db=float(spark_clip_db),
                        charset=str(spark_charset),
                        color=bool(spark_color),
                        bucket=str(spark_bucket),
                        auto_clip=bool(spark_auto_clip),
                    )
                )
            if spanbar:
                # marker at strongest peak (by power)
                if frame.power_bins:
                    peak_i = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
                    marker = int(round((peak_i / max(1, len(frame.power_bins) - 1)) * (int(spanbar_width) - 1)))
                else:
                    marker = None
                bar, strength = span_bar(
                    frame.power_bins,
                    width=int(spanbar_width),
                    scale=str(spanbar_scale),
                    color=bool(spanbar_color),
                    marker_index=marker,
                )
                print(f"SPAN [{frame.center_freq_hz-frame.span_hz/2.0:,.0f}..{frame.center_freq_hz+frame.span_hz/2.0:,.0f}]Hz lvl={strength:.2f} {bar}")
        persistent = tracker.update(frame.frame_index, peaks)

        # Optional wideband/SSB detection: treat the strongest bin as a candidate center,
        # then compute bandpower across a ~2.4 kHz window.
        if bool(ssb_detect) and frame.power_bins:
            ssb_frames_seen += 1
            if int(ssb_warmup_frames) > 0 and ssb_frames_seen <= int(ssb_warmup_frames):
                return
            threshold_for_ssb = float(threshold_db)
            if bool(ssb_adaptive_threshold):
                p50 = _percentile(frame.power_bins, 50.0)
                p95 = _percentile(frame.power_bins, 95.0)
                spread_db = max(0.0, float(p95) - float(p50))
                last_ssb_spread_db = float(spread_db)
                if int(ssb_frames_seen) <= int(ssb_warmup_frames) + 1:
                    min_ssb_spread_db = float(spread_db)
                    max_ssb_spread_db = float(spread_db)
                else:
                    min_ssb_spread_db = min(float(min_ssb_spread_db), float(spread_db))
                    max_ssb_spread_db = max(float(max_ssb_spread_db), float(spread_db))
                threshold_for_ssb = _adaptive_ssb_threshold_db(
                    power_bins=frame.power_bins,
                    base_threshold_db=float(threshold_db),
                    min_threshold_db=float(ssb_adaptive_min_db),
                    max_threshold_db=float(ssb_adaptive_max_db),
                    spread_gain=float(ssb_adaptive_spread_gain),
                    spread_offset_db=float(ssb_adaptive_spread_offset_db),
                    spread_target_db=float(ssb_adaptive_spread_target_db),
                )
            last_ssb_threshold_db = float(threshold_for_ssb)
            min_ssb_threshold_db = min(float(min_ssb_threshold_db), float(threshold_for_ssb))
            max_ssb_threshold_db = max(float(max_ssb_threshold_db), float(threshold_for_ssb))
            peak_i = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
            freq0 = bin_to_hz(
                center_freq_hz=frame.center_freq_hz,
                span_hz=frame.span_hz,
                n_bins=len(frame.power_bins),
                bin_center=float(peak_i),
            )
            rel_bp, width_bins_bp, width_hz_bp = _bandpower_rel_db(
                frame=frame,
                noise_floor=float(noise),
                center_hz=float(freq0),
                width_hz=2400.0,
            )
            occ_bw_hz, occ_bins, occ_frac, voice_score = _ssb_voice_metrics(
                frame=frame,
                noise_floor=float(noise),
                center_hz=float(freq0),
                window_hz=2400.0,
                occ_thresh_db=float(ssb_occ_thresh_db),
            )
            if float(min_width_hz) > 0 and float(occ_bw_hz) < float(min_width_hz):
                rel_bp = 0.0
            if float(ssb_voice_min_score) > 0:
                if float(voice_score) >= float(ssb_voice_min_score):
                    ssb_seen_good = True
                elif int(ssb_early_stop_frames) > 0 and (not ssb_seen_good) and ssb_frames_seen >= int(ssb_early_stop_frames):
                    ssb_stop = True
            if rel_bp >= float(threshold_for_ssb) and (float(ssb_voice_min_score) <= 0 or float(voice_score) >= float(ssb_voice_min_score)):
                bp = bandplan_label(float(freq0), region=bandplan_region)
                if (not phone_only) or bp == "Phone":
                    width_guess = str(guess_signal_type(width_hz=float(occ_bw_hz)))
                    det = Detection(
                        t_unix=time.time(),
                        frame_index=frame.frame_index,
                        noise_floor=float(noise),
                        threshold_db=float(threshold_for_ssb),
                        bin_center=float(peak_i),
                        width_bins=int(occ_bins) if int(occ_bins) > 0 else int(width_bins_bp),
                        width_hz=float(occ_bw_hz) if float(occ_bw_hz) > 0 else float(width_hz_bp),
                        type_guess=combine_type_hints(width_guess=width_guess, bandplan_label=bp),
                        bandplan=bp,
                        peak_power=float(noise) + float(rel_bp),
                        freq_mhz=float(freq0 / 1e6),
                        ssb_detect=True,
                        occ_bw_hz=float(occ_bw_hz),
                        occ_frac=float(occ_frac),
                        voice_score=float(voice_score),
                    )
                    print(
                        f"DETECT frame={det.frame_index} f={det.freq_mhz:.4f}MHz bin={det.bin_center:.1f} "
                        f"w={det.width_hz:.0f}Hz({det.width_bins}b) type={det.type_guess} "
                        f"p={det.peak_power:.1f} noise={det.noise_floor:.1f} (+{det.threshold_db:.1f})"
                    )

        for p in persistent:
            # crude de-dupe: only emit once every N frames per track
            key = float(round(p.bin_center, 1))
            last = last_emit.get(key, -10**9)
            if frame.frame_index - last < required_hits:
                continue
            last_emit[key] = frame.frame_index

            # Find the peak cluster that best matches this persistent track.
            if peaks:
                peak_cluster = min(peaks, key=lambda pk: abs(float(pk.bin_center) - float(p.bin_center)))
                width_bins = int(peak_cluster.bin_high - peak_cluster.bin_low + 1)
                width_hz = peak_width_hz(frame=frame, bin_low=peak_cluster.bin_low, bin_high=peak_cluster.bin_high)
            else:
                width_bins = 0
                width_hz = 0.0

            if float(min_width_hz) > 0 and float(width_hz) < float(min_width_hz):
                continue

            freq_hz = float(
                bin_to_hz(
                    center_freq_hz=frame.center_freq_hz,
                    span_hz=frame.span_hz,
                    n_bins=len(frame.power_bins),
                    bin_center=p.bin_center,
                )
            )
            bp = bandplan_label(freq_hz, region=bandplan_region)
            if phone_only and bp != "Phone":
                continue

            occ_bw_hz: float | None = None
            occ_frac: float | None = None
            voice_score: float | None = None
            temporal_summary = temporal_tracker.summarize_for_bin(bin_center=float(p.bin_center))
            if frame.power_bins:
                occ_bw_hz0, _occ_bins0, occ_frac0, voice_score0 = _ssb_voice_metrics(
                    frame=frame,
                    noise_floor=float(noise),
                    center_hz=float(freq_hz),
                    window_hz=max(2400.0, float(width_hz) if float(width_hz) > 0 else 2400.0),
                    occ_thresh_db=float(ssb_occ_thresh_db),
                )
                occ_bw_hz = float(occ_bw_hz0) if float(occ_bw_hz0) > 0 else (float(width_hz) if float(width_hz) > 0 else None)
                occ_frac = float(occ_frac0)
                voice_score = float(voice_score0)

            width_guess = str(guess_signal_type(width_hz=float(width_hz)))
            det = Detection(
                t_unix=time.time(),
                frame_index=frame.frame_index,
                noise_floor=float(noise),
                threshold_db=float(threshold_db),
                bin_center=float(p.bin_center),
                width_bins=int(width_bins),
                width_hz=float(width_hz),
                type_guess=combine_type_hints(width_guess=width_guess, bandplan_label=bp),
                bandplan=bp,
                peak_power=float(p.peak_power),
                freq_mhz=float(
                    freq_hz / 1e6
                ),
                candidate_type=classify_candidate_type(
                    width_hz=occ_bw_hz if occ_bw_hz is not None else float(width_hz),
                    type_guess=combine_type_hints(width_guess=width_guess, bandplan_label=bp),
                    bandplan_label=bp,
                    voice_score=voice_score,
                    occ_frac=occ_frac,
                    speech_score=temporal_summary.get("speech_envelope_score"),
                    sweep_score=temporal_summary.get("sweep_score"),
                    keying_score=temporal_summary.get("keying_score"),
                    cadence_score=temporal_summary.get("cadence_score"),
                    narrow_peak_count=temporal_summary.get("narrow_peak_count"),
                    envelope_variance=temporal_summary.get("envelope_variance"),
                    has_on_off_keying=temporal_summary.get("has_on_off_keying"),
                ),
                occ_bw_hz=occ_bw_hz,
                occ_frac=occ_frac,
                voice_score=voice_score,
                narrow_peak_count=int(temporal_summary.get("narrow_peak_count") or 0),
                narrow_peak_span_hz=float(temporal_summary.get("narrow_peak_span_hz") or 0.0),
                keying_score=float(temporal_summary.get("keying_score") or 0.0),
                steady_tone_score=float(temporal_summary.get("steady_tone_score") or 0.0),
                freq_stability_hz=float(temporal_summary.get("freq_stability_hz") or 0.0),
                envelope_variance=float(temporal_summary.get("envelope_variance") or 0.0),
                speech_envelope_score=float(temporal_summary.get("speech_envelope_score") or 0.0),
                sweep_score=float(temporal_summary.get("sweep_score") or 0.0),
                centroid_drift_hz=float(temporal_summary.get("centroid_drift_hz") or 0.0),
                observed_frames=int(temporal_summary.get("observed_frames") or 0),
                active_fraction=float(temporal_summary.get("active_fraction") or 0.0),
                cadence_score=float(temporal_summary.get("cadence_score") or 0.0),
                keying_edge_count=int(temporal_summary.get("keying_edge_count") or 0),
                has_on_off_keying=bool(temporal_summary.get("has_on_off_keying")),
                amplitude_span_db=float(temporal_summary.get("amplitude_span_db") or 0.0),
            )
            print(
                f"DETECT frame={det.frame_index} f={det.freq_mhz:.4f}MHz bin={det.bin_center:.1f} "
                f"w={det.width_hz:.0f}Hz({det.width_bins}b) type={det.type_guess} "
                f"p={det.peak_power:.1f} noise={det.noise_floor:.1f} (+{det.threshold_db:.1f})"
            )
            if out_f is not None:
                out_f.write(json.dumps(asdict(det), sort_keys=True) + "\n")
                out_f.flush()

            rel_db = float(det.peak_power) - float(det.noise_floor)
            if bool(signalbar):
                bar = _ansi_strength_bar(rel_db=rel_db, width=int(signalbar_width))
                if bar:
                    print(f"  rel={rel_db:.1f}dB {bar}")
            s_est = estimate_s_units(rel_db=rel_db, s1_db=s1_db, db_per_s=db_per_s)
            if s_est >= float(min_s):
                if best is None or rel_db > float(best["rel_db"]):
                    best = {
                        "freq_mhz": float(det.freq_mhz),
                        "s_est": float(s_est),
                        "rel_db": float(rel_db),
                    }

            # Optional event stream filtered by estimated S-level.
            if evt_f is not None:
                if s_est >= float(min_s):
                    evt = {
                        "t_unix": det.t_unix,
                        "frame_index": det.frame_index,
                        "freq_mhz": det.freq_mhz,
                        "center_freq_hz": frame.center_freq_hz,
                        "span_hz": frame.span_hz,
                        "width_bins": det.width_bins,
                        "width_hz": det.width_hz,
                        "type_guess": det.type_guess,
                        "candidate_type": det.candidate_type,
                        "bandplan": det.bandplan,
                        "peak_power": det.peak_power,
                        "noise_floor": det.noise_floor,
                        "rel_db": rel_db,
                        "s_est": s_est,
                        "occ_bw_hz": det.occ_bw_hz,
                        "occ_frac": det.occ_frac,
                        "voice_score": det.voice_score,
                        "narrow_peak_count": det.narrow_peak_count,
                        "narrow_peak_span_hz": det.narrow_peak_span_hz,
                        "keying_score": det.keying_score,
                        "steady_tone_score": det.steady_tone_score,
                        "freq_stability_hz": det.freq_stability_hz,
                        "envelope_variance": det.envelope_variance,
                        "speech_envelope_score": det.speech_envelope_score,
                        "sweep_score": det.sweep_score,
                        "centroid_drift_hz": det.centroid_drift_hz,
                        "observed_frames": det.observed_frames,
                        "active_fraction": det.active_fraction,
                        "cadence_score": det.cadence_score,
                    }
                    evt_f.write(json.dumps(evt, sort_keys=True) + "\n")
                    evt_f.flush()

            if record and not did_record:
                did_record = True
                try:
                    print(
                        f"RECORD starting {record_seconds}s @ {det.freq_mhz:.4f} MHz"
                    )
                    run_record(
                        RecordRequest(
                            host=host,
                            port=port,
                            password=password,
                            user=user,
                            freq_hz=float(det.freq_mhz) * 1e6,
                            duration_s=record_seconds,
                            mode=record_mode,
                            out_dir=record_out,
                        )
                    )
                    print(f"RECORD done: {record_out}")
                except RecorderUnavailable as e:
                    print(f"RECORD skipped: {e}")

    try:
        retry_start = time.time()
        busy_retries = 0
        transient_retries = 0
        while True:
            try:
                status_stop_event: threading.Event | None = None
                status_thread: threading.Thread | None = None
                status_ready_event: threading.Event | None = None
                ws_timestamp: int | None = None

                if rx_chan is not None and bool(status_parallel_snd):
                    ws_timestamp = allocate_ws_timestamp()
                    status_stop_event = threading.Event()
                    status_ready_event = threading.Event()

                    def _run_status_stream() -> None:
                        try:
                            set_receiver_frequency(
                                host=host,
                                port=port,
                                rx_chan=int(rx_chan),
                                freq_hz=float(center_freq_hz),
                                password=password,
                                user=user,
                                timeout_s=10.0,
                                hold_s=0.0,
                                rx_wait_timeout_s=rx_wait_timeout_s,
                                rx_wait_interval_s=rx_wait_interval_s,
                                rx_wait_max_retries=rx_wait_max_retries,
                                modulation=str(status_modulation),
                                acceptable_rx_chans=acceptable_rx_chans,
                                ws_timestamp=ws_timestamp,
                                hold_event=status_stop_event,
                                ready_event=status_ready_event,
                            )
                        except Exception:
                            return

                    status_thread = threading.Thread(
                        name=f"scan-status-rx{int(rx_chan)}",
                        target=_run_status_stream,
                        daemon=True,
                    )
                    status_thread.start()
                    status_ready_event.wait(timeout=max(0.5, min(2.0, float(status_hold_s) or 2.0)))

                if rx_chan is not None and bool(status_pre_tune) and not bool(status_parallel_snd):
                    ok = set_receiver_frequency(
                        host=host,
                        port=port,
                        rx_chan=int(rx_chan),
                        freq_hz=float(center_freq_hz),
                        password=password,
                        user=user,
                        timeout_s=10.0,
                        hold_s=float(status_hold_s),
                        rx_wait_timeout_s=rx_wait_timeout_s,
                        rx_wait_interval_s=rx_wait_interval_s,
                        rx_wait_max_retries=rx_wait_max_retries,
                        modulation=str(status_modulation),
                        acceptable_rx_chans=acceptable_rx_chans,
                        ws_timestamp=ws_timestamp,
                    )
                    if not ok:
                        raise KiwiCampRejected(requested_rx=int(rx_chan), response="tune failed")
                start_time = time.time()
                def _stop_scan() -> bool:
                    # Early stop for probe-style scans when we quickly determine
                    # a window is not voice-like.
                    nonlocal stop_reason
                    if bool(ssb_stop):
                        stop_reason = stop_reason or "ssb_early_stop"
                        return True
                    if float(max_runtime_s) <= 0:
                        return False
                    if (time.time() - start_time) >= float(max_runtime_s):
                        stop_reason = stop_reason or "max_runtime"
                        return True
                    return False

                subscribe_waterfall(
                    host=host,
                    port=port,
                    password=password,
                    user=user,
                    rx_chan=rx_chan,
                    center_freq_hz=center_freq_hz,
                    span_hz=span_hz,
                    on_frame=on_frame,
                    should_stop=_stop_scan if (float(max_runtime_s) > 0 or int(ssb_early_stop_frames) > 0) else None,
                    max_frames=max_frames,
                    min_duration_s=float(status_hold_s) if float(status_hold_s) > 0 else None,
                    max_duration_s=float(max_runtime_s) if float(max_runtime_s) > 0 else None,
                    debug=debug,
                    debug_messages=debug_messages,
                    status_modulation=str(status_modulation),
                    acceptable_rx_chans=acceptable_rx_chans,
                    ws_timestamp=ws_timestamp,
                )
                if frames_seen == 0 and stop_reason is None:
                    raise TimeoutError("waterfall exited before first frame")
                break
            except KiwiCampRejected as e:
                if rx_chan is None:
                    raise
                busy_retries += 1
                elapsed = time.time() - retry_start
                if int(rx_wait_max_retries) > 0 and busy_retries > int(rx_wait_max_retries):
                    print(f"RX{int(rx_chan)} unavailable after {rx_wait_max_retries} retries ({e})")
                    return 3
                if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                    print(f"RX{int(rx_chan)} unavailable after {elapsed:.1f}s ({e})")
                    return 3
                sleep_s = max(0.25, float(rx_wait_interval_s))
                print(f"RX{int(rx_chan)} unavailable ({e}); retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
            except Exception as e:
                # Some Kiwi/client versions raise custom exceptions on early
                # disconnects (e.g. server closed the connection unexpectedly).
                # Treat these as transient and retry a few times.
                name = type(e).__name__
                msg = str(e)
                msg_lower = msg.lower()
                is_busy = (
                    "all 8 client slots taken" in msg_lower
                    or "all client slots taken" in msg_lower
                    or "too busy now" in msg_lower
                )
                is_transient = name in {
                    "KiwiServerTerminatedConnection",
                    "ConnectionResetError",
                    "BrokenPipeError",
                    "TimeoutError",
                } or "server closed the connection" in msg_lower or "connection reset" in msg_lower

                if is_busy and rx_chan is not None:
                    busy_retries += 1
                    elapsed = time.time() - retry_start
                    if int(rx_wait_max_retries) > 0 and busy_retries > int(rx_wait_max_retries):
                        print(f"RX{int(rx_chan)} unavailable after {rx_wait_max_retries} retries ({msg})")
                        return 3
                    if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                        print(f"RX{int(rx_chan)} unavailable after {elapsed:.1f}s ({msg})")
                        return 3
                    sleep_s = max(0.25, float(rx_wait_interval_s))
                    print(f"RX{int(rx_chan)} busy ({msg}); retrying in {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
                    continue

                if not is_transient:
                    raise

                transient_retries += 1
                elapsed = time.time() - retry_start
                max_retry = int(rx_wait_max_retries) if int(rx_wait_max_retries) > 0 else 3
                if transient_retries > max_retry:
                    print(f"ERROR: waterfall transient disconnect after {transient_retries} retries: {name}: {msg}")
                    return 2
                if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                    print(f"ERROR: waterfall transient disconnect after {elapsed:.1f}s: {name}: {msg}")
                    return 2
                sleep_s = min(3.0, max(0.5, float(rx_wait_interval_s)))
                print(f"WARN: transient Kiwi disconnect ({name}); retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
            finally:
                if status_stop_event is not None:
                    status_stop_event.set()
                if status_thread is not None:
                    status_thread.join(timeout=1.0)
    except KiwiClientUnavailable as e:
        print(f"ERROR: {e}")
        return 2
    finally:
        if out_f is not None:
            out_f.close()
        if evt_f is not None:
            evt_f.close()

        if json_report_path is not None:
            json_report_path.parent.mkdir(parents=True, exist_ok=True)
            report = {
                "type": "scan_report",
                "min_s": float(min_s),
                "s1_db": float(s1_db),
                "db_per_s": float(db_per_s),
                "peak": best,
                "frames_seen": int(frames_seen),
                "ssb_frames_seen": int(ssb_frames_seen),
                "ssb_seen_good": bool(ssb_seen_good),
                "ssb_threshold_base_db": float(threshold_db),
                "ssb_threshold_last_db": float(last_ssb_threshold_db),
                "ssb_threshold_min_db": float(min_ssb_threshold_db),
                "ssb_threshold_max_db": float(max_ssb_threshold_db),
                "ssb_spread_last_db": float(last_ssb_spread_db),
                "ssb_spread_min_db": float(min_ssb_spread_db),
                "ssb_spread_max_db": float(max_ssb_spread_db),
                "ssb_warmup_frames": int(ssb_warmup_frames),
                "ssb_adaptive_threshold": bool(ssb_adaptive_threshold),
                "stop_reason": stop_reason,
            }
            json_report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")

    return 0


def run_sweep(
    *,
    host: str,
    port: int,
    password: str | None,
    user: str,
    rx_chan: int | None = None,
    start_hz: float | None,
    end_hz: float | None,
    span_hz: float,
    overlap: float,
    dwell_frames: int,
    threshold_db: float,
    min_width_bins: int,
    min_width_hz: float = 0.0,
    ssb_detect: bool = False,
    required_hits: int,
    tolerance_bins: float,
    expiry_frames: int,
    cache_ttl_s: float,
    cache_quantize_hz: float,
    record: bool = False,
    record_seconds: int = 30,
    record_mode: str = "usb",
    record_out: Path = Path("recordings"),
    show: bool = False,
    show_top: int = 5,
    spanbar: bool = False,
    spanbar_width: int = 80,
    spanbar_scale: str = "frame",
    spanbar_color: bool = False,
    spark: bool = False,
    spark_width: int = 80,
    spark_clip_db: float = 25.0,
    spark_charset: str = "block",
    spark_color: bool = False,
    spark_bucket: str = "p90",
    spark_auto_clip: bool = False,
    debug: bool = False,
    debug_messages: bool = False,
    jsonl_path: Path | None = None,
    jsonl_events_path: Path | None = None,
    json_report_path: Path | None = None,
    json_topn_path: Path | None = None,
    json_activity_path: Path | None = None,
    top_n: int = 5,
    top_quantize_hz: float = 25.0,
    min_s: float = 1.0,
    s1_db: float = 12.0,
    db_per_s: float = 6.0,
    phone_only: bool = False,
    bandplan_region: str = "region2",
    signalbar: bool = False,
    signalbar_width: int = 18,
    rx_wait_timeout_s: float = 0.0,
    rx_wait_interval_s: float = 2.0,
    rx_wait_max_retries: int = 0,
    status_hold_s: float = 0.0,
) -> int:
    # Allow sweeping the full Phone segment without providing explicit start/end.
    if start_hz is None or end_hz is None:
        if not phone_only:
            raise ValueError("--start-hz/--end-hz are required unless --phone-only is set")
        phone_ranges = bandplan_ranges_for_label("Phone", band="40m", region=bandplan_region)
        if not phone_ranges:
            raise ValueError("No Phone ranges available for this bandplan")
        start_hz = min(lo for lo, _ in phone_ranges)
        end_hz = max(hi for _, hi in phone_ranges)

    start_hz = float(start_hz)
    end_hz = float(end_hz)

    if phone_only:
        phone_ranges = bandplan_ranges_for_label("Phone", band="40m", region=bandplan_region)
        clipped: list[tuple[float, float]] = []
        sweep_lo = float(start_hz)
        sweep_hi = float(end_hz)
        for lo, hi in phone_ranges:
            lo2 = max(float(lo), sweep_lo)
            hi2 = min(float(hi), sweep_hi)
            if lo2 < hi2:
                clipped.append((lo2, hi2))
        parts = ", ".join(f"{lo/1e6:.4f}-{hi/1e6:.4f} MHz" for lo, hi in clipped) or "(none)"
        print(f"FILTER phone-only region={bandplan_region} keep={parts}")
    if end_hz <= start_hz:
        raise ValueError("end_hz must be > start_hz")
    if span_hz <= 0:
        raise ValueError("span_hz must be > 0")
    if not (0.0 <= overlap < 1.0):
        raise ValueError("overlap must be in [0,1)")
    if dwell_frames < 1:
        raise ValueError("dwell_frames must be >= 1")

    step_hz = span_hz * (1.0 - overlap)
    if step_hz <= 0:
        raise ValueError("invalid step_hz")

    cache = TTLCache(cache_ttl_s)

    out_f = None
    if jsonl_path is not None:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        out_f = jsonl_path.open("a", encoding="utf-8")

    evt_f = None
    if jsonl_events_path is not None:
        jsonl_events_path.parent.mkdir(parents=True, exist_ok=True)
        evt_f = jsonl_events_path.open("a", encoding="utf-8")

    try:
        # Sweep centers that cover [start_hz..end_hz] with `span_hz` windows.
        cf = float(start_hz) + float(span_hz) / 2.0
        last_cf = float(end_hz) - float(span_hz) / 2.0
        overall_best: dict[str, float] | None = None
        step_reports: list[dict[str, float]] = []
        top_map: dict[int, dict[str, float]] = {}
        while (cf - span_hz / 2.0) < end_hz:
            print(f"SWEEP center={cf/1e6:.6f}MHz span={span_hz/1e3:.1f}kHz")

            tracker = PersistenceTracker(
                tolerance_bins=tolerance_bins,
                required_hits=required_hits,
                expiry_frames=expiry_frames,
            )
            temporal_tracker = _TemporalFeatureTracker(
                tolerance_bins=tolerance_bins,
                expiry_frames=expiry_frames,
            )
            did_record = False
            step_best: dict[str, float] | None = None

            def on_frame(frame: WaterfallFrame) -> None:
                nonlocal did_record
                noise, peaks = detect_peaks_with_noise_floor(
                    frame.power_bins,
                    threshold_db=threshold_db,
                    min_width_bins=min_width_bins,
                )
                peak_details = _describe_peaks(frame=frame, noise_floor=float(noise), peaks=peaks)
                temporal_tracker.update(frame_index=frame.frame_index, peak_details=peak_details)

                # Track instantaneous strongest bin for sweep-wide top-N.
                if frame.power_bins:
                    peak_i0 = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
                    peak_power0 = float(frame.power_bins[peak_i0])
                    rel_db0 = peak_power0 - float(noise)
                    s_est0 = estimate_s_units(rel_db=rel_db0, s1_db=s1_db, db_per_s=db_per_s)
                    if s_est0 >= float(min_s):
                        # Approximate width by finding the above-threshold cluster containing peak_i0.
                        width_bins0 = 1
                        width_hz0 = 0.0
                        try:
                            for pk in peaks:
                                if int(pk.bin_low) <= int(peak_i0) <= int(pk.bin_high):
                                    width_bins0 = int(pk.bin_high - pk.bin_low + 1)
                                    width_hz0 = peak_width_hz(frame=frame, bin_low=pk.bin_low, bin_high=pk.bin_high)
                                    break
                        except Exception:
                            pass
                        freq0 = bin_to_hz(
                            center_freq_hz=frame.center_freq_hz,
                            span_hz=frame.span_hz,
                            n_bins=len(frame.power_bins),
                            bin_center=float(peak_i0),
                        )
                        qhz = float(top_quantize_hz) if float(top_quantize_hz) > 0 else float(cache_quantize_hz)
                        q = int(round(float(freq0) / qhz))
                        prev = top_map.get(q)
                        if prev is None or rel_db0 > float(prev["rel_db"]):
                            top_type_guess = combine_type_hints(
                                width_guess=str(guess_signal_type(width_hz=float(width_hz0))),
                                bandplan_label=bandplan_label(float(freq0)),
                            )
                            top_map[q] = {
                                "freq_mhz": float(freq0 / 1e6),
                                "s_est": float(s_est0),
                                "rel_db": float(rel_db0),
                                "width_hz": float(width_hz0),
                                "width_bins": int(width_bins0),
                                "bandplan": bandplan_label(float(freq0)),
                                "type_guess": top_type_guess,
                                "candidate_type": classify_candidate_type(
                                    width_hz=float(width_hz0),
                                    type_guess=top_type_guess,
                                    bandplan_label=bandplan_label(float(freq0)),
                                ),
                            }

                if show or spark or spanbar:
                    def _b2hz(b: float) -> float:
                        return bin_to_hz(
                            center_freq_hz=frame.center_freq_hz,
                            span_hz=frame.span_hz,
                            n_bins=len(frame.power_bins),
                            bin_center=b,
                        )

                    if show:
                        tps = top_peaks(
                            power_bins=frame.power_bins,
                            bin_to_hz_fn=_b2hz,
                            noise=float(noise),
                            n=int(show_top),
                        )
                        peaks_str = " ".join(
                            f"{p.freq_hz/1e6:.6f}MHz+{p.above_noise:.1f}dB" for p in tps
                        )
                        print(
                            f"FRAME f0={frame.center_freq_hz/1e6:.6f}MHz noise={float(noise):.1f} {peaks_str}"
                        )
                    if spark:
                        print(
                            sparkline(
                                frame.power_bins,
                                noise=float(noise),
                                width=int(spark_width),
                                clip_db=float(spark_clip_db),
                                charset=str(spark_charset),
                                color=bool(spark_color),
                                bucket=str(spark_bucket),
                                auto_clip=bool(spark_auto_clip),
                            )
                        )
                    if spanbar:
                        if frame.power_bins:
                            peak_i = max(range(len(frame.power_bins)), key=lambda i: frame.power_bins[i])
                            marker = int(round((peak_i / max(1, len(frame.power_bins) - 1)) * (int(spanbar_width) - 1)))
                        else:
                            marker = None
                        bar, strength = span_bar(
                            frame.power_bins,
                            width=int(spanbar_width),
                            scale=str(spanbar_scale),
                            color=bool(spanbar_color),
                            marker_index=marker,
                        )
                        print(f"SPAN [{frame.center_freq_hz-frame.span_hz/2.0:,.0f}..{frame.center_freq_hz+frame.span_hz/2.0:,.0f}]Hz lvl={strength:.2f} {bar}")
                persistent = tracker.update(frame.frame_index, peaks)
                now = time.time()
                for p in persistent:
                    if peaks:
                        peak_cluster = min(peaks, key=lambda pk: abs(float(pk.bin_center) - float(p.bin_center)))
                        width_bins = int(peak_cluster.bin_high - peak_cluster.bin_low + 1)
                        width_hz = peak_width_hz(frame=frame, bin_low=peak_cluster.bin_low, bin_high=peak_cluster.bin_high)
                    else:
                        width_bins = 0
                        width_hz = 0.0

                    if float(min_width_hz) > 0 and float(width_hz) < float(min_width_hz):
                        continue
                    freq_hz = bin_to_hz(
                        center_freq_hz=frame.center_freq_hz,
                        span_hz=frame.span_hz,
                        n_bins=len(frame.power_bins),
                        bin_center=p.bin_center,
                    )

                    bp = bandplan_label(float(freq_hz), region=bandplan_region)
                    if phone_only and bp != "Phone":
                        continue
                    q = int(round(freq_hz / cache_quantize_hz))
                    if not cache.allow(q, now=now):
                        continue

                    occ_bw_hz: float | None = None
                    occ_frac: float | None = None
                    voice_score: float | None = None
                    temporal_summary = temporal_tracker.summarize_for_bin(bin_center=float(p.bin_center))
                    if frame.power_bins:
                        occ_bw_hz0, _occ_bins0, occ_frac0, voice_score0 = _ssb_voice_metrics(
                            frame=frame,
                            noise_floor=float(noise),
                            center_hz=float(freq_hz),
                            window_hz=max(2400.0, float(width_hz) if float(width_hz) > 0 else 2400.0),
                            occ_thresh_db=float(ssb_occ_thresh_db),
                        )
                        occ_bw_hz = float(occ_bw_hz0) if float(occ_bw_hz0) > 0 else (float(width_hz) if float(width_hz) > 0 else None)
                        occ_frac = float(occ_frac0)
                        voice_score = float(voice_score0)

                    det = Detection(
                        t_unix=now,
                        frame_index=frame.frame_index,
                        noise_floor=float(noise),
                        threshold_db=float(threshold_db),
                        bin_center=float(p.bin_center),
                        width_bins=int(width_bins),
                        width_hz=float(width_hz),
                        type_guess=combine_type_hints(
                            width_guess=str(guess_signal_type(width_hz=float(width_hz))),
                            bandplan_label=bp,
                        ),
                        bandplan=bp,
                        peak_power=float(p.peak_power),
                        freq_mhz=float(freq_hz / 1e6),
                        candidate_type=classify_candidate_type(
                            width_hz=occ_bw_hz if occ_bw_hz is not None else float(width_hz),
                            type_guess=combine_type_hints(
                                width_guess=str(guess_signal_type(width_hz=float(width_hz))),
                                bandplan_label=bp,
                            ),
                            bandplan_label=bp,
                            voice_score=voice_score,
                            occ_frac=occ_frac,
                            speech_score=temporal_summary.get("speech_envelope_score"),
                            sweep_score=temporal_summary.get("sweep_score"),
                            keying_score=temporal_summary.get("keying_score"),
                            cadence_score=temporal_summary.get("cadence_score"),
                            narrow_peak_count=temporal_summary.get("narrow_peak_count"),
                            envelope_variance=temporal_summary.get("envelope_variance"),
                            has_on_off_keying=temporal_summary.get("has_on_off_keying"),
                        ),
                        occ_bw_hz=occ_bw_hz,
                        occ_frac=occ_frac,
                        voice_score=voice_score,
                        narrow_peak_count=int(temporal_summary.get("narrow_peak_count") or 0),
                        narrow_peak_span_hz=float(temporal_summary.get("narrow_peak_span_hz") or 0.0),
                        keying_score=float(temporal_summary.get("keying_score") or 0.0),
                        steady_tone_score=float(temporal_summary.get("steady_tone_score") or 0.0),
                        freq_stability_hz=float(temporal_summary.get("freq_stability_hz") or 0.0),
                        envelope_variance=float(temporal_summary.get("envelope_variance") or 0.0),
                        speech_envelope_score=float(temporal_summary.get("speech_envelope_score") or 0.0),
                        sweep_score=float(temporal_summary.get("sweep_score") or 0.0),
                        centroid_drift_hz=float(temporal_summary.get("centroid_drift_hz") or 0.0),
                        observed_frames=int(temporal_summary.get("observed_frames") or 0),
                        active_fraction=float(temporal_summary.get("active_fraction") or 0.0),
                        cadence_score=float(temporal_summary.get("cadence_score") or 0.0),
                        keying_edge_count=int(temporal_summary.get("keying_edge_count") or 0),
                        has_on_off_keying=bool(temporal_summary.get("has_on_off_keying")),
                        amplitude_span_db=float(temporal_summary.get("amplitude_span_db") or 0.0),
                    )
                    print(
                        f"DETECT f={det.freq_mhz:.4f}MHz w={det.width_hz:.0f}Hz({det.width_bins}b) "
                        f"type={det.type_guess} p={det.peak_power:.1f} noise={det.noise_floor:.1f} (+{det.threshold_db:.1f})"
                    )
                    if out_f is not None:
                        out_f.write(json.dumps(asdict(det), sort_keys=True) + "\n")
                        out_f.flush()

                    rel_db = float(det.peak_power) - float(det.noise_floor)
                    if bool(signalbar):
                        bar = _ansi_strength_bar(rel_db=rel_db, width=int(signalbar_width))
                        if bar:
                            print(f"  rel={rel_db:.1f}dB {bar}")
                    s_est = estimate_s_units(rel_db=rel_db, s1_db=s1_db, db_per_s=db_per_s)
                    if step_best is None or rel_db > float(step_best["rel_db"]):
                        step_best = {
                            "center_freq_hz": float(frame.center_freq_hz),
                            "span_hz": float(frame.span_hz),
                            "freq_mhz": float(det.freq_mhz),
                            "s_est": float(s_est),
                            "rel_db": float(rel_db),
                            "width_hz": float(det.width_hz),
                            "width_bins": int(det.width_bins),
                            "type_guess": str(det.type_guess),
                            "candidate_type": str(det.candidate_type),
                            "bandplan": det.bandplan,
                        }
                    if overall_best is None or rel_db > float(overall_best["rel_db"]):
                        overall_best = {
                            "freq_mhz": float(det.freq_mhz),
                            "s_est": float(s_est),
                            "rel_db": float(rel_db),
                            "width_hz": float(det.width_hz),
                            "width_bins": int(det.width_bins),
                            "type_guess": str(det.type_guess),
                            "candidate_type": str(det.candidate_type),
                            "bandplan": det.bandplan,
                        }

                    # Track top-N strongest frequencies sweep-wide (quantized to de-dup).
                    if s_est >= float(min_s):
                        if float(min_width_hz) > 0 and float(det.width_hz) < float(min_width_hz):
                            continue
                        qhz = float(top_quantize_hz) if float(top_quantize_hz) > 0 else float(cache_quantize_hz)
                        q = int(round(float(det.freq_mhz * 1e6) / qhz))
                        prev = top_map.get(q)
                        if prev is None or rel_db > float(prev["rel_db"]):
                            top_map[q] = {
                                "freq_mhz": float(det.freq_mhz),
                                "s_est": float(s_est),
                                "rel_db": float(rel_db),
                                "width_hz": float(det.width_hz),
                                "width_bins": int(det.width_bins),
                                "type_guess": str(det.type_guess),
                                "candidate_type": str(det.candidate_type),
                                "bandplan": det.bandplan,
                            }

                # Optional SSB/bandpower detection: evaluate a wide window around the
                # current tuned center. Report the *center frequency* for activity.
                if bool(ssb_detect):
                    freq0 = float(frame.center_freq_hz)
                    rel_bp, width_bins_bp, width_hz_bp = _bandpower_rel_db(
                        frame=frame,
                        noise_floor=float(noise),
                        center_hz=float(freq0),
                        width_hz=2400.0,
                    )
                    if float(min_width_hz) > 0 and float(width_hz_bp) < float(min_width_hz):
                        rel_bp = 0.0
                    if rel_bp >= float(threshold_db):
                        bp = bandplan_label(float(freq0), region=bandplan_region)
                        if (not phone_only) or bp == "Phone":
                            rel_db = float(rel_bp)
                            s_est = estimate_s_units(rel_db=rel_db, s1_db=s1_db, db_per_s=db_per_s)
                            if s_est >= float(min_s):
                                qhz = float(top_quantize_hz) if float(top_quantize_hz) > 0 else float(cache_quantize_hz)
                                q = int(round(float(freq0) / qhz))
                                prev = top_map.get(q)
                                if prev is None or rel_db > float(prev["rel_db"]):
                                    top_map[q] = {
                                        "freq_mhz": float(freq0 / 1e6),
                                        "s_est": float(s_est),
                                        "rel_db": float(rel_db),
                                        "width_hz": float(width_hz_bp),
                                        "width_bins": int(width_bins_bp),
                                        "type_guess": combine_type_hints(
                                            width_guess="ssb_bandpower",
                                            bandplan_label=bp,
                                        ),
                                        "candidate_type": classify_candidate_type(
                                            width_hz=float(width_hz_bp),
                                            type_guess=combine_type_hints(
                                                width_guess="ssb_bandpower",
                                                bandplan_label=bp,
                                            ),
                                            bandplan_label=bp,
                                        ),
                                        "bandplan": bp,
                                    }

                    if evt_f is not None:
                        if s_est >= float(min_s):
                            evt = {
                                "t_unix": det.t_unix,
                                "frame_index": det.frame_index,
                                "freq_mhz": det.freq_mhz,
                                "center_freq_hz": frame.center_freq_hz,
                                "span_hz": frame.span_hz,
                                "width_bins": det.width_bins,
                                "width_hz": det.width_hz,
                                "type_guess": det.type_guess,
                                "candidate_type": det.candidate_type,
                                "bandplan": det.bandplan,
                                "peak_power": det.peak_power,
                                "noise_floor": det.noise_floor,
                                "rel_db": rel_db,
                                "s_est": s_est,
                                "occ_bw_hz": det.occ_bw_hz,
                                "occ_frac": det.occ_frac,
                                "voice_score": det.voice_score,
                                "narrow_peak_count": det.narrow_peak_count,
                                "narrow_peak_span_hz": det.narrow_peak_span_hz,
                                "keying_score": det.keying_score,
                                "steady_tone_score": det.steady_tone_score,
                                "freq_stability_hz": det.freq_stability_hz,
                                "envelope_variance": det.envelope_variance,
                                "speech_envelope_score": det.speech_envelope_score,
                                "sweep_score": det.sweep_score,
                                "centroid_drift_hz": det.centroid_drift_hz,
                                "observed_frames": det.observed_frames,
                                "active_fraction": det.active_fraction,
                                "cadence_score": det.cadence_score,
                            }
                            evt_f.write(json.dumps(evt, sort_keys=True) + "\n")
                            evt_f.flush()

                    if record and not did_record:
                        did_record = True
                        try:
                            print(
                                f"RECORD starting {record_seconds}s @ {det.freq_mhz:.4f} MHz"
                            )
                            run_record(
                                RecordRequest(
                                    host=host,
                                    port=port,
                                    password=password,
                                    user=user,
                                    freq_hz=float(det.freq_mhz) * 1e6,
                                    duration_s=record_seconds,
                                    mode=record_mode,
                                    out_dir=record_out,
                                )
                            )
                            print(f"RECORD done: {record_out}")
                        except RecorderUnavailable as e:
                            print(f"RECORD skipped: {e}")

            retry_start = time.time()
            retries = 0
            while True:
                try:
                    if rx_chan is not None:
                        ok = set_receiver_frequency(
                            host=host,
                            port=port,
                            rx_chan=int(rx_chan),
                            freq_hz=float(cf),
                            password=password,
                            user=user,
                            timeout_s=10.0,
                            hold_s=float(status_hold_s),
                            rx_wait_timeout_s=rx_wait_timeout_s,
                            rx_wait_interval_s=rx_wait_interval_s,
                            rx_wait_max_retries=rx_wait_max_retries,
                            acceptable_rx_chans=acceptable_rx_chans,
                        )
                        if not ok:
                            raise KiwiCampRejected(requested_rx=int(rx_chan), response="tune failed")
                    subscribe_waterfall(
                        host=host,
                        port=port,
                        password=password,
                        user=user,
                        rx_chan=rx_chan,
                        center_freq_hz=cf,
                        span_hz=span_hz,
                        on_frame=on_frame,
                        max_frames=dwell_frames,
                        min_duration_s=float(status_hold_s) if float(status_hold_s) > 0 else None,
                        debug=debug,
                        debug_messages=debug_messages,
                        acceptable_rx_chans=acceptable_rx_chans,
                    )
                    break
                except KiwiCampRejected as e:
                    if rx_chan is None:
                        raise
                    retries += 1
                    elapsed = time.time() - retry_start
                    if int(rx_wait_max_retries) > 0 and retries > int(rx_wait_max_retries):
                        print(f"RX{int(rx_chan)} unavailable after {rx_wait_max_retries} retries ({e})")
                        return 3
                    if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                        print(f"RX{int(rx_chan)} unavailable after {elapsed:.1f}s ({e})")
                        return 3
                    sleep_s = max(0.25, float(rx_wait_interval_s))
                    print(f"RX{int(rx_chan)} unavailable ({e}); retrying in {sleep_s:.1f}s...")
                    time.sleep(sleep_s)

            if step_best is not None:
                step_reports.append(step_best)

            cache.gc()
            if cf >= last_cf:
                break
            cf += step_hz
    finally:
        if out_f is not None:
            out_f.close()
        if evt_f is not None:
            evt_f.close()

        if json_report_path is not None:
            json_report_path.parent.mkdir(parents=True, exist_ok=True)
            report = {
                "type": "sweep_report",
                "min_s": float(min_s),
                "s1_db": float(s1_db),
                "db_per_s": float(db_per_s),
                "overall_peak": overall_best,
                "step_peaks": step_reports,
            }
            json_report_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")

        if json_topn_path is not None:
            json_topn_path.parent.mkdir(parents=True, exist_ok=True)
            n = int(top_n) if int(top_n) > 0 else 5
            unique = sorted(top_map.values(), key=lambda d: float(d["rel_db"]), reverse=True)
            top = unique[:n]
            report = {
                "type": "sweep_top",
                "band": "40m",
                "bandplan_region": str(bandplan_region),
                "start_hz": float(start_hz),
                "end_hz": float(end_hz),
                "span_hz": float(span_hz),
                "min_s": float(min_s),
                "s1_db": float(s1_db),
                "db_per_s": float(db_per_s),
                "unique_count": int(len(unique)),
                "top": [_round_activity_entry(x) for x in top],
            }
            json_topn_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"SWEEP SUMMARY: {len(unique)} freqs >= S{min_s:g} (top {len(top)} written to {json_topn_path})")

        if json_activity_path is not None:
            json_activity_path.parent.mkdir(parents=True, exist_ok=True)
            unique = sorted(top_map.values(), key=lambda d: float(d["rel_db"]), reverse=True)
            report = {
                "type": "sweep_activity",
                "band": "40m",
                "bandplan_region": str(bandplan_region),
                "start_hz": float(start_hz),
                "end_hz": float(end_hz),
                "span_hz": float(span_hz),
                "min_s": float(min_s),
                "top_quantize_hz": float(top_quantize_hz),
                "unique_count": int(len(unique)),
                "activity": [_round_activity_entry(x) for x in unique],
            }
            json_activity_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"WROTE {json_activity_path}")

    return 0
