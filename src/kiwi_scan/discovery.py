from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .detect import PersistenceTracker, detect_peaks_with_noise_floor
from .kiwi_waterfall import (
    KiwiCampRejected,
    KiwiClientUnavailable,
    subscribe_waterfall,
)


logger = logging.getLogger(__name__)


def dbm_to_s_units(dbm: float) -> float:
    # S9 = -73 dBm, 6 dB / S-unit => S0 = -127 dBm.
    # Return a continuous (not rounded) S value.
    try:
        s = (float(dbm) + 127.0) / 6.0
    except Exception:
        return 0.0
    if s < 0.0:
        return 0.0
    return float(s)


FT8_WATERHOLES: Dict[str, float] = {
    "10m": 28.074e6,
    "12m": 24.915e6,
    "15m": 21.074e6,
    "17m": 18.100e6,
    "20m": 14.074e6,
    "40m": 7.074e6,
    "80m": 3.573e6,
    "160m": 1.840e6,
}


@dataclass
class FT8DiscoveryResult:
    band: str
    freq_hz: float
    frames_sampled: int
    hits: int
    score: float
    camp_ok: Optional[bool] = None
    camp_rx: Optional[int] = None
    # Debug/inspection metrics derived from waterfall bins
    avg_noise_floor_dbm: Optional[float] = None
    max_peak_rel_db: Optional[float] = None
    avg_peaks_per_frame: Optional[float] = None
    avg_persistent_per_frame: Optional[float] = None
    # S-meter style derived metrics.
    # Note: Kiwi waterfall bins are only approximately dBm; we therefore derive
    # S from the *noise floor* (median) plus an optional user calibration offset.
    # Mapping used: S9=-73 dBm, 6 dB per S unit (so S0=-127 dBm).
    avg_noise_s: Optional[float] = None
    # A more responsive “signal present” proxy based on the 95th percentile bin.
    # This tends to vary across bands more than the noise-floor S, and is less
    # sensitive to a single extreme bin than max_peak_dbm.
    p95_rel_db: Optional[float] = None
    p95_dbm: Optional[float] = None
    p95_s: Optional[float] = None
    # Debug only (often dominated by spurs/scaling; do not treat as S-meter truth)
    max_peak_dbm: Optional[float] = None
    max_peak_s: Optional[float] = None
    # Debug metrics for frames that were counted as hits
    hit_persistent_span_hz_avg: Optional[float] = None
    hit_persistent_span_hz_max: Optional[float] = None
    hit_persistent_offsets_hz_sample: Optional[List[float]] = None


class DiscoveryWorker:
    """Simple FT8 presence discovery worker.

    This tunes each FT8 watering-hole frequency for a short dwell and
        measures the fraction of waterfall frames that contain persistent
        above-noise peaks.

        For each frame we:
            1) estimate the noise floor as the median power
            2) detect contiguous peaks above (noise + threshold_db)
            3) track peaks that persist across frames (helps reject random noise)
            4) count the frame as a hit if it has enough persistent peaks

        The final score is hits / frames_seen in [0.0, 1.0].
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8073,
        password: Optional[str] = None,
        user: str = "kiwi-discover",
        rx_chan: Optional[int] = None,
        stop_event: Optional[threading.Event] = None,
        dwell_s: float = 30.0,
        span_hz: float = 3000.0,
        threshold_db: float = 15.0,
        frames_per_second: float = 2.0,
        s_meter_offset_db: float = 0.0,
        # Fast scan: if the band appears “closed” by a quick S estimate, stop
        # early instead of dwelling for the full dwell_s.
        fast_scan_enabled: bool = False,
        fast_scan_s_threshold: float = 3.0,
        fast_scan_min_frames: int = 2,
        fast_scan_min_duration_s: float = 1.5,
        debug: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.user = user
        self.rx_chan = rx_chan
        self.stop_event = stop_event
        self.dwell_s = float(dwell_s)
        self.span_hz = float(span_hz)
        self.threshold_db = float(threshold_db)
        self.frames_per_second = float(frames_per_second)
        # Calibration offset applied when converting noise-floor dBm to S-units.
        # Use this to line up our derived S values with the Kiwi's on-screen S-meter.
        self.s_meter_offset_db = float(s_meter_offset_db)
        self.fast_scan_enabled = bool(fast_scan_enabled)
        self.fast_scan_s_threshold = float(fast_scan_s_threshold)
        self.fast_scan_min_frames = int(fast_scan_min_frames)
        self.fast_scan_min_duration_s = float(fast_scan_min_duration_s)
        self.debug = bool(debug)

        # Detector constants
        self._tracker_tolerance_bins = 2.0
        # Require more persistence to reduce false positives from random noise.
        self._tracker_required_hits = 3
        self._tracker_expiry_frames = 8
        # With birdie filtering enabled, we can lower this so sparse FT8 activity
        # (a few signals) still counts.
        self._min_persistent_peaks = 3
        self._min_peak_width_bins = 2
        # Peaks that appear in nearly every frame are likely carriers/birdies.
        # Filter them out before counting hits.
        self._max_persistent_occupancy = 0.85
        # Require that persistent peaks are spread across a minimum bandwidth.
        # This helps reject a handful of clustered birdies/spurs that can look
        # "active" even when the FT8 watering hole is otherwise dead.
        self._min_persistent_span_hz = 120.0

    def _score_frames(self, frames: Sequence[Sequence[float]], *, threshold_db: float) -> Tuple[int, int, float]:
        """Run the same persistence detector used in measure_freq over a list of power-bin frames."""
        tracker = PersistenceTracker(
            tolerance_bins=self._tracker_tolerance_bins,
            required_hits=self._tracker_required_hits,
            expiry_frames=self._tracker_expiry_frames,
        )
        hit_frames = 0
        frames_seen = 0

        for i, power_bins in enumerate(frames):
            frames_seen += 1
            try:
                _noise, peaks = detect_peaks_with_noise_floor(
                    power_bins,
                    threshold_db=float(threshold_db),
                    min_width_bins=self._min_peak_width_bins,
                )
            except Exception:
                continue

            try:
                persistent = tracker.update(i, peaks)
                # Filter out always-on peaks (birdies) using occupancy.
                try:
                    persistent = [
                        p
                        for p in persistent
                        if (float(p.hits) / float(max(1, int(i) - int(getattr(p, "first_seen_frame", i)) + 1)))
                        <= float(self._max_persistent_occupancy)
                    ]
                except Exception:
                    pass

                if len(persistent) >= self._min_persistent_peaks:
                    # Keep calibration consistent with live scanning logic by
                    # requiring a minimum frequency span for the persistent peaks.
                    try:
                        n_bins = len(power_bins)
                        bin_hz = float(self.span_hz) / float(max(1, n_bins - 1))
                        centers = [float(p.bin_center) for p in persistent]
                        span_hz = (max(centers) - min(centers)) * bin_hz if centers else 0.0
                    except Exception:
                        span_hz = float("inf")

                    if float(span_hz) >= float(self._min_persistent_span_hz):
                        hit_frames += 1
            except Exception:
                continue

        score = float(hit_frames) / float(max(1, frames_seen))
        return hit_frames, frames_seen, score

    def collect_frames(self, *, freq_hz: float, span_hz: Optional[float] = None, duration_s: Optional[float] = None) -> List[List[float]]:
        """Collect raw waterfall frames (power bins) for later analysis/calibration."""
        if span_hz is None:
            span_hz = self.span_hz
        if duration_s is None:
            duration_s = self.dwell_s

        frames: List[List[float]] = []

        def _on_frame(frame) -> None:
            try:
                frames.append(list(frame.power_bins))
            except Exception:
                pass

        requested_rx = int(self.rx_chan) if self.rx_chan is not None else None

        def _should_stop() -> bool:
            return bool(self.stop_event is not None and self.stop_event.is_set())

        # RX0-only policy: do NOT fall back to a server-selected receiver here.
        # Instead, retry camped RX0 for a short bounded period. This avoids
        # calibrate_all accidentally occupying RX1 and then leaving RX0 busy.
        if requested_rx is None:
            # If no RX requested, just subscribe once.
            subscribe_waterfall(
                host=self.host,
                port=self.port,
                password=self.password,
                user=self.user,
                rx_chan=None,
                center_freq_hz=float(freq_hz),
                span_hz=float(span_hz),
                on_frame=_on_frame,
                should_stop=_should_stop,
                max_frames=None,
                max_duration_s=float(duration_s),
                camp_timeout_s=None,
                on_camp=None,
                debug=self.debug,
                debug_messages=self.debug,
            )
            return frames

        max_wait_s = float(min(8.0, max(2.0, float(duration_s))))
        deadline = time.time() + max_wait_s
        attempts = 0
        while time.time() < deadline and not _should_stop():
            attempts += 1
            try:
                subscribe_waterfall(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    user=self.user,
                    rx_chan=int(requested_rx),
                    center_freq_hz=float(freq_hz),
                    span_hz=float(span_hz),
                    on_frame=_on_frame,
                    should_stop=_should_stop,
                    max_frames=None,
                    max_duration_s=float(duration_s),
                    camp_timeout_s=max_wait_s,
                    on_camp=None,
                    debug=self.debug,
                    debug_messages=self.debug,
                )
                return frames
            except KiwiClientUnavailable:
                raise
            except KiwiCampRejected:
                try:
                    time.sleep(min(1.0, 0.25 * attempts))
                except Exception:
                    pass
                continue
            except Exception:
                try:
                    time.sleep(min(1.0, 0.25 * attempts))
                except Exception:
                    pass
                continue

        return frames

    def calibrate_threshold(
        self,
        *,
        freq_hz: float,
        duration_s: Optional[float] = None,
        target_score: float = 0.05,
        threshold_min: float = 0.0,
        threshold_max: float = 60.0,
        step_db: float = 1.0,
    ) -> Dict[str, object]:
        """Suggest a threshold_db that yields a low score on the sampled frames.

        This runs the detector across a sweep of thresholds and chooses the lowest
        threshold where score <= target_score.
        """
        if duration_s is None:
            duration_s = self.dwell_s

        frames = self.collect_frames(freq_hz=float(freq_hz), duration_s=float(duration_s))
        if not frames:
            return {
                "ok": False,
                "reason": "no_frames",
                "frames": 0,
                "suggested_threshold_db": None,
                "target_score": float(target_score),
                "curve": [],
            }

        curve = []
        suggested: Optional[float] = None
        t = float(threshold_min)
        while t <= float(threshold_max) + 1e-9:
            hits, frames_seen, score = self._score_frames(frames, threshold_db=float(t))
            curve.append({"threshold_db": float(t), "score": float(score), "hits": int(hits), "frames": int(frames_seen)})
            if suggested is None and float(score) <= float(target_score):
                suggested = float(t)
            t += float(step_db)

        if suggested is None:
            suggested = float(threshold_max)

        return {
            "ok": True,
            "frames": int(len(frames)),
            "duration_s": float(duration_s),
            "suggested_threshold_db": float(suggested),
            "target_score": float(target_score),
            "curve": curve,
        }

    def measure_freq(self, band: str, freq_hz: float) -> FT8DiscoveryResult:
        frames_needed = max(1, int(math.ceil(self.dwell_s * self.frames_per_second)))

        frames_seen = 0
        hit_frames = 0
        camp_ok: Optional[bool] = None
        camp_rx: Optional[int] = None
        start_t = time.time()

        # Debug stats to help explain why we count hits.
        noise_sum = 0.0
        noise_n = 0
        peaks_total = 0
        persistent_total = 0
        max_rel_db = float("-inf")
        max_peak_power_dbm = float("-inf")
        # Percentile-based signal proxy
        p95_power_dbm = float("-inf")
        p95_rel_db = float("-inf")
        p95_s_max: Optional[float] = None
        ended_early = False
        hit_span_sum_hz = 0.0
        hit_span_n = 0
        hit_span_max_hz = float("-inf")
        hit_offsets_sample: Optional[List[float]] = None

        tracker = PersistenceTracker(
            tolerance_bins=self._tracker_tolerance_bins,
            required_hits=self._tracker_required_hits,
            expiry_frames=self._tracker_expiry_frames,
        )

        def _on_frame(frame) -> None:  # frame is WaterfallFrame
            nonlocal frames_seen, hit_frames
            nonlocal noise_sum, noise_n, peaks_total, persistent_total, max_rel_db, max_peak_power_dbm
            nonlocal hit_span_sum_hz, hit_span_n, hit_span_max_hz, hit_offsets_sample
            nonlocal p95_power_dbm, p95_s_max, ended_early
            frames_seen += 1
            try:
                noise, peaks = detect_peaks_with_noise_floor(
                    frame.power_bins,
                    threshold_db=float(self.threshold_db),
                    min_width_bins=self._min_peak_width_bins,
                )
            except Exception:
                return

            # Track a percentile-based “signal present” proxy.
            # This does not depend on peak detection thresholds.
            try:
                bins = frame.power_bins
                if bins:
                    sb = sorted(float(x) for x in bins)
                    i95 = int(round(0.95 * float(len(sb) - 1)))
                    i95 = max(0, min(int(i95), len(sb) - 1))
                    p95 = float(sb[i95])
                    p95_power_dbm = max(float(p95_power_dbm), p95)
                    try:
                        p95_rel_db = max(float(p95_rel_db), float(p95) - float(noise))
                    except Exception:
                        pass
                    try:
                        p95_s_max = dbm_to_s_units(float(p95_power_dbm) + float(self.s_meter_offset_db))
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                noise_sum += float(noise)
                noise_n += 1
                peaks_total += int(len(peaks))
                for p in peaks:
                    try:
                        max_rel_db = max(float(max_rel_db), float(p.peak_power) - float(noise))
                        max_peak_power_dbm = max(float(max_peak_power_dbm), float(p.peak_power))
                    except Exception:
                        pass
            except Exception:
                pass

            # Fast-scan early exit: after a couple frames, if the signal proxy
            # never rises above the configured S threshold, stop dwelling.
            try:
                if self.fast_scan_enabled and frames_seen >= max(1, int(self.fast_scan_min_frames)):
                    elapsed_s = float(time.time() - start_t)
                    if elapsed_s >= float(self.fast_scan_min_duration_s):
                        if p95_s_max is not None and float(p95_s_max) < float(self.fast_scan_s_threshold):
                            ended_early = True
            except Exception:
                pass

            try:
                frame_index = frames_seen - 1
                persistent = tracker.update(frame_index, peaks)
                # Filter out always-on peaks (birdies) using occupancy.
                try:
                    persistent = [
                        p
                        for p in persistent
                        if (float(p.hits) / float(max(1, int(frame_index) - int(getattr(p, "first_seen_frame", frame_index)) + 1)))
                        <= float(self._max_persistent_occupancy)
                    ]
                except Exception:
                    pass
                try:
                    persistent_total += int(len(persistent))
                except Exception:
                    pass
                if len(persistent) >= self._min_persistent_peaks:
                    # Require the persistent peaks to cover a minimum span.
                    # FT8 activity tends to create multiple peaks across a
                    # wider portion of the 3 kHz window, not just a tight cluster.
                    try:
                        n_bins = len(frame.power_bins)
                        bin_hz = float(self.span_hz) / float(max(1, n_bins - 1))
                        centers = [float(p.bin_center) for p in persistent]
                        span_hz = (max(centers) - min(centers)) * bin_hz if centers else 0.0
                    except Exception:
                        span_hz = float("inf")

                    if float(span_hz) >= float(self._min_persistent_span_hz):
                        hit_frames += 1
                        try:
                            hit_span_sum_hz += float(span_hz)
                            hit_span_n += 1
                            hit_span_max_hz = max(float(hit_span_max_hz), float(span_hz))

                            # Capture a small, human-friendly sample of persistent peak offsets
                            # for the last hit frame. Offsets are relative to the tuned center.
                            mid_bin = (float(n_bins) - 1.0) / 2.0
                            offsets = sorted(((c - mid_bin) * float(bin_hz)) for c in centers)
                            hit_offsets_sample = [float(x) for x in offsets[:12]]
                        except Exception:
                            pass
            except Exception:
                return

        def _on_assigned_rx(ok: bool, rx: int) -> None:
            nonlocal camp_ok, camp_rx
            # subscribe_waterfall now calls this on rx assignment (not "camp").
            camp_ok = bool(ok)
            camp_rx = int(rx)

        attempts = 0
        max_attempts = 6
        while attempts < max_attempts and not (self.stop_event is not None and self.stop_event.is_set()):
            attempts += 1
            try:
                subscribe_waterfall(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    user=self.user,
                    # Always request the configured RX (RX0) so the Kiwi never
                    # silently drifts to RX1 when RX0 is temporarily busy.
                    rx_chan=int(self.rx_chan) if self.rx_chan is not None else None,
                    center_freq_hz=float(freq_hz),
                    span_hz=float(self.span_hz),
                    on_frame=_on_frame,
                    should_stop=(
                        lambda: bool(
                            (self.stop_event is not None and self.stop_event.is_set())
                            or (self.fast_scan_enabled and ended_early)
                        )
                    ),
                    # Use duration-based stop to keep the Kiwi camp active for the
                    # full dwell period rather than closing early based on frame
                    # counts (frames may arrive faster than expected).
                    max_frames=None,
                    max_duration_s=self.dwell_s,
                    camp_timeout_s=min(12.0, max(4.0, float(self.dwell_s) / 2.0)),
                    on_camp=_on_assigned_rx,
                    debug=self.debug,
                    debug_messages=self.debug,
                )
                # If we got no frames (e.g., transient camp/connection quirk),
                # retry a few times before giving up. This avoids “skipping” bands.
                if frames_seen == 0 and not ended_early:
                    try:
                        time.sleep(min(1.0, 0.25 * attempts))
                    except Exception:
                        pass
                    continue
                break
            except KiwiClientUnavailable:
                raise
            except (KiwiCampRejected,) as e:
                if self.debug:
                    logger.info(
                        "measure_freq: camp rejected/disconnected on attempt %d for %s (%s); retrying",
                        attempts,
                        band,
                        e,
                    )
                try:
                    time.sleep(min(2.0, 0.5 * attempts))
                except Exception:
                    pass
                continue
            except Exception as e:
                name = type(e).__name__
                # Treat these as transient connection/camp issues.
                if name in {"KiwiCampError", "TimeoutError"} or "timeout" in str(e).lower():
                    if self.debug:
                        logger.info(
                            "measure_freq: transient W/F error on attempt %d for %s (%s); retrying",
                            attempts,
                            band,
                            e,
                        )
                    try:
                        time.sleep(min(2.0, 0.5 * attempts))
                    except Exception:
                        pass
                    continue
                if self.debug:
                    logger.info(
                        "measure_freq: W/F error band=%s attempt=%d err=%s: %s",
                        band,
                        attempts,
                        type(e).__name__,
                        e,
                    )
                break

        # Ensure we waited at least dwell_s seconds when we actually collected
        # frames. Some kiwi clients may return early (or frames arrive faster
        # than expected), so sleep the remaining time to honor the configured
        # dwell. If we failed to collect frames (e.g. camp rejected), don't
        # stall the overall discovery loop.
        elapsed = time.time() - start_t
        if not ended_early and frames_seen > 0 and elapsed < float(self.dwell_s):
            try:
                time.sleep(float(self.dwell_s) - elapsed)
            except Exception:
                pass

        # Score is the fraction of frames that look "active".
        score = float(hit_frames) / float(max(1, frames_seen))

        avg_noise = (noise_sum / float(max(1, noise_n))) if noise_n else None
        max_rel = None if max_rel_db == float("-inf") else float(max_rel_db)
        max_peak_dbm = None if max_peak_power_dbm == float("-inf") else float(max_peak_power_dbm)
        avg_peaks = (float(peaks_total) / float(max(1, frames_seen))) if frames_seen else None
        avg_persist = (float(persistent_total) / float(max(1, frames_seen))) if frames_seen else None
        hit_span_avg = (hit_span_sum_hz / float(max(1, hit_span_n))) if hit_span_n else None
        hit_span_max = None if hit_span_max_hz == float("-inf") else float(hit_span_max_hz)
        # Derive S from the estimated noise floor, not the maximum peak.
        # Max peaks are frequently birdies/spurs and can be wildly misleading.
        avg_noise_s = None
        if avg_noise is not None:
            try:
                avg_noise_s = dbm_to_s_units(float(avg_noise) + float(self.s_meter_offset_db))
            except Exception:
                avg_noise_s = None

        p95_dbm = None if p95_power_dbm == float("-inf") else float(p95_power_dbm)
        p95_rel = None if p95_rel_db == float("-inf") else float(p95_rel_db)
        p95_s = None
        if p95_dbm is not None:
            try:
                p95_s = dbm_to_s_units(float(p95_dbm) + float(self.s_meter_offset_db))
            except Exception:
                p95_s = None

        max_peak_s = None
        if max_peak_dbm is not None:
            try:
                max_peak_s = dbm_to_s_units(float(max_peak_dbm) + float(self.s_meter_offset_db))
            except Exception:
                max_peak_s = None
        return FT8DiscoveryResult(
            band=band,
            freq_hz=float(freq_hz),
            frames_sampled=frames_seen,
            hits=hit_frames,
            score=score,
            camp_ok=camp_ok,
            camp_rx=camp_rx,
            avg_noise_floor_dbm=(None if avg_noise is None else float(avg_noise)),
            max_peak_rel_db=max_rel,
            avg_peaks_per_frame=(None if avg_peaks is None else float(avg_peaks)),
            avg_persistent_per_frame=(None if avg_persist is None else float(avg_persist)),
            avg_noise_s=avg_noise_s,
            p95_rel_db=p95_rel,
            p95_dbm=p95_dbm,
            p95_s=p95_s,
            max_peak_dbm=max_peak_dbm,
            max_peak_s=max_peak_s,
            hit_persistent_span_hz_avg=(None if hit_span_avg is None else float(hit_span_avg)),
            hit_persistent_span_hz_max=hit_span_max,
            hit_persistent_offsets_hz_sample=hit_offsets_sample,
        )

    def discover(self, freqs: Optional[Dict[str, float]] = None) -> List[FT8DiscoveryResult]:
        if freqs is None:
            freqs = FT8_WATERHOLES
        out: List[FT8DiscoveryResult] = []
        for band, f in freqs.items():
            try:
                res = self.measure_freq(band, f)
            except KiwiClientUnavailable:
                raise
            except Exception:
                # on error, emit zeroed result
                res = FT8DiscoveryResult(band=band, freq_hz=float(f), frames_sampled=0, hits=0, score=0.0)
            out.append(res)
        return out


def discover_all_cli() -> None:
    import argparse

    p = argparse.ArgumentParser(description="FT8 band discovery test runner")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=8073, type=int)
    p.add_argument("--dwell", default=30.0, type=float, help="seconds per watering hole")
    p.add_argument("--span", default=3000.0, type=float, help="waterfall span in Hz")
    p.add_argument("--threshold", default=15.0, type=float, help="dB above noise floor to count peaks")
    p.add_argument("--fps", default=2.0, type=float, help="assumed waterfall frames per second")
    p.add_argument("--json", default=False, action="store_true", help="print JSON output")
    args = p.parse_args()

    w = DiscoveryWorker(
        host=args.host,
        port=args.port,
        dwell_s=args.dwell,
        span_hz=args.span,
        threshold_db=args.threshold,
        frames_per_second=args.fps,
    )

    try:
        results = w.discover()
    except KiwiClientUnavailable as e:
        print("Kiwi client unavailable:", e)
        return

    if args.json:
        print(json.dumps([r.__dict__ for r in results], indent=2))
    else:
        for r in results:
            print(f"{r.band:5s} {r.freq_hz/1e6:.4f} MHz frames={r.frames_sampled} hits={r.hits} score={r.score:.2f}")


if __name__ == "__main__":
    discover_all_cli()
