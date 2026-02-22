from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class Peak:
    bin_low: int
    bin_high: int
    bin_center: float
    peak_power: float


def median(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("median() of empty sequence")
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    a = float(s[mid - 1])
    b = float(s[mid])
    return (a + b) / 2.0


def cluster_peaks(
    power_bins: Sequence[float],
    *,
    threshold: float,
    min_width_bins: int = 1,
) -> list[Peak]:
    peaks: list[Peak] = []

    in_peak = False
    start = 0
    peak_max = float("-inf")
    peak_max_bin = 0

    for i, p in enumerate(power_bins):
        if p > threshold:
            if not in_peak:
                in_peak = True
                start = i
                peak_max = p
                peak_max_bin = i
            else:
                if p > peak_max:
                    peak_max = p
                    peak_max_bin = i
        else:
            if in_peak:
                end = i - 1
                if (end - start + 1) >= min_width_bins:
                    peaks.append(
                        Peak(
                            bin_low=start,
                            bin_high=end,
                            bin_center=float(peak_max_bin),
                            peak_power=float(peak_max),
                        )
                    )
                in_peak = False

    if in_peak:
        end = len(power_bins) - 1
        if (end - start + 1) >= min_width_bins:
            peaks.append(
                Peak(
                    bin_low=start,
                    bin_high=end,
                    bin_center=float(peak_max_bin),
                    peak_power=float(peak_max),
                )
            )

    return peaks


@dataclass
class PersistentPeak:
    bin_center: float
    hits: int = 0
    first_seen_frame: int = -1
    last_seen_frame: int = -1
    peak_power: float = float("-inf")


class PersistenceTracker:
    def __init__(
        self,
        *,
        tolerance_bins: float = 2.0,
        required_hits: int = 3,
        expiry_frames: int = 8,
    ) -> None:
        if required_hits < 1:
            raise ValueError("required_hits must be >= 1")
        self.tolerance_bins = float(tolerance_bins)
        self.required_hits = int(required_hits)
        self.expiry_frames = int(expiry_frames)
        self._tracks: list[PersistentPeak] = []

    def update(self, frame_index: int, peaks: Iterable[Peak]) -> list[PersistentPeak]:
        seen: list[PersistentPeak] = []
        for peak in peaks:
            matched = None
            for t in self._tracks:
                if abs(t.bin_center - peak.bin_center) <= self.tolerance_bins:
                    matched = t
                    break
            if matched is None:
                matched = PersistentPeak(bin_center=peak.bin_center, first_seen_frame=int(frame_index))
                self._tracks.append(matched)

            if matched.last_seen_frame != frame_index:
                matched.hits += 1
                matched.last_seen_frame = frame_index
                matched.peak_power = max(matched.peak_power, peak.peak_power)

            if matched.hits >= self.required_hits:
                seen.append(matched)

        self._tracks = [
            t
            for t in self._tracks
            if (frame_index - t.last_seen_frame) <= self.expiry_frames
        ]

        return seen


def detect_peaks_with_noise_floor(
    power_bins: Sequence[float],
    *,
    threshold_db: float,
    min_width_bins: int = 1,
) -> tuple[float, list[Peak]]:
    noise = median(power_bins)
    threshold = noise + float(threshold_db)
    return noise, cluster_peaks(
        power_bins, threshold=threshold, min_width_bins=min_width_bins
    )
