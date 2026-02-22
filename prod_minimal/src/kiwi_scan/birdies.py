from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


@dataclass
class BirdieMaskConfig:
    # Bucket size for offset quantization (Hz)
    bucket_hz: float = 10.0
    # Matching tolerance when applying a mask (Hz)
    tolerance_hz: float = 25.0
    # Minimum number of different bands an offset must appear on
    min_bands: int = 3
    # Require each contributing band to have at least this many frames observed
    min_band_frames: int = 20
    # Within a band, the offset must appear in at least this fraction of frames
    # to be considered a stable birdie on that band.
    min_band_occupancy: float = 0.55


class BirdieMask:
    """Learns stable, fixed-frequency-offset tones (birdies/spurs) across bands.

    The goal is to suppress false 'activity' caused by local interference / DSP
    artifacts that show up as persistent peaks at fixed offsets from the tuned
    center.

    We only promote an offset into the global mask if it appears in many frames
    on multiple bands (cross-band criterion), which helps avoid masking real FT8
    activity that is typically band-specific and time-varying.
    """

    def __init__(
        self,
        *,
        config: Optional[BirdieMaskConfig] = None,
        state_path: Optional[Path] = None,
    ) -> None:
        self.config = config or BirdieMaskConfig()
        self.state_path = state_path

        # band -> total frames observed
        self._frames_by_band: Dict[str, int] = {}
        # band -> bucket -> frames where seen
        self._seen_by_band: Dict[str, Dict[int, int]] = {}
        # cached mask offsets (Hz)
        self._mask_offsets_hz: List[float] = []

        if self.state_path is not None:
            self._load(self.state_path)

    @staticmethod
    def _bucket(offset_hz: float, bucket_hz: float) -> int:
        if bucket_hz <= 0:
            return int(round(offset_hz))
        return int(round(float(offset_hz) / float(bucket_hz)))

    @staticmethod
    def _bucket_center(bucket: int, bucket_hz: float) -> float:
        return float(bucket) * float(bucket_hz)

    def observe_frame(
        self,
        *,
        band: str,
        span_hz: float,
        power_bins: Sequence[float],
        persistent_bin_centers: Iterable[float],
    ) -> None:
        """Observe a single frame's persistent peaks (by bin_center) for a band."""
        n_bins = len(power_bins)
        if n_bins <= 1:
            return

        self._frames_by_band[band] = int(self._frames_by_band.get(band, 0) + 1)
        band_seen = self._seen_by_band.setdefault(band, {})

        bin_hz = float(span_hz) / float(max(1, n_bins - 1))
        mid_bin = (float(n_bins) - 1.0) / 2.0

        buckets_seen_this_frame: Set[int] = set()
        for c in persistent_bin_centers:
            try:
                offset_hz = (float(c) - float(mid_bin)) * float(bin_hz)
            except Exception:
                continue
            b = self._bucket(offset_hz, self.config.bucket_hz)
            buckets_seen_this_frame.add(int(b))

        for b in buckets_seen_this_frame:
            band_seen[b] = int(band_seen.get(b, 0) + 1)

    def recompute_mask(self) -> List[float]:
        cfg = self.config
        chosen: List[float] = []

        # Find all buckets we've ever seen.
        all_buckets: Set[int] = set()
        for per_band in self._seen_by_band.values():
            all_buckets.update(per_band.keys())

        for bucket in sorted(all_buckets):
            bands_ok = 0
            for band, frames_total in self._frames_by_band.items():
                if int(frames_total) < int(cfg.min_band_frames):
                    continue
                seen = self._seen_by_band.get(band, {}).get(bucket, 0)
                occ = float(seen) / float(max(1, int(frames_total)))
                if occ >= float(cfg.min_band_occupancy):
                    bands_ok += 1

            if bands_ok >= int(cfg.min_bands):
                chosen.append(self._bucket_center(bucket, cfg.bucket_hz))

        self._mask_offsets_hz = chosen
        return list(self._mask_offsets_hz)

    @property
    def mask_offsets_hz(self) -> List[float]:
        return list(self._mask_offsets_hz)

    def is_masked(self, offset_hz: float) -> bool:
        tol = float(self.config.tolerance_hz)
        for m in self._mask_offsets_hz:
            if abs(float(offset_hz) - float(m)) <= tol:
                return True
        return False

    def save(self) -> None:
        if self.state_path is None:
            return
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        data = {
            "config": {
                "bucket_hz": float(self.config.bucket_hz),
                "tolerance_hz": float(self.config.tolerance_hz),
                "min_bands": int(self.config.min_bands),
                "min_band_frames": int(self.config.min_band_frames),
                "min_band_occupancy": float(self.config.min_band_occupancy),
            },
            "frames_by_band": dict(self._frames_by_band),
            "seen_by_band": {b: {str(k): int(v) for k, v in d.items()} for b, d in self._seen_by_band.items()},
            "mask_offsets_hz": list(self._mask_offsets_hz),
        }
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        tmp.replace(self.state_path)

    def _load(self, path: Path) -> None:
        try:
            if not path.exists():
                return
            data = json.loads(path.read_text(encoding="utf-8"))
            cfg = data.get("config") or {}
            self.config = BirdieMaskConfig(
                bucket_hz=float(cfg.get("bucket_hz", self.config.bucket_hz)),
                tolerance_hz=float(cfg.get("tolerance_hz", self.config.tolerance_hz)),
                min_bands=int(cfg.get("min_bands", self.config.min_bands)),
                min_band_frames=int(cfg.get("min_band_frames", self.config.min_band_frames)),
                min_band_occupancy=float(cfg.get("min_band_occupancy", self.config.min_band_occupancy)),
            )
            self._frames_by_band = {str(k): int(v) for k, v in (data.get("frames_by_band") or {}).items()}
            seen_raw = data.get("seen_by_band") or {}
            seen_by_band: Dict[str, Dict[int, int]] = {}
            for band, d in seen_raw.items():
                try:
                    seen_by_band[str(band)] = {int(k): int(v) for k, v in (d or {}).items()}
                except Exception:
                    continue
            self._seen_by_band = seen_by_band
            self._mask_offsets_hz = [float(x) for x in (data.get("mask_offsets_hz") or [])]
        except Exception:
            # Corrupt/partial state shouldn't break scanning.
            return
