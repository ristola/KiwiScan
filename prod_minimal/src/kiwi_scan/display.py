from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return float(min(values))
    if q >= 1:
        return float(max(values))
    s = sorted(float(v) for v in values)
    pos = q * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    frac = pos - lo
    return float(s[lo] * (1.0 - frac) + s[hi] * frac)


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(float(v) for v in values) / float(len(values)))


@dataclass(frozen=True)
class FramePeak:
    freq_hz: float
    power: float
    above_noise: float


def top_peaks(
    *,
    power_bins: Sequence[float],
    bin_to_hz_fn,
    noise: float,
    n: int = 5,
    min_separation_bins: int = 3,
) -> list[FramePeak]:
    idxs = sorted(range(len(power_bins)), key=lambda i: power_bins[i], reverse=True)
    picked: list[int] = []
    for i in idxs:
        if any(abs(i - j) < min_separation_bins for j in picked):
            continue
        picked.append(i)
        if len(picked) >= n:
            break

    out: list[FramePeak] = []
    for i in picked:
        p = float(power_bins[i])
        out.append(
            FramePeak(
                freq_hz=float(bin_to_hz_fn(float(i))),
                power=p,
                above_noise=p - float(noise),
            )
        )
    return out


_BARS_ASCII = " .:-=+*#%@"  # low -> high
_BARS_BLOCK = " ▁▂▃▄▅▆▇█"    # low -> high


def _supports_ansi(*, enable: bool) -> bool:
    return bool(enable)


def _ansi_color(rel_db: float, *, clip_db: float) -> str:
    # 256-color ramp: green -> yellow -> red
    if clip_db <= 0:
        return "\x1b[0m"
    t = max(0.0, min(1.0, float(rel_db) / float(clip_db)))
    if t < 0.5:
        return "\x1b[38;5;46m"   # green
    if t < 0.8:
        return "\x1b[38;5;226m"  # yellow
    return "\x1b[38;5;196m"      # red


def sparkline(
    power_bins: Sequence[float],
    *,
    noise: float,
    width: int = 80,
    clip_db: float = 25.0,
    charset: str = "block",
    color: bool = False,
    bucket: str = "p90",
    auto_clip: bool = False,
) -> str:
    if width < 8:
        width = 8
    n = len(power_bins)
    if n == 0:
        return ""

    bars = _BARS_BLOCK if charset.lower() in ("block", "blocks", "unicode") else _BARS_ASCII
    use_ansi = _supports_ansi(enable=color)

    bmode = bucket.lower().strip()

    # Downsample bins into `width` buckets.
    step = n / float(width)
    bucket_vals: list[float] = []
    chars: list[str] = []
    for x in range(width):
        lo = int(x * step)
        hi = int((x + 1) * step)
        if hi <= lo:
            hi = min(lo + 1, n)
        bucket = power_bins[lo:hi]
        if not bucket:
            v = float(noise)
        else:
            if bmode == "max":
                v = float(max(bucket))
            elif bmode in ("mean", "avg"):
                v = _mean(bucket)
            elif bmode in ("p50", "median"):
                v = _quantile(bucket, 0.50)
            elif bmode == "p75":
                v = _quantile(bucket, 0.75)
            else:
                # default p90
                v = _quantile(bucket, 0.90)

        rel = v - float(noise)
        bucket_vals.append(rel)
        if rel < 0:
            rel = 0.0
        # clip later once we know auto scale
        chars.append((rel,))

    # Decide clipping level.
    use_clip_db = float(clip_db)
    if auto_clip:
        # Use a high quantile of per-bucket values so a single giant carrier
        # doesn't force the entire line to full scale.
        use_clip_db = max(6.0, _quantile([max(0.0, float(v)) for v in bucket_vals], 0.85))

    # Convert to bars.
    out: list[str] = []
    for (rel,) in chars:
        rel = float(rel)
        if rel < 0:
            rel = 0.0
        if rel > use_clip_db:
            rel = use_clip_db
        t = rel / use_clip_db if use_clip_db > 0 else 0.0
        idx = int(round(t * (len(bars) - 1)))
        ch = bars[idx]
        if use_ansi:
            ch = _ansi_color(rel, clip_db=use_clip_db) + ch
        out.append(ch)

    if use_ansi:
        return "".join(out) + "\x1b[0m"
    return "".join(out)


def span_bar(
    power_bins: Sequence[float],
    *,
    width: int = 80,
    scale: str = "frame",
    color: bool = False,
    marker_index: int | None = None,
) -> tuple[str, float]:
    """Render a single-line bar spanning the current frequency window.

    Returns (bar_text, strength_0_1) where strength is a rough per-frame level.
    This is meant to provide a stable shape display even when the absolute
    waterfall scale isn't calibrated.
    """

    if width < 8:
        width = 8
    n = len(power_bins)
    if n == 0:
        return "", 0.0

    # Normalize values.
    p10 = _quantile(power_bins, 0.10)
    p50 = _quantile(power_bins, 0.50)
    p95 = _quantile(power_bins, 0.95)
    denom = max(1e-9, (p95 - p10))

    # Strength estimate: how far p50 is above p10 within p10..p95.
    strength = max(0.0, min(1.0, (p50 - p10) / denom))

    # Downsample bins into `width` buckets using p90 to preserve narrow peaks.
    step = n / float(width)
    vals: list[float] = []
    for x in range(width):
        lo = int(x * step)
        hi = int((x + 1) * step)
        if hi <= lo:
            hi = min(lo + 1, n)
        bucket = power_bins[lo:hi]
        v = _quantile(bucket, 0.90) if bucket else float(p10)
        vals.append(float(v))

    # Scale modes:
    # - frame: use p10..p95 of the whole frame (stable contrast)
    # - raw: use min..max of the downsampled values
    smode = scale.lower().strip()
    if smode == "raw":
        vmin = min(vals)
        vmax = max(vals)
    else:
        vmin = float(p10)
        vmax = float(p95)
    rng = max(1e-9, vmax - vmin)

    # 9-level density ramp. We avoid spaces so the bar reads clearly.
    ramp = "·░▒▓█"
    # Expand to more granularity by repeating mid levels.
    ramp = "·░░▒▒▓▓█"

    use_ansi = _supports_ansi(enable=color)
    out: list[str] = []
    for i, v in enumerate(vals):
        t = (float(v) - vmin) / rng
        t = max(0.0, min(1.0, t))
        idx = int(round(t * (len(ramp) - 1)))
        ch = ramp[idx]
        if marker_index is not None and i == marker_index:
            ch = "|"
        if use_ansi:
            # Map to green/yellow/red by the same t.
            if t < 0.5:
                ch = "\x1b[38;5;46m" + ch
            elif t < 0.8:
                ch = "\x1b[38;5;226m" + ch
            else:
                ch = "\x1b[38;5;196m" + ch
        out.append(ch)

    bar = "".join(out)
    if use_ansi:
        bar += "\x1b[0m"
    return bar, float(strength)
