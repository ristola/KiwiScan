from __future__ import annotations

import re
import wave
from pathlib import Path
from typing import Any

import numpy as np


MORSE_CODE: dict[str, str] = {
    ".-": "A",
    "-...": "B",
    "-.-.": "C",
    "-..": "D",
    ".": "E",
    "..-.": "F",
    "--.": "G",
    "....": "H",
    "..": "I",
    ".---": "J",
    "-.-": "K",
    ".-..": "L",
    "--": "M",
    "-.": "N",
    "---": "O",
    ".--.": "P",
    "--.-": "Q",
    ".-.": "R",
    "...": "S",
    "-": "T",
    "..-": "U",
    "...-": "V",
    ".--": "W",
    "-..-": "X",
    "-.--": "Y",
    "--..": "Z",
    "-----": "0",
    ".----": "1",
    "..---": "2",
    "...--": "3",
    "....-": "4",
    ".....": "5",
    "-....": "6",
    "--...": "7",
    "---..": "8",
    "----.": "9",
    ".-.-.-": ".",
    "--..--": ",",
    "..--..": "?",
    "-..-.": "/",
    "-....-": "-",
    "-.--.": "(",
    "-.--.-": ")",
}

_CW_CALLSIGN_RE = re.compile(r"\b(?:[A-Z]{1,2}\d[A-Z0-9]{1,4}|\d[A-Z]{1,4}|[A-Z]{1,3}\d[A-Z]{1,4})\b")
_CW_COMMON_TOKENS = {
    "CQ", "DE", "TEST", "QRZ", "QTH", "RST", "BK", "KN", "AR", "SK", "TU", "QSL",
    "OM", "NAME", "PSE", "AGN", "K", "R", "RR", "73", "5NN", "599", "SOS",
}
_CW_NOISE_HEAVY_LETTERS = set("EISHTMA")


def validate_cw_message(decoded_text: str, *, confidence: float = 0.0) -> dict[str, Any]:
    text = " ".join(str(decoded_text or "").upper().split())
    report: dict[str, Any] = {
        "valid": False,
        "normalized_text": text,
        "reason": "No CW symbols decoded",
        "summary": "CW decode did not produce a message",
        "callsigns": [],
        "common_tokens": [],
    }
    if not text:
        return report

    tokens = [tok for tok in re.split(r"[^A-Z0-9/?]+", text) if tok]
    callsigns = [tok for tok in tokens if _CW_CALLSIGN_RE.fullmatch(tok)]
    common_tokens = [tok for tok in tokens if tok in _CW_COMMON_TOKENS]
    report["callsigns"] = callsigns
    report["common_tokens"] = common_tokens

    compact = text.replace(" ", "")
    symbol_count = len(compact)
    unknown_count = compact.count("?")
    known_count = max(0, symbol_count - unknown_count)
    noise_heavy_ratio = (
        sum(1 for ch in compact if ch in _CW_NOISE_HEAVY_LETTERS) / float(symbol_count)
        if symbol_count
        else 1.0
    )

    if confidence < 0.45 and not callsigns and len(common_tokens) < 2:
        report["reason"] = "Decoder confidence is too low for a CW message"
        report["summary"] = "CW decode did not validate"
        return report
    if symbol_count >= 12 and not callsigns and len(common_tokens) < 2 and noise_heavy_ratio >= 0.72:
        report["reason"] = "Decoded text is dominated by ambiguous short-symbol letters"
        report["summary"] = "CW decode did not validate"
        return report
    if known_count < 3:
        report["reason"] = "Too few known CW symbols were decoded"
        report["summary"] = "CW decode did not validate"
        return report

    valid = bool(callsigns or len(common_tokens) >= 2 or text == "SOS")
    report["valid"] = valid
    if valid:
        preview = text if len(text) <= 64 else (text[:61] + "...")
        report["reason"] = "Validated against common CW message structure"
        report["summary"] = f"Validated CW message: {preview}"
    else:
        report["reason"] = "Decoded text does not resemble a standard CW exchange"
        report["summary"] = "CW decode did not validate"
    return report


def _load_wav(path: Path) -> tuple[int, np.ndarray]:
    with wave.open(str(path), "rb") as wav_file:
        sample_rate = int(wav_file.getframerate())
        channels = int(wav_file.getnchannels())
        sample_width = int(wav_file.getsampwidth())
        frames = wav_file.readframes(wav_file.getnframes())

    if sample_width == 1:
        samples = np.frombuffer(frames, dtype=np.uint8).astype(np.float32)
        samples = (samples - 128.0) / 128.0
    elif sample_width == 2:
        samples = np.frombuffer(frames, dtype="<i2").astype(np.float32) / 32768.0
    elif sample_width == 4:
        samples = np.frombuffer(frames, dtype="<i4").astype(np.float32) / 2147483648.0
    else:
        raise ValueError(f"Unsupported WAV sample width: {sample_width}")

    if channels > 1:
        samples = samples.reshape(-1, channels).mean(axis=1)
    return sample_rate, samples


def _dominant_tone_hz(samples: np.ndarray, sample_rate: int) -> float | None:
    if samples.size < max(2048, sample_rate // 2):
        return None
    window_samples = min(samples.size, max(sample_rate * 10, 8192))
    segment = np.asarray(samples[:window_samples], dtype=np.float32)
    segment = segment - float(np.mean(segment))
    if not np.any(segment):
        return None
    spectrum = np.abs(np.fft.rfft(segment * np.hanning(segment.size)))
    freqs = np.fft.rfftfreq(segment.size, d=1.0 / float(sample_rate))
    mask = (freqs >= 250.0) & (freqs <= 1200.0)
    if not np.any(mask):
        return None
    band = spectrum[mask]
    if band.size == 0 or float(np.max(band)) <= 0.0:
        return None
    return float(freqs[mask][int(np.argmax(band))])


def _tone_envelope(samples: np.ndarray, sample_rate: int, tone_hz: float) -> np.ndarray:
    t = np.arange(samples.size, dtype=np.float32) / float(sample_rate)
    phase = np.float32(2.0 * np.pi * float(tone_hz)) * t
    in_phase = samples * np.cos(phase)
    quadrature = samples * np.sin(phase)
    window = max(1, int(sample_rate * 0.008))
    kernel = np.ones(window, dtype=np.float32) / float(window)
    i_lp = np.convolve(in_phase, kernel, mode="same")
    q_lp = np.convolve(quadrature, kernel, mode="same")
    return np.sqrt((i_lp * i_lp) + (q_lp * q_lp))


def _binary_runs(mask: np.ndarray, sample_rate: int) -> list[tuple[bool, float]]:
    if mask.size == 0:
        return []
    runs: list[tuple[bool, float]] = []
    start = 0
    current = bool(mask[0])
    changes = np.flatnonzero(mask[1:] != mask[:-1]) + 1
    for idx in changes:
        runs.append((current, float(idx - start) / float(sample_rate)))
        start = int(idx)
        current = not current
    runs.append((current, float(mask.size - start) / float(sample_rate)))
    return runs


def _estimate_dot_seconds(on_runs: list[float]) -> float | None:
    if not on_runs:
        return None
    values = np.sort(np.asarray([dur for dur in on_runs if dur >= 0.015], dtype=np.float32))
    if values.size == 0:
        return None
    subset = values[: max(1, values.size // 3)]
    dot_s = float(np.median(subset))
    return float(min(0.4, max(0.03, dot_s)))


def try_decode_cw_wav(path: str | Path) -> dict[str, Any]:
    wav_path = Path(path)
    report: dict[str, Any] = {
        "ok": False,
        "wav_path": str(wav_path),
        "sample_rate": None,
        "tone_hz": None,
        "dot_ms": None,
        "wpm_est": None,
        "decoded_text": "",
        "confidence": 0.0,
        "summary": "CW decode not attempted",
    }

    if not wav_path.exists():
        report["summary"] = "CW WAV not found"
        return report

    sample_rate, samples = _load_wav(wav_path)
    report["sample_rate"] = int(sample_rate)
    if samples.size == 0:
        report["summary"] = "CW WAV is empty"
        return report

    samples = np.asarray(samples, dtype=np.float32)
    samples = samples - float(np.mean(samples))
    rms = float(np.sqrt(np.mean(samples * samples))) if samples.size else 0.0
    if rms < 1e-4:
        report["summary"] = "CW WAV is too quiet to decode"
        return report

    tone_hz = _dominant_tone_hz(samples, sample_rate)
    if tone_hz is None:
        report["summary"] = "Could not find a stable CW tone"
        return report
    report["tone_hz"] = round(float(tone_hz), 1)

    envelope = _tone_envelope(samples, sample_rate, tone_hz)
    lo = float(np.percentile(envelope, 60.0))
    hi = float(np.percentile(envelope, 99.0))
    if hi <= lo + 1e-6:
        report["summary"] = "CW envelope did not rise above noise floor"
        return report

    threshold = lo + (0.35 * (hi - lo))
    binary = envelope >= threshold
    smooth = max(1, int(sample_rate * 0.004))
    if smooth > 1:
        kernel = np.ones(smooth, dtype=np.float32) / float(smooth)
        binary = np.convolve(binary.astype(np.float32), kernel, mode="same") >= 0.5

    runs = _binary_runs(np.asarray(binary, dtype=bool), sample_rate)
    on_runs = [dur for is_on, dur in runs if is_on]
    dot_s = _estimate_dot_seconds(on_runs)
    if dot_s is None:
        report["summary"] = "Could not estimate CW timing"
        return report

    report["dot_ms"] = round(dot_s * 1000.0, 1)
    report["wpm_est"] = round(1.2 / dot_s, 1)

    decoded: list[str] = []
    current_symbol: list[str] = []
    have_signal = False
    for is_on, duration_s in runs:
        if is_on:
            have_signal = True
            current_symbol.append("." if duration_s < (2.0 * dot_s) else "-")
            continue

        if not current_symbol:
            continue
        if duration_s < (1.7 * dot_s):
            continue

        decoded.append(MORSE_CODE.get("".join(current_symbol), "?"))
        current_symbol = []
        if duration_s >= (5.5 * dot_s):
            decoded.append(" ")

    if current_symbol:
        decoded.append(MORSE_CODE.get("".join(current_symbol), "?"))

    if not have_signal:
        report["summary"] = "No keyed CW envelope detected"
        return report

    decoded_text = " ".join("".join(decoded).split())
    report["decoded_text"] = decoded_text
    symbol_count = sum(1 for ch in decoded_text if ch != " ")
    known_count = sum(1 for ch in decoded_text if ch not in {" ", "?"})
    confidence = (float(known_count) / float(symbol_count)) if symbol_count else 0.0
    report["confidence"] = round(confidence, 3)
    report["ok"] = bool(decoded_text.strip())
    if decoded_text.strip():
        report["summary"] = f"Decoded {known_count}/{symbol_count} CW symbols"
    else:
        report["summary"] = "No CW symbols decoded"
    return report