from __future__ import annotations

import wave
from pathlib import Path

import numpy as np

from kiwi_scan.cw_decode import try_decode_cw_wav, validate_cw_message


_CHAR_TO_MORSE = {
    "S": "...",
    "O": "---",
}


def _tone(duration_s: float, *, sample_rate: int, tone_hz: float) -> np.ndarray:
    count = int(round(duration_s * sample_rate))
    t = np.arange(count, dtype=np.float32) / float(sample_rate)
    return 0.7 * np.sin(2.0 * np.pi * float(tone_hz) * t)


def _silence(duration_s: float, *, sample_rate: int) -> np.ndarray:
    count = int(round(duration_s * sample_rate))
    return np.zeros(count, dtype=np.float32)


def _render_message(message: str, *, sample_rate: int, dot_s: float, tone_hz: float) -> np.ndarray:
    parts: list[np.ndarray] = [_silence(dot_s * 4.0, sample_rate=sample_rate)]
    letters = list(message)
    for letter_index, letter in enumerate(letters):
        symbols = _CHAR_TO_MORSE[letter]
        for symbol_index, symbol in enumerate(symbols):
            parts.append(_tone(dot_s if symbol == "." else (dot_s * 3.0), sample_rate=sample_rate, tone_hz=tone_hz))
            if symbol_index != len(symbols) - 1:
                parts.append(_silence(dot_s, sample_rate=sample_rate))
        if letter_index != len(letters) - 1:
            parts.append(_silence(dot_s * 3.0, sample_rate=sample_rate))
    parts.append(_silence(dot_s * 4.0, sample_rate=sample_rate))
    return np.concatenate(parts)


def test_try_decode_cw_wav_decodes_sos(tmp_path: Path) -> None:
    sample_rate = 12000
    dot_s = 0.08
    tone_hz = 700.0
    samples = _render_message("SOS", sample_rate=sample_rate, dot_s=dot_s, tone_hz=tone_hz)
    wav_path = tmp_path / "sos.wav"

    pcm = np.clip(samples * 32767.0, -32768.0, 32767.0).astype("<i2")
    with wave.open(str(wav_path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(pcm.tobytes())

    report = try_decode_cw_wav(wav_path)

    assert report["ok"] is True
    assert report["decoded_text"].replace(" ", "") == "SOS"
    assert report["tone_hz"] is not None
    assert abs(float(report["tone_hz"]) - tone_hz) < 30.0
    assert report["confidence"] >= 0.99


def test_validate_cw_message_accepts_common_exchange() -> None:
    report = validate_cw_message("CQ TEST DE W1AW", confidence=0.92)

    assert report["valid"] is True
    assert report["normalized_text"] == "CQ TEST DE W1AW"
    assert "W1AW" in report["callsigns"]


def test_validate_cw_message_rejects_ambiguous_noise_text() -> None:
    report = validate_cw_message(
        "IEES5S EEIISSHIE SE ISESIIEIEHEEEEHEIS E I SEEI",
        confidence=0.89,
    )

    assert report["valid"] is False
    assert report["summary"] == "CW decode did not validate"