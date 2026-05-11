from __future__ import annotations

import numpy as np

from kiwi_scan.audio_stream import _demodulate_iq_to_mono_pcm, _pcm16le_bytes


def _peak_hz(samples: np.ndarray, sample_rate: int) -> float:
    window = np.asarray(samples, dtype=np.float32)
    window = window - float(np.mean(window))
    spectrum = np.abs(np.fft.rfft(window * np.hanning(window.size)))
    freqs = np.fft.rfftfreq(window.size, d=1.0 / float(sample_rate))
    return float(freqs[int(np.argmax(spectrum))])


def test_demodulate_iq_to_mono_pcm_places_selected_signal_in_usb_preview_band() -> None:
    sample_rate = 12_000
    duration_s = 0.5
    sample_count = int(sample_rate * duration_s)
    time_axis = np.arange(sample_count, dtype=np.float32) / float(sample_rate)

    # Target tone sits 700 Hz above the selected mode's dial position.
    iq_samples = (12000.0 * np.exp(1j * (2.0 * np.pi * 1900.0 * time_axis))).astype(np.complex64)

    selected_pcm, _ = _demodulate_iq_to_mono_pcm(
        iq_samples,
        sample_rate=sample_rate,
        shift_hz=1200.0,
        phase_rad=0.0,
    )
    wrong_mode_pcm, _ = _demodulate_iq_to_mono_pcm(
        iq_samples,
        sample_rate=sample_rate,
        shift_hz=-3200.0,
        phase_rad=0.0,
    )

    selected_audio = np.frombuffer(selected_pcm, dtype=np.int16)
    wrong_mode_audio = np.frombuffer(wrong_mode_pcm, dtype=np.int16)

    assert selected_audio.size == sample_count
    assert wrong_mode_audio.size == sample_count
    assert abs(_peak_hz(selected_audio, sample_rate) - 2200.0) < 50.0
    assert int(np.max(np.abs(selected_audio))) > 5000
    assert int(np.max(np.abs(wrong_mode_audio))) < 50


def test_demodulate_iq_to_mono_pcm_separates_neighboring_mode_subbands() -> None:
    sample_rate = 12_000
    duration_s = 0.5
    sample_count = int(sample_rate * duration_s)
    time_axis = np.arange(sample_count, dtype=np.float32) / float(sample_rate)

    iq_samples = (
        12000.0 * np.exp(1j * (2.0 * np.pi * 1900.0 * time_axis))
        + 9000.0 * np.exp(1j * (2.0 * np.pi * 4100.0 * time_axis))
        + 6000.0 * np.exp(1j * (2.0 * np.pi * -2100.0 * time_axis))
    ).astype(np.complex64)

    ft8_pcm, _ = _demodulate_iq_to_mono_pcm(
        iq_samples,
        sample_rate=sample_rate,
        shift_hz=1200.0,
        phase_rad=0.0,
    )
    ft4_pcm, _ = _demodulate_iq_to_mono_pcm(
        iq_samples,
        sample_rate=sample_rate,
        shift_hz=3200.0,
        phase_rad=0.0,
    )
    wspr_pcm, _ = _demodulate_iq_to_mono_pcm(
        iq_samples,
        sample_rate=sample_rate,
        shift_hz=-3200.0,
        phase_rad=0.0,
    )

    ft8_audio = np.frombuffer(ft8_pcm, dtype=np.int16)
    ft4_audio = np.frombuffer(ft4_pcm, dtype=np.int16)
    wspr_audio = np.frombuffer(wspr_pcm, dtype=np.int16)

    assert abs(_peak_hz(ft8_audio, sample_rate) - 2200.0) < 50.0
    assert abs(_peak_hz(ft4_audio, sample_rate) - 2400.0) < 50.0
    assert abs(_peak_hz(wspr_audio, sample_rate) - 2600.0) < 50.0
    assert not np.array_equal(ft8_audio, ft4_audio)
    assert not np.array_equal(ft8_audio, wspr_audio)
    assert not np.array_equal(ft4_audio, wspr_audio)


def test_pcm16le_bytes_normalizes_big_endian_audio_samples() -> None:
    samples = np.array([0x1234, -0x1234, 0x007f, -0x0080], dtype=">i2")

    pcm = _pcm16le_bytes(samples)

    assert pcm == bytes([0x34, 0x12, 0xcc, 0xed, 0x7f, 0x00, 0x80, 0xff])
