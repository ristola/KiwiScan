from __future__ import annotations

import numpy as np

import kiwi_scan.audio_stream as audio_stream
from kiwi_scan.audio_stream import _demodulate_iq_to_mono_pcm, _normalize_camp_audio_samples, _pcm16le_bytes


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


def test_normalize_camp_audio_samples_byteswaps_compressed_big_endian_camp_audio() -> None:
    samples = np.array([0x3412, -0x3313, 0x7f00], dtype=np.int16)

    normalized = _normalize_camp_audio_samples(
        samples,
        is_camping=True,
        is_compressed=True,
        is_little_endian=False,
    )

    assert normalized.tolist() == [0x1234, -0x1234, 0x007f]


def test_normalize_camp_audio_samples_leaves_noncompressed_or_little_endian_audio_unchanged() -> None:
    samples = np.array([1200, -1200, 600], dtype=np.int16)

    unchanged_compressed_little = _normalize_camp_audio_samples(
        samples,
        is_camping=True,
        is_compressed=True,
        is_little_endian=True,
    )
    unchanged_not_compressed = _normalize_camp_audio_samples(
        samples,
        is_camping=True,
        is_compressed=False,
        is_little_endian=False,
    )

    assert unchanged_compressed_little.tolist() == samples.tolist()
    assert unchanged_not_compressed.tolist() == samples.tolist()


def test_stream_kiwi_audio_wav_retries_busy_camp_connection(monkeypatch) -> None:
    attempts = {"open": 0}

    class _FakeTooBusyError(RuntimeError):
        pass

    class _FakeKiwiSDRStream:
        def __init__(self) -> None:
            self._sample_rate = 12_000
            self._sent_audio = False

        def set_name(self, _name: str) -> None:
            return None

        def set_mod(self, _modulation: str, _lp_cut: int, _hp_cut: int, _freq_khz: float) -> None:
            return None

        def set_agc(self, on: bool = True) -> None:
            return None

        def _set_snd_comp(self, _enabled: bool) -> None:
            return None

        def connect(self, _host: str, _port: int) -> None:
            return None

        def open(self) -> None:
            attempts["open"] += 1
            if attempts["open"] == 1:
                raise _FakeTooBusyError("10.13.73.235: all 8 client slots taken")
            self._on_sample_rate_change()

        def run(self) -> None:
            if self._sent_audio:
                return
            self._sent_audio = True
            self._process_audio_samples(0, np.array([1200, -1200, 600], dtype=np.int16), -87.0, False)

        def close(self) -> None:
            return None

    class _FakeKiwiModule:
        KiwiSDRStream = _FakeKiwiSDRStream
        KiwiRedirectError = RuntimeError
        KiwiTooBusyError = _FakeTooBusyError

    monkeypatch.setattr(audio_stream, "_import_kiwiclient", lambda: _FakeKiwiModule)
    monkeypatch.setattr(audio_stream, "_CAMP_BUSY_RETRY_DELAY_S", 0.0)
    monkeypatch.setattr(audio_stream, "_CAMP_BUSY_RETRY_TIMEOUT_S", 1.0)

    iterator = audio_stream.stream_kiwi_audio_wav(
        host="10.13.73.235",
        port=8073,
        password=None,
        freq_hz=7_074_000.0,
        source_freq_hz=7_074_000.0,
        mode="FT8",
        user="KiwiScan Test",
        required_rx=None,
        camp_rx=4,
        stream_id="camp-rx4-test",
    )

    try:
        header = next(iterator)
        chunk = next(iterator)
    finally:
        close_fn = getattr(iterator, "close", None)
        if callable(close_fn):
            close_fn()

    assert header[:4] == b"RIFF"
    assert chunk == bytes(np.array([1200, -1200, 600], dtype="<i2"))
    assert attempts["open"] == 2
