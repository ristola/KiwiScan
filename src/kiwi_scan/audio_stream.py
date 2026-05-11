from __future__ import annotations

import math
import logging
import queue
import struct
import threading
import time
from types import SimpleNamespace
from typing import Iterator
from urllib.parse import urlparse

import numpy as np

from .kiwi_waterfall import KiwiClientUnavailable, _default_preview_passband, _import_kiwiclient, allocate_ws_timestamp

logger = logging.getLogger(__name__)

_ACTIVE_AUDIO_STREAMS_LOCK = threading.Lock()
_ACTIVE_AUDIO_STREAMS: dict[str, "_KiwiLiveAudioWavStream"] = {}
_IQ_USB_LOW_CUT_HZ = 300.0
_IQ_USB_HIGH_CUT_HZ = 2700.0
_IQ_USB_AUDIO_CENTER_HZ = (_IQ_USB_LOW_CUT_HZ + _IQ_USB_HIGH_CUT_HZ) / 2.0
_IQ_USB_TRANSITION_HZ = 150.0


class KiwiAudioStreamError(RuntimeError):
    pass


def _cosine_taper_weight(freqs_hz: np.ndarray, low_hz: float, high_hz: float, transition_hz: float) -> np.ndarray:
    weights = np.zeros(freqs_hz.shape, dtype=np.float32)
    if high_hz <= low_hz:
        return weights
    inner_low = low_hz + transition_hz
    inner_high = high_hz - transition_hz
    if inner_high <= inner_low:
        keep = (freqs_hz >= low_hz) & (freqs_hz <= high_hz)
        weights[keep] = 1.0
        return weights
    core = (freqs_hz >= inner_low) & (freqs_hz <= inner_high)
    weights[core] = 1.0
    if transition_hz > 0.0:
        rise = (freqs_hz >= low_hz) & (freqs_hz < inner_low)
        if np.any(rise):
            ramp = (freqs_hz[rise] - low_hz) / transition_hz
            weights[rise] = 0.5 - 0.5 * np.cos(np.pi * ramp)
        fall = (freqs_hz > inner_high) & (freqs_hz <= high_hz)
        if np.any(fall):
            ramp = (high_hz - freqs_hz[fall]) / transition_hz
            weights[fall] = 0.5 - 0.5 * np.cos(np.pi * ramp)
    return weights


class _IQSubbandDemodulator:
    def __init__(
        self,
        *,
        shift_hz: float,
        low_cut_hz: float = _IQ_USB_LOW_CUT_HZ,
        high_cut_hz: float = _IQ_USB_HIGH_CUT_HZ,
        audio_center_hz: float = _IQ_USB_AUDIO_CENTER_HZ,
        transition_hz: float = _IQ_USB_TRANSITION_HZ,
    ) -> None:
        self._shift_hz = float(shift_hz)
        self._low_cut_hz = float(low_cut_hz)
        self._high_cut_hz = float(high_cut_hz)
        self._audio_center_hz = float(audio_center_hz)
        self._transition_hz = float(transition_hz)
        self._phase_rad = 0.0
        self._mask_cache_key: tuple[int, float] | None = None
        self._mask_cache: np.ndarray | None = None

    def _band_mask(self, sample_count: int, sample_rate_hz: float) -> np.ndarray:
        cache_key = (int(sample_count), float(sample_rate_hz))
        if self._mask_cache is not None and self._mask_cache_key == cache_key:
            return self._mask_cache
        nyquist_hz = sample_rate_hz * 0.5
        low_cut_hz = min(max(0.0, self._low_cut_hz), nyquist_hz)
        high_cut_hz = min(max(low_cut_hz, self._high_cut_hz), nyquist_hz)
        transition_hz = min(max(0.0, self._transition_hz), max(0.0, (high_cut_hz - low_cut_hz) * 0.5))
        freqs_hz = np.fft.fftfreq(sample_count, d=1.0 / sample_rate_hz)
        mask = _cosine_taper_weight(freqs_hz, low_cut_hz, high_cut_hz, transition_hz)
        self._mask_cache_key = cache_key
        self._mask_cache = mask
        return mask

    def process(self, samples: object, *, sample_rate: float | int | None) -> bytes:
        iq = np.asarray(samples)
        if iq.size == 0:
            return b""
        if not np.iscomplexobj(iq):
            mono = np.asarray(iq, dtype=np.int16)
            return mono.tobytes()

        sample_rate_f = float(sample_rate or 0.0)
        iq_complex = np.asarray(iq, dtype=np.complex64)
        if sample_rate_f <= 0.0:
            mono = np.clip(np.rint(np.real(iq_complex)), -32768, 32767).astype(np.int16, copy=False)
            return mono.tobytes()

        mix_hz = self._shift_hz - self._audio_center_hz
        phase_step = -2.0 * np.pi * mix_hz / sample_rate_f
        phases = self._phase_rad + (phase_step * np.arange(iq_complex.size, dtype=np.float64))
        shifted = iq_complex * np.exp(1j * phases)
        self._phase_rad = float((self._phase_rad + (phase_step * iq_complex.size)) % (2.0 * np.pi))

        spectrum = np.fft.fft(shifted)
        band_limited = np.fft.ifft(spectrum * self._band_mask(iq_complex.size, sample_rate_f))
        mono = np.clip(np.rint(np.real(band_limited)), -32768, 32767).astype(np.int16, copy=False)
        return mono.tobytes()


def _parse_redirect_target(value: str) -> tuple[str, int]:
    parsed = urlparse(str(value or "").strip())
    host = str(parsed.hostname or "").strip()
    port = int(parsed.port or 0)
    if not host or port <= 0:
        raise KiwiAudioStreamError(f"Invalid Kiwi redirect target: {value}")
    return host, port


def _normalize_modulation(mode: str, freq_hz: float) -> str:
    normalized = str(mode or "usb").strip().lower()
    if normalized in {"ft8", "ft4", "wspr", "usb", "iq", "drm"}:
        return "usb"
    if normalized in {"cw", "cwn"}:
        return "cw"
    if normalized in {"am", "amn", "sam"}:
        return "am"
    if normalized in {"lsb", "usb", "nbfm"}:
        return normalized
    if normalized in {"ssb", "phone"}:
        return "lsb" if freq_hz and freq_hz < 10_000_000 else "usb"
    return "usb"


def _wav_header(sample_rate: int) -> bytes:
    channels = 1
    bits_per_sample = 16
    block_align = channels * (bits_per_sample // 8)
    byte_rate = sample_rate * block_align
    data_size = 0x7FFFFFF0
    riff_size = 36 + data_size
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        riff_size,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    )


def _pcm16le_bytes(samples: object) -> bytes:
    pcm = np.asarray(samples)
    if pcm.size == 0:
        return b""
    pcm = np.clip(np.rint(pcm), -32768, 32767).astype("<i2", copy=False)
    return pcm.tobytes()


def _demodulate_iq_to_mono_pcm(
    samples: object,
    *,
    sample_rate: float | int | None,
    shift_hz: float,
    phase_rad: float,
) -> tuple[bytes, float]:
    demodulator = _IQSubbandDemodulator(shift_hz=shift_hz)
    demodulator._phase_rad = float(phase_rad)
    return demodulator.process(samples, sample_rate=sample_rate), float(demodulator._phase_rad)


class _KiwiLiveAudioWavStream:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        freq_hz: float,
        mode: str,
        user: str,
        source_freq_hz: float | None = None,
        required_rx: int | None = None,
        camp_rx: int | None = None,
        stream_id: str | None = None,
    ) -> None:
        self._host = str(host or "").strip()
        self._port = int(port)
        self._password = password
        self._freq_hz = float(freq_hz)
        self._source_freq_hz = float(source_freq_hz) if source_freq_hz is not None else float(freq_hz)
        self._mode = _normalize_modulation(mode, self._freq_hz)
        self._user = str(user or "KiwiScan Audio").strip() or "KiwiScan Audio"
        self._required_rx = int(required_rx) if required_rx is not None else None
        self._camp_rx = int(camp_rx) if camp_rx is not None else None
        self._stream_id = str(stream_id or "").strip() or None
        self._sample_rate = 12_000
        self._ready_event = threading.Event()
        self._closed_event = threading.Event()
        self._stop_event = threading.Event()
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=128)
        self._worker: threading.Thread | None = None
        self._stream = None
        self._error: Exception | None = None
        self._iq_demodulator: _IQSubbandDemodulator | None = None

    def __iter__(self) -> Iterator[bytes]:
        if self._worker is None:
            self._worker = threading.Thread(target=self._run, name="kiwi-audio-stream", daemon=True)
            self._worker.start()

        deadline = time.monotonic() + 10.0
        while not self._ready_event.is_set() and time.monotonic() < deadline:
            if self._closed_event.is_set():
                break
            self._ready_event.wait(timeout=0.1)
        if not self._ready_event.is_set():
            self.close()
            if self._error is not None:
                raise KiwiAudioStreamError(str(self._error)) from self._error
            raise KiwiAudioStreamError("Timed out starting Kiwi audio stream")

        try:
            yield _wav_header(max(1, int(self._sample_rate or 12_000)))
            while True:
                try:
                    chunk = self._queue.get(timeout=0.5)
                except queue.Empty:
                    if self._closed_event.is_set():
                        break
                    continue
                if chunk is None:
                    break
                yield chunk
        finally:
            self.close()

    def close(self) -> None:
        if self._stop_event.is_set():
            self._unregister()
            return
        self._stop_event.set()
        stream = self._stream
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        self._closed_event.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._unregister()

    def _set_ready(self, sample_rate: float | int | None) -> None:
        try:
            rate = int(float(sample_rate or 0))
        except Exception:
            rate = 0
        if rate > 0:
            self._sample_rate = rate
        self._ready_event.set()

    def _push_audio(self, sample_bytes: bytes) -> None:
        if self._stop_event.is_set() or not sample_bytes:
            return
        self._ready_event.set()
        try:
            self._queue.put_nowait(sample_bytes)
        except queue.Full:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(sample_bytes)
            except queue.Full:
                pass

    def _finish(self, error: Exception | None = None) -> None:
        if error is not None:
            self._error = error
        self._closed_event.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._unregister()

    def _unregister(self) -> None:
        if not self._stream_id:
            return
        with _ACTIVE_AUDIO_STREAMS_LOCK:
            if _ACTIVE_AUDIO_STREAMS.get(self._stream_id) is self:
                _ACTIVE_AUDIO_STREAMS.pop(self._stream_id, None)

    def _run(self) -> None:
        try:
            kiwi = _import_kiwiclient()
            KiwiSDRStream = getattr(kiwi, "KiwiSDRStream", None)
            if KiwiSDRStream is None:
                raise KiwiClientUnavailable("KiwiSDRStream is unavailable")
            KiwiRedirectError = getattr(kiwi, "KiwiRedirectError", RuntimeError)

            controller = self
            modulation = self._mode
            freq_khz = self._freq_hz / 1000.0
            iq_shift_hz = float(self._freq_hz - self._source_freq_hz)
            self._iq_demodulator = _IQSubbandDemodulator(shift_hz=iq_shift_hz)
            lp_cut, hp_cut = _default_preview_passband(modulation)

            class _LiveAudioStream(KiwiSDRStream):
                def _on_sample_rate_change(self_inner) -> None:
                    controller._set_ready(getattr(self_inner, "_sample_rate", None))

                def _setup_rx_params(self_inner) -> None:
                    self_inner.set_name(controller._user)
                    self_inner.set_mod(modulation, lp_cut, hp_cut, freq_khz)
                    self_inner.set_agc(on=True)
                    try:
                        self_inner._set_snd_comp(False)
                    except Exception:
                        pass

                def _process_audio_samples(self_inner, seq, samples, rssi, is_compressed):
                    controller._push_audio(_pcm16le_bytes(samples))

                def _process_iq_samples(self_inner, seq, samples, rssi, gps, is_compressed=None):
                    try:
                        demodulator = controller._iq_demodulator
                        if demodulator is None:
                            demodulator = _IQSubbandDemodulator(shift_hz=iq_shift_hz)
                            controller._iq_demodulator = demodulator
                        pcm_bytes = demodulator.process(
                            samples,
                            sample_rate=getattr(self_inner, "_sample_rate", controller._sample_rate),
                        )
                    except Exception:
                        return
                    controller._push_audio(pcm_bytes)

            redirect_count = 0
            max_redirects = 4
            while not self._stop_event.is_set():
                stream = _LiveAudioStream()
                self._stream = stream
                stream._type = "SND"
                stream._reader = True
                stream._freq = freq_khz
                stream._freq_offset = 0
                stream._modulation = modulation
                stream._lowcut = lp_cut
                stream._highcut = hp_cut
                stream._ADC_OV = False
                stream._compression = False
                stream._raw = False
                stream._quiet = True
                stream._test_mode = False
                stream._kiwi_foff = 0
                stream._camp_chan = int(self._camp_rx) if self._camp_rx is not None else -1
                if self._camp_rx is not None:
                    stream._camp_wait_event = threading.Event()
                    stream._camp_wait_event.set()
                stream._waterfall_queue = None
                stream._audio_queue = None
                stream._full_name = self._user
                stream._options = SimpleNamespace(
                    socket_timeout=2.0,
                    ws_timestamp=allocate_ws_timestamp(),
                    wideband=False,
                    waterfall_cal=None,
                    password=self._password,
                    tlimit_password=None,
                    tlimit=None,
                    station=None,
                    filename="kiwi_audio_preview",
                    user=self._user,
                    writer_init=lambda path: None,
                    reader_init=lambda path: None,
                    is_kiwi_tdoa=False,
                    agc_gain=None,
                    thresh=None,
                    compression=False,
                    quiet=True,
                    tstamp=False,
                    waterfall_no_sync=False,
                    waterfall_maxdb=-10,
                    waterfall_mindb=-110,
                    nolocal=False,
                    admin=False,
                    sound=True,
                    S_meter=-1,
                    sdt=0,
                    ADC_OV=False,
                    netcat=False,
                    resample=0,
                    camp_allow_1ch=True,
                    test_mode=False,
                    multiple_connections=False,
                    idx=int(self._required_rx) if self._required_rx is not None else 0,
                    dir=None,
                    bad_cmd=False,
                    rev_bin=False,
                    wf_cal=None,
                    modulation=modulation,
                    lp_cut=lp_cut,
                    hp_cut=hp_cut,
                    freq_pbc=False,
                    nb=False,
                    nb_test=False,
                    de_emp=False,
                    nb_gate=100,
                    nb_thresh=50,
                    server_host=self._host,
                    server_port=self._port,
                    rx_chan=int(self._required_rx) if self._required_rx is not None else -1,
                )
                assigned_rx = [None]
                try:
                    orig_process = stream._process_msg_param

                    def _wrapped_process(name, value):
                        try:
                            if name == "rx_chan" and value is not None:
                                assigned_rx[0] = int(value)
                                if self._camp_rx is None and self._required_rx is not None and int(assigned_rx[0]) != int(self._required_rx):
                                    raise KiwiAudioStreamError(
                                        f"Assigned Kiwi rx {int(assigned_rx[0])} instead of requested {int(self._required_rx)}"
                                    )
                        except KiwiAudioStreamError:
                            raise
                        except Exception:
                            pass
                        return orig_process(name, value)

                    stream._process_msg_param = _wrapped_process
                except Exception:
                    pass
                try:
                    stream.connect(self._host, self._port)
                    stream.open()
                    while not self._stop_event.is_set():
                        stream.run()
                    break
                except KiwiRedirectError as exc:
                    redirect_count += 1
                    if redirect_count > max_redirects:
                        raise KiwiAudioStreamError(f"Kiwi redirect loop exceeded {max_redirects} hops") from exc
                    redirect_host, redirect_port = _parse_redirect_target(str(exc))
                    logger.info(
                        "Kiwi audio stream redirect %s:%s -> %s:%s",
                        self._host,
                        self._port,
                        redirect_host,
                        redirect_port,
                    )
                    try:
                        stream.close()
                    except Exception:
                        pass
                    self._host = redirect_host
                    self._port = redirect_port
                    self._stream = None
                    continue
        except Exception as exc:
            logger.warning("Kiwi audio stream failed for %s:%s: %s", self._host, self._port, exc)
            self._finish(exc)
            return

        self._finish()


def stream_kiwi_audio_wav(
    *,
    host: str,
    port: int,
    password: str | None,
    freq_hz: float,
    mode: str,
    user: str,
    source_freq_hz: float | None = None,
    required_rx: int | None = None,
    camp_rx: int | None = None,
    stream_id: str | None = None,
) -> Iterator[bytes]:
    stream = _KiwiLiveAudioWavStream(
        host=host,
        port=port,
        password=password,
        freq_hz=freq_hz,
        mode=mode,
        user=user,
        source_freq_hz=source_freq_hz,
        required_rx=required_rx,
        camp_rx=camp_rx,
        stream_id=stream_id,
    )
    if stream._stream_id:
        prior_stream = None
        with _ACTIVE_AUDIO_STREAMS_LOCK:
            prior_stream = _ACTIVE_AUDIO_STREAMS.get(stream._stream_id)
            _ACTIVE_AUDIO_STREAMS[stream._stream_id] = stream
        if prior_stream is not None and prior_stream is not stream:
            prior_stream.close()
    return iter(stream)


def stop_kiwi_audio_stream(stream_id: str | None) -> bool:
    normalized = str(stream_id or "").strip()
    if not normalized:
        return False
    with _ACTIVE_AUDIO_STREAMS_LOCK:
        stream = _ACTIVE_AUDIO_STREAMS.pop(normalized, None)
    if stream is None:
        return False
    stream.close()
    return True