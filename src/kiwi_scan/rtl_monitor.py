"""
rtl_monitor.py — Wideband IQ monitor for RTL-SDR.

Captures a full 2.4 MSPS IQ stream from one RTL-SDR device and simultaneously
monitors multiple narrowband channels within that bandwidth.  Designed for
VHF repeater monitoring: FFT the full span, detect carrier on each frequency,
FM-demodulate active channels, run VAD, and transcribe voice via faster-whisper.

Typical use: 8 VHF repeaters spanning ~1.95 MHz → one SDR, one IQ stream.

Device modes
------------
Local (rtl_sdr subprocess):
    device = "20000897"        # serial number
    device = "0"               # device index

Network (rtl_tcp over LAN):
    device = "tcp://10.13.73.185:1234"

For network mode, start rtl_tcp on the remote host first:
    rtl_tcp -d 20000897 -a 0.0.0.0 -p 1234 -s 2400000
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import re
import time
import wave
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.signal import bilinear, lfilter, lfilter_zi

RECORDINGS_DIR  = Path("outputs/recordings/repeater")
MIN_REC_SEC     = 2.0   # discard recordings shorter than this (noise-floor blips)

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

FFT_SIZE        = 4096          # frequency resolution: 2400000/4096 ≈ 586 Hz/bin
CHUNK_SAMPLES   = 65536         # IQ pairs per processing chunk (~27 ms at 2.4 MSPS)
AUDIO_RATE      = 48_000        # output audio sample rate after FM demod + decimate
CARRIER_THRESH  = 12.0          # dB above noise floor → carrier present
VOICE_ENERGY    = 0.04          # RMS² threshold for voice activity
STT_WINDOW_SEC  = 4.0           # seconds of audio before sending to Whisper
NOISE_ALPHA     = 0.05          # IIR weight for noise floor tracking

# ── Recording gate ────────────────────────────────────────────────────────────
# Carrier events are buffered before committing to a WAV file.  The gate
# classifier distinguishes voice from CW / bare carrier to avoid recording
# repeater ID pings and squelch-tail false triggers.

PRE_ROLL_SEC        = 1.5   # audio buffer kept before the recording decision
GATE_SEC            = 1.0   # carrier must be present this long before recording
MONITOR_WIN_SEC     = 0.5   # re-evaluation window after a CW skip
MIN_VOICE_GATE_FRAC = 0.25  # voice fraction in gate window → commit immediately
CW_TONE_RATIO       = 0.45  # spectral peak / band total > this → classify as CW
SUSTAINED_THRESH_SEC = 5.0  # force-record any carrier sustained longer than this
TAIL_DELAY_SEC      = 3.5   # keep recording this long after carrier drops (breath pause)

# rtl_tcp command IDs
_CMD_SET_FREQ        = 0x01
_CMD_SET_RATE        = 0x02
_CMD_SET_GAIN_MODE   = 0x03  # 0=auto, 1=manual
_CMD_SET_GAIN        = 0x04  # tenths of dB
_CMD_SET_AGC_MODE    = 0x08  # 1=enable hardware AGC

# ── NBFM audio chain constants ────────────────────────────────────────────────
# VHF FM repeaters use 75 µs pre-emphasis (US standard); de-emphasis undoes it.
_DEEMPH_B, _DEEMPH_A = bilinear([1.0], [75e-6, 1.0], AUDIO_RATE)
_DEEMPH_ZI_PROTO: np.ndarray = lfilter_zi(_DEEMPH_B, _DEEMPH_A).astype(np.float64)

# FM discriminator gain: ±5 kHz deviation → disc ≈ ±0.654 rad at 48 kHz.
# 0.7 brings 5 kHz dev to ~0.69 before de-emphasis, leaving headroom so that
# carrier noise sits at ~18% RMS (below VOICE_ENERGY threshold) and speech
# peaks at ~60% — giving ~10 dB perceptual SNR.
_RPT_FM_GAIN    = 0.7
_RPT_AUDIO_GAIN = 1.5  # tanh soft-limiter drive (higher = harder limiting)


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class RepeaterChannel:
    freq_hz: int
    label: str
    carrier: bool = False
    voice: bool = False
    power_db: float = -999.0
    noise_db: float = -999.0
    snr_db: float = 0.0
    active_since: Optional[float] = None
    total_active_sec: float = 0.0
    carrier_count: int = 0
    transcripts: list = field(default_factory=list)
    _audio_buf: list = field(default_factory=list, repr=False)
    _audio_buf_sec: float = field(default=0.0, repr=False)
    _audio_subscribers: list = field(default_factory=list, repr=False)
    mode:                str             = "idle"  # idle | voice | cw | carrier
    _rec_file:           Optional[object] = field(default=None,  repr=False)
    _rec_path:           Optional[str]    = field(default=None,  repr=False)
    _rec_start:          Optional[float]  = field(default=None,  repr=False)
    _rec_close_at:       Optional[float]  = field(default=None,  repr=False)
    # Gate / pre-roll state
    _prebuf_chunks:      list            = field(default_factory=list, repr=False)
    _prebuf_sec:         float           = field(default=0.0,  repr=False)
    _gate_start:         Optional[float] = field(default=None, repr=False)
    _gate_voice_frames:  int             = field(default=0,    repr=False)
    _gate_cw_frames:     int             = field(default=0,    repr=False)
    _gate_total_frames:  int             = field(default=0,    repr=False)
    # Post-skip monitoring (re-evaluate if voice appears after a CW skip)
    _monitor_start:        Optional[float] = field(default=None, repr=False)
    _monitor_voice_frames: int             = field(default=0,    repr=False)
    _monitor_total_frames: int             = field(default=0,    repr=False)
    # Per-channel FM demod state (phase continuity + de-emphasis IIR)
    _lo_phase: float                          = field(default=0.0,  repr=False)
    _last_iq:  complex                        = field(default=0j,   repr=False)
    _zi_dem:   Optional[np.ndarray]           = field(default=None, repr=False)


def _parse_tcp_device(device: str) -> Optional[tuple[str, int]]:
    """Return (host, port) if device is a tcp:// URL, else None."""
    if device.startswith("tcp://"):
        addr = device[6:]
        if ":" in addr:
            host, port_str = addr.rsplit(":", 1)
            try:
                return host, int(port_str)
            except ValueError:
                pass
    return None


# ── Core monitor ─────────────────────────────────────────────────────────────

class RtlRepeaterMonitor:
    """
    IQ-based simultaneous multi-channel VHF repeater monitor.

    Parameters
    ----------
    device : str
        RTL-SDR device serial number / index, or ``tcp://host:port`` for
        a remote rtl_tcp server.
    center_hz : int
        Center tuning frequency in Hz.
    sample_rate : int
        IQ sample rate (default 2 400 000).
    repeaters : list[dict]
        Each dict must have ``freq_hz`` (int) and ``label`` (str).
    enable_stt : bool
        Whether to transcribe voice via faster-whisper.
    rtl_gain : str
        Gain in tenths-of-dB for manual mode, or "0" / "auto" for AGC.
    """

    def __init__(
        self,
        device: str,
        center_hz: int,
        sample_rate: int = 2_400_000,
        repeaters: Optional[list] = None,
        enable_stt: bool = True,
        rtl_gain: str = "0",
    ):
        self.device      = device
        self.center_hz   = center_hz
        self.sample_rate = sample_rate
        self.enable_stt  = enable_stt
        self.rtl_gain    = rtl_gain

        self.channels: list[RepeaterChannel] = [
            RepeaterChannel(freq_hz=r["freq_hz"], label=r["label"])
            for r in (repeaters or [])
        ]

        self.record_enabled: bool = True
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._noise_floor = -999.0   # sentinel: first chunk snap-inits to real median
        self._warmup_chunks = 0      # carrier detection suppressed for first N chunks
        self._whisper = None
        self.started_at: Optional[float] = None
        self.error: Optional[str] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return

        self._loop = asyncio.get_running_loop()

        if self.enable_stt:
            self._load_whisper()

        tcp_addr = _parse_tcp_device(self.device)
        if tcp_addr:
            await self._start_tcp(*tcp_addr)
        else:
            await self._start_local()

        if not self.error:
            self._running = True
            self.started_at = time.time()
            self._task = asyncio.create_task(self._process_loop(), name="rtl-monitor")

    async def _start_local(self) -> None:
        cmd = [
            "rtl_sdr",
            "-d", str(self.device),
            "-f", str(self.center_hz),
            "-s", str(self.sample_rate),
            "-g", self.rtl_gain,
            "-",
        ]
        logger.info("RtlRepeaterMonitor local: %s", " ".join(cmd))
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            self.error = (
                "rtl_sdr binary not found — use tcp://host:port for a remote SDR "
                "or install the rtl-sdr package locally"
            )
            logger.error(self.error)

    async def _start_tcp(self, host: str, port: int) -> None:
        logger.info("RtlRepeaterMonitor tcp: %s:%d", host, port)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
        except (OSError, asyncio.TimeoutError) as exc:
            self.error = f"Cannot connect to rtl_tcp at {host}:{port} — {exc}"
            logger.error(self.error)
            return

        # rtl_tcp sends a 12-byte magic on connect: "RTL0" + tuner_type(4) + gain_count(4)
        try:
            magic = await asyncio.wait_for(reader.read(12), timeout=3.0)
        except asyncio.TimeoutError:
            self.error = f"rtl_tcp at {host}:{port} did not send handshake"
            writer.close()
            return

        if len(magic) < 4 or magic[:4] != b"RTL0":
            self.error = f"rtl_tcp handshake failed — got {magic[:4]!r}, expected b'RTL0'"
            writer.close()
            return

        def _cmd(cmd_id: int, value: int) -> bytes:
            return bytes([cmd_id]) + value.to_bytes(4, "big")

        # Configure: frequency, sample rate, gain
        gain_auto = self.rtl_gain in ("0", "auto", "")
        writer.write(_cmd(_CMD_SET_FREQ, self.center_hz))
        writer.write(_cmd(_CMD_SET_RATE, self.sample_rate))
        if gain_auto:
            writer.write(_cmd(_CMD_SET_GAIN_MODE, 0))   # auto gain
            writer.write(_cmd(_CMD_SET_AGC_MODE, 1))    # hardware AGC on
        else:
            writer.write(_cmd(_CMD_SET_GAIN_MODE, 1))   # manual gain
            writer.write(_cmd(_CMD_SET_GAIN, int(float(self.rtl_gain) * 10)))
        await writer.drain()

        self._tcp_reader = reader
        self._tcp_writer = writer
        logger.info("rtl_tcp connected and configured at %s:%d", host, port)

    async def stop(self) -> None:
        self._running = False
        for ch in self.channels:
            self._close_recording(ch)
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=3.0)
            except Exception:
                pass
        if self._tcp_writer:
            try:
                self._tcp_writer.close()
                await self._tcp_writer.wait_closed()
            except Exception:
                pass
            self._tcp_writer = None
            self._tcp_reader = None
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("RtlRepeaterMonitor stopped")

    # ── Processing loop ────────────────────────────────────────────────────

    async def _read_chunk(self, n_bytes: int) -> bytes:
        if self._tcp_reader:
            try:
                return await self._tcp_reader.readexactly(n_bytes)
            except asyncio.IncompleteReadError as exc:
                return exc.partial  # stream closed mid-chunk — let caller handle empty
        return await self._proc.stdout.read(n_bytes)

    async def _process_loop(self) -> None:
        bytes_per_chunk = CHUNK_SAMPLES * 2  # I byte + Q byte per sample
        reconnect_delay = 3.0
        tcp_addr = _parse_tcp_device(self.device)
        while self._running:
            # Reconnect if the TCP stream was lost
            if tcp_addr and self._tcp_reader is None:
                await self._start_tcp(*tcp_addr)
                if self.error:
                    logger.warning("rtl_tcp reconnect failed — retry in %.0fs", reconnect_delay)
                    try:
                        await asyncio.sleep(reconnect_delay)
                    except asyncio.CancelledError:
                        return
                    self.error = None
                    continue

            try:
                raw = await self._read_chunk(bytes_per_chunk)
                if not raw:
                    src = "rtl_tcp" if tcp_addr else "rtl_sdr"
                    logger.warning("%s stream closed — reconnecting in %.0fs", src, reconnect_delay)
                    if self._tcp_writer:
                        try:
                            self._tcp_writer.close()
                        except Exception:
                            pass
                    self._tcp_reader = None
                    self._tcp_writer = None
                    if not tcp_addr:
                        break  # local rtl_sdr subprocess — can't reconnect
                    await asyncio.sleep(reconnect_delay)
                    continue
                self.error = None
                await asyncio.get_event_loop().run_in_executor(
                    None, self._analyze, raw
                )
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.exception("RtlRepeaterMonitor error: %s", exc)
                self.error = str(exc)
                await asyncio.sleep(reconnect_delay)
        self._running = False

    # ── Signal analysis ────────────────────────────────────────────────────

    def _analyze(self, raw: bytes) -> None:
        # Convert raw uint8 I/Q to complex float in [-1, 1]
        # Trim to even byte count — TCP may deliver an odd-length partial chunk
        u8 = np.frombuffer(raw[:len(raw) & ~1], dtype=np.uint8).astype(np.float32)
        if len(u8) < 2:
            return
        iq = ((u8[0::2] - 127.5) + 1j * (u8[1::2] - 127.5)) / 127.5

        # Full-bandwidth FFT (power spectrum in dB, frequency-sorted)
        spectrum_db = self._fft_db(iq)

        # Update global noise floor estimate (median of spectrum)
        median_pwr = float(np.median(spectrum_db))
        if self._noise_floor < -900:
            self._noise_floor = median_pwr   # snap-init on first real measurement
        else:
            self._noise_floor += NOISE_ALPHA * (median_pwr - self._noise_floor)

        # Suppress carrier detection for the first ~3 seconds while the noise
        # floor IIR stabilises — prevents false triggers on every channel at startup.
        _WARMUP_CHUNKS = 110   # 110 × 27 ms ≈ 3 s
        self._warmup_chunks += 1
        if self._warmup_chunks <= _WARMUP_CHUNKS:
            return

        # Per-channel analysis
        for ch in self.channels:
            self._update_channel(ch, iq, spectrum_db)

    def _fft_db(self, iq: np.ndarray) -> np.ndarray:
        n = min(FFT_SIZE, len(iq))
        window = np.hanning(n)
        spectrum = np.abs(np.fft.fftshift(np.fft.fft(iq[:n] * window, n=FFT_SIZE))) ** 2
        return 10.0 * np.log10(spectrum + 1e-12)

    def _update_channel(
        self, ch: RepeaterChannel, iq: np.ndarray, spectrum_db: np.ndarray
    ) -> None:
        # Map channel frequency to FFT bin (after fftshift, bin 0 = center - sr/2)
        offset_hz = ch.freq_hz - self.center_hz
        bin_idx = int(round(offset_hz / (self.sample_rate / FFT_SIZE))) + FFT_SIZE // 2
        bin_idx = max(3, min(FFT_SIZE - 4, bin_idx))

        # Average power in ±4 bins (±2.3 kHz window)
        pwr = float(np.mean(spectrum_db[bin_idx - 4 : bin_idx + 5]))
        ch.power_db = pwr
        ch.noise_db = self._noise_floor
        ch.snr_db = round(pwr - self._noise_floor, 1)

        was_active = ch.carrier
        ch.carrier = ch.snr_db > CARRIER_THRESH

        # ── Carrier rising edge ────────────────────────────────────────────────
        if ch.carrier and not was_active:
            ch.carrier_count += 1
            ch.active_since = time.time()

            if ch._rec_file and ch._rec_close_at:
                # Carrier returned while a recording was still in its tail delay
                # (breath pause, momentary dropout).  Cancel the close and continue
                # writing to the same file — no gap, no new gate needed.
                ch._rec_close_at = None
                logger.debug("Carrier resumed during tail — continuing recording %s", ch.label)
            else:
                # Fresh carrier event — run the gate classifier before committing
                ch._gate_start        = time.time()
                ch._gate_voice_frames = 0
                ch._gate_cw_frames    = 0
                ch._gate_total_frames = 0

        # ── FM demodulation ────────────────────────────────────────────────────
        has_subs = bool(ch._audio_subscribers)
        if ch.carrier or has_subs or ch._rec_file:
            audio     = self._fm_demodulate(iq, ch)
            chunk_dur = len(audio) / AUDIO_RATE
            pcm_bytes = (np.clip(audio, -1.0, 1.0) * 32767).astype(np.int16).tobytes()

            # ── Pre-roll circular buffer ───────────────────────────────────────
            # Keep the last PRE_ROLL_SEC of audio so it can be flushed to disk
            # when the gate commits to a recording.
            ch._prebuf_chunks.append((audio, pcm_bytes))
            ch._prebuf_sec += chunk_dur
            while ch._prebuf_sec > PRE_ROLL_SEC + chunk_dur and len(ch._prebuf_chunks) > 1:
                old_a, _ = ch._prebuf_chunks.pop(0)
                ch._prebuf_sec -= len(old_a) / AUDIO_RATE

            # ── Gate: classify chunks and decide ──────────────────────────────
            if ch._gate_start is not None and ch.carrier:
                sig_mode = self._classify_chunk(audio)
                ch._gate_total_frames += 1
                if sig_mode == 'voice':
                    ch._gate_voice_frames += 1
                    ch.mode = 'voice'
                elif sig_mode == 'cw':
                    ch._gate_cw_frames += 1
                    ch.mode = 'cw'
                else:
                    ch.mode = 'carrier'

                gate_age     = time.time() - ch._gate_start
                voice_frac   = ch._gate_voice_frames / max(1, ch._gate_total_frames)
                cw_frac      = ch._gate_cw_frames    / max(1, ch._gate_total_frames)
                carrier_secs = time.time() - (ch.active_since or time.time())

                early_voice  = voice_frac >= MIN_VOICE_GATE_FRAC and gate_age >= 0.3
                gate_expired = gate_age >= GATE_SEC
                sustained    = carrier_secs >= SUSTAINED_THRESH_SEC

                if early_voice or gate_expired or sustained:
                    # Short CW burst with no voice → skip
                    skip = (
                        not sustained and not early_voice
                        and cw_frac > 0.50 and voice_frac < 0.10
                    )
                    ch._gate_start = None
                    if skip:
                        logger.debug(
                            "Gate skip CW  %s  cw=%.0f%%  voice=%.0f%%  %.1fs",
                            ch.label, cw_frac * 100, voice_frac * 100, gate_age,
                        )
                        # Begin post-skip monitoring window for voice re-evaluation
                        ch._monitor_start        = time.time()
                        ch._monitor_voice_frames = 0
                        ch._monitor_total_frames = 0
                    else:
                        self._commit_recording(ch)

            # ── Post-skip monitoring: re-evaluate if voice appears later ───────
            elif (ch.carrier and ch._gate_start is None and ch._rec_file is None
                    and ch.active_since):
                sig_mode     = self._classify_chunk(audio)
                carrier_secs = time.time() - ch.active_since

                if ch._monitor_start is not None:
                    ch._monitor_total_frames += 1
                    if sig_mode == 'voice':
                        ch._monitor_voice_frames += 1

                    mon_age    = time.time() - ch._monitor_start
                    voice_frac = ch._monitor_voice_frames / max(1, ch._monitor_total_frames)

                    if mon_age >= MONITOR_WIN_SEC:
                        if voice_frac >= MIN_VOICE_GATE_FRAC:
                            logger.debug(
                                "Voice detected post-skip on %s — committing recording",
                                ch.label,
                            )
                            self._commit_recording(ch)
                        else:
                            # Another quiet window; reset and keep watching
                            ch._monitor_start        = time.time()
                            ch._monitor_voice_frames = 0
                            ch._monitor_total_frames = 0

                # Sustained carrier override: record regardless after SUSTAINED_THRESH_SEC
                if ch._rec_file is None and carrier_secs >= SUSTAINED_THRESH_SEC:
                    logger.debug(
                        "Sustained carrier override on %s (%.0fs) — committing recording",
                        ch.label, carrier_secs,
                    )
                    self._commit_recording(ch)

            # ── Write to open recording ────────────────────────────────────────
            if ch._rec_file:
                try:
                    ch._rec_file.writeframes(pcm_bytes)
                except Exception as exc:
                    logger.warning("Recording write error on %s: %s", ch.label, exc)
                    self._close_recording(ch)

            # ── Live audio subscribers ─────────────────────────────────────────
            if has_subs and self._loop is not None:
                for q in list(ch._audio_subscribers):
                    try:
                        self._loop.call_soon_threadsafe(q.put_nowait, pcm_bytes)
                    except Exception:
                        pass  # queue full — drop chunk

            # ── Voice activity and STT ─────────────────────────────────────────
            if ch.carrier:
                rms2 = float(np.mean(audio ** 2))
                ch.voice = rms2 > VOICE_ENERGY
                if self.enable_stt and ch.voice:
                    ch._audio_buf.append(audio)
                    ch._audio_buf_sec += len(audio) / AUDIO_RATE
                    if ch._audio_buf_sec >= STT_WINDOW_SEC:
                        self._transcribe_channel(ch)
            else:
                ch.voice = False
                if self.enable_stt and ch._audio_buf_sec > 1.0:
                    self._transcribe_channel(ch)
        else:
            ch.voice = False
            ch.mode = 'idle'
            if self.enable_stt and ch._audio_buf_sec > 1.0:
                self._transcribe_channel(ch)

        # ── Carrier falling edge ───────────────────────────────────────────────
        if was_active and not ch.carrier and ch.active_since:
            ch.total_active_sec += time.time() - ch.active_since
            ch.active_since = None
            ch.mode = 'idle'

            if ch._gate_start is not None:
                # Carrier dropped before the gate could decide — discard (too short)
                logger.debug("Gate discard  %s  (carrier too short)", ch.label)
                ch._gate_start = None
                ch._prebuf_chunks.clear()
                ch._prebuf_sec = 0.0
            elif ch._rec_file:
                ch._rec_close_at = time.time() + TAIL_DELAY_SEC

            # Always clear monitoring state when the carrier drops
            ch._monitor_start        = None
            ch._monitor_voice_frames = 0
            ch._monitor_total_frames = 0

        # ── Flush tail and close recording after delay ─────────────────────────
        if ch._rec_close_at and ch._rec_file and time.time() >= ch._rec_close_at:
            ch._rec_close_at = None
            self._close_recording(ch)

    def _fm_demodulate(self, iq: np.ndarray, ch: RepeaterChannel) -> np.ndarray:
        """
        Mix the wideband IQ down to ch.freq_hz, box-decimate to AUDIO_RATE,
        FM-discriminate, apply 75 µs de-emphasis, and soft-limit.
        Phase continuity and de-emphasis IIR state are stored on `ch`.
        """
        decimate = self.sample_rate // AUDIO_RATE  # 50 for 2.4 MSPS → 48 kHz
        n = len(iq)

        # Mix down — maintain LO phase across chunks to avoid per-chunk click
        offset = ch.freq_hz - self.center_hz
        phase_vec = (
            2 * np.pi * offset / self.sample_rate * np.arange(n) + ch._lo_phase
        )
        mixed = iq * np.exp(-1j * phase_vec)
        ch._lo_phase = (ch._lo_phase + 2 * np.pi * offset / self.sample_rate * n) % (2 * np.pi)

        # Box decimate
        n_out = n // decimate
        chan = mixed[: n_out * decimate].reshape(n_out, decimate).mean(axis=1)

        # FM discriminator — prepend last sample from previous chunk for continuity
        extended = np.concatenate([[ch._last_iq], chan])
        ch._last_iq = chan[-1]
        disc = np.angle(np.conj(extended[:-1]) * extended[1:])

        # Fixed FM gain (no per-chunk peak normalization — that amplifies silence)
        audio = (disc * _RPT_FM_GAIN).astype(np.float64)

        # De-emphasis: 75 µs IIR (undoes transmitter pre-emphasis)
        if ch._zi_dem is None:
            ch._zi_dem = _DEEMPH_ZI_PROTO.copy() * float(audio[0]) if len(audio) else _DEEMPH_ZI_PROTO.copy()
        audio, ch._zi_dem = lfilter(_DEEMPH_B, _DEEMPH_A, audio, zi=ch._zi_dem)

        # Soft limiter
        return np.tanh(audio * _RPT_AUDIO_GAIN).astype(np.float32)

    # ── Signal classifier ─────────────────────────────────────────────────

    def _classify_chunk(self, audio: np.ndarray) -> str:
        """Return 'voice', 'cw', or 'carrier' for one FM-demodulated audio chunk.

        Uses spectral concentration in the 300–3000 Hz voice band.  CW tones
        produce a single dominant peak (high concentration); voice spreads energy
        across many bins (low concentration).
        """
        rms2 = float(np.mean(audio ** 2))
        if rms2 < VOICE_ENERGY * 0.3:
            return 'carrier'  # below audio floor — bare carrier, no modulation
        n = len(audio)
        if n < 64:
            return 'voice' if rms2 > VOICE_ENERGY else 'carrier'
        fft_pwr  = np.abs(np.fft.rfft(audio * np.hanning(n))) ** 2
        freqs    = np.fft.rfftfreq(n, d=1.0 / AUDIO_RATE)
        mask     = (freqs >= 300) & (freqs <= 3000)
        band_pwr = fft_pwr[mask]
        total    = float(band_pwr.sum())
        if total < 1e-9:
            return 'carrier'
        concentration = float(band_pwr.max()) / total
        return 'cw' if concentration > CW_TONE_RATIO else 'voice'

    # ── Recording commit ───────────────────────────────────────────────────

    def _commit_recording(self, ch: RepeaterChannel) -> None:
        """Open WAV file and flush pre-roll buffer, backdating start to carrier onset."""
        if not self.record_enabled:
            return
        self._open_recording(ch)
        if not ch._rec_file:
            return
        # Backdate so duration is measured from when the carrier first appeared
        if ch.active_since:
            ch._rec_start = ch.active_since
        # Flush pre-roll (all buffered chunks except current, which the main loop writes)
        for _, old_pcm in ch._prebuf_chunks[:-1]:
            try:
                ch._rec_file.writeframes(old_pcm)
            except Exception:
                pass
        ch._monitor_start        = None
        ch._monitor_voice_frames = 0
        ch._monitor_total_frames = 0

    # ── STT ───────────────────────────────────────────────────────────────

    def _load_whisper(self) -> None:
        try:
            from faster_whisper import WhisperModel  # type: ignore
            self._whisper = WhisperModel("base", device="cpu", compute_type="int8")
            logger.info("Whisper model loaded for repeater monitor STT")
        except Exception as exc:
            logger.warning("faster-whisper not available — STT disabled: %s", exc)
            self.enable_stt = False

    def _transcribe_channel(self, ch: RepeaterChannel) -> None:
        if not self._whisper or not ch._audio_buf:
            ch._audio_buf.clear()
            ch._audio_buf_sec = 0.0
            return
        audio = np.concatenate(ch._audio_buf)
        ch._audio_buf.clear()
        ch._audio_buf_sec = 0.0
        try:
            segments, _ = self._whisper.transcribe(
                audio, language="en", beam_size=1, vad_filter=True
            )
            text = " ".join(s.text.strip() for s in segments).strip()
            if text:
                ch.transcripts.append({"t": time.time(), "text": text})
                ch.transcripts = ch.transcripts[-20:]  # keep last 20
                logger.info("[%s] %s", ch.label, text)
        except Exception as exc:
            logger.debug("STT error on %s: %s", ch.label, exc)

    # ── Recording ─────────────────────────────────────────────────────────

    def _open_recording(self, ch: RepeaterChannel) -> None:
        RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^\w]", "_", ch.label)
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = RECORDINGS_DIR / f"{ts}_{slug}_{ch.freq_hz}Hz.wav"
        try:
            f = wave.open(str(path), "w")
            f.setnchannels(1)
            f.setsampwidth(2)
            f.setframerate(AUDIO_RATE)
            ch._rec_file  = f
            ch._rec_path  = str(path)
            ch._rec_start = time.time()
            logger.info("REC start  %s → %s", ch.label, path.name)
        except Exception as exc:
            logger.warning("Cannot open recording %s: %s", path, exc)
            ch._rec_file = ch._rec_path = ch._rec_start = None

    def _close_recording(self, ch: RepeaterChannel) -> None:
        if not ch._rec_file:
            return
        dur  = time.time() - (ch._rec_start or time.time())
        path = ch._rec_path
        try:
            ch._rec_file.close()
        except Exception:
            pass
        ch._rec_file = ch._rec_path = ch._rec_start = ch._rec_close_at = None
        ch._prebuf_chunks.clear()
        ch._prebuf_sec = 0.0
        ch._monitor_start        = None
        ch._monitor_voice_frames = 0
        ch._monitor_total_frames = 0
        if dur < MIN_REC_SEC and path:
            # Discard very short recordings — likely noise-floor startup blips
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
            logger.debug("REC discard  %s  %.2f s (< %.1f s)", path, dur, MIN_REC_SEC)
        else:
            logger.info("REC saved  %s  %.1f s", path, dur)

    # ── Audio subscriptions ────────────────────────────────────────────────

    def subscribe_audio(self, freq_hz: int) -> "asyncio.Queue[bytes]":
        """Return a queue that receives int16 PCM chunks for the given channel."""
        q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=60)
        for ch in self.channels:
            if ch.freq_hz == freq_hz:
                ch._audio_subscribers.append(q)
                return q
        raise ValueError(f"No channel at {freq_hz} Hz")

    def unsubscribe_audio(self, freq_hz: int, q: "asyncio.Queue[bytes]") -> None:
        for ch in self.channels:
            if ch.freq_hz == freq_hz:
                try:
                    ch._audio_subscribers.remove(q)
                except ValueError:
                    pass

    # ── Status export ──────────────────────────────────────────────────────

    def status(self) -> dict:
        uptime = round(time.time() - self.started_at, 1) if self.started_at else 0
        return {
            "running":      self._running,
            "device":       self.device,
            "center_hz":    self.center_hz,
            "sample_rate":  self.sample_rate,
            "noise_db":     round(self._noise_floor, 1),
            "uptime_sec":   uptime,
            "error":        self.error,
            "channels": [
                {
                    "freq_hz":        ch.freq_hz,
                    "label":          ch.label,
                    "carrier":        ch.carrier,
                    "voice":          ch.voice,
                    "mode":           ch.mode,
                    "power_db":       round(ch.power_db, 1),
                    "snr_db":         ch.snr_db,
                    "carrier_count":  ch.carrier_count,
                    "total_active_sec": round(ch.total_active_sec, 1),
                    "active_since":   ch.active_since,
                    "transcripts":    ch.transcripts[-5:],
                    "stream_url":     f"/api/rtl-monitor/audio/{ch.freq_hz}",
                    "recording":      ch._rec_file is not None,
                    "recording_start": ch._rec_start,
                }
                for ch in self.channels
            ],
        }


# ── Preset channel groups ─────────────────────────────────────────────────────

# Shenandoah Valley VHF repeaters (all within 145.130–147.075 MHz = 1.945 MHz span)
SHENANDOAH_REPEATERS = [
    {"freq_hz": 145_130_000, "label": "Massanutten"},
    {"freq_hz": 146_520_000, "label": "National Simplex"},
    {"freq_hz": 146_625_000, "label": "Luray Caverns"},
    {"freq_hz": 146_670_000, "label": "Stanley"},
    {"freq_hz": 146_700_000, "label": "Staunton"},
    {"freq_hz": 146_850_000, "label": "Verona"},
    {"freq_hz": 147_045_000, "label": "Elliott Knob"},
    {"freq_hz": 147_075_000, "label": "Bear Den"},
]

# Optimal center for the Shenandoah group
# midpoint of 145.130 and 147.075 = 146.1025 MHz; round to 146.100
SHENANDOAH_CENTER_HZ = 146_100_000
