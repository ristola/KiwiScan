"""
VHF Digital Monitor Service — FastAPI
Decodes FT8, FT4, WSPR (via jt9/wsprd) and APRS/Packet (via direwolf)
from a single RTL-SDR IQ stream.

POST /start    — start monitoring with channel config
POST /stop     — stop
GET  /status   — running state + per-mode decode counts
GET  /decodes  — recent decoded frames (JSON array)
DELETE /decodes — clear decode history
GET  /defaults — built-in channel list
WS   /ws       — 2-second push of status + last 20 decodes
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import re
import struct
import subprocess
import tempfile
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import socket as _socket

import numpy as np
from scipy import signal as sp_signal
from fastapi import FastAPI, Body, HTTPException, Query, WebSocket, WebSocketDisconnect


# ── Minimal rtl_tcp client ────────────────────────────────────────────────────
# pyrtlsdr 0.3's RtlSdrTcpClient uses a custom ACK protocol incompatible with
# the standard rtl_tcp server (librtlsdr).  This class speaks the real protocol:
# connect → 12-byte magic header → send 5-byte command frames → stream uint8 IQ.

class _RtlTcpClient:
    CMD_SET_FREQ        = 0x01
    CMD_SET_SAMPLE_RATE = 0x02
    CMD_SET_GAIN_MODE   = 0x03  # 0 = auto, 1 = manual
    CMD_SET_GAIN        = 0x04  # gain × 10 in dB

    def __init__(self, hostname: str, port: int, timeout: float = 10.0):
        self._sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        self._sock.settimeout(timeout)
        self._sock.connect((hostname, port))
        # Read 12-byte magic: "RTL0" + 4-byte tuner type + 4-byte gain count
        header = self._recv_exact(12)
        if header[:4] != b'RTL0':
            raise ConnectionError(f'Bad rtl_tcp magic: {header[:4]!r}')
        self._sock.settimeout(None)  # blocking reads for IQ stream

    def _recv_exact(self, n: int) -> bytes:
        buf = b''
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise EOFError('rtl_tcp connection closed')
            buf += chunk
        return buf

    def _cmd(self, cmd: int, value: int) -> None:
        self._sock.sendall(struct.pack('>BI', cmd, value))

    @property
    def sample_rate(self) -> int:
        return self._sample_rate

    @sample_rate.setter
    def sample_rate(self, value: int) -> None:
        self._sample_rate = value
        self._cmd(self.CMD_SET_SAMPLE_RATE, value)

    @property
    def center_freq(self) -> int:
        return self._center_freq

    @center_freq.setter
    def center_freq(self, value: int) -> None:
        self._center_freq = value
        self._cmd(self.CMD_SET_FREQ, value)

    @property
    def gain(self):
        return self._gain

    @gain.setter
    def gain(self, value) -> None:
        self._gain = value
        if str(value).lower() == 'auto':
            self._cmd(self.CMD_SET_GAIN_MODE, 0)
        else:
            self._cmd(self.CMD_SET_GAIN_MODE, 1)
            self._cmd(self.CMD_SET_GAIN, int(float(value) * 10))

    def read_bytes(self, n: int) -> bytes:
        return self._recv_exact(n)

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)
logger = logging.getLogger("vhf-digital")

# ── Mode constants ────────────────────────────────────────────────────────────

MODE_FT8  = "FT8"
MODE_FT4  = "FT4"
MODE_WSPR = "WSPR"
MODE_APRS = "APRS"
MODE_PKT  = "Packet"

WINDOWED_MODES = {MODE_FT8, MODE_FT4, MODE_WSPR}
FM_MODES       = {MODE_APRS, MODE_PKT}

# Decode-window lengths (seconds, aligned to UTC)
WINDOW_SECS = {MODE_FT8: 15.0, MODE_FT4: 7.5, MODE_WSPR: 120.0}

# SDR / audio parameters
SAMPLE_RATE_DEFAULT = 2_400_000
AUDIO_RATE          = 12_000   # jt9 / wsprd expect 12 kHz
APRS_RATE           = 48_000   # direwolf default
BLOCK_SIZE          = 256 * 1024  # ~107 ms at 2.4 MS/s

# ── Default channel list ──────────────────────────────────────────────────────

DEFAULT_CHANNELS: list[dict] = [
    {"freq_hz": 144_174_000, "label": "FT8",               "mode": MODE_FT8,  "enabled": True},
    {"freq_hz": 144_170_000, "label": "FT4",               "mode": MODE_FT4,  "enabled": True},
    {"freq_hz": 144_390_000, "label": "APRS",              "mode": MODE_APRS, "enabled": True},
    {"freq_hz": 144_490_500, "label": "WSPR",              "mode": MODE_WSPR, "enabled": True},
    {"freq_hz": 145_010_000, "label": "Packet",            "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_030_000, "label": "Packet",            "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_050_000, "label": "Packet",            "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_070_000, "label": "Packet",            "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_090_000, "label": "Packet",            "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_130_000, "label": "Packet Massanutten","mode": MODE_PKT,  "enabled": True},
    {"freq_hz": 145_570_000, "label": "Packet Page Co",    "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_690_000, "label": "Packet WV",         "mode": MODE_PKT,  "enabled": False},
    {"freq_hz": 145_730_000, "label": "Packet VDEN",       "mode": MODE_PKT,  "enabled": False},
]

# ── Audio DSP helpers ─────────────────────────────────────────────────────────

def _lpf_sos(cutoff: float, fs: float, order: int = 8):
    return sp_signal.butter(order, cutoff / (fs / 2.0), btype="low", output="sos")

def _bpf_sos(lo: float, hi: float, fs: float, order: int = 6):
    nyq = fs / 2.0
    return sp_signal.butter(order, [lo / nyq, hi / nyq], btype="band", output="sos")

def _mix(iq: np.ndarray, offset_hz: float, fs: float) -> np.ndarray:
    n = np.arange(len(iq), dtype=np.float32)
    return (iq * np.exp((-1j * 2 * math.pi * offset_hz / fs * n)).astype(np.complex64))

def _decimate_complex(sig: np.ndarray, factor: int, sos) -> np.ndarray:
    r = sp_signal.sosfilt(sos, sig.real) + 1j * sp_signal.sosfilt(sos, sig.imag)
    return r[::factor].astype(np.complex64)

def _fm_demod(iq: np.ndarray, deviation: float = 5000.0) -> np.ndarray:
    diff = np.angle(np.conj(iq[:-1]) * iq[1:])
    return (diff / (2 * math.pi * deviation / APRS_RATE)).astype(np.float32)

def _usb_demod(iq: np.ndarray) -> np.ndarray:
    return iq.real.astype(np.float32)

def _float_to_pcm16(audio: np.ndarray) -> bytes:
    return (np.clip(audio, -1.0, 1.0) * 32767).astype(np.int16).tobytes()

def _write_wav(path: str, audio: np.ndarray, rate: int) -> None:
    pcm = _float_to_pcm16(audio)
    with open(path, "wb") as f:
        f.write(b"RIFF"); f.write(struct.pack("<I", 36 + len(pcm)))
        f.write(b"WAVE")
        f.write(b"fmt "); f.write(struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16))
        f.write(b"data"); f.write(struct.pack("<I", len(pcm))); f.write(pcm)

# ── Decode store ──────────────────────────────────────────────────────────────

class DecodeStore:
    def __init__(self, maxlen: int = 2000):
        self._q: deque[dict] = deque(maxlen=maxlen)
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def add(self, frame: dict) -> None:
        with self._lock:
            self._q.appendleft(frame)
            m = frame.get("mode", "?")
            self._counts[m] = self._counts.get(m, 0) + 1

    def get(self, limit: int = 200, mode: str | None = None) -> list[dict]:
        with self._lock:
            items = list(self._q)
        if mode:
            items = [x for x in items if x.get("mode") == mode]
        return items[:limit]

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def clear(self) -> None:
        with self._lock:
            self._q.clear()
            self._counts.clear()

# ── Direwolf decoder (APRS / Packet) ─────────────────────────────────────────

_DIREWOLF_CONF = "CHANNEL 0\nMYCALL NOCALL\nMODEM 1200\nKISSPORT 0\nAGWPORT 0\n"

# Minimal ALSA config: map default PCM/CTL to the null plugin so direwolf can
# initialise without a real sound card being present in the container.
_ALSA_NULL_CONF = """\
pcm.!default {
  type null
}
ctl.!default {
  type null
}
"""

_ALSA_CONF_PATH = "/etc/asound.conf"

def _ensure_alsa_null() -> None:
    if not os.path.exists(_ALSA_CONF_PATH):
        try:
            with open(_ALSA_CONF_PATH, "w") as f:
                f.write(_ALSA_NULL_CONF)
        except OSError as exc:
            logger.warning("Could not write %s: %s", _ALSA_CONF_PATH, exc)

# Standard uncompressed APRS position: DDMMssN/DDDMMssW  (ss = decimal seconds/minutes)
_APRS_POS_RE = re.compile(r"(\d{2})(\d{2}\.\d+)([NS]).{0,40}?(\d{3})(\d{2}\.\d+)([EW])")

# Direwolf human-readable decoded position: "N 38 30.4400, W 078 43.5600"
_DW_POS_RE = re.compile(
    r"([NS])\s+(\d+)\s+(\d+\.\d+),\s*([EW])\s+(\d+)\s+(\d+\.\d+)"
)

# Direwolf channel prefix on frame lines: "[0]" "[0 L]" "[0 H]" etc.
_DW_PREFIX = re.compile(r"^\[[^\]]+\]\s*")


class DirewolfDecoder:
    def __init__(self, freq_hz: int, label: str, mode: str, store: DecodeStore):
        self.freq_hz = freq_hz
        self.label   = label
        self.mode    = mode
        self._store  = store
        self._proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        _ensure_alsa_null()
        conf = tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False)
        conf.write(_DIREWOLF_CONF)
        conf.close()
        self._proc = subprocess.Popen(
            ["direwolf", "-c", conf.name, "-r", str(APRS_RATE), "-b", "16", "-n", "1", "-t", "0", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        threading.Thread(target=self._read_loop, daemon=True, name=f"dw-{self.freq_hz}").start()
        threading.Thread(target=self._err_loop, daemon=True, name=f"dw-err-{self.freq_hz}").start()
        logger.info("Direwolf started — %s %.3f MHz", self.mode, self.freq_hz / 1e6)

    def feed(self, pcm: bytes) -> None:
        if self._proc and self._proc.stdin:
            try:
                self._proc.stdin.write(pcm)
                self._proc.stdin.flush()
            except OSError:
                pass

    def _read_loop(self) -> None:
        # Keep state across lines so compressed/MicE frames can use the
        # human-readable lat/lon that Direwolf prints on a following line.
        # pending is only cleared when a new frame line arrives or position is found.
        pending: dict = {}

        while self._proc:
            line = self._proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if not text or text.startswith("#"):
                continue

            # Strip Direwolf channel prefix "[0] " / "[0 L] " / "[0 H]" etc.
            clean = _DW_PREFIX.sub("", text)

            if ">" in clean:
                # APRS frame line: CALLSIGN>PATH:payload
                callsign = clean.split(">")[0].strip()
                lat, lon = self._parse_pos(clean)
                comment  = self._extract_comment(clean)
                if lat is not None:
                    # Uncompressed position found inline — emit immediately
                    self._emit(callsign, clean, comment, lat, lon)
                    pending = {}
                else:
                    # Compressed / MicE — watch following lines for decoded position
                    pending = {"callsign": callsign, "message": clean, "comment": comment}

            elif pending:
                # Search every continuation line for the decoded position.
                # Don't clear pending on unrecognised lines — Direwolf outputs
                # multiple info lines (Position, Speed, Comment, Symbol…) before
                # the N/S lat/lon line and their set varies by frame type.
                m = _DW_POS_RE.search(clean)
                if m:
                    ns, d_lat, m_lat, ew, d_lon, m_lon = m.groups()
                    lat = int(d_lat) + float(m_lat) / 60
                    lon = int(d_lon) + float(m_lon) / 60
                    if ns == "S": lat = -lat
                    if ew == "W": lon = -lon
                    self._emit(
                        pending["callsign"], pending["message"],
                        pending.get("comment", ""), round(lat, 5), round(lon, 5),
                    )
                    pending = {}

    def _err_loop(self) -> None:
        while self._proc:
            line = self._proc.stderr.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                logger.info("direwolf: %s", text)

    def _parse_pos(self, text: str):
        """Extract lat/lon from an uncompressed APRS position string."""
        m = _APRS_POS_RE.search(text)
        if not m:
            return None, None
        lat = int(m.group(1)) + float(m.group(2)) / 60
        lon = int(m.group(4)) + float(m.group(5)) / 60
        if m.group(3) == "S": lat = -lat
        if m.group(6) == "W": lon = -lon
        return round(lat, 5), round(lon, 5)

    def _extract_comment(self, frame: str) -> str:
        """Pull the human-readable comment out of an APRS frame payload."""
        try:
            payload = frame.split(":", 1)[1] if ":" in frame else ""
            # Strip position/symbol characters (first ~19 chars of most payloads)
            comment = re.sub(
                r"^[!=/@\*;][0-9 .NSEW/\\A-Za-z`'._\-\^]{8,19}", "", payload
            ).strip()
            return comment[:80]
        except Exception:
            return ""

    def _emit(self, callsign: str, message: str, comment: str, lat, lon) -> None:
        self._store.add({
            "id":          f"{self.freq_hz}_{time.time():.3f}",
            "mode":        self.mode,
            "freq_hz":     self.freq_hz,
            "label":       self.label,
            "callsign":    callsign,
            "message":     message,
            "comment":     comment,
            "lat":         lat,
            "lon":         lon,
            "received_at": datetime.now(timezone.utc).isoformat(),
        })

    def stop(self) -> None:
        if self._proc:
            try:
                self._proc.stdin.close()
                self._proc.terminate()
                self._proc.wait(timeout=3)
            except Exception:
                pass
            self._proc = None

# ── Windowed decoder (FT8 / FT4 / WSPR) ─────────────────────────────────────

class WindowedDecoder:
    def __init__(self, freq_hz: int, label: str, mode: str, store: DecodeStore):
        self.freq_hz  = freq_hz
        self.label    = label
        self.mode     = mode
        self._store   = store
        self._window  = WINDOW_SECS[mode]
        self._buf: list[np.ndarray] = []
        self._win_start = 0.0
        self._lock    = threading.Lock()
        self._running = False

    def start(self) -> None:
        self._running   = True
        w               = self._window
        self._win_start = math.floor(time.time() / w) * w
        threading.Thread(target=self._timer_loop, daemon=True,
                         name=f"wd-{self.mode}-{self.freq_hz}").start()
        logger.info("WindowedDecoder started — %s %.4f MHz (window %.1fs)",
                    self.mode, self.freq_hz / 1e6, self._window)

    def feed(self, audio: np.ndarray) -> None:
        with self._lock:
            self._buf.append(audio.copy())

    def _timer_loop(self) -> None:
        while self._running:
            now      = time.time()
            deadline = self._win_start + self._window + 0.3
            if now < deadline:
                time.sleep(min(1.0, deadline - now))
                continue
            with self._lock:
                chunks = list(self._buf)
                self._buf.clear()
                w = self._window
                self._win_start = math.floor(time.time() / w) * w
            if chunks:
                audio = np.concatenate(chunks)
                threading.Thread(target=self._decode, args=(audio,), daemon=True).start()

    def _decode(self, audio: np.ndarray) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            path = f.name
        try:
            _write_wav(path, audio, AUDIO_RATE)
            if self.mode == MODE_WSPR:
                self._run_wsprd(path)
            else:
                self._run_jt9(path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def _run_jt9(self, wav_path: str) -> None:
        flag = "--ft8" if self.mode == MODE_FT8 else "--ft4"
        try:
            r = subprocess.run(
                ["jt9", flag, "--decode", "-f", "1500", wav_path],
                capture_output=True, text=True, timeout=30,
            )
            self._ingest(r.stdout.splitlines())
        except FileNotFoundError:
            logger.warning("jt9 not found — %s decode unavailable", self.mode)
        except subprocess.TimeoutExpired:
            logger.debug("jt9 timed out for %s", self.mode)
        except Exception as exc:
            logger.debug("jt9 error: %s", exc)

    def _run_wsprd(self, wav_path: str) -> None:
        freq_mhz = f"{self.freq_hz / 1e6:.4f}"
        try:
            r = subprocess.run(
                ["wsprd", "-f", freq_mhz, wav_path],
                capture_output=True, text=True, timeout=60,
            )
            self._ingest(r.stdout.splitlines())
        except FileNotFoundError:
            logger.warning("wsprd not found — WSPR decode unavailable")
        except subprocess.TimeoutExpired:
            logger.debug("wsprd timed out")
        except Exception as exc:
            logger.debug("wsprd error: %s", exc)

    def _ingest(self, lines: list[str]) -> None:
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts    = line.split()
            callsign = next(
                (p for p in parts if len(p) >= 3 and p[0].isalpha() and any(c.isdigit() for c in p)),
                None,
            )
            self._store.add({
                "id":          f"{self.freq_hz}_{time.time():.3f}",
                "mode":        self.mode,
                "freq_hz":     self.freq_hz,
                "label":       self.label,
                "callsign":    callsign,
                "message":     line,
                "lat":         None,
                "lon":         None,
                "received_at": datetime.now(timezone.utc).isoformat(),
            })

    def stop(self) -> None:
        self._running = False

# ── Main monitor ──────────────────────────────────────────────────────────────

class VhfDigitalMonitor:
    def __init__(
        self,
        rtl_host:    str,
        rtl_port:    int,
        center_hz:   int,
        sample_rate: int,
        channels:    list[dict],
    ):
        self.rtl_host    = rtl_host
        self.rtl_port    = rtl_port
        self.center_hz   = center_hz
        self.sample_rate = sample_rate
        self.channels    = [c for c in channels if c.get("enabled", True)]
        self._running    = False
        self.error: str | None = None
        self._store      = DecodeStore()
        self._decoders: list[DirewolfDecoder | WindowedDecoder] = []
        self._sdr        = None
        self._start_ts   = 0.0

        fs = sample_rate
        # FM path  : 2.4 MHz → 48 kHz  (÷50)  LPF 12.5 kHz
        self._sos_fm   = _lpf_sos(12_500, fs)
        # APRS audio bandpass: 300 Hz – 3 kHz isolates Bell 202 tones (1200 / 2200 Hz)
        self._sos_aprs_audio = _bpf_sos(300, 3_000, APRS_RATE)
        # USB path : 2.4 MHz → 240 kHz (÷10)  LPF 3 kHz, then → 12 kHz (÷20)
        self._sos_usb1 = _lpf_sos(3_000, fs)
        self._sos_usb2 = _lpf_sos(3_000, fs / 10)

    async def start(self) -> None:
        try:
            self._sdr = _RtlTcpClient(hostname=self.rtl_host, port=self.rtl_port)
            self._sdr.sample_rate = self.sample_rate
            self._sdr.center_freq  = self.center_hz
            self._sdr.gain         = "auto"
        except Exception as exc:
            self.error = str(exc)
            logger.error("RTL-SDR connect failed: %s", exc)
            return

        for ch in self.channels:
            mode = ch.get("mode", "")
            if mode in FM_MODES:
                dec: DirewolfDecoder | WindowedDecoder = DirewolfDecoder(
                    ch["freq_hz"], ch["label"], mode, self._store
                )
            elif mode in WINDOWED_MODES:
                dec = WindowedDecoder(ch["freq_hz"], ch["label"], mode, self._store)
            else:
                continue
            try:
                dec.start()
            except FileNotFoundError as exc:
                logger.warning("Skipping %s decoder — binary missing: %s", mode, exc)
                continue
            except Exception as exc:
                logger.warning("Skipping %s decoder — start failed: %s", mode, exc)
                continue
            self._decoders.append(dec)

        if not self._decoders:
            self.error = "No decoders could start — check binary dependencies (direwolf, ft8modem)"
            logger.error(self.error)
            return

        self._running  = True
        self._start_ts = time.time()
        threading.Thread(target=self._iq_loop, daemon=True, name="iq-loop").start()

    def _iq_loop(self) -> None:
        logger.info("IQ loop started — center %.3f MHz, %d channels",
                    self.center_hz / 1e6, len(self.channels))
        blocks = 0
        while self._running:
            try:
                # rtl_tcp streams interleaved uint8 I/Q pairs; convert to complex64
                raw = self._sdr.read_bytes(BLOCK_SIZE * 2)
                buf = np.frombuffer(raw, dtype=np.uint8).astype(np.float32)
                iq  = (buf[0::2] - 127.5 + 1j * (buf[1::2] - 127.5)).astype(np.complex64)
                blocks += 1
            except Exception as exc:
                logger.error("IQ read error after %d blocks: %s", blocks, exc)
                self.error = str(exc)
                break

            try:
                for ch in self.channels:
                    offset = ch["freq_hz"] - self.center_hz
                    mode   = ch.get("mode", "")

                    if mode in FM_MODES:
                        mixed = _mix(iq, offset, self.sample_rate)
                        dec_c = _decimate_complex(mixed, 50, self._sos_fm)
                        audio = _fm_demod(dec_c)
                        audio = sp_signal.sosfilt(self._sos_aprs_audio, audio)
                        pcm   = _float_to_pcm16(audio)
                        for dec in self._decoders:
                            if isinstance(dec, DirewolfDecoder) and dec.freq_hz == ch["freq_hz"]:
                                dec.feed(pcm)

                    elif mode in WINDOWED_MODES:
                        mixed  = _mix(iq, offset, self.sample_rate)
                        stage1 = _decimate_complex(mixed, 10, self._sos_usb1)
                        stage2 = _decimate_complex(stage1, 20, self._sos_usb2)
                        audio  = _usb_demod(stage2)
                        for dec in self._decoders:
                            if isinstance(dec, WindowedDecoder) and dec.freq_hz == ch["freq_hz"]:
                                dec.feed(audio)
            except Exception as exc:
                logger.error("IQ processing error after %d blocks: %s", blocks, exc, exc_info=True)
                self.error = str(exc)
                break

        logger.info("IQ loop exited after %d blocks, _running=%s", blocks, self._running)
        self._running = False

    async def stop(self) -> None:
        self._running = False
        for dec in self._decoders:
            dec.stop()
        self._decoders.clear()
        if self._sdr:
            try:
                self._sdr.close()
            except Exception:
                pass
            self._sdr = None
        logger.info("Monitor stopped")

    def status(self) -> dict:
        uptime = int(time.time() - self._start_ts) if self._running else 0
        return {
            "running":    self._running,
            "error":      self.error,
            "rtl_host":   self.rtl_host,
            "rtl_port":   self.rtl_port,
            "center_hz":  self.center_hz,
            "channels":   self.channels,
            "counts":     self._store.counts(),
            "uptime_sec": uptime,
        }

# ── Config persistence ────────────────────────────────────────────────────────

_SAVED_CONFIG_PATH = "/opt/vhf-digital/decodes/last_config.json"

def _save_config(rtl_host: str, rtl_port: int, center_hz: int,
                 sample_rate: int, channels: list[dict]) -> None:
    try:
        with open(_SAVED_CONFIG_PATH, "w") as f:
            json.dump({"rtl_host": rtl_host, "rtl_port": rtl_port,
                       "center_hz": center_hz, "sample_rate": sample_rate,
                       "channels": channels}, f)
    except Exception as exc:
        logger.warning("Could not save config: %s", exc)

def _load_config() -> dict | None:
    try:
        with open(_SAVED_CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return None

# ── FastAPI app ───────────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _monitor
    cfg = _load_config()
    if cfg:
        enabled = [c for c in cfg.get("channels", []) if c.get("enabled", True)]
        if enabled:
            _monitor = VhfDigitalMonitor(
                cfg.get("rtl_host", os.getenv("RTL_TCP_HOST", "127.0.0.1")),
                int(cfg.get("rtl_port", os.getenv("RTL_TCP_PORT", "7374"))),
                int(cfg.get("center_hz", os.getenv("CENTER_HZ", "144950000"))),
                int(cfg.get("sample_rate", str(SAMPLE_RATE_DEFAULT))),
                enabled,
            )
            await _monitor.start()
            if _monitor.error:
                logger.error("Auto-start failed: %s", _monitor.error)
                _monitor = None
            else:
                logger.info("Auto-started from saved config — %d channels", len(_monitor._decoders))
    yield

app = FastAPI(title="VHF Digital Monitor", version="1.0.0", lifespan=lifespan)

_monitor: Optional[VhfDigitalMonitor] = None


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/defaults")
async def get_defaults():
    return {
        "channels":    DEFAULT_CHANNELS,
        "center_hz":   144_950_000,
        "sample_rate": SAMPLE_RATE_DEFAULT,
    }


@app.post("/start")
async def start(body: dict = Body(...)):
    global _monitor
    if _monitor and _monitor._running:
        raise HTTPException(409, "Already running — POST /stop first")

    rtl_host    = body.get("rtl_host",    os.getenv("RTL_TCP_HOST", "127.0.0.1"))
    rtl_port    = int(body.get("rtl_port",    os.getenv("RTL_TCP_PORT", "7374")))
    center_hz   = int(body.get("center_hz",   os.getenv("CENTER_HZ",    "144950000")))
    sample_rate = int(body.get("sample_rate", os.getenv("SAMPLE_RATE",  str(SAMPLE_RATE_DEFAULT))))
    channels    = body.get("channels", DEFAULT_CHANNELS)

    _monitor = VhfDigitalMonitor(rtl_host, rtl_port, center_hz, sample_rate, channels)
    await _monitor.start()

    if _monitor.error:
        raise HTTPException(500, _monitor.error)

    _save_config(rtl_host, rtl_port, center_hz, sample_rate, channels)
    return {"ok": True, "center_hz": center_hz, "active_channels": len(_monitor._decoders)}


@app.post("/stop")
async def stop():
    global _monitor
    if not _monitor:
        return {"ok": True, "message": "No active session"}
    await _monitor.stop()
    _monitor = None
    return {"ok": True}


@app.get("/status")
async def get_status():
    if not _monitor:
        return {"running": False}
    return _monitor.status()


@app.get("/decodes")
async def get_decodes(
    limit: int = Query(200, le=2000),
    mode:  str | None = Query(None),
):
    if not _monitor:
        return []
    return _monitor._store.get(limit, mode)


@app.delete("/decodes")
async def clear_decodes():
    if _monitor:
        _monitor._store.clear()
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            payload = _monitor.status() if _monitor else {"running": False, "counts": {}}
            payload["recent"] = _monitor._store.get(20) if _monitor else []
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(2.0)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        logger.debug("ws error: %s", exc)
