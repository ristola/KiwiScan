"""
api/noaa_monitor.py — NOAA Weather Radio: IQ hub + EAS alerts.

POST /api/noaa-monitor/start           — ensure rtl_tcp running, start IQ hub
GET  /api/noaa-monitor/status          — hub + rtl_tcp status, recent EAS alerts
POST /api/noaa-monitor/stop            — stop the IQ hub
GET  /api/noaa-monitor/alerts          — list EAS alert files from sigmon-2 file server
POST /api/noaa-monitor/inject-test-alert — inject a synthetic SAME/EAS alert for testing
GET  /api/noaa-monitor/alerts/{id}/audio — proxy alert WAV from sigmon-2
GET  /api/noaa-monitor/audio/live      — stream live FM audio for a WX channel as WAV
WS   /ws/noaa-monitor                  — push status every 10 s while open

Architecture
────────────
_IQHub holds a single TCP connection to rtl_tcp at 960 kHz centred on 162.475 MHz
(which covers all 7 WX channels within its ±480 kHz span).  Each browser speaker
button subscribes by WX frequency; the hub FM-demodulates each channel independently
from the shared wideband IQ stream.  A server-side SAME decoder also subscribes to
the primary WX channel if multimon-ng is available on the host.

960 kHz was chosen because 960 000 ÷ 20 = 48 000 Hz (perfect integer decimation)
and it is stable on the R820T2 tuner chip (minimum ~250 kHz).  multimon-ng is
invoked with -s 48000 so no resampling is needed for SAME/EAS detection.

Environment variables
─────────────────────
  NOAA_DOCKER_HOST    sigmon-2 IP (default 10.13.73.195)
  NOAA_DOCKER_PORT    Docker TCP API port (default 2375)
  NOAA_ALERT_URL      Base URL of the sigmon-2 alert file server
  NOAA_RTL_TCP_PORT   rtl_tcp port on sigmon-2 (default 7373)
  NOAA_SDR_SERIAL     RTL-SDR serial number (default 00162400)
  NOAA_SAME_CHANNEL   WX channel Hz to monitor for SAME (default 162475000 = WX3)
"""

from __future__ import annotations

import asyncio
import http.client
import json
import logging
import os
import re
import struct
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
from scipy.signal import firwin, lfilter, lfilter_zi
from fastapi import APIRouter, Body, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(tags=["noaa-monitor"])

# ── Constants ─────────────────────────────────────────────────────────────────

_LOG_PATH = Path("outputs") / "noaa_alert_log.jsonl"

RTLTCP_CONTAINER  = "kiwiscan-noaa-rtltcp"
DOCKER_IMAGE      = "n4ldr/noaa-weather-radio:latest"

DOCKER_HOST       = os.environ.get("NOAA_DOCKER_HOST", "10.13.73.195")
DOCKER_PORT       = int(os.environ.get("NOAA_DOCKER_PORT", "2375"))
ALERT_URL         = os.environ.get("NOAA_ALERT_URL", "http://10.13.73.195:8180").rstrip("/")
NOAA_RTL_TCP_PORT = int(os.environ.get("NOAA_RTL_TCP_PORT", "7373"))
NOAA_SDR_SERIAL   = os.environ.get("NOAA_SDR_SERIAL", "00162400")
# RTL-TCP host the IQ hub actually connects to for IQ samples.
# May differ from DOCKER_HOST when using a pre-existing system rtl_tcp instance.
# SN 00000978 at .193 is OpenWebRX SDR-3 (NOAA WX profile, always running).
# SN 00162400 at .195 is the KiwiScan-managed container (hardware failed 2026-06).
_RTL_TCP_HOST = os.environ.get("NOAA_RTL_TCP_IQ_HOST", "10.13.73.193")

WX_CHANNELS = [162_400_000, 162_425_000, 162_450_000, 162_475_000,
               162_500_000, 162_525_000, 162_550_000]
WX_NAMES    = ["WX2", "WX4", "WX5", "WX3", "WX6", "WX7", "WX1"]

# IQ capture parameters — 960 kHz ÷ 20 = 48 kHz audio (perfect integer decimation)
_NOAA_CENTER_HZ = 162_475_000
_CAPTURE_RATE   = 960_000
_AUDIO_RATE     = 48_000
_DECIMATE       = _CAPTURE_RATE // _AUDIO_RATE   # 20
_IQ_CHUNK       = _CAPTURE_RATE // 10 * 2        # 0.1 s = 192 000 bytes of uint8 IQ

# Primary WX channel to monitor for SAME alerts (centre of capture → best SNR)
_SAME_CHANNEL_HZ = int(os.environ.get("NOAA_SAME_CHANNEL", str(162_475_000)))

# NFM channel filter — LP at ±8 kHz before FM demod.
# NOAA WX peak deviation is ±5 kHz; 8 kHz gives a 3 kHz guard band to the
# adjacent channel (25 kHz spacing → ±12.5 kHz half-bandwidth).
# Tighter than the previous ±15 kHz: reduces noise floor by ~6 dB.
_NFM_CUTOFF_HZ = 8_000
_NFM_NTAPS     = 127
_NFM_TAPS: np.ndarray = firwin(
    _NFM_NTAPS,
    _NFM_CUTOFF_HZ / (_CAPTURE_RATE / 2),
    window="hamming",
).astype(np.float32)
_NFM_ZI_PROTO: np.ndarray = lfilter_zi(_NFM_TAPS, [1.0]).astype(np.float64)

# Audio post-processing filters (applied after FM discriminator + decimation).
# De-emphasis: NOAA WX uses 75 µs pre-emphasis on transmit; we must invert it
# (1-pole IIR LPF, τ=75 µs) or voice sounds tinny and unintelligible.
_DEEMPH_TAU = 75e-6
_DEEMPH_A   = float(np.exp(-1.0 / (_AUDIO_RATE * _DEEMPH_TAU)))   # ≈ 0.757
_DEEMPH_B   = np.array([1.0 - _DEEMPH_A], dtype=np.float64)
_DEEMPH_A_  = np.array([1.0, -_DEEMPH_A],  dtype=np.float64)
_DEEMPH_ZI  = lfilter_zi(_DEEMPH_B, _DEEMPH_A_)

# Voice HPF: 300 Hz — NOAA WX voice content starts at 300 Hz (telephony standard).
# Replaces the old 100 Hz DC-block; still removes carrier-offset DC but also
# eliminates sub-audio squelch tones and low-frequency rumble.
_DC_ALPHA  = float(1.0 - 2.0 * np.pi * 300.0 / _AUDIO_RATE)       # ≈ 0.961
_DC_B      = np.array([1.0, -1.0],       dtype=np.float64)
_DC_A      = np.array([1.0, -_DC_ALPHA], dtype=np.float64)
_DC_ZI     = lfilter_zi(_DC_B, _DC_A)

_start_time:      Optional[float] = None
_active_channels: list[int]       = list(WX_CHANNELS)
_recent_eas:      list[dict]      = []   # server-side SAME alerts (ring buffer, max 50)
_dismissed_ids:   set[str]        = set()  # alert IDs reviewed by operator — excluded from polls


# ── Docker remote TCP API ─────────────────────────────────────────────────────

def _docker(method: str, path: str, body: dict | None = None,
            timeout: float = 30.0) -> tuple[int, dict | str]:
    """Call sigmon-2's Docker daemon over plain TCP (no TLS, LAN-only)."""
    conn = http.client.HTTPConnection(DOCKER_HOST, DOCKER_PORT, timeout=timeout)
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if payload else {}
    conn.request(method, path, body=payload, headers=headers)
    r   = conn.getresponse()
    raw = r.read().decode("utf-8", errors="ignore")
    conn.close()
    try:
        return r.status, json.loads(raw) if raw.strip() else {}
    except Exception:
        return r.status, raw


def _docker_stop_rm(name: str) -> None:
    _docker("POST", f"/containers/{name}/stop?t=10", timeout=15)
    _docker("DELETE", f"/containers/{name}?force=true", timeout=10)


def _rtl_tcp_reachable() -> bool:
    """Return True if _RTL_TCP_HOST:NOAA_RTL_TCP_PORT accepts a TCP connection."""
    import socket as _socket
    try:
        s = _socket.create_connection((_RTL_TCP_HOST, NOAA_RTL_TCP_PORT), timeout=2)
        s.close()
        return True
    except Exception:
        return False


def _ensure_rtl_tcp_running() -> bool:
    """Ensure rtl_tcp is available for IQ capture.

    Priority:
      1. If _RTL_TCP_HOST:port is already reachable (system-level rtl_tcp such
         as the OpenWebRX SDR-3 instance at 10.13.73.193), use it directly —
         no Docker container needed.
      2. Fall back to creating/starting the kiwiscan-noaa-rtltcp Docker container
         via the sigmon-2 Docker TCP API (DOCKER_HOST:DOCKER_PORT).
    """
    if _rtl_tcp_reachable():
        logger.debug("rtl_tcp reachable at %s:%d — using pre-existing instance",
                     _RTL_TCP_HOST, NOAA_RTL_TCP_PORT)
        return True

    try:
        s, d = _docker("GET", f"/containers/{RTLTCP_CONTAINER}/json", timeout=5)
        if s == 200 and isinstance(d, dict) and d.get("State", {}).get("Running"):
            return True
        _docker("DELETE", f"/containers/{RTLTCP_CONTAINER}?force=true", timeout=5)
    except Exception:
        pass

    cmd = (
        "MY=$$; "
        f"for pid in $(ls /proc | grep -E '^[0-9]+$'); do "
        f"  [ \"$pid\" = \"$MY\" ] && continue; "
        f"  name=$(cat /proc/$pid/cmdline 2>/dev/null | tr '\\0' '\\n' | head -1 | xargs basename 2>/dev/null); "
        f"  c=$(cat /proc/$pid/cmdline 2>/dev/null | tr '\\0' ' '); "
        f"  case \"$name\" in "
        f"    rtl_fm) case \"$c\" in *{NOAA_SDR_SERIAL}*) echo \"kill rtl_fm $pid\"; kill -KILL $pid 2>/dev/null ;; esac ;; "
        f"    sh|bash) case \"$c\" in *source.sh*) echo \"kill source.sh $pid\"; kill -KILL $pid 2>/dev/null ;; esac ;; "
        f"  esac; "
        f"done; "
        f"sleep 0.5; "
        f"IDX=$(rtl_test -t 2>&1 | grep 'SN: {NOAA_SDR_SERIAL}' | grep -oE '[0-9]+' | head -1); "
        f"echo \"rtl_tcp on device ${{IDX:-5}} (SN {NOAA_SDR_SERIAL})\"; "
        f"exec rtl_tcp -d ${{IDX:-5}} -a {DOCKER_HOST} -p {NOAA_RTL_TCP_PORT} -s {_CAPTURE_RATE} -g 38"
    )
    body = {
        "Image": DOCKER_IMAGE,
        "Entrypoint": ["/bin/sh", "-c"],
        "Cmd": [cmd],
        "HostConfig": {
            "Privileged": True,
            "PidMode": "host",        # see and kill host processes
            "NetworkMode": "host",    # bind port visible on host network
            "Devices": [{
                "PathOnHost": "/dev/bus/usb",
                "PathInContainer": "/dev/bus/usb",
                "CgroupPermissions": "rwm",
            }],
            "RestartPolicy": {"Name": "unless-stopped"},
        },
    }
    try:
        s, d = _docker("POST", f"/containers/create?name={RTLTCP_CONTAINER}", body=body)
        if s in (200, 201):
            _docker("POST", f"/containers/{d['Id']}/start")
            time.sleep(5)
            logger.info("rtl_tcp started on sigmon-2 SN %s port %d @ %d kHz",
                        NOAA_SDR_SERIAL, NOAA_RTL_TCP_PORT, _CAPTURE_RATE // 1000)
            return True
    except Exception as exc:
        logger.warning("rtl_tcp container start failed: %s", exc)
    return False


# ── WAV header ────────────────────────────────────────────────────────────────

def _wav_header(sample_rate: int = _AUDIO_RATE, channels: int = 1, bits: int = 16) -> bytes:
    """44-byte WAV header for a streaming (unknown-length) PCM file."""
    byte_rate   = sample_rate * channels * bits // 8
    block_align = channels * bits // 8
    data_size   = 0x7FFF_FFFF
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", data_size + 36,
        b"WAVE",
        b"fmt ", 16,
        1, channels, sample_rate, byte_rate, block_align, bits,
        b"data", data_size,
    )


# ── IQ Hub ────────────────────────────────────────────────────────────────────

def _demod_channel(
    iq: np.ndarray,
    freq_hz: int,
    mixer_phase: float,
    last_iq: complex,
    zi: tuple | None,
) -> tuple[bytes, float, complex, tuple]:
    """NFM demodulator for one WX channel extracted from the wideband IQ stream.

    Steps:
      1. Complex mixer — shift desired channel to baseband.
      2. LP FIR filter (±15 kHz) — isolate WX channel; prevents 960 kHz noise
         from saturating the FM discriminator.
      3. FM discriminator — ∠(z[n] · conj(z[n-1])).
      4. Decimate 20× — 960 kHz → 48 kHz audio.
      5. De-emphasis (75 µs IIR LPF) — undo NOAA WX transmit pre-emphasis.
      6. DC-block (100 Hz IIR HPF) — remove carrier-offset DC from audio.

    All filter states are carried between calls (zi tuple) to avoid per-chunk
    transients.  Returns (int16_pcm_bytes, new_mixer_phase, new_last_iq, new_zi).
    """
    f_offset  = freq_hz - _NOAA_CENTER_HZ
    phase_inc = 2.0 * np.pi * (-f_offset) / _CAPTURE_RATE
    n         = len(iq)

    # Step 1: mix to baseband
    if f_offset != 0:
        idx         = np.arange(n, dtype=np.float32)
        iq          = iq * np.exp(1j * (mixer_phase + idx * phase_inc)).astype(np.complex64)
        mixer_phase = float((mixer_phase + n * phase_inc) % (2 * np.pi))

    # Step 2: NFM channel filter
    if zi is None:
        zi_r      = _NFM_ZI_PROTO * float(iq.real[0]) if n else _NFM_ZI_PROTO.copy()
        zi_i      = _NFM_ZI_PROTO * float(iq.imag[0]) if n else _NFM_ZI_PROTO.copy()
        zi_dem    = _DEEMPH_ZI.copy()
        zi_dc     = _DC_ZI.copy()
    else:
        zi_r, zi_i, zi_dem, zi_dc = zi

    filt_r, zi_r = lfilter(_NFM_TAPS, [1.0], iq.real.astype(np.float64), zi=zi_r)
    filt_i, zi_i = lfilter(_NFM_TAPS, [1.0], iq.imag.astype(np.float64), zi=zi_i)
    iq = (filt_r + 1j * filt_i).astype(np.complex64)

    # Step 3: FM discriminator
    z     = np.concatenate([np.array([last_iq], dtype=np.complex64), iq])
    phase = np.angle(z[1:] * np.conj(z[:-1]))

    # Step 4: decimate — 960 kHz → 48 kHz
    # Scale: use 7.5 kHz as the full-scale reference — NOAA peak deviation is ±5 kHz but
    # 75 µs pre-emphasis boosts high-freq consonants to ~±7-8 kHz before de-emphasis.
    # Using 7.5 kHz avoids hard clipping those transients while keeping good loudness.
    scale = _CAPTURE_RATE / (2.0 * np.pi * 7_500) * 0.5
    audio = phase[::_DECIMATE] * scale

    # Step 5: de-emphasis (75 µs)
    audio, zi_dem = lfilter(_DEEMPH_B, _DEEMPH_A_, audio, zi=zi_dem)

    # Step 6: DC-block (100 Hz HPF)
    audio, zi_dc = lfilter(_DC_B, _DC_A, audio, zi=zi_dc)

    pcm = (np.clip(audio, -1.0, 1.0) * 32767).astype(np.int16)

    return pcm.tobytes(), mixer_phase, complex(iq[-1]), (zi_r, zi_i, zi_dem, zi_dc)


class _IQHub:
    """Single wideband rtl_tcp connection → fan IQ to multiple audio subscribers.

    rtl_tcp serves only ONE TCP client.  This hub is that single client; all
    browser audio streams subscribe to queues that the hub fills.  This means
    all 7 WX channels can play simultaneously without fighting over SET_FREQUENCY.

    A server-side SAME decoder (multimon-ng subprocess) also subscribes to the
    primary WX channel if multimon-ng is installed on the host.  No noaa-wr
    Docker container is needed — it would conflict with this single-client model.
    """

    def __init__(self) -> None:
        self._subs:   dict[int, list[asyncio.Queue]] = {}
        # state per channel: (mixer_phase, last_iq, filter_zi | None)
        self._states: dict[int, tuple[float, complex, tuple | None]] = {}
        self._task:   asyncio.Task | None = None
        self._lock    = asyncio.Lock()
        self._same_proc: asyncio.subprocess.Process | None = None

    # ── Public API ─────────────────────────────────────────────────────────

    def subscribe(self, freq_hz: int) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._subs.setdefault(freq_hz, []).append(q)
        return q

    def unsubscribe(self, freq_hz: int, q: asyncio.Queue) -> None:
        lst = self._subs.get(freq_hz, [])
        try:
            lst.remove(q)
        except ValueError:
            pass
        # Keep SAME channel subscribed; clear filter state so it re-initialises cleanly
        if not lst and freq_hz != _SAME_CHANNEL_HZ:
            self._subs.pop(freq_hz, None)
            self._states.pop(freq_hz, None)

    async def ensure_running(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._task = asyncio.create_task(self._pump())

    @property
    def running(self) -> bool:
        return bool(self._task and not self._task.done())

    async def stop(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
            self._subs.clear()
            self._states.clear()   # clears (mixer_phase, last_iq, filter_zi) per channel
        if self._same_proc and self._same_proc.returncode is None:
            try:
                self._same_proc.terminate()
            except Exception:
                pass
        self._same_proc = None

    # ── Internal: SAME decoder ──────────────────────────────────────────────

    async def _start_same_decoder(self) -> None:
        """Start multimon-ng subprocess for server-side SAME/EAS detection.

        Feeds 48 kHz mono int16 PCM from the primary WX channel.
        Skips silently if multimon-ng is not installed on the host.
        """
        try:
            self._same_proc = await asyncio.create_subprocess_exec(
                "multimon-ng", "-t", "raw", "-s", str(_AUDIO_RATE),
                "-a", "SAME", "-a", "EAS", "-",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            # Subscribe internal queue for the primary monitoring channel
            same_q: asyncio.Queue = asyncio.Queue(maxsize=50)
            self._subs.setdefault(_SAME_CHANNEL_HZ, []).append(same_q)
            asyncio.create_task(self._feed_same(same_q))
            asyncio.create_task(self._read_same_output())
            logger.info("SAME decoder started (multimon-ng) on %d Hz", _SAME_CHANNEL_HZ)
        except FileNotFoundError:
            logger.debug("multimon-ng not found — server-side SAME decoding disabled")
            self._same_proc = None

    async def _feed_same(self, q: asyncio.Queue) -> None:
        """Pump FM-demodulated PCM from the primary WX channel into multimon-ng stdin."""
        proc = self._same_proc
        try:
            while proc and proc.returncode is None:
                try:
                    pcm = await asyncio.wait_for(q.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                if proc.stdin:
                    try:
                        proc.stdin.write(pcm)
                        await proc.stdin.drain()
                    except Exception:
                        break
        except asyncio.CancelledError:
            pass

    async def _read_same_output(self) -> None:
        """Parse multimon-ng stdout for ZCZC SAME messages → _recent_eas."""
        proc = self._same_proc
        if not proc or not proc.stdout:
            return
        try:
            async for line_bytes in proc.stdout:
                line = line_bytes.decode("utf-8", errors="ignore").strip()
                if "ZCZC" in line:
                    # SAME protocol sends each message 3× — deduplicate by raw content within 90s
                    now_ts = time.time()
                    is_dup = any(
                        e.get("raw", "").strip() == line.strip() and
                        abs(now_ts - _iso_to_epoch(e.get("received_at", ""))) < 90
                        for e in _recent_eas[-6:]
                    )
                    if is_dup:
                        logger.debug("SAME duplicate suppressed: %s", line[:60])
                        continue
                    logger.info("SAME alert: %s", line)
                    entry: dict = {"raw": line, "received_at": datetime.utcnow().isoformat()}
                    entry.update(_parse_same(line))
                    _recent_eas.append(entry)
                    if len(_recent_eas) > 50:
                        _recent_eas.pop(0)
                    # Fetch NWS text in background without blocking the reader
                    asyncio.create_task(_attach_nws_text(entry))
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("SAME reader: %s", exc)

    # ── Internal: IQ pump ───────────────────────────────────────────────────

    async def _pump(self) -> None:
        """Main loop: hold one rtl_tcp connection, fan IQ to all subscribers."""
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        loop   = asyncio.get_running_loop()

        await self._start_same_decoder()

        def _close() -> None:
            nonlocal reader, writer
            if writer:
                try:
                    writer.close()
                except Exception:
                    pass
            reader = writer = None

        while True:
            # Idle when no subscribers (SAME internal queue counts too)
            if not self._subs:
                _close()
                await asyncio.sleep(0.5)
                continue

            # Connect (or reconnect) to rtl_tcp
            if reader is None or reader.at_eof():
                _close()
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(_RTL_TCP_HOST, NOAA_RTL_TCP_PORT),
                        timeout=5.0,
                    )
                    hello = await asyncio.wait_for(reader.readexactly(12), timeout=5.0)
                    if hello[:4] != b"RTL0":
                        raise ValueError(f"bad magic: {hello!r}")

                    def _cmd(c: int, v: int) -> bytes:
                        return bytes([c]) + v.to_bytes(4, "big")

                    writer.write(
                        _cmd(0x03, 1) +               # SET_GAIN_MODE: manual (avoid AGC saturation)
                        _cmd(0x04, 0) +               # SET_TUNER_GAIN: 0 dB — NOAA is a strong
                        _cmd(0x08, 0) +               # SET_RTL_AGC: off — local signal, low gain
                        _cmd(0x02, _CAPTURE_RATE) +   # SET_SAMPLE_RATE: 960 kHz
                        _cmd(0x01, _NOAA_CENTER_HZ)   # SET_FREQUENCY: centre of all 7 WX
                    )
                    await writer.drain()
                    await asyncio.sleep(0.15)
                    logger.info("IQ hub connected @ %.3f MHz / %d kHz",
                                _NOAA_CENTER_HZ / 1e6, _CAPTURE_RATE // 1000)

                except Exception as exc:
                    logger.warning("IQ hub connect failed: %s", exc)
                    _close()
                    started = await loop.run_in_executor(None, _ensure_rtl_tcp_running)
                    await asyncio.sleep(3.0 if started else 5.0)
                    continue

            # Read one IQ chunk from rtl_tcp
            try:
                raw = await asyncio.wait_for(reader.read(_IQ_CHUNK), timeout=3.0)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                logger.debug("IQ hub read: %s", exc)
                _close()
                continue

            if not raw:
                _close()
                continue

            # Parse uint8 IQ → complex64 (ensure even byte count)
            u8 = np.frombuffer(raw, dtype=np.uint8).astype(np.float32)
            if len(u8) < 2:
                continue
            u8 = u8[: len(u8) & ~1]
            iq = ((u8[0::2] - 127.5) + 1j * (u8[1::2] - 127.5)) / 127.5

            # NFM-demodulate each subscribed channel and push PCM to its queues
            for freq_hz, queues in list(self._subs.items()):
                if not queues:
                    continue
                mp, li, zi = self._states.get(freq_hz, (0.0, complex(1, 0), None))  # zi=None → all filters init fresh
                try:
                    pcm, mp, li, zi = await loop.run_in_executor(
                        None, _demod_channel, iq.copy(), freq_hz, mp, li, zi
                    )
                except Exception as exc:
                    logger.debug("demod %d Hz: %s", freq_hz, exc)
                    continue
                self._states[freq_hz] = (mp, li, zi)

                dead: list = []
                for q in queues:
                    try:
                        q.put_nowait(pcm)
                    except asyncio.QueueFull:
                        dead.append(q)   # slow consumer — drop rather than stall
                for q in dead:
                    queues.remove(q)


_iq_hub = _IQHub()


# ── Alert helpers (sigmon-2 HTTP file server + server-side ring buffer) ────────

def _iso_to_epoch(iso: str) -> float:
    """Convert an ISO-8601 string to a Unix timestamp; returns 0.0 on failure."""
    try:
        return datetime.fromisoformat(iso.rstrip("Z")).timestamp()
    except Exception:
        return 0.0


def _parse_same(text: str) -> dict:
    m = re.search(
        r"ZCZC-(\w+)-(\w+)-([\d\-+]+)\+(\d{4})-(\d{7})-(\w+)/NWS", text
    )
    if not m:
        return {}
    org, event, areas_raw, duration, issue_ts, station = m.groups()
    dur_h, dur_m = int(duration[:2]), int(duration[2:])
    return {
        "org": org, "event": event,
        "areas": re.findall(r"\d{6}", areas_raw),
        "duration_min": dur_h * 60 + dur_m,
        "issue_day":  int(issue_ts[:3]),
        "issue_hour": int(issue_ts[3:5]),
        "issue_min":  int(issue_ts[5:7]),
        "station":    station,
    }


def _list_alerts(limit: int = 50) -> list:
    """Merge server-side SAME ring buffer with sigmon-2 HTTP file server alerts.

    Alerts whose IDs are in _dismissed_ids (reviewed by the operator) are excluded.
    """
    combined: list[dict] = [
        e for e in _recent_eas[-limit:] if e.get("id") not in _dismissed_ids
    ]
    try:
        with urllib.request.urlopen(ALERT_URL + "/", timeout=5) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        fnames = sorted(set(re.findall(r'href="(EAS_[^"]+\.txt)"', html)), reverse=True)[:limit]
        for fname in fnames:
            try:
                with urllib.request.urlopen(ALERT_URL + "/" + fname, timeout=5) as r:
                    content = r.read().decode("utf-8", errors="ignore")
                alert_id = fname.replace(".txt", "")
                if alert_id in _dismissed_ids:
                    continue
                has_audio = False
                try:
                    req = urllib.request.Request(
                        ALERT_URL + "/" + alert_id + ".wav", method="HEAD"
                    )
                    urllib.request.urlopen(req, timeout=3)
                    has_audio = True
                except Exception:
                    pass
                entry = {
                    "id": alert_id, "filename": fname, "has_audio": has_audio,
                    "raw": content.strip(), "received_at": datetime.utcnow().isoformat(),
                }
                entry.update(_parse_same(content))
                combined.append(entry)
            except Exception:
                pass
    except Exception as exc:
        logger.debug("noaa alert list fetch: %s", exc)
    return combined[:limit]


# SAME event-code → NWS event name (prefix-matched against API response)
_SAME_EVENT_NAMES: dict[str, str] = {
    "TOR": "Tornado Warning",             "TOA": "Tornado Watch",
    "SVR": "Severe Thunderstorm Warning", "SVA": "Severe Thunderstorm Watch",
    "FFW": "Flash Flood Warning",         "FFA": "Flash Flood Watch",
    "FLW": "Flood Warning",
    "HUW": "Hurricane Warning",           "HUA": "Hurricane Watch",
    "WSW": "Winter Storm Warning",        "WSA": "Winter Storm Watch",
    "BZW": "Blizzard Warning",
    "HWW": "High Wind Warning",           "HWA": "High Wind Watch",
    "EAN": "Emergency Action Notification",
    "LAE": "Local Area Emergency",        "LEW": "Law Enforcement Warning",
    "RMT": "Required Monthly Test",       "RWT": "Required Weekly Test",
    "NPT": "National Periodic Test",
    "ADR": "Administrative Message",      "EAT": "Emergency Action Termination",
}

_NWS_API = "https://api.weather.gov"
_NWS_HEADERS = {"User-Agent": "KiwiScan-NOC/1.0 (kiwiscan@shackmate.net)", "Accept": "application/geo+json"}


def _lookup_nws_text(areas: list[str], event_code: str) -> str | None:
    """Query NWS public API for the human-readable text of a SAME alert.

    Uses the first FIPS code from the SAME message as a filter.  Returns the
    'description' field (full text) or 'headline' as fallback.  Returns None
    on any failure so the caller can degrade gracefully.
    """
    if not areas:
        return None
    event_name = _SAME_EVENT_NAMES.get(event_code, "")
    try:
        url = f"{_NWS_API}/alerts/active?status=actual&same={areas[0]}&limit=20"
        req = urllib.request.Request(url, headers=_NWS_HEADERS)
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read())
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            if event_name and not props.get("event", "").startswith(event_name[:6]):
                continue
            text = (props.get("description") or props.get("headline") or "").strip()
            if text:
                return text
    except Exception as exc:
        logger.debug("NWS text lookup failed (%s %s): %s", event_code, areas, exc)
    return None


async def _attach_nws_text(entry: dict) -> None:
    """Background task: fetch NWS alert text and attach to the entry dict in-place."""
    areas      = entry.get("areas", [])
    event_code = entry.get("event", "")
    if not areas or not event_code:
        return
    loop = asyncio.get_running_loop()
    text = await loop.run_in_executor(None, _lookup_nws_text, areas, event_code)
    if text:
        entry["message"] = text
        logger.info("NWS text attached for %s: %d chars", event_code, len(text))


def _channel_payload(running: bool) -> list[dict]:
    return [
        {
            "freq_hz":    f,
            "name":       n,
            "monitoring": running and f in _active_channels,
            "stream_url": f"/api/noaa-monitor/audio/live?freq={f}",
        }
        for f, n in zip(WX_CHANNELS, WX_NAMES)
    ]


# ── Endpoints ─────────────────────────────────────────────────────────────────

class NoaaStartRequest(BaseModel):
    device:        str            # kept for API compat (ignored — hub connects internally)
    channels:      List[int]      = WX_CHANNELS
    enable_eas:    bool           = True
    record_alerts: bool           = True


@router.get("/api/noaa-monitor/status")
def noaa_status() -> dict:
    running = _iq_hub.running
    uptime  = round(time.time() - _start_time) if running and _start_time else 0
    rtltcp_up = False
    try:
        s, d  = _docker("GET", f"/containers/{RTLTCP_CONTAINER}/json", timeout=3)
        rtltcp_up = s == 200 and bool(d.get("State", {}).get("Running"))
    except Exception:
        pass
    return {
        "running":      running,
        "rtltcp_up":    rtltcp_up,
        "capture_rate": _CAPTURE_RATE,
        "center_hz":    _NOAA_CENTER_HZ,
        "docker_host":  f"{DOCKER_HOST}:{DOCKER_PORT}",
        "uptime_sec":   uptime,
        "channels":     _channel_payload(running),
        "alerts":       _list_alerts(10),
    }


@router.post("/api/noaa-monitor/start")
async def noaa_start(req: NoaaStartRequest) -> dict:
    global _start_time, _active_channels
    _active_channels = list(req.channels)

    loop = asyncio.get_running_loop()
    rtltcp_ok = await loop.run_in_executor(None, _ensure_rtl_tcp_running)
    if not rtltcp_ok:
        raise HTTPException(503, "rtl_tcp could not be started on sigmon-2")

    await _iq_hub.ensure_running()
    _start_time = time.time()

    return {
        "status":       "running",
        "capture_rate": _CAPTURE_RATE,
        "center_hz":    _NOAA_CENTER_HZ,
    }


@router.post("/api/noaa-monitor/stop")
async def noaa_stop() -> dict:
    global _start_time
    await _iq_hub.stop()
    _start_time = None
    return {"status": "stopped"}


@router.get("/api/noaa-monitor/alerts")
def noaa_alerts(limit: int = Query(50, le=200)) -> list:
    return _list_alerts(limit)


@router.post("/api/noaa-monitor/inject-test-alert")
def noaa_inject_test_alert(event_code: str = Query("RWT")) -> dict:
    """Inject a synthetic SAME/EAS alert for UI and logging tests.

    Builds a valid ZCZC message (WXR org, four Shenandoah-area VA FIPS codes,
    30-minute duration) and appends it to the server-side ring buffer exactly
    as a real multimon-ng decode would.  No SDR hardware is touched.
    """
    if not re.fullmatch(r"[A-Z]{3}", event_code):
        raise HTTPException(400, "event_code must be exactly 3 uppercase letters (e.g. TOR, RWT)")
    now = datetime.utcnow()
    julian = now.timetuple().tm_yday
    # Clarke, Warren, Shenandoah, Frederick counties VA
    raw = (
        f"ZCZC-WXR-{event_code}-051107-051131-051171-051069"
        f"+0030-{julian:03d}{now.hour:02d}{now.minute:02d}-NWSROA/NWS"
    )
    event_label = _SAME_EVENT_NAMES.get(event_code, event_code)
    entry: dict = {
        "id":          f"TEST-{event_code}-{int(time.time())}",
        "raw":         raw,
        "received_at": now.isoformat() + "Z",
        "has_audio":   False,
        "is_test":     True,
        "message": (
            f"[SIMULATED TEST — NOT A REAL ALERT]\n\n"
            f"The National Weather Service in Roanoke, Virginia has issued a "
            f"{event_label} for the following counties in Virginia: "
            f"Clarke, Warren, Shenandoah, Frederick — until "
            f"{now.strftime('%I:%M %p')} UTC. "
            f"This is a synthetic alert injected for KiwiScan NOC testing purposes."
        ),
    }
    entry.update(_parse_same(raw))
    _recent_eas.append(entry)
    if len(_recent_eas) > 50:
        _recent_eas.pop(0)
    logger.info("TEST EAS alert injected: %s  raw=%s", event_code, raw)
    return {"ok": True, "alert": entry}


@router.post("/api/noaa-monitor/log")
async def noaa_log_entry(body: dict = Body(...)) -> dict:
    """Append a reviewed EAS alert to the persistent operator log (JSONL).

    Also dismisses the alert from the in-memory ring buffer and the dismissed-IDs
    set so it no longer appears in status polls or WebSocket pushes.
    """
    global _recent_eas
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {**body, "logged_at": datetime.utcnow().isoformat() + "Z"}
    with _LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    alert_id = body.get("id")
    if alert_id:
        _dismissed_ids.add(alert_id)
        _recent_eas = [e for e in _recent_eas if e.get("id") != alert_id]
    logger.info("EAS alert reviewed+logged: %s %s", entry.get("event"), alert_id)
    return {"ok": True, "logged_at": entry["logged_at"]}


@router.get("/api/noaa-monitor/log")
def noaa_get_log(limit: int = Query(200, le=2000)) -> list:
    """Return recent operator-reviewed EAS alert log entries, newest first."""
    if not _LOG_PATH.exists():
        return []
    lines = _LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
    entries: list[dict] = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except Exception:
            pass
    return list(reversed(entries))


@router.delete("/api/noaa-monitor/log/entry")
def noaa_delete_log_entry(logged_at: str = Query(...)) -> dict:
    """Remove a single entry from the EAS alert log by its logged_at timestamp."""
    if not _LOG_PATH.exists():
        return {"ok": True, "removed": 0}
    lines = _LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
    kept: list[str] = []
    removed = 0
    for line in lines:
        try:
            if json.loads(line).get("logged_at") == logged_at:
                removed += 1
                continue
        except Exception:
            pass
        kept.append(line)
    _LOG_PATH.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    return {"ok": True, "removed": removed}


@router.delete("/api/noaa-monitor/log")
def noaa_clear_log() -> dict:
    """Clear the entire EAS alert log file."""
    if _LOG_PATH.exists():
        _LOG_PATH.unlink()
    return {"ok": True}


@router.get("/api/noaa-monitor/alerts/{alert_id}/audio")
async def noaa_alert_audio(alert_id: str) -> StreamingResponse:
    """Proxy an EAS alert WAV from sigmon-2's file server to the browser."""
    if not re.fullmatch(r"[\w\-]+", alert_id):
        raise HTTPException(400, "Invalid alert ID")
    wav_url = f"{ALERT_URL}/{alert_id}.wav"
    try:
        remote = urllib.request.urlopen(wav_url, timeout=10)
    except Exception:
        raise HTTPException(404, "Audio not found on sigmon-2")

    def _stream():
        while chunk := remote.read(65536):
            yield chunk

    return StreamingResponse(_stream(), media_type="audio/wav")


@router.get("/api/noaa-monitor/audio/live")
async def noaa_audio_live(freq: int = Query(..., description="WX frequency in Hz")) -> StreamingResponse:
    """Stream live FM audio for one NOAA WX channel as WAV PCM.

    The IQ hub is started automatically if it is not already running.
    Multiple channels can stream simultaneously from the same wideband capture.
    """
    if freq not in WX_CHANNELS:
        raise HTTPException(400, f"freq must be one of {WX_CHANNELS}")

    await _iq_hub.ensure_running()
    q = _iq_hub.subscribe(freq)

    async def _generate():
        yield _wav_header()
        try:
            while True:
                pcm = await asyncio.wait_for(q.get(), timeout=10.0)
                yield pcm
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        finally:
            _iq_hub.unsubscribe(freq, q)

    return StreamingResponse(
        _generate(),
        media_type="audio/wav",
        headers={"Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff"},
    )


@router.websocket("/ws/noaa-monitor")
async def noaa_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            running = _iq_hub.running
            uptime  = round(time.time() - _start_time) if running and _start_time else 0
            await websocket.send_text(json.dumps({
                "running":    running,
                "uptime_sec": uptime,
                "channels":   _channel_payload(running),
                "alerts":     _list_alerts(5),
            }))
            await asyncio.sleep(10)
    except (WebSocketDisconnect, Exception):
        pass
