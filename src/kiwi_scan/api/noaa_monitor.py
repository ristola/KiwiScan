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

SDR assignment
──────────────
The IQ hub and the built-in multimon-ng SAME decoder both derive their rtl_tcp
host/port from config/noc_assignments.json → noaa-monitor.device (tcp://HOST:PORT).
Changing that entry and restarting the server moves both NOAA audio and SAME
decoding to the new SDR automatically.  No separate dsame.py connection needed.

Environment variables (fallbacks when noc_assignments.json is absent)
──────────────────────────────────────────────────────────────────────
  NOAA_RTL_TCP_IQ_HOST  rtl_tcp / sdr-guard host (default 10.13.73.195)
  NOAA_RTL_TCP_PORT     rtl_tcp / sdr-guard port (default 7373)
  NOAA_DOCKER_HOST      Docker API host for rtl_tcp container mgmt (default = IQ host)
  NOAA_DOCKER_PORT      Docker TCP API port (default 2375)
  NOAA_ALERT_URL        Base URL of the noaa-eas-api container (default http://10.13.73.185:4027)
  NOAA_SDR_SERIAL       RTL-SDR serial number (default 00162400)
  NOAA_SAME_CHANNEL     WX channel Hz to monitor for SAME (default 162475000 = WX3)
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
import wave
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
from scipy.signal import firwin, lfilter, lfilter_zi, resample_poly
from fastapi import APIRouter, Body, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(tags=["noaa-monitor"])

# ── Constants ─────────────────────────────────────────────────────────────────

_LOG_PATH        = Path("outputs") / "noaa_alert_log.jsonl"
_LOG_RETAIN_DAYS = 30
_NOAA_REC_DIR    = Path("outputs/recordings/noaa")
_MAX_REC_SEC     = 600   # hard cap: 10 minutes per EAS alert recording

RTLTCP_CONTAINER  = "kiwiscan-noaa-rtltcp"
DOCKER_IMAGE      = "n4ldr/noaa-weather-radio:latest"

DOCKER_PORT     = int(os.environ.get("NOAA_DOCKER_PORT", "2375"))
ALERT_URL       = os.environ.get("NOAA_ALERT_URL", "http://10.13.73.185:4027").rstrip("/")
NOAA_SDR_SERIAL = os.environ.get("NOAA_SDR_SERIAL", "00162400")


def _noaa_sdr_from_config() -> tuple[str, int]:
    """Read rtl_tcp host/port from noc_assignments.json noaa-monitor.device.

    Both the IQ hub and the built-in multimon-ng SAME decoder use this value,
    so changing the SDR in noc_assignments.json moves both automatically.
    Falls back to NOAA_RTL_TCP_IQ_HOST / NOAA_RTL_TCP_PORT env vars.
    """
    try:
        device = json.loads(
            (Path("config/noc_assignments.json")).read_text()
        ).get("noaa-monitor", {}).get("device", "")
        if device.startswith("tcp://"):
            host, _, port_str = device[6:].rpartition(":")
            if host and port_str.isdigit():
                return host, int(port_str)
    except Exception:
        pass
    return (
        os.environ.get("NOAA_RTL_TCP_IQ_HOST", "10.13.73.195"),
        int(os.environ.get("NOAA_RTL_TCP_PORT", "7373")),
    )


_RTL_TCP_HOST, NOAA_RTL_TCP_PORT = _noaa_sdr_from_config()
# Docker management API lives on the same host as the SDR by convention
DOCKER_HOST = os.environ.get("NOAA_DOCKER_HOST", _RTL_TCP_HOST)

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

# multimon-ng raw mode expects 22050 Hz; hub produces 48000 Hz → resample_poly
# GCD(48000, 22050) = 150  →  up=147, down=320
_EAS_RATE = 22_050
_EAS_UP   = 147
_EAS_DOWN = 320

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

# Voice HPF: 150 Hz — removes DC and sub-audio squelch tones while preserving
# male voice fundamentals (100–300 Hz); keeps SAME/EAS detection unaffected.
_DC_ALPHA  = float(1.0 - 2.0 * np.pi * 150.0 / _AUDIO_RATE)       # ≈ 0.980
_DC_B      = np.array([1.0, -1.0],       dtype=np.float64)
_DC_A      = np.array([1.0, -_DC_ALPHA], dtype=np.float64)
_DC_ZI     = lfilter_zi(_DC_B, _DC_A)

# Voice LPF: 3 kHz — cuts FM discriminator noise above the NOAA WX voice band.
# De-emphasis attenuates 3–8 kHz by −7 to −14 dB but doesn't eliminate it;
# this FIR removes the remainder cleanly without touching SAME/EAS tones (≤2.1 kHz).
_VOICE_LPF_NTAPS     = 65
_VOICE_LPF_TAPS: np.ndarray = firwin(
    _VOICE_LPF_NTAPS,
    3_000.0 / (_AUDIO_RATE / 2),
    window="hamming",
).astype(np.float32)
_VOICE_LPF_ZI_PROTO: np.ndarray = lfilter_zi(_VOICE_LPF_TAPS, [1.0]).astype(np.float64)

# Post-deemph audio gain for the tanh soft limiter.
# AGC is ON for maximum sensitivity (NOAA signal is weak at this antenna).
# The voice LPF at 3 kHz below suppresses the FM noise that AGC amplifies during
# quiet NOAA periods, keeping the signal-to-noise ratio audible.
_NOAA_AUDIO_GAIN = 1.6

_start_time:      Optional[float] = None
_active_channels: list[int]       = list(WX_CHANNELS)
_recent_eas:      list[dict]      = []   # server-side SAME alerts (ring buffer, max 50)

def _prune_log() -> None:
    """Remove log entries whose event date is older than _LOG_RETAIN_DAYS days."""
    if not _LOG_PATH.exists():
        return
    try:
        cutoff = datetime.utcnow().timestamp() - _LOG_RETAIN_DAYS * 86400
        kept: list[str] = []
        for line in _LOG_PATH.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                # Use received_at (event time) preferring over logged_at
                ts_str = entry.get("received_at") or entry.get("logged_at", "")
                ts = datetime.fromisoformat(ts_str.rstrip("Z")).timestamp() if ts_str else cutoff
                if ts >= cutoff:
                    kept.append(line)
            except Exception:
                kept.append(line)  # keep malformed lines rather than silently drop
        _LOG_PATH.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    except OSError:
        pass


def _load_dismissed_from_log() -> set:
    """Pre-populate dismissed IDs from the persistent log so restarts don't resurface old alerts."""
    _prune_log()
    ids: set = set()
    if not _LOG_PATH.exists():
        return ids
    try:
        for line in _LOG_PATH.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
                if entry.get("id"):
                    ids.add(entry["id"])
            except (ValueError, KeyError):
                pass
    except OSError:
        pass
    return ids

_dismissed_ids: set = _load_dismissed_from_log()  # persists reviewed alert IDs across restarts


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
    """Return True if rtl_tcp at _RTL_TCP_HOST:NOAA_RTL_TCP_PORT sends the RTL0 magic header."""
    import socket as _socket
    try:
        s = _socket.create_connection((_RTL_TCP_HOST, NOAA_RTL_TCP_PORT), timeout=2)
        s.settimeout(2)
        magic = s.recv(4)
        s.close()
        return magic == b"RTL0"
    except Exception:
        return False


def _ensure_rtl_tcp_running() -> bool:
    """Ensure rtl_tcp is available for IQ capture.

    Priority:
      1. If _RTL_TCP_HOST:port is already reachable, use it directly.
      2. Fall back to creating/starting the kiwiscan-noaa-rtltcp Docker container
         via the sigmon-2 Docker TCP API (DOCKER_HOST:DOCKER_PORT).
         On success, _RTL_TCP_HOST is updated to DOCKER_HOST so the IQ hub
         connects to the container rather than whatever stale address was set.
    """
    global _RTL_TCP_HOST
    if _rtl_tcp_reachable():
        logger.debug("rtl_tcp reachable at %s:%d — using pre-existing instance",
                     _RTL_TCP_HOST, NOAA_RTL_TCP_PORT)
        return True

    try:
        s, d = _docker("GET", f"/containers/{RTLTCP_CONTAINER}/json", timeout=5)
        if s == 200 and isinstance(d, dict) and d.get("State", {}).get("Running"):
            _RTL_TCP_HOST = DOCKER_HOST
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
            _RTL_TCP_HOST = DOCKER_HOST
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
      2. LP FIR filter (±8 kHz) — isolate WX channel; prevents 960 kHz noise
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
        zi_lpf    = _VOICE_LPF_ZI_PROTO.copy()
        zi_dc     = _DC_ZI.copy()
    else:
        zi_r, zi_i, zi_dem, zi_lpf, zi_dc = zi

    filt_r, zi_r = lfilter(_NFM_TAPS, [1.0], iq.real.astype(np.float64), zi=zi_r)
    filt_i, zi_i = lfilter(_NFM_TAPS, [1.0], iq.imag.astype(np.float64), zi=zi_i)
    iq = (filt_r + 1j * filt_i).astype(np.complex64)

    # Step 3: FM discriminator
    z     = np.concatenate([np.array([last_iq], dtype=np.complex64), iq])
    phase = np.angle(z[1:] * np.conj(z[:-1]))

    # Step 4: decimate — 960 kHz → 48 kHz
    # Normalise to ±1.0 for ±5 kHz NOAA peak deviation (FCC limit).
    scale = _CAPTURE_RATE / (2.0 * np.pi * 5_000)
    audio = phase[::_DECIMATE] * scale

    # Step 5: de-emphasis (75 µs)
    audio, zi_dem = lfilter(_DEEMPH_B, _DEEMPH_A_, audio, zi=zi_dem)

    # Step 5.5: voice LPF at 3 kHz — cuts FM noise above the NOAA voice band
    audio, zi_lpf = lfilter(_VOICE_LPF_TAPS, [1.0], audio, zi=zi_lpf)

    # Step 6: DC-block (150 Hz HPF)
    audio, zi_dc = lfilter(_DC_B, _DC_A, audio, zi=zi_dc)

    # Step 7: soft limiter — tanh(x · _NOAA_AUDIO_GAIN).
    pcm = (np.tanh(audio * _NOAA_AUDIO_GAIN) * 32767).astype(np.int16)

    return pcm.tobytes(), mixer_phase, complex(iq[-1]), (zi_r, zi_i, zi_dem, zi_lpf, zi_dc)


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
        self._same_procs: dict[int, asyncio.subprocess.Process] = {}  # freq_hz → proc
        self._last_iq_at: float = 0.0   # epoch of last received IQ chunk
        # EAS alert recording state
        self._rec_file:  wave.Wave_write | None = None
        self._rec_path:  Path | None = None
        self._rec_q:     asyncio.Queue | None = None
        self._rec_task:  asyncio.Task | None = None
        self._rec_entry: dict | None = None

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

    @property
    def connected(self) -> bool:
        return time.time() - self._last_iq_at < 15.0

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
        for proc in list(self._same_procs.values()):
            if proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        self._same_procs.clear()
        self._close_recording()

    # ── Internal: EAS alert recording ──────────────────────────────────────

    def _open_recording(self, entry: dict) -> None:
        """Open a WAV file and subscribe the recording queue on the SAME channel."""
        self._close_recording()   # close any prior open recording first
        _NOAA_REC_DIR.mkdir(parents=True, exist_ok=True)
        alert_id = entry.get("id") or f"EAS_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        safe_id  = re.sub(r"[^\w\-]", "_", alert_id)
        path     = _NOAA_REC_DIR / f"{safe_id}.wav"
        try:
            wf = wave.open(str(path), "wb")
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(_AUDIO_RATE)
            self._rec_file  = wf
            self._rec_path  = path
            self._rec_entry = entry
            self._rec_q     = asyncio.Queue(maxsize=200)
            self._subs.setdefault(_SAME_CHANNEL_HZ, []).append(self._rec_q)
            dur_sec = min(entry.get("duration_min", 15) * 60 + 30, _MAX_REC_SEC)
            self._rec_task  = asyncio.create_task(self._record_loop(self._rec_q, dur_sec))
            entry["has_audio"]       = False
            entry["local_recording"] = path.name
            logger.info("EAS recording started: %s (max %.0fs)", path.name, dur_sec)
        except Exception as exc:
            logger.warning("EAS recording open failed: %s", exc)
            self._rec_file = self._rec_path = self._rec_q = self._rec_task = None

    async def _record_loop(self, q: asyncio.Queue, max_sec: float) -> None:
        deadline = time.time() + max_sec
        try:
            while time.time() < deadline:
                try:
                    pcm = await asyncio.wait_for(q.get(), timeout=1.0)
                    if self._rec_file:
                        self._rec_file.writeframes(pcm)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            self._close_recording()

    def _close_recording(self) -> None:
        if self._rec_task and not self._rec_task.done():
            self._rec_task.cancel()
        self._rec_task = None
        if self._rec_q is not None:
            lst = self._subs.get(_SAME_CHANNEL_HZ, [])
            try:
                lst.remove(self._rec_q)
            except ValueError:
                pass
            self._rec_q = None
        if self._rec_file:
            try:
                self._rec_file.close()
            except Exception:
                pass
            if self._rec_path and self._rec_path.exists():
                frames = (self._rec_path.stat().st_size - 44) // 2
                if frames < _AUDIO_RATE * 2:   # discard if < 2 s
                    self._rec_path.unlink(missing_ok=True)
                    logger.info("EAS recording too short, discarded: %s", self._rec_path.name)
                else:
                    if self._rec_entry:
                        self._rec_entry["has_audio"]       = True
                        self._rec_entry["local_recording"] = self._rec_path.name
                    logger.info("EAS recording saved: %s", self._rec_path.name)
            self._rec_file  = None
            self._rec_path  = None
            self._rec_entry = None

    # ── Internal: SAME decoder ──────────────────────────────────────────────

    async def _drain_keep_alive(self, q: asyncio.Queue) -> None:
        """Drain the keep-alive queue so it never fills and evicts itself.

        Runs as a background task when multimon-ng is unavailable, keeping
        self._subs non-empty so the IQ hub maintains the rtl_tcp connection.
        """
        try:
            while True:
                try:
                    await asyncio.wait_for(q.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def _start_same_decoder(self) -> None:
        """Start one multimon-ng subprocess per WX channel for SAME/EAS detection.

        Each channel gets its own process fed 48 kHz mono int16 PCM.  Monitoring
        all 7 channels ensures alerts broadcast on any WX frequency are caught.
        Falls back to a single drain-only keep-alive queue when multimon-ng is not
        installed so the IQ hub stays connected at startup without browser clients.
        """
        try:
            for freq_hz in WX_CHANNELS:
                proc = await asyncio.create_subprocess_exec(
                    "multimon-ng", "-t", "raw", "-a", "EAS", "-",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                self._same_procs[freq_hz] = proc
                same_q: asyncio.Queue = asyncio.Queue(maxsize=50)
                self._subs.setdefault(freq_hz, []).append(same_q)
                asyncio.create_task(self._feed_same(same_q, proc))
                asyncio.create_task(self._read_same_output(proc, freq_hz))
            logger.info("SAME decoders started on all %d WX channels", len(WX_CHANNELS))
        except FileNotFoundError:
            logger.info("multimon-ng not found — keep-alive queue so IQ hub stays connected")
            keep_q: asyncio.Queue = asyncio.Queue(maxsize=2)
            self._subs.setdefault(_SAME_CHANNEL_HZ, []).append(keep_q)
            asyncio.create_task(self._drain_keep_alive(keep_q))

    async def _feed_same(self, q: asyncio.Queue, proc: asyncio.subprocess.Process) -> None:
        """Pump FM-demodulated PCM from a WX channel into its multimon-ng stdin.

        Resamples from _AUDIO_RATE (48000 Hz) to _EAS_RATE (22050 Hz) because
        multimon-ng raw mode hardcodes 22050 Hz sample rate.
        """
        loop = asyncio.get_running_loop()
        try:
            while proc and proc.returncode is None:
                try:
                    pcm_bytes = await asyncio.wait_for(q.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                if proc.stdin:
                    try:
                        samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
                        resampled = await loop.run_in_executor(
                            None, resample_poly, samples, _EAS_UP, _EAS_DOWN
                        )
                        out = resampled.astype(np.int16).tobytes()
                        proc.stdin.write(out)
                        await proc.stdin.drain()
                    except Exception:
                        break
        except asyncio.CancelledError:
            pass

    async def _read_same_output(self, proc: asyncio.subprocess.Process, freq_hz: int = 0) -> None:
        """Parse multimon-ng stdout for ZCZC SAME messages → _recent_eas."""
        if not proc or not proc.stdout:
            return
        try:
            async for line_bytes in proc.stdout:
                line = line_bytes.decode("utf-8", errors="ignore").strip()
                # EOM — stop recording
                if "NNNN" in line or line.strip() == "EOM":
                    logger.info("SAME EOM detected — closing recording")
                    self._close_recording()
                    continue
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
                    logger.info("SAME alert on %.3f MHz: %s", freq_hz / 1e6, line)
                    entry: dict = {"raw": line, "received_at": datetime.utcnow().isoformat()}
                    entry.update(_parse_same(line))
                    _recent_eas.append(entry)
                    if len(_recent_eas) > 50:
                        _recent_eas.pop(0)
                    # Start recording this alert's audio
                    self._open_recording(entry)
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
        _chunks = 0

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

            # Connect (or reconnect) to rtl_tcp — re-read config each attempt so
            # an SDR re-assignment in noc_assignments.json is picked up immediately.
            if reader is None or reader.at_eof():
                _close()
                _host, _port = _noaa_sdr_from_config()
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(_host, _port),
                        timeout=5.0,
                    )
                    hello = await asyncio.wait_for(reader.readexactly(12), timeout=5.0)
                    if hello[:4] != b"RTL0":
                        raise ValueError(f"bad magic: {hello!r}")

                    def _cmd(c: int, v: int) -> bytes:
                        return bytes([c]) + v.to_bytes(4, "big")

                    writer.write(
                        _cmd(0x03, 0) +               # SET_GAIN_MODE: auto (tuner AGC — NOAA WX signal is weak at this antenna)
                        _cmd(0x08, 1) +               # SET_RTL_AGC: on  (RTL2832U chip AGC for maximum sensitivity)
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

            _chunks += 1
            self._last_iq_at = time.time()
            if _chunks % 300 == 0:   # every ~30 s (300 × 0.1 s chunks)
                iq_rms = float(np.sqrt(np.mean(np.abs(iq) ** 2)))
                logger.info("NOAA-IQ heartbeat: chunk=%d iq_rms=%.4f sdr=%s:%d",
                            _chunks, iq_rms, _RTL_TCP_HOST, NOAA_RTL_TCP_PORT)

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
    """Merge server-side SAME ring buffer with EAS API alerts (JSON).

    Fetches from ALERT_URL/alerts (the noaa-eas-api container at port 4027).
    Alerts in _dismissed_ids are excluded from both sources.
    """
    combined: list[dict] = [
        e for e in _recent_eas[-limit:] if e.get("id") not in _dismissed_ids
    ]
    seen_ids = {e["id"] for e in combined if e.get("id")}
    try:
        with urllib.request.urlopen(f"{ALERT_URL}/alerts?limit={limit}", timeout=5) as resp:
            remote: list = json.loads(resp.read().decode("utf-8", errors="ignore"))
        for entry in remote:
            alert_id = entry.get("id")
            if not alert_id or alert_id in _dismissed_ids or alert_id in seen_ids:
                continue
            seen_ids.add(alert_id)
            combined.append(entry)
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
            "freq_hz":      f,
            "name":         n,
            "monitoring":   running and f in _active_channels,
            "eas_active":   running and f in _iq_hub._same_procs
                            and _iq_hub._same_procs[f].returncode is None,
            "stream_url":   f"/api/noaa-monitor/audio/live?freq={f}",
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
    running   = _iq_hub.running
    uptime    = round(time.time() - _start_time) if running and _start_time else 0
    rtltcp_up = running and _iq_hub.connected
    eas_ch    = sum(1 for p in _iq_hub._same_procs.values() if p.returncode is None)
    return {
        "running":       running,
        "rtltcp_up":     rtltcp_up,
        "eas_channels":  eas_ch,
        "capture_rate":  _CAPTURE_RATE,
        "center_hz":     _NOAA_CENTER_HZ,
        "docker_host":   f"{DOCKER_HOST}:{DOCKER_PORT}",
        "uptime_sec":    uptime,
        "channels":      _channel_payload(running),
        "alerts":        _list_alerts(10),
    }


@router.post("/api/noaa-monitor/start")
async def noaa_start(req: NoaaStartRequest) -> dict:
    global _start_time, _active_channels, _RTL_TCP_HOST, NOAA_RTL_TCP_PORT
    _active_channels = list(req.channels)

    # Use the device from the request so the IQ hub follows whichever SDR card
    # the user assigned in the NOC dashboard (e.g. A2 at .192 vs A3 at .193).
    if req.device:
        m = re.match(r'(?:tcp://)?([^:]+):(\d+)', req.device)
        if m:
            new_host = m.group(1)
            new_port = int(m.group(2))
            if new_host != _RTL_TCP_HOST or new_port != NOAA_RTL_TCP_PORT:
                _RTL_TCP_HOST = new_host
                NOAA_RTL_TCP_PORT = new_port
                await _iq_hub.stop()   # force reconnect on the new host
                logger.info("NOAA: IQ hub switching to %s:%d", _RTL_TCP_HOST, NOAA_RTL_TCP_PORT)

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
    alert_id = body.get("id")
    # Skip if this alert ID was already logged (prevents duplicate entries on page refresh)
    if alert_id and alert_id in _dismissed_ids:
        return {"ok": True, "skipped": True, "reason": "already_logged"}
    entry = {**body, "logged_at": datetime.utcnow().isoformat() + "Z"}
    with _LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    _prune_log()
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
    """Proxy an EAS alert WAV from the noaa-eas-api container."""
    if not re.fullmatch(r"[\w\-]+", alert_id):
        raise HTTPException(400, "Invalid alert ID")
    wav_url = f"{ALERT_URL}/alerts/{alert_id}/audio"
    try:
        remote = urllib.request.urlopen(wav_url, timeout=10)
    except Exception:
        raise HTTPException(404, "Audio not found on sigmon-2")

    def _stream():
        while chunk := remote.read(65536):
            yield chunk

    return StreamingResponse(_stream(), media_type="audio/wav")


@router.get("/api/noaa-monitor/recordings")
def noaa_list_recordings():
    if not _NOAA_REC_DIR.exists():
        return {"recordings": []}
    files = sorted(
        _NOAA_REC_DIR.glob("*.wav"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    result = []
    for f in files:
        stat = f.stat()
        dur  = max(0.0, (stat.st_size - 44) / (_AUDIO_RATE * 2))
        result.append({
            "filename":     f.name,
            "created_at":   stat.st_mtime,
            "size_bytes":   stat.st_size,
            "duration_sec": round(dur, 1),
        })
    return {"recordings": result}


@router.get("/api/noaa-monitor/recordings/{filename}")
def noaa_get_recording(filename: str) -> StreamingResponse:
    path = _NOAA_REC_DIR / Path(filename).name
    if not path.exists() or path.suffix != ".wav":
        raise HTTPException(404, "Recording not found")
    size = path.stat().st_size

    def _stream():
        with open(str(path), "rb") as fh:
            while chunk := fh.read(65536):
                yield chunk

    return StreamingResponse(
        _stream(), media_type="audio/wav",
        headers={"Content-Disposition": f'attachment; filename="{path.name}"',
                 "Content-Length": str(size)},
    )


@router.delete("/api/noaa-monitor/recordings/{filename}")
def noaa_delete_recording(filename: str):
    path = _NOAA_REC_DIR / Path(filename).name
    if not path.exists():
        raise HTTPException(404, "Recording not found")
    path.unlink()
    return {"deleted": filename}


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


# ── NOC assignment hook ─────────────────────────────────────────────────────
# When noc_assignments.json changes via the NOC UI, reconnect the IQ hub to the
# new SDR without requiring a server restart.

async def _on_noaa_assignment_changed(changed_keys: set, _state: dict) -> None:
    if "noaa-monitor" not in changed_keys:
        return
    new_device = (_state.get("noaa-monitor") or {}).get("device", "?")
    logger.info("NOC: noaa-monitor SDR changed to %s — reconnecting IQ hub", new_device)
    await _iq_hub.stop()
    await _iq_hub.ensure_running()


def _register_noc_hook() -> None:
    try:
        from .noc import register_assignment_hook
        register_assignment_hook(_on_noaa_assignment_changed)
    except Exception as exc:
        logger.warning("Could not register NOC assignment hook: %s", exc)


_register_noc_hook()
