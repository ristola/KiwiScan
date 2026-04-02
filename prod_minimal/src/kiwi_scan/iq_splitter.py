#!/usr/bin/env python3
"""
iq_splitter.py – Extract N USB sub-bands from a raw IQ stream.

Reads raw 16-bit stereo (interleaved I, Q) PCM from stdin at ~12 kHz
(as produced by kiwirecorder in IQ mode with --nc --quiet), frequency-
shifts and low-pass-filters each sub-band, resamples 12 kHz → 48 kHz,
and forwards each channel to ft8modem via the ft8modem UDP audio protocol
(256-byte frames + 2-byte little-endian sequence number).

Usage:
  python -u iq_splitter.py <offset_hz> <udp_port> [<offset_hz> <udp_port> ...]

  Arguments come in pairs: (offset_hz, udp_port).  Two or more pairs
  are required (i.e. at least 4 arguments total).

Arguments:
  offset_hz   Frequency offset (Hz) of this mode's dial relative to the
              IQ stream centre.  Negative if the frequency is below centre.
  udp_port    UDP port where the corresponding ft8modem instance listens.

Examples:
  # 20m dual-mode (IQ centred at 14.077 MHz):
  python -u iq_splitter.py -3000 3100 3000 3200

  # 30m triple-mode (IQ centred at 10.138 MHz):
  python -u iq_splitter.py -2000 3100 2000 3200 700 3300
"""

from __future__ import annotations

import math
import socket
import struct
import sys

import numpy as np

# ── Constants ─────────────────────────────────────────────────────────────────

INPUT_RATE = 12_000       # kiwirecorder IQ output sample rate (Hz)
OUTPUT_RATE = 48_000      # ft8modem expects 48 kHz
UPSAMPLE = OUTPUT_RATE // INPUT_RATE   # 4×

LP_CUTOFF = 3_400.0       # low-pass cutoff (Hz) — preserves full FT8/FT4 passband
CHUNK = 512               # IQ samples per stdin read (~42 ms)
UDP_WIN = 256             # audio bytes per UDP frame (ft8modem AudioUDP protocol)

_BYTES_PER_IQ_SAMPLE = 4  # 2 channels × 2 bytes (int16)


# ── 2nd-order Butterworth low-pass biquad ─────────────────────────────────────

def _biquad_lp_coeffs(fc: float, fs: float) -> tuple[list[float], list[float]]:
    """Biquad coefficients for a 2nd-order Butterworth LP at fc/fs."""
    theta = 2.0 * math.pi * fc / fs
    Q = math.sqrt(0.5)           # Butterworth quality factor = 1/√2
    sin_t = math.sin(theta)
    cos_t = math.cos(theta)
    alpha = sin_t / (2.0 * Q)
    a0 = 1.0 + alpha
    b = [
        (1.0 - cos_t) / 2.0 / a0,
        (1.0 - cos_t) / a0,
        (1.0 - cos_t) / 2.0 / a0,
    ]
    a = [1.0, -2.0 * cos_t / a0, (1.0 - alpha) / a0]
    return b, a


class _BiquadState:
    """Streaming Direct-Form-II transposed biquad with scalar Python loop.

    Processes one float64 numpy array per call while keeping state
    across successive chunks.
    """

    __slots__ = ("b0", "b1", "b2", "a1", "a2", "w0", "w1")

    def __init__(self, b: list[float], a: list[float]) -> None:
        self.b0, self.b1, self.b2 = b
        self.a1, self.a2 = a[1], a[2]
        self.w0 = 0.0
        self.w1 = 0.0

    def run(self, x: np.ndarray) -> np.ndarray:
        b0, b1, b2 = self.b0, self.b1, self.b2
        a1, a2 = self.a1, self.a2
        w0, w1 = self.w0, self.w1
        y = np.empty(len(x), dtype=np.float64)
        for i in range(len(x)):
            xi = float(x[i])
            yi = b0 * xi + w0
            w0 = b1 * xi - a1 * yi + w1
            w1 = b2 * xi - a2 * yi
            y[i] = yi
        self.w0 = w0
        self.w1 = w1
        return y


# ── AudioUDP sender matching ft8modem's expected protocol ─────────────────────

class _UDPSender:
    """Buffers 16-bit PCM bytes and transmits UDP frames using ft8modem's
    AudioUDP framing: each packet = WIN bytes of audio + 2-byte LE seq number.
    """

    def __init__(self, port: int, win: int = UDP_WIN) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._addr = ("127.0.0.1", port)
        self._buf = bytearray()
        self._win = win
        self._seq = 0

    def push(self, pcm_bytes: bytes) -> None:
        self._buf.extend(pcm_bytes)
        while len(self._buf) >= self._win:
            frame = bytes(self._buf[: self._win]) + struct.pack("<H", self._seq & 0xFFFF)
            del self._buf[: self._win]
            self._seq += 1
            try:
                self._sock.sendto(frame, self._addr)
            except OSError:
                pass


# ── Per-mode channel processor ─────────────────────────────────────────────────

class _Channel:
    """
    Single sub-band extraction chain:
      1. Frequency-shift IQ by -offset so the mode dial moves to 0 Hz.
      2. LP-filter I and Q independently at LP_CUTOFF (removes mirror image).
      3. Take the real (I) part → USB audio.
      4. Normalise + clip to ±1.
      5. Zero-order-hold upsample 12 kHz → 48 kHz.
      6. Quantise to 16-bit and forward to ft8modem via UDP.
    """

    def __init__(self, offset_hz: float, udp_port: int) -> None:
        # Phase ramp for frequency shift: moving "offset_hz" to DC requires
        # multiplying by exp(-j·2π·offset·n/Fs)
        self._phase_inc = -2.0 * math.pi * offset_hz / INPUT_RATE
        self._phase = 0.0

        b, a = _biquad_lp_coeffs(LP_CUTOFF, INPUT_RATE)
        self._lp_i = _BiquadState(b, a)
        self._lp_q = _BiquadState(list(b), list(a))   # independent state for Q

        self._udp = _UDPSender(udp_port)

    def process(self, iq: np.ndarray) -> None:
        """Process one chunk of complex64 IQ samples."""
        n = len(iq)

        # Continuous phase ramp for this chunk
        ramp = self._phase + self._phase_inc * np.arange(n, dtype=np.float64)
        self._phase = float((ramp[-1] + self._phase_inc) % (2.0 * math.pi))

        # Frequency shift: multiply by complex exponential
        shift = (np.cos(ramp) + 1j * np.sin(ramp)).astype(np.complex64)
        z = iq * shift

        # LP filter I and Q paths
        i_filt = self._lp_i.run(z.real.astype(np.float64))
        self._lp_q.run(z.imag.astype(np.float64))   # keep Q state in sync

        # USB audio = real (I) part of LP-filtered analytic signal
        # Normalise: kiwirecorder IQ is 16-bit so peak is ~32768
        audio = np.clip(i_filt * (1.0 / 32768.0), -1.0, 1.0)

        # Zero-order-hold upsample from INPUT_RATE to OUTPUT_RATE (4×)
        upsampled = np.repeat(audio, UPSAMPLE)

        # Quantise to int16 and ship
        pcm = (upsampled * 32767.0).astype(np.int16)
        self._udp.push(pcm.tobytes())


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]
    if len(args) < 4 or len(args) % 2 != 0:
        sys.stderr.write(
            "Usage: iq_splitter.py <offset_hz> <udp_port> [<offset_hz> <udp_port> ...]\n"
            "  Provide at least 2 pairs (4 arguments).\n"
        )
        sys.exit(2)

    channels = []
    for i in range(0, len(args), 2):
        offset = float(args[i])
        port = int(args[i + 1])
        channels.append(_Channel(offset, port))

    stdin_buf = sys.stdin.buffer
    bytes_per_chunk = CHUNK * _BYTES_PER_IQ_SAMPLE   # 2048 bytes

    while True:
        raw = stdin_buf.read(bytes_per_chunk)
        if not raw:
            break
        # Pad any short last chunk with zeros
        if len(raw) < bytes_per_chunk:
            raw = raw + b"\x00" * (bytes_per_chunk - len(raw))
        # Decode stereo 16-bit LE: channel 0 = I, channel 1 = Q
        pcm = np.frombuffer(raw, dtype=np.int16).reshape(-1, 2)
        iq = pcm[:, 0].astype(np.float32) + 1j * pcm[:, 1].astype(np.float32)
        for ch in channels:
            ch.process(iq)


if __name__ == "__main__":
    main()
