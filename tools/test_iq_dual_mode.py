#!/usr/bin/env python3
"""
test_iq_dual_mode.py – Proof-of-concept: decode FT8 and FT4 from a single
KiwiSDR RX slot on 20m using IQ mode recording.

Strategy
--------
FT8 dial  : 14.074 MHz   (USB audio occupies +200 Hz to +3 kHz above dial)
FT4 dial  : 14.080 MHz   (USB audio occupies +200 Hz to +3 kHz above dial)
Separation: 6 kHz

We record one KiwiSDR slot in IQ mode centred at 14.077 MHz (midpoint).
The 12 kHz IQ bandwidth (±6 kHz) covers both mode passbands:
  FT8 signals → −3 kHz offset in IQ stream
  FT4 signals → +3 kHz offset in IQ stream

DSP steps per mode
  1. Multiply complex IQ by complex exponential to frequency-shift mode dial to DC.
  2. Low-pass FIR, cut-off 3.4 kHz (keeps entire FT8/FT4 audio passband).
  3. Take real part → this is the equivalent USB audio.
  4. Write 16-bit mono WAV at 12000 Hz.

Decoding
  Call `jt9 --ft8 -d 1 <wav>` and `jt9 --ft4 -d 1 <wav>` to batch-decode.
  jt9 is included with WSJT-X; we look for it in common macOS/Linux locations.

Usage
-----
  python3 tools/test_iq_dual_mode.py [--host HOST] [--port PORT] [--wait]

  --host  HOST   KiwiSDR hostname/IP (default: 192.168.1.93)
  --port  PORT   KiwiSDR port (default: 8073)
  --wait         Wait for the next 15-second UTC window boundary before recording
                 (recommended – ensures a full FT8/FT4 frame is captured)
  --keep         Keep intermediate WAV files after test completes
"""

from __future__ import annotations

import argparse
import math
import os
import shlex
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import wave
from pathlib import Path
from typing import List, Optional

import numpy as np
from scipy.signal import firwin, lfilter

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

IQ_CENTER_KHZ  = 14_077.0          # tune KiwiSDR here (kHz)
FT8_DIAL_KHZ   = 14_074.0          # FT8 20m USB dial
FT4_DIAL_KHZ   = 14_080.0          # FT4 20m USB dial

FT8_OFFSET_HZ  = (FT8_DIAL_KHZ - IQ_CENTER_KHZ) * 1_000   # −3000 Hz
FT4_OFFSET_HZ  = (FT4_DIAL_KHZ - IQ_CENTER_KHZ) * 1_000   # +3000 Hz

SAMP_RATE      = 12_000            # kiwirecorder IQ default (Hz)
RECORD_DUR_S   = 16                # slightly longer than 15s FT8 window
FIR_TAPS       = 127               # LP FIR filter length
LP_CUTOFF_HZ   = 3_400             # low-pass cut-off (Hz) — keeps full WSJT-X passband

KIWIRECORDER   = Path(__file__).resolve().parents[1] / "vendor" / "kiwiclient-jks" / "kiwirecorder.py"

# jt9 locations searched in order
JT9_SEARCH = [
    shutil.which("jt9"),
    "/Applications/WSJT-X.app/Contents/MacOS/jt9",
    "/Applications/WSJTX.app/Contents/MacOS/jt9",
    "/usr/bin/jt9",
    "/usr/local/bin/jt9",
]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def find_jt9() -> Optional[str]:
    for p in JT9_SEARCH:
        if p and Path(p).is_file() and os.access(p, os.X_OK):
            return p
    return None


def wait_for_window_boundary(window_s: int = 15) -> None:
    """Sleep until the next even multiple of window_s seconds UTC."""
    now    = time.time()
    phase  = now % window_s
    delay  = window_s - phase
    target = time.strftime("%H:%M:%S", time.gmtime(now + delay))
    print(f"  Waiting {delay:.1f}s for next {window_s}s boundary (UTC {target})…")
    time.sleep(delay)


def record_iq(host: str, port: int, out_wav: Path, dur_s: int) -> None:
    """Run kiwirecorder in IQ mode; centre at IQ_CENTER_KHZ."""
    cmd = [
        sys.executable, str(KIWIRECORDER),
        "--server-host", host,
        "--server-port", str(port),
        "--freq",        str(IQ_CENTER_KHZ),
        "--mode",        "iq",
        "--tlimit",      str(dur_s),
        "--fn",          out_wav.stem,
        "--dir",         str(out_wav.parent),
        "--quiet",
    ]
    print(f"  Recording {dur_s}s IQ from {host}:{port} @ {IQ_CENTER_KHZ} kHz…")
    print(f"  cmd: {shlex.join(cmd)}")
    result = subprocess.run(cmd, timeout=dur_s + 15, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  kiwirecorder stderr:\n{result.stderr[-600:]}")
        raise RuntimeError(f"kiwirecorder exited with code {result.returncode}")
    # kiwirecorder appends station info and timestamp; find the wav it created
    wavs = sorted(out_wav.parent.glob("*.wav"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not wavs:
        raise RuntimeError("kiwirecorder produced no WAV file")
    # rename to our expected name if different
    if wavs[0] != out_wav:
        wavs[0].rename(out_wav)
    print(f"  Recorded → {out_wav} ({out_wav.stat().st_size // 1024} kB)")


def load_iq_wav(path: Path) -> tuple[np.ndarray, int]:
    """
    Read a kiwirecorder IQ WAV.

    kiwirecorder IQ mode writes a 2-channel (stereo) 16-bit PCM WAV where
    channel 0 = I, channel 1 = Q.  Returns complex64 array and sample rate.
    """
    with wave.open(str(path), "rb") as wf:
        n_ch   = wf.getnchannels()
        fs     = wf.getframerate()
        n_samp = wf.getnframes()
        raw    = wf.readframes(n_samp)

    if n_ch == 2:
        pcm = np.frombuffer(raw, dtype=np.int16).reshape(-1, 2)
        iq  = pcm[:, 0].astype(np.float32) + 1j * pcm[:, 1].astype(np.float32)
    elif n_ch == 1:
        # Mono IQ packed as interleaved I,Q 16-bit — uncommon but handle it
        pcm = np.frombuffer(raw, dtype=np.int16)
        if len(pcm) % 2:
            pcm = pcm[:-1]
        iq = pcm[0::2].astype(np.float32) + 1j * pcm[1::2].astype(np.float32)
    else:
        raise ValueError(f"Unexpected channel count {n_ch} in IQ WAV")

    iq /= 32768.0   # normalise to ±1
    return iq, fs


def make_lp_fir(fs: int, cutoff_hz: float, n_taps: int) -> np.ndarray:
    """Design a symmetric low-pass FIR filter."""
    return firwin(n_taps, cutoff_hz / (fs / 2.0), window="hamming")


def extract_usb_channel(
    iq: np.ndarray,
    fs: int,
    offset_hz: float,
    fir: np.ndarray,
) -> np.ndarray:
    """
    Shift the IQ stream so that the sub-band centred on `offset_hz`
    (relative to the recorded centre) ends up at DC, then low-pass
    and take the real part to produce USB audio.

    offset_hz is negative for FT8 (dial is BELOW centre) and positive
    for FT4 (dial is ABOVE centre).
    """
    n    = len(iq)
    t    = np.arange(n, dtype=np.float64) / fs
    # Shift: multiply by e^{j 2π (−offset) t} to move offset_hz → 0 Hz
    shift = np.exp(-1j * 2.0 * math.pi * offset_hz * t).astype(np.complex64)
    shifted = iq * shift

    # Low-pass filter (applied to complex; filters both I and Q paths)
    filtered_r = lfilter(fir, 1.0, shifted.real)
    filtered_i = lfilter(fir, 1.0, shifted.imag)

    # USB demodulation: real part of the analytically-shifted complex signal
    audio = filtered_r   # real part = I = USB audio at this baseband

    # Normalise
    peak = np.max(np.abs(audio))
    if peak > 0:
        audio = audio / peak * 0.9

    return audio.astype(np.float32)


def write_mono_wav(path: Path, audio: np.ndarray, fs: int) -> None:
    """Write a float32 array as 16-bit mono PCM WAV."""
    pcm = (audio * 32767).astype(np.int16)
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)         # 16-bit
        wf.setframerate(fs)
        wf.writeframes(pcm.tobytes())
    print(f"  Wrote {path.name} ({path.stat().st_size // 1024} kB, {len(pcm) / fs:.1f}s@{fs}Hz)")


def run_jt9(jt9: str, wav: Path, mode: str) -> List[str]:
    """
    Run jt9 against a WAV file. mode is 'ft8' or 'ft4'.
    Returns list of decoded message lines.
    """
    flag = "--ft8" if mode == "ft8" else "--ft4"
    cmd  = [jt9, flag, "-d", "1", str(wav)]
    print(f"  Decoding {mode.upper()} → {shlex.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(wav.parent),     # jt9 writes temp files in cwd
        )
    except subprocess.TimeoutExpired:
        print(f"  jt9 timed out for {mode.upper()}")
        return []

    lines = [l for l in result.stdout.splitlines() if l.strip()]
    if result.returncode not in (0, 1):
        print(f"  jt9 stderr: {result.stderr[-300:]}")
    return lines


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host",  default="192.168.1.93", help="KiwiSDR hostname/IP")
    ap.add_argument("--port",  type=int, default=8073,  help="KiwiSDR port")
    ap.add_argument("--wait",  action="store_true",     help="Wait for next 15s window boundary")
    ap.add_argument("--keep",  action="store_true",     help="Keep WAV files after test")
    args = ap.parse_args()

    jt9 = find_jt9()
    print("=" * 68)
    print("IQ Dual-Mode FT8 + FT4  —  20m single-receiver test")
    print("=" * 68)
    print(f"  KiwiSDR   : {args.host}:{args.port}")
    print(f"  IQ centre : {IQ_CENTER_KHZ:.3f} kHz")
    print(f"  FT8 offset: {FT8_OFFSET_HZ:+.0f} Hz  (dial {FT8_DIAL_KHZ:.3f} kHz)")
    print(f"  FT4 offset: {FT4_OFFSET_HZ:+.0f} Hz  (dial {FT4_DIAL_KHZ:.3f} kHz)")
    print(f"  jt9 path  : {jt9 or 'NOT FOUND – decoding will be skipped'}")
    print()

    tmpdir = Path(tempfile.mkdtemp(prefix="kiwiscan_iq_test_"))
    iq_wav  = tmpdir / "iq_raw.wav"
    ft8_wav = tmpdir / "ft8_audio.wav"
    ft4_wav = tmpdir / "ft4_audio.wav"

    try:
        # ── 1. Time-align ────────────────────────────────────────────────────
        if args.wait:
            print("[1/4] Aligning to next FT8 window boundary…")
            wait_for_window_boundary(15)
        else:
            print("[1/4] Starting immediately (use --wait to align to frame boundary)")

        # ── 2. Record IQ ─────────────────────────────────────────────────────
        print(f"\n[2/4] Recording {RECORD_DUR_S}s IQ audio…")
        record_iq(args.host, args.port, iq_wav, RECORD_DUR_S)

        # ── 3. DSP extraction ─────────────────────────────────────────────────
        print("\n[3/4] Extracting FT8 and FT4 sub-bands…")
        iq, fs = load_iq_wav(iq_wav)
        print(f"  Loaded {len(iq)} samples @ {fs} Hz  ({len(iq)/fs:.1f}s)")

        fir = make_lp_fir(fs, LP_CUTOFF_HZ, FIR_TAPS)

        print(f"  Extracting FT8 channel (offset {FT8_OFFSET_HZ:+.0f} Hz)…")
        ft8_audio = extract_usb_channel(iq, fs, FT8_OFFSET_HZ, fir)
        write_mono_wav(ft8_wav, ft8_audio, fs)

        print(f"  Extracting FT4 channel (offset {FT4_OFFSET_HZ:+.0f} Hz)…")
        ft4_audio = extract_usb_channel(iq, fs, FT4_OFFSET_HZ, fir)
        write_mono_wav(ft4_wav, ft4_audio, fs)

        # ── 4. Decode ─────────────────────────────────────────────────────────
        print("\n[4/4] Decoding…")
        if jt9 is None:
            print("  jt9 not found — install WSJT-X to enable decoding.")
            print("  WAV files are available for manual inspection:")
            print(f"    FT8 audio : {ft8_wav}")
            print(f"    FT4 audio : {ft4_wav}")
        else:
            ft8_decodes = run_jt9(jt9, ft8_wav, "ft8")
            ft4_decodes = run_jt9(jt9, ft4_wav, "ft4")

            print()
            print("─" * 68)
            print(f"  FT8 DECODES ({len(ft8_decodes)} messages):")
            for line in ft8_decodes:
                print(f"    {line}")
            if not ft8_decodes:
                print("    (none — may need to use --wait for proper frame alignment)")

            print()
            print(f"  FT4 DECODES ({len(ft4_decodes)} messages):")
            for line in ft4_decodes:
                print(f"    {line}")
            if not ft4_decodes:
                print("    (none — FT4 is less active than FT8 on 20m)")

        print()
        print("─" * 68)
        print("  CONCLUSION:")
        if jt9 is not None:
            total = len(ft8_decodes) + len(ft4_decodes)
            if total > 0:
                print(f"  ✓  {total} total decode(s) from a single RX slot.")
                print("  ✓  IQ dual-mode capture is viable.")
            else:
                print("  ✗  No decodes. Check frame alignment (--wait) and band conditions.")
        else:
            print("  WAV files written; install WSJT-X and re-run to decode.")
        print("─" * 68)

        if args.keep:
            print(f"\n  WAV files kept in: {tmpdir}")
        else:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except KeyboardInterrupt:
        print("\nInterrupted.")
        shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit(1)
    except Exception as exc:
        print(f"\nERROR: {exc}")
        print(f"  Temp dir retained for inspection: {tmpdir}")
        raise


if __name__ == "__main__":
    main()
