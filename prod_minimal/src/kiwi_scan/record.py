from __future__ import annotations

import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class RecordRequest:
    host: str
    port: int
    password: str | None
    user: str
    freq_hz: float
    rx_chan: int | None = None
    duration_s: int = 30
    mode: str = "usb"
    out_dir: Path = Path("recordings")


class RecorderUnavailable(RuntimeError):
    pass


def find_kiwirecorder() -> str:
    exe = shutil.which("kiwirecorder.py")
    if exe:
        return exe
    exe = shutil.which("kiwirecorder")
    if exe:
        return exe
    vendor = Path(__file__).resolve().parents[2] / "vendor" / "kiwiclient-jks" / "kiwirecorder.py"
    if vendor.is_file():
        return str(vendor)
    raise RecorderUnavailable(
        "Could not find kiwirecorder on PATH (expected kiwirecorder.py or kiwirecorder)."
    )


def run_record(req: RecordRequest) -> Path:
    req.out_dir.mkdir(parents=True, exist_ok=True)

    recorder = find_kiwirecorder()
    cmd = [sys.executable, recorder] if recorder.endswith(".py") else [recorder]
    cmd.extend([
        "--server-host", req.host,
        "--server-port", str(req.port),
        "--user", req.user,
        "--freq", str(req.freq_hz / 1000.0),  # kiwirecorder usually wants kHz
        "--modulation", req.mode,
        "--tlimit", str(req.duration_s),
        "--dir", str(req.out_dir),
    ])
    if req.rx_chan is not None:
        cmd.extend(["--rx-chan", str(int(req.rx_chan))])
    if req.password:
        cmd.extend(["--password", req.password])

    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as e:
        raise RecorderUnavailable(str(e)) from e

    # kiwirecorder chooses filenames; return directory as the artifact location.
    return req.out_dir
