from __future__ import annotations

import logging
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class RxMonitor:
    def __init__(self, *, kiwirecorder_path: Path, mgr: object) -> None:
        self._kiwirecorder_path = kiwirecorder_path
        self._mgr = mgr
        self._lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._status = {
            "running": False,
            "rx_chan": 0,
            "freq_khz": None,
            "sideband": None,
            "rssi_db": None,
            "last_update": None,
            "last_line": None,
        }

    def status(self) -> dict:
        with self._lock:
            return dict(self._status)

    def start(self, *, freq_khz: float, sideband: str, rx_chan: int = 0) -> None:
        self.stop()
        self._stop.clear()
        with self._lock:
            self._status.update(
                {
                    "running": True,
                    "rx_chan": int(rx_chan),
                    "freq_khz": float(freq_khz),
                    "sideband": str(sideband).lower(),
                    "rssi_db": None,
                    "last_update": None,
                    "last_line": None,
                }
            )
        self._thread = threading.Thread(
            target=self._run,
            kwargs={"freq_khz": float(freq_khz), "sideband": str(sideband), "rx_chan": int(rx_chan)},
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        proc = self._proc
        if proc is not None:
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._proc = None
        with self._lock:
            self._status["running"] = False

    def _run(self, *, freq_khz: float, sideband: str, rx_chan: int) -> None:
        if not self._kiwirecorder_path.exists():
            logger.warning("kiwirecorder not found at %s", self._kiwirecorder_path)
            with self._lock:
                self._status["running"] = False
            return

        with self._mgr.lock:  # type: ignore[attr-defined]
            host = str(self._mgr.host)  # type: ignore[attr-defined]
            port = int(self._mgr.port)  # type: ignore[attr-defined]

        cmd = [
            sys.executable or "python3",
            str(self._kiwirecorder_path),
            "-s",
            host,
            "-p",
            str(port),
            "-f",
            f"{float(freq_khz):.3f}".rstrip("0").rstrip("."),
            "-m",
            str(sideband).lower(),
            "--rx-chan",
            str(rx_chan),
            "--user",
            f"RXMON_RX{rx_chan}",
            "--log_level=info",
        ]

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
        except Exception as exc:
            logger.warning("rx monitor spawn failed: %s", exc)
            with self._lock:
                self._status["running"] = False
            return

        self._proc = proc

        def _reader() -> None:
            if proc.stdout is None:
                return
            for line in proc.stdout:
                if self._stop.is_set():
                    break
                msg = line.strip()
                rssi_db = None
                if "RSSI" in msg:
                    try:
                        # Example: "RSSI: -116.5"
                        parts = msg.split("RSSI", 1)[1]
                        for token in parts.replace(":", " ").split():
                            if token.replace("-", "").replace(".", "").isdigit():
                                rssi_db = float(token)
                                break
                    except Exception:
                        rssi_db = None
                with self._lock:
                    self._status["last_line"] = msg
                    if rssi_db is not None:
                        self._status["rssi_db"] = rssi_db
                        self._status["last_update"] = time.time()
            try:
                proc.terminate()
            except Exception:
                pass

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        while not self._stop.is_set():
            if proc.poll() is not None:
                break
            time.sleep(0.2)

        with self._lock:
            self._status["running"] = False
