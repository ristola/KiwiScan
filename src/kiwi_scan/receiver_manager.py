from __future__ import annotations

import logging
import re
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

from .bandplan import bandplan_ranges_for_label
from .ssb_scan_hits import log_ssb_scan_hit, update_ssb_scan_status

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReceiverAssignment:
    rx: int
    band: str
    freq_hz: float
    mode_label: str
    ssb_scan: Optional[dict] = None
    sideband: Optional[str] = None


class _ReceiverWorker(threading.Thread):
    def __init__(
        self,
        *,
        kiwirecorder_path: Path,
        ft8modem_path: Path,
        af2udp_path: Path,
        sox_path: str,
        host: str,
        port: int,
        rx: int,
        band: str,
        freq_hz: float,
        mode_label: str,
        ssb_scan: Optional[dict] = None,
        sideband: Optional[str] = None,
        decode_callback: Optional[Callable[[dict], None]] = None,
        on_restart: Optional[Callable[[int, str, str, float, int], None]] = None,
    ) -> None:
        super().__init__(daemon=True)
        self._kiwirecorder_path = kiwirecorder_path
        self._ft8modem_path = ft8modem_path
        self._af2udp_path = af2udp_path
        self._sox_path = sox_path
        self._host = host
        self._port = int(port)
        self._rx = int(rx)
        self._band = str(band)
        self._freq_hz = float(freq_hz)
        self._mode_label = str(mode_label or "FT8")
        self._ssb_scan = dict(ssb_scan or {}) if ssb_scan else None
        self._sideband = str(sideband).strip().upper() if sideband else None
        self._decode_callback = decode_callback
        self._on_restart = on_restart
        self._stop = threading.Event()
        self._proc: Optional[subprocess.Popen] = None
        self._decoder_procs: list[subprocess.Popen] = []
        self._decoder_threads: list[threading.Thread] = []
        self._decoder_log_fps: list[Optional[object]] = []

    def stop(self) -> None:
        self._stop.set()
        self._terminate_proc()

    def _terminate_proc(self) -> None:
        for proc in list(self._decoder_procs):
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._decoder_procs.clear()
        self._decoder_threads.clear()
        for fp in self._decoder_log_fps:
            try:
                if fp:
                    fp.close()
            except Exception:
                pass
        self._decoder_log_fps.clear()
        proc = self._proc
        if proc is None:
            return
        try:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception:
                proc.terminate()
            proc.wait(timeout=2.0)
        except Exception:
            try:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    proc.kill()
            except Exception:
                pass
        self._proc = None

    def _is_digital_mode(self) -> bool:
        norm = self._mode_label.strip().upper()
        return norm in {"FT4", "FT8", "FT4 / FT8", "FT4/FT8", "FT8 / FT4", "FT8/FT4", "WSPR"}

    def _is_dual_mode(self) -> bool:
        norm = self._mode_label.strip().upper()
        return "FT4" in norm and "FT8" in norm

    def _decoder_mode(self) -> str:
        norm = self._mode_label.strip().upper()
        if norm == "WSPR":
            return "WSPR"
        if norm == "FT4":
            return "FT4"
        return "FT8"

    def _decoder_env(self, mode: str) -> Optional[dict]:
        """Return an environment override for decoder processes.

        For WSPR, ft8modem shells out to `wsprd` and expects it on PATH.
        On macOS, `wsprd` may live inside the WSJT-X app bundle.
        """

        if str(mode or "").strip().upper() != "WSPR":
            return None

        if shutil.which("wsprd"):
            return None

        candidates = [
            Path("/Applications/WSJT-X.app/Contents/MacOS/wsprd"),
            Path("/Applications/WSJTX.app/Contents/MacOS/wsprd"),
            Path("/Applications/wsjtx.app/Contents/MacOS/wsprd"),
        ]

        try:
            apps_dir = Path("/Applications")
            if apps_dir.exists():
                for app in apps_dir.glob("*.app"):
                    if "wsjt" not in app.name.lower():
                        continue
                    c = app / "Contents" / "MacOS" / "wsprd"
                    candidates.append(c)
        except Exception:
            pass
        for c in candidates:
            try:
                if c.exists():
                    env = dict(os.environ)
                    env["PATH"] = f"{c.parent}:{env.get('PATH', '')}"
                    return env
            except Exception:
                continue

        logger.warning("WSPR selected but wsprd not found on PATH")
        return None

    def _is_ssb_scan(self) -> bool:
        norm = self._mode_label.strip().upper()
        return norm in {"SSB", "PHONE"} and bool(self._ssb_scan)

    def _ssb_scan_sideband(self) -> str:
        ranges = self._ssb_scan_ranges()
        if ranges:
            max_hz = max(max(start_hz, end_hz) for start_hz, end_hz in ranges)
            return "lsb" if max_hz < 10_000_000 else "usb"
        sideband = str((self._ssb_scan or {}).get("sideband") or "USB").strip().upper()
        return "lsb" if sideband == "LSB" else "usb"

    def _ssb_assignment_sideband(self) -> str:
        if self._sideband:
            return "lsb" if self._sideband == "LSB" else "usb"
        ranges = self._ssb_scan_ranges()
        if ranges:
            max_hz = max(max(start_hz, end_hz) for start_hz, end_hz in ranges)
            return "lsb" if max_hz < 10_000_000 else "usb"
        return "usb"

    def _ssb_scan_step_sequence(self) -> list[float]:
        scan_cfg = self._ssb_scan or {}
        strategy = str(scan_cfg.get("step_strategy") or "adaptive").strip().lower()
        if strategy == "fixed":
            step = float(scan_cfg.get("step_khz") or 10.0)
            return [max(0.1, step)]
        return [10.0, 5.0, 2.5]

    def _ssb_scan_ranges(self) -> list[tuple[float, float]]:
        return bandplan_ranges_for_label("Phone", band=self._band)

    def _ssb_scan_freqs_khz(self, step_khz: float) -> list[float]:
        ranges = self._ssb_scan_ranges()
        if not ranges:
            return []
        out: list[float] = []
        step = max(0.1, float(step_khz))
        for start_hz, end_hz in ranges:
            start_khz = float(start_hz) / 1000.0
            end_khz = float(end_hz) / 1000.0
            f = start_khz
            while f <= end_khz:
                out.append(f)
                f += step
        return out

    def _write_ssb_scan_yaml(self, *, freqs_khz: list[float], path: Path) -> None:
        scan_cfg = self._ssb_scan or {}
        threshold = scan_cfg.get("threshold_db")
        wait_s = float(scan_cfg.get("wait_s") or 1.0)
        dwell_s = float(scan_cfg.get("dwell_s") or 6.0)
        lines = ["Scan:"]
        if threshold is not None:
            lines.append(f"  threshold: {float(threshold)}")
        lines.append(f"  wait: {wait_s}")
        lines.append(f"  dwell: {dwell_s}")
        freq_parts = []
        for f in freqs_khz:
            s = f"{float(f):.3f}".rstrip("0").rstrip(".")
            freq_parts.append(s)
        lines.append(f"  frequencies: [{', '.join(freq_parts)}]")
        lines.append("  pbc: true")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _run_ssb_scan_loop(self) -> None:
        scan_cfg = self._ssb_scan or {}
        wait_s = float(scan_cfg.get("wait_s") or 1.0)
        dwell_s = float(scan_cfg.get("dwell_s") or 6.0)
        tail_s = float(scan_cfg.get("tail_s") or 1.0)
        step_sequence = self._ssb_scan_step_sequence()
        step_index = 0

        while not self._stop.is_set():
            step_khz = step_sequence[min(step_index, len(step_sequence) - 1)]
            freqs = self._ssb_scan_freqs_khz(step_khz)
            if not freqs:
                time.sleep(5.0)
                continue

            yaml_path = Path("/tmp") / f"kiwi_scan_ssb_rx{self._rx}_{self._band}_{step_khz:.1f}.yaml"
            try:
                self._write_ssb_scan_yaml(freqs_khz=freqs, path=yaml_path)
            except Exception:
                time.sleep(2.0)
                continue

            cmd = [
                sys.executable or "python3",
                str(self._kiwirecorder_path),
                "-s",
                str(self._host),
                "-p",
                str(self._port),
                "-m",
                self._ssb_scan_sideband(),
                "--rx-chan",
                str(self._rx),
                "--user",
                f"AUTO_{self._band}_SSBSCAN",
                "--scan-yaml",
                str(yaml_path),
                "--squelch-tail",
                str(tail_s),
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
            except Exception:
                time.sleep(2.0)
                continue

            self._proc = proc
            hits = {"count": 0}
            freq_re = re.compile(r"DWELL\s*[:=]?\s*([0-9.]+)\s*kHz", re.IGNORECASE)
            freq_alt_re = re.compile(r"(?:FREQ|FREQUENCY|SCAN|TUNE|TUNED)\s*[:=]?\s*([0-9.]+)\s*(kHz|MHz)", re.IGNORECASE)
            freq_unit_re = re.compile(r"\b([0-9]+(?:\.[0-9]+)?)\s*(kHz|MHz)\b", re.IGNORECASE)
            rssi_re = re.compile(r"(?:RSSI|SNR|S[-_ ]?METER|SIGNAL)\s*[:=]?\s*([+-]?[0-9.]+)", re.IGNORECASE)
            last_freq_khz: Optional[float] = None
            last_rssi_db: Optional[float] = None
            last_rssi_at: float = 0.0
            log_path = Path("/tmp") / f"kiwi_ssb_scan_rx{self._rx}.log"
            log_fp = None

            def _reader() -> None:
                nonlocal last_freq_khz
                if proc.stdout is None:
                    return
                nonlocal log_fp
                try:
                    log_fp = open(log_path, "a", encoding="utf-8")
                except Exception:
                    log_fp = None
                for line in proc.stdout:
                    if log_fp:
                        try:
                            log_fp.write(line)
                            log_fp.flush()
                        except Exception:
                            pass
                    freq_khz = None
                    rssi_db = None
                    m = freq_re.search(line)
                    if m:
                        try:
                            freq_khz = float(m.group(1))
                        except Exception:
                            freq_khz = None
                    if freq_khz is None:
                        m = freq_alt_re.search(line)
                        if m:
                            try:
                                raw = float(m.group(1))
                                unit = str(m.group(2)).lower()
                                freq_khz = raw * 1000.0 if unit == "mhz" else raw
                            except Exception:
                                freq_khz = None
                    if freq_khz is None:
                        m = freq_unit_re.search(line)
                        if m:
                            try:
                                raw = float(m.group(1))
                                unit = str(m.group(2)).lower()
                                freq_khz = raw * 1000.0 if unit == "mhz" else raw
                            except Exception:
                                freq_khz = None
                    m = rssi_re.search(line)
                    if m:
                        try:
                            rssi_db = float(m.group(1))
                        except Exception:
                            rssi_db = None
                    if rssi_db is not None:
                        last_rssi_db = rssi_db
                        last_rssi_at = time.time()
                    if freq_khz is not None:
                        last_freq_khz = freq_khz
                    elif rssi_db is not None and last_freq_khz is not None:
                        freq_khz = last_freq_khz
                    if freq_khz is not None and rssi_db is None and last_rssi_db is not None:
                        if (time.time() - last_rssi_at) <= 2.5:
                            rssi_db = last_rssi_db
                    if freq_khz is not None or rssi_db is not None:
                        update_ssb_scan_status(
                            band=self._band,
                            rx=self._rx,
                            freq_khz=freq_khz,
                            rssi_db=rssi_db,
                            step_khz=step_khz,
                            sideband=self._ssb_scan_sideband(),
                            threshold_db=(self._ssb_scan or {}).get("threshold_db"),
                        )
                    if "Started a new file" in line:
                        hits["count"] += 1
                        log_ssb_scan_hit(
                            band=self._band,
                            rx=self._rx,
                            freq_khz=freq_khz,
                            step_khz=step_khz,
                            sideband=self._ssb_scan_sideband(),
                            threshold_db=(self._ssb_scan or {}).get("threshold_db"),
                        )
                    if self._stop.is_set():
                        break
                if log_fp:
                    try:
                        log_fp.close()
                    except Exception:
                        pass

            reader = threading.Thread(target=_reader, daemon=True)
            reader.start()

            sweep_s = max(1.0, len(freqs) * (wait_s + dwell_s))
            end_time = time.time() + sweep_s
            while not self._stop.is_set() and time.time() < end_time:
                if proc.poll() is not None:
                    break
                time.sleep(0.2)

            self._terminate_proc()

            if self._stop.is_set():
                break

            if len(step_sequence) > 1:
                if hits["count"] > 0:
                    step_index = 0
                else:
                    step_index = min(step_index + 1, len(step_sequence) - 1)
            time.sleep(0.5)

    def _start_decoder(self, udp_port: int, mode: str) -> None:
        if not self._ft8modem_path.exists():
            return
        try:
            subprocess.run(
                ["pkill", "-f", f"ft8modem.*udp:{udp_port}"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass
        log_suffix = mode.lower()
        log_path = Path("/tmp") / f"ft8modem_rx{self._rx}_{log_suffix}.log"
        cmd = [
            str(self._ft8modem_path),
            "-r",
            "48000",
            mode,
            f"udp:{udp_port}",
        ]
        log_fp = None
        try:
            log_fp = open(log_path, "a", encoding="utf-8")
            log_fp.write(f"START {time.strftime('%Y-%m-%d %H:%M:%S')} CMD: {' '.join(cmd)}\n")
            log_fp.flush()
        except Exception:
            log_fp = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                stderr=log_fp or subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=self._decoder_env(mode),
            )
        except Exception as e:
            logger.warning("decoder spawn failed: %s", e)
            if log_fp:
                try:
                    log_fp.write(f"SPAWN FAILED: {e}\n")
                    log_fp.flush()
                except Exception:
                    pass
            return

        def _reader() -> None:
            if proc.stdout is None:
                return
            for line in proc.stdout:
                if self._stop.is_set():
                    break
                msg = line.strip()
                if not msg:
                    continue
                if log_fp:
                    try:
                        log_fp.write(msg + "\n")
                        log_fp.flush()
                    except Exception:
                        pass
                if not msg.startswith("D:"):
                    continue
                if self._decode_callback is not None:
                    try:
                        self._decode_callback({
                            "rx": self._rx,
                            "band": self._band,
                            "freq_hz": self._freq_hz,
                            "mode_label": self._mode_label,
                            "message": msg,
                        })
                    except Exception:
                        pass
            if log_fp:
                try:
                    log_fp.write("STDOUT CLOSED\n")
                    log_fp.flush()
                except Exception:
                    pass

        self._decoder_threads.append(threading.Thread(target=_reader, daemon=True))
        self._decoder_threads[-1].start()

        def _watcher() -> None:
            rc = proc.wait()
            if log_fp:
                try:
                    log_fp.write(f"EXIT {rc}\n")
                    log_fp.flush()
                except Exception:
                    pass

        threading.Thread(target=_watcher, daemon=True).start()
        self._decoder_procs.append(proc)
        self._decoder_log_fps.append(log_fp)

    @staticmethod
    def _format_freq_khz(freq_hz: float) -> str:
        """Format a kHz frequency for kiwirecorder.

        kiwirecorder accepts floats (kHz). Some modes (e.g. WSPR) require
        sub-kHz precision, so do not round to an integer.
        """

        khz = float(freq_hz) / 1000.0
        # Keep a few decimals to preserve sub-kHz dial frequencies,
        # but avoid long float representations.
        s = f"{khz:.3f}".rstrip("0").rstrip(".")
        return s or str(khz)

    def _spawn(self) -> Optional[subprocess.Popen]:
        if not self._kiwirecorder_path.exists():
            logger.warning("kiwirecorder not found at %s", self._kiwirecorder_path)
            return None
        freq_khz = self._format_freq_khz(self._freq_hz)
        mode_tag = self._mode_label.strip().upper().replace(" ", "").replace("/", "")
        if self._is_digital_mode():
            if not self._af2udp_path.exists():
                logger.warning("af2udp not found at %s", self._af2udp_path)
                return None
            if not self._sox_path:
                logger.warning("sox not found in PATH")
                return None
            if self._is_dual_mode():
                udp_port_ft8 = 3100 + self._rx
                udp_port_ft4 = 3200 + self._rx
                self._start_decoder(udp_port_ft8, "FT8")
                self._start_decoder(udp_port_ft4, "FT4")
                fanout_path = Path(__file__).resolve().parent / "udp_fanout.py"
                pipeline_cmd = (
                    f"{sys.executable or 'python3'} {self._kiwirecorder_path} "
                    f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                    f"--rx-chan {self._rx} --user 'AUTO_{self._band}_{mode_tag}' --nc --quiet | "
                    f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                    f"-t raw -r 48000 -e signed -b 16 -c 1 - gain 3 | "
                    f"{sys.executable or 'python3'} -u {fanout_path} 127.0.0.1 {udp_port_ft8} {udp_port_ft4}"
                )
            else:
                udp_port = 3100 + self._rx
                self._start_decoder(udp_port, self._decoder_mode())
                pipeline_cmd = (
                    f"{sys.executable or 'python3'} {self._kiwirecorder_path} "
                    f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                    f"--rx-chan {self._rx} --user 'AUTO_{self._band}_{mode_tag}' --nc --quiet | "
                    f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                    f"-t raw -r 48000 -e signed -b 16 -c 1 - gain 3 | "
                    f"{self._af2udp_path} {udp_port}"
                )
            try:
                log_path = Path("/tmp") / f"kiwi_rx{self._rx}_pipeline.log"
                log_fp = open(log_path, "a", encoding="utf-8")
                return subprocess.Popen(
                    pipeline_cmd,
                    shell=True,
                    stdout=log_fp,
                    stderr=log_fp,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            except Exception as e:
                logger.warning("auto-set spawn failed: %s", e)
                return None
        mode = "usb"
        if self._mode_label.strip().upper() in {"SSB", "PHONE"}:
            mode = self._ssb_assignment_sideband()
        cmd = [
            sys.executable or "python3",
            str(self._kiwirecorder_path),
            "-s",
            str(self._host),
            "-p",
            str(self._port),
            "-f",
            str(freq_khz),
            "-m",
            mode,
            "--rx-chan",
            str(self._rx),
            "--user",
            f"AUTO_{self._band}_{mode_tag}",
            "--nc",
            "--quiet",
        ]
        try:
            return subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:
            logger.warning("auto-set spawn failed: %s", e)
            return None

    def run(self) -> None:
        if self._is_ssb_scan():
            self._run_ssb_scan_loop()
            return
        consecutive_failures = 0
        max_backoff_s = 30.0
        unstable_window_s = 20.0
        while not self._stop.is_set():
            start_monotonic = time.monotonic()
            self._proc = self._spawn()
            if self._proc is None:
                consecutive_failures += 1
                backoff_s = min(max_backoff_s, float(2 ** min(consecutive_failures, 5)))
                if self._on_restart is not None:
                    try:
                        self._on_restart(self._rx, self._band, "spawn_failed", backoff_s, consecutive_failures)
                    except Exception:
                        pass
                time.sleep(backoff_s)
                continue
            proc_exited = False
            while not self._stop.is_set():
                if self._proc.poll() is not None:
                    proc_exited = True
                    break
                time.sleep(0.5)
            self._terminate_proc()
            if not self._stop.is_set():
                run_time_s = max(0.0, time.monotonic() - start_monotonic)
                if run_time_s < unstable_window_s:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

                backoff_s = min(max_backoff_s, float(2 ** min(consecutive_failures, 5)))
                if proc_exited and self._on_restart is not None:
                    try:
                        self._on_restart(self._rx, self._band, "process_exited", backoff_s, consecutive_failures)
                    except Exception:
                        pass
                time.sleep(backoff_s)


class ReceiverManager:
    def __init__(
        self,
        *,
        kiwirecorder_path: Path,
        ft8modem_path: Path,
        af2udp_path: Path,
        sox_path: str = "sox",
        decode_callback: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self._kiwirecorder_path = kiwirecorder_path
        self._ft8modem_path = ft8modem_path
        self._af2udp_path = af2udp_path
        self._sox_path = sox_path
        if self._sox_path == "sox":
            resolved = shutil.which("sox")
            self._sox_path = resolved or ""
            if not self._sox_path:
                logger.warning("sox not found in PATH; digital decode disabled")
        self._decode_callback = decode_callback
        self._lock = threading.Lock()
        self._workers: Dict[int, _ReceiverWorker] = {}
        self._assignments: Dict[int, ReceiverAssignment] = {}
        self._restart_total = 0
        self._restart_by_rx: Dict[int, int] = {}
        self._restart_last_unix: Optional[float] = None
        self._watchdog_state_by_rx: Dict[int, Dict[str, object]] = {}
        self._cleanup_orphan_processes()

    def _on_worker_restart(self, rx: int, band: str, reason: str, backoff_s: float, consecutive_failures: int) -> None:
        with self._lock:
            self._restart_total += 1
            self._restart_by_rx[int(rx)] = int(self._restart_by_rx.get(int(rx), 0)) + 1
            self._restart_last_unix = time.time()
            self._watchdog_state_by_rx[int(rx)] = {
                "band": str(band),
                "reason": str(reason),
                "backoff_s": float(backoff_s),
                "consecutive_failures": int(consecutive_failures),
                "updated_unix": float(self._restart_last_unix),
            }

    def metrics_snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {
                "restart_total": int(self._restart_total),
                "restart_by_rx": {str(k): int(v) for k, v in self._restart_by_rx.items()},
                "restart_last_unix": float(self._restart_last_unix) if self._restart_last_unix is not None else None,
                "active_workers": len(self._workers),
                "assigned_receivers": len(self._assignments),
                "watchdog_state_by_rx": {
                    str(k): dict(v)
                    for k, v in self._watchdog_state_by_rx.items()
                },
            }

    def health_summary(self) -> Dict[str, object]:
        with self._lock:
            channels: Dict[str, Dict[str, object]] = {}
            unstable = 0
            for rx in sorted(set(list(self._assignments.keys()) + list(self._watchdog_state_by_rx.keys()))):
                assignment = self._assignments.get(rx)
                wd = self._watchdog_state_by_rx.get(rx, {})
                consecutive = int(wd.get("consecutive_failures", 0) or 0)
                backoff_s = float(wd.get("backoff_s", 0.0) or 0.0)
                is_unstable = consecutive >= 3 or backoff_s >= 8.0
                if is_unstable:
                    unstable += 1
                channels[str(rx)] = {
                    "rx": int(rx),
                    "band": assignment.band if assignment else wd.get("band"),
                    "mode": assignment.mode_label if assignment else None,
                    "active": assignment is not None,
                    "restart_count": int(self._restart_by_rx.get(rx, 0)),
                    "consecutive_failures": consecutive,
                    "backoff_s": backoff_s,
                    "last_reason": wd.get("reason"),
                    "last_updated_unix": wd.get("updated_unix"),
                    "is_unstable": is_unstable,
                }

            active = len(self._assignments)
            overall = "healthy"
            if unstable > 0:
                overall = "degraded"
            if active == 0:
                overall = "idle"

            now = time.time()
            latest_update = None
            for payload in self._watchdog_state_by_rx.values():
                try:
                    ts = float(payload.get("updated_unix"))
                    latest_update = ts if latest_update is None else max(latest_update, ts)
                except Exception:
                    continue
            stale_seconds = None
            if active > 0:
                if latest_update is None:
                    stale_seconds = 0.0
                else:
                    stale_seconds = max(0.0, now - latest_update)

            return {
                "overall": overall,
                "active_receivers": active,
                "unstable_receivers": unstable,
                "restart_total": int(self._restart_total),
                "health_stale_seconds": stale_seconds,
                "channels": channels,
            }

    def _cleanup_orphan_processes(self) -> None:
        try:
            subprocess.run(
                ["pkill", "-f", "kiwirecorder.py.*AUTO_"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass
        try:
            subprocess.run(
                ["pkill", "-f", "ft8modem.*udp:"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def apply_assignments(self, host: str, port: int, assignments: Dict[int, ReceiverAssignment]) -> None:
        with self._lock:
            if self._assignments == assignments:
                return

            for worker in list(self._workers.values()):
                worker.stop()
            self._workers.clear()
            self._assignments.clear()
            self._cleanup_orphan_processes()

            if not assignments:
                return

            time.sleep(0.5)

            for rx in sorted(assignments.keys()):
                desired = assignments[rx]
                worker = _ReceiverWorker(
                    kiwirecorder_path=self._kiwirecorder_path,
                    ft8modem_path=self._ft8modem_path,
                    af2udp_path=self._af2udp_path,
                    sox_path=self._sox_path,
                    host=host,
                    port=port,
                    rx=desired.rx,
                    band=desired.band,
                    freq_hz=desired.freq_hz,
                    mode_label=desired.mode_label,
                    ssb_scan=desired.ssb_scan,
                    sideband=desired.sideband,
                    decode_callback=self._decode_callback,
                    on_restart=self._on_worker_restart,
                )
                self._workers[rx] = worker
                self._assignments[rx] = desired
                worker.start()
                time.sleep(0.4)

    def stop_all(self) -> None:
        with self._lock:
            for worker in list(self._workers.values()):
                worker.stop()
            self._workers.clear()
            self._assignments.clear()
