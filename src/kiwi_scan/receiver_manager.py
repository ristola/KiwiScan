from __future__ import annotations

import logging
import re
import os
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import json
import urllib.parse
import urllib.request
import importlib.util
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
        self._python_cmd = self._resolve_python_cmd()
        self._stop = threading.Event()
        self._proc: Optional[subprocess.Popen] = None
        self._decoder_procs: list[subprocess.Popen] = []
        self._decoder_threads: list[threading.Thread] = []
        self._decoder_log_fps: list[Optional[object]] = []
        self._last_spawn_error_reason = "spawn_failed"
        self._active_user_label: str = ""
        self._cfg_lock = threading.Lock()
        self._reconfigure = threading.Event()
        try:
            self._rx_chan_adjust = int(str(os.environ.get("KIWISCAN_RX_CHAN_OFFSET", "0")).strip())
        except Exception:
            self._rx_chan_adjust = 0

    @staticmethod
    def _env_float(name: str, default: float, *, min_v: float, max_v: float) -> float:
        raw = str(os.environ.get(name, "")).strip()
        if not raw:
            return float(default)
        try:
            value = float(raw)
        except Exception:
            return float(default)
        return max(float(min_v), min(float(max_v), value))

    def _watchdog_base_backoff_s(self) -> float:
        return self._env_float("KIWISCAN_WATCHDOG_BASE_BACKOFF_S", 0.5, min_v=0.1, max_v=5.0)

    def _watchdog_max_backoff_s(self) -> float:
        return self._env_float("KIWISCAN_WATCHDOG_MAX_BACKOFF_S", 2.0, min_v=0.2, max_v=10.0)

    def _watchdog_channel_check_s(self) -> float:
        return self._env_float("KIWISCAN_WATCHDOG_CHANNEL_CHECK_S", 0.75, min_v=0.2, max_v=5.0)

    def _watchdog_loop_sleep_s(self) -> float:
        return self._env_float("KIWISCAN_WATCHDOG_LOOP_SLEEP_S", 0.2, min_v=0.05, max_v=2.0)

    def _watchdog_retry_backoff_s(self, consecutive_failures: int) -> float:
        base = self._watchdog_base_backoff_s()
        max_backoff = self._watchdog_max_backoff_s()
        failures = max(1, int(consecutive_failures))
        return min(max_backoff, float(base * (2 ** min(failures - 1, 3))))

    def _watchdog_spawn_retry_s(self) -> float:
        return min(self._watchdog_max_backoff_s(), max(self._watchdog_base_backoff_s(), 0.5))

    def _pipeline_log_path(self) -> Path:
        return Path("/tmp") / f"kiwi_rx{self._rx}_pipeline.log"

    def _classify_process_exit_reason(self) -> str:
        log_path = self._pipeline_log_path()
        if not log_path.exists():
            return "process_exited"
        try:
            lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-120:]
            joined = "\n".join(lines)
            if "Too busy now" in joined:
                return "kiwi_busy"
            if re.search(r"Connection refused|timed out|No route to host|Name or service not known|Temporary failure", joined, re.IGNORECASE):
                return "kiwi_connect_failed"
            if "ModuleNotFoundError" in joined and "numpy" in joined:
                return "missing_numpy"
            if re.search(r"No such file or directory|permission denied", joined, re.IGNORECASE):
                return "spawn_io_error"
        except Exception:
            pass
        return "process_exited"

    @staticmethod
    def _resolve_python_cmd() -> str:
        candidates: list[Path] = []
        venv = os.environ.get("VIRTUAL_ENV")
        if venv:
            candidates.append(Path(venv) / "bin" / "python")
            candidates.append(Path(venv) / "bin" / "python3")
        candidates.append(Path(sys.prefix) / "bin" / "python")
        candidates.append(Path(sys.prefix) / "bin" / "python3")
        try:
            candidates.append(Path(sys.executable))
        except Exception:
            pass
        for c in candidates:
            try:
                if c.exists() and os.access(str(c), os.X_OK):
                    return str(c)
            except Exception:
                continue
        return shutil.which("python3") or "python3"

    @staticmethod
    def _is_executable_file(path: Path) -> bool:
        try:
            return path.exists() and path.is_file() and os.access(str(path), os.X_OK)
        except Exception:
            return False

    @classmethod
    def _resolve_tool_path(cls, binary_name: str, fallback_path: Path) -> Optional[Path]:
        try:
            resolved = shutil.which(binary_name)
        except Exception:
            resolved = None
        if resolved:
            resolved_text = str(resolved).strip()
            if resolved_text and not resolved_text.startswith("/opt/local/"):
                candidate = Path(resolved_text)
                if cls._is_executable_file(candidate):
                    return candidate
        if cls._is_executable_file(fallback_path):
            return fallback_path
        return None

    @staticmethod
    def _resolve_ft8modem_temp_root() -> Optional[Path]:
        candidates: list[Path] = []
        override = str(os.environ.get("KIWISCAN_FT8MODEM_TMP", "")).strip()
        if override:
            candidates.append(Path(override))
        candidates.extend([
            Path("/tmp/ft8modem"),
            Path("/var/tmp/ft8modem"),
        ])
        for candidate in candidates:
            try:
                candidate.mkdir(mode=0o700, parents=True, exist_ok=True)
                if candidate.is_dir() and os.access(str(candidate), os.W_OK | os.X_OK):
                    return candidate
            except Exception:
                continue
        return None

    def _use_python_udp_sender(self) -> bool:
        raw = str(os.environ.get("KIWISCAN_USE_PY_UDP_AUDIO", "")).strip().lower()
        if raw:
            return raw not in {"0", "false", "no", "off"}
        return False

    def _resolve_python_udp_sender(self) -> Optional[Path]:
        candidates = [
            Path(__file__).resolve().parents[2] / "vendor" / "ft8modem-sm" / "udpaf.py",
            Path(__file__).resolve().parents[3] / "vendor" / "ft8modem-sm" / "udpaf.py",
        ]
        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_file():
                    return candidate
            except Exception:
                continue
        return None

    def _udp_audio_sender_cmd(self, *, udp_port: int, af2udp_path: Path) -> str:
        if self._use_python_udp_sender():
            udpaf_path = self._resolve_python_udp_sender()
            if udpaf_path is not None:
                return f"{shlex.quote(self._python_cmd)} -u {shlex.quote(str(udpaf_path))} {int(udp_port)}"
        return f"{shlex.quote(str(af2udp_path))} {int(udp_port)} 256 48000"

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

    @staticmethod
    def _terminate_external_proc(proc: Optional[subprocess.Popen]) -> None:
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

    def _kiwi_rx_chan(self) -> int:
        mode_norm = self._mode_label.strip().upper()
        if ("SSB" in mode_norm) or ("PHONE" in mode_norm):
            return int(self._rx)
        return int((int(self._rx) + int(self._rx_chan_adjust)) % 8)

    def _adapt_rx_chan_adjust(self, *, expected_rx: int, actual_rx: int, user_label: str) -> None:
        try:
            expected = int(expected_rx)
            actual = int(actual_rx)
        except Exception:
            return
        delta = int((expected - actual) % 8)
        if delta == 0:
            return
        old_adjust = int(self._rx_chan_adjust)
        self._rx_chan_adjust = int((old_adjust + delta) % 8)
        logger.warning(
            "Adapting rx-chan request offset for user=%s expected_rx=%s actual_rx=%s old_adjust=%s new_adjust=%s",
            user_label,
            expected,
            actual,
            old_adjust,
            int(self._rx_chan_adjust),
        )

    def _verify_kiwi_rx_channel(self, *, user_label: str, expected_rx: int, timeout_s: float = 3.0, strict: bool = True, require_visible: bool = False) -> bool:
        deadline = time.time() + max(0.5, float(timeout_s))
        expected = int(expected_rx)
        wanted = str(user_label or "").strip()
        if not wanted:
            return True
        status_url = f"http://{self._host}:{self._port}/users"
        while time.time() < deadline and not self._stop.is_set():
            try:
                with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if not isinstance(payload, list):
                    return True
                found_rx = None
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                    if not name:
                        continue
                    truncated_match = wanted.startswith(name) and len(name) >= max(8, len(wanted) - 3)
                    if not (name == wanted or name.startswith(wanted) or truncated_match):
                        continue
                    try:
                        found_rx = int(row.get("i"))
                    except Exception:
                        found_rx = None
                    break
                if found_rx is None:
                    time.sleep(0.15)
                    continue
                if bool(strict):
                    valid = (found_rx == expected)
                else:
                    valid = found_rx >= 0
                if valid:
                    if not bool(strict) and found_rx != expected:
                        logger.info(
                            "Kiwi remapped digital worker user=%s expected_rx=%s actual_rx=%s; keeping worker",
                            wanted,
                            expected,
                            found_rx,
                        )
                    return True
                logger.warning(
                    "Kiwi remapped worker user=%s expected_rx=%s actual_rx=%s strict=%s; forcing retry",
                    wanted,
                    expected,
                    found_rx,
                    bool(strict),
                )
                return False
            except Exception:
                return True
        if bool(strict):
            if bool(require_visible):
                logger.warning(
                    "Kiwi user=%s not visible on /users within %.1fs (expected_rx=%s); forcing retry",
                    wanted,
                    float(timeout_s),
                    expected,
                )
                return False
            logger.debug(
                "Kiwi user=%s not visible on /users within %.1fs (expected_rx=%s); keeping worker",
                wanted,
                float(timeout_s),
                expected,
            )
            return True
        if bool(require_visible):
            logger.warning(
                "Kiwi user=%s not visible on /users within %.1fs (expected_rx=%s); forcing retry",
                wanted,
                float(timeout_s),
                expected,
            )
            return False
        return True

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

    @staticmethod
    def _decoder_keep_wavs_enabled() -> bool:
        raw = str(os.environ.get("KIWISCAN_FT8MODEM_KEEP", "0") or "0").strip().lower()
        return raw not in {"", "0", "false", "no", "off"}

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
        return (norm in {"SSB", "PHONE"} or ("SSB" in norm) or ("PHONE" in norm)) and bool(self._ssb_scan)

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
        if int(self._rx) not in {0, 1}:
            logger.error(
                "Refusing to start SSB scan outside RX0/RX1: rx=%s band=%s mode=%s",
                self._rx,
                self._band,
                self._mode_label,
            )
            return

        scan_cfg = self._ssb_scan or {}
        wait_s = float(scan_cfg.get("wait_s") or 1.0)
        dwell_s = float(scan_cfg.get("dwell_s") or 6.0)
        tail_s = float(scan_cfg.get("tail_s") or 1.0)
        step_sequence = self._ssb_scan_step_sequence()
        step_index = 0
        mismatch_failures = 0

        while not self._stop.is_set():
            self._reconfigure.clear()
            step_khz = step_sequence[min(step_index, len(step_sequence) - 1)]
            freqs = self._ssb_scan_freqs_khz(step_khz)
            if not freqs:
                time.sleep(self._watchdog_spawn_retry_s())
                continue

            yaml_path = Path("/tmp") / f"kiwi_scan_ssb_rx{self._rx}_{self._band}_{step_khz:.1f}.yaml"
            try:
                self._write_ssb_scan_yaml(freqs_khz=freqs, path=yaml_path)
            except Exception:
                time.sleep(self._watchdog_spawn_retry_s())
                continue

            cmd = [
                self._python_cmd,
                str(self._kiwirecorder_path),
                "-s",
                str(self._host),
                "-p",
                str(self._port),
                "-m",
                self._ssb_scan_sideband(),
                "--rx-chan",
                str(self._kiwi_rx_chan()),
                "--user",
                f"AUTO_{self._band}_SSBSCAN",
                "--scan-yaml",
                str(yaml_path),
                "--squelch-tail",
                str(tail_s),
                "--log_level=info",
            ]
            self._active_user_label = f"AUTO_{self._band}_SSBSCAN"

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
                time.sleep(self._watchdog_spawn_retry_s())
                continue

            if not self._verify_kiwi_rx_channel(
                user_label=f"AUTO_{self._band}_SSBSCAN",
                expected_rx=self._rx,
                timeout_s=6.0,
                strict=True,
                require_visible=True,
            ):
                self._terminate_external_proc(proc)
                mismatch_failures += 1
                backoff_s = self._watchdog_retry_backoff_s(mismatch_failures)
                if self._on_restart is not None:
                    try:
                        self._on_restart(self._rx, self._band, "ssb_rx_mismatch", backoff_s, mismatch_failures)
                    except Exception:
                        pass
                time.sleep(backoff_s)
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
            proc_started_at = time.time()
            rapid_exit = False
            next_channel_check = time.time() + self._watchdog_channel_check_s()
            while not self._stop.is_set() and time.time() < end_time:
                if self._reconfigure.is_set():
                    rapid_exit = True
                    self._last_spawn_error_reason = "ssb_reconfigure"
                    break
                if proc.poll() is not None:
                    rapid_exit = (time.time() - proc_started_at) < 3.0
                    break
                if time.time() >= next_channel_check:
                    next_channel_check = time.time() + self._watchdog_channel_check_s()
                    if not self._verify_kiwi_rx_channel(
                        user_label=f"AUTO_{self._band}_SSBSCAN",
                        expected_rx=self._rx,
                        timeout_s=0.9,
                        strict=True,
                        require_visible=True,
                    ):
                        self._last_spawn_error_reason = "ssb_rx_mismatch"
                        mismatch_failures += 1
                        backoff_s = self._watchdog_retry_backoff_s(mismatch_failures)
                        if self._on_restart is not None:
                            try:
                                self._on_restart(self._rx, self._band, "ssb_rx_mismatch", backoff_s, mismatch_failures)
                            except Exception:
                                pass
                        rapid_exit = True
                        break
                time.sleep(self._watchdog_loop_sleep_s())

            self._terminate_proc()

            if self._stop.is_set():
                break

            if self._reconfigure.is_set():
                time.sleep(self._watchdog_loop_sleep_s())
                continue

            if rapid_exit:
                if str(self._last_spawn_error_reason or "") == "ssb_rx_mismatch":
                    backoff_s = self._watchdog_retry_backoff_s(mismatch_failures)
                    time.sleep(backoff_s)
                else:
                    time.sleep(self._watchdog_spawn_retry_s())
                continue

            mismatch_failures = 0

            if len(step_sequence) > 1:
                if hits["count"] > 0:
                    step_index = 0
                else:
                    step_index = min(step_index + 1, len(step_sequence) - 1)
            time.sleep(0.5)

    def update_assignment(
        self,
        *,
        band: str,
        freq_hz: float,
        mode_label: str,
        ssb_scan: Optional[dict],
        sideband: Optional[str],
    ) -> None:
        with self._cfg_lock:
            self._band = str(band)
            self._freq_hz = float(freq_hz)
            self._mode_label = str(mode_label or "FT8")
            self._ssb_scan = dict(ssb_scan or {}) if ssb_scan else None
            self._sideband = str(sideband).strip().upper() if sideband else None
        self._reconfigure.set()
        self._terminate_proc()

    def _start_decoder(self, udp_port: int, mode: str) -> None:
        ft8modem_path = self._resolve_tool_path("ft8modem", self._ft8modem_path)
        if ft8modem_path is None:
            return
        temp_root = self._resolve_ft8modem_temp_root()
        if temp_root is None:
            logger.warning("ft8modem temp root unavailable (tried /tmp/ft8modem, /var/tmp/ft8modem)")
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
            str(ft8modem_path),
            "-t",
            str(temp_root),
        ]
        if self._decoder_keep_wavs_enabled():
            cmd.append("-k")
        cmd.extend([
            "-r",
            "48000",
            mode,
            f"udp:{udp_port}",
        ])
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
            self._last_spawn_error_reason = "kiwirecorder_missing"
            return None
        freq_khz = self._format_freq_khz(self._freq_hz)
        mode_tag = self._mode_label.strip().upper().replace(" ", "").replace("/", "")
        user_label = f"AUTO_{self._band}_{mode_tag}"
        self._active_user_label = user_label
        if self._is_digital_mode():
            af2udp_path = self._resolve_tool_path("af2udp", self._af2udp_path)
            if af2udp_path is None:
                logger.warning("af2udp not executable on PATH or fallback at %s", self._af2udp_path)
                self._last_spawn_error_reason = "af2udp_missing"
                return None
            if self._is_dual_mode():
                udp_port_ft8 = 3100 + self._rx
                udp_port_ft4 = 3200 + self._rx
                self._start_decoder(udp_port_ft8, "FT8")
                self._start_decoder(udp_port_ft4, "FT4")
                fanout_path = Path(__file__).resolve().parent / "udp_fanout.py"
                pipeline_cmd = (
                    f"{self._python_cmd} {self._kiwirecorder_path} "
                    f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                    f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                    f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                        f"-t raw -r 48000 -e signed -b 16 -c 1 - | "
                    f"{self._python_cmd} -u {fanout_path} 127.0.0.1 {udp_port_ft8} {udp_port_ft4}"
                )
            else:
                udp_port = 3100 + self._rx
                self._start_decoder(udp_port, self._decoder_mode())
                udp_sender_cmd = self._udp_audio_sender_cmd(udp_port=udp_port, af2udp_path=af2udp_path)
                pipeline_cmd = (
                    f"{self._python_cmd} {self._kiwirecorder_path} "
                    f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                    f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                    f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                        f"-t raw -r 48000 -e signed -b 16 -c 1 - | "
                    f"{udp_sender_cmd}"
                )
            try:
                log_path = Path("/tmp") / f"kiwi_rx{self._rx}_pipeline.log"
                log_fp = open(log_path, "a", encoding="utf-8")
                log_fp.write(f"START {time.strftime('%Y-%m-%d %H:%M:%S')} CMD: {pipeline_cmd}\n")
                log_fp.flush()
                self._last_spawn_error_reason = "process_exited"
                proc = subprocess.Popen(
                    pipeline_cmd,
                    shell=True,
                    stdout=log_fp,
                    stderr=log_fp,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
                if int(self._rx) >= 2:
                    if not self._verify_kiwi_rx_channel(
                        user_label=user_label,
                        expected_rx=self._rx,
                        timeout_s=6.0,
                        strict=False,
                        require_visible=False,
                    ):
                        self._terminate_external_proc(proc)
                        self._last_spawn_error_reason = "nonssb_rx_mismatch"
                        return None
                return proc
            except Exception as e:
                logger.warning("auto-set spawn failed: %s", e)
                self._last_spawn_error_reason = "spawn_exception"
                return None
        mode = "usb"
        if ("SSB" in self._mode_label.strip().upper()) or ("PHONE" in self._mode_label.strip().upper()):
            if int(self._rx) not in {0, 1}:
                logger.error(
                    "Refusing to spawn SSB/PHONE outside RX0/RX1: rx=%s band=%s mode=%s",
                    self._rx,
                    self._band,
                    self._mode_label,
                )
                self._last_spawn_error_reason = "ssb_rx_policy_violation"
                return None
            mode = self._ssb_assignment_sideband()
        cmd = [
            self._python_cmd,
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
            str(self._kiwi_rx_chan()),
            "--user",
            user_label,
            "--nc",
            "--quiet",
        ]
        try:
            self._last_spawn_error_reason = "process_exited"
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            if ("SSB" in self._mode_label.strip().upper()) or ("PHONE" in self._mode_label.strip().upper()):
                if not self._verify_kiwi_rx_channel(user_label=user_label, expected_rx=self._rx, timeout_s=6.0, strict=True, require_visible=True):
                    self._terminate_external_proc(proc)
                    self._last_spawn_error_reason = "ssb_rx_mismatch"
                    return None
            return proc
        except Exception as e:
            logger.warning("auto-set spawn failed: %s", e)
            self._last_spawn_error_reason = "spawn_exception"
            return None

    def run(self) -> None:
        if self._is_ssb_scan():
            self._run_ssb_scan_loop()
            return
        consecutive_failures = 0
        unstable_window_s = 20.0
        while not self._stop.is_set():
            start_monotonic = time.monotonic()
            self._proc = self._spawn()
            if self._proc is None:
                consecutive_failures += 1
                backoff_s = self._watchdog_retry_backoff_s(consecutive_failures)
                if self._on_restart is not None:
                    try:
                        self._on_restart(self._rx, self._band, str(self._last_spawn_error_reason or "spawn_failed"), backoff_s, consecutive_failures)
                    except Exception:
                        pass
                time.sleep(backoff_s)
                continue
            proc_exited = False
            next_channel_check = time.time() + self._watchdog_channel_check_s()
            while not self._stop.is_set():
                if self._proc.poll() is not None:
                    proc_exited = True
                    break
                if time.time() >= next_channel_check:
                    next_channel_check = time.time() + self._watchdog_channel_check_s()
                    mode_norm = self._mode_label.strip().upper()
                    is_ssb = ("SSB" in mode_norm) or ("PHONE" in mode_norm)
                    strict = bool(is_ssb)
                    if not self._verify_kiwi_rx_channel(
                        user_label=self._active_user_label,
                        expected_rx=self._rx,
                        timeout_s=0.9,
                        strict=strict,
                        require_visible=is_ssb,
                    ):
                        self._last_spawn_error_reason = "ssb_rx_mismatch" if is_ssb else "nonssb_rx_mismatch"
                        proc_exited = True
                        break
                time.sleep(self._watchdog_loop_sleep_s())
            self._terminate_proc()
            if not self._stop.is_set():
                run_time_s = max(0.0, time.monotonic() - start_monotonic)
                if run_time_s < unstable_window_s:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

                backoff_s = self._watchdog_retry_backoff_s(consecutive_failures)
                reason = str(self._last_spawn_error_reason or "")
                if not reason or reason in {"process_exited", "spawn_failed", "spawn_exception"}:
                    reason = self._classify_process_exit_reason()
                if proc_exited and self._on_restart is not None:
                    try:
                        self._on_restart(self._rx, self._band, reason, backoff_s, consecutive_failures)
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
        self._last_dependency_report: Dict[str, object] = {}
        self._cleanup_orphan_processes()
        self._last_dependency_report = self.dependency_report()
        missing = self._last_dependency_report.get("missing")
        if isinstance(missing, list) and missing:
            logger.error("Receiver runtime dependencies missing: %s", ", ".join(str(m) for m in missing))

    @staticmethod
    def _mode_requires_digital(mode_label: str) -> bool:
        norm = str(mode_label or "").strip().upper()
        return norm in {"FT4", "FT8", "FT4 / FT8", "FT4/FT8", "FT8 / FT4", "FT8/FT4", "WSPR"}

    @staticmethod
    def _mode_is_wspr(mode_label: str) -> bool:
        return str(mode_label or "").strip().upper() == "WSPR"

    @staticmethod
    def _find_wsprd_path() -> str:
        direct = shutil.which("wsprd")
        if direct:
            return direct
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
                    candidates.append(app / "Contents" / "MacOS" / "wsprd")
        except Exception:
            pass
        for c in candidates:
            try:
                if c.exists() and os.access(str(c), os.X_OK):
                    return str(c)
            except Exception:
                continue
        return ""

    @staticmethod
    def _module_available(module_name: str) -> bool:
        try:
            return importlib.util.find_spec(module_name) is not None
        except Exception:
            return False

    @staticmethod
    def _is_executable_file(path: Path) -> bool:
        try:
            return path.exists() and path.is_file() and os.access(str(path), os.X_OK)
        except Exception:
            return False

    @classmethod
    def _resolve_tool_path(cls, binary_name: str, fallback_path: Path) -> Optional[Path]:
        try:
            resolved = shutil.which(binary_name)
        except Exception:
            resolved = None
        if resolved:
            resolved_text = str(resolved).strip()
            if resolved_text and not resolved_text.startswith("/opt/local/"):
                candidate = Path(resolved_text)
                if cls._is_executable_file(candidate):
                    return candidate
        if cls._is_executable_file(fallback_path):
            return fallback_path
        return None

    def dependency_report(self) -> Dict[str, object]:
        kiwirecorder_ok = self._kiwirecorder_path.exists()
        ft8modem_path = self._resolve_tool_path("ft8modem", self._ft8modem_path)
        af2udp_path = self._resolve_tool_path("af2udp", self._af2udp_path)
        ft8modem_ok = ft8modem_path is not None
        af2udp_ok = af2udp_path is not None
        sox_ok = bool(self._sox_path and self._is_executable_file(Path(self._sox_path)))
        wsprd_path = self._find_wsprd_path()
        wsprd_ok = bool(wsprd_path)
        numpy_ok = self._module_available("numpy")
        kiwi_ok = self._module_available("kiwi")
        kiwiclient_ok = self._module_available("kiwiclient")

        missing: list[str] = []
        if not kiwirecorder_ok:
            missing.append("kiwirecorder")
        if not ft8modem_ok:
            missing.append("ft8modem")
        if not af2udp_ok:
            missing.append("af2udp")
        if not sox_ok:
            missing.append("sox")
        if not numpy_ok:
            missing.append("numpy")

        report: Dict[str, object] = {
            "checked_unix": time.time(),
            "paths": {
                "kiwirecorder": str(self._kiwirecorder_path),
                "ft8modem": str(ft8modem_path or self._ft8modem_path),
                "af2udp": str(af2udp_path or self._af2udp_path),
                "sox": str(self._sox_path or ""),
                "wsprd": wsprd_path,
            },
            "status": {
                "kiwirecorder": bool(kiwirecorder_ok),
                "ft8modem": bool(ft8modem_ok),
                "af2udp": bool(af2udp_ok),
                "sox": bool(sox_ok),
                "wsprd": bool(wsprd_ok),
            },
            "python_modules": {
                "numpy": bool(numpy_ok),
                "kiwi": bool(kiwi_ok),
                "kiwiclient": bool(kiwiclient_ok),
            },
            "missing": missing,
            "ready_core": bool(kiwirecorder_ok),
            "ready_digital": bool(kiwirecorder_ok and ft8modem_ok and af2udp_ok and sox_ok and numpy_ok and (kiwi_ok or kiwiclient_ok)),
        }
        self._last_dependency_report = dict(report)
        return report

    def _required_dependency_errors(self, assignments: Dict[int, ReceiverAssignment]) -> list[str]:
        report = self.dependency_report()
        status = report.get("status") if isinstance(report, dict) else {}
        modules = report.get("python_modules") if isinstance(report, dict) else {}
        status = status if isinstance(status, dict) else {}
        modules = modules if isinstance(modules, dict) else {}

        errs: list[str] = []
        if not bool(status.get("kiwirecorder", False)):
            errs.append(f"kiwirecorder missing: {self._kiwirecorder_path}")

        requires_digital = any(self._mode_requires_digital(a.mode_label) for a in assignments.values())
        requires_wspr = any(self._mode_is_wspr(a.mode_label) for a in assignments.values())

        if requires_digital:
            if not bool(status.get("ft8modem", False)):
                errs.append(f"ft8modem missing: {self._ft8modem_path}")
            if not bool(status.get("af2udp", False)):
                errs.append(f"af2udp missing: {self._af2udp_path}")
            if not bool(status.get("sox", False)):
                errs.append("sox missing on PATH")
            if not bool(modules.get("numpy", False)):
                errs.append("python module missing: numpy")

        if requires_wspr and not bool(status.get("wsprd", False)):
            errs.append("wsprd missing (required for WSPR decode)")

        return errs

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

    def reset_metrics(self) -> Dict[str, object]:
        with self._lock:
            self._restart_total = 0
            self._restart_by_rx.clear()
            self._restart_last_unix = None
            self._watchdog_state_by_rx.clear()
        return self.metrics_snapshot()

    def health_summary(self) -> Dict[str, object]:
        with self._lock:
            assignments = dict(self._assignments)
            watchdog_by_rx = {int(k): dict(v) for k, v in self._watchdog_state_by_rx.items()}
            restart_by_rx = {int(k): int(v) for k, v in self._restart_by_rx.items()}
            restart_total = int(self._restart_total)
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)

        users_by_rx: Dict[int, str] = {}
        if active_host and active_port > 0:
            try:
                status_url = f"http://{active_host}:{active_port}/users"
                with urllib.request.urlopen(status_url, timeout=0.8) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if isinstance(payload, list):
                    for row in payload:
                        if not isinstance(row, dict):
                            continue
                        try:
                            rx_i = int(row.get("i"))
                        except Exception:
                            continue
                        name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                        users_by_rx[int(rx_i)] = name
            except Exception:
                users_by_rx = {}

        channels: Dict[str, Dict[str, object]] = {}
        unstable = 0
        reason_counts: Dict[str, int] = {}
        now = time.time()
        rx_set = sorted(set(list(assignments.keys()) + list(watchdog_by_rx.keys())))
        for rx in rx_set:
            assignment = assignments.get(rx)
            wd = watchdog_by_rx.get(rx, {})
            consecutive = int(wd.get("consecutive_failures", 0) or 0)
            backoff_s = float(wd.get("backoff_s", 0.0) or 0.0)
            updated_unix = wd.get("updated_unix")
            try:
                updated_unix_f = float(updated_unix)
            except Exception:
                updated_unix_f = None
            cooldown_remaining_s = 0.0
            if updated_unix_f is not None and backoff_s > 0.0:
                cooldown_remaining_s = max(0.0, (updated_unix_f + backoff_s) - now)
            cooling_down = cooldown_remaining_s > 0.0
            mode_label = str(assignment.mode_label or "").strip().upper() if assignment else ""
            is_ssb = mode_label in {"SSB", "PHONE"}

            is_active = assignment is not None
            last_reason = wd.get("reason")
            if is_ssb and assignment is not None:
                seen = str(users_by_rx.get(int(rx), "") or "")
                seen_upper = seen.upper()
                expected_prefix = f"AUTO_{str(assignment.band).upper()}_SSB"
                is_active = expected_prefix in seen_upper
                if not is_active:
                    last_reason = last_reason or "kiwi_not_visible"

            is_unstable = (assignment is not None) and (
                consecutive >= 3 or backoff_s >= 8.0 or (is_ssb and not is_active)
            )
            if is_unstable:
                unstable += 1
                reason = str(last_reason or "unknown")
                reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

            channels[str(rx)] = {
                "rx": int(rx),
                "band": assignment.band if assignment else wd.get("band"),
                "mode": assignment.mode_label if assignment else None,
                "active": bool(is_active),
                "restart_count": int(restart_by_rx.get(rx, 0)),
                "consecutive_failures": consecutive,
                "backoff_s": backoff_s,
                "cooling_down": bool(cooling_down),
                "cooldown_remaining_s": float(cooldown_remaining_s),
                "last_reason": last_reason,
                "last_updated_unix": updated_unix_f,
                "is_unstable": is_unstable,
            }

        active = sum(1 for ch in channels.values() if bool(ch.get("active")))
        overall = "healthy"
        if unstable > 0:
            overall = "degraded"
        if active == 0:
            overall = "idle"

        latest_update = None
        for rx, assignment in assignments.items():
            if assignment is None:
                continue
            payload = watchdog_by_rx.get(rx, {})
            try:
                ts = float(payload.get("updated_unix"))
                latest_update = ts if latest_update is None else max(latest_update, ts)
            except Exception:
                continue
        stale_seconds = None
        if active > 0:
            if unstable <= 0:
                stale_seconds = 0.0
            elif latest_update is None:
                stale_seconds = 0.0
            else:
                stale_seconds = max(0.0, now - latest_update)

        return {
            "overall": overall,
            "active_receivers": active,
            "unstable_receivers": unstable,
            "restart_total": restart_total,
            "health_stale_seconds": stale_seconds,
            "reason_counts": reason_counts,
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

    def _wait_for_orphan_cleanup(self, timeout_s: float = 6.0) -> None:
        deadline = time.time() + max(0.5, float(timeout_s))
        while time.time() < deadline:
            try:
                r1 = subprocess.run(
                    ["pgrep", "-f", "kiwirecorder.py.*AUTO_"],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                r2 = subprocess.run(
                    ["pgrep", "-f", "ft8modem.*udp:"],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                active_auto = bool(str(r1.stdout or "").strip())
                active_ft8modem = bool(str(r2.stdout or "").strip())
                if not active_auto and not active_ft8modem:
                    return
            except Exception:
                return
            time.sleep(0.2)
        try:
            subprocess.run(
                ["pkill", "-f", "ft8modem.*udp:"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def _wait_for_kiwi_auto_users_clear(self, *, host: str, port: int, timeout_s: float = 10.0) -> None:
        deadline = time.time() + max(1.0, float(timeout_s))
        status_url = f"http://{host}:{int(port)}/users"
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if not isinstance(payload, list):
                    return
                auto_count = 0
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                    if name.startswith("AUTO_"):
                        auto_count += 1
                if auto_count == 0:
                    return
            except Exception:
                return
            time.sleep(0.25)

    @staticmethod
    def _is_ssb_mode_label(mode_label: str) -> bool:
        norm = str(mode_label or "").strip().upper()
        return norm in {"SSB", "PHONE"} or ("SSB" in norm) or ("PHONE" in norm)

    @classmethod
    def _is_ssb_assignment(cls, assignment: ReceiverAssignment) -> bool:
        return bool(assignment.ssb_scan) or cls._is_ssb_mode_label(assignment.mode_label)

    @staticmethod
    def _normalized_ssb_scan_cfg(scan_cfg: Optional[dict]) -> dict:
        if not scan_cfg:
            return {}
        cfg = dict(scan_cfg)
        cfg.pop("threshold_db", None)
        return cfg

    @classmethod
    def _assignment_equivalent(cls, current: ReceiverAssignment, desired: ReceiverAssignment) -> bool:
        if int(current.rx) != int(desired.rx):
            return False
        if str(current.band) != str(desired.band):
            return False
        if str(current.mode_label or "").strip().upper() != str(desired.mode_label or "").strip().upper():
            return False
        if str(current.sideband or "").strip().upper() != str(desired.sideband or "").strip().upper():
            return False
        try:
            if abs(float(current.freq_hz) - float(desired.freq_hz)) > 0.5:
                return False
        except Exception:
            if current.freq_hz != desired.freq_hz:
                return False
        if cls._normalized_ssb_scan_cfg(current.ssb_scan) != cls._normalized_ssb_scan_cfg(desired.ssb_scan):
            return False
        return True

    @classmethod
    def _assignment_maps_equivalent(cls, current: Dict[int, ReceiverAssignment], desired: Dict[int, ReceiverAssignment]) -> bool:
        if set(current.keys()) != set(desired.keys()):
            return False
        for rx in current.keys():
            if not cls._assignment_equivalent(current[rx], desired[rx]):
                return False
        return True

    @classmethod
    def _can_hot_reconfigure_ssb(cls, current: ReceiverAssignment, desired: ReceiverAssignment) -> bool:
        return (
            int(current.rx) == int(desired.rx)
            and cls._is_ssb_assignment(current)
            and cls._is_ssb_assignment(desired)
        )

    def _normalize_ssb_receivers(self, assignments: Dict[int, ReceiverAssignment]) -> Dict[int, ReceiverAssignment]:
        if not assignments:
            return {}

        ssb_slots = [0, 1]
        normalized: Dict[int, ReceiverAssignment] = {}
        used_ssb_slots: set[int] = set()

        for rx in sorted(assignments.keys()):
            desired = assignments[rx]
            if not self._is_ssb_assignment(desired):
                continue

            preferred_slots: list[int] = []
            req_rx = int(desired.rx)
            key_rx = int(rx)
            if req_rx in ssb_slots:
                preferred_slots.append(req_rx)
            if key_rx in ssb_slots and key_rx not in preferred_slots:
                preferred_slots.append(key_rx)

            target_rx: Optional[int] = None
            for candidate in preferred_slots + ssb_slots:
                if candidate not in used_ssb_slots:
                    target_rx = int(candidate)
                    break

            if target_rx is None:
                logger.warning(
                    "Dropping SSB assignment outside RX0/RX1 capacity: requested rx=%s band=%s mode=%s",
                    key_rx,
                    desired.band,
                    desired.mode_label,
                )
                continue

            if target_rx != key_rx or target_rx != req_rx:
                logger.warning(
                    "Remapping SSB assignment to RX%s (requested key=%s desired.rx=%s band=%s mode=%s)",
                    target_rx,
                    key_rx,
                    req_rx,
                    desired.band,
                    desired.mode_label,
                )

            normalized[target_rx] = ReceiverAssignment(
                rx=target_rx,
                band=desired.band,
                freq_hz=desired.freq_hz,
                mode_label=desired.mode_label,
                ssb_scan=desired.ssb_scan,
                sideband=desired.sideband,
            )
            used_ssb_slots.add(target_rx)

        for rx in sorted(assignments.keys()):
            desired = assignments[rx]
            if self._is_ssb_assignment(desired):
                continue
            key_rx = int(rx)
            if key_rx in normalized:
                logger.warning(
                    "Dropping non-SSB assignment due to RX collision after SSB policy enforcement: rx=%s band=%s mode=%s",
                    key_rx,
                    desired.band,
                    desired.mode_label,
                )
                continue
            normalized[key_rx] = desired

        return normalized

    def apply_assignments(self, host: str, port: int, assignments: Dict[int, ReceiverAssignment]) -> None:
        with self._lock:
            assignments = self._normalize_ssb_receivers(assignments)
            if self._assignment_maps_equivalent(self._assignments, assignments):
                return

            dep_errors = self._required_dependency_errors(assignments)
            if dep_errors:
                logger.error("Cannot apply Kiwi receiver assignments due to missing runtime dependencies: %s", "; ".join(dep_errors))
                now = time.time()
                for rx in sorted(assignments.keys()):
                    self._watchdog_state_by_rx[int(rx)] = {
                        "band": str(assignments[rx].band),
                        "reason": "dependency_missing",
                        "backoff_s": 30.0,
                        "consecutive_failures": 1,
                        "updated_unix": float(now),
                    }
                return

            host_changed = (
                str(getattr(self, "_active_host", "")) != str(host)
                or int(getattr(self, "_active_port", -1)) != int(port)
            )

            current_rxs = set(int(rx) for rx in self._assignments.keys())
            desired_rxs = set(int(rx) for rx in assignments.keys())

            to_stop: set[int] = set()
            to_reconfigure: set[int] = set()
            if host_changed:
                to_stop |= current_rxs
            else:
                for rx in sorted(current_rxs):
                    if rx not in assignments:
                        to_stop.add(rx)
                        continue
                    if not self._assignment_equivalent(self._assignments[rx], assignments[rx]):
                        if self._can_hot_reconfigure_ssb(self._assignments[rx], assignments[rx]):
                            to_reconfigure.add(rx)
                        else:
                            to_stop.add(rx)

            for rx in sorted(to_stop):
                worker = self._workers.pop(rx, None)
                if worker is not None:
                    worker.stop()

            for rx in sorted(to_reconfigure):
                worker = self._workers.get(rx)
                desired = assignments.get(rx)
                if worker is None or desired is None:
                    continue
                worker.update_assignment(
                    band=desired.band,
                    freq_hz=desired.freq_hz,
                    mode_label=desired.mode_label,
                    ssb_scan=desired.ssb_scan,
                    sideband=desired.sideband,
                )

            if not assignments:
                self._assignments.clear()
                self._active_host = str(host)
                self._active_port = int(port)
                self._cleanup_orphan_processes()
                self._wait_for_orphan_cleanup(timeout_s=6.0)
                self._wait_for_kiwi_auto_users_clear(host=host, port=port, timeout_s=10.0)
                return

            time.sleep(0.2)

            for rx in sorted(desired_rxs):
                if rx in to_reconfigure and rx in self._workers:
                    continue
                if rx in self._workers and rx in self._assignments and self._assignment_equivalent(self._assignments[rx], assignments[rx]) and not host_changed:
                    continue
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
                worker.start()
                time.sleep(0.25)

            self._assignments = {int(rx): assignments[int(rx)] for rx in sorted(desired_rxs)}
            self._active_host = str(host)
            self._active_port = int(port)

    def stop_all(self) -> None:
        with self._lock:
            for worker in list(self._workers.values()):
                worker.stop()
            self._workers.clear()
            self._assignments.clear()
