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

# FT8 and FT4 dial frequencies (Hz) per band, used to compute the IQ centre
# and sub-band offsets when running dual-mode (FT4 / FT8).
# 60m has no standard FT4 frequency and is excluded.
# Bands where the two separations are > 10 kHz fall back to USB+fanout.
_BAND_DUAL_FREQS: dict[str, tuple[float, float]] = {
    "10m":  (28.074e6, 28.180e6),   # 106 kHz apart — too wide for IQ
    "12m":  (24.915e6, 24.919e6),   # 4 kHz — IQ OK
    "15m":  (21.074e6, 21.140e6),   # 66 kHz apart — too wide for IQ
    "17m":  (18.100e6, 18.104e6),   # 4 kHz — IQ OK
    "20m":  (14.074e6, 14.080e6),   # 6 kHz — IQ OK
    "30m":  (10.136e6, 10.140e6),   # 4 kHz — IQ OK
    "40m":  (7.074e6,  7.0475e6),   # 26.5 kHz apart — too wide for IQ
    "80m":  (3.573e6,  3.575e6),    # 2 kHz — IQ OK
    "160m": (1.840e6,  1.843e6),    # 3 kHz — IQ OK
}
# FT4 + WSPR IQ pairs (bands where FT4 and WSPR dial freqs are < 10 kHz apart)
_BAND_FT4_WSPR_PAIRS: dict[str, tuple[float, float]] = {
    "40m": (7.0475e6, 7.0386e6),   # 8.9 kHz span — IQ OK
}

# WSPR dial frequencies (Hz) per band
_BAND_WSPR_FREQS: dict[str, float] = {
    "160m": 1.8366e6,
    "80m":  3.5686e6,
    "40m":  7.0386e6,
    "30m":  10.1387e6,
    "20m":  14.0956e6,
    "17m":  18.1046e6,
    "15m":  21.0946e6,
    "12m":  24.9246e6,
    "10m":  28.1246e6,
    "60m":  5.2887e6,
}
_IQ_MAX_SEPARATION_HZ = 10_000  # ±5 kHz fits safely within 12 kHz IQ bandwidth


def _ft4_wspr_iq_params(band: str) -> "tuple[str, float, float] | None":
    """Return (iq_centre_khz_str, ft4_offset_hz, wspr_offset_hz) for FT4+WSPR IQ dual-mode."""
    pair = _BAND_FT4_WSPR_PAIRS.get(band)
    if pair is None:
        return None
    ft4_hz, wspr_hz = pair
    if abs(ft4_hz - wspr_hz) > _IQ_MAX_SEPARATION_HZ:
        return None
    centre_hz = (ft4_hz + wspr_hz) / 2.0
    ft4_off = ft4_hz - centre_hz
    wspr_off = wspr_hz - centre_hz
    khz = centre_hz / 1000.0
    centre_str = f"{khz:.3f}".rstrip("0").rstrip(".")
    return centre_str, ft4_off, wspr_off


def _dual_mode_iq_params(band: str) -> "tuple[str, float, float] | None":
    """Return (iq_centre_khz_str, ft8_offset_hz, ft4_offset_hz) when the band's
    FT8 and FT4 dial frequencies are close enough to share a 12 kHz IQ window.
    Returns None for bands where they are too far apart.
    """
    pair = _BAND_DUAL_FREQS.get(band)
    if pair is None:
        return None
    ft8_hz, ft4_hz = pair
    if abs(ft8_hz - ft4_hz) > _IQ_MAX_SEPARATION_HZ:
        return None
    centre_hz = (ft8_hz + ft4_hz) / 2.0
    ft8_off = ft8_hz - centre_hz
    ft4_off = ft4_hz - centre_hz
    khz = centre_hz / 1000.0
    centre_str = f"{khz:.3f}".rstrip("0").rstrip(".")
    return centre_str, ft8_off, ft4_off


def _triple_mode_iq_params(band: str) -> "tuple[str, float, float, float] | None":
    """Return (iq_centre_khz_str, ft8_off, ft4_off, wspr_off) when FT8, FT4,
    and WSPR dial frequencies all fit inside the 12 kHz IQ window.
    Returns None when the span is too wide or the band has no FT4.
    """
    pair = _BAND_DUAL_FREQS.get(band)
    if pair is None:
        return None  # band has no FT4 (e.g. 60m, 160m standalone)
    ft8_hz, ft4_hz = pair
    wspr_hz = _BAND_WSPR_FREQS.get(band)
    if wspr_hz is None:
        return None
    freqs = [ft8_hz, ft4_hz, wspr_hz]
    lo, hi = min(freqs), max(freqs)
    if hi - lo > _IQ_MAX_SEPARATION_HZ:
        return None
    centre_hz = (lo + hi) / 2.0
    ft8_off  = ft8_hz  - centre_hz
    ft4_off  = ft4_hz  - centre_hz
    wspr_off = wspr_hz - centre_hz
    khz = centre_hz / 1000.0
    centre_str = f"{khz:.3f}".rstrip("0").rstrip(".")
    return centre_str, ft8_off, ft4_off, wspr_off


@dataclass(frozen=True)
class ReceiverAssignment:
    rx: int
    band: str
    freq_hz: float
    mode_label: str
    ssb_scan: Optional[dict] = None
    sideband: Optional[str] = None
    ignore_slot_check: bool = False


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
        on_activity: Optional[Callable[[int, str, str, str, Optional[float]], None]] = None,
        initial_rx_chan_adjust: int = 0,
        ignore_slot_check: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self._ignore_slot_check = bool(ignore_slot_check)
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
        self._on_activity = on_activity
        self._python_cmd = self._resolve_python_cmd()
        self._stop_event = threading.Event()
        self._proc: Optional[subprocess.Popen] = None
        self._decoder_procs: list[subprocess.Popen] = []
        self._decoder_threads: list[threading.Thread] = []
        self._decoder_log_fps: list[Optional[object]] = []
        self._last_spawn_error_reason = "spawn_failed"
        self._active_user_label: str = ""
        self._cfg_lock = threading.Lock()
        self._reconfigure = threading.Event()
        self._slot_ready = threading.Event()
        try:
            env_adjust = int(str(os.environ.get("KIWISCAN_RX_CHAN_OFFSET", "0")).strip())
        except Exception:
            env_adjust = 0
        self._rx_chan_adjust = int(initial_rx_chan_adjust) if initial_rx_chan_adjust else env_adjust

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

    def _digital_remap_grace_s(self) -> float:
        return self._env_float("KIWISCAN_DIGITAL_REMAP_GRACE_S", 20.0, min_v=5.0, max_v=300.0)

    def _strict_digital_slot_enforcement(self) -> bool:
        raw = str(os.environ.get("KIWISCAN_STRICT_DIGITAL_SLOT_ENFORCEMENT", "1")).strip().lower()
        return raw not in {"0", "false", "no", "off"}

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

    def stop(self, join_timeout_s: float = 3.0) -> None:
        self._stop_event.set()
        self._terminate_proc()
        if threading.current_thread() is self:
            return
        try:
            self.join(timeout=max(0.0, float(join_timeout_s)))
        except Exception:
            pass

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

    def _wait_for_kiwi_user_connected(self, user_label: str, timeout_s: float = 8.0) -> None:
        """Poll /users until user label appears, confirming Kiwi connection is established."""
        deadline = time.time() + max(1.0, float(timeout_s))
        wanted = str(user_label or "").strip()
        if not wanted:
            return
        status_url = f"http://{self._host}:{self._port}/users"
        while time.time() < deadline and not self._stop_event.is_set():
            try:
                with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if isinstance(payload, list):
                    for row in payload:
                        if not isinstance(row, dict):
                            continue
                        name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                        if name == wanted or name.startswith(wanted) or wanted.startswith(name):
                            return
            except Exception:
                pass
            time.sleep(0.3)

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
        while time.time() < deadline and not self._stop_event.is_set():
            try:
                with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if not isinstance(payload, list):
                    return True
                found_rx = None
                found_age_s = None
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
                    try:
                        age_text = str(row.get("t") or "").strip()
                        parts = [int(part) for part in age_text.split(":") if str(part).strip()]
                        if len(parts) == 3:
                            found_age_s = max(0, (parts[0] * 3600) + (parts[1] * 60) + parts[2])
                        elif len(parts) == 2:
                            found_age_s = max(0, (parts[0] * 60) + parts[1])
                    except Exception:
                        found_age_s = None
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
                        grace_s = self._digital_remap_grace_s()
                        if found_age_s is not None and found_age_s >= grace_s:
                            self._adapt_rx_chan_adjust(expected_rx=expected, actual_rx=found_rx, user_label=wanted)
                            logger.warning(
                                "Kiwi remapped digital worker user=%s expected_rx=%s actual_rx=%s age_s=%s exceeds grace %.1fs; keeping worker with adapted offset",
                                wanted,
                                expected,
                                found_rx,
                                found_age_s,
                                grace_s,
                            )
                            return True
                        logger.info(
                            "Kiwi remapped digital worker user=%s expected_rx=%s actual_rx=%s age_s=%s; keeping worker during grace",
                            wanted,
                            expected,
                            found_rx,
                            found_age_s,
                        )
                    return True
                if expected >= 2 and found_rx >= 0:
                    self._adapt_rx_chan_adjust(expected_rx=expected, actual_rx=found_rx, user_label=wanted)
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
        return norm in {
            "FT4", "FT8", "FT4 / FT8", "FT4/FT8", "FT8 / FT4", "FT8/FT4",
            "WSPR", "FT4 / FT8 / WSPR", "FT4 / WSPR",
        }

    def _is_triple_mode(self) -> bool:
        norm = self._mode_label.strip().upper()
        return "FT4" in norm and "FT8" in norm and "WSPR" in norm

    def _is_dual_mode(self) -> bool:
        norm = self._mode_label.strip().upper()
        return "FT4" in norm and "FT8" in norm and "WSPR" not in norm

    def _is_ft4_wspr_mode(self) -> bool:
        norm = self._mode_label.strip().upper()
        return norm == "FT4 / WSPR"

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

        while not self._stop_event.is_set():
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
                expected_rx=self._kiwi_rx_chan(),
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
                    if self._stop_event.is_set():
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
            while not self._stop_event.is_set() and time.time() < end_time:
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
                        expected_rx=self._kiwi_rx_chan(),
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

            if self._stop_event.is_set():
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

        def _decode_line_snr(msg: str) -> Optional[float]:
            text = str(msg or "").strip()
            if not text.startswith("D:"):
                return None
            parts = text.split()
            if len(parts) < 4:
                return None
            mode_name = str(parts[1] or "").strip().upper()
            if mode_name in {"FT8", "FT4", "JT9", "JT65"}:
                try:
                    return float(parts[3])
                except Exception:
                    return None
            # D: WSPR <epoch> <date> <time> <sync> <snr> ...
            if mode_name == "WSPR" and len(parts) >= 7:
                try:
                    return float(parts[6])
                except Exception:
                    return None
            return None

        def _reader() -> None:
            if proc.stdout is None:
                return
            for line in proc.stdout:
                if self._stop_event.is_set():
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
                if self._on_activity is not None:
                    try:
                        self._on_activity(self._rx, self._band, mode, "decoder_output")
                    except Exception:
                        pass
                if not msg.startswith("D:"):
                    continue
                snr_db = _decode_line_snr(msg)
                if self._on_activity is not None:
                    try:
                        self._on_activity(self._rx, self._band, mode, "decode", snr_db)
                    except Exception:
                        pass
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

        # For WSPR mode, wsprd writes decoded spots to a file rather than stdout.
        # Tail that file and fire decode + activity callbacks for each new spot.
        if str(mode or "").strip().upper() == "WSPR":
            spots_path = Path(str(temp_root)) / f"udp-{udp_port}" / "wspr_spots.txt"

            def _wspr_spots_reader() -> None:
                """Continuously tail wspr_spots.txt and publish each new spot."""
                file_pos: Optional[int] = None
                while not self._stop_event.is_set():
                    try:
                        if not spots_path.exists():
                            self._stop_event.wait(2.0)
                            continue
                        with open(spots_path, "r", encoding="utf-8", errors="replace") as f:
                            if file_pos is None:
                                # First open: seek to end so we don't replay old spots.
                                f.seek(0, 2)
                                file_pos = f.tell()
                            else:
                                f.seek(file_pos)
                            for raw_line in f:
                                if self._stop_event.is_set():
                                    break
                                stripped = raw_line.strip()
                                if not stripped:
                                    continue
                                spot_parts = stripped.split()
                                # wsprd: date time sync snr dt freq_audio_mhz call grid power [...]
                                if len(spot_parts) < 9:
                                    continue
                                epoch = int(time.time())
                                d_line = f"D: WSPR {epoch} {stripped}"
                                snr_db: Optional[float] = None
                                try:
                                    snr_db = float(spot_parts[3])
                                except Exception:
                                    pass
                                if self._on_activity is not None:
                                    try:
                                        self._on_activity(
                                            self._rx, self._band,
                                            mode, "decode", snr_db,
                                        )
                                    except Exception:
                                        pass
                                if self._decode_callback is not None:
                                    try:
                                        self._decode_callback({
                                            "rx": self._rx,
                                            "band": self._band,
                                            "freq_hz": self._freq_hz,
                                            "mode_label": mode,
                                            "message": d_line,
                                        })
                                    except Exception:
                                        pass
                            file_pos = f.tell()
                    except Exception:
                        file_pos = None
                    self._stop_event.wait(5.0)

            self._decoder_threads.append(threading.Thread(target=_wspr_spots_reader, daemon=True))
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
        if self._stop_event.is_set():
            self._last_spawn_error_reason = "stop_requested"
            return None
        if self._is_digital_mode():
            af2udp_path = self._resolve_tool_path("af2udp", self._af2udp_path)
            if af2udp_path is None:
                logger.warning("af2udp not executable on PATH or fallback at %s", self._af2udp_path)
                self._last_spawn_error_reason = "af2udp_missing"
                return None
            if self._is_triple_mode():
                udp_port_ft8  = 3100 + self._rx
                udp_port_ft4  = 3200 + self._rx
                udp_port_wspr = 3300 + self._rx
                self._start_decoder(udp_port_ft8,  "FT8")
                self._start_decoder(udp_port_ft4,  "FT4")
                self._start_decoder(udp_port_wspr, "WSPR")
                iq_params = _triple_mode_iq_params(self._band)
                if iq_params is not None:
                    iq_centre_khz, ft8_off, ft4_off, wspr_off = iq_params
                    iq_splitter_path = Path(__file__).resolve().parent / "iq_splitter.py"
                    logger.info(
                        "IQ triple-mode rx=%s band=%s centre=%s kHz "
                        "ft8_off=%.0f ft4_off=%.0f wspr_off=%.0f Hz",
                        self._rx, self._band, iq_centre_khz, ft8_off, ft4_off, wspr_off,
                    )
                    pipeline_cmd = (
                        f"{self._python_cmd} {self._kiwirecorder_path} "
                        f"-s {self._host} -p {self._port} -f {iq_centre_khz} -m iq "
                        f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                        f"{self._python_cmd} -u {iq_splitter_path} "
                        f"{ft8_off:.1f} {udp_port_ft8} "
                        f"{ft4_off:.1f} {udp_port_ft4} "
                        f"{wspr_off:.1f} {udp_port_wspr}"
                    )
                else:
                    # Span too wide for a single IQ window; degrade to dual FT8+FT4
                    # and drop WSPR rather than waste two slots.
                    iq_dual = _dual_mode_iq_params(self._band)
                    if iq_dual is not None:
                        iq_centre_khz, ft8_off, ft4_off = iq_dual
                        iq_splitter_path = Path(__file__).resolve().parent / "iq_splitter.py"
                        logger.warning(
                            "Triple IQ not feasible for band=%s — falling back to FT8+FT4 dual IQ",
                            self._band,
                        )
                        pipeline_cmd = (
                            f"{self._python_cmd} {self._kiwirecorder_path} "
                            f"-s {self._host} -p {self._port} -f {iq_centre_khz} -m iq "
                            f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                            f"{self._python_cmd} -u {iq_splitter_path} "
                            f"{ft8_off:.1f} {udp_port_ft8} {ft4_off:.1f} {udp_port_ft4}"
                        )
                    else:
                        logger.warning(
                            "Triple/dual IQ not feasible for band=%s — USB FT8 only",
                            self._band,
                        )
                        udp_sender_cmd = self._udp_audio_sender_cmd(
                            udp_port=udp_port_ft8, af2udp_path=af2udp_path
                        )
                        pipeline_cmd = (
                            f"{self._python_cmd} {self._kiwirecorder_path} "
                            f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                            f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                            f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                                f"-t raw -r 48000 -e signed -b 16 -c 1 - | "
                            f"{udp_sender_cmd}"
                        )
            elif self._is_dual_mode():
                udp_port_ft8 = 3100 + self._rx
                udp_port_ft4 = 3200 + self._rx
                self._start_decoder(udp_port_ft8, "FT8")
                self._start_decoder(udp_port_ft4, "FT4")
                iq_params = _dual_mode_iq_params(self._band)
                if iq_params is not None:
                    # IQ dual-mode: tune to the midpoint and extract each sub-band
                    # via DSP in iq_splitter.py — both modes decoded from one RX slot.
                    iq_centre_khz, ft8_off_hz, ft4_off_hz = iq_params
                    iq_splitter_path = Path(__file__).resolve().parent / "iq_splitter.py"
                    logger.info(
                        "IQ dual-mode rx=%s band=%s centre=%s kHz "
                        "ft8_off=%.0f Hz ft4_off=%.0f Hz",
                        self._rx, self._band, iq_centre_khz, ft8_off_hz, ft4_off_hz,
                    )
                    pipeline_cmd = (
                        f"{self._python_cmd} {self._kiwirecorder_path} "
                        f"-s {self._host} -p {self._port} -f {iq_centre_khz} -m iq "
                        f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                        f"{self._python_cmd} -u {iq_splitter_path} "
                        f"{ft8_off_hz:.1f} {udp_port_ft8} {ft4_off_hz:.1f} {udp_port_ft4}"
                    )
                else:
                    # Bands too far apart for IQ window: fall back to USB + fanout.
                    # Both decoders receive the same audio tuned to the FT8 frequency.
                    fanout_path = Path(__file__).resolve().parent / "udp_fanout.py"
                    logger.info(
                        "USB fanout dual-mode fallback rx=%s band=%s (modes too far apart for IQ)",
                        self._rx, self._band,
                    )
                    pipeline_cmd = (
                        f"{self._python_cmd} {self._kiwirecorder_path} "
                        f"-s {self._host} -p {self._port} -f {freq_khz} -m usb "
                        f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                        f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                            f"-t raw -r 48000 -e signed -b 16 -c 1 - | "
                        f"{self._python_cmd} -u {fanout_path} 127.0.0.1 {udp_port_ft8} {udp_port_ft4}"
                    )
            elif self._is_ft4_wspr_mode():
                udp_port_ft4  = 3200 + self._rx
                udp_port_wspr = 3300 + self._rx
                self._start_decoder(udp_port_ft4,  "FT4")
                self._start_decoder(udp_port_wspr, "WSPR")
                iq_params = _ft4_wspr_iq_params(self._band)
                if iq_params is not None:
                    iq_centre_khz, ft4_off_hz, wspr_off_hz = iq_params
                    iq_splitter_path = Path(__file__).resolve().parent / "iq_splitter.py"
                    logger.info(
                        "IQ FT4+WSPR dual-mode rx=%s band=%s centre=%s kHz "
                        "ft4_off=%.0f Hz wspr_off=%.0f Hz",
                        self._rx, self._band, iq_centre_khz, ft4_off_hz, wspr_off_hz,
                    )
                    pipeline_cmd = (
                        f"{self._python_cmd} {self._kiwirecorder_path} "
                        f"-s {self._host} -p {self._port} -f {iq_centre_khz} -m iq "
                        f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                        f"{self._python_cmd} -u {iq_splitter_path} "
                        f"{ft4_off_hz:.1f} {udp_port_ft4} {wspr_off_hz:.1f} {udp_port_wspr}"
                    )
                else:
                    # IQ not feasible for this band; fall back to WSPR only.
                    logger.warning(
                        "FT4/WSPR IQ not feasible for band=%s — falling back to WSPR only",
                        self._band,
                    )
                    wspr_freq_hz = _BAND_WSPR_FREQS.get(self._band)
                    wspr_freq_khz = (
                        f"{wspr_freq_hz / 1000.0:.3f}".rstrip("0").rstrip(".")
                        if wspr_freq_hz else freq_khz
                    )
                    udp_sender_cmd = self._udp_audio_sender_cmd(
                        udp_port=udp_port_wspr, af2udp_path=af2udp_path
                    )
                    pipeline_cmd = (
                        f"{self._python_cmd} {self._kiwirecorder_path} "
                        f"-s {self._host} -p {self._port} -f {wspr_freq_khz} -m usb "
                        f"--rx-chan {self._kiwi_rx_chan()} --user '{user_label}' --nc --quiet | "
                        f"{self._sox_path} -t raw -r 12000 -e signed -b 16 -c 1 - "
                            f"-t raw -r 48000 -e signed -b 16 -c 1 - | "
                        f"{udp_sender_cmd}"
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
                if self._stop_event.is_set():
                    self._terminate_external_proc(proc)
                    self._last_spawn_error_reason = "stop_requested"
                    return None
                if int(self._rx) >= 2 and not self._ignore_slot_check:
                    strict_digital = self._strict_digital_slot_enforcement()
                    if not self._verify_kiwi_rx_channel(
                        user_label=user_label,
                        expected_rx=self._kiwi_rx_chan(),
                        timeout_s=6.0,
                        strict=bool(strict_digital),
                        require_visible=bool(strict_digital),
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
            if self._stop_event.is_set():
                self._terminate_external_proc(proc)
                self._last_spawn_error_reason = "stop_requested"
                return None
            if ("SSB" in self._mode_label.strip().upper()) or ("PHONE" in self._mode_label.strip().upper()):
                if not self._verify_kiwi_rx_channel(user_label=user_label, expected_rx=self._kiwi_rx_chan(), timeout_s=6.0, strict=True, require_visible=True):
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
        while not self._stop_event.is_set():
            start_monotonic = time.monotonic()
            self._proc = self._spawn()
            if self._proc is None:
                self._slot_ready.set()  # unblock waiter even on failure
                if self._stop_event.is_set():
                    break
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
            # Wait until Kiwi acknowledges the connection before signalling _slot_ready.
            # This ensures sequential slot acquisition so workers claim slots in order
            # (preventing racing when multiple workers start rapidly).
            if self._is_digital_mode() and self._active_user_label:
                self._wait_for_kiwi_user_connected(self._active_user_label, timeout_s=8.0)
            self._slot_ready.set()  # slot confirmed by _spawn()
            next_channel_check = time.time() + self._watchdog_channel_check_s()
            while not self._stop_event.is_set():
                if self._proc.poll() is not None:
                    proc_exited = True
                    break
                if time.time() >= next_channel_check:
                    next_channel_check = time.time() + self._watchdog_channel_check_s()
                    mode_norm = self._mode_label.strip().upper()
                    is_ssb = ("SSB" in mode_norm) or ("PHONE" in mode_norm)
                    if not self._ignore_slot_check:
                        strict = bool(is_ssb) or bool(self._strict_digital_slot_enforcement())
                        if not self._verify_kiwi_rx_channel(
                            user_label=self._active_user_label,
                            expected_rx=self._kiwi_rx_chan(),
                            timeout_s=0.9,
                            strict=strict,
                            require_visible=bool(strict),
                        ):
                            self._last_spawn_error_reason = "ssb_rx_mismatch" if is_ssb else "nonssb_rx_mismatch"
                            proc_exited = True
                            break
                time.sleep(self._watchdog_loop_sleep_s())
            self._terminate_proc()
            if not self._stop_event.is_set():
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
        self._activity_by_rx: Dict[int, Dict[str, object]] = {}
        self._stale_watch_state_by_rx: Dict[int, Dict[str, object]] = {}
        self._mismatch_global_streak = 0
        self._auto_kick_total = 0
        self._auto_kick_last_unix: Optional[float] = None
        self._auto_kick_last_reason = ""
        self._auto_kick_last_result = ""
        self._manager_stop = threading.Event()
        self._last_dependency_report: Dict[str, object] = {}
        self._cleanup_orphan_processes()
        self._last_dependency_report = self.dependency_report()
        missing = self._last_dependency_report.get("missing")
        if isinstance(missing, list) and missing:
            logger.error("Receiver runtime dependencies missing: %s", ", ".join(str(m) for m in missing))
        self._stale_recovery_thread = threading.Thread(target=self._stale_recovery_loop, daemon=True)
        self._stale_recovery_thread.start()

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = str(os.environ.get(name, "")).strip().lower()
        if not raw:
            return bool(default)
        return raw not in {"0", "false", "no", "off"}

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

    @staticmethod
    def _env_int(name: str, default: int, *, min_v: int, max_v: int) -> int:
        raw = str(os.environ.get(name, "")).strip()
        if not raw:
            return int(default)
        try:
            value = int(raw)
        except Exception:
            return int(default)
        return max(int(min_v), min(int(max_v), value))

    def _stale_recovery_enabled(self) -> bool:
        return self._env_bool("KIWISCAN_STALE_RECOVERY_ENABLED", True)

    def _stale_recovery_check_s(self) -> float:
        return self._env_float("KIWISCAN_STALE_RECOVERY_CHECK_S", 5.0, min_v=1.0, max_v=30.0)

    def _stale_recovery_min_age_s(self) -> float:
        return self._env_float("KIWISCAN_STALE_RECOVERY_MIN_AGE_S", 30.0, min_v=15.0, max_v=900.0)

    def _stale_recovery_cooldown_s(self) -> float:
        return self._env_float("KIWISCAN_STALE_RECOVERY_COOLDOWN_S", 180.0, min_v=10.0, max_v=3600.0)

    def _stale_recovery_required_checks(self) -> int:
        return self._env_int("KIWISCAN_STALE_RECOVERY_REQUIRED_CHECKS", 2, min_v=1, max_v=10)

    def _stale_recovery_mismatch_window_s(self) -> float:
        return self._env_float("KIWISCAN_STALE_RECOVERY_MISMATCH_WINDOW_S", 20.0, min_v=5.0, max_v=300.0)

    def _mismatch_autokick_enabled(self) -> bool:
        return self._env_bool("KIWISCAN_MISMATCH_AUTOKICK_ENABLED", True)

    def _mismatch_autokick_cooldown_s(self) -> float:
        return self._env_float("KIWISCAN_MISMATCH_AUTOKICK_COOLDOWN_S", 90.0, min_v=30.0, max_v=3600.0)

    def _mismatch_autokick_required_checks(self) -> int:
        return self._env_int("KIWISCAN_MISMATCH_AUTOKICK_REQUIRED_CHECKS", 4, min_v=1, max_v=30)

    def _mismatch_autokick_min_stalled(self) -> int:
        return self._env_int("KIWISCAN_MISMATCH_AUTOKICK_MIN_STALLED", 4, min_v=1, max_v=16)

    def _mismatch_autokick_min_fraction(self) -> float:
        return self._env_float("KIWISCAN_MISMATCH_AUTOKICK_MIN_FRACTION", 0.75, min_v=0.25, max_v=1.0)

    def _mismatch_autokick_timeout_s(self) -> float:
        return self._env_float("KIWISCAN_MISMATCH_AUTOKICK_TIMEOUT_S", 12.0, min_v=2.0, max_v=60.0)

    def _mismatch_require_decoder_gap(self) -> bool:
        return self._env_bool("KIWISCAN_MISMATCH_REQUIRE_DECODER_GAP", True)

    def _strict_digital_slot_enforcement(self) -> bool:
        return self._env_bool("KIWISCAN_STRICT_DIGITAL_SLOT_ENFORCEMENT", True)

    def _propagation_min_samples(self) -> int:
        return self._env_int("KIWISCAN_PROPAGATION_MIN_SAMPLES", 3, min_v=1, max_v=50)

    def _propagation_recent_max_s(self, silent_threshold_s: float) -> float:
        default_v = max(1200.0, float(silent_threshold_s))
        return self._env_float("KIWISCAN_PROPAGATION_RECENT_MAX_S", default_v, min_v=60.0, max_v=7200.0)

    def _propagation_good_snr_db(self) -> float:
        return self._env_float("KIWISCAN_PROPAGATION_GOOD_SNR_DB", -12.0, min_v=-40.0, max_v=20.0)

    def _propagation_fair_snr_db(self) -> float:
        return self._env_float("KIWISCAN_PROPAGATION_FAIR_SNR_DB", -20.0, min_v=-40.0, max_v=20.0)

    def _propagation_marginal_snr_db(self) -> float:
        return self._env_float("KIWISCAN_PROPAGATION_MARGINAL_SNR_DB", -25.0, min_v=-40.0, max_v=20.0)

    @staticmethod
    def _find_admin_kick_script() -> Optional[Path]:
        here = Path(__file__).resolve()
        for parent in here.parents:
            candidate = parent / "tools" / "kiwi_admin_kick.py"
            if candidate.exists():
                return candidate
        return None

    def _run_admin_kick_all(self, *, host: str, port: int, force_all: bool = False) -> bool:
        script = self._find_admin_kick_script()
        if script is None:
            return False

        live_auto_users = self._fetch_live_auto_users(str(host), int(port))
        kick_targets = sorted({int(rx) for rx in live_auto_users.keys()})

        cmd = [
            sys.executable,
            str(script),
            "--host",
            str(host),
            "--port",
            str(int(port)),
            "--user",
            "KiwiScanAutoKick",
        ]
        # force_all=True (used at initial startup) ensures ALL Kiwi channels are freed,
        # including sessions from a previous container run that may still be closing.
        if force_all or not kick_targets:
            cmd.append("--kick-all")
        else:
            for target in kick_targets:
                cmd.extend(["--kick", str(int(target))])
        password = str(os.environ.get("KIWISCAN_KIWI_ADMIN_PASSWORD", "") or "").strip()
        if password:
            cmd.extend(["--password", password])
        if self._env_bool("KIWISCAN_MISMATCH_AUTOKICK_TAKE_ADMIN", True):
            cmd.append("--take-admin")

        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self._mismatch_autokick_timeout_s(),
                check=False,
            )
        except Exception as exc:
            logger.warning("Mismatch auto-kick failed to execute: %s", exc)
            return False

        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if completed.returncode == 0:
            if stdout:
                logger.info("Mismatch auto-kick success: %s", stdout.splitlines()[-1])
            return True

        # Fallback: attempt generic kick-all in case per-target kick did not clear stuck sessions.
        fallback_cmd = [
            sys.executable,
            str(script),
            "--host",
            str(host),
            "--port",
            str(int(port)),
            "--kick-all",
            "--user",
            "KiwiScanAutoKick",
        ]
        if password:
            fallback_cmd.extend(["--password", password])
        if self._env_bool("KIWISCAN_MISMATCH_AUTOKICK_TAKE_ADMIN", True):
            fallback_cmd.append("--take-admin")
        try:
            fallback = subprocess.run(
                fallback_cmd,
                capture_output=True,
                text=True,
                timeout=self._mismatch_autokick_timeout_s(),
                check=False,
            )
            if fallback.returncode == 0:
                f_stdout = str(fallback.stdout or "").strip()
                if f_stdout:
                    logger.info("Mismatch auto-kick fallback success: %s", f_stdout.splitlines()[-1])
                return True
        except Exception:
            pass

        detail = stderr or stdout or f"exit={completed.returncode}"
        logger.warning("Mismatch auto-kick command failed: %s", detail)
        return False

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

    def _on_worker_activity(self, rx: int, band: str, mode_label: str, event_type: str, snr_db: Optional[float] = None) -> None:
        now = time.time()
        with self._lock:
            current = dict(self._activity_by_rx.get(int(rx), {}))
            current["band"] = str(band)
            current["mode"] = str(mode_label)
            current["last_event_type"] = str(event_type)
            current["last_event_unix"] = float(now)
            if event_type in {"decoder_output", "decode"}:
                current["last_decoder_output_unix"] = float(now)
            if event_type == "decode":
                current["last_decode_unix"] = float(now)
                current["decode_total"] = int(current.get("decode_total", 0) or 0) + 1
                ts_list: list = list(current.get("decode_timestamps", []) or [])
                ts_list.append(float(now))
                cutoff_1h = now - 3600.0
                ts_list = [t for t in ts_list if t >= cutoff_1h]
                current["decode_timestamps"] = ts_list
                _by_mode: dict = dict(current.get("_decode_ts_by_mode", {}) or {})
                _mode_ts: list = list(_by_mode.get(mode_label, []) or [])
                _mode_ts.append(float(now))
                _mode_ts = [t for t in _mode_ts if t >= cutoff_1h]
                _by_mode[mode_label] = _mode_ts
                current["_decode_ts_by_mode"] = _by_mode
                _total_by_mode: dict = dict(current.get("_decode_total_by_mode", {}) or {})
                _total_by_mode[mode_label] = int(_total_by_mode.get(mode_label, 0) or 0) + 1
                current["_decode_total_by_mode"] = _total_by_mode
            if event_type == "decode" and isinstance(snr_db, (float, int)):
                snr_value = float(snr_db)
                current["snr_last_db"] = snr_value
                current["last_snr_unix"] = float(now)
                prior_avg = current.get("snr_avg_db")
                try:
                    prior_avg_f = float(prior_avg)
                except Exception:
                    prior_avg_f = snr_value
                alpha = 0.2
                current["snr_avg_db"] = float((alpha * snr_value) + ((1.0 - alpha) * prior_avg_f))
                current["snr_samples"] = int(current.get("snr_samples", 0) or 0) + 1
            self._activity_by_rx[int(rx)] = current

    def _make_worker(self, *, host: str, port: int, assignment: ReceiverAssignment, rx_chan_adjust: int = 0) -> _ReceiverWorker:
        return _ReceiverWorker(
            kiwirecorder_path=self._kiwirecorder_path,
            ft8modem_path=self._ft8modem_path,
            af2udp_path=self._af2udp_path,
            sox_path=self._sox_path,
            host=host,
            port=port,
            rx=assignment.rx,
            band=assignment.band,
            freq_hz=assignment.freq_hz,
            mode_label=assignment.mode_label,
            ssb_scan=assignment.ssb_scan,
            sideband=assignment.sideband,
            decode_callback=self._decode_callback,
            on_restart=self._on_worker_restart,
            on_activity=self._on_worker_activity,
            initial_rx_chan_adjust=rx_chan_adjust,
            ignore_slot_check=bool(getattr(assignment, "ignore_slot_check", False)),
        )

    @staticmethod
    def _stop_worker(worker: Optional[_ReceiverWorker], *, join_timeout_s: float = 3.0) -> None:
        if worker is None:
            return
        try:
            worker.stop(join_timeout_s=join_timeout_s)
        except TypeError:
            worker.stop()

    def _restart_receiver_worker(self, rx: int, reason: str) -> bool:
        with self._lock:
            assignment = self._assignments.get(int(rx))
            host = str(getattr(self, "_active_host", "") or "")
            port = int(getattr(self, "_active_port", 0) or 0)
            old_worker = self._workers.pop(int(rx), None)
            self._activity_by_rx.pop(int(rx), None)
        if assignment is None or not host or port <= 0:
            if old_worker is not None:
                self._stop_worker(old_worker)
            return False

        old_adjust = int(old_worker._rx_chan_adjust) if old_worker is not None else 0
        if old_worker is not None:
            self._stop_worker(old_worker)

        new_worker = self._make_worker(host=host, port=port, assignment=assignment, rx_chan_adjust=old_adjust)
        with self._lock:
            if int(rx) not in self._assignments:
                new_worker.stop()
                return False
            self._workers[int(rx)] = new_worker
        new_worker.start()
        self._on_worker_restart(int(rx), str(assignment.band), str(reason), 0.0, 1)
        return True

    def _stale_recovery_loop(self) -> None:
        while not self._manager_stop.is_set():
            sleep_s = self._stale_recovery_check_s()
            if not self._stale_recovery_enabled():
                self._manager_stop.wait(timeout=sleep_s)
                continue

            min_age_s = self._stale_recovery_min_age_s()
            cooldown_s = self._stale_recovery_cooldown_s()
            required_checks = self._stale_recovery_required_checks()
            mismatch_window_s = self._stale_recovery_mismatch_window_s()

            now = time.time()
            summary = self.health_summary()
            channels = summary.get("channels") if isinstance(summary, dict) else {}
            channels = channels if isinstance(channels, dict) else {}

            with self._lock:
                active_rxs = set(int(rx) for rx in self._assignments.keys())
                self._stale_watch_state_by_rx = {
                    int(rx): dict(state)
                    for rx, state in self._stale_watch_state_by_rx.items()
                    if int(rx) in active_rxs
                }

            to_restart: list[tuple[int, str, float | None]] = []
            for rx in sorted(active_rxs):
                ch = channels.get(str(int(rx)))
                if not isinstance(ch, dict):
                    continue
                is_stalled = bool(ch.get("is_stalled"))
                reason = str(ch.get("last_reason") or "unknown")
                try:
                    decoder_age_s = float(ch.get("decoder_output_age_s"))
                except Exception:
                    decoder_age_s = None
                try:
                    kiwi_user_age_s = float(ch.get("kiwi_user_age_s"))
                except Exception:
                    kiwi_user_age_s = None

                stale_reason = reason in {"stalled_no_decoder_output", "kiwi_assignment_mismatch"}
                age_samples = [v for v in (decoder_age_s, kiwi_user_age_s) if isinstance(v, (float, int))]
                generic_age_ok = (not age_samples) or (max(float(v) for v in age_samples) >= min_age_s)

                with self._lock:
                    state = dict(self._stale_watch_state_by_rx.get(int(rx), {}))
                    prev_reason = str(state.get("reason") or "")
                    streak = int(state.get("streak", 0) or 0)
                    last_kick_unix = float(state.get("last_kick_unix", 0.0) or 0.0)
                    first_seen_unix = float(state.get("first_seen_unix", now) or now)

                    stale_age_ok = generic_age_ok
                    if reason == "kiwi_assignment_mismatch":
                        if prev_reason != reason:
                            first_seen_unix = float(now)
                        mismatch_age_s = max(0.0, now - first_seen_unix)
                        stale_age_ok = bool(generic_age_ok or mismatch_age_s >= mismatch_window_s)

                    is_candidate = bool(is_stalled and stale_reason and stale_age_ok)

                    if is_candidate:
                        streak = (streak + 1) if prev_reason == reason else 1
                        state["reason"] = reason
                        state["streak"] = int(streak)
                        state["first_seen_unix"] = float(first_seen_unix)
                        state["last_seen_unix"] = float(now)
                        if streak >= required_checks and (now - last_kick_unix) >= cooldown_s:
                            state["last_kick_unix"] = float(now)
                            state["streak"] = 0
                            self._stale_watch_state_by_rx[int(rx)] = state
                            to_restart.append((int(rx), reason, decoder_age_s))
                            continue
                    else:
                        state["streak"] = 0
                        state["reason"] = ""
                        state["first_seen_unix"] = float(now)
                        state["last_seen_unix"] = float(now)

                    self._stale_watch_state_by_rx[int(rx)] = state

            for rx, reason, decoder_age_s in to_restart:
                recycle_reason = f"stale_recovery_{reason}"
                restarted = self._restart_receiver_worker(rx=rx, reason=recycle_reason)
                if restarted:
                    logger.warning(
                        "Stale receiver recovery: recycled rx=%s reason=%s decoder_age_s=%s",
                        rx,
                        reason,
                        decoder_age_s,
                    )

            active_receivers = int(summary.get("active_receivers", 0) or 0)
            reason_counts = summary.get("reason_counts") if isinstance(summary, dict) else {}
            reason_counts = reason_counts if isinstance(reason_counts, dict) else {}
            mismatch_stalled = int(reason_counts.get("kiwi_assignment_mismatch", 0) or 0) + int(reason_counts.get("kiwi_assignment_mismatch_observed", 0) or 0)
            with self._lock:
                self._mismatch_global_streak = (
                    self._mismatch_global_streak + 1
                    if (
                        active_receivers > 0
                        and mismatch_stalled >= self._mismatch_autokick_min_stalled()
                        and (float(mismatch_stalled) / float(active_receivers)) >= self._mismatch_autokick_min_fraction()
                    )
                    else 0
                )
                mismatch_streak = int(self._mismatch_global_streak)
                last_auto_kick_unix = float(self._auto_kick_last_unix or 0.0)

            should_autokick = bool(
                self._mismatch_autokick_enabled()
                and mismatch_streak >= self._mismatch_autokick_required_checks()
                and (time.time() - last_auto_kick_unix) >= self._mismatch_autokick_cooldown_s()
            )
            duplicate_autokick = False
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)
            if active_host and active_port > 0:
                try:
                    duplicate_live_labels = self._has_duplicate_live_auto_labels(active_host, active_port)
                except Exception:
                    duplicate_live_labels = False
                duplicate_autokick = bool(
                    duplicate_live_labels
                    and (time.time() - last_auto_kick_unix) >= min(30.0, self._mismatch_autokick_cooldown_s())
                )

            if should_autokick:
                if active_host and active_port > 0:
                    kicked = self._run_admin_kick_all(host=active_host, port=active_port)
                    result = "ok" if kicked else "failed"
                    with self._lock:
                        self._auto_kick_last_unix = float(time.time())
                        self._auto_kick_last_reason = "kiwi_assignment_mismatch"
                        self._auto_kick_last_result = result
                        if kicked:
                            self._auto_kick_total += 1
                            self._mismatch_global_streak = 0
                    if kicked:
                        with self._lock:
                            reclaim_rxs = sorted(int(rx) for rx in self._assignments.keys())
                        for rx in reclaim_rxs:
                            self._restart_receiver_worker(rx=rx, reason="auto_kick_reclaim")
                        logger.warning(
                            "Mismatch auto-kick: reclaimed receivers after widespread mismatch (%s/%s)",
                            mismatch_stalled,
                            active_receivers,
                        )
                    else:
                        logger.warning(
                            "Mismatch auto-kick: command failed (%s/%s)",
                            mismatch_stalled,
                            active_receivers,
                        )

            if duplicate_autokick:
                kicked = self._run_admin_kick_all(host=active_host, port=active_port)
                result = "ok" if kicked else "failed"
                with self._lock:
                    self._auto_kick_last_unix = float(time.time())
                    self._auto_kick_last_reason = "duplicate_auto_labels"
                    self._auto_kick_last_result = result
                    if kicked:
                        self._auto_kick_total += 1
                if kicked:
                    with self._lock:
                        reclaim_rxs = sorted(int(rx) for rx in self._assignments.keys())
                    for rx in reclaim_rxs:
                        self._restart_receiver_worker(rx=rx, reason="duplicate_label_reclaim")
                    logger.warning("Duplicate-label auto-kick: reclaimed receivers after duplicate AUTO labels")
                else:
                    logger.warning("Duplicate-label auto-kick: command failed")

            self._manager_stop.wait(timeout=sleep_s)

    @staticmethod
    def _mode_decode_cycle_seconds(mode_label: str) -> float:
        norm = str(mode_label or "").strip().upper()
        if norm == "WSPR":
            return 120.0
        if norm == "FT4":
            return 7.5
        return 15.0

    @classmethod
    def _stall_threshold_seconds(cls, mode_label: str) -> float:
        cycle = cls._mode_decode_cycle_seconds(mode_label)
        return max(75.0, cycle * 4.0)

    @classmethod
    def _silent_threshold_seconds(cls, mode_label: str) -> float:
        norm = str(mode_label or "").strip().upper()
        if norm == "WSPR":
            return 1800.0
        if "FT4" in norm and "FT8" in norm:
            return 900.0
        if norm == "FT4":
            return 600.0
        return 900.0

    @staticmethod
    def _no_decode_warning_seconds() -> float:
        raw = str(os.environ.get("KIWISCAN_NO_DECODE_WARN_S", "")).strip()
        if not raw:
            return 120.0
        try:
            value = float(raw)
        except Exception:
            return 120.0
        return max(30.0, min(3600.0, value))

    @staticmethod
    def _digital_remap_grace_seconds() -> float:
        raw = str(os.environ.get("KIWISCAN_DIGITAL_REMAP_GRACE_S", "")).strip()
        if not raw:
            return 20.0
        try:
            value = float(raw)
        except Exception:
            return 20.0
        return max(5.0, min(300.0, value))

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
                "activity_by_rx": {
                    str(k): dict(v)
                    for k, v in self._activity_by_rx.items()
                },
                "mismatch_global_streak": int(self._mismatch_global_streak),
                "auto_kick_total": int(self._auto_kick_total),
                "auto_kick_last_unix": float(self._auto_kick_last_unix) if self._auto_kick_last_unix is not None else None,
                "auto_kick_last_reason": str(self._auto_kick_last_reason or ""),
                "auto_kick_last_result": str(self._auto_kick_last_result or ""),
            }

    def reset_metrics(self) -> Dict[str, object]:
        with self._lock:
            self._restart_total = 0
            self._restart_by_rx.clear()
            self._restart_last_unix = None
            self._watchdog_state_by_rx.clear()
            self._activity_by_rx.clear()
            self._mismatch_global_streak = 0
            self._auto_kick_total = 0
            self._auto_kick_last_unix = None
            self._auto_kick_last_reason = ""
            self._auto_kick_last_result = ""
        return self.metrics_snapshot()

    def health_summary(self) -> Dict[str, object]:
        with self._lock:
            assignments = dict(self._assignments)
            watchdog_by_rx = {int(k): dict(v) for k, v in self._watchdog_state_by_rx.items()}
            restart_by_rx = {int(k): int(v) for k, v in self._restart_by_rx.items()}
            activity_by_rx = {int(k): dict(v) for k, v in self._activity_by_rx.items()}
            restart_total = int(self._restart_total)
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)

        users_by_rx: Dict[int, str] = {}
        user_age_by_rx: Dict[int, int] = {}
        if active_host and active_port > 0:
            payload = None
            for path in ("/users?json=1", "/users?admin=1", "/users"):
                try:
                    status_url = f"http://{active_host}:{active_port}{path}"
                    with urllib.request.urlopen(status_url, timeout=0.8) as resp:
                        maybe = json.loads(resp.read().decode("utf-8", errors="ignore"))
                    if isinstance(maybe, list):
                        payload = maybe
                        break
                except Exception:
                    continue
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
                    try:
                        age_text = str(row.get("t") or "").strip()
                        parts = [int(part) for part in age_text.split(":") if str(part).strip()]
                        if len(parts) == 3:
                            user_age_by_rx[int(rx_i)] = max(0, (parts[0] * 3600) + (parts[1] * 60) + parts[2])
                        elif len(parts) == 2:
                            user_age_by_rx[int(rx_i)] = max(0, (parts[0] * 60) + parts[1])
                    except Exception:
                        pass

        live_auto_locations: Dict[str, list[tuple[int, int | None]]] = {}
        for rx_i, name in users_by_rx.items():
            label = str(name or "").strip().upper()
            if not label.startswith("AUTO_"):
                continue
            live_auto_locations.setdefault(label, []).append((int(rx_i), user_age_by_rx.get(int(rx_i))))

        channels: Dict[str, Dict[str, object]] = {}
        propagation_counts: Dict[str, int] = {"good": 0, "fair": 0, "marginal": 0, "poor": 0, "unknown": 0}
        propagation_score_total = 0.0
        propagation_score_count = 0
        unstable = 0
        silent = 0
        stalled = 0
        no_decode_warning = 0
        reason_counts: Dict[str, int] = {}
        now = time.time()
        remap_grace_s = self._digital_remap_grace_seconds()
        rx_set = sorted(set(list(assignments.keys()) + list(watchdog_by_rx.keys())))
        for rx in rx_set:
            assignment = assignments.get(rx)
            wd = watchdog_by_rx.get(rx, {})
            activity = activity_by_rx.get(rx, {})
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
            visible_on_kiwi = False
            kiwi_user_age_s = None
            visible_slot: int | None = None
            if is_ssb and assignment is not None:
                expected_prefix = f"AUTO_{str(assignment.band).upper()}_SSB"
                # Search all kiwi slots — workers may land on any slot index at connect time.
                # Use _user_label_matches to handle KiwiSDR username truncation (e.g. 16-char limit).
                visible_slot = next(
                    (slot for slot, name in users_by_rx.items() if self._user_label_matches(expected_prefix, name)),
                    None,
                )
                is_active = visible_slot is not None
                visible_on_kiwi = bool(is_active)
                kiwi_user_age_s = user_age_by_rx.get(visible_slot if visible_slot is not None else int(rx))
                if not is_active:
                    last_reason = last_reason or "kiwi_not_visible"
            elif assignment is not None:
                _mode_tag = str(assignment.mode_label or "FT8").strip().upper().replace(" ", "").replace("/", "")
                expected_prefix = f"AUTO_{str(assignment.band).upper()}_{_mode_tag}"
                # Search all kiwi slots — workers may land on any slot index at connect time.
                # Use _user_label_matches to handle KiwiSDR username truncation (e.g. 16-char limit).
                visible_slot = next(
                    (slot for slot, name in users_by_rx.items() if self._user_label_matches(expected_prefix, name)),
                    None,
                )
                visible_on_kiwi = visible_slot is not None
                kiwi_user_age_s = user_age_by_rx.get(visible_slot if visible_slot is not None else int(rx))

            def _to_float(value: object) -> float | None:
                try:
                    return float(value)
                except Exception:
                    return None

            last_decoder_output_unix = _to_float(activity.get("last_decoder_output_unix"))
            last_decode_unix = _to_float(activity.get("last_decode_unix"))
            decoder_output_age_s = max(0.0, now - last_decoder_output_unix) if last_decoder_output_unix is not None else None
            decode_age_s = max(0.0, now - last_decode_unix) if last_decode_unix is not None else None
            snr_last_db = _to_float(activity.get("snr_last_db"))
            snr_avg_db = _to_float(activity.get("snr_avg_db"))
            snr_samples = int(activity.get("snr_samples", 0) or 0)
            last_snr_unix = _to_float(activity.get("last_snr_unix"))
            snr_age_s = max(0.0, now - last_snr_unix) if last_snr_unix is not None else None
            decode_total = int(activity.get("decode_total", 0) or 0)
            _decode_ts: list = list(activity.get("decode_timestamps", []) or [])
            decode_rate_per_min = sum(1 for t in _decode_ts if t >= now - 60.0)
            decode_rate_per_hour = len(_decode_ts)
            _decode_ts_by_mode: dict = {k: list(v) for k, v in (activity.get("_decode_ts_by_mode") or {}).items()}
            _decode_total_by_mode: dict = dict(activity.get("_decode_total_by_mode") or {})
            decode_rates_by_mode: dict = {
                m: {
                    "decode_rate_per_min": sum(1 for t in mts if t >= now - 60.0),
                    "decode_rate_per_hour": sum(1 for t in mts if t >= now - 3600.0),
                    "decode_total": int(_decode_total_by_mode.get(m, 0) or 0),
                }
                for m, mts in _decode_ts_by_mode.items()
                if mts
            }

            stall_threshold_s = self._stall_threshold_seconds(mode_label)
            silent_threshold_s = self._silent_threshold_seconds(mode_label)
            is_digital = self._mode_requires_digital(mode_label)

            health_state = "healthy" if is_active else "inactive"
            propagation_state = "unknown"
            if is_digital and assignment is not None:
                recent_enough = (snr_age_s is not None and snr_age_s <= self._propagation_recent_max_s(silent_threshold_s))
                min_samples = self._propagation_min_samples()
                good_db = self._propagation_good_snr_db()
                fair_db = self._propagation_fair_snr_db()
                marginal_db = self._propagation_marginal_snr_db()
                # Ensure ordering remains monotonic if env vars are set inconsistently.
                fair_db = min(good_db, fair_db)
                marginal_db = min(fair_db, marginal_db)
                if snr_samples >= min_samples and recent_enough and snr_avg_db is not None:
                    if snr_avg_db >= good_db:
                        propagation_state = "good"
                    elif snr_avg_db >= fair_db:
                        propagation_state = "fair"
                    elif snr_avg_db >= marginal_db:
                        propagation_state = "marginal"
                    else:
                        propagation_state = "poor"

            if is_digital and assignment is not None:
                decoder_missing = (decoder_output_age_s is None) or (decoder_output_age_s > stall_threshold_s)
                expected_label = self._expected_user_label(assignment).upper()
                # Also search for truncated key variants (KiwiSDR may truncate long usernames)
                _live_locs_raw = live_auto_locations.get(expected_label)
                if _live_locs_raw is None:
                    for _key, _locs in live_auto_locations.items():
                        if self._user_label_matches(expected_label, _key):
                            _live_locs_raw = _locs
                            break
                live_locations = list(_live_locs_raw or [])
                wrong_slot_stale = False
                kiwi_actual_rx: int | None = None
                kiwi_occupant: str | None = None
                if live_locations:
                    live_rxs = {int(loc_rx) for loc_rx, _ in live_locations}
                    if int(rx) not in live_rxs:
                        ages = [age for _, age in live_locations]
                        wrong_slot_stale = all(age is None or age >= remap_grace_s for age in ages)
                        # Record where the worker actually landed
                        kiwi_actual_rx = int(live_locations[0][0])
                occupant = str(users_by_rx.get(int(rx), "") or "").strip()
                occupant_age_s = user_age_by_rx.get(int(rx))
                displaced_by_stale_auto = bool(
                    occupant.startswith("AUTO_")
                    and not self._user_label_matches(expected_label, occupant)
                    and (occupant_age_s is None or occupant_age_s >= remap_grace_s)
                )
                # Record what is blocking the expected slot (if anything foreign is there)
                if occupant and not self._user_label_matches(expected_label, occupant):
                    kiwi_occupant = occupant
                mismatch_detected = bool(wrong_slot_stale or displaced_by_stale_auto)
                # Only act on slot mismatch if the decoder is also not producing output.
                # When a worker adapts to a different KiwiSDR slot but is running fine,
                # decoder_missing=False means data is flowing — no stall action needed.
                mismatch_actionable = bool(mismatch_detected and decoder_missing)
                if mismatch_actionable:
                    health_state = "stalled"
                    last_reason = "kiwi_assignment_mismatch"
                elif mismatch_detected and not last_reason:
                    last_reason = "kiwi_assignment_mismatch_observed"
                elif visible_on_kiwi and decoder_missing:
                    health_state = "stalled"
                    last_reason = "stalled_no_decoder_output"
                elif visible_on_kiwi and decode_age_s is not None and decode_age_s > silent_threshold_s:
                    health_state = "silent"
                    if not last_reason:
                        last_reason = "silent_no_decodes"

            no_decode_warn = False
            if is_digital and assignment is not None and health_state not in {"stalled", "silent"}:
                warn_after_s = self._no_decode_warning_seconds()
                heartbeat_ok = (decoder_output_age_s is not None) and (decoder_output_age_s <= stall_threshold_s)
                no_decode_warn = bool(visible_on_kiwi and heartbeat_ok and decode_age_s is not None and decode_age_s >= warn_after_s)

            is_unstable = (assignment is not None) and (
                consecutive >= 3 or backoff_s >= 8.0 or (is_ssb and not is_active) or health_state == "stalled"
            )
            if is_unstable:
                unstable += 1
                if health_state == "stalled":
                    stalled += 1
                reason = str(last_reason or "unknown")
                reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1
            elif last_reason == "kiwi_assignment_mismatch_observed":
                # Ghost connection: slot is occupied by a stale foreign AUTO_ process.
                # Not yet stalled (decoder may still be alive) but counts toward auto-kick.
                reason_counts["kiwi_assignment_mismatch_observed"] = int(reason_counts.get("kiwi_assignment_mismatch_observed", 0)) + 1
            elif health_state == "silent":
                silent += 1
                reason_counts["silent_no_decodes"] = int(reason_counts.get("silent_no_decodes", 0)) + 1
            elif no_decode_warn:
                no_decode_warning += 1
                reason_counts["no_recent_decodes"] = int(reason_counts.get("no_recent_decodes", 0)) + 1

            if health_state == "stalled":
                status_level = "fault"
            elif health_state == "silent" or no_decode_warn:
                status_level = "warning"
            else:
                status_level = "healthy"

            latest_health_unix = updated_unix_f
            for candidate in (last_decoder_output_unix, last_decode_unix):
                if candidate is None:
                    continue
                latest_health_unix = candidate if latest_health_unix is None else max(latest_health_unix, candidate)

            channels[str(rx)] = {
                "rx": int(rx),
                "kiwi_rx": visible_slot if visible_slot is not None else int(rx),
                "band": assignment.band if assignment else wd.get("band"),
                "mode": assignment.mode_label if assignment else None,
                "active": bool(is_active),
                "visible_on_kiwi": bool(visible_on_kiwi),
                "kiwi_user_age_s": kiwi_user_age_s,
                "kiwi_actual_rx": kiwi_actual_rx if is_digital and assignment is not None else None,
                "kiwi_occupant": kiwi_occupant if is_digital and assignment is not None else None,
                "restart_count": int(restart_by_rx.get(rx, 0)),
                "consecutive_failures": consecutive,
                "backoff_s": backoff_s,
                "cooling_down": bool(cooling_down),
                "cooldown_remaining_s": float(cooldown_remaining_s),
                "last_reason": last_reason,
                "last_updated_unix": latest_health_unix,
                "last_decoder_output_unix": last_decoder_output_unix,
                "last_decode_unix": last_decode_unix,
                "decoder_output_age_s": decoder_output_age_s,
                "decode_age_s": decode_age_s,
                "snr_last_db": snr_last_db,
                "snr_avg_db": snr_avg_db,
                "snr_samples": snr_samples,
                "snr_age_s": snr_age_s,
                "decode_total": decode_total,
                "decode_rate_per_min": decode_rate_per_min,
                "decode_rate_per_hour": decode_rate_per_hour,
                "decode_rates_by_mode": decode_rates_by_mode,
                "propagation_state": propagation_state,
                "health_state": health_state,
                "status_level": status_level,
                "is_no_decode_warning": bool(no_decode_warn),
                "is_silent": health_state == "silent",
                "is_stalled": health_state == "stalled",
                "is_unstable": is_unstable,
            }

            if is_digital and assignment is not None:
                propagation_counts[propagation_state] = int(propagation_counts.get(propagation_state, 0)) + 1
                score_map = {"good": 3.0, "fair": 2.0, "marginal": 1.0, "poor": 0.0}
                if propagation_state in score_map:
                    propagation_score_total += float(score_map[propagation_state])
                    propagation_score_count += 1

        # If one mode on a band has valid SNR but another mode has no decodes yet,
        # infer band-level propagation for the unknown channel.
        band_known_scores: Dict[str, list[float]] = {}
        score_map = {"good": 3.0, "fair": 2.0, "marginal": 1.0, "poor": 0.0}
        for channel in channels.values():
            if not isinstance(channel, dict):
                continue
            if str(channel.get("health_state") or "") in {"inactive", "stalled"}:
                continue
            band_key = str(channel.get("band") or "").strip().lower()
            state = str(channel.get("propagation_state") or "")
            if not band_key or state not in score_map:
                continue
            band_known_scores.setdefault(band_key, []).append(float(score_map[state]))

        for channel in channels.values():
            if not isinstance(channel, dict):
                continue
            if str(channel.get("propagation_state") or "") != "unknown":
                continue
            if str(channel.get("health_state") or "") in {"inactive", "stalled"}:
                continue
            band_key = str(channel.get("band") or "").strip().lower()
            band_scores = band_known_scores.get(band_key) or []
            if not band_key or not band_scores:
                continue
            inferred_score = sum(band_scores) / float(len(band_scores))
            if inferred_score >= 2.5:
                inferred_state = "good"
            elif inferred_score >= 1.5:
                inferred_state = "fair"
            elif inferred_score >= 0.5:
                inferred_state = "marginal"
            else:
                inferred_state = "poor"
            channel["propagation_state"] = inferred_state
            channel["propagation_inferred"] = True

        # Recompute propagation summary after per-band inference.
        propagation_counts = {"good": 0, "fair": 0, "marginal": 0, "poor": 0, "unknown": 0}
        propagation_score_total = 0.0
        propagation_score_count = 0
        for channel in channels.values():
            if not isinstance(channel, dict):
                continue
            mode_norm = str(channel.get("mode") or "").strip().upper()
            if not self._mode_requires_digital(mode_norm):
                continue
            if not bool(channel.get("active")):
                continue
            state = str(channel.get("propagation_state") or "unknown")
            propagation_counts[state] = int(propagation_counts.get(state, 0)) + 1
            if state in score_map:
                propagation_score_total += float(score_map[state])
                propagation_score_count += 1

        active = sum(1 for ch in channels.values() if bool(ch.get("active")))
        overall = "healthy"
        if unstable > 0:
            overall = "degraded"
        elif silent > 0 or no_decode_warning > 0:
            overall = "quiet"
        if active == 0:
            overall = "idle"

        latest_update = None
        for channel in channels.values():
            if not bool(channel.get("active")):
                continue
            try:
                ts = float(channel.get("last_updated_unix"))
            except Exception:
                continue
            latest_update = ts if latest_update is None else max(latest_update, ts)
        stale_seconds = None
        if active > 0:
            if unstable <= 0 and silent <= 0:
                stale_seconds = 0.0
            elif latest_update is None:
                stale_seconds = 0.0
            else:
                stale_seconds = max(0.0, now - latest_update)

        if propagation_score_count <= 0:
            propagation_overall = "unknown"
            propagation_score_avg = None
        else:
            propagation_score_avg = propagation_score_total / float(propagation_score_count)
            if propagation_score_avg >= 2.5:
                propagation_overall = "good"
            elif propagation_score_avg >= 1.5:
                propagation_overall = "fair"
            elif propagation_score_avg >= 0.5:
                propagation_overall = "marginal"
            else:
                propagation_overall = "poor"

        return {
            "overall": overall,
            "active_receivers": active,
            "unstable_receivers": unstable,
            "stalled_receivers": stalled,
            "silent_receivers": silent,
            "no_decode_warning_receivers": no_decode_warning,
            "restart_total": restart_total,
            "health_stale_seconds": stale_seconds,
            "reason_counts": reason_counts,
            "channels": channels,
            "propagation": {
                "overall": propagation_overall,
                "counts": propagation_counts,
                "score_avg": propagation_score_avg,
                "sampled_channels": int(propagation_score_count),
            },
            "auto_kick": {
                "total": int(self._auto_kick_total),
                "last_unix": float(self._auto_kick_last_unix) if self._auto_kick_last_unix is not None else None,
                "last_reason": str(self._auto_kick_last_reason or ""),
                "last_result": str(self._auto_kick_last_result or ""),
                "mismatch_streak": int(self._mismatch_global_streak),
            },
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

    def _wait_for_kiwi_auto_users_missing(
        self,
        *,
        host: str,
        port: int,
        labels: set[str],
        timeout_s: float = 10.0,
    ) -> None:
        expected_labels = {str(label or "").strip() for label in labels if str(label or "").strip()}
        if not expected_labels:
            return
        deadline = time.time() + max(1.0, float(timeout_s))
        while time.time() < deadline:
            live_users = self._fetch_live_auto_users(host, port)
            if not live_users:
                return
            live_labels = [str(label or "").strip() for label in live_users.values()]
            still_present = False
            for expected in expected_labels:
                if any(self._user_label_matches(expected, live) for live in live_labels):
                    still_present = True
                    break
            if not still_present:
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

    @staticmethod
    def _force_full_reset_on_band_change_enabled() -> bool:
        raw = str(os.environ.get("KIWISCAN_RESET_ALL_ON_BAND_CHANGE", "1")).strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def _force_full_reset_on_reconcile_enabled() -> bool:
        raw = str(os.environ.get("KIWISCAN_RESET_ALL_ON_RECONCILE", "1")).strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def _band_plan_changed(current: Dict[int, ReceiverAssignment], desired: Dict[int, ReceiverAssignment]) -> bool:
        if not current or not desired:
            return False
        if set(current.keys()) != set(desired.keys()):
            return True
        for rx in current.keys():
            cur_band = str(current[rx].band or "").strip().lower()
            new_band = str(desired[rx].band or "").strip().lower()
            if cur_band != new_band:
                return True
        return False

    @staticmethod
    def _user_label_matches(expected: str, actual: str) -> bool:
        def _canon(value: str) -> str:
            # Kiwi user names can vary in casing (e.g. AUTO_20m_FT8);
            # compare canonicalized text to avoid false mismatches.
            return str(value or "").strip().upper()

        expected_text = _canon(expected)
        actual_text = _canon(urllib.parse.unquote(str(actual or "").strip()))
        if not expected_text or not actual_text:
            return False
        if actual_text == expected_text:
            return True
        if actual_text.startswith(expected_text) or expected_text.startswith(actual_text):
            return True
        return len(actual_text) >= max(8, len(expected_text) - 3) and expected_text.startswith(actual_text)

    @classmethod
    def _expected_user_label(cls, assignment: ReceiverAssignment) -> str:
        if bool(assignment.ssb_scan) and cls._is_ssb_assignment(assignment):
            return f"AUTO_{str(assignment.band).upper()}_SSBSCAN"
        mode_tag = str(assignment.mode_label or "FT8").strip().upper().replace(" ", "").replace("/", "")
        return f"AUTO_{str(assignment.band).upper()}_{mode_tag}"

    @classmethod
    def _fetch_live_auto_users(cls, host: str, port: int) -> Dict[int, str]:
        out: Dict[int, str] = {}
        status_url = f"http://{host}:{int(port)}/users?json=1"
        try:
            with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if not isinstance(payload, list):
                return {}
            for row in payload:
                if not isinstance(row, dict):
                    continue
                try:
                    rx_i = int(row.get("i"))
                except Exception:
                    continue
                name = urllib.parse.unquote(str(row.get("n") or "")).strip()
                if name.startswith("AUTO_"):
                    out[int(rx_i)] = name
        except Exception:
            return {}
        return out

    @classmethod
    def _has_duplicate_live_auto_labels(cls, host: str, port: int) -> bool:
        live_users = cls._fetch_live_auto_users(host, port)
        if not live_users:
            return False
        counts: Dict[str, int] = {}
        for label in live_users.values():
            canon = str(label or "").strip().upper()
            if not canon:
                continue
            counts[canon] = int(counts.get(canon, 0)) + 1
        return any(v > 1 for v in counts.values())

    def _assignment_slots_needing_reconcile(
        self,
        *,
        host: str,
        port: int,
        assignments: Dict[int, ReceiverAssignment],
    ) -> set[int]:
        live_users = self._fetch_live_auto_users(host, port)
        if not live_users:
            return set()

        expected_by_rx = {
            int(rx): self._expected_user_label(assignment)
            for rx, assignment in assignments.items()
        }
        out: set[int] = set()
        for rx, expected_label in expected_by_rx.items():
            assignment = assignments.get(int(rx))
            ignore_slot = bool(getattr(assignment, "ignore_slot_check", False))
            if ignore_slot:
                # For fixed receivers (ignore_slot_check=True), the Kiwi slot won't
                # match the app rx number — only check if the worker is alive and correct.
                worker = self._workers.get(int(rx))
                if worker is None:
                    out.add(int(rx))
                    continue
                active_label = str(getattr(worker, "_active_user_label", "") or "").strip()
                if active_label and not self._user_label_matches(expected_label, active_label):
                    out.add(int(rx))
                continue
            # For roaming receivers, check if the expected label appears on ANY active Kiwi
            # slot (not just the "expected" slot number). This avoids false drift triggers
            # when a human listener occupies the expected slot.
            label_found = any(
                self._user_label_matches(expected_label, lbl)
                for lbl in live_users.values()
            )
            if not label_found:
                out.add(int(rx))
                continue
            worker = self._workers.get(int(rx))
            if worker is None:
                out.add(int(rx))
                continue
            active_label = str(getattr(worker, "_active_user_label", "") or "").strip()
            if active_label and not self._user_label_matches(expected_label, active_label):
                out.add(int(rx))
        return out

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
            prior_host = str(getattr(self, "_active_host", "") or "")
            prior_port = int(getattr(self, "_active_port", 0) or 0)
            assignments = self._normalize_ssb_receivers(assignments)
            equivalent_assignments = self._assignment_maps_equivalent(self._assignments, assignments)
            reconcile_rxs: set[int] = set()
            force_reconcile_full_reset = False
            if equivalent_assignments:
                reconcile_rxs = self._assignment_slots_needing_reconcile(host=host, port=port, assignments=assignments)
                if not reconcile_rxs:
                    return
                logger.warning(
                    "Receiver assignment drift detected; restarting workers for RXs %s",
                    ", ".join(str(rx) for rx in sorted(reconcile_rxs)),
                )
                force_reconcile_full_reset = self._force_full_reset_on_reconcile_enabled()
                if force_reconcile_full_reset:
                    logger.warning("Drift reconcile policy forcing full Kiwi receiver reset before re-apply")

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
            force_full_reset = (
                self._force_full_reset_on_band_change_enabled()
                and self._band_plan_changed(self._assignments, assignments)
            )
            did_full_reset = bool(host_changed or force_full_reset or force_reconcile_full_reset)
            if force_full_reset:
                logger.warning("Band-plan change detected; forcing full Kiwi receiver reset before re-apply")

            to_stop: set[int] = set()
            to_reconfigure: set[int] = set()
            if did_full_reset:
                for rx in current_rxs:
                    assignment = assignments.get(int(rx))
                    # Fixed receivers (ignore_slot_check) keep running through a full reset
                    # if their assignment is unchanged and this isn't a host-change reset.
                    if (
                        not host_changed
                        and assignment is not None
                        and bool(getattr(assignment, "ignore_slot_check", False))
                        and int(rx) in self._assignments
                        and self._assignment_equivalent(self._assignments[int(rx)], assignment)
                        and self._workers.get(int(rx)) is not None
                    ):
                        logger.debug("Full reset: preserving fixed receiver RX%s (%s %s)", rx, assignment.band, assignment.mode_label)
                        continue
                    to_stop.add(int(rx))
                to_reconfigure.clear()
            else:
                to_stop |= set(int(rx) for rx in reconcile_rxs)
                for rx in sorted(current_rxs):
                    if rx not in assignments:
                        to_stop.add(rx)
                        continue
                    if not self._assignment_equivalent(self._assignments[rx], assignments[rx]):
                        if self._can_hot_reconfigure_ssb(self._assignments[rx], assignments[rx]):
                            to_reconfigure.add(rx)
                        else:
                            to_stop.add(rx)

            # Capture offsets from workers about to be stopped so replacements inherit them.
            adjust_cache: Dict[int, int] = {}
            for rx in set(to_stop) | desired_rxs:
                w = self._workers.get(rx)
                if w is not None:
                    adjust_cache[int(rx)] = int(w._rx_chan_adjust)

            stopped_labels: set[str] = set()
            for rx in sorted(to_stop):
                current_assignment = self._assignments.get(rx)
                if current_assignment is not None:
                    stopped_labels.add(self._expected_user_label(current_assignment))

            for rx in sorted(to_stop):
                worker = self._workers.pop(rx, None)
                if worker is not None:
                    self._stop_worker(worker)
                self._activity_by_rx.pop(int(rx), None)

            for rx in sorted(to_reconfigure):
                worker = self._workers.get(rx)
                desired = assignments.get(rx)
                if worker is None or desired is None:
                    continue
                self._activity_by_rx.pop(int(rx), None)
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

            if stopped_labels:
                wait_host = prior_host if prior_host else str(host)
                wait_port = prior_port if prior_port > 0 else int(port)
                self._wait_for_kiwi_auto_users_missing(
                    host=wait_host,
                    port=wait_port,
                    labels=stopped_labels,
                    timeout_s=8.0,
                )

            if did_full_reset:
                self._cleanup_orphan_processes()
                self._wait_for_orphan_cleanup(timeout_s=6.0)
                # Only kick ALL Kiwi channels when the host changed or every fixed receiver
                # is also being stopped. When fixed receivers are preserved (band-plan only
                # changes for roaming slots) a kick-all would disconnect their Kiwi sessions.
                any_fixed_preserved = any(
                    assignments.get(int(rx)) is not None
                    and bool(getattr(assignments[int(rx)], "ignore_slot_check", False))
                    and int(rx) not in to_stop
                    for rx in current_rxs | desired_rxs
                )
                if host_changed or not any_fixed_preserved:
                    # Use force_all=True on initial host connection so ALL Kiwi channels are
                    # freed (including sessions from a previous container run that are still
                    # in a TCP-closing state and wouldn't appear as AUTO users).
                    _ = self._run_admin_kick_all(host=str(host), port=int(port), force_all=bool(host_changed))
                    self._wait_for_kiwi_auto_users_clear(host=str(host), port=int(port), timeout_s=8.0)
                    # If stale AUTO_ users still remain after the first wait (e.g. a previous
                    # container's connection that hadn't fully closed), kick once more with a
                    # longer wait to ensure those ghost slots are freed before workers start.
                    if self._fetch_live_auto_users(str(host), int(port)):
                        logger.info("Stale AUTO_ users still present after kick; sending second kick")
                        _ = self._run_admin_kick_all(host=str(host), port=int(port), force_all=True)
                        self._wait_for_kiwi_auto_users_clear(host=str(host), port=int(port), timeout_s=12.0)

            time.sleep(0.2)

            for rx in sorted(desired_rxs, key=lambda rx: rx if rx >= 2 else rx + 8):
                if rx in to_reconfigure and rx in self._workers:
                    continue
                if rx in self._workers and rx in self._assignments and self._assignment_equivalent(self._assignments[rx], assignments[rx]) and not host_changed:
                    continue
                desired = assignments[rx]
                worker = self._make_worker(host=host, port=port, assignment=desired, rx_chan_adjust=adjust_cache.get(int(rx), 0))
                self._workers[rx] = worker
                worker.start()
                worker._slot_ready.wait(timeout=8.0)

            self._assignments = {int(rx): assignments[int(rx)] for rx in sorted(desired_rxs)}
            self._active_host = str(host)
            self._active_port = int(port)

            if self._has_duplicate_live_auto_labels(str(host), int(port)):
                logger.warning("Detected duplicate AUTO labels on Kiwi; forcing worker recycle and Kiwi kick-all")
                dup_adjust_cache = {rx: int(w._rx_chan_adjust) for rx, w in self._workers.items() if w is not None}
                for worker in list(self._workers.values()):
                    self._stop_worker(worker)
                self._workers.clear()
                self._activity_by_rx.clear()
                self._cleanup_orphan_processes()
                self._wait_for_orphan_cleanup(timeout_s=6.0)
                _ = self._run_admin_kick_all(host=str(host), port=int(port))
                self._wait_for_kiwi_auto_users_clear(host=str(host), port=int(port), timeout_s=8.0)
                for rx in sorted(desired_rxs, key=lambda rx: rx if rx >= 2 else rx + 8):
                    desired = self._assignments.get(int(rx))
                    if desired is None:
                        continue
                    worker = self._make_worker(host=str(host), port=int(port), assignment=desired, rx_chan_adjust=dup_adjust_cache.get(int(rx), 0))
                    self._workers[int(rx)] = worker
                    worker.start()
                    worker._slot_ready.wait(timeout=8.0)

    def stop_all(self) -> None:
        self._manager_stop.set()
        with self._lock:
            for worker in list(self._workers.values()):
                self._stop_worker(worker)
            self._workers.clear()
            self._assignments.clear()
            self._activity_by_rx.clear()
            self._stale_watch_state_by_rx.clear()
        try:
            self._stale_recovery_thread.join(timeout=1.0)
        except Exception:
            pass
