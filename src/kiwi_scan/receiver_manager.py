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


def _read_env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw not in {"0", "false", "no", "off"}


def _read_env_float(name: str, default: float, *, min_v: float, max_v: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return max(float(min_v), min(float(max_v), value))


def _read_env_int(name: str, default: int, *, min_v: int, max_v: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except Exception:
        return int(default)
    return max(int(min_v), min(int(max_v), value))


def _normalize_user_label_band(band: str) -> str:
    raw = str(band or "").strip().replace("_", "")
    match = re.match(r"^(\d+)\s*[mM]?$", raw)
    if match:
        return f"{match.group(1)}m"
    return raw or "--"


def _normalize_user_label_mode(mode_tag: str) -> str:
    raw = str(mode_tag or "").strip().upper()
    normalized = raw.replace(" ", "").replace("-", "/").replace("_", "/")
    if not normalized:
        return ""
    if normalized == "ALL":
        return "ALL"
    if normalized == "MIX":
        return "MIX"
    if "SSB" in normalized or "PHONE" in normalized:
        return "SSB"
    digital_modes: list[str] = []
    if "FT8" in normalized:
        digital_modes.append("FT8")
    if "FT4" in normalized:
        digital_modes.append("FT4")
    if "WSPR" in normalized or normalized == "WS":
        digital_modes.append("WSPR")
    if len(digital_modes) >= 3:
        return "ALL"
    if len(digital_modes) >= 2:
        return "MIX"
    if len(digital_modes) == 1:
        return digital_modes[0]
    return re.sub(r"\W+", "_", raw).strip("_")


def _compact_user_label(prefix: str, band: str, mode_tag: str) -> str:
    prefix_text = str(prefix or "").strip().upper().replace(" ", "")
    band_text = _normalize_user_label_band(band)
    mode_text = _normalize_user_label_mode(mode_tag)
    return "_".join(part for part in (prefix_text, band_text, mode_text) if part)


def _preferred_user_label_prefix(rx: int) -> str:
    return "FIXED" if int(rx) >= 2 else "ROAM"


def _compatible_user_label_prefixes(rx: int) -> tuple[str, ...]:
    rx_i = int(rx)
    if rx_i >= 2:
        return ("FIXED",)
    return ("ROAM", f"ROAM{rx_i + 1}")


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
        env_adjust = self._env_int("KIWISCAN_RX_CHAN_OFFSET", 0, min_v=-64, max_v=64)
        self._rx_chan_adjust = int(initial_rx_chan_adjust) if initial_rx_chan_adjust else env_adjust

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        return _read_env_bool(name, default)

    @staticmethod
    def _env_float(name: str, default: float, *, min_v: float, max_v: float) -> float:
        return _read_env_float(name, default, min_v=min_v, max_v=max_v)

    @staticmethod
    def _env_int(name: str, default: int, *, min_v: int, max_v: int) -> int:
        return _read_env_int(name, default, min_v=min_v, max_v=max_v)

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
        return self._env_bool("KIWISCAN_STRICT_DIGITAL_SLOT_ENFORCEMENT", True)

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
        return self._env_bool("KIWISCAN_USE_PY_UDP_AUDIO", False)

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

    def stop(
        self,
        join_timeout_s: float = 3.0,
        *,
        graceful: bool = False,
        graceful_timeout_s: float = 5.0,
    ) -> None:
        self._stop_event.set()
        self._terminate_proc(graceful=graceful, graceful_timeout_s=graceful_timeout_s)
        if threading.current_thread() is self:
            return
        try:
            self.join(timeout=max(0.0, float(join_timeout_s)))
        except Exception:
            pass

    def _terminate_proc(self, *, graceful: bool = False, graceful_timeout_s: float = 5.0) -> None:
        for proc in list(self._decoder_procs):
            try:
                if graceful:
                    proc.terminate()
                    proc.wait(timeout=max(0.1, min(2.0, float(graceful_timeout_s))))
                else:
                    proc.kill()
                    proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=2.0)
                except Exception:
                    pass
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
        _pid = proc.pid
        _pgid: Optional[int] = None
        try:
            _pgid = os.getpgid(_pid)
        except Exception:
            pass
        if graceful:
            try:
                try:
                    os.killpg(_pid, signal.SIGTERM)
                except Exception:
                    pass
                if _pgid is not None and _pgid != _pid:
                    try:
                        os.killpg(_pgid, signal.SIGTERM)
                    except Exception:
                        pass
                try:
                    proc.terminate()
                except Exception:
                    pass
                proc.wait(timeout=max(0.1, float(graceful_timeout_s)))
                self._proc = None
                return
            except subprocess.TimeoutExpired:
                pass
            except Exception:
                pass
        try:
            # Use SIGKILL directly so the OS sends an immediate TCP RST.
            try:
                os.killpg(_pid, signal.SIGKILL)
            except Exception:
                pass
            # Fallback: also kill by PGID explicitly (in case PGID ≠ PID)
            if _pgid is not None and _pgid != _pid:
                try:
                    os.killpg(_pgid, signal.SIGKILL)
                except Exception:
                    pass
            # Final fallback: kill just the process
            try:
                proc.kill()
            except Exception:
                pass
            # Also try killing all children of this PID directly
            try:
                _children = subprocess.run(
                    ["pgrep", "-P", str(_pid)],
                    capture_output=True, text=True, timeout=1,
                )
                for _cpid in _children.stdout.strip().split():
                    try:
                        os.kill(int(_cpid), signal.SIGKILL)
                    except Exception:
                        pass
            except Exception:
                pass
            logger.debug(
                "_terminate_proc rx=%s pid=%s pgid=%s stop_event=%s",
                self._rx, _pid, _pgid, self._stop_event.is_set(),
            )
            proc.wait(timeout=3.0)
        except Exception:
            pass
        self._proc = None

    @staticmethod
    def _terminate_external_proc(proc: Optional[subprocess.Popen]) -> None:
        if proc is None:
            return
        try:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                proc.kill()
            proc.wait(timeout=2.0)
        except Exception:
            pass

    @property
    def _user_prefix(self) -> str:
        return _preferred_user_label_prefix(int(self._rx))

    @staticmethod
    def _kill_local_kiwi_user_processes(user_label: str) -> None:
        label = str(user_label or "").strip()
        if not label:
            return
        try:
            subprocess.run(
                ["pkill", "-9", "-f", f"kiwirecorder.py.*{re.escape(label)}"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def _kiwi_rx_chan(self) -> int:
        mode_norm = self._mode_label.strip().upper()
        if ("SSB" in mode_norm) or ("PHONE" in mode_norm):
            return int(self._rx)
        return int((int(self._rx) + int(self._rx_chan_adjust)) % 8)

    def _wait_for_kiwi_user_connected(self, user_label: str, timeout_s: float = 8.0) -> None:
        """Poll /users until user label appears with a fresh connection, confirming Kiwi connection
        is established by the newly spawned process (not a stale entry from a previous session)."""
        deadline = time.time() + max(1.0, float(timeout_s))
        # Only count connections that appeared *after* this call started.  We allow
        # up to (timeout_s + 2) seconds of age to cover slow Kiwi handshakes while
        # still rejecting connections from the previous incarnation of the same worker
        # (which can linger in /users for several seconds after the process exits).
        max_fresh_age_s = float(timeout_s) + 2.0
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
                        if not (name == wanted or name.startswith(wanted) or wanted.startswith(name)):
                            continue
                        # Parse connected_seconds from the "t" field ("[hh:]mm:ss" format).
                        age_s: float | None = None
                        try:
                            parts = [int(p) for p in str(row.get("t") or "").split(":") if str(p).strip()]
                            if len(parts) == 3:
                                age_s = float(parts[0] * 3600 + parts[1] * 60 + parts[2])
                            elif len(parts) == 2:
                                age_s = float(parts[0] * 60 + parts[1])
                            elif len(parts) == 1:
                                age_s = float(parts[0])
                        except Exception:
                            pass
                        # Accept if age is unknown (field missing) or clearly fresh.
                        if age_s is None or age_s <= max_fresh_age_s:
                            return
                        # Otherwise this is a stale entry from a previous session; keep waiting.
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
        return _ReceiverWorker._env_bool("KIWISCAN_FT8MODEM_KEEP", False)

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
                _compact_user_label(self._user_prefix, self._band, "SSB"),
                "--scan-yaml",
                str(yaml_path),
                "--squelch-tail",
                str(tail_s),
                "--log_level=info",
            ]
            self._active_user_label = _compact_user_label(self._user_prefix, self._band, "SSB")
            self._kill_local_kiwi_user_processes(self._active_user_label)

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
                user_label=self._active_user_label,
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
                        user_label=f"{self._user_prefix}_{self._band}_SSBSCAN",
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
        user_label = _compact_user_label(self._user_prefix, self._band, self._mode_label)
        self._active_user_label = user_label
        self._kill_local_kiwi_user_processes(user_label)
        logger.info(
            "_spawn START rx=%s label=%s stop_event=%s",
            self._rx, user_label, self._stop_event.is_set(),
        )
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
                        f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                            f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                            f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                        f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                        f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                        f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                        f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                    f"{self._python_cmd} {self._kiwirecorder_path} --busy-timeout 5 "
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
                if not self._ignore_slot_check and self._is_digital_mode():
                    # RX0/RX1 roaming workers must be strict about slot placement.
                    strict_digital = bool(int(self._rx) < 2) or self._strict_digital_slot_enforcement()
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
                self._stop_event.wait(timeout=backoff_s)  # interruptible sleep
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
                        strict_roaming_digital = bool(int(self._rx) < 2 and self._is_digital_mode())
                        strict = bool(is_ssb) or strict_roaming_digital or bool(self._strict_digital_slot_enforcement())
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
                self._stop_event.wait(timeout=backoff_s)  # interruptible sleep


class ReceiverManager:
    @staticmethod
    def _is_auto_label(label: str) -> bool:
        v = str(label or "").strip().upper()
        return v.startswith("AUTO") or v.startswith("FIXED") or v.startswith("ROAM")

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
        self._startup_eviction_active = threading.Event()  # suppresses monitoring autokick during startup eviction
        self._metrics_snapshot_cache: Dict[str, object] = {}  # last known-good metrics result for lock-timeout fallback
        self._health_summary_cache: Dict[str, object] = {}  # last known-good result for lock-timeout fallback
        self._truth_snapshot_cache: Dict[str, object] = {}  # last known-good truth snapshot for lock-timeout fallback
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
        return _read_env_bool(name, default)

    @staticmethod
    def _env_float(name: str, default: float, *, min_v: float, max_v: float) -> float:
        return _read_env_float(name, default, min_v=min_v, max_v=max_v)

    @staticmethod
    def _env_int(name: str, default: int, *, min_v: int, max_v: int) -> int:
        return _read_env_int(name, default, min_v=min_v, max_v=max_v)

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

    def _run_admin_kick_all(
        self,
        *,
        host: str,
        port: int,
        force_all: bool = False,
        kick_only_slots: Optional[list[int]] = None,
    ) -> bool:
        script = self._find_admin_kick_script()
        if script is None:
            return False

        if kick_only_slots is not None:
            kick_targets = sorted(int(s) for s in kick_only_slots)
        else:
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

    def _make_worker(self, *, host: str, port: int, assignment: ReceiverAssignment, rx_chan_adjust: int = 0, ignore_slot_check: Optional[bool] = None) -> _ReceiverWorker:
        _isc = bool(getattr(assignment, "ignore_slot_check", False))
        if ignore_slot_check is not None:
            _isc = bool(ignore_slot_check)
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
            ignore_slot_check=_isc,
        )

    @staticmethod
    def _stop_worker(
        worker: Optional[_ReceiverWorker],
        *,
        join_timeout_s: float = 3.0,
        graceful: bool = False,
        graceful_timeout_s: float = 5.0,
    ) -> None:
        if worker is None:
            return
        try:
            worker.stop(
                join_timeout_s=join_timeout_s,
                graceful=graceful,
                graceful_timeout_s=graceful_timeout_s,
            )
        except TypeError:
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

        expected_labels = self._expected_user_label_aliases(assignment)
        stale_labels: set[str] = set(expected_labels)
        active_label = str(getattr(old_worker, "_active_user_label", "") or "").strip() if old_worker is not None else ""
        if active_label:
            stale_labels.add(active_label)

        old_adjust = int(old_worker._rx_chan_adjust) if old_worker is not None else 0
        if old_worker is not None:
            self._stop_worker(old_worker)

        if stale_labels:
            self._wait_for_kiwi_auto_users_missing(
                host=host,
                port=port,
                labels=stale_labels,
                timeout_s=4.0,
            )
            live_labels = self._fetch_live_auto_users(host, port)
            if any(
                self._user_label_matches(expected, live)
                for expected in stale_labels
                for live in live_labels.values()
            ):
                self._cleanup_orphan_processes_for_labels(stale_labels)
                self._wait_for_kiwi_auto_users_missing(
                    host=host,
                    port=port,
                    labels=stale_labels,
                    timeout_s=4.0,
                )

        if "kiwi_assignment_mismatch" in str(reason or ""):
            mismatch_slots = {int(rx)}
            live_users = self._fetch_live_users(host, port)
            for live_slot, live_label in live_users.items():
                if self._label_matches_any(expected_labels, live_label):
                    mismatch_slots.add(int(live_slot))
            for _kick_attempt in range(2):
                try:
                    self._run_admin_kick_all(
                        host=host,
                        port=port,
                        kick_only_slots=sorted(mismatch_slots),
                    )
                except Exception:
                    pass
                if self._wait_for_kiwi_slots_clear(
                    host=host,
                    port=port,
                    slots=mismatch_slots,
                    stable_secs=2.0,
                    timeout_s=6.0,
                ):
                    break
            else:
                logger.warning(
                    "Mismatch restart rx=%d: slot(s) %s still occupied before respawn",
                    int(rx),
                    sorted(mismatch_slots),
                )

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
                if self._startup_eviction_active.is_set():
                    # Eviction loop is running; don't fight it by recycling workers mid-correction.
                    break
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
                and not self._startup_eviction_active.is_set()
            )
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)

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
                        # Stop all workers and CLEAR assignments instead of restarting them
                        # individually.  Individual restarts after a kick-all ignore the
                        # Kiwi's P-pointer and reproduce the same slot rotation.  Clearing
                        # assignments here ensures the next apply_assignments call detects
                        # starting_from_empty=True and runs the P-probe eviction loop.
                        with self._lock:
                            workers_to_stop = list(self._workers.values())
                            self._workers.clear()
                            self._activity_by_rx.clear()
                            self._assignments.clear()
                        for w in workers_to_stop:
                            self._stop_worker(w)
                        self._cleanup_orphan_processes()
                        logger.warning(
                            "Mismatch auto-kick: stopped all workers and cleared assignments "
                            "(%s/%s stalled); next apply cycle will P-probe correct slots",
                            mismatch_stalled,
                            active_receivers,
                        )
                    else:
                        logger.warning(
                            "Mismatch auto-kick: command failed (%s/%s)",
                            mismatch_stalled,
                            active_receivers,
                        )

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

    @classmethod
    def _no_decode_warning_seconds(cls) -> float:
        return cls._env_float("KIWISCAN_NO_DECODE_WARN_S", 120.0, min_v=30.0, max_v=3600.0)

    @classmethod
    def _digital_remap_grace_seconds(cls) -> float:
        return cls._env_float("KIWISCAN_DIGITAL_REMAP_GRACE_S", 20.0, min_v=5.0, max_v=300.0)

    @staticmethod
    def _cache_with_meta(payload: Dict[str, object]) -> Dict[str, object] | None:
        cached = dict(payload) if payload else None
        if not cached:
            return None
        cached_unix_raw = cached.get("_cached_unix")
        try:
            cached_unix = float(cached_unix_raw) if isinstance(cached_unix_raw, (int, float, str)) else None
        except Exception:
            cached_unix = None
        if cached_unix is not None:
            cached["_cache_age_s"] = max(0.0, time.time() - cached_unix)
        cached["_from_cache"] = True
        return cached

    @staticmethod
    def _prepare_cached_payload(payload: Dict[str, object]) -> Dict[str, object]:
        cached = dict(payload)
        cached["_cached_unix"] = time.time()
        return cached

    def _metrics_cache_with_meta(self) -> Dict[str, object] | None:
        return self._cache_with_meta(self._metrics_snapshot_cache)

    def _store_metrics_snapshot_cache(self, payload: Dict[str, object]) -> None:
        self._metrics_snapshot_cache = self._prepare_cached_payload(payload)

    def metrics_snapshot(self) -> Dict[str, object]:
        acquired = self._lock.acquire(timeout=0.5)
        if not acquired:
            cached = self._metrics_cache_with_meta()
            if cached:
                return cached
            return {
                "restart_total": 0,
                "restart_by_rx": {},
                "restart_last_unix": None,
                "active_workers": 0,
                "assigned_receivers": 0,
                "watchdog_state_by_rx": {},
                "activity_by_rx": {},
                "mismatch_global_streak": 0,
                "auto_kick_total": 0,
                "auto_kick_last_unix": None,
                "auto_kick_last_reason": "",
                "auto_kick_last_result": "",
                "_from_cache": True,
            }
        try:
            result = {
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
        finally:
            self._lock.release()
        self._store_metrics_snapshot_cache(result)
        result["_from_cache"] = False
        return result

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
            self._metrics_snapshot_cache = {}
        return self.metrics_snapshot()

    def active_label_to_rx(self) -> Dict[str, int]:
        """Return a mapping of active worker user labels → internal rx slot numbers.

        Used by the system-info API to display the KiwiScan internal receiver number
        rather than the raw Kiwi channel index (which may differ when the Kiwi does not
        honour our ``--rx-chan`` request and assigns a different channel slot).
        Uses a short acquire timeout so HTTP handlers never block indefinitely.
        """
        acquired = self._lock.acquire(timeout=1.0)
        if not acquired:
            return {}
        try:
            result: Dict[str, int] = {}
            for rx, worker in self._workers.items():
                if worker is None:
                    continue
                label = str(getattr(worker, "_active_user_label", "") or "").strip()
                if label:
                    result[label] = int(rx)
            return result
        finally:
            self._lock.release()

    def _truth_cache_with_meta(self) -> Dict[str, object] | None:
        return self._cache_with_meta(self._truth_snapshot_cache)

    def _store_truth_snapshot_cache(self, payload: Dict[str, object]) -> None:
        self._truth_snapshot_cache = self._prepare_cached_payload(payload)

    def _health_cache_with_meta(self) -> Dict[str, object] | None:
        return self._cache_with_meta(self._health_summary_cache)

    def _store_health_summary_cache(self, payload: Dict[str, object]) -> None:
        self._health_summary_cache = self._prepare_cached_payload(payload)

    @staticmethod
    def _empty_propagation_summary() -> Dict[str, object]:
        return {
            "overall": "unknown",
            "counts": {"good": 0, "fair": 0, "marginal": 0, "poor": 0, "unknown": 0},
            "score_avg": None,
            "sampled_channels": 0,
        }

    @staticmethod
    def _empty_auto_kick_summary() -> Dict[str, object]:
        return {
            "total": 0,
            "last_unix": None,
            "last_reason": "",
            "last_result": "",
            "mismatch_streak": 0,
        }

    def _auto_kick_summary(self) -> Dict[str, object]:
        return {
            "total": int(self._auto_kick_total),
            "last_unix": float(self._auto_kick_last_unix) if self._auto_kick_last_unix is not None else None,
            "last_reason": str(self._auto_kick_last_reason or ""),
            "last_result": str(self._auto_kick_last_result or ""),
            "mismatch_streak": int(self._mismatch_global_streak),
        }

    def _seed_health_summary_cache(self, assignments: Dict[int, ReceiverAssignment]) -> None:
        channels: Dict[str, Dict[str, object]] = {}
        for rx in sorted(assignments.keys()):
            assignment = assignments[int(rx)]
            channels[str(rx)] = {
                "rx": int(rx),
                "kiwi_rx": int(rx),
                "freq_hz": float(assignment.freq_hz),
                "band": str(assignment.band),
                "mode": str(assignment.mode_label),
                "active": False,
                "visible_on_kiwi": False,
                "kiwi_user_age_s": None,
                "kiwi_actual_rx": None,
                "kiwi_occupant": None,
                "restart_count": 0,
                "consecutive_failures": 0,
                "backoff_s": 0.0,
                "cooling_down": False,
                "cooldown_remaining_s": 0.0,
                "last_reason": "starting",
                "last_updated_unix": None,
                "last_decoder_output_unix": None,
                "last_decode_unix": None,
                "decoder_output_age_s": None,
                "decode_age_s": None,
                "snr_last_db": None,
                "snr_avg_db": None,
                "snr_samples": 0,
                "snr_age_s": None,
                "decode_total": 0,
                "decode_rate_per_min": 0,
                "decode_rate_per_hour": 0,
                "decode_rates_by_mode": {},
                "propagation_state": "unknown",
                "health_state": "inactive",
                "status_level": "healthy",
                "is_no_decode_warning": False,
                "is_silent": False,
                "is_stalled": False,
                "is_unstable": False,
            }

        self._store_health_summary_cache({
            "overall": "starting" if channels else "idle",
            "active_receivers": 0,
            "unstable_receivers": 0,
            "stalled_receivers": 0,
            "silent_receivers": 0,
            "no_decode_warning_receivers": 0,
            "restart_total": 0,
            "health_stale_seconds": None,
            "reason_counts": {},
            "channels": channels,
            "propagation": self._empty_propagation_summary(),
            "auto_kick": self._empty_auto_kick_summary(),
            "_from_cache": True,
        })

    def _fallback_health_summary_locked(self) -> Dict[str, object]:
        cached = self._health_cache_with_meta()
        if cached:
            return cached

        active_host = str(getattr(self, "_active_host", "") or "")
        active_port = int(getattr(self, "_active_port", 0) or 0)
        try:
            assignments = {int(k): v for k, v in self._assignments.items()}
            watchdog_by_rx = {int(k): dict(v) for k, v in self._watchdog_state_by_rx.items()}
            restart_by_rx = {int(k): int(v) for k, v in self._restart_by_rx.items()}
            activity_by_rx = {int(k): dict(v) for k, v in self._activity_by_rx.items()}
            restart_total = int(self._restart_total)
            workers = {int(k): v for k, v in self._workers.items()}
        except Exception:
            assignments = {}
            watchdog_by_rx = {}
            restart_by_rx = {}
            activity_by_rx = {}
            restart_total = 0
            workers = {}

        if (not active_host or active_port <= 0) and workers:
            for worker in workers.values():
                try:
                    host = str(getattr(worker, "_host", "") or "")
                    port = int(getattr(worker, "_port", 0) or 0)
                except Exception:
                    host, port = "", 0
                if host and port > 0:
                    active_host, active_port = host, port
                    break

        users_with_age = self._fetch_live_users_with_age(active_host, active_port) if (active_host and active_port > 0) else {}
        users_by_rx = {
            int(slot): str(entry[0] if isinstance(entry, tuple) and len(entry) >= 1 else "").strip()
            for slot, entry in users_with_age.items()
        }
        user_age_by_rx = {
            int(slot): (
                float(entry[1])
                if isinstance(entry, tuple) and len(entry) >= 2 and isinstance(entry[1], (int, float))
                else None
            )
            for slot, entry in users_with_age.items()
        }

        channels: Dict[str, Dict[str, object]] = {}
        now = time.time()
        for rx in sorted(set(list(assignments.keys()) + list(watchdog_by_rx.keys()))):
            assignment = assignments.get(int(rx))
            wd = watchdog_by_rx.get(int(rx), {})
            activity = activity_by_rx.get(int(rx), {})
            expected_label = self._expected_user_label(assignment) if assignment is not None else ""
            expected_labels = self._expected_user_label_aliases(assignment)

            observed_slot: int | None = None
            observed_label: str | None = None
            observed_age_s: float | None = None
            if expected_label:
                for slot, entry in users_with_age.items():
                    label = str(entry[0] if isinstance(entry, tuple) and len(entry) >= 1 else "").strip()
                    if not label:
                        continue
                    if self._label_matches_any(expected_labels, label):
                        observed_slot = int(slot)
                        observed_label = label
                        age_s = entry[1] if isinstance(entry, tuple) and len(entry) >= 2 else None
                        try:
                            observed_age_s = float(age_s) if age_s is not None else None
                        except Exception:
                            observed_age_s = None
                        break

            worker = workers.get(int(rx))
            worker_alive = bool(worker is not None and worker.is_alive())
            last_decoder_output_unix = activity.get("last_decoder_output_unix")
            last_decode_unix = activity.get("last_decode_unix")
            try:
                last_decoder_output_unix_f = float(last_decoder_output_unix) if last_decoder_output_unix is not None else None
            except Exception:
                last_decoder_output_unix_f = None
            try:
                last_decode_unix_f = float(last_decode_unix) if last_decode_unix is not None else None
            except Exception:
                last_decode_unix_f = None

            channels[str(rx)] = {
                "rx": int(rx),
                "kiwi_rx": observed_slot if observed_slot is not None else int(rx),
                "freq_hz": float(assignment.freq_hz) if assignment is not None else None,
                "band": str(assignment.band) if assignment is not None else wd.get("band"),
                "mode": str(assignment.mode_label) if assignment is not None else None,
                "active": bool(observed_slot is not None or worker_alive),
                "visible_on_kiwi": bool(observed_slot is not None),
                "kiwi_user_age_s": observed_age_s,
                "kiwi_actual_rx": observed_slot,
                "kiwi_occupant": str(users_by_rx.get(int(rx), "") or "") or None,
                "restart_count": int(restart_by_rx.get(int(rx), 0)),
                "consecutive_failures": int(wd.get("consecutive_failures", 0) or 0),
                "backoff_s": float(wd.get("backoff_s", 0.0) or 0.0),
                "cooling_down": False,
                "cooldown_remaining_s": 0.0,
                "last_reason": str(wd.get("reason") or ("starting" if assignment is not None else "unknown")),
                "last_updated_unix": max(v for v in [
                    float(wd.get("updated_unix")) if wd.get("updated_unix") is not None else None,
                    last_decoder_output_unix_f,
                    last_decode_unix_f,
                ] if v is not None) if any(v is not None for v in [wd.get("updated_unix"), last_decoder_output_unix_f, last_decode_unix_f]) else None,
                "last_decoder_output_unix": last_decoder_output_unix_f,
                "last_decode_unix": last_decode_unix_f,
                "decoder_output_age_s": max(0.0, now - last_decoder_output_unix_f) if last_decoder_output_unix_f is not None else None,
                "decode_age_s": max(0.0, now - last_decode_unix_f) if last_decode_unix_f is not None else None,
                "snr_last_db": None,
                "snr_avg_db": None,
                "snr_samples": int(activity.get("snr_samples", 0) or 0),
                "snr_age_s": None,
                "decode_total": int(activity.get("decode_total", 0) or 0),
                "decode_rate_per_min": 0,
                "decode_rate_per_hour": 0,
                "decode_rates_by_mode": {},
                "propagation_state": "unknown",
                "health_state": "healthy" if (observed_slot is not None or worker_alive) else "inactive",
                "status_level": "healthy",
                "is_no_decode_warning": False,
                "is_silent": False,
                "is_stalled": False,
                "is_unstable": False,
            }

        result = {
            "overall": "starting" if channels else "idle",
            "active_receivers": sum(1 for ch in channels.values() if bool(ch.get("active"))),
            "unstable_receivers": 0,
            "stalled_receivers": 0,
            "silent_receivers": 0,
            "no_decode_warning_receivers": 0,
            "restart_total": restart_total,
            "health_stale_seconds": None,
            "reason_counts": {},
            "channels": channels,
            "propagation": self._empty_propagation_summary(),
            "auto_kick": self._auto_kick_summary(),
            "_from_cache": True,
        }
        self._store_health_summary_cache(result)
        return result

    def truth_snapshot(self) -> Dict[str, object]:
        """Return an explicit expected-vs-observed receiver view.

        This helps distinguish real worker intent from transient/ghost labels
        reported by Kiwi /users during reconnect churn.
        """
        lock_acquired = self._lock.acquire(timeout=0.5)
        if not lock_acquired:
            cached = self._truth_cache_with_meta()
            if cached:
                return cached

            # Best-effort fallback while apply_assignments holds the lock:
            # first try a non-blocking snapshot of current assignments/workers.
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)
            try:
                fallback_assignments = {int(k): v for k, v in self._assignments.items()}
                fallback_workers = {int(k): v for k, v in self._workers.items()}
            except Exception:
                fallback_assignments = {}
                fallback_workers = {}

            if (not active_host or active_port <= 0) and fallback_workers:
                for worker in fallback_workers.values():
                    try:
                        h = str(getattr(worker, "_host", "") or "")
                        p = int(getattr(worker, "_port", 0) or 0)
                    except Exception:
                        h, p = "", 0
                    if h and p > 0:
                        active_host, active_port = h, p
                        break

            users_with_age = self._fetch_live_users_with_age(active_host, active_port) if (active_host and active_port > 0) else {}

            if fallback_assignments:
                channels: Dict[str, object] = {}
                for rx in sorted(fallback_assignments.keys()):
                    assignment = fallback_assignments[int(rx)]
                    expected_label = self._expected_user_label(assignment)
                    expected_labels = self._expected_user_label_aliases(assignment)
                    observed_slot: int | None = None
                    observed_label: str = ""
                    observed_age_s: float | None = None
                    for slot, entry in users_with_age.items():
                        label = str(entry[0] if isinstance(entry, tuple) and len(entry) >= 1 else "").strip()
                        if not label:
                            continue
                        if self._label_matches_any(expected_labels, label):
                            observed_slot = int(slot)
                            observed_label = label
                            try:
                                age_s = entry[1] if isinstance(entry, tuple) and len(entry) >= 2 else None
                                observed_age_s = float(age_s) if age_s is not None else None
                            except Exception:
                                observed_age_s = None
                            break

                    worker = fallback_workers.get(int(rx))
                    worker_alive = bool(worker is not None and worker.is_alive())
                    worker_user = str(getattr(worker, "_active_user_label", "") or "").strip() if worker is not None else ""
                    proc = getattr(worker, "_proc", None) if worker is not None else None
                    proc_pid = int(getattr(proc, "pid", 0) or 0) if proc is not None else 0
                    proc_alive = bool(proc is not None and proc.poll() is None)

                    channels[str(rx)] = {
                        "rx": int(rx),
                        "band": str(assignment.band),
                        "mode": str(assignment.mode_label),
                        "expected_label": expected_label,
                        "worker_user": worker_user,
                        "worker_alive": worker_alive,
                        "proc_pid": proc_pid if proc_pid > 0 else None,
                        "proc_alive": proc_alive,
                        "observed_slot": observed_slot,
                        "observed_label": observed_label or None,
                        "observed_age_s": observed_age_s,
                        "label_match": bool(observed_slot is not None),
                        "slot_match": bool(observed_slot == int(rx)) if observed_slot is not None else False,
                    }

                result = {
                    "overall": "degraded",
                    "host": active_host,
                    "port": int(active_port),
                    "channels": channels,
                    "_from_cache": True,
                }
                self._store_truth_snapshot_cache(result)
                return result

            # Final fallback: derive expected labels from health summary + live /users.
            health = self.health_summary()
            health_channels = health.get("channels") if isinstance(health, dict) else {}
            health_channels = health_channels if isinstance(health_channels, dict) else {}
            users_with_age = self._fetch_live_users_with_age(active_host, active_port) if (active_host and active_port > 0) else {}

            channels: Dict[str, object] = {}
            for rx_key, ch in sorted(health_channels.items(), key=lambda kv: int(str(kv[0]))):
                if not isinstance(ch, dict):
                    continue
                rx_i = int(ch.get("rx", int(rx_key)))
                band = str(ch.get("band") or "").strip().upper()
                mode_label = str(ch.get("mode") or "").strip().upper()
                prefix = _preferred_user_label_prefix(rx_i)
                expected_label = _compact_user_label(prefix, band, mode_label)
                expected_labels = self._user_label_aliases_for_rx(rx_i, band, mode_label)

                observed_slot: int | None = None
                observed_label: str = ""
                observed_age_s: float | None = None
                for slot, entry in users_with_age.items():
                    label = str(entry[0] if isinstance(entry, tuple) and len(entry) >= 1 else "").strip()
                    if not label:
                        continue
                    if self._label_matches_any(expected_labels, label):
                        observed_slot = int(slot)
                        observed_label = label
                        try:
                            age_s = entry[1] if isinstance(entry, tuple) and len(entry) >= 2 else None
                            observed_age_s = float(age_s) if age_s is not None else None
                        except Exception:
                            observed_age_s = None
                        break

                channels[str(rx_i)] = {
                    "rx": rx_i,
                    "band": band,
                    "mode": mode_label,
                    "expected_label": expected_label,
                    "worker_user": None,
                    "worker_alive": None,
                    "proc_pid": None,
                    "proc_alive": None,
                    "observed_slot": observed_slot,
                    "observed_label": observed_label or None,
                    "observed_age_s": observed_age_s,
                    "label_match": bool(observed_slot is not None),
                    "slot_match": bool(observed_slot == rx_i) if observed_slot is not None else False,
                }

            if channels:
                result = {
                    "overall": str(health.get("overall") or "busy") if isinstance(health, dict) else "busy",
                    "host": active_host,
                    "port": int(active_port),
                    "channels": channels,
                    "_from_cache": True,
                }
                self._store_truth_snapshot_cache(result)
                return result

            return {
                "overall": "busy",
                "host": "",
                "port": 0,
                "channels": {},
                "_from_cache": True,
            }
        try:
            assignments = {int(k): v for k, v in self._assignments.items()}
            workers = {int(k): v for k, v in self._workers.items()}
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)
        finally:
            self._lock.release()

        users_with_age = self._fetch_live_users_with_age(active_host, active_port) if (active_host and active_port > 0) else {}
        channels: Dict[str, object] = {}

        for rx in sorted(assignments.keys()):
            assignment = assignments[int(rx)]
            expected_label = self._expected_user_label(assignment)
            expected_labels = self._expected_user_label_aliases(assignment)
            observed_slot: int | None = None
            observed_label: str = ""
            observed_age_s: float | None = None
            for slot, entry in users_with_age.items():
                try:
                    label = str(entry[0] if isinstance(entry, tuple) and len(entry) >= 1 else "").strip()
                    age_s = entry[1] if isinstance(entry, tuple) and len(entry) >= 2 else None
                except Exception:
                    continue
                if not label:
                    continue
                if self._label_matches_any(expected_labels, label):
                    observed_slot = int(slot)
                    observed_label = label
                    try:
                        observed_age_s = float(age_s) if age_s is not None else None
                    except Exception:
                        observed_age_s = None
                    break

            worker = workers.get(int(rx))
            worker_alive = bool(worker is not None and worker.is_alive())
            worker_user = str(getattr(worker, "_active_user_label", "") or "").strip() if worker is not None else ""
            proc = getattr(worker, "_proc", None) if worker is not None else None
            proc_pid = int(getattr(proc, "pid", 0) or 0) if proc is not None else 0
            proc_alive = bool(proc is not None and proc.poll() is None)

            channels[str(rx)] = {
                "rx": int(rx),
                "band": str(assignment.band),
                "mode": str(assignment.mode_label),
                "expected_label": expected_label,
                "worker_user": worker_user,
                "worker_alive": worker_alive,
                "proc_pid": proc_pid if proc_pid > 0 else None,
                "proc_alive": proc_alive,
                "observed_slot": observed_slot,
                "observed_label": observed_label or None,
                "observed_age_s": observed_age_s,
                "label_match": bool(observed_slot is not None),
                "slot_match": bool(observed_slot == int(rx)) if observed_slot is not None else False,
            }

        result = {
            "overall": "ok",
            "host": active_host,
            "port": int(active_port),
            "channels": channels,
            "_from_cache": False,
        }
        self._store_truth_snapshot_cache(result)
        return result

    def health_summary(self) -> Dict[str, object]:
        # Use a short timeout on the lock so this method never blocks indefinitely.
        # apply_assignments() holds self._lock for the entire startup/eviction cycle
        # (which can last minutes).  Without a timeout, FastAPI threadpool threads
        # pile up waiting here and the HTTP server becomes completely unresponsive.
        # On timeout, return the last successfully-computed result (or a minimal
        # "busy" placeholder on the very first call before any result is cached).
        lock_acquired = self._lock.acquire(timeout=0.5)
        if not lock_acquired:
            return self._fallback_health_summary_locked()
        try:
            assignments = dict(self._assignments)
            watchdog_by_rx = {int(k): dict(v) for k, v in self._watchdog_state_by_rx.items()}
            restart_by_rx = {int(k): int(v) for k, v in self._restart_by_rx.items()}
            activity_by_rx = {int(k): dict(v) for k, v in self._activity_by_rx.items()}
            restart_total = int(self._restart_total)
            active_host = str(getattr(self, "_active_host", "") or "")
            active_port = int(getattr(self, "_active_port", 0) or 0)
        finally:
            self._lock.release()

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
            if not self._is_auto_label(label):
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
            expected_label = self._expected_user_label(assignment) if assignment is not None else ""
            expected_labels = self._expected_user_label_aliases(assignment)
            if is_ssb and assignment is not None:
                # Search all kiwi slots — workers may land on any slot index at connect time.
                # Use the same compact label builder as the worker so health checks stay
                # aligned with Kiwi's truncated /users labels.
                visible_slot = next(
                    (slot for slot, name in users_by_rx.items() if self._label_matches_any(expected_labels, name)),
                    None,
                )
                is_active = visible_slot is not None
                visible_on_kiwi = bool(is_active)
                kiwi_user_age_s = user_age_by_rx.get(visible_slot if visible_slot is not None else int(rx))
                if not is_active:
                    last_reason = last_reason or "kiwi_not_visible"
            elif assignment is not None:
                # Search all kiwi slots — workers may land on any slot index at connect time.
                # Use the same compact label builder as the worker so health checks stay
                # aligned with Kiwi's truncated /users labels.
                visible_slot = next(
                    (slot for slot, name in users_by_rx.items() if self._label_matches_any(expected_labels, name)),
                    None,
                )
                visible_on_kiwi = visible_slot is not None
                is_active = bool(visible_on_kiwi)
                kiwi_user_age_s = user_age_by_rx.get(visible_slot if visible_slot is not None else int(rx))
                if not is_active:
                    last_reason = last_reason or "kiwi_not_visible"

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
                expected_label_keys = {
                    str(label or "").strip().upper()
                    for label in expected_labels
                    if str(label or "").strip()
                }
                # Also search for truncated key variants (KiwiSDR may truncate long usernames)
                _live_locs_raw = None
                for _expected_key in expected_label_keys:
                    _live_locs_raw = live_auto_locations.get(_expected_key)
                    if _live_locs_raw is not None:
                        break
                if _live_locs_raw is None:
                    for _key, _locs in live_auto_locations.items():
                        if self._label_matches_any(expected_labels, _key):
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
                    self._is_auto_label(occupant)
                    and not self._label_matches_any(expected_labels, occupant)
                    and (occupant_age_s is None or occupant_age_s >= remap_grace_s)
                )
                # Record what is blocking the expected slot (if anything foreign is there)
                if occupant and not self._label_matches_any(expected_labels, occupant):
                    kiwi_occupant = occupant
                mismatch_detected = bool(wrong_slot_stale or displaced_by_stale_auto)
                strict_roaming_slot = bool(int(rx) < 2 and not bool(getattr(assignment, "ignore_slot_check", False)))
                # When the worker is completely absent from Kiwi AND the expected slot is
                # occupied by a stale alien AUTO_ process, the worker clearly failed to
                # connect — treat as actionable even if the decoder has recent output
                # (output may be stale from before the disconnect).
                fully_displaced = bool(
                    displaced_by_stale_auto
                    and not visible_on_kiwi
                    and not live_locations
                )
                # Only act on slot mismatch if the decoder is also not producing output,
                # OR if the worker is fully displaced (invisible everywhere on Kiwi).
                # When a worker adapts to a different KiwiSDR slot but is running fine,
                # decoder_missing=False means data is flowing — no stall action needed.
                mismatch_actionable = bool(
                    mismatch_detected
                    and (
                        decoder_missing
                        or fully_displaced
                        or (strict_roaming_slot and wrong_slot_stale)
                    )
                )
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
                "freq_hz": float(assignment.freq_hz) if assignment is not None else None,
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

        result = {
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
            "auto_kick": self._auto_kick_summary(),
        }
        # Store as fallback for callers that time out waiting for the lock.
        self._store_health_summary_cache(result)
        return result

    def _cleanup_orphan_processes(self) -> None:
        try:
            subprocess.run(
                ["pkill", "-9", "-f", "kiwirecorder.py.*(AUTO_|FIXED_|ROAM)"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    def _cleanup_orphan_processes_for_labels(self, labels: set[str]) -> None:
        for label in {str(v or "").strip() for v in labels if str(v or "").strip()}:
            try:
                subprocess.run(
                    ["pkill", "-9", "-f", f"kiwirecorder.py.*{re.escape(label)}"],
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
                    ["pgrep", "-f", "kiwirecorder.py.*(AUTO_|FIXED_|ROAM)"],
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
                    if self._is_auto_label(name):
                        auto_count += 1
                if auto_count == 0:
                    return
            except Exception:
                return
            time.sleep(0.25)

    def _wait_for_kiwi_slots_stable_clear(
        self, *, host: str, port: int, stable_secs: float = 5.0, timeout_s: float = 30.0
    ) -> None:
        """Wait until /users shows 0 AUTO users AND stays at 0 for `stable_secs` seconds.

        The Kiwi's /users REST endpoint drops entries immediately when a session is
        kicked or when its TCP connection closes, but the internal slot counter may
        take several more seconds to decrement (especially when connections traverse
        a VPN tunnel where RST/FIN delivery is delayed).  New connections during that
        window are rejected with "Too busy", yet /users already shows zero users.
        Requiring stability for `stable_secs` ensures the internal state has settled.
        """
        deadline = time.time() + max(float(stable_secs) + 1.0, float(timeout_s))
        status_url = f"http://{host}:{int(port)}/users"
        stable_since: Optional[float] = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(status_url, timeout=1.2) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if not isinstance(payload, list):
                    return
                auto_count = sum(
                    1 for row in payload
                    if isinstance(row, dict)
                    and urllib.parse.unquote(str(row.get("n") or "")).strip().startswith(("AUTO_", "FIXED_", "ROAM"))
                )
                if auto_count == 0:
                    if stable_since is None:
                        stable_since = time.time()
                    elif time.time() - stable_since >= float(stable_secs):
                        return  # Stable at zero long enough
                else:
                    stable_since = None  # Reset stability timer
            except Exception:
                return  # Network error — assume clear
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
            live_users = self._fetch_live_users(host, port)
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

    def _wait_for_kiwi_slots_clear(
        self,
        *,
        host: str,
        port: int,
        slots: set[int] | list[int],
        stable_secs: float = 2.0,
        timeout_s: float = 8.0,
    ) -> bool:
        target_slots = sorted({int(slot) for slot in slots})
        if not target_slots:
            return True
        deadline = time.time() + max(float(timeout_s), float(stable_secs) + 0.5)
        stable_since: Optional[float] = None
        while time.time() < deadline:
            live_users = self._fetch_live_users(host, port)
            if not any(int(slot) in live_users for slot in target_slots):
                if stable_since is None:
                    stable_since = time.time()
                elif time.time() - stable_since >= float(stable_secs):
                    return True
            else:
                stable_since = None
            time.sleep(0.25)
        return False

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
    def _force_full_reset_on_band_change_enabled(cls) -> bool:
        return cls._env_bool("KIWISCAN_RESET_ALL_ON_BAND_CHANGE", True)

    @classmethod
    def _force_full_reset_on_reconcile_enabled(cls) -> bool:
        return cls._env_bool("KIWISCAN_RESET_ALL_ON_RECONCILE", True)

    @classmethod
    def _band_plan_changed(cls, current: Dict[int, ReceiverAssignment], desired: Dict[int, ReceiverAssignment]) -> bool:
        if not current or not desired:
            return False
        fixed_rxs = {int(rx) for rx in (set(current.keys()) | set(desired.keys())) if int(rx) >= 2}
        if fixed_rxs:
            for rx in fixed_rxs:
                cur_assignment = current.get(int(rx))
                new_assignment = desired.get(int(rx))
                if cur_assignment is None or new_assignment is None:
                    return True
                if not cls._assignment_equivalent(cur_assignment, new_assignment):
                    return True
            # In fixed mode, adding or rotating RX0/RX1 roaming should not force a
            # full reset of the already-stable fixed receivers.
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
            return str(value or "").strip().upper().replace("_", "")

        expected_text = _canon(expected)
        actual_text = _canon(urllib.parse.unquote(str(actual or "").strip()))
        if not expected_text or not actual_text:
            return False
        if actual_text == expected_text:
            return True
        if actual_text.startswith(expected_text) or expected_text.startswith(actual_text):
            return True
        return len(actual_text) >= max(8, len(expected_text) - 3) and expected_text.startswith(actual_text)

    @staticmethod
    def _legacy_user_label_mode_token(mode_label: str) -> str:
        mode_text = str(mode_label or "").strip().upper().replace(" ", "")
        if "FT8" in mode_text:
            return "FT8"
        if "FT4" in mode_text:
            return "FT4"
        if "WSPR" in mode_text:
            return "WS"
        if "SSB" in mode_text or "PHONE" in mode_text:
            return "SSB"
        cleaned = "".join(ch for ch in mode_text if ch.isalnum())
        return cleaned[:4]

    @classmethod
    def _legacy_compact_user_label(cls, prefix: str, band: str, mode_label: str) -> str:
        prefix_text = "".join(ch for ch in str(prefix or "").strip().upper() if ch.isalnum())
        band_text = "".join(ch for ch in str(band or "").strip().upper() if ch.isalnum())
        mode_text = cls._legacy_user_label_mode_token(mode_label)
        return f"{prefix_text}{band_text}{mode_text}"[:12]

    @classmethod
    def _user_label_aliases(cls, prefix: str, band: str, mode_label: str) -> set[str]:
        labels: set[str] = set()
        readable = _compact_user_label(prefix, band, mode_label)
        if readable:
            labels.add(readable)
        legacy = cls._legacy_compact_user_label(prefix, band, mode_label)
        if legacy:
            labels.add(legacy)
        return labels

    @classmethod
    def _user_label_aliases_for_rx(cls, rx: int, band: str, mode_label: str) -> set[str]:
        labels: set[str] = set()
        for prefix in _compatible_user_label_prefixes(int(rx)):
            labels.update(cls._user_label_aliases(prefix, band, mode_label))
        return labels

    @classmethod
    def _expected_user_label_aliases(cls, assignment: Optional[ReceiverAssignment]) -> set[str]:
        if assignment is None:
            return set()
        mode_label = "SSB" if bool(assignment.ssb_scan) and cls._is_ssb_assignment(assignment) else assignment.mode_label
        return cls._user_label_aliases_for_rx(int(assignment.rx), assignment.band, str(mode_label or ""))

    @classmethod
    def _label_matches_any(cls, expected_labels: set[str], actual: str) -> bool:
        return any(
            cls._user_label_matches(expected, actual)
            for expected in expected_labels
            if str(expected or "").strip()
        )

    @classmethod
    def _expected_user_label(cls, assignment: ReceiverAssignment) -> str:
        prefix = _preferred_user_label_prefix(int(assignment.rx))
        if bool(assignment.ssb_scan) and cls._is_ssb_assignment(assignment):
            return _compact_user_label(prefix, assignment.band, "SSB")
        return _compact_user_label(prefix, assignment.band, assignment.mode_label)

    def _seed_truth_snapshot_cache(self, *, host: str, port: int, assignments: Dict[int, ReceiverAssignment]) -> None:
        """Seed truth cache before long apply phases so endpoint has immediate structure."""
        workers = {int(k): v for k, v in self._workers.items()}
        channels: Dict[str, object] = {}
        for rx in sorted(assignments.keys()):
            assignment = assignments[int(rx)]
            expected_label = self._expected_user_label(assignment)
            worker = workers.get(int(rx))
            worker_alive = bool(worker is not None and worker.is_alive())
            worker_user = str(getattr(worker, "_active_user_label", "") or "").strip() if worker is not None else ""
            proc = getattr(worker, "_proc", None) if worker is not None else None
            proc_pid = int(getattr(proc, "pid", 0) or 0) if proc is not None else 0
            proc_alive = bool(proc is not None and proc.poll() is None)

            channels[str(rx)] = {
                "rx": int(rx),
                "band": str(assignment.band),
                "mode": str(assignment.mode_label),
                "expected_label": expected_label,
                "worker_user": worker_user,
                "worker_alive": worker_alive,
                "proc_pid": proc_pid if proc_pid > 0 else None,
                "proc_alive": proc_alive,
                "observed_slot": None,
                "observed_label": None,
                "observed_age_s": None,
                "label_match": False,
                "slot_match": False,
            }

        self._store_truth_snapshot_cache({
            "overall": "starting",
            "host": str(host),
            "port": int(port),
            "channels": channels,
            "_from_cache": True,
        })

    @classmethod
    def _fetch_live_users(cls, host: str, port: int) -> Dict[int, str]:
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
                if name:
                    out[int(rx_i)] = name
        except Exception:
            return {}
        return out

    @classmethod
    def _fetch_live_auto_users(cls, host: str, port: int) -> Dict[int, str]:
        return {
            int(slot): label
            for slot, label in cls._fetch_live_users(host, port).items()
            if cls._is_auto_label(label)
        }

    @classmethod
    def _fetch_live_users_with_age(cls, host: str, port: int) -> Dict[int, tuple]:
        """Return {slot: (label, age_seconds_or_None)} for all visible Kiwi users."""
        out: Dict[int, tuple] = {}
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
                if not name:
                    continue
                age_s: Optional[float] = None
                try:
                    parts = [int(p) for p in str(row.get("t") or "").split(":") if str(p).strip()]
                    if len(parts) == 3:
                        age_s = float(parts[0] * 3600 + parts[1] * 60 + parts[2])
                    elif len(parts) == 2:
                        age_s = float(parts[0] * 60 + parts[1])
                    elif len(parts) == 1:
                        age_s = float(parts[0])
                except Exception:
                    pass
                out[int(rx_i)] = (name, age_s)
        except Exception:
            return {}
        return out

    def _assignment_slots_needing_reconcile(
        self,
        *,
        host: str,
        port: int,
        assignments: Dict[int, ReceiverAssignment],
    ) -> set[int]:
        live_users = self._fetch_live_users(host, port)
        if not live_users:
            return set()

        out: set[int] = set()
        for rx, assignment in assignments.items():
            expected_labels = self._expected_user_label_aliases(assignment)
            ignore_slot = bool(getattr(assignment, "ignore_slot_check", False))
            if ignore_slot:
                # For fixed receivers (ignore_slot_check=True), the Kiwi slot won't
                # match the app rx number. Still require the expected FIXED_* label to
                # be visible somewhere on the Kiwi, otherwise stale AUTO users can keep
                # the worker blocked indefinitely while the local thread looks healthy.
                label_found = any(
                    self._label_matches_any(expected_labels, lbl)
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
                if not active_label:
                    # Worker exists but has no active Kiwi connection.  If it has
                    # already attempted to connect (slot_ready set) it lost the slot
                    # race — flag for reconcile so a fresh kick-all frees a slot.
                    slot_ready = getattr(worker, "_slot_ready", None)
                    if slot_ready is not None and slot_ready.is_set():
                        out.add(int(rx))
                    continue
                if not self._label_matches_any(expected_labels, active_label):
                    out.add(int(rx))
                continue
            # For roaming receivers, check if the expected label appears on ANY active Kiwi
            # slot (not just the "expected" slot number). This avoids false drift triggers
            # when a human listener occupies the expected slot.
            label_found = any(
                self._label_matches_any(expected_labels, lbl)
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
            if active_label and not self._label_matches_any(expected_labels, active_label):
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

            self._seed_health_summary_cache(assignments)
            self._seed_truth_snapshot_cache(host=str(host), port=int(port), assignments=assignments)
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
            desired_fixed_rxs = sorted(
                int(rx)
                for rx in desired_rxs
                if bool(getattr(assignments[int(rx)], "ignore_slot_check", False))
            )
            force_full_reset = (
                self._force_full_reset_on_band_change_enabled()
                and self._band_plan_changed(self._assignments, assignments)
            )
            starting_from_empty = bool(desired_rxs and not current_rxs)
            bootstrap_fixed_first = bool(
                starting_from_empty
                and desired_fixed_rxs
                and any(int(rx) < 2 for rx in desired_rxs)
            )
            if starting_from_empty:
                if bootstrap_fixed_first:
                    logger.info(
                        "Starting receiver set from empty state; fixed-first bootstrap enabled (skip full Kiwi reset)"
                    )
                else:
                    logger.info("Starting receiver set from empty state; forcing full Kiwi receiver reset before re-apply")
            did_full_reset = bool(host_changed or force_full_reset or force_reconcile_full_reset)
            if starting_from_empty and not bootstrap_fixed_first:
                did_full_reset = True
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
                    stopped_labels.update(self._expected_user_label_aliases(current_assignment))

            for rx in sorted(to_stop):
                worker = self._workers.pop(rx, None)
                if worker is not None:
                    self._stop_worker(
                        worker,
                        join_timeout_s=6.0 if not assignments else 3.0,
                        graceful=not assignments,
                        graceful_timeout_s=6.0,
                    )
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
                if stopped_labels:
                    wait_host = prior_host if prior_host else str(host)
                    wait_port = prior_port if prior_port > 0 else int(port)
                    self._wait_for_kiwi_auto_users_missing(
                        host=wait_host,
                        port=wait_port,
                        labels=stopped_labels,
                        timeout_s=12.0,
                    )
                self._cleanup_orphan_processes()
                self._wait_for_orphan_cleanup(timeout_s=6.0)
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
                # Only kick ALL Kiwi channels when the host changed, starting from empty,
                # or every fixed receiver is also being stopped. When fixed receivers are
                # preserved (band-plan only changes for roaming slots) a kick-all would
                # disconnect their Kiwi sessions.  When starting from empty (e.g. after
                # Manual mode or a fresh container start) there are no running workers to
                # protect, so we must always kick to clear any stale sessions left by a
                # previous container run before our workers try to claim slots.
                any_fixed_preserved = any(
                    assignments.get(int(rx)) is not None
                    and bool(getattr(assignments[int(rx)], "ignore_slot_check", False))
                    and int(rx) not in to_stop
                    and self._workers.get(int(rx)) is not None
                    for rx in current_rxs | desired_rxs
                )
                if host_changed or starting_from_empty or not any_fixed_preserved:
                    # When starting from empty (e.g. after Manual mode), always use
                    # force_all=True so ALL users (including competing controllers) are
                    # evicted.  Then start workers immediately (2 s sleep) to claim
                    # slots before the competing device can reconnect.  For other full
                    # resets (band-plan change, host change) keep the conservative
                    # wait-for-clear + second-kick behaviour.
                    _force_all = bool(host_changed) or starting_from_empty
                    _ = self._run_admin_kick_all(host=str(host), port=int(port), force_all=_force_all)
                    # For both starting_from_empty and band-plan changes: wait for all
                    # kicked sessions to fully disconnect before starting workers.  The
                    # old 0.5 s sleep was not enough — stale sessions from the previous
                    # container run could still be in a "closing" state, causing Kiwi to
                    # assign our workers to the next-available slot instead of the
                    # requested one.  The same double-kick / wait-for-clear pattern used
                    # by the non-empty path is applied here.
                    _clear_timeout = 6.0 if starting_from_empty else 8.0
                    self._wait_for_kiwi_auto_users_clear(host=str(host), port=int(port), timeout_s=_clear_timeout)
                    # If stale AUTO_ users still remain after the first wait (e.g. a previous
                    # container's connection that hadn't fully closed), kick once more with a
                    # longer wait to ensure those ghost slots are freed before workers start.
                    if self._fetch_live_auto_users(str(host), int(port)):
                        logger.info("Stale AUTO_ users still present after kick; sending second kick")
                        _ = self._run_admin_kick_all(host=str(host), port=int(port), force_all=True)
                        self._wait_for_kiwi_auto_users_clear(host=str(host), port=int(port), timeout_s=12.0)

                    if starting_from_empty and desired_fixed_rxs:
                        logger.info(
                            "Starting from empty with fixed receivers configured; skipping global stable-clear wait and claiming fixed slots immediately"
                        )
                        time.sleep(1.0)
                    else:
                        # Stable-clear wait: the Kiwi's REST /users clears immediately after
                        # kick but its internal slot counter may take several more seconds to
                        # decrement (especially over VPN).  Requiring /users to stay at 0 for
                        # stable_secs guarantees the internal state has settled before workers
                        # connect.
                        self._wait_for_kiwi_slots_stable_clear(
                            host=str(host), port=int(port), stable_secs=10.0, timeout_s=60.0
                        )
                else:
                    # Fixed workers are preserved — /users will never reach 0, so the
                    # stable-clear wait above would burn the full 60 s timeout without
                    # benefit.  Instead, kick only the ROAMING workers' target slots (0
                    # and 1) if anything is occupying them (e.g. a fixed worker stuck at
                    # the wrong slot from a prior P-pointer misalignment).  This clears
                    # exactly the slots the roaming workers need without disconnecting the
                    # healthy fixed receivers.
                    _roaming_target_slots = sorted(
                        int(rx) for rx in to_stop if int(rx) in desired_rxs
                    )
                    if _roaming_target_slots:
                        _live_fk = self._fetch_live_auto_users(str(host), int(port))
                        _blocked_slots = [s for s in _roaming_target_slots if s in _live_fk]
                        if _blocked_slots:
                            logger.info(
                                "Roaming update: target slot(s) %s occupied; kicking before restart",
                                _blocked_slots,
                            )
                            self._run_admin_kick_all(
                                host=str(host), port=int(port), kick_only_slots=_blocked_slots
                            )
                            # Wait for kicked slots to clear in /users before workers start
                            _fk_deadline = time.time() + 8.0
                            while time.time() < _fk_deadline:
                                _lv_fk2 = self._fetch_live_auto_users(str(host), int(port))
                                if not any(s in _lv_fk2 for s in _blocked_slots):
                                    break
                                time.sleep(0.5)
                            time.sleep(0.5)  # Brief extra pause for Kiwi internal state

            # Suppress monitoring-thread autokick for the entire initial connection
            # phase + eviction loop so the two code paths don't fight each other.
            # The flag is cleared (and the streak reset) after the eviction loop.
            self._startup_eviction_active.set()

            # Start workers SEQUENTIALLY in rx order (rx0, rx1, …, rx7).
            # The KiwiSDR assigns receiver slots in connection-arrival order.
            # For full resets (starting_from_empty or no preserved fixed workers), all
            # Kiwi slots are free after the stable-clear wait above.  For roaming-only
            # updates (fixed workers preserved), target slots 0/1 have been cleared by
            # the targeted kick above; fixed workers still hold the other slots.
            #
            # IMPORTANT: Do NOT kick individual slots during this phase.  Rapid-fire
            # kicks create VPN TCP churn that causes the Kiwi to say "Too busy" for
            # all subsequent connections.  Any wrong-slot placements are handled by
            # the eviction loop below once all workers are up.
            def _start_worker_sequential(rx: int) -> None:
                if rx in to_reconfigure and rx in self._workers:
                    return
                if rx in self._workers and rx in self._assignments and self._assignment_equivalent(self._assignments[rx], assignments[rx]) and not host_changed:
                    return
                desired = assignments[rx]
                worker = self._make_worker(host=host, port=port, assignment=desired, rx_chan_adjust=0)
                self._workers[rx] = worker
                worker.start()
                # Poll /users from the main thread until the expected label is present
                # and stable for >=2 s. This covers kiwirecorder's 15 s "Too busy"
                # retry cycle without any kicks.
                _exp_lbl = self._expected_user_label(desired)
                _exp_lbls = self._expected_user_label_aliases(desired)
                _poll_timeout_s = 15.0 if int(rx) >= 2 else 15.0
                if starting_from_empty and int(rx) >= 2:
                    # On empty-state fixed bootstrap, avoid long per-rx stalls so we can
                    # launch all fixed workers quickly before AUTO reconnect churn refills slots.
                    _poll_timeout_s = 6.0
                _poll_deadline = time.time() + _poll_timeout_s
                _stable_slot: Optional[int] = None
                _stable_since: float = 0.0
                _last_live_p: Dict[int, str] = {}
                while time.time() < _poll_deadline:
                    _live_p = self._fetch_live_users(str(host), int(port))
                    _last_live_p = dict(_live_p)
                    _cur_slot = next(
                        (int(_s) for _s, _l in _live_p.items() if self._label_matches_any(_exp_lbls, _l)),
                        None,
                    )
                    if _cur_slot is not None and _cur_slot == _stable_slot:
                        if time.time() - _stable_since >= 2.0:
                            break
                    else:
                        _stable_slot = _cur_slot
                        _stable_since = time.time()
                    time.sleep(0.4)
                if _stable_slot == int(rx):
                    logger.info("Sequential start rx=%d: correct slot=%d", rx, rx)
                elif _stable_slot is not None:
                    logger.info(
                        "Sequential start rx=%d: in slot=%d (expected %d); eviction loop will correct",
                        rx, _stable_slot, rx,
                    )
                else:
                    logger.warning(
                        "Sequential start rx=%d: not connected within %.1fs timeout",
                        rx,
                        _poll_timeout_s,
                    )
                    _keep_timed_out_worker = bool(starting_from_empty and int(rx) >= 2)
                    if _keep_timed_out_worker:
                        logger.info(
                            "Sequential start rx=%d: keeping timed-out worker running during bootstrap",
                            rx,
                        )
                    else:
                        _timed_out_worker = self._workers.pop(int(rx), None)
                        if _timed_out_worker is not None:
                            self._stop_worker(_timed_out_worker, join_timeout_s=0.5)
                        self._activity_by_rx.pop(int(rx), None)
                    _stale_slots = sorted(
                        {
                            int(_slot)
                            for _slot, _label in _last_live_p.items()
                            if self._label_matches_any(_exp_lbls, _label)
                            or int(_slot) == int(rx)
                        }
                    )
                    if _stale_slots:
                        try:
                            self._run_admin_kick_all(
                                host=str(host),
                                port=int(port),
                                kick_only_slots=_stale_slots,
                            )
                        except Exception:
                            pass
                        self._wait_for_kiwi_auto_users_missing(
                            host=str(host),
                            port=int(port),
                            labels=_exp_lbls,
                            timeout_s=3.0,
                        )

            _startup_order = sorted(desired_rxs)
            _defer_roaming_start = bool(bootstrap_fixed_first)
            if _defer_roaming_start:
                # On empty-state bootstrap, bring up fixed receivers first and
                # let the eviction loop settle before introducing roaming slots.
                # This avoids early RX0/RX1 correction churn that Kiwi can
                # transiently relabel as AUTO_* during slot rotation.
                _startup_order = list(desired_fixed_rxs)
                logger.info(
                    "Deferring roaming startup on empty-state bootstrap; starting fixed RXs first: %s",
                    _startup_order,
                )
            elif starting_from_empty and desired_fixed_rxs:
                _startup_order = desired_fixed_rxs + [int(rx) for rx in sorted(desired_rxs) if int(rx) not in desired_fixed_rxs]
            for rx in _startup_order:
                _start_worker_sequential(int(rx))

            _deferred_roaming_rxs = sorted(
                int(rx)
                for rx in desired_rxs
                if int(rx) < 2 and int(rx) not in _startup_order
            )
            if _defer_roaming_start and _deferred_roaming_rxs:
                logger.info(
                    "Fixed bootstrap complete; starting deferred roaming RXs: %s",
                    _deferred_roaming_rxs,
                )
                time.sleep(0.5)
                for rx in _deferred_roaming_rxs:
                    _start_worker_sequential(int(rx))

            roaming_rxs_to_verify = sorted(
                int(rx)
                for rx in desired_rxs
                if int(rx) < 2 and (int(rx) not in current_rxs or int(rx) in to_stop or int(rx) in reconcile_rxs)
            )
            if _defer_roaming_start and _deferred_roaming_rxs:
                roaming_rxs_to_verify = sorted(set(roaming_rxs_to_verify) | set(_deferred_roaming_rxs))
            if roaming_rxs_to_verify and (not starting_from_empty or _defer_roaming_start):
                MAX_ROAMING_CORRECTION_RETRIES = 3
                for _roam_attempt in range(MAX_ROAMING_CORRECTION_RETRIES):
                    if _roam_attempt > 0:
                        time.sleep(0.75)
                    _live_roam = self._fetch_live_users(str(host), int(port))
                    _expected_roam = {
                        int(rx): self._expected_user_label_aliases(assignments[int(rx)])
                        for rx in roaming_rxs_to_verify
                    }
                    _actual_slots: Dict[int, int] = {}
                    for _slot, _label in _live_roam.items():
                        for _rx, _expected_labels in _expected_roam.items():
                            if self._label_matches_any(_expected_labels, _label):
                                _actual_slots.setdefault(int(_rx), int(_slot))
                                break
                    _needs_fix = [
                        int(_rx)
                        for _rx in roaming_rxs_to_verify
                        if _actual_slots.get(int(_rx)) != int(_rx)
                    ]
                    _blocked_targets = [
                        int(_slot)
                        for _slot in roaming_rxs_to_verify
                        if int(_slot) in _live_roam
                        and not self._label_matches_any(_expected_roam[int(_slot)], _live_roam[int(_slot)])
                    ]
                    if not _needs_fix and not _blocked_targets:
                        break

                    _rxs_to_restart = sorted(set(_needs_fix) | set(_blocked_targets))
                    _slots_to_kick = sorted(
                        set(int(_rx) for _rx in _rxs_to_restart)
                        | {
                            int(_slot)
                            for _rx, _slot in _actual_slots.items()
                            if int(_rx) in _rxs_to_restart
                        }
                    )
                    logger.info(
                        "Roaming slot correction (attempt %d/%d): restart roaming RXs %s kick slots %s actual=%s",
                        _roam_attempt + 1,
                        MAX_ROAMING_CORRECTION_RETRIES,
                        _rxs_to_restart,
                        _slots_to_kick,
                        _actual_slots,
                    )
                    for _rx in _rxs_to_restart:
                        _worker = self._workers.pop(int(_rx), None)
                        if _worker is not None:
                            self._stop_worker(_worker, join_timeout_s=0.5)
                        self._activity_by_rx.pop(int(_rx), None)

                    if _slots_to_kick:
                        try:
                            self._run_admin_kick_all(
                                host=str(host),
                                port=int(port),
                                kick_only_slots=_slots_to_kick,
                            )
                        except Exception:
                            pass
                        _clear_deadline = time.time() + 20.0
                        _clear_stable_since: Optional[float] = None
                        while time.time() < _clear_deadline:
                            _live_after_kick = self._fetch_live_users(str(host), int(port))
                            if not any(int(_slot) in _live_after_kick for _slot in _slots_to_kick):
                                if _clear_stable_since is None:
                                    _clear_stable_since = time.time()
                                elif time.time() - _clear_stable_since >= 4.0:
                                    break
                            else:
                                _clear_stable_since = None
                            time.sleep(0.4)
                        time.sleep(0.5)

                    for _rx in _rxs_to_restart:
                        _start_worker_sequential(int(_rx))

            self._assignments = {int(rx): assignments[int(rx)] for rx in sorted(desired_rxs)}
            self._active_host = str(host)
            self._active_port = int(port)

            # Post-startup slot correction loop: handles two cases:
            #   (a) ABSENT workers — ghost blocked their slot entirely (rx not in live_now)
            #   (b) WRONGLY-PLACED workers — ghost displaced them to a different slot
            #       (connected, correct label, but Kiwi slot ≠ rx number)
            # For each attempt: stop affected workers, kick ghost/target slots, restart
            # all affected workers simultaneously, then re-check.
            if starting_from_empty and not _defer_roaming_start:
                MAX_EVICT_RETRIES = 16
                for _evict_attempt in range(MAX_EVICT_RETRIES):
                    # Brief wait on retries so we see the ghost after it reconnects
                    if _evict_attempt > 0:
                        time.sleep(0.75)
                    live_now = self._fetch_live_users(str(host), int(port))
                    expected_labels_by_rx = {
                        int(rx): self._expected_user_label_aliases(assignments[int(rx)])
                        for rx in desired_rxs
                    }
                    expected_label_by_rx = {
                        int(rx): self._expected_user_label(assignments[int(rx)])
                        for rx in desired_rxs
                    }
                    # Build rx → actual Kiwi slot mapping
                    rx_to_actual_slot: Dict[int, int] = {}
                    for _slot, _lbl in live_now.items():
                        for _rx in desired_rxs:
                            if self._label_matches_any(expected_labels_by_rx[int(_rx)], _lbl):
                                if int(_rx) not in rx_to_actual_slot:
                                    rx_to_actual_slot[int(_rx)] = int(_slot)
                                break
                    # Count how many workers legitimately share each expected label
                    # (e.g. two 20m FT8 receivers → AUTO_20M_FT8 expected count = 2).
                    _expected_label_counts: Dict[str, int] = {}
                    for _rx in desired_rxs:
                        _el = expected_label_by_rx[int(_rx)]
                        _expected_label_counts[_el] = _expected_label_counts.get(_el, 0) + 1
                    # Clone detection: label appears MORE times than expected (true ghost)
                    live_expected_counts: Dict[str, int] = {}
                    for _lbl in live_now.values():
                        for _rx in desired_rxs:
                            _exp = expected_label_by_rx[int(_rx)]
                            if self._label_matches_any(expected_labels_by_rx[int(_rx)], _lbl):
                                live_expected_counts[_exp] = live_expected_counts.get(_exp, 0) + 1
                                break
                    _cloned_labels = {
                        exp for exp, cnt in live_expected_counts.items()
                        if cnt > _expected_label_counts.get(exp, 1)
                    }
                    # Ghost slots: label not matching any expected, OR cloned
                    ghost_slots = [
                        _slot for _slot, _lbl in live_now.items()
                        if (
                            not any(self._label_matches_any(expected_labels_by_rx[int(_rx)], _lbl) for _rx in desired_rxs)
                            or any(
                                expected_label_by_rx[int(_rx)] in _cloned_labels
                                and self._label_matches_any(expected_labels_by_rx[int(_rx)], _lbl)
                                for _rx in desired_rxs
                            )
                        )
                    ]
                    # Absent: our label not found in live_now at all, or cloned
                    absent = [
                        int(_rx) for _rx in desired_rxs
                        if int(_rx) not in rx_to_actual_slot
                        or expected_label_by_rx[int(_rx)] in _cloned_labels
                    ]
                    # Wrongly placed: connected but Kiwi slot ≠ rx number
                    wrongly_placed = [
                        int(_rx) for _rx in desired_rxs
                        if int(_rx) in rx_to_actual_slot
                        and rx_to_actual_slot[int(_rx)] != int(_rx)
                        and int(_rx) not in absent
                    ]
                    needs_fix = sorted(set(absent + wrongly_placed))
                    # Slots to kick: ghosts + the TARGET slots for needs_fix workers
                    # (if those target slots are occupied by something that won't
                    # vacate on its own)
                    target_to_kick = [
                        int(_rx) for _rx in needs_fix
                        if int(_rx) in live_now
                        and not self._label_matches_any(expected_labels_by_rx[int(_rx)], live_now[int(_rx)])
                    ]
                    all_slots_to_evict = sorted(set(ghost_slots + target_to_kick))
                    if not all_slots_to_evict and not needs_fix:
                        break  # All workers at correct slots, no ghosts
                    if not needs_fix and not wrongly_placed:
                        # Ghost present but not blocking anyone — no corrective action
                        break

                    # If all missing workers are simply absent (not connected yet) AND
                    # nothing blocks their target slots, they are still in the process
                    # of connecting from the initial simultaneous start.  Killing and
                    # restarting them would discard in-flight connections; just wait
                    # for them to establish themselves on the next iteration.
                    if not ghost_slots and not wrongly_placed and not target_to_kick:
                        continue

                    logger.info(
                        "Post-startup slot correction (attempt %d/%d): "
                        "evicting slot(s) %s  absent=%s  wrongly-placed=%s",
                        _evict_attempt + 1, MAX_EVICT_RETRIES,
                        all_slots_to_evict, absent, wrongly_placed,
                    )
                    # Only stop workers that have confirmed a wrong placement OR
                    # whose target slot is currently occupied (blocking them).
                    # Never stop a worker that is simply "absent" with its target
                    # slot free — it may still be connecting.
                    workers_to_restart = sorted(set(
                        [int(_rx) for _rx in wrongly_placed]
                        + [int(_rx) for _rx in absent if int(_rx) in live_now]
                    ))
                    # Keep correction targeted. A full desired-set restart here can evict
                    # already-correct fixed receivers and allow external AUTO clients to
                    # reclaim slots before the restarted workers reconnect.
                    # --- PARALLEL STOP (critical for clean nuclear cleanup) ---
                    # Sequential stop_worker(join=3s) leaves workers N+1..7 running
                    # with stop_event=False for up to 8×3=24 s while we join each
                    # worker one-by-one.  Those live workers can spawn fresh
                    # kiwirecorder connections that race to Kiwi just as they are
                    # killed, creating zombie entries that appear in /users right at
                    # probe time (after a ~5–15 s VPNKit propagation delay).
                    # Fix: set ALL stop_events first (non-blocking) so every thread
                    # immediately stops retrying, then kill all kiwirecorder processes
                    # in one pass, then join threads.
                    _evict_stop_workers = [
                        self._workers.pop(int(_rx), None) for _rx in workers_to_restart
                    ]
                    _evict_stop_workers = [_w2 for _w2 in _evict_stop_workers if _w2 is not None]
                    for _w2 in _evict_stop_workers:   # 1. signal all threads to stop
                        _w2._stop_event.set()
                    for _w2 in _evict_stop_workers:   # 2. kill all processes simultaneously
                        try:
                            _w2._terminate_proc()
                        except Exception:
                            pass
                    for _w2 in _evict_stop_workers:   # 3. join (fast: procs dead, stop_events set)
                        try:
                            _w2.join(timeout=1.0)
                        except Exception:
                            pass
                    # Give Kiwi a moment to see the disconnections
                    if workers_to_restart:
                        time.sleep(0.1)
                    # Kick ghost slots and occupied target slots
                    if all_slots_to_evict:
                        self._run_admin_kick_all(
                            host=str(host),
                            port=int(port),
                            kick_only_slots=all_slots_to_evict,
                        )
                    # Cleanup must stay scoped to the workers we are about to restart.
                    # A global pkill here can tear down already-correct fixed receivers
                    # and recreate the exact AUTO_* churn this targeted correction pass
                    # is trying to remove.
                    if all_slots_to_evict:
                        _restart_labels = {
                            str(getattr(_w2, "_active_user_label", "") or "").strip()
                            for _w2 in _evict_stop_workers
                        }
                        _restart_labels.update(
                            _label
                            for _rx in workers_to_restart
                            for _label in self._expected_user_label_aliases(assignments[int(_rx)])
                        )
                        _restart_labels.update(
                            str(getattr(self._workers.get(int(_rx)), "_active_user_label", "") or "").strip()
                            for _rx in workers_to_restart
                        )
                        _restart_labels = {
                            _label for _label in _restart_labels if _label
                        }
                        if _restart_labels:
                            self._cleanup_orphan_processes_for_labels(_restart_labels)
                            self._wait_for_kiwi_auto_users_missing(
                                host=str(host),
                                port=int(port),
                                labels=_restart_labels,
                                timeout_s=8.0,
                            )
                        time.sleep(0.5)
                    # Restart workers sequentially using P-probe rotation.
                    #
                    # The KiwiSDR assigns slots from a persistent round-robin pointer P,
                    # ignoring --rx-chan.  We detect P by starting rx0 as a probe first:
                    #   - If probe lands in slot 0 (P==0): continue rx1..7 in order.
                    #   - If probe lands in slot K (K≠0): kick the probe, then start
                    #     workers in rotation [rx_{K+1}..rx_{K-1}] which fills slots
                    #     K+1..K-1 and slot 0 in correct associations.  Then wait for
                    #     the probe's VPN-lag on slot K to clear and start rx_K last.
                    # Only apply the rotation when ALL 8 workers are being restarted
                    # (partial restarts fall back to simple sorted order).
                    def _ev_start_one(the_rx: int, probe: bool = False) -> Optional[int]:
                        """Start worker for the_rx, poll 15 s for stable slot; return slot or None.

                        For non-probe workers (probe=False) the target slot is the_rx itself
                        (P-rotation guarantees each rx lands in its matching slot), so we
                        look only at that specific slot.  This prevents shared-label
                        workers (e.g. two 20 m FT8 peers) from spoofing each other.

                        The freshness filter (age ≤ elapsed + 5 s) rejects ghost connections
                        that survived a failed SIGKILL attempt in a prior eviction pass.

                        Non-probe workers get up to 3 attempts: on each timeout the target
                        slot is re-kicked and we wait for it to actually clear before
                        connecting again.  This handles VPNKit/Docker NAT zombie connections
                        that grab the target slot immediately after a single kick.
                        """
                        _r = assignments[int(the_rx)]
                        _MAX_TRIES = 1 if probe else 3
                        _lv_raw: dict = {}
                        for _try_n in range(_MAX_TRIES):
                            _w = self._make_worker(
                                host=str(host), port=int(port), assignment=_r,
                                rx_chan_adjust=0, ignore_slot_check=True,
                            )
                            self._workers[int(the_rx)] = _w
                            # For rotation (non-probe) workers: kick the target slot and
                            # wait until it actually disappears from /users before connecting.
                            # Re-kick every 3 s if the slot remains occupied; give up after
                            # 8 s and proceed anyway to avoid stalling indefinitely on a
                            # truly persistent VPNKit zombie.
                            if not probe:
                                _pre_kick_slot = int(the_rx)
                                _clear_deadline = time.time() + 8.0
                                _last_rekick_t: float = 0.0
                                while True:
                                    _pre_lv = self._fetch_live_auto_users(str(host), int(port))
                                    if _pre_kick_slot not in _pre_lv:
                                        break  # slot is free — connect immediately
                                    if time.time() - _last_rekick_t >= 3.0:
                                        logger.info(
                                            "_ev_start_one rx=%d (try %d/%d): kicking occupied"
                                            " target slot %d (%s) before connect",
                                            int(the_rx), _try_n + 1, _MAX_TRIES,
                                            _pre_kick_slot, _pre_lv[_pre_kick_slot],
                                        )
                                        try:
                                            self._run_admin_kick_all(
                                                host=str(host), port=int(port),
                                                kick_only_slots=[_pre_kick_slot],
                                            )
                                        except Exception:
                                            pass
                                        _last_rekick_t = time.time()
                                    if time.time() >= _clear_deadline:
                                        break  # gave up waiting; start anyway
                                    time.sleep(0.3)
                            _w.start()
                            _lbl = self._expected_user_label(_r)
                            _lbls = self._expected_user_label_aliases(_r)
                            _target_slot = None if probe else int(the_rx)
                            logger.info(
                                "_ev_start_one rx=%d probe=%s lbl=%s target_slot=%s",
                                int(the_rx), probe, _lbl, _target_slot,
                            )
                            _poll_start = time.time()
                            _dl = _poll_start + 15.0
                            _ss: Optional[int] = None
                            _st: float = 0.0
                            while time.time() < _dl:
                                _lv_raw = self._fetch_live_users_with_age(str(host), int(port))
                                _elapsed = time.time() - _poll_start
                                _max_age = _elapsed + 5.0  # allow 5 s grace above elapsed
                                if _target_slot is not None:
                                    # Slot-targeted: only check our exact target slot
                                    _entry = _lv_raw.get(_target_slot)
                                    _sc: Optional[int] = None
                                    if _entry is not None:
                                        _el, _ea = _entry
                                        _lbl_ok = self._label_matches_any(_lbls, _el)
                                        _age_ok = (_ea is None or _ea <= _max_age)
                                        if _lbl_ok and _age_ok:
                                            _sc = _target_slot
                                        elif not _lbl_ok:
                                            logger.debug(
                                                "_ev_start_one rx=%d slot=%d: label mismatch expected=%s got=%s",
                                                int(the_rx), _target_slot, _lbl, _el,
                                            )
                                        elif not _age_ok:
                                            logger.debug(
                                                "_ev_start_one rx=%d slot=%d: age rejected ea=%.1fs max=%.1fs",
                                                int(the_rx), _target_slot, _ea or -1, _max_age,
                                            )
                                else:
                                    # Probe: any slot is fine (we are discovering P)
                                    _sc = next(
                                        (
                                            int(_s)
                                            for _s, (_el, _ea) in _lv_raw.items()
                                            if self._label_matches_any(_lbls, _el)
                                            and (_ea is None or _ea <= _max_age)
                                        ),
                                        None,
                                    )
                                if _sc is not None and _sc == _ss:
                                    if time.time() - _st >= 2.0:
                                        break
                                else:
                                    if _sc is not None and _ss is None:
                                        # First detection — log full /users state
                                        logger.info(
                                            "_ev_start_one rx=%d FIRST_CONNECT slot=%d; /users: %s",
                                            int(the_rx), _sc,
                                            {s: f"{lbl}@{age}s" for s, (lbl, age) in _lv_raw.items()},
                                        )
                                    _ss = _sc
                                    _st = time.time()
                                time.sleep(0.4)
                            if _ss is not None:
                                return _ss
                            logger.info(
                                "_ev_start_one rx=%d TIMED OUT after 15s (try %d/%d); "
                                "last /users state: %s",
                                int(the_rx), _try_n + 1, _MAX_TRIES,
                                {s: f"{lbl}@{age}s" for s, (lbl, age) in _lv_raw.items()},
                            )
                            # Stop the timed-out worker immediately so it no longer
                            # retries in the background and recreates ghost AUTO users.
                            _timed_out_w = self._workers.pop(int(the_rx), None)
                            if _timed_out_w is not None:
                                self._stop_worker(_timed_out_w, join_timeout_s=0.5)
                            self._activity_by_rx.pop(int(the_rx), None)
                            _stale_slots = sorted(
                                {
                                    int(_slot)
                                    for _slot, (_label, _age) in _lv_raw.items()
                                    if self._label_matches_any(_lbls, _label)
                                    or int(_slot) == int(the_rx)
                                }
                            )
                            if _stale_slots:
                                try:
                                    self._run_admin_kick_all(
                                        host=str(host),
                                        port=int(port),
                                        kick_only_slots=_stale_slots,
                                    )
                                except Exception:
                                    pass
                                self._wait_for_kiwi_auto_users_missing(
                                    host=str(host),
                                    port=int(port),
                                    labels=_lbls,
                                    timeout_s=3.0,
                                )
                            if _try_n < _MAX_TRIES - 1:
                                time.sleep(1.0)  # brief pause before retry
                        return None

                    _w2r_set = set(int(_r) for _r in workers_to_restart)
                    _desired_set = set(int(_r) for _r in desired_rxs)
                    _full_restart = (_w2r_set == _desired_set == set(range(8)))
                    _probe_kick_at: Optional[float] = None
                    _deferred_rx_K: Optional[int] = None

                    if _full_restart and workers_to_restart:
                        _probe_rx = sorted(workers_to_restart)[0]  # = rx0
                        _probe_slot = _ev_start_one(_probe_rx, probe=True)
                        logger.info(
                            "Eviction loop P-probe rx=%d: got slot=%s (expected %d)",
                            _probe_rx, _probe_slot, int(_probe_rx),
                        )
                        if _probe_slot is not None:
                            # Post-probe stability wait: VPNKit may complete in-flight TCP
                            # handshakes after our parallel stop, creating zombie connections
                            # that appear in Kiwi's /users ~10–15 s after the kill.  Probe
                            # stays alive (blocking slot K) while we kick non-probe slots.
                            # 20 s is enough to clear fresh VPNKit zombies; persistent older
                            # zombies are handled by the per-worker retry in _ev_start_one
                            # (up to 3 attempts with individual re-kicks per target slot).
                            _ppsw_start = time.time()
                            _ppsw_stable_since: Optional[float] = None
                            _ppsw_last_kick_t: float = 0.0
                            while time.time() - _ppsw_start < 20.0:
                                _ppsw_lv = self._fetch_live_auto_users(str(host), int(port))
                                _ppsw_others = {
                                    s: lbl for s, lbl in _ppsw_lv.items()
                                    if s != int(_probe_slot)
                                }
                                if not _ppsw_others:
                                    if _ppsw_stable_since is None:
                                        _ppsw_stable_since = time.time()
                                        logger.info(
                                            "Post-probe stability: /users clear (only probe at slot %d)",
                                            _probe_slot,
                                        )
                                    elif time.time() - _ppsw_stable_since >= 5.0:
                                        break  # probe alone for 5 stable seconds
                                else:
                                    if _ppsw_stable_since is not None:
                                        logger.info(
                                            "Post-probe stability: extra connections appeared: %s",
                                            _ppsw_others,
                                        )
                                    _ppsw_stable_since = None
                                    # Kick non-probe slots to accelerate fresh-zombie clearance
                                    _now_k2 = time.time()
                                    if _now_k2 - _ppsw_last_kick_t >= 5.0:
                                        try:
                                            self._run_admin_kick_all(
                                                host=str(host), port=int(port),
                                                kick_only_slots=sorted(_ppsw_others.keys()),
                                            )
                                        except Exception:
                                            pass
                                        _ppsw_last_kick_t = _now_k2
                                time.sleep(0.5)
                            else:
                                logger.info(
                                    "Post-probe stability: timed out after 20s; "
                                    "remaining /users: %s — proceeding anyway (per-worker retry active)",
                                    {s: lbl for s, lbl in
                                     self._fetch_live_auto_users(str(host), int(port)).items()
                                     if s != int(_probe_slot)},
                                )
                        if _probe_slot is None:
                            # Probe timed out; stop probe, restart all in sorted order
                            logger.warning("Eviction loop P-probe timed out; fallback to sorted restart")
                            self._stop_worker(self._workers.pop(int(_probe_rx), None), join_timeout_s=0.3)
                            for _rx2 in sorted(workers_to_restart):
                                _ev_start_one(_rx2)
                        elif _probe_slot == int(_probe_rx):
                            # P == 0, probe landed correctly; continue with the rest
                            for _rx2 in sorted(set(workers_to_restart) - {_probe_rx}):
                                _ev_start_one(_rx2)
                        else:
                            # P == K ≠ 0: keep probe ALIVE to block slot K, then rotate.
                            #
                            # Kiwi may "gap fill" a freshly freed slot (K) before strictly
                            # following its round-robin pointer P.  If we kick the probe
                            # before the rotation workers connect, the first new worker often
                            # lands in slot K (filling the gap) rather than slot K+1 (P).
                            # Keeping the probe alive at slot K blocks the gap so each
                            # rotation worker gets the expected sequential slot K+1, K+2…
                            # After all 7 rotation workers have connected (using up 7 of the
                            # 7 remaining free slots), stop the probe to free slot K, then
                            # start rx_K which naturally connects to the now-free slot K.
                            #
                            # IMPORTANT: the rotation loop calls _ev_start_one(rx0) which
                            # overwrites self._workers[0] (the probe) with the new rx0
                            # rotation worker.  Save the probe reference BEFORE the loop so
                            # we can stop the probe (not the rotation worker) at the end.
                            _K = int(_probe_slot)
                            _probe_worker_ref = self._workers.get(int(_probe_rx))
                            _rot = [(_K + 1 + j) % 8 for j in range(7)]
                            for _rx2 in _rot:
                                _ev_start_one(_rx2)
                            # All 7 non-K workers placed; now free slot K for rx_K.
                            # Use the saved reference to stop the PROBE, not the new
                            # rx0 rotation worker that _ev_start_one(rx0) put in
                            # self._workers[0].
                            self._stop_worker(_probe_worker_ref, join_timeout_s=0.3)
                            _probe_kick_at = time.time()
                            _deferred_rx_K = _K  # rx_K fills newly freed slot K
                    else:
                        # Partial restart or empty: sorted order (best effort)
                        for _rx2 in sorted(workers_to_restart):
                            _ev_start_one(_rx2)

                    # Deferred rx_K: probe slot K was freed by SIGKILL→RST in <1s.
                    # Wait a small safety margin before starting rx_K in case RST
                    # propagation through the VPN takes a few extra seconds.
                    if _deferred_rx_K is not None and _probe_kick_at is not None:
                        _remaining_vlan = max(0.0, 5.0 - (time.time() - _probe_kick_at))
                        if _remaining_vlan > 0.5:
                            logger.info(
                                "Eviction loop: waiting %.1fs for VPN lag on slot %d "
                                "to clear before starting rx%d",
                                _remaining_vlan, _deferred_rx_K, _deferred_rx_K,
                            )
                            time.sleep(_remaining_vlan)
                        _ev_start_one(int(_deferred_rx_K))

            self._startup_eviction_active.clear()
            self._mismatch_global_streak = 0  # reset streak; slots are now correct

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
