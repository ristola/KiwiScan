from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .auto_set_loop import AutoSetLoop, _FIXED_ASSIGNMENTS
from .record import RecordRequest, RecorderUnavailable, run_record
from .receiver_manager import ReceiverAssignment
from .scan import run_scan
from .transcribe import (
    DEFAULT_TRANSCRIBE_MODEL_NAME,
    TranscriberUnavailable,
    transcribe_audio,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NetMonitorProfile:
    name: str
    band: str
    mode_label: str
    target_freqs_mhz: tuple[float, ...]
    span_hz: float
    threshold_db: float
    min_width_hz: float
    voice_min_score: float
    max_frames: int
    cycle_sleep_s: float
    history_limit: int


class NetMonitorService:
    DEFAULT_PROFILE = "20m-net"
    PROFILES: dict[str, NetMonitorProfile] = {
        "20m-net": NetMonitorProfile(
            name="20m-net",
            band="20m",
            mode_label="20m NET",
            target_freqs_mhz=(14.295, 14.300, 14.305, 14.310),
            span_hz=12_000.0,
            threshold_db=6.0,
            min_width_hz=1_000.0,
            voice_min_score=0.12,
            max_frames=12,
            cycle_sleep_s=2.0,
            history_limit=120,
        ),
    }
    HOLD_REASON = "net_monitor"
    RESERVED_RECEIVERS = (0, 1)
    TARGET_RX_CHAN = 1
    STATUS_HOLD_S = 1.5
    STATUS_LOCK_TIMEOUT_S = 0.25
    OCC_THRESH_DB = 5.0
    CAPTURE_DURATION_S = 20
    TRANSCRIBE_MODEL_NAME = DEFAULT_TRANSCRIBE_MODEL_NAME

    @classmethod
    def normalize_profile(cls, profile_name: object, *, fallback: str | None = None) -> str | None:
        profile_text = str(profile_name or "").strip().lower()
        for candidate in cls.PROFILES:
            if candidate.lower() == profile_text:
                return candidate
        fallback_text = str(fallback or "").strip().lower()
        for candidate in cls.PROFILES:
            if candidate.lower() == fallback_text:
                return candidate
        return None

    def _current_profile(self) -> NetMonitorProfile:
        profile_key = self.normalize_profile(getattr(self, "_profile_name", self.DEFAULT_PROFILE), fallback=self.DEFAULT_PROFILE)
        return self.PROFILES[profile_key or self.DEFAULT_PROFILE]

    @property
    def profile_name(self) -> str:
        return self._current_profile().name

    @property
    def band(self) -> str:
        return self._current_profile().band

    @property
    def mode_label(self) -> str:
        return self._current_profile().mode_label

    @property
    def target_freqs_mhz(self) -> list[float]:
        return [round(float(freq_mhz), 3) for freq_mhz in self._current_profile().target_freqs_mhz]

    @classmethod
    def _idle_transcription_state(cls) -> dict[str, Any]:
        return {
            "status": "idle",
            "running": False,
            "model": cls.TRANSCRIBE_MODEL_NAME,
            "audio_variant": "raw",
            "used_preprocessed_audio": False,
            "attempt_count": 0,
            "language": None,
            "language_probability": None,
            "duration_s": None,
            "segment_count": 0,
            "text": "",
            "text_path": None,
            "json_path": None,
            "started_ts": None,
            "finished_ts": None,
            "summary": "No transcript generated yet",
        }

    @classmethod
    def _idle_capture_state(cls) -> dict[str, Any]:
        return {
            "status": "idle",
            "running": False,
            "duration_s": 0,
            "source_freq_mhz": None,
            "capture_freq_mhz": None,
            "recording_path": None,
            "wav_path": None,
            "started_ts": None,
            "finished_ts": None,
            "summary": "No caption audio captured yet",
            "transcription": cls._idle_transcription_state(),
        }

    @staticmethod
    def _copy_capture_state(capture: dict[str, Any]) -> dict[str, Any]:
        snapshot = dict(capture)
        transcription = snapshot.get("transcription")
        if isinstance(transcription, dict):
            snapshot["transcription"] = dict(transcription)
        return snapshot

    def __init__(
        self,
        *,
        receiver_mgr: object,
        auto_set_loop: AutoSetLoop | None = None,
        output_root: Path | None = None,
    ) -> None:
        self._receiver_mgr = receiver_mgr
        self._auto_set_loop = auto_set_loop
        self._output_root = output_root or (Path(__file__).resolve().parents[2] / "outputs" / "net_monitor")
        self._lock = threading.Lock()
        self._profile_name = self.DEFAULT_PROFILE
        self._thread: threading.Thread | None = None
        self._capture_thread: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._activating = False
        self._mode_active = False
        self._running = False
        self._last_error: str | None = None
        self._last_started_ts: float | None = None
        self._last_finished_ts: float | None = None
        self._last_cycle_started_ts: float | None = None
        self._last_cycle_finished_ts: float | None = None
        self._cycle_count = 0
        self._current_freq_mhz: float | None = None
        self._current_note = "Idle"
        self._session_id: str | None = None
        self._last_report_path: str | None = None
        self._last_report_paths: list[str] = []
        self._threshold_db = float(self._current_profile().threshold_db)
        self._cycle_sleep_s = float(self._current_profile().cycle_sleep_s)
        self._max_cycles = 0
        self._history: list[dict[str, Any]] = []
        self._last_cycle: list[dict[str, Any]] = []
        self._summary: list[dict[str, Any]] = []
        self._caption_candidate: dict[str, Any] | None = None
        self._capture: dict[str, Any] = self._idle_capture_state()

    def _spawn_thread(self, *, name: str, target: Callable[[], None]) -> threading.Thread:
        return threading.Thread(name=name, target=target, daemon=True)

    def _build_fixed_assignments(self) -> dict[int, ReceiverAssignment]:
        assignments: dict[int, ReceiverAssignment] = {}
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            assignments[rx] = ReceiverAssignment(
                rx=rx,
                band=str(entry["band"]),
                freq_hz=float(entry["freq_hz"]),
                mode_label=str(entry["mode"]),
                ignore_slot_check=True,
            )
        return assignments

    def _clear_reserved_slot(self, *, host: str, port: int, rx_chan: int) -> None:
        kick = getattr(self._receiver_mgr, "_run_admin_kick_all", None)
        wait_clear = getattr(self._receiver_mgr, "_wait_for_kiwi_slots_clear", None)

        if callable(kick):
            try:
                kick(
                    host=host,
                    port=int(port),
                    kick_only_slots=[int(rx_chan)],
                    allow_fallback_kick_all=False,
                )
            except Exception:
                logger.exception("NET Monitor failed clearing reserved RX%s", int(rx_chan))

        if callable(wait_clear):
            try:
                wait_clear(
                    host=host,
                    port=int(port),
                    slots={int(rx_chan)},
                    stable_secs=0.75,
                    timeout_s=4.0,
                )
            except Exception:
                logger.exception("NET Monitor failed waiting for reserved RX%s clear", int(rx_chan))

    def _clear_reserved_slots(self, *, host: str, port: int) -> None:
        for rx_chan in self.RESERVED_RECEIVERS:
            self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(rx_chan))

    def _enter_mode(self, *, host: str, port: int) -> None:
        if self._auto_set_loop is not None:
            self._auto_set_loop.pause_for_external(self.HOLD_REASON)
        self._clear_reserved_slots(host=host, port=int(port))
        assignments = self._build_fixed_assignments()
        self._receiver_mgr.apply_assignments(  # type: ignore[attr-defined]
            host,
            int(port),
            assignments,
            allow_starting_from_empty_full_reset=False,
        )
        with self._lock:
            self._mode_active = True

    def _leave_mode(self) -> None:
        should_resume = False
        with self._lock:
            if self._mode_active:
                should_resume = True
            self._mode_active = False
        if should_resume and self._auto_set_loop is not None:
            self._auto_set_loop.resume_from_external(self.HOLD_REASON)

    def _state_label_locked(self) -> str:
        if self._activating:
            return "stopping" if self._stop_requested.is_set() else "starting"
        if self._running and self._stop_requested.is_set():
            return "stopping"
        if bool(self._capture.get("running")):
            capture_status = str(self._capture.get("status") or "").strip().lower()
            return "transcribing" if capture_status == "transcribing" else "capturing"
        if self._running:
            return "running"
        if self._mode_active:
            return "ready"
        return "idle"

    def _status_payload(self) -> dict[str, Any]:
        history = [dict(item) for item in self._history[-24:]][::-1]
        capture = self._copy_capture_state(self._capture)
        return {
            "ok": True,
            "status": self._state_label_locked(),
            "activating": bool(self._activating),
            "mode_active": bool(self._mode_active),
            "running": bool(self._running),
            "stop_requested": bool(self._stop_requested.is_set()),
            "profile": self.profile_name,
            "band": self.band,
            "mode_label": self.mode_label,
            "target_freqs_mhz": list(self.target_freqs_mhz),
            "window_range_mhz": {
                "start": float(min(self.target_freqs_mhz)),
                "end": float(max(self.target_freqs_mhz)),
            },
            "rx_chan": int(self.TARGET_RX_CHAN),
            "threshold_db": float(self._threshold_db),
            "cycle_sleep_s": float(self._cycle_sleep_s),
            "max_cycles": int(self._max_cycles),
            "cycle_count": int(self._cycle_count),
            "current_freq_mhz": self._current_freq_mhz,
            "current_note": self._current_note,
            "session_id": self._session_id,
            "last_report_path": self._last_report_path,
            "last_report_paths": list(self._last_report_paths),
            "last_error": self._last_error,
            "last_started_ts": self._last_started_ts,
            "last_finished_ts": self._last_finished_ts,
            "last_cycle_started_ts": self._last_cycle_started_ts,
            "last_cycle_finished_ts": self._last_cycle_finished_ts,
            "last_cycle": [dict(item) for item in self._last_cycle],
            "history": history,
            "summary": [dict(item) for item in self._summary],
            "caption_candidate": dict(self._caption_candidate) if isinstance(self._caption_candidate, dict) else None,
            "capture": capture,
        }

    def status(self) -> dict[str, Any]:
        acquired = self._lock.acquire(timeout=float(self.STATUS_LOCK_TIMEOUT_S))
        if acquired:
            try:
                return self._status_payload()
            finally:
                self._lock.release()

        logger.warning("NET Monitor status lock busy; returning best-effort snapshot")
        return self._status_payload()

    def start(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        profile_name: str | None = None,
        threshold_db: float | None = None,
        cycle_sleep_s: float | None = None,
        max_cycles: int | None = None,
    ) -> dict[str, Any]:
        selected_profile = self.normalize_profile(profile_name, fallback=self.profile_name)
        if selected_profile is None:
            payload = self.status()
            payload["ok"] = False
            payload["status"] = "error"
            payload["last_error"] = f"Unsupported NET monitor profile: {profile_name}"
            return payload

        profile = self.PROFILES[selected_profile]
        resolved_threshold_db = float(threshold_db) if threshold_db is not None else float(profile.threshold_db)
        resolved_cycle_sleep_s = max(0.0, float(cycle_sleep_s) if cycle_sleep_s is not None else float(profile.cycle_sleep_s))
        resolved_max_cycles = max(0, int(max_cycles or 0))

        already_active = False
        with self._lock:
            if self._running or self._activating:
                already_active = True
            else:
                self._profile_name = selected_profile
                self._threshold_db = resolved_threshold_db
                self._cycle_sleep_s = resolved_cycle_sleep_s
                self._max_cycles = resolved_max_cycles
                self._activating = True
                self._stop_requested.clear()
                self._running = False
                self._last_error = None
                self._last_started_ts = time.time()
                self._last_finished_ts = None
                self._last_cycle_started_ts = None
                self._last_cycle_finished_ts = None
                self._cycle_count = 0
                self._current_freq_mhz = None
                self._current_note = f"Activating {profile.mode_label} monitor"
                self._session_id = time.strftime("net_monitor_%Y%m%d_%H%M%S")
                self._last_report_path = None
                self._last_report_paths = []
                self._history = []
                self._last_cycle = []
                self._summary = []
                self._caption_candidate = None

        if already_active:
            payload = self.status()
            payload["ok"] = False
            return payload

        thread = self._spawn_thread(
            name="net-monitor",
            target=lambda: self._activate_and_monitor(
                host=host,
                port=int(port),
                password=password,
            ),
        )
        with self._lock:
            self._thread = thread
        thread.start()
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            running = bool(self._running)
            activating = bool(self._activating)
        if not running and not activating:
            return self.status()
        self._stop_requested.set()
        payload = self.status()
        payload["status"] = "stopping"
        return payload

    def deactivate(self, *, wait_timeout_s: float = 8.0) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            capture_thread = self._capture_thread
            active = bool(self._running or self._activating)
            capture_running = bool(self._capture.get("running"))
            if active:
                self._stop_requested.set()

        if active and thread is not None:
            thread.join(timeout=max(0.0, float(wait_timeout_s)))
        if capture_running and capture_thread is not None:
            capture_thread.join(timeout=max(0.0, float(wait_timeout_s)))

        return self.status()

    def capture(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        duration_s: int | None = None,
        freq_mhz: float | None = None,
    ) -> dict[str, Any]:
        resolved_duration_s = max(1, int(duration_s or self.CAPTURE_DURATION_S))
        with self._lock:
            if self._running or self._activating:
                payload = self.status()
                payload["ok"] = False
                payload["status"] = "busy"
                payload["last_error"] = "Stop NET Monitor before capturing candidate audio"
                return payload
            if bool(self._capture.get("running")):
                payload = self.status()
                payload["ok"] = False
                return payload

            candidate = dict(self._caption_candidate) if isinstance(self._caption_candidate, dict) else None
            if freq_mhz is not None:
                source_freq_mhz = float(freq_mhz)
                capture_freq_mhz = float(freq_mhz)
            elif candidate is not None:
                source_freq_mhz = float(candidate.get("center_freq_mhz") or 0.0)
                capture_freq_mhz = float(
                    candidate.get("capture_freq_mhz")
                    or candidate.get("last_peak_freq_mhz")
                    or candidate.get("center_freq_mhz")
                    or 0.0
                )
            else:
                payload = self.status()
                payload["ok"] = False
                payload["status"] = "error"
                payload["last_error"] = "No caption candidate is available yet"
                return payload

            if capture_freq_mhz <= 0.0:
                payload = self.status()
                payload["ok"] = False
                payload["status"] = "error"
                payload["last_error"] = "No valid caption frequency is available for capture"
                return payload

            session_id = self._session_id or time.strftime("net_monitor_%Y%m%d_%H%M%S")
            capture_dir = self._output_root / session_id / "caption_audio"
            summary = f"Recording {resolved_duration_s}s of caption audio on {capture_freq_mhz:.3f} MHz"
            self._capture = {
                "status": "recording",
                "running": True,
                "duration_s": int(resolved_duration_s),
                "source_freq_mhz": float(source_freq_mhz),
                "capture_freq_mhz": float(capture_freq_mhz),
                "recording_path": str(capture_dir),
                "wav_path": None,
                "started_ts": time.time(),
                "finished_ts": None,
                "summary": summary,
                "transcription": {
                    **self._idle_transcription_state(),
                    "summary": f"Transcript will start after the {resolved_duration_s}s capture completes",
                },
            }
            self._current_note = summary

        thread = self._spawn_thread(
            name="net-monitor-capture",
            target=lambda: self._capture_audio(
                host=host,
                port=int(port),
                password=password,
                duration_s=int(resolved_duration_s),
                source_freq_mhz=float(source_freq_mhz),
                capture_freq_mhz=float(capture_freq_mhz),
                capture_dir=capture_dir,
            ),
        )
        with self._lock:
            self._capture_thread = thread
        thread.start()
        return self.status()

    def _activate_and_monitor(self, *, host: str, port: int, password: str | None) -> None:
        try:
            logger.info("NET Monitor activation starting on %s:%s", host, port)
            self._enter_mode(host=host, port=port)
        except Exception as exc:
            logger.exception("NET Monitor activation failed")
            self._leave_mode()
            with self._lock:
                self._activating = False
                self._running = False
                self._thread = None
                self._last_error = f"NET Monitor activation failed: {exc}"
                self._last_finished_ts = time.time()
                self._current_note = self._last_error
            self._stop_requested.clear()
            return

        with self._lock:
            stop_requested = bool(self._stop_requested.is_set())
            self._activating = False
            if stop_requested:
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                self._current_note = "NET Monitor stopped before probes began"
            else:
                self._running = True
                self._current_note = f"Monitoring {self.mode_label} on RX{int(self.TARGET_RX_CHAN)}"

        if stop_requested:
            self._stop_requested.clear()
            self._leave_mode()
            return

        try:
            while not self._stop_requested.is_set():
                with self._lock:
                    cycle_index = int(self._cycle_count) + 1
                    self._last_cycle_started_ts = time.time()
                    self._current_note = f"Cycle {cycle_index}: probing {self.mode_label}"

                cycle_results = self._run_cycle(
                    cycle_index=cycle_index,
                    host=host,
                    port=port,
                    password=password,
                )

                with self._lock:
                    self._cycle_count = cycle_index
                    self._last_cycle = [dict(item) for item in cycle_results]
                    self._summary = self._build_summary_locked()
                    self._caption_candidate = self._select_caption_candidate_locked()
                    self._last_cycle_finished_ts = time.time()
                    self._current_freq_mhz = None
                    self._current_note = (
                        f"Cycle {cycle_index} complete"
                        if cycle_results
                        else f"Cycle {cycle_index} complete with no samples"
                    )
                    report_paths = [str(item.get("report_path")) for item in cycle_results if item.get("report_path")]
                    self._last_report_paths = report_paths
                    self._last_report_path = report_paths[-1] if report_paths else None

                self._write_session_summary(self._session_id)

                if self._max_cycles > 0 and cycle_index >= self._max_cycles:
                    break
                if self._stop_requested.is_set():
                    break
                self._sleep_until_next_cycle()
        except Exception as exc:
            logger.exception("NET Monitor loop failed")
            with self._lock:
                self._last_error = f"NET Monitor failed: {exc}"
                self._current_note = self._last_error
        finally:
            with self._lock:
                self._running = False
                self._thread = None
                self._activating = False
                self._current_freq_mhz = None
                self._last_finished_ts = time.time()
                if self._stop_requested.is_set():
                    self._current_note = "NET Monitor stopped"
            self._stop_requested.clear()
            self._leave_mode()

    def _capture_audio(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        duration_s: int,
        source_freq_mhz: float,
        capture_freq_mhz: float,
        capture_dir: Path,
    ) -> None:
        wav_path: Path | None = None
        capture_summary: str | None = None
        capture_finished_ts: float | None = None
        transcription_started_ts: float | None = None
        try:
            self._enter_mode(host=host, port=port)
            self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(self.TARGET_RX_CHAN))
            run_record(
                RecordRequest(
                    host=host,
                    port=int(port),
                    password=password,
                    user="NET Monitor Caption Capture",
                    freq_hz=float(capture_freq_mhz) * 1e6,
                    rx_chan=int(self.TARGET_RX_CHAN),
                    duration_s=int(duration_s),
                    mode="usb",
                    out_dir=capture_dir,
                )
            )
            wav_path = self._latest_wav_path(capture_dir)
            if wav_path is None:
                raise FileNotFoundError("Caption capture completed but no WAV file was found")

            capture_summary = (
                f"Captured {int(duration_s)}s on {capture_freq_mhz:.3f} MHz"
                if round(float(source_freq_mhz), 3) == round(float(capture_freq_mhz), 3)
                else f"Captured {int(duration_s)}s on {capture_freq_mhz:.3f} MHz from {source_freq_mhz:.3f} MHz candidate"
            )
            capture_finished_ts = time.time()
            transcription_started_ts = time.time()
            with self._lock:
                self._capture = {
                    **self._capture,
                    "status": "transcribing",
                    "running": True,
                    "wav_path": str(wav_path),
                    "finished_ts": capture_finished_ts,
                    "summary": f"{capture_summary}. Starting transcript",
                    "transcription": {
                        **self._idle_transcription_state(),
                        "status": "running",
                        "running": True,
                        "model": self.TRANSCRIBE_MODEL_NAME,
                        "started_ts": transcription_started_ts,
                        "summary": f"Transcribing {wav_path.name} with {self.TRANSCRIBE_MODEL_NAME}",
                    },
                }
                self._current_note = self._capture["summary"]

            transcript = transcribe_audio(
                wav_path,
                model_name=self.TRANSCRIBE_MODEL_NAME,
            )
            text_path, json_path = self._write_transcript_artifacts(wav_path=wav_path, transcript=transcript)
            transcript_text = str(transcript.get("text") or "").strip()
            segment_count = int(transcript.get("segment_count") or 0)
            language = str(transcript.get("language") or "unknown")
            audio_variant = str(transcript.get("audio_variant") or "raw")
            used_preprocessed_audio = bool(transcript.get("used_preprocessed_audio"))
            attempt_count = max(1, int(transcript.get("attempt_count") or 1))
            transcript_summary = (
                f"Transcript ready ({language}, {segment_count} segment{'s' if segment_count != 1 else ''})"
                + (" after speech cleanup" if used_preprocessed_audio else "")
                if transcript_text
                else (
                    f"Transcript completed with no recognizable speech after {attempt_count} attempts"
                    if attempt_count > 1
                    else "Transcript completed with no recognizable speech"
                )
            )
            with self._lock:
                self._capture = {
                    **self._capture,
                    "status": "complete",
                    "running": False,
                    "wav_path": str(wav_path),
                    "finished_ts": capture_finished_ts,
                    "summary": capture_summary,
                    "transcription": {
                        "status": "complete",
                        "running": False,
                        "model": str(transcript.get("model") or self.TRANSCRIBE_MODEL_NAME),
                        "audio_variant": audio_variant,
                        "used_preprocessed_audio": used_preprocessed_audio,
                        "attempt_count": attempt_count,
                        "language": transcript.get("language"),
                        "language_probability": transcript.get("language_probability"),
                        "duration_s": transcript.get("duration_s"),
                        "segment_count": segment_count,
                        "text": transcript_text,
                        "text_path": str(text_path),
                        "json_path": str(json_path),
                        "started_ts": transcription_started_ts,
                        "finished_ts": time.time(),
                        "summary": transcript_summary,
                    },
                }
                self._current_note = transcript_summary
        except RecorderUnavailable as exc:
            summary = f"Caption capture unavailable: {exc}"
            with self._lock:
                self._capture = {
                    **self._capture,
                    "status": "error",
                    "running": False,
                    "wav_path": str(wav_path) if wav_path is not None else None,
                    "finished_ts": time.time(),
                    "summary": summary,
                    "transcription": {
                        **self._idle_transcription_state(),
                        "status": "error",
                        "summary": "No transcript generated because audio capture failed",
                    },
                }
                self._current_note = summary
        except TranscriberUnavailable as exc:
            summary = f"Transcription unavailable: {exc}"
            with self._lock:
                self._capture = {
                    **self._capture,
                    "status": "complete",
                    "running": False,
                    "wav_path": str(wav_path) if wav_path is not None else None,
                    "finished_ts": capture_finished_ts,
                    "summary": f"{capture_summary}. Transcript unavailable" if capture_summary else summary,
                    "transcription": {
                        **self._idle_transcription_state(),
                        "status": "error",
                        "running": False,
                        "model": self.TRANSCRIBE_MODEL_NAME,
                        "started_ts": transcription_started_ts,
                        "finished_ts": time.time(),
                        "summary": summary,
                    },
                }
                self._current_note = summary
        except Exception as exc:
            if wav_path is not None and capture_summary is not None:
                summary = f"Transcription failed: {type(exc).__name__}: {exc}"
                with self._lock:
                    self._capture = {
                        **self._capture,
                        "status": "complete",
                        "running": False,
                        "wav_path": str(wav_path),
                        "finished_ts": capture_finished_ts,
                        "summary": f"{capture_summary}. Transcript failed",
                        "transcription": {
                            **self._idle_transcription_state(),
                            "status": "error",
                            "running": False,
                            "model": self.TRANSCRIBE_MODEL_NAME,
                            "started_ts": transcription_started_ts,
                            "finished_ts": time.time(),
                            "summary": summary,
                        },
                    }
                    self._current_note = summary
            else:
                summary = f"Caption capture failed: {type(exc).__name__}: {exc}"
                with self._lock:
                    self._capture = {
                        **self._capture,
                        "status": "error",
                        "running": False,
                        "wav_path": str(wav_path) if wav_path is not None else None,
                        "finished_ts": time.time(),
                        "summary": summary,
                        "transcription": {
                            **self._idle_transcription_state(),
                            "status": "error",
                            "summary": "No transcript generated because audio capture failed",
                        },
                    }
                    self._current_note = summary
        finally:
            with self._lock:
                self._capture_thread = None
            self._leave_mode()

    def _run_cycle(
        self,
        *,
        cycle_index: int,
        host: str,
        port: int,
        password: str | None,
    ) -> list[dict[str, Any]]:
        profile = self._current_profile()
        cycle_results: list[dict[str, Any]] = []
        for probe_index, freq_mhz in enumerate(profile.target_freqs_mhz, start=1):
            if self._stop_requested.is_set():
                break
            with self._lock:
                self._current_freq_mhz = float(freq_mhz)
                self._current_note = f"Cycle {cycle_index}: probing {float(freq_mhz):.3f} MHz"

            result = self._scan_frequency(
                cycle_index=cycle_index,
                probe_index=probe_index,
                probe_total=len(profile.target_freqs_mhz),
                freq_mhz=float(freq_mhz),
                host=host,
                port=port,
                password=password,
            )
            result["cycle_index"] = int(cycle_index)
            result["sample_ts"] = time.time()
            cycle_results.append(dict(result))

            with self._lock:
                self._append_history_locked(result)
        return cycle_results

    def _scan_frequency(
        self,
        *,
        cycle_index: int,
        probe_index: int,
        probe_total: int,
        freq_mhz: float,
        host: str,
        port: int,
        password: str | None,
    ) -> dict[str, Any]:
        profile = self._current_profile()
        session_id = self._session_id or time.strftime("net_monitor_%Y%m%d_%H%M%S")
        cycle_dir = self._output_root / session_id / f"cycle_{cycle_index:03d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        freq_tag = f"{freq_mhz:.3f}".replace(".", "_")
        report_path = cycle_dir / f"probe_{probe_index:02d}_{freq_tag}.json"
        hits_path = cycle_dir / f"probe_{probe_index:02d}_{freq_tag}_hits.jsonl"
        events_path = cycle_dir / f"probe_{probe_index:02d}_{freq_tag}_events.jsonl"

        self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(self.TARGET_RX_CHAN))
        rc = run_scan(
            host=host,
            port=int(port),
            password=password,
            user=f"{profile.mode_label} Monitor",
            rx_chan=int(self.TARGET_RX_CHAN),
            band=profile.band,
            center_freq_hz=float(freq_mhz) * 1e6,
            span_hz=float(profile.span_hz),
            threshold_db=float(self._threshold_db),
            min_width_bins=2,
            min_width_hz=float(profile.min_width_hz),
            ssb_detect=True,
            ssb_only=True,
            required_hits=1,
            tolerance_bins=2.5,
            expiry_frames=6,
            max_frames=int(profile.max_frames),
            jsonl_path=hits_path,
            jsonl_events_path=events_path,
            json_report_path=report_path,
            min_s=1.0,
            status_hold_s=float(self.STATUS_HOLD_S),
            max_runtime_s=6.0,
            rx_wait_timeout_s=12.0,
            rx_wait_interval_s=1.0,
            rx_wait_max_retries=12,
            phone_only=True,
            status_modulation="iq",
            status_pre_tune=False,
            status_parallel_snd=True,
            ssb_occ_thresh_db=float(self.OCC_THRESH_DB),
            ssb_voice_min_score=float(profile.voice_min_score),
            ssb_early_stop_frames=0,
            ssb_warmup_frames=2,
            ssb_adaptive_threshold=True,
            ssb_adaptive_min_db=6.0,
            ssb_adaptive_max_db=18.0,
            ssb_adaptive_spread_gain=0.18,
            ssb_adaptive_spread_offset_db=0.0,
            ssb_adaptive_spread_target_db=55.0,
            show=False,
        )
        return self._summarize_probe(
            freq_mhz=freq_mhz,
            probe_index=probe_index,
            probe_total=probe_total,
            rc=rc,
            report_path=report_path,
            events_path=events_path,
        )

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        entries: list[dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                entries.append(item)
        return entries

    @staticmethod
    def _clamp_score(value: float) -> int:
        return int(max(0.0, min(100.0, round(float(value)))))

    def _summarize_probe(
        self,
        *,
        freq_mhz: float,
        probe_index: int,
        probe_total: int,
        rc: int,
        report_path: Path,
        events_path: Path,
    ) -> dict[str, Any]:
        report = self._read_json(report_path)
        events = self._read_jsonl(events_path)
        peak = report.get("peak") if isinstance(report.get("peak"), dict) else {}
        peak_freq_mhz = float(peak.get("freq_mhz")) if isinstance(peak, dict) and peak.get("freq_mhz") is not None else None
        rel_values = [float(item.get("rel_db")) for item in events if item.get("rel_db") is not None]
        s_values = [float(item.get("s_est")) for item in events if item.get("s_est") is not None]
        voice_values = [float(item.get("voice_score")) for item in events if item.get("voice_score") is not None]
        occ_values = [float(item.get("occ_bw_hz")) for item in events if item.get("occ_bw_hz") is not None]
        occ_frac_values = [float(item.get("occ_frac")) for item in events if item.get("occ_frac") is not None]
        max_rel_db = max(rel_values) if rel_values else (
            float(peak.get("rel_db")) if isinstance(peak, dict) and peak.get("rel_db") is not None else None
        )
        best_s_est = max(s_values) if s_values else (
            float(peak.get("s_est")) if isinstance(peak, dict) and peak.get("s_est") is not None else None
        )
        voice_score = max(voice_values) if voice_values else (
            float(peak.get("voice_score")) if isinstance(peak, dict) and peak.get("voice_score") is not None else None
        )
        occupied_bw_hz = max(occ_values) if occ_values else (
            float(peak.get("occ_bw_hz")) if isinstance(peak, dict) and peak.get("occ_bw_hz") is not None else None
        )
        occ_frac = max(occ_frac_values) if occ_frac_values else (
            float(peak.get("occ_frac")) if isinstance(peak, dict) and peak.get("occ_frac") is not None else None
        )
        frames_seen = int(report.get("frames_seen") or 0)
        ssb_seen_good = bool(report.get("ssb_seen_good"))
        stop_reason = report.get("stop_reason")

        if rc == 3:
            return {
                "center_freq_mhz": float(freq_mhz),
                "peak_freq_mhz": peak_freq_mhz,
                "status": "unavailable",
                "score": 0,
                "summary": f"RX{int(self.TARGET_RX_CHAN)} unavailable for this probe",
                "rel_db": max_rel_db,
                "best_s_est": best_s_est,
                "voice_score": voice_score,
                "occupied_bw_hz": occupied_bw_hz,
                "occ_frac": occ_frac,
                "frames_seen": frames_seen,
                "ssb_seen_good": ssb_seen_good,
                "stop_reason": stop_reason,
                "return_code": int(rc),
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
                "report_path": str(report_path),
            }
        if rc != 0:
            return {
                "center_freq_mhz": float(freq_mhz),
                "peak_freq_mhz": peak_freq_mhz,
                "status": "error",
                "score": 0,
                "summary": f"Probe failed with rc={int(rc)}",
                "rel_db": max_rel_db,
                "best_s_est": best_s_est,
                "voice_score": voice_score,
                "occupied_bw_hz": occupied_bw_hz,
                "occ_frac": occ_frac,
                "frames_seen": frames_seen,
                "ssb_seen_good": ssb_seen_good,
                "stop_reason": stop_reason,
                "return_code": int(rc),
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
                "report_path": str(report_path),
            }

        score = self._clamp_score(
            (float(max_rel_db or 0.0) * 4.0)
            + (float(voice_score or 0.0) * 45.0)
            + (min(float(occupied_bw_hz or 0.0), 2800.0) / 90.0)
        )
        status = "activity" if score >= 45 else "watch" if score >= 30 else "quiet"
        summary = (
            f"Speech-like energy voice={float(voice_score or 0.0):.2f}, bw={float(occupied_bw_hz or 0.0):.0f} Hz"
            if score >= 30
            else "No speech-like energy"
        )
        return {
            "center_freq_mhz": float(freq_mhz),
            "peak_freq_mhz": peak_freq_mhz,
            "status": status,
            "score": score,
            "summary": summary,
            "rel_db": max_rel_db,
            "best_s_est": best_s_est,
            "voice_score": voice_score,
            "occupied_bw_hz": occupied_bw_hz,
            "occ_frac": occ_frac,
            "frames_seen": frames_seen,
            "ssb_seen_good": ssb_seen_good,
            "stop_reason": stop_reason,
            "return_code": int(rc),
            "probe_index": int(probe_index),
            "probe_total": int(probe_total),
            "report_path": str(report_path),
        }

    def _append_history_locked(self, result: dict[str, Any]) -> None:
        history_entry = {
            "cycle_index": int(result.get("cycle_index") or 0),
            "sample_ts": float(result.get("sample_ts") or time.time()),
            "center_freq_mhz": result.get("center_freq_mhz"),
            "peak_freq_mhz": result.get("peak_freq_mhz"),
            "status": result.get("status"),
            "score": result.get("score"),
            "summary": result.get("summary"),
            "rel_db": result.get("rel_db"),
            "best_s_est": result.get("best_s_est"),
            "voice_score": result.get("voice_score"),
            "occupied_bw_hz": result.get("occupied_bw_hz"),
            "occ_frac": result.get("occ_frac"),
            "frames_seen": result.get("frames_seen"),
            "ssb_seen_good": result.get("ssb_seen_good"),
            "stop_reason": result.get("stop_reason"),
            "return_code": result.get("return_code"),
            "report_path": result.get("report_path"),
        }
        self._history.append(history_entry)
        history_limit = max(1, int(self._current_profile().history_limit))
        if len(self._history) > history_limit:
            self._history = self._history[-history_limit:]

    def _build_summary_locked(self) -> list[dict[str, Any]]:
        history = [dict(item) for item in self._history]
        summaries: list[dict[str, Any]] = []
        for center_freq_mhz in self.target_freqs_mhz:
            rows = [item for item in history if round(float(item.get("center_freq_mhz") or 0.0), 3) == round(float(center_freq_mhz), 3)]
            if not rows:
                summaries.append(
                    {
                        "center_freq_mhz": float(center_freq_mhz),
                        "samples": 0,
                        "available_samples": 0,
                        "activity_samples": 0,
                        "last_status": "idle",
                        "last_seen_ts": None,
                        "last_peak_freq_mhz": None,
                        "last_rel_db": None,
                        "avg_rel_db": None,
                        "max_rel_db": None,
                        "last_voice_score": None,
                        "avg_voice_score": None,
                        "last_occ_bw_hz": None,
                        "max_occ_bw_hz": None,
                    }
                )
                continue

            latest = rows[-1]
            available_rows = [row for row in rows if str(row.get("status") or "") not in {"unavailable", "error"}]
            rel_values = [float(row.get("rel_db")) for row in available_rows if row.get("rel_db") is not None]
            voice_values = [float(row.get("voice_score")) for row in available_rows if row.get("voice_score") is not None]
            occ_values = [float(row.get("occupied_bw_hz")) for row in available_rows if row.get("occupied_bw_hz") is not None]

            summaries.append(
                {
                    "center_freq_mhz": float(center_freq_mhz),
                    "samples": len(rows),
                    "available_samples": len(available_rows),
                    "activity_samples": sum(1 for row in rows if str(row.get("status") or "") == "activity"),
                    "last_status": latest.get("status") or "idle",
                    "last_seen_ts": latest.get("sample_ts"),
                    "last_peak_freq_mhz": latest.get("peak_freq_mhz"),
                    "last_rel_db": latest.get("rel_db"),
                    "avg_rel_db": (sum(rel_values) / len(rel_values)) if rel_values else None,
                    "max_rel_db": max(rel_values) if rel_values else None,
                    "last_voice_score": latest.get("voice_score"),
                    "avg_voice_score": (sum(voice_values) / len(voice_values)) if voice_values else None,
                    "last_occ_bw_hz": latest.get("occupied_bw_hz"),
                    "max_occ_bw_hz": max(occ_values) if occ_values else None,
                }
            )
        return summaries

    def _select_caption_candidate_locked(self) -> dict[str, Any] | None:
        if not self._summary:
            return None

        ranked = [
            item
            for item in self._summary
            if item.get("avg_rel_db") is not None or item.get("last_rel_db") is not None
        ]
        if not ranked:
            return None

        best = max(
            ranked,
            key=lambda item: (
                float(item.get("avg_rel_db") if item.get("avg_rel_db") is not None else item.get("last_rel_db") or float("-inf")),
                float(item.get("avg_voice_score") if item.get("avg_voice_score") is not None else item.get("last_voice_score") or 0.0),
                int(item.get("activity_samples") or 0),
            ),
        )
        candidate = dict(best)
        candidate["label"] = f"{float(candidate['center_freq_mhz']):.3f} MHz"
        candidate["capture_freq_mhz"] = float(candidate.get("last_peak_freq_mhz") or candidate["center_freq_mhz"])
        return candidate

    def _sleep_until_next_cycle(self) -> None:
        end_ts = time.time() + max(0.0, float(self._cycle_sleep_s))
        while not self._stop_requested.is_set() and time.time() < end_ts:
            remaining = end_ts - time.time()
            with self._lock:
                self._current_note = f"Sleeping {remaining:.1f}s before next cycle"
            time.sleep(min(0.25, max(0.0, remaining)))

    def _write_session_summary(self, session_id: str | None) -> None:
        if not session_id:
            return
        session_dir = self._output_root / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        summary_path = session_dir / "net_monitor_session.json"
        summary_path.write_text(json.dumps(self.status(), sort_keys=True) + "\n", encoding="utf-8")

    @staticmethod
    def _write_transcript_artifacts(wav_path: Path, transcript: dict[str, Any]) -> tuple[Path, Path]:
        text_path = wav_path.with_suffix(".txt")
        json_path = wav_path.with_suffix(".json")
        transcript_text = str(transcript.get("text") or "").strip()
        payload = {
            "wav_path": str(wav_path),
            "model": transcript.get("model"),
            "audio_variant": transcript.get("audio_variant"),
            "used_preprocessed_audio": bool(transcript.get("used_preprocessed_audio")),
            "attempt_count": int(transcript.get("attempt_count") or 0),
            "language": transcript.get("language"),
            "language_probability": transcript.get("language_probability"),
            "duration_s": transcript.get("duration_s"),
            "segment_count": transcript.get("segment_count"),
            "text": transcript_text,
            "segments": list(transcript.get("segments") or []),
            "attempts": list(transcript.get("attempts") or []),
        }
        text_path.write_text((transcript_text + "\n") if transcript_text else "", encoding="utf-8")
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return text_path, json_path

    @staticmethod
    def _latest_wav_path(root: Path) -> Path | None:
        wavs = [path for path in root.glob("*.wav") if path.is_file()]
        if not wavs:
            return None
        return max(wavs, key=lambda path: path.stat().st_mtime)