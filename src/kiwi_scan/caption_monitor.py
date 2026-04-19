from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .auto_set_loop import AutoSetLoop
from .record import RecordRequest, RecorderUnavailable, run_record
from .transcribe import DEFAULT_TRANSCRIBE_MODEL_NAME, TranscriberUnavailable, transcribe_audio
from .voice_mode import resolve_voice_sideband


logger = logging.getLogger(__name__)


class CaptionMonitorService:
    HOLD_REASON = "caption_monitor"
    STATUS_LOCK_TIMEOUT_S = 0.25
    DEFAULT_CHUNK_DURATION_S = 15
    DEFAULT_HISTORY_LIMIT = 120
    TRANSCRIBE_MODEL_NAME = DEFAULT_TRANSCRIBE_MODEL_NAME

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
            "chunk_index": 0,
            "freq_khz": None,
            "sideband": None,
            "duration_s": 0,
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
        self._output_root = output_root or (Path(__file__).resolve().parents[2] / "outputs" / "caption_monitor")
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._starting = False
        self._running = False
        self._mode_active = False
        self._last_error: str | None = None
        self._last_started_ts: float | None = None
        self._last_finished_ts: float | None = None
        self._current_note = "Idle"
        self._session_id: str | None = None
        self._session_path: str | None = None
        self._freq_khz: float | None = None
        self._sideband: str | None = None
        self._rx_chan = 0
        self._chunk_duration_s = int(self.DEFAULT_CHUNK_DURATION_S)
        self._max_chunks = 0
        self._chunk_count = 0
        self._history: list[dict[str, Any]] = []
        self._capture: dict[str, Any] = self._idle_capture_state()

    def _spawn_thread(self, *, name: str, target: Callable[[], None]) -> threading.Thread:
        return threading.Thread(name=name, target=target, daemon=True)

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
                logger.exception("Caption monitor failed clearing RX%s", int(rx_chan))

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
                logger.exception("Caption monitor failed waiting for RX%s clear", int(rx_chan))

    def _enter_mode(self) -> None:
        if self._auto_set_loop is not None:
            self._auto_set_loop.pause_for_external(self.HOLD_REASON)
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
        if self._starting:
            return "stopping" if self._stop_requested.is_set() else "starting"
        if self._running and self._stop_requested.is_set():
            return "stopping"
        if bool(self._capture.get("running")):
            capture_status = str(self._capture.get("status") or "").strip().lower()
            return "transcribing" if capture_status == "transcribing" else "capturing"
        if self._running:
            return "running"
        return "idle"

    def _status_payload(self) -> dict[str, Any]:
        capture = self._copy_capture_state(self._capture)
        history = [dict(item) for item in self._history[-24:]][::-1]
        aggregate_text = "\n".join(
            str(item.get("text") or "").strip()
            for item in self._history
            if str(item.get("text") or "").strip()
        )
        latest_entry = dict(self._history[-1]) if self._history else None
        return {
            "ok": True,
            "status": self._state_label_locked(),
            "starting": bool(self._starting),
            "running": bool(self._running),
            "mode_active": bool(self._mode_active),
            "stop_requested": bool(self._stop_requested.is_set()),
            "freq_khz": self._freq_khz,
            "freq_mhz": round(float(self._freq_khz) / 1000.0, 3) if self._freq_khz is not None else None,
            "sideband": self._sideband,
            "rx_chan": int(self._rx_chan),
            "chunk_duration_s": int(self._chunk_duration_s),
            "max_chunks": int(self._max_chunks),
            "chunk_count": int(self._chunk_count),
            "session_id": self._session_id,
            "session_path": self._session_path,
            "current_note": self._current_note,
            "last_error": self._last_error,
            "last_started_ts": self._last_started_ts,
            "last_finished_ts": self._last_finished_ts,
            "latest_entry": latest_entry,
            "history": history,
            "aggregate_text": aggregate_text,
            "capture": capture,
        }

    def status(self) -> dict[str, Any]:
        acquired = self._lock.acquire(timeout=float(self.STATUS_LOCK_TIMEOUT_S))
        if acquired:
            try:
                return self._status_payload()
            finally:
                self._lock.release()

        logger.warning("Caption monitor status lock busy; returning best-effort snapshot")
        return self._status_payload()

    def start(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        freq_khz: float,
        sideband: str | None = None,
        rx_chan: int = 0,
        chunk_duration_s: int | None = None,
        max_chunks: int | None = None,
    ) -> dict[str, Any]:
        resolved_sideband = resolve_voice_sideband(float(freq_khz), sideband)
        resolved_chunk_duration_s = max(1, int(chunk_duration_s or self.DEFAULT_CHUNK_DURATION_S))
        resolved_max_chunks = max(0, int(max_chunks or 0))
        resolved_rx_chan = max(0, int(rx_chan))

        with self._lock:
            if self._running or self._starting:
                payload = self._status_payload()
                payload["ok"] = False
                payload["status"] = "busy"
                return payload

            self._starting = True
            self._running = False
            self._stop_requested.clear()
            self._last_error = None
            self._last_started_ts = time.time()
            self._last_finished_ts = None
            self._current_note = f"Starting captions on {float(freq_khz):.2f} kHz {resolved_sideband}"
            self._session_id = time.strftime("caption_monitor_%Y%m%d_%H%M%S")
            self._session_path = str(self._output_root / self._session_id)
            self._freq_khz = float(freq_khz)
            self._sideband = resolved_sideband
            self._rx_chan = resolved_rx_chan
            self._chunk_duration_s = resolved_chunk_duration_s
            self._max_chunks = resolved_max_chunks
            self._chunk_count = 0
            self._history = []
            self._capture = self._idle_capture_state()

        thread = self._spawn_thread(
            name="caption-monitor",
            target=lambda: self._run_session(
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
            active = bool(self._running or self._starting)
        if not active:
            return self.status()
        self._stop_requested.set()
        payload = self.status()
        payload["status"] = "stopping"
        return payload

    def deactivate(self, *, wait_timeout_s: float = 20.0) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            active = bool(self._running or self._starting)
            if active:
                self._stop_requested.set()

        if active and thread is not None:
            thread.join(timeout=max(0.0, float(wait_timeout_s)))
        return self.status()

    def _run_session(self, *, host: str, port: int, password: str | None) -> None:
        try:
            self._enter_mode()
            with self._lock:
                self._starting = False
                self._running = True
                self._current_note = (
                    f"Capturing {self._chunk_duration_s}s chunks on {float(self._freq_khz or 0.0):.2f} kHz {self._sideband}"
                )

            while not self._stop_requested.is_set():
                chunk_index = int(self._chunk_count) + 1
                self._capture_chunk(
                    host=host,
                    port=port,
                    password=password,
                    chunk_index=chunk_index,
                )
                with self._lock:
                    self._chunk_count = chunk_index
                self._write_session_summary(self._session_id)

                if self._max_chunks > 0 and chunk_index >= self._max_chunks:
                    break
                if self._stop_requested.is_set():
                    break
        except Exception as exc:
            logger.exception("Caption monitor failed")
            with self._lock:
                self._last_error = f"Caption monitor failed: {exc}"
                self._current_note = self._last_error
        finally:
            with self._lock:
                self._starting = False
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                if self._stop_requested.is_set():
                    self._current_note = "Caption monitor stopped"
            self._stop_requested.clear()
            self._write_session_summary(self._session_id)
            self._leave_mode()

    def _capture_chunk(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        chunk_index: int,
    ) -> None:
        freq_khz = float(self._freq_khz or 0.0)
        sideband = str(self._sideband or "USB")
        duration_s = int(self._chunk_duration_s)
        rx_chan = int(self._rx_chan)
        session_id = self._session_id or time.strftime("caption_monitor_%Y%m%d_%H%M%S")
        chunk_dir = self._output_root / session_id / f"chunk_{chunk_index:03d}"
        summary = f"Recording chunk {chunk_index} on {freq_khz:.2f} kHz {sideband}"

        with self._lock:
            self._capture = {
                "status": "recording",
                "running": True,
                "chunk_index": int(chunk_index),
                "freq_khz": freq_khz,
                "sideband": sideband,
                "duration_s": duration_s,
                "recording_path": str(chunk_dir),
                "wav_path": None,
                "started_ts": time.time(),
                "finished_ts": None,
                "summary": summary,
                "transcription": {
                    **self._idle_transcription_state(),
                    "summary": f"Transcript will start after chunk {chunk_index} capture completes",
                },
            }
            self._current_note = summary

        wav_path: Path | None = None
        capture_finished_ts: float | None = None
        transcription_started_ts: float | None = None
        entry: dict[str, Any] | None = None
        try:
            self._clear_reserved_slot(host=host, port=port, rx_chan=rx_chan)
            run_record(
                RecordRequest(
                    host=host,
                    port=int(port),
                    password=password,
                    user="Caption Monitor",
                    freq_hz=freq_khz * 1000.0,
                    rx_chan=rx_chan,
                    duration_s=duration_s,
                    mode=sideband.lower(),
                    out_dir=chunk_dir,
                )
            )
            wav_path = self._latest_wav_path(chunk_dir)
            if wav_path is None:
                raise FileNotFoundError("Caption capture completed but no WAV file was found")

            capture_finished_ts = time.time()
            transcription_started_ts = time.time()
            with self._lock:
                self._capture = {
                    **self._capture,
                    "status": "transcribing",
                    "running": True,
                    "wav_path": str(wav_path),
                    "finished_ts": capture_finished_ts,
                    "summary": f"Captured chunk {chunk_index}. Starting transcript",
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
            attempt_count = max(1, int(transcript.get("attempt_count") or 1))
            used_preprocessed_audio = bool(transcript.get("used_preprocessed_audio"))
            language = str(transcript.get("language") or "unknown")
            transcript_summary = (
                f"Chunk {chunk_index} transcript ready ({language}, {segment_count} segment{'s' if segment_count != 1 else ''})"
                + (" after speech cleanup" if used_preprocessed_audio else "")
                if transcript_text
                else (
                    f"Chunk {chunk_index} completed with no recognizable speech after {attempt_count} attempts"
                    if attempt_count > 1
                    else f"Chunk {chunk_index} completed with no recognizable speech"
                )
            )
            entry = {
                "chunk_index": int(chunk_index),
                "freq_khz": freq_khz,
                "freq_mhz": round(freq_khz / 1000.0, 3),
                "sideband": sideband,
                "duration_s": duration_s,
                "wav_path": str(wav_path),
                "text_path": str(text_path),
                "json_path": str(json_path),
                "language": transcript.get("language"),
                "segment_count": segment_count,
                "audio_variant": transcript.get("audio_variant"),
                "used_preprocessed_audio": used_preprocessed_audio,
                "attempt_count": attempt_count,
                "text": transcript_text,
                "started_ts": self._capture.get("started_ts"),
                "finished_ts": time.time(),
                "summary": transcript_summary,
            }
            with self._lock:
                self._history.append(entry)
                if len(self._history) > self.DEFAULT_HISTORY_LIMIT:
                    self._history = self._history[-self.DEFAULT_HISTORY_LIMIT :]
                self._capture = {
                    **self._capture,
                    "status": "complete",
                    "running": False,
                    "wav_path": str(wav_path),
                    "finished_ts": capture_finished_ts,
                    "summary": f"Captured chunk {chunk_index}",
                    "transcription": {
                        "status": "complete",
                        "running": False,
                        "model": str(transcript.get("model") or self.TRANSCRIBE_MODEL_NAME),
                        "audio_variant": str(transcript.get("audio_variant") or "raw"),
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
            self._set_capture_error(summary=summary, wav_path=wav_path)
        except TranscriberUnavailable as exc:
            summary = f"Transcription unavailable: {exc}"
            self._set_transcription_error(
                summary=summary,
                chunk_index=chunk_index,
                wav_path=wav_path,
                capture_finished_ts=capture_finished_ts,
                transcription_started_ts=transcription_started_ts,
            )
        except Exception as exc:
            if wav_path is not None:
                summary = f"Transcription failed: {type(exc).__name__}: {exc}"
                self._set_transcription_error(
                    summary=summary,
                    chunk_index=chunk_index,
                    wav_path=wav_path,
                    capture_finished_ts=capture_finished_ts,
                    transcription_started_ts=transcription_started_ts,
                )
            else:
                summary = f"Caption capture failed: {type(exc).__name__}: {exc}"
                self._set_capture_error(summary=summary, wav_path=wav_path)

    def _set_capture_error(self, *, summary: str, wav_path: Path | None) -> None:
        with self._lock:
            self._last_error = summary
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

    def _set_transcription_error(
        self,
        *,
        summary: str,
        chunk_index: int,
        wav_path: Path | None,
        capture_finished_ts: float | None,
        transcription_started_ts: float | None,
    ) -> None:
        with self._lock:
            self._last_error = summary
            self._capture = {
                **self._capture,
                "status": "complete",
                "running": False,
                "wav_path": str(wav_path) if wav_path is not None else None,
                "finished_ts": capture_finished_ts,
                "summary": f"Captured chunk {chunk_index}. Transcript unavailable",
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

    def _write_session_summary(self, session_id: str | None) -> None:
        if not session_id:
            return
        session_dir = self._output_root / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        summary_path = session_dir / "caption_monitor_session.json"
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