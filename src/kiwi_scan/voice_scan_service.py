from __future__ import annotations

import logging
import threading
import time
import wave
from pathlib import Path
from typing import Any

import numpy as np

from .auto_set_loop import AutoSetLoop
from .record import RecordRequest, RecorderUnavailable, run_record
from .voice_activity_detector import VADEngine
from .voice_bands import VoiceBand, get_voice_band

logger = logging.getLogger(__name__)


class VoiceScanService:
    HOLD_REASON = "voice_scan"
    STATUS_LOCK_TIMEOUT_S = 0.25

    def __init__(
        self,
        *,
        receiver_mgr: object,
        auto_set_loop: AutoSetLoop | None = None,
        output_root: Path | None = None,
    ) -> None:
        self._receiver_mgr = receiver_mgr
        self._auto_set_loop = auto_set_loop
        self._output_root = output_root or Path("/tmp/kiwiscan_voice")
        self._output_root.mkdir(parents=True, exist_ok=True)

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

        self._band_name: str | None = None
        self._sideband: str = "LSB"
        self._rx_chan: int = 0
        self._session_id: str | None = None

        self._step_hz: int = 1000
        self._chunk_duration_s: int = 2
        self._stability_wait_s: int = 10
        self._max_memories: int = 5
        self._min_stability_s: float = 2.0
        self._min_snr_db: float = 3.0
        self._min_voice_energy_rms: float = 0.01

        self._scan_start_hz: float | None = None
        self._scan_end_hz: float | None = None
        self._current_freq_hz: float | None = None
        self._frequencies_scanned = 0
        self._memory_channels: list[dict[str, Any]] = []

        self._vad = VADEngine(sample_rate_hz=8000)

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
        if self._running:
            return "running"
        return "idle"

    def _status_payload(self) -> dict[str, Any]:
        with self._lock:
            memory = [dict(item) for item in self._memory_channels]
            band_name = self._band_name
            sideband = self._sideband
            session_id = self._session_id
            start_hz = self._scan_start_hz
            end_hz = self._scan_end_hz
            current_hz = self._current_freq_hz
            scanned = self._frequencies_scanned
            running = self._running
            starting = self._starting
            mode_active = self._mode_active
            note = self._current_note
            last_error = self._last_error
            started_ts = self._last_started_ts
            finished_ts = self._last_finished_ts
            rx_chan = self._rx_chan

        progress_percent = 0
        if start_hz and end_hz and end_hz > start_hz and current_hz:
            progress_percent = int(max(0, min(100, ((current_hz - start_hz) / (end_hz - start_hz)) * 100)))

        return {
            "ok": True,
            "status": self._state_label_locked(),
            "starting": bool(starting),
            "running": bool(running),
            "mode_active": bool(mode_active),
            "stop_requested": bool(self._stop_requested.is_set()),
            "band": band_name,
            "sideband": sideband,
            "rx_chan": int(rx_chan),
            "session_id": session_id,
            "scan_start_khz": round(float(start_hz) / 1000.0, 3) if start_hz else None,
            "scan_end_khz": round(float(end_hz) / 1000.0, 3) if end_hz else None,
            "current_freq_khz": round(float(current_hz) / 1000.0, 3) if current_hz else None,
            "frequencies_scanned": int(scanned),
            "progress_percent": int(progress_percent),
            "memory_channels": memory,
            "memory_count": len(memory),
            "max_memories": int(self._max_memories),
            "current_note": note,
            "last_error": last_error,
            "last_started_ts": started_ts,
            "last_finished_ts": finished_ts,
        }

    def status(self) -> dict[str, Any]:
        acquired = self._lock.acquire(timeout=float(self.STATUS_LOCK_TIMEOUT_S))
        if acquired:
            try:
                return self._status_payload()
            finally:
                self._lock.release()
        logger.warning("Voice scan status lock busy; returning best-effort snapshot")
        return self._status_payload()

    def start(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        band: str,
        sideband: str = "LSB",
        rx_chan: int = 0,
        step_hz: int = 1000,
        chunk_duration_s: int = 2,
        stability_wait_s: int = 10,
        max_memories: int = 5,
    ) -> dict[str, Any]:
        voice_band = get_voice_band(str(band).strip())
        if voice_band is None:
            return {"ok": False, "status": "error", "detail": f"Unsupported voice band: {band}"}

        resolved_sideband = str(sideband or "LSB").strip().upper()
        if resolved_sideband not in {"LSB", "USB"}:
            return {"ok": False, "status": "error", "detail": "sideband must be LSB or USB"}

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
            self._current_note = f"Starting voice scan on {voice_band.name} {resolved_sideband}"

            self._band_name = str(band)
            self._sideband = resolved_sideband
            self._rx_chan = resolved_rx_chan
            self._session_id = time.strftime("voice_scan_%Y%m%d_%H%M%S")

            self._step_hz = max(500, int(step_hz))
            self._chunk_duration_s = max(1, int(chunk_duration_s))
            self._stability_wait_s = max(2, int(stability_wait_s))
            self._max_memories = max(1, min(5, int(max_memories)))

            self._scan_start_hz = float(voice_band.ssb_start_hz)
            self._scan_end_hz = float(voice_band.ssb_end_hz)
            self._current_freq_hz = self._scan_start_hz
            self._frequencies_scanned = 0
            # Keep existing memories until explicitly cleared.

        thread = threading.Thread(
            name="voice-scan",
            target=lambda: self._run_scan(host=host, port=int(port), password=password, voice_band=voice_band),
            daemon=True,
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

    def clear(self) -> dict[str, Any]:
        with self._lock:
            self._memory_channels = []
        return self.status()

    def _run_scan(self, *, host: str, port: int, password: str | None, voice_band: VoiceBand) -> None:
        try:
            self._enter_mode()
            with self._lock:
                self._starting = False
                self._running = True
                self._current_note = (
                    f"Scanning {voice_band.name} {self._sideband} "
                    f"{voice_band.ssb_start_khz():.1f}-{voice_band.ssb_end_khz():.1f} kHz"
                )

            freq_hz = float(voice_band.ssb_start_hz)
            while freq_hz <= float(voice_band.ssb_end_hz):
                if self._stop_requested.is_set():
                    break
                with self._lock:
                    if len(self._memory_channels) >= self._max_memories:
                        break
                    self._current_freq_hz = float(freq_hz)
                    self._frequencies_scanned += 1
                    self._current_note = f"Scanning {freq_hz / 1000.0:.1f} kHz {self._sideband}"

                self._scan_single_frequency(host=host, port=port, password=password, freq_hz=float(freq_hz))
                freq_hz += float(self._step_hz)

            with self._lock:
                if self._stop_requested.is_set():
                    self._current_note = "Voice scan stopped"
                elif len(self._memory_channels) >= self._max_memories:
                    self._current_note = f"Voice scan complete ({len(self._memory_channels)} memories)"
                else:
                    self._current_note = f"Voice scan complete ({len(self._memory_channels)} memories)"
        except Exception as exc:
            logger.exception("Voice scan failed")
            with self._lock:
                self._last_error = f"Voice scan failed: {exc}"
                self._current_note = self._last_error
        finally:
            with self._lock:
                self._starting = False
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
            self._stop_requested.clear()
            self._leave_mode()

    def _scan_single_frequency(self, *, host: str, port: int, password: str | None, freq_hz: float) -> None:
        detection_started_ts: float | None = None
        best_detection: dict[str, Any] | None = None
        best_audio: np.ndarray | None = None
        deadline = time.time() + float(self._stability_wait_s)

        while time.time() < deadline:
            if self._stop_requested.is_set():
                return

            audio, wav_path = self._capture_audio_chunk(
                host=host,
                port=port,
                password=password,
                freq_hz=freq_hz,
            )
            if audio is None:
                return

            detection = self._vad.analyze_chunk(
                audio_chunk=audio,
                frequency_hz=freq_hz,
                sideband=self._sideband,
                chunk_timestamp_s=time.time(),
            )

            is_voice = (
                detection.voice_energy_rms >= self._min_voice_energy_rms
                and detection.snr_db >= self._min_snr_db
                and detection.detection_confidence >= 0.3
            )

            if is_voice:
                if detection_started_ts is None:
                    detection_started_ts = time.time()
                if best_detection is None or detection.detection_confidence > float(best_detection.get("confidence") or 0.0):
                    best_detection = {
                        "voice_energy_rms": float(detection.voice_energy_rms),
                        "carrier_offset_hz": float(detection.carrier_offset_hz),
                        "snr_db": float(detection.snr_db),
                        "confidence": float(detection.detection_confidence),
                        "noise_floor_rms": float(detection.noise_floor_rms),
                        "wav_path": str(wav_path) if wav_path else None,
                    }
                    best_audio = audio

                stable_for_s = time.time() - detection_started_ts
                if stable_for_s >= self._min_stability_s:
                    self._append_memory_channel(
                        freq_hz=freq_hz,
                        detection=best_detection,
                        audio=best_audio,
                    )
                    return

    def _append_memory_channel(self, *, freq_hz: float, detection: dict[str, Any], audio: np.ndarray | None) -> None:
        with self._lock:
            if len(self._memory_channels) >= self._max_memories:
                return
            next_index = len(self._memory_channels) + 1

        clip_path = None
        clip_duration_s = 0.0
        if audio is not None and len(audio) > 0:
            clip_path, clip_duration_s = self._save_audio_clip(freq_hz=freq_hz, audio=audio)

        memory = {
            "memory_index": int(next_index),
            "frequency_hz": float(freq_hz),
            "frequency_khz": round(float(freq_hz) / 1000.0, 3),
            "sideband": self._sideband,
            "voice_energy_rms": float(detection.get("voice_energy_rms") or 0.0),
            "carrier_offset_hz": float(detection.get("carrier_offset_hz") or 0.0),
            "snr_db": float(detection.get("snr_db") or 0.0),
            "confidence": float(detection.get("confidence") or 0.0),
            "noise_floor_rms": float(detection.get("noise_floor_rms") or 0.0),
            "audio_clip_path": str(clip_path) if clip_path else str(detection.get("wav_path") or ""),
            "audio_duration_s": float(clip_duration_s),
            "detected_at_unix": time.time(),
        }

        with self._lock:
            if len(self._memory_channels) >= self._max_memories:
                return
            self._memory_channels.append(memory)
            self._current_note = (
                f"Found voice memory {memory['memory_index']} at {memory['frequency_khz']:.3f} kHz "
                f"({memory['snr_db']:.1f} dB)"
            )

    def _capture_audio_chunk(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        freq_hz: float,
    ) -> tuple[np.ndarray | None, Path | None]:
        session_id = self._session_id or time.strftime("voice_scan_%Y%m%d_%H%M%S")
        ts = int(time.time() * 1000)
        chunk_dir = self._output_root / session_id / f"f_{int(freq_hz)}_{ts}"

        try:
            run_record(
                RecordRequest(
                    host=host,
                    port=int(port),
                    password=password,
                    user="Voice Scan",
                    freq_hz=float(freq_hz),
                    rx_chan=int(self._rx_chan),
                    duration_s=int(self._chunk_duration_s),
                    mode=self._sideband.lower(),
                    out_dir=chunk_dir,
                )
            )
            wav_path = self._latest_wav_path(chunk_dir)
            if wav_path is None:
                raise FileNotFoundError("Voice scan capture completed but no WAV file was found")
            audio = self._read_wav_pcm_int16(wav_path)
            return audio, wav_path
        except RecorderUnavailable as exc:
            with self._lock:
                self._last_error = f"Voice scan unavailable: {exc}"
                self._current_note = self._last_error
            return None, None
        except Exception as exc:
            logger.warning("Voice scan capture failed at %.1f kHz: %s", freq_hz / 1000.0, exc)
            return None, None

    @staticmethod
    def _latest_wav_path(folder: Path) -> Path | None:
        wav_files = sorted(folder.glob("*.wav"), key=lambda path: path.stat().st_mtime, reverse=True)
        return wav_files[0] if wav_files else None

    @staticmethod
    def _read_wav_pcm_int16(path: Path) -> np.ndarray:
        with wave.open(str(path), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            nframes = wav_file.getnframes()
            raw = wav_file.readframes(nframes)

        if sample_width != 2:
            raise ValueError(f"Unsupported sample width: {sample_width * 8} bit")

        pcm = np.frombuffer(raw, dtype=np.int16)
        if channels > 1:
            pcm = pcm[::channels]
        return pcm

    def _save_audio_clip(self, *, freq_hz: float, audio: np.ndarray, sample_rate_hz: int = 8000) -> tuple[Path | None, float]:
        session_id = self._session_id or "voice_scan"
        filename = f"memory_{int(freq_hz)}_{int(time.time())}.wav"
        out_dir = self._output_root / session_id / "memories"
        out_dir.mkdir(parents=True, exist_ok=True)
        filepath = out_dir / filename

        try:
            audio_int16 = audio.astype(np.int16, copy=False)
            with wave.open(str(filepath), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sample_rate_hz)
                wav_file.writeframes(audio_int16.tobytes())
            duration_s = len(audio_int16) / float(sample_rate_hz)
            return filepath, duration_s
        except Exception:
            logger.exception("Failed to save memory audio clip")
            return None, 0.0
