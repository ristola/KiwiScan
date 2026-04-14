from __future__ import annotations

from functools import lru_cache
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any


logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip().lower()
    if not text:
        return default
    return text not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


DEFAULT_TRANSCRIBE_MODEL_NAME = os.getenv("KIWISCAN_TRANSCRIBE_MODEL", "tiny.en").strip() or "tiny.en"
DEFAULT_TRANSCRIBE_DEVICE = os.getenv("KIWISCAN_TRANSCRIBE_DEVICE", "cpu").strip() or "cpu"
DEFAULT_TRANSCRIBE_COMPUTE_TYPE = os.getenv("KIWISCAN_TRANSCRIBE_COMPUTE_TYPE", "int8").strip() or "int8"
DEFAULT_TRANSCRIBE_LANGUAGE = os.getenv("KIWISCAN_TRANSCRIBE_LANGUAGE", "en").strip() or None
DEFAULT_TRANSCRIBE_ENHANCE_AUDIO = _env_flag("KIWISCAN_TRANSCRIBE_ENHANCE_AUDIO", True)
DEFAULT_TRANSCRIBE_SOX_PATH = os.getenv("KIWISCAN_TRANSCRIBE_SOX_PATH", "sox").strip() or "sox"
DEFAULT_TRANSCRIBE_RETRY_BEAM_SIZE = max(1, _env_int("KIWISCAN_TRANSCRIBE_RETRY_BEAM_SIZE", 5))
DEFAULT_TRANSCRIBE_RETRY_BEST_OF = max(1, _env_int("KIWISCAN_TRANSCRIBE_RETRY_BEST_OF", DEFAULT_TRANSCRIBE_RETRY_BEAM_SIZE))


class TranscriberUnavailable(RuntimeError):
    pass


def _model_cache_root() -> Path:
    return Path(__file__).resolve().parents[2] / "outputs" / "models" / "faster_whisper"


def _resolve_sox_path(sox_path: str) -> str | None:
    candidate = str(sox_path or DEFAULT_TRANSCRIBE_SOX_PATH).strip() or DEFAULT_TRANSCRIBE_SOX_PATH
    if candidate == "sox":
        return shutil.which("sox")
    resolved = shutil.which(candidate)
    if resolved:
        return resolved
    candidate_path = Path(candidate).expanduser()
    if candidate_path.is_file():
        return str(candidate_path.resolve())
    return None


def _speech_enhanced_audio_path(
    audio_path: Path,
    *,
    temp_dir: Path,
    sox_path: str,
) -> Path:
    resolved_sox_path = _resolve_sox_path(sox_path)
    if not resolved_sox_path:
        raise TranscriberUnavailable("sox is unavailable for speech-enhanced transcription retry")

    output_path = temp_dir / f"{audio_path.stem}.speech.wav"
    command = [
        resolved_sox_path,
        str(audio_path),
        "-r",
        "16000",
        "-c",
        "1",
        str(output_path),
        "highpass",
        "180",
        "lowpass",
        "3400",
        "compand",
        "0.02,0.10",
        "6:-70,-60,-20",
        "-5",
        "-90",
        "0.2",
        "gain",
        "-n",
        "-3",
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not output_path.is_file():
        detail = (completed.stderr or completed.stdout or "unknown error").strip()
        raise TranscriberUnavailable(f"speech-enhanced transcription retry failed: {detail}")
    if completed.stderr.strip():
        logger.info("Speech-enhanced transcription audio preparation: %s", completed.stderr.strip())
    return output_path


@lru_cache(maxsize=4)
def _load_model(model_name: str, device: str, compute_type: str, download_root: str):
    try:
        from faster_whisper import WhisperModel
    except Exception as exc:
        raise TranscriberUnavailable(f"faster-whisper is unavailable: {exc}") from exc

    cache_root = Path(download_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    try:
        return WhisperModel(
            model_name,
            device=device,
            compute_type=compute_type,
            download_root=str(cache_root),
        )
    except Exception as exc:
        raise TranscriberUnavailable(
            f"failed to initialize faster-whisper model '{model_name}': {exc}"
        ) from exc


def _transcribe_attempt(
    model: Any,
    audio_path: Path,
    *,
    language: str | None,
    beam_size: int,
    best_of: int,
    vad_filter: bool,
    audio_variant: str,
) -> dict[str, Any]:
    try:
        segment_iter, info = model.transcribe(
            str(audio_path),
            language=language,
            beam_size=int(beam_size),
            best_of=int(best_of),
            condition_on_previous_text=False,
            vad_filter=bool(vad_filter),
        )
        segments: list[dict[str, Any]] = []
        text_chunks: list[str] = []
        for segment in segment_iter:
            segment_text = str(getattr(segment, "text", "") or "").strip()
            start_s = getattr(segment, "start", None)
            end_s = getattr(segment, "end", None)
            segments.append(
                {
                    "start_s": float(start_s) if start_s is not None else None,
                    "end_s": float(end_s) if end_s is not None else None,
                    "text": segment_text,
                }
            )
            if segment_text:
                text_chunks.append(segment_text)
    except Exception:
        logger.exception("Audio transcription failed for %s", audio_path)
        raise

    transcript_text = " ".join(chunk for chunk in text_chunks if chunk).strip()
    duration_s = getattr(info, "duration", None)
    language_probability = getattr(info, "language_probability", None)
    return {
        "audio_variant": str(audio_variant or "raw"),
        "beam_size": int(beam_size),
        "best_of": int(best_of),
        "vad_filter": bool(vad_filter),
        "language": getattr(info, "language", None),
        "language_probability": float(language_probability) if language_probability is not None else None,
        "duration_s": float(duration_s) if duration_s is not None else None,
        "segment_count": len(segments),
        "text": transcript_text,
        "segments": segments,
    }


def transcribe_audio(
    audio_path: str | Path,
    *,
    model_name: str = DEFAULT_TRANSCRIBE_MODEL_NAME,
    language: str | None = DEFAULT_TRANSCRIBE_LANGUAGE,
    device: str = DEFAULT_TRANSCRIBE_DEVICE,
    compute_type: str = DEFAULT_TRANSCRIBE_COMPUTE_TYPE,
    enhance_audio: bool = DEFAULT_TRANSCRIBE_ENHANCE_AUDIO,
    sox_path: str = DEFAULT_TRANSCRIBE_SOX_PATH,
) -> dict[str, Any]:
    resolved_audio_path = Path(audio_path).expanduser().resolve()
    if not resolved_audio_path.is_file():
        raise FileNotFoundError(f"audio file not found: {resolved_audio_path}")

    resolved_model_name = str(model_name or DEFAULT_TRANSCRIBE_MODEL_NAME)
    model = _load_model(
        resolved_model_name,
        str(device or DEFAULT_TRANSCRIBE_DEVICE),
        str(compute_type or DEFAULT_TRANSCRIBE_COMPUTE_TYPE),
        str(_model_cache_root()),
    )

    attempts: list[dict[str, Any]] = []
    first_attempt = _transcribe_attempt(
        model,
        resolved_audio_path,
        language=language,
        beam_size=1,
        best_of=1,
        vad_filter=True,
        audio_variant="raw",
    )
    attempts.append(first_attempt)

    if not first_attempt["text"] and int(first_attempt["segment_count"] or 0) <= 0 and enhance_audio:
        with tempfile.TemporaryDirectory(prefix="kiwiscan_transcribe_") as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            try:
                enhanced_audio_path = _speech_enhanced_audio_path(
                    resolved_audio_path,
                    temp_dir=temp_dir,
                    sox_path=sox_path,
                )
            except TranscriberUnavailable as exc:
                logger.warning(
                    "Speech-enhanced transcription retry skipped for %s: %s",
                    resolved_audio_path,
                    exc,
                )
            else:
                attempts.append(
                    _transcribe_attempt(
                        model,
                        enhanced_audio_path,
                        language=language,
                        beam_size=DEFAULT_TRANSCRIBE_RETRY_BEAM_SIZE,
                        best_of=DEFAULT_TRANSCRIBE_RETRY_BEST_OF,
                        vad_filter=False,
                        audio_variant="speech_enhanced",
                    )
                )

    final_attempt = attempts[-1]
    return {
        "audio_path": str(resolved_audio_path),
        "model": resolved_model_name,
        "language": final_attempt.get("language"),
        "language_probability": final_attempt.get("language_probability"),
        "duration_s": final_attempt.get("duration_s"),
        "segment_count": int(final_attempt.get("segment_count") or 0),
        "text": str(final_attempt.get("text") or ""),
        "segments": list(final_attempt.get("segments") or []),
        "audio_variant": str(final_attempt.get("audio_variant") or "raw"),
        "used_preprocessed_audio": bool(final_attempt.get("audio_variant") == "speech_enhanced"),
        "attempt_count": len(attempts),
        "attempts": [
            {
                "audio_variant": str(attempt.get("audio_variant") or "raw"),
                "beam_size": int(attempt.get("beam_size") or 0),
                "best_of": int(attempt.get("best_of") or 0),
                "vad_filter": bool(attempt.get("vad_filter")),
                "language": attempt.get("language"),
                "language_probability": attempt.get("language_probability"),
                "duration_s": attempt.get("duration_s"),
                "segment_count": int(attempt.get("segment_count") or 0),
                "text": str(attempt.get("text") or ""),
            }
            for attempt in attempts
        ],
    }