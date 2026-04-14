from __future__ import annotations

from pathlib import Path

from kiwi_scan import transcribe as transcribe_module


class _FakeSegment:
    def __init__(self, text: str, start_s: float, end_s: float) -> None:
        self.text = text
        self.start = start_s
        self.end = end_s


class _FakeInfo:
    def __init__(self, *, language: str, language_probability: float, duration: float) -> None:
        self.language = language
        self.language_probability = language_probability
        self.duration = duration


class _FakeModel:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def transcribe(self, audio_path: str, **kwargs):
        audio_name = Path(audio_path).name
        self.calls.append({"audio_name": audio_name, **kwargs})
        response = self._responses[audio_name]
        segments = [
            _FakeSegment(
                text=str(item["text"]),
                start_s=float(item["start_s"]),
                end_s=float(item["end_s"]),
            )
            for item in list(response.get("segments") or [])
        ]
        info_payload = dict(response.get("info") or {})
        info = _FakeInfo(
            language=str(info_payload.get("language") or "en"),
            language_probability=float(info_payload.get("language_probability") or 0.0),
            duration=float(info_payload.get("duration") or 0.0),
        )
        return iter(segments), info


def test_transcribe_audio_retries_with_speech_enhanced_audio_when_raw_is_empty(monkeypatch, tmp_path: Path) -> None:
    audio_path = tmp_path / "sample.wav"
    audio_path.write_bytes(b"RIFF")

    fake_model = _FakeModel(
        {
            "sample.wav": {
                "segments": [],
                "info": {"language": "en", "language_probability": 0.99, "duration": 5.0},
            },
            "sample.speech.wav": {
                "segments": [
                    {"start_s": 0.0, "end_s": 1.2, "text": "Net control ready."},
                ],
                "info": {"language": "en", "language_probability": 0.99, "duration": 5.0},
            },
        }
    )

    monkeypatch.setattr(transcribe_module, "_load_model", lambda *args, **kwargs: fake_model)

    def _fake_speech_enhanced_audio_path(audio_path: Path, *, temp_dir: Path, sox_path: str) -> Path:
        assert sox_path == transcribe_module.DEFAULT_TRANSCRIBE_SOX_PATH
        enhanced_path = temp_dir / f"{audio_path.stem}.speech.wav"
        enhanced_path.write_bytes(b"RIFF")
        return enhanced_path

    monkeypatch.setattr(transcribe_module, "_speech_enhanced_audio_path", _fake_speech_enhanced_audio_path)

    result = transcribe_module.transcribe_audio(audio_path, model_name="tiny.en")

    assert result["model"] == "tiny.en"
    assert result["text"] == "Net control ready."
    assert result["segment_count"] == 1
    assert result["audio_variant"] == "speech_enhanced"
    assert result["used_preprocessed_audio"] is True
    assert result["attempt_count"] == 2
    assert [attempt["audio_variant"] for attempt in result["attempts"]] == ["raw", "speech_enhanced"]
    assert fake_model.calls[0]["audio_name"] == "sample.wav"
    assert fake_model.calls[0]["vad_filter"] is True
    assert fake_model.calls[0]["beam_size"] == 1
    assert fake_model.calls[1]["audio_name"] == "sample.speech.wav"
    assert fake_model.calls[1]["vad_filter"] is False
    assert fake_model.calls[1]["beam_size"] == transcribe_module.DEFAULT_TRANSCRIBE_RETRY_BEAM_SIZE


def test_transcribe_audio_skips_speech_enhanced_retry_when_raw_transcript_exists(monkeypatch, tmp_path: Path) -> None:
    audio_path = tmp_path / "sample.wav"
    audio_path.write_bytes(b"RIFF")

    fake_model = _FakeModel(
        {
            "sample.wav": {
                "segments": [
                    {"start_s": 0.0, "end_s": 0.8, "text": "Good evening."},
                ],
                "info": {"language": "en", "language_probability": 0.97, "duration": 2.0},
            },
        }
    )

    monkeypatch.setattr(transcribe_module, "_load_model", lambda *args, **kwargs: fake_model)

    def _should_not_run(*args, **kwargs):
        raise AssertionError("speech-enhanced retry should not run when raw transcript already has text")

    monkeypatch.setattr(transcribe_module, "_speech_enhanced_audio_path", _should_not_run)

    result = transcribe_module.transcribe_audio(audio_path, model_name="tiny.en")

    assert result["text"] == "Good evening."
    assert result["audio_variant"] == "raw"
    assert result["used_preprocessed_audio"] is False
    assert result["attempt_count"] == 1
    assert len(result["attempts"]) == 1
    assert len(fake_model.calls) == 1