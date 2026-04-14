from __future__ import annotations

import json
from pathlib import Path

from kiwi_scan.net_monitor import NetMonitorService


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, dict[int, object], bool]] = []
        self.kick_calls: list[tuple[str, int, tuple[int, ...], bool]] = []
        self.wait_clear_calls: list[tuple[str, int, tuple[int, ...], float, float]] = []

    def apply_assignments(
        self,
        host: str,
        port: int,
        assignments: dict[int, object],
        *,
        allow_starting_from_empty_full_reset: bool = True,
    ) -> None:
        self.calls.append((host, port, dict(assignments), bool(allow_starting_from_empty_full_reset)))

    def _run_admin_kick_all(
        self,
        *,
        host: str,
        port: int,
        force_all: bool = False,
        kick_only_slots: list[int] | None = None,
        allow_fallback_kick_all: bool = True,
    ) -> bool:
        del force_all
        self.kick_calls.append(
            (
                host,
                port,
                tuple(int(slot) for slot in (kick_only_slots or [])),
                bool(allow_fallback_kick_all),
            )
        )
        return True

    def _wait_for_kiwi_slots_clear(
        self,
        *,
        host: str,
        port: int,
        slots: set[int] | list[int],
        stable_secs: float = 2.0,
        timeout_s: float = 8.0,
    ) -> bool:
        normalized = tuple(sorted(int(slot) for slot in slots))
        self.wait_clear_calls.append((host, port, normalized, float(stable_secs), float(timeout_s)))
        return True


class _AutoSetLoopStub:
    def __init__(self) -> None:
        self.pause_calls: list[str] = []
        self.resume_calls: list[str] = []

    def pause_for_external(self, reason: str) -> None:
        self.pause_calls.append(str(reason))

    def resume_from_external(self, reason: str) -> None:
        self.resume_calls.append(str(reason))


class _InlineThread:
    def __init__(self, target) -> None:
        self._target = target
        self._alive = False

    def start(self) -> None:
        self._alive = True
        try:
            self._target()
        finally:
            self._alive = False

    def join(self, timeout: float | None = None) -> None:
        return None


def test_net_monitor_start_collects_rolling_history_and_summary(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = NetMonitorService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    sample_map = {
        14.295: (14.2914, 2.2, 0.149, 1806.5),
        14.300: (14.2945, 1.83, 0.129, 1677.4),
        14.305: (14.3014, 1.14, 0.088, 2017.6),
        14.310: (14.3045, 2.67, 0.161, 1665.7),
    }

    def _fake_run_scan(**kwargs):
        center_freq_mhz = round(float(kwargs["center_freq_hz"]) / 1e6, 3)
        peak_freq_mhz, rel_db, voice_score, occ_bw_hz = sample_map[center_freq_mhz]
        report_path = Path(kwargs["json_report_path"])
        events_path = Path(kwargs["jsonl_events_path"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "peak": {
                        "freq_mhz": peak_freq_mhz,
                        "rel_db": rel_db,
                        "s_est": -0.5,
                        "voice_score": voice_score,
                        "occ_bw_hz": occ_bw_hz,
                        "occ_frac": 0.25,
                    },
                    "frames_seen": 10,
                    "ssb_seen_good": voice_score >= 0.12,
                    "stop_reason": "max_frames",
                }
            ),
            encoding="utf-8",
        )
        events_path.write_text(
            json.dumps(
                {
                    "freq_mhz": peak_freq_mhz,
                    "rel_db": rel_db,
                    "s_est": -0.5,
                    "voice_score": voice_score,
                    "occ_bw_hz": occ_bw_hz,
                    "occ_frac": 0.25,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr("kiwi_scan.net_monitor.run_scan", _fake_run_scan)

    result = service.start(host="kiwi.local", port=8073, password=None, max_cycles=1)

    assert result["ok"] is True
    status = service.status()
    assert status["running"] is False
    assert status["cycle_count"] == 1
    assert status["target_freqs_mhz"] == [14.295, 14.3, 14.305, 14.31]
    assert len(status["history"]) == 4
    assert [item["center_freq_mhz"] for item in status["last_cycle"]] == [14.295, 14.3, 14.305, 14.31]
    assert status["caption_candidate"]["center_freq_mhz"] == 14.31
    assert receiver_mgr.kick_calls[:2] == [
        ("kiwi.local", 8073, (0,), False),
        ("kiwi.local", 8073, (1,), False),
    ]
    assert len(receiver_mgr.kick_calls) == 6
    assert auto_set_loop.pause_calls == ["net_monitor"]
    assert auto_set_loop.resume_calls == ["net_monitor"]


def test_net_monitor_capture_records_candidate_audio(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = NetMonitorService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    def _fake_run_record(req):
        req.out_dir.mkdir(parents=True, exist_ok=True)
        (req.out_dir / "caption_capture.wav").write_bytes(b"RIFF")
        return req.out_dir

    def _fake_transcribe_audio(audio_path, *, model_name):
        assert Path(audio_path).name == "caption_capture.wav"
        assert model_name == service.TRANSCRIBE_MODEL_NAME
        return {
            "audio_path": str(audio_path),
            "model": model_name,
            "audio_variant": "speech_enhanced",
            "used_preprocessed_audio": True,
            "attempt_count": 2,
            "language": "en",
            "language_probability": 0.99,
            "duration_s": 5.0,
            "segment_count": 2,
            "text": "Net control, good evening.",
            "segments": [
                {"start_s": 0.0, "end_s": 2.5, "text": "Net control,"},
                {"start_s": 2.5, "end_s": 5.0, "text": "good evening."},
            ],
            "attempts": [
                {
                    "audio_variant": "raw",
                    "beam_size": 1,
                    "best_of": 1,
                    "vad_filter": True,
                    "language": "en",
                    "language_probability": 0.99,
                    "duration_s": 5.0,
                    "segment_count": 0,
                    "text": "",
                },
                {
                    "audio_variant": "speech_enhanced",
                    "beam_size": 5,
                    "best_of": 5,
                    "vad_filter": False,
                    "language": "en",
                    "language_probability": 0.99,
                    "duration_s": 5.0,
                    "segment_count": 2,
                    "text": "Net control, good evening.",
                },
            ],
        }

    monkeypatch.setattr("kiwi_scan.net_monitor.run_record", _fake_run_record)
    monkeypatch.setattr("kiwi_scan.net_monitor.transcribe_audio", _fake_transcribe_audio)
    service._caption_candidate = {
        "center_freq_mhz": 14.295,
        "last_peak_freq_mhz": 14.2895,
        "capture_freq_mhz": 14.2895,
        "label": "14.295 MHz",
    }

    result = service.capture(host="kiwi.local", port=8073, password=None, duration_s=5)

    assert result["ok"] is True
    status = service.status()
    assert status["capture"]["status"] == "complete"
    assert status["capture"]["duration_s"] == 5
    assert status["capture"]["source_freq_mhz"] == 14.295
    assert status["capture"]["capture_freq_mhz"] == 14.2895
    assert status["capture"]["wav_path"].endswith("caption_capture.wav")
    assert status["capture"]["transcription"]["status"] == "complete"
    assert status["capture"]["transcription"]["audio_variant"] == "speech_enhanced"
    assert status["capture"]["transcription"]["used_preprocessed_audio"] is True
    assert status["capture"]["transcription"]["attempt_count"] == 2
    assert status["capture"]["transcription"]["language"] == "en"
    assert status["capture"]["transcription"]["segment_count"] == 2
    assert status["capture"]["transcription"]["text"] == "Net control, good evening."
    assert status["capture"]["transcription"]["text_path"].endswith("caption_capture.txt")
    assert status["capture"]["transcription"]["json_path"].endswith("caption_capture.json")
    assert Path(status["capture"]["transcription"]["text_path"]).read_text(encoding="utf-8").strip() == "Net control, good evening."
    transcript_json = json.loads(Path(status["capture"]["transcription"]["json_path"]).read_text(encoding="utf-8"))
    assert transcript_json["audio_variant"] == "speech_enhanced"
    assert transcript_json["used_preprocessed_audio"] is True
    assert transcript_json["attempt_count"] == 2
    assert len(transcript_json["attempts"]) == 2
    assert transcript_json["text"] == "Net control, good evening."
    assert transcript_json["segment_count"] == 2
    assert receiver_mgr.kick_calls[:2] == [
        ("kiwi.local", 8073, (0,), False),
        ("kiwi.local", 8073, (1,), False),
    ]
    assert receiver_mgr.kick_calls[-1] == ("kiwi.local", 8073, (1,), False)
    assert auto_set_loop.pause_calls == ["net_monitor"]
    assert auto_set_loop.resume_calls == ["net_monitor"]


def test_net_monitor_status_returns_when_lock_is_busy(tmp_path: Path) -> None:
    service = NetMonitorService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )
    service._capture = {
        **service._capture,
        "status": "recording",
        "running": True,
        "capture_freq_mhz": 14.3045,
        "source_freq_mhz": 14.31,
    }

    assert service._lock.acquire(timeout=0.1) is True
    try:
        status = service.status()
    finally:
        service._lock.release()

    assert status["ok"] is True
    assert status["status"] == "capturing"
    assert status["capture"]["running"] is True
