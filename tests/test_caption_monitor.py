from __future__ import annotations

import json
from pathlib import Path

from kiwi_scan.caption_monitor import CaptionMonitorService


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self.kick_calls: list[tuple[str, int, tuple[int, ...], bool]] = []
        self.wait_clear_calls: list[tuple[str, int, tuple[int, ...], float, float]] = []

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

    def start(self) -> None:
        self._target()

    def join(self, timeout: float | None = None) -> None:
        return None


def test_caption_monitor_records_lsb_chunks_for_7179(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = CaptionMonitorService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    record_calls: list[object] = []

    def _fake_run_record(req):
        record_calls.append(req)
        req.out_dir.mkdir(parents=True, exist_ok=True)
        (req.out_dir / f"caption_{len(record_calls):02d}.wav").write_bytes(b"RIFF")
        return req.out_dir

    def _fake_transcribe_audio(audio_path, *, model_name):
        index = len(record_calls)
        return {
            "audio_path": str(audio_path),
            "model": model_name,
            "audio_variant": "raw",
            "used_preprocessed_audio": False,
            "attempt_count": 1,
            "language": "en",
            "language_probability": 0.99,
            "duration_s": 5.0,
            "segment_count": 1,
            "text": f"chunk {index} text",
            "segments": [{"start_s": 0.0, "end_s": 5.0, "text": f"chunk {index} text"}],
            "attempts": [],
        }

    monkeypatch.setattr("kiwi_scan.caption_monitor.run_record", _fake_run_record)
    monkeypatch.setattr("kiwi_scan.caption_monitor.transcribe_audio", _fake_transcribe_audio)

    result = service.start(
        host="kiwi.local",
        port=8073,
        password=None,
        freq_khz=7179.0,
        rx_chan=3,
        chunk_duration_s=5,
        max_chunks=2,
    )

    assert result["ok"] is True
    status = service.status()
    assert status["running"] is False
    assert status["sideband"] == "LSB"
    assert status["chunk_count"] == 2
    assert status["aggregate_text"] == "chunk 1 text\nchunk 2 text"
    assert len(status["history"]) == 2
    assert status["latest_entry"]["text"] == "chunk 2 text"
    assert status["capture"]["transcription"]["status"] == "complete"
    assert Path(status["latest_entry"]["text_path"]).read_text(encoding="utf-8").strip() == "chunk 2 text"
    transcript_json = json.loads(Path(status["latest_entry"]["json_path"]).read_text(encoding="utf-8"))
    assert transcript_json["text"] == "chunk 2 text"
    assert len(record_calls) == 2
    assert [req.mode for req in record_calls] == ["lsb", "lsb"]
    assert [req.freq_hz for req in record_calls] == [7179000.0, 7179000.0]
    assert [req.rx_chan for req in record_calls] == [3, 3]
    assert receiver_mgr.kick_calls == [
        ("kiwi.local", 8073, (3,), False),
        ("kiwi.local", 8073, (3,), False),
    ]
    assert auto_set_loop.pause_calls == ["caption_monitor"]
    assert auto_set_loop.resume_calls == ["caption_monitor"]


def test_caption_monitor_status_returns_when_lock_is_busy(tmp_path: Path) -> None:
    service = CaptionMonitorService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )
    service._capture = {
        **service._capture,
        "status": "recording",
        "running": True,
        "freq_khz": 7179.0,
        "sideband": "LSB",
    }

    assert service._lock.acquire(timeout=0.1) is True
    try:
        status = service.status()
    finally:
        service._lock.release()

    assert status["ok"] is True
    assert status["status"] == "capturing"
    assert status["capture"]["running"] is True