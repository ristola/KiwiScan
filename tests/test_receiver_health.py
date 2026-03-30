from __future__ import annotations

import json
import time
from pathlib import Path

import kiwi_scan.receiver_manager as receiver_manager
from kiwi_scan.receiver_manager import ReceiverAssignment, ReceiverManager


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _make_manager() -> ReceiverManager:
    return ReceiverManager(
        kiwirecorder_path=Path("/bin/sh"),
        ft8modem_path=Path("/bin/sh"),
        af2udp_path=Path("/bin/sh"),
        sox_path="/bin/sh",
    )


def _set_visible_user(monkeypatch, user_label: str) -> None:
    payload = [{"i": 2, "n": user_label, "t": "0:10:00"}]

    def fake_urlopen(req, timeout=0.0):
        return _FakeResponse(payload)

    monkeypatch.setattr(receiver_manager.urllib.request, "urlopen", fake_urlopen)


def _set_users_payload(monkeypatch, payload: list[dict[str, object]]) -> None:
    def fake_urlopen(req, timeout=0.0):
        return _FakeResponse(payload)

    monkeypatch.setattr(receiver_manager.urllib.request, "urlopen", fake_urlopen)


def test_health_summary_marks_silent_receiver_when_heartbeat_is_recent(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_visible_user(monkeypatch, "AUTO_20M_FT8")

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 10.0,
        "last_decode_unix": now - 901.0,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "quiet"
    assert summary["silent_receivers"] == 1
    assert summary["stalled_receivers"] == 0
    channel = summary["channels"]["2"]
    assert channel["health_state"] == "silent"
    assert channel["is_silent"] is True
    assert channel["is_unstable"] is False


def test_health_summary_marks_stalled_receiver_when_heartbeat_is_missing(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_visible_user(monkeypatch, "AUTO_20M_FT8")

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 180.0,
        "last_decode_unix": now - 181.0,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "degraded"
    assert summary["silent_receivers"] == 0
    assert summary["stalled_receivers"] == 1
    channel = summary["channels"]["2"]
    assert channel["health_state"] == "stalled"
    assert channel["is_stalled"] is True
    assert channel["is_unstable"] is True
    assert channel["last_reason"] == "stalled_no_decoder_output"


def test_health_summary_marks_stalled_receiver_when_assignment_is_stuck_on_wrong_rx(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_users_payload(
        monkeypatch,
        [
            {"i": 2, "n": "AUTO_40M_FT8", "t": "0:10:00"},
            {"i": 7, "n": "AUTO_20M_FT8", "t": "0:10:00"},
        ],
    )

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 5.0,
        "last_decode_unix": None,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "degraded"
    assert summary["stalled_receivers"] == 1
    channel = summary["channels"]["2"]
    assert channel["visible_on_kiwi"] is False
    assert channel["health_state"] == "stalled"
    assert channel["is_stalled"] is True
    assert channel["last_reason"] == "kiwi_assignment_mismatch"
