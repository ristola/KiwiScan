from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api import decodes_status as decodes_status_api


class _UrlResponse:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self, _size: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_host = "kiwi.local"
        self._active_port = 8073
        self._host = "kiwi.local"
        self._port = 8073
        self._assignments = {
            1: SimpleNamespace(band="12m", mode_label="FT8", freq_hz=24_915_000.0),
            2: SimpleNamespace(band="20m", mode_label="FT4 / FT8", freq_hz=14_077_000.0),
            3: SimpleNamespace(band="20m", mode_label="WSPR", freq_hz=14_095_600.0),
            5: SimpleNamespace(band="40m", mode_label="FT4 / WSPR", freq_hz=7_043_050.0),
            7: SimpleNamespace(band="17m", mode_label="FT4 / FT8 / WSPR", freq_hz=18_102_000.0),
        }
        self._workers = {}

    def health_summary(self) -> dict:
        return {
            "channels": {
                str(rx): {"health_state": "healthy", "last_reason": None}
                for rx in self._assignments
            }
        }


def test_decodes_status_accepts_compact_live_labels(monkeypatch) -> None:
    payload = [
        {"i": 0, "n": "ROAM212MFT8", "f": 24915000.0, "t": "0:14:52"},
        {"i": 2, "n": "FIXED20MFT8", "f": 14077000.0, "t": "0:17:38"},
        {"i": 3, "n": "FIXED20MWS", "f": 14095600.0, "t": "0:17:32"},
        {"i": 5, "n": "FIXED40MFT4", "f": 7043050.0, "t": "0:17:19"},
        {"i": 7, "n": "FIXED17MFT8", "f": 18102300.0, "t": "0:16:42"},
    ]

    def _fake_urlopen(req, timeout=0.5):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        assert url == "http://kiwi.local:8073/users?json=1"
        assert timeout == 0.5
        return _UrlResponse(payload)

    monkeypatch.setattr(decodes_status_api.urllib.request, "urlopen", _fake_urlopen)

    app = FastAPI()
    app.include_router(
        decodes_status_api.make_router(
            receiver_mgr=_ReceiverMgrStub(),
            af2udp_path=Path("/tmp/af2udp"),
            ft8modem_path=Path("/tmp/ft8modem"),
        )
    )
    client = TestClient(app)

    response = client.get("/decodes/status")
    assert response.status_code == 200

    body = response.json()
    assert body["assignments_source"] == "kiwi_users"
    assert body["assignments_mismatch_rxs"] == []
    assert body["assignments"]["0"] == {"band": "12m", "mode": "FT8", "freq_hz": 24915000.0}
    assert body["assignments"]["2"] == {"band": "20m", "mode": "FT8", "freq_hz": 14077000.0}
    assert body["assignments"]["3"] == {"band": "20m", "mode": "WSPR", "freq_hz": 14095600.0}
    assert body["assignments"]["5"] == {"band": "40m", "mode": "FT4", "freq_hz": 7043050.0}
    assert body["assignments"]["7"] == {"band": "17m", "mode": "FT8", "freq_hz": 18102300.0}


def test_decodes_status_returns_cached_payload_when_lock_is_busy(monkeypatch) -> None:
    payload = [
        {"i": 2, "n": "FIXED20MFT8", "f": 14077000.0, "t": "0:00:10"},
    ]

    def _fake_urlopen(req, timeout=0.5):
        return _UrlResponse(payload)

    monkeypatch.setattr(decodes_status_api.urllib.request, "urlopen", _fake_urlopen)

    receiver_mgr = _ReceiverMgrStub()
    receiver_mgr._assignments = {
        2: SimpleNamespace(band="20m", mode_label="FT4 / FT8", freq_hz=14_077_000.0),
    }
    app = FastAPI()
    app.include_router(
        decodes_status_api.make_router(
            receiver_mgr=receiver_mgr,
            af2udp_path=Path("/tmp/af2udp"),
            ft8modem_path=Path("/tmp/ft8modem"),
        )
    )
    client = TestClient(app)

    first = client.get("/decodes/status")
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["assignments_source"] == "kiwi_users"

    held = receiver_mgr._lock.acquire(blocking=False)
    assert held is True
    try:
        second = client.get("/decodes/status")
    finally:
        receiver_mgr._lock.release()

    assert second.status_code == 200
    second_body = second.json()
    assert second_body["assignments"] == first_body["assignments"]
    assert second_body["assignments_source"] == first_body["assignments_source"]
    assert second_body["_from_cache"] is True


def test_decodes_status_accepts_readable_live_labels_with_mix_and_all(monkeypatch) -> None:
    payload = [
        {"i": 0, "n": "ROAM_12m_FT8", "f": 24915000.0, "t": "0:14:52"},
        {"i": 2, "n": "FIXED_20m_MIX", "f": 14077000.0, "t": "0:17:38"},
        {"i": 3, "n": "FIXED_20m_WSPR", "f": 14095600.0, "t": "0:17:32"},
        {"i": 5, "n": "FIXED_40m_MIX", "f": 7043050.0, "t": "0:17:19"},
        {"i": 7, "n": "FIXED_17m_ALL", "f": 18102300.0, "t": "0:16:42"},
    ]

    def _fake_urlopen(req, timeout=0.5):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        assert url == "http://kiwi.local:8073/users?json=1"
        assert timeout == 0.5
        return _UrlResponse(payload)

    monkeypatch.setattr(decodes_status_api.urllib.request, "urlopen", _fake_urlopen)

    app = FastAPI()
    app.include_router(
        decodes_status_api.make_router(
            receiver_mgr=_ReceiverMgrStub(),
            af2udp_path=Path("/tmp/af2udp"),
            ft8modem_path=Path("/tmp/ft8modem"),
        )
    )
    client = TestClient(app)

    response = client.get("/decodes/status")
    assert response.status_code == 200

    body = response.json()
    assert body["assignments_source"] == "kiwi_users"
    assert body["assignments_mismatch_rxs"] == []
    assert body["assignments"]["0"] == {"band": "12m", "mode": "FT8", "freq_hz": 24915000.0}
    assert body["assignments"]["2"] == {"band": "20m", "mode": "MIX", "freq_hz": 14077000.0}
    assert body["assignments"]["3"] == {"band": "20m", "mode": "WSPR", "freq_hz": 14095600.0}
    assert body["assignments"]["5"] == {"band": "40m", "mode": "MIX", "freq_hz": 7043050.0}
    assert body["assignments"]["7"] == {"band": "17m", "mode": "ALL", "freq_hz": 18102300.0}
