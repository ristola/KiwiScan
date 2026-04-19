from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.caption import make_router


class _ManagerStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "kiwi.local"
        self.port = 8073
        self.password = None


class _CaptionMonitorStub:
    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def status(self):
        return {"ok": True}

    def stop(self):
        return {"ok": True}

    def deactivate(self):
        return {"ok": True}


class _ServiceStub:
    def __init__(self) -> None:
        self.deactivate_calls = 0
        self.stop_calls = 0

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}

    def stop(self):
        self.stop_calls += 1
        return {"ok": True}


def test_caption_start_uses_lsb_for_7179_and_deactivates_conflicts() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    caption_monitor = _CaptionMonitorStub()
    receiver_scan = _ServiceStub()
    net_monitor = _ServiceStub()
    rx_monitor = _ServiceStub()
    app.include_router(
        make_router(
            mgr=manager,
            caption_monitor=caption_monitor,
            receiver_scan=receiver_scan,
            net_monitor=net_monitor,
            rx_monitor=rx_monitor,
        )
    )
    client = TestClient(app)

    response = client.post("/caption/start", json={"freq_khz": 7179.0, "rx_chan": 2, "chunk_duration_s": 7, "max_chunks": 3})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert receiver_scan.deactivate_calls == 1
    assert net_monitor.deactivate_calls == 1
    assert rx_monitor.stop_calls == 1
    assert caption_monitor.start_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "freq_khz": 7179.0,
            "sideband": "LSB",
            "rx_chan": 2,
            "chunk_duration_s": 7,
            "max_chunks": 3,
        }
    ]


def test_caption_start_rejects_30m_phone() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/caption/start", json={"freq_khz": 10125.0})

    assert response.status_code == 400
    assert response.json()["detail"] == "30m has no phone operation"
    assert caption_monitor.start_calls == []