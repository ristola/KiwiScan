from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.receiver_scan import make_router


class _ManagerStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "kiwi.local"
        self.port = 8073
        self.password = None
        self.threshold_db = 9.0
        self.threshold_db_by_band = {
            "20m": 8.0,
            "40m": 12.5,
        }


class _ReceiverScanStub:
    BAND = "40m"

    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []
        self.prepare_calls: list[dict[str, object]] = []

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def prepare(self, **kwargs):
        self.prepare_calls.append(dict(kwargs))
        return {"ok": True, "mode_active": True, "fixed_receivers": [2, 3, 4, 5, 6, 7]}

    def status(self):
        return {"ok": True}

    def stop(self):
        return {"ok": True}

    def deactivate(self):
        return {"ok": True}


class _CaptionMonitorStub:
    def __init__(self) -> None:
        self.deactivate_calls = 0

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


def test_receiver_scan_start_uses_threshold_for_scan_band() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/start")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.deactivate_calls == 1
    assert receiver_scan.start_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "threshold_db": 12.5,
        }
    ]


def test_receiver_scan_start_uses_requested_band_threshold() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/start", json={"band": "20m"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.deactivate_calls == 1
    assert receiver_scan.start_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "threshold_db": 8.0,
            "band": "20m",
        }
    ]


def test_receiver_scan_prepare_reserves_scan_slots_without_starting_scan() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/prepare")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.deactivate_calls == 1
    assert receiver_scan.prepare_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
        }
    ]
    assert receiver_scan.start_calls == []