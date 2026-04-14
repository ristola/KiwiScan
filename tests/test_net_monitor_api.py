from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.net_monitor import make_router


class _ManagerStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "kiwi.local"
        self.port = 8073
        self.password = None


class _NetMonitorStub:
    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []
        self.capture_calls: list[dict[str, object]] = []

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def capture(self, **kwargs):
        self.capture_calls.append(dict(kwargs))
        return {"ok": True}

    def status(self):
        return {"ok": True}

    def stop(self):
        return {"ok": True}

    def deactivate(self):
        return {"ok": True}


class _ReceiverScanStub:
    def __init__(self) -> None:
        self.deactivate_calls = 0

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


def test_net_monitor_start_deactivates_receiver_scan_and_passes_options() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _NetMonitorStub()
    receiver_scan = _ReceiverScanStub()
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor, receiver_scan=receiver_scan))
    client = TestClient(app)

    response = client.post("/net_monitor/start", json={"max_cycles": 1, "cycle_sleep_s": 3.5})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert receiver_scan.deactivate_calls == 1
    assert net_monitor.start_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "profile_name": "20m-net",
            "cycle_sleep_s": 3.5,
            "max_cycles": 1,
        }
    ]


def test_net_monitor_capture_passes_duration_and_frequency() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _NetMonitorStub()
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor))
    client = TestClient(app)

    response = client.post("/net_monitor/capture", json={"duration_s": 9, "freq_mhz": 14.2895})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert net_monitor.capture_calls == [
        {
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "duration_s": 9,
            "freq_mhz": 14.2895,
        }
    ]