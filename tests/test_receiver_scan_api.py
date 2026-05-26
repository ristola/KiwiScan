from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.receiver_scan import make_router
from kiwi_scan.targeted_service_registry import normalize_target_service_key


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
        self.configured_kiwis = [
            {"host": "kiwi.local", "port": 8073},
            {"host": "kiwi-b.local", "port": 8074},
        ]
        self.active_kiwi_index = 0

    def resolve_runtime_target(self, *, kiwi_key=None, kiwi_index=None) -> dict[str, object]:
        if kiwi_key is not None and str(kiwi_key).strip():
            requested_key = str(kiwi_key).strip().lower()
            for index, entry in enumerate(self.configured_kiwis):
                entry_key = f"{entry['host']}:{entry['port']}"
                if entry_key.lower() == requested_key:
                    return {
                        "host": str(entry["host"]),
                        "port": int(entry["port"]),
                        "password": self.password,
                        "kiwi_index": int(index),
                        "kiwi_key": entry_key,
                    }
            raise ValueError(f"Unknown Kiwi target: {str(kiwi_key).strip()}")
        if kiwi_index is not None and str(kiwi_index).strip() != "":
            index = int(kiwi_index)
            if index < 0 or index >= len(self.configured_kiwis):
                raise ValueError(f"Unknown Kiwi target index: {kiwi_index}")
            entry = self.configured_kiwis[index]
            return {
                "host": str(entry["host"]),
                "port": int(entry["port"]),
                "password": self.password,
                "kiwi_index": int(index),
                "kiwi_key": f"{entry['host']}:{entry['port']}",
            }
        entry = self.configured_kiwis[self.active_kiwi_index]
        return {
            "host": str(entry["host"]),
            "port": int(entry["port"]),
            "password": self.password,
            "kiwi_index": int(self.active_kiwi_index),
            "kiwi_key": f"{entry['host']}:{entry['port']}",
        }


class _ReceiverScanStub:
    BAND = "40m"

    def __init__(self, *, name: str = "default") -> None:
        self.name = str(name)
        self.start_calls: list[dict[str, object]] = []
        self.prepare_calls: list[dict[str, object]] = []
        self.stop_calls = 0
        self.deactivate_calls = 0

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def prepare(self, **kwargs):
        self.prepare_calls.append(dict(kwargs))
        return {"ok": True, "mode_active": True, "fixed_receivers": []}

    def status(self):
        return {"ok": True, "service_name": self.name}

    def stop(self):
        self.stop_calls += 1
        return {"ok": True}

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


class _TargetedReceiverScanRegistryStub:
    def __init__(self) -> None:
        self.services: dict[str, _ReceiverScanStub] = {}

    def resolve_for_target(self, *, target=None) -> _ReceiverScanStub:
        service_key = normalize_target_service_key(target)
        service = self.services.get(service_key)
        if service is None:
            service = _ReceiverScanStub(name=service_key)
            self.services[service_key] = service
        return service


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


def test_receiver_scan_start_can_target_secondary_kiwi_by_key() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/start", json={"band": "20m", "kiwi_key": "kiwi-b.local:8074"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.deactivate_calls == 1
    assert receiver_scan.start_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
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


def test_receiver_scan_prepare_can_target_secondary_kiwi_by_index() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/prepare", json={"kiwi_index": 1})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.deactivate_calls == 1
    assert receiver_scan.prepare_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
        }
    ]


def test_receiver_scan_start_rejects_unknown_kiwi_target() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/receiver_scan/start", json={"kiwi_key": "missing.local:8073"})

    assert response.status_code == 400
    assert response.json() == {"detail": "Unknown Kiwi target: missing.local:8073"}
    assert caption_monitor.deactivate_calls == 0
    assert receiver_scan.start_calls == []


def test_receiver_scan_status_can_return_targeted_service_instance() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    receiver_scan = _TargetedReceiverScanRegistryStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    client.post("/receiver_scan/start", json={"band": "20m"})
    client.post("/receiver_scan/start", json={"band": "20m", "kiwi_key": "kiwi-b.local:8074"})

    default_status = client.get("/receiver_scan/status")
    targeted_status = client.get("/receiver_scan/status", params={"kiwi_key": "kiwi-b.local:8074"})

    assert default_status.status_code == 200
    assert default_status.json() == {"ok": True, "service_name": "kiwi.local:8073"}
    assert targeted_status.status_code == 200
    assert targeted_status.json() == {"ok": True, "service_name": "kiwi-b.local:8074"}
    assert receiver_scan.services["kiwi.local:8073"].start_calls != []
    assert receiver_scan.services["kiwi-b.local:8074"].start_calls != []