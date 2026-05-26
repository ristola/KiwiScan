from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.net_monitor import make_router
from kiwi_scan.targeted_service_registry import normalize_target_service_key


class _ManagerStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "kiwi.local"
        self.port = 8073
        self.password = None
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


class _NetMonitorStub:
    def __init__(self, *, name: str = "default") -> None:
        self.name = str(name)
        self.start_calls: list[dict[str, object]] = []
        self.capture_calls: list[dict[str, object]] = []
        self.stop_calls = 0
        self.deactivate_calls = 0

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def capture(self, **kwargs):
        self.capture_calls.append(dict(kwargs))
        return {"ok": True}

    def status(self):
        return {"ok": True, "service_name": self.name}

    def stop(self):
        self.stop_calls += 1
        return {"ok": True}

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


class _ReceiverScanStub:
    def __init__(self) -> None:
        self.deactivate_calls = 0

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


class _CaptionMonitorStub:
    def __init__(self) -> None:
        self.deactivate_calls = 0

    def deactivate(self):
        self.deactivate_calls += 1
        return {"ok": True}


class _TargetedRegistryStub:
    def __init__(self, factory) -> None:
        self._factory = factory
        self.services: dict[str, object] = {}

    def resolve_for_target(self, *, target=None):
        service_key = normalize_target_service_key(target)
        service = self.services.get(service_key)
        if service is None:
            service = self._factory(service_key)
            self.services[service_key] = service
        return service


def test_net_monitor_start_deactivates_receiver_scan_and_passes_options() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _NetMonitorStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/net_monitor/start", json={"max_cycles": 1, "cycle_sleep_s": 3.5})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert receiver_scan.deactivate_calls == 1
    assert caption_monitor.deactivate_calls == 1
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


def test_net_monitor_start_can_target_secondary_kiwi_by_key() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _NetMonitorStub()
    receiver_scan = _ReceiverScanStub()
    caption_monitor = _CaptionMonitorStub()
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/net_monitor/start", json={"kiwi_key": "kiwi-b.local:8074", "profile": "40m-net"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert receiver_scan.deactivate_calls == 1
    assert caption_monitor.deactivate_calls == 1
    assert net_monitor.start_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "profile_name": "40m-net",
        }
    ]


def test_net_monitor_capture_can_target_secondary_kiwi_by_index() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _NetMonitorStub()
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor))
    client = TestClient(app)

    response = client.post("/net_monitor/capture", json={"kiwi_index": 1, "duration_s": 5})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert net_monitor.capture_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "duration_s": 5,
        }
    ]


def test_net_monitor_status_can_resolve_targeted_service_instance() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    net_monitor = _TargetedRegistryStub(lambda service_key: _NetMonitorStub(name=service_key))
    receiver_scan = _TargetedRegistryStub(lambda _service_key: _ReceiverScanStub())
    caption_monitor = _TargetedRegistryStub(lambda _service_key: _CaptionMonitorStub())
    app.include_router(make_router(mgr=manager, net_monitor=net_monitor, receiver_scan=receiver_scan, caption_monitor=caption_monitor))
    client = TestClient(app)

    response = client.post("/net_monitor/start", json={"kiwi_key": "kiwi-b.local:8074", "profile": "40m-net"})
    status_response = client.get("/net_monitor/status", params={"kiwi_key": "kiwi-b.local:8074"})

    assert response.status_code == 200
    assert status_response.status_code == 200
    assert status_response.json() == {"ok": True, "service_name": "kiwi-b.local:8074"}
    assert net_monitor.services["kiwi-b.local:8074"].start_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "profile_name": "40m-net",
        }
    ]
    assert receiver_scan.services["kiwi-b.local:8074"].deactivate_calls == 1
    assert caption_monitor.services["kiwi-b.local:8074"].deactivate_calls == 1
    assert "kiwi.local:8073" not in receiver_scan.services