from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.caption import make_router
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


class _CaptionMonitorStub:
    def __init__(self, *, name: str = "default") -> None:
        self.name = str(name)
        self.start_calls: list[dict[str, object]] = []
        self.stop_calls = 0
        self.deactivate_calls = 0

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True}

    def status(self):
        return {"ok": True, "service_name": self.name}

    def stop(self):
        self.stop_calls += 1
        return {"ok": True}

    def deactivate(self):
        self.deactivate_calls += 1
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


def test_caption_start_can_target_secondary_kiwi_by_key() -> None:
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

    response = client.post("/caption/start", json={"freq_khz": 7179.0, "kiwi_key": "kiwi-b.local:8074"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert caption_monitor.start_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "freq_khz": 7179.0,
            "sideband": "LSB",
            "rx_chan": 0,
        }
    ]


def test_caption_status_can_resolve_targeted_service_instance() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    caption_monitor = _TargetedRegistryStub(lambda service_key: _CaptionMonitorStub(name=service_key))
    receiver_scan = _TargetedRegistryStub(lambda _service_key: _ServiceStub())
    net_monitor = _TargetedRegistryStub(lambda _service_key: _ServiceStub())
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

    response = client.post("/caption/start", json={"freq_khz": 7179.0, "kiwi_key": "kiwi-b.local:8074"})
    status_response = client.get("/caption/status", params={"kiwi_key": "kiwi-b.local:8074"})

    assert response.status_code == 200
    assert status_response.status_code == 200
    assert status_response.json() == {"ok": True, "service_name": "kiwi-b.local:8074"}
    assert caption_monitor.services["kiwi-b.local:8074"].start_calls == [
        {
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "freq_khz": 7179.0,
            "sideband": "LSB",
            "rx_chan": 0,
        }
    ]
    assert receiver_scan.services["kiwi-b.local:8074"].deactivate_calls == 1
    assert net_monitor.services["kiwi-b.local:8074"].deactivate_calls == 1
    assert rx_monitor.stop_calls == 1