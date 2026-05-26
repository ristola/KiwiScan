from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.band_scan import make_router
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


class _BandScannerStub:
    def __init__(self, *, name: str = "default") -> None:
        self.name = str(name)
        self.start_calls: list[dict[str, object]] = []
        self.stop_calls = 0

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True, "results_path": "/tmp/band_scan_results_20m.json"}

    def status(self):
        return {"running": False, "last_results_report": "/tmp/band_scan_results_20m.json", "service_name": self.name}

    def results(self):
        return {
            "ok": True,
            "status": "ready",
            "report_path": "/tmp/band_scan_results_20m.json",
            "band": "20m",
            "result_count": 1,
            "results": [{"selection_freq_mhz": 14.074, "selection_rank": 1}],
        }

    def stop(self):
        self.stop_calls += 1
        return {"ok": True}


class _TargetedBandScannerRegistryStub:
    def __init__(self) -> None:
        self.services: dict[str, _BandScannerStub] = {}

    def resolve_for_target(self, *, target=None) -> _BandScannerStub:
        service_key = normalize_target_service_key(target)
        service = self.services.get(service_key)
        if service is None:
            service = _BandScannerStub(name=service_key)
            self.services[service_key] = service
        return service


def test_band_scan_start_uses_requested_band_threshold() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    band_scanner = _BandScannerStub()
    app.include_router(make_router(mgr=manager, band_scanner=band_scanner))
    client = TestClient(app)

    response = client.post("/band_scan", json={"band": "20m"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert band_scanner.start_calls == [
        {
            "band": "20m",
            "host": "kiwi.local",
            "port": 8073,
            "password": None,
            "user": "Band Scanning 20m",
            "threshold_db": 8.0,
            "rx_chan": 0,
            "wf_rx_chan": 0,
            "span_hz": 30000.0,
            "step_hz": None,
            "max_frames": 10,
            "record_seconds": 6,
            "record_hits": True,
            "detector": "waterfall",
            "ssb_probe_only": True,
            "required_hits": None,
            "probe_freqs_mhz": None,
            "allow_rx_fallback": True,
            "on_hit": band_scanner.start_calls[0]["on_hit"],
            "session_id": None,
        }
    ]


def test_band_scan_results_endpoint_returns_latest_results() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    band_scanner = _BandScannerStub()
    app.include_router(make_router(mgr=manager, band_scanner=band_scanner))
    client = TestClient(app)

    response = client.get("/band_scan/results")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "status": "ready",
        "report_path": "/tmp/band_scan_results_20m.json",
        "band": "20m",
        "result_count": 1,
        "results": [{"selection_freq_mhz": 14.074, "selection_rank": 1}],
    }


def test_band_scan_start_can_target_secondary_kiwi_by_key() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    band_scanner = _BandScannerStub()
    app.include_router(make_router(mgr=manager, band_scanner=band_scanner))
    client = TestClient(app)

    response = client.post("/band_scan", json={"band": "20m", "kiwi_key": "kiwi-b.local:8074"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert band_scanner.start_calls == [
        {
            "band": "20m",
            "host": "kiwi-b.local",
            "port": 8074,
            "password": None,
            "user": "Band Scanning 20m",
            "threshold_db": 8.0,
            "rx_chan": 0,
            "wf_rx_chan": 0,
            "span_hz": 30000.0,
            "step_hz": None,
            "max_frames": 10,
            "record_seconds": 6,
            "record_hits": True,
            "detector": "waterfall",
            "ssb_probe_only": True,
            "required_hits": None,
            "probe_freqs_mhz": None,
            "allow_rx_fallback": True,
            "on_hit": band_scanner.start_calls[0]["on_hit"],
            "session_id": None,
        }
    ]


def test_band_scan_status_can_return_targeted_service_instance() -> None:
    app = FastAPI()
    manager = _ManagerStub()
    band_scanner = _TargetedBandScannerRegistryStub()
    app.include_router(make_router(mgr=manager, band_scanner=band_scanner))
    client = TestClient(app)

    client.post("/band_scan", json={"band": "20m"})
    client.post("/band_scan", json={"band": "20m", "kiwi_key": "kiwi-b.local:8074"})

    default_status = client.get("/band_scan/status")
    targeted_status = client.get("/band_scan/status", params={"kiwi_key": "kiwi-b.local:8074"})

    assert default_status.status_code == 200
    assert default_status.json()["service_name"] == "kiwi.local:8073"
    assert targeted_status.status_code == 200
    assert targeted_status.json()["service_name"] == "kiwi-b.local:8074"
    assert band_scanner.services["kiwi.local:8073"].start_calls != []
    assert band_scanner.services["kiwi-b.local:8074"].start_calls != []