from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.band_scan import make_router


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


class _BandScannerStub:
    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        return {"ok": True, "results_path": "/tmp/band_scan_results_20m.json"}

    def status(self):
        return {"running": False, "last_results_report": "/tmp/band_scan_results_20m.json"}

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
        return {"ok": True}


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