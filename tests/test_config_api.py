from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

import kiwi_scan.api.config as config_api


class _MgrStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.dwell_s = 18.0
        self.span_hz = 3000.0
        self.threshold_db = 12.0
        self.threshold_db_by_band = {}
        self.fps = 2.0
        self.s_meter_offset_db = 0.0
        self.latitude = 37.431
        self.longitude = -79.1231
        self.fast_scan_enabled = True
        self.fast_scan_s_threshold = 3.0
        self.fast_scan_min_frames = 2
        self.fast_scan_min_duration_s = 1.5
        self.retune_pause_s = 0.5
        self.rx_chan = None
        self.host = "10.13.1.236"
        self.port = 8074
        self.debug = False
        self.runtime_dependencies = {}
        self.discovered_kiwis: list[dict[str, object]] = []
        self.discovery_source = ""
        self.discovery_updated_unix = 0.0
        self.save_calls = 0

    def _save_config(self) -> None:
        self.save_calls += 1

    def set_discovered_kiwis(self, discovery: dict[str, object], *, save: bool = True) -> None:
        found = discovery.get("found")
        out: list[dict[str, object]] = []
        if isinstance(found, list):
            for item in found:
                if not isinstance(item, dict):
                    continue
                host = str(item.get("host") or "").strip()
                try:
                    port = int(item.get("port") or 0)
                except Exception:
                    port = 0
                if not host or not (1 <= port <= 65535):
                    continue
                entry: dict[str, object] = {"host": host, "port": port}
                for key in ("name", "grid", "sdr_hw", "sw_version", "loc"):
                    value = str(item.get(key) or "").strip()
                    if value:
                        entry[key] = value
                out.append(entry)
        self.discovered_kiwis = out
        self.discovery_source = str(discovery.get("source") or "").strip()
        self.discovery_updated_unix = 1778291400.0
        if save:
            self._save_config()

    def get_discovered_kiwis(self) -> dict[str, object]:
        return {
            "found": list(self.discovered_kiwis),
            "source": self.discovery_source,
            "updated_unix": self.discovery_updated_unix,
        }


def test_config_discover_probes_default_and_configured_ports(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    calls: dict[str, object] = {}

    def _fake_discover_kiwis(*, client_ip: str, port: int, ports, timeout_s: float, max_hosts: int):
        calls["client_ip"] = client_ip
        calls["port"] = port
        calls["ports"] = list(ports)
        calls["timeout_s"] = timeout_s
        calls["max_hosts"] = max_hosts
        return {
            "ok": True,
            "source": "lan_scan",
            "found": [
                {"host": "10.13.1.235", "port": 8073},
                {"host": "10.13.1.236", "port": 8074},
            ],
        }

    monkeypatch.setattr(config_api, "discover_kiwis", _fake_discover_kiwis)

    response = client.get("/config/discover?port=8073&timeout_s=0.2&max_hosts=32")

    assert response.status_code == 200
    assert calls["port"] == 8073
    assert calls["ports"] == [8073, 8074]
    assert mgr.discovered_kiwis == [
        {"host": "10.13.1.235", "port": 8073},
        {"host": "10.13.1.236", "port": 8074},
    ]


def test_post_config_persists_discovered_kiwis(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "host": "10.13.1.236",
            "port": 8074,
            "discovered_kiwis": [
                {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
                {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2"},
            ],
            "discovery_source": "lan_scan",
        },
    )

    assert response.status_code == 200
    assert mgr.save_calls == 1

    config_response = client.get("/config")

    assert config_response.status_code == 200
    payload = config_response.json()
    assert payload["discovered_kiwis"] == [
        {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
        {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2"},
    ]
    assert payload["discovery_source"] == "lan_scan"