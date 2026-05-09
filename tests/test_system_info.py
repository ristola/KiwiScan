from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

import kiwi_scan.api.system_info as system_info


class _MgrStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "192.168.1.93"
        self.port = 8073

    def get_discovered_kiwis(self) -> dict[str, object]:
        return {"found": [], "source": "", "updated_unix": 0.0}


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def active_label_to_rx(self) -> dict[str, int]:
        # Mimic the real implementation: use a short timeout so tests don't hang.
        acquired = self._lock.acquire(timeout=0.05)
        if not acquired:
            return {}
        try:
            return {"AUTO_20M_FT8": 2}
        finally:
            self._lock.release()


def test_system_info_returns_raw_users_when_receiver_manager_lock_is_busy(monkeypatch) -> None:
    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()

    monkeypatch.setattr(
        system_info,
        "read_kiwi_status",
        lambda host, port, timeout_s: {"name": "Test Kiwi", "users": "1", "users_max": "8"},
    )
    monkeypatch.setattr(
        system_info,
        "_fetch_kiwi_users",
        lambda host, port: [
            {"i": 5, "n": "AUTO_20M_FT8", "g": "Test%20Location", "f": 14074000, "m": "usb", "a": "127.0.0.1", "t": "0:00:05"},
        ],
    )

    with system_info._SYSTEM_INFO_CACHE_LOCK:
        system_info._SYSTEM_INFO_CACHE["payload"] = None
        system_info._SYSTEM_INFO_CACHE["timestamp"] = 0.0
        system_info._SYSTEM_INFO_CACHE["future"] = None

    held = receiver_mgr._lock.acquire(blocking=False)
    assert held is True
    try:
        app = FastAPI()
        app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=receiver_mgr))
        client = TestClient(app)

        response = client.get("/system/info")
        assert response.status_code == 200
        payload = response.json()
        kiwi = payload["kiwi"]
        assert kiwi["reachable"] is True
        assert kiwi["raw_users"] == [
            {
                "rx": 5,
                "name": "AUTO_20M_FT8",
                "location": "Test Location",
                "freq_khz": 14074.0,
                "mode": "USB",
                "ip": "127.0.0.1",
                "connected_seconds": 5,
            }
        ]
        assert kiwi["active_users"] == []
    finally:
        receiver_mgr._lock.release()


def test_system_info_includes_cached_discovered_kiwis(monkeypatch) -> None:
    class _DiscoveredMgr(_MgrStub):
        def get_discovered_kiwis(self) -> dict[str, object]:
            return {
                "found": [
                    {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
                    {"host": "10.13.1.236", "port": 8073, "sdr_hw": "KiwiSDR 2"},
                ],
                "source": "my.kiwisdr.com",
                "updated_unix": 1778291400.0,
            }

    mgr = _DiscoveredMgr()
    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    monkeypatch.setattr(system_info, "read_kiwi_status", lambda host, port, timeout_s: {})
    monkeypatch.setattr(system_info, "_fetch_kiwi_users", lambda host, port: [])

    response = client.get("/system/info")

    assert response.status_code == 200
    payload = response.json()
    assert payload["kiwi"]["discovered_kiwis"] == [
        {
            "host": "10.13.1.235",
            "port": 8073,
            "sdr_hw": "KiwiSDR 1",
            "reachable": False,
            "status": {
                "name": None,
                "sdr_hw": None,
                "sw_version": None,
                "bands": None,
                "users": None,
                "users_max": None,
                "preempt": None,
                "gps": None,
                "grid": None,
                "gps_good": None,
                "fixes": None,
                "loc": None,
                "antenna": None,
                "snr": None,
                "adc_ov": None,
                "uptime_seconds": None,
                "date": None,
                "offline": None,
            },
            "raw_user_count": 0,
            "active_user_count": 0,
        },
        {
            "host": "10.13.1.236",
            "port": 8073,
            "sdr_hw": "KiwiSDR 2",
            "reachable": False,
            "status": {
                "name": None,
                "sdr_hw": None,
                "sw_version": None,
                "bands": None,
                "users": None,
                "users_max": None,
                "preempt": None,
                "gps": None,
                "grid": None,
                "gps_good": None,
                "fixes": None,
                "loc": None,
                "antenna": None,
                "snr": None,
                "adc_ov": None,
                "uptime_seconds": None,
                "date": None,
                "offline": None,
            },
            "raw_user_count": 0,
            "active_user_count": 0,
        },
    ]
    assert payload["kiwi"]["discovery_source"] == "my.kiwisdr.com"


def test_system_info_enriches_discovered_kiwis_with_live_status(monkeypatch) -> None:
    class _DiscoveredMgr(_MgrStub):
        def __init__(self) -> None:
            super().__init__()
            self.host = "10.13.1.235"
            self.port = 8073

        def get_discovered_kiwis(self) -> dict[str, object]:
            return {
                "found": [
                    {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1", "name": "Primary"},
                    {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2", "name": "Secondary"},
                ],
                "source": "my.kiwisdr.com",
                "updated_unix": 1778291400.0,
            }

    mgr = _DiscoveredMgr()

    def _fake_status(host: str, port: int, timeout_s: float) -> dict[str, object]:
        if host == "10.13.1.235" and port == 8073:
            return {
                "name": "Primary Kiwi",
                "sdr_hw": "KiwiSDR 1 v1.0",
                "sw_version": "Kiwi_v1",
                "fixes": "42",
                "snr": "15,10",
                "users": "8",
                "users_max": "8",
                "antenna": "20' LoG",
                "uptime": "3600",
                "date": "Sat May 9 05:25:50 2026",
            }
        if host == "10.13.1.236" and port == 8074:
            return {
                "name": "Secondary Kiwi",
                "sdr_hw": "KiwiSDR 2 v1.0",
                "sw_version": "Kiwi_v1",
                "fixes": "49",
                "snr": "14,20",
                "users": "6",
                "users_max": "8",
                "antenna": "Dipole",
                "uptime": "1800",
                "date": "Sat May 9 05:25:55 2026",
            }
        return {}

    def _fake_users(host: str, port: int) -> list[dict[str, object]]:
        if host == "10.13.1.235" and port == 8073:
            return [
                {"i": 0, "n": "User 1", "m": "usb", "a": "10.0.0.1", "t": "0:10:00"},
                {"i": 1, "n": "User 2", "m": "iq", "a": "10.0.0.2", "t": "0:05:00"},
            ]
        if host == "10.13.1.236" and port == 8074:
            return [
                {"i": 0, "n": "User 1", "m": "usb", "a": "10.0.0.3", "t": "0:03:00"},
                {"i": 1, "n": "User 2", "m": "iq", "a": "10.0.0.4", "t": "0:02:00"},
                {"i": 2, "n": "User 3", "m": "usb", "a": "10.0.0.5", "t": "0:01:00"},
            ]
        return []

    monkeypatch.setattr(system_info, "read_kiwi_status", _fake_status)
    monkeypatch.setattr(system_info, "_fetch_kiwi_users", _fake_users)

    payload = system_info._build_kiwi_payload(mgr)

    assert payload["status"]["name"] == "Primary Kiwi"
    assert payload["discovered_kiwis"][0]["status"]["name"] == "Primary Kiwi"
    assert payload["discovered_kiwis"][0]["reachable"] is True
    assert payload["discovered_kiwis"][0]["raw_user_count"] == 2
    assert payload["discovered_kiwis"][0]["active_user_count"] == 2
    assert payload["discovered_kiwis"][1]["status"] == {
        "name": "Secondary Kiwi",
        "sdr_hw": "KiwiSDR 2 v1.0",
        "sw_version": "Kiwi_v1",
        "bands": None,
        "users": 6,
        "users_max": 8,
        "preempt": None,
        "gps": None,
        "grid": None,
        "gps_good": None,
        "fixes": 49,
        "loc": None,
        "antenna": "Dipole",
        "snr": "14,20",
        "adc_ov": None,
        "uptime_seconds": 1800,
        "date": "Sat May 9 05:25:55 2026",
        "offline": None,
    }
    assert payload["discovered_kiwis"][1]["raw_user_count"] == 3
    assert payload["discovered_kiwis"][1]["active_user_count"] == 3
    assert payload["discovered_kiwis"][1]["reachable"] is True
