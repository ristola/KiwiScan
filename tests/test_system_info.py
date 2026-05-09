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
        {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
        {"host": "10.13.1.236", "port": 8073, "sdr_hw": "KiwiSDR 2"},
    ]
    assert payload["kiwi"]["discovery_source"] == "my.kiwisdr.com"
