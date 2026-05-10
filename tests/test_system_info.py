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
        self.configured_kiwis: list[dict[str, object]] = []

    def get_discovered_kiwis(self) -> dict[str, object]:
        return {"found": [], "source": "", "updated_unix": 0.0}

    def get_configured_kiwis(self) -> list[dict[str, object]]:
        return list(self.configured_kiwis)


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._assignments: dict[int, object] = {}

    def active_label_to_rx(self) -> dict[str, int]:
        # Mimic the real implementation: use a short timeout so tests don't hang.
        acquired = self._lock.acquire(timeout=0.05)
        if not acquired:
            return {}
        try:
            return {"AUTO_20M_FT8": 2}
        finally:
            self._lock.release()

    def health_summary(self) -> dict[str, object]:
        return {
            "channels": {
                "2": {"host": "10.13.1.236", "port": 8074, "kiwi_actual_rx": 6}
            }
        }


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
                "host": "192.168.1.93",
                "port": 8073,
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


def test_system_info_includes_configured_kiwis_in_known_entries(monkeypatch) -> None:
    mgr = _MgrStub()
    mgr.host = "10.13.1.235"
    mgr.port = 8073
    mgr.configured_kiwis = [
        {"host": "10.13.1.235", "port": 8073, "grid": "FM18"},
        {"host": "10.13.1.236", "port": 8074, "grid": "FM19"},
    ]

    monkeypatch.setattr(
        system_info,
        "read_kiwi_status",
        lambda host, port, timeout_s: {"name": "Primary Kiwi", "grid": "FM18"} if host == "10.13.1.235" and port == 8073 else {},
    )
    monkeypatch.setattr(system_info, "_fetch_kiwi_users", lambda host, port: [])

    payload = system_info._build_kiwi_payload(mgr)

    assert payload["configured_kiwis"] == [
        {"host": "10.13.1.235", "port": 8073, "grid": "FM18"},
        {"host": "10.13.1.236", "port": 8074, "grid": "FM19"},
    ]
    assert payload["discovered_kiwis"] == [
        {
            "host": "10.13.1.235",
            "port": 8073,
            "grid": "FM18",
            "known_source": "configured",
            "known_order": 0,
            "reachable": True,
            "status": {
                "name": "Primary Kiwi",
                "sdr_hw": None,
                "sw_version": None,
                "bands": None,
                "users": None,
                "users_max": None,
                "preempt": None,
                "gps": None,
                "grid": "FM18",
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
            "port": 8074,
            "grid": "FM19",
            "known_source": "configured",
            "known_order": 1,
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


def test_system_audio_stream_uses_requested_kiwi_endpoint_and_slot(monkeypatch) -> None:
    mgr = _MgrStub()
    mgr.host = "10.13.1.235"
    mgr.port = 8073
    mgr.configured_kiwis = [
        {"host": "10.13.1.235", "port": 8073},
        {"host": "10.13.1.236", "port": 8074},
    ]

    captured: dict[str, object] = {}

    def _fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([b"RIFF"])

    monkeypatch.setattr(system_info, "stream_kiwi_audio_wav", _fake_stream)

    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    response = client.get(
        "/system/audio_stream",
        params={
            "freq_khz": 3573.0,
            "source_freq_khz": 3571.8,
            "mode": "FT8",
            "rx": 1,
            "kiwi_rx": 6,
            "host": "10.13.1.236",
            "port": 8074,
            "stream_id": "stream-236-6",
        },
    )

    assert response.status_code == 200
    assert captured["host"] == "10.13.1.236"
    assert captured["port"] == 8074
    assert captured["required_rx"] == 6
    assert captured["freq_hz"] == 3_573_000.0
    assert captured["source_freq_hz"] == 3_571_800.0
    assert captured["stream_id"] == "stream-236-6"


def test_system_audio_stream_uses_requested_kiwi_endpoint_and_camp_slot(monkeypatch) -> None:
    mgr = _MgrStub()
    mgr.host = "10.13.1.235"
    mgr.port = 8073
    mgr.configured_kiwis = [
        {"host": "10.13.1.235", "port": 8073},
        {"host": "10.13.1.236", "port": 8074},
    ]

    captured: dict[str, object] = {}

    def _fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([b"RIFF"])

    monkeypatch.setattr(system_info, "stream_kiwi_audio_wav", _fake_stream)

    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    response = client.get(
        "/system/audio_stream",
        params={
            "freq_khz": 7074.0,
            "mode": "FT8",
            "rx": 4,
            "kiwi_rx": 4,
            "camp": 1,
            "host": "10.13.1.235",
            "port": 8073,
            "stream_id": "camp-rx4",
        },
    )

    assert response.status_code == 200
    assert captured["host"] == "10.13.1.235"
    assert captured["port"] == 8073
    assert captured["camp_rx"] == 4
    assert captured["required_rx"] is None
    assert captured["stream_id"] == "camp-rx4"


def test_system_audio_stream_prefers_receiver_health_mapping_over_stale_client_slot(monkeypatch) -> None:
    mgr = _MgrStub()
    mgr.host = "10.13.1.235"
    mgr.port = 8073
    mgr.configured_kiwis = [
        {"host": "10.13.1.235", "port": 8073},
        {"host": "10.13.1.236", "port": 8074},
    ]
    receiver_mgr = _ReceiverMgrStub()

    captured: dict[str, object] = {}

    def _fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([b"RIFF"])

    monkeypatch.setattr(system_info, "stream_kiwi_audio_wav", _fake_stream)

    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=receiver_mgr))
    client = TestClient(app)

    response = client.get(
        "/system/audio_stream",
        params={
            "freq_khz": 14074.0,
            "mode": "FT8",
            "rx": 2,
            "kiwi_rx": 0,
            "camp": 1,
            "host": "10.13.1.235",
            "port": 8073,
        },
    )

    assert response.status_code == 200
    assert captured["host"] == "10.13.1.236"
    assert captured["port"] == 8074
    assert captured["required_rx"] is None
    assert captured["camp_rx"] == 6


def test_system_audio_stream_prefers_managed_assignment_center_frequency(monkeypatch) -> None:
    class _Assignment:
        def __init__(self, freq_hz: float) -> None:
            self.freq_hz = freq_hz

    mgr = _MgrStub()
    mgr.host = "10.13.1.235"
    mgr.port = 8073
    mgr.configured_kiwis = [{"host": "10.13.1.235", "port": 8073}]
    receiver_mgr = _ReceiverMgrStub()
    receiver_mgr._assignments = {7: _Assignment(18_102_000.0)}

    captured: dict[str, object] = {}

    def _fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([b"RIFF"])

    monkeypatch.setattr(system_info, "stream_kiwi_audio_wav", _fake_stream)

    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=receiver_mgr))
    client = TestClient(app)

    response = client.get(
        "/system/audio_stream",
        params={
            "freq_khz": 18100.0,
            "source_freq_khz": 18102.3,
            "mode": "FT8",
            "rx": 7,
        },
    )

    assert response.status_code == 200
    assert captured["source_freq_hz"] == 18_102_000.0


def test_system_audio_stream_rejects_unknown_endpoint() -> None:
    mgr = _MgrStub()
    mgr.configured_kiwis = [{"host": "10.13.1.235", "port": 8073}]

    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    response = client.get(
        "/system/audio_stream",
        params={
            "freq_khz": 14074.0,
            "mode": "FT8",
            "host": "10.13.1.250",
            "port": 8079,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Audio stream endpoint is not a configured KiwiSDR"


def test_system_audio_stream_uses_active_kiwi_connection(monkeypatch) -> None:
    mgr = _MgrStub()
    mgr.password = "secret"
    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    captured: dict[str, object] = {}

    def _fake_stream(**kwargs):
        captured.update(kwargs)
        return iter([b"RIFFdemo"])

    monkeypatch.setattr(system_info, "stream_kiwi_audio_wav", _fake_stream)

    response = client.get("/system/audio_stream", params={"freq_khz": 7074.0, "mode": "FT8", "rx": 2})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("audio/wav")
    assert response.content == b"RIFFdemo"
    assert captured == {
        "host": "192.168.1.93",
        "port": 8073,
        "password": "secret",
        "freq_hz": 7_074_000.0,
        "source_freq_hz": None,
        "mode": "FT8",
        "user": "KiwiScan Web Audio RX2",
        "required_rx": None,
        "camp_rx": None,
        "stream_id": None,
    }


def test_system_audio_stream_stop_closes_named_stream(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(system_info.make_router(mgr=mgr, receiver_mgr=None))
    client = TestClient(app)

    captured: list[str] = []

    def _fake_stop(stream_id: str | None) -> bool:
        captured.append(str(stream_id))
        return True

    monkeypatch.setattr(system_info, "stop_kiwi_audio_stream", _fake_stop)

    response = client.post("/system/audio_stream/stop", params={"stream_id": "active-rx-audio-123"})

    assert response.status_code == 200
    assert response.json() == {"stopped": True}
    assert captured == ["active-rx-audio-123"]


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
