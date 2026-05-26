from __future__ import annotations

from kiwi_scan.discovery_manager import DiscoveryManager


async def _noop_broadcast_status(_payload: dict) -> None:
    return None


def _make_manager(monkeypatch) -> DiscoveryManager:
    monkeypatch.setattr(DiscoveryManager, "_load_config", lambda self: None)
    monkeypatch.setattr(DiscoveryManager, "_load_secrets", lambda self: None)
    return DiscoveryManager(
        get_loop=lambda: None,
        broadcast_status=_noop_broadcast_status,
        compute_s_metrics=lambda results, offset: results,
        waterholes={"20m": 14_074_000.0},
    )


def test_resolve_runtime_target_uses_active_configured_kiwi(monkeypatch) -> None:
    manager = _make_manager(monkeypatch)
    with manager.lock:
        manager.configured_kiwis = [
            {"host": "kiwi-a.local", "port": 8073},
            {"host": "kiwi-b.local", "port": 8074},
        ]
        manager.active_kiwi_index = 1
        manager.host = "kiwi-b.local"
        manager.port = 8074
        manager.password = "secret"

    assert manager.resolve_runtime_target() == {
        "host": "kiwi-b.local",
        "port": 8074,
        "password": "secret",
        "kiwi_index": 1,
        "kiwi_key": "kiwi-b.local:8074",
    }


def test_resolve_runtime_target_can_select_by_kiwi_key(monkeypatch) -> None:
    manager = _make_manager(monkeypatch)
    with manager.lock:
        manager.configured_kiwis = [
            {"host": "kiwi-a.local", "port": 8073},
            {"host": "kiwi-b.local", "port": 8074},
        ]
        manager.active_kiwi_index = 1
        manager.host = "kiwi-b.local"
        manager.port = 8074
        manager.password = "secret"

    assert manager.resolve_runtime_target(kiwi_key="KIWI-A.LOCAL:8073") == {
        "host": "kiwi-a.local",
        "port": 8073,
        "password": "secret",
        "kiwi_index": 0,
        "kiwi_key": "kiwi-a.local:8073",
    }


def test_set_active_kiwi_index_does_not_switch_primary_endpoint(monkeypatch) -> None:
    manager = _make_manager(monkeypatch)
    with manager.lock:
        manager.configured_kiwis = [
            {"host": "kiwi-a.local", "port": 8073, "latitude": 38.1, "longitude": -78.2},
            {"host": "kiwi-b.local", "port": 8074, "latitude": 39.1, "longitude": -77.2},
        ]
        manager.host = "kiwi-a.local"
        manager.port = 8073
        manager.latitude = 38.1
        manager.longitude = -78.2

    manager.set_active_kiwi_index(1, save=False)

    configured_kiwis = manager.get_configured_kiwis()

    with manager.lock:
        assert manager.active_kiwi_index == 1
        assert manager.host == "kiwi-a.local"
        assert manager.port == 8073
        assert manager.latitude == 38.1
        assert manager.longitude == -78.2
    assert configured_kiwis == [
        {"host": "kiwi-a.local", "port": 8073, "latitude": 38.1, "longitude": -78.2},
        {"host": "kiwi-b.local", "port": 8074, "latitude": 39.1, "longitude": -77.2},
    ]