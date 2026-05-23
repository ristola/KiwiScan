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
        self.configured_kiwis: list[dict[str, object]] = []
        self.discovered_kiwis: list[dict[str, object]] = []
        self.discovery_source = ""
        self.discovery_updated_unix = 0.0
        self.password: str | None = None
        self.admin_password: str | None = None
        self.save_calls = 0
        self.secret_save_calls = 0

    def _save_config(self) -> None:
        self.save_calls += 1

    def _save_secrets(self) -> None:
        self.secret_save_calls += 1

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

    def set_configured_kiwis(self, kiwis: object, *, save: bool = True) -> None:
        out: list[dict[str, object]] = []
        if isinstance(kiwis, list):
            for item in kiwis:
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
                for key in ("grid", "name", "loc", "sdr_hw", "sw_version"):
                    value = str(item.get(key) or "").strip()
                    if value:
                        entry[key] = value
                for key in ("latitude", "longitude"):
                    try:
                        value = float(item.get(key))
                    except Exception:
                        continue
                    entry[key] = value
                out.append(entry)
        self.configured_kiwis = out
        if out:
            self.host = str(out[0]["host"])
            self.port = int(out[0]["port"])
            if "latitude" in out[0]:
                self.latitude = float(out[0]["latitude"])
            if "longitude" in out[0]:
                self.longitude = float(out[0]["longitude"])
            self.rx_chan = None
        if save:
            self._save_config()

    def get_configured_kiwis(self) -> list[dict[str, object]]:
        return list(self.configured_kiwis)

    def get_discovered_kiwis(self) -> dict[str, object]:
        return {
            "found": list(self.discovered_kiwis),
            "source": self.discovery_source,
            "updated_unix": self.discovery_updated_unix,
        }

    def set_kiwi_password(self, password: object, *, save: bool = True) -> None:
        self.password = str(password or "").strip() or None

    def set_kiwi_admin_password(self, password: object, *, save: bool = True) -> None:
        self.admin_password = str(password or "").strip() or None

    def has_kiwi_password(self) -> bool:
        return bool(str(getattr(self, "password", "") or "").strip())

    def has_kiwi_admin_password(self) -> bool:
        return bool(str(getattr(self, "admin_password", "") or "").strip())


class _LockingMgrStub(_MgrStub):
    def get_configured_kiwis(self) -> list[dict[str, object]]:
        with self.lock:
            return list(self.configured_kiwis)

    def has_kiwi_password(self) -> bool:
        with self.lock:
            return bool(str(getattr(self, "password", "") or "").strip())

    def has_kiwi_admin_password(self) -> bool:
        with self.lock:
            return bool(str(getattr(self, "admin_password", "") or "").strip())

    def set_kiwi_password(self, password: object, *, save: bool = True) -> None:
        with self.lock:
            value = str(password or "").strip()
            self.password = value or None
            if save:
                self._save_secrets()

    def set_kiwi_admin_password(self, password: object, *, save: bool = True) -> None:
        with self.lock:
            value = str(password or "").strip()
            self.admin_password = value or None
            if save:
                self._save_secrets()


class _AutoSetLoopStub:
    def __init__(self, *, apply_result: bool = True, raise_on_apply: bool = False) -> None:
        self.apply_result = bool(apply_result)
        self.raise_on_apply = bool(raise_on_apply)
        self.apply_calls: list[tuple[bool, bool]] = []
        self.notify_calls = 0

    def apply_current_settings(self, *, force: bool = False, sync_state: bool = True) -> bool:
        self.apply_calls.append((bool(force), bool(sync_state)))
        if self.raise_on_apply:
            raise RuntimeError("apply failed")
        return self.apply_result

    def notify_settings_changed(self) -> None:
        self.notify_calls += 1


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


def test_post_config_persists_configured_kiwis(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "kiwisdrs": [
                {"host": "10.13.1.235", "port": 8073, "latitude": 38.1, "longitude": -78.2, "grid": "FM08so"},
                {"host": "10.13.1.236", "port": 8074, "latitude": 39.1, "longitude": -77.2, "grid": "FM09aa"},
            ],
        },
    )

    assert response.status_code == 200
    assert mgr.save_calls == 1
    assert mgr.host == "10.13.1.235"
    assert mgr.port == 8073

    config_response = client.get("/config")

    assert config_response.status_code == 200
    payload = config_response.json()
    assert payload["kiwisdrs"] == [
        {"host": "10.13.1.235", "port": 8073, "latitude": 38.1, "longitude": -78.2, "grid": "FM08so"},
        {"host": "10.13.1.236", "port": 8074, "latitude": 39.1, "longitude": -77.2, "grid": "FM09aa"},
    ]


def test_post_config_reapplies_receivers_when_endpoint_changes(monkeypatch) -> None:
    mgr = _MgrStub()
    auto_set_loop = _AutoSetLoopStub(apply_result=True)
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}, auto_set_loop=auto_set_loop))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "host": "10.13.1.250",
            "port": 8073,
        },
    )

    assert response.status_code == 200
    assert mgr.host == "10.13.1.250"
    assert mgr.port == 8073
    assert auto_set_loop.apply_calls == [(True, True)]
    assert auto_set_loop.notify_calls == 0


def test_post_config_wakes_loop_when_endpoint_reapply_not_available(monkeypatch) -> None:
    mgr = _MgrStub()
    auto_set_loop = _AutoSetLoopStub(apply_result=False)
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}, auto_set_loop=auto_set_loop))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "host": "10.13.1.251",
            "port": 8075,
        },
    )

    assert response.status_code == 200
    assert auto_set_loop.apply_calls == [(True, True)]
    assert auto_set_loop.notify_calls == 1


def test_post_config_can_prune_discovered_kiwis_to_configured(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "kiwisdrs": [
                {"host": "10.13.1.236", "port": 8074, "latitude": 39.1, "longitude": -77.2, "grid": "FM09aa"},
            ],
            "discovered_kiwis": [
                {"host": "10.13.1.235", "port": 8073, "sdr_hw": "KiwiSDR 1"},
                {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2"},
            ],
            "discovery_source": "lan_scan",
            "sync_discovered_to_configured": True,
        },
    )

    assert response.status_code == 200

    config_response = client.get("/config")

    assert config_response.status_code == 200
    payload = config_response.json()
    assert payload["kiwisdrs"] == [
        {"host": "10.13.1.236", "port": 8074, "latitude": 39.1, "longitude": -77.2, "grid": "FM09aa"},
    ]
    assert payload["discovered_kiwis"] == [
        {"host": "10.13.1.236", "port": 8074, "sdr_hw": "KiwiSDR 2"},
    ]


def test_get_config_does_not_deadlock_with_locking_password_helper(monkeypatch) -> None:
    mgr = _LockingMgrStub()
    mgr.password = "user-secret"
    mgr.admin_password = "secret"
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.get("/config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["kiwi_password_set"] is True
    assert payload["kiwi_admin_password_set"] is True


def test_post_config_password_only_does_not_deadlock(monkeypatch) -> None:
    mgr = _LockingMgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={"kiwi_password": "user-secret", "kiwi_admin_password": "secret"},
    )

    assert response.status_code == 200
    assert mgr.has_kiwi_password() is True
    assert mgr.has_kiwi_admin_password() is True


def test_post_config_persists_kiwi_password_state(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "kiwi_password": "user-pass",
        },
    )

    assert response.status_code == 200
    assert mgr.password == "user-pass"
    assert mgr.secret_save_calls == 1

    payload = client.get("/config").json()
    assert payload["kiwi_password_set"] is True

    clear_response = client.post(
        "/config",
        json={
            "clear_kiwi_password": True,
        },
    )

    assert clear_response.status_code == 200
    assert mgr.password is None
    assert mgr.secret_save_calls == 2

    cleared_payload = client.get("/config").json()
    assert cleared_payload["kiwi_password_set"] is False


def test_post_config_persists_kiwi_admin_password_state(monkeypatch) -> None:
    mgr = _MgrStub()
    app = FastAPI()
    app.include_router(config_api.make_router(mgr=mgr, waterholes={"20m": 14074.0}))
    client = TestClient(app)

    monkeypatch.setattr(config_api, "read_kiwi_status", lambda host, port, timeout_s: {})

    response = client.post(
        "/config",
        json={
            "kiwi_admin_password": "secret-pass",
        },
    )

    assert response.status_code == 200
    assert mgr.admin_password == "secret-pass"
    assert mgr.secret_save_calls == 1

    payload = client.get("/config").json()
    assert payload["kiwi_admin_password_set"] is True

    clear_response = client.post(
        "/config",
        json={
            "clear_kiwi_admin_password": True,
        },
    )

    assert clear_response.status_code == 200
    assert mgr.admin_password is None
    assert mgr.secret_save_calls == 2

    cleared_payload = client.get("/config").json()
    assert cleared_payload["kiwi_admin_password_set"] is False