from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api import automation as automation_api
from kiwi_scan.auto_set_loop import AutoSetLoop


def test_automation_settings_post_notifies_auto_set_loop(monkeypatch) -> None:
    saved: list[dict] = []

    class _LoopStub:
        def __init__(self) -> None:
            self.notifications = 0

        def notify_settings_changed(self) -> None:
            self.notifications += 1

    loop_stub = _LoopStub()
    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router(auto_set_loop=loop_stub))
    client = TestClient(app)

    response = client.post("/automation/settings", json={"fixedModeEnabled": False})

    assert response.status_code == 200
    assert loop_stub.notifications == 1
    assert saved
    assert saved[-1]["fixedModeEnabled"] is False


def test_automation_settings_normalize_receivers_mode(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post("/automation/settings", json={"receiversMode": "scan"})

    assert response.status_code == 200
    assert saved
    assert saved[-1]["receiversMode"] == "scan"


def test_automation_settings_normalize_semi_receivers_mode(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post("/automation/settings", json={"receiversMode": "semi"})

    assert response.status_code == 200
    assert saved
    assert saved[-1]["receiversMode"] == "semi"


def test_automation_settings_drop_invalid_receivers_mode(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post("/automation/settings", json={"receiversMode": "bogus"})

    assert response.status_code == 200
    assert saved
    assert saved[-1]["receiversMode"] == "auto"


def test_automation_settings_strip_deprecated_legacy_keys(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(
        automation_api,
        "_load_settings",
        lambda: {
            "headlessEnabled": True,
            "autoScanWspr": True,
            "alertsEnabled": True,
            "alertThreshold": 12,
            "bandHopMinutes": 60,
            "bandHopSeconds": 120,
            "quietEnd": "06:00",
            "quietStart": "22:00",
            "scheduleProfiles": {
                "ft8": {"00-04": {"selectedBands": ["20m"], "bandModes": {"20m": "FT8"}}},
                "phone": {"00-06": {"selectedBands": ["40m"], "bandModes": {"40m": "SSB"}}},
            },
            "wsprStartBand": "20m",
            "wsprHopState": {"active_band": "20m"},
        },
    )
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post(
        "/automation/settings",
        json={"fixedModeEnabled": False, "autoScanWspr": True, "alertsEnabled": False, "bandHopSeconds": 90},
    )

    assert response.status_code == 200
    assert saved
    assert "autoScanWspr" not in saved[-1]
    assert "alertsEnabled" not in saved[-1]
    assert "alertThreshold" not in saved[-1]
    assert "bandHopMinutes" not in saved[-1]
    assert "bandHopSeconds" not in saved[-1]
    assert "quietEnd" not in saved[-1]
    assert "quietStart" not in saved[-1]
    assert saved[-1]["scheduleProfiles"] == {
        "ft8": {"00-04": {"selectedBands": ["20m"], "bandModes": {"20m": "FT8"}}}
    }
    assert "wsprStartBand" not in saved[-1]
    assert "wsprHopState" not in saved[-1]


def test_auto_set_loop_manual_mode_clears_once_then_parks(monkeypatch) -> None:
    loop = AutoSetLoop()
    current_settings = {"headlessEnabled": True, "fixedModeEnabled": False}
    posted_payloads: list[dict[str, object]] = []
    cleared_once = threading.Event()
    auto_applied = threading.Event()

    monkeypatch.setattr(loop, "_load_settings", lambda: dict(current_settings))
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings: ("ft8", "00-04"))
    monkeypatch.setattr(loop, "_apply_signature", lambda _settings, _schedule_key: "sig")
    monkeypatch.setattr(loop, "_build_payload", lambda _settings, schedule_key=None: {"enabled": True, "force": True})

    def _fake_post(payload: dict[str, object]) -> None:
        posted_payloads.append(dict(payload))
        if payload.get("enabled") is False:
            cleared_once.set()
            return
        auto_applied.set()
        loop.stop()

    monkeypatch.setattr(loop, "_post_auto_set", _fake_post)

    worker = threading.Thread(target=loop._run, daemon=True)
    worker.start()

    assert cleared_once.wait(timeout=1.0)
    assert posted_payloads == [{"enabled": False, "force": True}]

    current_settings["fixedModeEnabled"] = True
    loop.notify_settings_changed()

    assert auto_applied.wait(timeout=1.0)
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert posted_payloads == [
        {"enabled": False, "force": True},
        {"enabled": True, "force": True},
    ]