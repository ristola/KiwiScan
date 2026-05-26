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


def test_automation_settings_post_can_skip_auto_set_loop_notify(monkeypatch) -> None:
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

    response = client.post("/automation/settings?notify_auto_set=0", json={"receiversMode": "auto"})

    assert response.status_code == 200
    assert loop_stub.notifications == 0
    assert saved
    assert saved[-1]["receiversMode"] == "auto"


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


def test_automation_settings_store_kiwi_profiles(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post(
        "/automation/settings",
        json={
            "kiwiProfiles": {
                "10.13.73.235:8073": {
                    "receiversMode": "semi",
                    "overviewTopics": {
                        "status": True,
                        "assignments": False,
                        "faults": True,
                        "messages": True,
                        "map": False,
                        "active-receivers": True,
                        "receiver-scan": False,
                        "order": ["messages", "status", "messages", "map"],
                    },
                }
            }
        },
    )

    assert response.status_code == 200
    assert saved
    assert saved[-1]["kiwiProfiles"] == {
        "10.13.73.235:8073": {
            "receiversMode": "semi",
            "overviewTopics": {
                "status": True,
                "assignments": False,
                "faults": True,
                "messages": True,
                "map": False,
                "active-receivers": True,
                "receiver-scan": False,
                "order": ["messages", "status", "map"],
            },
        }
    }


def test_automation_settings_drop_invalid_kiwi_profiles(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post(
        "/automation/settings",
        json={
            "kiwiProfiles": {
                "": {"receiversMode": "auto"},
                "10.13.73.236:8073": {
                    "receiversMode": "bogus",
                    "overviewTopics": {"invalid": True},
                },
                "10.13.73.237:8073": "bad-profile",
            }
        },
    )

    assert response.status_code == 200
    assert saved
    assert saved[-1]["kiwiProfiles"] == {}


def test_automation_settings_store_active_kiwi_key_and_kiwi_modes(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(automation_api, "_configured_kiwi_keys_from_config", lambda: None)
    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post(
        "/automation/settings",
        json={
            "activeKiwiKey": " 10.13.73.235:8073 ",
            "kiwiModes": {
                "10.13.73.235:8073": "semi",
                "10.13.73.236:8073": "bogus",
                "": "manual",
            },
        },
    )

    assert response.status_code == 200
    assert saved
    assert saved[-1]["activeKiwiKey"] == "10.13.73.235:8073"
    assert saved[-1]["kiwiModes"] == {"10.13.73.235:8073": "semi"}


def test_automation_settings_prune_keys_not_in_configured_kiwis(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(
        automation_api,
        "_configured_kiwi_keys_from_config",
        lambda: {"10.13.73.235:8073", "10.13.73.236:8073"},
    )
    monkeypatch.setattr(automation_api, "_load_settings", lambda: {"headlessEnabled": True})
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.post(
        "/automation/settings",
        json={
            "activeKiwiKey": "10.123.73.61:8073",
            "kiwiModes": {
                "10.13.73.235:8073": "auto",
                "10.123.73.61:8073": "manual",
            },
            "kiwiProfiles": {
                "10.13.73.236:8073": {"receiversMode": "semi"},
                "10.123.73.61:8073": {"receiversMode": "manual"},
            },
        },
    )

    assert response.status_code == 200
    assert saved
    assert saved[-1]["activeKiwiKey"] == ""
    assert saved[-1]["kiwiModes"] == {"10.13.73.235:8073": "auto"}
    assert saved[-1]["kiwiProfiles"] == {
        "10.13.73.236:8073": {"receiversMode": "semi"},
    }


def test_automation_settings_get_prunes_stale_keys_after_config_cleanup(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(
        automation_api,
        "_configured_kiwi_keys_from_config",
        lambda: {"10.13.73.235:8073", "10.13.73.236:8073"},
    )
    monkeypatch.setattr(
        automation_api,
        "_load_settings",
        lambda: {
            "headlessEnabled": True,
            "activeKiwiKey": "10.123.73.61:8073",
            "kiwiModes": {
                "10.13.73.235:8073": "auto",
                "10.123.73.61:8073": "auto",
            },
            "kiwiProfiles": {
                "10.13.73.236:8073": {"receiversMode": "semi"},
                "10.123.73.61:8073": {"receiversMode": "manual"},
            },
        },
    )
    monkeypatch.setattr(automation_api, "_save_settings", lambda payload: saved.append(dict(payload)))

    app = FastAPI()
    app.include_router(automation_api.make_router())
    client = TestClient(app)

    response = client.get("/automation/settings")

    assert response.status_code == 200
    assert response.json()["activeKiwiKey"] == ""
    assert response.json()["kiwiModes"] == {"10.13.73.235:8073": "auto"}
    assert response.json()["kiwiProfiles"] == {
        "10.13.73.236:8073": {"receiversMode": "semi"},
    }
    assert saved


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
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings, kiwi_key=None: ("ft8", "00-04"))
    monkeypatch.setattr(loop, "_apply_signature", lambda _settings, _schedule_key, kiwi_key=None: "sig")
    monkeypatch.setattr(
        loop,
        "_build_payload",
        lambda _settings, schedule_key=None, kiwi_key=None: {"enabled": True, "force": True},
    )

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


def test_auto_set_loop_apply_current_settings_uses_active_kiwi_mode(monkeypatch) -> None:
    loop = AutoSetLoop()

    monkeypatch.setattr(
        loop,
        "_load_settings",
        lambda: {
            "fixedModeEnabled": False,
            "receiversMode": "manual",
            "activeKiwiKey": "kiwi-1",
            "kiwiModes": {"kiwi-1": "auto"},
        },
    )
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings, kiwi_key=None: ("fixed", "day"))
    monkeypatch.setattr(loop, "_apply_signature", lambda _settings, _schedule_key, kiwi_key=None: "sig")
    monkeypatch.setattr(loop, "_build_payload", lambda _settings, schedule_key=None, kiwi_key=None: {"enabled": True})

    posted: list[dict[str, object]] = []
    monkeypatch.setattr(loop, "_post_auto_set", lambda payload: posted.append(dict(payload)))

    applied = loop.apply_current_settings(force=True, sync_state=True)

    assert applied is True
    assert posted == [{"enabled": True, "kiwi_key": "kiwi-1", "force": True}]


def test_auto_set_loop_manual_kiwi_does_not_block_other_auto_kiwi(monkeypatch) -> None:
    loop = AutoSetLoop()
    current_settings = {
        "headlessEnabled": True,
        "fixedModeEnabled": True,
        "receiversMode": "auto",
        "activeKiwiKey": "kiwi-2",
        "kiwiModes": {
            "kiwi-1": "auto",
            "kiwi-2": "manual",
        },
    }
    posted_payloads: list[dict[str, object]] = []
    cleared_once = threading.Event()
    auto_applied = threading.Event()

    monkeypatch.setattr(loop, "_load_settings", lambda: dict(current_settings))
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings, kiwi_key=None: ("fixed", "day"))
    monkeypatch.setattr(loop, "_apply_signature", lambda _settings, _schedule_key, kiwi_key=None: "sig")
    monkeypatch.setattr(
        loop,
        "_build_payload",
        lambda _settings, schedule_key=None, kiwi_key=None: {"enabled": True, "force": True},
    )

    def _fake_post(payload: dict[str, object]) -> None:
        posted_payloads.append(dict(payload))
        if payload.get("enabled") is False and payload.get("kiwi_key") == "kiwi-2":
            cleared_once.set()
        if payload.get("enabled") is True and payload.get("kiwi_key") == "kiwi-1":
            auto_applied.set()
        if cleared_once.is_set() and auto_applied.is_set():
            loop.stop()

    monkeypatch.setattr(loop, "_post_auto_set", _fake_post)

    worker = threading.Thread(target=loop._run, daemon=True)
    worker.start()

    assert cleared_once.wait(timeout=1.0)
    assert auto_applied.wait(timeout=1.0)
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert posted_payloads == [
        {"enabled": True, "force": True, "kiwi_key": "kiwi-1"},
        {"enabled": False, "force": True, "kiwi_key": "kiwi-2"},
    ]