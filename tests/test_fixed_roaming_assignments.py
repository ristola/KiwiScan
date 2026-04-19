from __future__ import annotations

import json
import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api import auto_set as auto_set_api
from kiwi_scan.api import decodes as decodes_api
from kiwi_scan.api.auto_set import make_router
from kiwi_scan.auto_set_loop import AutoSetLoop
from kiwi_scan.receiver_manager import ReceiverAssignment


class _MgrStub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.host = "127.0.0.1"
        self.port = 8073

    def set_runtime_dependencies(self, *_args, **_kwargs) -> None:
        return None


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self.last_assignments: dict[int, ReceiverAssignment] = {}
        self.apply_calls = 0

    def dependency_report(self) -> dict:
        return {}

    def apply_assignments(self, _host: str, _port: int, assignments: dict[int, ReceiverAssignment]) -> None:
        self.apply_calls += 1
        self.last_assignments = dict(assignments)


class _AutoSetLoopStatusStub:
    def __init__(self, hold_reason: str | None = None) -> None:
        self._hold_reason = hold_reason

    def status(self) -> dict[str, object]:
        return {"external_hold_reason": self._hold_reason}


class _AutoSetLoopDirectHoldStub:
    def __init__(self, hold_reason: str | None = None, status_reason: str | None = None) -> None:
        self._state_lock = threading.Lock()
        self._external_hold_reason = hold_reason
        self._status_reason = status_reason

    def status(self) -> dict[str, object]:
        return {"external_hold_reason": self._status_reason}


class _LoopResponse:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self, _size: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fixed_roaming_payload_pins_rx2_to_rx7() -> None:
    loop = AutoSetLoop()

    payload = loop._build_fixed_roaming_payload({}, "day")

    fixed = payload.get("fixed_assignments")
    assert isinstance(fixed, list)
    assert fixed == [
        {"rx": 2, "band": "20m", "mode": "FT4 / FT8", "freq_hz": 14_077_000.0},
        {"rx": 3, "band": "20m", "mode": "WSPR", "freq_hz": 14_095_600.0},
        {"rx": 4, "band": "40m", "mode": "FT8", "freq_hz": 7_074_000.0},
        {"rx": 5, "band": "40m", "mode": "FT4 / WSPR", "freq_hz": 7_043_050.0},
        {"rx": 6, "band": "30m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 10_138_000.0},
        {"rx": 7, "band": "17m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 18_102_000.0},
    ]
    assert payload.get("selected_bands") == ["10m", "12m"]
    # band_modes covers the full roaming pool for the block (all 3 day bands).
    assert payload.get("band_modes") == {"10m": "FT8", "12m": "FT8", "15m": "FT8"}
    assert set(payload.get("selected_bands", [])) <= {"10m", "12m", "15m"}


def test_fixed_roaming_payload_night_uses_only_night_roaming_pool() -> None:
    loop = AutoSetLoop()

    payload = loop._build_fixed_roaming_payload({}, "night")

    assert payload.get("selected_bands") == ["60m", "80m"]
    assert payload.get("band_modes") == {"60m": "FT8", "80m": "FT4 / FT8", "160m": "WSPR"}
    assert set(payload.get("selected_bands", [])) <= {"60m", "80m", "160m"}


def test_fixed_roaming_payload_passes_current_roaming_to_smart_scheduler(monkeypatch) -> None:
    loop = AutoSetLoop()

    class _SmartSchedulerStub:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], list[str]]] = []

        def rank_roaming_bands(self, available_bands: list[str], current_roaming: list[str]) -> list[str]:
            self.calls.append((list(available_bands), list(current_roaming)))
            return ["15m", "12m", "10m"]

    scheduler = _SmartSchedulerStub()
    loop.set_smart_scheduler(scheduler)
    monkeypatch.setattr(loop, "_current_roaming_bands", lambda: ["10m", "12m"])

    payload = loop._build_fixed_roaming_payload({}, "day")

    assert scheduler.calls == [(["10m", "12m", "15m"], ["10m", "12m"])]
    assert payload.get("selected_bands") == ["15m", "12m"]


def test_fixed_roaming_payload_excludes_closed_bands(monkeypatch) -> None:
    loop = AutoSetLoop()

    class _SmartSchedulerStub:
        def __init__(self) -> None:
            self.calls: list[tuple[list[str], list[str]]] = []

        def get_closed_bands(self, _mode: str = "ft8") -> set[str]:
            return {"10m"}

        def rank_roaming_bands(self, available_bands: list[str], current_roaming: list[str]) -> list[str]:
            self.calls.append((list(available_bands), list(current_roaming)))
            return ["15m", "12m"]

    scheduler = _SmartSchedulerStub()
    loop.set_smart_scheduler(scheduler)
    monkeypatch.setattr(loop, "_current_roaming_bands", lambda: ["10m", "12m"])

    payload = loop._build_fixed_roaming_payload({}, "day")

    assert scheduler.calls == [(["12m", "15m"], ["10m", "12m"])]
    assert payload.get("selected_bands") == ["15m", "12m"]
    assert payload.get("band_modes") == {"12m": "FT8", "15m": "FT8"}
    assert payload.get("closed_bands") == ["10m"]


def test_fixed_mode_only_uses_rx0_rx1_for_roaming(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)
    monkeypatch.setattr(
        auto_set_api,
        "_load_automation_settings",
        lambda: {
            "fixedModeEnabled": False,
            "headlessEnabled": True,
        },
    )

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            band_order=["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"],
            band_freqs_hz={
                "10m": 28_074_000.0,
                "15m": 21_074_000.0,
            },
            band_ft4_freqs_hz={},
            band_wspr_freqs_hz={},
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "ft8",
            "block": "day",
            "selected_bands": ["15m", "10m"],
            "band_modes": {
                "15m": "FT8",
                "10m": "FT8",
            },
            "fixed_assignments": [
                {"rx": 2, "band": "20m", "mode": "FT4 / FT8", "freq_hz": 14_077_000.0},
                {"rx": 3, "band": "20m", "mode": "WSPR", "freq_hz": 14_095_600.0},
                {"rx": 4, "band": "40m", "mode": "FT8", "freq_hz": 7_074_000.0},
                {"rx": 5, "band": "40m", "mode": "FT4 / WSPR", "freq_hz": 7_043_050.0},
                {"rx": 6, "band": "30m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 10_138_000.0},
                {"rx": 7, "band": "17m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 18_102_000.0},
            ],
        },
    )

    assert response.status_code == 200
    assert sorted(receiver_mgr.last_assignments.keys()) == list(range(8))

    roaming_assignments = {
        int(rx): (receiver_mgr.last_assignments[rx].band, receiver_mgr.last_assignments[rx].mode_label)
        for rx in (0, 1)
    }
    assert set(roaming_assignments.keys()) == {0, 1}
    assert set(roaming_assignments.values()) == {("10m", "FT8"), ("15m", "FT8")}
    assert receiver_mgr.last_assignments[0].ignore_slot_check is False
    assert receiver_mgr.last_assignments[1].ignore_slot_check is False

    expected_fixed = {
        2: ("20m", "FT4 / FT8"),
        3: ("20m", "WSPR"),
        4: ("40m", "FT8"),
        5: ("40m", "FT4 / WSPR"),
        6: ("30m", "FT4 / FT8 / WSPR"),
        7: ("17m", "FT4 / FT8 / WSPR"),
    }
    for rx, (band, mode_label) in expected_fixed.items():
        assignment = receiver_mgr.last_assignments[rx]
        assert assignment.band == band
        assert assignment.mode_label == mode_label
        assert assignment.ignore_slot_check is True  # fixed workers float to any Kiwi slot


def test_auto_set_suppressed_while_external_hold_active(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            auto_set_loop=_AutoSetLoopStatusStub("receiver_scan"),
            band_order=["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"],
            band_freqs_hz={
                "10m": 28_074_000.0,
                "15m": 21_074_000.0,
            },
            band_ft4_freqs_hz={},
            band_wspr_freqs_hz={},
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "ft8",
            "block": "day",
            "selected_bands": ["15m", "10m"],
            "band_modes": {
                "15m": "FT8",
                "10m": "FT8",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["held"] is True
    assert payload["hold_reason"] == "receiver_scan"
    assert receiver_mgr.apply_calls == 0
    assert receiver_mgr.last_assignments == {}


def test_auto_set_suppressed_while_direct_hold_flag_is_active(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            auto_set_loop=_AutoSetLoopDirectHoldStub("caption_monitor", None),
            band_order=["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"],
            band_freqs_hz={
                "10m": 28_074_000.0,
                "15m": 21_074_000.0,
            },
            band_ft4_freqs_hz={},
            band_wspr_freqs_hz={},
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "ft8",
            "block": "day",
            "selected_bands": ["15m", "10m"],
            "band_modes": {
                "15m": "FT8",
                "10m": "FT8",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["held"] is True
    assert payload["hold_reason"] == "caption_monitor"
    assert receiver_mgr.apply_calls == 0
    assert receiver_mgr.last_assignments == {}


def test_fixed_mode_roaming_drops_bands_reserved_by_fixed_assignments(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            band_order=["17m", "20m", "30m", "40m", "60m", "80m"],
            band_freqs_hz={
                "17m": 18_100_000.0,
                "40m": 7_074_000.0,
                "60m": 5_357_000.0,
                "80m": 3_573_000.0,
            },
            band_ft4_freqs_hz={
                "80m": 3_575_000.0,
            },
            band_wspr_freqs_hz={},
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "ft8",
            "block": "night",
            "selected_bands": ["17m", "40m", "60m", "80m"],
            "band_modes": {
                "17m": "FT8",
                "40m": "FT8",
                "60m": "FT8",
                "80m": "FT4 / FT8",
            },
            "fixed_assignments": [
                {"rx": 2, "band": "40m", "mode": "FT8", "freq_hz": 7_074_000.0},
                {"rx": 7, "band": "17m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 18_102_000.0},
            ],
        },
    )

    assert response.status_code == 200
    roaming_assignments = {
        int(rx): receiver_mgr.last_assignments[rx].band
        for rx in receiver_mgr.last_assignments
        if int(rx) < 2
    }

    assert set(roaming_assignments.keys()) == {0, 1}
    assert set(roaming_assignments.values()) == {"60m", "80m"}
    assert all(band not in {"17m", "40m"} for band in roaming_assignments.values())
    assert receiver_mgr.last_assignments[0].ignore_slot_check is False
    assert receiver_mgr.last_assignments[1].ignore_slot_check is False


def test_fixed_receivers_healthy_requires_correct_kiwi_slots(monkeypatch) -> None:
    loop = AutoSetLoop()

    channels = {
        str(rx): {
            "active": True,
            "visible_on_kiwi": True,
            "status_level": "healthy",
            "kiwi_rx": rx,
        }
        for rx in range(2, 8)
    }
    channels["7"]["kiwi_rx"] = 6

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        if url.endswith("/system/info"):
            return _LoopResponse({
                "kiwi": {
                    "raw_users": [
                        {"rx": 2, "name": "AUTO_20m_FT8"},
                        {"rx": 3, "name": "AUTO_20m_WS"},
                        {"rx": 4, "name": "AUTO_40m_FT8"},
                        {"rx": 5, "name": "AUTO_40m_FT4"},
                        {"rx": 6, "name": "AUTO_30m_FT8"},
                        {"rx": 7, "name": "AUTO_17m_FT8"},
                    ]
                }
            })
        assert url.endswith("/health/rx")
        return _LoopResponse({"channels": channels})

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    assert loop._fixed_receivers_healthy() is False


def test_fixed_receivers_healthy_requires_correct_kiwi_occupants(monkeypatch) -> None:
    loop = AutoSetLoop()

    channels = {
        str(rx): {
            "active": True,
            "visible_on_kiwi": True,
            "status_level": "healthy",
            "kiwi_rx": rx,
        }
        for rx in range(2, 8)
    }

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        if url.endswith("/system/info"):
            return _LoopResponse({
                "kiwi": {
                    "raw_users": [
                        {"rx": 2, "name": "AUTO_17m_FT8"},
                        {"rx": 3, "name": "AUTO_20m_WS"},
                        {"rx": 4, "name": "AUTO_40m_FT8"},
                        {"rx": 5, "name": "AUTO_40m_FT4"},
                        {"rx": 6, "name": "AUTO_30m_FT8"},
                        {"rx": 7, "name": "AUTO_20m_FT8"},
                    ]
                }
            })
        assert url.endswith("/health/rx")
        return _LoopResponse({"channels": channels})

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    assert loop._fixed_receivers_healthy() is False


def test_sick_fixed_receivers_ignores_cached_empty_health_snapshot(monkeypatch) -> None:
    loop = AutoSetLoop()

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        assert url.endswith("/health/rx")
        return _LoopResponse({"overall": "idle", "channels": {}, "_from_cache": True})

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    assert loop._sick_fixed_receivers() == []


def test_fixed_health_state_treats_cached_nonempty_snapshot_as_unknown(monkeypatch) -> None:
    loop = AutoSetLoop()

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        assert url.endswith("/health/rx")
        return _LoopResponse(
            {
                "overall": "healthy",
                "_from_cache": True,
                "channels": {
                    str(rx): {"active": True, "status_level": "healthy"}
                    for rx in range(2, 8)
                },
            }
        )

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    state, sick = loop._fixed_health_state()

    assert state == "unknown"
    assert sick == [
        {"rx": 2, "band": "20m", "mode": "FT4 / FT8", "freq_hz": 14_077_000.0},
        {"rx": 3, "band": "20m", "mode": "WSPR", "freq_hz": 14_095_600.0},
        {"rx": 4, "band": "40m", "mode": "FT8", "freq_hz": 7_074_000.0},
        {"rx": 5, "band": "40m", "mode": "FT4 / WSPR", "freq_hz": 7_043_050.0},
        {"rx": 6, "band": "30m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 10_138_000.0},
        {"rx": 7, "band": "17m", "mode": "FT4 / FT8 / WSPR", "freq_hz": 18_102_000.0},
    ]


def test_fixed_roaming_payload_keeps_fallback_roaming_when_health_is_unknown(monkeypatch) -> None:
    loop = AutoSetLoop()

    monkeypatch.setattr(loop, "_fixed_health_state", lambda: ("unknown", []))

    payload = loop._build_fixed_roaming_payload({}, "night")

    assert payload.get("selected_bands") == ["60m", "80m"]


def test_roaming_health_state_reports_sick_missing_roam(monkeypatch) -> None:
    loop = AutoSetLoop()

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        assert url.endswith("/health/rx")
        return _LoopResponse(
            {
                "overall": "degraded",
                "channels": {
                    "0": {"active": False, "visible_on_kiwi": False, "status_level": "fault"},
                    "1": {"active": True, "visible_on_kiwi": True, "status_level": "healthy"},
                },
            }
        )

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    state, sick = loop._roaming_health_state()

    assert state == "sick"
    assert sick == [0]


def test_roaming_health_state_treats_cached_health_as_unknown(monkeypatch) -> None:
    loop = AutoSetLoop()

    def _fake_urlopen(request, timeout=0.0):
        del timeout
        url = request.full_url if hasattr(request, "full_url") else str(request)
        assert url.endswith("/health/rx")
        return _LoopResponse({"overall": "healthy", "_from_cache": True, "channels": {}})

    monkeypatch.setattr("kiwi_scan.auto_set_loop.urllib.request.urlopen", _fake_urlopen)

    state, sick = loop._roaming_health_state()

    assert state == "unknown"
    assert sick == [0, 1]


def test_ws4010_apply_preserves_fixed_assignments_in_fixed_mode(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b'{"ok": true}'

    def _fake_urlopen(request, timeout=0):
        del timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return _Response()

    monkeypatch.setattr(decodes_api.urllib_request, "urlopen", _fake_urlopen)

    result = decodes_api._trigger_auto_set_apply(
        {
            "fixedModeEnabled": True,
            "headlessEnabled": True,
        },
        "ft8",
    )

    assert result["ok"] is True
    payload = captured["payload"]
    assert isinstance(payload, dict)
    fixed = payload.get("fixed_assignments")
    assert isinstance(fixed, list)
    assert [row.get("rx") for row in fixed] == [2, 3, 4, 5, 6, 7]
    assert payload.get("enabled") is True


def test_ws4010_phone_profile_maps_to_ft8_schedule(monkeypatch) -> None:
    saved: list[dict] = []

    monkeypatch.setattr(decodes_api, "_load_automation_settings", lambda: {"scheduleProfiles": {}})
    monkeypatch.setattr(decodes_api, "_save_automation_settings", lambda payload: saved.append(payload))
    monkeypatch.setattr(decodes_api, "_trigger_auto_set_apply", lambda settings, mode: {"ok": True, "mode": mode})

    result = decodes_api._apply_ws4010_band_command(
        {
            "enabled": True,
            "profile": "phone",
            "band": "20m",
        }
    )

    assert result["mode"] == "ft8"
    assert saved
    assert "ft8" in saved[-1]["scheduleProfiles"]
    assert "phone" not in saved[-1]["scheduleProfiles"]


def test_force_reassign_skips_when_external_hold_active(monkeypatch) -> None:
    loop = AutoSetLoop()
    loop.pause_for_external("receiver_scan")

    monkeypatch.setattr(loop, "_load_settings", lambda: {"fixedModeEnabled": True})
    monkeypatch.setattr(
        loop,
        "_build_payload",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("build should not be called")),
    )

    posted: list[dict] = []
    monkeypatch.setattr(loop, "_post_auto_set", lambda payload: posted.append(dict(payload)))

    loop.force_reassign()

    assert posted == []
    assert loop.status()["external_hold_reason"] == "receiver_scan"


def test_force_reassign_posts_payload_when_not_held(monkeypatch) -> None:
    loop = AutoSetLoop()
    loop._last_apply_signature = "sig"
    loop._last_schedule_key = ("ft8", "00-04")
    loop._last_applied_band_config = "bands"

    monkeypatch.setattr(loop, "_load_settings", lambda: {"fixedModeEnabled": True})
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings: ("ft8", "00-04"))
    monkeypatch.setattr(loop, "_build_payload", lambda _settings, schedule_key=None: {"enabled": True, "schedule_key": schedule_key})

    posted: list[dict] = []
    monkeypatch.setattr(loop, "_post_auto_set", lambda payload: posted.append(dict(payload)))

    loop.force_reassign()

    assert posted == [{"enabled": True, "schedule_key": ("ft8", "00-04"), "force": True}]
    assert loop._last_apply_signature is None
    assert loop._last_schedule_key is None
    assert loop._last_applied_band_config is None


def test_run_reapplies_when_scored_band_config_changes_without_schedule_change(monkeypatch) -> None:
    loop = AutoSetLoop()
    loop._did_startup_apply = True
    loop._last_schedule_key = ("fixed", "day")
    loop._last_apply_signature = "sig"
    loop._last_applied_band_config = json.dumps(
        {"bands": ["10m", "12m"], "modes": {"10m": "FT8", "12m": "FT8", "15m": "FT8"}, "closed": []},
        separators=(",", ":"),
    )
    loop._recovery_backoff_until_ts = 0.0

    settings = {"headlessEnabled": True, "fixedModeEnabled": True}
    payload = {
        "enabled": True,
        "mode": "ft8",
        "block": "day",
        "selected_bands": ["10m", "15m"],
        "band_modes": {"10m": "FT8", "12m": "FT8", "15m": "FT8"},
    }

    monkeypatch.setattr(loop, "_load_settings", lambda: settings)
    monkeypatch.setattr(loop, "_current_schedule_key", lambda _settings: ("fixed", "day"))
    monkeypatch.setattr(loop, "_apply_signature", lambda _settings, _schedule_key: "sig")
    monkeypatch.setattr(loop, "_fixed_health_state", lambda: ("healthy", []))
    monkeypatch.setattr(loop, "_roaming_health_state", lambda: ("healthy", []))
    monkeypatch.setattr(loop, "_build_payload", lambda _settings, schedule_key=None: dict(payload))

    posted: list[dict] = []
    monkeypatch.setattr(loop, "_post_auto_set", lambda posted_payload: posted.append(dict(posted_payload)))
    monkeypatch.setattr(loop, "_wait_for_notification", lambda timeout_s=None: loop._stop.set())

    loop._run()

    assert posted == [payload]
    assert loop._last_schedule_key == ("fixed", "day")
    assert loop._last_apply_signature == "sig"
    assert loop._last_applied_band_config == loop._band_config_signature(payload)


def test_non_fixed_payload_ignores_schedule_block_for_assignments(monkeypatch) -> None:
    # Freeze time to 14:00 so block_for_hour returns "10-16", which is not defined
    # in the test profile → fallback lands on the nearest prior block "00-04".
    # Without this the test is time-sensitive and fails between 20:00 and 24:00.
    import kiwi_scan.auto_set_loop as _asl_mod
    from datetime import datetime as _real_dt, timezone as _tz
    class _FakeDT(_real_dt):
        @classmethod
        def now(cls, tz=None):
            return _real_dt(2026, 4, 10, 14, 0, 0, tzinfo=_tz.utc).astimezone()
    monkeypatch.setattr(_asl_mod, "datetime", _FakeDT)

    loop = AutoSetLoop()
    settings = {
        "fixedModeEnabled": False,
        "autoScanMode": "ft8",
        "scheduleProfiles": {
            "ft8": {
                "00-04": {
                    "selectedBands": ["17m", "30m"],
                    "bandModes": {
                        "17m": "FT4 / FT8 / WSPR",
                        "30m": "FT4 / FT8 / WSPR",
                    },
                },
                "20-24": {
                    "selectedBands": ["10m"],
                    "bandModes": {"10m": "FT8"},
                },
            }
        },
    }

    payload_a = loop._build_payload(settings, schedule_key=("ft8", "00-04"))
    payload_b = loop._build_payload(settings, schedule_key=("ft8", "20-24"))

    assert payload_a["selected_bands"] == ["17m", "30m"]
    assert payload_b["selected_bands"] == ["17m", "30m"]
    assert payload_a["band_modes"] == payload_b["band_modes"]


def test_fixed_mode_endpoint_overrides_nonfixed_caller_payload(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)
    monkeypatch.setattr(
        auto_set_api,
        "_load_automation_settings",
        lambda: {
            "fixedModeEnabled": True,
            "headlessEnabled": True,
        },
    )

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            band_order=["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"],
            band_freqs_hz={
                "10m": 28_074_000.0,
                "15m": 21_074_000.0,
                "20m": 14_074_000.0,
                "30m": 10_136_000.0,
                "40m": 7_074_000.0,
                "60m": 5_357_000.0,
                "80m": 3_573_000.0,
                "160m": 1_840_000.0,
            },
            band_ft4_freqs_hz={
                "17m": 18_104_000.0,
                "20m": 14_080_000.0,
                "30m": 10_140_000.0,
                "40m": 7_047_500.0,
                "80m": 3_575_000.0,
            },
            band_wspr_freqs_hz={
                "17m": 18_104_600.0,
                "20m": 14_095_600.0,
                "30m": 10_138_700.0,
                "40m": 7_038_600.0,
                "160m": 1_836_600.0,
            },
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "ft8",
            "block": "ignored",
            "selected_bands": ["10m"],
            "band_modes": {"10m": "FT8"},
        },
    )

    assert response.status_code == 200
    assert receiver_mgr.last_assignments[2].band == "20m"
    assert receiver_mgr.last_assignments[2].mode_label == "FT4 / FT8"
    assert receiver_mgr.last_assignments[3].band == "20m"
    assert receiver_mgr.last_assignments[3].mode_label == "WSPR"


def test_auto_set_rejects_phone_mode(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)
    monkeypatch.setattr(
        auto_set_api,
        "_load_automation_settings",
        lambda: {"fixedModeEnabled": False, "headlessEnabled": True},
    )

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            band_order=["20m"],
            band_freqs_hz={"20m": 14_074_000.0},
            band_ft4_freqs_hz={},
            band_wspr_freqs_hz={},
        )
    )

    client = TestClient(app)
    response = client.post(
        "/auto_set_receivers",
        json={
            "enabled": True,
            "force": True,
            "mode": "phone",
            "block": "00-04",
            "selected_bands": ["20m"],
            "band_modes": {"20m": "SSB"},
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "mode must be 'ft8'"}
    assert receiver_mgr.last_assignments == {}
