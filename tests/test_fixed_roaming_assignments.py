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

    def dependency_report(self) -> dict:
        return {}

    def apply_assignments(self, _host: str, _port: int, assignments: dict[int, ReceiverAssignment]) -> None:
        self.last_assignments = dict(assignments)


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
    assert payload.get("selected_bands") == ["15m", "10m"]
    assert payload.get("band_modes") == {"15m": "FT8", "10m": "FT8"}


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
            band_ssb_freqs_hz={},
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
            "wspr_scan_enabled": False,
            "ssb_scan": {"use_kiwi_snr": False},
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
        assert assignment.ignore_slot_check is False


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


def test_non_fixed_payload_ignores_schedule_block_for_assignments() -> None:
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
            band_ssb_freqs_hz={},
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
            "wspr_scan_enabled": False,
        },
    )

    assert response.status_code == 200
    assert receiver_mgr.last_assignments[2].band == "20m"
    assert receiver_mgr.last_assignments[2].mode_label == "FT4 / FT8"
    assert receiver_mgr.last_assignments[3].band == "20m"
    assert receiver_mgr.last_assignments[3].mode_label == "WSPR"
