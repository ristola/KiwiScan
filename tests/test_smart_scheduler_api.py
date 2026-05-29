from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.smart_scheduler import make_router
from kiwi_scan.smart_scheduler import SmartScheduler


class _ReceiverMgrStub:
    def health_summary(self):
        return {"overall": "healthy", "channels": {}}


class _DiscoveryMgrStub:
    def __init__(self) -> None:
        import threading

        self.lock = threading.Lock()
        self.host = "default.local"
        self.port = 8073

    def resolve_runtime_target(self, *, kiwi_key=None):
        if str(kiwi_key or "").strip() == "kiwi-2":
            return {"host": "kiwi-2.local", "port": 8073, "kiwi_key": "kiwi-2"}
        return {"host": "default.local", "port": 8073, "kiwi_key": "default"}


def test_smart_scheduler_status_returns_ft8_snapshot() -> None:
    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrStub())
    app = FastAPI()
    app.include_router(make_router(mgr=_DiscoveryMgrStub(), smart_scheduler=scheduler))
    client = TestClient(app)

    response = client.get("/smart_scheduler/status")

    assert response.status_code == 200
    assert response.json()["mode"] == "ft8"
    assert response.json()["conditions"]
    assert all(entry["score"] is None for entry in response.json()["conditions"].values())


def test_smart_scheduler_status_includes_cached_solar_activity(monkeypatch) -> None:
    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrStub())

    monkeypatch.setattr(
        "kiwi_scan.smart_scheduler._fetch_hamqsl_solar_activity",
        lambda timeout_s=4.0: {
            "source": "hamqsl",
            "source_name": "N0NBH",
            "updated": "09 May 2026 1645 GMT",
            "solar_flux": 120.0,
            "k_index": 1.0,
            "hf_conditions": {
                "80m-40m": {"day": "Fair", "night": "Good"},
                "30m-20m": {"day": "Good", "night": "Good"},
            },
            "hf_score_day": 68,
            "hf_score_night": 80,
        },
    )

    scheduler._refresh_solar_activity(force=True)
    status = scheduler.get_status()

    assert status["solar_activity"]["source_name"] == "N0NBH"
    assert status["solar_activity"]["solar_flux"] == 120.0
    assert status["solar_activity"]["hf_conditions"]["80m-40m"]["night"] == "Good"


def test_smart_scheduler_status_scores_quiet_current_roaming_band_without_empirical() -> None:
    class _ReceiverMgrQuietRoamingStatusStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "0": {
                        "band": "12m",
                        "mode": "FT8",
                        "health_state": "healthy",
                        "propagation_state": "unknown",
                        "decode_rate_per_hour": 0,
                    },
                },
            }

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrQuietRoamingStatusStub())
    status = scheduler.get_status()

    assert status["conditions"]["12m"]["empirical"] is None
    assert status["conditions"]["12m"]["score"] == 0


def test_smart_scheduler_prefers_unused_band_when_current_roaming_is_quiet(monkeypatch) -> None:
    class _ReceiverMgrQuietRoamingStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "0": {"band": "10m", "decode_rate_per_hour": 0},
                    "1": {"band": "12m", "decode_rate_per_hour": 9},
                },
            }

    monkeypatch.setattr("kiwi_scan.smart_scheduler.get_recent_decodes", lambda _seconds: [])

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrQuietRoamingStub())
    ranked = scheduler.rank_roaming_bands(["10m", "12m", "15m"], ["10m", "12m"])

    assert ranked == ["12m", "15m", "10m"]


def test_smart_scheduler_night_pool_uses_lower_quiet_threshold(monkeypatch) -> None:
    class _ReceiverMgrNightRoamingStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "0": {"band": "60m", "decode_rate_per_hour": 8},
                    "1": {"band": "160m", "decode_rate_per_hour": 2},
                },
            }

    monkeypatch.setattr("kiwi_scan.smart_scheduler.get_recent_decodes", lambda _seconds: [])

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrNightRoamingStub())
    ranked = scheduler.rank_roaming_bands(["60m", "80m", "160m"], ["60m", "160m"])

    assert ranked == ["60m", "160m", "80m"]


def test_smart_scheduler_ignores_non_roaming_bands_in_rank_inputs(monkeypatch) -> None:
    class _ReceiverMgrQuietRoamingStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "0": {"band": "20m", "decode_rate_per_hour": 20},
                    "1": {"band": "12m", "decode_rate_per_hour": 9},
                },
            }

    monkeypatch.setattr("kiwi_scan.smart_scheduler.get_recent_decodes", lambda _seconds: [])

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrQuietRoamingStub())
    ranked = scheduler.rank_roaming_bands(["20m", "10m", "12m", "15m"], ["20m", "12m"])

    assert ranked == ["12m", "10m", "15m"]
    status = scheduler.get_status()
    assert status["roaming_decision"]["available_bands"] == ["10m", "12m", "15m"]
    assert status["roaming_decision"]["current_roaming"] == ["12m"]


def test_smart_scheduler_status_includes_last_roaming_decision(monkeypatch) -> None:
    class _ReceiverMgrQuietRoamingStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "0": {"band": "10m", "decode_rate_per_hour": 0},
                    "1": {"band": "12m", "decode_rate_per_hour": 9},
                },
            }

    monkeypatch.setattr("kiwi_scan.smart_scheduler.get_recent_decodes", lambda _seconds: [])

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrQuietRoamingStub())
    scheduler.rank_roaming_bands(["10m", "12m", "15m"], ["10m", "12m"])
    status = scheduler.get_status()

    assert status["roaming_decision"]["selected_bands"] == ["12m", "15m"]
    assert status["roaming_decision"]["promoted_bands"] == ["15m"]
    assert status["roaming_decision"]["displaced_bands"] == ["10m"]
    assert status["roaming_decision"]["low_rate_bands"] == ["10m"]


def test_smart_scheduler_scores_combo_digital_modes(monkeypatch) -> None:
    class _ReceiverMgrComboModeStub:
        def health_summary(self):
            return {
                "overall": "healthy",
                "channels": {
                    "6": {
                        "band": "30m",
                        "mode": "FT4 / FT8 / WSPR",
                        "health_state": "healthy",
                        "propagation_state": "fair",
                    },
                    "7": {
                        "band": "17m",
                        "mode": "FT4 / WSPR",
                        "health_state": "healthy",
                        "propagation_state": "marginal",
                    },
                },
            }

    monkeypatch.setattr("kiwi_scan.smart_scheduler._fetch_hamqsl_solar_activity", lambda timeout_s=4.0: {})

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrComboModeStub())
    scheduler._check_once()
    status = scheduler.get_status()

    assert status["conditions"]["30m"]["empirical"] == "OPEN"
    assert status["conditions"]["30m"]["score"] is not None
    assert status["conditions"]["17m"]["empirical"] == "MARGINAL"
    assert status["conditions"]["17m"]["score"] is not None


def test_smart_scheduler_status_routes_to_targeted_scheduler() -> None:
    class _SchedulerStub:
        def __init__(self, label: str) -> None:
            self._label = label

        def get_status(self):
            return {"mode": "ft8", "label": self._label, "conditions": {"40m": {}}}

        def get_scan_config(self):
            return {"allowed_bands": ["40m"]}

        def set_scan_config(self, _allowed_bands):
            return None

        def set_override(self, _band, _condition):
            return None

        def clear_override(self, _band):
            return None

        def force_check(self):
            return None

    class _SchedulerRegistryStub:
        def resolve_for_target(self, *, target=None):
            target_key = str((target or {}).get("kiwi_key") or "default")
            return _SchedulerStub(target_key)

    app = FastAPI()
    app.include_router(make_router(mgr=_DiscoveryMgrStub(), smart_scheduler=_SchedulerRegistryStub()))
    client = TestClient(app)

    default_response = client.get("/smart_scheduler/status")
    targeted_response = client.get("/smart_scheduler/status?kiwi_key=kiwi-2")

    assert default_response.status_code == 200
    assert default_response.json()["label"] == "default"
    assert targeted_response.status_code == 200
    assert targeted_response.json()["label"] == "kiwi-2"