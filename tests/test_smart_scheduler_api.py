from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.smart_scheduler import make_router
from kiwi_scan.smart_scheduler import SmartScheduler


class _ReceiverMgrStub:
    def health_summary(self):
        return {"overall": "healthy", "channels": {}}


def test_smart_scheduler_status_returns_ft8_snapshot() -> None:
    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrStub())
    app = FastAPI()
    app.include_router(make_router(smart_scheduler=scheduler))
    client = TestClient(app)

    response = client.get("/smart_scheduler/status")

    assert response.status_code == 200
    assert response.json()["mode"] == "ft8"
    assert response.json()["conditions"]
    assert all(entry["score"] is None for entry in response.json()["conditions"].values())


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


def test_smart_scheduler_scores_combo_digital_modes() -> None:
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

    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrComboModeStub())
    scheduler._check_once()
    status = scheduler.get_status()

    assert status["conditions"]["30m"]["empirical"] == "OPEN"
    assert status["conditions"]["30m"]["score"] is not None
    assert status["conditions"]["17m"]["empirical"] == "MARGINAL"
    assert status["conditions"]["17m"]["score"] is not None