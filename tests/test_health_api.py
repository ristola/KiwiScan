from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.health import make_router


class _ReceiverMgrStub:
    def health_summary(self) -> dict:
        return {
            "overall": "healthy",
            "active_receivers": 1,
            "unstable_receivers": 0,
            "stalled_receivers": 0,
            "silent_receivers": 0,
            "no_decode_warning_receivers": 0,
            "restart_total": 0,
            "health_stale_seconds": 0.0,
            "reason_counts": {},
            "channels": {
                "2": {
                    "rx": 2,
                    "band": "20m",
                    "mode": "FT8",
                    "active": True,
                    "visible_on_kiwi": True,
                    "health_state": "healthy",
                    "status_level": "healthy",
                    "is_unstable": False,
                    "is_stalled": False,
                    "is_silent": False,
                    "is_no_decode_warning": False,
                }
            },
            "propagation": {"overall": "good", "counts": {}, "score_avg": 3.0, "sampled_channels": 1},
            "auto_kick": {},
        }


class _ReceiverScanStub:
    def health_channels(self) -> dict[str, dict[str, object]]:
        return {
            "0": {
                "rx": 0,
                "band": "40m",
                "mode": "CW",
                "active": True,
                "visible_on_kiwi": True,
                "health_state": "Scanning 7.025 MHz",
                "status_level": "healthy",
                "is_unstable": False,
                "is_stalled": False,
                "is_silent": False,
                "is_no_decode_warning": False,
                "last_updated_unix": 1000.0,
                "display_name": "Receiver Scan CW",
            },
            "1": {
                "rx": 1,
                "band": "40m",
                "mode": "PHONE",
                "active": True,
                "visible_on_kiwi": True,
                "health_state": "Waiting for CW follow-up",
                "status_level": "warning",
                "is_unstable": False,
                "is_stalled": False,
                "is_silent": False,
                "is_no_decode_warning": True,
                "last_reason": "Waiting for CW follow-up",
                "last_updated_unix": 1001.0,
                "display_name": "Receiver Scan Phone",
            },
        }


def test_health_api_merges_receiver_scan_channels() -> None:
    app = FastAPI()
    app.include_router(make_router(receiver_mgr=_ReceiverMgrStub(), receiver_scan=_ReceiverScanStub()))
    client = TestClient(app)

    response = client.get("/health/rx")

    assert response.status_code == 200
    payload = response.json()
    assert payload["active_receivers"] == 3
    assert payload["no_decode_warning_receivers"] == 1
    assert payload["overall"] == "quiet"
    assert payload["channels"]["0"]["display_name"] == "Receiver Scan CW"
    assert payload["channels"]["1"]["display_name"] == "Receiver Scan Phone"
    assert payload["reason_counts"]["Waiting for CW follow-up"] == 1