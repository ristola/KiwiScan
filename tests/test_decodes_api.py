from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api import decodes as decodes_api


def setup_function() -> None:
    decodes_api.reset_decode_metrics()


def test_decodes_endpoint_hides_events_without_grid() -> None:
    app = FastAPI()
    app.include_router(decodes_api.router)
    client = TestClient(app)

    decodes_api.publish_decode(
        {
            "timestamp": "16:16:21",
            "frequency_mhz": 8.996,
            "mode": "ALE",
            "callsign": None,
            "grid": None,
            "message": "ALE 8.9960 MHz | smoke ALE 2G sounding",
            "band": "utility",
            "rx": 1,
            "source": "utility_monitor",
        }
    )
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 14.074,
            "mode": "FT8",
            "callsign": "K1ABC",
            "grid": "FN31",
            "message": "CQ K1ABC FN31",
            "band": "20m",
            "rx": 2,
        }
    )

    response = client.get("/decodes")
    assert response.status_code == 200

    body = response.json()
    assert body["latest"] == 2
    assert [item["mode"] for item in body["items"]] == ["FT8"]
    assert all(item.get("source") != "utility_monitor" for item in body["items"])


def test_prune_decode_buffer_discards_events_without_grid() -> None:
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:21",
            "frequency_mhz": 11.174,
            "mode": "ALE",
            "callsign": None,
            "grid": None,
            "message": "ALE 11.1740 MHz | smoke ALE 2G sounding",
            "band": "utility",
            "rx": 1,
            "source": "utility_monitor",
        }
    )
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 14.074,
            "mode": "FT8",
            "callsign": "K1ABC",
            "grid": "FN31",
            "message": "CQ K1ABC FN31",
            "band": "20m",
            "rx": 2,
        }
    )

    decodes_api.prune_decode_buffer({"utility"})

    items = decodes_api.get_recent_decodes(900)
    assert items == []