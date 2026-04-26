from __future__ import annotations

import json

from fastapi import FastAPI, WebSocket
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


def test_parse_decode_line_handles_short_wspr_format() -> None:
    parsed = decodes_api._parse_decode_line(
        "D: WSPR 1777127280 -22 -0.3 0.001416 0 N9KBV EM30 37"
    )

    assert parsed["mode"] == "WSPR"
    assert parsed["callsign"] == "N9KBV"
    assert parsed["grid"] == "EM30"
    assert parsed["message"] == "N9KBV EM30 37"
    assert parsed["snr"] == -22.0
    assert parsed["dt"] == -0.3
    assert parsed["hz"] == 1416.0
    assert parsed["power"] == 37


def test_ws4010_websocket_ingests_decode_payload_and_infers_band() -> None:
    app = FastAPI()
    app.include_router(decodes_api.router)

    @app.websocket("/ws4010")
    async def _ws4010(websocket: WebSocket):
        await decodes_api.websocket_decodes_4010(websocket)

    client = TestClient(app)

    with client.websocket_connect("/ws4010") as websocket:
        websocket.send_text(json.dumps({
            "timestamp": "16:18:00",
            "frequency_mhz": 14.0956,
            "mode": "WSPR",
            "callsign": "N9KBV",
            "grid": "EM30",
            "message": "N9KBV EM30 37",
            "snr": -22,
            "dt": -0.3,
            "power": 37,
            "rx": 3,
            "source": "ws4010",
        }))

    response = client.get("/decodes")

    assert response.status_code == 200
    body = response.json()
    assert body["latest"] == 1
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["mode"] == "WSPR"
    assert item["band"] == "20m"
    assert item["grid"] == "EM30"
    assert item["source"] == "ws4010"


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


def test_published_decode_stats_track_band_mode_counts() -> None:
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 7.074,
            "mode": "FT8",
            "callsign": "K1ABC",
            "grid": "FN31",
            "message": "CQ K1ABC FN31",
            "band": "40m",
            "rx": 4,
        }
    )
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:23",
            "frequency_mhz": 7.074,
            "mode": "FT8",
            "callsign": "K2XYZ",
            "grid": "EM12",
            "message": "CQ K2XYZ EM12",
            "band": "40m",
            "rx": 4,
        }
    )

    stats = decodes_api.get_published_decode_stats_by_rx()

    assert stats["4"]["bands"]["40m"]["decode_total"] == 2
    assert stats["4"]["bands"]["40m"]["decode_rates_by_mode"]["FT8"]["decode_total"] == 2
    assert stats["4"]["bands"]["40m"]["decode_rate_per_min"] == 2
    assert stats["4"]["bands"]["40m"]["decode_rate_per_hour"] == 2


def test_published_decode_stats_track_mixed_modes_on_same_rx_and_band(monkeypatch) -> None:
    timeline = iter([1000.0, 1010.0, 1070.0])
    monkeypatch.setattr(decodes_api.time, "time", lambda: next(timeline))

    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 14.077,
            "mode": "FT8",
            "callsign": "NO9D",
            "grid": "EM54",
            "message": "CQ NO9D EM54",
            "band": "20m",
            "rx": 2,
        }
    )
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:32",
            "frequency_mhz": 14.077,
            "mode": "FT4",
            "callsign": "CO8LY",
            "grid": "FL20",
            "message": "CQ CO8LY FL20",
            "band": "20m",
            "rx": 2,
        }
    )

    stats = decodes_api.get_published_decode_stats_by_rx()

    assert stats["2"]["bands"]["20m"]["decode_total"] == 2
    assert stats["2"]["bands"]["20m"]["decode_rate_per_min"] == 1
    assert stats["2"]["bands"]["20m"]["decode_rate_per_hour"] == 2
    assert stats["2"]["bands"]["20m"]["decode_rates_by_mode"] == {
        "FT8": {
            "decode_total": 1,
            "decode_rate_per_min": 0,
            "decode_rate_per_hour": 1,
        },
        "FT4": {
            "decode_total": 1,
            "decode_rate_per_min": 1,
            "decode_rate_per_hour": 1,
        },
    }


def test_decodes_chart_omits_in_progress_bucket(monkeypatch) -> None:
    app = FastAPI()
    app.include_router(decodes_api.router)
    client = TestClient(app)

    monkeypatch.setattr(decodes_api.time, "time", lambda: 100.0)
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 7.074,
            "mode": "FT8",
            "callsign": "K1ABC",
            "grid": "FN31",
            "message": "CQ K1ABC FN31",
            "band": "40m",
            "rx": 4,
        }
    )

    first_response = client.get("/decodes/chart")

    assert first_response.status_code == 200
    assert first_response.json()["buckets"] == []

    monkeypatch.setattr(decodes_api.time, "time", lambda: 116.0)
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:38",
            "frequency_mhz": 14.074,
            "mode": "FT8",
            "callsign": "K2XYZ",
            "grid": "EM12",
            "message": "CQ K2XYZ EM12",
            "band": "20m",
            "rx": 2,
        }
    )

    second_response = client.get("/decodes/chart")
    body = second_response.json()

    assert second_response.status_code == 200
    assert body["bucket_s"] == 15.0
    assert body["buckets"] == [
        {
            "ts": 90.0,
            "bands": {
                "40m": {
                    "total": 1,
                    "breakdown": {"RX4|FT8": 1},
                }
            },
        }
    ]


def test_reset_decode_metrics_clears_chart_history(monkeypatch) -> None:
    monkeypatch.setattr(decodes_api.time, "time", lambda: 100.0)
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:22",
            "frequency_mhz": 7.074,
            "mode": "FT8",
            "callsign": "K1ABC",
            "grid": "FN31",
            "message": "CQ K1ABC FN31",
            "band": "40m",
            "rx": 4,
        }
    )

    monkeypatch.setattr(decodes_api.time, "time", lambda: 116.0)
    decodes_api.publish_decode(
        {
            "timestamp": "16:16:38",
            "frequency_mhz": 14.074,
            "mode": "FT8",
            "callsign": "K2XYZ",
            "grid": "EM12",
            "message": "CQ K2XYZ EM12",
            "band": "20m",
            "rx": 2,
        }
    )

    decodes_api.reset_decode_metrics()

    assert decodes_api.get_decodes_chart() == {"bucket_s": 15.0, "buckets": []}


def test_decodes_chart_retains_up_to_24_hours_of_completed_buckets() -> None:
    total_buckets = decodes_api._CHART_MAX_BUCKETS + 2
    payload = {
        "timestamp": "16:16:22",
        "frequency_mhz": 14.074,
        "mode": "FT8",
        "callsign": "K1ABC",
        "grid": "FN31",
        "message": "CQ K1ABC FN31",
        "band": "20m",
        "rx": 2,
    }

    for idx in range(total_buckets):
        decodes_api._chart_ingest(dict(payload), 100.0 + (idx * decodes_api._CHART_BUCKET_S))

    body = decodes_api.get_decodes_chart()

    assert body["bucket_s"] == 15.0
    assert len(body["buckets"]) == decodes_api._CHART_MAX_BUCKETS
    assert body["buckets"][0]["ts"] == 105.0
    assert body["buckets"][-1]["ts"] == 90.0 + (decodes_api._CHART_MAX_BUCKETS * decodes_api._CHART_BUCKET_S)