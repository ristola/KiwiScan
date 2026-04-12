import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.auto_set import make_router
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


def test_dual_mode_bands_can_fill_all_eight_receivers(monkeypatch):
    monkeypatch.delenv("KIWISCAN_AUTOSET_MAX_RX", raising=False)
    from kiwi_scan.api import auto_set as _auto_set_mod
    monkeypatch.setattr(_auto_set_mod, "_load_automation_settings", lambda: {"fixedModeEnabled": False})

    mgr = _MgrStub()
    receiver_mgr = _ReceiverMgrStub()
    app = FastAPI()
    app.include_router(
        make_router(
            mgr=mgr,
            receiver_mgr=receiver_mgr,
            band_order=["10m", "12m", "15m", "17m", "20m", "30m", "40m", "60m", "80m", "160m"],
            band_freqs_hz={
                "20m": 14_074_000.0,
                "30m": 10_136_000.0,
                "40m": 7_074_000.0,
                "60m": 5_357_000.0,
                "80m": 3_573_000.0,
                "160m": 1_840_000.0,
            },
            band_ft4_freqs_hz={
                "20m": 14_080_000.0,
                "40m": 7_047_500.0,
            },
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
            "block": "00-04",
            "selected_bands": ["20m", "30m", "40m", "60m", "80m", "160m"],
            "band_modes": {
                "20m": "FT4 / FT8",
                "30m": "FT8",
                "40m": "FT4 / FT8",
                "60m": "FT8",
                "80m": "FT8",
                "160m": "FT8",
            },
            "ssb_scan": {"use_kiwi_snr": False},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    ok_assignments = [row for row in payload["assignments"] if row.get("ok")]
    counts_by_band_mode: dict[tuple[str, str], int] = {}
    for row in ok_assignments:
        key = (str(row["band"]), str(row["mode"]))
        counts_by_band_mode[key] = counts_by_band_mode.get(key, 0) + 1

    # 20m FT8=14074 / FT4=14080 are only 6 kHz apart — fits the 12 kHz IQ window,
    # so a single IQ receiver covers both modes.  40m FT8=7074 / FT4=7047.5 are
    # 26.5 kHz apart — exceeds the IQ window, so two receivers are required.
    # Expected layout: 1 (20m IQ) + 2 (40m) + 4 (30m/60m/80m/160m) = 7 receivers.
    assert payload["assigned_other_tasks"] == 7
    assert payload["other_max_receivers"] == 8
    assert len(ok_assignments) == 7
    assert counts_by_band_mode[("20m", "FT4 / FT8")] == 1  # single IQ receiver
    assert ("20m", "FT4") not in counts_by_band_mode  # no split; IQ covers both
    assert ("20m", "FT8") not in counts_by_band_mode
    assert counts_by_band_mode[("40m", "FT4")] == 1
    assert counts_by_band_mode[("40m", "FT8")] == 1
    assert counts_by_band_mode[("30m", "FT8")] == 1
    assert counts_by_band_mode[("60m", "FT8")] == 1
    assert counts_by_band_mode[("80m", "FT8")] == 1
    assert counts_by_band_mode[("160m", "FT8")] == 1
    assert len(receiver_mgr.last_assignments) == 7