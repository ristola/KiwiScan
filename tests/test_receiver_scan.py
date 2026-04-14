from __future__ import annotations

import json
import time
from pathlib import Path

from kiwi_scan.receiver_scan import ReceiverScanService


class _ReceiverMgrStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, dict[int, object], bool]] = []
        self.kick_calls: list[tuple[str, int, tuple[int, ...], bool]] = []
        self.wait_clear_calls: list[tuple[str, int, tuple[int, ...], float, float]] = []

    def apply_assignments(
        self,
        host: str,
        port: int,
        assignments: dict[int, object],
        *,
        allow_starting_from_empty_full_reset: bool = True,
    ) -> None:
        self.calls.append((host, port, dict(assignments), bool(allow_starting_from_empty_full_reset)))

    def _run_admin_kick_all(
        self,
        *,
        host: str,
        port: int,
        force_all: bool = False,
        kick_only_slots: list[int] | None = None,
        allow_fallback_kick_all: bool = True,
    ) -> bool:
        del force_all
        self.kick_calls.append(
            (
                host,
                port,
                tuple(int(slot) for slot in (kick_only_slots or [])),
                bool(allow_fallback_kick_all),
            )
        )
        return True

    def _wait_for_kiwi_slots_clear(
        self,
        *,
        host: str,
        port: int,
        slots: set[int] | list[int],
        stable_secs: float = 2.0,
        timeout_s: float = 8.0,
    ) -> bool:
        normalized = tuple(sorted(int(slot) for slot in slots))
        self.wait_clear_calls.append((host, port, normalized, float(stable_secs), float(timeout_s)))
        return True


class _AutoSetLoopStub:
    def __init__(self) -> None:
        self.pause_calls: list[str] = []
        self.resume_calls: list[str] = []

    def pause_for_external(self, reason: str) -> None:
        self.pause_calls.append(str(reason))

    def resume_from_external(self, reason: str) -> None:
        self.resume_calls.append(str(reason))


class _InlineThread:
    def __init__(self, target) -> None:
        self._target = target
        self._alive = False

    def start(self) -> None:
        self._alive = True
        try:
            self._target()
        finally:
            self._alive = False

    def join(self, timeout: float | None = None) -> None:
        return None

    def is_alive(self) -> bool:
        return self._alive


def test_receiver_scan_start_applies_fixed_only_assignments_and_collects_results(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    def _fake_scan_frequency(**kwargs):
        lane_key = str(kwargs["lane_key"])
        freq_mhz = float(kwargs["freq_mhz"])
        rx_chan = int(kwargs["rx_chan"])
        probe_index = int(kwargs["probe_index"])
        probe_total = int(kwargs["probe_total"])
        return {
            "lane": lane_key,
            "rx_chan": rx_chan,
            "freq_mhz": freq_mhz,
            "status": "activity" if lane_key == "cw" else "watch",
            "score": 72 if lane_key == "cw" else 44,
            "summary": f"{lane_key} probe {probe_index}",
            "signal_count": 2 if lane_key == "cw" else 1,
            "event_count": 2,
            "max_rel_db": 12.5,
            "best_s_est": 5.0,
            "voice_score": 0.63 if lane_key == "phone" else None,
            "occupied_bw_hz": 2100.0 if lane_key == "phone" else None,
            "probe_index": probe_index,
            "probe_total": probe_total,
        }

    monkeypatch.setattr(service, "_scan_frequency", _fake_scan_frequency)
    monkeypatch.setattr(service, "_run_cw_followup", lambda **kwargs: None)

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0)

    assert result["ok"] is True
    assert result["running"] is False
    assert result["mode_active"] is True
    assert auto_set_loop.pause_calls == ["receiver_scan"]
    assert len(receiver_mgr.calls) == 1
    assert receiver_mgr.kick_calls[:2] == [
        ("kiwi.local", 8073, (0,), False),
        ("kiwi.local", 8073, (1,), False),
    ]
    _, _, assignments, allow_starting_from_empty_full_reset = receiver_mgr.calls[0]
    assert allow_starting_from_empty_full_reset is False
    assert sorted(assignments.keys()) == [2, 3, 4, 5, 6, 7]
    assert all(getattr(assignment, "ignore_slot_check", False) for assignment in assignments.values())

    status = service.status()
    assert [item["freq_mhz"] for item in status["results"]["cw"]] == service.CW_FREQS_MHZ
    assert [item["freq_mhz"] for item in status["results"]["phone"]] == service.PHONE_FREQS_MHZ
    assert status["lanes"]["cw"]["status"] == "complete"
    assert status["lanes"]["phone"]["status"] == "complete"


def test_receiver_scan_start_supports_20m_band(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    def _fake_scan_frequency(**kwargs):
        lane_key = str(kwargs["lane_key"])
        freq_mhz = float(kwargs["freq_mhz"])
        rx_chan = int(kwargs["rx_chan"])
        probe_index = int(kwargs["probe_index"])
        probe_total = int(kwargs["probe_total"])
        return {
            "lane": lane_key,
            "rx_chan": rx_chan,
            "freq_mhz": freq_mhz,
            "status": "activity" if lane_key == "cw" else "watch",
            "score": 68 if lane_key == "cw" else 41,
            "summary": f"{lane_key} probe {probe_index}",
            "signal_count": 1,
            "event_count": 1,
            "max_rel_db": 11.0,
            "best_s_est": 4.0,
            "voice_score": 0.57 if lane_key == "phone" else None,
            "occupied_bw_hz": 2050.0 if lane_key == "phone" else None,
            "probe_index": probe_index,
            "probe_total": probe_total,
        }

    monkeypatch.setattr(service, "_scan_frequency", _fake_scan_frequency)
    monkeypatch.setattr(service, "_run_cw_followup", lambda **kwargs: None)

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m")

    assert result["ok"] is True
    status = service.status()
    assert status["band"] == "20m"
    assert status["mode_label"] == "20m IQ"
    assert status["supported_bands"] == ["20m", "40m"]
    assert status["plan"]["cw_freqs_mhz"] == [14.025, 14.035, 14.045, 14.055]
    assert status["plan"]["phone_range_mhz"] == {"start": 14.15, "end": 14.35}
    assert status["plan"]["phone_priority_freqs_mhz"] == [14.295, 14.3, 14.305, 14.31]
    assert [item["freq_mhz"] for item in status["results"]["cw"]] == service.CW_FREQS_MHZ
    assert [item["freq_mhz"] for item in status["results"]["phone"][:4]] == [14.295, 14.3, 14.305, 14.31]
    assert status["results"]["phone"][-1]["freq_mhz"] == 14.35


def test_receiver_scan_runs_phone_in_parallel_with_cw(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    import threading

    order: list[str] = []
    phone_started = threading.Event()

    def _fake_scan_frequency(**kwargs):
        lane_key = str(kwargs["lane_key"])
        freq_mhz = float(kwargs["freq_mhz"])
        probe_index = int(kwargs["probe_index"])
        probe_total = int(kwargs["probe_total"])
        order.append(f"scan:{lane_key}:{freq_mhz:.3f}")
        if lane_key == "cw":
            if probe_index == probe_total:
                assert phone_started.wait(timeout=1.0)
            if abs(freq_mhz - 7.035) < 1e-6:
                signal_count = 4
                score = 74
            elif abs(freq_mhz - 7.055) < 1e-6:
                signal_count = 2
                score = 92
            else:
                signal_count = 0
                score = 12
            summary = f"cw hits={signal_count}"
            return {
                "lane": lane_key,
                "rx_chan": 0,
                "freq_mhz": freq_mhz,
                "status": "activity" if signal_count else "quiet",
                "score": score,
                "summary": summary,
                "signal_count": signal_count,
                "event_count": signal_count,
                "max_rel_db": 12.0 + signal_count,
                "best_s_est": 5.0,
                "voice_score": None,
                "occupied_bw_hz": None,
                "probe_index": probe_index,
                "probe_total": probe_total,
            }
        if probe_index == 1:
            phone_started.set()
        return {
            "lane": lane_key,
            "rx_chan": 1,
            "freq_mhz": freq_mhz,
            "status": "watch",
            "score": 40,
            "summary": "phone probe",
            "signal_count": 1,
            "event_count": 1,
            "max_rel_db": 10.0,
            "best_s_est": 4.0,
            "voice_score": 0.61,
            "occupied_bw_hz": 2200.0,
            "probe_index": probe_index,
            "probe_total": probe_total,
        }

    def _fake_run_record(req):
        order.append(f"record:{req.freq_hz / 1e6:.3f}")
        req.out_dir.mkdir(parents=True, exist_ok=True)
        (req.out_dir / "followup.wav").write_bytes(b"cw")
        return req.out_dir

    monkeypatch.setattr(service, "_scan_frequency", _fake_scan_frequency)
    monkeypatch.setattr("kiwi_scan.receiver_scan.run_record", _fake_run_record)
    monkeypatch.setattr(
        "kiwi_scan.receiver_scan.try_decode_cw_wav",
        lambda path: {
            "ok": True,
            "decoded_text": "CQ TEST",
            "confidence": 0.98,
            "tone_hz": 702.0,
            "dot_ms": 72.0,
            "wpm_est": 16.7,
            "summary": "Decoded 7/7 CW symbols",
            "wav_path": str(path),
        },
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0)

    assert result["ok"] is True
    assert order.index("record:7.035") > order.index("scan:phone:7.125")
    assert order.index("record:7.055") > order.index("record:7.035")
    assert any(item.startswith("scan:cw:") for item in order)
    assert any(item.startswith("scan:phone:") for item in order)

    status = service.status()
    assert status["cw_followup"]["status"] == "complete"
    assert status["cw_followup"]["completed"] == 2
    assert status["cw_followup"]["total"] == 2
    assert status["cw_followup"]["validated_count"] == 2
    assert status["cw_followup"]["summary"] == "Completed 2 CW follow-up decodes; validated 2"
    assert [item["selected_freq_mhz"] for item in status["cw_followup"]["items"]] == [7.035, 7.055]
    assert all(item["decoded_text"] == "CQ TEST" for item in status["cw_followup"]["items"])
    assert status["lanes"]["cw"]["status"] == "complete"
    assert status["lanes"]["phone"]["status"] == "complete"
    assert status["lanes"]["cw"]["last_summary"] == "Completed 2 CW follow-up decodes; validated 2"
    selected_anchor = next(item for item in status["results"]["cw"] if abs(float(item["freq_mhz"]) - 7.035) < 1e-6)
    assert selected_anchor["followup_selected"] is True
    assert selected_anchor["followup_message_valid"] is True
    assert selected_anchor["followup_decoded_text"] == "CQ TEST"
    assert selected_anchor["followup_validation_summary"] == "Validated CW message: CQ TEST"
    second_anchor = next(item for item in status["results"]["cw"] if abs(float(item["freq_mhz"]) - 7.055) < 1e-6)
    assert second_anchor["followup_selected"] is True
    assert second_anchor["followup_message_valid"] is True


def test_receiver_scan_phone_probe_requires_confirmed_phone_iq_clusters(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    report_path = tmp_path / "phone_report.json"
    events_path = tmp_path / "phone_events.jsonl"
    report_path.write_text(
        json.dumps(
            {
                "peak": {
                    "rel_db": 13.0,
                    "s_est": 4.5,
                    "voice_score": 0.62,
                    "occ_bw_hz": 2400.0,
                },
                "frames_seen": 8,
            }
        ),
        encoding="utf-8",
    )
    events_path.write_text("", encoding="utf-8")

    result = service._summarize_probe(
        lane_key="phone",
        rx_chan=1,
        freq_mhz=7.185,
        probe_index=1,
        probe_total=1,
        rc=0,
        report_path=report_path,
        events_path=events_path,
    )

    assert result["status"] == "watch"
    assert result["event_count"] == 0
    assert result["raw_event_count"] == 0
    assert result["mode_hint"] == "SSB Phone"
    assert "Unconfirmed SSB Phone IQ cluster" in result["summary"]


def test_receiver_scan_health_channels_include_reserved_receivers(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    service._mode_active = True
    service._running = True
    service._last_started_ts = time.time() - 18.0
    service._lanes = service._initial_lanes()
    service._lanes["cw"]["status"] = "followup"
    service._lanes["cw"]["last_summary"] = "Recording 60s CW follow-up on 7.035 MHz"
    service._lanes["phone"]["status"] = "scanning"
    service._lanes["phone"]["current_freq_mhz"] = 7.185
    service._lanes["phone"]["last_summary"] = "Speech-like energy voice=0.18, bw=2100 Hz"
    service._cw_followup = {
        "status": "recording",
        "rx_chan": 0,
        "duration_s": 60,
        "selected_freq_mhz": 7.035,
        "signal_count": 3,
        "score": 80,
        "recording_path": "/tmp/recording",
        "wav_path": None,
        "decoded_text": "",
        "confidence": 0.0,
        "tone_hz": None,
        "dot_ms": None,
        "wpm_est": None,
        "summary": "Recording 60s CW follow-up on 7.035 MHz",
    }

    channels = service.health_channels()

    assert sorted(channels.keys()) == ["0", "1"]
    assert channels["0"]["display_name"] == "Receiver Scan CW Follow-up"
    assert channels["0"]["band"] == "40m"
    assert channels["0"]["mode"] == "CW"
    assert channels["0"]["freq_hz"] == 7.035e6
    assert channels["0"]["health_state"] == "Recording 60s CW follow-up on 7.035 MHz"
    assert channels["0"]["status_level"] == "healthy"
    assert channels["0"]["kiwi_user_age_s"] >= 18
    assert channels["1"]["display_name"] == "Receiver Scan Phone"
    assert channels["1"]["health_state"] == "Speech-like energy voice=0.18, bw=2100 Hz"
    assert channels["1"]["status_level"] == "healthy"


def test_receiver_scan_deactivate_releases_external_hold(tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    service._mode_active = True

    status = service.deactivate()

    assert status["mode_active"] is False
    assert auto_set_loop.resume_calls == ["receiver_scan"]


class _BlockingReceiverMgrStub(_ReceiverMgrStub):
    def __init__(self) -> None:
        super().__init__()
        import threading

        self.started = threading.Event()
        self.release = threading.Event()

    def apply_assignments(
        self,
        host: str,
        port: int,
        assignments: dict[int, object],
        *,
        allow_starting_from_empty_full_reset: bool = True,
    ) -> None:
        super().apply_assignments(
            host,
            port,
            assignments,
            allow_starting_from_empty_full_reset=allow_starting_from_empty_full_reset,
        )
        assert allow_starting_from_empty_full_reset is False
        self.started.set()
        assert self.release.wait(timeout=2.0)


def test_receiver_scan_rejects_duplicate_start_while_activation_in_progress(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _BlockingReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        output_root=tmp_path,
    )

    def _fake_scan_frequency(**kwargs):
        lane_key = str(kwargs["lane_key"])
        freq_mhz = float(kwargs["freq_mhz"])
        rx_chan = int(kwargs["rx_chan"])
        probe_index = int(kwargs["probe_index"])
        probe_total = int(kwargs["probe_total"])
        return {
            "lane": lane_key,
            "rx_chan": rx_chan,
            "freq_mhz": freq_mhz,
            "status": "quiet",
            "score": 0,
            "summary": f"{lane_key} probe {probe_index}",
            "signal_count": 0,
            "event_count": 0,
            "max_rel_db": None,
            "best_s_est": None,
            "voice_score": None,
            "occupied_bw_hz": None,
            "probe_index": probe_index,
            "probe_total": probe_total,
        }

    monkeypatch.setattr(service, "_scan_frequency", _fake_scan_frequency)

    first = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0)
    assert first["ok"] is True
    assert first["status"] == "starting"
    assert first["activating"] is True
    assert receiver_mgr.started.wait(timeout=1.0)

    second = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0)
    assert second["ok"] is False
    assert second["status"] == "starting"
    assert second["activating"] is True
    assert len(receiver_mgr.calls) == 1

    receiver_mgr.release.set()

    import time

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if not service.status()["running"] and not service.status()["activating"]:
            break
        time.sleep(0.01)

    status = service.status()
    assert status["activating"] is False
    assert status["running"] is False
    assert status["mode_active"] is True


def test_receiver_scan_probe_disables_status_pre_tune(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    captured: dict[str, object] = {}

    def _fake_run_scan(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr("kiwi_scan.receiver_scan.run_scan", _fake_run_scan)

    result = service._scan_frequency(
        lane_key="cw",
        rx_chan=0,
        freq_mhz=7.025,
        probe_index=1,
        probe_total=4,
        host="kiwi.local",
        port=8073,
        password=None,
        threshold_db=8.0,
    )

    assert captured["status_modulation"] == "iq"
    assert captured["status_pre_tune"] is False
    assert captured["status_parallel_snd"] is True
    assert captured["rx_wait_timeout_s"] == 20.0
    assert captured["rx_wait_max_retries"] == 0
    assert receiver_mgr.kick_calls == [("kiwi.local", 8073, (0,), False)]
    assert receiver_mgr.wait_clear_calls == [("kiwi.local", 8073, (0,), 0.75, 4.0)]
    assert result["lane"] == "cw"
    assert result["status"] == "quiet"


def test_receiver_scan_phone_probe_uses_expanded_phone_span(monkeypatch, tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    captured: dict[str, object] = {}

    def _fake_run_scan(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr("kiwi_scan.receiver_scan.run_scan", _fake_run_scan)

    result = service._scan_frequency(
        lane_key="phone",
        rx_chan=1,
        freq_mhz=7.125,
        probe_index=1,
        probe_total=len(service.PHONE_FREQS_MHZ),
        host="kiwi.local",
        port=8073,
        password=None,
        threshold_db=8.0,
    )

    assert captured["span_hz"] == service.PHONE_SPAN_HZ
    assert captured["phone_only"] is True
    assert captured["ssb_only"] is True
    assert receiver_mgr.kick_calls == [("kiwi.local", 8073, (1,), False)]
    assert receiver_mgr.wait_clear_calls == [("kiwi.local", 8073, (1,), 0.75, 4.0)]
    assert result["lane"] == "phone"
    assert result["status"] == "quiet"