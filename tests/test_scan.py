from __future__ import annotations

import json
from pathlib import Path

import kiwi_scan.scan as scan_mod


def _emit_frame(kwargs: dict[str, object], *, center_freq_hz: float, span_hz: float) -> None:
    kwargs["on_frame"](
        scan_mod.WaterfallFrame(
            frame_index=0,
            center_freq_hz=center_freq_hz,
            span_hz=span_hz,
            power_bins=[-110.0, -96.0, -84.0, -102.0],
        )
    )


def test_run_scan_skips_status_pre_tune_when_disabled(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _fake_set_receiver_frequency(**kwargs):
        calls.append(("tune", dict(kwargs)))
        return True

    def _fake_subscribe_waterfall(**kwargs):
        calls.append(("wf", dict(kwargs)))
        _emit_frame(kwargs, center_freq_hz=14.025e6, span_hz=2400.0)
        return None

    monkeypatch.setattr(scan_mod, "set_receiver_frequency", _fake_set_receiver_frequency)
    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)

    report_path = tmp_path / "report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-scan",
        rx_chan=0,
        center_freq_hz=14.025e6,
        span_hz=2400.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        status_pre_tune=False,
    )

    assert rc == 0
    assert [kind for kind, _ in calls] == ["wf"]
    assert calls[0][1]["status_modulation"] == "iq"
    assert report_path.exists()


def test_run_scan_retries_direct_busy_exception(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def _fake_subscribe_waterfall(**kwargs):
        calls.append("wf")
        if len(calls) == 1:
            raise RuntimeError("192.168.1.93: all 8 client slots taken")
        _emit_frame(kwargs, center_freq_hz=14.025e6, span_hz=2400.0)
        return None

    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)
    monkeypatch.setattr(scan_mod.time, "sleep", lambda _seconds: None)

    report_path = tmp_path / "busy-report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="busy-scan",
        rx_chan=0,
        center_freq_hz=14.025e6,
        span_hz=2400.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        status_pre_tune=False,
        rx_wait_timeout_s=5.0,
        rx_wait_interval_s=0.0,
        rx_wait_max_retries=3,
    )

    assert rc == 0
    assert calls == ["wf", "wf"]
    assert report_path.exists()


def test_run_scan_pairs_status_stream_with_waterfall(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _fake_set_receiver_frequency(**kwargs):
        calls.append(("tune", dict(kwargs)))
        ready_event = kwargs.get("ready_event")
        if ready_event is not None and hasattr(ready_event, "set"):
            ready_event.set()
        hold_event = kwargs.get("hold_event")
        if hold_event is not None and hasattr(hold_event, "wait"):
            hold_event.wait(timeout=0.2)
        return True

    def _fake_subscribe_waterfall(**kwargs):
        calls.append(("wf", dict(kwargs)))
        _emit_frame(kwargs, center_freq_hz=14.200e6, span_hz=3200.0)
        return None

    monkeypatch.setattr(scan_mod, "set_receiver_frequency", _fake_set_receiver_frequency)
    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)

    report_path = tmp_path / "paired-status-report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="paired-status",
        rx_chan=1,
        center_freq_hz=14.200e6,
        span_hz=3200.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        status_pre_tune=False,
        status_parallel_snd=True,
    )

    assert rc == 0
    assert [kind for kind, _ in calls] == ["tune", "wf"]
    assert calls[0][1]["modulation"] == "iq"
    assert calls[1][1]["status_modulation"] == "iq"
    assert calls[0][1]["ws_timestamp"] == calls[1][1]["ws_timestamp"]
    assert report_path.exists()


def test_run_scan_passes_acceptable_rx_chans_to_helpers(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _fake_set_receiver_frequency(**kwargs):
        calls.append(("tune", dict(kwargs)))
        return True

    def _fake_subscribe_waterfall(**kwargs):
        calls.append(("wf", dict(kwargs)))
        _emit_frame(kwargs, center_freq_hz=14.200e6, span_hz=3200.0)
        return None

    monkeypatch.setattr(scan_mod, "set_receiver_frequency", _fake_set_receiver_frequency)
    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)

    report_path = tmp_path / "acceptable-rx-report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="acceptable-rx",
        rx_chan=0,
        center_freq_hz=14.200e6,
        span_hz=3200.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        acceptable_rx_chans=(0, 1),
    )

    assert rc == 0
    assert [kind for kind, _ in calls] == ["tune", "wf"]
    assert calls[0][1]["acceptable_rx_chans"] == (0, 1)
    assert calls[1][1]["acceptable_rx_chans"] == (0, 1)
    assert report_path.exists()


def test_run_scan_transient_retries_do_not_inherit_busy_retry_count(monkeypatch, tmp_path: Path) -> None:
    class KiwiServerTerminatedConnection(Exception):
        pass

    calls: list[str] = []

    def _fake_subscribe_waterfall(**kwargs):
        calls.append("wf")
        if len(calls) <= 4:
            raise RuntimeError("192.168.1.93: all 8 client slots taken")
        if len(calls) == 5:
            raise KiwiServerTerminatedConnection("server closed the connection unexpectedly")
        _emit_frame(kwargs, center_freq_hz=14.025e6, span_hz=2400.0)
        return None

    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)
    monkeypatch.setattr(scan_mod.time, "sleep", lambda _seconds: None)

    report_path = tmp_path / "transient-after-busy-report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="transient-after-busy",
        rx_chan=0,
        center_freq_hz=14.025e6,
        span_hz=2400.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        status_pre_tune=False,
        rx_wait_interval_s=0.0,
    )

    assert rc == 0
    assert calls == ["wf", "wf", "wf", "wf", "wf", "wf"]
    assert report_path.exists()


def test_run_scan_retries_when_waterfall_returns_zero_frames(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def _fake_subscribe_waterfall(**kwargs):
        calls.append("wf")
        if len(calls) == 1:
            return None
        _emit_frame(kwargs, center_freq_hz=14.025e6, span_hz=2400.0)
        return None

    monkeypatch.setattr(scan_mod, "subscribe_waterfall", _fake_subscribe_waterfall)
    monkeypatch.setattr(scan_mod.time, "sleep", lambda _seconds: None)

    report_path = tmp_path / "zero-frame-report.json"
    rc = scan_mod.run_scan(
        host="kiwi.local",
        port=8073,
        password=None,
        user="zero-frame-retry",
        rx_chan=0,
        center_freq_hz=14.025e6,
        span_hz=2400.0,
        threshold_db=8.0,
        min_width_bins=2,
        required_hits=1,
        tolerance_bins=2.5,
        expiry_frames=6,
        max_frames=1,
        jsonl_path=None,
        json_report_path=report_path,
        status_modulation="iq",
        status_pre_tune=False,
        rx_wait_interval_s=0.0,
    )

    assert rc == 0
    assert calls == ["wf", "wf"]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["frames_seen"] == 1