from __future__ import annotations

import json
from pathlib import Path

import kiwi_scan.band_scanner as band_scanner_mod


class _InlineThread:
    def __init__(self, target=None, args=(), kwargs=None, daemon=None) -> None:
        del daemon
        self._target = target
        self._args = tuple(args)
        self._kwargs = dict(kwargs or {})

    def start(self) -> None:
        if self._target is not None:
            self._target(*self._args, **self._kwargs)

    def join(self, timeout: float | None = None) -> None:
        del timeout


def _write_probe_report(path: Path, *, frames_seen: int, stop_reason: str | None = None) -> None:
    path.write_text(
        json.dumps(
            {
                "type": "scan_report",
                "min_s": 1.0,
                "s1_db": 12.0,
                "db_per_s": 6.0,
                "peak": None if frames_seen == 0 else {"freq_mhz": 14.074, "rel_db": 21.0, "s_est": 2.5},
                "frames_seen": int(frames_seen),
                "ssb_frames_seen": 0,
                "ssb_seen_good": False,
                "ssb_threshold_base_db": 12.0,
                "ssb_threshold_last_db": 12.0,
                "ssb_threshold_min_db": 12.0,
                "ssb_threshold_max_db": 12.0,
                "ssb_spread_last_db": 0.0,
                "ssb_spread_min_db": 0.0,
                "ssb_spread_max_db": 0.0,
                "ssb_warmup_frames": 0,
                "ssb_adaptive_threshold": False,
                "stop_reason": stop_reason,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_hit_rows(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def test_band_scanner_retries_zero_frame_window_before_advancing(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "band_scan"
    record_dir = tmp_path / "records"
    run_calls: list[int] = []

    def _fake_run_scan(**kwargs):
        run_calls.append(len(run_calls) + 1)
        report_path = Path(kwargs["json_report_path"])
        if len(run_calls) == 1:
            _write_probe_report(report_path, frames_seen=0, stop_reason=None)
            return 2
        _write_probe_report(report_path, frames_seen=10, stop_reason=None)
        return 0

    monkeypatch.setattr(band_scanner_mod, "run_scan", _fake_run_scan)
    monkeypatch.setattr(band_scanner_mod.threading, "Thread", _InlineThread)
    monkeypatch.setattr(band_scanner_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(band_scanner_mod, "set_receiver_frequency", lambda **kwargs: None)

    scanner = band_scanner_mod.BandScanner()
    result = scanner.start(
        band="20m",
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-band-scan",
        threshold_db=12.0,
        rx_chan=0,
        wf_rx_chan=0,
        span_hz=1_000_000.0,
        step_hz=1_000_000.0,
        max_frames=10,
        record_hits=False,
        output_dir=output_dir,
        record_dir=record_dir,
        detector="waterfall",
    )

    assert result["ok"] is True
    assert run_calls == [1, 2]

    report = json.loads(Path(result["report_path"]).read_text(encoding="utf-8"))
    assert report["hits"] == []
    results_report = json.loads(Path(result["results_path"]).read_text(encoding="utf-8"))
    assert results_report["result_count"] == 0
    assert results_report["results"] == []
    assert len(report["probe_summaries"]) == 1
    assert report["probe_summaries"][0]["frames_seen"] == 10
    assert report["probe_summaries"][0]["return_code"] == 0


def test_band_scanner_records_single_error_after_zero_frame_retries_exhausted(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "band_scan"
    record_dir = tmp_path / "records"
    run_calls: list[int] = []

    def _fake_run_scan(**kwargs):
        run_calls.append(len(run_calls) + 1)
        _write_probe_report(Path(kwargs["json_report_path"]), frames_seen=0, stop_reason=None)
        return 2

    monkeypatch.setattr(band_scanner_mod, "run_scan", _fake_run_scan)
    monkeypatch.setattr(band_scanner_mod.threading, "Thread", _InlineThread)
    monkeypatch.setattr(band_scanner_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(band_scanner_mod, "set_receiver_frequency", lambda **kwargs: None)

    scanner = band_scanner_mod.BandScanner()
    result = scanner.start(
        band="20m",
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-band-scan",
        threshold_db=12.0,
        rx_chan=0,
        wf_rx_chan=0,
        span_hz=1_000_000.0,
        step_hz=1_000_000.0,
        max_frames=10,
        record_hits=False,
        output_dir=output_dir,
        record_dir=record_dir,
        detector="waterfall",
    )

    assert result["ok"] is True
    assert len(run_calls) == scanner.ZERO_FRAME_WINDOW_RETRIES + 1

    report = json.loads(Path(result["report_path"]).read_text(encoding="utf-8"))
    assert len(report["probe_summaries"]) == 1
    assert report["probe_summaries"][0]["frames_seen"] == 0
    assert report["probe_summaries"][0]["return_code"] == 2
    assert report["hits"] == [{"center_freq_hz": 14175000.0, "error": "scan_error: return_code=2"}]
    results_report = json.loads(Path(result["results_path"]).read_text(encoding="utf-8"))
    assert results_report["result_count"] == 0
    assert results_report["results"] == []


def test_band_scanner_calls_before_window_attempt_for_each_retry(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "band_scan"
    record_dir = tmp_path / "records"
    run_calls: list[int] = []
    attempt_calls: list[tuple[int, float, int]] = []

    def _fake_run_scan(**kwargs):
        run_calls.append(len(run_calls) + 1)
        report_path = Path(kwargs["json_report_path"])
        if len(run_calls) == 1:
            _write_probe_report(report_path, frames_seen=0, stop_reason=None)
            return 2
        _write_probe_report(report_path, frames_seen=10, stop_reason=None)
        return 0

    monkeypatch.setattr(band_scanner_mod, "run_scan", _fake_run_scan)
    monkeypatch.setattr(band_scanner_mod.threading, "Thread", _InlineThread)
    monkeypatch.setattr(band_scanner_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(band_scanner_mod, "set_receiver_frequency", lambda **kwargs: None)

    scanner = band_scanner_mod.BandScanner()
    result = scanner.start(
        band="20m",
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-band-scan",
        threshold_db=12.0,
        rx_chan=0,
        wf_rx_chan=0,
        span_hz=1_000_000.0,
        step_hz=1_000_000.0,
        max_frames=10,
        record_hits=False,
        output_dir=output_dir,
        record_dir=record_dir,
        detector="waterfall",
        before_window_attempt=lambda idx, center_freq_hz, attempt: attempt_calls.append((idx, center_freq_hz, attempt)),
    )

    assert result["ok"] is True
    assert run_calls == [1, 2]
    assert attempt_calls == [(1, 14175000.0, 1), (1, 14175000.0, 2)]


def test_band_scanner_passes_acceptable_rx_chans_to_run_scan(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "band_scan"
    record_dir = tmp_path / "records"
    seen_values: list[tuple[int, ...] | None] = []

    def _fake_run_scan(**kwargs):
        seen_values.append(kwargs.get("acceptable_rx_chans"))
        _write_probe_report(Path(kwargs["json_report_path"]), frames_seen=10, stop_reason=None)
        return 0

    monkeypatch.setattr(band_scanner_mod, "run_scan", _fake_run_scan)
    monkeypatch.setattr(band_scanner_mod.threading, "Thread", _InlineThread)
    monkeypatch.setattr(band_scanner_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(band_scanner_mod, "set_receiver_frequency", lambda **kwargs: None)

    scanner = band_scanner_mod.BandScanner()
    result = scanner.start(
        band="20m",
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-band-scan",
        threshold_db=12.0,
        rx_chan=0,
        wf_rx_chan=0,
        span_hz=1_000_000.0,
        step_hz=1_000_000.0,
        max_frames=10,
        record_hits=False,
        output_dir=output_dir,
        record_dir=record_dir,
        detector="waterfall",
        acceptable_rx_chans=(0, 1),
    )

    assert result["ok"] is True
    assert seen_values == [(0, 1)]


def test_band_scanner_writes_selection_results_file(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "band_scan"
    record_dir = tmp_path / "records"

    def _fake_run_scan(**kwargs):
        _write_probe_report(Path(kwargs["json_report_path"]), frames_seen=10, stop_reason=None)
        _write_hit_rows(
            Path(kwargs["jsonl_path"]),
            [
                {
                    "freq_mhz": 14.07404,
                    "width_hz": 220.0,
                    "width_bins": 3,
                    "type_guess": "narrow",
                    "candidate_type": "DIGITAL_CLUSTER",
                    "bandplan": "FT8",
                    "peak_power": 28.0,
                    "noise_floor": 10.0,
                    "occ_bw_hz": 210.0,
                    "voice_score": 0.08,
                    "observed_frames": 7,
                },
                {
                    "freq_mhz": 14.07402,
                    "width_hz": 180.0,
                    "width_bins": 2,
                    "type_guess": "narrow",
                    "candidate_type": "DIGITAL_CLUSTER",
                    "bandplan": "FT8",
                    "peak_power": 26.0,
                    "noise_floor": 10.0,
                    "occ_bw_hz": 170.0,
                    "voice_score": 0.05,
                    "observed_frames": 6,
                },
                {
                    "freq_mhz": 14.23012,
                    "width_hz": 2400.0,
                    "width_bins": 12,
                    "type_guess": "wide",
                    "candidate_type": "WIDEBAND_VOICE",
                    "bandplan": "PHONE",
                    "peak_power": 25.0,
                    "noise_floor": 11.0,
                    "occ_bw_hz": 2300.0,
                    "voice_score": 0.62,
                    "observed_frames": 4,
                },
            ],
        )
        return 0

    monkeypatch.setattr(band_scanner_mod, "run_scan", _fake_run_scan)
    monkeypatch.setattr(band_scanner_mod.threading, "Thread", _InlineThread)
    monkeypatch.setattr(band_scanner_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(band_scanner_mod, "set_receiver_frequency", lambda **kwargs: None)

    scanner = band_scanner_mod.BandScanner()
    result = scanner.start(
        band="20m",
        host="kiwi.local",
        port=8073,
        password=None,
        user="test-band-scan",
        threshold_db=12.0,
        rx_chan=0,
        wf_rx_chan=0,
        span_hz=1_000_000.0,
        step_hz=1_000_000.0,
        max_frames=10,
        record_hits=False,
        output_dir=output_dir,
        record_dir=record_dir,
        detector="waterfall",
    )

    assert result["ok"] is True
    results_report = json.loads(Path(result["results_path"]).read_text(encoding="utf-8"))
    assert results_report["raw_hit_count"] == 3
    assert results_report["result_count"] == 2
    assert [item["selection_key"] for item in results_report["results"]] == ["14.0740", "14.2301"]
    assert results_report["results"][0]["hit_count"] == 2
    assert results_report["results"][0]["bandplan"] == "FT8"
    assert results_report["results"][0]["max_rel_db"] == 18.0

    status = scanner.status()
    assert status["last_results_report"] == result["results_path"]