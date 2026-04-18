from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from kiwi_scan.receiver_scan import ReceiverScanService


class _ReceiverMgrStub:
    def __init__(self, *, health_summary_payload: dict | None = None) -> None:
        self.calls: list[tuple[str, int, dict[int, object], bool]] = []
        self.kick_calls: list[tuple[str, int, tuple[int, ...], bool]] = []
        self.wait_clear_calls: list[tuple[str, int, tuple[int, ...], float, float]] = []
        self._startup_eviction_active = threading.Event()
        self._health_summary_payload = dict(health_summary_payload or {"channels": {}})

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

    def health_summary(self) -> dict:
        return json.loads(json.dumps(self._health_summary_payload))


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


class _DormantThread:
    def __init__(self, target) -> None:
        del target
        self._alive = False

    def start(self) -> None:
        self._alive = True

    def join(self, timeout: float | None = None) -> None:
        return None

    def is_alive(self) -> bool:
        return self._alive


class _BandScannerStub:
    def __init__(self, report: dict) -> None:
        self._report = dict(report)
        self.start_calls: list[dict[str, object]] = []
        self.stop_calls = 0
        self._last_report: str | None = None
        self._running = False

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        self._running = True
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "smart_band_scan_report.json"
        report_path.write_text(json.dumps(self._report), encoding="utf-8")
        on_hit = kwargs.get("on_hit")
        if callable(on_hit):
            for hit in self._report.get("hits", []):
                if isinstance(hit, dict) and not hit.get("error"):
                    on_hit(dict(hit))
        self._last_report = str(report_path)
        self._running = False
        return {
            "ok": True,
            "status": "started",
            "band": self._report.get("band"),
            "report_path": str(report_path),
        }

    def status(self):
        return {
            "running": self._running,
            "stop_requested": False,
            "last_run_ts": None,
            "last_report": self._last_report,
            "last_error": None,
            "progress": {},
            "last_progress": {},
        }

    def stop(self):
        self.stop_calls += 1
        self._running = False
        return {"ok": True, "status": "stopped"}


class _ProgressBandScannerStub:
    def __init__(self, *, band: str, progress_steps: list[dict[str, object]], report: dict) -> None:
        self._band = str(band)
        self._progress_steps = [dict(step) for step in progress_steps]
        self._report = dict(report)
        self.start_calls: list[dict[str, object]] = []
        self.stop_calls = 0
        self._last_report: str | None = None
        self._status_calls = 0

    def start(self, **kwargs):
        self.start_calls.append(dict(kwargs))
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "smart_band_scan_report.json"
        report_path.write_text(json.dumps(self._report), encoding="utf-8")
        self._last_report = str(report_path)
        self._status_calls = 0
        return {
            "ok": True,
            "status": "started",
            "band": self._band,
            "report_path": str(report_path),
        }

    def status(self):
        if self._status_calls < len(self._progress_steps):
            step = dict(self._progress_steps[self._status_calls])
            self._status_calls += 1
            return {
                "running": True,
                "stop_requested": False,
                "last_run_ts": None,
                "last_report": self._last_report,
                "last_error": None,
                "progress": step,
                "last_progress": step,
            }
        last_progress = dict(self._progress_steps[-1]) if self._progress_steps else {}
        return {
            "running": False,
            "stop_requested": False,
            "last_run_ts": None,
            "last_report": self._last_report,
            "last_error": None,
            "progress": {},
            "last_progress": last_progress,
        }

    def stop(self):
        self.stop_calls += 1
        return {"ok": True, "status": "stopped"}


class _SequenceEvent:
    def __init__(self, states: list[bool]) -> None:
        self._states = [bool(state) for state in states]
        self.calls = 0

    def is_set(self) -> bool:
        self.calls += 1
        if not self._states:
            return False
        if len(self._states) == 1:
            return self._states[0]
        return self._states.pop(0)


def _shutdown_regression_hits() -> list[dict[str, object]]:
    center_freq_hz = 14_078_000.0
    rows = [
        (14_072_000.0, 14.072, 11.730205278843641, 1196.4809384159744, 0.7864077669902912, 0.5078241005139922, 5, 1536.6568914949894, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "MEDIUM_DIGITAL", 22.0),
        (14_072_269.794721408, 14.072269794721407, 11.730205278843641, 1466.2756598237902, 0.7936507936507936, 0.503734827264239, 6, 1806.4516129028052, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "MEDIUM_DIGITAL", 20.0),
        (14_072_563.049853373, 14.072563049853374, 281.52492668665946, 1759.5307917892933, 0.8278145695364238, 0.5164783794312428, 8, 1994.1348973605782, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "narrow+rtty", "UNKNOWN", 34.0),
        (14_072_727.272727273, 14.072727272727274, 11.730205278843641, 1923.753665689379, 0.8303030303030303, 0.4981818181818183, 11, 2252.1994134895504, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 14.0),
        (14_073_243.401759531, 14.073243401759532, 23.460410557687283, 2240.4692082107067, 0.8292682926829268, 0.4955347091932459, 12, 2803.5190615840256, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 14.0),
        (14_073_536.656891495, 14.073536656891495, 11.730205278843641, 2310.8504398837686, 0.7951219512195122, 0.4749718574108819, 13, 2897.360703812912, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 13.0),
        (14_073_806.451612903, 14.073806451612903, 11.730205278843641, 2310.850439881906, 0.7463414634146341, 0.4342213883677297, 12, 2627.5659824050963, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 39.0),
        (14_073_900.293255132, 14.073900293255132, 11.730205278843641, 2217.0087976530194, 0.7073170731707317, 0.41005628517823645, 11, 2170.0879765395075, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 18.0),
        (14_073_994.13489736, 14.07399413489736, 11.730205278843641, 2381.231671553105, 0.6780487804878049, 0.3916697936210132, 11, 2170.0879765395075, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "WIDEBAND_IMAGE", 16.0),
        (14_074_193.548387097, 14.074193548387097, 11.730205278843641, 2205.278592374176, 0.6146341463414634, 0.3555722326454033, 12, 3073.313782989979, 0.07573958944231272, 0.37653958944231275, 11.730205278843641, 0.08571428571428572, 0.05757960522321398, 0.19781732379260164, 121.48175313137472, 2, 1.0, 0.0, 0, False, 4.799999999999997, "very_narrow+rtty", "WIDEBAND_IMAGE", 32.0),
        (14_074_252.19941349, 14.07425219941349, 11.730205278843641, 2146.62756598182, 0.6, 0.34776735459662284, 12, 3073.313782989979, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "DIGITAL_CLUSTER", 26.0),
        (14_074_803.519061584, 14.074803519061584, 23.460410557687283, 2252.1994134895504, 0.4195121951219512, 0.24525328330206375, 11, 2557.1847507320344, 0.04000000000000001, 0.2, 0.0, 0.0, 0.06, 0.0, 0.0, 1, 1.0, 0.0, 0, False, 0.0, "very_narrow+rtty", "DIGITAL_CLUSTER", 38.0),
    ]
    hits: list[dict[str, object]] = []
    for (
        freq_hz,
        freq_mhz,
        width_hz,
        occ_bw_hz,
        occ_frac,
        voice_score,
        narrow_peak_count,
        narrow_peak_span_hz,
        keying_score,
        steady_tone_score,
        freq_stability_hz,
        envelope_variance,
        speech_envelope_score,
        sweep_score,
        centroid_drift_hz,
        observed_frames,
        active_fraction,
        cadence_score,
        keying_edge_count,
        has_on_off_keying,
        amplitude_span_db,
        type_guess,
        candidate_type,
        rel_db,
    ) in rows:
        hits.append(
            {
                "band": "20m",
                "freq_hz": freq_hz,
                "freq_mhz": freq_mhz,
                "center_freq_hz": center_freq_hz,
                "width_hz": width_hz,
                "occ_bw_hz": occ_bw_hz,
                "occ_frac": occ_frac,
                "voice_score": voice_score,
                "narrow_peak_count": narrow_peak_count,
                "narrow_peak_span_hz": narrow_peak_span_hz,
                "keying_score": keying_score,
                "steady_tone_score": steady_tone_score,
                "freq_stability_hz": freq_stability_hz,
                "envelope_variance": envelope_variance,
                "speech_envelope_score": speech_envelope_score,
                "sweep_score": sweep_score,
                "centroid_drift_hz": centroid_drift_hz,
                "observed_frames": observed_frames,
                "active_fraction": active_fraction,
                "cadence_score": cadence_score,
                "keying_edge_count": keying_edge_count,
                "has_on_off_keying": has_on_off_keying,
                "amplitude_span_db": amplitude_span_db,
                "type_guess": type_guess,
                "candidate_type": candidate_type,
                "bandplan": "RTTY",
                "rel_db": rel_db,
                "detector": "waterfall",
            }
        )
    return hits


def test_receiver_scan_smart_start_uses_single_receiver_and_collects_smart_results(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    auto_set_loop = _AutoSetLoopStub()
    band_scanner = _BandScannerStub(
        {
            "band": "20m",
            "windows": 2,
            "hits": [
                {
                    "band": "20m",
                    "detector": "waterfall",
                    "freq_hz": 14_074_000.0,
                    "freq_mhz": 14.074,
                    "center_freq_hz": 14_074_000.0,
                    "width_hz": 50.0,
                    "occ_bw_hz": 50.0,
                    "narrow_peak_count": 5,
                    "narrow_peak_span_hz": 340.0,
                    "keying_score": 0.08,
                    "steady_tone_score": 0.26,
                    "freq_stability_hz": 9.0,
                    "observed_frames": 8,
                    "active_fraction": 0.58,
                    "cadence_score": 0.22,
                    "speech_envelope_score": 0.03,
                    "sweep_score": 0.02,
                    "centroid_drift_hz": 42.0,
                    "voice_score": 0.0,
                    "type_guess": "digital",
                    "candidate_type": "NARROW_MULTI",
                    "bandplan": "Phone",
                    "rel_db": 11.5,
                }
            ],
        }
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        band_scanner=band_scanner,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    def _unexpected_followup(**kwargs):
        raise AssertionError("CW follow-up should not run during SMART scan")

    monkeypatch.setattr(service, "_run_cw_followup", _unexpected_followup)

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is True
    assert result["running"] is False
    assert result["mode_active"] is True
    assert auto_set_loop.pause_calls == ["receiver_scan"]
    assert len(receiver_mgr.calls) == 1
    assert receiver_mgr.kick_calls
    assert {call[2] for call in receiver_mgr.kick_calls} == {(0,), (1,)}
    assert receiver_mgr.wait_clear_calls
    assert {call[2] for call in receiver_mgr.wait_clear_calls} == {(0,), (1,)}
    _, _, assignments, allow_starting_from_empty_full_reset = receiver_mgr.calls[0]
    assert allow_starting_from_empty_full_reset is False
    assert sorted(assignments.keys()) == [2, 3, 4, 5, 6, 7]
    assert all(getattr(assignment, "ignore_slot_check", False) for assignment in assignments.values())

    status = service.status()
    assert status["reserved_receivers"] == [0, 1]
    assert status["plan"]["active_lanes"] == ["smart"]
    assert status["plan"]["scan_order"] == ["smart"]
    assert status["results"]["cw"] == []
    assert status["results"]["phone"] == []
    assert [item["signal_type"] for item in status["results"]["smart"]] == ["FT8"]
    assert status["cw_followup"]["status"] == "inactive"
    assert status["lanes"]["smart"]["status"] == "complete"


def test_receiver_scan_phone_mode_supports_20m_band(monkeypatch, tmp_path: Path) -> None:
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
    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="phone")

    assert result["ok"] is True
    status = service.status()
    assert status["band"] == "20m"
    assert status["mode_label"] == "20m IQ"
    assert status["supported_bands"] == ["20m", "40m"]
    assert status["reserved_receivers"] == [1]
    assert status["plan"]["active_lanes"] == ["phone"]
    assert status["plan"]["cw_freqs_mhz"] == [14.025, 14.035, 14.045, 14.055]
    assert status["plan"]["phone_range_mhz"] == {"start": 14.15, "end": 14.35}
    assert status["plan"]["phone_priority_freqs_mhz"] == [14.295, 14.3, 14.305, 14.31]
    assert status["results"]["cw"] == []
    assert [item["freq_mhz"] for item in status["results"]["phone"][:4]] == [14.295, 14.3, 14.305, 14.31]
    assert status["results"]["phone"][-1]["freq_mhz"] == 14.35


def test_receiver_scan_start_preserves_last_finished_timestamp_during_activation(
    monkeypatch, tmp_path: Path
) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=object(),
        output_root=tmp_path,
    )
    service._last_finished_ts = 1234.5

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _DormantThread(target),
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m")

    assert result["ok"] is True
    assert result["activating"] is True
    assert result["last_finished_ts"] == 1234.5
    assert service.status()["last_finished_ts"] == 1234.5


def test_receiver_scan_waits_for_receiver_manager_startup_before_reserving_slots(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    receiver_mgr._startup_eviction_active = _SequenceEvent([True, True, False])
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=_BandScannerStub({"band": "20m", "windows": 0, "hits": []}),
        output_root=tmp_path,
    )
    service.RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S = 0.0
    service.RECEIVER_MANAGER_SETTLE_TIMEOUT_S = 1.0

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is True
    assert receiver_mgr._startup_eviction_active.calls >= 3
    assert receiver_mgr.kick_calls
    assert receiver_mgr.calls


def test_receiver_scan_smart_mode_uses_band_scanner_for_full_band_results(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    band_scanner = _BandScannerStub(
        {
            "band": "17m",
            "windows": 4,
            "hits": [
                {
                    "band": "17m",
                    "detector": "waterfall",
                    "freq_hz": 18_100_000.0,
                    "freq_mhz": 18.1,
                    "center_freq_hz": 18_100_000.0,
                    "width_hz": 62.0,
                    "occ_bw_hz": 62.0,
                    "narrow_peak_count": 6,
                    "narrow_peak_span_hz": 420.0,
                    "keying_score": 0.09,
                    "steady_tone_score": 0.29,
                    "freq_stability_hz": 8.0,
                    "observed_frames": 8,
                    "active_fraction": 0.61,
                    "cadence_score": 0.24,
                    "speech_envelope_score": 0.04,
                    "sweep_score": 0.03,
                    "centroid_drift_hz": 55.0,
                    "hit_count": 1,
                    "voice_score": 0.0,
                    "type_guess": "digital",
                    "candidate_type": "NARROW_MULTI",
                    "bandplan": "Phone",
                    "rel_db": 12.0,
                },
                {
                    "band": "17m",
                    "detector": "waterfall",
                    "freq_hz": 18_100_080.0,
                    "freq_mhz": 18.10008,
                    "center_freq_hz": 18_100_000.0,
                    "width_hz": 58.0,
                    "occ_bw_hz": 58.0,
                    "narrow_peak_count": 5,
                    "narrow_peak_span_hz": 360.0,
                    "keying_score": 0.08,
                    "steady_tone_score": 0.28,
                    "freq_stability_hz": 9.0,
                    "observed_frames": 8,
                    "active_fraction": 0.59,
                    "cadence_score": 0.23,
                    "speech_envelope_score": 0.04,
                    "sweep_score": 0.03,
                    "centroid_drift_hz": 50.0,
                    "voice_score": 0.0,
                    "type_guess": "digital",
                    "candidate_type": "NARROW_MULTI",
                    "bandplan": "Phone",
                    "rel_db": 10.5,
                },
                {
                    "band": "17m",
                    "detector": "waterfall",
                    "freq_hz": 18_069_250.0,
                    "freq_mhz": 18.06925,
                    "center_freq_hz": 18_068_500.0,
                    "width_hz": 140.0,
                    "occ_bw_hz": 140.0,
                    "narrow_peak_count": 1,
                    "narrow_peak_span_hz": 0.0,
                    "keying_score": 0.43,
                    "steady_tone_score": 0.18,
                    "freq_stability_hz": 11.0,
                    "observed_frames": 7,
                    "active_fraction": 0.52,
                    "cadence_score": 0.31,
                    "speech_envelope_score": 0.03,
                    "sweep_score": 0.01,
                    "centroid_drift_hz": 12.0,
                    "voice_score": 0.0,
                    "type_guess": "cw",
                    "candidate_type": "NARROW_SINGLE",
                    "bandplan": "CW",
                    "rel_db": 7.0,
                },
                {
                    "band": "17m",
                    "detector": "waterfall",
                    "freq_hz": 18_118_500.0,
                    "freq_mhz": 18.1185,
                    "center_freq_hz": 18_118_000.0,
                    "width_hz": 2_300.0,
                    "occ_bw_hz": 2_300.0,
                    "narrow_peak_count": 1,
                    "narrow_peak_span_hz": 0.0,
                    "keying_score": 0.07,
                    "steady_tone_score": 0.16,
                    "freq_stability_hz": 32.0,
                    "observed_frames": 6,
                    "active_fraction": 0.76,
                    "cadence_score": 0.18,
                    "speech_envelope_score": 0.46,
                    "sweep_score": 0.14,
                    "centroid_drift_hz": 280.0,
                    "voice_score": 0.46,
                    "type_guess": "phone",
                    "candidate_type": "WIDEBAND_VOICE",
                    "bandplan": "Phone",
                    "rel_db": 9.0,
                },
            ],
        }
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=band_scanner,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    def _unexpected_followup(**kwargs):
        raise AssertionError("CW follow-up should not run during SMART scan")

    monkeypatch.setattr(service, "_run_cw_followup", _unexpected_followup)

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="17m", mode="smart")

    assert result["ok"] is True
    assert band_scanner.start_calls
    start_call = band_scanner.start_calls[0]
    assert start_call["band"] == "17m"
    assert start_call["rx_chan"] == 0
    assert start_call["wf_rx_chan"] == 0
    assert start_call["span_hz"] == service.SMART_SCAN_SPAN_HZ
    assert start_call["step_hz"] == service.SMART_SCAN_STEP_HZ
    assert start_call["record_hits"] is False
    assert start_call["allow_rx_fallback"] is False
    assert start_call["acceptable_rx_chans"] == (0, 1)
    assert callable(start_call["before_window_attempt"])

    status = service.status()
    assert status["band"] == "17m"
    assert status["reserved_receivers"] == [0, 1]
    assert status["plan"]["active_lanes"] == ["smart"]
    assert status["plan"]["scan_order"] == ["smart"]
    assert "17m" in status["supported_bands"]
    assert status["smart_summary"]["counts"] == {"CW": 1, "PHONE": 1, "FT8": 1}
    smart_results = status["results"]["smart"]
    assert len(smart_results) == 3
    assert [item["signal_type"] for item in smart_results] == ["FT8", "PHONE", "CW"]
    assert [item["candidate_type"] for item in smart_results] == ["DIGITAL_CLUSTER", "WIDEBAND_VOICE", "NARROW_SINGLE"]
    ft8_item = smart_results[0]
    assert ft8_item["freq_mhz"] == 18.1
    assert ft8_item["hit_count"] == 2
    assert ft8_item["confidence"] >= 0.9
    assert status["lanes"]["smart"]["status"] == "complete"
    assert status["cw_followup"]["status"] == "inactive"
    assert status["results"]["cw"] == []
    assert status["results"]["phone"] == []


def test_receiver_scan_smart_mode_preserves_last_tuned_frequency(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    band_scanner = _ProgressBandScannerStub(
        band="20m",
        progress_steps=[
            {"window_index": 1, "window_total": 3, "center_freq_hz": 14_104_000.0},
            {"window_index": 2, "window_total": 3, "center_freq_hz": 14_112_000.0},
        ],
        report={"band": "20m", "windows": 3, "hits": []},
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=band_scanner,
        output_root=tmp_path,
    )
    service.SMART_SCAN_POLL_INTERVAL_S = 0.0

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is True
    status = service.status()
    assert status["running"] is False
    assert status["lanes"]["smart"]["status"] == "complete"
    assert status["lanes"]["smart"]["completed"] == 3


def test_receiver_scan_status_adds_decoder_backed_digital_modes_for_current_band(tmp_path: Path) -> None:
    receiver_mgr = _ReceiverMgrStub(
        health_summary_payload={
            "channels": {
                "7": {
                    "rx": 7,
                    "band": "17m",
                    "mode": "FT4 / FT8 / WSPR",
                    "active": True,
                    "visible_on_kiwi": True,
                    "health_state": "healthy",
                    "decode_total": 1913,
                    "decode_rate_per_min": 17,
                    "decode_rate_per_hour": 1102,
                    "decode_rates_by_mode": {
                        "FT4": {
                            "decode_total": 3,
                            "decode_rate_per_min": 0,
                            "decode_rate_per_hour": 0,
                        },
                        "FT8": {
                            "decode_total": 1892,
                            "decode_rate_per_min": 17,
                            "decode_rate_per_hour": 1098,
                        },
                        "WSPR": {
                            "decode_total": 18,
                            "decode_rate_per_min": 0,
                            "decode_rate_per_hour": 4,
                        },
                    },
                }
            }
        }
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )
    service._band = "17m"
    service._scan_mode = "smart"
    service._mode_active = True
    service._results["smart"] = [
        {
            "band": "17m",
            "freq_hz": 18_118_000.0,
            "freq_mhz": 18.118,
            "signal_type": "PHONE",
            "candidate_type": "WIDEBAND_VOICE",
            "confidence": 0.82,
            "score": 86,
            "hit_count": 2,
        }
    ]

    status = service.status()

    assert status["smart_summary"]["counts"] == {"PHONE": 1, "FT8": 1, "FT4": 1, "WSPR": 1}
    smart_results = status["results"]["smart"]
    assert [item["signal_type"] for item in smart_results] == ["FT8", "WSPR", "FT4", "PHONE"]

    ft8_item = smart_results[0]
    assert ft8_item["decoder_backed"] is True
    assert ft8_item["freq_mhz"] == 18.1
    assert ft8_item["rx_channels"] == [7]

    wspr_item = next(item for item in smart_results if item["signal_type"] == "WSPR")
    assert wspr_item["freq_mhz"] == 18.1046

    ft4_item = next(item for item in smart_results if item["signal_type"] == "FT4")
    assert ft4_item["freq_mhz"] == 18.104


def test_receiver_scan_smart_partial_report_uses_completed_probe_count(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    band_scanner = _ProgressBandScannerStub(
        band="20m",
        progress_steps=[
            {"window_index": 11, "window_total": 43, "center_freq_hz": 14_086_000.0},
        ],
        report={
            "band": "20m",
            "windows": 43,
            "probe_summaries": [
                {
                    "frames_seen": 10,
                    "ssb_frames_seen": 0,
                    "stop_reason": None,
                    "return_code": 0,
                }
                for _ in range(10)
            ]
            + [
                {
                    "frames_seen": 0,
                    "ssb_frames_seen": 0,
                    "stop_reason": None,
                    "return_code": 3,
                }
            ],
            "hits": [],
        },
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=band_scanner,
        output_root=tmp_path,
    )
    service.SMART_SCAN_POLL_INTERVAL_S = 0.0

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is True
    status = service.status()
    assert status["running"] is False
    assert status["lanes"]["smart"]["status"] == "complete"
    assert status["lanes"]["smart"]["completed"] == 10
    assert status["lanes"]["smart"]["total"] == 43


def test_receiver_scan_start_fails_cleanly_when_receiver_manager_startup_stays_active(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    receiver_mgr._startup_eviction_active = _SequenceEvent([True])
    auto_set_loop = _AutoSetLoopStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        band_scanner=_BandScannerStub({"band": "20m", "windows": 1, "hits": []}),
        output_root=tmp_path,
    )
    service.RECEIVER_MANAGER_SETTLE_TIMEOUT_S = 0.01
    service.RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S = 0.0

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["activating"] is False
    assert result["running"] is False
    assert result["mode_active"] is False
    assert result["lanes"]["smart"]["status"] == "error"
    assert "startup is still active" in str(result["last_error"])
    assert receiver_mgr.calls == []
    assert auto_set_loop.pause_calls == [service.HOLD_REASON]
    assert auto_set_loop.resume_calls == [service.HOLD_REASON]


def test_receiver_scan_start_fails_cleanly_when_receiver_manager_lock_stays_busy(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    receiver_mgr._lock = threading.Lock()
    assert receiver_mgr._lock.acquire(blocking=False) is True
    auto_set_loop = _AutoSetLoopStub()
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=auto_set_loop,
        band_scanner=_BandScannerStub({"band": "20m", "windows": 1, "hits": []}),
        output_root=tmp_path,
    )
    service.RECEIVER_MANAGER_LOCK_TIMEOUT_S = 0.01
    service.RECEIVER_MANAGER_SETTLE_POLL_INTERVAL_S = 0.0

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )

    try:
        result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")
    finally:
        receiver_mgr._lock.release()

    assert result["activating"] is False
    assert result["running"] is False
    assert result["mode_active"] is False
    assert result["lanes"]["smart"]["status"] == "error"
    assert "busy applying assignments" in str(result["last_error"])
    assert receiver_mgr.calls == []
    assert auto_set_loop.pause_calls == [service.HOLD_REASON]
    assert auto_set_loop.resume_calls == [service.HOLD_REASON]


def test_receiver_scan_smart_shutdown_preserves_live_merged_results(
    monkeypatch, tmp_path: Path
) -> None:
    receiver_mgr = _ReceiverMgrStub()
    band_scanner = _BandScannerStub(
        {
            "band": "20m",
            "windows": 1,
            "hits": _shutdown_regression_hits(),
        }
    )
    service = ReceiverScanService(
        receiver_mgr=receiver_mgr,
        auto_set_loop=_AutoSetLoopStub(),
        band_scanner=band_scanner,
        output_root=tmp_path,
    )

    monkeypatch.setattr(
        service,
        "_spawn_thread",
        lambda *, name, target: _InlineThread(target),
    )
    monkeypatch.setattr(service, "_run_cw_followup", lambda **kwargs: None)

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is True
    status = service.status()
    smart_results = status["results"]["smart"]
    assert [(item["signal_type"], item["freq_mhz"]) for item in smart_results] == [
        ("WIDEBAND_UNKNOWN", 14.072),
        ("DIGITAL", 14.074804),
    ]

    report_path = Path(str(status["smart_summary"]["report_path"]))
    persisted_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert [(item["signal_type"], item["freq_mhz"]) for item in persisted_report["finalized_results"]] == [
        ("WIDEBAND_UNKNOWN", 14.072),
        ("DIGITAL", 14.074804),
    ]
    assert persisted_report["smart_summary"]["counts"] == {"DIGITAL": 1, "WIDEBAND_UNKNOWN": 1}


def test_receiver_scan_smart_classifier_preserves_candidate_type_when_mode_is_unknown(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_091_000.0,
            "freq_mhz": 14.091,
            "center_freq_hz": 14_091_000.0,
            "width_hz": 900.0,
            "occ_bw_hz": 900.0,
            "occ_frac": 0.22,
            "voice_score": 0.04,
            "type_guess": "digital",
            "bandplan": "RTTY",
            "rel_db": 9.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "MEDIUM_DIGITAL"
    assert result["signal_type"] == "UNKNOWN"
    assert result["mode_hint"] == "DIGITAL_CANDIDATE"
    assert "DIGITAL_CANDIDATE" in result["summary"]


def test_receiver_scan_smart_classifier_requires_cluster_for_ft8(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_074_000.0,
            "freq_mhz": 14.074,
            "center_freq_hz": 14_074_000.0,
            "width_hz": 55.0,
            "occ_bw_hz": 55.0,
            "occ_frac": 0.08,
            "voice_score": 0.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.09,
            "steady_tone_score": 0.66,
            "freq_stability_hz": 7.0,
            "speech_envelope_score": 0.02,
            "sweep_score": 0.01,
            "centroid_drift_hz": 18.0,
            "type_guess": "digital",
            "bandplan": "Phone",
            "rel_db": 11.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "NARROW_MULTI"
    assert result["signal_type"] == "UNKNOWN"
    assert result["mode_hint"] == "DIGITAL_CANDIDATE"


def test_receiver_scan_smart_classifier_requires_keying_for_cw(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_025_100.0,
            "freq_mhz": 14.0251,
            "center_freq_hz": 14_025_000.0,
            "width_hz": 120.0,
            "occ_bw_hz": 120.0,
            "occ_frac": 0.05,
            "voice_score": 0.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.08,
            "steady_tone_score": 0.74,
            "freq_stability_hz": 5.0,
            "speech_envelope_score": 0.01,
            "sweep_score": 0.0,
            "centroid_drift_hz": 8.0,
            "type_guess": "cw",
            "bandplan": "CW",
            "rel_db": 8.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "NARROW_MULTI"
    assert result["signal_type"] == "UNKNOWN"


def test_receiver_scan_smart_classifier_uses_on_off_keying_for_very_narrow_cw(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_025_100.0,
            "freq_mhz": 14.0251,
            "center_freq_hz": 14_025_000.0,
            "width_hz": 35.0,
            "occ_bw_hz": 35.0,
            "occ_frac": 0.04,
            "voice_score": 0.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.72,
            "steady_tone_score": 0.24,
            "freq_stability_hz": 4.0,
            "observed_frames": 7,
            "active_fraction": 0.57,
            "cadence_score": 0.56,
            "keying_edge_count": 6,
            "has_on_off_keying": True,
            "amplitude_span_db": 4.2,
            "speech_envelope_score": 0.01,
            "sweep_score": 0.0,
            "centroid_drift_hz": 7.0,
            "type_guess": "cw",
            "bandplan": "CW",
            "rel_db": 11.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "NARROW_SINGLE"
    assert result["signal_type"] == "CW"
    assert result["has_on_off_keying"] is True


def test_receiver_scan_smart_classifier_requires_real_pulse_flag_when_pulse_metrics_exist(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_069_250.0,
            "freq_mhz": 18.06925,
            "center_freq_hz": 18_068_500.0,
            "width_hz": 140.0,
            "occ_bw_hz": 140.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.43,
            "steady_tone_score": 0.18,
            "freq_stability_hz": 11.0,
            "observed_frames": 7,
            "active_fraction": 0.52,
            "cadence_score": 0.31,
            "keying_edge_count": 0,
            "has_on_off_keying": False,
            "amplitude_span_db": 1.2,
            "speech_envelope_score": 0.03,
            "sweep_score": 0.01,
            "centroid_drift_hz": 12.0,
            "voice_score": 0.0,
            "type_guess": "cw",
            "candidate_type": "NARROW_SINGLE",
            "bandplan": "CW",
            "rel_db": 7.0,
        }
    )

    assert result is not None
    assert result["signal_type"] == "UNKNOWN"
    assert result["has_on_off_keying"] is False


def test_receiver_scan_smart_classifier_requires_speech_for_phone(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_245_000.0,
            "freq_mhz": 14.245,
            "center_freq_hz": 14_244_000.0,
            "width_hz": 2_400.0,
            "occ_bw_hz": 2_400.0,
            "occ_frac": 0.54,
            "voice_score": 0.44,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.05,
            "steady_tone_score": 0.12,
            "freq_stability_hz": 28.0,
            "speech_envelope_score": 0.08,
            "sweep_score": 0.04,
            "centroid_drift_hz": 110.0,
            "type_guess": "phone",
            "bandplan": "Phone",
            "rel_db": 10.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_IMAGE"
    assert result["signal_type"] == "WIDEBAND_UNKNOWN"


def test_receiver_scan_smart_classifier_rejects_phone_inside_cw_bandplan(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_102_300.0,
            "freq_mhz": 18.1023,
            "center_freq_hz": 18_102_000.0,
            "width_hz": 2_250.0,
            "occ_bw_hz": 2_250.0,
            "occ_frac": 0.56,
            "voice_score": 0.28,
            "narrow_peak_count": 12,
            "narrow_peak_span_hz": 2_250.0,
            "keying_score": 0.12,
            "steady_tone_score": 0.22,
            "freq_stability_hz": 18.0,
            "observed_frames": 8,
            "active_fraction": 0.84,
            "cadence_score": 0.16,
            "speech_envelope_score": 0.38,
            "sweep_score": 0.05,
            "centroid_drift_hz": 140.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "CW",
            "rel_db": 14.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_VOICE"
    assert result["signal_type"] == "WIDEBAND_UNKNOWN"
    assert result["mode_hint"] == "WIDEBAND_UNKNOWN"


def test_receiver_scan_smart_classifier_rejects_sstv_inside_cw_bandplan(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_104_400.0,
            "freq_mhz": 18.1044,
            "center_freq_hz": 18_104_000.0,
            "width_hz": 2_350.0,
            "occ_bw_hz": 2_350.0,
            "occ_frac": 0.15,
            "voice_score": 0.08,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 1_500.0,
            "keying_score": 0.16,
            "steady_tone_score": 0.34,
            "freq_stability_hz": 22.0,
            "observed_frames": 6,
            "active_fraction": 0.86,
            "cadence_score": 0.18,
            "speech_envelope_score": 0.09,
            "sweep_score": 0.54,
            "centroid_drift_hz": 820.0,
            "candidate_type": "WIDEBAND_IMAGE",
            "type_guess": "sstv",
            "bandplan": "CW",
            "rel_db": 13.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_IMAGE"
    assert result["signal_type"] == "WIDEBAND_UNKNOWN"


def test_receiver_scan_smart_merge_arbitrates_duplicate_frequency_group_to_digital(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    merged = service._merge_smart_results(
        [
            {
                "band": "20m",
                "freq_hz": 14_024_000.0,
                "freq_mhz": 14.024,
                "center_freq_hz": 14_024_000.0,
                "width_hz": 1_196.0,
                "occ_bw_hz": 1_196.0,
                "occ_frac": 0.28,
                "voice_score": 0.12,
                "narrow_peak_count": 7,
                "narrow_peak_span_hz": 980.0,
                "keying_score": 0.12,
                "steady_tone_score": 0.48,
                "freq_stability_hz": 11.0,
                "observed_frames": 8,
                "active_fraction": 0.74,
                "cadence_score": 0.41,
                "keying_edge_count": 2,
                "has_on_off_keying": False,
                "amplitude_span_db": 6.0,
                "envelope_variance": 0.05,
                "speech_envelope_score": 0.14,
                "sweep_score": 0.08,
                "centroid_drift_hz": 80.0,
                "candidate_type": "MEDIUM_DIGITAL",
                "type_guess": "digital",
                "bandplan": "CW",
                "rel_db": 13.0,
            },
            {
                "band": "20m",
                "freq_hz": 14_024_180.0,
                "freq_mhz": 14.02418,
                "center_freq_hz": 14_024_000.0,
                "width_hz": 1_607.0,
                "occ_bw_hz": 1_607.0,
                "occ_frac": 0.42,
                "voice_score": 0.29,
                "narrow_peak_count": 7,
                "narrow_peak_span_hz": 1_100.0,
                "keying_score": 0.09,
                "steady_tone_score": 0.54,
                "freq_stability_hz": 13.0,
                "observed_frames": 7,
                "active_fraction": 0.78,
                "cadence_score": 0.32,
                "keying_edge_count": 1,
                "has_on_off_keying": False,
                "amplitude_span_db": 5.0,
                "envelope_variance": 0.04,
                "speech_envelope_score": 0.17,
                "sweep_score": 0.07,
                "centroid_drift_hz": 90.0,
                "candidate_type": "WIDEBAND_VOICE",
                "type_guess": "phone",
                "bandplan": "CW",
                "rel_db": 12.0,
            },
        ]
    )

    assert len(merged) == 1
    result = merged[0]
    assert result["signal_type"] == "DIGITAL"
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["merged_count"] == 2
    assert result["bandplan_label"] == "CW"


def test_receiver_scan_smart_merge_arbitrates_wide_voice_group_to_phone(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    merged = service._merge_smart_results(
        [
            {
                "band": "20m",
                "freq_hz": 14_257_500.0,
                "freq_mhz": 14.2575,
                "center_freq_hz": 14_257_000.0,
                "width_hz": 2_240.0,
                "occ_bw_hz": 2_240.0,
                "occ_frac": 0.56,
                "voice_score": 0.48,
                "narrow_peak_count": 2,
                "narrow_peak_span_hz": 180.0,
                "keying_score": 0.05,
                "steady_tone_score": 0.12,
                "freq_stability_hz": 24.0,
                "observed_frames": 8,
                "active_fraction": 0.82,
                "cadence_score": 0.18,
                "keying_edge_count": 1,
                "has_on_off_keying": False,
                "amplitude_span_db": 9.0,
                "envelope_variance": 0.21,
                "speech_envelope_score": 0.37,
                "sweep_score": 0.10,
                "centroid_drift_hz": 210.0,
                "candidate_type": "WIDEBAND_VOICE",
                "type_guess": "phone",
                "bandplan": "Phone",
                "rel_db": 14.0,
            },
            {
                "band": "20m",
                "freq_hz": 14_258_600.0,
                "freq_mhz": 14.2586,
                "center_freq_hz": 14_258_000.0,
                "width_hz": 920.0,
                "occ_bw_hz": 920.0,
                "occ_frac": 0.22,
                "voice_score": 0.10,
                "narrow_peak_count": 3,
                "narrow_peak_span_hz": 260.0,
                "keying_score": 0.08,
                "steady_tone_score": 0.34,
                "freq_stability_hz": 16.0,
                "observed_frames": 6,
                "active_fraction": 0.61,
                "cadence_score": 0.24,
                "keying_edge_count": 2,
                "has_on_off_keying": False,
                "amplitude_span_db": 5.0,
                "envelope_variance": 0.05,
                "speech_envelope_score": 0.09,
                "sweep_score": 0.04,
                "centroid_drift_hz": 70.0,
                "candidate_type": "MEDIUM_DIGITAL",
                "type_guess": "digital",
                "bandplan": "Phone",
                "rel_db": 10.0,
            },
        ]
    )

    assert len(merged) == 1
    result = merged[0]
    assert result["signal_type"] == "PHONE"
    assert result["candidate_type"] == "WIDEBAND_VOICE"
    assert result["merged_count"] == 2


def test_receiver_scan_smart_classifier_promotes_medium_digital_ft8_near_waterhole(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_100_000.0,
            "freq_mhz": 18.1,
            "center_freq_hz": 18_100_000.0,
            "width_hz": 35.0,
            "occ_bw_hz": 850.0,
            "occ_frac": 0.31,
            "voice_score": 0.22,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 1_450.0,
            "keying_score": 0.67,
            "steady_tone_score": 0.56,
            "freq_stability_hz": 8.0,
            "observed_frames": 8,
            "active_fraction": 0.74,
            "cadence_score": 0.62,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 10.5,
            "speech_envelope_score": 0.32,
            "sweep_score": 0.18,
            "centroid_drift_hz": 320.0,
            "hit_count": 2,
            "candidate_type": "MEDIUM_DIGITAL",
            "type_guess": "digital",
            "bandplan": "CW",
            "rel_db": 14.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "MEDIUM_DIGITAL"
    assert result["signal_type"] == "FT8"
    assert result["confidence"] >= 0.8


def test_receiver_scan_smart_classifier_promotes_voice_like_ft8_cluster_in_rtty_window(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_074_745.0,
            "freq_mhz": 14.074745,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 2_263.929618768394,
            "occ_bw_hz": 2_263.929618768394,
            "occ_frac": 0.44878048780487806,
            "voice_score": 0.2540736896730687,
            "narrow_peak_count": 16,
            "narrow_peak_span_hz": 2_697.9472140762955,
            "keying_score": 0.688,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.932,
            "cadence_score": 0.633,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 9.1,
            "envelope_variance": 0.152,
            "speech_envelope_score": 0.597,
            "sweep_score": 0.251,
            "centroid_drift_hz": 514.5,
            "hit_count": 5,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 35.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["confidence"] >= 0.8


def test_receiver_scan_smart_classifier_uses_cluster_for_wspr(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_095_600.0,
            "freq_mhz": 14.0956,
            "center_freq_hz": 14_095_600.0,
            "width_hz": 45.0,
            "occ_bw_hz": 45.0,
            "occ_frac": 0.12,
            "voice_score": 0.0,
            "narrow_peak_count": 3,
            "narrow_peak_span_hz": 160.0,
            "keying_score": 0.08,
            "steady_tone_score": 0.28,
            "freq_stability_hz": 9.0,
            "speech_envelope_score": 0.02,
            "sweep_score": 0.01,
            "centroid_drift_hz": 28.0,
            "hit_count": 2,
            "type_guess": "digital",
            "bandplan": "Phone",
            "rel_db": 10.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "NARROW_MULTI"
    assert result["signal_type"] == "WSPR"


def test_receiver_scan_smart_merge_promotes_voice_like_cw_cluster_to_digital(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    merged = service._merge_smart_results(
        [
            {
                "band": "20m",
                "freq_hz": 14_024_000.0,
                "freq_mhz": 14.024,
                "center_freq_hz": 14_022_000.0,
                "width_hz": 2_804.0,
                "occ_bw_hz": 2_804.0,
                "occ_frac": 0.96,
                "voice_score": 0.54,
                "narrow_peak_count": 20,
                "narrow_peak_span_hz": 2_909.1,
                "keying_score": 0.901,
                "steady_tone_score": 1.0,
                "freq_stability_hz": 0.0,
                "observed_frames": 10,
                "active_fraction": 0.704,
                "cadence_score": 1.0,
                "keying_edge_count": 6,
                "has_on_off_keying": True,
                "amplitude_span_db": 10.4,
                "envelope_variance": 0.241,
                "speech_envelope_score": 0.681,
                "sweep_score": 0.648,
                "centroid_drift_hz": 609.2,
                "candidate_type": "WIDEBAND_VOICE",
                "type_guess": "phone",
                "bandplan": "CW",
                "rel_db": 49.0,
                "hit_count": 145,
                "event_count": 145,
                "raw_event_count": 145,
            },
            {
                "band": "20m",
                "freq_hz": 14_024_745.0,
                "freq_mhz": 14.024745,
                "center_freq_hz": 14_022_000.0,
                "width_hz": 2_263.929618768394,
                "occ_bw_hz": 2_263.929618768394,
                "occ_frac": 0.44878048780487806,
                "voice_score": 0.2540736896730687,
                "narrow_peak_count": 16,
                "narrow_peak_span_hz": 2_697.9472140762955,
                "keying_score": 0.688,
                "steady_tone_score": 1.0,
                "freq_stability_hz": 0.0,
                "observed_frames": 10,
                "active_fraction": 0.932,
                "cadence_score": 0.633,
                "keying_edge_count": 4,
                "has_on_off_keying": True,
                "amplitude_span_db": 9.1,
                "envelope_variance": 0.152,
                "speech_envelope_score": 0.597,
                "sweep_score": 0.251,
                "centroid_drift_hz": 514.5,
                "candidate_type": "WIDEBAND_VOICE",
                "type_guess": "phone",
                "bandplan": "CW",
                "rel_db": 35.0,
                "hit_count": 5,
                "event_count": 5,
                "raw_event_count": 5,
            },
        ]
    )

    assert len(merged) == 1
    result = merged[0]
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "DIGITAL"
    assert result["bandplan_label"] == "CW"


def test_receiver_scan_smart_merge_preserves_ft8_hint_across_low_edge_cluster(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    base_item = {
        "band": "20m",
        "center_freq_hz": 14_078_000.0,
        "width_hz": 2_815.0,
        "occ_bw_hz": 2_815.0,
        "occ_frac": 0.2146341463414634,
        "voice_score": 0.16384615384615386,
        "narrow_peak_count": 12,
        "narrow_peak_span_hz": 2_979.5,
        "keying_score": 0.854,
        "steady_tone_score": 1.0,
        "freq_stability_hz": 0.0,
        "envelope_variance": 0.2,
        "speech_envelope_score": 0.508,
        "sweep_score": 0.516,
        "centroid_drift_hz": 423.7,
        "observed_frames": 10,
        "active_fraction": 0.794,
        "cadence_score": 0.86,
        "keying_edge_count": 6,
        "has_on_off_keying": True,
        "amplitude_span_db": 7.3,
        "candidate_type": "DIGITAL_CLUSTER",
        "type_guess": "very_narrow+rtty",
        "bandplan": "RTTY",
    }

    merged = service._merge_smart_results(
        [
            {
                **base_item,
                "freq_hz": 14_072_938.416422287,
                "freq_mhz": 14.072938,
                "rel_db": 88.0,
                "hit_count": 120,
                "event_count": 120,
                "raw_event_count": 120,
            },
            {
                **base_item,
                "freq_hz": 14_074_252.19941349,
                "freq_mhz": 14.074252,
                "rel_db": 26.0,
                "hit_count": 60,
                "event_count": 60,
                "raw_event_count": 60,
            },
            {
                **base_item,
                "freq_hz": 14_074_803.519061584,
                "freq_mhz": 14.074804,
                "rel_db": 38.0,
                "hit_count": 38,
                "event_count": 38,
                "raw_event_count": 38,
            },
        ]
    )

    assert len(merged) == 1
    result = merged[0]
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"
    assert 14.0735 < result["freq_mhz"] < 14.075
    assert result["freq_low_hz"] < 14_074_000.0 < result["freq_high_hz"]


def test_receiver_scan_smart_merge_limits_total_span_for_adjacent_chain(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    base_item = {
        "band": "20m",
        "center_freq_hz": 14_086_000.0,
        "width_hz": 2_299.0,
        "occ_bw_hz": 2_299.0,
        "occ_frac": 0.42,
        "voice_score": 0.18,
        "narrow_peak_count": 3,
        "narrow_peak_span_hz": 656.0,
        "keying_score": 0.46,
        "steady_tone_score": 0.97,
        "freq_stability_hz": 0.0,
        "envelope_variance": 0.12,
        "speech_envelope_score": 0.48,
        "sweep_score": 0.18,
        "centroid_drift_hz": 180.0,
        "observed_frames": 8,
        "active_fraction": 0.98,
        "cadence_score": 0.30,
        "keying_edge_count": 2,
        "has_on_off_keying": True,
        "amplitude_span_db": 5.0,
        "candidate_type": "WIDEBAND_IMAGE",
        "type_guess": "very_narrow+rtty",
        "bandplan": "RTTY",
        "rel_db": 8.0,
        "hit_count": 1,
        "event_count": 1,
        "raw_event_count": 1,
    }

    merged = service._merge_smart_results(
        [
            {
                **base_item,
                "freq_hz": 14_080_011.730205279,
                "freq_mhz": 14.080012,
                "candidate_type": "MEDIUM_DIGITAL",
            },
            {
                **base_item,
                "freq_hz": 14_082_287.390029326,
                "freq_mhz": 14.082287,
                "candidate_type": "MEDIUM_DIGITAL",
            },
            {
                **base_item,
                "freq_hz": 14_082_627.565982405,
                "freq_mhz": 14.082628,
                "candidate_type": "WIDEBAND_IMAGE",
            },
        ]
    )

    assert len(merged) == 2
    assert [item["merged_count"] for item in merged] == [2, 1]
    assert [item["freq_mhz"] for item in merged] == [14.080012, 14.082628]


def test_receiver_scan_smart_merge_preserves_candidate_mix_across_remerge(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    base_item = {
        "band": "20m",
        "center_freq_hz": 14_101_500.0,
        "width_hz": 1_100.0,
        "occ_bw_hz": 1_100.0,
        "occ_frac": 0.18,
        "voice_score": 0.08,
        "narrow_peak_count": 4,
        "narrow_peak_span_hz": 420.0,
        "keying_score": 0.24,
        "steady_tone_score": 0.44,
        "freq_stability_hz": 6.0,
        "observed_frames": 8,
        "active_fraction": 0.61,
        "cadence_score": 0.36,
        "keying_edge_count": 3,
        "has_on_off_keying": True,
        "amplitude_span_db": 3.0,
        "envelope_variance": 0.05,
        "speech_envelope_score": 0.04,
        "sweep_score": 0.03,
        "centroid_drift_hz": 35.0,
        "type_guess": "digital",
        "bandplan": "RTTY",
        "rel_db": 12.0,
        "hit_count": 1,
        "event_count": 1,
        "raw_event_count": 1,
    }

    first_pass = service._merge_smart_results(
        [
            {
                **base_item,
                "freq_hz": 14_101_000.0,
                "freq_mhz": 14.101,
                "candidate_type": "MEDIUM_DIGITAL",
            },
            {
                **base_item,
                "freq_hz": 14_101_800.0,
                "freq_mhz": 14.1018,
                "candidate_type": "UNKNOWN",
            },
        ]
    )

    assert len(first_pass) == 1

    second_pass = service._merge_smart_results(
        [
            first_pass[0],
            {
                **base_item,
                "freq_hz": 14_102_300.0,
                "freq_mhz": 14.1023,
                "candidate_type": "MEDIUM_DIGITAL",
            },
        ]
    )

    assert len(second_pass) == 1
    result = second_pass[0]
    assert result["candidate_total_count"] == 3
    assert result["digital_candidate_count"] == 2
    assert result["unknown_candidate_count"] == 1
    assert result["merged_candidate_counts"] == {"MEDIUM_DIGITAL": 2, "UNKNOWN": 1}


def test_receiver_scan_smart_classifier_uses_candidate_mix_for_ft4_window_cluster(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_080_011.730205279,
            "freq_mhz": 14.080012,
            "weighted_freq_hz": 14_080_400.0,
            "freq_low_hz": 14_080_011.730205279,
            "freq_high_hz": 14_082_287.390029326,
            "center_freq_hz": 14_086_000.0,
            "width_hz": 2_275.6598240472376,
            "occ_bw_hz": 2_275.6598240472376,
            "occ_frac": 0.41,
            "voice_score": 0.3737579042457092,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 2_275.6598240472376,
            "keying_score": 0.642837500402993,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.9651058201058201,
            "cadence_score": 0.6433333333333333,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 7.0,
            "envelope_variance": 0.16,
            "speech_envelope_score": 0.5729844114046942,
            "sweep_score": 0.28515582301348463,
            "centroid_drift_hz": 360.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "very_narrow+rtty",
            "bandplan": "RTTY",
            "rel_db": 0.0,
            "hit_count": 30,
            "candidate_total_count": 30,
            "digital_candidate_count": 17,
            "wideband_candidate_count": 1,
            "unknown_candidate_count": 12,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 17, "UNKNOWN": 12, "WIDEBAND_IMAGE": 1},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT4"
    assert result["mode_hint"] == "FT4"


def test_receiver_scan_smart_classifier_uses_candidate_mix_for_ft8_window_cluster(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_072_441.0,
            "freq_mhz": 14.072441,
            "weighted_freq_hz": 14_073_540.285896162,
            "freq_low_hz": 14_072_011.730205279,
            "freq_high_hz": 14_074_686.217008797,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 2_674.486803517677,
            "occ_bw_hz": 2_674.486803517677,
            "occ_frac": 0.41,
            "voice_score": 0.24,
            "narrow_peak_count": 3,
            "narrow_peak_span_hz": 2_674.486803517677,
            "keying_score": 0.70,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.94,
            "cadence_score": 0.696,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 8.0,
            "envelope_variance": 0.16,
            "speech_envelope_score": 0.49,
            "sweep_score": 0.22,
            "centroid_drift_hz": 340.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "very_narrow+rtty",
            "bandplan": "RTTY",
            "rel_db": 40.0,
            "hit_count": 34,
            "candidate_total_count": 34,
            "digital_candidate_count": 14,
            "wideband_candidate_count": 10,
            "unknown_candidate_count": 10,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 14, "UNKNOWN": 10, "WIDEBAND_IMAGE": 10},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_candidate_mix_for_heavier_ft8_window_cluster(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_073_676.0,
            "freq_mhz": 14.073676,
            "weighted_freq_hz": 14_073_676.0,
            "freq_low_hz": 14_072_011.730205279,
            "freq_high_hz": 14_075_307.917888563,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 3_296.1876832842827,
            "occ_bw_hz": 3_296.1876832842827,
            "occ_frac": 0.42,
            "voice_score": 0.26,
            "narrow_peak_count": 3,
            "narrow_peak_span_hz": 3_296.1876832842827,
            "keying_score": 0.70,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.94,
            "cadence_score": 0.696,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 8.0,
            "envelope_variance": 0.16,
            "speech_envelope_score": 0.49,
            "sweep_score": 0.22,
            "centroid_drift_hz": 340.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "very_narrow+rtty",
            "bandplan": "RTTY",
            "rel_db": 40.0,
            "hit_count": 35,
            "candidate_total_count": 35,
            "digital_candidate_count": 14,
            "wideband_candidate_count": 11,
            "unknown_candidate_count": 10,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 14, "UNKNOWN": 10, "WIDEBAND_IMAGE": 11},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_candidate_mix_for_wspr_window_cluster(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_096_481.0,
            "freq_mhz": 14.096481,
            "weighted_freq_hz": 14_097_112.911049584,
            "freq_low_hz": 14_095_999.0,
            "freq_high_hz": 14_098_932.0,
            "center_freq_hz": 14_102_000.0,
            "width_hz": 2_933.0,
            "occ_bw_hz": 2_933.0,
            "occ_frac": 0.39,
            "voice_score": 0.22,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 2_933.0,
            "keying_score": 0.44,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.93,
            "cadence_score": 0.357,
            "keying_edge_count": 2,
            "has_on_off_keying": True,
            "amplitude_span_db": 8.0,
            "envelope_variance": 0.15,
            "speech_envelope_score": 0.46,
            "sweep_score": 0.19,
            "centroid_drift_hz": 350.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "very_narrow+rtty",
            "bandplan": "RTTY",
            "rel_db": 39.0,
            "hit_count": 41,
            "candidate_total_count": 41,
            "digital_candidate_count": 15,
            "wideband_candidate_count": 12,
            "unknown_candidate_count": 14,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 15, "UNKNOWN": 14, "WIDEBAND_IMAGE": 6, "WIDEBAND_VOICE": 6},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "WSPR"
    assert result["mode_hint"] == "WSPR"


def test_receiver_scan_smart_classifier_uses_hint_locked_ft8_window_cluster_from_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_072_616.786586177,
            "freq_mhz": 14.072617,
            "weighted_freq_hz": 14_073_753.762278,
            "freq_low_hz": 14_072_023.460410558,
            "freq_high_hz": 14_075_718.475073313,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 3_695.0146627556533,
            "occ_bw_hz": 3_695.0146627556533,
            "occ_frac": 0.744,
            "voice_score": 0.2577383592017739,
            "narrow_peak_count": 3,
            "narrow_peak_span_hz": 3_695.0,
            "keying_score": 0.903,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 12,
            "active_fraction": 0.744,
            "cadence_score": 0.883,
            "keying_edge_count": 6,
            "has_on_off_keying": True,
            "amplitude_span_db": 16.0,
            "envelope_variance": 0.251,
            "speech_envelope_score": 0.761,
            "sweep_score": 0.44,
            "centroid_drift_hz": 747.9,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 40.0,
            "hit_count": 39,
            "candidate_total_count": 39,
            "digital_candidate_count": 12,
            "wideband_candidate_count": 21,
            "unknown_candidate_count": 6,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 12, "UNKNOWN": 6, "WIDEBAND_IMAGE": 11, "WIDEBAND_VOICE": 10},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_hint_locked_wspr_window_cluster_from_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_096_471.835009033,
            "freq_mhz": 14.096472,
            "weighted_freq_hz": 14_097_369.31363469,
            "freq_low_hz": 14_096_011.730205279,
            "freq_high_hz": 14_099_718.475073313,
            "center_freq_hz": 14_102_000.0,
            "width_hz": 3_706.744868034497,
            "occ_bw_hz": 3_706.744868034497,
            "occ_frac": 0.967,
            "voice_score": 0.2959645232815964,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 3_706.7,
            "keying_score": 0.724,
            "steady_tone_score": 0.977,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.967,
            "cadence_score": 0.667,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 11.8,
            "envelope_variance": 0.176,
            "speech_envelope_score": 0.507,
            "sweep_score": 0.409,
            "centroid_drift_hz": 934.4,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 40.0,
            "hit_count": 39,
            "candidate_total_count": 39,
            "digital_candidate_count": 13,
            "wideband_candidate_count": 14,
            "unknown_candidate_count": 12,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 13, "UNKNOWN": 12, "WIDEBAND_IMAGE": 10, "WIDEBAND_VOICE": 4},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "WSPR"
    assert result["mode_hint"] == "WSPR"


def test_receiver_scan_smart_classifier_uses_range_locked_ft8_window_cluster_from_broad_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_072_559.825573128,
            "freq_mhz": 14.07256,
            "weighted_freq_hz": 14_072_994.524894409,
            "freq_low_hz": 14_072_011.730205279,
            "freq_high_hz": 14_076_445.747800587,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 4_434.0175953079015,
            "occ_bw_hz": 4_434.0175953079015,
            "occ_frac": 0.857,
            "voice_score": 0.24008875739644964,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 4_434.0,
            "keying_score": 0.706,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.857,
            "cadence_score": 0.733,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 18.3,
            "envelope_variance": 0.267,
            "speech_envelope_score": 0.66,
            "sweep_score": 0.507,
            "centroid_drift_hz": 1207.1,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 44.0,
            "hit_count": 48,
            "candidate_total_count": 48,
            "digital_candidate_count": 14,
            "wideband_candidate_count": 23,
            "unknown_candidate_count": 11,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 14, "UNKNOWN": 11, "WIDEBAND_IMAGE": 14, "WIDEBAND_VOICE": 9},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_range_locked_wspr_window_cluster_from_broad_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_096_568.974622905,
            "freq_mhz": 14.096569,
            "weighted_freq_hz": 14_097_612.591021528,
            "freq_low_hz": 14_096_000.0,
            "freq_high_hz": 14_101_278.592375366,
            "center_freq_hz": 14_102_000.0,
            "width_hz": 5_278.592375366017,
            "occ_bw_hz": 5_278.592375366017,
            "occ_frac": 0.624,
            "voice_score": 0.23546813532651445,
            "narrow_peak_count": 5,
            "narrow_peak_span_hz": 5_278.6,
            "keying_score": 0.751,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.624,
            "cadence_score": 0.744,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 10.6,
            "envelope_variance": 0.163,
            "speech_envelope_score": 0.577,
            "sweep_score": 0.549,
            "centroid_drift_hz": 899.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 37.0,
            "hit_count": 56,
            "candidate_total_count": 56,
            "digital_candidate_count": 14,
            "wideband_candidate_count": 27,
            "unknown_candidate_count": 15,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 14, "UNKNOWN": 15, "WIDEBAND_IMAGE": 22, "WIDEBAND_VOICE": 5},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "WSPR"
    assert result["mode_hint"] == "WSPR"


def test_receiver_scan_smart_classifier_uses_range_locked_ft8_window_cluster_from_current_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_073_225.76482571,
            "freq_mhz": 14.073226,
            "weighted_freq_hz": 14_074_226.985004967,
            "freq_low_hz": 14_072_011.730205279,
            "freq_high_hz": 14_077_266.862170087,
            "center_freq_hz": 14_078_000.0,
            "width_hz": 5_255.13196480833,
            "occ_bw_hz": 5_255.13196480833,
            "occ_frac": 0.884,
            "voice_score": 0.18391817466561763,
            "narrow_peak_count": 5,
            "narrow_peak_span_hz": 5_255.1,
            "keying_score": 0.729,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.884,
            "cadence_score": 0.783,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 16.5,
            "envelope_variance": 0.24,
            "speech_envelope_score": 0.645,
            "sweep_score": 0.639,
            "centroid_drift_hz": 943.5,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 39.0,
            "hit_count": 54,
            "candidate_total_count": 54,
            "digital_candidate_count": 14,
            "wideband_candidate_count": 27,
            "unknown_candidate_count": 13,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 14, "UNKNOWN": 13, "WIDEBAND_IMAGE": 16, "WIDEBAND_VOICE": 11},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_range_locked_wspr_window_cluster_from_current_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_096_522.328755368,
            "freq_mhz": 14.096522,
            "weighted_freq_hz": 14_097_454.292936519,
            "freq_low_hz": 14_096_011.730205279,
            "freq_high_hz": 14_099_718.475073313,
            "center_freq_hz": 14_102_000.0,
            "width_hz": 3_706.744868034497,
            "occ_bw_hz": 3_706.744868034497,
            "occ_frac": 0.648,
            "voice_score": 0.27058031959629947,
            "narrow_peak_count": 6,
            "narrow_peak_span_hz": 3_706.7,
            "keying_score": 0.913,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.648,
            "cadence_score": 1.0,
            "keying_edge_count": 6,
            "has_on_off_keying": True,
            "amplitude_span_db": 10.6,
            "envelope_variance": 0.201,
            "speech_envelope_score": 0.652,
            "sweep_score": 0.5,
            "centroid_drift_hz": 733.2,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 39.0,
            "hit_count": 52,
            "candidate_total_count": 52,
            "digital_candidate_count": 17,
            "wideband_candidate_count": 29,
            "unknown_candidate_count": 6,
            "merged_candidate_counts": {"MEDIUM_DIGITAL": 17, "UNKNOWN": 6, "WIDEBAND_IMAGE": 18, "WIDEBAND_VOICE": 11},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "WSPR"
    assert result["mode_hint"] == "WSPR"


def test_receiver_scan_smart_classifier_uses_range_locked_wspr_window_cluster_from_heavier_live_mix(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_097_524.971625011,
            "freq_mhz": 14.097525,
            "weighted_freq_hz": 14_098_241.370655185,
            "freq_low_hz": 14_096_000.0,
            "freq_high_hz": 14_099_741.935483871,
            "center_freq_hz": 14_102_000.0,
            "width_hz": 3_741.935483871028,
            "occ_bw_hz": 3_741.935483871028,
            "voice_score": 0.3615667894208237,
            "narrow_peak_count": 15,
            "narrow_peak_span_hz": 3_741.9,
            "keying_score": 0.837,
            "steady_tone_score": 1.0,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.77,
            "cadence_score": 0.912,
            "keying_edge_count": 6,
            "has_on_off_keying": True,
            "amplitude_span_db": 6.8,
            "envelope_variance": 0.165,
            "speech_envelope_score": 0.56,
            "sweep_score": 0.592,
            "centroid_drift_hz": 461.3,
            "candidate_type": "WIDEBAND_VOICE",
            "bandplan": "RTTY",
            "rel_db": 49.0,
            "hit_count": 142,
            "candidate_total_count": 142,
            "digital_candidate_count": 87,
            "wideband_candidate_count": 51,
            "unknown_candidate_count": 4,
            "merged_candidate_counts": {
                "DIGITAL_CLUSTER": 72,
                "MEDIUM_DIGITAL": 15,
                "UNKNOWN": 4,
                "WIDEBAND_IMAGE": 27,
                "WIDEBAND_VOICE": 24,
            },
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "WSPR"
    assert result["mode_hint"] == "WSPR"


def test_receiver_scan_smart_classifier_keeps_zero_digital_evidence_ft4_side_cluster_generic(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_082_651.026392963,
            "freq_mhz": 14.082651,
            "weighted_freq_hz": 14_083_215.140364047,
            "freq_low_hz": 14_082_651.026392963,
            "freq_high_hz": 14_084_410.55718475,
            "center_freq_hz": 14_086_000.0,
            "width_hz": 2_310.850439881906,
            "occ_bw_hz": 2_310.850439881906,
            "occ_frac": 0.653,
            "voice_score": 0.5180487804878049,
            "narrow_peak_count": 13,
            "narrow_peak_span_hz": 2_909.1,
            "keying_score": 0.767,
            "steady_tone_score": 0.965,
            "freq_stability_hz": 0.0,
            "observed_frames": 10,
            "active_fraction": 0.653,
            "cadence_score": 0.713,
            "keying_edge_count": 4,
            "has_on_off_keying": True,
            "amplitude_span_db": 14.3,
            "envelope_variance": 0.244,
            "speech_envelope_score": 0.728,
            "sweep_score": 0.712,
            "centroid_drift_hz": 884.9,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "phone",
            "bandplan": "RTTY",
            "rel_db": 35.0,
            "hit_count": 26,
            "candidate_total_count": 26,
            "digital_candidate_count": 0,
            "wideband_candidate_count": 26,
            "unknown_candidate_count": 0,
            "merged_candidate_counts": {"WIDEBAND_IMAGE": 15, "WIDEBAND_VOICE": 11},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_VOICE"
    assert result["signal_type"] == "WIDEBAND_UNKNOWN"
    assert result["mode_hint"] == "WIDEBAND_UNKNOWN"


def test_receiver_scan_smart_classifier_keeps_wideband_side_image_generic_near_ft4(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_082_627.565982405,
            "freq_mhz": 14.082628,
            "weighted_freq_hz": 14_082_667.155425219,
            "freq_low_hz": 14_082_627.565982405,
            "freq_high_hz": 14_082_944.281524926,
            "center_freq_hz": 14_086_000.0,
            "width_hz": 2_299.120234604925,
            "occ_bw_hz": 2_299.120234604925,
            "occ_frac": 0.44,
            "voice_score": 0.16857509627727862,
            "narrow_peak_count": 3,
            "narrow_peak_span_hz": 656.8914956003428,
            "keying_score": 0.46017083373632633,
            "steady_tone_score": 0.9723516410087315,
            "freq_stability_hz": 0.0,
            "observed_frames": 8,
            "active_fraction": 0.984375,
            "cadence_score": 0.3041666666666667,
            "keying_edge_count": 2,
            "has_on_off_keying": True,
            "amplitude_span_db": 5.0,
            "envelope_variance": 0.13,
            "speech_envelope_score": 0.4836066928259858,
            "sweep_score": 0.18172043010756375,
            "centroid_drift_hz": 190.0,
            "candidate_type": "WIDEBAND_VOICE",
            "type_guess": "very_narrow+rtty",
            "bandplan": "RTTY",
            "rel_db": 0.0,
            "hit_count": 8,
            "candidate_total_count": 8,
            "digital_candidate_count": 0,
            "wideband_candidate_count": 8,
            "unknown_candidate_count": 0,
            "merged_candidate_counts": {"WIDEBAND_IMAGE": 6, "WIDEBAND_VOICE": 2},
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_VOICE"
    assert result["signal_type"] == "WIDEBAND_UNKNOWN"


def test_receiver_scan_smart_classifier_uses_frequency_bonus_for_ft4(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_104_000.0,
            "freq_mhz": 18.104,
            "center_freq_hz": 18_104_000.0,
            "width_hz": 85.0,
            "occ_bw_hz": 85.0,
            "occ_frac": 0.16,
            "voice_score": 0.0,
            "narrow_peak_count": 5,
            "narrow_peak_span_hz": 340.0,
            "keying_score": 0.09,
            "steady_tone_score": 0.28,
            "freq_stability_hz": 9.0,
            "observed_frames": 8,
            "active_fraction": 0.58,
            "cadence_score": 0.23,
            "speech_envelope_score": 0.03,
            "sweep_score": 0.01,
            "centroid_drift_hz": 34.0,
            "hit_count": 2,
            "type_guess": "digital",
            "bandplan": "Phone",
            "rel_db": 11.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT4"


def test_receiver_scan_smart_classifier_near_ft8_with_sparse_cluster_stays_generic(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_074_000.0,
            "freq_mhz": 14.074,
            "center_freq_hz": 14_074_000.0,
            "width_hz": 55.0,
            "occ_bw_hz": 55.0,
            "occ_frac": 0.11,
            "voice_score": 0.0,
            "narrow_peak_count": 4,
            "narrow_peak_span_hz": 240.0,
            "keying_score": 0.14,
            "steady_tone_score": 0.32,
            "freq_stability_hz": 8.0,
            "observed_frames": 8,
            "active_fraction": 0.58,
            "cadence_score": 0.24,
            "speech_envelope_score": 0.03,
            "sweep_score": 0.01,
            "centroid_drift_hz": 30.0,
            "hit_count": 2,
            "type_guess": "digital",
            "bandplan": "Phone",
            "rel_db": 11.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "NARROW_MULTI"
    assert result["signal_type"] == "UNKNOWN"
    assert result["mode_hint"] == "DIGITAL_CANDIDATE"


def test_receiver_scan_smart_classifier_strong_cluster_inside_ft8_passband_promotes_ft8(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "17m",
            "freq_hz": 18_102_000.0,
            "freq_mhz": 18.102,
            "center_freq_hz": 18_102_000.0,
            "width_hz": 85.0,
            "occ_bw_hz": 85.0,
            "occ_frac": 0.16,
            "voice_score": 0.0,
            "narrow_peak_count": 5,
            "narrow_peak_span_hz": 340.0,
            "keying_score": 0.09,
            "steady_tone_score": 0.28,
            "freq_stability_hz": 9.0,
            "observed_frames": 8,
            "active_fraction": 0.58,
            "cadence_score": 0.23,
            "speech_envelope_score": 0.03,
            "sweep_score": 0.01,
            "centroid_drift_hz": 34.0,
            "type_guess": "digital",
            "bandplan": "Phone",
            "rel_db": 11.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "DIGITAL_CLUSTER"
    assert result["signal_type"] == "FT8"
    assert result["mode_hint"] == "FT8"


def test_receiver_scan_smart_classifier_uses_sweep_for_sstv(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_230_000.0,
            "freq_mhz": 14.23,
            "center_freq_hz": 14_230_000.0,
            "width_hz": 2_400.0,
            "occ_bw_hz": 2_400.0,
            "occ_frac": 0.48,
            "voice_score": 0.12,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.04,
            "steady_tone_score": 0.08,
            "freq_stability_hz": 36.0,
            "speech_envelope_score": 0.12,
            "sweep_score": 0.46,
            "centroid_drift_hz": 760.0,
            "type_guess": "sstv",
            "bandplan": "Phone",
            "rel_db": 9.0,
        }
    )

    assert result is not None
    assert result["candidate_type"] == "WIDEBAND_IMAGE"
    assert result["signal_type"] == "SSTV"


def test_receiver_scan_smart_classifier_rejects_steady_center_birdie(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_200_000.0,
            "freq_mhz": 14.2,
            "center_freq_hz": 14_200_000.0,
            "width_hz": 35.0,
            "occ_bw_hz": 35.0,
            "occ_frac": 0.05,
            "voice_score": 0.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.02,
            "steady_tone_score": 0.93,
            "freq_stability_hz": 2.0,
            "envelope_variance": 0.01,
            "observed_frames": 10,
            "active_fraction": 1.0,
            "cadence_score": 0.01,
            "speech_envelope_score": 0.01,
            "sweep_score": 0.0,
            "centroid_drift_hz": 1.0,
            "type_guess": "cw",
            "bandplan": "Phone",
            "rel_db": 18.0,
        }
    )

    assert result is not None
    assert result["signal_type"] == "BIRDIE"
    assert result["score"] < 50


def test_receiver_scan_smart_classifier_rejects_steady_narrow_carrier(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service._classify_smart_hit(
        {
            "band": "20m",
            "freq_hz": 14_025_400.0,
            "freq_mhz": 14.0254,
            "center_freq_hz": 14_024_600.0,
            "width_hz": 40.0,
            "occ_bw_hz": 40.0,
            "occ_frac": 0.06,
            "voice_score": 0.0,
            "narrow_peak_count": 1,
            "narrow_peak_span_hz": 0.0,
            "keying_score": 0.02,
            "steady_tone_score": 0.91,
            "freq_stability_hz": 3.0,
            "envelope_variance": 0.02,
            "observed_frames": 10,
            "active_fraction": 0.96,
            "cadence_score": 0.02,
            "speech_envelope_score": 0.01,
            "sweep_score": 0.0,
            "centroid_drift_hz": 2.0,
            "type_guess": "cw",
            "bandplan": "CW",
            "rel_db": 16.0,
        }
    )

    assert result is not None
    assert result["signal_type"] == "CARRIER"
    assert result["score"] < 50


def test_receiver_scan_smart_mode_requires_band_scanner(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, band="20m", mode="smart")

    assert result["ok"] is False
    assert result["status"] == "error"
    assert result["last_error"] == "SMART band scan engine is not available"
    assert service.status()["mode_active"] is False


def test_receiver_scan_cw_mode_runs_followup_after_scan(monkeypatch, tmp_path: Path) -> None:
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

    order: list[str] = []

    def _fake_scan_frequency(**kwargs):
        lane_key = str(kwargs["lane_key"])
        assert lane_key == "cw"
        freq_mhz = float(kwargs["freq_mhz"])
        probe_index = int(kwargs["probe_index"])
        probe_total = int(kwargs["probe_total"])
        order.append(f"scan:{lane_key}:{freq_mhz:.3f}")
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

    result = service.start(host="kiwi.local", port=8073, password=None, threshold_db=8.0, mode="cw")

    assert result["ok"] is True
    assert order.index("record:7.035") > order.index("scan:cw:7.055")
    assert order.index("record:7.055") > order.index("record:7.035")
    assert any(item.startswith("scan:cw:") for item in order)
    assert not any(item.startswith("scan:phone:") for item in order)

    status = service.status()
    assert status["cw_followup"]["status"] == "complete"
    assert status["cw_followup"]["completed"] == 2
    assert status["cw_followup"]["total"] == 2
    assert status["cw_followup"]["validated_count"] == 2
    assert status["cw_followup"]["summary"] == "Completed 2 CW follow-up decodes; validated 2"
    assert [item["selected_freq_mhz"] for item in status["cw_followup"]["items"]] == [7.035, 7.055]
    assert all(item["decoded_text"] == "CQ TEST" for item in status["cw_followup"]["items"])
    assert status["lanes"]["cw"]["status"] == "complete"
    assert status["lanes"]["phone"]["status"] == "inactive"
    assert status["results"]["phone"] == []
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


def test_receiver_scan_health_channels_include_cw_followup_receiver(tmp_path: Path) -> None:
    service = ReceiverScanService(
        receiver_mgr=_ReceiverMgrStub(),
        auto_set_loop=_AutoSetLoopStub(),
        output_root=tmp_path,
    )

    service._scan_mode = "cw"
    service._mode_active = True
    service._running = True
    service._last_started_ts = time.time() - 18.0
    service._lanes = service._initial_lanes(scan_mode="cw")
    service._lanes["cw"]["status"] = "followup"
    service._lanes["cw"]["last_summary"] = "Recording 60s CW follow-up on 7.035 MHz"
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

    assert sorted(channels.keys()) == ["0"]
    assert channels["0"]["display_name"] == "Receiver Scan CW Follow-up"
    assert channels["0"]["band"] == "40m"
    assert channels["0"]["mode"] == "CW"
    assert channels["0"]["freq_hz"] == 7.035e6
    assert channels["0"]["health_state"] == "Recording 60s CW follow-up on 7.035 MHz"
    assert channels["0"]["status_level"] == "healthy"
    assert channels["0"]["kiwi_user_age_s"] >= 18


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
    service._scan_mode = "phone"

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