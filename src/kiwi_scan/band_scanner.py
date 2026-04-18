from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict
from pathlib import Path
from statistics import fmean
from typing import Callable, Optional

from .bandplan import BANDPLAN, bandplan_label
from .record import RecordRequest, RecorderUnavailable, run_record
from .scan import run_scan
from .kiwi_waterfall import set_receiver_frequency


class BandScanner:
    ZERO_FRAME_WINDOW_RETRIES = 2

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_run_ts: Optional[float] = None
        self._running = False
        self._stop_requested = False
        self._last_report: Optional[str] = None
        self._last_results_report: Optional[str] = None
        self._last_error: Optional[str] = None
        self._progress: dict[str, object] = {}
        self._last_progress: dict[str, object] = {}

    @staticmethod
    def _build_results_report(
        *,
        band: str,
        started_ts: str,
        detector: str,
        report_path: Path,
        hits: list[dict],
    ) -> dict:
        grouped: dict[str, dict[str, object]] = {}
        for hit in hits:
            try:
                freq_mhz = float(hit.get("freq_mhz"))
            except (TypeError, ValueError):
                continue
            if freq_mhz <= 0.0:
                continue

            key = f"{freq_mhz:.4f}"
            group = grouped.setdefault(
                key,
                {
                    "selection_freq_mhz": round(freq_mhz, 4),
                    "hits": [],
                    "center_freqs_hz": set(),
                },
            )
            cast_hits = group["hits"]
            if isinstance(cast_hits, list):
                cast_hits.append(hit)
            center_freq_hz = hit.get("center_freq_hz")
            try:
                if center_freq_hz is not None:
                    cast_centers = group["center_freqs_hz"]
                    if isinstance(cast_centers, set):
                        cast_centers.add(float(center_freq_hz))
            except (TypeError, ValueError):
                pass

        results: list[dict[str, object]] = []
        for key, group in grouped.items():
            group_hits = group.get("hits")
            if not isinstance(group_hits, list) or not group_hits:
                continue
            best_hit = max(
                group_hits,
                key=lambda item: (
                    float(item.get("rel_db") or float("-inf")),
                    float(item.get("observed_frames") or 0.0),
                    float(item.get("width_hz") or 0.0),
                ),
            )
            rel_values = [float(item.get("rel_db")) for item in group_hits if item.get("rel_db") is not None]
            center_freqs_hz = sorted(
                float(value)
                for value in (group.get("center_freqs_hz") or set())
                if value is not None
            )
            best_freq_hz = best_hit.get("freq_hz")
            try:
                best_freq_hz = float(best_freq_hz) if best_freq_hz is not None else round(float(key) * 1_000_000.0, 3)
            except (TypeError, ValueError):
                best_freq_hz = round(float(key) * 1_000_000.0, 3)

            results.append(
                {
                    "selection_freq_mhz": round(float(group["selection_freq_mhz"]), 4),
                    "selection_key": key,
                    "best_freq_mhz": round(float(best_hit.get("freq_mhz") or float(key)), 6),
                    "best_freq_hz": best_freq_hz,
                    "hit_count": len(group_hits),
                    "window_count": len(center_freqs_hz),
                    "center_freqs_mhz": [round(float(value) / 1_000_000.0, 6) for value in center_freqs_hz],
                    "max_rel_db": round(max(rel_values), 1) if rel_values else None,
                    "avg_rel_db": round(fmean(rel_values), 1) if rel_values else None,
                    "bandplan": best_hit.get("bandplan"),
                    "candidate_type": best_hit.get("candidate_type"),
                    "type_guess": best_hit.get("type_guess"),
                    "detector": best_hit.get("detector") or detector,
                    "width_hz": best_hit.get("width_hz"),
                    "occupied_bw_hz": best_hit.get("occ_bw_hz"),
                    "voice_score": best_hit.get("voice_score"),
                    "recording": best_hit.get("recording"),
                }
            )

        results.sort(
            key=lambda item: (
                -float(item.get("max_rel_db") or float("-inf")),
                float(item.get("selection_freq_mhz") or 0.0),
            )
        )
        for index, item in enumerate(results, start=1):
            item["selection_rank"] = index

        return {
            "band": band,
            "started_ts": started_ts,
            "detector": detector,
            "source_report_path": str(report_path),
            "raw_hit_count": len([hit for hit in hits if hit.get("freq_mhz") is not None]),
            "result_count": len(results),
            "results": results,
        }

    def start(
        self,
        *,
        band: str,
        host: str,
        port: int,
        password: Optional[str],
        user: str,
        threshold_db: float,
        rx_chan: int | None = None,
        wf_rx_chan: int | None = None,
        span_hz: float = 30000.0,
        step_hz: Optional[float] = None,
        max_frames: int = 10,
        record_seconds: int = 6,
        record_hits: bool = True,
        output_dir: Path | None = None,
        record_dir: Path | None = None,
        detector: str = "waterfall",
        ssb_probe_only: bool = True,
        required_hits: int | None = None,
        probe_freqs_mhz: list[float] | None = None,
        allow_rx_fallback: bool = True,
        acceptable_rx_chans: tuple[int, ...] | list[int] | None = None,
        before_window_attempt: Optional[Callable[[int, float, int], None]] = None,
        on_hit: Optional[Callable[[dict], None]] = None,
        session_id: Optional[str] = None,
    ) -> dict:
        with self._lock:
            if self._running:
                return {
                    "ok": False,
                    "status": "running",
                    "last_run_ts": self._last_run_ts,
                    "last_report": self._last_report,
                    "last_results_report": self._last_results_report,
                }
            self._running = True
            self._stop_requested = False
            self._last_error = None

        band_key = str(band).strip()
        if band_key not in BANDPLAN:
            with self._lock:
                self._running = False
                self._last_error = f"unknown band: {band_key}"
            return {"ok": False, "status": "error", "error": self._last_error}

        ssb_probes_mhz: dict[str, list[float]] = {
            # These are "anchor" frequencies to quickly find activity.
            # They are not guaranteed to be valid in all regions/bandplans.
            "10m": [28.300, 28.400],
            "12m": [24.950, 24.930],
            "15m": [21.285, 21.300],
            "17m": [18.130, 18.150],
            "20m": [14.300, 14.285, 14.250],
            "40m": [7.285, 7.200, 7.175],
            "60m": [5.357, 5.348, 5.373],
            "80m": [3.900, 3.950, 3.850],
            "160m": [1.900, 1.885],
        }

        segs = BANDPLAN.get(band_key, ())
        band_start = min(float(s.start_hz) for s in segs)
        band_end = max(float(s.end_hz) for s in segs)
        span_hz = float(span_hz)
        if span_hz <= 0:
            span_hz = 30000.0
        step = float(step_hz) if step_hz is not None else float(span_hz) * 0.8
        if step <= 0:
            step = float(span_hz) * 0.8

        if output_dir is None:
            output_dir = Path(__file__).resolve().parents[2] / "outputs" / "band_scans"
        if record_dir is None:
            record_dir = Path(__file__).resolve().parents[2] / "outputs" / "band_scan_recordings"

        output_dir.mkdir(parents=True, exist_ok=True)
        record_dir.mkdir(parents=True, exist_ok=True)

        ts = time.strftime("%Y%m%d_%H%M%S")
        report_path = output_dir / f"band_scan_{band_key}_{ts}.json"
        results_path = output_dir / f"band_scan_results_{band_key}_{ts}.json"

        detector_key = str(detector or "waterfall").strip().lower()
        ssb_mode = detector_key in {"ssb", "phone", "voice"}
        # For SSB probes we want to:
        # - be fast (low hold time)
        # - reject narrow/spiky digital/CW by requiring some occupied bandwidth
        min_width_hz = 1200.0 if ssb_mode else 0.0
        hold_s = 1.5 if ssb_mode else 5.0
        default_required_hits = 1 if ssb_mode else 2
        try:
            ssb_required_hits = int(required_hits) if required_hits is not None else int(default_required_hits)
        except Exception:
            ssb_required_hits = int(default_required_hits)
        if ssb_required_hits < 1:
            ssb_required_hits = 1
        ssb_occ_thresh_db = 5.0 if ssb_mode else 6.0
        ssb_voice_min_score = 0.45 if ssb_mode else 0.0

        centers: list[float] = []
        if ssb_mode and isinstance(probe_freqs_mhz, list) and probe_freqs_mhz:
            centers = [float(mhz) * 1e6 for mhz in probe_freqs_mhz if mhz is not None and float(mhz) > 0]
        elif ssb_mode and ssb_probe_only and band_key in ssb_probes_mhz:
            probes = list(ssb_probes_mhz[band_key])
            # Light time-of-day ordering: at night, put lower-frequency anchors first.
            try:
                hr = time.localtime().tm_hour
            except Exception:
                hr = 12
            is_night = (hr >= 18) or (hr < 7)
            if is_night and band_key in {"40m", "80m", "160m"}:
                probes = sorted(probes)
            centers = [float(mhz) * 1e6 for mhz in probes]
        else:
            center = band_start + span_hz / 2.0
            while center <= band_end - span_hz / 2.0:
                centers.append(center)
                center += step
        if not centers:
            centers = [(band_start + band_end) / 2.0]
        if centers:
            centers = sorted({float(c) for c in centers if c is not None and float(c) > 0})

        def _read_probe_report(path: Path) -> dict | None:
            if not path.exists():
                return None
            try:
                probe_report = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return None
            return probe_report if isinstance(probe_report, dict) else None

        def _run() -> None:
            hits: list[dict] = []
            probe_summaries: list[dict] = []
            effective_allow_rx_fallback = bool(allow_rx_fallback)
            try:
                for idx, center_freq in enumerate(centers, start=1):
                    with self._lock:
                        if self._stop_requested:
                            break
                    scan_rx = wf_rx_chan if wf_rx_chan is not None else rx_chan
                    if ssb_mode:
                        if scan_rx is None:
                            scan_rx = 0
                        else:
                            try:
                                scan_rx = int(scan_rx)
                            except Exception:
                                scan_rx = 0
                        if int(scan_rx) not in {0, 1}:
                            scan_rx = 0
                        effective_allow_rx_fallback = False
                    snd_thread = None
                    use_snd_hold = rx_chan is not None and (wf_rx_chan is None or int(wf_rx_chan) != int(rx_chan))
                    if use_snd_hold:
                        def _hold_snd(freq_hz: float) -> None:
                            try:
                                set_receiver_frequency(
                                    host=host,
                                    port=int(port),
                                    rx_chan=int(rx_chan),
                                    freq_hz=float(freq_hz),
                                    password=password,
                                    user=f"{user}-snd",
                                    timeout_s=10.0,
                                    hold_s=12.0,
                                    rx_wait_timeout_s=8.0,
                                    rx_wait_interval_s=1.0,
                                    rx_wait_max_retries=5,
                                )
                            except Exception:
                                pass
                        snd_thread = threading.Thread(target=_hold_snd, args=(float(center_freq),), daemon=True)
                        snd_thread.start()
                    if idx > 1 and (wf_rx_chan is not None or rx_chan is not None):
                        time.sleep(1.0)
                    with self._lock:
                        self._progress = {
                            "band": band_key,
                            "window_index": idx,
                            "window_total": len(centers),
                            "center_freq_hz": center_freq,
                            "rx_chan_requested": int(scan_rx) if scan_rx is not None else None,
                            "rx_mode": "fixed" if scan_rx is not None else "auto",
                            "rx_fallback": False,
                        }
                        self._last_progress = dict(self._progress)
                    window_tag = f"{band_key}_{ts}_{idx:02d}"
                    jsonl_path = output_dir / f"band_scan_hits_{window_tag}.jsonl"
                    jsonl_events_path = output_dir / f"band_scan_events_{window_tag}.jsonl"
                    json_probe_report_path = output_dir / f"band_scan_probe_{window_tag}.json"
                    rc = None
                    pr = None
                    window_error: Exception | None = None
                    for window_attempt in range(1, int(self.ZERO_FRAME_WINDOW_RETRIES) + 2):
                        window_error = None
                        if callable(before_window_attempt):
                            try:
                                before_window_attempt(int(idx), float(center_freq), int(window_attempt))
                            except Exception:
                                pass
                        try:
                            rc = run_scan(
                                host=host,
                                port=int(port),
                                password=password,
                                user=user,
                                rx_chan=scan_rx,
                                band=band_key,
                                center_freq_hz=float(center_freq),
                                span_hz=float(span_hz),
                                threshold_db=float(threshold_db),
                                min_width_bins=2,
                                min_width_hz=float(min_width_hz),
                                ssb_detect=bool(ssb_mode),
                                ssb_only=bool(ssb_mode),
                                required_hits=int(ssb_required_hits),
                                tolerance_bins=2.5,
                                expiry_frames=6,
                                max_frames=int(max_frames),
                                jsonl_path=jsonl_path,
                                jsonl_events_path=jsonl_events_path,
                                json_report_path=json_probe_report_path,
                                min_s=1.0,
                                status_hold_s=float(hold_s),
                                record=False,
                                record_seconds=int(record_seconds),
                                record_mode="usb",
                                record_out=record_dir,
                                phone_only=bool(ssb_mode),
                                ssb_occ_thresh_db=float(ssb_occ_thresh_db),
                                ssb_voice_min_score=float(ssb_voice_min_score),
                                ssb_early_stop_frames=2 if ssb_mode else 0,
                                ssb_warmup_frames=2 if ssb_mode else 0,
                                ssb_adaptive_threshold=bool(ssb_mode),
                                ssb_adaptive_min_db=8.0,
                                ssb_adaptive_max_db=22.0,
                                ssb_adaptive_spread_gain=0.18,
                                ssb_adaptive_spread_offset_db=0.0,
                                ssb_adaptive_spread_target_db=55.0,
                                acceptable_rx_chans=acceptable_rx_chans,
                                rx_wait_timeout_s=12.0,
                                rx_wait_interval_s=1.0,
                                rx_wait_max_retries=12,
                                max_runtime_s=12.0,
                                show=False,
                            )
                            if rc == 3 and ssb_mode and bool(effective_allow_rx_fallback):
                                with self._lock:
                                    self._progress = {
                                        **self._progress,
                                        "rx_chan_requested": None,
                                        "rx_mode": "auto",
                                        "rx_fallback": True,
                                    }
                                    self._last_progress = dict(self._progress)
                                rc = run_scan(
                                    host=host,
                                    port=int(port),
                                    password=password,
                                    user=user,
                                    rx_chan=None,
                                    band=band_key,
                                    center_freq_hz=float(center_freq),
                                    span_hz=float(span_hz),
                                    threshold_db=float(threshold_db),
                                    min_width_bins=2,
                                    min_width_hz=float(min_width_hz),
                                    ssb_detect=bool(ssb_mode),
                                    ssb_only=bool(ssb_mode),
                                    required_hits=int(ssb_required_hits),
                                    tolerance_bins=2.5,
                                    expiry_frames=6,
                                    max_frames=int(max_frames),
                                    jsonl_path=jsonl_path,
                                    jsonl_events_path=jsonl_events_path,
                                    json_report_path=json_probe_report_path,
                                    min_s=1.0,
                                    status_hold_s=float(hold_s),
                                    record=False,
                                    record_seconds=int(record_seconds),
                                    record_mode="usb",
                                    record_out=record_dir,
                                    phone_only=bool(ssb_mode),
                                    ssb_occ_thresh_db=float(ssb_occ_thresh_db),
                                    ssb_voice_min_score=float(ssb_voice_min_score),
                                    ssb_early_stop_frames=2 if ssb_mode else 0,
                                    ssb_warmup_frames=2 if ssb_mode else 0,
                                    ssb_adaptive_threshold=bool(ssb_mode),
                                    ssb_adaptive_min_db=8.0,
                                    ssb_adaptive_max_db=22.0,
                                    ssb_adaptive_spread_gain=0.18,
                                    ssb_adaptive_spread_offset_db=0.0,
                                    ssb_adaptive_spread_target_db=55.0,
                                    acceptable_rx_chans=acceptable_rx_chans,
                                    rx_wait_timeout_s=12.0,
                                    rx_wait_interval_s=1.0,
                                    rx_wait_max_retries=12,
                                    max_runtime_s=12.0,
                                    show=False,
                                )
                        except Exception as e:
                            window_error = e
                            rc = None

                        pr = _read_probe_report(json_probe_report_path)
                        zero_frame_exit = (
                            isinstance(pr, dict)
                            and int(pr.get("frames_seen") or 0) == 0
                            and pr.get("stop_reason") is None
                        )
                        if zero_frame_exit and window_attempt <= int(self.ZERO_FRAME_WINDOW_RETRIES):
                            print(
                                f"WARN: band scan window {idx}/{len(centers)} exited before first frame; retrying window"
                            )
                            continue
                        break

                    if isinstance(pr, dict):
                        probe_summaries.append(
                            {
                                "center_freq_hz": float(center_freq),
                                "peak": pr.get("peak"),
                                "frames_seen": pr.get("frames_seen"),
                                "ssb_frames_seen": pr.get("ssb_frames_seen"),
                                "ssb_seen_good": pr.get("ssb_seen_good"),
                                "stop_reason": pr.get("stop_reason"),
                                "return_code": int(rc) if rc is not None else (2 if window_error is not None else 0),
                            }
                        )

                    if window_error is not None:
                        hits.append({
                            "center_freq_hz": float(center_freq),
                            "error": f"scan_error: {type(window_error).__name__}: {window_error}",
                        })
                        continue

                    if rc not in (0, None):
                        hits.append({
                            "center_freq_hz": float(center_freq),
                            "error": f"scan_error: return_code={rc}",
                        })
                    if snd_thread is not None:
                        snd_thread.join(timeout=0.1)

                    with self._lock:
                        if self._stop_requested:
                            break

                    if jsonl_path.exists():
                        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
                            try:
                                det = json.loads(line)
                            except Exception:
                                continue
                            freq_mhz = det.get("freq_mhz")
                            freq_hz = float(freq_mhz) * 1e6 if freq_mhz is not None else None
                            label = bandplan_label(freq_hz) if freq_hz is not None else None
                            peak_power = det.get("peak_power")
                            noise_floor = det.get("noise_floor")
                            rel_db = None
                            try:
                                if peak_power is not None and noise_floor is not None:
                                    rel_db = float(peak_power) - float(noise_floor)
                            except Exception:
                                rel_db = None
                            hit = {
                                "freq_hz": freq_hz,
                                "freq_mhz": freq_mhz,
                                "center_freq_hz": float(center_freq),
                                "width_hz": det.get("width_hz"),
                                "width_bins": det.get("width_bins"),
                                "occ_bw_hz": det.get("occ_bw_hz"),
                                "occ_frac": det.get("occ_frac"),
                                "voice_score": det.get("voice_score"),
                                "narrow_peak_count": det.get("narrow_peak_count"),
                                "narrow_peak_span_hz": det.get("narrow_peak_span_hz"),
                                "keying_score": det.get("keying_score"),
                                "steady_tone_score": det.get("steady_tone_score"),
                                "freq_stability_hz": det.get("freq_stability_hz"),
                                "envelope_variance": det.get("envelope_variance"),
                                "speech_envelope_score": det.get("speech_envelope_score"),
                                "sweep_score": det.get("sweep_score"),
                                "centroid_drift_hz": det.get("centroid_drift_hz"),
                                "observed_frames": det.get("observed_frames"),
                                "active_fraction": det.get("active_fraction"),
                                "cadence_score": det.get("cadence_score"),
                                "keying_edge_count": det.get("keying_edge_count"),
                                "has_on_off_keying": det.get("has_on_off_keying"),
                                "amplitude_span_db": det.get("amplitude_span_db"),
                                "type_guess": det.get("type_guess"),
                                "candidate_type": det.get("candidate_type"),
                                "bandplan": det.get("bandplan") or label,
                                "detector": detector_key,
                                "t_unix": det.get("t_unix"),
                                "rel_db": rel_db,
                                "band": band_key,
                            }
                            hits.append(hit)
                            if on_hit is not None:
                                try:
                                    on_hit(dict(hit))
                                except Exception:
                                    pass
                    
                    # Keep a fixed RX channel visibly active between windows.
                    # This helps Kiwi admin pages show the selected receiver as
                    # occupied/frequency-held instead of briefly appearing idle
                    # during per-window scanner teardown/startup transitions.
                    if rx_chan is not None and idx < len(centers):
                        try:
                            set_receiver_frequency(
                                host=host,
                                port=int(port),
                                rx_chan=int(rx_chan),
                                freq_hz=float(center_freq),
                                password=password,
                                user=f"{user}-hold",
                                timeout_s=6.0,
                                hold_s=1.2,
                                rx_wait_timeout_s=3.0,
                                rx_wait_interval_s=0.5,
                                rx_wait_max_retries=2,
                            )
                        except Exception:
                            pass

                recordings: dict[str, str] = {}
                if record_hits:
                    unique = {}
                    for h in hits:
                        if h.get("freq_mhz") is None:
                            continue
                        key = f"{float(h['freq_mhz']):.4f}"
                        unique[key] = h
                    for key, h in unique.items():
                        try:
                            out_dir = record_dir / f"{band_key}_{ts}"
                            out_dir.mkdir(parents=True, exist_ok=True)
                            run_record(
                                RecordRequest(
                                    host=host,
                                    port=int(port),
                                    password=password,
                                    user=user,
                                    freq_hz=float(h["freq_hz"]),
                                    duration_s=int(record_seconds),
                                    mode="usb",
                                    out_dir=out_dir,
                                )
                            )
                            recordings[key] = str(out_dir)
                        except RecorderUnavailable as e:
                            recordings[key] = f"record_failed: {e}"
                        except Exception as e:
                            recordings[key] = f"record_failed: {type(e).__name__}: {e}"

                for h in hits:
                    if h.get("freq_mhz") is None:
                        continue
                    key = f"{float(h['freq_mhz']):.4f}"
                    if key in recordings:
                        h["recording"] = recordings[key]

                report = {
                    "band": band_key,
                    "started_ts": ts,
                    "host": host,
                    "port": int(port),
                    "detector": detector_key,
                    "span_hz": float(span_hz),
                    "step_hz": float(step),
                    "windows": len(centers),
                    "ssb_probes_mhz": ssb_probes_mhz.get(band_key) if ssb_mode else None,
                    "probe_summaries": probe_summaries,
                    "hits": hits,
                }
                report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
                results_report = self._build_results_report(
                    band=band_key,
                    started_ts=ts,
                    detector=detector_key,
                    report_path=report_path,
                    hits=hits,
                )
                results_path.write_text(json.dumps(results_report, indent=2), encoding="utf-8")
                if session_id:
                    try:
                        session_path = output_dir / f"band_scan_session_{session_id}.json"
                        combined = {
                            "session_id": str(session_id),
                            "updated_ts": time.strftime("%Y%m%d_%H%M%S"),
                            "runs": [],
                            "results_reports": [],
                        }
                        if session_path.exists():
                            try:
                                combined = json.loads(session_path.read_text(encoding="utf-8"))
                            except Exception:
                                pass
                        if not isinstance(combined.get("runs"), list):
                            combined["runs"] = []
                        if not isinstance(combined.get("results_reports"), list):
                            combined["results_reports"] = []
                        combined["runs"].append(report)
                        combined["results_reports"].append(results_report)
                        combined["updated_ts"] = time.strftime("%Y%m%d_%H%M%S")
                        session_path.write_text(json.dumps(combined, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                with self._lock:
                    self._last_report = str(report_path)
                    self._last_results_report = str(results_path)
            except Exception as e:
                with self._lock:
                    self._last_error = f"{type(e).__name__}: {e}"
            finally:
                with self._lock:
                    if self._progress:
                        self._last_progress = dict(self._progress)
                    self._running = False
                    self._stop_requested = False
                    self._last_run_ts = time.time()
                    self._progress = {}

        threading.Thread(target=_run, daemon=True).start()
        return {
            "ok": True,
            "status": "started",
            "band": band_key,
            "last_run_ts": self._last_run_ts,
            "report_path": str(report_path),
            "results_path": str(results_path),
        }

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self._running,
                "stop_requested": self._stop_requested,
                "last_run_ts": self._last_run_ts,
                "last_report": self._last_report,
                "last_results_report": self._last_results_report,
                "last_error": self._last_error,
                "progress": dict(self._progress),
                "last_progress": dict(self._last_progress),
            }

    def results(self) -> dict:
        with self._lock:
            results_path = self._last_results_report

        if not results_path:
            return {
                "ok": False,
                "status": "idle",
                "report_path": None,
                "results": [],
            }

        try:
            payload = json.loads(Path(results_path).read_text(encoding="utf-8"))
        except Exception as exc:
            return {
                "ok": False,
                "status": "error",
                "report_path": str(results_path),
                "error": f"{type(exc).__name__}: {exc}",
                "results": [],
            }

        if not isinstance(payload, dict):
            return {
                "ok": False,
                "status": "error",
                "report_path": str(results_path),
                "error": "invalid results payload",
                "results": [],
            }

        payload["ok"] = True
        payload["status"] = "ready"
        payload["report_path"] = str(results_path)
        return payload

    def stop(self) -> dict:
        with self._lock:
            if not self._running:
                return {
                    "ok": False,
                    "status": "idle",
                    "stop_requested": False,
                }
            self._stop_requested = True
            return {
                "ok": True,
                "status": "stopping",
                "stop_requested": True,
            }
