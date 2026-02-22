from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Optional

from .bandplan import BANDPLAN, bandplan_label
from .record import RecordRequest, RecorderUnavailable, run_record
from .scan import run_scan
from .kiwi_waterfall import set_receiver_frequency


class BandScanner:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_run_ts: Optional[float] = None
        self._running = False
        self._stop_requested = False
        self._last_report: Optional[str] = None
        self._last_error: Optional[str] = None
        self._progress: dict[str, object] = {}
        self._last_progress: dict[str, object] = {}

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
        allow_rx_fallback: bool = True,
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

        detector_key = str(detector or "waterfall").strip().lower()
        ssb_mode = detector_key in {"ssb", "phone", "voice"}
        # For SSB probes we want to:
        # - be fast (low hold time)
        # - reject narrow/spiky digital/CW by requiring some occupied bandwidth
        min_width_hz = 1200.0 if ssb_mode else 0.0
        hold_s = 1.5 if ssb_mode else 5.0
        ssb_required_hits = 1 if ssb_mode else 2
        ssb_occ_thresh_db = 5.0 if ssb_mode else 6.0
        ssb_voice_min_score = 0.45 if ssb_mode else 0.0

        centers: list[float] = []
        if ssb_mode and ssb_probe_only and band_key in ssb_probes_mhz:
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

        def _run() -> None:
            hits: list[dict] = []
            probe_summaries: list[dict] = []
            try:
                for idx, center_freq in enumerate(centers, start=1):
                    with self._lock:
                        if self._stop_requested:
                            break
                    scan_rx = wf_rx_chan if wf_rx_chan is not None else rx_chan
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
                            rx_wait_timeout_s=12.0,
                            rx_wait_interval_s=1.0,
                            rx_wait_max_retries=12,
                            max_runtime_s=12.0,
                            show=False,
                        )
                        if rc == 3 and ssb_mode and bool(allow_rx_fallback):
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
                                rx_wait_timeout_s=12.0,
                                rx_wait_interval_s=1.0,
                                rx_wait_max_retries=12,
                                max_runtime_s=12.0,
                                show=False,
                            )

                        # Always capture per-probe summary (even if no hits)
                        if json_probe_report_path.exists():
                            try:
                                pr = json.loads(json_probe_report_path.read_text(encoding="utf-8"))
                            except Exception:
                                pr = None
                            if isinstance(pr, dict):
                                probe_summaries.append(
                                    {
                                        "center_freq_hz": float(center_freq),
                                        "peak": pr.get("peak"),
                                        "frames_seen": pr.get("frames_seen"),
                                        "ssb_frames_seen": pr.get("ssb_frames_seen"),
                                        "ssb_seen_good": pr.get("ssb_seen_good"),
                                        "stop_reason": pr.get("stop_reason"),
                                        "return_code": int(rc) if rc is not None else 0,
                                    }
                                )

                        if rc not in (0, None):
                            hits.append({
                                "center_freq_hz": float(center_freq),
                                "error": f"scan_error: return_code={rc}",
                            })
                    except Exception as e:
                        hits.append({
                            "center_freq_hz": float(center_freq),
                            "error": f"scan_error: {type(e).__name__}: {e}",
                        })
                        continue
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
                                "type_guess": det.get("type_guess"),
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
                if session_id:
                    try:
                        session_path = output_dir / f"band_scan_session_{session_id}.json"
                        combined = {
                            "session_id": str(session_id),
                            "updated_ts": time.strftime("%Y%m%d_%H%M%S"),
                            "runs": [],
                        }
                        if session_path.exists():
                            try:
                                combined = json.loads(session_path.read_text(encoding="utf-8"))
                            except Exception:
                                pass
                        if not isinstance(combined.get("runs"), list):
                            combined["runs"] = []
                        combined["runs"].append(report)
                        combined["updated_ts"] = time.strftime("%Y%m%d_%H%M%S")
                        session_path.write_text(json.dumps(combined, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                with self._lock:
                    self._last_report = str(report_path)
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
        }

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self._running,
                "stop_requested": self._stop_requested,
                "last_run_ts": self._last_run_ts,
                "last_report": self._last_report,
                "last_error": self._last_error,
                "progress": dict(self._progress),
                "last_progress": dict(self._last_progress),
            }

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
