from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .auto_set_loop import AutoSetLoop, _FIXED_ASSIGNMENTS
from .cw_decode import try_decode_cw_wav, validate_cw_message
from .record import RecordRequest, RecorderUnavailable, run_record
from .receiver_manager import ReceiverAssignment
from .scan import run_scan


logger = logging.getLogger(__name__)


def _build_stepwise_freqs_mhz(*, start_mhz: float, end_mhz: float, step_hz: float) -> list[float]:
    start_hz = int(round(float(start_mhz) * 1_000_000.0))
    end_hz = int(round(float(end_mhz) * 1_000_000.0))
    step_hz_int = max(1, int(round(float(step_hz))))
    freqs: list[float] = []
    current_hz = start_hz
    while current_hz <= end_hz:
        freqs.append(round(current_hz / 1_000_000.0, 3))
        current_hz += step_hz_int
    end_freq_mhz = round(end_hz / 1_000_000.0, 3)
    if not freqs or freqs[-1] != end_freq_mhz:
        freqs.append(end_freq_mhz)
    return freqs


class ReceiverScanService:
    BAND = "40m"
    MODE_LABEL = "40m IQ"
    HOLD_REASON = "receiver_scan"
    RESERVED_RECEIVERS = (0, 1)
    LISTEN_SECONDS = 2.5
    CW_FOLLOWUP_SECONDS = 60
    CW_FREQS_MHZ = [7.025, 7.035, 7.045, 7.055]
    PHONE_SCAN_START_MHZ = 7.125
    PHONE_SCAN_END_MHZ = 7.300
    PHONE_STEP_HZ = 5_000.0
    PHONE_SPAN_HZ = 5_000.0
    PHONE_MIN_WIDTH_HZ = 1_000.0
    PHONE_VOICE_MIN_SCORE = 0.18
    PHONE_MAX_FRAMES = 12
    PHONE_EARLY_STOP_FRAMES = 0
    PHONE_ACTIVITY_MIN_SCORE = 45
    PHONE_FREQS_MHZ = _build_stepwise_freqs_mhz(
        start_mhz=PHONE_SCAN_START_MHZ,
        end_mhz=PHONE_SCAN_END_MHZ,
        step_hz=PHONE_STEP_HZ,
    )

    def __init__(
        self,
        *,
        receiver_mgr: object,
        auto_set_loop: AutoSetLoop | None = None,
        output_root: Path | None = None,
    ) -> None:
        self._receiver_mgr = receiver_mgr
        self._auto_set_loop = auto_set_loop
        self._output_root = output_root or (Path(__file__).resolve().parents[2] / "outputs" / "receiver_scans")
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._activating = False
        self._mode_active = False
        self._release_requested = False
        self._running = False
        self._last_error: str | None = None
        self._last_started_ts: float | None = None
        self._last_finished_ts: float | None = None
        self._session_id: str | None = None
        self._results: dict[str, list[dict[str, Any]]] = {"cw": [], "phone": []}
        self._lanes: dict[str, dict[str, Any]] = self._initial_lanes()
        self._cw_followup: dict[str, Any] = self._initial_cw_followup()

    def _initial_lanes(self) -> dict[str, dict[str, Any]]:
        return {
            "cw": {
                "lane": "cw",
                "label": "CW Anchors",
                "rx_chan": 0,
                "status": "idle",
                "completed": 0,
                "total": len(self.CW_FREQS_MHZ),
                "current_freq_mhz": None,
                "last_score": None,
                "last_summary": "Waiting for scan",
            },
            "phone": {
                "lane": "phone",
                "label": "Phone Anchors",
                "rx_chan": 1,
                "status": "idle",
                "completed": 0,
                "total": len(self.PHONE_FREQS_MHZ),
                "current_freq_mhz": None,
                "last_score": None,
                "last_summary": "Waiting for CW scan",
            },
        }

    def _initial_cw_followup(self) -> dict[str, Any]:
        return {
            "status": "idle",
            "rx_chan": int(self.RESERVED_RECEIVERS[0]),
            "duration_s": int(self.CW_FOLLOWUP_SECONDS),
            "selected_freq_mhz": None,
            "signal_count": 0,
            "score": None,
            "recording_path": None,
            "wav_path": None,
            "decoded_text": "",
            "validated_text": "",
            "message_valid": False,
            "validation_reason": "",
            "validation_summary": "Waiting for CW scan",
            "confidence": 0.0,
            "tone_hz": None,
            "dot_ms": None,
            "wpm_est": None,
            "summary": "Waiting for CW scan",
        }

    def _spawn_thread(self, *, name: str, target: Callable[[], None]) -> threading.Thread:
        return threading.Thread(name=name, target=target, daemon=True)

    def _build_fixed_assignments(self) -> dict[int, ReceiverAssignment]:
        assignments: dict[int, ReceiverAssignment] = {}
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            assignments[rx] = ReceiverAssignment(
                rx=rx,
                band=str(entry["band"]),
                freq_hz=float(entry["freq_hz"]),
                mode_label=str(entry["mode"]),
                ignore_slot_check=True,
            )
        return assignments

    def _clear_reserved_slot(self, *, host: str, port: int, rx_chan: int) -> None:
        kick = getattr(self._receiver_mgr, "_run_admin_kick_all", None)
        wait_clear = getattr(self._receiver_mgr, "_wait_for_kiwi_slots_clear", None)

        if callable(kick):
            try:
                kick(
                    host=host,
                    port=int(port),
                    kick_only_slots=[int(rx_chan)],
                    allow_fallback_kick_all=False,
                )
            except Exception:
                logger.exception("Receiver Scan failed clearing reserved RX%s", int(rx_chan))

        if callable(wait_clear):
            try:
                wait_clear(
                    host=host,
                    port=int(port),
                    slots={int(rx_chan)},
                    stable_secs=0.75,
                    timeout_s=4.0,
                )
            except Exception:
                logger.exception("Receiver Scan failed waiting for reserved RX%s clear", int(rx_chan))

    def _clear_reserved_slots(self, *, host: str, port: int) -> None:
        for rx_chan in self.RESERVED_RECEIVERS:
            self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(rx_chan))

    def _enter_mode(self, *, host: str, port: int) -> None:
        if self._auto_set_loop is not None:
            self._auto_set_loop.pause_for_external(self.HOLD_REASON)
        self._clear_reserved_slots(host=host, port=int(port))
        assignments = self._build_fixed_assignments()
        self._receiver_mgr.apply_assignments(  # type: ignore[attr-defined]
            host,
            int(port),
            assignments,
            allow_starting_from_empty_full_reset=False,
        )
        with self._lock:
            self._mode_active = True
            self._release_requested = False

    def _leave_mode(self) -> None:
        should_resume = False
        with self._lock:
            if self._mode_active:
                should_resume = True
            self._mode_active = False
            self._release_requested = False
        if should_resume and self._auto_set_loop is not None:
            self._auto_set_loop.resume_from_external(self.HOLD_REASON)

    def _state_label_locked(self) -> str:
        if self._activating:
            return "stopping" if self._stop_requested.is_set() else "starting"
        if self._running and self._stop_requested.is_set():
            return "stopping"
        if self._running:
            return "running"
        if self._mode_active:
            return "ready"
        return "idle"

    def status(self) -> dict[str, Any]:
        with self._lock:
            cw_followup = dict(self._cw_followup)
            results = {key: [dict(item) for item in value] for key, value in self._results.items()}
            return {
                "ok": True,
                "status": self._state_label_locked(),
                "activating": bool(self._activating),
                "mode_active": bool(self._mode_active),
                "running": bool(self._running),
                "stop_requested": bool(self._stop_requested.is_set()),
                "band": self.BAND,
                "mode_label": self.MODE_LABEL,
                "listen_seconds": float(self.LISTEN_SECONDS),
                "session_id": self._session_id,
                "reserved_receivers": [0, 1],
                "fixed_receivers": [int(entry["rx"]) for entry in _FIXED_ASSIGNMENTS],
                "plan": {
                    "scan_order": ["cw", "phone", "cw_followup"],
                    "parallel_lanes": True,
                    "cw_freqs_mhz": list(self.CW_FREQS_MHZ),
                    "cw_followup_seconds": int(self.CW_FOLLOWUP_SECONDS),
                    "phone_range_mhz": {
                        "start": float(self.PHONE_SCAN_START_MHZ),
                        "end": float(self.PHONE_SCAN_END_MHZ),
                    },
                    "phone_freqs_mhz": list(self.PHONE_FREQS_MHZ),
                },
                "lanes": {key: dict(value) for key, value in self._lanes.items()},
                "cw_followup": cw_followup,
                "results": self._annotate_results(results=results, cw_followup=cw_followup),
                "last_error": self._last_error,
                "last_started_ts": self._last_started_ts,
                "last_finished_ts": self._last_finished_ts,
            }

    @staticmethod
    def _annotate_results(
        *,
        results: dict[str, list[dict[str, Any]]],
        cw_followup: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        target_freq = cw_followup.get("selected_freq_mhz")
        if target_freq is None:
            return results
        try:
            target_freq = float(target_freq)
        except Exception:
            return results

        for item in results.get("cw", []):
            try:
                freq_mhz = float(item.get("freq_mhz") or 0.0)
            except Exception:
                continue
            if abs(freq_mhz - target_freq) > 1e-6:
                continue
            item["followup_selected"] = True
            item["followup_status"] = cw_followup.get("status")
            item["followup_summary"] = cw_followup.get("summary")
            item["followup_decoded_text"] = cw_followup.get("decoded_text")
            item["followup_validated_text"] = cw_followup.get("validated_text")
            item["followup_message_valid"] = bool(cw_followup.get("message_valid"))
            item["followup_validation_reason"] = cw_followup.get("validation_reason")
            item["followup_validation_summary"] = cw_followup.get("validation_summary")
            item["followup_confidence"] = cw_followup.get("confidence")
            item["followup_tone_hz"] = cw_followup.get("tone_hz")
            item["followup_wpm_est"] = cw_followup.get("wpm_est")
            break
        return results

    def health_channels(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            if not self._mode_active:
                return {}
            running = bool(self._running)
            lanes = {key: dict(value) for key, value in self._lanes.items()}
            cw_followup = dict(self._cw_followup)
            started_ts = float(self._last_started_ts) if self._last_started_ts is not None else None
            last_error = self._last_error

        now = time.time()
        connected_seconds = max(0, int(now - started_ts)) if started_ts is not None else None
        followup_status = str(cw_followup.get("status") or "idle").strip().lower()
        followup_freq_mhz = cw_followup.get("selected_freq_mhz")
        followup_summary = str(cw_followup.get("summary") or "").strip()

        channels: dict[str, dict[str, Any]] = {}
        for lane_key, lane in lanes.items():
            rx_chan = int(lane.get("rx_chan") or 0)
            lane_status = str(lane.get("status") or "idle").strip().lower()
            lane_summary = str(lane.get("last_summary") or "").strip()
            current_freq_mhz = lane.get("current_freq_mhz")
            display_name = "Receiver Scan CW" if lane_key == "cw" else "Receiver Scan Phone"
            mode = "CW" if lane_key == "cw" else "PHONE"
            if lane_key == "cw" and followup_status in {"recording", "decoding"}:
                display_name = "Receiver Scan CW Follow-up"
                current_freq_mhz = followup_freq_mhz if followup_freq_mhz is not None else current_freq_mhz
                lane_summary = followup_summary or lane_summary
            elif lane_key == "cw" and followup_status in {"complete", "error", "stopped"} and followup_freq_mhz is not None:
                current_freq_mhz = followup_freq_mhz

            status_level = "healthy"
            if lane_status == "error" or (lane_key == "cw" and followup_status == "error"):
                status_level = "fault"
            elif lane_status in {"stopped", "waiting"}:
                status_level = "warning"

            state_text = lane_summary or lane_status.replace("_", " ").title() or "Waiting for scan"
            active = bool(running or lane_status not in {"idle"})
            channels[str(rx_chan)] = {
                "rx": int(rx_chan),
                "kiwi_rx": int(rx_chan),
                "freq_hz": (float(current_freq_mhz) * 1e6) if current_freq_mhz is not None else None,
                "band": self.BAND,
                "mode": mode,
                "active": active,
                "visible_on_kiwi": active,
                "kiwi_user_age_s": connected_seconds,
                "kiwi_actual_rx": int(rx_chan),
                "restart_count": 0,
                "consecutive_failures": 0,
                "backoff_s": 0.0,
                "cooling_down": False,
                "cooldown_remaining_s": 0.0,
                "last_reason": last_error if status_level == "fault" and last_error else (state_text if status_level != "healthy" else None),
                "last_updated_unix": now,
                "last_decoder_output_unix": None,
                "last_decode_unix": None,
                "decoder_output_age_s": None,
                "decode_age_s": None,
                "snr_last_db": None,
                "snr_avg_db": None,
                "snr_samples": 0,
                "snr_age_s": None,
                "decode_total": 0,
                "decode_rate_per_min": 0,
                "decode_rate_per_hour": 0,
                "decode_rates_by_mode": {},
                "propagation_state": "unknown",
                "health_state": state_text,
                "status_level": status_level,
                "is_no_decode_warning": False,
                "is_silent": False,
                "is_stalled": status_level == "fault",
                "is_unstable": status_level == "fault",
                "display_name": display_name,
                "is_scan_channel": True,
            }
        return channels

    def start(self, *, host: str, port: int, password: str | None, threshold_db: float) -> dict[str, Any]:
        already_active = False
        with self._lock:
            if self._running or self._activating:
                already_active = True
            else:
                self._activating = True
                self._stop_requested.clear()
                self._release_requested = False
                self._last_error = None
                self._last_started_ts = time.time()
                self._last_finished_ts = None
                self._session_id = time.strftime("receiver_scan_%Y%m%d_%H%M%S")
                self._results = {"cw": [], "phone": []}
                self._lanes = self._initial_lanes()
                self._cw_followup = self._initial_cw_followup()
                for lane in self._lanes.values():
                    lane["status"] = "starting"
                    lane["last_summary"] = "Activating receivers"

        if already_active:
            payload = self.status()
            payload["ok"] = False
            return payload

        thread = self._spawn_thread(
            name="receiver-scan",
            target=lambda: self._activate_and_run_session(
                host=host,
                port=int(port),
                password=password,
                threshold_db=float(threshold_db),
            ),
        )
        with self._lock:
            self._thread = thread
        thread.start()
        payload = self.status()
        return payload

    def stop(self) -> dict[str, Any]:
        with self._lock:
            running = bool(self._running)
            activating = bool(self._activating)
        if not running and not activating:
            return self.status()
        self._stop_requested.set()
        payload = self.status()
        payload["status"] = "stopping"
        return payload

    def deactivate(self, *, wait_timeout_s: float = 8.0) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            active = bool(self._running or self._activating)
            if active:
                self._release_requested = True
                self._stop_requested.set()

        if active and thread is not None:
            thread.join(timeout=max(0.0, float(wait_timeout_s)))

        with self._lock:
            still_running = bool(self._running or self._activating)
        if not still_running:
            self._leave_mode()
        payload = self.status()
        payload["status"] = "stopping" if still_running else "idle"
        return payload

    def _activate_and_run_session(
        self,
        *,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> None:
        try:
            logger.info("Receiver Scan activation starting on %s:%s", host, port)
            self._enter_mode(host=host, port=port)
        except Exception as exc:
            logger.exception("Receiver Scan activation failed")
            self._leave_mode()
            with self._lock:
                self._activating = False
                self._running = False
                self._thread = None
                self._last_error = f"Receiver Scan activation failed: {exc}"
                self._last_finished_ts = time.time()
                for lane in self._lanes.values():
                    lane["status"] = "error"
                    lane["current_freq_mhz"] = None
                    lane["last_summary"] = self._last_error
            self._stop_requested.clear()
            return

        with self._lock:
            stop_requested = bool(self._stop_requested.is_set())
            release_requested = bool(self._release_requested)
            self._activating = False
            if stop_requested:
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                self._cw_followup["status"] = "stopped"
                self._cw_followup["summary"] = "Scan stopped before CW follow-up began"
                for lane in self._lanes.values():
                    lane["status"] = "stopped"
                    lane["current_freq_mhz"] = None
                    lane["last_summary"] = "Scan stopped before probes began"
            else:
                self._running = True
                self._lanes["cw"]["status"] = "ready"
                self._lanes["cw"]["current_freq_mhz"] = None
                self._lanes["cw"]["last_summary"] = "Starting CW scan"
                self._lanes["phone"]["status"] = "ready"
                self._lanes["phone"]["current_freq_mhz"] = None
                self._lanes["phone"]["last_summary"] = "Starting Phone scan"
                self._cw_followup = self._initial_cw_followup()

        if stop_requested:
            logger.info("Receiver Scan activation completed after stop request; session will not start")
            self._stop_requested.clear()
            if release_requested:
                self._leave_mode()
            return

        logger.info("Receiver Scan activation complete; starting probes")
        self._run_session(
            host=host,
            port=port,
            password=password,
            threshold_db=threshold_db,
        )

    def _run_session(self, *, host: str, port: int, password: str | None, threshold_db: float) -> None:
        try:
            lane_errors: list[tuple[str, Exception]] = []
            lane_error_lock = threading.Lock()

            def _run_lane_safe(*, lane_key: str, rx_chan: int, freqs_mhz: list[float]) -> None:
                try:
                    self._run_lane(
                        lane_key=lane_key,
                        rx_chan=rx_chan,
                        freqs_mhz=freqs_mhz,
                        host=host,
                        port=port,
                        password=password,
                        threshold_db=threshold_db,
                    )
                except Exception as exc:
                    with lane_error_lock:
                        lane_errors.append((lane_key, exc))

            cw_thread = threading.Thread(
                name="receiver-scan-cw",
                target=lambda: _run_lane_safe(
                    lane_key="cw",
                    rx_chan=0,
                    freqs_mhz=list(self.CW_FREQS_MHZ),
                ),
                daemon=True,
            )
            phone_thread = threading.Thread(
                name="receiver-scan-phone",
                target=lambda: _run_lane_safe(
                    lane_key="phone",
                    rx_chan=1,
                    freqs_mhz=list(self.PHONE_FREQS_MHZ),
                ),
                daemon=True,
            )
            cw_thread.start()
            phone_thread.start()
            cw_thread.join()
            if not self._stop_requested.is_set():
                self._run_cw_followup(host=host, port=port, password=password)
            phone_thread.join()
            if lane_errors:
                lane_key, exc = lane_errors[0]
                raise RuntimeError(f"{lane_key} lane failed: {exc}") from exc
        except Exception as exc:
            with self._lock:
                self._last_error = f"Receiver Scan failed: {exc}"
        finally:
            release_requested = False
            session_id = None
            with self._lock:
                for lane in self._lanes.values():
                    if lane["status"] not in {"error", "stopped"}:
                        lane["status"] = "complete"
                        lane["current_freq_mhz"] = None
                if self._stop_requested.is_set():
                    for lane in self._lanes.values():
                        if lane["status"] != "error":
                            lane["status"] = "stopped"
                    if self._cw_followup["status"] not in {"complete", "error", "skipped", "stopped"}:
                        self._cw_followup["status"] = "stopped"
                        self._cw_followup["summary"] = "Scan stopped during CW follow-up"
                self._running = False
                self._thread = None
                self._last_finished_ts = time.time()
                release_requested = bool(self._release_requested)
                session_id = self._session_id
                self._release_requested = False
            self._write_session_summary(session_id)
            self._stop_requested.clear()
            if release_requested:
                self._leave_mode()

    def _run_lane(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freqs_mhz: list[float],
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> None:
        total = len(freqs_mhz)
        for index, freq_mhz in enumerate(freqs_mhz, start=1):
            if self._stop_requested.is_set():
                break
            with self._lock:
                lane = self._lanes[lane_key]
                lane["status"] = "scanning"
                lane["current_freq_mhz"] = float(freq_mhz)
                lane["completed"] = int(index - 1)
            try:
                result = self._scan_frequency(
                    lane_key=lane_key,
                    rx_chan=rx_chan,
                    freq_mhz=float(freq_mhz),
                    probe_index=index,
                    probe_total=total,
                    host=host,
                    port=port,
                    password=password,
                    threshold_db=threshold_db,
                )
            except Exception as exc:
                result = {
                    "lane": lane_key,
                    "rx_chan": int(rx_chan),
                    "freq_mhz": float(freq_mhz),
                    "status": "error",
                    "score": 0,
                    "summary": f"Probe failed: {exc}",
                    "signal_count": 0,
                    "event_count": 0,
                    "max_rel_db": None,
                    "best_s_est": None,
                    "voice_score": None,
                    "occupied_bw_hz": None,
                    "probe_index": int(index),
                    "probe_total": int(total),
                }
                with self._lock:
                    self._last_error = str(result["summary"])
            with self._lock:
                self._results[lane_key].append(dict(result))
                lane = self._lanes[lane_key]
                lane["completed"] = int(index)
                lane["last_score"] = result.get("score")
                lane["last_summary"] = result.get("summary")
                lane["current_freq_mhz"] = None
                lane["status"] = "stopped" if self._stop_requested.is_set() else "ready"
        with self._lock:
            lane = self._lanes[lane_key]
            if self._stop_requested.is_set():
                lane["status"] = "stopped"
            elif lane_key == "cw":
                lane["status"] = "followup"
                lane["last_summary"] = "Selecting best CW anchor for follow-up"
            elif lane["status"] != "error":
                lane["status"] = "complete"

    def _run_cw_followup(self, *, host: str, port: int, password: str | None) -> None:
        best = self._select_best_cw_result()
        if best is None:
            with self._lock:
                self._cw_followup["status"] = "skipped"
                self._cw_followup["summary"] = "No CW hits found; follow-up skipped"
                self._lanes["cw"]["status"] = "complete"
                self._lanes["cw"]["current_freq_mhz"] = None
                self._lanes["cw"]["last_summary"] = "No CW hits found for follow-up"
            return

        if self._stop_requested.is_set():
            with self._lock:
                self._cw_followup["status"] = "stopped"
                self._cw_followup["summary"] = "Stop requested before CW follow-up started"
            return

        freq_mhz = float(best["freq_mhz"])
        rx_chan = int(self.RESERVED_RECEIVERS[0])
        session_id = self._session_id or time.strftime("receiver_scan_%Y%m%d_%H%M%S")
        followup_dir = self._output_root / session_id / "cw_followup"
        followup_dir.mkdir(parents=True, exist_ok=True)
        initial_summary = f"Recording {int(self.CW_FOLLOWUP_SECONDS)}s CW follow-up on {freq_mhz:.3f} MHz"

        with self._lock:
            self._cw_followup = {
                "status": "recording",
                "rx_chan": rx_chan,
                "duration_s": int(self.CW_FOLLOWUP_SECONDS),
                "selected_freq_mhz": freq_mhz,
                "signal_count": int(best.get("signal_count") or 0),
                "score": best.get("score"),
                "recording_path": str(followup_dir),
                "wav_path": None,
                "decoded_text": "",
                "validated_text": "",
                "message_valid": False,
                "validation_reason": "",
                "validation_summary": "Recording CW follow-up",
                "confidence": 0.0,
                "tone_hz": None,
                "dot_ms": None,
                "wpm_est": None,
                "summary": initial_summary,
            }
            self._lanes["cw"]["status"] = "followup"
            self._lanes["cw"]["current_freq_mhz"] = freq_mhz
            self._lanes["cw"]["last_summary"] = initial_summary

        wav_path: Path | None = None
        try:
            self._clear_reserved_slot(host=host, port=int(port), rx_chan=rx_chan)
            run_record(
                RecordRequest(
                    host=host,
                    port=int(port),
                    password=password,
                    user="Receiver Scan CW Follow-up",
                    freq_hz=freq_mhz * 1e6,
                    rx_chan=rx_chan,
                    duration_s=int(self.CW_FOLLOWUP_SECONDS),
                    mode="cw",
                    out_dir=followup_dir,
                )
            )
            wav_path = self._latest_wav_path(followup_dir)
            if wav_path is None:
                raise FileNotFoundError("CW follow-up recording completed but no WAV file was found")
            if self._stop_requested.is_set():
                with self._lock:
                    self._cw_followup["status"] = "stopped"
                    self._cw_followup["wav_path"] = str(wav_path)
                    self._cw_followup["summary"] = "Stop requested after CW recording finished"
                return

            with self._lock:
                self._cw_followup["status"] = "decoding"
                self._cw_followup["wav_path"] = str(wav_path)
                self._cw_followup["summary"] = f"Decoding CW follow-up from {freq_mhz:.3f} MHz"
                self._lanes["cw"]["last_summary"] = self._cw_followup["summary"]

            decode = try_decode_cw_wav(wav_path)
            decoded_text = str(decode.get("decoded_text") or "").strip()
            validation = validate_cw_message(decoded_text, confidence=float(decode.get("confidence") or 0.0))
            validated_text = str(validation.get("normalized_text") or "").strip()
            message_valid = bool(validation.get("valid"))
            validation_reason = str(validation.get("reason") or "").strip()
            validation_summary = str(validation.get("summary") or "CW decode did not validate").strip()
            summary = validation_summary if validated_text else str(decode.get("summary") or "CW follow-up complete")

            with self._lock:
                self._cw_followup["status"] = "complete"
                self._cw_followup["wav_path"] = str(wav_path)
                self._cw_followup["decoded_text"] = decoded_text
                self._cw_followup["validated_text"] = validated_text
                self._cw_followup["message_valid"] = message_valid
                self._cw_followup["validation_reason"] = validation_reason
                self._cw_followup["validation_summary"] = validation_summary
                self._cw_followup["confidence"] = float(decode.get("confidence") or 0.0)
                self._cw_followup["tone_hz"] = decode.get("tone_hz")
                self._cw_followup["dot_ms"] = decode.get("dot_ms")
                self._cw_followup["wpm_est"] = decode.get("wpm_est")
                self._cw_followup["summary"] = summary
                self._lanes["cw"]["status"] = "complete"
                self._lanes["cw"]["current_freq_mhz"] = None
                self._lanes["cw"]["last_summary"] = validation_summary
        except RecorderUnavailable as exc:
            summary = f"CW follow-up recording unavailable: {exc}"
            with self._lock:
                self._last_error = summary
                self._cw_followup["status"] = "error"
                self._cw_followup["wav_path"] = str(wav_path) if wav_path is not None else None
                self._cw_followup["summary"] = summary
                self._lanes["cw"]["status"] = "complete"
                self._lanes["cw"]["current_freq_mhz"] = None
                self._lanes["cw"]["last_summary"] = summary
        except Exception as exc:
            summary = f"CW follow-up failed: {type(exc).__name__}: {exc}"
            with self._lock:
                self._last_error = summary
                self._cw_followup["status"] = "error"
                self._cw_followup["wav_path"] = str(wav_path) if wav_path is not None else None
                self._cw_followup["summary"] = summary
                self._lanes["cw"]["status"] = "complete"
                self._lanes["cw"]["current_freq_mhz"] = None
                self._lanes["cw"]["last_summary"] = summary

    def _select_best_cw_result(self) -> dict[str, Any] | None:
        with self._lock:
            results = [dict(item) for item in self._results.get("cw", [])]
        ranked = [
            item
            for item in results
            if str(item.get("status") or "") not in {"error", "unavailable"}
        ]
        ranked = [item for item in ranked if int(item.get("signal_count") or 0) > 0]
        if not ranked:
            return None
        return max(
            ranked,
            key=lambda item: (
                int(item.get("signal_count") or 0),
                int(item.get("score") or 0),
                float(item.get("max_rel_db") or float("-inf")),
                -float(item.get("freq_mhz") or 0.0),
            ),
        )

    @staticmethod
    def _latest_wav_path(root: Path) -> Path | None:
        wavs = [path for path in root.glob("*.wav") if path.is_file()]
        if not wavs:
            return None
        return max(wavs, key=lambda path: path.stat().st_mtime)

    def _scan_frequency(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freq_mhz: float,
        probe_index: int,
        probe_total: int,
        host: str,
        port: int,
        password: str | None,
        threshold_db: float,
    ) -> dict[str, Any]:
        session_id = self._session_id or time.strftime("receiver_scan_%Y%m%d_%H%M%S")
        lane_dir = self._output_root / session_id / lane_key
        lane_dir.mkdir(parents=True, exist_ok=True)
        freq_tag = f"{freq_mhz:.3f}".replace(".", "_")
        report_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}.json"
        hits_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}_hits.jsonl"
        events_path = lane_dir / f"probe_{probe_index:02d}_{freq_tag}_events.jsonl"

        is_phone = lane_key == "phone"
        self._clear_reserved_slot(host=host, port=int(port), rx_chan=int(rx_chan))
        rc = run_scan(
            host=host,
            port=int(port),
            password=password,
            user=f"Receiver Scan {lane_key.upper()}",
            rx_chan=int(rx_chan),
            band=self.BAND,
            center_freq_hz=float(freq_mhz) * 1e6,
            span_hz=float(self.PHONE_SPAN_HZ) if is_phone else 2400.0,
            threshold_db=float(threshold_db),
            min_width_bins=2,
            min_width_hz=float(self.PHONE_MIN_WIDTH_HZ) if is_phone else 20.0,
            ssb_detect=bool(is_phone),
            ssb_only=bool(is_phone),
            required_hits=1 if is_phone else 2,
            tolerance_bins=2.5,
            expiry_frames=6,
            max_frames=int(self.PHONE_MAX_FRAMES) if is_phone else 10,
            jsonl_path=hits_path,
            jsonl_events_path=events_path,
            json_report_path=report_path,
            min_s=1.0,
            status_hold_s=float(self.LISTEN_SECONDS),
            max_runtime_s=4.0,
            rx_wait_timeout_s=20.0,
            rx_wait_interval_s=1.0,
            rx_wait_max_retries=0,
            phone_only=bool(is_phone),
            status_modulation="iq",
            status_pre_tune=False,
            status_parallel_snd=True,
            ssb_occ_thresh_db=5.0,
            ssb_voice_min_score=float(self.PHONE_VOICE_MIN_SCORE) if is_phone else 0.45,
            ssb_early_stop_frames=int(self.PHONE_EARLY_STOP_FRAMES) if is_phone else 0,
            ssb_warmup_frames=2 if is_phone else 1,
            ssb_adaptive_threshold=bool(is_phone),
            ssb_adaptive_min_db=8.0,
            ssb_adaptive_max_db=22.0,
            ssb_adaptive_spread_gain=0.18,
            ssb_adaptive_spread_offset_db=0.0,
            ssb_adaptive_spread_target_db=55.0,
            show=False,
        )
        return self._summarize_probe(
            lane_key=lane_key,
            rx_chan=rx_chan,
            freq_mhz=freq_mhz,
            probe_index=probe_index,
            probe_total=probe_total,
            rc=rc,
            report_path=report_path,
            events_path=events_path,
        )

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        entries: list[dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                entries.append(item)
        return entries

    @staticmethod
    def _clamp_score(value: float) -> int:
        return int(max(0.0, min(100.0, round(float(value)))))

    def _summarize_probe(
        self,
        *,
        lane_key: str,
        rx_chan: int,
        freq_mhz: float,
        probe_index: int,
        probe_total: int,
        rc: int,
        report_path: Path,
        events_path: Path,
    ) -> dict[str, Any]:
        report = self._read_json(report_path)
        events = self._read_jsonl(events_path)
        peak = report.get("peak") if isinstance(report.get("peak"), dict) else {}
        if rc == 3:
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": "unavailable",
                "score": 0,
                "summary": f"RX{int(rx_chan)} unavailable for this probe",
                "signal_count": 0,
                "event_count": len(events),
                "max_rel_db": None,
                "best_s_est": None,
                "voice_score": None,
                "occupied_bw_hz": None,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }
        if rc != 0:
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": "error",
                "score": 0,
                "summary": f"Probe failed with rc={int(rc)}",
                "signal_count": 0,
                "event_count": len(events),
                "max_rel_db": None,
                "best_s_est": None,
                "voice_score": None,
                "occupied_bw_hz": None,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }

        rel_values = [float(item.get("rel_db")) for item in events if item.get("rel_db") is not None]
        s_values = [float(item.get("s_est")) for item in events if item.get("s_est") is not None]
        max_rel_db = max(rel_values) if rel_values else (
            float(peak.get("rel_db")) if isinstance(peak, dict) and peak.get("rel_db") is not None else None
        )
        best_s_est = max(s_values) if s_values else (
            float(peak.get("s_est")) if isinstance(peak, dict) and peak.get("s_est") is not None else None
        )

        if lane_key == "cw":
            distinct_hits: dict[float, float] = {}
            for item in events:
                try:
                    key = round(float(item.get("freq_mhz", freq_mhz)), 4)
                    rel_db = float(item.get("rel_db", 0.0))
                except Exception:
                    continue
                distinct_hits[key] = max(rel_db, distinct_hits.get(key, rel_db))
            signal_count = len(distinct_hits)
            score = self._clamp_score((float(max_rel_db or 0.0) * 4.0) + (signal_count * 18.0))
            status = "activity" if score >= 60 else "watch" if score >= 30 else "quiet"
            summary = (
                f"{signal_count} persistent narrow signal{'s' if signal_count != 1 else ''}"
                if signal_count
                else "No persistent CW-like tones"
            )
            return {
                "lane": lane_key,
                "rx_chan": int(rx_chan),
                "freq_mhz": float(freq_mhz),
                "status": status,
                "score": score,
                "summary": summary,
                "signal_count": signal_count,
                "event_count": len(events),
                "max_rel_db": max_rel_db,
                "best_s_est": best_s_est,
                "voice_score": None,
                "occupied_bw_hz": None,
                "probe_index": int(probe_index),
                "probe_total": int(probe_total),
            }

        voice_values = [float(item.get("voice_score")) for item in events if item.get("voice_score") is not None]
        occ_values = [float(item.get("occ_bw_hz")) for item in events if item.get("occ_bw_hz") is not None]
        voice_score = max(voice_values) if voice_values else (
            float(peak.get("voice_score")) if isinstance(peak, dict) and peak.get("voice_score") is not None else None
        )
        occupied_bw_hz = max(occ_values) if occ_values else (
            float(peak.get("occ_bw_hz")) if isinstance(peak, dict) and peak.get("occ_bw_hz") is not None else None
        )
        score = self._clamp_score(
            (float(max_rel_db or 0.0) * 4.0)
            + (float(voice_score or 0.0) * 45.0)
            + (min(float(occupied_bw_hz or 0.0), 2800.0) / 90.0)
        )
        status = "activity" if score >= int(self.PHONE_ACTIVITY_MIN_SCORE) else "watch" if score >= 30 else "quiet"
        summary = (
            f"Speech-like energy voice={float(voice_score or 0.0):.2f}, bw={float(occupied_bw_hz or 0.0):.0f} Hz"
            if score >= 30
            else "No speech-like energy"
        )
        return {
            "lane": lane_key,
            "rx_chan": int(rx_chan),
            "freq_mhz": float(freq_mhz),
            "status": status,
            "score": score,
            "summary": summary,
            "signal_count": 1 if status == "activity" else 0,
            "event_count": len(events),
            "max_rel_db": max_rel_db,
            "best_s_est": best_s_est,
            "voice_score": voice_score,
            "occupied_bw_hz": occupied_bw_hz,
            "probe_index": int(probe_index),
            "probe_total": int(probe_total),
        }

    def _write_session_summary(self, session_id: str | None) -> None:
        if not session_id:
            return
        session_dir = self._output_root / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        summary_path = session_dir / "receiver_scan_session.json"
        summary_path.write_text(json.dumps(self.status(), sort_keys=True) + "\n", encoding="utf-8")