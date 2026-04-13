from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence


def _wait_event_cleared(event: object, *, timeout_s: float) -> None:
    """Wait until a threading.Event transitions to cleared.

    Vendored jks-prv/kiwiclient uses `_camp_wait_event` with semantics:
    - event is SET while waiting for camp to complete
    - event is CLEARED when camp is acknowledged (ready)
    """

    try:
        end = time.time() + float(timeout_s)
        while getattr(event, "is_set")() and time.time() < end:
            time.sleep(0.05)
    except Exception:
        return


@dataclass(frozen=True)
class WaterfallFrame:
    frame_index: int
    center_freq_hz: float
    span_hz: float
    power_bins: Sequence[float]


class KiwiClientUnavailable(RuntimeError):
    pass


class KiwiCampRejected(RuntimeError):
    def __init__(self, *, requested_rx: int, response: str | None = None):
        msg = f"Kiwi RX unavailable for requested rx={requested_rx}"
        if response:
            msg = f"{msg} (detail={response})"
        super().__init__(msg)
        self.requested_rx = int(requested_rx)
        self.response = response


class _KiwiAssignedRxMismatch(RuntimeError):
    def __init__(self, *, requested_rx: int, assigned_rx: int):
        super().__init__(f"Kiwi assigned rx={assigned_rx} but requested rx={requested_rx}")
        self.requested_rx = int(requested_rx)
        self.assigned_rx = int(assigned_rx)


_WS_TS_COUNTER = 0


def _unique_ws_timestamp() -> int:
    global _WS_TS_COUNTER
    try:
        _WS_TS_COUNTER = int(_WS_TS_COUNTER) + 1
        return int(time.time() + os.getpid() + _WS_TS_COUNTER) & 0xFFFFFFFF
    except Exception:
        try:
            _WS_TS_COUNTER = int(_WS_TS_COUNTER) + 1
        except Exception:
            _WS_TS_COUNTER = 1
        return int(time.time() + _WS_TS_COUNTER) & 0xFFFFFFFF


def allocate_ws_timestamp() -> int:
    return _unique_ws_timestamp()


def _default_preview_passband(modulation: str) -> tuple[int, int]:
    mod = str(modulation or "usb").strip().lower()
    if mod in {"iq", "drm", "sas", "qam"}:
        return (-6000, 6000)
    if mod in {"am", "amn", "amw"}:
        return (-5000, 5000)
    if mod in {"lsb", "lsn"}:
        return (-2700, -300)
    if mod in {"cw", "cwn"}:
        return (300, 700)
    return (300, 2700)


def _build_set_mod_cmd(*, s: object, freq_khz: float) -> str:
    opt = getattr(s, "_options", None)
    mod = str(getattr(opt, "modulation", "usb")).lower()
    lp_cut = getattr(opt, "lp_cut", 300)
    hp_cut = getattr(opt, "hp_cut", 2700)
    freq_pbc = bool(getattr(opt, "freq_pbc", False))

    lc = lp_cut
    hc = hp_cut
    if mod in {"iq", "drm", "sas", "qam"}:
        try:
            lc_i = int(lc) if lc is not None else None
            hc_i = int(hc) if hc is not None else None
        except Exception:
            lc_i = None
            hc_i = None
        if lc_i is None or hc_i is None or lc_i >= 0 or hc_i <= 0:
            lc, hc = _default_preview_passband(mod)
    if mod in {"am", "amn", "amw"}:
        hc = int(hp_cut) if hp_cut is not None else hp_cut
        lc = -hc if hc is not None else hc

    baseband_freq = float(freq_khz)
    try:
        if hasattr(s, "_remove_freq_offset"):
            baseband_freq = float(s._remove_freq_offset(float(freq_khz)))  # type: ignore[attr-defined]
    except Exception:
        baseband_freq = float(freq_khz)

    try:
        if freq_pbc and mod in {"lsb", "lsn", "usb", "usn", "cw", "cwn"} and lc is not None and hc is not None:
            pbc = (float(lc) + (float(hc) - float(lc)) / 2.0) / 1000.0
            baseband_freq = float(baseband_freq) - float(pbc)
    except Exception:
        pass

    return f"SET mod={mod} low_cut={int(lc)} high_cut={int(hc)} freq={baseband_freq:.3f}"


def _import_kiwiclient() -> object:
    here = os.path.dirname(__file__)
    vendor_root = os.path.abspath(os.path.join(here, "..", "..", "vendor", "kiwiclient-jks"))
    if os.path.isdir(vendor_root) and vendor_root not in sys.path:
        sys.path.insert(0, vendor_root)

    spec = importlib.util.find_spec("kiwi")
    if spec is None:
        raise KiwiClientUnavailable(
            "Missing KiwiSDR client library. Expected vendored `kiwi` package at "
            "kiwi_scan/vendor/kiwiclient-jks or an installed package providing `kiwi`."
        )
    return importlib.import_module("kiwi")


def set_receiver_frequency(
    host: str,
    port: int = 8073,
    rx_chan: int = 0,
    freq_hz: float = 1840000,
    password: Optional[str] = None,
    user: str = "kiwi-scan",
    timeout_s: float = 10.0,
    hold_s: float = 0.0,
    rx_wait_timeout_s: float = 0.0,
    rx_wait_interval_s: float = 2.0,
    rx_wait_max_retries: int = 0,
    modulation: str = "usb",
    ws_timestamp: int | None = None,
    hold_event: object | None = None,
    ready_event: object | None = None,
) -> bool:
    kiwi = _import_kiwiclient()
    KiwiSDRStream = getattr(kiwi, "KiwiSDRStream", None)
    if KiwiSDRStream is None:
        raise KiwiClientUnavailable("Could not import kiwi.KiwiSDRStream")

    start = time.time()
    retries = 0
    while True:
        try:
            return _set_receiver_frequency_once(
                KiwiSDRStream=KiwiSDRStream,
                host=host,
                port=int(port),
                rx_chan=int(rx_chan),
                freq_hz=float(freq_hz),
                password=password,
                user=user,
                timeout_s=float(timeout_s),
                hold_s=float(hold_s),
                modulation=str(modulation),
                ws_timestamp=ws_timestamp,
                hold_event=hold_event,
                ready_event=ready_event,
            )
        except _KiwiAssignedRxMismatch as e:
            # Server assigned a different RX than requested; treat as "busy".
            raise KiwiCampRejected(requested_rx=int(rx_chan), response=str(e))
        except KiwiCampRejected as e:
            retries += 1
            elapsed = time.time() - start
            if int(rx_wait_max_retries) > 0 and retries > int(rx_wait_max_retries):
                return False
            if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                return False
            time.sleep(max(0.25, float(rx_wait_interval_s)))
        except Exception as e:
            # Some Kiwi/client versions raise custom exceptions on early
            # disconnects (e.g. server closed the connection unexpectedly).
            # Treat these as transient and retry a few times.
            name = type(e).__name__
            msg = str(e)
            is_transient = name in {
                "KiwiServerTerminatedConnection",
                "ConnectionResetError",
                "BrokenPipeError",
                "TimeoutError",
            } or "server closed the connection" in msg.lower() or "connection reset" in msg.lower()

            if not is_transient:
                raise

            retries += 1
            elapsed = time.time() - start
            max_retry = int(rx_wait_max_retries) if int(rx_wait_max_retries) > 0 else 3
            if retries > max_retry:
                return False
            if float(rx_wait_timeout_s) > 0 and elapsed >= float(rx_wait_timeout_s):
                return False
            time.sleep(min(3.0, max(0.5, float(rx_wait_interval_s))))


def _set_receiver_frequency_once(
    *,
    KiwiSDRStream: object,
    host: str,
    port: int,
    rx_chan: int,
    freq_hz: float,
    password: Optional[str],
    user: str,
    timeout_s: float,
    hold_s: float,
    modulation: str,
    ws_timestamp: int | None = None,
    hold_event: object | None = None,
    ready_event: object | None = None,
) -> bool:
    freq_khz = float(freq_hz) / 1000.0
    preview_mod = str(modulation or "usb").strip().lower()
    lp_cut, hp_cut = _default_preview_passband(preview_mod)

    class _SND(KiwiSDRStream):  # type: ignore[misc]
        def _setup_rx_params(self) -> None:  # type: ignore[override]
            try:
                if hasattr(self, "set_name"):
                    self.set_name(user)  # type: ignore[attr-defined]
                else:
                    self._send_message(f"SET ident_user={user}")  # type: ignore[attr-defined]
            except Exception:
                pass

            try:
                self._send_message(_build_set_mod_cmd(s=self, freq_khz=float(freq_khz)))  # type: ignore[attr-defined]
            except Exception:
                pass

            try:
                if hasattr(self, "set_agc") and preview_mod not in {"iq", "drm", "sas", "qam"}:
                    self.set_agc(True)  # type: ignore[attr-defined]
            except Exception:
                pass

    s = _SND()  # type: ignore[no-untyped-call]
    s._type = "SND"  # type: ignore[attr-defined]
    s._freq = float(freq_khz)  # type: ignore[attr-defined]
    try:
        s._camp_chan = -1  # type: ignore[attr-defined]
    except Exception:
        pass
    # Do NOT use Kiwi camp mode. Camp mode has been observed to immediately
    # disconnect on some Kiwis, resulting in zero waterfall frames.
    # Instead, connect normally and verify the server-assigned rx_chan is the
    # requested receiver before allowing setup/tuning to proceed.

    try:
        import threading as _threading

        s._camp_wait_event = _threading.Event()  # type: ignore[attr-defined]
        s._camp_wait_event.set()  # type: ignore[attr-defined]
    except Exception:
        pass

    class _Opt:
        pass

    opt = _Opt()
    opt.server_host = host
    opt.server_port = int(port)
    opt.password = password or ""
    opt.tlimit_password = ""
    opt.user = user
    opt.wideband = False
    opt.ws_timestamp = int(ws_timestamp) if ws_timestamp is not None else _unique_ws_timestamp()
    opt.socket_timeout = float(timeout_s)
    opt.admin = False
    opt.nolocal = False
    opt.bad_cmd = False
    opt.stats = False
    opt.S_meter = -1
    opt.sdt = 0
    opt.tstamp = False
    opt.zoom = 0
    opt.netcat = False
    opt.idx = int(rx_chan)
    opt.rx_chan = int(rx_chan)
    opt.wf_cal = None
    opt.tlimit = None
    opt.rev_bin = False
    opt.modulation = preview_mod
    opt.lp_cut = int(lp_cut)
    opt.hp_cut = int(hp_cut)
    opt.freq_pbc = False
    opt.no_api = False
    opt.agc_gain = None
    opt.agc_yaml_file = None
    opt.compression = None
    opt.nb = False
    opt.nb_test = False
    opt.nb_gate = 100
    opt.nb_thresh = 50
    opt.de_emp = False
    opt.test_mode = False
    opt.resample = 0
    opt.devel = None
    s._options = opt  # type: ignore[attr-defined]

    assigned_rx: list[int | None] = [None]
    tuned: list[bool] = [False]

    try:
        orig_process = s._process_msg_param  # type: ignore[attr-defined]

        def _wrapped_process(name, value):
            # Capture the server-assigned receiver channel.
            try:
                if name == "rx_chan" and value is not None:
                    assigned_rx[0] = int(value)
            except Exception:
                pass

            # Before the client performs its SND setup (triggered by sample_rate),
            # ensure we are on the requested receiver.
            if name == "sample_rate" and assigned_rx[0] is not None and int(assigned_rx[0]) != int(rx_chan):
                raise _KiwiAssignedRxMismatch(requested_rx=int(rx_chan), assigned_rx=int(assigned_rx[0]))

            return orig_process(name, value)

        s._process_msg_param = _wrapped_process  # type: ignore[attr-defined]
    except Exception:
        pass

    try:
        s.connect(host, int(port))  # type: ignore[no-untyped-call]
        try:
            s.open()  # type: ignore[no-untyped-call]
        except Exception:
            pass

        start = time.time()
        while (time.time() - start) < float(timeout_s):
            try:
                s.run()  # type: ignore[no-untyped-call]
            except Exception as e:
                if isinstance(e, _KiwiAssignedRxMismatch):
                    raise KiwiCampRejected(
                        requested_rx=int(rx_chan), response=f"assigned_rx={int(e.assigned_rx)}"
                    )
                break
            # SND setup will call set_freq() and begin streaming. We treat this
            # as "tuned" once we have a sample_rate (i.e. setup completed).
            if s._sample_rate is not None and (assigned_rx[0] is None or int(assigned_rx[0]) == int(rx_chan)):  # type: ignore[attr-defined]
                tuned[0] = True
                break

        # Keep the connection alive briefly so it becomes visible on the
        # KiwiSDR status page (which refreshes at a coarse interval).
        if tuned[0]:
            try:
                if ready_event is not None and hasattr(ready_event, "set"):
                    ready_event.set()
            except Exception:
                pass

        if tuned[0] and (float(hold_s) > 0 or hold_event is not None):
            end = (time.time() + float(hold_s)) if float(hold_s) > 0 else None
            while True:
                if end is not None and time.time() >= end:
                    break
                try:
                    if hold_event is not None and getattr(hold_event, "is_set")():
                        break
                except Exception:
                    pass
                try:
                    s.run()  # type: ignore[no-untyped-call]
                except Exception:
                    break
    finally:
        try:
            s.close()  # type: ignore[no-untyped-call]
        except Exception:
            pass

    # If we never managed to tune within timeout, treat as "RX unavailable".
    if not tuned[0]:
        raise KiwiCampRejected(requested_rx=int(rx_chan), response="timeout")

    return bool(tuned[0])


def subscribe_waterfall(
    *,
    host: str,
    port: int = 8073,
    password: Optional[str] = None,
    user: str = "kiwi-scan",
    rx_chan: Optional[int] = None,
    center_freq_hz: float,
    span_hz: float,
    on_frame: Callable[[WaterfallFrame], None],
    should_stop: Optional[Callable[[], bool]] = None,
    on_camp: Optional[Callable[[bool, int], None]] = None,
    camp_timeout_s: Optional[float] = 10.0,
    max_frames: Optional[int] = None,
    min_duration_s: Optional[float] = None,
    max_duration_s: Optional[float] = None,
    debug: bool = False,
    debug_messages: bool = False,
    status_modulation: str = "usb",
    ws_timestamp: int | None = None,
) -> None:
    """Subscribe to a KiwiSDR waterfall and call `on_frame` per frame.

    This is intentionally thin and may need adapting to the exact kiwiclient API
    you have installed (there are multiple forks/versions).
    """

    kiwi = _import_kiwiclient()

    if debug:
        logging.basicConfig(level=logging.INFO)
    if debug_messages:
        # When websocket message level debugging is requested, enable DEBUG
        # level so the client 'camp' and related MSG debug lines are shown.
        logging.basicConfig(level=logging.DEBUG)

    KiwiSDRStream = getattr(kiwi, "KiwiSDRStream", None)
    if KiwiSDRStream is None:
        raise KiwiClientUnavailable("Could not import kiwi.KiwiSDRStream")

    frame_counter = 0
    start_time = time.time()
    assigned_rx: list[int | None] = [None]
    required_rx = rx_chan

    class _WF(KiwiSDRStream):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()  # type: ignore[no-untyped-call]

        def _setup_rx_params(self) -> None:  # type: ignore[override]
            # Configure waterfall similar to vendor's `KiwiWaterfallRecorder`.
            cf_khz = center_freq_hz / 1000.0
            try:
                zoom = getattr(self._options, "zoom", 0)  # type: ignore[attr-defined]
            except Exception:
                zoom = 0

            def _send_direct(msg: str) -> None:
                # Fallback raw send. Prefer calling the vendored setter methods
                # because they also update internal state (e.g. _compression).
                try:
                    self._send_message(msg)  # type: ignore[attr-defined]
                    return
                except Exception:
                    pass
                try:
                    if hasattr(self, "_stream") and getattr(self, "_stream") is not None:
                        self._stream.send_message(msg)  # type: ignore[attr-defined]
                except Exception:
                    pass

            # Ensure waterfall uses *uncompressed* byte samples. If we only send
            # 'SET wf_comp=0' without also updating client._compression, the
            # vendored client will incorrectly ADPCM-decode raw bytes into large
            # +/-32767 values, which pegs scoring and calibration.
            try:
                if hasattr(self, "_set_wf_comp"):
                    self._set_wf_comp(False)  # type: ignore[attr-defined]
                else:
                    _send_direct("SET wf_comp=0")
                    try:
                        self._compression = False  # type: ignore[attr-defined]
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                if hasattr(self, "_set_maxdb_mindb"):
                    self._set_maxdb_mindb(-10, -110)  # type: ignore[attr-defined]
                else:
                    _send_direct("SET maxdb=-10 mindb=-110")
            except Exception:
                pass

            try:
                if hasattr(self, "_set_zoom_cf"):
                    # Uses correct API depending on Kiwi version.
                    self._set_zoom_cf(int(zoom), float(cf_khz))  # type: ignore[attr-defined]
                else:
                    _send_direct(f"SET zoom={int(zoom)} cf={float(cf_khz):f}")
            except Exception:
                # If version isn't known yet, assume modern API.
                try:
                    _send_direct(f"SET zoom={int(zoom)} cf={float(cf_khz):f}")
                except Exception:
                    pass

            try:
                if hasattr(self, "_set_wf_speed"):
                    self._set_wf_speed(1)  # type: ignore[attr-defined]
                else:
                    _send_direct("SET wf_speed=1")
            except Exception:
                pass

            try:
                if hasattr(self, "set_name"):
                    self.set_name(user)  # type: ignore[attr-defined]
                else:
                    _send_direct(f"SET ident_user={user}")
            except Exception:
                pass

            # Ensure RX status reflects a tuned frequency/mode even for WF streams.
            try:
                _send_direct(_build_set_mod_cmd(s=self, freq_khz=float(cf_khz)))
            except Exception:
                pass

        def _process_waterfall_samples(self, seq: int, samples: Sequence[float]) -> None:  # type: ignore[override]
            nonlocal frame_counter
            nonlocal start_time

            # Allow callers (e.g. the scanner pause mechanism) to stop an in-flight
            # waterfall stream quickly without waiting for max_duration_s.
            try:
                if should_stop is not None and bool(should_stop()):
                    try:
                        self.close()  # type: ignore[no-untyped-call]
                    except Exception:
                        pass
                    return
            except Exception:
                pass

            # Vendored kiwiclient provides waterfall values as unsigned bytes.
            # Convert to an approximate dBm scale used by upstream tools:
            #   55..255 => -200..0 dBm via (byte - 255)
            # Optionally include wf_cal if present.
            try:
                wf_cal = getattr(self._options, "wf_cal", None)  # type: ignore[attr-defined]
                wf_cal_i = int(wf_cal) if wf_cal is not None else 0
            except Exception:
                wf_cal_i = 0

            try:
                power_bins = [float(int(v) - 255 + wf_cal_i) for v in samples]
            except Exception:
                # Best-effort fallback
                power_bins = [float(v) for v in samples]

            if debug and (frame_counter % 10 == 0):
                logging.info("wf frames=%d seq=%s bins=%d", frame_counter, seq, len(power_bins))
            on_frame(
                WaterfallFrame(
                    frame_index=frame_counter,
                    center_freq_hz=float(center_freq_hz),
                    span_hz=float(span_hz),
                    power_bins=power_bins,
                )
            )
            frame_counter += 1
            # Close when:
            # - max_duration_s has elapsed (hard stop), OR
            # - max_frames reached AND (optional) min_duration_s has elapsed.
            # This keeps very short runs visible in the Kiwi status page.
            elapsed = time.time() - start_time
            frames_done = (max_frames is not None and frame_counter >= max_frames)
            min_time_done = (min_duration_s is None or elapsed >= float(min_duration_s))
            time_done = (max_duration_s is not None and elapsed >= float(max_duration_s))
            if time_done or (frames_done and min_time_done):
                try:
                    self.close()  # type: ignore[no-untyped-call]
                except Exception:
                    pass

        def _process_ws_message(self, message: object) -> None:  # type: ignore[override]
            if debug_messages:
                try:
                    if isinstance(message, (bytes, bytearray)):
                        preview = bytes(message[:200])
                        logging.info("ws recv bytes len=%d head=%r", len(message), preview)
                    else:
                        s = str(message)
                        logging.info("ws recv type=%s head=%r", type(message).__name__, s[:200])
                except Exception:
                    pass
            return super()._process_ws_message(message)  # type: ignore[misc]

    s = _WF()
    # Some client handlers expect a _camp_wait_event attribute to exist
    # (the worker-based API wires one in). Provide one, matching upstream
    # semantics: SET means "waiting for camp", CLEARED means "camp ready".
    try:
        import threading as _threading
        s._camp_wait_event = _threading.Event()
        s._camp_wait_event.set()
    except Exception:
        pass
    # jks-prv-style client uses an options object; create a tiny instance.
    class _Opt:
        pass

    opt = _Opt()
    opt.server_host = host
    opt.server_port = port
    opt.password = password or ""
    opt.tlimit_password = ""
    opt.user = user

    opt.wideband = False
    opt.ws_timestamp = int(ws_timestamp) if ws_timestamp is not None else _unique_ws_timestamp()
    opt.socket_timeout = 10
    opt.admin = False
    opt.nolocal = False
    opt.bad_cmd = False
    opt.stats = False
    opt.S_meter = -1
    opt.sdt = 0
    opt.tstamp = False
    opt.zoom = 0
    opt.netcat = False
    # If required_rx is None, allow the Kiwi to auto-assign a free receiver by
    # omitting rx_chan from the websocket URL query (client only includes it
    # when rx_chan is not None and >= 0).
    opt.idx = int(required_rx) if required_rx is not None else 0
    opt.rx_chan = int(required_rx) if required_rx is not None else -1
    opt.modulation = str(status_modulation or "usb").strip().lower()
    opt.lp_cut, opt.hp_cut = _default_preview_passband(opt.modulation)
    opt.freq_pbc = False
    opt.wf_cal = None
    opt.tlimit = None
    opt.rev_bin = False

    s._options = opt  # type: ignore[attr-defined]
    s._type = 'W/F'  # type: ignore[attr-defined]
    s._freq = center_freq_hz / 1000.0  # type: ignore[attr-defined]

    # Do NOT set s._camp_chan here. Camp mode has proven unreliable (immediate
    # camp_disconnect) and also suppresses setup commands in the vendored client.

    # Wrap message parsing so we can enforce a specific receiver assignment
    # (RX0-only policy) without relying on Kiwi camp mode.
    rx_error = False
    rx_response: str | None = None
    try:
        orig_process = s._process_msg_param

        def _wrapped_process(name, value):
            nonlocal rx_error, rx_response
            # Capture server-assigned rx_chan early in the connect sequence.
            try:
                if name == "rx_chan" and value is not None:
                    assigned_rx[0] = int(value)
                    try:
                        if on_camp is not None:
                            on_camp(True, int(assigned_rx[0]))
                    except Exception:
                        pass
            except Exception:
                pass

            # Before the client starts waterfall setup (triggered by wf_setup),
            # ensure we're assigned the requested receiver.
            if name == "wf_setup" and required_rx is not None and assigned_rx[0] is not None:
                if int(assigned_rx[0]) != int(required_rx):
                    rx_error = True
                    rx_response = f"assigned_rx={int(assigned_rx[0])}"
                    raise _KiwiAssignedRxMismatch(requested_rx=int(required_rx), assigned_rx=int(assigned_rx[0]))

            return orig_process(name, value)

        s._process_msg_param = _wrapped_process  # type: ignore[attr-defined]
    except Exception:
        pass

    s.connect(host, port)  # type: ignore[no-untyped-call]
    s.open()  # type: ignore[no-untyped-call]
    while True:
        # Allow callers to abort quickly (e.g. pause scanning) even if no
        # waterfall samples have arrived yet.
        try:
            if should_stop is not None and bool(should_stop()):
                try:
                    s.close()  # type: ignore[no-untyped-call]
                except Exception:
                    pass
                return
        except Exception:
            pass
        try:
            s.run()  # type: ignore[no-untyped-call]
        except Exception as e:
            if rx_error and required_rx is not None:
                raise KiwiCampRejected(requested_rx=int(required_rx), response=rx_response)
            # Normal shutdown path: we close after max_frames/max_duration_s,
            # but the vendored client may still try to send keepalives.
            if type(e).__name__ == "BadOperationException" or "closing handshake" in str(e).lower():
                if debug:
                    logging.info("waterfall loop exit (closing handshake): %s", e)
                return
            # We don't use camp mode; treat any early disconnects as a normal
            # error and let callers decide whether to retry.
            # Kiwi client uses exceptions for normal termination conditions.
            if debug:
                logging.info("waterfall loop exit: %s: %s", type(e).__name__, e)
            # Propagate the exception so callers can detect camp disconnects
            # and decide whether to retry or handle the error.
            raise

        # If a specific RX was requested, bound how long we'll wait for the
        # server to assign that receiver.
        if required_rx is not None and camp_timeout_s is not None:
            try:
                if assigned_rx[0] is None:
                    # assignment message not seen yet
                    if (time.time() - start_time) >= float(camp_timeout_s):
                        try:
                            s.close()  # type: ignore[no-untyped-call]
                        except Exception:
                            pass
                        raise KiwiCampRejected(requested_rx=int(required_rx), response="rx assignment timeout")
            except KiwiCampRejected:
                raise
            except Exception:
                pass
