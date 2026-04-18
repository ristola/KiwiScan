from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import kiwi_scan.receiver_manager as receiver_manager
from kiwi_scan.receiver_manager import ReceiverAssignment, ReceiverManager, _ReceiverWorker


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _make_manager() -> ReceiverManager:
    return ReceiverManager(
        kiwirecorder_path=Path("/bin/sh"),
        ft8modem_path=Path("/bin/sh"),
        af2udp_path=Path("/bin/sh"),
        sox_path="/bin/sh",
    )


def _make_worker(*, initial_rx_chan_adjust: int = 0) -> _ReceiverWorker:
    return _ReceiverWorker(
        kiwirecorder_path=Path("/bin/sh"),
        ft8modem_path=Path("/bin/sh"),
        af2udp_path=Path("/bin/sh"),
        sox_path="/bin/sh",
        host="kiwi.local",
        port=8073,
        rx=2,
        band="20m",
        freq_hz=14_074_000.0,
        mode_label="FT8",
        initial_rx_chan_adjust=initial_rx_chan_adjust,
    )


def _set_visible_user(monkeypatch, user_label: str) -> None:
    payload = [{"i": 2, "n": user_label, "t": "0:10:00"}]

    def fake_urlopen(req, timeout=0.0):
        return _FakeResponse(payload)

    monkeypatch.setattr(receiver_manager.urllib.request, "urlopen", fake_urlopen)


def _set_users_payload(monkeypatch, payload: list[dict[str, object]]) -> None:
    def fake_urlopen(req, timeout=0.0):
        return _FakeResponse(payload)

    monkeypatch.setattr(receiver_manager.urllib.request, "urlopen", fake_urlopen)


def test_receiver_manager_float_env_helpers_clamp_and_default(monkeypatch) -> None:
    monkeypatch.delenv("KIWISCAN_NO_DECODE_WARN_S", raising=False)
    monkeypatch.setenv("KIWISCAN_DIGITAL_REMAP_GRACE_S", "bad-value")

    assert ReceiverManager._no_decode_warning_seconds() == 120.0
    assert ReceiverManager._digital_remap_grace_seconds() == 20.0

    monkeypatch.setenv("KIWISCAN_NO_DECODE_WARN_S", "10")
    monkeypatch.setenv("KIWISCAN_DIGITAL_REMAP_GRACE_S", "999")

    assert ReceiverManager._no_decode_warning_seconds() == 30.0
    assert ReceiverManager._digital_remap_grace_seconds() == 300.0


def test_receiver_manager_bool_env_helpers_honor_false_values(monkeypatch) -> None:
    monkeypatch.setenv("KIWISCAN_RESET_ALL_ON_BAND_CHANGE", "off")
    monkeypatch.setenv("KIWISCAN_RESET_ALL_ON_RECONCILE", "0")

    assert ReceiverManager._force_full_reset_on_band_change_enabled() is False
    assert ReceiverManager._force_full_reset_on_reconcile_enabled() is False

    monkeypatch.delenv("KIWISCAN_RESET_ALL_ON_BAND_CHANGE", raising=False)
    monkeypatch.delenv("KIWISCAN_RESET_ALL_ON_RECONCILE", raising=False)

    assert ReceiverManager._force_full_reset_on_band_change_enabled() is True
    assert ReceiverManager._force_full_reset_on_reconcile_enabled() is True


def test_receiver_worker_env_helpers_respect_defaults_and_overrides(monkeypatch) -> None:
    monkeypatch.setenv("KIWISCAN_RX_CHAN_OFFSET", "7")
    monkeypatch.setenv("KIWISCAN_STRICT_DIGITAL_SLOT_ENFORCEMENT", "off")
    monkeypatch.setenv("KIWISCAN_USE_PY_UDP_AUDIO", "1")
    monkeypatch.setenv("KIWISCAN_FT8MODEM_KEEP", "yes")

    worker = _make_worker()

    assert worker._rx_chan_adjust == 7
    assert worker._strict_digital_slot_enforcement() is False
    assert worker._use_python_udp_sender() is True
    assert worker._decoder_keep_wavs_enabled() is True


def test_receiver_worker_env_helpers_clamp_and_fallback(monkeypatch) -> None:
    monkeypatch.setenv("KIWISCAN_RX_CHAN_OFFSET", "bad-value")
    monkeypatch.delenv("KIWISCAN_STRICT_DIGITAL_SLOT_ENFORCEMENT", raising=False)
    monkeypatch.delenv("KIWISCAN_USE_PY_UDP_AUDIO", raising=False)
    monkeypatch.setenv("KIWISCAN_FT8MODEM_KEEP", "0")

    worker = _make_worker(initial_rx_chan_adjust=0)
    bounded = _ReceiverWorker._env_int("KIWISCAN_RX_CHAN_OFFSET", 0, min_v=-64, max_v=64)

    assert worker._rx_chan_adjust == 0
    assert bounded == 0
    assert worker._strict_digital_slot_enforcement() is True
    assert worker._use_python_udp_sender() is False
    assert worker._decoder_keep_wavs_enabled() is False

    monkeypatch.setenv("KIWISCAN_RX_CHAN_OFFSET", "999")

    assert _ReceiverWorker._env_int("KIWISCAN_RX_CHAN_OFFSET", 0, min_v=-64, max_v=64) == 64


def test_mode_requires_digital_accepts_combo_digital_modes() -> None:
    assert ReceiverManager._mode_requires_digital("FT4 / FT8 / WSPR") is True
    assert ReceiverManager._mode_requires_digital("FT4 / WSPR") is True
    assert ReceiverManager._mode_requires_digital("FT4 / FT8") is True
    assert ReceiverManager._mode_requires_digital("SSB") is False


def test_health_summary_marks_silent_receiver_when_heartbeat_is_recent(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_visible_user(monkeypatch, "FIXED_20M_FT8")

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 10.0,
        "last_decode_unix": now - 901.0,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "quiet"
    assert summary["silent_receivers"] == 1
    assert summary["stalled_receivers"] == 0
    channel = summary["channels"]["2"]
    assert channel["health_state"] == "silent"
    assert channel["is_silent"] is True
    assert channel["is_unstable"] is False


def test_health_summary_marks_stalled_receiver_when_heartbeat_is_missing(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_visible_user(monkeypatch, "FIXED_20M_FT8")

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 180.0,
        "last_decode_unix": now - 181.0,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "degraded"
    assert summary["silent_receivers"] == 0
    assert summary["stalled_receivers"] == 1
    channel = summary["channels"]["2"]
    assert channel["health_state"] == "stalled"
    assert channel["is_stalled"] is True
    assert channel["is_unstable"] is True
    assert channel["last_reason"] == "stalled_no_decoder_output"


def test_health_summary_marks_stalled_receiver_when_assignment_is_stuck_on_wrong_rx(monkeypatch) -> None:
    """Stall is raised when the expected slot has an alien AUTO_ user and our
    worker is not visible at ANY slot.  This covers the case where the worker
    failed to connect and a wrong-band process has taken its slot."""
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    # Only an alien band at the expected slot; our 20m worker is absent entirely.
    _set_users_payload(
        monkeypatch,
        [
            {"i": 2, "n": "AUTO_40M_FT8", "t": "0:10:00"},
        ],
    )

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 5.0,
        "last_decode_unix": None,
    }

    summary = manager.health_summary()

    assert summary["overall"] == "idle"
    assert summary["stalled_receivers"] == 1
    channel = summary["channels"]["2"]
    assert channel["visible_on_kiwi"] is False
    assert channel["health_state"] == "stalled"
    assert channel["is_stalled"] is True
    assert channel["last_reason"] == "kiwi_assignment_mismatch"


def test_health_summary_healthy_when_worker_visible_at_offset_slot(monkeypatch) -> None:
    """A worker connected at a non-matching physical slot (e.g. because a legacy
    service occupies the expected slot) is healthy as long as the KiwiSDR shows
    it active and the decoder is producing output."""
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    # Slot 2 (expected) is taken by a legacy service; our worker landed at slot 7.
    _set_users_payload(
        monkeypatch,
        [
            {"i": 2, "n": "AUTO_40M_FT8", "t": "0:10:00"},
            {"i": 7, "n": "FIXED_20M_FT8", "t": "0:10:00"},
        ],
    )

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 5.0,
        "last_decode_unix": None,
    }

    summary = manager.health_summary()

    # Worker is visible (at slot 7) and decoder is active — should be healthy.
    channel = summary["channels"]["2"]
    assert channel["visible_on_kiwi"] is True
    assert channel["health_state"] == "healthy"
    assert channel["is_stalled"] is False


def test_health_summary_recognizes_compact_fixed_dual_mode_label(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_077_000.0, mode_label="FT4 / FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_users_payload(
        monkeypatch,
        [
            {"i": 2, "n": "FIXED20MFT8", "t": "0:00:10"},
        ],
    )

    now = time.time()
    manager._activity_by_rx[2] = {
        "last_decoder_output_unix": now - 5.0,
        "last_decode_unix": now - 5.0,
    }

    summary = manager.health_summary()

    channel = summary["channels"]["2"]
    assert channel["visible_on_kiwi"] is True
    assert channel["health_state"] == "healthy"
    assert channel["last_reason"] is None


def test_health_summary_recognizes_compact_roaming_dual_mode_label_at_offset_slot(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        1: ReceiverAssignment(rx=1, band="80m", freq_hz=3_574_000.0, mode_label="FT4 / FT8")
    }
    manager._active_host = "kiwi.local"
    manager._active_port = 8073
    _set_users_payload(
        monkeypatch,
        [
            {"i": 0, "n": "ROAM280MFT8", "t": "0:10:00"},
        ],
    )

    now = time.time()
    manager._activity_by_rx[1] = {
        "last_decoder_output_unix": now - 5.0,
        "last_decode_unix": now - 5.0,
    }

    summary = manager.health_summary()

    channel = summary["channels"]["1"]
    assert channel["visible_on_kiwi"] is True
    assert channel["kiwi_actual_rx"] == 0
    assert channel["health_state"] == "stalled"
    assert channel["last_reason"] == "kiwi_assignment_mismatch"


def test_fetch_live_auto_users_includes_compact_fixed_and_roam_labels(monkeypatch) -> None:
    _set_users_payload(
        monkeypatch,
        [
            {"i": 0, "n": "ROAM160MFT8", "t": "0:00:10"},
            {"i": 2, "n": "FIXED20MFT8", "t": "0:00:10"},
            {"i": 7, "n": "listener", "t": "0:00:10"},
        ],
    )

    users = ReceiverManager._fetch_live_auto_users("kiwi.local", 8073)

    assert users == {0: "ROAM160MFT8", 2: "FIXED20MFT8"}


def test_wait_for_kiwi_auto_users_clear_retries_until_managed_labels_are_gone(monkeypatch) -> None:
    manager = _make_manager()
    payloads = [
        [{"i": 0, "n": "AUTO_15M_FT8", "t": "0:10:00"}],
        [{"i": 0, "n": "AUTO_15M_FT8", "t": "0:10:01"}],
        [],
    ]
    call_count = {"value": 0}
    fake_now = {"value": 0.0}

    def fake_urlopen(req, timeout=0.0):
        idx = min(call_count["value"], len(payloads) - 1)
        call_count["value"] += 1
        return _FakeResponse(payloads[idx])

    monkeypatch.setattr(receiver_manager.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(receiver_manager.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(
        receiver_manager.time,
        "sleep",
        lambda seconds: fake_now.__setitem__("value", fake_now["value"] + seconds),
    )

    manager._wait_for_kiwi_auto_users_clear(host="kiwi.local", port=8073, timeout_s=2.0)

    assert call_count["value"] == 3
    assert fake_now["value"] == 0.5


def test_expected_user_label_uses_readable_band_and_mode_summary() -> None:
    fixed_mix = ReceiverAssignment(rx=2, band="20m", freq_hz=14_077_000.0, mode_label="FT4 / FT8")
    fixed_all = ReceiverAssignment(rx=7, band="17m", freq_hz=18_102_000.0, mode_label="FT4 / FT8 / WSPR")
    roam = ReceiverAssignment(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8")

    assert ReceiverManager._expected_user_label(fixed_mix) == "FIXED_20m_MIX"
    assert ReceiverManager._expected_user_label(fixed_all) == "FIXED_17m_ALL"
    assert ReceiverManager._expected_user_label(roam) == "ROAM_10m_FT8"


def test_user_label_matching_accepts_compact_kiwi_variants() -> None:
    assert ReceiverManager._user_label_matches("FIXED_20m_MIX", "FIXED20MMIX") is True
    assert ReceiverManager._user_label_matches("FIXED_17m_ALL", "FIXED17MALL") is True
    roam = ReceiverAssignment(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8")
    roam_labels = ReceiverManager._expected_user_label_aliases(roam)

    assert "ROAM_10m_FT8" in roam_labels
    assert "ROAM1_10m_FT8" in roam_labels
    assert ReceiverManager._label_matches_any(roam_labels, "ROAM110MFT8") is True


def test_health_summary_lock_timeout_returns_seeded_channels() -> None:
    manager = _make_manager()
    assignments = {
        0: ReceiverAssignment(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8"),
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8"),
    }
    manager._seed_health_summary_cache(assignments)

    assert manager._lock.acquire(timeout=0.1) is True
    try:
        summary = manager.health_summary()
    finally:
        manager._lock.release()

    assert summary["_from_cache"] is True
    assert summary["overall"] == "starting"
    assert set(summary["channels"].keys()) == {"0", "2"}
    assert summary["channels"]["0"]["band"] == "10m"
    assert summary["channels"]["2"]["band"] == "20m"


def test_apply_assignments_empty_manual_mode_prefers_graceful_stop_before_kick(monkeypatch) -> None:
    manager = _make_manager()
    manager._assignments = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    }

    events: list[tuple[str, object]] = []

    class _FakeWorker:
        def __init__(self) -> None:
            self._rx_chan_adjust = 0

        def stop(
            self,
            join_timeout_s: float = 3.0,
            *,
            graceful: bool = False,
            graceful_timeout_s: float = 5.0,
        ) -> None:
            events.append(
                (
                    "stop",
                    {
                        "join_timeout_s": join_timeout_s,
                        "graceful": graceful,
                        "graceful_timeout_s": graceful_timeout_s,
                    },
                )
            )

    manager._workers = {2: _FakeWorker()}

    monkeypatch.setattr(manager, "_normalize_ssb_receivers", lambda assignments: assignments)
    monkeypatch.setattr(manager, "_seed_health_summary_cache", lambda assignments: None)
    monkeypatch.setattr(manager, "_seed_truth_snapshot_cache", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_required_dependency_errors", lambda assignments: [])
    monkeypatch.setattr(manager, "_cleanup_orphan_processes", lambda: events.append(("cleanup", None)))
    monkeypatch.setattr(manager, "_wait_for_orphan_cleanup", lambda timeout_s=6.0: events.append(("wait_orphans", timeout_s)))
    monkeypatch.setattr(
        manager,
        "_wait_for_kiwi_auto_users_missing",
        lambda **kwargs: events.append(("wait_missing", set(kwargs["labels"]))),
    )
    monkeypatch.setattr(manager, "_fetch_live_auto_users", lambda host, port: {})
    monkeypatch.setattr(
        manager,
        "_run_admin_kick_all",
        lambda **kwargs: events.append(("kick", dict(kwargs))) or True,
    )

    manager.apply_assignments("kiwi.local", 8073, {})

    assert events[0] == (
        "stop",
        {
            "join_timeout_s": 6.0,
            "graceful": True,
            "graceful_timeout_s": 6.0,
        },
    )
    assert ("wait_missing", {"FIXED20MFT8", "FIXED_20m_FT8"}) in events
    assert ("cleanup", None) in events
    assert ("wait_orphans", 6.0) in events
    assert not any(event[0] == "kick" for event in events)


def test_apply_assignments_empty_start_bootstraps_fixed_receivers_first(monkeypatch) -> None:
    manager = _make_manager()
    assignments = {
        0: ReceiverAssignment(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8"),
        2: ReceiverAssignment(
            rx=2,
            band="20m",
            freq_hz=14_074_000.0,
            mode_label="FT8",
            ignore_slot_check=True,
        ),
        3: ReceiverAssignment(
            rx=3,
            band="40m",
            freq_hz=7_074_000.0,
            mode_label="FT8",
            ignore_slot_check=True,
        ),
    }

    started_rxs: list[int] = []
    live_users: dict[int, str] = {}
    stable_clear_calls: list[tuple[str, int, float, float]] = []
    fake_now = {"value": 0.0}

    class _FakeWorker:
        def __init__(self, assignment: ReceiverAssignment) -> None:
            self.assignment = assignment
            self._rx_chan_adjust = 0

        def start(self) -> None:
            started_rxs.append(int(self.assignment.rx))
            live_users[int(self.assignment.rx)] = ReceiverManager._expected_user_label(self.assignment)

    monkeypatch.setattr(receiver_manager.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(receiver_manager.time, "sleep", lambda seconds: fake_now.__setitem__("value", fake_now["value"] + seconds))
    monkeypatch.setattr(manager, "_required_dependency_errors", lambda assignments: [])
    monkeypatch.setattr(manager, "_cleanup_orphan_processes", lambda: None)
    monkeypatch.setattr(manager, "_wait_for_orphan_cleanup", lambda timeout_s=6.0: None)
    monkeypatch.setattr(manager, "_run_admin_kick_all", lambda **kwargs: True)
    monkeypatch.setattr(manager, "_wait_for_kiwi_auto_users_clear", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_fetch_live_auto_users", lambda host, port: {})
    monkeypatch.setattr(
        manager,
        "_wait_for_kiwi_slots_stable_clear",
        lambda host, port, stable_secs, timeout_s: stable_clear_calls.append((host, port, stable_secs, timeout_s)),
    )
    monkeypatch.setattr(manager, "_fetch_live_users", lambda host, port: dict(live_users))
    monkeypatch.setattr(
        manager,
        "_make_worker",
        lambda host, port, assignment, rx_chan_adjust=0: _FakeWorker(assignment),
    )

    manager.apply_assignments("kiwi.local", 8073, assignments)

    assert stable_clear_calls == []
    assert started_rxs == [2, 3, 0]
    assert fake_now["value"] == 3.5
    assert manager._health_summary_cache.get("active_receivers") == 3
    assert manager._health_summary_cache.get("overall") == "healthy"


def test_apply_assignments_targeted_correction_keeps_healthy_workers_running(monkeypatch) -> None:
    manager = _make_manager()
    assignments = {
        2: ReceiverAssignment(
            rx=2,
            band="20m",
            freq_hz=14_074_000.0,
            mode_label="FT8",
            ignore_slot_check=True,
        ),
        3: ReceiverAssignment(
            rx=3,
            band="40m",
            freq_hz=7_074_000.0,
            mode_label="FT8",
            ignore_slot_check=True,
        ),
    }

    fake_now = {"value": 0.0}
    live_users: dict[int, str] = {}
    start_counts: dict[int, int] = {}
    global_cleanup_calls: list[str] = []
    scoped_cleanup_calls: list[set[str]] = []

    class _FakeStopEvent:
        def __init__(self) -> None:
            self._set = False

        def set(self) -> None:
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class _FakeWorker:
        def __init__(self, assignment: ReceiverAssignment) -> None:
            self.assignment = assignment
            self._rx_chan_adjust = 0
            self._stop_event = _FakeStopEvent()
            self._active_user_label = ReceiverManager._expected_user_label(assignment)

        def start(self) -> None:
            rx = int(self.assignment.rx)
            count = int(start_counts.get(rx, 0)) + 1
            start_counts[rx] = count
            if rx == 2:
                live_users[2] = self._active_user_label
                return
            if rx == 3 and count == 1:
                live_users[0] = self._active_user_label
                live_users[7] = "AUTO_15m_FT8"
                return
            if rx == 3:
                live_users[3] = self._active_user_label

        def stop(self, join_timeout_s: float = 3.0) -> None:
            for slot, label in list(live_users.items()):
                if ReceiverManager._user_label_matches(self._active_user_label, label):
                    live_users.pop(slot, None)

        def _terminate_proc(self) -> None:
            self.stop()

        def join(self, timeout: float | None = None) -> None:
            return None

    monkeypatch.setattr(receiver_manager.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(
        receiver_manager.time,
        "sleep",
        lambda seconds: fake_now.__setitem__("value", fake_now["value"] + seconds),
    )
    monkeypatch.setattr(manager, "_required_dependency_errors", lambda assignments: [])
    monkeypatch.setattr(
        manager,
        "_cleanup_orphan_processes",
        lambda: global_cleanup_calls.append("global"),
    )
    monkeypatch.setattr(manager, "_wait_for_orphan_cleanup", lambda timeout_s=6.0: None)
    monkeypatch.setattr(
        manager,
        "_cleanup_orphan_processes_for_labels",
        lambda labels: scoped_cleanup_calls.append(set(labels)),
    )
    monkeypatch.setattr(manager, "_wait_for_kiwi_auto_users_clear", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_wait_for_kiwi_slots_stable_clear", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_wait_for_kiwi_auto_users_missing", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_wait_for_kiwi_slots_clear", lambda **kwargs: True)
    monkeypatch.setattr(manager, "_fetch_live_auto_users", lambda host, port: dict(live_users))
    monkeypatch.setattr(manager, "_fetch_live_users", lambda host, port: dict(live_users))
    monkeypatch.setattr(
        manager,
        "_fetch_live_users_with_age",
        lambda host, port: {int(slot): (label, 0.0) for slot, label in live_users.items()},
    )
    monkeypatch.setattr(
        manager,
        "_run_admin_kick_all",
        lambda host, port, force_all=False, kick_only_slots=None: (
            live_users.clear()
            if force_all or kick_only_slots is None
            else [live_users.pop(int(slot), None) for slot in kick_only_slots],
            True,
        )[-1],
    )
    monkeypatch.setattr(
        manager,
        "_make_worker",
        lambda host, port, assignment, rx_chan_adjust=0, ignore_slot_check=None: _FakeWorker(assignment),
    )

    manager.apply_assignments("kiwi.local", 8073, assignments)

    assert global_cleanup_calls == ["global"]
    assert scoped_cleanup_calls == [{"FIXED40MFT8", "FIXED_40m_FT8"}]
    assert live_users == {2: "FIXED_20m_FT8", 3: "FIXED_40m_FT8"}
    assert sorted(manager._workers.keys()) == [2, 3]
    assert start_counts == {2: 1, 3: 2}


def test_reconcile_marks_fixed_receiver_missing_when_expected_label_absent(monkeypatch) -> None:
    manager = _make_manager()
    assignment = ReceiverAssignment(
        rx=5,
        band="40m",
        freq_hz=7_043_050.0,
        mode_label="FT4 / WSPR",
        ignore_slot_check=True,
    )
    slot_ready = threading.Event()
    slot_ready.set()
    manager._workers[5] = SimpleNamespace(
        _active_user_label="FIXED_40M_FT4",
        _slot_ready=slot_ready,
    )

    _set_users_payload(
        monkeypatch,
        [
            {"i": 2, "n": "AUTO_40M_FT8", "t": "0:10:00"},
            {"i": 7, "n": "FIXED_30M_FT8", "t": "0:10:00"},
        ],
    )

    reconcile = manager._assignment_slots_needing_reconcile(
        host="kiwi.local",
        port=8073,
        assignments={5: assignment},
    )

    assert reconcile == {5}


def test_reconcile_keeps_fixed_receiver_when_expected_label_visible(monkeypatch) -> None:
    manager = _make_manager()
    assignment = ReceiverAssignment(
        rx=5,
        band="40m",
        freq_hz=7_043_050.0,
        mode_label="FT4 / WSPR",
        ignore_slot_check=True,
    )
    slot_ready = threading.Event()
    slot_ready.set()
    manager._workers[5] = SimpleNamespace(
        _active_user_label="FIXED40MFT4",
        _slot_ready=slot_ready,
    )

    _set_users_payload(
        monkeypatch,
        [
            {"i": 7, "n": "FIXED40MFT4", "t": "0:00:10"},
        ],
    )

    reconcile = manager._assignment_slots_needing_reconcile(
        host="kiwi.local",
        port=8073,
        assignments={5: assignment},
    )

    assert reconcile == set()


def test_band_plan_change_ignores_roaming_only_delta_when_fixed_slots_match() -> None:
    current = {
        2: ReceiverAssignment(rx=2, band="20m", freq_hz=14_077_000.0, mode_label="FT4 / FT8"),
        3: ReceiverAssignment(rx=3, band="20m", freq_hz=14_095_600.0, mode_label="WSPR"),
        4: ReceiverAssignment(rx=4, band="40m", freq_hz=7_074_000.0, mode_label="FT8"),
        5: ReceiverAssignment(rx=5, band="40m", freq_hz=7_043_050.0, mode_label="FT4 / WSPR"),
        6: ReceiverAssignment(rx=6, band="30m", freq_hz=10_138_000.0, mode_label="FT4 / FT8 / WSPR"),
        7: ReceiverAssignment(rx=7, band="17m", freq_hz=18_102_000.0, mode_label="FT4 / FT8 / WSPR"),
    }
    desired = dict(current)
    desired[0] = ReceiverAssignment(rx=0, band="60m", freq_hz=5_357_000.0, mode_label="FT8")
    desired[1] = ReceiverAssignment(rx=1, band="80m", freq_hz=3_574_000.0, mode_label="FT4 / FT8")

    assert ReceiverManager._band_plan_changed(current, desired) is False


def test_restart_receiver_waits_for_old_label_and_cleans_up_stragglers(monkeypatch) -> None:
    manager = _make_manager()
    assignment = ReceiverAssignment(rx=2, band="20m", freq_hz=14_074_000.0, mode_label="FT8")
    manager._assignments = {2: assignment}
    manager._active_host = "kiwi.local"
    manager._active_port = 8073

    old_worker = SimpleNamespace(_active_user_label="FIXED_20M_FT8", _rx_chan_adjust=3)
    manager._workers = {2: old_worker}

    events: list[tuple[str, object]] = []

    class _NewWorker:
        def start(self) -> None:
            events.append(("start", None))

        def stop(self, join_timeout_s: float = 3.0) -> None:
            events.append(("stop_new", join_timeout_s))

    monkeypatch.setattr(manager, "_stop_worker", lambda worker, join_timeout_s=3.0: events.append(("stop_old", worker)))
    monkeypatch.setattr(
        manager,
        "_wait_for_kiwi_auto_users_missing",
        lambda **kwargs: events.append(("wait_missing", set(kwargs["labels"]))),
    )
    monkeypatch.setattr(manager, "_fetch_live_auto_users", lambda host, port: {2: "FIXED_20M_FT8"})
    monkeypatch.setattr(
        manager,
        "_cleanup_orphan_processes_for_labels",
        lambda labels: events.append(("cleanup_labels", set(labels))),
    )
    monkeypatch.setattr(
        manager,
        "_make_worker",
        lambda **kwargs: events.append(("make_worker", kwargs["rx_chan_adjust"])) or _NewWorker(),
    )
    monkeypatch.setattr(
        manager,
        "_on_worker_restart",
        lambda rx, band, reason, backoff_s, consecutive_failures: events.append(("restart", (rx, band, reason))),
    )

    restarted = manager._restart_receiver_worker(2, "stale_recovery_kiwi_assignment_mismatch")

    assert restarted is True
    assert ("stop_old", old_worker) in events
    wait_events = [event for event in events if event[0] == "wait_missing"]
    assert wait_events == [
        ("wait_missing", {"FIXED20MFT8", "FIXED_20M_FT8", "FIXED_20m_FT8"}),
        ("wait_missing", {"FIXED20MFT8", "FIXED_20M_FT8", "FIXED_20m_FT8"}),
    ]
    assert ("cleanup_labels", {"FIXED20MFT8", "FIXED_20M_FT8", "FIXED_20m_FT8"}) in events
    assert ("make_worker", 3) in events
    assert ("start", None) in events


def test_restart_receiver_kicks_and_waits_for_target_slot_on_mismatch(monkeypatch) -> None:
    manager = _make_manager()
    assignment = ReceiverAssignment(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8")
    manager._assignments = {0: assignment}
    manager._active_host = "kiwi.local"
    manager._active_port = 8073

    old_worker = SimpleNamespace(_active_user_label="ROAM110MFT8", _rx_chan_adjust=0)
    manager._workers = {0: old_worker}

    events: list[tuple[str, object]] = []

    class _NewWorker:
        def start(self) -> None:
            events.append(("start", None))

        def stop(self, join_timeout_s: float = 3.0) -> None:
            events.append(("stop_new", join_timeout_s))

    monkeypatch.setattr(manager, "_stop_worker", lambda worker, join_timeout_s=3.0: events.append(("stop_old", worker)))
    monkeypatch.setattr(manager, "_wait_for_kiwi_auto_users_missing", lambda **kwargs: None)
    monkeypatch.setattr(manager, "_fetch_live_auto_users", lambda host, port: {})
    monkeypatch.setattr(manager, "_fetch_live_users", lambda host, port: {0: "AUTO_15m_FT8"})
    monkeypatch.setattr(
        manager,
        "_run_admin_kick_all",
        lambda **kwargs: events.append(("kick_slots", list(kwargs["kick_only_slots"]))) or True,
    )
    monkeypatch.setattr(
        manager,
        "_wait_for_kiwi_slots_clear",
        lambda **kwargs: events.append(("wait_slots_clear", sorted(kwargs["slots"]))) or True,
    )
    monkeypatch.setattr(manager, "_make_worker", lambda **kwargs: _NewWorker())
    monkeypatch.setattr(manager, "_on_worker_restart", lambda rx, band, reason, backoff_s, consecutive_failures: None)

    restarted = manager._restart_receiver_worker(0, "stale_recovery_kiwi_assignment_mismatch")

    assert restarted is True
    assert ("kick_slots", [0]) in events
    assert ("wait_slots_clear", [0]) in events
    assert ("start", None) in events
