import os
import signal
import threading
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.admin import make_router


def test_runtime_restart_endpoint_schedules_sigterm_for_container(monkeypatch):
    monkeypatch.setenv("KIWISCAN_UPDATE_MODE", "container")

    killed: list[tuple[int, int]] = []
    kill_event = threading.Event()

    def _fake_kill(pid: int, sig: int) -> None:
        killed.append((pid, sig))
        kill_event.set()

    monkeypatch.setattr(os, "kill", _fake_kill)

    app = FastAPI()
    app.include_router(make_router())
    client = TestClient(app)

    response = client.post("/admin/runtime/restart")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["mode"] == "container"
    assert payload["status"] == "restarting_container"
    assert kill_event.wait(timeout=1.0)
    assert killed == [(os.getpid(), signal.SIGTERM)]


def test_clear_roaming_receivers_endpoint_clears_only_rx0_rx1() -> None:
    kick_calls: list[tuple[str, int, list[int], bool]] = []
    stop_calls: list[str] = []
    wait_missing_calls: list[tuple[str, int, set[str], float]] = []
    cleanup_calls: list[set[str]] = []
    wait_calls: list[tuple[str, int, set[int], float, float]] = []
    apply_calls: list[tuple[str, int, dict[int, object], bool]] = []
    loop_calls: list[tuple[str, str]] = []

    class _FakeWorker:
        def __init__(self, label: str) -> None:
            self._active_user_label = label

        def stop(self) -> None:
            stop_calls.append(self._active_user_label)

    assignment_rx0 = SimpleNamespace(rx=0, band="60m", mode_label="FT8")
    assignment_rx1 = SimpleNamespace(rx=1, band="80m", mode_label="MIX")

    receiver_mgr = SimpleNamespace(
        _lock=threading.Lock(),
        _startup_eviction_active=threading.Event(),
        _assignments={0: assignment_rx0, 1: assignment_rx1},
        _workers={0: _FakeWorker("ROAM_60m_FT8"), 1: _FakeWorker("ROAM_80m_MIX")},
        _activity_by_rx={0: {"band": "60m"}, 1: {"band": "80m"}},
        _expected_user_label_aliases=lambda assignment: {
            f"ROAM_{assignment.band}_{assignment.mode_label}",
        },
        _stop_worker=lambda worker, join_timeout_s=3.0: stop_calls.append(str(getattr(worker, "_active_user_label", ""))),
        _wait_for_kiwi_auto_users_missing=lambda host, port, labels, timeout_s=10.0: wait_missing_calls.append(
            (str(host), int(port), set(labels), float(timeout_s))
        ),
        _fetch_live_users=lambda host, port: {0: "ROAM_60m_FT8", 1: "ROAM_80m_MIX"},
        _label_matches_any=lambda expected_labels, actual: str(actual) in set(expected_labels),
        _cleanup_orphan_processes_for_labels=lambda labels: cleanup_calls.append(set(labels)),
        _run_admin_kick_all=lambda host, port, kick_only_slots, allow_fallback_kick_all: kick_calls.append(
            (str(host), int(port), list(kick_only_slots), bool(allow_fallback_kick_all))
        ),
        apply_assignments=lambda host, port, assignments, allow_starting_from_empty_full_reset=True: apply_calls.append(
            (str(host), int(port), dict(assignments), bool(allow_starting_from_empty_full_reset))
        ),
        _wait_for_kiwi_slots_clear=lambda host, port, slots, stable_secs, timeout_s: wait_calls.append(
            (str(host), int(port), set(slots), float(stable_secs), float(timeout_s))
        ),
    )
    mgr = SimpleNamespace(lock=threading.Lock(), host="kiwi.local", port=8073)
    auto_set_loop = SimpleNamespace(
        pause_for_external=lambda reason: loop_calls.append(("pause", str(reason))),
        resume_from_external=lambda reason: loop_calls.append(("resume", str(reason))),
    )

    app = FastAPI()
    app.include_router(make_router(receiver_mgr=receiver_mgr, mgr=mgr, auto_set_loop=auto_set_loop))
    client = TestClient(app)

    response = client.post("/admin/clear-roaming-receivers")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "status": "cleared", "reserved_receivers": [0, 1]}
    assert loop_calls == [("pause", "semi_transition"), ("resume", "semi_transition")]
    assert stop_calls == ["ROAM_60m_FT8", "ROAM_80m_MIX"]
    assert wait_missing_calls == [
        ("kiwi.local", 8073, {"ROAM_60m_FT8", "ROAM_80m_MIX"}, 4.0),
        ("kiwi.local", 8073, {"ROAM_60m_FT8", "ROAM_80m_MIX"}, 4.0),
    ]
    assert cleanup_calls == [{"ROAM_60m_FT8", "ROAM_80m_MIX"}]
    assert kick_calls == [("kiwi.local", 8073, [0, 1], False)]
    assert len(apply_calls) == 1
    host, port, assignments, allow_full_reset = apply_calls[0]
    assert (host, port, allow_full_reset) == ("kiwi.local", 8073, False)
    assert sorted(assignments.keys()) == [2, 3, 4, 5, 6, 7]
    assert all(getattr(assignment, "ignore_slot_check", False) for assignment in assignments.values())
    assert wait_calls == [("kiwi.local", 8073, {0, 1}, 0.75, 4.0)]
    assert receiver_mgr._assignments == {}
    assert receiver_mgr._workers == {}
    assert receiver_mgr._activity_by_rx == {}


def test_restore_roaming_receivers_endpoint_applies_current_auto_settings() -> None:
    apply_calls: list[tuple[bool, bool]] = []

    auto_set_loop = SimpleNamespace(
        apply_current_settings=lambda force=False, sync_state=True: apply_calls.append((bool(force), bool(sync_state))) or True,
    )

    app = FastAPI()
    app.include_router(make_router(auto_set_loop=auto_set_loop))
    client = TestClient(app)

    response = client.post("/admin/restore-roaming-receivers")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "status": "restored", "reserved_receivers": [0, 1]}
    assert apply_calls == [(True, True)]


def test_manual_assignments_endpoint_targets_requested_kiwi_and_applies_enabled_receivers() -> None:
    apply_calls: list[tuple[str, int, dict[int, object], bool]] = []
    resolve_calls: list[tuple[object, object]] = []

    receiver_mgr = SimpleNamespace(
        apply_assignments=lambda host, port, assignments, allow_starting_from_empty_full_reset=True: apply_calls.append(
            (str(host), int(port), dict(assignments), bool(allow_starting_from_empty_full_reset))
        ),
    )
    mgr = SimpleNamespace(
        lock=threading.Lock(),
        resolve_runtime_target=lambda kiwi_key=None, kiwi_index=None: resolve_calls.append((kiwi_key, kiwi_index)) or {
            "host": "kiwi-2.local",
            "port": 8074,
            "kiwi_key": str(kiwi_key or "kiwi-2.local:8074"),
        },
    )

    app = FastAPI()
    app.include_router(make_router(receiver_mgr=receiver_mgr, mgr=mgr))
    client = TestClient(app)

    response = client.post(
        "/admin/manual-assignments",
        json={
            "kiwi_key": "kiwi-2.local:8074",
            "assignments": [
                {"rx": 0, "enabled": True, "band": "20m", "freq_khz": 14074.0, "mode": "FT8"},
                {"rx": 3, "enabled": True, "band": "40m", "freq_khz": 7290.0, "mode": "AM"},
                {"rx": 5, "enabled": False, "band": "10m", "freq_khz": 28074.0, "mode": "FT8"},
                {"rx": 6, "enabled": True, "band": "20m", "freq_khz": 14200.0, "mode": "USB"},
            ],
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "host": "kiwi-2.local",
        "port": 8074,
        "kiwi_key": "kiwi-2.local:8074",
        "active_receivers": [0, 3, 6],
        "status": "applied",
    }
    assert resolve_calls == [("kiwi-2.local:8074", None)]
    assert len(apply_calls) == 1
    host, port, assignments, allow_full_reset = apply_calls[0]
    assert (host, port, allow_full_reset) == ("kiwi-2.local", 8074, False)
    assert sorted(assignments.keys()) == [0, 3, 6]
    assert getattr(assignments[0], "band") == "20m"
    assert getattr(assignments[0], "mode_label") == "FT8"
    assert getattr(assignments[0], "freq_hz") == 14_074_000.0
    assert getattr(assignments[0], "user_label_override") == "MANUAL_RX0_20m_FT8"
    assert getattr(assignments[3], "band") == "40m"
    assert getattr(assignments[3], "mode_label") == "AM"
    assert getattr(assignments[3], "freq_hz") == 7_290_000.0
    assert getattr(assignments[3], "user_label_override") == "MANUAL_RX3_40m_AM"
    assert getattr(assignments[6], "mode_label") == "USB"
    assert getattr(assignments[6], "freq_hz") == 14_200_000.0
    assert getattr(assignments[6], "user_label_override") == "MANUAL_RX6_20m_USB"