from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parents[1]
PROD_MINIMAL_PACKAGE_NAME = "_prod_minimal_kiwi_scan"
PROD_MINIMAL_PACKAGE_ROOT = REPO_ROOT / "prod_minimal" / "src" / "kiwi_scan"


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeWorker:
    def __init__(self, *, host: str, port: int, user_label: str) -> None:
        self._host = str(host)
        self._port = int(port)
        self._active_user_label = str(user_label)

    def is_alive(self) -> bool:
        return True


class _NeverAcquireLock:
    def acquire(self, timeout: float | None = None) -> bool:
        return False

    def release(self) -> None:
        raise AssertionError("release should not be called when acquire fails")


def _load_prod_minimal_package() -> ModuleType:
    package = sys.modules.get(PROD_MINIMAL_PACKAGE_NAME)
    if package is not None:
        return package

    spec = importlib.util.spec_from_file_location(
        PROD_MINIMAL_PACKAGE_NAME,
        PROD_MINIMAL_PACKAGE_ROOT / "__init__.py",
        submodule_search_locations=[str(PROD_MINIMAL_PACKAGE_ROOT)],
    )
    assert spec is not None and spec.loader is not None
    package = importlib.util.module_from_spec(spec)
    sys.modules[PROD_MINIMAL_PACKAGE_NAME] = package
    spec.loader.exec_module(package)
    return package


def _load_prod_minimal_receiver_manager() -> ModuleType:
    _load_prod_minimal_package()
    full_name = f"{PROD_MINIMAL_PACKAGE_NAME}.receiver_manager"
    module = sys.modules.get(full_name)
    if module is not None:
        return module

    spec = importlib.util.spec_from_file_location(full_name, PROD_MINIMAL_PACKAGE_ROOT / "receiver_manager.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return module


def _make_manager(prod_receiver_manager: ModuleType, monkeypatch) -> object:
    receiver_manager_cls = prod_receiver_manager.ReceiverManager
    monkeypatch.setattr(receiver_manager_cls, "_cleanup_orphan_processes", lambda self: None)
    monkeypatch.setattr(receiver_manager_cls, "dependency_report", lambda self: {"missing": []})
    monkeypatch.setattr(prod_receiver_manager.threading.Thread, "start", lambda self: None)
    return receiver_manager_cls(
        kiwirecorder_path=Path("/bin/sh"),
        ft8modem_path=Path("/bin/sh"),
        af2udp_path=Path("/bin/sh"),
        sox_path="/bin/sh",
    )


def _install_live_users(prod_receiver_manager: ModuleType, monkeypatch, payloads: dict[tuple[str, int], list[dict[str, object]]]) -> None:
    def fake_urlopen(req, timeout=0.0):
        url = getattr(req, "full_url", req)
        url_text = str(url)
        for (host, port), payload in payloads.items():
            if f"{host}:{port}" in url_text:
                return _FakeResponse(payload)
        raise AssertionError(f"unexpected urlopen target: {url_text}")

    monkeypatch.setattr(prod_receiver_manager.urllib.request, "urlopen", fake_urlopen)


def _build_dual_target_manager(prod_receiver_manager: ModuleType, monkeypatch) -> tuple[object, object, object]:
    manager = _make_manager(prod_receiver_manager, monkeypatch)
    receiver_assignment_cls = prod_receiver_manager.ReceiverAssignment
    runtime_target_state_cls = prod_receiver_manager._RuntimeTargetState

    assignment_a = receiver_assignment_cls(rx=0, band="10m", freq_hz=28_074_000.0, mode_label="FT8")
    assignment_b = receiver_assignment_cls(rx=0, band="20m", freq_hz=14_074_000.0, mode_label="FT8")

    worker_a = _FakeWorker(
        host="kiwi-a.local",
        port=8073,
        user_label=manager._expected_user_label(assignment_a),
    )
    worker_b = _FakeWorker(
        host="kiwi-b.local",
        port=8074,
        user_label=manager._expected_user_label(assignment_b),
    )

    manager._active_host = "kiwi-a.local"
    manager._active_port = 8073
    manager._assignments = {0: assignment_a}
    manager._workers = {0: worker_a}
    manager._runtime_target_states[manager._runtime_target_key("kiwi-b.local", 8074)] = runtime_target_state_cls(
        host="kiwi-b.local",
        port=8074,
        resource_slot=1,
        assignments={0: assignment_b},
        workers={0: worker_b},
    )

    _install_live_users(
        prod_receiver_manager,
        monkeypatch,
        {
            ("kiwi-a.local", 8073): [{"i": 0, "n": manager._expected_user_label(assignment_a), "t": "0:05:00"}],
            ("kiwi-b.local", 8074): [{"i": 0, "n": manager._expected_user_label(assignment_b), "t": "0:05:00"}],
        },
    )

    return manager, assignment_a, assignment_b


def test_prod_minimal_health_summary_for_target_uses_requested_runtime_target(monkeypatch) -> None:
    prod_receiver_manager = _load_prod_minimal_receiver_manager()
    manager, _, assignment_b = _build_dual_target_manager(prod_receiver_manager, monkeypatch)

    summary = manager.health_summary_for_target("kiwi-b.local", 8074)

    assert summary["host"] == "kiwi-b.local"
    assert summary["port"] == 8074
    assert summary["active_receivers"] == 1
    assert summary["channels"]["0"]["band"] == assignment_b.band
    assert summary["channels"]["0"]["freq_hz"] == assignment_b.freq_hz
    assert summary["channels"]["0"]["visible_on_kiwi"] is True


def test_prod_minimal_health_summary_for_target_fallback_stays_target_local(monkeypatch) -> None:
    prod_receiver_manager = _load_prod_minimal_receiver_manager()
    manager, _, assignment_b = _build_dual_target_manager(prod_receiver_manager, monkeypatch)
    manager._lock = _NeverAcquireLock()

    summary = manager.health_summary_for_target("kiwi-b.local", 8074)

    assert summary["host"] == "kiwi-b.local"
    assert summary["port"] == 8074
    assert summary["channels"]["0"]["band"] == assignment_b.band
    assert summary["channels"]["0"]["freq_hz"] == assignment_b.freq_hz
    assert summary["_from_cache"] is True