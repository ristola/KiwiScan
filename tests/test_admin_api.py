import os
import signal
import threading

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