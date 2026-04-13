from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.smart_scheduler import make_router
from kiwi_scan.smart_scheduler import SmartScheduler


class _ReceiverMgrStub:
    def health_summary(self):
        return {"overall": "healthy", "channels": {}}


def test_smart_scheduler_status_returns_ft8_snapshot() -> None:
    scheduler = SmartScheduler(receiver_mgr=_ReceiverMgrStub())
    app = FastAPI()
    app.include_router(make_router(smart_scheduler=scheduler))
    client = TestClient(app)

    response = client.get("/smart_scheduler/status")

    assert response.status_code == 200
    assert response.json()["mode"] == "ft8"