from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.schedule import make_router


def test_schedule_defaults_to_ft8() -> None:
    app = FastAPI()
    app.include_router(make_router())
    client = TestClient(app)

    response = client.get("/schedule")

    assert response.status_code == 200
    assert response.json()["mode"] == "ft8"


def test_schedule_rejects_phone_mode_query() -> None:
    app = FastAPI()
    app.include_router(make_router())
    client = TestClient(app)

    response = client.get("/schedule?mode=phone")

    assert response.status_code == 400
    assert response.json() == {"detail": "mode must be 'ft8'"}