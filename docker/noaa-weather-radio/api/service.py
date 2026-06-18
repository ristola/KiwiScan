"""
NOAA WX EAS API — watches the shared eas_messages volume for new alert files
written by the noaa-weather-radio decoder and pushes them to all subscribers.

GET  /                         health / endpoint list
GET  /status                   uptime, alert count, connected WebSocket clients
GET  /alerts?limit=50          recent EAS alerts, newest first (JSON)
GET  /alerts/{id}/audio        proxy WAV audio file for an alert
WS   /ws/alerts                real-time push — one JSON event per new alert
                               also sends a 'backlog' frame on connect
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

DATA_DIR   = Path(os.environ.get("EAS_DIR", "/data"))
MAX_ALERTS = int(os.environ.get("MAX_ALERTS", "200"))
POLL_SEC   = float(os.environ.get("POLL_SEC", "3"))

_alerts:      list[dict]      = []
_ws_clients:  list[WebSocket] = []
_seen:        set[str]        = set()
_start_time:  float           = time.time()

_SAME_EVENT_NAMES: dict[str, str] = {
    "TOR": "Tornado Warning",             "TOA": "Tornado Watch",
    "SVR": "Severe Thunderstorm Warning", "SVA": "Severe Thunderstorm Watch",
    "FFW": "Flash Flood Warning",         "FFA": "Flash Flood Watch",
    "FLW": "Flood Warning",
    "HUW": "Hurricane Warning",           "HUA": "Hurricane Watch",
    "WSW": "Winter Storm Warning",        "WSA": "Winter Storm Watch",
    "BZW": "Blizzard Warning",
    "HWW": "High Wind Warning",           "HWA": "High Wind Watch",
    "EAN": "Emergency Action Notification",
    "LAE": "Local Area Emergency",        "LEW": "Law Enforcement Warning",
    "RMT": "Required Monthly Test",       "RWT": "Required Weekly Test",
    "NPT": "National Periodic Test",
    "ADR": "Administrative Message",      "EAT": "Emergency Action Termination",
}


def _parse_same(text: str) -> dict:
    m = re.search(r"ZCZC-(\w+)-(\w+)-([\d\-+]+)\+(\d{4})-(\d{7})-(\w+)/NWS", text)
    if not m:
        return {}
    org, event, areas_raw, duration, issue_ts, station = m.groups()
    dur_h, dur_m = int(duration[:2]), int(duration[2:])
    return {
        "org":          org,
        "event":        event,
        "event_name":   _SAME_EVENT_NAMES.get(event, event),
        "areas":        re.findall(r"\d{6}", areas_raw),
        "duration_min": dur_h * 60 + dur_m,
        "issue_day":    int(issue_ts[:3]),
        "issue_hour":   int(issue_ts[3:5]),
        "issue_min":    int(issue_ts[5:7]),
        "station":      station,
    }


def _load_file(path: Path) -> dict | None:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            return None
        alert_id  = path.stem
        wav_path  = path.with_suffix(".wav")
        mtime     = path.stat().st_mtime
        entry: dict = {
            "id":          alert_id,
            "filename":    path.name,
            "received_at": datetime.utcfromtimestamp(mtime).isoformat() + "Z",
            "raw":         text,
            "has_audio":   wav_path.exists(),
        }
        entry.update(_parse_same(text))
        return entry
    except Exception:
        return None


async def _push_to_clients(alert: dict) -> None:
    dead = []
    msg  = json.dumps({"type": "alert", "alert": alert})
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            _ws_clients.remove(ws)
        except ValueError:
            pass


async def _watcher() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing files on startup (oldest-first so ring buffer order is chronological)
    for path in sorted(DATA_DIR.glob("EAS_*.txt"), key=lambda p: p.stat().st_mtime):
        entry = _load_file(path)
        if entry:
            _alerts.append(entry)
            _seen.add(path.name)
    if len(_alerts) > MAX_ALERTS:
        del _alerts[: len(_alerts) - MAX_ALERTS]

    while True:
        await asyncio.sleep(POLL_SEC)
        new_paths = sorted(
            (p for p in DATA_DIR.glob("EAS_*.txt") if p.name not in _seen),
            key=lambda p: p.stat().st_mtime,
        )
        for path in new_paths:
            entry = _load_file(path)
            if not entry:
                continue
            _alerts.append(entry)
            _seen.add(path.name)
            if len(_alerts) > MAX_ALERTS:
                _alerts.pop(0)
            await _push_to_clients(entry)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_watcher())
    yield


app = FastAPI(title="NOAA WX EAS API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict:
    return {
        "service":   "NOAA WX EAS API",
        "endpoints": ["/status", "/alerts", "/alerts/{id}/audio", "/ws/alerts"],
    }


@app.get("/status")
async def get_status() -> dict:
    return {
        "ok":         True,
        "uptime_sec": round(time.time() - _start_time),
        "alerts":     len(_alerts),
        "clients":    len(_ws_clients),
        "data_dir":   str(DATA_DIR),
    }


@app.get("/alerts")
async def get_alerts(limit: int = 50) -> list:
    limit = min(limit, MAX_ALERTS)
    return list(reversed(_alerts[-limit:]))


@app.get("/alerts/{alert_id}/audio")
async def get_alert_audio(alert_id: str) -> FileResponse:
    if not re.fullmatch(r"[\w\-]+", alert_id):
        raise HTTPException(400, "Invalid alert ID")
    wav = DATA_DIR / f"{alert_id}.wav"
    if not wav.exists():
        raise HTTPException(404, "Audio not found")
    return FileResponse(str(wav), media_type="audio/wav")


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket) -> None:
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        # Send recent backlog so the client is immediately up to date
        await websocket.send_text(json.dumps({
            "type":   "backlog",
            "alerts": list(reversed(_alerts[-10:])),
        }))
        # Hold connection open; new alerts arrive via _push_to_clients()
        while True:
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"type": "ping"}))
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        try:
            _ws_clients.remove(websocket)
        except ValueError:
            pass
