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
import urllib.request
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

# SAME 3-digit state FIPS → NWS 2-letter state code
_FIPS_STATE: dict[str, str] = {
    "001":"AL","002":"AK","004":"AZ","005":"AR","006":"CA","008":"CO","009":"CT",
    "010":"DE","011":"DC","012":"FL","013":"GA","015":"HI","016":"ID","017":"IL",
    "018":"IN","019":"IA","020":"KS","021":"KY","022":"LA","023":"ME","024":"MD",
    "025":"MA","026":"MI","027":"MN","028":"MS","029":"MO","030":"MT","031":"NE",
    "032":"NV","033":"NH","034":"NJ","035":"NM","036":"NY","037":"NC","038":"ND",
    "039":"OH","040":"OK","041":"OR","042":"PA","044":"RI","045":"SC","046":"SD",
    "047":"TN","048":"TX","049":"UT","050":"VT","051":"VA","053":"WA","054":"WV",
    "055":"WI","056":"WY","072":"PR","078":"VI",
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


def _nws_fetch(event_name: str, same_areas: list[str]) -> dict:
    """Blocking NWS API lookup — run in a thread executor."""
    if not same_areas or not event_name:
        return {}
    # Derive unique state abbreviations from SAME FIPS codes (format PSSCCC)
    states: set[str] = set()
    for code in same_areas:
        if len(code) == 6:
            fips3 = "0" + code[1:3]  # chars 1-2 are 2-digit state FIPS, zero-pad to 3
            st = _FIPS_STATE.get(fips3)
            if st:
                states.add(st)
    if not states:
        return {}

    area_param = ",".join(sorted(states))
    url = (
        "https://api.weather.gov/alerts/active"
        f"?area={area_param}&event={urllib.request.quote(event_name)}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "noaa-eas-api/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception:
        return {}

    same_set = set(same_areas)
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        nws_same = set(props.get("geocode", {}).get("SAME", []))
        if nws_same & same_set:  # at least one overlapping FIPS code
            return {
                "nws_headline":    props.get("headline", ""),
                "nws_description": props.get("description", ""),
                "nws_instruction": props.get("instruction", ""),
                "nws_area_desc":   props.get("areaDesc", ""),
                "nws_id":          props.get("id", ""),
            }
    return {}


async def _nws_enrich(entry: dict) -> None:
    """Fetch NWS full text in a thread, update the entry, push update to WS clients."""
    loop = asyncio.get_event_loop()
    nws  = await loop.run_in_executor(
        None, _nws_fetch, entry.get("event_name", ""), entry.get("areas", [])
    )
    if not nws:
        return
    entry.update(nws)
    # Push an update frame so connected dashboards get the text without reconnecting
    dead = []
    msg  = json.dumps({"type": "alert_update", "id": entry["id"], "nws": nws})
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
            # Enrich with NWS full text in background — doesn't block the watcher
            asyncio.create_task(_nws_enrich(entry))


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
