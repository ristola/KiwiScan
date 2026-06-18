"""
TPMS Monitor Service — FastAPI
Decodes 315 MHz / 433.92 MHz tire-pressure monitoring system (TPMS)
sensor broadcasts via rtl_433 connected to an RTL-SDR over rtl_tcp.

POST /start        — start monitoring (rtl_tcp host/port + frequencies)
POST /stop         — stop
GET  /status       — running state + sensor table
GET  /detections   — recent raw detections (JSON array)
DELETE /detections — clear history
GET  /defaults     — default config
WS   /ws           — 2-second push of status + last 20 detections
GET  /             — self-hosted sensor dashboard HTML
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Body, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)
logger = logging.getLogger("tpms")

# ── Env / defaults ────────────────────────────────────────────────
SERVICE_PORT     = int(os.getenv("SERVICE_PORT", "4026"))
DEFAULT_RTL_HOST = os.getenv("RTL_TCP_HOST", "127.0.0.1")
DEFAULT_RTL_PORT = int(os.getenv("RTL_TCP_PORT", "1234"))
# Comma-separated list: 315M covers North America, 433.92M covers EU/Asia
DEFAULT_FREQS    = os.getenv("TPMS_FREQS", "315M,433.92M").split(",")
LOW_PRESSURE_PSI = float(os.getenv("LOW_PRESSURE_PSI", "30.0"))
MAX_DETECTIONS   = 500

_SAVED_CONFIG_PATH = "/opt/tpms/data/last_config.json"

# ── Unit helpers ──────────────────────────────────────────────────
def _kpa_to_psi(kpa: float) -> float:
    return round(float(kpa) * 0.145038, 1)

def _bar_to_psi(bar: float) -> float:
    return round(float(bar) * 14.5038, 1)

def _c_to_f(c: float) -> float:
    return round(float(c) * 9.0 / 5.0 + 32.0, 1)

def _parse_pressure(packet: dict) -> float | None:
    if "pressure_kPa" in packet:
        return _kpa_to_psi(packet["pressure_kPa"])
    if "pressure_PSI" in packet:
        return round(float(packet["pressure_PSI"]), 1)
    if "pressure_bar" in packet:
        return _bar_to_psi(packet["pressure_bar"])
    return None

def _parse_temp(packet: dict) -> float | None:
    if "temperature_C" in packet:
        return _c_to_f(packet["temperature_C"])
    if "temperature_F" in packet:
        return round(float(packet["temperature_F"]), 1)
    return None

# ── Detection store ───────────────────────────────────────────────
class DetectionStore:
    def __init__(self, maxlen: int = MAX_DETECTIONS):
        self._q: deque[dict] = deque(maxlen=maxlen)
        self._sensors: dict[str, dict] = {}  # sensor_id → latest packet
        self._lock = threading.Lock()

    def add(self, packet: dict) -> None:
        with self._lock:
            self._q.appendleft(packet)
            sid = str(packet.get("id") or "")
            if sid:
                self._sensors[sid] = packet

    def detections(self, limit: int = 100) -> list[dict]:
        with self._lock:
            return list(self._q)[:limit]

    def sensors(self) -> list[dict]:
        with self._lock:
            return sorted(self._sensors.values(),
                          key=lambda x: x.get("ts", 0), reverse=True)

    def total(self) -> int:
        with self._lock:
            return len(self._q)

    def clear(self) -> None:
        with self._lock:
            self._q.clear()
            self._sensors.clear()

# ── TPMS monitor ──────────────────────────────────────────────────
class TpmsMonitor:
    def __init__(self, rtl_host: str, rtl_port: int, freqs: list[str]):
        self.rtl_host  = rtl_host
        self.rtl_port  = rtl_port
        self.freqs     = [f.strip() for f in freqs] if freqs else DEFAULT_FREQS
        self._running  = False
        self.error: str | None = None
        self._store    = DetectionStore()
        self._proc: Optional[subprocess.Popen] = None
        self._start_ts = 0.0

    def start(self) -> None:
        cmd = [
            "rtl_433",
            "-d", f"rtl_tcp:{self.rtl_host}:{self.rtl_port}",
            "-F", "json",
            "-M", "time:unix",
        ]
        for f in self.freqs:
            cmd += ["-f", f]

        logger.info("Starting rtl_433: %s", " ".join(cmd))
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            self.error = "rtl_433 not found — not installed in the container"
            logger.error(self.error)
            return

        self._running  = True
        self._start_ts = time.time()
        threading.Thread(target=self._read_loop, daemon=True,
                         name="tpms-read").start()
        threading.Thread(target=self._err_loop, daemon=True,
                         name="tpms-err").start()
        logger.info("TPMS monitor started — %s:%d  freqs=%s",
                    self.rtl_host, self.rtl_port, self.freqs)

    def _read_loop(self) -> None:
        for line in self._proc.stdout:
            if not self._running:
                break
            line = line.strip()
            if not line:
                continue
            try:
                pkt = json.loads(line)
            except json.JSONDecodeError:
                continue

            model = str(pkt.get("model", "")).upper()
            ptype = str(pkt.get("type",  "")).upper()

            # Keep only TPMS packets; rtl_433 decodes many sensor types.
            if "TPMS" not in model and "TPMS" not in ptype and "TIRE" not in model:
                continue

            ts           = float(pkt.get("time", time.time()))
            pressure_psi = _parse_pressure(pkt)
            temp_f       = _parse_temp(pkt)
            battery_ok   = bool(int(pkt.get("battery_ok", 1) or 1))
            flags        = int(pkt.get("flags", 0) or 0)
            sensor_id    = str(pkt.get("id") or "")
            alarm        = bool(flags) or (
                pressure_psi is not None and pressure_psi < LOW_PRESSURE_PSI
            )

            enriched = {
                "ts":           ts,
                "received_at":  datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "model":        pkt.get("model", "Unknown"),
                "id":           sensor_id,
                "pressure_psi": pressure_psi,
                "temp_f":       temp_f,
                "battery_ok":   battery_ok,
                "flags":        flags,
                "alarm":        alarm,
            }
            self._store.add(enriched)
            logger.info("TPMS %-20s id=%-10s psi=%-6s temp=%s°F alarm=%s",
                        enriched["model"], sensor_id,
                        f"{pressure_psi:.1f}" if pressure_psi is not None else "?",
                        f"{temp_f:.1f}"       if temp_f       is not None else "?",
                        alarm)

        self._running = False
        logger.info("TPMS read loop exited")

    def _err_loop(self) -> None:
        for line in self._proc.stderr:
            t = line.strip()
            if t:
                logger.info("rtl_433: %s", t)
        rc = self._proc.returncode
        if rc and rc != 0:
            self.error    = f"rtl_433 exited with code {rc}"
            self._running = False

    def stop(self) -> None:
        self._running = False
        if self._proc:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=3)
            except Exception:
                pass
            self._proc = None
        logger.info("TPMS monitor stopped")

    def status(self) -> dict:
        return {
            "running":    self._running,
            "error":      self.error,
            "rtl_host":   self.rtl_host,
            "rtl_port":   self.rtl_port,
            "freqs":      self.freqs,
            "total":      self._store.total(),
            "sensors":    self._store.sensors(),
            "uptime_sec": int(time.time() - self._start_ts) if self._running else 0,
        }

# ── Config persistence ─────────────────────────────────────────────
def _save_config(rtl_host: str, rtl_port: int, freqs: list[str]) -> None:
    try:
        import pathlib
        pathlib.Path(_SAVED_CONFIG_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(_SAVED_CONFIG_PATH, "w") as f:
            json.dump({"rtl_host": rtl_host, "rtl_port": rtl_port, "freqs": freqs}, f)
    except Exception as exc:
        logger.warning("Could not save config: %s", exc)

def _load_config() -> dict | None:
    try:
        with open(_SAVED_CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return None

# ── Self-hosted dashboard HTML ─────────────────────────────────────
_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TPMS Monitor · ShackMate</title>
<style>
  :root { --bg:#0b1016; --bg2:#111820; --bg3:#182030; --fg:#d4dce8;
          --muted:#5a6a80; --ok:#3fb950; --warn:#d29922; --fault:#f85149;
          --blue:#2f8cff; --border:rgba(255,255,255,0.08); }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--fg); font:13px/1.5 "Segoe UI",system-ui,sans-serif; padding:1rem; }
  h1 { font-size:1rem; font-weight:700; letter-spacing:0.04em; margin-bottom:0.15rem; }
  .subtitle { font-size:0.7rem; color:var(--muted); margin-bottom:1.2rem; }
  .status-bar { display:flex; align-items:center; gap:0.75rem; margin-bottom:1.2rem;
                background:var(--bg2); border:1px solid var(--border); border-radius:6px; padding:0.5rem 0.75rem; }
  .dot { width:8px; height:8px; border-radius:50%; background:var(--muted); flex-shrink:0; }
  .dot.ok { background:var(--ok); box-shadow:0 0 6px var(--ok); }
  .dot.fault { background:var(--fault); }
  .stat { font-size:0.68rem; color:var(--muted); }
  .stat strong { color:var(--fg); }
  table { width:100%; border-collapse:collapse; margin-bottom:1.2rem; }
  th { font-size:0.65rem; font-weight:700; letter-spacing:0.06em; text-transform:uppercase;
       color:var(--muted); padding:0.35rem 0.6rem; text-align:left;
       border-bottom:1px solid var(--border); }
  td { padding:0.45rem 0.6rem; border-bottom:1px solid var(--border); font-size:0.75rem; vertical-align:middle; }
  tr:last-child td { border-bottom:0; }
  .sensor-id { font-family:monospace; font-size:0.72rem; color:var(--muted); }
  .psi { font-weight:700; font-size:0.95rem; }
  .psi.ok   { color:var(--ok); }
  .psi.warn { color:var(--warn); }
  .psi.low  { color:var(--fault); }
  .alarm-badge { display:inline-block; background:rgba(248,81,73,0.18); color:var(--fault);
                 border:1px solid rgba(248,81,73,0.35); border-radius:4px;
                 font-size:0.62rem; font-weight:700; padding:0.1rem 0.4rem; letter-spacing:0.06em; }
  .ok-badge  { display:inline-block; background:rgba(63,185,80,0.12); color:var(--ok);
               border:1px solid rgba(63,185,80,0.25); border-radius:4px;
               font-size:0.62rem; font-weight:700; padding:0.1rem 0.4rem; letter-spacing:0.06em; }
  .ts { font-size:0.65rem; color:var(--muted); }
  .section-label { font-size:0.65rem; font-weight:700; letter-spacing:0.08em; text-transform:uppercase;
                   color:var(--muted); margin-bottom:0.4rem; }
  .empty { color:var(--muted); font-size:0.75rem; padding:0.75rem 0; }
  #log { background:var(--bg2); border:1px solid var(--border); border-radius:6px;
         padding:0.5rem 0.75rem; font-family:monospace; font-size:0.68rem;
         color:var(--muted); height:160px; overflow-y:auto; }
  #log .entry { color:var(--fg); margin-bottom:0.2rem; }
  #log .entry.alarm { color:var(--fault); }
</style>
</head>
<body>
<h1>TPMS Monitor</h1>
<div class="subtitle">ShackMate · Tire Pressure Monitoring System · rtl_433</div>

<div class="status-bar">
  <div class="dot" id="dot"></div>
  <span id="status-text" style="font-size:0.78rem">Connecting…</span>
  <span class="stat" style="margin-left:auto">Detections: <strong id="total">0</strong></span>
  <span class="stat">Uptime: <strong id="uptime">--</strong></span>
  <span class="stat">Freqs: <strong id="freqs">--</strong></span>
</div>

<div class="section-label">Live Sensors</div>
<table id="sensor-table">
  <thead><tr><th>Sensor ID</th><th>Model</th><th>Pressure</th><th>Temp</th><th>Battery</th><th>Status</th><th>Last Seen</th></tr></thead>
  <tbody id="sensor-body"><tr><td colspan="7" class="empty">No sensors detected yet — listening…</td></tr></tbody>
</table>

<div class="section-label">Recent Detections</div>
<div id="log"><span style="color:var(--muted)">Waiting for signals…</span></div>

<script>
const fmtAge = (ts) => {
  const s = Math.round((Date.now()/1000) - ts);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
};
const fmtPsi = (v) => v !== null && v !== undefined ? v.toFixed(1) + ' PSI' : '—';
const fmtF   = (v) => v !== null && v !== undefined ? v.toFixed(0) + '°F' : '—';

const psiClass = (v) => {
  if (v === null || v === undefined) return '';
  if (v < 28) return 'low';
  if (v < 32) return 'warn';
  return 'ok';
};

let ws, wsTimer;
const connect = () => {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);
  ws.onmessage = (e) => {
    const d = JSON.parse(e.data);
    render(d);
  };
  ws.onclose = () => { clearTimeout(wsTimer); wsTimer = setTimeout(connect, 3000); };
  ws.onerror = () => ws.close();
};

const render = (st) => {
  document.getElementById('dot').className = 'dot ' + (st.running ? 'ok' : 'fault');
  document.getElementById('status-text').textContent = st.running
    ? `Online · ${st.rtl_host}:${st.rtl_port}`
    : (st.error || 'Stopped');
  document.getElementById('total').textContent  = st.total  || 0;
  document.getElementById('uptime').textContent = st.uptime_sec > 0
    ? `${Math.floor(st.uptime_sec/3600)}h ${Math.floor((st.uptime_sec%3600)/60)}m` : '--';
  document.getElementById('freqs').textContent = (st.freqs || []).join(', ') || '--';

  const sensors = st.sensors || [];
  const tbody = document.getElementById('sensor-body');
  if (!sensors.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">No sensors detected yet — listening…</td></tr>';
  } else {
    tbody.innerHTML = sensors.map(s => `
      <tr>
        <td class="sensor-id">${s.id || '—'}</td>
        <td>${s.model || '—'}</td>
        <td><span class="psi ${psiClass(s.pressure_psi)}">${fmtPsi(s.pressure_psi)}</span></td>
        <td>${fmtF(s.temp_f)}</td>
        <td>${s.battery_ok ? '<span class="ok-badge">OK</span>' : '<span class="alarm-badge">LOW</span>'}</td>
        <td>${s.alarm ? '<span class="alarm-badge">ALARM</span>' : '<span class="ok-badge">NORMAL</span>'}</td>
        <td class="ts">${fmtAge(s.ts)}</td>
      </tr>`).join('');
  }

  const recent = st.recent || [];
  if (recent.length) {
    const log = document.getElementById('log');
    log.innerHTML = recent.map(r => {
      const t = new Date(r.received_at).toLocaleTimeString();
      const p = fmtPsi(r.pressure_psi), tmp = fmtF(r.temp_f);
      const cls = r.alarm ? 'entry alarm' : 'entry';
      return `<div class="${cls}">[${t}] ${r.model} id=${r.id || '?'} ${p} ${tmp}${r.alarm ? ' ⚠ ALARM' : ''}</div>`;
    }).join('');
    log.scrollTop = 0;
  }
};

connect();
</script>
</body>
</html>"""

# ── FastAPI app ────────────────────────────────────────────────────
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _monitor
    cfg = _load_config()
    if cfg:
        _monitor = TpmsMonitor(
            cfg.get("rtl_host", DEFAULT_RTL_HOST),
            int(cfg.get("rtl_port", DEFAULT_RTL_PORT)),
            cfg.get("freqs", DEFAULT_FREQS),
        )
        _monitor.start()
        if _monitor.error:
            logger.error("Auto-start failed: %s", _monitor.error)
            _monitor = None
        else:
            logger.info("Auto-started from saved config")
    yield

app = FastAPI(title="TPMS Monitor", version="1.0.0", lifespan=lifespan)

_monitor: Optional[TpmsMonitor] = None


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/defaults")
async def get_defaults():
    return {"rtl_host": DEFAULT_RTL_HOST, "rtl_port": DEFAULT_RTL_PORT, "freqs": DEFAULT_FREQS}


@app.post("/start")
async def start(body: dict = Body(...)):
    global _monitor
    if _monitor and _monitor._running:
        raise HTTPException(409, "Already running — POST /stop first")

    rtl_host = body.get("rtl_host", DEFAULT_RTL_HOST)
    rtl_port = int(body.get("rtl_port", DEFAULT_RTL_PORT))
    freqs    = body.get("freqs", DEFAULT_FREQS)
    if isinstance(freqs, str):
        freqs = [f.strip() for f in freqs.split(",")]

    _monitor = TpmsMonitor(rtl_host, rtl_port, freqs)
    _monitor.start()

    if _monitor.error:
        raise HTTPException(500, _monitor.error)

    _save_config(rtl_host, rtl_port, freqs)
    return {"ok": True, "rtl_host": rtl_host, "rtl_port": rtl_port, "freqs": freqs}


@app.post("/stop")
async def stop():
    global _monitor
    if not _monitor:
        return {"ok": True, "message": "No active session"}
    _monitor.stop()
    _monitor = None
    return {"ok": True}


@app.get("/status")
async def get_status():
    if not _monitor:
        return {"running": False, "sensors": [], "total": 0}
    return _monitor.status()


@app.get("/detections")
async def get_detections(limit: int = 100):
    if not _monitor:
        return []
    return _monitor._store.detections(limit)


@app.delete("/detections")
async def clear_detections():
    if _monitor:
        _monitor._store.clear()
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            if _monitor:
                payload = _monitor.status()
                payload["recent"] = _monitor._store.detections(20)
            else:
                payload = {"running": False, "sensors": [], "total": 0, "recent": []}
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(2.0)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        logger.debug("ws error: %s", exc)
