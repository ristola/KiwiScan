#!/usr/bin/env python3
"""
P25 Band Scanner
Sweeps 700 MHz (769-775) and 800 MHz (851-869) public safety bands
via an rtl_tcp source, identifies signal peaks, and serves an HTML report.

Usage: python scanner.py   (then open http://sigmon:8099)
       RTL_HOST / RTL_PORT / SERVE_PORT override via environment.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────

RTL_HOST    = os.getenv("RTL_HOST",   "10.13.73.201")
RTL_PORT    = int(os.getenv("RTL_PORT",   "17373"))
SERVE_PORT  = int(os.getenv("SERVE_PORT", "8099"))
GAIN_TENTH  = int(os.getenv("GAIN_TENTH", "350"))   # 35.0 dB
SAMPLE_RATE = 1_024_000

# Frequency bands to scan
BANDS: list[tuple[str, int, int]] = [
    ("700 MHz PS", 769_000_000, 775_000_000),
    ("800 MHz PS", 851_000_000, 869_000_000),
]

FFT_SIZE  = 16_384   # bins per FFT window
N_AVG     = 12       # FFTs averaged per center-frequency step
SETTLE_S  = 0.18     # seconds to let tuner settle after set_freq
STEP_FRAC = 0.90     # fraction of bandwidth per step (10% overlap)
STEP_HZ   = int(SAMPLE_RATE * STEP_FRAC)

PEAK_SIGMA      = 2.0      # peaks must be this many σ above mean
PEAK_MIN_SEP_HZ = 75_000   # min Hz between reported peaks

# ── rtl_tcp client ────────────────────────────────────────────────────────────

class RTLTCPClient:
    _CMD_FREQ    = 0x01
    _CMD_RATE    = 0x02
    _CMD_GAINMOD = 0x03
    _CMD_GAIN    = 0x04
    _CMD_AGC     = 0x08

    def __init__(self, host: str, port: int, timeout: float = 20.0) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(timeout)
        self._sock.connect((host, port))
        header = self._sock.recv(12)    # "RTL0" + tuner_type(4) + gain_count(4)
        if len(header) < 4 or header[:4] != b"RTL0":
            raise IOError(f"unexpected rtl_tcp header: {header!r}")
        self._sock.settimeout(None)
        print(f"  connected to rtl_tcp at {host}:{port}", flush=True)

    def _cmd(self, cmd: int, value: int) -> None:
        self._sock.sendall(struct.pack(">BI", cmd, int(value)))

    def set_freq(self, hz: int)       -> None: self._cmd(self._CMD_FREQ,    hz)
    def set_rate(self, hz: int)       -> None: self._cmd(self._CMD_RATE,    hz)
    def set_gain_mode(self, manual: int) -> None: self._cmd(self._CMD_GAINMOD, manual)
    def set_gain(self, tenth_db: int) -> None: self._cmd(self._CMD_GAIN,    tenth_db)
    def set_agc(self, on: bool)       -> None: self._cmd(self._CMD_AGC,     int(on))

    def read_iq(self, n: int) -> np.ndarray:
        need = n * 2
        buf  = bytearray(need)
        view = memoryview(buf)
        pos  = 0
        while pos < need:
            got = self._sock.recv_into(view[pos:], need - pos)
            if not got:
                raise IOError("rtl_tcp disconnected mid-read")
            pos += got
        raw = np.frombuffer(buf, dtype=np.uint8).astype(np.float32)
        raw = (raw - 127.5) / 127.5
        return raw[0::2] + 1j * raw[1::2]

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass


# ── Scanning logic ────────────────────────────────────────────────────────────

_WINDOW = np.blackman(FFT_SIZE).astype(np.float32)

def _spectrum_at(client: RTLTCPClient, center_hz: int) -> np.ndarray:
    """Return averaged power spectrum (linear) for one center frequency."""
    client.set_freq(center_hz)
    time.sleep(SETTLE_S)
    avg: np.ndarray | None = None
    for _ in range(N_AVG):
        iq   = client.read_iq(FFT_SIZE)
        spec = np.abs(np.fft.fftshift(np.fft.fft(iq * _WINDOW, FFT_SIZE))) ** 2
        avg  = spec if avg is None else avg + spec
    return avg / N_AVG   # type: ignore[return-value]


def scan_band(
    client: RTLTCPClient,
    name: str,
    f_start: int,
    f_end: int,
    on_step: object = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Sweep one band; return (freqs_hz, power_db) sorted arrays."""
    centers = list(range(f_start + STEP_HZ // 2, f_end + STEP_HZ // 2, STEP_HZ))
    all_f: list[np.ndarray] = []
    all_p: list[np.ndarray] = []

    for i, center in enumerate(centers):
        print(
            f"  [{name}] step {i+1}/{len(centers)}  center={center/1e6:.3f} MHz",
            flush=True,
        )
        if on_step:
            on_step(name, i + 1, len(centers))

        pwr_lin  = _spectrum_at(client, center)
        freq_ax  = center + np.linspace(-SAMPLE_RATE / 2, SAMPLE_RATE / 2, FFT_SIZE, endpoint=False)
        mask     = (freq_ax >= f_start) & (freq_ax < f_end)
        all_f.append(freq_ax[mask])
        all_p.append(10.0 * np.log10(pwr_lin[mask] + 1e-12))

    if not all_f:
        return np.array([]), np.array([])

    freqs  = np.concatenate(all_f)
    powers = np.concatenate(all_p)
    order  = np.argsort(freqs)
    return freqs[order], powers[order]


def find_peaks(
    freqs: np.ndarray,
    powers: np.ndarray,
    n: int = 30,
) -> list[dict]:
    if len(powers) == 0:
        return []

    threshold = float(np.mean(powers) + PEAK_SIGMA * np.std(powers))
    used      = np.zeros(len(powers), dtype=bool)
    peaks: list[dict] = []

    for _ in range(n * 4):
        masked_pwr       = np.where(used, -np.inf, powers)
        idx              = int(np.argmax(masked_pwr))
        if powers[idx] < threshold:
            break
        freq_hz          = float(freqs[idx])
        used            |= np.abs(freqs - freq_hz) < PEAK_MIN_SEP_HZ
        peaks.append({
            "freq_hz":  freq_hz,
            "freq_mhz": round(freq_hz / 1e6, 4),
            "power_db": round(float(powers[idx]), 1),
            "above_db": round(float(powers[idx]) - threshold, 1),
        })
        if len(peaks) >= n:
            break

    return sorted(peaks, key=lambda x: x["freq_mhz"])


def run_scan(on_step=None) -> dict:
    """Run the full scan; return result dict."""
    print(f"\nConnecting to rtl_tcp {RTL_HOST}:{RTL_PORT} …", flush=True)
    client = RTLTCPClient(RTL_HOST, RTL_PORT)
    client.set_rate(SAMPLE_RATE)
    client.set_gain_mode(1)    # manual gain
    client.set_gain(GAIN_TENTH)
    client.set_agc(False)
    print(f"  gain={GAIN_TENTH/10:.1f} dB  rate={SAMPLE_RATE/1e6:.3f} MS/s", flush=True)

    band_results = []
    try:
        for name, f_start, f_end in BANDS:
            print(f"\nScanning {name}  ({f_start/1e6:.1f}–{f_end/1e6:.1f} MHz) …", flush=True)
            freqs, powers = scan_band(client, name, f_start, f_end, on_step=on_step)
            peaks         = find_peaks(freqs, powers)
            band_results.append({
                "name":      name,
                "f_start":   f_start,
                "f_end":     f_end,
                "freqs_mhz": (freqs / 1e6).tolist(),
                "powers_db": powers.tolist(),
                "peaks":     peaks,
            })
            print(f"  found {len(peaks)} peaks above threshold", flush=True)
    finally:
        client.close()

    all_peaks = sorted(
        [dict(p, band=b["name"]) for b in band_results for p in b["peaks"]],
        key=lambda x: -x["power_db"],
    )

    return {
        "ts":          datetime.now(timezone.utc).isoformat(),
        "rtl":         f"{RTL_HOST}:{RTL_PORT}",
        "gain_db":     GAIN_TENTH / 10,
        "sample_rate": SAMPLE_RATE,
        "bands":       band_results,
        "all_peaks":   all_peaks,
    }


# ── HTML report ───────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>P25 Band Scan — Sigmon</title>
<style>
:root{--bg:#0d1117;--bg2:#161b22;--fg:#c9d1d9;--accent:#58a6ff;--warn:#f0a500;--ok:#3fb950;--dim:#8b949e}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--fg);font-family:'Courier New',monospace;padding:1.2rem}
h1{color:var(--accent);font-size:1.4rem;margin-bottom:.3rem}
h2{color:var(--accent);font-size:1rem;margin:1.4rem 0 .4rem;border-bottom:1px solid #30363d;padding-bottom:.3rem}
p.meta{color:var(--dim);font-size:.8rem;margin-bottom:1rem}
canvas{display:block;width:100%;height:220px;background:#0a0f16;border:1px solid #30363d;margin:.5rem 0;cursor:crosshair}
table{width:100%;border-collapse:collapse;font-size:.82rem;margin-top:.5rem}
th{background:#1c2128;color:var(--accent);padding:.4rem .7rem;text-align:left;font-weight:600}
td{padding:.35rem .7rem;border-bottom:1px solid #21262d}
tr:hover td{background:#161b22}
.hi{color:var(--warn);font-weight:700}
.note{color:var(--ok);font-style:italic;font-size:.78rem}
.dim{color:var(--dim)}
button{background:#1f6feb;color:#fff;border:none;padding:.45rem 1rem;border-radius:5px;cursor:pointer;font-family:inherit;font-size:.85rem;margin-top:1rem}
button:hover{background:#388bfd}
#status{display:inline-block;margin-left:1rem;color:var(--warn);font-size:.85rem;vertical-align:middle}
</style>
</head>
<body>
<h1>P25 Band Scan · Sigmon-2</h1>
<p class="meta">Scanned: __TS__ &nbsp;|&nbsp; RTL: __RTL__ &nbsp;|&nbsp; Gain: __GAIN__ dB &nbsp;|&nbsp; SR: __SR__ MS/s</p>

__BAND_SECTIONS__

<h2>All Peaks — Ranked by Signal Strength</h2>
<table>
<tr><th>#</th><th>Frequency (MHz)</th><th>Power (dB)</th><th>Above noise (dB)</th><th>Band</th><th>Hint</th></tr>
__PEAKS_ROWS__
</table>

<br>
<button onclick="rescan()">&#x21bb; Re-scan</button>
<span id="status"></span>

<script>
const SCAN = __JSON__;

function hint(mhz) {
  if (mhz >= 769 && mhz <= 775) return "700 MHz PS — possible P25 control/voice";
  if (mhz >= 851 && mhz <= 869) return "800 MHz rebanded PS — likely P25 control/voice";
  return "";
}

function drawSpectrum(canvasId, band) {
  const cv  = document.getElementById(canvasId);
  const ctx = cv.getContext("2d");
  cv.width  = cv.offsetWidth;
  cv.height = 220;
  const {freqs_mhz, powers_db, peaks, f_start, f_end} = band;
  const W = cv.width, H = cv.height;
  const PAD = {t:20, b:30, l:48, r:12};
  const pw = W - PAD.l - PAD.r, ph = H - PAD.t - PAD.b;

  const minF = f_start/1e6, maxF = f_end/1e6;
  const minP = Math.min(...powers_db) - 3;
  const maxP = Math.max(...powers_db) + 6;

  const fx = f => PAD.l + ((f - minF) / (maxF - minF)) * pw;
  const fy = p => PAD.t + ph - ((p - minP) / (maxP - minP)) * ph;

  // background
  ctx.fillStyle = "#0a0f16"; ctx.fillRect(0,0,W,H);

  // grid lines
  ctx.strokeStyle = "#1e2530"; ctx.lineWidth = 1;
  for (let db = Math.ceil(minP/5)*5; db <= maxP; db += 5) {
    const y = fy(db);
    ctx.beginPath(); ctx.moveTo(PAD.l, y); ctx.lineTo(W - PAD.r, y); ctx.stroke();
    ctx.fillStyle = "#555"; ctx.font = "9px monospace";
    ctx.fillText(db + "dB", 2, y + 3);
  }
  for (let f = Math.ceil(minF*10)/10; f <= maxF; f = +(f+0.5).toFixed(3)) {
    const x = fx(f);
    ctx.beginPath(); ctx.moveTo(x, PAD.t); ctx.lineTo(x, H - PAD.b); ctx.stroke();
    ctx.fillStyle = "#555"; ctx.font = "9px monospace";
    ctx.fillText(f.toFixed(1), x - 12, H - PAD.b + 12);
  }

  // spectrum fill
  ctx.beginPath(); ctx.moveTo(fx(freqs_mhz[0]), fy(minP));
  for (let i = 0; i < freqs_mhz.length; i++) ctx.lineTo(fx(freqs_mhz[i]), fy(powers_db[i]));
  ctx.lineTo(fx(freqs_mhz[freqs_mhz.length-1]), fy(minP)); ctx.closePath();
  ctx.fillStyle = "rgba(30,80,160,0.3)"; ctx.fill();

  // spectrum line
  ctx.beginPath(); ctx.strokeStyle = "#58a6ff"; ctx.lineWidth = 1.2;
  for (let i = 0; i < freqs_mhz.length; i++) {
    i === 0 ? ctx.moveTo(fx(freqs_mhz[i]), fy(powers_db[i]))
            : ctx.lineTo(fx(freqs_mhz[i]), fy(powers_db[i]));
  }
  ctx.stroke();

  // peak markers
  peaks.forEach((p, idx) => {
    const x = fx(p.freq_mhz);
    ctx.strokeStyle = "#f0a500"; ctx.lineWidth = 1;
    ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(x, PAD.t); ctx.lineTo(x, H - PAD.b); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "#f0a500"; ctx.font = "bold 9px monospace";
    ctx.fillText(p.freq_mhz.toFixed(3), x + 2, PAD.t + 12 + (idx % 3) * 12);
  });

  // crosshair + tooltip
  cv.onmousemove = e => {
    const rect = cv.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (W / rect.width);
    const mhz = minF + ((mx - PAD.l) / pw) * (maxF - minF);
    cv.title = mhz >= minF && mhz <= maxF ? mhz.toFixed(4) + " MHz" : "";
  };
}

SCAN.bands.forEach((b, i) => drawSpectrum("cv" + i, b));
window.addEventListener("resize", () => SCAN.bands.forEach((b,i) => drawSpectrum("cv"+i,b)));

async function rescan() {
  document.getElementById("status").textContent = "scanning…";
  try {
    const r = await fetch("/rescan", {method:"POST"});
    if (r.ok) {
      document.getElementById("status").textContent = "done — reloading…";
      setTimeout(() => location.reload(), 500);
    } else {
      document.getElementById("status").textContent = "scan already running";
    }
  } catch(e) {
    document.getElementById("status").textContent = "error: " + e;
  }
}
</script>
</body>
</html>
"""


def _band_section(band: dict, idx: int) -> str:
    name    = band["name"]
    f_start = band["f_start"] / 1e6
    f_end   = band["f_end"]   / 1e6
    return (
        f'<h2>{name} ({f_start:.1f}–{f_end:.1f} MHz)</h2>\n'
        f'<canvas id="cv{idx}"></canvas>\n'
    )


def _peaks_rows(all_peaks: list[dict]) -> str:
    if not all_peaks:
        return '<tr><td colspan="6" class="dim">No peaks detected above threshold.</td></tr>'
    rows = []
    for i, p in enumerate(all_peaks[:40], 1):
        mhz  = p["freq_mhz"]
        band = p.get("band", "")
        h = ""
        if 769 <= mhz <= 775:
            h = '<span class="note">700 MHz PS — P25 candidate</span>'
        elif 851 <= mhz <= 869:
            h = '<span class="note">800 MHz PS — P25 candidate</span>'
        cls = ' class="hi"' if i <= 5 else ""
        rows.append(
            f'<tr><td{cls}>{i}</td>'
            f'<td{cls}>{mhz:.4f}</td>'
            f'<td{cls}>{p["power_db"]}</td>'
            f'<td>{p["above_db"]}</td>'
            f'<td class="dim">{band}</td>'
            f'<td>{h}</td></tr>'
        )
    return "\n".join(rows)


def build_html(result: dict) -> str:
    band_sections = "\n".join(_band_section(b, i) for i, b in enumerate(result["bands"]))
    return (
        _HTML_TEMPLATE
        .replace("__TS__",            result["ts"][:19].replace("T", " ") + " UTC")
        .replace("__RTL__",           result["rtl"])
        .replace("__GAIN__",          str(result["gain_db"]))
        .replace("__SR__",            f"{result['sample_rate']/1e6:.3f}")
        .replace("__BAND_SECTIONS__", band_sections)
        .replace("__PEAKS_ROWS__",    _peaks_rows(result["all_peaks"]))
        .replace("__JSON__",          json.dumps(result, separators=(",", ":")))
    )


# ── HTTP server ───────────────────────────────────────────────────────────────

_scan_lock  = threading.Lock()
_scan_busy  = False
_html_cache: str | None = None
_scan_result: dict | None = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):   # suppress default access log
        pass

    def _send(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        global _html_cache
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            if _html_cache is None:
                body = b"<html><body style='background:#0d1117;color:#ccc;font-family:monospace;padding:2rem'>"
                body += b"<h2 style='color:#58a6ff'>Scanning in progress &hellip;</h2>"
                body += b"<p>Check back in 30 seconds or <a href='/' style='color:#58a6ff'>refresh</a>.</p></body></html>"
                self._send(200, "text/html", body)
            else:
                self._send(200, "text/html", _html_cache.encode())
        elif path == "/status":
            payload = json.dumps({"busy": _scan_busy, "has_result": _html_cache is not None}).encode()
            self._send(200, "application/json", payload)
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self) -> None:
        global _scan_busy
        path = urlparse(self.path).path
        if path == "/rescan":
            acquired = _scan_lock.acquire(blocking=False)
            if not acquired:
                self._send(409, "text/plain", b"scan already running")
                return
            _scan_busy = True
            _scan_lock.release()
            threading.Thread(target=_do_scan, daemon=True).start()
            self._send(200, "text/plain", b"scan started")
        else:
            self._send(404, "text/plain", b"not found")


def _do_scan() -> None:
    global _html_cache, _scan_result, _scan_busy
    try:
        with _scan_lock:
            result       = run_scan()
            _scan_result = result
            _html_cache  = build_html(result)
            print("\nScan complete — report ready at http://0.0.0.0:{SERVE_PORT}/", flush=True)
    except Exception as exc:
        print(f"\nScan ERROR: {exc}", file=sys.stderr, flush=True)
    finally:
        _scan_busy = False


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"P25 Band Scanner", flush=True)
    print(f"  RTL source : {RTL_HOST}:{RTL_PORT}", flush=True)
    print(f"  Gain       : {GAIN_TENTH/10:.1f} dB", flush=True)
    print(f"  Bands      : " + ", ".join(f"{n} ({s/1e6:.0f}–{e/1e6:.0f} MHz)" for n,s,e in BANDS), flush=True)
    print(f"  HTTP       : http://0.0.0.0:{SERVE_PORT}/", flush=True)
    print("", flush=True)

    # Start background scan immediately
    _scan_busy = True
    threading.Thread(target=_do_scan, daemon=True).start()

    # Start HTTP server (blocking)
    server = HTTPServer(("0.0.0.0", SERVE_PORT), Handler)
    print(f"Serving on port {SERVE_PORT} — open http://sigmon:{SERVE_PORT}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)
