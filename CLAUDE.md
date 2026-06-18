# KiwiScan — AI Context

## Project at a glance

KiwiScan is a **KiWi SDR scanning and monitoring utility** built by N4LDR (ShackMate).
It exposes a FastAPI web server on port **4020** (HTTP + WebSocket) and a legacy
WebSocket/UDP relay on port **4010**. Users interact through vanilla HTML dashboards.

Current version: **0.1.30** (see `pyproject.toml`).

---

## Repo layout

```
src/kiwi_scan/          Active source package
  api/                  FastAPI routers (one file per domain)
  static/               HTML dashboards (pro.html, monitor.html, config.html, shackmate-noc.html)
  *.py                  Core service modules (see below)

prod_minimal/           Stable production snapshot — mirrors src/kiwi_scan/ structure.
                        Kept as a known-good reference; NOT the dev target.

tests/                  pytest suite (~35 test files, one per module)
config/                 Runtime config JSON (host, port, automation_settings, etc.)
outputs/                Scan results, band scans, recordings, models
vendor/                 kiwiclient-jks, ft8modem-sm submodules
tools/                  install_latest.sh and other helper scripts
```

---

## Core modules (src/kiwi_scan/)

| Module | Purpose |
|---|---|
| `receiver_manager.py` (6 400 lines) | Central KiWi receiver orchestration — assignment, state, health |
| `receiver_scan.py` (3 300 lines) | Per-receiver scanning logic, SSB/FT8/CW mode dispatch |
| `scan.py` (2 200 lines) | Waterfall-based frequency scan engine |
| `auto_set_loop.py` (1 300 lines) | Auto-assignment loop (SEMI / FULL / MANUAL modes) |
| `net_monitor.py` (1 200 lines) | Net/frequency monitoring service |
| `smart_scheduler.py` (930 lines) | Band-condition-aware scheduling |
| `discovery_manager.py` (810 lines) | KiWi host discovery and management |
| `server.py` (800 lines) | FastAPI app wiring — imports all routers |
| `kiwi_waterfall.py` (770 lines) | Raw WebSocket waterfall client |
| `band_scanner.py` (740 lines) | Multi-band sweep orchestration |
| `discovery.py` (690 lines) | FT8 watering-hole probing (DiscoveryWorker) |
| `app_lifecycle.py` (316 lines) | FastAPI startup/shutdown handlers |
| `audio_stream.py` (586 lines) | KiWi audio streaming and recording |
| `caption_monitor.py` (650 lines) | Caption/decode monitoring service |
| `voice_mode.py` | Voice/SSB scan mode |
| `udp4010_server.py` | UDP relay on port 4010 |
| `ws4010_server.py` | WebSocket relay on port 4010 |

---

## API routers (src/kiwi_scan/api/)

Each file exports a `make_router(...)` or `router` and covers one domain:

`decodes`, `decodes_status`, `band_scan`, `caption`, `net_monitor`, `receiver_scan`,
`config`, `status`, `schedule`, `auto_set`, `ws_status` (WebSocket),
`calibrate`, `admin`, `automation`, `metrics`, `health`, `system_info`, `backup`, `ui`

---

## Frontend

Four single-file HTML dashboards in `src/kiwi_scan/static/`:

- **pro.html** — operator console (Space Grotesk + IBM Plex Mono, Leaflet map, light theme with CSS custom properties)
- **monitor.html** — band/signal monitoring view
- **config.html** — configuration UI
- **shackmate-noc.html** — NOC / network-operations-center dashboard (active development on `shackmate-noc` branch)

Conventions:
- No build step, no bundler — plain HTML/CSS/JS served directly by FastAPI static mount
- CSS custom properties on `:root` for theming (`--bg-a`, `--ink`, `--ok`, `--fault`, etc.)
- WebSocket connection to `/ws/status` for live updates
- Vanilla `fetch()` against the REST API (`/api/...`)

---

## CLI entry points (`__main__.py`)

```
kiwi-scan scan   --host HOST --center-hz HZ [options]
kiwi-scan sweep  --host HOST [options]
kiwi-scan ft8    --host HOST [options]
```

---

## Dev workflow

```zsh
# Activate venv
source .venv-py3/bin/activate

# Install in editable mode
pip install -e .

# Run web server
./run_server.sh           # wraps: uvicorn kiwi_scan.server:app --port 4020

# Run tests
pytest                    # all tests
pytest tests/test_scan.py # single file
```

See `.env.example` for environment variables.

---

## Docker

```zsh
docker run -d --name kiwiscan --pull always --restart unless-stopped --platform linux/amd64 \
  -p 4010:4010/tcp -p 4010:4010/udp -p 4020:4020 \
  -v kiwiscan-config:/opt/kiwiscan/config \
  -v kiwiscan-outputs:/opt/kiwiscan/outputs \
  n4ldr/kiwiscan:0.1.30
```

Image: `n4ldr/kiwiscan` on Docker Hub.

---

## Key configuration files (runtime, not committed)

| Path | Contents |
|---|---|
| `config/config.json` | Host, port, rx_chan, mode |
| `outputs/automation_settings.json` | Auto-set / SEMI / FULL / MANUAL mode settings |
| `outputs/band_scan_config.json` | Band scan schedule config |
| `outputs/thresholds_by_band.json` | Per-band signal thresholds |

---

## Active branch notes

- Branch `shackmate-noc` — building the ShackMate NOC dashboard (`shackmate-noc.html`)
- `prod_minimal/` is a **read-only reference snapshot** — edits go in `src/kiwi_scan/`, not `prod_minimal/`

---

## Testing conventions

- `pytest` with `pytest.ini` at root
- One test file per module: `tests/test_<module>.py`
- Integration tests hit the real FastAPI app via `httpx.AsyncClient` (no mocking the DB/service layer)
- Docker test: `prod_minimal/` has its own `run_server.sh` used in container health checks

---

## Dependencies

Core: `fastapi`, `uvicorn[standard]`, `numpy`, `faster-whisper`, `reverse_geocoder`, `pycountry`o
Test: `pytest`
Vendored: `kiwiclient-jks` (KiWi WebSocket client), `ft8modem-sm` (FT8 decode)
