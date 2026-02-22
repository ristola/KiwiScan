# kiwi_scan

Minimal starter for a KiWi SDR scanning/monitoring utility.

## Quick start

```zsh
cd /opt/ShackMate/kiwi_scan
python3 -m venv .venv-py3
source .venv-py3/bin/activate
python -m pip install -U pip
python -m pip install -e .
kiwi-scan --help
kiwi-scan scan --help
```

## Installer (shareable URL)

Use the browser-friendly install page:
- `INSTALL.md`

Direct one-line installer:

```zsh
curl -fsSL https://raw.githubusercontent.com/ristola/KiwiScan/main/tools/install_latest.sh | bash
```

Operator shell/env reference:
- See `.env.example` for preferred `.venv-py3` activation and common run/test commands.

## Running the web UI/server

```zsh
./run_server.sh
```

Check runtime dependencies used by receiver automation (kiwirecorder/ft8modem/af2udp/sox):

```zsh
./tools/check_runtime_deps.sh
```

Optionally attempt a local build of missing `ft8modem`/`af2udp` from `../ft8modem`:

```zsh
./tools/check_runtime_deps.sh --build-missing
```

`run_server.sh` can now auto-bootstrap by default:
- Creates `.venv-py3` if missing (falls back to `.venv` if present).
- Installs required Python packages if `fastapi`/`uvicorn` are missing.
- Then starts the server.

Disable auto-bootstrap (manual mode):

```zsh
AUTO_SETUP=0 ./run_server.sh
```

Notes:
- `run_server.sh` now exits on a clean shutdown (e.g. Ctrl+C) instead of restarting forever.
- To force the old behavior, set `ALWAYS_RESTART=1`.
- To run once with no restart loop, set `NO_RESTART=1`.
- Auto-reload on code changes is enabled by default; set `AUTO_RELOAD=0` to disable.

Automation note:
- SSB scan now supports **Adaptive threshold** (Automation tab) which smooths per-band thresholding from live SNR conditions to reduce jumpy squelch behavior.

Observability note:
- A lightweight metrics endpoint is available at `/metrics` (Prometheus text format), including decode rate, receiver restart counters, and API latency gauges.
- `/metrics` also exports health gauges (`health_active_receivers`, `health_unstable_receivers`, `health_overall{state=...}`).
- `/metrics` exports `health_stale_seconds` for freshness-based alerting.

Receiver watchdog note:
- RX workers now use per-receiver restart cooldown/backoff to avoid tight crash loops; watchdog backoff/failure counts are exported in `/metrics`.
- A human-readable health summary is available at `/health/rx`.
- The web UI header shows a live RX Health badge sourced from `/health/rx`.
- Click the RX Health badge to show/hide unstable receiver details.
- The details panel includes a "Last refresh" timestamp for health data freshness.
- The details panel also shows relative age (e.g., `3s ago`) and updates it live while open.
- Health freshness warning thresholds: delayed at 15s, stale at 30s.
- Header includes a unified status strip (Server, RX Health, Decode Stream, Last Update).
- UI polish pass adds consistent controls, card-like panel surfaces, sticky header, and decode empty/loading state.
- Config tab includes a `Theme` control (`Light` / `Auto` / `Dark`) with local persistence.
- In `Auto` theme mode, `Night starts (hour)` controls when the UI switches to dark mode (local time).
- Config tab includes a `Density` control (`Normal` / `Dense`) with local persistence for compact operator layouts.
- Config tab includes a `Reset UI` button that clears UI-only preferences (e.g., theme, density mode, map filters/hints).
- Schedule tab now includes an `Info Metrics` card sourced from `/metrics` (decode rate, totals, restart count, API p95, RX health counts, stale seconds).
- Info Metrics card uses visual thresholds for quick triage: API p95 (`warn` >= 400 ms, `error` >= 1000 ms), health stale (`warn` >= 15 s, `error` >= 30 s), and RX instability (`warn/error` when unstable receivers are present).
- Info Metrics labels include hover tooltips describing each metric and are keyboard-focusable for accessibility.
- Info Metrics header includes a compact `?` help button that toggles an at-a-glance definitions panel.
- Info Metrics help panel open/closed state is persisted in browser local storage and is cleared by `Reset UI`.
- When switching away from the Schedule tab, the help panel closes temporarily; returning to Schedule restores your saved help-panel preference.
- Clicking outside the help panel closes it.
- Lightweight toast notifications confirm key UI actions (save/reset/restart/start/stop/clear) for operator feedback.
- GridSquare display now prefers live Kiwi GPS/grid from `/status` via `/config` (`kiwi_grid` or `kiwi_latitude/kiwi_longitude`) and falls back to configured latitude/longitude.
- GridSquare now re-checks `/config` shortly after startup and then periodically, so transient startup fetch failures no longer leave it blank.
- Map tab includes an `Age` filter (`All`, `15 min`, `1 hour`, `6 hours`, `24 hours`) to limit visible spots to recent activity.
- Map tab includes an `Auto-fit` button that zooms/pans to the currently visible filtered spots.
- Opening the Map tab auto-fits the viewport to currently visible filtered spots (when available).
- Map metadata shows an `Auto-fit` timestamp after each successful fit operation.
- Changing map filters clears the `Auto-fit` timestamp until the next successful fit.
- Map legend items (`WSPR`, `FT8`, `FT4`, `SSB`) act as mode selectors for display filtering.
- Map legend includes a small hint line to indicate legend items are clickable mode filters.
- The legend hint auto-hides after the first legend click and stays hidden across reloads; `Reset UI` shows it again.
- Map tab remembers `Band`, legend-selected `Modes`, and `Age` filter selections across reloads; `Reset UI` restores them to defaults.
- If the Map tab is active, `Reset UI` also performs a silent auto-fit using the reset/default filters.
- Map mode selection always keeps at least one legend mode active to avoid an empty/none mode state.
- Added an `SSB Waterfall` tab (MVP) to visualize live `SCAN SSB` detections while band scanning.
- `SSB Waterfall` supports band selection, continuous full-band `PHONE` scan start/stop, a live scan-cursor line from `/band_scan/status`, and click-to-tune (starts `rx_monitor` at clicked frequency).
- Sideband guardrails are enforced: below `10 MHz` uses `LSB`, above `10 MHz` uses `USB`, `60m` is forced to `USB`, and `30m` PHONE operation is blocked.
- `SSB Waterfall` now uses rolling percentile auto-contrast and displays live floor/median/p95/SNR stats to keep weak activity visible across changing band noise levels.
- `SSB Waterfall` includes a `Contrast` selector (`Auto` / `Fixed`) for quick visual A/B between adaptive and baseline intensity mapping.
- `SSB Waterfall` includes `Quick Scout` for a rapid strong-signal first pass (PHONE probe anchors, short frame window) before starting continuous full-band scan.
- `Quick Scout` supports `Fast`, `Balanced`, and `Deep` presets to trade scan speed for confidence before full monitoring.
- The selected `Quick Scout` preset is persisted in browser storage and restored on reload (`Reset UI` returns it to default `Balanced`).
- Hovering over SSB waterfall dots shows a tooltip with frequency, relative level, and how recently the hit was seen.
- SSB Waterfall meta now shows `Active RX` (fixed receiver or auto/fallback) from live `/band_scan/status` progress, making scan receiver behavior easy to verify without relying only on Kiwi admin page frequency lines.
- SSB Waterfall includes `Copy Status JSON` to copy the latest `/band_scan/status` snapshot for troubleshooting and sharing.
- `/band_scan/status` now includes `last_progress`, so even after a scan ends (`running=false`) you can still verify the most recent center frequency and RX selection/fallback behavior.
- SSB Waterfall meta includes `Last Scan` (time, last center frequency, and RX mode/fallback) for quick at-a-glance verification without opening JSON.
- SSB Waterfall controls include an `RX` selector (`Auto` or fixed `RX0..RX7`) used by both `Quick Scout` and full `Start PHONE Scan` requests.
- Default SSB Waterfall RX mode is `RX0` (not `Auto`) so scan activity remains pinned to a visible receiver unless you explicitly change to `Auto`.
- With fixed `RXn` selected, scanner auto-fallback is disabled for that run; fallback to `auto` only occurs when `RX` mode is explicitly set to `Auto`.
- Before starting a fixed-RX scan, UI now checks `/health/rx`; if the selected RX appears busy, it first prompts to switch to the first free RX.
- Busy preflight is advisory for stale/unstable entries, but an actively busy fixed RX now requires explicit force-override confirmation to start on that same RX.
- SSB scan sensitivity is tuned to reduce missed audible voice activity (lower voice-score threshold and fewer required hit frames in phone/SSB detector mode).
- When a fixed RX is selected, scans now briefly hold that RX between window hops so the Kiwi admin receiver list is less likely to appear idle during hop transitions.

Example:

```zsh
NO_RESTART=1 PORT=4020 ./run_server.sh
```

## Cleanup (remove generated files)

The project generates logs/results under `outputs/` and may create `detections*.jsonl`.

Dry run:

```zsh
python3 tools/cleanup.py --dry-run --outputs --detections
```

Delete outputs (optionally keep thresholds config):

```zsh
python3 tools/cleanup.py --outputs --keep-thresholds
```

Delete the local virtualenv (recreate it via Quick start):

```zsh
python3 tools/cleanup.py --venv
```

## Backup (kiwi_scan only)

Create a kiwi_scan-only ZIP in `/opt/ShackMate/backup` (excludes `.venv-py3`, `.venv`, and `outputs` by default):

```zsh
python3 tools/make_kiwi_scan_backup.py
```

## Testing

Unit tests (fast, offline):

```zsh
cd /opt/ShackMate/kiwi_scan
source .venv-py3/bin/activate

# Install test extras (preferred)
python -m pip install -e ".[test]"

PYTHONPATH=src python -m pytest -q
```

## Notes
- There is a PyPI name collision: a package named `kiwiclient` exists that is NOT the KiwiSDR client.
- This repo vendors a known-good KiwiSDR client under `kiwi_scan/vendor/kiwiclient-jks` (provides the `kiwi` package).
- `kiwi_scan` will prefer the vendored `kiwi` package automatically.

## Minimal run (one span)

```zsh
PYTHONPATH=src python3 -m kiwi_scan scan \
	--host 192.168.1.93 \
	--rx 0 \
	--center-hz 7100000 \
	--span-hz 12000 \
	--threshold-db 10 \
	--required-hits 3 \
	--jsonl detections.jsonl
```

## Live display (per-frame)

Show per-frame top peaks and an ASCII sparkline while parked on 7.154 MHz:

```zsh
PYTHONPATH=src python3 -m kiwi_scan scan \
	--host 192.168.1.93 \
	--rx 0 \
	--center-hz 7154000 \
	--span-hz 12000 \
	--show --show-top 5 \
	--sparkline --spark-width 100 \
	--max-frames 200

```

To let the server choose any available receiver (disable explicit RX tuning), add:

```zsh
PYTHONPATH=src python3 -m kiwi_scan scan --host 192.168.1.93 --no-rx --center-hz 7154000 --span-hz 12000
```

## FT8 “waterhole” scan (per band)

Probe each band’s FT8 dial frequency and report a simple activity score:

```zsh
PYTHONPATH=src python3 -m kiwi_scan ft8 \
	--host 192.168.1.93 \
	--rx 0 \
	--dwell-s 20 \
	--span-hz 3000 \
	--threshold-db 6
```

Limit to specific bands:

```zsh
PYTHONPATH=src python3 -m kiwi_scan ft8 --host 192.168.1.93 --bands 40m,20m,10m
```

Continuous monitoring to JSONL (one line per band measurement):

```zsh
PYTHONPATH=src python3 -m kiwi_scan ft8 \
	--host 192.168.1.93 \
	--rx 0 \
	--dwell-s 20 \
	--repeat 0 \
	--cycle-sleep-s 2 \
	--jsonl-out outputs/ft8_activity.jsonl \
	--quiet

tail -f outputs/ft8_activity.jsonl
```
```

## Optional recording

If `kiwirecorder.py` is on your `PATH`, you can record a short WAV when a persistent peak is detected:

```zsh
kiwi-scan scan \
	--host YOUR_KIWI_HOST \
	--center-hz 7100000 \
	--span-hz 12000 \
	--record \
	--record-seconds 30 \
	--record-mode usb \
	--record-out recordings
```

Current limitation: the first minimal build records at the scan center frequency (not the detected peak center) — next iteration will map `bin_center` to Hz within the span.

## Sweep example (40m)

Sweep 7.000–7.300 MHz in ~12 kHz chunks with overlap:

```zsh
PYTHONPATH=src python3 -m kiwi_scan sweep \
	--host 192.168.1.93 \
	--start-hz 7000000 \
	--end-hz 7300000 \
	--span-hz 12000 \
	--overlap 0.25 \
	--dwell-frames 40 \
	--threshold-db 10 \
	--required-hits 3 \
	--cache-ttl-s 120 \
	--cache-quantize-hz 25
```

To enable recording and use the vendored `kiwirecorder.py`:

```zsh
PYTHONPATH=src PATH="/opt/ShackMate/kiwi_scan/vendor/kiwiclient-jks:$PATH" python3 -m kiwi_scan sweep \
	--host 192.168.1.93 \
	--start-hz 7000000 \
	--end-hz 7300000 \
	--record \
	--record-seconds 30
```
