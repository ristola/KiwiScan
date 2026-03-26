from __future__ import annotations

import copy
from datetime import datetime
import logging
import json
import re
import time
from urllib.request import urlopen
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request

from ..receiver_manager import ReceiverAssignment
from ..scheduler import block_for_hour, get_table, season_for_date
from .decodes import prune_decode_buffer


logger = logging.getLogger(__name__)

_STATUS_WEIGHT = {
    "OPEN": 3.0,
    "MARGINAL": 1.0,
    "CLOSED": 0.0,
}


def _block_sort_key(block_key: object) -> tuple[int, int] | None:
    raw = str(block_key or "").strip()
    m = re.match(r"^(\d{2})-(\d{2})$", raw)
    if not m:
        return None
    try:
        start = int(m.group(1))
        end = int(m.group(2))
    except Exception:
        return None
    if not (0 <= start <= 24 and 0 <= end <= 24):
        return None
    return start, end


def _fallback_profile_entry(by_mode: dict, block_key: str) -> dict | None:
    exact = by_mode.get(str(block_key))
    if isinstance(exact, dict):
        return exact

    target_key = _block_sort_key(block_key)
    if target_key is None:
        return None

    ordered: list[tuple[int, str, dict]] = []
    for candidate_key, candidate_entry in by_mode.items():
        if not isinstance(candidate_entry, dict):
            continue
        sort_key = _block_sort_key(candidate_key)
        if sort_key is None:
            continue
        ordered.append((sort_key[0], str(candidate_key), candidate_entry))
    if not ordered:
        return None

    ordered.sort(key=lambda item: item[0])
    target_start = target_key[0]
    prior = [entry for start, _, entry in ordered if start <= target_start]
    if prior:
        return prior[-1]
    return ordered[-1][2]


def _block_bounds(block_key: object) -> tuple[int, int] | None:
    sort_key = _block_sort_key(block_key)
    if sort_key is None:
        return None
    start, end = sort_key
    if end == 0 and start == 24:
        return 0, 24
    if end <= start:
        end += 24
    return start, end


def _ordered_blocks(blocks: Dict[str, Dict[str, str]]) -> List[str]:
    ordered: list[tuple[int, str]] = []
    for block_key in blocks.keys():
        bounds = _block_bounds(block_key)
        if bounds is None:
            continue
        ordered.append((bounds[0], str(block_key)))
    ordered.sort(key=lambda item: item[0])
    return [key for _, key in ordered]


def _adjacent_blocks(blocks: Dict[str, Dict[str, str]], block_key: str) -> tuple[str | None, str | None]:
    ordered = _ordered_blocks(blocks)
    if not ordered:
        return None, None
    try:
        idx = ordered.index(str(block_key))
    except ValueError:
        return None, None
    prev_key = ordered[idx - 1] if ordered else None
    next_key = ordered[(idx + 1) % len(ordered)] if ordered else None
    return prev_key, next_key


def _block_progress(local_dt: datetime, block_key: str) -> float:
    bounds = _block_bounds(block_key)
    if bounds is None:
        return 0.5
    start_hour, end_hour = bounds
    duration_hours = max(1.0, float(end_hour - start_hour))
    current_hour = float(local_dt.hour) + (float(local_dt.minute) / 60.0) + (float(local_dt.second) / 3600.0)
    if current_hour < start_hour:
        current_hour += 24.0
    progress = (current_hour - float(start_hour)) / duration_hours
    return max(0.0, min(1.0, progress))


def _status_weight(status: object) -> float:
    return _STATUS_WEIGHT.get(str(status or "").strip().upper(), 0.0)


def _band_activity_score(
    *,
    blocks: Dict[str, Dict[str, str]],
    block_key: str,
    band: str,
    local_dt: datetime,
) -> float:
    current_status = _status_weight(blocks.get(block_key, {}).get(band))
    prev_key, next_key = _adjacent_blocks(blocks, block_key)
    prev_status = _status_weight(blocks.get(prev_key or "", {}).get(band))
    next_status = _status_weight(blocks.get(next_key or "", {}).get(band))
    progress = _block_progress(local_dt, block_key)
    carry_score = ((1.0 - progress) * prev_status) + (progress * next_status)
    return (current_status * 100.0) + (carry_score * 10.0)


def _sort_other_tasks_by_activity(
    *,
    tasks: List[Dict[str, str]],
    band_order: List[str],
    blocks: Dict[str, Dict[str, str]],
    block_key: str,
    local_dt: datetime,
) -> List[Dict[str, str]]:
    band_positions = {band: idx for idx, band in enumerate(band_order)}

    def _task_key(task: Dict[str, str]) -> tuple[float, int]:
        band = str(task.get("band") or "")
        mode = str(task.get("mode") or "").strip().upper()
        score = _band_activity_score(blocks=blocks, block_key=block_key, band=band, local_dt=local_dt)
        if mode == "WSPR":
            score += 1000.0
        return (-score, band_positions.get(band, len(band_order)))

    return sorted(tasks, key=_task_key)


def make_router(
    *,
    mgr: object,
    receiver_mgr: object,
    band_order: List[str],
    band_freqs_hz: Dict[str, float],
    band_ft4_freqs_hz: Dict[str, float],
    band_ssb_freqs_hz: Dict[str, float],
    band_wspr_freqs_hz: Dict[str, float],
) -> APIRouter:
    """Create router for POST /auto_set_receivers.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()
    _settings_path = Path(__file__).resolve().parents[3] / "outputs" / "automation_settings.json"
    _last_apply_signature: str | None = None
    _last_apply_response: dict | None = None
    _last_apply_ts: float = 0.0

    band_ranges_khz = {
        "160m": (1800.0, 2000.0),
        "80m": (3500.0, 4000.0),
        "60m": (5250.0, 5450.0),
        "40m": (7000.0, 7300.0),
        "30m": (10100.0, 10150.0),
        "20m": (14000.0, 14350.0),
        "17m": (18068.0, 18168.0),
        "15m": (21000.0, 21450.0),
        "12m": (24890.0, 24990.0),
        "10m": (28000.0, 29700.0),
    }

    def _snr_to_threshold(value: object) -> float | None:
        try:
            snr = float(value)
        except Exception:
            return None
        # Use a conservative offset so squelch sits above noise but below strong signals.
        return max(6.0, min(40.0, snr + 10.0))

    def _fetch_snr_by_band(host: str, port: int) -> Dict[str, float]:
        url = f"http://{host}:{port}/snr"
        try:
            with urlopen(url, timeout=0.6) as resp:
                data = json.loads(resp.read(1024 * 1024).decode("utf-8", errors="ignore"))
        except Exception:
            return {}
        if not isinstance(data, list) or not data:
            return {}
        latest = data[-1]
        snr_list = latest.get("snr") if isinstance(latest, dict) else None
        if not isinstance(snr_list, list):
            return {}

        out: Dict[str, float] = {}
        for band, (lo_b, hi_b) in band_ranges_khz.items():
            best = None
            best_overlap = -1.0
            for item in snr_list:
                if not isinstance(item, dict):
                    continue
                try:
                    lo = float(item.get("lo"))
                    hi = float(item.get("hi"))
                except Exception:
                    continue
                overlap = max(0.0, min(hi, hi_b) - max(lo, lo_b))
                if overlap <= 0:
                    continue
                if overlap > best_overlap:
                    best_overlap = overlap
                    best = item
            if not best:
                continue
            threshold = _snr_to_threshold(best.get("snr"))
            if threshold is None:
                try:
                    p50 = float(best.get("p50"))
                    p95 = float(best.get("p95"))
                    threshold = _snr_to_threshold(p95 - p50)
                except Exception:
                    threshold = None
            if threshold is not None:
                out[band] = float(threshold)
        return out

    def _load_automation_settings() -> Dict[str, object]:
        try:
            if not _settings_path.exists():
                return {}
            data = json.loads(_settings_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_automation_settings(payload: Dict[str, object]) -> None:
        try:
            _settings_path.parent.mkdir(parents=True, exist_ok=True)
            _settings_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        except Exception:
            pass

    @router.post("/auto_set_receivers")
    async def auto_set_receivers(request: Request):
        nonlocal _last_apply_signature, _last_apply_response, _last_apply_ts
        payload = await request.json()
        enabled = bool(payload.get("enabled", False))
        mode = str(payload.get("mode", "ft8")).strip().lower()
        if mode not in {"ft8", "phone"}:
            raise HTTPException(status_code=400, detail="mode must be 'ft8' or 'phone'")

        ssb_scan_raw = payload.get("ssb_scan") if isinstance(payload, dict) else None
        ssb_scan_raw = ssb_scan_raw if isinstance(ssb_scan_raw, dict) else {}

        def _num(value: object, default: float, min_v: float, max_v: float) -> float:
            try:
                v = float(value)
            except Exception:
                v = float(default)
            v = max(min_v, min(max_v, v))
            return v

        ssb_scan_cfg = {
            "enabled": bool(ssb_scan_raw.get("enabled", True)),
            "threshold_db": _num(ssb_scan_raw.get("threshold_db"), 20.0, 1.0, 60.0),
            "wait_s": _num(ssb_scan_raw.get("wait_s"), 1.0, 0.1, 10.0),
            "dwell_s": _num(ssb_scan_raw.get("dwell_s"), 6.0, 1.0, 60.0),
            "tail_s": _num(ssb_scan_raw.get("tail_s"), 1.0, 0.1, 10.0),
            "step_strategy": str(ssb_scan_raw.get("step_strategy") or "adaptive").strip().lower(),
            "step_khz": _num(ssb_scan_raw.get("step_khz"), 10.0, 0.1, 20.0),
            "sideband": str(ssb_scan_raw.get("sideband") or "USB").strip().upper(),
            "adaptive_threshold": bool(ssb_scan_raw.get("adaptive_threshold", True)),
            "use_kiwi_snr": bool(ssb_scan_raw.get("use_kiwi_snr", True)),
        }

        local_dt = datetime.now().astimezone()
        season = season_for_date(local_dt)
        try:
            table = get_table(season, mode)
        except KeyError as e:
            raise HTTPException(status_code=400, detail=str(e))

        block = str(payload.get("block") or "").strip()
        if not block or block not in table.blocks:
            block = block_for_hour(local_dt.hour, mode=mode)

        settings = _load_automation_settings()

        def _profile_for(mode_key: str, block_key: str) -> tuple[Optional[List[str]], Dict[str, str]]:
            if not isinstance(settings, dict):
                return None, {}
            raw_profiles = settings.get("scheduleProfiles")
            if not isinstance(raw_profiles, dict):
                return None, {}
            by_mode = raw_profiles.get(str(mode_key).lower())
            if not isinstance(by_mode, dict):
                return None, {}
            entry = _fallback_profile_entry(by_mode, str(block_key))
            if not isinstance(entry, dict):
                return None, {}

            selected_raw = entry.get("selectedBands")
            selected: Optional[List[str]] = None
            if isinstance(selected_raw, list):
                selected = []
                seen: set[str] = set()
                for item in selected_raw:
                    band = str(item)
                    if band in band_order and band not in seen:
                        selected.append(band)
                        seen.add(band)

            band_modes_out: Dict[str, str] = {}
            band_modes_raw = entry.get("bandModes")
            if isinstance(band_modes_raw, dict):
                for band in band_order:
                    if band not in band_modes_raw:
                        continue
                    val = str(band_modes_raw.get(band) or "FT8").strip().upper()
                    if val in {"FT8", "FT4", "FT4 / FT8", "WSPR", "SSB"}:
                        band_modes_out[band] = val
                    else:
                        band_modes_out[band] = "FT8"
            return selected, band_modes_out

        selected_bands = payload.get("selected_bands")
        selected_set = set(selected_bands) if isinstance(selected_bands, list) else None
        band_modes_raw = payload.get("band_modes")
        band_modes = band_modes_raw if isinstance(band_modes_raw, dict) else {}
        wspr_scan_enabled = bool(payload.get("wspr_scan_enabled", False))
        profile_selected, profile_band_modes = _profile_for(mode, block)
        if selected_set is None and profile_selected is not None:
            selected_set = set(profile_selected)
        if not band_modes and profile_band_modes:
            band_modes = dict(profile_band_modes)

        block_data = table.blocks.get(block, {})
        open_bands = [
            b for b in band_order
            if str(block_data.get(b, "")).upper() == "OPEN"
        ]
        if selected_set is not None:
            desired_bands = [b for b in band_order if b in selected_set]
        else:
            desired_bands = list(open_bands)

        has_selected_ssb_band = False
        if desired_bands:
            for band in desired_bands:
                mode_label = str(band_modes.get(band) or "FT8").strip().upper()
                if mode_label in {"SSB", "PHONE"}:
                    has_selected_ssb_band = True
                    break

        if wspr_scan_enabled and not has_selected_ssb_band:
            hop_s_raw = payload.get("band_hop_seconds", settings.get("bandHopSeconds", 105))
            try:
                hop_s = max(10.0, float(hop_s_raw))
            except Exception:
                hop_s = 105.0
            start_band = str(payload.get("wspr_start_band") or settings.get("wsprStartBand") or "10m")

            hop_pool = [b for b in band_order if b in desired_bands]
            if not hop_pool:
                hop_pool = [b for b in band_order if b in open_bands]
            if not hop_pool:
                hop_pool = list(band_order)

            if hop_pool:
                start_idx = hop_pool.index(start_band) if start_band in hop_pool else 0
                hop_step = int(time.time() // hop_s)
                active_idx = (start_idx + hop_step) % len(hop_pool)
                active_band = hop_pool[active_idx]
                band_modes = dict(band_modes)
                band_modes[active_band] = "WSPR"

        apply_signature = json.dumps(
            {
                "enabled": bool(enabled),
                "mode": str(mode),
                "block": str(block),
                "desired_bands": [str(b) for b in desired_bands],
                "band_modes": {str(k): str(v) for k, v in sorted(dict(band_modes).items())},
                "wspr_scan_enabled": bool(wspr_scan_enabled),
                "ssb_scan_cfg": ssb_scan_cfg,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

        if (
            _last_apply_signature == apply_signature
            and _last_apply_response is not None
            and (time.time() - _last_apply_ts) < 15.0
        ):
            cached = copy.deepcopy(_last_apply_response)
            cached["deduped"] = True
            return cached

        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)
            port = int(mgr.port)

        adaptive_enabled = bool(ssb_scan_cfg.get("adaptive_threshold", True))
        snr_poll_enabled = adaptive_enabled and bool(ssb_scan_cfg.get("use_kiwi_snr", True))
        snr_thresholds = _fetch_snr_by_band(host, port) if snr_poll_enabled else {}
        adaptive_alpha = 0.35
        adaptive_min_db = 6.0
        adaptive_max_db = 40.0
        adaptive_state_raw = settings.get("ssbAdaptiveThresholdByBand") if isinstance(settings, dict) else None
        adaptive_state: Dict[str, float] = {}
        if isinstance(adaptive_state_raw, dict):
            for band_key, value in adaptive_state_raw.items():
                try:
                    adaptive_state[str(band_key)] = float(value)
                except Exception:
                    continue

        if not enabled:
            # Stop RX0-RX7 processes.
            receiver_mgr.apply_assignments(host, port, {})  # type: ignore[attr-defined]
            prune_decode_buffer(set())
            response = {
                "enabled": False,
                "mode": mode,
                "season": season,
                "block": block,
                "open_bands": open_bands,
                "assignments": [],
                "ssb_max_receivers": 2,
                "other_max_receivers": 6,
                "requested_ssb_tasks": 0,
                "requested_other_tasks": 0,
                "assigned_ssb_tasks": 0,
                "assigned_other_tasks": 0,
                "skipped_ssb_due_to_wspr": 0,
                "skipped_ssb_tasks": 0,
                "skipped_other_tasks": 0,
                "skipped_tasks": 0,
            }
            _last_apply_signature = apply_signature
            _last_apply_response = copy.deepcopy(response)
            _last_apply_ts = time.time()
            return response

        def _normalize_mode(value: object) -> str:
            raw = str(value or "").strip().lower()
            if raw in {"ft4", "ft4/ft8", "ft4-ft8", "ft4+ft8"}:
                return "ft4"
            if raw in {"ft8", "ft8/ft4", "ft8-ft4"}:
                return "ft8"
            if raw in {"ssb", "phone"}:
                return "ssb"
            if raw in {"wspr"}:
                return "wspr"
            return "ft8"

        def _is_dual_mode(value: object) -> bool:
            raw = str(value or "").strip().lower()
            return "ft4" in raw and "ft8" in raw and raw not in {"ft4", "ft8"}

        def _freq_for_band_mode(band: str, mode_label: object) -> Optional[float]:
            norm = _normalize_mode(mode_label)
            if norm == "ssb":
                return band_ssb_freqs_hz.get(band)
            if norm == "wspr":
                return band_wspr_freqs_hz.get(band)
            if norm == "ft4":
                return band_ft4_freqs_hz.get(band) or band_freqs_hz.get(band)
            return band_freqs_hz.get(band)

        ordered_bands = [b for b in band_order if b in desired_bands]
        tasks: List[Dict[str, str]] = []
        ssb_enabled = bool(ssb_scan_cfg.get("enabled", True))
        skipped_ssb_due_to_wspr = 0
        for band in ordered_bands:
            mode_label = str(band_modes.get(band) or "FT8")
            if _is_dual_mode(mode_label):
                tasks.append({"band": str(band), "mode": "FT4"})
                tasks.append({"band": str(band), "mode": "FT8"})
            else:
                norm = _normalize_mode(mode_label)
                tasks.append({"band": str(band), "mode": norm.upper()})

        ssb_tasks = [t for t in tasks if str(t.get("mode") or "").strip().upper() == "SSB"]
        other_tasks = [t for t in tasks if str(t.get("mode") or "").strip().upper() != "SSB"]
        other_tasks = _sort_other_tasks_by_activity(
            tasks=other_tasks,
            band_order=band_order,
            blocks=table.blocks,
            block_key=block,
            local_dt=local_dt,
        )

        ssb_capacity = min(2, len(ssb_tasks))
        ssb_rx_request_list = [0, 1][:ssb_capacity]
        desired_ssb_tasks = ssb_tasks[: len(ssb_rx_request_list)]
        if len(desired_ssb_tasks) > 0:
            other_rx_request_list = [2, 3, 4, 5, 6, 7]
        else:
            other_rx_request_list = [0, 1, 2, 3, 4, 5, 6, 7]

        desired_other_tasks = other_tasks[: len(other_rx_request_list)]
        requested_ssb_tasks = len(ssb_tasks)
        requested_other_tasks = len(other_tasks)
        assigned_ssb_tasks = len(desired_ssb_tasks)
        assigned_other_tasks = len(desired_other_tasks)
        skipped_ssb_tasks = max(0, requested_ssb_tasks - assigned_ssb_tasks)
        skipped_other_tasks = max(0, requested_other_tasks - assigned_other_tasks)
        skipped_tasks = skipped_ssb_tasks + skipped_other_tasks

        task_slots: List[tuple[int, Dict[str, str]]] = []
        for i, task in enumerate(desired_ssb_tasks):
            task_slots.append((int(ssb_rx_request_list[i]), task))
        for i, task in enumerate(desired_other_tasks):
            task_slots.append((int(other_rx_request_list[i]), task))

        assignments: Dict[int, ReceiverAssignment] = {}
        assignment_results: List[Dict[str, object]] = []
        allowed_bands: set[str] = set()

        for rx_request, t in task_slots:
            band = str(t.get("band") or "")
            mode_task = str(t.get("mode") or "FT8").strip().upper()
            freq_hz = _freq_for_band_mode(band, mode_task)
            if freq_hz is None:
                assignment_results.append({
                    "rx": rx_request,
                    "rx_request": rx_request,
                    "band": band,
                    "mode": mode_task,
                    "freq_hz": None,
                    "ok": False,
                })
                continue
            freq_hz_f = float(freq_hz)
            scan_cfg = None
            if mode_task == "SSB" and ssb_enabled:
                scan_cfg = dict(ssb_scan_cfg)
                target_threshold = float(scan_cfg.get("threshold_db") or 20.0)
                if band in snr_thresholds:
                    target_threshold = float(snr_thresholds[band])
                target_threshold = max(adaptive_min_db, min(adaptive_max_db, target_threshold))

                effective_threshold = target_threshold
                if adaptive_enabled:
                    prev = adaptive_state.get(band)
                    if prev is not None:
                        effective_threshold = (adaptive_alpha * target_threshold) + ((1.0 - adaptive_alpha) * float(prev))
                scan_cfg["threshold_db"] = max(adaptive_min_db, min(adaptive_max_db, float(effective_threshold)))
                adaptive_state[band] = float(scan_cfg["threshold_db"])
            assignments[rx_request] = ReceiverAssignment(
                rx=rx_request,
                band=band,
                freq_hz=freq_hz_f,
                mode_label=mode_task,
                ssb_scan=scan_cfg,
                sideband=None,
            )
            allowed_bands.add(band)
            assignment_results.append({
                "rx": rx_request,
                "rx_request": rx_request,
                "band": band,
                "mode": mode_task,
                "freq_hz": freq_hz_f,
                "ok": True,
            })

        # Final API-layer safety: force any SSB/PHONE assignment onto RX0/RX1.
        # This protects against any upstream/UI edge cases before workers start.
        ssb_slots = [0, 1]
        taken_ssb: set[int] = set()
        normalized_assignments: Dict[int, ReceiverAssignment] = {}
        remap_by_band_mode: Dict[tuple[str, str], int] = {}

        for rx in sorted(assignments.keys()):
            a = assignments[rx]
            mode_norm = str(a.mode_label or "").strip().upper()
            is_ssb = mode_norm in {"SSB", "PHONE"} or bool(a.ssb_scan)
            if not is_ssb:
                continue
            target = None
            for candidate in ssb_slots:
                if candidate not in taken_ssb:
                    target = candidate
                    break
            if target is None:
                logger.warning("Dropping extra SSB assignment in auto_set: band=%s mode=%s rx=%s", a.band, a.mode_label, rx)
                continue
            normalized_assignments[target] = ReceiverAssignment(
                rx=target,
                band=a.band,
                freq_hz=a.freq_hz,
                mode_label=a.mode_label,
                ssb_scan=a.ssb_scan,
                sideband=a.sideband,
            )
            remap_by_band_mode[(str(a.band), str(a.mode_label))] = int(target)
            taken_ssb.add(target)

        for rx in sorted(assignments.keys()):
            a = assignments[rx]
            mode_norm = str(a.mode_label or "").strip().upper()
            is_ssb = mode_norm in {"SSB", "PHONE"} or bool(a.ssb_scan)
            if is_ssb:
                continue
            if int(rx) in normalized_assignments:
                logger.warning("Dropping colliding non-SSB assignment in auto_set: band=%s mode=%s rx=%s", a.band, a.mode_label, rx)
                continue
            normalized_assignments[int(rx)] = a

        assignments = normalized_assignments

        for row in assignment_results:
            try:
                band = str(row.get("band") or "")
                mode = str(row.get("mode") or "")
                mode_norm = mode.strip().upper()
                is_ssb = mode_norm in {"SSB", "PHONE"}
                if is_ssb:
                    mapped = remap_by_band_mode.get((band, mode))
                    if mapped is not None:
                        row["rx"] = int(mapped)
                        row["rx_request"] = int(mapped)
                    else:
                        row["ok"] = False
            except Exception:
                continue

        receiver_mgr.apply_assignments(host, port, assignments)  # type: ignore[attr-defined]
        if adaptive_enabled:
            try:
                latest_settings = _load_automation_settings()
                if not isinstance(latest_settings, dict):
                    latest_settings = {}
                latest_settings["ssbAdaptiveThresholdByBand"] = {
                    str(k): round(float(v), 2)
                    for k, v in adaptive_state.items()
                }
                _save_automation_settings(latest_settings)
            except Exception:
                pass
        prune_decode_buffer(allowed_bands)
        response = {
            "enabled": True,
            "mode": mode,
            "season": season,
            "block": block,
            "open_bands": open_bands,
            "assignments": assignment_results,
            "ssb_max_receivers": len(ssb_rx_request_list),
            "other_max_receivers": len(other_rx_request_list),
            "requested_ssb_tasks": requested_ssb_tasks,
            "requested_other_tasks": requested_other_tasks,
            "assigned_ssb_tasks": assigned_ssb_tasks,
            "assigned_other_tasks": assigned_other_tasks,
            "skipped_ssb_due_to_wspr": skipped_ssb_due_to_wspr,
            "skipped_ssb_tasks": skipped_ssb_tasks,
            "skipped_other_tasks": skipped_other_tasks,
            "skipped_tasks": skipped_tasks,
        }
        _last_apply_signature = apply_signature
        _last_apply_response = copy.deepcopy(response)
        _last_apply_ts = time.time()
        return response

    return router
