from __future__ import annotations

import asyncio
import copy
from datetime import datetime
import logging
import json
import os
import re
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request

from ..receiver_manager import ReceiverAssignment
from ..scheduler import block_for_hour, get_table, season_for_date
from ..auto_set_loop import _FIXED_ASSIGNMENTS
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


def _normalize_other_tasks(
    *,
    tasks: List[Dict[str, str]],
    band_modes: Dict[str, str],
    is_dual_mode: Callable[[object], bool],
    freq_for_band_mode: Callable[[str, object], Optional[float]],
) -> List[Dict[str, str]]:
    """Deduplicate (band, mode) tasks and preserve FT4/FT8 diversity for dual bands."""

    normalized: List[Dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for task in tasks:
        band = str(task.get("band") or "")
        mode = str(task.get("mode") or "FT8").strip().upper()
        pair = (band, mode)
        if pair not in seen_pairs:
            normalized.append({"band": band, "mode": mode})
            seen_pairs.add(pair)
            continue

        # If we see a duplicate mode for a dual-mode band, replace it with
        # the missing FT4/FT8 counterpart when the frequency is available.
        if not is_dual_mode(band_modes.get(band)):
            continue
        if mode not in {"FT4", "FT8"}:
            continue

        alternate = "FT4" if mode == "FT8" else "FT8"
        alt_pair = (band, alternate)
        if alt_pair in seen_pairs:
            continue
        if freq_for_band_mode(band, alternate) is None:
            continue

        normalized.append({"band": band, "mode": alternate})
        seen_pairs.add(alt_pair)

    return normalized


def _select_other_tasks_with_band_coverage(
    *,
    tasks: List[Dict[str, str]],
    capacity: int,
    preferred_band_order: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """Select tasks with a first pass that covers as many unique bands as possible.

    This keeps one receiver on each selected band before allocating extra
    slots to duplicate modes on the same band.
    """

    limit = max(0, int(capacity))
    if limit <= 0 or not tasks:
        return []

    selected: List[Dict[str, str]] = []
    selected_pairs: set[tuple[str, str]] = set()
    seen_bands: set[str] = set()

    ordered_bands: List[str] = []
    if isinstance(preferred_band_order, list):
        seen_pref: set[str] = set()
        for band in preferred_band_order:
            band_text = str(band or "")
            if not band_text or band_text in seen_pref:
                continue
            ordered_bands.append(band_text)
            seen_pref.add(band_text)

    if ordered_bands:
        for band in ordered_bands:
            for task in tasks:
                task_band = str(task.get("band") or "")
                mode = str(task.get("mode") or "").strip().upper()
                pair = (task_band, mode)
                if task_band != band or not mode or task_band in seen_bands:
                    continue
                selected.append(task)
                selected_pairs.add(pair)
                seen_bands.add(task_band)
                break
            if len(selected) >= limit:
                return selected
    else:
        for task in tasks:
            band = str(task.get("band") or "")
            mode = str(task.get("mode") or "").strip().upper()
            pair = (band, mode)
            if not band or not mode or band in seen_bands:
                continue
            selected.append(task)
            selected_pairs.add(pair)
            seen_bands.add(band)
            if len(selected) >= limit:
                return selected

    for task in tasks:
        if len(selected) >= limit:
            break
        band = str(task.get("band") or "")
        mode = str(task.get("mode") or "").strip().upper()
        pair = (band, mode)
        if not band or not mode or pair in selected_pairs:
            continue
        selected.append(task)
        selected_pairs.add(pair)

    return selected


def make_router(
    *,
    mgr: object,
    receiver_mgr: object,
    band_order: List[str],
    band_freqs_hz: Dict[str, float],
    band_ft4_freqs_hz: Dict[str, float],
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

    def _max_auto_receivers() -> int:
        raw = str(os.environ.get("KIWISCAN_AUTOSET_MAX_RX", "8") or "8").strip()
        try:
            value = int(raw)
        except Exception:
            value = 8
        return max(2, min(8, value))
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
        import logging
        import json
        logger = logging.getLogger(__name__)
        nonlocal _last_apply_signature, _last_apply_response, _last_apply_ts
        payload = await request.json()
        logger.info(f"auto_set_receivers invoked with payload: {json.dumps(payload)}")
        enabled = bool(payload.get("enabled", False))
        requested_mode = str(payload.get("mode", "ft8")).strip().lower()
        if requested_mode != "ft8":
            raise HTTPException(status_code=400, detail="mode must be 'ft8'")
        mode = "ft8"

        local_dt = datetime.now().astimezone()
        season = season_for_date(local_dt)
        try:
            table = get_table(season, "ft8")
        except KeyError as e:
            raise HTTPException(status_code=400, detail=str(e))

        block = str(payload.get("block") or "").strip()
        if not block or block not in table.blocks:
            block = block_for_hour(local_dt.hour, mode="ft8")

        settings = _load_automation_settings()

        def _sanitize_band_modes(source: object) -> Dict[str, str]:
            out: Dict[str, str] = {}
            if not isinstance(source, dict):
                return out
            for band in band_order:
                if band not in source:
                    continue
                val = str(source.get(band) or "FT8").strip().upper()
                if val not in {"FT8", "FT4", "FT4 / FT8", "FT4 / FT8 / WSPR", "FT4 / WSPR", "WSPR"}:
                    val = "FT8"
                out[band] = val
            return out

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
            return selected, _sanitize_band_modes(entry.get("bandModes"))

        selected_bands = payload.get("selected_bands")
        selected_set = set(selected_bands) if isinstance(selected_bands, list) else None
        band_modes = _sanitize_band_modes(payload.get("band_modes"))
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

        # Empirically-closed bands supplied by SmartScheduler (via AutoSetLoop).
        # These override the static schedule so receivers aren't wasted on dead bands.
        closed_bands_raw = payload.get("closed_bands")
        empirical_closed: set[str] = set()
        if isinstance(closed_bands_raw, list):
            empirical_closed = {str(b) for b in closed_bands_raw if b}

        fixed_rx_slots: set[int] = set()
        fixed_assignments_list: List[Dict[str, object]] = []
        fixed_band_set: set[str] = set()
        raw_fixed = payload.get("fixed_assignments")
        # When fixedModeEnabled is active in automation settings, inject the
        # canonical fixed-RX assignments regardless of what the caller sent.
        if isinstance(settings, dict) and bool(settings.get("fixedModeEnabled", False)):
            raw_fixed = list(_FIXED_ASSIGNMENTS)
        max_total_rx = _max_auto_receivers()

        if isinstance(raw_fixed, list):
            for entry in raw_fixed:
                if not isinstance(entry, dict):
                    continue
                try:
                    rx_f = int(entry.get("rx", -1))
                    freq_f = float(entry.get("freq_hz", 0))
                    band_f = str(entry.get("band") or "").strip()
                    mode_f = str(entry.get("mode") or "FT8").strip()
                except Exception:
                    continue
                if rx_f < 0 or rx_f >= max_total_rx or freq_f <= 0 or not band_f:
                    continue
                fixed_rx_slots.add(rx_f)
                fixed_band_set.add(str(band_f).strip().lower())
                fixed_assignments_list.append({"rx": rx_f, "band": band_f, "mode": mode_f, "freq_hz": freq_f})

        if selected_set is not None:
            desired_bands = [b for b in band_order if b in selected_set and b not in empirical_closed]
        else:
            desired_bands = [b for b in open_bands if b not in empirical_closed]
        if fixed_band_set:
            desired_bands = [b for b in desired_bands if str(b).strip().lower() not in fixed_band_set]

        apply_signature = json.dumps(
            {
                "enabled": bool(enabled),
                "mode": str(mode),
                "block": str(block),
                "desired_bands": [str(b) for b in desired_bands],
                "band_modes": {str(k): str(v) for k, v in sorted(dict(band_modes).items())},
            },
            sort_keys=True,
            separators=(",", ":"),
        )

        force = bool(payload.get("force", False))
        if (
            not force
            and _last_apply_signature == apply_signature
            and _last_apply_response is not None
            and (time.time() - _last_apply_ts) < 15.0
        ):
            cached = copy.deepcopy(_last_apply_response)
            cached["deduped"] = True
            return cached

        with mgr.lock:  # type: ignore[attr-defined]
            host = str(mgr.host)
            port = int(mgr.port)

        if not enabled:
            # Stop RX0-RX7 processes.  Run in a thread so the event loop is
            # never blocked by apply_assignments() / _wait_for_kiwi_slots_stable_clear().
            await asyncio.to_thread(receiver_mgr.apply_assignments, host, port, {})  # type: ignore[attr-defined]
            prune_decode_buffer(set())
            response = {
                "enabled": False,
                "mode": mode,
                "season": season,
                "block": block,
                "open_bands": open_bands,
                "assignments": [],
                "ssb_max_receivers": 0,
                "other_max_receivers": _max_auto_receivers(),
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
            if raw in {"wspr"}:
                return "wspr"
            return "ft8"

        def _is_dual_mode(value: object) -> bool:
            raw = str(value or "").strip().lower()
            return "ft4" in raw and "ft8" in raw and "wspr" not in raw and raw not in {"ft4", "ft8"}

        def _is_triple_mode(value: object) -> bool:
            raw = str(value or "").strip().lower()
            return "ft4" in raw and "ft8" in raw and "wspr" in raw

        def _is_ft4_wspr_mode(value: object) -> bool:
            raw = str(value or "").strip().lower().replace(" ", "").replace("/", "")
            return raw == "ft4wspr"

        def _freq_for_band_mode(band: str, mode_label: object) -> Optional[float]:
            # For IQ triple-mode return the centre of (ft8, ft4, wspr) span.
            if _is_triple_mode(mode_label):
                ft8_hz = band_freqs_hz.get(band)
                ft4_hz = band_ft4_freqs_hz.get(band)
                wspr_hz = band_wspr_freqs_hz.get(band)
                if ft8_hz and ft4_hz and wspr_hz:
                    freqs = [float(ft8_hz), float(ft4_hz), float(wspr_hz)]
                    return (min(freqs) + max(freqs)) / 2.0
                return band_freqs_hz.get(band)
            # For IQ dual-mode return the midpoint of FT8 and FT4 dials.
            if _is_dual_mode(mode_label):
                ft8_hz = band_freqs_hz.get(band)
                ft4_hz = band_ft4_freqs_hz.get(band)
                if ft8_hz and ft4_hz:
                    return (float(ft8_hz) + float(ft4_hz)) / 2.0
                return band_freqs_hz.get(band)
            # For FT4+WSPR IQ dual-mode return the midpoint of FT4 and WSPR dials.
            if _is_ft4_wspr_mode(mode_label):
                ft4_hz = band_ft4_freqs_hz.get(band)
                wspr_hz = band_wspr_freqs_hz.get(band)
                if ft4_hz and wspr_hz:
                    return (float(ft4_hz) + float(wspr_hz)) / 2.0
                return band_wspr_freqs_hz.get(band)
            norm = _normalize_mode(mode_label)
            if norm == "wspr":
                return band_wspr_freqs_hz.get(band)
            if norm == "ft4":
                return band_ft4_freqs_hz.get(band) or band_freqs_hz.get(band)
            return band_freqs_hz.get(band)

        ordered_bands = [b for b in band_order if b in desired_bands]
        tasks: List[Dict[str, str]] = []
        skipped_ssb_due_to_wspr = 0
        for band in ordered_bands:
            mode_label = str(band_modes.get(band) or "FT8")
            if _is_triple_mode(mode_label):
                # Check all three dial frequencies fit within the 12 kHz IQ window.
                ft8_hz = band_freqs_hz.get(band)
                ft4_hz = band_ft4_freqs_hz.get(band)
                wspr_hz = band_wspr_freqs_hz.get(band)
                if ft8_hz and ft4_hz and wspr_hz:
                    freqs = [float(ft8_hz), float(ft4_hz), float(wspr_hz)]
                    if max(freqs) - min(freqs) <= 10_000:
                        tasks.append({"band": str(band), "mode": "FT4 / FT8 / WSPR"})
                    else:
                        # Too wide: fall back to FT8+FT4 dual if possible, else single
                        if abs(float(ft8_hz) - float(ft4_hz)) <= 10_000:
                            tasks.append({"band": str(band), "mode": "FT4 / FT8"})
                        else:
                            tasks.append({"band": str(band), "mode": "FT8"})
                        tasks.append({"band": str(band), "mode": "WSPR"})
                else:
                    tasks.append({"band": str(band), "mode": "FT8"})
            elif _is_dual_mode(mode_label):
                # Check if the two mode dial frequencies fit within a 12 kHz IQ window.
                ft8_hz = band_freqs_hz.get(band)
                ft4_hz = band_ft4_freqs_hz.get(band)
                if ft8_hz and ft4_hz and abs(float(ft8_hz) - float(ft4_hz)) <= 10_000:
                    # IQ-capable: one receiver decodes both FT8 and FT4 simultaneously.
                    tasks.append({"band": str(band), "mode": "FT4 / FT8"})
                else:
                    # Modes too far apart for the 12 kHz IQ window: two receivers.
                    tasks.append({"band": str(band), "mode": "FT4"})
                    tasks.append({"band": str(band), "mode": "FT8"})
            else:
                norm = _normalize_mode(mode_label)
                tasks.append({"band": str(band), "mode": norm.upper()})

        if fixed_band_set:
            tasks = [
                task for task in tasks
                if str(task.get("band") or "").strip().lower() not in fixed_band_set
            ]

        other_tasks = _sort_other_tasks_by_activity(
            tasks=tasks,
            band_order=band_order,
            blocks=table.blocks,
            block_key=block,
            local_dt=local_dt,
        )
        other_tasks = _normalize_other_tasks(
            tasks=other_tasks,
            band_modes=band_modes,
            is_dual_mode=_is_dual_mode,
            freq_for_band_mode=_freq_for_band_mode,
        )

        # Fixed assignments: pin specified RX slots directly, leaving remaining slots for roaming tasks.

        all_rx_slots = [s for s in range(max_total_rx) if s not in fixed_rx_slots]
        other_rx_request_list = list(all_rx_slots)

        desired_other_tasks = _select_other_tasks_with_band_coverage(
            tasks=other_tasks,
            capacity=len(other_rx_request_list),
            preferred_band_order=ordered_bands,
        )
        requested_ssb_tasks = 0
        requested_other_tasks = len(other_tasks)
        assigned_ssb_tasks = 0
        assigned_other_tasks = len(desired_other_tasks)
        skipped_ssb_tasks = 0
        skipped_other_tasks = max(0, requested_other_tasks - assigned_other_tasks)
        skipped_tasks = skipped_other_tasks

        task_slots: List[tuple[int, Dict[str, str]]] = []
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
            assignments[rx_request] = ReceiverAssignment(
                rx=rx_request,
                band=band,
                freq_hz=freq_hz_f,
                mode_label=mode_task,
                sideband=None,
                # Keep RX0/RX1 as strict roaming slots; only fixed RX2+ should float.
                ignore_slot_check=True if int(rx_request) >= 2 else False,
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

        # Inject fixed assignments — these pin specific RX slots bypassing the task machinery.
        for entry in fixed_assignments_list:
            rx_f = int(entry["rx"])
            band_f = str(entry["band"])
            mode_f = str(entry["mode"])
            freq_hz_f = float(entry["freq_hz"])
            assignments[rx_f] = ReceiverAssignment(
                rx=rx_f, band=band_f, freq_hz=freq_hz_f, mode_label=mode_f,
                ignore_slot_check=True,
            )
            allowed_bands.add(band_f)
            assignment_results.append({
                "rx": rx_f,
                "rx_request": rx_f,
                "band": band_f,
                "mode": mode_f,
                "freq_hz": freq_hz_f,
                "ok": True,
                "fixed": True,
            })

        try:
            if hasattr(receiver_mgr, "dependency_report") and hasattr(mgr, "set_runtime_dependencies"):
                report = receiver_mgr.dependency_report()  # type: ignore[attr-defined]
                mgr.set_runtime_dependencies(report, save=True)  # type: ignore[attr-defined]
        except Exception:
            pass

        # Run apply_assignments in a thread — it calls _wait_for_kiwi_slots_stable_clear()
        # which can block for minutes.  Awaiting to_thread keeps the event loop free.
        await asyncio.to_thread(receiver_mgr.apply_assignments, host, port, assignments)  # type: ignore[attr-defined]

        try:
            latest_settings = _load_automation_settings()
            if not isinstance(latest_settings, dict):
                latest_settings = {}
            changed = False
            if "ssbAdaptiveThresholdByBand" in latest_settings:
                del latest_settings["ssbAdaptiveThresholdByBand"]
                changed = True
            if changed:
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
            "ssb_max_receivers": 0,
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
