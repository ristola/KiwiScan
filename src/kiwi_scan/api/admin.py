from __future__ import annotations

import json
import os
import plistlib
import re
import signal
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from ..auto_set_loop import _FIXED_ASSIGNMENTS
from ..receiver_manager import ReceiverAssignment
from ..ws4010_server import restart_ws4010


def _runtime_mode() -> str:
    requested = str(os.environ.get("KIWISCAN_UPDATE_MODE", "") or "").strip().lower()
    if requested in {"host", "container"}:
        return requested
    try:
        if Path("/.dockerenv").exists():
            return "container"
    except Exception:
        pass
    try:
        cgroup = Path("/proc/1/cgroup")
        if cgroup.exists():
            raw = cgroup.read_text(encoding="utf-8", errors="ignore").lower()
            if any(token in raw for token in ("docker", "containerd", "kubepods", "podman")):
                return "container"
    except Exception:
        pass
    return "host"


def _schedule_runtime_restart(delay_s: float = 0.15) -> dict[str, object]:
    current_pid = os.getpid()
    mode = _runtime_mode()

    def _restart() -> None:
        try:
            time.sleep(max(0.0, float(delay_s)))
            os.kill(current_pid, signal.SIGTERM)
        except Exception:
            pass

    threading.Thread(target=_restart, daemon=True, name="kiwi-runtime-restart").start()
    return {
        "ok": True,
        "mode": mode,
        "status": "restarting_container" if mode == "container" else "restarting_runtime",
        "pid": int(current_pid),
    }


def _launchd_candidates() -> list[dict[str, str]]:
    roots = [
        Path.home() / "Library" / "LaunchAgents",
        Path("/Library/LaunchAgents"),
        Path("/Library/LaunchDaemons"),
    ]
    out: dict[str, dict[str, str]] = {}
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for path in root.glob("*.plist"):
            try:
                payload = plistlib.loads(path.read_bytes())
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            label = str(payload.get("Label") or "").strip()
            program = str(payload.get("Program") or "").strip()
            program_args = payload.get("ProgramArguments")
            arg_text = ""
            if isinstance(program_args, list):
                try:
                    arg_text = " ".join(str(v) for v in program_args)
                except Exception:
                    arg_text = ""
            hay = f"{label} {program} {arg_text} {path}".lower()
            if (
                ("kiwi_scan" not in hay)
                and ("run_server.sh" not in hay)
                and ("kiwi-scan-web" not in hay)
            ):
                continue
            if label:
                out[label] = {
                    "label": label,
                    "plist": str(path),
                }
    return sorted(out.values(), key=lambda x: x.get("label") or "")


def _launchctl_disabled_labels() -> set[str]:
    disabled: set[str] = set()
    uid = os.getuid()
    domains = [f"gui/{uid}", "system"]
    pat = re.compile(r'"([^"]+)"\s*=>\s*(true|false|disabled|enabled)', re.IGNORECASE)
    for domain in domains:
        try:
            proc = subprocess.run(
                ["launchctl", "print-disabled", domain],
                capture_output=True,
                text=True,
                timeout=2.5,
                check=False,
            )
        except Exception:
            continue
        if proc.returncode != 0:
            continue
        for m in pat.finditer(proc.stdout or ""):
            label = str(m.group(1) or "").strip()
            val = str(m.group(2) or "").strip().lower()
            if label and val in {"true", "disabled"}:
                disabled.add(label)
    return disabled


def _launchd_status() -> dict[str, object]:
    candidates = _launchd_candidates()
    disabled_labels = _launchctl_disabled_labels()
    if not candidates:
        return {
            "launchd_enabled": False,
            "launchd_running": False,
            "launchd_labels": [],
        }

    enabled = False
    running = False
    labels: list[str] = []
    uid = os.getuid()

    for item in candidates:
        label = str(item.get("label") or "").strip()
        if not label:
            continue
        labels.append(label)
        is_disabled = label in disabled_labels
        if not is_disabled:
            enabled = True

        is_running = False
        for target in (f"gui/{uid}/{label}", f"system/{label}"):
            try:
                proc = subprocess.run(
                    ["launchctl", "print", target],
                    capture_output=True,
                    text=True,
                    timeout=2.5,
                    check=False,
                )
            except Exception:
                continue
            if proc.returncode == 0:
                txt = (proc.stdout or "").lower()
                if ("state = running" in txt) or ("pid =" in txt):
                    is_running = True
                    break
        if is_running:
            running = True

    return {
        "launchd_enabled": bool(enabled),
        "launchd_running": bool(running),
        "launchd_labels": labels,
    }


def _set_launchd_enabled(enabled: bool) -> dict[str, object]:
    uid = os.getuid()
    candidates = _launchd_candidates()
    labels = [str(item.get("label") or "").strip() for item in candidates if str(item.get("label") or "").strip()]
    if not labels:
        return {
            "ok": True,
            "changed": False,
            "enabled": bool(enabled),
            "labels": [],
            "updated": 0,
            "started": 0,
            "stopped": 0,
            "launchd_enabled": False,
            "launchd_running": False,
            "failed": [],
        }

    action = "enable" if enabled else "disable"
    updated = 0
    failed: list[dict[str, str]] = []

    def _scoped_targets(label: str, plist_path: str | None) -> list[tuple[str, str]]:
        gui_domain = f"gui/{uid}"
        system_domain = "system"
        plist = str(plist_path or "")
        if plist.startswith(str(Path.home() / "Library" / "LaunchAgents")):
            return [(gui_domain, f"{gui_domain}/{label}")]
        if plist.startswith("/Library/LaunchDaemons"):
            return [(system_domain, f"{system_domain}/{label}")]
        return [
            (gui_domain, f"{gui_domain}/{label}"),
            (system_domain, f"{system_domain}/{label}"),
        ]

    for item in candidates:
        label = str(item.get("label") or "").strip()
        plist = str(item.get("plist") or "").strip()
        if not label:
            continue
        scoped_targets = _scoped_targets(label, plist)
        target_updated = False
        target_failed = 0
        for _, scoped in scoped_targets:
            try:
                proc = subprocess.run(
                    ["launchctl", action, scoped],
                    capture_output=True,
                    text=True,
                    timeout=3.0,
                    check=False,
                )
            except Exception as e:
                target_failed += 1
                failed.append({"label": label, "scope": scoped, "error": str(e)})
                continue

            if proc.returncode == 0:
                target_updated = True
            else:
                target_failed += 1
                err = (proc.stderr or proc.stdout or "launchctl failed").strip()
                failed.append({"label": label, "scope": scoped, "error": err})

        if target_updated:
            updated += 1

    # When disabling, also stop any currently-running instances via bootout so
    # the service halts immediately rather than lingering until next reboot.
    stopped = 0
    if not enabled:
        for item in candidates:
            label = str(item.get("label") or "").strip()
            plist = str(item.get("plist") or "").strip()
            if not label:
                continue
            scoped_targets = _scoped_targets(label, plist)
            for domain, scoped in scoped_targets:
                try:
                    proc = subprocess.run(
                        ["launchctl", "bootout", scoped],
                        capture_output=True,
                        text=True,
                        timeout=5.0,
                        check=False,
                    )
                    if proc.returncode == 0:
                        stopped += 1
                        break
                except Exception:
                    pass

    started = 0
    if enabled:
        for item in candidates:
            label = str(item.get("label") or "").strip()
            plist = str(item.get("plist") or "").strip()
            if not label:
                continue
            scoped_targets = _scoped_targets(label, plist)
            start_ok = False
            for domain, scoped in scoped_targets:
                try:
                    loaded = subprocess.run(
                        ["launchctl", "print", scoped],
                        capture_output=True,
                        text=True,
                        timeout=3.0,
                        check=False,
                    )
                except Exception:
                    loaded = None

                needs_bootstrap = (loaded is None) or (loaded.returncode != 0)
                if needs_bootstrap and plist:
                    try:
                        subprocess.run(
                            ["launchctl", "bootstrap", domain, plist],
                            capture_output=True,
                            text=True,
                            timeout=4.0,
                            check=False,
                        )
                    except Exception:
                        pass

                try:
                    kick = subprocess.run(
                        ["launchctl", "kickstart", "-k", scoped],
                        capture_output=True,
                        text=True,
                        timeout=4.0,
                        check=False,
                    )
                except Exception as e:
                    failed.append({"label": label, "scope": scoped, "error": str(e)})
                    continue

                if kick.returncode == 0:
                    start_ok = True
                    break
                err = (kick.stderr or kick.stdout or "launchctl kickstart failed").strip()
                failed.append({"label": label, "scope": scoped, "error": err})

            if start_ok:
                started += 1

    post_status = _launchd_status()
    return {
        "ok": True,
        "changed": updated > 0,
        "enabled": bool(enabled),
        "labels": labels,
        "updated": int(updated),
        "started": int(started),
        "stopped": int(stopped),
        "launchd_enabled": post_status["launchd_enabled"],
        "launchd_running": post_status["launchd_running"],
        "failed": failed,
    }


def make_router(
    *,
    auto_set_loop: object | None = None,
    receiver_mgr: object | None = None,
    mgr: object | None = None,
) -> APIRouter:
    """Create router for admin endpoints."""

    router = APIRouter()
    semi_hold_reason = "semi_transition"

    def _build_fixed_assignments() -> dict[int, ReceiverAssignment]:
        assignments: dict[int, ReceiverAssignment] = {}
        for entry in _FIXED_ASSIGNMENTS:
            rx = int(entry["rx"])
            assignments[rx] = ReceiverAssignment(
                rx=rx,
                band=str(entry["band"]),
                freq_hz=float(entry["freq_hz"]),
                mode_label=str(entry["mode"]),
                ignore_slot_check=True,
            )
        return assignments

    def _wait_for_receiver_manager_settle(timeout_s: float = 8.0, poll_interval_s: float = 0.1) -> bool:
        startup_event = getattr(receiver_mgr, "_startup_eviction_active", None)
        if startup_event is None or not hasattr(startup_event, "is_set"):
            return True
        deadline = time.time() + max(0.0, float(timeout_s))
        while bool(startup_event.is_set()):
            if timeout_s > 0.0 and time.time() >= deadline:
                return False
            time.sleep(max(0.0, float(poll_interval_s)))
        return True

    def _wait_for_receiver_manager_lock(timeout_s: float = 8.0, poll_interval_s: float = 0.1) -> bool:
        manager_lock = getattr(receiver_mgr, "_lock", None)
        if manager_lock is None or not hasattr(manager_lock, "acquire") or not hasattr(manager_lock, "release"):
            return True

        deadline = time.time() + max(0.0, float(timeout_s))
        while True:
            acquired = False
            try:
                acquired = bool(manager_lock.acquire(blocking=False))
            except TypeError:
                acquired = bool(manager_lock.acquire(False))
            except Exception:
                return True
            if acquired:
                manager_lock.release()
                return True
            if timeout_s > 0.0 and time.time() >= deadline:
                return False
            time.sleep(max(0.0, float(poll_interval_s)))

    def _clear_reserved_receiver_workers(host: str, port: int, reserved_slots: list[int]) -> list[int]:
        stale_labels: set[str] = set()
        stale_slots: set[int] = {int(rx) for rx in reserved_slots}
        workers_to_stop: list[object] = []

        expected_aliases = getattr(receiver_mgr, "_expected_user_label_aliases", None)
        receiver_lock = getattr(receiver_mgr, "_lock", None)

        def _collect_state() -> None:
            assignments = getattr(receiver_mgr, "_assignments", None)
            workers = getattr(receiver_mgr, "_workers", None)
            activity = getattr(receiver_mgr, "_activity_by_rx", None)
            for rx in reserved_slots:
                assignment = assignments.pop(int(rx), None) if isinstance(assignments, dict) else None
                worker = workers.pop(int(rx), None) if isinstance(workers, dict) else None
                if isinstance(activity, dict):
                    activity.pop(int(rx), None)
                if assignment is not None and callable(expected_aliases):
                    try:
                        stale_labels.update(str(label).strip() for label in expected_aliases(assignment) if str(label).strip())
                    except Exception:
                        pass
                active_label = str(getattr(worker, "_active_user_label", "") or "").strip()
                if active_label:
                    stale_labels.add(active_label)
                if worker is not None:
                    workers_to_stop.append(worker)

        if receiver_lock is not None and hasattr(receiver_lock, "acquire") and hasattr(receiver_lock, "release"):
            with receiver_lock:
                _collect_state()
        else:
            _collect_state()

        stop_worker = getattr(receiver_mgr, "_stop_worker", None)
        for worker in workers_to_stop:
            if callable(stop_worker):
                stop_worker(worker, join_timeout_s=6.0)
            elif hasattr(worker, "stop"):
                worker.stop()

        wait_missing = getattr(receiver_mgr, "_wait_for_kiwi_auto_users_missing", None)
        if stale_labels and callable(wait_missing):
            wait_missing(host=host, port=port, labels=set(stale_labels), timeout_s=4.0)

        fetch_live_users = getattr(receiver_mgr, "_fetch_live_users", None)
        label_matches_any = getattr(receiver_mgr, "_label_matches_any", None)
        user_label_matches = getattr(receiver_mgr, "_user_label_matches", None)
        if callable(fetch_live_users) and stale_labels:
            try:
                live_users = fetch_live_users(host, port)
            except Exception:
                live_users = {}
            for slot, live_label in dict(live_users).items():
                matches = False
                if callable(label_matches_any):
                    try:
                        matches = bool(label_matches_any(set(stale_labels), live_label))
                    except Exception:
                        matches = False
                elif callable(user_label_matches):
                    try:
                        matches = any(bool(user_label_matches(expected, live_label)) for expected in stale_labels)
                    except Exception:
                        matches = False
                else:
                    live_text = str(live_label or "").strip().upper()
                    matches = any(str(expected or "").strip().upper() == live_text for expected in stale_labels)
                if matches:
                    stale_slots.add(int(slot))

        cleanup_labels = getattr(receiver_mgr, "_cleanup_orphan_processes_for_labels", None)
        if stale_labels and callable(cleanup_labels):
            try:
                cleanup_labels(set(stale_labels))
            except Exception:
                pass
            if callable(wait_missing):
                wait_missing(host=host, port=port, labels=set(stale_labels), timeout_s=4.0)

        return sorted(stale_slots)

    @router.post("/admin/ws4010/restart")
    def restart_ws4010_endpoint() -> dict:
        restart_ws4010()
        return {"status": "ok"}

    @router.get("/admin/ws4010/status")
    def ws4010_status_endpoint() -> dict:
        try:
            req = urllib.request.Request("http://127.0.0.1:4010/ws_status")
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8", "ignore"))
            return {
                "ok": True,
                "ws4010_clients": int(data.get("ws4010_clients", 0) or 0),
                "ws4010_total_clients": int(data.get("ws4010_total_clients", 0) or 0),
                "source": "ws4010",
            }
        except Exception:
            return {
                "ok": False,
                "ws4010_clients": 0,
                "ws4010_total_clients": 0,
                "source": "ws4010",
            }

    @router.get("/admin/headless/status")
    def headless_status_endpoint() -> dict:
        if auto_set_loop is None:
            return {
                "ok": False,
                "error": "auto_set_loop_unavailable",
            }
        try:
            status = auto_set_loop.status()  # type: ignore[attr-defined]
        except Exception:
            return {
                "ok": False,
                "error": "auto_set_loop_status_failed",
            }
        if not isinstance(status, dict):
            status = {}
        out = {"ok": True}
        out.update(status)
        out.update(_launchd_status())
        return out

    @router.post("/admin/launchd/set")
    async def launchd_set_endpoint(request: Request) -> dict:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be an object")
        enabled = bool(payload.get("enabled", False))
        return _set_launchd_enabled(enabled)

    @router.post("/admin/force-reassign")
    def force_reassign_endpoint() -> dict:
        """Re-apply the current schedule assignments immediately.

        Reads the saved automation settings and calls /auto_set_receivers with
        ``force=True``, bypassing the endpoint's dedup cache.  This kicks any
        receivers running on bands that are no longer selected and corrects Kiwi
        slot drift detected by the assignment reconcile logic.

        The reassign runs in a background thread so the response is instant.
        """
        if auto_set_loop is None:
            raise HTTPException(status_code=503, detail="auto_set_loop unavailable")
        import threading

        def _bg() -> None:
            try:
                auto_set_loop.force_reassign()  # type: ignore[attr-defined]
            except Exception:
                pass

        threading.Thread(target=_bg, daemon=True, name="force-reassign-bg").start()
        return {"ok": True, "status": "reassigning"}

    @router.post("/admin/clear-roaming-receivers")
    def clear_roaming_receivers_endpoint() -> dict:
        if receiver_mgr is None or mgr is None:
            raise HTTPException(status_code=503, detail="receiver management unavailable")
        reserved_slots = [0, 1]
        paused_external = False
        try:
            if auto_set_loop is not None:
                auto_set_loop.pause_for_external(semi_hold_reason)  # type: ignore[attr-defined]
                paused_external = True
            manager_lock = getattr(mgr, "lock", None)
            if manager_lock is not None:
                with manager_lock:  # type: ignore[union-attr]
                    host = str(getattr(mgr, "host"))
                    port = int(getattr(mgr, "port"))
            else:
                host = str(getattr(mgr, "host"))
                port = int(getattr(mgr, "port"))

            if not _wait_for_receiver_manager_settle():
                raise RuntimeError("receiver manager startup is still settling")
            if not _wait_for_receiver_manager_lock():
                raise RuntimeError("receiver manager is busy applying assignments")

            stale_slots = _clear_reserved_receiver_workers(host, port, reserved_slots)

            receiver_mgr._run_admin_kick_all(  # type: ignore[attr-defined]
                host=host,
                port=port,
                kick_only_slots=list(stale_slots),
                allow_fallback_kick_all=False,
            )
            receiver_mgr.apply_assignments(  # type: ignore[attr-defined]
                host,
                port,
                _build_fixed_assignments(),
                allow_starting_from_empty_full_reset=False,
            )
            receiver_mgr._wait_for_kiwi_slots_clear(  # type: ignore[attr-defined]
                host=host,
                port=port,
                slots=set(stale_slots),
                stable_secs=0.75,
                timeout_s=4.0,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed clearing roaming receivers: {exc}")
        finally:
            if paused_external and auto_set_loop is not None:
                try:
                    auto_set_loop.resume_from_external(semi_hold_reason)  # type: ignore[attr-defined]
                except Exception:
                    pass
        return {
            "ok": True,
            "status": "cleared",
            "reserved_receivers": reserved_slots,
        }

    @router.post("/admin/runtime/restart")
    def restart_runtime_endpoint() -> dict:
        return _schedule_runtime_restart()

    @router.post("/admin/restart-receivers")
    async def restart_receivers_endpoint(request: Request) -> dict:
        """Restart specific receiver workers by RX number without disturbing others.

        Accepts ``{"rx_list": [2, 5]}`` and calls ``_restart_receiver_worker`` for
        each listed RX.  Used by the auto_set_loop for surgical single-receiver
        recovery instead of a full reassign.
        """
        if receiver_mgr is None:
            raise HTTPException(status_code=503, detail="receiver_mgr unavailable")
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be an object")
        rx_list_raw = payload.get("rx_list", [])
        if not isinstance(rx_list_raw, list):
            raise HTTPException(status_code=400, detail="rx_list must be an array")
        rx_list: list[int] = []
        for item in rx_list_raw:
            try:
                rx_list.append(int(item))
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid rx value: {item!r}")
        results: list[dict] = []
        import threading
        import asyncio
        for rx in rx_list:
            try:
                ok = await asyncio.to_thread(
                    receiver_mgr._restart_receiver_worker,  # type: ignore[attr-defined]
                    rx, "admin_restart",
                )
                results.append({"rx": rx, "ok": bool(ok)})
            except Exception as exc:
                results.append({"rx": rx, "ok": False, "error": str(exc)})
        return {"ok": True, "results": results}

    return router
