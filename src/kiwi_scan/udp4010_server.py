from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_thread: threading.Thread | None = None
_sock: socket.socket | None = None
_stop = threading.Event()
_clients: Dict[Tuple[str, int], float] = {}


def _is_enabled() -> bool:
    raw = os.environ.get("KIWI_SCAN_UDP4010", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _port() -> int:
    try:
        return int(str(os.environ.get("KIWI_SCAN_UDP4010_PORT", "4010")).strip())
    except Exception:
        return 4010


def _client_ttl_s() -> float:
    try:
        return max(15.0, float(str(os.environ.get("KIWI_SCAN_UDP4010_CLIENT_TTL_S", "300")).strip()))
    except Exception:
        return 300.0


def _prune_clients(now: float) -> None:
    ttl = _client_ttl_s()
    stale = [addr for addr, seen in _clients.items() if (now - seen) > ttl]
    for addr in stale:
        _clients.pop(addr, None)


def _status_payload() -> bytes:
    with _lock:
        _prune_clients(time.time())
        payload = {
            "ok": True,
            "service": "kiwi_scan udp4010",
            "port": _port(),
            "clients": len(_clients),
        }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _run() -> None:
    global _sock
    port = _port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    sock.settimeout(0.5)
    with _lock:
        _sock = sock
    logger.info("UDP4010 decode publisher listening on udp://0.0.0.0:%s", port)
    try:
        while not _stop.is_set():
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                with _lock:
                    _prune_clients(time.time())
                continue
            except OSError:
                break

            now = time.time()
            with _lock:
                _clients[addr] = now
                _prune_clients(now)

            text = data.decode("utf-8", errors="ignore").strip().lower()
            if text in {"", "hello", "ping", "status", "subscribe"}:
                try:
                    sock.sendto(_status_payload(), addr)
                except Exception:
                    logger.debug("UDP4010 status reply failed", exc_info=True)
    finally:
        with _lock:
            if _sock is sock:
                _sock = None
        try:
            sock.close()
        except Exception:
            pass


def ensure_udp4010_started() -> None:
    if not _is_enabled():
        return

    global _thread
    with _lock:
        if _thread is not None and _thread.is_alive():
            return
        if _thread is not None and not _thread.is_alive():
            _thread = None
        _stop.clear()
        _thread = threading.Thread(target=_run, name="kiwi-scan-udp4010", daemon=True)
        _thread.start()

    time.sleep(0.05)


def stop_udp4010() -> None:
    global _thread
    _stop.set()
    with _lock:
        sock = _sock
    if sock is not None:
        try:
            sock.close()
        except Exception:
            pass
    if _thread is not None:
        _thread.join(timeout=2.0)
        if not _thread.is_alive():
            with _lock:
                _thread = None
                _clients.clear()


def publish_udp4010(payload: Dict) -> None:
    encoded = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    with _lock:
        sock = _sock
        if sock is None:
            return
        now = time.time()
        _prune_clients(now)
        targets = list(_clients.keys())

    dead: list[Tuple[str, int]] = []
    for target in targets:
        try:
            sock.sendto(encoded, target)
        except Exception:
            dead.append(target)

    if dead:
        with _lock:
            for target in dead:
                _clients.pop(target, None)


def udp4010_client_count() -> int:
    with _lock:
        _prune_clients(time.time())
        return len(_clients)