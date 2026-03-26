from __future__ import annotations

import concurrent.futures
import ipaddress
import re
import socket
import time
from typing import Any
from urllib.request import urlopen

DEFAULT_KIWI_HOST = "0.0.0.0"
LEGACY_DEFAULT_KIWI_HOST = "192.168.1.93"


def is_unconfigured_kiwi_host(host: object) -> bool:
    value = str(host or "").strip().lower()
    return value in {
        "",
        DEFAULT_KIWI_HOST,
        "127.0.0.1",
        "localhost",
        LEGACY_DEFAULT_KIWI_HOST,
    }


def parse_kiwi_status(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def extract_gps_lat_lon(status: dict[str, str]) -> tuple[float | None, float | None]:
    gps = status.get("gps")
    if not gps:
        return None, None
    match = re.search(r"\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*\)", gps)
    if not match:
        return None, None
    try:
        return float(match.group(1)), float(match.group(2))
    except Exception:
        return None, None


def read_kiwi_status(host: str, port: int, *, timeout_s: float) -> dict[str, str] | None:
    try:
        with urlopen(f"http://{host}:{port}/status", timeout=max(timeout_s, 0.5)) as response:
            data = response.read(65536)
        text = data.decode("utf-8", errors="ignore")
        if "status=" not in text:
            return None
        return parse_kiwi_status(text)
    except Exception:
        return None


def _looks_like_kiwi_http(host: str, port: int, *, timeout_s: float) -> bool:
    try:
        with urlopen(f"http://{host}:{port}/", timeout=max(timeout_s, 0.5)) as response:
            data = response.read(8192)
        text = data.decode("utf-8", errors="ignore").lower()
        return "kiwisdr" in text or "kiwi sdr" in text
    except Exception:
        return False


def _parse_my_kiwisdr_for_lan_hosts(html: str) -> list[tuple[str, int]]:
    pattern = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{1,5})\b")
    out: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for match in pattern.finditer(html or ""):
        host = str(match.group(1) or "").strip()
        port_text = str(match.group(2) or "").strip()
        try:
            ip_obj = ipaddress.IPv4Address(host)
            port = int(port_text)
        except Exception:
            continue
        if not ip_obj.is_private or not (1 <= port <= 65535):
            continue
        item = (host, port)
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _private_prefixes_for_lan_scan(client_ip: str) -> list[str]:
    prefixes: list[str] = []

    def _add_prefix(ip_text: str) -> None:
        try:
            ip_obj = ipaddress.IPv4Address(str(ip_text or "").strip())
        except Exception:
            return
        if not ip_obj.is_private:
            return
        octets = str(ip_obj).split(".")
        if len(octets) != 4:
            return
        prefix = f"{octets[0]}.{octets[1]}.{octets[2]}"
        if prefix not in prefixes:
            prefixes.append(prefix)

    _add_prefix(client_ip)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            _add_prefix(sock.getsockname()[0])
    except Exception:
        pass

    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_STREAM)
        for info in infos:
            if not isinstance(info, tuple) or len(info) < 5:
                continue
            sockaddr = info[4]
            if isinstance(sockaddr, tuple) and sockaddr:
                _add_prefix(str(sockaddr[0]))
    except Exception:
        pass

    if not prefixes:
        prefixes = ["192.168.1", "192.168.0", "10.0.0", "10.0.1", "172.16.0", "172.20.0", "172.31.0"]
    return prefixes[:8]


def discover_kiwis(
    *,
    client_ip: str = "",
    port: int = 8073,
    timeout_s: float = 0.20,
    max_hosts: int = 32,
) -> dict[str, Any]:
    if port < 1 or port > 65535:
        raise ValueError("port must be 1..65535")
    if timeout_s <= 0 or timeout_s > 2:
        raise ValueError("timeout_s must be > 0 and <= 2")
    if max_hosts < 1 or max_hosts > 256:
        raise ValueError("max_hosts must be 1..256")

    started = time.time()
    candidates: list[tuple[str, int]] = []
    source = ""

    try:
        with urlopen("http://my.kiwisdr.com/", timeout=2.0) as response:
            html = response.read(1024 * 1024).decode("utf-8", errors="ignore")
        candidates = _parse_my_kiwisdr_for_lan_hosts(html)
        if candidates:
            source = "my.kiwisdr.com"
    except Exception:
        candidates = []

    if not candidates:
        prefixes = _private_prefixes_for_lan_scan(client_ip)
        source = "lan_scan"

        def has_port_open(ip: str) -> bool:
            try:
                with socket.create_connection((ip, port), timeout=timeout_s):
                    return True
            except OSError:
                return False

        for prefix in prefixes:
            ips = [f"{prefix}.{idx}" for idx in range(1, 255)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
                for ip, ok in zip(ips, executor.map(has_port_open, ips)):
                    if ok:
                        candidates.append((ip, port))
                        if len(candidates) >= max_hosts:
                            break
            if len(candidates) >= max_hosts:
                break

    found: list[dict[str, Any]] = []
    for host, candidate_port in candidates:
        if len(found) >= max_hosts:
            break
        if not _looks_like_kiwi_http(host, candidate_port, timeout_s=timeout_s):
            continue
        status = read_kiwi_status(host, candidate_port, timeout_s=timeout_s)
        latitude, longitude = (None, None)
        name = None
        grid = None
        gps_good = None
        if status:
            latitude, longitude = extract_gps_lat_lon(status)
            name = status.get("name")
            grid = status.get("grid")
            gps_good = status.get("gps_good")
        found.append(
            {
                "host": host,
                "port": candidate_port,
                "latitude": latitude,
                "longitude": longitude,
                "grid": grid,
                "gps_good": gps_good,
                "name": name,
            }
        )

    return {
        "ok": True,
        "source": source,
        "found": found,
        "elapsed_s": round(time.time() - started, 3),
    }