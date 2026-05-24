from __future__ import annotations

import concurrent.futures
from html import unescape
import ipaddress
import re
import socket
import time
from typing import Any, Iterable
from urllib.request import urlopen

DEFAULT_KIWI_HOST = "0.0.0.0"
LEGACY_DEFAULT_KIWI_HOST = "192.168.1.93"
PLACEHOLDER_KIWI_HOSTS = frozenset({
    "1.2.3.4",
})


def is_unconfigured_kiwi_host(host: object) -> bool:
    value = str(host or "").strip().lower()
    return value in {
        "",
        DEFAULT_KIWI_HOST,
        "127.0.0.1",
        "localhost",
        *PLACEHOLDER_KIWI_HOSTS,
    }


def normalize_kiwi_host(host: object) -> str:
    value = str(host or "").strip()
    if is_unconfigured_kiwi_host(value):
        return DEFAULT_KIWI_HOST
    return value


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


def _collapse_html_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", value or ""))).strip()


def _parse_my_kiwisdr_entries(html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for known_order, match in enumerate(re.finditer(r"<tr\b[^>]*>(.*?)</tr>", html or "", re.IGNORECASE | re.DOTALL)):
        row_html = match.group(1) or ""
        endpoint: tuple[str, int] | None = None
        for candidate in re.finditer(r"\b((?:\d{1,3}\.){3}\d{1,3}):(\d{1,5})\b", row_html):
            host = str(candidate.group(1) or "").strip()
            try:
                ip_obj = ipaddress.IPv4Address(host)
                port = int(candidate.group(2) or 0)
            except Exception:
                continue
            if not ip_obj.is_private or not (1 <= port <= 65535):
                continue
            endpoint = (host, port)
            break
        if not endpoint or endpoint in seen:
            continue
        seen.add(endpoint)
        row_text = _collapse_html_text(row_html)
        entry: dict[str, Any] = {
            "host": endpoint[0],
            "port": endpoint[1],
            "known_source": "my.kiwisdr.com",
            "known_order": known_order,
        }
        sw_version_match = re.search(r"Kiwi\s+v[^,<>]*,\s*Debian\s+[^\s<>]+", row_text, re.IGNORECASE)
        if sw_version_match:
            entry["sw_version"] = sw_version_match.group(0).strip()
        name_match = re.search(r"\((kiwisdr-[^)]+)\)", row_text, re.IGNORECASE)
        if name_match:
            entry["name"] = name_match.group(1).strip()
        hardware_match = re.search(r"(KiwiSDR\s+\d+\s+\([^)]+\))", row_text)
        if hardware_match:
            entry["sdr_hw"] = hardware_match.group(1).strip()
        out.append(entry)
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


def _normalize_discovery_ports(port: int, ports: Iterable[object] | None = None) -> list[int]:
    candidates = [port]
    if ports is not None:
        candidates.extend(list(ports))
    out: list[int] = []
    seen: set[int] = set()
    for candidate in candidates:
        try:
            value = int(candidate)
        except Exception as exc:
            raise ValueError("port must be 1..65535") from exc
        if value < 1 or value > 65535:
            raise ValueError("port must be 1..65535")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def sort_discovered_kiwis(entries: Iterable[object]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        host = str(item.get("host") or "").strip()
        try:
            port = int(item.get("port") or 0)
        except Exception:
            port = 0
        if not host or not (1 <= port <= 65535):
            continue
        key = (host, port)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(dict(item))

    return sorted(
        normalized,
        key=lambda entry: (
            0 if int(entry.get("port") or 0) == 8073 else 1,
            int(entry.get("port") or 0),
            str(entry.get("host") or ""),
        ),
    )


def preferred_discovered_kiwi(entries: Iterable[object]) -> dict[str, Any] | None:
    ordered = sort_discovered_kiwis(entries)
    return ordered[0] if ordered else None


def discover_kiwis(
    *,
    client_ip: str = "",
    port: int = 8073,
    ports: Iterable[object] | None = None,
    timeout_s: float = 0.20,
    max_hosts: int = 32,
) -> dict[str, Any]:
    discovery_ports = _normalize_discovery_ports(port, ports)
    if timeout_s <= 0 or timeout_s > 2:
        raise ValueError("timeout_s must be > 0 and <= 2")
    if max_hosts < 1 or max_hosts > 256:
        raise ValueError("max_hosts must be 1..256")

    started = time.time()
    candidates: list[tuple[str, int]] = []
    discovered_meta: dict[tuple[str, int], dict[str, Any]] = {}
    source = ""

    try:
        with urlopen("http://my.kiwisdr.com/", timeout=2.0) as response:
            html = response.read(1024 * 1024).decode("utf-8", errors="ignore")
        my_kiwi_entries = _parse_my_kiwisdr_entries(html)
        candidates = [(str(entry.get("host") or ""), int(entry.get("port") or 0)) for entry in my_kiwi_entries]
        discovered_meta = {
            (str(entry.get("host") or ""), int(entry.get("port") or 0)): dict(entry)
            for entry in my_kiwi_entries
            if str(entry.get("host") or "") and 1 <= int(entry.get("port") or 0) <= 65535
        }
        if not candidates:
            candidates = _parse_my_kiwisdr_for_lan_hosts(html)
        if candidates:
            source = "my.kiwisdr.com"
    except Exception:
        candidates = []
        discovered_meta = {}

    if not candidates:
        prefixes = _private_prefixes_for_lan_scan(client_ip)
        source = "lan_scan"

        def has_port_open(endpoint: tuple[str, int]) -> bool:
            ip, candidate_port = endpoint
            try:
                with socket.create_connection((ip, candidate_port), timeout=timeout_s):
                    return True
            except OSError:
                return False

        for prefix in prefixes:
            ips = [f"{prefix}.{idx}" for idx in range(1, 255)]
            endpoints = [(ip, candidate_port) for ip in ips for candidate_port in discovery_ports]
            with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
                for endpoint, ok in zip(endpoints, executor.map(has_port_open, endpoints)):
                    if ok:
                        candidates.append(endpoint)
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
        row_meta = discovered_meta.get((host, candidate_port), {})
        status = read_kiwi_status(host, candidate_port, timeout_s=timeout_s)
        latitude, longitude = (None, None)
        name = str(row_meta.get("name") or "").strip() or None
        grid = None
        gps_good = None
        sdr_hw = str(row_meta.get("sdr_hw") or "").strip() or None
        sw_version = str(row_meta.get("sw_version") or "").strip() or None
        if status:
            latitude, longitude = extract_gps_lat_lon(status)
            name = name or status.get("name")
            grid = status.get("grid")
            gps_good = status.get("gps_good")
            sdr_hw = sdr_hw or status.get("sdr_hw")
            sw_version = sw_version or status.get("sw_version")
            loc = status.get("loc")
        else:
            loc = None
        found.append(
            {
                "host": host,
                "port": candidate_port,
                "known_source": str(row_meta.get("known_source") or source or "").strip(),
                "latitude": latitude,
                "longitude": longitude,
                "grid": grid,
                "gps_good": gps_good,
                "name": name,
                "sdr_hw": sdr_hw,
                "sw_version": sw_version,
                "loc": loc,
                "known_order": row_meta.get("known_order"),
            }
        )

    found = sort_discovered_kiwis(found)

    return {
        "ok": True,
        "source": source,
        "found": found,
        "elapsed_s": round(time.time() - started, 3),
    }