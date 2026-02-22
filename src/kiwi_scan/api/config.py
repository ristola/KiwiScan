from __future__ import annotations

import concurrent.futures
import re
import socket
import time
from urllib.error import URLError
from urllib.request import urlopen
from typing import Dict

from fastapi import APIRouter, HTTPException, Request


def make_router(*, mgr: object, waterholes: Dict[str, float]) -> APIRouter:
    """Create router for GET/POST /config.

    Extracted from server.py for cleanliness; keeps behavior identical.
    """

    router = APIRouter()

    def _parse_kiwi_status(text: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in (text or "").splitlines():
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
        return out

    def _extract_gps_lat_lon(status: dict[str, str]) -> tuple[float | None, float | None]:
        gps = status.get("gps")
        if not gps:
            return None, None
        # gps=(38.594989, -78.431794)
        m = re.search(r"\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*\)", gps)
        if not m:
            return None, None
        try:
            return float(m.group(1)), float(m.group(2))
        except Exception:
            return None, None

    def _looks_like_kiwi_http(ip: str, port: int, *, timeout_s: float) -> bool:
        try:
            with urlopen(f"http://{ip}:{port}/", timeout=max(timeout_s, 0.5)) as resp:
                data = resp.read(8192)
            txt = data.decode("utf-8", errors="ignore").lower()
            return "kiwisdr" in txt or "kiwi sdr" in txt
        except Exception:
            return False

    def _read_kiwi_status(ip: str, port: int, *, timeout_s: float) -> dict[str, str] | None:
        try:
            with urlopen(f"http://{ip}:{port}/status", timeout=max(timeout_s, 0.5)) as resp:
                data = resp.read(65536)
            txt = data.decode("utf-8", errors="ignore")
            if "status=" not in txt:
                return None
            return _parse_kiwi_status(txt)
        except Exception:
            return None

    def _parse_my_kiwisdr_for_lan_hosts(html: str) -> list[tuple[str, int]]:
        # Extract RFC1918 host:port strings shown as links, e.g. 192.168.1.93:8073
        pat = re.compile(
            r"\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}"
            r"|192\.168\.(?:\d{1,3}\.)\d{1,3}"
            r"|172\.(?:1[6-9]|2\d|3[0-1])\.(?:\d{1,3}\.)\d{1,3})"
            r":(\d{1,5})\b"
        )
        out: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()
        for m in pat.finditer(html or ""):
            hostport = m.group(0)
            host, port_s = hostport.split(":", 1)
            try:
                port = int(port_s)
            except Exception:
                continue
            if port < 1 or port > 65535:
                continue
            t = (host, port)
            if t in seen:
                continue
            seen.add(t)
            out.append(t)
        return out

    @router.get("/config/discover")
    def discover_kiwi(request: Request, port: int = 8073, timeout_s: float = 0.20, max_hosts: int = 32):
        """Best-effort LAN discovery for KiwiSDR.

        Scans the caller's /24 (based on request.client.host) for the given TCP port
        and returns any hosts whose HTTP root page looks like a KiwiSDR.

        This is intentionally conservative (small timeouts, bounded results) so it
        can't hang the server.
        """

        if port < 1 or port > 65535:
            raise HTTPException(status_code=400, detail="port must be 1..65535")
        if timeout_s <= 0 or timeout_s > 2:
            raise HTTPException(status_code=400, detail="timeout_s must be > 0 and <= 2")
        if max_hosts < 1 or max_hosts > 256:
            raise HTTPException(status_code=400, detail="max_hosts must be 1..256")

        started = time.time()

        # Preferred: use my.kiwisdr.com to obtain the LAN address/port shown for this Kiwi.
        candidates: list[tuple[str, int]] = []
        source = ""
        try:
            with urlopen("http://my.kiwisdr.com/", timeout=2.0) as resp:
                html = resp.read(1024 * 1024).decode("utf-8", errors="ignore")
            candidates = _parse_my_kiwisdr_for_lan_hosts(html)
            if candidates:
                source = "my.kiwisdr.com"
        except Exception:
            candidates = []

        # Fallback: scan the caller's /24 based on request.client.host.
        if not candidates:
            client_ip = (request.client.host if request.client else "") or ""
            m = re.match(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$", client_ip)
            if not m:
                prefixes = ["192.168.1", "192.168.0", "10.0.0"]
            else:
                prefixes = [f"{m.group(1)}.{m.group(2)}.{m.group(3)}"]
            source = "lan_scan"

            def has_port_open(ip: str) -> bool:
                try:
                    with socket.create_connection((ip, port), timeout=timeout_s):
                        return True
                except OSError:
                    return False

            for prefix in prefixes:
                ips = [f"{prefix}.{i}" for i in range(1, 255)]
                with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ex:
                    for ip, ok in zip(ips, ex.map(has_port_open, ips)):
                        if ok:
                            candidates.append((ip, port))
                            if len(candidates) >= max_hosts:
                                break
                if candidates:
                    break

        found: list[dict[str, object]] = []
        for host, hp_port in candidates:
            if len(found) >= max_hosts:
                break
            if not _looks_like_kiwi_http(host, hp_port, timeout_s=timeout_s):
                continue
            st = _read_kiwi_status(host, hp_port, timeout_s=timeout_s)
            lat, lon = (None, None)
            name = None
            grid = None
            gps_good = None
            if st:
                lat, lon = _extract_gps_lat_lon(st)
                name = st.get("name")
                grid = st.get("grid")
                gps_good = st.get("gps_good")
            found.append(
                {
                    "host": host,
                    "port": hp_port,
                    "latitude": lat,
                    "longitude": lon,
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

    @router.get("/config")
    def get_config():
        kiwi_lat = None
        kiwi_lon = None
        kiwi_grid = None
        kiwi_gps_good = None
        try:
            with mgr.lock:  # type: ignore[attr-defined]
                host = str(mgr.host)
                port = int(mgr.port)
            st = _read_kiwi_status(host, port, timeout_s=0.75)
            if st:
                kiwi_lat, kiwi_lon = _extract_gps_lat_lon(st)
                kiwi_grid = st.get("grid")
                kiwi_gps_good = st.get("gps_good")
        except Exception:
            pass

        with mgr.lock:  # type: ignore[attr-defined]
            return {
                "dwell_s": mgr.dwell_s,
                "span_hz": mgr.span_hz,
                "threshold_db": mgr.threshold_db,
                "threshold_db_by_band": mgr.threshold_db_by_band,
                "fps": mgr.fps,
                "s_meter_offset_db": mgr.s_meter_offset_db,
                "latitude": mgr.latitude,
                "longitude": mgr.longitude,
                "fast_scan_enabled": mgr.fast_scan_enabled,
                "fast_scan_s_threshold": mgr.fast_scan_s_threshold,
                "fast_scan_min_frames": mgr.fast_scan_min_frames,
                "fast_scan_min_duration_s": mgr.fast_scan_min_duration_s,
                "retune_pause_s": mgr.retune_pause_s,
                "rx_chan": mgr.rx_chan,
                "host": mgr.host,
                "port": mgr.port,
                "kiwi_latitude": kiwi_lat,
                "kiwi_longitude": kiwi_lon,
                "kiwi_grid": kiwi_grid,
                "kiwi_gps_good": kiwi_gps_good,
            }

    @router.post("/config")
    async def set_config(request: Request):
        data = await request.json()
        # rx_chan is intentionally not user-configurable: let Kiwi choose.
        allowed = {
            "dwell_s",
            "span_hz",
            "threshold_db",
            "threshold_db_by_band",
            "fps",
            "host",
            "port",
            "debug",
            "s_meter_offset_db",
            "latitude",
            "longitude",
            "fast_scan_enabled",
            "fast_scan_s_threshold",
            "fast_scan_min_frames",
            "fast_scan_min_duration_s",
            "retune_pause_s",
        }
        with mgr.lock:  # type: ignore[attr-defined]
            for k, v in data.items():
                if k not in allowed:
                    continue
                try:
                    if k == "dwell_s":
                        val = float(v)
                        if val <= 0 or val > 600:
                            raise ValueError("dwell_s must be > 0 and <= 600 seconds")
                        mgr.dwell_s = val
                    elif k == "span_hz":
                        val = float(v)
                        if val <= 0 or val > 30000:
                            raise ValueError("span_hz must be > 0 and <= 30000 Hz")
                        mgr.span_hz = val
                    elif k == "threshold_db":
                        val = float(v)
                        if val < 0 or val > 60:
                            raise ValueError("threshold_db must be between 0 and 60 dB")
                        mgr.threshold_db = val
                        mgr._save_thresholds()  # type: ignore[attr-defined]
                    elif k == "threshold_db_by_band":
                        if v is None:
                            mgr.threshold_db_by_band = {}
                            mgr._save_thresholds()  # type: ignore[attr-defined]
                        elif not isinstance(v, dict):
                            raise ValueError("threshold_db_by_band must be an object mapping band->dB")
                        else:
                            new_map: Dict[str, float] = dict(mgr.threshold_db_by_band)
                            for bk, bv in v.items():
                                band = str(bk)
                                if band not in waterholes:
                                    raise ValueError(f"unknown band in threshold_db_by_band: {band}")
                                val = float(bv)
                                if val < 0 or val > 60:
                                    raise ValueError(f"threshold_db_by_band[{band}] must be between 0 and 60 dB")
                                new_map[band] = val
                            mgr.threshold_db_by_band = new_map
                            mgr._save_thresholds()  # type: ignore[attr-defined]
                    elif k == "fps":
                        val = float(v)
                        if val <= 0 or val > 10:
                            raise ValueError("fps must be > 0 and <= 10 frames/sec")
                        mgr.fps = val
                    elif k == "port":
                        val = int(v)
                        if val < 1 or val > 65535:
                            raise ValueError("port must be between 1 and 65535")
                        mgr.port = val
                    elif k == "host":
                        mgr.host = str(v)
                    elif k == "debug":
                        mgr.debug = bool(v)
                    elif k == "s_meter_offset_db":
                        val = float(v)
                        if val < -60 or val > 60:
                            raise ValueError("s_meter_offset_db must be between -60 and +60 dB")
                        mgr.s_meter_offset_db = float(val)
                    elif k == "latitude":
                        val = float(v)
                        if val < -90 or val > 90:
                            raise ValueError("latitude must be between -90 and 90")
                        mgr.latitude = float(val)
                    elif k == "longitude":
                        val = float(v)
                        if val < -180 or val > 180:
                            raise ValueError("longitude must be between -180 and 180")
                        mgr.longitude = float(val)
                    elif k == "fast_scan_enabled":
                        mgr.fast_scan_enabled = bool(v)
                    elif k == "fast_scan_s_threshold":
                        val = float(v)
                        if val < 0 or val > 25:
                            raise ValueError("fast_scan_s_threshold must be between 0 and 25")
                        mgr.fast_scan_s_threshold = float(val)
                    elif k == "fast_scan_min_frames":
                        val = int(v)
                        if val < 1 or val > 20:
                            raise ValueError("fast_scan_min_frames must be between 1 and 20")
                        mgr.fast_scan_min_frames = int(val)
                    elif k == "fast_scan_min_duration_s":
                        val = float(v)
                        if val < 0.5 or val > 10:
                            raise ValueError("fast_scan_min_duration_s must be between 0.5 and 10 seconds")
                        mgr.fast_scan_min_duration_s = float(val)
                    elif k == "retune_pause_s":
                        val = float(v)
                        if val < 0 or val > 10:
                            raise ValueError("retune_pause_s must be between 0 and 10 seconds")
                        mgr.retune_pause_s = float(val)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"invalid value for {k}: {e}")

            # Do not force an RX channel; allow the server to choose.
            mgr.rx_chan = None
            mgr._save_config()  # type: ignore[attr-defined]

        return {"ok": True}

    return router
