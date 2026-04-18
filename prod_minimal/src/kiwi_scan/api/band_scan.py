from __future__ import annotations

from datetime import datetime
from typing import Dict

from fastapi import APIRouter, Request

from .decodes import publish_decode


def make_router(*, mgr: object, band_scanner: object) -> APIRouter:
	"""Create router for band scan endpoints.

	Extracted from server.py to keep the HTTP layer modular.
	"""

	router = APIRouter()

	@router.post("/band_scan")
	async def start_band_scan(request: Request):
		payload = await request.json() if request is not None else {}
		band = str(payload.get("band") or "20m")
		rx_chan = payload.get("rx_chan", 0)
		wf_rx_chan = payload.get("wf_rx_chan", 0)
		span_hz = payload.get("span_hz", 30000.0)
		step_hz = payload.get("step_hz", None)
		max_frames = payload.get("max_frames", 10)
		record_seconds = payload.get("record_seconds", 6)
		record_hits = bool(payload.get("record_hits", True))
		detector = payload.get("detector", "waterfall")
		ssb_probe_only = bool(payload.get("ssb_probe_only", True))
		required_hits = payload.get("required_hits", None)
		probe_freqs_mhz_raw = payload.get("probe_freqs_mhz")
		probe_freqs_mhz = None
		if isinstance(probe_freqs_mhz_raw, list):
			vals = []
			for value in probe_freqs_mhz_raw:
				try:
					mhz = float(value)
				except Exception:
					continue
				if mhz > 0:
					vals.append(mhz)
			if vals:
				probe_freqs_mhz = vals
		allow_rx_fallback = bool(payload.get("allow_rx_fallback", True))
		session_id = payload.get("session_id", None)
		detector_key = str(detector or "waterfall").strip().lower()
		if detector_key in {"ssb", "phone", "voice"}:
			try:
				rx = int(rx_chan) if rx_chan is not None else 0
			except Exception:
				rx = 0
			if rx not in {0, 1}:
				rx = 0
			rx_chan = rx
			wf_rx_chan = rx
			allow_rx_fallback = False

		with mgr.lock:  # type: ignore[attr-defined]
			host = str(mgr.host)  # type: ignore[attr-defined]
			port = int(mgr.port)  # type: ignore[attr-defined]
			password = mgr.password if hasattr(mgr, "password") else None  # type: ignore[attr-defined]
			threshold_db = float(mgr.threshold_db_by_band.get(band, mgr.threshold_db))  # type: ignore[attr-defined]

		def _emit_scan_hit(hit: Dict) -> None:
			try:
				if str(hit.get("detector") or "").lower() not in {"ssb", "phone", "voice"}:
					return
				freq_mhz = hit.get("freq_mhz")
				if freq_mhz is None:
					return
				rel_db = hit.get("rel_db")
				rel_txt = f" {float(rel_db):+.1f} dB" if rel_db is not None else ""
				msg = f"SCAN SSB {float(freq_mhz):.4f} MHz{rel_txt}".strip()
				ts_str = datetime.now().astimezone().strftime("%H:%M:%S")
				publish_decode(
					{
						"timestamp": ts_str,
						"frequency_mhz": round(float(freq_mhz), 3),
						"mode": "SSB",
						"callsign": None,
						"grid": "----",
						"message": msg,
						"band": hit.get("band") or band,
						"rx": None,
					}
				)
			except Exception:
				pass

		return band_scanner.start(  # type: ignore[attr-defined]
			band=band,
			host=host,
			port=port,
			password=password,
			user=f"Band Scanning {band}",
			threshold_db=threshold_db,
			rx_chan=int(rx_chan) if rx_chan is not None else None,
			wf_rx_chan=int(wf_rx_chan) if wf_rx_chan is not None else None,
			span_hz=float(span_hz),
			step_hz=float(step_hz) if step_hz is not None else None,
			max_frames=int(max_frames),
			record_seconds=int(record_seconds),
			record_hits=record_hits,
			detector=str(detector) if detector is not None else "waterfall",
			ssb_probe_only=ssb_probe_only,
			required_hits=int(required_hits) if required_hits is not None else None,
			probe_freqs_mhz=probe_freqs_mhz,
			allow_rx_fallback=allow_rx_fallback,
			on_hit=_emit_scan_hit,
			session_id=str(session_id) if session_id else None,
		)

	@router.get("/band_scan/status")
	def band_scan_status():
		return band_scanner.status()  # type: ignore[attr-defined]

	@router.get("/band_scan/results")
	def band_scan_results():
		return band_scanner.results()  # type: ignore[attr-defined]

	@router.post("/band_scan/stop")
	def stop_band_scan():
		return band_scanner.stop()  # type: ignore[attr-defined]

	return router
