from __future__ import annotations

from pathlib import Path
from typing import Dict

from fastapi import APIRouter


def make_router(*, receiver_mgr: object, af2udp_path: Path, ft8modem_path: Path) -> APIRouter:
    """Create router for decode process status.

    This is a small extraction to keep server.py slimmer without changing behavior.
    """

    router = APIRouter()

    @router.get("/decodes/status")
    def get_decode_status():
        rx_status: Dict[int, Dict[str, object]] = {}
        # receiver_mgr is a ReceiverManager instance; access its assignments under lock.
        with receiver_mgr._lock:  # type: ignore[attr-defined]
            assignments = dict(receiver_mgr._assignments)  # type: ignore[attr-defined]
        for rx, a in assignments.items():
            rx_status[int(rx)] = {
                "band": a.band,
                "mode": a.mode_label,
                "freq_hz": a.freq_hz,
            }

        def _tail_lines(path: Path, max_lines: int = 10) -> list[str]:
            if not path.exists():
                return []
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                return []
            lines = [ln for ln in text.splitlines() if ln.strip()]
            return lines[-max_lines:]

        logs = []
        for rx in range(2, 8):
            dec_log = Path("/tmp") / f"ft8modem_rx{rx}.log"
            dec_log_ft4 = Path("/tmp") / f"ft8modem_rx{rx}_ft4.log"
            dec_log_ft8 = Path("/tmp") / f"ft8modem_rx{rx}_ft8.log"
            pipe_log = Path("/tmp") / f"kiwi_rx{rx}_pipeline.log"
            logs.append(
                {
                    "rx": rx,
                    "decoder_log": str(dec_log),
                    "decoder_exists": dec_log.exists(),
                    "decoder_size": dec_log.stat().st_size if dec_log.exists() else 0,
                    "decoder_tail": _tail_lines(dec_log),
                    "decoder_ft4_log": str(dec_log_ft4),
                    "decoder_ft4_exists": dec_log_ft4.exists(),
                    "decoder_ft4_size": dec_log_ft4.stat().st_size if dec_log_ft4.exists() else 0,
                    "decoder_ft4_tail": _tail_lines(dec_log_ft4),
                    "decoder_ft8_log": str(dec_log_ft8),
                    "decoder_ft8_exists": dec_log_ft8.exists(),
                    "decoder_ft8_size": dec_log_ft8.stat().st_size if dec_log_ft8.exists() else 0,
                    "decoder_ft8_tail": _tail_lines(dec_log_ft8),
                    "pipeline_log": str(pipe_log),
                    "pipeline_exists": pipe_log.exists(),
                    "pipeline_size": pipe_log.stat().st_size if pipe_log.exists() else 0,
                    "pipeline_tail": _tail_lines(pipe_log),
                }
            )

        return {
            "assignments": rx_status,
            "logs": logs,
            "af2udp": str(af2udp_path),
            "ft8modem": str(ft8modem_path),
        }

    return router
