from __future__ import annotations

from typing import Callable, Dict

from fastapi import APIRouter, Response

from .decodes import get_decode_metrics, reset_decode_metrics


def _prom_name(name: str) -> str:
    return "kiwi_scan_" + name


def make_router(
    *,
    receiver_mgr: object,
    get_api_metrics: Callable[[], Dict[str, float | int]],
    reset_api_metrics: Callable[[], Dict[str, float | int]] | None = None,
) -> APIRouter:
    """Create router for GET /metrics (Prometheus text format)."""

    router = APIRouter()

    @router.get("/metrics")
    def get_metrics() -> Response:
        decode = get_decode_metrics()
        rx = receiver_mgr.metrics_snapshot() if hasattr(receiver_mgr, "metrics_snapshot") else {}
        health = receiver_mgr.health_summary() if hasattr(receiver_mgr, "health_summary") else {}
        api = get_api_metrics()

        lines: list[str] = []

        lines.append(f"# TYPE {_prom_name('decode_rate_60s')} gauge")
        lines.append(f"{_prom_name('decode_rate_60s')} {float(decode.get('decode_rate_per_sec_60s', 0.0)):.6f}")
        lines.append(f"# TYPE {_prom_name('decode_rate_300s')} gauge")
        lines.append(f"{_prom_name('decode_rate_300s')} {float(decode.get('decode_rate_per_sec_300s', 0.0)):.6f}")
        lines.append(f"# TYPE {_prom_name('decodes_total')} counter")
        lines.append(f"{_prom_name('decodes_total')} {int(decode.get('total_decodes', 0))}")

        lines.append(f"# TYPE {_prom_name('receiver_restarts_total')} counter")
        lines.append(f"{_prom_name('receiver_restarts_total')} {int(rx.get('restart_total', 0))}")
        restart_by_rx = rx.get("restart_by_rx") if isinstance(rx, dict) else None
        if isinstance(restart_by_rx, dict):
            lines.append(f"# TYPE {_prom_name('receiver_restarts_by_rx')} counter")
            for rx_id, value in sorted(restart_by_rx.items(), key=lambda item: str(item[0])):
                lines.append(f"{_prom_name('receiver_restarts_by_rx')}{{rx=\"{rx_id}\"}} {int(value)}")

        watchdog_state = rx.get("watchdog_state_by_rx") if isinstance(rx, dict) else None
        if isinstance(watchdog_state, dict):
            lines.append(f"# TYPE {_prom_name('receiver_watchdog_backoff_seconds')} gauge")
            lines.append(f"# TYPE {_prom_name('receiver_watchdog_consecutive_failures')} gauge")
            for rx_id, state in sorted(watchdog_state.items(), key=lambda item: str(item[0])):
                if not isinstance(state, dict):
                    continue
                backoff = float(state.get("backoff_s", 0.0) or 0.0)
                consec = int(state.get("consecutive_failures", 0) or 0)
                reason = str(state.get("reason", "unknown") or "unknown")
                lines.append(
                    f"{_prom_name('receiver_watchdog_backoff_seconds')}{{rx=\"{rx_id}\",reason=\"{reason}\"}} {backoff:.3f}"
                )
                lines.append(
                    f"{_prom_name('receiver_watchdog_consecutive_failures')}{{rx=\"{rx_id}\"}} {consec}"
                )

        lines.append(f"# TYPE {_prom_name('api_requests_total')} counter")
        lines.append(f"{_prom_name('api_requests_total')} {int(api.get('request_total', 0))}")
        lines.append(f"# TYPE {_prom_name('api_errors_total')} counter")
        lines.append(f"{_prom_name('api_errors_total')} {int(api.get('error_total', 0))}")
        lines.append(f"# TYPE {_prom_name('api_latency_avg_ms')} gauge")
        lines.append(f"{_prom_name('api_latency_avg_ms')} {float(api.get('latency_avg_ms', 0.0)):.3f}")
        lines.append(f"# TYPE {_prom_name('api_latency_p95_ms')} gauge")
        lines.append(f"{_prom_name('api_latency_p95_ms')} {float(api.get('latency_p95_ms', 0.0)):.3f}")

        overall = str(health.get("overall", "unknown") or "unknown").lower() if isinstance(health, dict) else "unknown"
        active_receivers = int(health.get("active_receivers", 0) or 0) if isinstance(health, dict) else 0
        unstable_receivers = int(health.get("unstable_receivers", 0) or 0) if isinstance(health, dict) else 0
        stalled_receivers = int(health.get("stalled_receivers", 0) or 0) if isinstance(health, dict) else 0
        silent_receivers = int(health.get("silent_receivers", 0) or 0) if isinstance(health, dict) else 0
        lines.append(f"# TYPE {_prom_name('health_active_receivers')} gauge")
        lines.append(f"{_prom_name('health_active_receivers')} {active_receivers}")
        lines.append(f"# TYPE {_prom_name('health_unstable_receivers')} gauge")
        lines.append(f"{_prom_name('health_unstable_receivers')} {unstable_receivers}")
        lines.append(f"# TYPE {_prom_name('health_stalled_receivers')} gauge")
        lines.append(f"{_prom_name('health_stalled_receivers')} {stalled_receivers}")
        lines.append(f"# TYPE {_prom_name('health_silent_receivers')} gauge")
        lines.append(f"{_prom_name('health_silent_receivers')} {silent_receivers}")
        stale_seconds = 0.0
        if isinstance(health, dict):
            raw_stale = health.get("health_stale_seconds")
            if raw_stale is not None:
                try:
                    stale_seconds = float(raw_stale)
                except Exception:
                    stale_seconds = 0.0
        lines.append(f"# TYPE {_prom_name('health_stale_seconds')} gauge")
        lines.append(f"{_prom_name('health_stale_seconds')} {stale_seconds:.3f}")
        lines.append(f"# TYPE {_prom_name('health_overall')} gauge")
        lines.append(f"{_prom_name('health_overall')}{{state=\"healthy\"}} {1 if overall == 'healthy' else 0}")
        lines.append(f"{_prom_name('health_overall')}{{state=\"quiet\"}} {1 if overall == 'quiet' else 0}")
        lines.append(f"{_prom_name('health_overall')}{{state=\"degraded\"}} {1 if overall == 'degraded' else 0}")
        lines.append(f"{_prom_name('health_overall')}{{state=\"idle\"}} {1 if overall == 'idle' else 0}")
        lines.append(f"{_prom_name('health_overall')}{{state=\"unknown\"}} {1 if overall not in {'healthy', 'quiet', 'degraded', 'idle'} else 0}")

        body = "\n".join(lines) + "\n"
        return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")

    @router.post("/metrics/reset")
    def reset_metrics() -> Dict[str, object]:
        decode_after = reset_decode_metrics()
        rx_after: Dict[str, object] = {}
        if hasattr(receiver_mgr, "reset_metrics"):
            try:
                rx_after = receiver_mgr.reset_metrics()  # type: ignore[attr-defined]
            except Exception:
                rx_after = {}
        api_after: Dict[str, float | int] = {}
        if reset_api_metrics is not None:
            try:
                api_after = reset_api_metrics()
            except Exception:
                api_after = {}
        return {
            "ok": True,
            "decode": decode_after,
            "receiver": rx_after,
            "api": api_after,
        }

    return router
