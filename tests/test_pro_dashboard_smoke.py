from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.ui import mount_static, router as ui_router


def _make_ui_client() -> TestClient:
    app = FastAPI()
    mount_static(app)
    app.include_router(ui_router)
    return TestClient(app)


def _assert_dashboard_core(html: str) -> None:
    required_snippets = [
        'id="receiver-scan"',
        'id="caption-monitor"',
        'id="receiver-scan-smart-start"',
        'id="receiver-scan-cw-start"',
        'id="receiver-scan-phone-start"',
        'id="band-scan-results-card"',
        'id="net-monitor"',
        'id="net-monitor-start"',
        'id="net-monitor-capture"',
        'id="receiver-scan-mode-state"',
        'id="net-monitor-mode-state"',
        'id="net-monitor-transcript-state"',
        'function applyReceiverScanStatus(scan)',
        'function applyNetMonitorStatus(status)',
        'function getPreparedScanReservedReceiverSet()',
        'const rightPanels = ["assignments", "faults", "receiver-scan", "net-monitor", "caption-monitor", "messages", "map", "active-receivers", "settings", "system"]',
        'const panelVisible = panelId === navId || ((panelId === "net-monitor" || panelId === "caption-monitor") && navId === "receiver-scan")',
        'function renderBandScanResults(results = latestBandScanResults)',
        'function syncNetMonitorControls(status = latestNetMonitorStatus)',
        'if (latestReceiverScanStatus && latestReceiverScanStatus.running && Array.isArray(latestReceiverScanStatus.reserved_receivers)) {',
        'display_name: "Reserved for Scan"',
        'getJson("/receiver_scan/status", TIMEOUT)',
        'getJson("/net_monitor/status", TIMEOUT)',
        'getJson("/band_scan/results", TIMEOUT)',
        'await postJson("/receiver_scan/start", { band: targetBand, mode: targetRole })',
        'await postJson("/net_monitor/start", {})',
        'await postJson("/net_monitor/deactivate", {})',
        'const MAP_MODE_FILTERS = ["FT8", "FT4", "WSPR", "SSB"]',
        'const digits = mode === "WSPR" ? 4 : 3;',
    ]

    forbidden_snippets = [
        'id="utility-monitor"',
        'utility-monitor-profile',
        'utility-monitor-start',
        'function applyUtilityMonitorStatus(status)',
        'function syncUtilityMonitorControls(status = latestUtilityMonitorStatus)',
        'getJson("/utility_monitor/status", TIMEOUT)',
        'await postJson("/utility_monitor/start", { profile })',
        'await postJson("/utility_monitor/deactivate", {})',
        'data-mode="HFDL"',
        'data-mode="ALE"',
        'HFDL: "#14b8a6"',
        'ALE: "#f97316"',
    ]

    for snippet in required_snippets:
        assert snippet in html

    for snippet in forbidden_snippets:
        assert snippet not in html


def test_pro_dashboard_serves_receiver_scan_and_net_monitor_without_utility_monitor() -> None:
    client = _make_ui_client()

    response = client.get("/pro")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"

    _assert_dashboard_core(response.text)


def test_prod_minimal_pro_template_keeps_receiver_scan_and_net_monitor_without_utility_monitor() -> None:
    html = Path("/Users/imacpro/Development/KiwiScan/prod_minimal/src/kiwi_scan/static/pro.html").read_text(encoding="utf-8")
    _assert_dashboard_core(html)