from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from kiwi_scan.api.ui import mount_static, router as ui_router


def _make_ui_client() -> TestClient:
    app = FastAPI()
    mount_static(app)
    app.include_router(ui_router)
    return TestClient(app)


def test_pro_dashboard_serves_fault_summary_and_receiver_health_hooks() -> None:
    client = _make_ui_client()

    response = client.get("/pro")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"

    html = response.text
    required_snippets = [
        '<h2 data-icon="radio">Receiver Matrix</h2>',
        'id="chip-rx-health"',
        'id="rx-grid"',
        'id="assignment-balance"',
        'id="overview-board"',
        'data-overview-key="status"',
        'id="settings-automation-title">Automation<',
        'id="settings-automation-auto-scan-title">Auto',
        'id="auto-refresh-schedule"',
        'id="auto-scan-on-startup"',
        'id="auto-scan-on-block"',
        'id="settings-automation-ssb-title">SSB',
        'id="auto-ssb-sideband"',
        'id="auto-ssb-step-strategy"',
        'id="auto-status"',
        'function levelToClass(level)',
        'function titleLevel(level)',
        'function renderReceivers(channels)',
        'function updateAutomationSsbStepVisibility(strategyValue)',
        'byId("chip-rx-health").innerHTML = `Active: ${active}<br>Warn: ${silent + warn}<br>Faulty: ${unstable}`;',
        '<h2 data-icon="alert">Fault Summary</h2>',
        'id="summary-fault-load"',
        'id="reasons"',
        'function renderReasons(reasonCounts)',
        'el.innerHTML = "<li>No active faults</li>";',
        'faultEl.textContent = `fault:${unstable} warn:${warns} reasons:${totalFaultReasons}`;',
        'renderReasons(h.reason_counts || {});',
    ]

    for snippet in required_snippets:
        assert snippet in html

    forbidden_snippets = [
        '<a href="#trend" data-nav="trend" data-icon="trend">Decode Trend</a>',
        '<article class="panel" id="trend">',
        'id="cfg-overview-trend"',
        'id="cfg-overview-summary"',
        'id="cfg-overview-balance"',
        'id="assignment-balance-panel"',
        'data-nav="automation"',
        '<article class="panel" id="automation">',
        'id="auto-alerts-enabled"',
        'id="auto-alert-threshold"',
        'id="auto-quiet-start"',
        'id="auto-quiet-end"',
        'id="auto-ui-theme-mode"',
        'id="auto-ui-density"',
        'id="auto-apply-mode"',
        'id="auto-selected-bands"',
        'data-nav="summary"',
        'id="summary-busiest-band"',
    ]

    for snippet in forbidden_snippets:
        assert snippet not in html


def test_prod_minimal_pro_template_keeps_fault_summary_and_receiver_health_hooks() -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    prod_template = workspace_root / "prod_minimal" / "src" / "kiwi_scan" / "static" / "pro.html"

    html = prod_template.read_text(encoding="utf-8")
    required_snippets = [
        '<h2 data-icon="radio">Receiver Matrix</h2>',
        'id="chip-rx-health"',
        'id="rx-grid"',
        'id="assignment-balance"',
        'id="overview-board"',
        'data-overview-key="status"',
        'id="settings-automation-title">Automation<',
        'id="settings-automation-auto-scan-title">Auto',
        'id="auto-refresh-schedule"',
        'id="auto-scan-on-startup"',
        'id="auto-scan-on-block"',
        'id="settings-automation-ssb-title">SSB',
        'id="auto-ssb-sideband"',
        'id="auto-ssb-step-strategy"',
        'id="auto-status"',
        'function renderReceivers(channels)',
        'function updateAutomationSsbStepVisibility(strategyValue)',
        '<h2 data-icon="alert">Fault Summary</h2>',
        'id="summary-fault-load"',
        'id="reasons"',
        'function renderReasons(reasonCounts)',
        'el.innerHTML = "<li>No active faults</li>";',
        'renderReasons(h.reason_counts || {});',
    ]

    for snippet in required_snippets:
        assert snippet in html

    forbidden_snippets = [
        '<a href="#trend" data-nav="trend" data-icon="trend">Decode Trend</a>',
        '<article class="panel" id="trend">',
        'id="cfg-overview-trend"',
        'id="cfg-overview-summary"',
        'id="cfg-overview-balance"',
        'id="assignment-balance-panel"',
        'data-nav="automation"',
        '<article class="panel" id="automation">',
        'id="auto-alerts-enabled"',
        'id="auto-alert-threshold"',
        'id="auto-quiet-start"',
        'id="auto-quiet-end"',
        'id="auto-ui-theme-mode"',
        'id="auto-ui-density"',
        'id="auto-apply-mode"',
        'id="auto-selected-bands"',
        'data-nav="summary"',
        'id="summary-busiest-band"',
    ]

    for snippet in forbidden_snippets:
        assert snippet not in html