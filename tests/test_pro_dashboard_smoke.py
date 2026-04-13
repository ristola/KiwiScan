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
        '<h2 data-icon="sliders">Receiver Statistics</h2>',
        'id="chip-rx-health"',
        'id="assignments-band-cards"',
        'id="overview-board"',
        'data-overview-key="status"',
        'data-overview-key="receiver-scan"',
        'id="cfg-overview-receiver-scan"',
        'id="settings-automation-title">Receiver Mode<',
        'id="auto-fixed-plan-card"',
        'id="auto-fixed-plan-rows"',
        'id="auto-mode-enabled"',
        'id="auto-status"',
        'data-nav="receiver-scan"',
        'id="receiver-scan"',
        'id="receiver-scan-start"',
        'id="receiver-scan-stop"',
        'id="receiver-scan-mode-state"',
        'id="receiver-scan-cw-current"',
        'id="receiver-scan-phone-current"',
        'id="receiver-scan-cw-results"',
        'id="receiver-scan-phone-results"',
        'class="receiver-scan-metrics"',
        'function syncReceiverScanControls(scan = latestReceiverScanStatus)',
        'function levelToClass(level)',
        'function titleLevel(level)',
        'function renderReceivers(channels)',
        'function applyReceiverScanStatus(scan)',
        'function stopReceiverScanRun()',
        'getJson("/receiver_scan/status", TIMEOUT)',
        'getJson("/decodes/ws_status", DECODE_WS_CLIENT_POLL_TIMEOUT_MS)',
        'await postJson("/receiver_scan/start", {})',
        'await postJson("/receiver_scan/deactivate", {})',
        'localStorage.setItem(UI_DENSITY_STORAGE_KEY, density);',
        'const storedThemeMode = readStoredUiThemeMode();',
        'byId("chip-rx-health").innerHTML = `Active: ${active}<br>Warn: ${silent + warn}<br>Faulty: ${unstable}`;',
        '<h2 data-icon="alert">Fault Summary</h2>',
        'id="summary-fault-load"',
        'id="reasons"',
        'function renderReasons(reasonCounts)',
        'el.innerHTML = "<li>No active faults</li>";',
        'faultEl.textContent = `fault:${unstable} warn:${warns} reasons:${totalFaultReasons}`;',
        'renderReasons(h.reason_counts || {});',
        'class="status-chip status-chip-stream" data-icon="stream">',
        'id="chip-stream">Connecting...</div>',
        'class="status-chip status-chip-rx-summary" data-icon="shield">',
        'class="status-chip-head">',
        'class="chip-label chip-label-secondary" data-icon="signal">Busiest Band</div>',
        'class="chip-side-value" id="chip-busiest-band">...</div>',
        'class="chip-side-label" data-icon="signal">All Decodes</div>',
        'class="chip-side-value" id="chip-all-decodes">...</div>',
        'class="status-chip status-chip-empty" aria-hidden="true"></article>',
        'function displayManagedReceiverPrefix(value)',
        'return prefix.startsWith("ROAM") ? "ROAM" : prefix;',
        'const readableMatch = raw.match(/^(AUTO|FIXED|ROAM(?:\\d+)?)_([^_]+)_(.+)$/i);',
        '|| /^(AUTO|FIXED|ROAM(?:\\d+)?)_([^_]+)_(.+)$/i.test(raw);',
        'if (digitalModes.length >= 3) return "ALL";',
        'if (digitalModes.length >= 2) return "MIX";',
        'function fmtCount(value) {',
        'function scheduleRecentDecodePanelRender()',
        'ingestDecodeItem(item, { renderList: false, renderMap: false });',
        'const healthCounts = {};',
        'const busiestCounts = healthDecodeTotal > 0 ? healthCounts : counts;',
        'byId("chip-all-decodes").textContent = fmtCount(totalDecodes);',
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
        'class="autosave-badge">Autosave On</span>',
        'id="auto-reload"',
        'id="auto-apply-now"',
        'id="auto-ui-theme-mode"',
        'id="auto-ui-density"',
        'previewUiPreferencesFromAutomation',
        'byId("auto-ui-theme-mode")',
        'byId("auto-ui-density")',
        'auto-scan-on-block',
        'auto-refresh-schedule',
        'auto-scan-on-startup',
        'auto-ssb-enabled',
        'function updateAutomationSsbStepVisibility(strategyValue)',
        'const autoSsbStepStrategy = byId("auto-ssb-step-strategy");',
        'bcCurrentMode',
        'bc-mode-toggle',
        'bc-mode-btn',
        '/smart_scheduler/status?mode=',
        'byId("auto-apply-mode")',
        'mode === "phone" ? "SSB" : "FT8"',
        'id="auto-apply-mode"',
        'id="auto-selected-bands"',
        'data-nav="summary"',
        'id="summary-busiest-band"',
        'id="chip-stream-4010-count"',
        'id="chip-stream-4020-count"',
        'UDP: 4010</div>',
        '<article class="status-chip" data-icon="signal">',
    ]

    for snippet in forbidden_snippets:
        assert snippet not in html


def test_prod_minimal_pro_template_keeps_fault_summary_and_receiver_health_hooks() -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    prod_template = workspace_root / "prod_minimal" / "src" / "kiwi_scan" / "static" / "pro.html"

    html = prod_template.read_text(encoding="utf-8")
    required_snippets = [
        '<h2 data-icon="sliders">Receiver Statistics</h2>',
        'id="chip-rx-health"',
        'id="assignments-band-cards"',
        'id="overview-board"',
        'data-overview-key="status"',
        'data-overview-key="receiver-scan"',
        'id="cfg-overview-receiver-scan"',
        'id="settings-automation-title">Receiver Mode<',
        'id="auto-fixed-plan-card"',
        'id="auto-fixed-plan-rows"',
        'id="auto-mode-enabled"',
        'id="auto-status"',
        'data-nav="receiver-scan"',
        'id="receiver-scan"',
        'id="receiver-scan-start"',
        'id="receiver-scan-stop"',
        'id="receiver-scan-mode-state"',
        'id="receiver-scan-cw-current"',
        'id="receiver-scan-phone-current"',
        'id="receiver-scan-cw-results"',
        'id="receiver-scan-phone-results"',
        'class="receiver-scan-metrics"',
        'function syncReceiverScanControls(scan = latestReceiverScanStatus)',
        'function renderReceivers(channels)',
        'function applyReceiverScanStatus(scan)',
        'function stopReceiverScanRun()',
        'getJson("/receiver_scan/status", TIMEOUT)',
        'getJson("/decodes/ws_status", DECODE_WS_CLIENT_POLL_TIMEOUT_MS)',
        'await postJson("/receiver_scan/start", {})',
        'await postJson("/receiver_scan/deactivate", {})',
        'localStorage.setItem(UI_DENSITY_STORAGE_KEY, density);',
        'const storedThemeMode = readStoredUiThemeMode();',
        '<h2 data-icon="alert">Fault Summary</h2>',
        'id="summary-fault-load"',
        'id="reasons"',
        'function renderReasons(reasonCounts)',
        'el.innerHTML = "<li>No active faults</li>";',
        'renderReasons(h.reason_counts || {});',
        'class="status-chip status-chip-stream" data-icon="stream">',
        'id="chip-stream">Connecting...</div>',
        'class="status-chip status-chip-rx-summary" data-icon="shield">',
        'class="status-chip-head">',
        'class="chip-label chip-label-secondary" data-icon="signal">Busiest Band</div>',
        'class="chip-side-value" id="chip-busiest-band">...</div>',
        'class="chip-side-label" data-icon="signal">All Decodes</div>',
        'class="chip-side-value" id="chip-all-decodes">...</div>',
        'class="status-chip status-chip-empty" aria-hidden="true"></article>',
        'function displayManagedReceiverPrefix(value)',
        'return prefix.startsWith("ROAM") ? "ROAM" : prefix;',
        'const readableMatch = raw.match(/^(AUTO|FIXED|ROAM(?:\\d+)?)_([^_]+)_(.+)$/i);',
        '|| /^(AUTO|FIXED|ROAM(?:\\d+)?)_([^_]+)_(.+)$/i.test(raw);',
        'if (digitalModes.length >= 3) return "ALL";',
        'if (digitalModes.length >= 2) return "MIX";',
        'function fmtCount(value) {',
        'function scheduleRecentDecodePanelRender()',
        'ingestDecodeItem(item, { renderList: false, renderMap: false });',
        'const healthCounts = {};',
        'const busiestCounts = healthDecodeTotal > 0 ? healthCounts : counts;',
        'byId("chip-all-decodes").textContent = fmtCount(totalDecodes);',
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
        'class="autosave-badge">Autosave On</span>',
        'id="auto-reload"',
        'id="auto-apply-now"',
        'id="auto-ui-theme-mode"',
        'id="auto-ui-density"',
        'previewUiPreferencesFromAutomation',
        'byId("auto-ui-theme-mode")',
        'byId("auto-ui-density")',
        'auto-scan-on-block',
        'auto-refresh-schedule',
        'auto-scan-on-startup',
        'auto-ssb-enabled',
        'function updateAutomationSsbStepVisibility(strategyValue)',
        'const autoSsbStepStrategy = byId("auto-ssb-step-strategy");',
        'bcCurrentMode',
        'bc-mode-toggle',
        'bc-mode-btn',
        '/smart_scheduler/status?mode=',
        'byId("auto-apply-mode")',
        'mode === "phone" ? "SSB" : "FT8"',
        'id="auto-apply-mode"',
        'id="auto-selected-bands"',
        'data-nav="summary"',
        'id="summary-busiest-band"',
        'id="chip-stream-4010-count"',
        'id="chip-stream-4020-count"',
        'UDP: 4010</div>',
        '<article class="status-chip" data-icon="signal">',
    ]

    for snippet in forbidden_snippets:
        assert snippet not in html