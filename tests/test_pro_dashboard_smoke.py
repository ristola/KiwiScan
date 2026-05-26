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
        'function getReservedManagedReceiverSet(mode = getCurrentReceiversMode())',
        'function syncReceiversModeFromBackendState()',
        'let receiverScanPreparePending = false;',
        'receiverScanPreparePending',
        'function receiversModeShowsDecodeOverview(mode = getCurrentReceiversMode()) {',
        'function isOverviewTopicModeVisible(key, mode = getCurrentReceiversMode()) {',
        'return merged.length ? merged.slice(0, MAX_CONFIGURED_KIWIS) : [buildDefaultConfiguredKiwiEntry()];',
        'function getChannelEffectiveReason(ch) {',
        'function summarizeReasonAffectedReceivers(reason, channels, limit = 3) {',
        'const rightPanels = ["assignments", "faults", "receiver-scan", "net-monitor", "caption-monitor", "messages", "map", "active-receivers", "settings", "system"]',
        'const panelVisible = panelId === navId || ((panelId === "net-monitor" || panelId === "caption-monitor") && navId === "receiver-scan")',
        'if (receiverScanPanel) receiverScanPanel.hidden = mode !== "scan";',
        'if (!isOverviewTopicModeVisible(navId)) return false;',
        'show(getOverviewTopicSectionElement(key), !!overviewTopicPrefs[key] && isOverviewTopicModeVisible(key));',
        'function renderBandScanResults(results = latestBandScanResults)',
        'function syncNetMonitorControls(status = latestNetMonitorStatus)',
        'if (!scan || scan.running || !scan.mode_active || !Array.isArray(scan.reserved_receivers)) return new Set();',
        'if (latestReceiverScanStatus && latestReceiverScanStatus.running && Array.isArray(latestReceiverScanStatus.reserved_receivers)) {',
        'rowName: "Reserved for Scan"',
        'cardTitle: "Semi Reserve"',
        'labelEl.textContent = isSemi ? "SEMI" : "AUTO";',
        'Affected: ${affectedReceivers}.',
        'function activeKiwiTargetPayload(payload = {})',
        'getJson(appendActiveKiwiKey("/receiver_scan/status"), TIMEOUT)',
        'getJson(appendActiveKiwiKey("/net_monitor/status"), TIMEOUT)',
        'getJson(appendActiveKiwiKey("/caption/status"), TIMEOUT)',
        'getJson("/band_scan/results", TIMEOUT)',
        'await postJson("/receiver_scan/start", activeKiwiTargetPayload({ band: targetBand, mode: targetRole }))',
        'await postJson("/receiver_scan/prepare", activeKiwiTargetPayload({}))',
        'await postJson("/net_monitor/start", activeKiwiTargetPayload({}))',
        'await postJson("/net_monitor/deactivate", activeKiwiTargetPayload({}))',
        'await postJson("/caption/start", activeKiwiTargetPayload({',
        'await postJson("/caption/stop", activeKiwiTargetPayload({}))',
        'const MAP_MODE_FILTERS = ["FT8", "FT4", "WSPR", "SSB"]',
        'mapInstance.fitBounds(bounds, { paddingTopLeft: [24, 88], paddingBottomRight: [24, 48], maxZoom: 6 });',
        'window.scrollTo(pageScrollX, pageScrollY);',
        'fitBtn.addEventListener("click", handleMapAutoFitButtonClick);',
        'const digits = mode === "WSPR" ? 4 : 3;',
        'Decoder heartbeat is current; receiver stayed visible on Kiwi but produced no decodes past the silent threshold.',
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

    prepare_scan_mode_section = html.split('async function prepareReceiversScanMode() {', 1)[1].split('function initConfigAutomationControls() {', 1)[0]
    assert 'if (typeof window.kiwiProSetView === "function") window.kiwiProSetView("receiver-scan", true);' not in prepare_scan_mode_section


def test_pro_dashboard_serves_receiver_scan_and_net_monitor_without_utility_monitor() -> None:
    client = _make_ui_client()

    response = client.get("/pro")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"

    _assert_dashboard_core(response.text)
    assert 'function preferredActiveReceiverListenModeFromDecodeRates(modeValue, decodeRatesByMode) {' in response.text
    assert 'data-preferred-mode="${escapeHtml(safeText(user && user.preferred_listen_mode, ""))}"' in response.text
    assert '.filter((modeName) => modeName !== "WSPR")' in response.text
    assert 'id="chip-stream-activity"' in response.text
    assert 'function hfConditionsEnabledForAllConfiguredKiwis() {' in response.text
    assert 'if (mode !== "auto" && mode !== "semi") return false;' in response.text
    assert 'activityEl.style.display = showHfConditions ? "" : "none";' in response.text
    assert 'class="server-chip-link"' in response.text
    assert 'target="_blank" rel="noopener noreferrer"' in response.text
    assert 'return `http://${normalizedHost}:${portNumber}/admin`;' in response.text
    assert response.text.count('await postJson("/admin/force-reassign", {});') == 1


def test_prod_minimal_pro_template_keeps_receiver_scan_and_net_monitor_without_utility_monitor() -> None:
    html = Path("/Users/imacpro/Development/KiwiScan/prod_minimal/src/kiwi_scan/static/pro.html").read_text(encoding="utf-8")
    _assert_dashboard_core(html)
    assert 'id="chip-stream-activity"' in html
    assert 'function hfConditionsEnabledForAllConfiguredKiwis() {' in html
    assert 'if (mode !== "auto" && mode !== "semi") return false;' in html
    assert 'activityEl.style.display = showHfConditions ? "" : "none";' in html
    assert 'class="server-chip-link"' in html
    assert 'target="_blank" rel="noopener noreferrer"' in html
    assert 'return `http://${normalizedHost}:${portNumber}/admin`;' in html
    assert html.count('await postJson("/admin/force-reassign", {});') == 1