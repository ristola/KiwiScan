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
    assert 'return configuredCount > 0;' in response.text
    assert 'if (mode !== "auto" && mode !== "semi") return false;' not in response.text
    assert 'activityEl.style.display = showHfConditions ? "" : "none";' in response.text
    assert 'class="server-chip-link"' in response.text
    assert 'target="_blank" rel="noopener noreferrer"' in response.text
    assert 'return `http://${normalizedHost}:${portNumber}/admin`;' in response.text
    assert response.text.count('await postJson("/admin/force-reassign", {});') >= 1
    assert 'hasMismatch ? "Assigning Receivers." : "Setting up Receiver Assignments !"' in response.text
    assert 'const hasBusyPlaceholder = statusSource === "busy"' in response.text
    assert 'statusSource === "busy" && !hasBusyPlaceholder' in response.text
    assert 'Object.prototype.hasOwnProperty.call(normalized, "enabled") || normalized.band || normalized.mode || normalized.freq_khz' in response.text
    assert 'enabled: !!draft.enabled,' in response.text
    assert '<script src="https://unpkg.com/three@0.179.1/build/three.min.js"></script>' not in response.text
    assert '<script src="https://unpkg.com/globe.gl@2.41.4/dist/globe.gl.min.js"></script>' not in response.text
    assert 'function ensureGlobeDependencies() {' in response.text
    manual_seed_section = response.text.split('function buildManualAssignmentSeed(rxNum, usersByRx = currentManualReceiverUsersByRx()) {', 1)[1].split('function getManualAssignmentDraft(', 1)[0]
    assert 'const persistedEnabled = Boolean(' in manual_seed_section
    assert 'const live = Boolean(user && user.active);' in manual_seed_section
    assert 'const enabled = live || (hasPersisted ? persistedEnabled : assignmentEnabled);' in manual_seed_section
    assert 'safeText(assignment && assignment.band, "")' not in manual_seed_section
    assert 'Number.isFinite(Number(assignment && assignment.freq_hz))' in manual_seed_section
    assert 'Number.isFinite(Number(user && user.freq_khz))' in manual_seed_section
    assert 'getJson(appendActiveKiwiKey("/system/info"), SYSTEM_INFO_TIMEOUT_MS)' in response.text
    assert 'getJson("/system/info", SYSTEM_INFO_TIMEOUT_MS)' not in response.text
    assert 'async function refresh(force = false) {' in response.text
    assert 'function manualAssignmentDraftDirtyActive() {' in response.text
    assert 'function manualAssignmentEditorMounted(container = byId("active-receivers-table")) {' in response.text
    assert 'function preserveOverviewScrollPosition() {' in response.text
    assert response.text.count('if (manualAssignmentDashboardInteractionActive()) return;') >= 2
    assert 'markManualAssignmentInteraction(field === "freq" ? 12000 : 1800);' in response.text
    assert 'if (manualAssignmentEditorMounted(usersEl)) {' in response.text
    assert 'manualAssignmentDraftDirtyActive() || manualAssignmentInteractionLocked() || manualAssignmentFieldHasFocus(usersEl)' in response.text
    assert 'if (focusedManualField || manualAssignmentEditorActive || manualAssignmentDraftDirtyActive() || manualAssignmentInteractionLocked()) {' in response.text

    active_kiwi_switch_section = response.text.split('async function saveActiveConfiguredKiwiSelection(index) {', 1)[1].split('async function saveConfigSection(options = {}) {', 1)[0]
    assert 'latestSystemInfo = null;' in active_kiwi_switch_section
    assert 'lastSystemInfoRefreshMs = 0;' in active_kiwi_switch_section
    assert 'latestChannelsMap = {};' in active_kiwi_switch_section
    assert 'await refreshSystemInfo(true).catch(() => { });' in active_kiwi_switch_section
    assert 'await refresh(true);' in active_kiwi_switch_section
    assert 'window.kiwiProSetView("overview", false);' in active_kiwi_switch_section

    manual_commit_section = response.text.split('async function commitManualAssignmentChanges({ applyRuntime = false } = {}) {', 1)[1].split('} finally {', 1)[0]
    assert 'await refresh();' not in manual_commit_section

    sync_mode_section = response.text.split('function syncReceiversModeFromBackendState() {', 1)[1].split('function buildActiveReceiverRows(', 1)[0]
    assert 'if (manualAssignmentDashboardInteractionActive()) return;' in sync_mode_section

    manual_input_section = response.text.split('container.addEventListener("input", (event) => {', 1)[1].split('container.addEventListener("change", (event) => {', 1)[0]
    assert 'const validForCommit = Number.isFinite(freqNum) && freqNum > 0;' in manual_input_section
    assert 'manualAssignmentCommitApplyRuntime = false;' in manual_input_section
    assert 'if (validForCommit) {' in manual_input_section
    assert 'applyRuntime: !!draft.enabled,' in manual_input_section


def test_prod_minimal_pro_template_keeps_receiver_scan_and_net_monitor_without_utility_monitor() -> None:
    html = Path("/Users/imacpro/Development/KiwiScan/prod_minimal/src/kiwi_scan/static/pro.html").read_text(encoding="utf-8")
    _assert_dashboard_core(html)
    assert 'id="chip-stream-activity"' in html
    assert 'function hfConditionsEnabledForAllConfiguredKiwis() {' in html
    assert 'return configuredCount > 0;' in html
    assert 'if (mode !== "auto" && mode !== "semi") return false;' not in html
    assert 'activityEl.style.display = showHfConditions ? "" : "none";' in html
    assert 'class="server-chip-link"' in html
    assert 'target="_blank" rel="noopener noreferrer"' in html
    assert 'return `http://${normalizedHost}:${portNumber}/admin`;' in html
    assert html.count('await postJson("/admin/force-reassign", {});') == 1
    assert 'hasMismatch ? "Assigning Receivers." : "Setting up Receiver Assignments !"' in html
    assert 'const hasBusyPlaceholder = statusSource === "busy"' in html
    assert 'statusSource === "busy" && !hasBusyPlaceholder' in html
    assert 'Object.prototype.hasOwnProperty.call(normalized, "enabled") || normalized.band || normalized.mode || normalized.freq_khz' in html
    assert 'enabled: !!draft.enabled,' in html
    assert '<script src="https://unpkg.com/three@0.179.1/build/three.min.js"></script>' not in html
    assert '<script src="https://unpkg.com/globe.gl@2.41.4/dist/globe.gl.min.js"></script>' not in html
    assert 'function ensureGlobeDependencies() {' in html
    manual_seed_section = html.split('function buildManualAssignmentSeed(rxNum, usersByRx = currentManualReceiverUsersByRx()) {', 1)[1].split('function getManualAssignmentDraft(', 1)[0]
    assert 'const persistedEnabled = Boolean(' in manual_seed_section
    assert 'const live = Boolean(user && user.active);' in manual_seed_section
    assert 'const enabled = live || (hasPersisted ? persistedEnabled : assignmentEnabled);' in manual_seed_section
    assert 'safeText(assignment && assignment.band, "")' not in manual_seed_section
    assert 'Number.isFinite(Number(assignment && assignment.freq_hz))' in manual_seed_section
    assert 'Number.isFinite(Number(user && user.freq_khz))' in manual_seed_section
    assert 'getJson(appendActiveKiwiKey("/system/info"), SYSTEM_INFO_TIMEOUT_MS)' in html
    assert 'getJson("/system/info", SYSTEM_INFO_TIMEOUT_MS)' not in html
    assert 'async function refresh(force = false) {' in html
    assert 'function manualAssignmentDraftDirtyActive() {' in html
    assert 'function manualAssignmentEditorMounted(container = byId("active-receivers-table")) {' in html
    assert 'markManualAssignmentInteraction(field === "freq" ? 12000 : 1800);' in html
    assert 'if (manualAssignmentEditorMounted(usersEl)) {' in html
    assert 'manualAssignmentDraftDirtyActive() || manualAssignmentInteractionLocked() || manualAssignmentFieldHasFocus(usersEl)' in html
    assert 'if (focusedManualField || manualAssignmentEditorActive || manualAssignmentDraftDirtyActive() || manualAssignmentInteractionLocked()) {' in html

    active_kiwi_switch_section = html.split('async function saveActiveConfiguredKiwiSelection(index) {', 1)[1].split('async function saveConfigSection(options = {}) {', 1)[0]
    assert 'latestSystemInfo = null;' in active_kiwi_switch_section
    assert 'lastSystemInfoRefreshMs = 0;' in active_kiwi_switch_section
    assert 'latestChannelsMap = {};' in active_kiwi_switch_section
    assert 'await refreshSystemInfo(true).catch(() => { });' in active_kiwi_switch_section
    assert 'await refresh(true);' in active_kiwi_switch_section
    assert 'window.kiwiProSetView("overview", false);' in active_kiwi_switch_section

    sync_mode_section = html.split('function syncReceiversModeFromBackendState() {', 1)[1].split('function buildActiveReceiverRows(', 1)[0]
    assert 'if (manualAssignmentDashboardInteractionActive()) return;' in sync_mode_section

    manual_input_section = html.split('container.addEventListener("input", (event) => {', 1)[1].split('container.addEventListener("change", (event) => {', 1)[0]
    assert 'const validForCommit = Number.isFinite(freqNum) && freqNum > 0;' in manual_input_section
    assert 'manualAssignmentCommitApplyRuntime = false;' in manual_input_section
    assert 'if (validForCommit) {' in manual_input_section
    assert 'applyRuntime: !!draft.enabled,' in manual_input_section

    manual_commit_section = html.split('async function commitManualAssignmentChanges({ applyRuntime = false } = {}) {', 1)[1].split('} finally {', 1)[0]
    assert 'await refresh();' not in manual_commit_section