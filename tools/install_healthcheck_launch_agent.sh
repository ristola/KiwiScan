#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.shackmate.kiwiscan-healthcheck"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${AGENT_DIR}/${LABEL}.plist"
INTERVAL="${INTERVAL:-300}"
CONTAINER_NAME="${CONTAINER_NAME:-kiwiscan}"
EXPECTED_IMAGE="${EXPECTED_IMAGE:-n4ldr/kiwiscan:0.1.6}"
VERSION_URL="${VERSION_URL:-http://127.0.0.1:4020/version}"
EXPECTED_MAX_SIZE="${EXPECTED_MAX_SIZE:-10m}"
EXPECTED_MAX_FILE="${EXPECTED_MAX_FILE:-5}"
MIN_RECEIVERS="${MIN_RECEIVERS:-8}"
MIN_NONEMPTY_DECODES="${MIN_NONEMPTY_DECODES:-1}"
FRESH_WITHIN_S="${FRESH_WITHIN_S:-600}"
MIN_FRESH_DECODES="${MIN_FRESH_DECODES:-3}"
REQUIRE_SLOTS="${REQUIRE_SLOTS:-}"
SHOW_DECODE_DETAILS="${SHOW_DECODE_DETAILS:-0}"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/logs}"
STDOUT_PATH="${STDOUT_PATH:-${LOG_DIR}/container_healthcheck.launchd.log}"
STDERR_PATH="${STDERR_PATH:-${LOG_DIR}/container_healthcheck.launchd.err.log}"
PATH_VALUE="${PATH_VALUE:-/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:/usr/bin:/bin:/usr/sbin:/sbin}"

mkdir -p "$AGENT_DIR" "$LOG_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/tools/run_container_healthcheck.sh</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CONTAINER_NAME</key>
    <string>${CONTAINER_NAME}</string>
    <key>EXPECTED_IMAGE</key>
    <string>${EXPECTED_IMAGE}</string>
    <key>VERSION_URL</key>
    <string>${VERSION_URL}</string>
    <key>EXPECTED_MAX_SIZE</key>
    <string>${EXPECTED_MAX_SIZE}</string>
    <key>EXPECTED_MAX_FILE</key>
    <string>${EXPECTED_MAX_FILE}</string>
    <key>MIN_RECEIVERS</key>
    <string>${MIN_RECEIVERS}</string>
    <key>MIN_NONEMPTY_DECODES</key>
    <string>${MIN_NONEMPTY_DECODES}</string>
    <key>FRESH_WITHIN_S</key>
    <string>${FRESH_WITHIN_S}</string>
    <key>MIN_FRESH_DECODES</key>
    <string>${MIN_FRESH_DECODES}</string>
    <key>REQUIRE_SLOTS</key>
    <string>${REQUIRE_SLOTS}</string>
    <key>SHOW_DECODE_DETAILS</key>
    <string>${SHOW_DECODE_DETAILS}</string>
    <key>PATH</key>
    <string>${PATH_VALUE}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>${INTERVAL}</integer>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>StandardOutPath</key>
  <string>${STDOUT_PATH}</string>
  <key>StandardErrorPath</key>
  <string>${STDERR_PATH}</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/${LABEL}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"

cat <<EOF
Installed LaunchAgent:
  ${PLIST_PATH}

Label:
  ${LABEL}

Interval:
  ${INTERVAL} seconds

Logs:
  ${STDOUT_PATH}
  ${STDERR_PATH}
EOF