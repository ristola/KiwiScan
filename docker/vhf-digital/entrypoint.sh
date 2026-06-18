#!/bin/bash
set -e

RTL_TCP_HOST="${RTL_TCP_HOST:-127.0.0.1}"
RTL_TCP_PORT="${RTL_TCP_PORT:-7374}"
CENTER_HZ="${CENTER_HZ:-144950000}"
SAMPLE_RATE="${SAMPLE_RATE:-2400000}"
SERVICE_PORT="${SERVICE_PORT:-4025}"

echo "VHF Digital Monitor — starting"
echo "  rtl_tcp  : ${RTL_TCP_HOST}:${RTL_TCP_PORT}"
echo "  center   : ${CENTER_HZ} Hz"
echo "  API port : ${SERVICE_PORT}"

# Verify tools present
for bin in direwolf jt9 wsprd; do
    if ! command -v "$bin" &>/dev/null; then
        echo "WARNING: $bin not found in PATH — that decoder will be unavailable"
    fi
done

export RTL_TCP_HOST RTL_TCP_PORT CENTER_HZ SAMPLE_RATE

exec uvicorn service:app --host 0.0.0.0 --port "${SERVICE_PORT}" --log-level info
