#!/bin/sh
set -e

RTL_HOST="${RTL_TCP_HOST:-127.0.0.1}"
RTL_PORT="${RTL_TCP_PORT:-1234}"
SVC_PORT="${SERVICE_PORT:-4026}"

echo "TPMS Monitor — starting"
echo "  rtl_tcp  : ${RTL_HOST}:${RTL_PORT}"
echo "  API port : ${SVC_PORT}"

exec uvicorn service:app \
    --host 0.0.0.0 \
    --port "${SVC_PORT}" \
    --log-level info
