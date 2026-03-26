#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:${PATH:-/usr/bin:/bin:/usr/sbin:/sbin}"

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

args=(
  --container "$CONTAINER_NAME"
  --expected-image "$EXPECTED_IMAGE"
  --version-url "$VERSION_URL"
  --expected-max-size "$EXPECTED_MAX_SIZE"
  --expected-max-file "$EXPECTED_MAX_FILE"
  --min-receivers "$MIN_RECEIVERS"
  --min-nonempty-decodes "$MIN_NONEMPTY_DECODES"
)

if [[ -n "$FRESH_WITHIN_S" ]]; then
  args+=(--fresh-within-s "$FRESH_WITHIN_S")
fi

if [[ -n "$MIN_FRESH_DECODES" ]]; then
  args+=(--min-fresh-decodes "$MIN_FRESH_DECODES")
fi

if [[ -n "$REQUIRE_SLOTS" ]]; then
  args+=(--require-slots "$REQUIRE_SLOTS")
fi

if [[ "$SHOW_DECODE_DETAILS" == "1" ]]; then
  args+=(--show-decode-details)
fi

exec python3 "$ROOT_DIR/tools/container_healthcheck.py" "${args[@]}" "$@"