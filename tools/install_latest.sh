#!/usr/bin/env bash
set -euo pipefail

REPO="${REPO:-ristola/KiwiScan}"
BRANCH="${BRANCH:-main}"
DEST_DIR="${1:-/opt/kiwi_scan_prod}"
ARCHIVE_URL="https://github.com/${REPO}/archive/refs/heads/${BRANCH}.zip"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: missing required command: $1" >&2
    exit 2
  fi
}

need_cmd curl
need_cmd unzip
need_cmd rsync
need_cmd python3

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

ZIP_PATH="$TMP_DIR/kiwiscan.zip"

echo "Downloading ${ARCHIVE_URL} ..."
curl -fL "$ARCHIVE_URL" -o "$ZIP_PATH"

echo "Extracting archive ..."
unzip -q "$ZIP_PATH" -d "$TMP_DIR"

SRC_DIR="$(find "$TMP_DIR" -maxdepth 1 -type d -name 'KiwiScan-*' | head -n 1 || true)"
if [[ -z "$SRC_DIR" || ! -d "$SRC_DIR" ]]; then
  echo "Error: could not locate extracted source directory." >&2
  exit 2
fi

echo "Installing to ${DEST_DIR} ..."
mkdir -p "$DEST_DIR"
rsync -a --delete \
  --exclude '.git/' \
  --exclude '.github/' \
  --exclude '.venv/' \
  --exclude '.venv-py3/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.mypy_cache/' \
  --exclude '.ruff_cache/' \
  "$SRC_DIR/" "$DEST_DIR/"

if [[ -f "$DEST_DIR/run_server.sh" ]]; then
  chmod +x "$DEST_DIR/run_server.sh" || true
fi

cat <<EOF

Install complete.

Next steps:
  cd "$DEST_DIR"
  ./run_server.sh

Notes:
  - On first run, run_server.sh auto-creates .venv-py3 and installs Python deps.
  - Default URL: http://<host>:4020
  - Override port: PORT=4021 ./run_server.sh

EOF
