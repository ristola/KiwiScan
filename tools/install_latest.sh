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
if ! mkdir -p "$DEST_DIR" 2>/dev/null; then
  echo "Error: cannot create destination: $DEST_DIR" >&2
  if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    echo "Hint: /opt typically requires admin rights." >&2
    echo "Run either:" >&2
    echo "  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/tools/install_latest.sh | sudo bash -s -- $DEST_DIR" >&2
    echo "or install without sudo to your home directory:" >&2
    echo "  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/tools/install_latest.sh | bash -s -- \"$HOME/KiwiScan\"" >&2
  fi
  exit 1
fi
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
