#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
PKG_ROOT="$(mktemp -d)"
trap 'rm -rf "$PKG_ROOT"' EXIT

VERSION="${VERSION:-0.1.0}"
IDENTIFIER="${IDENTIFIER:-com.ristola.kiwiscan.installer}"
OUT_PKG="${OUT_PKG:-$DIST_DIR/KiwiScan-Installer-${VERSION}-unsigned.pkg}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: missing required command: $1" >&2
    exit 2
  fi
}

need_cmd pkgbuild
need_cmd bash

mkdir -p "$DIST_DIR"

mkdir -p "$PKG_ROOT/usr/local/bin"
mkdir -p "$PKG_ROOT/usr/local/share/kiwiscan-installer"

cp "$ROOT_DIR/tools/install_latest.sh" "$PKG_ROOT/usr/local/share/kiwiscan-installer/install_latest.sh"
chmod +x "$PKG_ROOT/usr/local/share/kiwiscan-installer/install_latest.sh"

cat > "$PKG_ROOT/usr/local/bin/kiwiscan-install" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
DEST_DIR="${1:-/opt/kiwi_scan_prod}"
exec /usr/local/share/kiwiscan-installer/install_latest.sh "$DEST_DIR"
EOF
chmod +x "$PKG_ROOT/usr/local/bin/kiwiscan-install"

chmod +x "$ROOT_DIR/tools/pkg/scripts/postinstall"

pkgbuild \
  --root "$PKG_ROOT" \
  --scripts "$ROOT_DIR/tools/pkg/scripts" \
  --identifier "$IDENTIFIER" \
  --version "$VERSION" \
  --install-location / \
  "$OUT_PKG"

echo "Created package: $OUT_PKG"
echo "Share this package file via GitHub Releases for click-to-install UX on macOS."
