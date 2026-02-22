#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
BUILD_SCRIPT="$ROOT_DIR/tools/build_macos_installer_pkg.sh"

VERSION="${VERSION:-0.1.0}"
IDENTIFIER="${IDENTIFIER:-com.ristola.kiwiscan.installer}"
UNSIGNED_PKG="$DIST_DIR/KiwiScan-Installer-${VERSION}-unsigned.pkg"
SIGNED_PKG="$DIST_DIR/KiwiScan-Installer-${VERSION}.pkg"

SIGN_PKG="${SIGN_PKG:-0}"
NOTARIZE="${NOTARIZE:-0}"

# Required only when SIGN_PKG=1:
#   PKG_SIGN_IDENTITY="Developer ID Installer: ..."
PKG_SIGN_IDENTITY="${PKG_SIGN_IDENTITY:-}"

# Required only when NOTARIZE=1:
#   NOTARY_PROFILE: keychain profile created with notarytool
#   e.g. xcrun notarytool store-credentials "AC_PROFILE" ...
NOTARY_PROFILE="${NOTARY_PROFILE:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: missing required command: $1" >&2
    exit 2
  fi
}

need_cmd pkgbuild
need_cmd productsign
need_cmd xcrun
need_cmd bash

mkdir -p "$DIST_DIR"

echo "[1/4] Building unsigned installer package..."
VERSION="$VERSION" IDENTIFIER="$IDENTIFIER" OUT_PKG="$UNSIGNED_PKG" "$BUILD_SCRIPT"

if [[ "$SIGN_PKG" != "1" ]]; then
  echo "[2/4] Skipping signing (SIGN_PKG=0)."
  echo "Artifact: $UNSIGNED_PKG"
  exit 0
fi

if [[ -z "$PKG_SIGN_IDENTITY" ]]; then
  echo "Error: SIGN_PKG=1 but PKG_SIGN_IDENTITY is empty." >&2
  exit 2
fi

echo "[2/4] Signing package with identity: $PKG_SIGN_IDENTITY"
productsign --sign "$PKG_SIGN_IDENTITY" "$UNSIGNED_PKG" "$SIGNED_PKG"

echo "[3/4] Verifying signature"
pkgutil --check-signature "$SIGNED_PKG" || true

if [[ "$NOTARIZE" != "1" ]]; then
  echo "[4/4] Skipping notarization (NOTARIZE=0)."
  echo "Artifact: $SIGNED_PKG"
  exit 0
fi

if [[ -z "$NOTARY_PROFILE" ]]; then
  echo "Error: NOTARIZE=1 but NOTARY_PROFILE is empty." >&2
  exit 2
fi

echo "[4/4] Submitting for notarization with profile: $NOTARY_PROFILE"
xcrun notarytool submit "$SIGNED_PKG" --keychain-profile "$NOTARY_PROFILE" --wait

echo "Stapling notarization ticket..."
xcrun stapler staple "$SIGNED_PKG"

echo "Done. Notarized artifact: $SIGNED_PKG"
