#!/bin/bash
set -euo pipefail

DEST_DEFAULT="$HOME/KiwiScan"
SCRIPT_URL="https://raw.githubusercontent.com/ristola/KiwiScan/main/tools/install_latest.sh"

echo "KiwiScan Installer"
echo "------------------"
echo "This will download and install the latest KiwiScan to: $DEST_DEFAULT"
echo

read -p "Press Enter to continue (or Ctrl+C to cancel) ... " _

/bin/bash -lc "curl -fsSL '$SCRIPT_URL' | bash -s -- '$DEST_DEFAULT'"

echo
echo "Done. Press Enter to close."
read -r _
