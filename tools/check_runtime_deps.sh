#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SHACKMATE_ROOT="$(cd "$ROOT_DIR/.." && pwd)"

BUILD_MISSING=0
QUIET=0

for arg in "$@"; do
  case "$arg" in
    --build-missing) BUILD_MISSING=1 ;;
    --quiet) QUIET=1 ;;
    -h|--help)
      cat <<'EOF'
Usage: tools/check_runtime_deps.sh [--build-missing] [--quiet]

Checks runtime dependencies used by kiwi_scan receiver automation.

Options:
  --build-missing  Attempt to build ft8modem/af2udp from ../ft8modem when missing
  --quiet          Print only summary + errors
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
  esac
done

say() {
  if [ "$QUIET" != "1" ]; then
    echo "$@"
  fi
}

find_first_path() {
  for p in "$@"; do
    if [ -n "$p" ] && [ -e "$p" ]; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

find_first_executable_path() {
  for p in "$@"; do
    if [ -n "$p" ] && [ -f "$p" ] && [ -x "$p" ]; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

find_first_cmd() {
  for c in "$@"; do
    if command -v "$c" >/dev/null 2>&1; then
      command -v "$c"
      return 0
    fi
  done
  return 1
}

try_build_ft8modem() {
  local dir="$SHACKMATE_ROOT/ft8modem"
  if [ ! -d "$dir" ] || [ ! -f "$dir/Makefile" ]; then
    return 1
  fi
  say "[build] Attempting to build missing ft8modem/af2udp in $dir"
  (cd "$dir" && make af2udp ft8modem)
}

missing=0

kiwi_vendor="$ROOT_DIR/vendor/kiwiclient-jks/kiwirecorder.py"
kiwi_cmd="$(find_first_cmd kiwirecorder.py kiwirecorder || true)"
kiwi_path="$(find_first_path "$kiwi_vendor" "$kiwi_cmd" || true)"

ft8_local="$SHACKMATE_ROOT/ft8modem/ft8modem"
ft8_sys="/usr/local/bin/ft8modem"
ft8_cmd="$(find_first_cmd ft8modem || true)"
if [[ "$ft8_cmd" == /opt/local/* ]]; then
  ft8_cmd=""
fi
ft8_path="$(find_first_executable_path "$ft8_cmd" "$ft8_local" "$ft8_sys" || true)"

af2_local="$SHACKMATE_ROOT/ft8modem/af2udp"
af2_sys="/usr/local/bin/af2udp"
af2_cmd="$(find_first_cmd af2udp || true)"
if [[ "$af2_cmd" == /opt/local/* ]]; then
  af2_cmd=""
fi
af2_path="$(find_first_executable_path "$af2_cmd" "$af2_local" "$af2_sys" || true)"

sox_path="$(find_first_cmd sox || true)"

venv_py=""
if [ -x "$ROOT_DIR/.venv-py3/bin/python" ]; then
  venv_py="$ROOT_DIR/.venv-py3/bin/python"
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  venv_py="$ROOT_DIR/.venv/bin/python"
fi

if [ "$BUILD_MISSING" = "1" ] && { [ -z "$ft8_path" ] || [ -z "$af2_path" ]; }; then
  if try_build_ft8modem; then
    ft8_cmd="$(find_first_cmd ft8modem || true)"
    af2_cmd="$(find_first_cmd af2udp || true)"
    if [[ "$ft8_cmd" == /opt/local/* ]]; then
      ft8_cmd=""
    fi
    if [[ "$af2_cmd" == /opt/local/* ]]; then
      af2_cmd=""
    fi
    ft8_path="$(find_first_executable_path "$ft8_cmd" "$ft8_local" "$ft8_sys" || true)"
    af2_path="$(find_first_executable_path "$af2_cmd" "$af2_local" "$af2_sys" || true)"
  fi
fi

say "[check] kiwi_scan root: $ROOT_DIR"
say "[check] ShackMate root: $SHACKMATE_ROOT"

if [ -n "$kiwi_path" ]; then
  say "[ok]  kiwirecorder: $kiwi_path"
else
  echo "[err] kiwirecorder not found (expected $kiwi_vendor or PATH)" >&2
  missing=$((missing+1))
fi

if [ -n "$ft8_path" ]; then
  say "[ok]  ft8modem: $ft8_path"
else
  echo "[err] ft8modem not found (checked PATH excluding /opt/local, $ft8_local, $ft8_sys)" >&2
  missing=$((missing+1))
fi

if [ -n "$af2_path" ]; then
  say "[ok]  af2udp: $af2_path"
else
  echo "[err] af2udp not found (checked PATH excluding /opt/local, $af2_local, $af2_sys)" >&2
  missing=$((missing+1))
fi

if [ -n "$sox_path" ]; then
  say "[ok]  sox: $sox_path"
else
  echo "[err] sox not found on PATH" >&2
  missing=$((missing+1))
fi

if [ -n "$venv_py" ]; then
  if "$venv_py" -c "import fastapi,uvicorn,websockets,numpy" >/dev/null 2>&1; then
    say "[ok]  python deps (fastapi/uvicorn/websockets/numpy) in $(dirname "$venv_py")"
  else
    echo "[err] missing python deps in $(dirname "$venv_py") (run: $venv_py -m pip install -r $ROOT_DIR/requirements.txt)" >&2
    missing=$((missing+1))
  fi
else
  echo "[err] no project virtualenv found (.venv-py3 or .venv)" >&2
  missing=$((missing+1))
fi

if [ "$missing" -eq 0 ]; then
  echo "Summary: OK (all runtime dependencies present)"
  exit 0
fi

echo "Summary: FAIL ($missing missing dependency checks)" >&2
exit 1
