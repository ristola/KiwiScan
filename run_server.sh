#!/usr/bin/env bash
# Helper to run the FastAPI server with the project's src on PYTHONPATH
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PRIMARY="$ROOT_DIR/.venv-py3"
VENV_FALLBACK="$ROOT_DIR/.venv"
VENV_PY=""
APP_DIR="$ROOT_DIR/src"
REQ_FILE="$ROOT_DIR/requirements.txt"
AUTO_SETUP="${AUTO_SETUP:-1}"
AUTO_SYSTEM_DEPS="${AUTO_SYSTEM_DEPS:-1}"

ensure_venv() {
  if [ -x "$VENV_PRIMARY/bin/python" ]; then
    VENV_PY="$VENV_PRIMARY/bin/python"
    return 0
  fi
  if [ -x "$VENV_FALLBACK/bin/python" ]; then
    VENV_PY="$VENV_FALLBACK/bin/python"
    return 0
  fi
  if [ "$AUTO_SETUP" != "1" ]; then
    echo "Error: venv python not found at $VENV_PRIMARY/bin/python (or $VENV_FALLBACK/bin/python)" >&2
    echo "Create a venv first: python3 -m venv .venv-py3; . .venv-py3/bin/activate; python -m pip install -r requirements.txt" >&2
    echo "See $ROOT_DIR/.env.example for preferred local setup/run/test commands." >&2
    exit 2
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 is required to auto-create .venv-py3" >&2
    exit 2
  fi
  echo "[setup] Creating virtual environment at $VENV_PRIMARY" >&2
  python3 -m venv "$VENV_PRIMARY"
  VENV_PY="$VENV_PRIMARY/bin/python"
}

install_requirements_if_needed() {
  if "$VENV_PY" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
    return 0
  fi
  if [ "$AUTO_SETUP" != "1" ]; then
    echo "Error: required Python packages missing in virtualenv (fastapi/uvicorn)" >&2
    echo "Install them with: $VENV_PY -m pip install -r $REQ_FILE" >&2
    echo "See $ROOT_DIR/.env.example for preferred local setup/run/test commands." >&2
    exit 2
  fi
  echo "[setup] Installing Python requirements" >&2
  "$VENV_PY" -m pip install -U pip
  if [ -f "$REQ_FILE" ]; then
    "$VENV_PY" -m pip install -r "$REQ_FILE"
  else
    "$VENV_PY" -m pip install -e "$ROOT_DIR"
  fi
}

bootstrap_runtime_tools_if_needed() {
  if [ "$AUTO_SETUP" != "1" ] || [ "$AUTO_SYSTEM_DEPS" != "1" ]; then
    return 0
  fi

  if [ -x "$ROOT_DIR/tools/check_runtime_deps.sh" ]; then
    echo "[setup] Checking/building runtime tools (ft8modem/af2udp)" >&2
    "$ROOT_DIR/tools/check_runtime_deps.sh" --build-missing --quiet >/dev/null 2>&1 || true
  fi

  if command -v sox >/dev/null 2>&1; then
    return 0
  fi

  if [ "$(uname -s)" = "Darwin" ]; then
    if command -v brew >/dev/null 2>&1; then
      echo "[setup] sox missing; installing via Homebrew" >&2
      brew list sox >/dev/null 2>&1 || brew install sox || true
    else
      echo "[setup] sox missing and Homebrew not found; install Homebrew or run: brew install sox" >&2
    fi
  fi
}

ensure_venv
install_requirements_if_needed
bootstrap_runtime_tools_if_needed

PORT="${PORT:-4020}"
RESTART_DELAY_S="${RESTART_DELAY_S:-2}"
ALWAYS_RESTART="${ALWAYS_RESTART:-0}"
NO_RESTART="${NO_RESTART:-0}"
AUTO_RELOAD="${AUTO_RELOAD:-0}"

echo "Starting uvicorn using: $VENV_PY (app-dir=$APP_DIR, port=$PORT)"
echo "Tip: see $ROOT_DIR/.env.example for preferred .venv-py3 activation and common run/test commands."

while true; do
  if lsof -ti tcp:"$PORT" >/dev/null 2>&1; then
    echo "Port $PORT is in use; stopping existing server..." >&2
    pids=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
    if [ -n "$pids" ]; then
      kill $pids >/dev/null 2>&1 || true
      for _ in 1 2 3 4 5; do
        if ! lsof -ti tcp:"$PORT" >/dev/null 2>&1; then
          break
        fi
        sleep 0.4
      done
      if lsof -ti tcp:"$PORT" >/dev/null 2>&1; then
        echo "Port $PORT still busy; forcing stop." >&2
        kill -9 $pids >/dev/null 2>&1 || true
      fi
    fi
  fi

  set +e
  if [ "$AUTO_RELOAD" = "1" ]; then
    "$VENV_PY" -m uvicorn --app-dir "$APP_DIR" kiwi_scan.server:app --host 0.0.0.0 --port "$PORT" --reload --reload-dir "$APP_DIR"
    code=$?
  else
    "$VENV_PY" -m uvicorn --app-dir "$APP_DIR" kiwi_scan.server:app --host 0.0.0.0 --port "$PORT"
    code=$?
  fi
  set -e
  if [ "${NO_RESTART}" = "1" ]; then
    exit "$code"
  fi

  # If uvicorn exits cleanly (e.g. Ctrl+C), default behavior is to stop.
  # Set ALWAYS_RESTART=1 if you want the old "restart forever" behavior.
  if [ "$code" = "0" ] && [ "${ALWAYS_RESTART}" != "1" ]; then
    exit 0
  fi

  echo "uvicorn exited with code $code; restarting in ${RESTART_DELAY_S}s..." >&2
  sleep "$RESTART_DELAY_S"
done
