#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="$ROOT_DIR/pyproject.toml"
NO_PUSH=0
TARGET_VERSION=""

usage() {
  cat <<'EOF'
Usage: tools/release_commit.sh [--version X.Y.Z] [--no-push]

Bumps kiwi-scan version in pyproject.toml, commits all changes, and pushes.

Options:
  --version X.Y.Z  Set explicit version instead of auto patch bump
  --no-push        Commit only, do not push
  -h, --help       Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      TARGET_VERSION="${2:-}"
      shift 2
      ;;
    --no-push)
      NO_PUSH=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cd "$ROOT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: not in a git repository: $ROOT_DIR" >&2
  exit 2
fi

if [[ ! -f "$PYPROJECT" ]]; then
  echo "Error: missing pyproject.toml at $PYPROJECT" >&2
  exit 2
fi

CURRENT_VERSION="$(python3 - <<'PY'
import re
from pathlib import Path
raw = Path('pyproject.toml').read_text(encoding='utf-8')
m = re.search(r'^version\s*=\s*"([^"]+)"\s*$', raw, flags=re.MULTILINE)
print(m.group(1) if m else '')
PY
)"

if [[ -z "$CURRENT_VERSION" ]]; then
  echo "Error: could not parse current version from pyproject.toml" >&2
  exit 2
fi

if [[ -z "$TARGET_VERSION" ]]; then
  TARGET_VERSION="$(python3 - "$CURRENT_VERSION" <<'PY'
import sys
v = sys.argv[1].strip()
parts = v.split('.')
if len(parts) != 3 or not all(p.isdigit() for p in parts):
    raise SystemExit('error')
major, minor, patch = map(int, parts)
print(f"{major}.{minor}.{patch+1}")
PY
)" || {
    echo "Error: auto-bump requires semantic version X.Y.Z, got: $CURRENT_VERSION" >&2
    exit 2
  }
fi

if ! [[ "$TARGET_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: --version must be semantic X.Y.Z, got: $TARGET_VERSION" >&2
  exit 2
fi

if [[ "$TARGET_VERSION" == "$CURRENT_VERSION" ]]; then
  echo "Error: target version equals current version ($CURRENT_VERSION)" >&2
  exit 2
fi

python3 - "$TARGET_VERSION" <<'PY'
import re
import sys
from pathlib import Path
new_v = sys.argv[1]
p = Path('pyproject.toml')
raw = p.read_text(encoding='utf-8')
out, n = re.subn(
    r'^(version\s*=\s*")([^"]+)("\s*)$',
    rf'\g<1>{new_v}\g<3>',
    raw,
    flags=re.MULTILINE,
)
if n != 1:
    raise SystemExit('failed to update version')
p.write_text(out, encoding='utf-8')
PY

git add pyproject.toml
git add -A

if git diff --cached --quiet; then
  echo "No changes to commit after version bump." >&2
  exit 1
fi

COMMIT_MSG="Release v$TARGET_VERSION"
git commit -m "$COMMIT_MSG"

if [[ "$NO_PUSH" == "1" ]]; then
  echo "Committed $COMMIT_MSG (push skipped)."
  exit 0
fi

git push origin main

echo "Released $TARGET_VERSION and pushed to origin/main."