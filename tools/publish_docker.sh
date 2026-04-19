#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="$ROOT_DIR/pyproject.toml"
SOURCE_IMAGE="kiwiscan-local:latest"
TARGET_REPO="n4ldr/kiwiscan"
DO_BUILD=0
DO_VERSION=1
DO_LATEST=1
EXTRA_TAGS=()

usage() {
  cat <<'EOF'
Usage: tools/publish_docker.sh [--build] [--source-image IMAGE] [--repo NAME] [--no-version] [--no-latest] [--tag TAG ...]

Tags the current local KiwiScan image with the version from pyproject.toml,
then pushes the selected tags to Docker Hub.

Options:
  --build               Build the local source image before tagging and pushing
  --source-image IMAGE  Local image to publish (default: kiwiscan-local:latest)
  --repo NAME           Target repo (default: n4ldr/kiwiscan)
  --no-version          Skip pushing the version tag from pyproject.toml
  --no-latest           Skip pushing the latest tag
  --tag TAG             Push an additional explicit tag; repeat as needed
  -h, --help            Show this help
EOF
}

append_unique_tag() {
  local candidate="$1"
  local existing

  for existing in "${PUBLISH_TAGS[@]:-}"; do
    if [[ "$existing" == "$candidate" ]]; then
      return 0
    fi
  done

  PUBLISH_TAGS+=("$candidate")
}

validate_tag() {
  local candidate="$1"

  if [[ ! "$candidate" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Error: invalid Docker tag: $candidate" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD=1
      shift
      ;;
    --source-image)
      SOURCE_IMAGE="${2:-}"
      shift 2
      ;;
    --repo)
      TARGET_REPO="${2:-}"
      shift 2
      ;;
    --no-version)
      DO_VERSION=0
      shift
      ;;
    --no-latest)
      DO_LATEST=0
      shift
      ;;
    --tag)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --tag requires a value" >&2
        exit 2
      fi
      EXTRA_TAGS+=("$2")
      shift 2
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

if [[ ! -f "$PYPROJECT" ]]; then
  echo "Error: missing pyproject.toml at $PYPROJECT" >&2
  exit 2
fi

VERSION="$(python3 - <<'PY'
import re
from pathlib import Path
raw = Path('pyproject.toml').read_text(encoding='utf-8')
m = re.search(r'^version\s*=\s*"([^"]+)"\s*$', raw, flags=re.MULTILINE)
print(m.group(1) if m else '')
PY
)"

if [[ -z "$VERSION" ]]; then
  echo "Error: could not parse version from pyproject.toml" >&2
  exit 2
fi

BUILD_COMMIT="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || true)"
if [[ -z "$BUILD_COMMIT" ]]; then
  BUILD_COMMIT="unknown"
fi

if [[ "$DO_BUILD" == "1" ]]; then
  docker build --build-arg GIT_COMMIT="$BUILD_COMMIT" -t "$SOURCE_IMAGE" .
fi

if ! docker image inspect "$SOURCE_IMAGE" >/dev/null 2>&1; then
  echo "Error: source image not found: $SOURCE_IMAGE" >&2
  echo "Hint: run tools/publish_docker.sh --build or build the image first." >&2
  exit 2
fi

PUBLISH_TAGS=()

if [[ "$DO_VERSION" == "1" ]]; then
  append_unique_tag "$VERSION"
fi

if [[ "$DO_LATEST" == "1" ]]; then
  append_unique_tag "latest"
fi

for extra_tag in "${EXTRA_TAGS[@]}"; do
  validate_tag "$extra_tag"
  append_unique_tag "$extra_tag"
done

if [[ "${#PUBLISH_TAGS[@]}" -eq 0 ]]; then
  echo "Error: no tags selected. Use the defaults or provide --tag." >&2
  exit 2
fi

for publish_tag in "${PUBLISH_TAGS[@]}"; do
  full_tag="$TARGET_REPO:$publish_tag"
  docker tag "$SOURCE_IMAGE" "$full_tag"
  docker push "$full_tag"
  echo "Published $full_tag"
done