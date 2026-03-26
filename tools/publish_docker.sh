#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="$ROOT_DIR/pyproject.toml"
SOURCE_IMAGE="kiwiscan-kiwiscan:latest"
TARGET_REPO="n4ldr/kiwiscan"
DO_BUILD=0
DO_LATEST=1

usage() {
  cat <<'EOF'
Usage: tools/publish_docker.sh [--build] [--source-image IMAGE] [--repo NAME] [--no-latest]

Tags the current local KiwiScan image with the version from pyproject.toml,
then pushes the versioned tag and, by default, the latest tag to Docker Hub.

Options:
  --build               Build the local source image before tagging and pushing
  --source-image IMAGE  Local image to publish (default: kiwiscan-kiwiscan:latest)
  --repo NAME           Target repo (default: n4ldr/kiwiscan)
  --no-latest           Push only the versioned tag
  -h, --help            Show this help
EOF
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
    --no-latest)
      DO_LATEST=0
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

if [[ "$DO_BUILD" == "1" ]]; then
  docker build -t "$SOURCE_IMAGE" .
fi

if ! docker image inspect "$SOURCE_IMAGE" >/dev/null 2>&1; then
  echo "Error: source image not found: $SOURCE_IMAGE" >&2
  echo "Hint: run tools/publish_docker.sh --build or build the image first." >&2
  exit 2
fi

VERSION_TAG="$TARGET_REPO:$VERSION"
LATEST_TAG="$TARGET_REPO:latest"

docker tag "$SOURCE_IMAGE" "$VERSION_TAG"
docker push "$VERSION_TAG"

if [[ "$DO_LATEST" == "1" ]]; then
  docker tag "$SOURCE_IMAGE" "$LATEST_TAG"
  docker push "$LATEST_TAG"
fi

echo "Published $VERSION_TAG"
if [[ "$DO_LATEST" == "1" ]]; then
  echo "Published $LATEST_TAG"
fi