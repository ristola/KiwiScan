#!/usr/bin/env python3
"""Cleanup generated artifacts in the kiwi_scan project.

This is intentionally conservative by default: it only removes caches.
Use flags to remove outputs, detections logs, and/or the local venv.

Examples:
  python tools/cleanup.py
  python tools/cleanup.py --outputs
  python tools/cleanup.py --outputs --keep-thresholds
  python tools/cleanup.py --detections
    python tools/cleanup.py --venv
  python tools/cleanup.py --dry-run --outputs --detections --venv
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
import signal


# Avoid BrokenPipeError when piping output (e.g. through `head`).
try:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except Exception:
    pass


def rm_tree(path: Path, *, dry_run: bool) -> int:
    if not path.exists():
        return 0
    if dry_run:
        print(f"DRY delete dir: {path}")
        return 1
    shutil.rmtree(path, ignore_errors=True)
    print(f"Deleted dir: {path}")
    return 1


def rm_file(path: Path, *, dry_run: bool) -> int:
    if not path.exists():
        return 0
    if dry_run:
        print(f"DRY delete file: {path}")
        return 1
    try:
        path.unlink()
    except Exception:
        try:
            path.chmod(0o600)
            path.unlink()
        except Exception:
            return 0
    print(f"Deleted file: {path}")
    return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print what would be deleted")
    ap.add_argument("--outputs", action="store_true", help="Delete outputs/ (scan results, logs)")
    ap.add_argument(
        "--keep-thresholds",
        action="store_true",
        help="If deleting outputs/, keep outputs/thresholds_by_band.json",
    )
    ap.add_argument("--detections", action="store_true", help="Delete detections*.jsonl")
    ap.add_argument("--venv", action="store_true", help="Delete .venv-py3/ and .venv/")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]

    deleted = 0

    # Always safe caches
    for p in root.rglob("__pycache__"):
        deleted += rm_tree(p, dry_run=args.dry_run)
    for p in root.rglob(".pytest_cache"):
        deleted += rm_tree(p, dry_run=args.dry_run)
    for p in root.rglob(".mypy_cache"):
        deleted += rm_tree(p, dry_run=args.dry_run)
    for p in root.rglob(".ruff_cache"):
        deleted += rm_tree(p, dry_run=args.dry_run)

    if args.detections:
        for p in root.glob("detections*.jsonl"):
            deleted += rm_file(p, dry_run=args.dry_run)

    if args.outputs:
        out = root / "outputs"
        if args.keep_thresholds and (out / "thresholds_by_band.json").exists():
            tmp = root / "outputs_thresholds_by_band.json.tmp"
            if args.dry_run:
                print(f"DRY preserve: {out / 'thresholds_by_band.json'}")
                deleted += rm_tree(out, dry_run=True)
            else:
                tmp.write_bytes((out / "thresholds_by_band.json").read_bytes())
                rm_tree(out, dry_run=False)
                out.mkdir(parents=True, exist_ok=True)
                (out / "thresholds_by_band.json").write_bytes(tmp.read_bytes())
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass
                print("Restored outputs/thresholds_by_band.json")
            deleted += 1
        else:
            deleted += rm_tree(out, dry_run=args.dry_run)

    if args.venv:
        deleted += rm_tree(root / ".venv-py3", dry_run=args.dry_run)
        deleted += rm_tree(root / ".venv", dry_run=args.dry_run)

    try:
        print(f"Done. Deleted items: {deleted}")
    except BrokenPipeError:
        return 0
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(0)
