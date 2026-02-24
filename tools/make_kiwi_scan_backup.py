#!/usr/bin/env python3
"""Create a kiwi_scan-only ZIP backup under /opt/ShackMate/backup.

Defaults exclude virtualenv, outputs, and caches.

Usage:
  python3 kiwi_scan/tools/make_kiwi_scan_backup.py
  python3 kiwi_scan/tools/make_kiwi_scan_backup.py --include-outputs
    python3 kiwi_scan/tools/make_kiwi_scan_backup.py --include-venv

Output:
  /opt/ShackMate/backup/kiwi_scan_only_YYYYmmdd_HHMMSS.zip
"""

from __future__ import annotations

import argparse
import fnmatch
import time
import zipfile
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-outputs", action="store_true", help="Include kiwi_scan/outputs")
    ap.add_argument(
        "--include-venv",
        action="store_true",
        help="Include kiwi_scan/.venv-py3 and kiwi_scan/.venv",
    )
    ap.add_argument("--compresslevel", type=int, default=6, help="ZIP compression level (0-9)")
    args = ap.parse_args()

    root = Path("/opt/ShackMate").resolve()
    base = root / "kiwi_scan"
    if not base.is_dir():
        raise SystemExit(f"Expected {base} to exist")

    backup_dir = root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out = backup_dir / f"kiwi_scan_only_{ts}.zip"

    excludes = [
        "kiwi_scan/**/__pycache__/*",
        "kiwi_scan/**/*.pyc",
        "kiwi_scan/**/.pytest_cache/*",
        "kiwi_scan/**/.mypy_cache/*",
        "kiwi_scan/**/.ruff_cache/*",
        "kiwi_scan/**/.DS_Store",
        "kiwi_scan/**/build/*",
        "kiwi_scan/dist/*",
        "kiwi_scan/*.egg-info/*",
    ]
    if not args.include_outputs:
        excludes.append("kiwi_scan/outputs/*")
    if not args.include_venv:
        excludes.append("kiwi_scan/.venv-py3/*")
        excludes.append("kiwi_scan/.venv/*")

    def is_excluded(rel: str) -> bool:
        rel = rel.replace("\\", "/")
        return any(fnmatch.fnmatch(rel, pat) for pat in excludes)

    file_count = 0
    with zipfile.ZipFile(
        out,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=max(0, min(9, int(args.compresslevel))),
    ) as z:
        for p in base.rglob("*"):
            if p.is_dir():
                continue
            rel = p.relative_to(root).as_posix()  # kiwi_scan/...
            if is_excluded(rel):
                continue
            z.write(p, arcname=rel)
            file_count += 1

    size_mib = out.stat().st_size / (1024 * 1024)
    print(f"Created: {out}")
    print(f"Files:   {file_count}")
    print(f"Size:    {size_mib:.1f} MiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
