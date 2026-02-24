from __future__ import annotations

import asyncio
import fnmatch
import threading
import time
import zipfile
from pathlib import Path
from typing import Dict

from fastapi import APIRouter, HTTPException

router = APIRouter()
_backup_lock = threading.Lock()


def _find_shackmate_root() -> Path:
    """Find the ShackMate project root.

    We expect a directory that contains the `kiwi_scan/` folder.
    """

    here = Path(__file__).resolve()
    for p in (here,) + tuple(here.parents):
        if (p / "kiwi_scan").is_dir():
            return p
    # Fallback to a reasonable guess (src/kiwi_scan/api/backup.py -> parents[4] is the workspace root)
    return here.parents[4]


def _create_project_backup_zip(*, include_backup_folder: bool = False) -> Dict[str, object]:
    root = _find_shackmate_root()
    backup_dir = root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out = backup_dir / f"ShackMate_backup_{ts}.zip"

    excludes = [
        ".venv/*",
        "kiwi_scan/.venv/*",
        "**/__pycache__/*",
        "**/*.pyc",
        "**/.pytest_cache/*",
        "**/.mypy_cache/*",
        "**/.ruff_cache/*",
        "**/.DS_Store",
        "kiwi_scan/outputs/*",
        "**/build/*",
    ]

    def is_excluded(rel: str) -> bool:
        rel = rel.replace("\\\\", "/")
        if not include_backup_folder and rel.startswith("backup/"):
            return True
        for pat in excludes:
            if fnmatch.fnmatch(rel, pat):
                return True
        return False

    if out.exists():
        out.unlink()

    file_count = 0
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for path in root.rglob("*"):
            if path.is_dir():
                continue
            if path == out:
                continue
            rel = path.relative_to(root).as_posix()
            if is_excluded(rel):
                continue
            try:
                z.write(path, arcname=rel)
                file_count += 1
            except Exception:
                # best-effort backup
                continue

    size_b = out.stat().st_size
    return {
        "ok": True,
        "backup_zip": str(out),
        "files": int(file_count),
        "size_mib": round(float(size_b) / (1024.0 * 1024.0), 1),
    }


@router.get("/project_backup")
async def project_backup(include_backup_folder: bool = False) -> Dict[str, object]:
    """Create a ZIP backup into /opt/ShackMate/backup.

    This exists because the dev terminal is often busy running the server loop.
    """

    if not _backup_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="backup already running")
    try:
        return await asyncio.to_thread(
            _create_project_backup_zip,
            include_backup_folder=bool(include_backup_folder),
        )
    finally:
        try:
            _backup_lock.release()
        except Exception:
            pass
