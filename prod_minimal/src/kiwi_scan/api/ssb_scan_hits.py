from __future__ import annotations

from fastapi import APIRouter

from ..ssb_scan_hits import clear_ssb_scan_hits, get_ssb_scan_hits

router = APIRouter()


@router.get("/ssb_scan/hits")
def ssb_scan_hits(since: int = 0):
    return get_ssb_scan_hits(since=since)


@router.post("/ssb_scan/hits/clear")
def clear_ssb_hits():
    clear_ssb_scan_hits()
    return {"ok": True}

