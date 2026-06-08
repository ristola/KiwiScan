from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

router = APIRouter()

_HTML_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


class _NoCacheHtmlStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs) -> Response:
        response = super().file_response(*args, **kwargs)
        media_type = str(getattr(response, "media_type", "") or "").lower()
        if media_type.startswith("text/html"):
            response.headers.update(_HTML_NO_CACHE_HEADERS)
        return response


def _static_dir() -> Path:
    # Package root is kiwi_scan/; static assets live in kiwi_scan/static/
    return Path(__file__).resolve().parents[1] / "static"


def mount_static(app: FastAPI) -> None:
    """Mount `/static` if the package static dir exists."""

    d = _static_dir()
    if d.is_dir():
        app.mount("/static", _NoCacheHtmlStaticFiles(directory=str(d), html=True), name="static")


@router.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@router.get("/", response_class=HTMLResponse)
def root_index() -> HTMLResponse:
    """Redirect root to the Pro UI."""
    idx = _static_dir() / "pro.html"
    if idx.exists():
        return HTMLResponse(
            content=idx.read_text(encoding="utf-8"),
            media_type="text/html",
            headers=_HTML_NO_CACHE_HEADERS,
        )
    return HTMLResponse(
        content="<html><body><h1>UI not found</h1></body></html>",
        media_type="text/html",
        headers=_HTML_NO_CACHE_HEADERS,
    )


@router.get("/pro", response_class=HTMLResponse)
def pro_index() -> HTMLResponse:
    idx = _static_dir() / "pro.html"
    if idx.exists():
        return HTMLResponse(
            content=idx.read_text(encoding="utf-8"),
            media_type="text/html",
            headers=_HTML_NO_CACHE_HEADERS,
        )
    return HTMLResponse(
        content="<html><body><h1>Pro UI not found</h1></body></html>",
        media_type="text/html",
        headers=_HTML_NO_CACHE_HEADERS,
    )


@router.get("/shackmate-noc", response_class=HTMLResponse)
def shackmate_noc_index() -> HTMLResponse:
    idx = _static_dir() / "shackmate-noc.html"
    if idx.exists():
        return HTMLResponse(
            content=idx.read_text(encoding="utf-8"),
            media_type="text/html",
            headers=_HTML_NO_CACHE_HEADERS,
        )
    return HTMLResponse(
        content="<html><body><h1>Shackmate NOC UI not found</h1></body></html>",
        media_type="text/html",
        headers=_HTML_NO_CACHE_HEADERS,
    )
