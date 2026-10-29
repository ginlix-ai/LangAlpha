"""
SEC EDGAR Document Proxy.

Proxies requests to SEC EDGAR to bypass CORS restrictions for iframe embedding.
SEC filings are immutable once published, so aggressive caching is safe.
"""

import logging
import re
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sec-proxy", tags=["SEC Proxy"])

# Allowed SEC domains for security
ALLOWED_HOSTS = {"www.sec.gov", "sec.gov", "efts.sec.gov"}

# SEC requires a User-Agent with contact info
SEC_USER_AGENT = "PTC-Agent contact@example.com"

# The viewer frames a filing in a rounded panel, and a filing rarely styles a
# scrollbar of its own, so the browser's would run flush against that panel's
# edge. This one matches the app's (web/src/styles/tokens.css). A filing gets
# no theme, so its thumb is a fixed grey that reads on a white page.
_FILING_SCROLLBARS = (
    b"<style>"
    b"::-webkit-scrollbar{width:12px;height:12px}"
    b"::-webkit-scrollbar-track{background:transparent;margin:4px}"
    b"::-webkit-scrollbar-thumb{background-color:rgba(128,128,128,.45);"
    b"background-clip:padding-box;border:3px solid transparent;border-radius:6px}"
    b"::-webkit-scrollbar-thumb:hover{background-color:rgba(128,128,128,.7)}"
    b"</style>"
)
# A filing's <head> opens within its first few kilobytes. Bounding the search
# keeps a multi-megabyte filing without one, or with a run of unclosed "<head",
# from tying up the event loop on a scan of the whole document.
_HEAD_OPEN = re.compile(rb"<head\b[^>]{0,1024}>", re.IGNORECASE)
_HEAD_WINDOW = 64 * 1024
_STYLES_OWN_SCROLLBAR = re.compile(rb"-webkit-scrollbar", re.IGNORECASE)


def _with_filing_scrollbars(body: bytes) -> bytes:
    """Splice the scrollbar style in after <head>.

    Never ahead of the doctype, where it would drop the filing into quirks
    mode, so a document without a <head> keeps the browser's scrollbar. A
    filing with WebKit scrollbar rules of its own keeps those whole: the two
    would merge rule by rule, and a narrow scrollbar less the inset paints no
    thumb at all.
    """
    if _STYLES_OWN_SCROLLBAR.search(body):
        return body
    match = _HEAD_OPEN.search(body, 0, _HEAD_WINDOW)
    if match is None:
        return body
    return body[: match.end()] + _FILING_SCROLLBARS + body[match.end() :]


@router.get("/document")
async def proxy_sec_document(
    url: str = Query(..., description="SEC EDGAR document URL"),
):
    """Proxy SEC EDGAR documents to bypass CORS for iframe embedding."""
    # Validate URL domain
    try:
        parsed = urlparse(url)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URL")

    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        raise HTTPException(
            status_code=400,
            detail="Only SEC EDGAR URLs (sec.gov) are allowed",
        )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                headers={"User-Agent": SEC_USER_AGENT},
                follow_redirects=True,
            )
            resp.raise_for_status()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="SEC EDGAR request timed out")
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"SEC EDGAR returned {e.response.status_code}",
        )
    except httpx.HTTPError as e:
        logger.warning(f"SEC proxy fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch from SEC EDGAR")

    content_type = resp.headers.get("content-type", "text/html")
    content = resp.content
    if content_type.split(";", 1)[0].strip().lower() == "text/html":
        content = _with_filing_scrollbars(content)

    return Response(
        content=content,
        media_type=content_type,
        headers={
            "Cache-Control": "public, max-age=86400",
        },
    )
