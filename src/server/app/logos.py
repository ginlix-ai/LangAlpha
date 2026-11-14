"""A company's logo as an image, relayed from the data service.

No credential, since an ``<img>`` cannot send one and a logo is the company's
own public mark; the data service bounds what an anonymous caller can cost.
Without a data service the answer is a long-lived 404, so a web client draws
the ticker's letter and asks again only the next day.
"""

import logging
from typing import Literal

import httpx
from fastapi import APIRouter, Header, Query, Response

from src.config.env import GINLIX_DATA_ENABLED
from src.data_client.ginlix_data.logos import fetch_logo

logger = logging.getLogger(__name__)

LOGOS_PREFIX = "/api/v1/market-data/logos"

router = APIRouter(prefix=LOGOS_PREFIX, tags=["Market Data"])

# What a logo response carries through: the image, how long it may be kept, and
# the tag a revalidation sends back.
_RELAYED_HEADERS = ("content-type", "cache-control", "etag", "retry-after")


@router.get("/{symbol}")
async def get_ticker_logo(
    symbol: str,
    size: int | None = Query(None),
    format: Literal["webp", "png"] | None = Query(None),
    if_none_match: str | None = Header(None),
) -> Response:
    if not GINLIX_DATA_ENABLED:
        return Response(status_code=404, headers={"Cache-Control": "public, max-age=86400"})
    try:
        upstream = await fetch_logo(symbol, size=size, format=format, if_none_match=if_none_match)
    except httpx.HTTPError as e:
        logger.warning("logo relay failed for %s: %s", symbol, e)
        return Response(status_code=503, headers={"Cache-Control": "no-store", "Retry-After": "60"})
    headers = {k: upstream.headers[k] for k in _RELAYED_HEADERS if k in upstream.headers}
    if upstream.status_code >= 500:
        # A failure holds only for now, whatever the upstream said about caching it.
        headers["cache-control"] = "no-store"
    return Response(content=upstream.content, status_code=upstream.status_code, headers=headers)
