"""
InfoFlow Content API Router.

Proxies requests to external InfoFlow REST API for content feed data.
Supports server-side category filtering since the external API does not.

Endpoints:
- GET /api/v1/infoflow/results - List results with optional category filter
- GET /api/v1/infoflow/results/{index_number} - Get result detail
"""

import logging
import os
import time as _time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/infoflow", tags=["InfoFlow"])

VALID_CATEGORIES = {"hot_topic", "market", "industry"}

# In-memory TTL cache for the 180-item external API fetch.
# Shared across category requests so only one external call is made.
_results_cache_data: list | None = None
_results_cache_ts: float = 0.0
_CACHE_TTL: float = 300.0  # 5 minutes


def _get_config():
    base_url = os.getenv("INFOFLOW_BASE_URL", "").rstrip("/")
    api_key = os.getenv("INFOFLOW_API_KEY", "")
    return base_url, api_key


def _build_headers(api_key: str) -> dict:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


@router.get("/results")
async def get_infoflow_results(
    category: Optional[str] = Query(None, description="Filter: hot_topic, market, industry"),
    limit: int = Query(10, ge=1, le=50),
    offset: int = Query(0, ge=0),
):
    """
    Fetch InfoFlow results, optionally filtered by category.
    Returns empty results if INFOFLOW_BASE_URL is not configured.
    """
    base_url, api_key = _get_config()

    if not base_url:
        return {"results": [], "total": 0, "limit": limit, "offset": offset, "has_more": False}

    if category and category not in VALID_CATEGORIES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid category. Must be one of: {', '.join(VALID_CATEGORIES)}",
        )

    try:
        url = f"{base_url}/api/v2/infoflow/results"
        headers = _build_headers(api_key)
        need = offset + limit  # total filtered items we need to collect

        if not category:
            # No filtering needed, pass through directly
            params = {"limit": limit, "offset": offset, "display_locale": "en-US"}
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                data = resp.json()
            results = data.get("results", [])
            # Use pagination info from external API if available
            ext_total = data.get("pagination", {}).get("total", len(results))
            return {
                "results": results,
                "total": ext_total,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < ext_total,
            }

        # With category filter: use cached results or fetch once
        global _results_cache_data, _results_cache_ts
        now = _time.time()

        if _results_cache_data is not None and (now - _results_cache_ts) < _CACHE_TTL:
            all_results = _results_cache_data
        else:
            fetch_size = 180
            params = {"limit": fetch_size, "offset": 0, "display_locale": "en-US"}
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                data = resp.json()
            all_results = data.get("results", [])
            _results_cache_data = all_results
            _results_cache_ts = now
        filtered = [r for r in all_results if r.get("category") == category]

        total = len(filtered)
        page = filtered[offset: offset + limit]

        return {
            "results": page,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < total,
        }
    except httpx.HTTPStatusError as e:
        logger.error(f"InfoFlow API error: {e.response.status_code} {e.response.text[:200]}")
        raise HTTPException(status_code=502, detail="InfoFlow API returned an error")
    except Exception as e:
        logger.error(f"Failed to fetch InfoFlow results: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch InfoFlow data")


@router.get("/results/{index_number}")
async def get_infoflow_detail(index_number: str):
    """Fetch detail for a specific InfoFlow result by indexNumber."""
    base_url, api_key = _get_config()

    if not base_url:
        raise HTTPException(status_code=404, detail="InfoFlow API not configured")

    try:
        url = f"{base_url}/api/v2/infoflow/results/index/{index_number}"
        params = {"display_locale": "en-US"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params=params, headers=_build_headers(api_key))
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Result not found")
        logger.error(f"InfoFlow detail API error: {e.response.status_code}")
        raise HTTPException(status_code=502, detail="InfoFlow API returned an error")
    except Exception as e:
        logger.error(f"Failed to fetch InfoFlow detail for {index_number}: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch InfoFlow detail")
