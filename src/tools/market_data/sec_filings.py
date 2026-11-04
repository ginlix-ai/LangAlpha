"""An issuer's periodic reports (10-Q, 10-K), read from SEC EDGAR.

FMP's ``sec-filings-search/symbol`` ignores ``formType`` and serves every
filing newest first, so an overview that asked it for 10-Qs printed 8-Ks and
Form 4s under that label, and a heavy filer's real 10-Qs sit thousands of
424B2s deep. EDGAR's full-text search filters by form and CIK in one request
and names each report's period, which the overview labels it by.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict

import httpx

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
# SEC refuses requests without a contact User-Agent.
_USER_AGENT = "OpenSource user@example.com"
_FORMS = ("10-Q", "10-K")
# Long enough to always hold a 10-K and three 10-Qs, whatever the fiscal year.
_WINDOW_DAYS = 450
# The whole lookup, cache included: the overview waits on its slowest fetch.
_DEADLINE_S = 5.0
# The cache is optional, so a slow Redis (up to its 5s socket timeouts) gets a
# slice of the deadline for its read and another for its write, no more.
_CACHE_DEADLINE_S = 0.5
_CACHE_TTL_S = 6 * 3600
_FAILURE_TTL_S = 10 * 60


class PeriodicFiling(TypedDict):
    """One 10-Q or 10-K; a dict so it round-trips the JSON cache as is."""

    form: str
    filingDate: str
    periodOfReport: Optional[str]
    accession: str


def _cache_key(cik: str) -> str:
    return f"sec:periodic_filings:v1:{cik}"


async def _cache():
    from src.utils.cache import get_cache_client

    cache = get_cache_client()
    if not cache.client:
        await cache.connect()
    return cache


async def _read_cache(cik: str):
    cache = await _cache()
    return cache, await cache.get(_cache_key(cik))


async def _bounded(awaitable):
    try:
        return await asyncio.wait_for(awaitable, _CACHE_DEADLINE_S)
    except Exception as e:
        logger.debug(f"Cache not available for SEC filings: {e!r}")
        return None


def _rows(payload: Dict[str, Any]) -> List[PeriodicFiling]:
    """One row per filing, exact form only (no amendments), newest first."""
    seen, out = set(), []
    for hit in (payload.get("hits") or {}).get("hits") or []:
        src = hit.get("_source") or {}
        form, filed = src.get("form"), src.get("file_date")
        accession = src.get("adsh") or str(hit.get("_id", "")).split(":")[0]
        if form not in _FORMS or not filed or accession in seen:
            continue
        seen.add(accession)
        out.append(PeriodicFiling(
            form=form, filingDate=filed, periodOfReport=src.get("period_ending"), accession=accession,
        ))
    out.sort(key=lambda r: r["filingDate"], reverse=True)
    return out


async def _search(cik: str) -> List[PeriodicFiling]:
    today = datetime.now(timezone.utc).date()
    params = {
        "forms": ",".join(_FORMS),
        "ciks": cik,
        "dateRange": "custom",
        "startdt": (today - timedelta(days=_WINDOW_DAYS)).isoformat(),
        "enddt": today.isoformat(),
    }
    verify = os.getenv("EDGAR_VERIFY_SSL", "true").lower() != "false"
    async with httpx.AsyncClient(
        timeout=_DEADLINE_S, headers={"User-Agent": _USER_AGENT}, verify=verify
    ) as client:
        resp = await client.get(_SEARCH_URL, params=params)
        resp.raise_for_status()
        return _rows(resp.json())


async def fetch_periodic_filings(cik: Optional[str]) -> List[PeriodicFiling]:
    """The issuer's 10-Qs and 10-Ks of the last ~15 months, newest first.

    Empty for an issuer without a CIK or on any EDGAR failure: the overview
    drops its filings section rather than print rows it cannot vouch for. The
    overview waits on its slowest fetch, so the lookup, cache included, has one
    deadline, and an empty answer (a 20-F filer) or a failure is cached too,
    the failure briefly.
    """
    if not cik or not str(cik).strip().isdigit():
        return []
    cik = str(cik).strip().zfill(10)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _DEADLINE_S
    cache, cached = await _bounded(_read_cache(cik)) or (None, None)
    if isinstance(cached, list):
        return cached
    try:
        # EDGAR gets what is left, less the write's slice.
        budget = deadline - loop.time() - _CACHE_DEADLINE_S
        rows, ttl = await asyncio.wait_for(_search(cik), budget), _CACHE_TTL_S
    except Exception as e:
        logger.warning(f"EDGAR periodic filings lookup failed for CIK {cik}: {e!r}")
        rows, ttl = [], _FAILURE_TTL_S
    if cache is not None:
        await _bounded(cache.set(_cache_key(cik), rows, ttl=ttl))
    return rows
