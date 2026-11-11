"""Company profiles for many listings, each cached on its own instrument.

A profile barely moves, so it is kept a day; a listing with none is kept an
hour, so a bad ticker on a dashboard is not asked for on every refresh. Reads
are one MGET, and the misses are fetched side by side.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from src.data_client import get_financial_data_provider
from src.data_client.ginlix_data.directory import display_names_many, profile_name
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

_PROFILE_TTL = 24 * 3600
_NO_PROFILE_TTL = 3600
_FETCH_CONCURRENCY = 8
# Per listing, so one upstream that hangs fails alone instead of holding a
# slot while the rest of the request waits behind it.
_FETCH_TIMEOUT_S = 8.0
_FAILED = "Failed to fetch profile"


@dataclass
class Profiles:
    #: By listing: its fields, or None for a listing with no profile.
    found: dict[str, Optional[dict[str, Any]]] = field(default_factory=dict)
    #: By listing: why it has no answer this time; never cached.
    failed: dict[str, str] = field(default_factory=dict)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def profile_fields(
    row: dict[str, Any], local_name: Optional[str]
) -> Optional[dict[str, Any]]:
    """A provider's profile row in the route's fields, or None when it names
    nothing. yfinance and the CN source say ``marketCap`` where older FMP rows
    said ``mktCap``."""
    cap = row.get("marketCap", row.get("mktCap"))
    fields = {
        "name": profile_name(row, local_name),
        "sector": row.get("sector") or None,
        "industry": row.get("industry") or None,
        "marketCap": float(cap) if _is_number(cap) else None,
        "currency": row.get("currency") or None,
        "exchange": row.get("exchangeShortName") or row.get("exchange") or None,
        "country": row.get("country") or None,
    }
    return fields if any(v is not None for v in fields.values()) else None


def _profile_row(rows: Any) -> Optional[dict[str, Any]] | bool:
    """The first row, None for an answer with none (an empty row is one), or
    False for a vendor's error body on a 200 (``{"Error Message": ...}``),
    which cached as "no profile" would hide the listing for an hour after the
    vendor recovers."""
    if not isinstance(rows, list):
        return False
    first = rows[0] if rows else None
    if not isinstance(first, dict) or not first:
        return None
    return False if "Error Message" in first else first


async def get_profiles(listings: list[str]) -> Profiles:
    """Profiles for ``listings`` (each spelled as the providers know it)."""
    out = Profiles()
    if not listings:
        return out
    cache = get_cache_client()
    # A cached miss is stored as {"profile": None}.
    hits = await cache.mget([f"profile:{s}" for s in listings])
    for listing, hit in zip(listings, hits):
        if isinstance(hit, dict) and "profile" in hit:
            out.found[listing] = hit["profile"]
    missing = [s for s in listings if s not in out.found]
    if not missing:
        return out
    try:
        provider = await get_financial_data_provider()
    except Exception as e:
        # The cached hits still answer; the rest fail one by one.
        logger.warning("Financial data provider unavailable: %s", type(e).__name__)
        provider = None
    if provider is None or provider.financial is None:
        out.failed.update({s: "No financial data provider available" for s in missing})
        return out

    gate = asyncio.Semaphore(_FETCH_CONCURRENCY)
    rows: dict[str, Optional[dict[str, Any]]] = {}

    async def fetch(listing: str) -> None:
        async with gate:
            try:
                answer = await asyncio.wait_for(
                    provider.financial.get_company_profile(listing), _FETCH_TIMEOUT_S
                )
            except Exception as e:
                # An upstream error's text can carry the request URL.
                logger.warning(
                    "Profile fetch failed for %s: %s", listing, type(e).__name__
                )
                out.failed[listing] = _FAILED
                return
        row = _profile_row(answer)
        if row is False:
            logger.warning(
                "Profile fetch for %s answered %s", listing, type(answer).__name__
            )
            out.failed[listing] = _FAILED
            return
        rows[listing] = row

    await asyncio.gather(*(fetch(s) for s in missing))
    names = await display_names_many([s for s, row in rows.items() if row])
    for listing, row in rows.items():
        out.found[listing] = (
            profile_fields(row, names.get(listing, (None, None))[0])
            if row is not None
            else None
        )
    writes = [
        (
            f"profile:{s}",
            {"profile": out.found[s]},
            _PROFILE_TTL if out.found[s] else _NO_PROFILE_TTL,
        )
        for s in rows
    ]
    if writes:
        await cache.set_many(writes)
    return out
