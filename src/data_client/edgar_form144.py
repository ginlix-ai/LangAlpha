"""An issuer's Form 144 notices read from EDGAR through edgar_engine (the build without LangAlpha Data).

A page costs EDGAR the issuer's filing list plus one request per notice on the page; notices before the offset are
never fetched, and raw notices are kept by accession in edgar_engine's submission cache, so a page read again asks
only for the list. Rows take the shape LangAlpha Data's proposed_insider_sales route serves: one per notice and security
class, newest filed first.
"""

from __future__ import annotations

import asyncio
from typing import Any

from data_client.edgar_access import edgar_identity, filing_url, unknown_company


def _row(r: dict[str, Any]) -> dict[str, Any]:
    accession = r["accession"]
    return {
        "filing_date": r["filed"], "seller": r["seller"],
        "seller_cik": None if r["seller_cik"] is None else f"{int(r['seller_cik']):010d}",
        "relationship": r["relationship"], "security_class": r["security_class"],
        "shares_to_sell": r["shares_to_sell"], "market_value": r["market_value"],
        "approx_sale_date": r["approx_sale_date"], "plan_adoption_date": r["plan_adoption_date"],
        "acquired": r["acquired"], "sold_past_3_months": r["sold_past_3_months"], "accession": accession,
        "url": filing_url(r["issuer_cik"], accession), "form": r["form"], "amends": r["previous_accession"],
    }


def _read(symbol: str, limit: int, offset: int) -> dict[str, Any] | None:
    from edgar_engine import get_form144_notices
    from edgar_engine.cache import SubmissionCache
    from edgar_engine.ownership import notice_rows

    edgar_identity()
    try:
        got = get_form144_notices(symbol, limit=limit, offset=offset, submissions=SubmissionCache())
    except Exception as exc:
        if unknown_company(exc):
            return None
        raise
    return {"rows": [_row(r) for n in got.notices for r in notice_rows(n)], "total": got.total,
            "next_offset": got.next_offset}


async def notices(symbol: str, *, limit: int, offset: int) -> dict[str, Any] | None:
    """A page of *symbol*'s Form 144 and 144/A notices as issuer (``rows``, ``total`` and ``next_offset``, the
    last two counting notices), newest filed first; None when EDGAR knows no such company."""
    return await asyncio.to_thread(_read, symbol, limit, offset)
