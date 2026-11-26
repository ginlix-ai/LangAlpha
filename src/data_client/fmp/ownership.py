"""FMP's 13F holders of a security as get_institutional_holders publishes them (the build without LangAlpha Data).

FMP serves a quarter's holders 100 to a page, in no stated order, with sold-out positions and option lines among
them, and its positions summary counts holders differently from what the pages hold. So the whole list is read once
per symbol and quarter, the last few lists kept for an hour, and ranked here, most shares first then by filer CIK,
and holders_total counts the holders ranked. FMP files no accession and decides no value units, so both stay null.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import httpx

from data_client.edgar_access import edgar_identity
from data_client.ttl_cache import TtlCache

logger = logging.getLogger(__name__)

PAGE = 100
MAX_PAGES = 300  # 30,000 rows, past the most widely held name
_BATCH = 5
_TTL_S = 3600.0
_DEPOSITARY = re.compile(r"\bAD[RS]S?\b|DEPOSIT[AO]R", re.IGNORECASE)
_SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"

_ranked: TtlCache[list[dict[str, Any]]] = TtlCache(size=16, ttl=_TTL_S)
_classes: tuple[float, dict[str, int], dict[int, int]] | None = None


async def _all_rows(client: Any, symbol: str, year: int, quarter: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for start in range(0, MAX_PAGES, _BATCH):
        pages = [asyncio.ensure_future(client.get_institutional_holders(symbol, year, quarter, page=p, limit=PAGE))
                 for p in range(start, start + _BATCH)]
        try:
            for page in pages:  # in page order: a short page ends the list, so no page after it is waited for
                got = await page
                rows.extend(got or [])
                if len(got or []) < PAGE:
                    return rows
        finally:
            for page in pages:
                page.cancel()
            await asyncio.gather(*pages, return_exceptions=True)
    logger.warning("fmp holders of %s %sQ%s stopped at %d pages", symbol, year, quarter, MAX_PAGES)
    return rows


async def _share_classes(symbol: str) -> int | None:
    """How many tickers SEC's ticker map gives the issuer of *symbol* (2 or more for a multi-class issuer), or
    None when the map cannot be read or lists no such ticker."""
    global _classes
    if _classes is None or time.monotonic() - _classes[0] > 24 * _TTL_S:
        try:
            async with httpx.AsyncClient(timeout=30, headers={"User-Agent": edgar_identity()}) as http:
                resp = await http.get(_SEC_TICKERS)
                resp.raise_for_status()
                entries = resp.json().values()
        except Exception:  # noqa: BLE001 - pctheld is left null instead
            logger.warning("SEC ticker map unavailable; pctheld left null", exc_info=True)
            return None
        ciks = {str(e["ticker"]).upper(): int(e["cik_str"]) for e in entries}
        counts: dict[int, int] = {}
        for cik in ciks.values():
            counts[cik] = counts.get(cik, 0) + 1
        _classes = (time.monotonic(), ciks, counts)
    cik = _classes[1].get(symbol.upper().replace(".", "-"))
    return None if cik is None else _classes[2][cik]


def rank(rows: list[dict[str, Any]], *, pctheld: bool) -> list[dict[str, Any]]:
    """FMP's rows as holder records: held share positions only, one per filer (FMP's repeated rows dropped, a
    filer's remaining rows added up), most shares first, then filer CIK. pctheld is FMP's ownership as a fraction
    when *pctheld*, else null; it is null for a depositary security either way. FMP rounds ownership to four
    decimals of a percent, so every holder's pctheld divides by the shares count the largest holder's ownership
    implies, not by its own rounded figure."""
    seen: set[tuple] = set()
    by_cik: dict[str, dict[str, Any]] = {}
    for r in rows:
        if r.get("putCallShare") != "Share" or r.get("isSoldOut") or not r.get("sharesNumber"):
            continue
        key = (r.get("cik"), r.get("securityCusip"), r.get("investmentDiscretion"), r.get("sharesNumber"),
               r.get("marketValue"), r.get("filingDate"))
        if key in seen:
            continue
        seen.add(key)
        h = by_cik.setdefault(str(r.get("cik")), {
            "holder": r.get("investorName"), "filer_cik": r.get("cik"), "shares": 0, "value": 0, "ownership": 0.0,
            "last": 0, "new": False, "filing_date": None, "depositary": False,
        })
        h["shares"] += r["sharesNumber"]
        h["value"] = None if h["value"] is None or r.get("marketValue") is None else h["value"] + r["marketValue"]
        h["ownership"] += r.get("ownership") or 0.0
        h["last"] += r.get("lastSharesNumber") or 0
        h["new"] = h["new"] or bool(r.get("isNew"))
        h["filing_date"] = max(filter(None, (h["filing_date"], str(r.get("filingDate") or "")[:10] or None)),
                               default=None)
        h["depositary"] = h["depositary"] or bool(_DEPOSITARY.search(
            f"{r.get('typeOfSecurity') or ''} {r.get('securityName') or ''}"))
    top = max(by_cik.values(), key=lambda h: h["shares"], default=None)
    outstanding = top["shares"] * 100 / top["ownership"] if top and top["ownership"] else None
    out = [{
        "holder": h["holder"], "filer_cik": h["filer_cik"], "shares": h["shares"], "value": h["value"],
        "pctheld": round(h["shares"] / outstanding, 6) if pctheld and outstanding and not h["depositary"] else None,
        "pctchange": round((h["shares"] - h["last"]) / h["last"], 4) if h["last"] and not h["new"] else None,
        "value_status": None, "filing_date": h["filing_date"], "accession": None,
    } for h in by_cik.values()]
    return sorted(out, key=lambda h: (-h["shares"], h["filer_cik"] or ""))


async def holders(client: Any, symbol: str, period: str) -> list[dict[str, Any]]:
    """Every 13F holder of *symbol* for the quarter ending *period*, ranked (see :func:`rank`), kept an hour."""
    key = (symbol.upper(), period)
    if (hit := _ranked.get(key)) is not None:
        return hit
    year, month = int(period[:4]), int(period[5:7])
    rows, classes = await asyncio.gather(_all_rows(client, symbol, year, (month - 1) // 3 + 1),
                                         _share_classes(symbol))
    ranked = rank(rows, pctheld=classes == 1)
    _ranked.put(key, ranked)
    return ranked
