"""A 13F manager's portfolio read from EDGAR through edgar_engine (the build without LangAlpha Data).

Each answer costs EDGAR a few requests: the manager's filing list and its reports for the quarter and the one before
(amendments resolved by the engine). Raw reports are kept by accession in edgar_engine's submission cache, and the
last few portfolios for an hour, so the next page asks nothing; a manager named rather than given by CIK is found
through data_client.edgar_13f_filers. Value units follow the report: thousands before 2023 (and where the report's own
implied prices say so) are rescaled to dollars, as_filed otherwise; no peer check runs here, so no value is excluded.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from data_client.edgar_access import edgar_identity, filing_url, unknown_company
from data_client.ttl_cache import TtlCache

logger = logging.getLogger(__name__)

_portfolios: TtlCache[dict[str, Any]] = TtlCache(size=8, ttl=3600.0)


class UnreadableReports(Exception):
    """Every 13F holdings report a filer listed for the period failed to read; url is the first one's filing."""

    def __init__(self, url: str):
        super().__init__(url)
        self.url = url


def _lines(filings: list[Any]) -> list[dict[str, Any]]:
    from edgar_engine.thirteenf import tickers_for_cusips, value_multiplier

    out = []
    for f in filings:
        mult, status = value_multiplier(f)
        out += [{"accession": f.accession, "issuer": h.issuer, "title_class": h.title_class, "cusip": h.cusip,
                 "sh_prn": h.sh_prn, "put_call": h.put_call, "shares": h.shares,
                 "value": None if h.value is None else h.value * mult, "value_status": status}
                for h in f.holdings]
    tickers = tickers_for_cusips({line["cusip"] for line in out if line["cusip"]})
    return [{**line, "ticker": tickers.get(line["cusip"])} for line in out]


def _read(cik: int, period: str, prior: str) -> dict[str, Any] | None:
    from edgar_engine import get_13f_reports
    from edgar_engine.cache import SubmissionCache
    from edgar_engine.thirteenf import portfolio

    edgar_identity()
    cache = SubmissionCache()
    try:
        now = get_13f_reports(cik, period, submissions=cache)
    except Exception as exc:
        if unknown_company(exc):
            return None
        raise
    lines = _lines(now.reports) if now.reports else []
    if now.unreadable and not lines:
        raise UnreadableReports(filing_url(cik, now.unreadable[0]))
    if not now.reports:
        return None
    prior_lines, prior_reports = None, ()
    if now.unreadable:
        logger.warning("13F reports of CIK %s for %s left out as unreadable: %s", cik, period, now.unreadable)
    else:
        before = get_13f_reports(cik, prior, submissions=cache)
        prior_lines = None if before.unreadable else _lines(before.reports)
        prior_reports = before.reports
    latest = max(now.reports, key=lambda f: f.filed or "")
    confidential = any(f.confidential_omitted for f in (*now.reports, *prior_reports))
    compared = prior_lines is not None and prior_reports and not confidential
    prior_latest = max(prior_reports, key=lambda f: f.filed or "") if compared else None
    return {"filer_name": latest.filer_name, "filing_date": latest.filed, "url": filing_url(cik, latest.accession),
            "prior_url": prior_latest and filing_url(cik, prior_latest.accession),
            "rows": portfolio(lines, prior_lines, omits_confidential=confidential)}


async def manager_portfolio(cik: int, period: str, prior: str) -> dict[str, Any] | None:
    """The manager's positions for *period* against *prior* (edgar_engine.thirteenf.portfolio, ranked), with its
    latest report's name, filing date and filing url, and prior_url, the latest report of *prior* when the quarters
    were compared; None when it filed no holdings report for the period or EDGAR has no such CIK, UnreadableReports when
    a report it filed for the period fails to read and the others list no position. Change and shares_prior stay
    null unless every report of both quarters was read and none leaves holdings out under confidential treatment, so
    a position an unread or confidential part still lists is never taken for sold."""
    key = (cik, period)
    if (hit := _portfolios.get(key)) is not None:
        return hit
    got = await asyncio.to_thread(_read, cik, period, prior)
    if got is not None:
        _portfolios.put(key, got)
    return got
