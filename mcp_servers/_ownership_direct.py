"""The ownership routes answered without LangAlpha Data: an ``OwnershipReader`` over FMP and EDGAR.

13F holders come from FMP, a manager's portfolio and its name search, and Form 144 notices, from EDGAR through
edgar_engine (set EDGAR_IDENTITY to a name and an email). Each call returns the body its LangAlpha Data route would,
build null since these sources have none, so _ownership answers both builds with the same code. The N-PORT fund
holders, the insider roster and the ownership breakdown need LangAlpha Data's SEC store and raise ``client_unavailable``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx

from data_client import edgar_13f, edgar_13f_filers, edgar_form144
from data_client.edgar_access import engine_available
from data_client.fmp import get_fmp_client
from data_client.fmp import ownership as fmp_ownership
from market_protocol.enums import AssetClass
from market_protocol.symbology import to_canonical, to_legacy_api
from mcp_servers._envelope import error_from_exception, page
from mcp_servers._ownership import SourceError

logger = logging.getLogger(__name__)

_FMP_UNAVAILABLE = "FMP client is unavailable"
_FMP_FAILED = "FMP request failed"
_EDGAR_FAILED = "EDGAR request failed; a retry may succeed"
_EDGAR_UNAVAILABLE = "manager holdings are not available in this deployment, so a retry will not help"
_FORM144_UNAVAILABLE = "proposed insider sales are not available in this deployment, so a retry will not help"
_NPORT_UNAVAILABLE = "mutual fund holders are not available in this deployment, so a retry will not help"
_ROSTER_UNAVAILABLE = (
    "the insider roster is not available in this deployment, so a retry will not help; get_insider_trades lists "
    "insiders' Form 4 trades"
)
_BREAKDOWN_UNAVAILABLE = (
    "the ownership breakdown is not available in this deployment, so a retry will not help; "
    "get_institutional_holders gives the 13F holders and get_shares_float the shares outstanding"
)


def _today() -> date:
    """Today in New York, where SEC's filing days run."""
    return datetime.now(ZoneInfo("America/New_York")).date()


def _default_period() -> str:
    """The period a 13F read takes when none is given (edgar_engine.periods.expected_period, the rule LangAlpha Data
    follows), today in New York. It needs no edgartools, so FMP's holders take it too."""
    from edgar_engine.periods import expected_period

    return expected_period(_today())


def _vendor_symbol(instrument_key: str) -> str:
    """The ticker FMP and EDGAR read for an equity's instrument key (``BRK-B.XNYS`` as ``BRK-B``)."""
    return to_legacy_api(to_canonical(instrument_key, asset_class=AssetClass.EQUITY))


def _require_engine(unavailable: str) -> None:
    if not engine_available():
        raise SourceError("client_unavailable", unavailable)


def _refuse_build(build: Optional[str], offset: int) -> None:
    """Refuse a later page asked with a build, with the route's 409 for a build it no longer serves (the tools'
    ``rebuilt``). This reader issues none, so a build passed back is LangAlpha Data's, from a page read before the
    deployment fell back to this reader, and its offset would continue another source's list."""
    if build is not None and offset:
        request = httpx.Request("GET", "/")
        raise httpx.HTTPStatusError("a build this reader did not issue", request=request,
                                    response=httpx.Response(409, request=request))


class DirectReader:
    """The calls of LangAlpha Data's ownership routes, answered from FMP and EDGAR in the routes' shapes.

    *build* and *offset* are the manager page a name lookup is for: find_filers_v2 takes neither, so they come here,
    and a later page's build is refused before the name is searched, as it is before a CIK is read."""

    def __init__(self, build: Optional[str] = None, offset: int = 0) -> None:
        self._page = (build, offset)

    async def get_holders_page_v2(
        self, instrument_key: str, *, period: Optional[str], limit: int, offset: int, build: Optional[str],
    ) -> dict[str, Any]:
        _refuse_build(build, offset)
        period = period or _default_period()
        try:
            client = await get_fmp_client()
        except Exception:  # noqa: BLE001
            raise SourceError("client_unavailable", _FMP_UNAVAILABLE) from None
        try:
            ranked = await fmp_ownership.holders(client, _vendor_symbol(instrument_key), period)
        except Exception as exc:  # noqa: BLE001
            failed = error_from_exception(exc, _FMP_FAILED)
            raise SourceError(failed["error"], failed["detail"]) from None
        rows, next_offset = page(ranked, offset, limit)
        return {"rows": rows, "period": period, "total": len(ranked), "offset": offset, "next_offset": next_offset,
                "build": None, "publisher": "fmp"}

    async def find_filers_v2(self, name: str, *, period: Optional[str]) -> dict[str, Any]:
        _require_engine(_EDGAR_UNAVAILABLE)
        _refuse_build(*self._page)
        try:
            return await edgar_13f_filers.find_managers(name, period)
        except Exception:  # noqa: BLE001
            logger.warning("EDGAR 13F filer search failed for %r", name, exc_info=True)
            raise SourceError("upstream_error", _EDGAR_FAILED) from None

    async def get_filer_holdings_v2(
        self, cik: int, *, period: Optional[str], limit: int, offset: int, build: Optional[str],
    ) -> dict[str, Any]:
        _require_engine(_EDGAR_UNAVAILABLE)
        _refuse_build(build, offset)
        from edgar_engine.periods import prior_period

        period = period or _default_period()
        try:
            got = await edgar_13f.manager_portfolio(cik, period, prior_period(period))
        except edgar_13f.UnreadableReports as exc:
            got = {"filer_name": None, "filing_date": None, "url": None, "rows": [], "unreadable_url": exc.url}
        except Exception:  # noqa: BLE001
            logger.warning("EDGAR 13F read failed for CIK %s %s", cik, period, exc_info=True)
            raise SourceError("upstream_error", _EDGAR_FAILED) from None
        got = got or {"filer_name": None, "filing_date": None, "url": None, "rows": []}
        rows, next_offset = page(got["rows"], offset, limit)
        return {"rows": rows, "cik": f"{cik:010d}", "period": period, "filer_name": got["filer_name"],
                "filing_date": got["filing_date"], "url": got["url"], "prior_url": got.get("prior_url"),
                "total": len(got["rows"]), "offset": offset, "next_offset": next_offset, "build": None,
                "publisher": "sec", "unreadable_url": got.get("unreadable_url")}

    async def get_fund_holders_page_v2(
        self, instrument_key: str, *, limit: int, offset: int, build: Optional[str],
    ) -> dict[str, Any]:
        raise SourceError("client_unavailable", _NPORT_UNAVAILABLE)

    async def get_form144_page_v2(
        self, instrument_key: str, *, limit: int, offset: int, build: Optional[str],
    ) -> dict[str, Any]:
        _require_engine(_FORM144_UNAVAILABLE)
        _refuse_build(build, offset)
        ticker = _vendor_symbol(instrument_key)
        try:
            got = await edgar_form144.notices(ticker, limit=limit, offset=offset)
        except Exception:  # noqa: BLE001
            logger.warning("EDGAR Form 144 read failed for %s", instrument_key, exc_info=True)
            raise SourceError("upstream_error", _EDGAR_FAILED) from None
        if got is None:
            raise SourceError("not_found", f"EDGAR lists no company with ticker {ticker}; check that it is a US-listed "
                                           "ticker")
        return {**got, "offset": offset, "build": None, "publisher": "sec"}

    async def get_insider_roster_v2(self, instrument_key: str) -> dict[str, Any]:
        raise SourceError("client_unavailable", _ROSTER_UNAVAILABLE)

    async def get_ownership_breakdown_v2(self, instrument_key: str, *, period: Optional[str]) -> dict[str, Any]:
        raise SourceError("client_unavailable", _BREAKDOWN_UNAVAILABLE)
