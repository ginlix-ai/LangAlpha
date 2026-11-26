"""The 13F tools' answers (get_institutional_holders, get_manager_holdings), built once for every source.

A source hands over ranked rows; this module owns what the agent sees: the caps, the envelope and its paging
echo (``holders_total``/``positions_total``, ``offset``, ``next_offset``, ``build``), the row fields, a manager
named by CIK or by name, and the errors (``rebuilt`` for a page of a replaced build; a route's own ``detail``, a
not-ready 503's included). Every source is read through an ``OwnershipReader``, the 13F ``*_v2`` route calls of
ginlix-data: ``GinlixMCPClient``, ginlix-data's own MCP server, or, in the open-source build without ginlix-data,
a reader that answers them from FMP and EDGAR in the routes' shapes and raises ``SourceError`` for its own
failures.
"""

from __future__ import annotations

import calendar
import re
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any, Optional, Protocol

import httpx

from mcp_servers._envelope import error_from_upstream, make_error, make_response, page_error

HOLDERS_MOST = 500
POSITIONS_MOST = 5000
HOLDER_FIELDS = (
    "holder", "filer_cik", "shares", "value", "pctheld", "pctchange", "value_status", "filing_date", "accession", "url",
)
POSITION_FIELDS = (
    "ticker", "issuer", "cusip", "title_class", "sh_prn", "put_call", "shares", "value", "value_status", "weight",
    "change", "shares_prior", "accession",
)
CANDIDATES = 20
_COUNTS = ("shares", "value", "shares_prior")
_CIK = re.compile(r"\d{1,10}")
_FAILED = "ownership data request failed; a retry may succeed"
_REBUILT = "the build passed was replaced since the last page; start again at offset 0 with the same period and no build"


class OwnershipReader(Protocol):
    """ginlix-data's 13F routes as calls: each returns the route's JSON body, None for its 404, and raises
    httpx.HTTPStatusError for any other failing status (409 when a passed build was replaced) or SourceError."""

    async def get_holders_page_v2(self, instrument_key: str, *, period: Optional[str], limit: int, offset: int,
                                  build: Optional[str]) -> Optional[dict[str, Any]]: ...

    async def find_filers_v2(self, name: str, *, period: Optional[str]) -> Optional[dict[str, Any]]: ...

    async def get_filer_holdings_v2(self, cik: int, *, period: Optional[str], limit: int, offset: int,
                                    build: Optional[str]) -> Optional[dict[str, Any]]: ...


class SourceError(Exception):
    """A reader's own failure, already worded for the agent: route_error answers its code and detail."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code, self.detail = code, detail


# -- periods -----------------------------------------------------------------


def quarter_end(value: Optional[str]) -> Optional[str]:
    """*value* as a 13F period ("YYYY-MM-DD" ending a calendar quarter); ValueError naming it otherwise."""
    if value is None:
        return None
    try:
        day = date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"period must be a quarter-end date \"YYYY-MM-DD\", not {value!r}") from None
    if day.month not in (3, 6, 9, 12) or day.day != calendar.monthrange(day.year, day.month)[1]:
        raise ValueError(f'period must be a quarter end such as "2026-06-30", not {value!r}')
    return day.isoformat()


# -- answers -----------------------------------------------------------------


def _num(value: Any) -> Any:
    """A whole float as an int (shares 1500.0 as 1500), so every source's counts read alike."""
    return int(value) if isinstance(value, float) and value.is_integer() else value


def _record(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {k: _num(row.get(k)) if k in _COUNTS else row.get(k) for k in fields}


def holders_answer(
    rows: list[dict[str, Any]], *, symbol: str, period: Optional[str], build: Optional[str], total: int,
    offset: int, next_offset: Optional[int], source: str,
) -> dict[str, Any]:
    return make_response(
        [_record(r, HOLDER_FIELDS) for r in rows], source=source, symbol=symbol, period=period,
        build=build, holders_total=total, offset=offset, next_offset=next_offset,
    )


def manager_answer(
    rows: list[dict[str, Any]], *, manager: Optional[str], filer_cik: str, period: Optional[str],
    build: Optional[str], filing_date: Optional[str], url: Optional[str], prior_url: Optional[str], total: int,
    offset: int, next_offset: Optional[int], source: str,
) -> dict[str, Any]:
    return make_response(
        [_record(r, POSITION_FIELDS) for r in rows], source=source, manager=manager, filer_cik=filer_cik,
        period=period, build=build, filing_date=filing_date, url=url, prior_url=prior_url, positions_total=total,
        offset=offset, next_offset=next_offset,
    )


def no_report(manager: str, period: Optional[str]) -> dict[str, Any]:
    when = f"for {period}" if period else "for that quarter"
    return make_error(
        "not_found",
        f"manager {manager!r} reported no 13F holdings {when}: it filed no report, or one listing no positions; "
        "pass another quarter end as period, or check that the manager files Form 13F",
        manager=manager,
    )


def unreadable(manager: str, period: Optional[str], url: str) -> dict[str, Any]:
    when = f"for {period}" if period else "for that quarter"
    return make_error(
        "upstream_error",
        f"the 13F holdings report {when} could not be read, so its positions are unknown and a retry now will not help; "
        f"the filing is at {url}",
        manager=manager,
    )


def ambiguous(manager: str, candidates: list[dict[str, Any]], total: int) -> dict[str, Any]:
    """``invalid_argument`` listing the managers a name matches, each as ``{manager, filer_cik}``."""
    listed = [{"manager": c.get("filer_name"), "filer_cik": c.get("filer_cik")} for c in candidates[:CANDIDATES]]
    more = f", the first {len(listed)} listed" if total > len(listed) else ""
    return make_error(
        "invalid_argument",
        f"manager {manager!r} matches {total} 13F filers{more}; pass one's filer_cik as manager",
        manager=manager, candidates=listed,
    )


def route_error(exc: Exception, **context: Any) -> dict[str, Any]:
    """A reader's failure: a SourceError's own code and detail; ``rebuilt`` for a route's 409 (the page's build
    was replaced); else the status's code with the route's own detail word for word, which a 503 keeps only when
    the route marks it ``not_ready`` (its ``NotReady`` hint, written for the agent); any other failure is
    ``upstream_error``."""
    if isinstance(exc, SourceError):
        return make_error(exc.code, exc.detail, **context)
    if not isinstance(exc, httpx.HTTPStatusError):
        return make_error("upstream_error", _FAILED, **context)
    status = exc.response.status_code
    if status == 409:
        return make_error("rebuilt", _REBUILT, **context)
    try:
        body = exc.response.json()
    except Exception:  # noqa: BLE001 - a body that is not the route's JSON
        body = None
    body = body if isinstance(body, dict) else {}
    detail = body.get("detail")
    if not isinstance(detail, str) or (status == 503 and body.get("reason") != "not_ready"):
        detail = f"ownership data request failed ({status})" + ("; a retry may succeed" if status >= 500 else "")
    return error_from_upstream(detail, status=status, **context)


# -- LangAlpha Data's routes -------------------------------------------------


async def holders_from_route(
    reader: OwnershipReader, instrument_key: str, symbol: str, *, period: Optional[str], limit: int, offset: int,
    build: Optional[str],
) -> dict[str, Any]:
    if (refused := page_error(limit, offset, HOLDERS_MOST, symbol=symbol)) is not None:
        return refused
    try:
        period = quarter_end(period)
    except ValueError as exc:
        return make_error("invalid_argument", str(exc), symbol=symbol)
    try:
        body = await reader.get_holders_page_v2(
            instrument_key, period=period, limit=limit, offset=offset, build=build
        )
    except Exception as exc:  # noqa: BLE001
        return route_error(exc, symbol=symbol)
    if body is None:  # the route's 404: a period it refuses, or a symbol it does not know
        detail = (
            f"no 13F holders are stored for {symbol} for {period}; pass another quarter end as period, or check "
            f"that {symbol} is a US-listed ticker" if period
            else f"no 13F holders are stored for {symbol}; check that it is a US-listed ticker"
        )
        return make_error("not_found", detail, symbol=symbol)
    return holders_answer(
        body.get("rows") or [], symbol=symbol, period=body.get("period", period), build=body.get("build"),
        total=body.get("total", 0), offset=offset, next_offset=body.get("next_offset"),
        source=body.get("publisher") or "sec",
    )


async def manager_cik(
    find: Callable[[str], Awaitable[Optional[dict[str, Any]]]], manager: str,
) -> tuple[Optional[int], Optional[dict[str, Any]]]:
    """The filer CIK *manager* names, as digits ("1067983" or "0001067983") or by name through *find* (a name to
    ``{rows, total}`` of ``{filer_cik, filer_name}``), or the error to answer instead: ``not_found`` for no
    match, ``invalid_argument`` with the candidates for several."""
    text = manager.strip()
    if _CIK.fullmatch(text):
        return int(text), None
    try:
        body = await find(text) or {}
    except Exception as exc:  # noqa: BLE001
        return None, route_error(exc, manager=manager)
    rows = body.get("rows") or []
    total = body.get("total", len(rows))
    if not rows:
        return None, make_error(
            "not_found",
            f"no 13F filer is named {manager!r}; pass the manager's SEC CIK, or fewer words of its name as filed",
            manager=manager,
        )
    if total > 1:
        return None, ambiguous(manager, rows, total)
    return int(rows[0]["filer_cik"]), None


async def manager_from_route(
    reader: OwnershipReader, manager: str, *, period: Optional[str], limit: int, offset: int, build: Optional[str],
) -> dict[str, Any]:
    if (refused := page_error(limit, offset, POSITIONS_MOST, manager=manager)) is not None:
        return refused
    try:
        period = quarter_end(period)
    except ValueError as exc:
        return make_error("invalid_argument", str(exc), manager=manager)
    cik, error = await manager_cik(lambda text: reader.find_filers_v2(text, period=period), manager)
    if error is not None:
        return error
    try:
        body = await reader.get_filer_holdings_v2(
            cik, period=period, limit=limit, offset=offset, build=build
        )
    except Exception as exc:  # noqa: BLE001
        return route_error(exc, manager=manager)
    if body is None:  # the route's 404: the store holds no complete report set for the quarter
        when = period or "the latest quarter past its filing deadline"
        return make_error("not_found", f"no 13F holdings are stored for {when}", manager=manager)
    if not body.get("total"):
        if body.get("unreadable_url"):  # a report filed for the quarter could not be read, and no other lists a position
            return unreadable(manager, body.get("period") or period, body["unreadable_url"])
        if not (body.get("url") or body.get("filing_date")):  # no report names it: an empty page is a filed empty report
            return no_report(manager, body.get("period") or period)
    return manager_answer(
        body.get("rows") or [], manager=body.get("filer_name"), filer_cik=body.get("cik") or f"{cik:010d}",
        period=body.get("period"), build=body.get("build"), filing_date=body.get("filing_date"), url=body.get("url"),
        prior_url=body.get("prior_url"), total=body.get("total") or 0, offset=offset,
        next_offset=body.get("next_offset"),
        source=body.get("publisher") or "sec",
    )

