"""The fundamentals server's 13F tools: get_institutional_holders and get_manager_holdings.

Each reads through one OwnershipReader (mcp_servers._ownership): LangAlpha Data's routes when it is configured, else
DirectReader over FMP and EDGAR (mcp_servers._ownership_direct). register adds them to the server.
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

from typing import Any

from data_client.ginlix_data import get_ginlix_mcp_client
from market_protocol.enums import AssetClass
from market_protocol.symbology import market_of, to_canonical, to_display
from mcp_servers._envelope import make_error
from mcp_servers._ownership import (
    OwnershipReader,
    holders_from_route,
    manager_from_route,
)
from mcp_servers._ownership_direct import DirectReader
from mcp_servers._schemas import (
    INT,
    NULLABLE_INT,
    NULLABLE_STR,
    RECORDS,
    STR,
    described,
    envelope_schema,
    output_model,
)


async def _reader(build: str | None = None, offset: int = 0) -> OwnershipReader:
    """LangAlpha Data's routes when it is configured, else DirectReader, given the page a manager name is looked up
    for (*build*, *offset*)."""
    client = get_ginlix_mcp_client()
    return client if await client.ensure() else DirectReader(build, offset)


def _issuer(symbol: str) -> tuple[str, str | None]:
    """*symbol*'s display spelling (the input when it does not parse) and, when it resolves as an equity not listed on
    a known non-US venue (the options tools' rule; SEC ownership forms cover US-listed securities), its instrument
    key: a company ticker that collides with an index alias (COMP) stays the company."""
    try:
        disp = to_display(to_canonical(symbol))
    except Exception:  # noqa: BLE001
        disp = symbol
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
    except Exception:  # noqa: BLE001
        return disp, None
    return disp, ref.instrument_key if market_of(ref) in ("us", "other") else None


def _unknown(symbol: str, disp: str) -> dict[str, Any]:
    return make_error("invalid_argument", f'unknown symbol {symbol!r}; pass a US ticker, e.g. "AAPL"', symbol=disp)


_PAGE_ECHO = {
    "period": described(NULLABLE_STR, 'Quarter end the rows report, "YYYY-MM-DD".'),
    "build": described(
        NULLABLE_STR,
        "Build of the period this page was read from; pass it back as build with next_offset. "
        "Null when the source has none.",
    ),
    "offset": described(INT, "Holders skipped before this page."),
    "next_offset": described(NULLABLE_INT, "Offset of the next page; null on the last page."),
}

_OUT_GET_INSTITUTIONAL_HOLDERS = output_model(
    "GetInstitutionalHoldersOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={**_PAGE_ECHO, "holders_total": described(INT, "Holders in the whole ranked list.")},
    ),
)


async def get_institutional_holders(
    symbol: str,
    period: str | None = None,
    limit: int = 100,
    offset: int = 0,
    build: str | None = None,
) -> _OUT_GET_INSTITUTIONAL_HOLDERS:
    """Investment managers holding a US stock, from SEC Form 13F: each manager's share
    position for one quarter and its change from the quarter before.
    13F covers managers with $100M+ in US equities, long share positions only.

    Args:
        symbol: Ticker, e.g. "AAPL".
        period: Quarter end "YYYY-MM-DD"; default the latest quarter at least
            3 days past its 45-day filing deadline; a quarter before its
            deadline is partial.
        limit: Holders to return (default 100, at most 500).
        offset: Holders to skip. Next page: offset=next_offset with the
            answer's period and build. A build replaced since fails with error
            rebuilt; start again at offset 0.

    Returns:
        dict: {symbol, period, build, holders_total, offset, next_offset, count,
        data, source}. data is a list of {holder, filer_cik, shares, value, pctheld,
        pctchange, value_status, filing_date, accession, url}, most shares first;
        next_offset is null on the last page. value is US dollars; pctheld (of
        shares outstanding, null for multi-class issuers and ADRs) and pctchange
        (null for a new holder, or when either quarter's reports are not all
        held and readable) are fractions. value_status: as_filed, rescaled
        (filed in thousands or with a unit error, now dollars), excluded
        (value null) or null (units not checked). accession is ";"-joined
        when an amendment added lines; url is the 13F filing (the first listed
        when accession holds several), null without an accession. Dates
        "YYYY-MM-DD". On error: {error: <code>, detail, symbol}.
    """
    disp, key = _issuer(symbol)
    if key is None:
        return _unknown(symbol, disp)
    return await holders_from_route(await _reader(), key, disp, period=period, limit=limit, offset=offset, build=build)


_OUT_GET_MANAGER_HOLDINGS = output_model(
    "GetManagerHoldingsOut",
    envelope_schema(
        RECORDS,
        echo={
            "manager": described(NULLABLE_STR, "The manager's name as filed."),
            "filer_cik": described(STR, "The manager's SEC CIK, 10 digits."),
            **_PAGE_ECHO,
            "offset": described(INT, "Positions skipped before this page."),
            "filing_date": described(NULLABLE_STR, 'Latest filing date of the report, "YYYY-MM-DD".'),
            "url": described(NULLABLE_STR, "The 13F report's filing on sec.gov."),
            "prior_url": described(NULLABLE_STR, "The quarter before's 13F filing on sec.gov; null when not held."),
            "positions_total": described(INT, "Positions in the whole ranked list."),
        },
    ),
)


async def get_manager_holdings(
    manager: str,
    period: str | None = None,
    limit: int = 500,
    offset: int = 0,
    build: str | None = None,
) -> _OUT_GET_MANAGER_HOLDINGS:
    """One investment manager's SEC Form 13F portfolio for a quarter, with what it bought
    and sold since the quarter before. 13F filers are managers (Berkshire Hathaway,
    Vanguard Group), not single funds.

    Args:
        manager: SEC CIK, e.g. "1067983" or "0001067983", or the name as filed.
            A name matching several managers returns invalid_argument with
            candidates.
        period: Quarter end "YYYY-MM-DD"; default the latest quarter at least
            3 days past its 45-day filing deadline. A later quarter answers once
            the manager has filed it.
        limit: Positions to return (default 500, at most 5000).
        offset: Positions to skip. Next page: offset=next_offset with the
            answer's period and build. A build replaced since fails with error
            rebuilt; start again at offset 0.

    Returns:
        dict: {manager, filer_cik, period, build, filing_date, url, prior_url,
        positions_total, offset, next_offset, count, data, source}. url is the
        13F report's filing; prior_url the quarter before's, null when not
        held. data is a list of {ticker, issuer, cusip, title_class, sh_prn,
        put_call, shares, value, value_status, weight, change, shares_prior,
        accession}, one per cusip, sh_prn and put_call,
        largest value first, null values and sold_out last; next_offset is
        null on the last page. value is US dollars; weight is a fraction of the
        portfolio; change is new, increased, decreased, unchanged or sold_out
        (shares 0). change and shares_prior are null when either quarter's
        reports are not all held and readable. accession is the report a
        position comes from, ";"-joined when several, null for sold_out. On
        error: {error: <code>, detail, manager, candidates?}.
    """
    return await manager_from_route(
        await _reader(build, offset), manager, period=period, limit=limit, offset=offset, build=build
    )




TOOLS = (get_institutional_holders, get_manager_holdings)


def register(mcp: Any) -> None:
    """Add the 13F tools to *mcp*, in this order in its tools/list."""
    for tool in TOOLS:
        mcp.tool()(tool)
