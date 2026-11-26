"""The fundamentals server's six ownership tools: get_institutional_holders, get_manager_holdings,
get_mutual_fund_holders, get_insider_roster, get_ownership_breakdown and get_proposed_insider_sales.

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
    breakdown_from_route,
    fund_holders_from_route,
    holders_from_route,
    manager_from_route,
    notices_from_route,
    roster_from_route,
)
from mcp_servers._ownership_direct import DirectReader
from mcp_servers._schemas import (
    INT,
    NULLABLE_INT,
    NULLABLE_STR,
    OBJECT,
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


_OUT_GET_MUTUAL_FUND_HOLDERS = output_model(
    "GetMutualFundHoldersOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={
            "build": described(
                NULLABLE_STR, "Build this page was read from; pass it back as build with next_offset."
            ),
            "holders_total": described(INT, "Holders in the whole ranked list."),
            "offset": _PAGE_ECHO["offset"],
            "next_offset": _PAGE_ECHO["next_offset"],
        },
    ),
)


async def get_mutual_fund_holders(
    symbol: str,
    limit: int = 100,
    offset: int = 0,
    build: str | None = None,
) -> _OUT_GET_MUTUAL_FUND_HOLDERS:
    """Registered funds (mutual funds, ETFs, closed-end funds) holding a US-listed
    security, from each fund's latest public SEC Form N-PORT report.
    No money market funds. A fund's report turns public 60 days after its fiscal
    quarter ends, so funds report as of different dates.

    Args:
        symbol: Ticker, e.g. "AAPL".
        limit: Holders to return (default 100, at most 500).
        offset: Holders to skip. Next page: offset=next_offset with the
            answer's build. A build replaced since fails with error rebuilt;
            start again at offset 0.

    Returns:
        dict: {symbol, build, holders_total, offset, next_offset, count, data,
        source}.
        data is a list of {fund, series_id, registrant, shares, value, pctheld,
        pct_of_fund, pctchange, date_reported, filing_date, accession, url}, most
        shares first; next_offset is null on the last page. value is US dollars
        on date_reported; pctheld (of shares outstanding, null for multi-class
        issuers and ADRs), pct_of_fund (of the fund's net assets) and pctchange
        (null for a new holder) are fractions; url is the fund's N-PORT filing.
        Dates "YYYY-MM-DD". On error: {error: <code>, detail, symbol}.
    """
    disp, key = _issuer(symbol)
    if key is None:
        return _unknown(symbol, disp)
    return await fund_holders_from_route(await _reader(), key, disp, limit=limit, offset=offset, build=build)


_OUT_GET_INSIDER_ROSTER = output_model(
    "GetInsiderRosterOut",
    envelope_schema(RECORDS, frame=("symbol",)),
)


async def get_insider_roster(
    symbol: str,
) -> _OUT_GET_INSIDER_ROSTER:
    """Insiders of a US company (officers, directors, 10% owners) and the shares each
    last reported holding on SEC Forms 3, 4 and 5.
    No filing marks an exit, so the roster lists owners who filed in the last two
    years.

    Args:
        symbol: Ticker, e.g. "AAPL".

    Returns:
        dict: {symbol, count, data, source}. data is a list of {name, owner_cik,
        relationship, officer_title, shares_direct, as_of_direct,
        shares_indirect, as_of_indirect, indirect_detail, latest_transaction,
        latest_transaction_date, latest_filing_date, url}, most shares (direct
        plus indirect) first. shares_* are common shares as of the latest report
        stating them (as_of_*); indirect_detail is a list of {nature, shares};
        relationship is a list of director, officer, ten_percent_owner, other;
        latest_transaction is a Form 4 code, e.g. "S-Sale"; url is the latest
        filing. Dates "YYYY-MM-DD". On error: {error: <code>, detail, symbol}.
    """
    disp, key = _issuer(symbol)
    if key is None:
        return _unknown(symbol, disp)
    return await roster_from_route(await _reader(), key, disp)


_OUT_GET_OWNERSHIP_BREAKDOWN = output_model(
    "GetOwnershipBreakdownOut",
    envelope_schema(
        OBJECT,
        frame=("symbol",),
        echo={
            "period": described(
                NULLABLE_STR,
                '13F quarter end the institution figures report, "YYYY-MM-DD"; insider shares are the latest reported.',
            ),
        },
    ),
)


async def get_ownership_breakdown(
    symbol: str,
    period: str | None = None,
) -> _OUT_GET_OWNERSHIP_BREAKDOWN:
    """The shares of a US company held by insiders (officers and directors, by their
    last Forms 3, 4 and 5) and by 13F institutions, as fractions of shares
    outstanding, with the institution count.
    Fractions are null for multi-class issuers and ADRs, which state no single share
    count.

    Args:
        symbol: Ticker, e.g. "AAPL".
        period: 13F quarter end "YYYY-MM-DD"; default the latest quarter at
            least 3 days past its 45-day filing deadline. Insider shares are the
            latest reported, whatever the period.

    Returns:
        dict: {symbol, period, count, data, source}. data is {insiders_pct,
        institutions_pct, institutions_float_pct, institutions_count,
        insider_shares, institution_shares, shares_outstanding,
        shares_outstanding_date}. *_pct are fractions (0.66 = 66%), capped at 1
        (managers sharing a block can both report it); institutions_float_pct
        divides by shares outstanding less insider shares. On error: {error:
        <code>, detail, symbol}.
    """
    disp, key = _issuer(symbol)
    if key is None:
        return _unknown(symbol, disp)
    return await breakdown_from_route(await _reader(), key, disp, period=period)


_OUT_GET_PROPOSED_INSIDER_SALES = output_model(
    "GetProposedInsiderSalesOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={
            "build": described(
                NULLABLE_STR,
                "Anchor of the list this page was read from; pass it back as build with next_offset. Null when the "
                "source has none.",
            ),
            "notices_total": described(
                INT, "Notices in the whole list; it can count some that pages skip once read (another issuer's, or "
                "unreadable)."
            ),
            "offset": described(INT, "Notices skipped before this page."),
            "next_offset": _PAGE_ECHO["next_offset"],
            "covered_from": described(
                STR, "Earliest filing date held while older notices still load; absent once every notice is held."
            ),
        },
    ),
)


async def get_proposed_insider_sales(
    symbol: str,
    limit: int = 50,
    offset: int = 0,
    build: str | None = None,
) -> _OUT_GET_PROPOSED_INSIDER_SALES:
    """Form 144 notices for a US company: an insider's or affiliate's planned sale of
    restricted or control shares, filed when the order is placed, before any Form 4.
    A notice does not oblige the sale. EDGAR holds notices from 2023-04-13 on.

    Args:
        symbol: Ticker, e.g. "AAPL".
        limit: Notices to return (default 50, at most 100).
        offset: Notices to skip. Next page: offset=next_offset with the
            answer's build. A build replaced since fails with error rebuilt;
            start again at offset 0.

    Returns:
        dict: {symbol, build, notices_total, offset, next_offset, covered_from?,
        count, data, source}. covered_from, present while older notices still
        load, is the earliest filing date held.
        data is a list of {filing_date, seller, seller_cik, relationship,
        security_class, shares_to_sell, market_value, approx_sale_date,
        plan_adoption_date, acquired, sold_past_3_months, accession, url, form,
        amends}, one per notice and security class, newest filed first;
        next_offset is null on the last page. form is 144 or 144/A; a 144/A
        restates in full the notice whose accession is amends (null for a 144),
        so use it in place of that notice. market_value is US dollars; relationship
        is a list of director, officer, ten_percent_owner, other; acquired is a
        list of {date, nature, shares}; sold_past_3_months of {date, shares,
        proceeds}. Dates "YYYY-MM-DD". On error: {error: <code>, detail, symbol}.
    """
    disp, key = _issuer(symbol)
    if key is None:
        return _unknown(symbol, disp)
    return await notices_from_route(await _reader(), key, disp, limit=limit, offset=offset, build=build)


TOOLS = (get_institutional_holders, get_manager_holdings, get_mutual_fund_holders, get_insider_roster, get_ownership_breakdown, get_proposed_insider_sales)


def register(mcp: Any) -> None:
    """Add the six tools to *mcp*, in this order in its tools/list."""
    for tool in TOOLS:
        mcp.tool()(tool)
