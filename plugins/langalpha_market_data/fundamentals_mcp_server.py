#!/usr/bin/env python3
"""Fundamentals MCP Server.

Fundamental data for programmatic analysis via MCP. A CN listing is served
first from ginlix-data (vendor Tushare), whose rows come in FMP's camelCase
shape; every other listing, and any CN miss, comes from FMP. A CN read that
failed, rather than came back empty, still falls back to FMP, but the answer
then says so (`fallback_from`), and an FMP miss after it is an error.
Historical valuation, insider trades and technical indicators come from FMP
alone. `source` names the vendor that answered, and the envelope around
`data` is the standard market-data contract (AGENT_CONTRACT.md).

Tools:
- get_financial_statements: Raw income/balance/cash flow (multi-year)
- get_financial_ratios: Raw ratios and key metrics (multi-year)
- get_growth_metrics: Raw growth rates (multi-year)
- get_historical_valuation: Raw DCF and enterprise value (multi-year)
- get_insider_trades: Insider trading transactions and aggregate stats
- get_dividends_and_splits: Dividend history and stock split history
- get_shares_float: Shares float, outstanding shares, and float percentage
- get_key_executives: Key executives with title and compensation
- get_technical_indicator: Technical indicators (RSI, EMA, MACD, etc.)
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    from _bootstrap import MCPServer  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._bootstrap import MCPServer

import asyncio
import logging
from typing import Literal

import httpx

from data_client.fmp import close_fmp_client, get_fmp_client
from data_client.lifespan import closing_lifespan
from data_client.market_data_provider import symbol_market
from data_client.normalize import (
    listing_currency,
    minor_unit_places,
    populated,
    scale_price,
    series_labels,
)
from data_client.ginlix_data import close_ginlix_mcp_client, get_ginlix_mcp_client
from data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource
from mcp_servers._envelope import error_from_exception, make_error, make_response
from mcp_servers._schemas import (
    ANY,
    BAR_LABELS,
    FALLBACK_FROM,
    INT,
    RECORDS,
    RECORDS_BY_KEY,
    STR,
    described,
    envelope_schema,
    output_model,
)
from market_protocol.enums import AssetClass
from market_protocol.symbology import (
    is_family_index,
    to_canonical,
    to_display,
    to_legacy_api,
)

logger = logging.getLogger(__name__)


_lifespan = closing_lifespan(close_ginlix_mcp_client, close_fmp_client)


mcp = MCPServer("FundamentalsMCP", lifespan=_lifespan)

_SOURCE = "fmp"
_SOURCE_TUSHARE = "tushare"
_CLIENT_UNAVAILABLE = "FMP client is unavailable"
_UPSTREAM_FAILED = "FMP request failed"
_CN_FAILED = "tushare failed"

# The bar sizes the hosted technicals read accepts, in the order its refusal
# lists them. FMP answers any other as a bare 400, so the refusal is made here,
# in the hosted words, before FMP is asked.
_BAR_SIZES = ("1min", "5min", "15min", "30min", "1hour", "4hour", "1day")

_tushare_financial = None

#: Echo key of the CN tools: set only when a failed tushare read left the answer to FMP.
_FALLBACK = {"fallback_from": FALLBACK_FROM}


def _canonical(symbol: str) -> str:
    """Canonical display spelling for an input ticker; echo input on failure."""
    try:
        return to_display(to_canonical(symbol))
    except Exception:  # noqa: BLE001
        return symbol


def _fmp_symbol(symbol: str) -> str:
    """FMP's spelling for an input ticker (``^GSPC``, ``AAPL``, ``600519.SH``).

    Not the display spelling: FMP reads an index as ``^GSPC``, never ``SPX``, and
    a canonical key (``AAPL.XNAS``) not at all. Resolved as an equity so a company
    ticker that collides with an index alias (COMP) stays the company; the FMP
    client respells ``.SH`` itself.
    """
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
    except Exception:  # noqa: BLE001
        return symbol
    legacy = to_legacy_api(ref)
    return f"^{legacy}" if is_family_index(ref) else legacy


def _tushare_for(symbol: str) -> GinlixDataCnFinancialSource | None:
    """CN fundamentals (vendor: Tushare) served by ginlix-data, for CN symbols.

    Returns ``None`` otherwise, and :func:`_cn_first` goes straight to FMP.
    """
    if symbol_market(symbol) != "cn":
        return None
    global _tushare_financial
    if _tushare_financial is None:
        _tushare_financial = GinlixDataCnFinancialSource(get_ginlix_mcp_client())
    return _tushare_financial


def _http_miss(exc: httpx.HTTPError) -> str:
    """The error class, and the status when there was one; never the URL."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{type(exc).__name__} {exc.response.status_code}"
    return type(exc).__name__


async def _from_fmp(disp: str, read, *, data_type: str, **extra) -> dict:
    """Answer a tool from FMP: *read* takes the client and returns the payload.

    The one place an FMP read gets its client, its envelope and its error, so a
    tool passes only what it reads.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)
    try:
        payload = await read(client)
    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)
    return make_response(
        payload or [], source=_SOURCE, symbol=disp, data_type=data_type, **extra
    )


async def _cn_first(disp: str, cn_read, fmp_read, *, data_type: str, **extra) -> dict:
    """Serve a tool from tushare for a CN listing, from FMP otherwise.

    The single definition of the cn-branch contract: gate on ``_tushare_for``
    and a configured ginlix-data, envelope a tushare hit with
    ``source="tushare"``, and let an empty payload fall through to FMP, read by
    *fmp_read*. A tushare error falls through too, but it is not a miss: FMP's
    rows then carry ``fallback_from``, an FMP error keeps its code with the
    tushare failure named, and an empty FMP answer becomes ``upstream_error``,
    since neither source answered. *disp* is the display spelling, so a
    canonical ``600519.XSHG`` still gates in as a CN listing.
    """
    def fmp():
        return _from_fmp(disp, fmp_read, data_type=data_type, **extra)

    ts = _tushare_for(disp)
    if ts is None or not await get_ginlix_mcp_client().ensure():
        return await fmp()
    try:
        data = await cn_read(ts)
    except httpx.HTTPError as exc:
        # An upstream miss (a 503, a 429, a dropped connection) is expected; the
        # FMP path answers instead, so one line is enough.
        failed = _http_miss(exc)
        logger.warning("ginlix-data %s unavailable for %s: %s", data_type, disp, failed)
    except Exception as exc:  # noqa: BLE001 — FMP answers instead
        failed = type(exc).__name__
        logger.warning("ginlix-data %s failed for %s", data_type, disp, exc_info=True)
    else:
        if populated(data):
            return make_response(
                data, source=_SOURCE_TUSHARE, symbol=disp, data_type=data_type, **extra
            )
        return await fmp()
    answer = await fmp()
    if "error" in answer:
        return {**answer, "detail": f"{_CN_FAILED} ({failed}) and FMP did too: {answer['detail']}"}
    if not populated(answer["data"]):
        return make_error(
            "upstream_error",
            f"{_CN_FAILED} ({failed}) and FMP has no rows for this listing",
            symbol=disp,
        )
    return {**answer, "fallback_from": _SOURCE_TUSHARE}


#: Set by ``_add_quarterly_empty_note``; omitted unless a quarterly read is empty.
_QUARTERLY_NOTE = described(
    STR, "Why a quarterly read returned no records; omitted otherwise."
)


def _add_quarterly_empty_note(result: dict, period: str) -> None:
    if period == "quarter" and result.get("count") == 0:
        result["note"] = (
            "Quarterly data returned 0 records. The symbol may have no "
            "quarterly filings, or the FMP subscription tier may not include "
            "quarterly fundamentals. Try period='annual'."
        )


_OUT_GET_FINANCIAL_STATEMENTS = output_model(
    "GetFinancialStatementsOut",
    envelope_schema(
        ANY,
        frame=("symbol",),
        echo={
            "data_type": STR,
            "statement_type": STR,
            "period": STR,
            "note": _QUARTERLY_NOTE,
            **_FALLBACK,
        },
    ),
)


@mcp.tool()
async def get_financial_statements(
    symbol: str,
    statement_type: Literal["income", "balance", "cash", "all"] = "all",
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_FINANCIAL_STATEMENTS:
    """Fetch raw historical financial statements for trend analysis or model
    building. statement_type="all" returns income, balance sheet, and cash flow.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        statement_type: "income" | "balance" | "cash" | "all".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, statement_type, period,
        fallback_from?, note?}. One statement_type → a list of period records;
        "all" → {income_statement, balance_sheet, cash_flow} lists with count the
        total. Common camelCase fields: date, revenue, netIncome, eps,
        totalAssets, operatingCashFlow. date is "YYYY-MM-DD"; records are
        newest-first. fallback_from: source that failed before FMP answered.
        An empty quarterly read adds a note. On error: {error: <code>, detail,
        symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _cn_statements(ts: GinlixDataCnFinancialSource):
        if statement_type == "income":
            return await ts.get_income_statements(disp, period=period, limit=limit)
        if statement_type == "balance":
            return await ts.get_balance_sheets(disp, period=period, limit=limit)
        if statement_type == "cash":
            return await ts.get_cash_flows(disp, period=period, limit=limit)
        income, balance, cash = await asyncio.gather(  # "all"
            ts.get_income_statements(disp, period=period, limit=limit),
            ts.get_balance_sheets(disp, period=period, limit=limit),
            ts.get_cash_flows(disp, period=period, limit=limit),
        )
        return {"income_statement": income, "balance_sheet": balance, "cash_flow": cash}

    async def _fmp_statements(client):
        if statement_type == "income":
            return await client.get_income_statement(api, period=period, limit=limit)
        if statement_type == "balance":
            return await client.get_balance_sheet(api, period=period, limit=limit)
        if statement_type == "cash":
            return await client.get_cash_flow(api, period=period, limit=limit)
        income = await client.get_income_statement(api, period=period, limit=limit)  # "all"
        balance = await client.get_balance_sheet(api, period=period, limit=limit)
        cash_flow = await client.get_cash_flow(api, period=period, limit=limit)
        return {
            "income_statement": income or [],
            "balance_sheet": balance or [],
            "cash_flow": cash_flow or [],
        }

    result = await _cn_first(
        disp,
        _cn_statements,
        _fmp_statements,
        data_type="financial_statements",
        statement_type=statement_type,
        period=period,
    )
    _add_quarterly_empty_note(result, period)
    return result


_OUT_GET_FINANCIAL_RATIOS = output_model(
    "GetFinancialRatiosOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, "period": STR, "note": _QUARTERLY_NOTE, **_FALLBACK},
    ),
)


@mcp.tool()
async def get_financial_ratios(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_FINANCIAL_RATIOS:
    """Fetch raw historical key metrics and financial ratios.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period, fallback_from?,
        note?}. data is {key_metrics, ratios}: period records, newest first.
        key_metrics fields: marketCap, enterpriseValue, evToEBITDA,
        freeCashFlowYield. ratios fields: netProfitMargin, returnOnEquity,
        currentRatio, debtToEquityRatio, priceToEarningsRatio. source "tushare"
        rows lack price-based fields (see get_historical_valuation). date is
        "YYYY-MM-DD". Quarter rows: flow-based fields cover that quarter alone;
        P/E fields annualize its EPS (x4). fallback_from: source that failed
        before FMP answered. An empty quarterly read adds a note. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _cn_ratios(ts: GinlixDataCnFinancialSource):
        key_metrics, ratios = await asyncio.gather(
            ts.get_key_metrics_periods(disp, period=period, limit=limit),
            ts.get_ratios_periods(disp, period=period, limit=limit),
        )
        return {"key_metrics": key_metrics, "ratios": ratios}

    async def _fmp_ratios(client):
        key_metrics = await client.get_key_metrics(api, period=period, limit=limit)
        ratios = await client.get_financial_ratios(api, period=period, limit=limit)
        return {"key_metrics": key_metrics or [], "ratios": ratios or []}

    # CN rows carry the accounting half only: ginlix-data prices no period, so
    # they have no marketCap, enterpriseValue or priceToEarningsRatio, and FMP's
    # are not mixed in to stand for them.
    result = await _cn_first(
        disp, _cn_ratios, _fmp_ratios, data_type="financial_ratios", period=period
    )
    _add_quarterly_empty_note(result, period)
    return result


_OUT_GET_GROWTH_METRICS = output_model(
    "GetGrowthMetricsOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, "period": STR, "note": _QUARTERLY_NOTE, **_FALLBACK},
    ),
)


@mcp.tool()
async def get_growth_metrics(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_GROWTH_METRICS:
    """Fetch raw historical growth rates.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period, fallback_from?,
        note?}. data is {financial_growth, income_statement_growth}, each a
        list, newest first; count is the total. financial_growth fields: date,
        revenueGrowth, netIncomeGrowth, epsgrowth; income_statement_growth
        fields: date, growthRevenue, growthNetIncome, growthEPS. Growth values
        are decimal fractions (0.1 = 10%); a quarter row grows on the prior
        quarter, not the year-ago one. Field names are FMP-native camelCase; date is
        "YYYY-MM-DD". fallback_from: source that failed before FMP answered.
        An empty quarterly read adds a note. On error: {error: <code>, detail,
        symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _fmp_growth(client):
        financial_growth = await client.get_financial_growth(api, period=period, limit=limit)
        income_growth = await client.get_income_statement_growth(api, period=period, limit=limit)
        return {
            "financial_growth": financial_growth or [],
            "income_statement_growth": income_growth or [],
        }

    result = await _cn_first(
        disp,
        lambda ts: ts.get_growth_periods(disp, period=period, limit=limit),
        _fmp_growth,
        data_type="growth_metrics",
        period=period,
    )
    _add_quarterly_empty_note(result, period)
    return result


_OUT_GET_HISTORICAL_VALUATION = output_model(
    "GetHistoricalValuationOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={
            "currency": described(
                STR,
                "ISO 4217 currency of current_dcf's dcf and stockPrice;"
                " omitted when the listing's venue is unknown.",
            ),
            "data_type": STR,
            "period": STR,
        },
    ),
)


@mcp.tool()
async def get_historical_valuation(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_HISTORICAL_VALUATION:
    """Fetch DCF fair value and enterprise value history.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, currency, count, data, source, data_type, period}. data
        is {current_dcf, historical_dcf, enterprise_value}, each a list; count
        is the total. current_dcf fields: symbol, date, dcf, stockPrice, per
        share in `currency` (absent when the venue is unknown). historical_dcf
        is always []. enterprise_value fields, newest first: date,
        reportedCurrency, stockPrice, marketCapitalization, enterpriseValue,
        priced in reportedCurrency (the filings' currency, which can differ
        from `currency`; null when unknown). Field names are FMP-native
        camelCase; date is "YYYY-MM-DD".
        On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        current_dcf = await client.get_dcf(api)
        historical_dcf = await client.get_historical_dcf(api, period=period, limit=limit)
        enterprise_value = await client.get_enterprise_value(api, period=period, limit=limit)
    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)

    try:
        key_metrics = await client.get_key_metrics(api, period=period, limit=limit)
    except Exception as exc:  # noqa: BLE001 - it only labels the rows, which still answer
        logger.warning("FMP key metrics unavailable for %s: %s", disp, type(exc).__name__)
        key_metrics = []

    # FMP prices the DCF row in the listing's trading currency (in pence on a
    # pence venue) and the enterprise-value rows in the filings' currency, which
    # only the key-metrics row for the same period names.
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
    except Exception:  # noqa: BLE001
        ref = None
    # A venue the protocol does not know resolves to a USD default, not its currency.
    currency = listing_currency(ref) if ref else None
    places = minor_unit_places(symbol)
    reported = {row.get("date"): row.get("reportedCurrency") for row in key_metrics or []}
    return make_response(
        {
            "current_dcf": [
                {
                    **{k: v for k, v in row.items() if k != "Stock Price"},
                    "dcf": scale_price(row.get("dcf"), places),
                    "stockPrice": scale_price(row.get("Stock Price", row.get("stockPrice")), places),
                }
                for row in current_dcf or []
            ],
            "historical_dcf": historical_dcf or [],
            "enterprise_value": [
                {**row, "reportedCurrency": reported.get(row.get("date"))}
                for row in enterprise_value or []
            ],
        },
        source=_SOURCE,
        symbol=disp,
        currency=currency,
        data_type="historical_valuation",
        period=period,
    )


_OUT_GET_INSIDER_TRADES = output_model(
    "GetInsiderTradesOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR},
    ),
)


@mcp.tool()
async def get_insider_trades(
    symbol: str,
    limit: int = 50,
) -> _OUT_GET_INSIDER_TRADES:
    """Fetch insider transactions from SEC Form 4 filings, with quarterly totals.
    HK and A-share listings return empty lists.

    Args:
        symbol: Ticker, e.g. "AAPL".
        limit: Number of recent transactions (default 50).

    Returns:
        dict: {symbol, count, data, source, data_type}. data is {trades, stats},
        each a list; count is the total across both. trades fields, newest
        first: filingDate, transactionDate, reportingName, transactionType,
        securitiesTransacted, price. Only "P-Purchase" and "S-Sale" are
        open-market trades; others are awards, exercises or tax withholding.
        stats, one row per calendar quarter: year, quarter, totalPurchases,
        totalSales (counts of P and S trades), totalAcquired, totalDisposed
        (shares, all transaction types). Field names are FMP-native camelCase;
        dates are "YYYY-MM-DD". On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        trades = await client.get_insider_trades(api, limit=limit)
        stats = await client.get_insider_trade_stats(api)

        return make_response(
            {"trades": trades or [], "stats": stats or []},
            source=_SOURCE,
            symbol=disp,
            data_type="insider_trades",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


_OUT_GET_DIVIDENDS_AND_SPLITS = output_model(
    "GetDividendsAndSplitsOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, **_FALLBACK},
    ),
)


@mcp.tool()
async def get_dividends_and_splits(
    symbol: str,
) -> _OUT_GET_DIVIDENDS_AND_SPLITS:
    """Fetch historical dividend payments and stock splits.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source, data_type, fallback_from?}. data is
        {dividends, splits}, each a list, newest first; count is the total
        across both. dividends fields: date, recordDate, paymentDate,
        declarationDate, adjDividend (split-adjusted), dividend (as paid), yield
        (percent, 0.34 = 0.34%), frequency. splits fields: date, numerator,
        denominator. Field names are FMP-native camelCase; date is "YYYY-MM-DD".
        fallback_from: source that failed before FMP answered. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _fmp_dividends(client):
        dividends = await client.get_dividends(api)
        splits = await client.get_splits(api)
        return {"dividends": dividends or [], "splits": splits or []}

    return await _cn_first(
        disp,
        lambda ts: ts.get_dividends(disp),
        _fmp_dividends,
        data_type="dividends_and_splits",
    )


_OUT_GET_SHARES_FLOAT = output_model(
    "GetSharesFloatOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={"data_type": STR, **_FALLBACK},
    ),
)


@mcp.tool()
async def get_shares_float(
    symbol: str,
) -> _OUT_GET_SHARES_FLOAT:
    """Fetch shares float, outstanding shares, and float percentage.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source, data_type, fallback_from?}. data is
        a list of records; count is the record total. Fields: symbol, date, freeFloat
        (percent, 99.8 = 99.8%), floatShares, outstandingShares; an ADR counts
        ADS units, not ordinary shares. date is "YYYY-MM-DD": the day FMP
        refreshed the record, not the as-of date of the counts, which can be
        months older; an A-share row's date is the trading day of its counts.
        Field names are FMP-native camelCase. fallback_from: source that failed
        before FMP answered. On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _fmp_float(client):
        # FMP stamps the row with its refresh time; the contract is a bare date.
        return [
            {**row, "date": str(row["date"])[:10]} if row.get("date") else row
            for row in await client.get_shares_float(api) or []
        ]

    return await _cn_first(
        disp,
        lambda ts: ts.get_shares_float(disp),
        _fmp_float,
        data_type="shares_float",
    )


_OUT_GET_KEY_EXECUTIVES = output_model(
    "GetKeyExecutivesOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={"data_type": STR, **_FALLBACK},
    ),
)


@mcp.tool()
async def get_key_executives(
    symbol: str,
) -> _OUT_GET_KEY_EXECUTIVES:
    """Fetch key executives with title and compensation — identify the
    management team or compare executive pay across peers.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source, data_type, fallback_from?}. data is
        a list of executive records; count is the record total. Fields: name,
        title, pay, currencyPay, gender, yearBorn, titleSince. Field names are
        FMP-native camelCase; pay is an integer in currencyPay units; not
        time-ordered. fallback_from: source that failed before FMP answered.
        On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    return await _cn_first(
        disp,
        lambda ts: ts.get_key_executives(disp),
        lambda client: client.get_key_executives(api),
        data_type="key_executives",
    )


_OUT_GET_TECHNICAL_INDICATOR = output_model(
    "GetTechnicalIndicatorOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={
            "data_type": STR,
            "indicator": STR,
            "period": described(INT, "Indicator lookback length."),
            "timeframe": described(STR, "FMP-native bar size."),
            **BAR_LABELS,
        },
    ),
)


@mcp.tool()
async def get_technical_indicator(
    symbol: str,
    indicator: str,
    period: int = 14,
    timeframe: str = "1day",
) -> _OUT_GET_TECHNICAL_INDICATOR:
    """Fetch a technical indicator time series over OHLCV bars.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        indicator: FMP name — "rsi", "ema", "sma", "wma", "adx", "williams".
        period: Lookback length (default 14).
        timeframe: FMP-native bar — "1min"…"4hour", "1day" (default "1day").

    Returns:
        dict: {symbol, count, data, source, data_type, indicator, period,
        timeframe, price_treatment, in_progress?}. data: list of {date, open,
        high, low, close, volume, <indicator>} newest first, field names
        FMP-native camelCase. date is "YYYY-MM-DD" (1day) or
        "YYYY-MM-DD HH:MM:SS" (intraday). price_treatment:
        raw|split_adjusted|dividend_adjusted. in_progress: date of the newest
        bar while it forms. On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    if timeframe not in _BAR_SIZES:
        return make_error(
            "invalid_argument",
            f"timeframe must be one of {', '.join(_BAR_SIZES)}, not {timeframe!r}",
            symbol=disp,
        )
    api = _fmp_symbol(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
    except Exception:  # noqa: BLE001
        ref = None
    try:
        data = await client.get_technical_indicator(
            api, indicator=indicator, period=period, timeframe=timeframe
        )
        rows = data or []
        if timeframe == "1day":
            # FMP stamps a daily row with a midnight time; the contract is a bare date.
            rows = [
                {**row, "date": str(row["date"])[:10]}
                if isinstance(row, dict) and row.get("date") else row
                for row in rows
            ]
        newest = rows[0].get("date") if rows and isinstance(rows[0], dict) else None

        return make_response(
            rows,
            source=_SOURCE,
            symbol=disp,
            data_type="technical_indicator",
            indicator=indicator,
            period=period,
            timeframe=timeframe,
            **series_labels(_SOURCE, ref, timeframe, newest),
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


if __name__ == "__main__":
    mcp.run(transport="stdio")
