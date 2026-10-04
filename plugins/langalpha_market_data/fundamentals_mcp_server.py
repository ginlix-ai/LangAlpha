#!/usr/bin/env python3
"""Fundamentals MCP Server.

Fundamental data for programmatic analysis via MCP. A CN listing is served
first from ginlix-data (vendor Tushare), whose rows come in FMP's camelCase
shape; every other listing, and any CN miss, comes from FMP. Ratios are the
exception: FMP answers first there, since only it prices the valuation fields,
and ginlix-data answers a CN listing FMP cannot. Historical valuation, insider
trades and technical indicators come from FMP alone. `source` names the vendor
that answered, and the envelope around `data` is the standard market-data
contract (AGENT_CONTRACT.md).

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
from data_client.normalize import populated
from data_client.ginlix_data import close_ginlix_mcp_client, get_ginlix_mcp_client
from data_client.ginlix_data.cn_financial import GinlixDataCnFinancialSource
from mcp_servers._envelope import error_from_exception, make_error, make_response
from mcp_servers._schemas import (
    ANY,
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

_tushare_financial = None


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

    Returns ``None`` otherwise; callers fall through to the FMP path, and an
    unreachable ginlix-data or an empty result does the same (soft miss).
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


async def _from_tushare(disp: str, fetch, *, data_type: str, **extra):
    """Serve a CN tool from tushare, or ``None`` to fall through to FMP.

    The single definition of the cn-branch contract: gate on ``_tushare_for``,
    treat any tushare error or empty payload as a soft miss (``None`` → the
    caller's FMP path answers), and envelope hits with ``source="tushare"``.
    *disp* is the display spelling, so a canonical ``600519.XSHG`` still gates
    in as a CN listing.
    """
    ts = _tushare_for(disp)
    if ts is None:
        return None
    try:
        data = await fetch(ts)
    except httpx.HTTPError as exc:
        # An upstream miss (a 503, a 429, a dropped connection) is expected; the
        # FMP path answers instead, so one line is enough.
        logger.warning(
            "ginlix-data %s unavailable for %s: %s", data_type, disp, _http_miss(exc)
        )
        return None
    except Exception:  # noqa: BLE001 — soft miss, fall through to FMP
        logger.warning("ginlix-data %s failed for %s", data_type, disp, exc_info=True)
        return None
    if not populated(data):
        return None
    return make_response(
        data, source=_SOURCE_TUSHARE, symbol=disp, data_type=data_type, **extra
    )


_OUT_GET_FINANCIAL_STATEMENTS = output_model(
    "GetFinancialStatementsOut",
    envelope_schema(
        ANY,
        frame=("symbol",),
        echo={"data_type": STR, "statement_type": STR, "period": STR},
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
        dict: {symbol, count, data, source, data_type, statement_type, period}.
        One statement_type → a list of period records; "all" → {income_statement,
        balance_sheet, cash_flow} lists with count the total. Common camelCase
        fields: date, revenue, netIncome, eps, totalAssets, operatingCashFlow.
        date is "YYYY-MM-DD"; records are newest-first as returned by FMP. On
        error: {error: <code>, detail, symbol}.
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

    cn = await _from_tushare(
        disp, _cn_statements,
        data_type="financial_statements", statement_type=statement_type, period=period,
    )
    if cn is not None:
        return cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        if statement_type == "income":
            data = await client.get_income_statement(api, period=period, limit=limit)
        elif statement_type == "balance":
            data = await client.get_balance_sheet(api, period=period, limit=limit)
        elif statement_type == "cash":
            data = await client.get_cash_flow(api, period=period, limit=limit)
        else:  # "all"
            income = await client.get_income_statement(api, period=period, limit=limit)
            balance = await client.get_balance_sheet(api, period=period, limit=limit)
            cash_flow = await client.get_cash_flow(api, period=period, limit=limit)
            data = {
                "income_statement": income or [],
                "balance_sheet": balance or [],
                "cash_flow": cash_flow or [],
            }

        return make_response(
            data if statement_type == "all" else (data or []),
            source=_SOURCE,
            symbol=disp,
            data_type="financial_statements",
            statement_type=statement_type,
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


_OUT_GET_FINANCIAL_RATIOS = output_model(
    "GetFinancialRatiosOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, "period": STR},
    ),
)


@mcp.tool()
async def get_financial_ratios(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_FINANCIAL_RATIOS:
    """Fetch raw historical key metrics and financial ratios — track P/E, ROE,
    and margins over time, or compare valuation across companies.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {key_metrics, ratios}, each a list of period records; count is the total.
        key_metrics fields: date, marketCap, enterpriseValue, evToEBITDA,
        freeCashFlowYield. ratios fields: date, netProfitMargin, returnOnEquity,
        currentRatio, debtToEquityRatio, priceToEarningsRatio. Field names are
        FMP-native camelCase; date is "YYYY-MM-DD"; records are newest-first as
        returned by FMP. On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    async def _cn_ratios(ts: GinlixDataCnFinancialSource):
        key_metrics, ratios = await asyncio.gather(
            ts.get_key_metrics_periods(disp, period=period, limit=limit),
            ts.get_ratios_periods(disp, period=period, limit=limit),
        )
        return {"key_metrics": key_metrics, "ratios": ratios}

    # FMP first, the reverse of the other CN branches: the valuation fields
    # promised above (marketCap, enterpriseValue, priceToEarningsRatio) are
    # priced per period, and ginlix-data's CN rows carry only the accounting
    # half. Those rows still answer a CN listing that FMP cannot.
    async def _or_cn(fmp_answer: dict) -> dict:
        cn = await _from_tushare(
            disp, _cn_ratios, data_type="financial_ratios", period=period
        )
        return fmp_answer if cn is None else cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return await _or_cn(
            make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)
        )

    try:
        key_metrics = await client.get_key_metrics(api, period=period, limit=limit)
        ratios = await client.get_financial_ratios(api, period=period, limit=limit)
    except Exception as e:  # noqa: BLE001
        return await _or_cn(error_from_exception(e, _UPSTREAM_FAILED, symbol=disp))

    data = {"key_metrics": key_metrics or [], "ratios": ratios or []}
    answer = make_response(
        data, source=_SOURCE, symbol=disp, data_type="financial_ratios", period=period
    )
    return answer if populated(data) else await _or_cn(answer)


_OUT_GET_GROWTH_METRICS = output_model(
    "GetGrowthMetricsOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, "period": STR},
    ),
)


@mcp.tool()
async def get_growth_metrics(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_GROWTH_METRICS:
    """Fetch raw historical growth rates for trend analysis — chart revenue/EPS
    trajectory or compare growth across competitors.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {financial_growth, income_statement_growth}, each a list; count is the
        total. financial_growth fields: date, revenueGrowth, netIncomeGrowth,
        epsgrowth; income_statement_growth fields: date, growthRevenue,
        growthNetIncome, growthEPS. Growth values are decimal fractions
        (0.1 = 10%). Field names are FMP-native camelCase; date is "YYYY-MM-DD";
        records are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)

    cn = await _from_tushare(
        disp,
        lambda ts: ts.get_growth_periods(disp, period=period, limit=limit),
        data_type="growth_metrics", period=period,
    )
    if cn is not None:
        return cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        financial_growth = await client.get_financial_growth(api, period=period, limit=limit)
        income_growth = await client.get_income_statement_growth(api, period=period, limit=limit)

        return make_response(
            {
                "financial_growth": financial_growth or [],
                "income_statement_growth": income_growth or [],
            },
            source=_SOURCE,
            symbol=disp,
            data_type="growth_metrics",
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


_OUT_GET_HISTORICAL_VALUATION = output_model(
    "GetHistoricalValuationOut",
    envelope_schema(
        RECORDS_BY_KEY,
        frame=("symbol",),
        echo={"data_type": STR, "period": STR},
    ),
)


@mcp.tool()
async def get_historical_valuation(
    symbol: str,
    period: Literal["annual", "quarter"] = "annual",
    limit: int = 10,
) -> _OUT_GET_HISTORICAL_VALUATION:
    """Fetch DCF fair value and enterprise value history — track fair value vs
    price or build valuation trend charts.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        period: "annual" | "quarter".
        limit: Number of periods (default 10).

    Returns:
        dict: {symbol, count, data, source, data_type, period}. data is
        {current_dcf, historical_dcf, enterprise_value}, each a list; count is
        the total. current_dcf fields: symbol, date, dcf, stockPrice.
        historical_dcf is always [] — the stable FMP API no longer exposes it.
        enterprise_value fields: date, stockPrice, marketCapitalization,
        enterpriseValue. Field names are FMP-native camelCase; date is
        "YYYY-MM-DD"; enterprise_value is newest-first as returned by FMP. On
        error: {error: <code>, detail, symbol}.
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

        return make_response(
            {
                "current_dcf": current_dcf or [],
                "historical_dcf": historical_dcf or [],
                "enterprise_value": enterprise_value or [],
            },
            source=_SOURCE,
            symbol=disp,
            data_type="historical_valuation",
            period=period,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


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
    """Fetch insider trading transactions and aggregate buy/sell statistics —
    detect insider buying clusters, screen unusual activity, or gauge C-suite
    confidence.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".
        limit: Number of recent transactions to fetch (default 50).

    Returns:
        dict: {symbol, count, data, source, data_type}. data is {trades, stats},
        each a list; count is the total across both. trades fields: symbol,
        filingDate, transactionDate, reportingName, transactionType,
        securitiesTransacted, price. stats fields: year, quarter, totalBought,
        totalSold, buySellRatio. Field names are FMP-native camelCase; dates are
        "YYYY-MM-DD"; trades are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
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
        echo={"data_type": STR},
    ),
)


@mcp.tool()
async def get_dividends_and_splits(
    symbol: str,
) -> _OUT_GET_DIVIDENDS_AND_SPLITS:
    """Fetch historical dividend payments and stock splits — analyze dividend
    growth, adjust prices for splits, or compare dividend history across peers.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source, data_type}. data is
        {dividends, splits}, each a list; count is the total across both.
        dividends fields: date, recordDate, paymentDate, declarationDate,
        adjDividend, dividend, yield, frequency. splits fields: date, numerator,
        denominator. Field names are FMP-native camelCase; date is "YYYY-MM-DD";
        records are newest-first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    cn = await _from_tushare(
        disp, lambda ts: ts.get_dividends(disp), data_type="dividends_and_splits"
    )
    if cn is not None:
        return cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        dividends = await client.get_dividends(api)
        splits = await client.get_splits(api)

        return make_response(
            {"dividends": dividends or [], "splits": splits or []},
            source=_SOURCE,
            symbol=disp,
            data_type="dividends_and_splits",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


_OUT_GET_SHARES_FLOAT = output_model(
    "GetSharesFloatOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={"data_type": STR},
    ),
)


@mcp.tool()
async def get_shares_float(
    symbol: str,
) -> _OUT_GET_SHARES_FLOAT:
    """Fetch shares float, outstanding shares, and float percentage — flag
    low-float names, gauge ownership concentration, or screen squeeze candidates.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SH".

    Returns:
        dict: {symbol, count, data, source, data_type}. data is a list of
        records; count is the record total. Fields: symbol, date, freeFloat,
        floatShares, outstandingShares. Field names are FMP-native camelCase;
        date is "YYYY-MM-DD"; latest snapshot first as returned by FMP. On error:
        {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    cn = await _from_tushare(
        disp, lambda ts: ts.get_shares_float(disp), data_type="shares_float"
    )
    if cn is not None:
        return cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_shares_float(api)

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="shares_float",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


_OUT_GET_KEY_EXECUTIVES = output_model(
    "GetKeyExecutivesOut",
    envelope_schema(
        RECORDS,
        frame=("symbol",),
        echo={"data_type": STR},
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
        dict: {symbol, count, data, source, data_type}. data is a list of
        executive records; count is the record total. Fields: name, title, pay,
        currencyPay, gender, yearBorn, titleSince. Field names are FMP-native
        camelCase; pay is an integer in currencyPay units; order is as returned
        by FMP (not time-ordered). On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    cn = await _from_tushare(
        disp, lambda ts: ts.get_key_executives(disp), data_type="key_executives"
    )
    if cn is not None:
        return cn

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_key_executives(api)

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="key_executives",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


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
    """Fetch a technical indicator time series over OHLCV bars — plot RSI,
    overlay EMA/MACD, or screen by technical signals.

    Args:
        symbol: Ticker, e.g. "AAPL", "0700.HK", "600519.SH".
        indicator: FMP name — "rsi", "ema", "sma", "wma", "adx", "williams".
        period: Indicator lookback length (default 14).
        timeframe: FMP-native bar — "1min"…"4hour", "1day" (default "1day").

    Returns:
        dict: {symbol, count, data, source, data_type, indicator, period,
        timeframe}. data is a list of bars; count is the bar total. Fields: date,
        open, high, low, close, volume, and the indicator value keyed by its
        name. date is "YYYY-MM-DD" (1day) or "YYYY-MM-DD HH:MM:SS"
        (intraday); field names are FMP-native camelCase; bars newest-first.
        On error: {error: <code>, detail, symbol}.
    """
    disp = _canonical(symbol)
    api = _fmp_symbol(symbol)
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, symbol=disp)

    try:
        data = await client.get_technical_indicator(
            api, indicator=indicator, period=period, timeframe=timeframe
        )

        return make_response(
            data or [],
            source=_SOURCE,
            symbol=disp,
            data_type="technical_indicator",
            indicator=indicator,
            period=period,
            timeframe=timeframe,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, symbol=disp)


if __name__ == "__main__":
    mcp.run(transport="stdio")
