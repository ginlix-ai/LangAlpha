#!/usr/bin/env python3
"""Macro MCP Server.

Raw FMP macro data — economic indicators, treasury rates, risk premium, and
event calendars — via MCP. Payloads stay vendor-native inside `data`; the
envelope around them is the standard market-data contract (AGENT_CONTRACT.md).

Tools:
- get_economic_indicator: Time series for GDP, CPI, unemployment, etc.
- get_economic_calendar: Upcoming macro events with prior/estimate/actual values
- get_treasury_rates: Full yield curve (1M to 30Y)
- get_market_risk_premium: Risk premium by country for CAPM/WACC
- get_earnings_calendar: All companies reporting in a date range
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    import _bootstrap  # noqa: F401  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers import _bootstrap  # noqa: F401

from typing import Optional

from mcp.server.fastmcp import FastMCP

from data_client.fmp import get_fmp_client, fmp_lifespan
from mcp_servers._envelope import error_from_exception, make_error, make_response


mcp = FastMCP("MacroMCP", lifespan=fmp_lifespan)

_SOURCE = "fmp"
_CLIENT_UNAVAILABLE = "FMP client is unavailable"
_UPSTREAM_FAILED = "FMP request failed"


@mcp.tool()
async def get_economic_indicator(
    name: str,
    limit: int = 50,
) -> dict:
    """Fetch an economic indicator time series — GDP growth for the macro
    outlook, CPI/inflation for discount-rate assumptions, or unemployment and
    the Fed funds rate for economic context.

    Args:
        name: Indicator — "GDP", "CPI", "unemploymentRate", "federalFundsRate",
            "inflationRate", "retailSales", "consumerSentiment", "nonFarmPayrolls".
        limit: Number of observations to fetch (default 50).

    Returns:
        dict: {count, data, source, data_type, indicator}. data is a list of
        observations; count is the observation total. Fields: date, value (and
        for some series, name). Field names are FMP-native camelCase; date is
        "YYYY-MM-DD"; observations are newest-first as returned by FMP. On error:
        {error: <code>, detail, indicator}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, indicator=name)

    try:
        data = await client.get_economic_indicators(name, limit=limit)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="economic_indicator",
            indicator=name,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, indicator=name)


@mcp.tool()
async def get_economic_calendar(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """Fetch economic events with prior, estimate, and actual values — build a
    catalyst calendar, generate a morning note, or track Fed meetings, jobs
    reports, and CPI releases.

    Args:
        from_date: Start date "YYYY-MM-DD" (default: today).
        to_date: End date "YYYY-MM-DD" (default: 7 days from today).

    Returns:
        dict: {count, data, source, data_type, from_date, to_date}. data is a
        list of events; count is the event total. Fields: date, country, event,
        currency, previous, estimate, actual, change, changePercentage, impact,
        unit. actual/estimate/previous are null for events not yet released.
        Field names are FMP-native camelCase; date is "YYYY-MM-DD HH:MM:SS";
        order is as returned by FMP. On error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        data = await client.get_economic_calendar(from_date=from_date, to_date=to_date)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="economic_calendar",
            from_date=from_date,
            to_date=to_date,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


@mcp.tool()
async def get_treasury_rates(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """Fetch US Treasury rates across the full yield curve (1M to 30Y) — get the
    risk-free rate for DCF/WACC (typically 10Y), read the curve shape, or track
    rate trends.

    Args:
        from_date: Start date "YYYY-MM-DD" (default: recent data).
        to_date: End date "YYYY-MM-DD" (default: today).

    Returns:
        dict: {count, data, source, data_type, from_date, to_date}. data is a
        list of daily records; count is the record total. Each record has a date
        plus per-tenor rate columns: month1, month2, month3, month6, year1,
        year2, year3, year5, year7, year10, year20, year30. Rates are percent
        (4.5 = 4.5%). Field names are FMP-native camelCase; date is "YYYY-MM-DD";
        records are newest-first as returned by FMP. On error:
        {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        data = await client.get_treasury_rates(from_date=from_date, to_date=to_date)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="treasury_rates",
            from_date=from_date,
            to_date=to_date,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


@mcp.tool()
async def get_market_risk_premium() -> dict:
    """Fetch market risk premium by country for CAPM/WACC — get the equity risk
    premium for a DCF cost of equity, or compare premiums across markets.

    Returns:
        dict: {count, data, source, data_type}. data is a list of country
        records; count is the record total. Fields: country, continent,
        countryRiskPremium, totalEquityRiskPremium. Premiums are percent
        (5.5 = 5.5%). Field names are FMP-native camelCase; order is by country
        as returned by FMP (not time-ordered). On error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        data = await client.get_market_risk_premium()

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="market_risk_premium",
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


@mcp.tool()
async def get_earnings_calendar(
    from_date: str,
    to_date: str,
) -> dict:
    """Fetch the earnings calendar for all companies reporting in a date range —
    build a catalyst calendar, generate a morning note, or track earnings-season
    volume.

    Args:
        from_date: Start date "YYYY-MM-DD".
        to_date: End date "YYYY-MM-DD".

    Returns:
        dict: {count, data, source, data_type, from_date, to_date}. data is a
        list of reporter records; count is the record total. Fields: symbol,
        date, epsActual, epsEstimated, revenueActual, revenueEstimated,
        lastUpdated. actual fields are null before a company reports. Field names
        are FMP-native camelCase; date is "YYYY-MM-DD"; order is as returned by
        FMP. On error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        data = await client.get_earnings_calendar_by_date(from_date=from_date, to_date=to_date)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="earnings_calendar",
            from_date=from_date,
            to_date=to_date,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


if __name__ == "__main__":
    mcp.run()
