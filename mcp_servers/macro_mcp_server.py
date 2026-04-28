#!/usr/bin/env python3
"""Macro MCP Server.

Provides macro-economic data, treasury rates, risk premium, and event calendars via MCP.
Designed for DCF/WACC calculations, catalyst tracking, and morning note generation.

Tools:
- get_economic_indicator: Time series for GDP, CPI, unemployment, etc.
- get_economic_calendar: Upcoming macro events with prior/estimate/actual values
- get_treasury_rates: Full yield curve (1M to 30Y)
- get_market_risk_premium: Risk premium by country for CAPM/WACC
- get_earnings_calendar: All companies reporting in a date range
"""

from __future__ import annotations

from typing import Optional

from mcp.server.fastmcp import FastMCP

from data_client.fmp import get_fmp_client, fmp_lifespan


mcp = FastMCP("MacroMCP", lifespan=fmp_lifespan)


def _mark_truncated_if_provider_cap_reached(result: dict, data: object) -> None:
    if isinstance(data, list) and len(data) >= 4000:
        result["truncated"] = True
        result["note"] = (
            "Results capped at 4000 records by the FMP provider. "
            "Narrow the date range to ensure complete data."
        )


@mcp.tool()
async def get_economic_indicator(
    name: str,
    limit: int = 50,
) -> dict:
    """Fetch economic indicator time series data.

    Use cases:
    - Get GDP growth trend for macro outlook
    - Track CPI/inflation for discount rate assumptions
    - Monitor unemployment, Fed funds rate, retail sales for economic context

    Args:
        name: Indicator name — "GDP", "CPI", "unemploymentRate", "federalFundsRate",
              "inflationRate", "retailSales", "industrialProductionTotalIndex",
              "housingStarts", "consumerSentiment", "nonFarmPayrolls"
        limit: Number of data points to fetch (default: 50)

    Returns:
        Raw JSON with date and value for each observation

    Note: this tool is limit-driven (not date-driven). Some economic indicators
    (GDP, federalFundsRate, nonFarmPayrolls) have sparse historical depth on
    standard FMP tiers.
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}", "indicator": name}

    try:
        data = await client.get_economic_indicators(name, limit=limit)

        return {
            "data_type": "economic_indicator",
            "indicator": name,
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "indicator": name}


@mcp.tool()
async def get_economic_calendar(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """Fetch upcoming economic events with prior, estimate, and actual values.

    Use cases:
    - Build catalyst calendar with upcoming macro releases
    - Generate morning note with today's economic events
    - Track Fed meetings, jobs reports, CPI releases

    Args:
        from_date: Start date in YYYY-MM-DD format (default: today)
        to_date: End date in YYYY-MM-DD format (default: 7 days from today)

    Returns:
        Raw JSON with event name, country, date, prior/estimate/actual values
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}"}

    try:
        data = await client.get_economic_calendar(
            from_date=from_date, to_date=to_date
        )

        result = {
            "data_type": "economic_calendar",
            "from_date": from_date,
            "to_date": to_date,
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }
        _mark_truncated_if_provider_cap_reached(result, data)

        return result

    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


@mcp.tool()
async def get_treasury_rates(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """Fetch treasury rates across the full yield curve (1M to 30Y).

    Use cases:
    - Get risk-free rate for DCF/WACC calculations (typically 10Y)
    - Analyze yield curve shape (normal, inverted, flat)
    - Track interest rate trends for valuation sensitivity

    Args:
        from_date: Start date in YYYY-MM-DD format (default: recent data)
        to_date: End date in YYYY-MM-DD format (default: today)

    Returns:
        Raw JSON with date and rates for 1M, 2M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y, 30Y
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}"}

    try:
        data = await client.get_treasury_rates(
            from_date=from_date, to_date=to_date
        )

        return {
            "data_type": "treasury_rates",
            "from_date": from_date,
            "to_date": to_date,
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


@mcp.tool()
async def get_market_risk_premium() -> dict:
    """Fetch market risk premium by country for CAPM/WACC calculations.

    Use cases:
    - Get equity risk premium for DCF cost of equity calculation
    - Compare risk premiums across markets
    - Input for CAPM: E(R) = Rf + Beta * (Rm - Rf)

    Returns:
        Raw JSON with country, risk premium, and total equity risk premium
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}"}

    try:
        data = await client.get_market_risk_premium()

        return {
            "data_type": "market_risk_premium",
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }

    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


@mcp.tool()
async def get_earnings_calendar(
    from_date: str,
    to_date: str,
) -> dict:
    """Fetch earnings calendar for all companies reporting in a date range.

    Use cases:
    - Build catalyst calendar with upcoming earnings dates
    - Generate morning note with today's/this week's earnings reporters
    - Track earnings season volume and key reporters

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format

    Returns:
        Raw JSON with symbol, date, EPS estimate, EPS actual, revenue estimate, revenue actual
    """
    try:
        client = await get_fmp_client()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Failed to initialize FMP client: {e}"}

    try:
        data = await client.get_earnings_calendar_by_date(
            from_date=from_date, to_date=to_date
        )

        result = {
            "data_type": "earnings_calendar",
            "from_date": from_date,
            "to_date": to_date,
            "count": len(data) if data else 0,
            "data": data or [],
            "source": "fmp",
        }
        _mark_truncated_if_provider_cap_reached(result, data)

        return result

    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run()
