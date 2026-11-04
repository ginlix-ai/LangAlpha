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
    from _bootstrap import MCPServer  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._bootstrap import MCPServer

from typing import Optional


from data_client.fmp import get_fmp_client, fmp_lifespan
from mcp_servers._dates import calendar_days, rates_days, upcoming_days
from mcp_servers._envelope import error_from_exception, make_error, make_response
from mcp_servers._schemas import (
    NULLABLE_STR,
    OBJECT,
    RECORDS,
    STR,
    STR_LIST,
    TRUNCATED,
    TRUNCATED_LATEST,
    described,
    envelope_schema,
    output_model,
)


mcp = MCPServer("MacroMCP", lifespan=fmp_lifespan)

_SOURCE = "fmp"
_CLIENT_UNAVAILABLE = "FMP client is unavailable"
_UPSTREAM_FAILED = "FMP request failed"

# The tool documents these two names, but FMP serves the series under others and
# answers the documented ones with a non-JSON error. Every other name is FMP's own.
_FMP_INDICATOR_NAMES = {
    "federalFundsRate": "federalFunds",
    "nonFarmPayrolls": "totalNonfarmPayroll",
}

# What FMP's series are, by FMP name. FMP serves FRED's US series under bare
# names and sends rows of date and value only; each entry here is the series
# the name resolves to, checked against FRED's latest values.
_SERIES = {
    "GDP": {
        "description": "Gross domestic product, nominal (a level, not growth)",
        "unit": "billions of US dollars",
        "frequency": "quarterly",
        "adjustment": "seasonally adjusted annual rate",
    },
    "CPI": {
        "description": "Consumer price index, all urban consumers, all items (an index level, not a rate of change)",
        "unit": "index, 1982-1984 = 100",
        "frequency": "monthly",
        "adjustment": "seasonally adjusted",
    },
    "unemploymentRate": {
        "description": "Civilian unemployment rate",
        "unit": "percent",
        "frequency": "monthly",
        "adjustment": "seasonally adjusted",
    },
    "federalFunds": {
        "description": "Effective federal funds rate, monthly average (not the target range)",
        "unit": "percent",
        "frequency": "monthly",
        "adjustment": "not seasonally adjusted",
    },
    "inflationRate": {
        "description": "10-year breakeven inflation rate, implied by Treasury yields (market-implied; not CPI inflation)",
        "unit": "percent",
        "frequency": "daily",
        "adjustment": "not seasonally adjusted",
    },
    "retailSales": {
        "description": "Retail sales excluding food services",
        "unit": "millions of US dollars",
        "frequency": "monthly",
        "adjustment": "seasonally adjusted",
    },
    "consumerSentiment": {
        "description": "University of Michigan consumer sentiment index",
        "unit": "index, 1966 Q1 = 100",
        "frequency": "monthly",
        "adjustment": "not seasonally adjusted",
    },
    "totalNonfarmPayroll": {
        "description": "Total nonfarm payroll employment (a level, not the monthly change)",
        "unit": "thousands of persons",
        "frequency": "monthly",
        "adjustment": "seasonally adjusted",
    },
}

_PERIOD_MONTHS = {"monthly": 1, "quarterly": 3}


def _missing_periods(rows: list, months: int) -> list[str]:
    """First days of the periods absent between a series' observations, newest first."""
    indices = set()
    for row in rows:
        try:
            indices.add(int(row["date"][:4]) * 12 + int(row["date"][5:7]) - 1)
        except (KeyError, TypeError, ValueError):
            continue
    ordered = sorted(indices)
    missing = [
        index
        for earlier, later in zip(ordered, ordered[1:])
        for index in range(earlier + months, later, months)
    ]
    return [f"{index // 12}-{index % 12 + 1:02d}-01" for index in reversed(missing)]


_OUT_GET_ECONOMIC_INDICATOR = output_model(
    "GetEconomicIndicatorOut",
    envelope_schema(
        RECORDS,
        echo={
            "data_type": STR,
            "indicator": described(STR, "Echoed indicator name."),
            "truncated": TRUNCATED,
            "series": described(
                OBJECT,
                "What the series is: description, country, unit, frequency, adjustment. "
                "Absent for a series not described here.",
            ),
            "missing_periods": described(
                STR_LIST,
                "First days of monthly or quarterly periods that fall between the observations "
                "returned but that FMP has no observation for (present only when there are some).",
            ),
        },
    ),
)


@mcp.tool()
async def get_economic_indicator(
    name: str,
    limit: int = 50,
) -> _OUT_GET_ECONOMIC_INDICATOR:
    """Fetch a US economic indicator series: GDP, CPI, unemployment, Fed funds.

    Reads about 16 years at most.

    Args:
        name: "GDP", "CPI", "unemploymentRate", "federalFundsRate", "inflationRate"
            (10Y breakeven, not CPI), "retailSales", "consumerSentiment",
            "nonFarmPayrolls".
        limit: Observation count (default 50).

    Returns:
        dict: {count, data, source, data_type, indicator, series?, missing_periods?,
        truncated?}. data is {date, value} levels, newest-first; date is the period
        start; CPI is an index (YoY %: 100 * (value / same month a year ago - 1),
        matched by date). series: country, unit, frequency, adjustment;
        missing_periods: absent months or quarters. truncated: true if a size
        cap cut the oldest rows. On error: {error: <code>, detail, indicator}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE, indicator=name)

    try:
        fmp_name = _FMP_INDICATOR_NAMES.get(name, name)
        data = await client.get_economic_indicators(fmp_name, limit=limit)
        extra: dict = {}
        series = _SERIES.get(fmp_name)
        if series:
            extra["series"] = {"country": "US", **series}
            months = _PERIOD_MONTHS.get(series["frequency"])
            missing = _missing_periods(data, months) if months and isinstance(data, list) else []
            if missing:
                extra["missing_periods"] = missing

        if not isinstance(data or [], list):
            # FMP answers some refusals with a 200 and an error object, not rows.
            return make_error("upstream_error", _UPSTREAM_FAILED, indicator=name)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="economic_indicator",
            truncated=getattr(data, "truncated", False),
            indicator=name,
            **extra,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED, indicator=name)


_OUT_GET_ECONOMIC_CALENDAR = output_model(
    "GetEconomicCalendarOut",
    envelope_schema(
        RECORDS,
        frame=("timezone",),
        echo={
            "data_type": STR,
            "from_date": NULLABLE_STR,
            "to_date": NULLABLE_STR,
            "truncated": TRUNCATED_LATEST,
        },
    ),
)


@mcp.tool()
async def get_economic_calendar(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> _OUT_GET_ECONOMIC_CALENDAR:
    """Fetch economic events, such as Fed meetings, jobs reports, and CPI releases.

    Reads about two years at most.

    Args:
        from_date: "YYYY-MM-DD" (default: yesterday UTC, or 7 days before an
            earlier to_date).
        to_date: "YYYY-MM-DD" (default: 7 days after from_date).

    Returns:
        dict: {count, data, source, data_type, timezone, from_date, to_date,
        truncated?}. data is a list of events, oldest first: date, country, event,
        currency, previous, estimate, actual, change, changePercentage, impact,
        unit. actual is null before release; change and changePercentage (null
        before release) compare actual with previous, not estimate. date is UTC
        "YYYY-MM-DD HH:MM:SS". truncated: true if a size cap cut the latest dates.
        On error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        lo, hi = upcoming_days(from_date, to_date)
        data = await client.get_economic_calendar(from_date=lo, to_date=hi)
        truncated = getattr(data, "truncated", False)
        # An unreleased event has no change to measure; FMP sends 0.
        if isinstance(data, list):
            data = [
                {**event, "changePercentage": None}
                if event.get("actual") is None and event.get("changePercentage") is not None
                else event
                for event in data
            ]

        if not isinstance(data or [], list):
            # FMP answers some refusals with a 200 and an error object, not rows.
            return make_error("upstream_error", _UPSTREAM_FAILED)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="economic_calendar",
            truncated=truncated,
            timezone="UTC",
            from_date=lo,
            to_date=hi,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


_OUT_GET_TREASURY_RATES = output_model(
    "GetTreasuryRatesOut",
    envelope_schema(
        RECORDS,
        echo={
            "data_type": STR,
            "from_date": NULLABLE_STR,
            "to_date": NULLABLE_STR,
            "truncated": TRUNCATED,
        },
    ),
)


@mcp.tool()
async def get_treasury_rates(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> _OUT_GET_TREASURY_RATES:
    """Fetch US Treasury rates across the yield curve (1M to 30Y): the risk-free
    rate for DCF/WACC (typically 10Y), the curve shape, or rate trends.

    Reads about two years at most.

    Args:
        from_date: Start date "YYYY-MM-DD" (default: recent data).
        to_date: End date "YYYY-MM-DD" (default: today).

    Returns:
        dict: {count, data, source, data_type, from_date, to_date, truncated?}.
        data: list of daily {date, month1, month2, month3, month6, year1, year2,
        year3, year5, year7, year10, year20, year30}, newest first; rates in
        percent (4.5 = 4.5%); date "YYYY-MM-DD". truncated: true if a size cap
        cut the oldest rows. On error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        lo, hi = rates_days(from_date, to_date)
        data = await client.get_treasury_rates(from_date=lo, to_date=hi)

        if not isinstance(data or [], list):
            # FMP answers some refusals with a 200 and an error object, not rows.
            return make_error("upstream_error", _UPSTREAM_FAILED)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="treasury_rates",
            truncated=getattr(data, "truncated", False),
            from_date=lo,
            to_date=hi,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


_RISK_PREMIUM_METHOD = (
    "Damodaran's country risk premium table (rating-based), republished by FMP without a date"
)

_OUT_GET_MARKET_RISK_PREMIUM = output_model(
    "GetMarketRiskPremiumOut",
    envelope_schema(
        RECORDS,
        echo={
            "data_type": STR,
            "method": described(STR, "What the premiums are and who publishes them."),
            "as_of": described(NULLABLE_STR, "Date of the table; null because FMP does not date it."),
        },
    ),
)


@mcp.tool()
async def get_market_risk_premium() -> _OUT_GET_MARKET_RISK_PREMIUM:
    """Fetch market risk premium by country for CAPM/WACC — get the equity risk
    premium for a DCF cost of equity, or compare premiums across markets.

    Returns:
        dict: {count, data, source, data_type, method, as_of}. data is a list of
        country records; count is the record total. Fields: country, continent,
        countryRiskPremium, totalEquityRiskPremium. Premiums are percent (5.5 =
        5.5%). as_of is null: cite method (the source table) and report the figure
        as undated. On error: {error: <code>, detail}.
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
            method=_RISK_PREMIUM_METHOD,
            as_of=None,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


_OUT_GET_EARNINGS_CALENDAR = output_model(
    "GetEarningsCalendarOut",
    envelope_schema(
        RECORDS,
        echo={"data_type": STR, "from_date": STR, "to_date": STR, "truncated": TRUNCATED_LATEST},
    ),
)


@mcp.tool()
async def get_earnings_calendar(
    from_date: str,
    to_date: str,
) -> _OUT_GET_EARNINGS_CALENDAR:
    """Fetch the earnings calendar for all companies reporting in a date range —
    build a catalyst calendar.

    Reads about a month in peak season. Rows carry no market cap; yf_market
    get_earnings_calendar ranks US listings by it.

    Args:
        from_date: "YYYY-MM-DD".
        to_date: "YYYY-MM-DD".

    Returns:
        dict: {count, data, source, data_type, from_date, to_date, truncated?}. data
        is a list of reporter records, oldest first. Fields: symbol, date,
        epsActual, epsEstimated, revenueActual, revenueEstimated, lastUpdated.
        Amounts are in each listing's own currency, not converted (a suffixed
        symbol such as 7203.T is non-US): compare only within one currency. date
        is "YYYY-MM-DD". truncated: true if a size cap cut the latest dates. On
        error: {error: <code>, detail}.
    """
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error("client_unavailable", _CLIENT_UNAVAILABLE)

    try:
        lo, hi = calendar_days(from_date, to_date)
        data = await client.get_earnings_calendar_by_date(from_date=lo, to_date=hi)

        if not isinstance(data or [], list):
            # FMP answers some refusals with a 200 and an error object, not rows.
            return make_error("upstream_error", _UPSTREAM_FAILED)

        return make_response(
            data or [],
            source=_SOURCE,
            data_type="earnings_calendar",
            truncated=getattr(data, "truncated", False),
            from_date=from_date,
            to_date=to_date,
        )

    except Exception as e:  # noqa: BLE001
        return error_from_exception(e, _UPSTREAM_FAILED)


if __name__ == "__main__":
    mcp.run(transport="stdio")
