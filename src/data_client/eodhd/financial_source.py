"""FinancialDataSource implementation backed by EODHD.

Maps the FinancialDataSource protocol to EODHD's fundamentals API.
Methods not supported by the Fundamentals plan return empty results.
"""

from __future__ import annotations

from typing import Any

from .eodhd_client import EODHDClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _num(val: Any) -> float | int | None:
    """Convert EODHD string numerics to Python numbers."""
    if val is None or val == "None" or val == "NA":
        return None
    if isinstance(val, (int, float)):
        return val
    try:
        f = float(val)
        return int(f) if f == int(f) and abs(f) > 1 else f
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Field mapping helpers
# ---------------------------------------------------------------------------

def _map_profile(raw: dict[str, Any]) -> dict[str, Any]:
    """Map EODHD General section → FMP-compatible profile dict."""
    return {
        "symbol": raw.get("Code", ""),
        "companyName": raw.get("Name", ""),
        "currency": raw.get("CurrencyCode"),
        "exchange": raw.get("Exchange"),
        "exchangeShortName": raw.get("Exchange"),
        "industry": raw.get("Industry"),
        "sector": raw.get("Sector"),
        "country": raw.get("CountryName"),
        "description": raw.get("Description"),
        "website": raw.get("WebURL"),
        "image": raw.get("LogoURL"),
        "ipoDate": raw.get("IPODate"),
        "isin": raw.get("ISIN"),
        "fullTimeEmployees": raw.get("FullTimeEmployees"),
    }


def _map_income_stmt(raw: dict[str, Any]) -> dict[str, Any]:
    """Map EODHD income statement → FMP-compatible dict."""
    rev = _num(raw.get("totalRevenue"))
    gp = _num(raw.get("grossProfit"))
    oi = _num(raw.get("operatingIncome"))
    ni = _num(raw.get("netIncome"))
    # Compute margin ratios (EODHD only has absolute values)
    gp_ratio = round(gp / rev, 6) if rev and gp and rev != 0 else None
    oi_ratio = round(oi / rev, 6) if rev and oi and rev != 0 else None
    ni_ratio = round(ni / rev, 6) if rev and ni and rev != 0 else None
    return {
        "date": raw.get("date"),
        "period": raw.get("period"),
        "calendarYear": raw.get("date", "")[:4] if raw.get("date") else None,
        "revenue": rev,
        "costOfRevenue": _num(raw.get("costOfRevenue")),
        "grossProfit": gp,
        "operatingIncome": oi,
        "netIncome": ni,
        "ebitda": _num(raw.get("ebitda")),
        "epsdiluted": _num(raw.get("epsDiluted")),
        "grossProfitRatio": gp_ratio,
        "operatingIncomeRatio": oi_ratio,
        "netIncomeRatio": ni_ratio,
        "researchAndDevelopmentExpenses": _num(raw.get("researchDevelopment")),
        "sellingGeneralAndAdministrativeExpenses": _num(
            raw.get("sellingGeneralAdministrative")
        ),
    }


def _map_cashflow(raw: dict[str, Any]) -> dict[str, Any]:
    """Map EODHD cash flow → FMP-compatible dict."""
    return {
        "date": raw.get("date"),
        "period": raw.get("period"),
        "calendarYear": raw.get("date", "")[:4] if raw.get("date") else None,
        "operatingCashFlow": _num(raw.get("totalCashFromOperatingActivities")),
        "capitalExpenditure": _num(raw.get("capitalExpenditures")),
        "freeCashFlow": _num(raw.get("freeCashFlow")),
        "netIncome": _num(raw.get("netIncome")),
        "depreciationAndAmortization": _num(raw.get("depreciation")),
        "stockBasedCompensation": _num(raw.get("stockBasedCompensation")),
        "dividendsPaid": _num(raw.get("dividendsPaid")),
    }


def _map_earnings(raw: dict[str, Any]) -> dict[str, Any]:
    """Map EODHD earnings history entry → FMP-compatible dict."""
    return {
        "date": raw.get("reportDate") or raw.get("date"),
        "fiscalDateEnding": raw.get("date"),
        "eps": _num(raw.get("epsActual")),
        "epsEstimated": _num(raw.get("epsEstimate")),
        "epsDifference": _num(raw.get("epsDifference")),
        "surprisePercent": _num(raw.get("surprisePercent")),
        "revenue": None,
        "revenueEstimated": None,
    }


# ---------------------------------------------------------------------------
# Financial source
# ---------------------------------------------------------------------------

class EODHDFinancialSource:
    """FinancialDataSource backed by EODHD."""

    def __init__(self, client: EODHDClient) -> None:
        self._client = client

    async def get_company_profile(self, symbol: str) -> list[dict[str, Any]]:
        raw = await self._client.get_profile(symbol)
        if not raw or not raw.get("Name"):
            return []
        return [_map_profile(raw)]

    async def get_realtime_quote(self, symbol: str) -> list[dict[str, Any]]:
        # Not available on Fundamentals plan — return empty so Alpaca/YFinance
        # fallback fills this in.
        return []

    async def get_income_statements(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        eodhd_period = "quarterly" if period == "quarter" else "yearly"
        raw_list = await self._client.get_income_statement(
            symbol, period=eodhd_period, limit=limit
        )
        return [_map_income_stmt(r) for r in raw_list if isinstance(r, dict)]

    async def get_cash_flows(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        eodhd_period = "quarterly" if period == "quarter" else "yearly"
        raw_list = await self._client.get_cash_flow(
            symbol, period=eodhd_period, limit=limit
        )
        return [_map_cashflow(r) for r in raw_list if isinstance(r, dict)]

    async def get_key_metrics(self, symbol: str) -> list[dict[str, Any]]:
        raw = await self._client.get_highlights(symbol)
        if not raw:
            return []
        pe = _num(raw.get("PERatio"))
        return [
            {
                "symbol": symbol,
                "marketCap": _num(raw.get("MarketCapitalization")),
                "peRatio": pe,
                "pegRatio": _num(raw.get("PEGRatio")),
                "earningsYield": round(1 / pe, 6) if pe else None,
                "dividendYield": _num(raw.get("DividendYield")),
                "returnOnEquity": _num(raw.get("ReturnOnEquityTTM")),
                "returnOnAssets": _num(raw.get("ReturnOnAssetsTTM")),
                "revenuePerShare": _num(raw.get("RevenuePerShareTTM")),
                "bookValuePerShare": _num(raw.get("BookValue")),
                "beta": None,
            }
        ]

    async def get_financial_ratios(self, symbol: str) -> list[dict[str, Any]]:
        raw = await self._client.get_fundamentals(
            symbol, "Highlights,Valuation,Technicals"
        )
        if not raw:
            return []
        hl = raw.get("Highlights", {})
        val = raw.get("Valuation", {})
        tech = raw.get("Technicals", {})
        return [
            {
                "symbol": symbol,
                "peRatio": _num(hl.get("PERatio")) or _num(val.get("TrailingPE")),
                "forwardPE": _num(val.get("ForwardPE")),
                "priceToBookRatio": _num(val.get("PriceBookMRQ")),
                "priceToSalesRatio": _num(val.get("PriceSalesTTM")),
                "enterpriseValueOverEBITDA": _num(val.get("EnterpriseValueEbitda")),
                "enterpriseValue": _num(val.get("EnterpriseValue")),
                "grossProfitMargin": _num(hl.get("ProfitMargin")),
                "operatingProfitMargin": _num(hl.get("OperatingMarginTTM")),
                "returnOnEquity": _num(hl.get("ReturnOnEquityTTM")),
                "returnOnAssets": _num(hl.get("ReturnOnAssetsTTM")),
                "dividendYield": _num(hl.get("DividendYield")),
                "beta": _num(tech.get("Beta")),
            }
        ]

    async def get_price_performance(self, symbol: str) -> list[dict[str, Any]]:
        # Not available on Fundamentals plan — Alpaca/YFinance fallback handles.
        return []

    async def get_analyst_price_targets(
        self, symbol: str
    ) -> list[dict[str, Any]]:
        raw = await self._client.get_analyst_ratings(symbol)
        if not raw or raw == "NA" or not raw.get("TargetPrice"):
            return []
        return [
            {
                "symbol": symbol,
                "targetConsensus": _num(raw.get("TargetPrice")),
                "targetHigh": None,
                "targetLow": None,
                "targetMedian": None,
            }
        ]

    async def get_analyst_ratings(self, symbol: str) -> list[dict[str, Any]]:
        raw = await self._client.get_analyst_ratings(symbol)
        if not raw or raw == "NA":
            return []
        return [
            {
                "strongBuy": _num(raw.get("StrongBuy")) or 0,
                "buy": _num(raw.get("Buy")) or 0,
                "hold": _num(raw.get("Hold")) or 0,
                "sell": _num(raw.get("Sell")) or 0,
                "strongSell": _num(raw.get("StrongSell")) or 0,
                "consensus": raw.get("Rating", "N/A"),
            }
        ]

    async def get_earnings_history(
        self, symbol: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        raw_list = await self._client.get_earnings_history(symbol, limit=limit)
        return [_map_earnings(r) for r in raw_list if isinstance(r, dict)]

    async def get_revenue_by_segment(
        self, symbol: str, segment_type: str = "product", **kwargs: Any
    ) -> list[dict[str, Any]]:
        # Not available in EODHD
        return []

    async def get_sector_performance(self) -> list[dict[str, Any]]:
        # Not available in EODHD
        return []

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        # Not available on Fundamentals plan
        return []

    async def search_stocks(
        self, query: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        # Not available on Fundamentals plan
        return []

    async def close(self) -> None:
        pass
