"""FinancialDataSource implementation backed by yfinance.

Free fallback provider — used when FMP is unavailable. Some protocol
methods that have no yfinance equivalent return empty results; callers
already handle this gracefully.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import yfinance as yf

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def _clean_value(val: Any) -> Any:
    """Convert NaN / Timestamp / numpy scalars / pandas NA to JSON-safe values."""
    if val is None:
        return None
    # pandas NA/NaT
    try:
        import pandas as pd

        if val is pd.NA or val is pd.NaT:
            return None
    except ImportError:
        pass
    if isinstance(val, float) and val != val:  # NaN
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    # numpy scalars → native Python (np.float64→float, np.int64→int, etc.)
    if hasattr(val, "item"):
        return val.item()
    return val


def _dataframe_to_records(df, limit: int | None = None) -> list[dict[str, Any]]:
    """Convert a yfinance financial statement DataFrame to list[dict].

    yfinance statements have metrics as rows and dates as columns.
    We transpose so each dict represents one period, matching FMP's format.
    """
    if df is None or df.empty:
        return []
    df = df.copy()
    records = []
    for col in df.columns:
        period: dict[str, Any] = {}
        if hasattr(col, "strftime"):
            period["date"] = col.strftime("%Y-%m-%d")
        else:
            period["date"] = str(col)
        for metric in df.index:
            key = str(metric).lower().replace(" ", "_")
            period[key] = _clean_value(df.loc[metric, col])
        records.append(period)
        if limit and len(records) >= limit:
            break
    return records


def _remap_keys(record: dict[str, Any], key_map: dict[str, str]) -> dict[str, Any]:
    """Remap snake_case keys to FMP-compatible names."""
    return {key_map.get(k, k): v for k, v in record.items()}


# yfinance pretty-print row names → lowercase_underscore → FMP camelCase
_INCOME_STMT_KEY_MAP: dict[str, str] = {
    "total_revenue": "revenue",
    "net_income": "netIncome",
    "gross_profit": "grossProfit",
    "operating_income": "operatingIncome",
    "cost_of_revenue": "costOfRevenue",
    "diluted_eps": "epsdiluted",
    "basic_eps": "basicEps",
    "interest_expense": "interestExpense",
    "tax_provision": "incomeTaxExpense",
    "pretax_income": "incomeBeforeTax",
    "research_and_development": "researchAndDevelopmentExpenses",
    "selling_general_and_administration": "sellingGeneralAndAdministrativeExpenses",
    "net_income_from_continuing_operations": "netIncomeFromContinuingOperations",
}

_CASHFLOW_KEY_MAP: dict[str, str] = {
    "operating_cash_flow": "operatingCashFlow",
    "capital_expenditure": "capitalExpenditure",
    "free_cash_flow": "freeCashFlow",
    "investing_cash_flow": "investingCashFlow",
    "financing_cash_flow": "financingCashFlow",
    "depreciation_and_amortization": "depreciationAndAmortization",
    "stock_based_compensation": "stockBasedCompensation",
    "net_income_from_continuing_operations": "netIncome",
    "change_in_working_capital": "changeInWorkingCapital",
    "cash_dividends_paid": "dividendsPaid",
    "repurchase_of_capital_stock": "commonStockRepurchased",
}

_EARNINGS_KEY_MAP: dict[str, str] = {
    "earnings_date": "date",
    "reported_eps": "eps",
    "eps_estimate": "epsEstimated",
    "surprisepct": "surprisePercentage",
}

# SPDR sector ETF symbols — stable proxies for GICS sector performance
_SECTOR_ETFS: dict[str, str] = {
    "Basic Materials": "XLB",
    "Communication Services": "XLC",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Industrials": "XLI",
    "Real Estate": "XLRE",
    "Technology": "XLK",
    "Utilities": "XLU",
}

# FMP camelCase screener filter → (yfinance screener field, operator)
_SCREENER_FILTER_MAP: dict[str, tuple[str, str]] = {
    "marketCapMoreThan": ("intradaymarketcap", "GT"),
    "marketCapLowerThan": ("intradaymarketcap", "LT"),
    "priceMoreThan": ("intradayprice", "GT"),
    "priceLowerThan": ("intradayprice", "LT"),
    "volumeMoreThan": ("dayvolume", "GT"),
    "volumeLowerThan": ("dayvolume", "LT"),
    "betaMoreThan": ("beta", "GT"),
    "betaLowerThan": ("beta", "LT"),
    "dividendMoreThan": ("forward_dividend_yield", "GT"),
    "dividendLowerThan": ("forward_dividend_yield", "LT"),
}

# FMP exchange name → Yahoo Finance exchange codes
_EXCHANGE_TO_YF: dict[str, list[str]] = {
    "NASDAQ": ["NMS", "NGM", "NCM"],
    "NYSE": ["NYQ"],
    "AMEX": ["ASE"],
}


def _get_profile(symbol: str) -> list[dict[str, Any]]:
    ticker = yf.Ticker(symbol)
    fi = ticker.fast_info
    info = ticker.info or {}
    if not info:
        return []
    profile = {
        "symbol": symbol,
        "companyName": info.get("longName") or info.get("shortName"),
        "currency": fi.get("currency"),
        "exchange": fi.get("exchange"),
        "exchangeShortName": fi.get("exchange"),
        "industry": info.get("industry"),
        "sector": info.get("sector"),
        "country": info.get("country"),
        "description": info.get("longBusinessSummary"),
        "website": info.get("website"),
        "ceo": None,
        "fullTimeEmployees": info.get("fullTimeEmployees"),
        "ipoDate": None,
        "mktCap": fi.get("marketCap"),
        "price": float(fi.get("lastPrice", 0) or 0) or None,
        "volAvg": fi.get("threeMonthAverageVolume"),
        "beta": info.get("beta"),
        "lastDiv": info.get("dividendRate"),
    }
    return [{k: _clean_value(v) for k, v in profile.items()}]


def _get_realtime_quote(symbol: str) -> list[dict[str, Any]]:
    ticker = yf.Ticker(symbol)
    fi = ticker.fast_info
    info = ticker.info or {}
    price = float(fi.get("lastPrice", 0) or 0)
    prev = float(fi.get("previousClose", 0) or 0)
    change = price - prev if prev else 0.0
    change_pct = (change / prev * 100) if prev else 0.0
    return [
        {
            "symbol": symbol,
            "name": info.get("longName") or info.get("shortName"),
            "price": round(price, 4),
            "change": round(change, 4),
            "changesPercentage": round(change_pct, 4),
            "previousClose": round(prev, 4),
            "open": round(float(fi.get("open", 0) or 0), 4),
            "dayHigh": round(float(fi.get("dayHigh", 0) or 0), 4),
            "dayLow": round(float(fi.get("dayLow", 0) or 0), 4),
            "volume": int(fi.get("lastVolume", 0) or 0),
            "marketCap": fi.get("marketCap"),
            "pe": info.get("trailingPE"),
            "eps": info.get("trailingEps"),
        }
    ]


def _get_income_statements(
    symbol: str, period: str, limit: int
) -> list[dict[str, Any]]:
    ticker = yf.Ticker(symbol)
    df = ticker.quarterly_income_stmt if period == "quarter" else ticker.income_stmt
    result = []
    for r in _dataframe_to_records(df):
        mapped = _remap_keys(r, _INCOME_STMT_KEY_MAP)
        rev = mapped.get("revenue")
        # yfinance creates a row for each newly-reported quarter where only EPS
        # is populated; income line items land later. FMP never returns such rows.
        if not isinstance(rev, (int, float)) or rev == 0:
            continue
        gp = mapped.get("grossProfit")
        oi = mapped.get("operatingIncome")
        ni = mapped.get("netIncome")
        if isinstance(gp, (int, float)):
            mapped["grossProfitRatio"] = round(gp / rev, 6)
        if isinstance(oi, (int, float)):
            mapped["operatingIncomeRatio"] = round(oi / rev, 6)
        if isinstance(ni, (int, float)):
            mapped["netIncomeRatio"] = round(ni / rev, 6)
        result.append(mapped)
        if len(result) >= limit:
            break
    return result


def _get_cash_flows(symbol: str, period: str, limit: int) -> list[dict[str, Any]]:
    ticker = yf.Ticker(symbol)
    df = ticker.quarterly_cashflow if period == "quarter" else ticker.cashflow
    return [_remap_keys(r, _CASHFLOW_KEY_MAP) for r in _dataframe_to_records(df, limit)]


def _get_key_metrics(symbol: str) -> list[dict[str, Any]]:
    ticker = yf.Ticker(symbol)
    fi = ticker.fast_info
    info = ticker.info or {}
    if not info:
        return []
    metrics = {
        "symbol": symbol,
        "peRatio": info.get("trailingPE"),
        "forwardPERatio": info.get("forwardPE"),
        "priceToBookRatio": info.get("priceToBook"),
        "priceToSalesRatio": info.get("priceToSalesTrailing12Months"),
        "enterpriseValueOverEBITDA": info.get("enterpriseToEbitda"),
        "enterpriseValue": info.get("enterpriseValue"),
        "marketCap": fi.get("marketCap"),
        "beta": info.get("beta"),
        "dividendYield": info.get("dividendYield"),
        "payoutRatio": info.get("payoutRatio"),
        "returnOnEquity": info.get("returnOnEquity"),
        "returnOnAssets": info.get("returnOnAssets"),
        "debtToEquity": info.get("debtToEquity"),
        "currentRatio": info.get("currentRatio"),
        "quickRatio": info.get("quickRatio"),
        "revenuePerShare": info.get("revenuePerShare"),
        "bookValuePerShare": info.get("bookValue"),
        "earningsYield": (
            (1.0 / info["trailingPE"]) if info.get("trailingPE") else None
        ),
    }
    return [{k: _clean_value(v) for k, v in metrics.items()}]


def _get_financial_ratios(symbol: str) -> list[dict[str, Any]]:
    info = yf.Ticker(symbol).info or {}
    if not info:
        return []
    ratios = {
        "symbol": symbol,
        "grossProfitMargin": info.get("grossMargins"),
        "operatingProfitMargin": info.get("operatingMargins"),
        "netProfitMargin": info.get("profitMargins"),
        "returnOnEquity": info.get("returnOnEquity"),
        "returnOnAssets": info.get("returnOnAssets"),
        "debtToEquity": info.get("debtToEquity"),
        "currentRatio": info.get("currentRatio"),
        "quickRatio": info.get("quickRatio"),
        "dividendYield": info.get("dividendYield"),
        "payoutRatio": info.get("payoutRatio"),
        "peRatio": info.get("trailingPE"),
        "priceToBookRatio": info.get("priceToBook"),
        "priceToSalesRatio": info.get("priceToSalesTrailing12Months"),
    }
    return [{k: _clean_value(v) for k, v in ratios.items()}]


_perf_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_PERF_CACHE_TTL = 300  # 5 minutes


def _get_price_performance(symbol: str) -> list[dict[str, Any]]:
    """Compute price returns over standard periods from daily history."""
    now_ts = time.monotonic()
    cached = _perf_cache.get(symbol)
    if cached and (now_ts - cached[0]) < _PERF_CACHE_TTL:
        return cached[1]

    ticker = yf.Ticker(symbol)
    now = datetime.now(_ET)
    try:
        df = ticker.history(start=(now - timedelta(days=3650)).strftime("%Y-%m-%d"), interval="1d")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    df = df.copy()
    close = df["Close"]
    latest = float(close.iloc[-1])

    def _pct(days: int) -> float | None:
        target = now - timedelta(days=days)
        subset = close.loc[:target]
        if subset.empty:
            return None
        ref = float(subset.iloc[-1])
        return round((latest - ref) / ref * 100, 4) if ref else None

    result = [
        {
            "symbol": symbol,
            "1D": _pct(1),
            "5D": _pct(5),
            "1M": _pct(30),
            "3M": _pct(90),
            "6M": _pct(180),
            "ytd": _pct((now - datetime(now.year, 1, 1, tzinfo=_ET)).days),
            "1Y": _pct(365),
            "3Y": _pct(1095),
            "5Y": _pct(1825),
            "10Y": _pct(3650),
        }
    ]
    _perf_cache[symbol] = (time.monotonic(), result)
    return result


def _get_analyst_price_targets(symbol: str) -> list[dict[str, Any]]:
    try:
        targets = yf.Ticker(symbol).analyst_price_targets
    except Exception:
        return []
    if not targets:
        return []
    if isinstance(targets, dict):
        return [
            {
                "symbol": symbol,
                "targetHigh": _clean_value(targets.get("high")),
                "targetLow": _clean_value(targets.get("low")),
                "targetMean": _clean_value(targets.get("mean")),
                "targetMedian": _clean_value(targets.get("median")),
                "targetConsensus": _clean_value(targets.get("current")),
            }
        ]
    return []


def _get_analyst_ratings(symbol: str) -> list[dict[str, Any]]:
    try:
        recs = yf.Ticker(symbol).recommendations_summary
    except Exception:
        return []
    if recs is None or (hasattr(recs, "empty") and recs.empty):
        return []
    if not hasattr(recs, "to_dict"):
        return []
    records = recs.to_dict(orient="records")
    result = []
    for r in records:
        cleaned = {k: _clean_value(v) for k, v in r.items()}
        # Derive consensus label from rating counts (FMP provides this; yfinance does not)
        weights = {"strongBuy": 5, "buy": 4, "hold": 3, "sell": 2, "strongSell": 1}
        total = sum(cleaned.get(k) or 0 for k in weights)
        if total > 0:
            score = sum((cleaned.get(k) or 0) * w for k, w in weights.items()) / total
            if score >= 4.5:
                cleaned["consensus"] = "Strong Buy"
            elif score >= 3.5:
                cleaned["consensus"] = "Buy"
            elif score >= 2.5:
                cleaned["consensus"] = "Hold"
            elif score >= 1.5:
                cleaned["consensus"] = "Sell"
            else:
                cleaned["consensus"] = "Strong Sell"
        result.append(cleaned)
    return result


def _get_earnings_history(symbol: str, limit: int) -> list[dict[str, Any]]:
    try:
        dates = yf.Ticker(symbol).earnings_dates
    except Exception:
        return []
    if dates is None or dates.empty:
        return []
    dates = dates.copy().head(limit).reset_index()
    records = []
    for _, row in dates.iterrows():
        record: dict[str, Any] = {}
        for col in dates.columns:
            key = str(col).lower().replace(" ", "_").replace("(", "").replace(")", "").replace("%", "pct")
            record[key] = _clean_value(row[col])
        records.append(_remap_keys(record, _EARNINGS_KEY_MAP))
    return records


def _get_single_sector_perf(sector_name: str, etf_symbol: str) -> dict[str, Any] | None:
    """Fetch daily change for one sector via its representative ETF."""
    try:
        fi = yf.Ticker(etf_symbol).fast_info
        price = float(fi.get("lastPrice", 0) or 0)
        prev = float(fi.get("previousClose", 0) or 0)
        if not prev:
            return None
        pct = (price - prev) / prev * 100
        sign = "+" if pct >= 0 else ""
        return {
            "sector": sector_name,
            "changesPercentage": f"{sign}{pct:.2f}%",
        }
    except Exception:
        logger.debug("Failed to fetch sector perf for %s (%s)", sector_name, etf_symbol)
        return None


def _screen_stocks_sync(**filters: Any) -> list[dict[str, Any]]:
    """Run yfinance screener with FMP-compatible filter kwargs."""
    try:
        from yfinance.screener.query import EquityQuery
        from yfinance.screener.screener import screen as yf_screen
    except ImportError:
        logger.warning("yfinance screener module not available")
        return []

    limit = min(int(filters.pop("limit", 25)), 250)

    # Build EquityQuery nodes from FMP-style camelCase filters
    nodes: list[EquityQuery] = []

    for fmp_key, (yf_field, op) in _SCREENER_FILTER_MAP.items():
        val = filters.get(fmp_key)
        if val is not None:
            nodes.append(EquityQuery(op, [yf_field, float(val)]))

    sector = filters.get("sector")
    if sector:
        nodes.append(EquityQuery("EQ", ["sector", sector]))

    industry = filters.get("industry")
    if industry:
        nodes.append(EquityQuery("EQ", ["industry", industry]))

    exchange = filters.get("exchange")
    if exchange:
        yf_codes = _EXCHANGE_TO_YF.get(exchange.upper())
        if yf_codes:
            nodes.append(EquityQuery("IS-IN", ["exchange", *yf_codes]))
        else:
            nodes.append(EquityQuery("EQ", ["exchange", exchange]))

    country = filters.get("country")
    if country:
        nodes.append(EquityQuery("EQ", ["region", country.lower()]))

    try:
        if not nodes:
            result = yf_screen("most_actives", count=limit)
        elif len(nodes) == 1:
            result = yf_screen(nodes[0], size=limit)
        else:
            result = yf_screen(EquityQuery("AND", nodes), size=limit)
    except Exception:
        logger.warning("yfinance screener query failed", exc_info=True)
        return []

    quotes = result.get("quotes", []) if isinstance(result, dict) else []

    # Remap Yahoo Finance quote fields → FMP-compatible keys
    output: list[dict[str, Any]] = []
    for q in quotes:
        output.append({
            "symbol": q.get("symbol", ""),
            "companyName": q.get("shortName") or q.get("longName", ""),
            "price": _clean_value(q.get("regularMarketPrice")),
            "marketCap": _clean_value(q.get("marketCap")),
            "sector": q.get("sector"),
            "beta": _clean_value(q.get("beta")),
            "volume": _clean_value(q.get("regularMarketVolume")),
            "changes": _clean_value(q.get("regularMarketChangePercent")),
        })
    return output


def _search_stocks(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        results = yf.Search(query, max_results=limit)
        quotes = results.quotes if hasattr(results, "quotes") else []
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for q in quotes[:limit]:
        out.append({
            "symbol": q.get("symbol", ""),
            "name": q.get("shortname") or q.get("longname", ""),
            "currency": q.get("currency"),
            "stockExchange": q.get("exchange"),
            "exchangeShortName": q.get("exchDisp"),
        })
    return out


class YFinanceFinancialSource:
    """FinancialDataSource backed by Yahoo Finance (yfinance library)."""

    async def get_company_profile(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_profile, symbol)

    async def get_realtime_quote(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_realtime_quote, symbol)

    async def get_income_statements(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_income_statements, symbol, period, limit)

    async def get_cash_flows(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_cash_flows, symbol, period, limit)

    async def get_key_metrics(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_key_metrics, symbol)

    async def get_financial_ratios(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_financial_ratios, symbol)

    async def get_price_performance(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_price_performance, symbol)

    async def get_analyst_price_targets(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_analyst_price_targets, symbol)

    async def get_analyst_ratings(self, symbol: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_analyst_ratings, symbol)

    async def get_earnings_history(
        self, symbol: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_get_earnings_history, symbol, limit)

    async def get_revenue_by_segment(
        self, symbol: str, segment_type: str = "product", **kwargs: Any
    ) -> list[dict[str, Any]]:
        return []  # Not available in yfinance

    async def get_sector_performance(self) -> list[dict[str, Any]]:
        tasks = [
            asyncio.to_thread(_get_single_sector_perf, name, etf)
            for name, etf in _SECTOR_ETFS.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, dict)]

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_screen_stocks_sync, **filters)

    async def search_stocks(
        self, query: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_search_stocks, query, limit)

    async def close(self) -> None:
        pass
