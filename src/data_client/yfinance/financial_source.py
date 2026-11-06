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
from market_protocol import display_spelling, to_major_units
from market_protocol.symbology import MINOR_UNIT_PLACES

from src.data_client.base import SCREEN_HOME_EXCHANGES
from src.data_client.normalize import scale_quote_prices

from .data_source import _fetch_single_snapshot
from .yahoo import yahoo_ticker

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
    "reported_eps": "epsActual",
    "eps_estimate": "epsEstimated",
    "surprisepct": "surprisePercentage",
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
    # Per share, as FMP and the CN screen compare it, not a yield.
    "dividendMoreThan": ("forward_dividend_per_share", "GT"),
    "dividendLowerThan": ("forward_dividend_per_share", "LT"),
}

# The country of each screen exchange name: its home exchanges, and BSE, which
# India's screen leaves out only because its lines repeat NSE's.
_EXCHANGE_COUNTRY = {n: c for c, names in SCREEN_HOME_EXCHANGES.items() for n in names} | {"BSE": "IN"}

# Yahoo's codes for each screen exchange name.
_EXCHANGE_TO_YF: dict[str, list[str]] = {
    "NYSE": ["NYQ"],
    "NASDAQ": ["NMS", "NGM", "NCM"],
    "AMEX": ["ASE"],
    "CBOE": ["BTS"],
    "SHH": ["SHH"],
    "SHZ": ["SHZ"],
    "HKSE": ["HKG"],
    "LSE": ["LSE"],
    "JPX": ["JPX"],
    "TSX": ["TOR"],
    "ASX": ["ASX"],
    "XETRA": ["GER"],
    "PAR": ["PAR"],
    "AMS": ["AMS"],
    "MIL": ["MIL"],
    "BME": ["MCE"],
    "SIX": ["EBS"],
    "KSC": ["KSC"],
    "KOE": ["KOE"],
    "TAI": ["TAI"],
    "SES": ["SES"],
    "NSE": ["NSI"],
    "BSE": ["BSE"],
}


def _get_profile(symbol: str) -> list[dict[str, Any]]:
    ticker = yahoo_ticker(symbol)
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
        "marketCap": fi.get("marketCap"),
        "price": float(fi.get("lastPrice", 0) or 0) or None,
        "beta": info.get("beta"),
    }
    return scale_quote_prices([{k: _clean_value(v) for k, v in profile.items()}], symbol)


def _get_realtime_quote(symbol: str) -> list[dict[str, Any]]:
    snap = _fetch_single_snapshot(symbol)
    if snap is None:
        return []
    ticker = yahoo_ticker(symbol)
    info = ticker.info or {}
    row = {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName"),
        "price": snap["price"],
        "change": snap["change"],
        "changePercentage": snap["change_percent"],
        "previousClose": snap["previous_close"],
        "open": snap["open"],
        "dayHigh": snap["high"],
        "dayLow": snap["low"],
        "volume": snap["volume"],
        "marketCap": info.get("marketCap"),
        "pe": info.get("trailingPE"),
        "eps": info.get("trailingEps"),
    }
    row = scale_quote_prices([row], symbol)[0]
    if row["marketCap"] is None:
        # Yahoo's info omits it for some listings (A-shares); price it here on
        # the scaled price, since fast_info's own multiplies a pence quote.
        shares = _clean_value(ticker.fast_info.get("shares"))
        row["marketCap"] = shares * row["price"] if shares else None
    return [row]


def _get_income_statements(
    symbol: str, period: str, limit: int
) -> list[dict[str, Any]]:
    ticker = yahoo_ticker(symbol)
    df = ticker.quarterly_income_stmt if period == "quarter" else ticker.income_stmt
    result = []
    # `limit` is applied after the placeholder filter below, not in
    # _dataframe_to_records, so a limit=N request returns N complete quarters.
    for r in _dataframe_to_records(df):
        mapped = _remap_keys(r, _INCOME_STMT_KEY_MAP)
        rev = mapped.get("revenue")
        # yfinance creates a row for each newly-reported quarter where only EPS
        # is populated; income line items land later. Treat any non-numeric or
        # zero revenue as an unusable row — matches FMP, which never returns one.
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
    return _stamp_reported_currency(ticker, result)


def _get_cash_flows(symbol: str, period: str, limit: int) -> list[dict[str, Any]]:
    ticker = yahoo_ticker(symbol)
    df = ticker.quarterly_cashflow if period == "quarter" else ticker.cashflow
    rows = [_remap_keys(r, _CASHFLOW_KEY_MAP) for r in _dataframe_to_records(df, limit)]
    return _stamp_reported_currency(ticker, rows)


def _stamp_reported_currency(ticker: yf.Ticker, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Label statement rows with the issuer's reporting currency, as FMP's rows are.

    A HK listing of a mainland issuer quotes in HKD but reports in CNY, so the
    listing currency cannot stand in. Unknown stays absent rather than guessed.
    """
    if not rows:
        return rows
    try:
        code = (ticker.info or {}).get("financialCurrency")
    except Exception as exc:
        logger.debug("yfinance financialCurrency lookup failed: %s", exc)
        return rows
    if isinstance(code, str) and code:
        for row in rows:
            row["reportedCurrency"] = code
    return rows


def _percent_to_fraction(val: Any) -> float | None:
    """Yahoo reports debtToEquity and dividendYield as percents (154.0, 0.41).

    A decimal shift, since 0.41 / 100 is 0.0040999999999999995.
    """
    val = _clean_value(val)
    return to_major_units(val, 2) if isinstance(val, (int, float)) else None


# These two stand in for FMP's stable key-metrics-ttm and ratios-ttm, so each
# carries the names and fraction units of its FMP counterpart; the TTM overview
# table reads one shape whichever provider served it.
def _get_key_metrics(symbol: str) -> list[dict[str, Any]]:
    ticker = yahoo_ticker(symbol)
    fi = ticker.fast_info
    info = ticker.info or {}
    if not info:
        return []
    metrics = {
        "symbol": symbol,
        "marketCap": fi.get("marketCap"),
        "enterpriseValueTTM": info.get("enterpriseValue"),
        "evToEBITDATTM": info.get("enterpriseToEbitda"),
        "returnOnEquityTTM": info.get("returnOnEquity"),
        "returnOnAssetsTTM": info.get("returnOnAssets"),
        "currentRatioTTM": info.get("currentRatio"),
        "earningsYieldTTM": (
            (1.0 / info["trailingPE"]) if info.get("trailingPE") else None
        ),
    }
    return [{k: _clean_value(v) for k, v in metrics.items()}]


def _get_financial_ratios(symbol: str) -> list[dict[str, Any]]:
    info = yahoo_ticker(symbol).info or {}
    if not info:
        return []
    ratios = {
        "symbol": symbol,
        "grossProfitMarginTTM": info.get("grossMargins"),
        "operatingProfitMarginTTM": info.get("operatingMargins"),
        "netProfitMarginTTM": info.get("profitMargins"),
        "debtToEquityRatioTTM": _percent_to_fraction(info.get("debtToEquity")),
        "currentRatioTTM": info.get("currentRatio"),
        "quickRatioTTM": info.get("quickRatio"),
        "dividendYieldTTM": _percent_to_fraction(info.get("dividendYield")),
        "dividendPayoutRatioTTM": info.get("payoutRatio"),
        "priceToEarningsRatioTTM": info.get("trailingPE"),
        "priceToEarningsGrowthRatioTTM": info.get("trailingPegRatio"),
        "priceToBookRatioTTM": info.get("priceToBook"),
        "priceToSalesRatioTTM": info.get("priceToSalesTrailing12Months"),
        "revenuePerShareTTM": info.get("revenuePerShare"),
        "bookValuePerShareTTM": info.get("bookValue"),
    }
    return [{k: _clean_value(v) for k, v in ratios.items()}]


_perf_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_PERF_CACHE_TTL = 300  # 5 minutes


def _get_price_performance(symbol: str) -> list[dict[str, Any]]:
    """Compute price returns over standard periods from daily history."""
    symbol = display_spelling(symbol)
    now_ts = time.monotonic()
    cached = _perf_cache.get(symbol)
    if cached and (now_ts - cached[0]) < _PERF_CACHE_TTL:
        return cached[1]

    ticker = yahoo_ticker(symbol)
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
        targets = yahoo_ticker(symbol).analyst_price_targets
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
        recs = yahoo_ticker(symbol).recommendations_summary
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
        dates = yahoo_ticker(symbol).earnings_dates
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


# Yahoo's largest screen page, and the pages a screen that filters its rows
# reads at most: enough for every London line (about 3,450).
_PAGE = 250
_HELD_PAGES = 16

# Regions whose screens can hold a line quoted in a minor unit.
_MINOR_UNIT_REGIONS = {"GB", "ZA", "IL"}

# Yahoo's spelling of the minor units it compares a line in, and the venues
# (Yahoo codes) that quote lines in them.
_MINOR_UNIT_CURRENCIES = {"GBp", "ZAc", "ILA"}
_MINOR_UNIT_VENUES = ["LSE", "CXE", "AQS", "JNB", "TLV"]

# A minor unit is a hundredth of its currency (pence, cents, agorot).
_MINOR = 10 ** MINOR_UNIT_PLACES["GBX"]

# Bars Yahoo compares on a minor-unit line apart from price: the row field each
# is held to, and the bar's scale on such a line (its cap in pence, its
# dividend in pounds while the bar is in pence).
_HELD_PER_ROW = {
    "intradaymarketcap": ("marketCap", _MINOR),
    "forward_dividend_per_share": ("lastAnnualDividend", 1 / _MINOR),
}


def _quote_unit(currency: str | None, dividend_rate: float | None) -> dict[str, Any]:
    """The unit a Yahoo screen row is in, with its dividend in that unit.

    Yahoo names each line's currency, a minor unit in its own spelling. A pence,
    cents or agorot line quotes its price in that unit but its dividend rate in
    the major one, so the rate moves to the minor unit beside it. Cents (ZAc),
    agorot (ILA) and fils (KWF) are units the protocol does not scale, so those
    rows name no currency.
    """
    dividend = dividend_rate
    if currency in _MINOR_UNIT_CURRENCIES and dividend_rate is not None:
        dividend = dividend_rate * _MINOR
    if currency == "GBp":
        return {"currency": "GBP", "quoteUnit": "GBX", "lastAnnualDividend": dividend}
    if currency in ("ZAc", "ILA", "KWF"):
        return {"currency": None, "lastAnnualDividend": dividend}
    return {"currency": currency, "lastAnnualDividend": dividend_rate}


def _screen_query(
    filters: dict[str, Any], minor_lines: bool = False,
) -> tuple[Any, list[tuple[str, str, float]]]:
    """Yahoo's query for FMP-style *filters*, and the bars each row is then held to.

    A bar arrives as FMP compares it: a London price and dividend in pence, a
    cap in the major unit. Yahoo compares a pence line's cap in pence but its
    dividend in pounds, and a major-unit line in its own unit. Where both kinds
    of line can appear, Yahoo gets the bound loose enough for either, and each
    row is held to the bar in its own unit; a row lacking the field fails.
    *minor_lines* asks for the minor-unit venues alone, with each bar as such a
    line is compared.
    """
    from yfinance.screener.query import EquityQuery

    country = (filters.get("country") or "").upper()
    names = [n.strip() for n in (filters.get("exchange") or "").upper().split(",") if n.strip()]
    codes = [c for n in names for c in _EXCHANGE_TO_YF.get(n, [n])]
    # Yahoo's region is where a line lists, so a GB screen also returns Cboe
    # Europe copies of UK lines; a country screen keeps to its own exchanges.
    home = [c for n in SCREEN_HOME_EXCHANGES.get(country, ()) for c in _EXCHANGE_TO_YF[n]]
    if country and any(_EXCHANGE_COUNTRY.get(n, country) != country for n in names):
        raise ValueError(
            "Yahoo's country is where a line lists, so it cannot pair with another country's exchange;"
            " pass exchange alone to screen listings abroad"
        )
    mixed = country in _MINOR_UNIT_REGIONS or any(c in _MINOR_UNIT_VENUES for c in codes)

    nodes: list[EquityQuery] = []
    held: list[tuple[str, str, float]] = []
    for fmp_key, (yf_field, op) in _SCREENER_FILTER_MAP.items():
        val = filters.get(fmp_key)
        if val is None:
            continue
        val = float(val)
        if minor_lines and yf_field in _HELD_PER_ROW:
            val *= _HELD_PER_ROW[yf_field][1]
        elif mixed and yf_field in _HELD_PER_ROW:
            row_field, scale = _HELD_PER_ROW[yf_field]
            held.append((row_field, op, val))
            val = (min if op == "GT" else max)(val, val * scale)
        nodes.append(EquityQuery(op, [yf_field, val]))
    for field in ("sector", "industry"):
        if filters.get(field):
            nodes.append(EquityQuery("EQ", [field, filters[field]]))
    if minor_lines:
        nodes.append(EquityQuery("IS-IN", ["exchange", *_MINOR_UNIT_VENUES]))
    elif codes or home:
        nodes.append(EquityQuery("IS-IN", ["exchange", *(codes or home)]))
    if country:
        nodes.append(EquityQuery("EQ", ["region", country.lower()]))
    if not nodes:
        # A screen with no filter is every listing with a cap, as FMP's is.
        nodes.append(EquityQuery("GT", ["intradaymarketcap", 0]))
    return (nodes[0] if len(nodes) == 1 else EquityQuery("AND", nodes)), held


def _screen_stocks_sync(**filters: Any) -> list[dict[str, Any]]:
    """Run yfinance screener with FMP-compatible filter kwargs, largest caps first."""
    try:
        from yfinance.screener.screener import screen as yf_screen
    except ImportError:
        logger.warning("yfinance screener module not available")
        return []

    if filters.get("isEtf") or filters.get("isFund"):
        raise ValueError("Yahoo's screen covers stocks only; a fund or ETF screen needs FMP")
    limit = min(int(filters.pop("limit", 25)), _PAGE)
    query, held = _screen_query(filters)
    unscoped = not filters.get("country") and not filters.get("exchange")
    if not (unscoped and _sets_a_held_bar(filters)):
        return _read_screen(yf_screen, query, limit, held=held)
    # A screen of every venue compares a minor-unit line in that unit, so it
    # reads the other lines and the minor-unit lines as two screens, each against
    # the bar in its own unit, and ranks the two by cap in the major unit.
    major = _read_screen(
        yf_screen, query, limit, keep=lambda q: q.get("currency") not in _MINOR_UNIT_CURRENCIES,
    )
    minor = _read_screen(
        yf_screen, _screen_query(filters, minor_lines=True)[0], limit,
        keep=lambda q: q.get("currency") in _MINOR_UNIT_CURRENCIES,
    )
    rows = sorted(major + minor, key=lambda r: (r["marketCap"] is not None, r["marketCap"] or 0), reverse=True)
    return rows[:limit]


def _sets_a_held_bar(filters: dict[str, Any]) -> bool:
    """Whether *filters* bound a field Yahoo compares in a minor unit apart from price."""
    return any(
        filters.get(key) is not None
        for key, (yf_field, _) in _SCREENER_FILTER_MAP.items()
        if yf_field in _HELD_PER_ROW
    )


def _read_screen(
    yf_screen: Any,
    query: Any,
    limit: int,
    held: list[tuple[str, str, float]] = (),
    keep: Any = None,
    pages: int = _HELD_PAGES,
) -> list[dict[str, Any]]:
    """Up to *limit* rows of *query*, largest caps first.

    A row is kept when *keep* takes its quote and it clears every *held* bar.
    A screen that drops rows pages on until it has its rows or Yahoo runs out,
    reading at most *pages* pages.
    """
    filtered = bool(held) or keep is not None
    kept: list[dict[str, Any]] = []
    for page in range(pages if filtered else 1):
        # A failed page raises: a part of the screen, or none, is not its answer.
        result = yf_screen(
            query, offset=page * _PAGE, size=_PAGE if filtered else limit,
            sortField="intradaymarketcap", sortAsc=False,
        )
        quotes = result.get("quotes", []) if isinstance(result, dict) else []
        kept += [
            row for q in quotes if keep is None or keep(q)
            for row in [_screen_row(q)]
            if all(row[f] is not None and (row[f] > bar if op == "GT" else row[f] < bar) for f, op, bar in held)
        ]
        if len(kept) >= limit or len(quotes) < _PAGE:
            break
        if filtered and page == pages - 1:
            logger.warning("yfinance screen read %d pages short of %d rows", pages, limit)
    return kept[:limit]


def _screen_row(q: dict[str, Any]) -> dict[str, Any]:
    """A Yahoo screen quote in FMP's screener keys."""
    return {
        **_quote_unit(q.get("currency"), _clean_value(q.get("dividendRate"))),
        "symbol": display_spelling(q.get("symbol") or ""),
        "companyName": q.get("shortName") or q.get("longName", ""),
        "price": _clean_value(q.get("regularMarketPrice")),
        "marketCap": _clean_value(q.get("marketCap")),
        "sector": q.get("sector"),
        "beta": _clean_value(q.get("beta")),
        "volume": _clean_value(q.get("regularMarketVolume")),
        "change": _clean_value(q.get("regularMarketChangePercent")),
    }


def _search_stocks(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        results = yf.Search(query, max_results=limit)
        quotes = results.quotes if hasattr(results, "quotes") else []
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for q in quotes[:limit]:
        out.append({
            "symbol": display_spelling(q.get("symbol") or ""),
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

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_screen_stocks_sync, **filters)

    async def search_stocks(
        self, query: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(_search_stocks, query, limit)

    async def close(self) -> None:
        pass
