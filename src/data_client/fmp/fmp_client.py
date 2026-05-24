"""FMP (Financial Modeling Prep) async client — targets the stable API.

Responses pass through unchanged except for the sector performance snapshot;
see :func:`_format_sector_change_pct`.
"""

import asyncio
import json
import os
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import httpx

_CACHE_MAX_SIZE = 512


def _today_utc() -> date:
    """UTC equivalent of ``date.today()`` — the server runs in UTC; local-clock dates drift."""
    return datetime.now(timezone.utc).date()


def _format_sector_change_pct(row: Dict[str, Any]) -> Dict[str, Any]:
    """Render sector snapshot's ``averageChange`` as a percentage string.

    Stable returns ``averageChange`` as a float already in percent units (e.g.
    ``1.42832`` means 1.43%, not 142%); the canonical shape downstream parses
    is the same value as a percentage string under ``changePctStr`` (e.g.
    ``"1.42832%"``). The numeric copy survives on the artifact under
    ``changePercentage``, so string and numeric live alongside without ambiguity.

    Returns a fresh dict — the source row may be held by the LRU cache, so
    in-place mutation would leak the derived field back into cached responses.
    """
    if "averageChange" not in row or "changePctStr" in row:
        return row
    val = row["averageChange"]
    formatted = f"{val:.5f}%" if isinstance(val, (int, float)) else str(val)
    return {**row, "changePctStr": formatted}


class FMPClient:
    """Async client for Financial Modeling Prep's stable API."""

    BASE_URL = "https://financialmodelingprep.com/api"
    STABLE_BASE = "https://financialmodelingprep.com/stable"
    DEFAULT_VERSION = "stable"

    def __init__(self, api_key: Optional[str] = None, cache_ttl: int = 300):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError(
                "FMP API key required. Set FMP_API_KEY environment variable or pass api_key parameter"
            )

        self.cache_ttl = cache_ttl
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._cache_timestamps: Dict[str, datetime] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                http2=True,
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=10),
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    def _build_url(self, endpoint: str, version: Optional[str] = None) -> str:
        version = version or self.DEFAULT_VERSION
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        if version == "stable":
            return f"{self.STABLE_BASE}{endpoint}"
        return f"{self.BASE_URL}/{version}{endpoint}"

    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key not in self._cache_timestamps:
            return False
        cached_time = self._cache_timestamps[cache_key]
        return (datetime.now() - cached_time).total_seconds() < self.cache_ttl

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None,
        use_cache: bool = True,
    ) -> Union[Dict, List]:
        params = params or {}
        params["apikey"] = self.api_key

        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        if use_cache and self._is_cache_valid(cache_key):
            self._cache.move_to_end(cache_key)
            return self._cache[cache_key]

        url = self._build_url(endpoint, version)
        client = await self._get_client()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if use_cache and data:
                self._cache[cache_key] = data
                self._cache_timestamps[cache_key] = datetime.now()
                while len(self._cache) > _CACHE_MAX_SIZE:
                    oldest_key, _ = self._cache.popitem(last=False)
                    self._cache_timestamps.pop(oldest_key, None)

            return data

        except httpx.HTTPStatusError as e:
            raise Exception(f"FMP API request failed: {str(e)}")
        except httpx.TimeoutException as e:
            raise Exception(f"FMP API request timed out: {str(e)}")
        except httpx.RequestError as e:
            raise Exception(f"FMP API request failed: {str(e)}")

    # =====================================================================
    # Financial Statements
    # =====================================================================

    async def get_income_statement(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "income-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_income_statement_ttm(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "income-statement-ttm",
            params={"symbol": symbol, "limit": 1},
        )

    async def get_balance_sheet(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "balance-sheet-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_balance_sheet_ttm(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "balance-sheet-statement-ttm",
            params={"symbol": symbol, "limit": 1},
        )

    async def get_cash_flow(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "cash-flow-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_cash_flow_ttm(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "cash-flow-statement-ttm",
            params={"symbol": symbol, "limit": 1},
        )

    # =====================================================================
    # Key Metrics & Ratios
    # =====================================================================

    async def get_key_metrics(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "key-metrics",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_key_metrics_ttm(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "key-metrics-ttm", params={"symbol": symbol}
        )

    async def get_financial_ratios(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "ratios",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_ratios_ttm(self, symbol: str) -> List[Dict]:
        return await self._make_request("ratios-ttm", params={"symbol": symbol})

    # =====================================================================
    # Growth Metrics
    # =====================================================================

    async def get_financial_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "financial-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_income_statement_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "income-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_balance_sheet_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "balance-sheet-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_cash_flow_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "cash-flow-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    # =====================================================================
    # Valuation
    # =====================================================================

    async def get_dcf(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "discounted-cash-flow", params={"symbol": symbol}
        )

    async def get_historical_dcf(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Historical DCF time series.

        The stable API no longer exposes this endpoint, so we return an
        empty list rather than 404. Callers (fundamentals_mcp_server) treat
        an empty list as "data unavailable".
        """
        return []

    async def get_custom_dcf(
        self,
        symbol: str,
        revenue_growth_pct: float,
        ebitda_pct: float,
        depreciation_and_amortization_pct: float,
        cash_and_short_term_investments_pct: float,
        receivables_pct: float,
        inventories_pct: float,
        payable_pct: float,
        ebit_pct: float,
        capital_expenditure_pct: float,
        operating_cash_flow_pct: float,
        selling_general_and_administrative_expenses_pct: float,
        tax_rate: float,
        long_term_growth_rate: float,
        cost_of_debt: float,
        cost_of_equity: float,
        market_risk_premium: float,
        beta: float,
        risk_free_rate: float,
    ) -> List[Dict]:
        """Run a custom DCF with user-defined assumptions."""
        params = {
            "symbol": symbol,
            "revenueGrowthPct": revenue_growth_pct,
            "ebitdaPct": ebitda_pct,
            "depreciationAndAmortizationPct": depreciation_and_amortization_pct,
            "cashAndShortTermInvestmentsPct": cash_and_short_term_investments_pct,
            "receivablesPct": receivables_pct,
            "inventoriesPct": inventories_pct,
            "payablePct": payable_pct,
            "ebitPct": ebit_pct,
            "capitalExpenditurePct": capital_expenditure_pct,
            "operatingCashFlowPct": operating_cash_flow_pct,
            "sellingGeneralAndAdministrativeExpensesPct": selling_general_and_administrative_expenses_pct,
            "taxRate": tax_rate,
            "longTermGrowthRate": long_term_growth_rate,
            "costOfDebt": cost_of_debt,
            "costOfEquity": cost_of_equity,
            "marketRiskPremium": market_risk_premium,
            "beta": beta,
            "riskFreeRate": risk_free_rate,
        }
        return await self._make_request(
            "custom-discounted-cash-flow", params=params, use_cache=False
        )

    async def get_enterprise_value(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "enterprise-values",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    # =====================================================================
    # Company Information
    # =====================================================================

    async def get_profile(self, symbol: str) -> List[Dict]:
        return await self._make_request("profile", params={"symbol": symbol})

    async def get_market_cap(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "market-capitalization", params={"symbol": symbol}
        )

    async def get_historical_market_cap(
        self, symbol: str, limit: int = 100
    ) -> List[Dict]:
        return await self._make_request(
            "historical-market-capitalization",
            params={"symbol": symbol, "limit": limit},
        )

    async def get_stock_peers(self, symbol: str) -> List[str]:
        """Return peer ticker symbols.

        Stable returns a flat array of peer company objects
        (``[{symbol, companyName, price, marketCap}, ...]``), unlike v4's
        ``[{symbol, peersList: [...]}]``. We extract the ``symbol`` of each
        peer to preserve the original ``List[str]`` contract.
        """
        response = await self._make_request("stock-peers", params={"symbol": symbol})
        if not isinstance(response, list):
            return []
        return [row["symbol"] for row in response if isinstance(row, dict) and "symbol" in row]

    # =====================================================================
    # Ownership & Capital Structure
    # =====================================================================

    async def get_insider_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self._make_request(
            "insider-trading/search", params={"symbol": symbol, "limit": limit}
        )

    async def get_insider_trade_stats(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "insider-trading/statistics", params={"symbol": symbol}
        )

    async def get_dividends(self, symbol: str) -> List[Dict]:
        return await self._make_request("dividends", params={"symbol": symbol})

    async def get_splits(self, symbol: str) -> List[Dict]:
        return await self._make_request("splits", params={"symbol": symbol})

    async def get_shares_float(self, symbol: str) -> List[Dict]:
        return await self._make_request("shares-float", params={"symbol": symbol})

    async def get_key_executives(self, symbol: str) -> List[Dict]:
        return await self._make_request("key-executives", params={"symbol": symbol})

    # =====================================================================
    # Analyst Data
    # =====================================================================

    async def get_analyst_estimates(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        return await self._make_request(
            "analyst-estimates",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_price_target(self, symbol: str) -> List[Dict]:
        """Analyst price target summary.

        Legacy v4 ``price-target`` returned per-analyst news-style entries;
        the stable API consolidated this into ``price-target-summary``, so
        this delegates to :meth:`get_price_target_summary` for the same shape.
        """
        return await self.get_price_target_summary(symbol)

    async def get_price_target_summary(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "price-target-summary", params={"symbol": symbol}
        )

    async def get_rating(self, symbol: str) -> List[Dict]:
        """Stock rating snapshot.

        Stable replaces v3's ``rating`` with ``ratings-snapshot``.
        """
        return await self._make_request(
            "ratings-snapshot", params={"symbol": symbol}
        )

    async def get_ratings_snapshot(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "ratings-snapshot", params={"symbol": symbol}
        )

    async def get_price_target_consensus(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "price-target-consensus", params={"symbol": symbol}
        )

    async def get_stock_grades(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self._make_request(
            "grades", params={"symbol": symbol, "limit": limit}
        )

    async def get_grades_summary(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "grades-consensus", params={"symbol": symbol}
        )

    async def get_earnings_report(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self._make_request(
            "earnings", params={"symbol": symbol, "limit": limit}
        )

    async def get_earnings_call_transcript(
        self, symbol: str, year: int, quarter: int
    ) -> List[Dict]:
        return await self._make_request(
            "earning-call-transcript",
            params={"symbol": symbol, "year": year, "quarter": quarter},
        )

    async def get_earnings_call_dates(self, symbol: str) -> List[List]:
        """All earnings-call transcript dates for ``symbol``.

        Stable returns ``[{quarter, fiscalYear, date}, ...]``; callers
        historically iterate as ``[quarter, fiscal_year, date]`` triples
        (see :func:`src.tools.sec.earnings_call.fetch_matching_earnings_call`),
        so we reshape to that form.
        """
        data = await self._make_request(
            "earning-call-transcript-dates", params={"symbol": symbol}
        )
        result: List[List] = []
        for row in (data or []):
            if not isinstance(row, dict):
                continue
            quarter = row.get("quarter")
            fiscal_year = row.get("fiscalYear") or row.get("year")
            call_date = row.get("date")
            if quarter is None or fiscal_year is None or call_date is None:
                continue
            result.append([quarter, fiscal_year, call_date])
        return result

    async def get_sec_filings(
        self,
        symbol: str,
        filing_type: Optional[str] = None,
        limit: int = 20,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """SEC filings for a company.

        Stable's ``sec-filings-search/symbol`` requires ``from`` and ``to``
        dates; when omitted we default to "last 5 years → today" so the
        contract still works for callers that only pass ``symbol``.
        Param ``type`` was renamed to ``formType``.
        """
        if not from_date:
            from_date = (_today_utc() - timedelta(days=365 * 5)).isoformat()
        if not to_date:
            to_date = _today_utc().isoformat()

        params: Dict[str, Any] = {
            "symbol": symbol,
            "limit": limit,
            "from": from_date,
            "to": to_date,
        }
        if filing_type:
            params["formType"] = filing_type

        return await self._make_request("sec-filings-search/symbol", params=params)

    async def get_historical_earnings_calendar(
        self, symbol: str, limit: int = 20
    ) -> List[Dict]:
        """Historical + upcoming earnings calendar.

        Stable consolidates v3's ``historical/earning_calendar`` into
        ``earnings``. Rows expose ``epsActual``/``revenueActual`` (reported)
        and ``epsEstimated``/``revenueEstimated``. ``fiscalDateEnding`` and
        ``time`` (amc/bmo) are not present on stable — callers fall back to
        ``date``.
        """
        data = await self._make_request(
            "earnings", params={"symbol": symbol, "limit": limit}
        )
        rows = list(data or [])
        return rows[:limit] if limit else rows

    # =====================================================================
    # Financial Scores
    # =====================================================================

    async def get_financial_score(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "financial-scores", params={"symbol": symbol}
        )

    # =====================================================================
    # Revenue Segmentation
    # =====================================================================

    async def get_revenue_product_segmentation(
        self, symbol: str, period: str = "annual", structure: str = "flat"
    ) -> List[Dict]:
        return await self._make_request(
            "revenue-product-segmentation",
            params={"symbol": symbol, "period": period, "structure": structure},
        )

    async def get_revenue_geographic_segmentation(
        self, symbol: str, period: str = "annual", structure: str = "flat"
    ) -> List[Dict]:
        return await self._make_request(
            "revenue-geographic-segmentation",
            params={"symbol": symbol, "period": period, "structure": structure},
        )

    # =====================================================================
    # Real-Time Quotes
    # =====================================================================

    async def get_quote(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "quote", params={"symbol": symbol}, use_cache=False
        )

    async def get_aftermarket_quote(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "aftermarket-quote", params={"symbol": symbol}, use_cache=False
        )

    async def get_stock_price_change(self, symbol: str) -> List[Dict]:
        return await self._make_request(
            "stock-price-change", params={"symbol": symbol}
        )

    # =====================================================================
    # Batch Operations
    # =====================================================================

    async def get_batch_profiles(self, symbols: List[str]) -> List[Dict]:
        """Profiles for multiple symbols.

        Stable does not support CSV-batch on ``/profile``; we fan out one
        request per symbol in parallel.
        """
        results = await asyncio.gather(
            *(self.get_profile(s) for s in symbols),
            return_exceptions=True,
        )
        flat: List[Dict] = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if isinstance(r, list):
                flat.extend(r)
        return flat

    async def get_batch_quotes(self, symbols: List[str]) -> List[Dict]:
        return await self._make_request(
            "batch-quote", params={"symbols": ",".join(symbols)}
        )

    async def get_batch_market_cap(self, symbols: List[str]) -> List[Dict]:
        return await self._make_request(
            "market-capitalization-batch", params={"symbols": ",".join(symbols)}
        )

    # =====================================================================
    # News & Press Releases
    # =====================================================================

    async def get_fmp_articles(self, limit: int = 10, page: int = 0) -> List[Dict]:
        result = await self._make_request(
            "fmp-articles", params={"limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_general_news(self, limit: int = 10, page: int = 0) -> List[Dict]:
        result = await self._make_request(
            "news/general-latest", params={"limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_stock_news(
        self, tickers: str, limit: int = 20, page: int = 0
    ) -> List[Dict]:
        """Stock-specific news. Stable renames ``stock_news`` to
        ``news/stock`` and ``tickers`` to ``symbols``.
        """
        result = await self._make_request(
            "news/stock",
            params={"symbols": tickers, "limit": limit, "page": page},
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_press_releases(
        self, symbol: str, limit: int = 10, page: int = 0
    ) -> List[Dict]:
        result = await self._make_request(
            "news/press-releases",
            params={"symbols": symbol, "limit": limit, "page": page},
        )
        return result[:limit] if isinstance(result, list) else result

    # =====================================================================
    # Hot lists
    # =====================================================================

    async def get_biggest_losers(self, limit: int = 50) -> List[Dict]:
        result = await self._make_request("biggest-losers")
        return result[:limit] if isinstance(result, list) else result

    async def get_most_actives(self, limit: int = 50) -> List[Dict]:
        result = await self._make_request("most-actives")
        return result[:limit] if isinstance(result, list) else result

    async def get_biggest_gainers(self, limit: int = 50) -> List[Dict]:
        result = await self._make_request("biggest-gainers")
        return result[:limit] if isinstance(result, list) else result

    # =====================================================================
    # Company Screener
    # =====================================================================

    async def get_company_screener(self, **filters) -> List[Dict]:
        params = {k: v for k, v in filters.items() if v is not None}
        return await self._make_request("company-screener", params=params)

    # =====================================================================
    # Sector Performance
    # =====================================================================

    async def get_sector_performance(
        self, target_date: Optional[str] = None
    ) -> List[Dict]:
        """Daily snapshot of US sector performance (NASDAQ-listed names).

        Stable's ``sector-performance-snapshot?date=`` requires a date and
        returns ``[]`` on weekends/holidays; when no date is supplied we walk
        back up to 7 calendar days to find the most recent trading day, which
        matches v3's implicit "latest available" behaviour. The numeric
        ``averageChange`` is rendered as a percentage string under
        ``changePctStr`` (see :func:`_format_sector_change_pct`).

        The endpoint is per-exchange and defaults to NASDAQ-listed constituents
        only — the same scope v3's ``sectors-performance`` returned.
        """
        if target_date:
            data = await self._make_request(
                "sector-performance-snapshot", params={"date": target_date}
            )
            return [_format_sector_change_pct(row) for row in (data or [])]

        today = _today_utc()
        for offset in range(7):
            probe = (today - timedelta(days=offset)).isoformat()
            data = await self._make_request(
                "sector-performance-snapshot", params={"date": probe}
            )
            if data:
                return [_format_sector_change_pct(row) for row in data]
        return []

    # =====================================================================
    # Technical Indicators
    # =====================================================================

    async def get_sma(
        self,
        symbol: str,
        period_length: int,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timeframe: str = "1day",
    ) -> List[Dict]:
        if from_date is None:
            from_date = (_today_utc() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()
        if to_date is None:
            to_date = _today_utc().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        return await self._make_request(
            "technical-indicators/sma",
            params={
                "symbol": symbol,
                "periodLength": period_length,
                "timeframe": timeframe,
                "from": from_date,
                "to": to_date,
            },
        )

    async def get_technical_indicator(
        self,
        symbol: str,
        indicator: str,
        period: int = 14,
        timeframe: str = "1day",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        if from_date is None:
            from_date = (_today_utc() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()
        if to_date is None:
            to_date = _today_utc().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        return await self._make_request(
            f"technical-indicators/{indicator}",
            params={
                "symbol": symbol,
                "periodLength": period,
                "timeframe": timeframe,
                "from": from_date,
                "to": to_date,
            },
        )

    # =====================================================================
    # Historical Price Data
    # =====================================================================

    async def get_stock_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        if from_date is None:
            from_date = (_today_utc() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()
        if to_date is None:
            to_date = _today_utc().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        return await self._make_request(
            "historical-price-eod/full",
            params={"symbol": symbol, "from": from_date, "to": to_date},
        )

    async def get_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        params: Dict[str, Any] = {"symbol": symbol}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(
            f"historical-chart/{interval}", params=params
        )

    async def get_commodity_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_stock_price(symbol, from_date, to_date)

    async def get_commodity_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_intraday_chart(symbol, interval, from_date, to_date)

    async def get_crypto_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_stock_price(symbol, from_date, to_date)

    async def get_crypto_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_intraday_chart(symbol, interval, from_date, to_date)

    async def get_forex_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_stock_price(symbol, from_date, to_date)

    async def get_forex_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        return await self.get_intraday_chart(symbol, interval, from_date, to_date)

    # =====================================================================
    # Stock Search
    # =====================================================================

    async def search_stocks(self, query: str, limit: int = 50) -> List[Dict]:
        """Search across symbols AND company names.

        v3 ``search`` matched both. Stable splits this into ``search-symbol``
        (symbol prefix match) and ``search-name`` (company name match);
        we call both in parallel and merge by symbol.
        """
        sym_task = self._make_request(
            "search-symbol", params={"query": query, "limit": limit}
        )
        name_task = self._make_request(
            "search-name", params={"query": query, "limit": limit}
        )
        sym_data, name_data = await asyncio.gather(
            sym_task, name_task, return_exceptions=True
        )

        merged: Dict[str, Dict] = {}
        for data in (sym_data, name_data):
            if isinstance(data, Exception) or not isinstance(data, list):
                continue
            for row in data:
                sym = row.get("symbol")
                if sym and sym not in merged:
                    merged[sym] = row
        return list(merged.values())[:limit]

    # =====================================================================
    # Macro & Economic Data
    # =====================================================================

    async def get_economic_indicators(self, name: str, limit: int = 50) -> List[Dict]:
        return await self._make_request(
            "economic-indicators", params={"name": name, "limit": limit}
        )

    async def get_economic_calendar(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("economic-calendar", params=params)

    async def get_treasury_rates(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("treasury-rates", params=params)

    async def get_market_risk_premium(self) -> List[Dict]:
        return await self._make_request("market-risk-premium")

    async def get_earnings_calendar_by_date(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("earnings-calendar", params=params)
