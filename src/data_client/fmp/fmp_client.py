"""
FMP (Financial Modeling Prep) API Client
Central client for all FMP API calls with caching, rate limiting, and error handling

Uses the new stable API (https://financialmodelingprep.com/stable/)
where symbols are passed as QUERY PARAMETERS, not path segments.
"""

import os
import json
from collections import OrderedDict
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import httpx

_CACHE_MAX_SIZE = 512


class FMPClient:
    """Central client for Financial Modeling Prep API (Async)"""

    BASE_URL = "https://financialmodelingprep.com/api"
    DEFAULT_VERSION = "stable"

    def __init__(self, api_key: Optional[str] = None, cache_ttl: int = 300):
        """
        Initialize FMP API client

        Args:
            api_key: FMP API key (will use env var FMP_API_KEY if not provided)
            cache_ttl: Cache time-to-live in seconds (default 5 minutes)
        """
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
        """Lazy initialization of async client with HTTP/2"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                http2=True,
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=10),
            )
        return self._client

    async def close(self):
        """Close the HTTP client"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    def _build_url(self, endpoint: str, version: str = None) -> str:
        """Build full API URL"""
        version = version or self.DEFAULT_VERSION
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        # Handle stable version differently (it's not under /api/)
        if version == "stable":
            return f"https://financialmodelingprep.com/stable{endpoint}"
        else:
            return f"{self.BASE_URL}/{version}{endpoint}"

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self._cache_timestamps:
            return False

        cached_time = self._cache_timestamps[cache_key]
        return (datetime.now() - cached_time).total_seconds() < self.cache_ttl

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        version: str = None,
        use_cache: bool = True,
    ) -> Union[Dict, List]:
        """
        Make API request with caching and error handling

        Args:
            endpoint: API endpoint path
            params: Query parameters
            version: API version (default stable)
            use_cache: Whether to use caching

        Returns:
            API response data
        """
        params = params or {}
        params["apikey"] = self.api_key

        # Create cache key
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        # Check cache (move to end on hit for LRU ordering)
        if use_cache and self._is_cache_valid(cache_key):
            self._cache.move_to_end(cache_key)
            return self._cache[cache_key]

        # Build URL and make request
        url = self._build_url(endpoint, version)
        client = await self._get_client()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Cache successful response (bounded LRU — evict oldest when full)
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

    # ── Financial Statements ──────────────────────────────────────────

    async def get_income_statement(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get income statement data → /stable/income-statement?symbol=AAPL"""
        return await self._make_request(
            "income-statement", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_income_statement_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM income statement → /stable/income-statement-ttm?symbol=AAPL"""
        return await self._make_request(
            "income-statement-ttm", params={"symbol": symbol, "limit": 1}
        )

    async def get_balance_sheet(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get balance sheet data → /stable/balance-sheet-statement?symbol=AAPL"""
        return await self._make_request(
            "balance-sheet-statement",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_balance_sheet_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM balance sheet → /stable/balance-sheet-statement-ttm?symbol=AAPL"""
        return await self._make_request(
            "balance-sheet-statement-ttm", params={"symbol": symbol, "limit": 1}
        )

    async def get_cash_flow(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get cash flow statement → /stable/cash-flow-statement?symbol=AAPL"""
        return await self._make_request(
            "cash-flow-statement", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_cash_flow_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM cash flow → /stable/cash-flow-statement-ttm?symbol=AAPL"""
        return await self._make_request(
            "cash-flow-statement-ttm", params={"symbol": symbol, "limit": 1}
        )

    # ── Key Metrics & Ratios ───────────────────────────────────────────

    async def get_key_metrics(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get key financial metrics → /stable/key-metrics?symbol=AAPL"""
        return await self._make_request(
            "key-metrics", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_key_metrics_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM key metrics → /stable/key-metrics-ttm?symbol=AAPL"""
        return await self._make_request(
            "key-metrics-ttm", params={"symbol": symbol, "limit": 1}
        )

    async def get_financial_ratios(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get financial ratios → /stable/ratios?symbol=AAPL"""
        return await self._make_request(
            "ratios", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_ratios_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM financial ratios → /stable/ratios-ttm?symbol=AAPL"""
        return await self._make_request(
            "ratios-ttm", params={"symbol": symbol, "limit": 1}
        )

    # ── Growth Metrics ─────────────────────────────────────────────────

    async def get_financial_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get financial statement growth → /stable/financial-growth?symbol=AAPL"""
        return await self._make_request(
            "financial-growth", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_income_statement_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get income statement growth → /stable/income-statement-growth?symbol=AAPL"""
        return await self._make_request(
            "income-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_balance_sheet_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get balance sheet growth → /stable/balance-sheet-statement-growth?symbol=AAPL"""
        return await self._make_request(
            "balance-sheet-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    async def get_cash_flow_growth(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get cash flow growth → /stable/cash-flow-statement-growth?symbol=AAPL"""
        return await self._make_request(
            "cash-flow-statement-growth",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

    # ── Valuation ──────────────────────────────────────────────────────

    async def get_dcf(self, symbol: str) -> List[Dict]:
        """Get DCF valuation → /stable/discounted-cash-flow?symbol=AAPL"""
        return await self._make_request(
            "discounted-cash-flow", params={"symbol": symbol}
        )

    async def get_historical_dcf(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get historical DCF valuations (may not be available in stable API)"""
        return await self._make_request(
            "historical-discounted-cash-flow",
            params={"symbol": symbol, "period": period, "limit": limit},
        )

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
        """
        Run custom DCF with user-defined assumptions

        Endpoint: /stable/custom-discounted-cash-flow

        Args:
            symbol: Stock ticker symbol
            revenue_growth_pct: Revenue growth rate (e.g., 0.10 for 10%)
            ebitda_pct: EBITDA margin (e.g., 0.31 for 31%)
            depreciation_and_amortization_pct: D&A as % of revenue
            cash_and_short_term_investments_pct: Cash & ST investments as % of revenue
            receivables_pct: Receivables as % of revenue
            inventories_pct: Inventory as % of revenue
            payable_pct: Payables as % of revenue
            ebit_pct: EBIT margin
            capital_expenditure_pct: Capex as % of revenue
            operating_cash_flow_pct: OCF as % of revenue
            selling_general_and_administrative_expenses_pct: SG&A as % of revenue
            tax_rate: Effective tax rate (e.g., 0.15 for 15%)
            long_term_growth_rate: Terminal growth rate (e.g., 4 for 4%)
            cost_of_debt: Cost of debt (e.g., 3.64 for 3.64%)
            cost_of_equity: Cost of equity (e.g., 9.52 for 9.52%)
            market_risk_premium: Market risk premium (e.g., 4.72 for 4.72%)
            beta: Stock beta (e.g., 1.244)
            risk_free_rate: Risk-free rate (e.g., 3.64 for 3.64%)

        Returns:
            List with custom DCF result including fair value
        """
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
            "custom-discounted-cash-flow",
            params=params,
            use_cache=False,  # Don't cache custom DCF results
        )

    async def get_enterprise_value(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get enterprise value → /stable/enterprise-values?symbol=AAPL"""
        return await self._make_request(
            "enterprise-values", params={"symbol": symbol, "period": period, "limit": limit}
        )

    # ── Company Information ────────────────────────────────────────────

    async def get_profile(self, symbol: str) -> List[Dict]:
        """Get company profile → /stable/profile?symbol=AAPL"""
        return await self._make_request("profile", params={"symbol": symbol})

    async def get_market_cap(self, symbol: str) -> List[Dict]:
        """Get current market capitalization → /stable/market-capitalization?symbol=AAPL"""
        return await self._make_request("market-capitalization", params={"symbol": symbol})

    async def get_historical_market_cap(
        self, symbol: str, limit: int = 100
    ) -> List[Dict]:
        """Get historical market cap → /stable/historical-market-capitalization?symbol=AAPL"""
        return await self._make_request(
            "historical-market-capitalization", params={"symbol": symbol, "limit": limit}
        )

    async def get_stock_peers(self, symbol: str) -> List[str]:
        """Get peer companies list → /stable/stock-peers?symbol=AAPL"""
        response = await self._make_request(
            "stock-peers", params={"symbol": symbol}
        )
        # Extract the actual peer list from the API response
        if response and len(response) > 0 and isinstance(response[0], dict):
            if "peersList" in response[0]:
                return response[0]["peersList"]
        return []

    # ── Ownership & Capital Structure ──────────────────────────────────

    async def get_insider_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get insider trading transactions (SEC Form 4 filings)
        → /stable/insider-trading/search?symbol=AAPL"""
        return await self._make_request(
            "insider-trading/search", params={"symbol": symbol, "limit": limit}
        )

    async def get_insider_trade_stats(self, symbol: str) -> List[Dict]:
        """Get aggregate insider trading statistics (buy/sell totals)
        → /stable/insider-trading/statistics?symbol=AAPL"""
        return await self._make_request(
            "insider-trading/statistics", params={"symbol": symbol}
        )

    async def get_dividends(self, symbol: str) -> List[Dict]:
        """Get historical dividend payments → /stable/dividends?symbol=AAPL"""
        return await self._make_request("dividends", params={"symbol": symbol})

    async def get_splits(self, symbol: str) -> List[Dict]:
        """Get historical stock splits → /stable/splits?symbol=AAPL"""
        return await self._make_request("splits", params={"symbol": symbol})

    async def get_shares_float(self, symbol: str) -> List[Dict]:
        """Get shares float → /stable/shares-float?symbol=AAPL"""
        return await self._make_request("shares-float", params={"symbol": symbol})

    async def get_key_executives(self, symbol: str) -> List[Dict]:
        """Get key executives → /stable/key-executives?symbol=AAPL"""
        return await self._make_request("key-executives", params={"symbol": symbol})

    # ── Analyst Data ───────────────────────────────────────────────────

    async def get_analyst_estimates(
        self, symbol: str, period: str = "annual", limit: int = 5
    ) -> List[Dict]:
        """Get analyst estimates → /stable/analyst-estimates?symbol=AAPL"""
        return await self._make_request(
            "analyst-estimates", params={"symbol": symbol, "period": period, "limit": limit}
        )

    async def get_price_target(self, symbol: str) -> List[Dict]:
        """Get analyst price targets (alias for price-target-summary)
        → /stable/price-target-summary?symbol=AAPL"""
        return await self._make_request(
            "price-target-summary", params={"symbol": symbol}
        )

    async def get_price_target_summary(self, symbol: str) -> List[Dict]:
        """Get price target summary → /stable/price-target-summary?symbol=AAPL"""
        return await self._make_request(
            "price-target-summary", params={"symbol": symbol}
        )

    async def get_rating(self, symbol: str) -> List[Dict]:
        """Get stock rating (alias for ratings-snapshot)
        → /stable/ratings-snapshot?symbol=AAPL"""
        return await self._make_request(
            "ratings-snapshot", params={"symbol": symbol}
        )

    async def get_ratings_snapshot(self, symbol: str) -> List[Dict]:
        """
        Get comprehensive financial ratings snapshot → /stable/ratings-snapshot?symbol=AAPL

        Provides ratings based on key financial ratios including:
        - Overall score
        - Discounted cash flow score
        - Return on equity score
        - Return on assets score
        - Debt to equity score
        - Price to earnings score
        - Price to book score
        """
        return await self._make_request(
            "ratings-snapshot", params={"symbol": symbol}
        )

    async def get_price_target_consensus(self, symbol: str) -> List[Dict]:
        """
        Get analyst price target consensus → /stable/price-target-consensus?symbol=AAPL

        Provides high, low, median, and consensus price targets from analysts.
        """
        return await self._make_request(
            "price-target-consensus", params={"symbol": symbol}
        )

    async def get_stock_grades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Get latest stock grades from analysts → /stable/grades?symbol=AAPL

        Track analyst grading actions (upgrades, downgrades, maintained ratings)
        from various financial institutions over time.
        """
        return await self._make_request(
            "grades", params={"symbol": symbol, "limit": limit}
        )

    async def get_grades_summary(self, symbol: str) -> List[Dict]:
        """
        Get consolidated analyst ratings summary → /stable/grades-consensus?symbol=AAPL

        Provides a summary of analyst sentiment with counts for:
        - Strong buy, Buy, Hold, Sell, Strong sell
        - Overall consensus rating
        """
        return await self._make_request(
            "grades-consensus", params={"symbol": symbol}
        )

    async def get_earnings_report(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Get earnings report information → /stable/earnings?symbol=AAPL

        Retrieves earnings data including:
        - Earnings report dates
        - EPS estimates and actuals
        - Revenue estimates and actuals
        - Earnings surprises
        """
        return await self._make_request(
            "earnings", params={"symbol": symbol, "limit": limit}
        )

    async def get_earnings_call_transcript(
        self, symbol: str, year: int, quarter: int
    ) -> List[Dict]:
        """
        Get earnings call transcript → /stable/earning-call-transcript?symbol=AAPL&year=2020&quarter=3

        Retrieves the full transcript of a company's earnings call, including
        management's prepared remarks and Q&A session.
        """
        return await self._make_request(
            "earning-call-transcript",
            params={"symbol": symbol, "year": year, "quarter": quarter},
        )

    async def get_earnings_call_dates(self, symbol: str) -> List[List]:
        """
        Get all available earnings call dates for a symbol.
        → /stable/earning-call-transcript-dates?symbol=AAPL

        Returns a list of all earnings call transcripts with their dates,
        allowing date-based matching rather than fiscal year/quarter guessing.
        """
        return await self._make_request(
            "earning-call-transcript-dates", params={"symbol": symbol}
        )

    async def get_sec_filings(
        self, symbol: str, filing_type: Optional[str] = None, limit: int = 20
    ) -> List[Dict]:
        """
        Get SEC filings for a company → /stable/sec-filings-search/symbol?symbol=AAPL

        Retrieves SEC filings including 10-K (annual), 10-Q (quarterly),
        8-K (current reports), and other filing types.
        """
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if filing_type:
            params["formType"] = filing_type

        return await self._make_request("sec-filings-search/symbol", params=params)

    async def get_historical_earnings_calendar(
        self, symbol: str, limit: int = 20
    ) -> List[Dict]:
        """
        Get historical and upcoming earnings calendar for a symbol.
        → /stable/earnings?symbol=AAPL

        Provides earnings announcement dates with fiscal period end dates,
        enabling accurate fiscal period identification.
        """
        result = await self._make_request(
            "earnings", params={"symbol": symbol}
        )
        if result and limit:
            return result[:limit]
        return result

    # ── Financial Scores ───────────────────────────────────────────────

    async def get_financial_score(self, symbol: str) -> List[Dict]:
        """Get financial health scores (Altman Z, Piotroski)
        → /stable/financial-scores?symbol=AAPL"""
        return await self._make_request("financial-scores", params={"symbol": symbol})

    # ── Revenue Segmentation ───────────────────────────────────────────

    async def get_revenue_product_segmentation(
        self, symbol: str, period: str = "annual", structure: str = "flat"
    ) -> List[Dict]:
        """Get revenue breakdown by product
        → /stable/revenue-product-segmentation?symbol=AAPL"""
        return await self._make_request(
            "revenue-product-segmentation",
            params={"symbol": symbol, "period": period, "structure": structure},
        )

    async def get_revenue_geographic_segmentation(
        self, symbol: str, period: str = "annual", structure: str = "flat"
    ) -> List[Dict]:
        """Get revenue breakdown by geography
        → /stable/revenue-geographic-segmentation?symbol=AAPL"""
        return await self._make_request(
            "revenue-geographic-segmentation",
            params={"symbol": symbol, "period": period, "structure": structure},
        )

    # ── Real-Time Quotes ───────────────────────────────────────────────

    async def get_quote(self, symbol: str) -> List[Dict]:
        """
        Get real-time stock quote → /stable/quote?symbol=AAPL

        Provides current market data including price, volume, bid/ask, and daily changes.
        Updated in real-time during market hours.
        """
        return await self._make_request("quote", params={"symbol": symbol}, use_cache=False)

    async def get_aftermarket_quote(self, symbol: str) -> List[Dict]:
        """
        Get after-market quote (post-market hours)
        → /stable/aftermarket-quote?symbol=AAPL

        Provides post-market trading data including price, volume, and bid/ask
        during after-hours trading sessions (typically 4:00 PM - 8:00 PM ET).
        """
        return await self._make_request(
            "aftermarket-quote", params={"symbol": symbol}, use_cache=False
        )

    async def get_stock_price_change(self, symbol: str) -> List[Dict]:
        """
        Get stock price changes over multiple time periods
        → /stable/stock-price-change?symbol=AAPL

        Tracks stock price fluctuations across various time periods
        including 1D, 5D, 1M, 3M, 6M, YTD, 1Y, 3Y, 5Y, 10Y, max.
        """
        return await self._make_request(
            "stock-price-change", params={"symbol": symbol}
        )

    # ── Batch Operations ───────────────────────────────────────────────

    async def get_batch_profiles(self, symbols: List[str]) -> List[Dict]:
        """Get profiles for multiple companies → /stable/profile?symbol=AAPL,MSFT"""
        symbol_str = ",".join(symbols)
        return await self._make_request("profile", params={"symbol": symbol_str})

    async def get_batch_quotes(self, symbols: List[str]) -> List[Dict]:
        """Get quotes for multiple companies → /stable/batch-quote?symbols=AAPL,MSFT"""
        symbol_str = ",".join(symbols)
        return await self._make_request("batch-quote", params={"symbols": symbol_str})

    async def get_batch_market_cap(self, symbols: List[str]) -> Dict:
        """Get market cap for multiple companies
        → /stable/market-capitalization-batch?symbols=AAPL,MSFT"""
        return await self._make_request(
            "market-capitalization-batch", params={"symbols": ",".join(symbols)}
        )

    # ── News & Press Releases ──────────────────────────────────────────

    async def get_fmp_articles(self, limit: int = 10, page: int = 0) -> List[Dict]:
        """
        Get latest FMP articles → /stable/fmp-articles?page=0&limit=10
        """
        result = await self._make_request(
            "fmp-articles", params={"limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_general_news(self, limit: int = 10, page: int = 0) -> List[Dict]:
        """
        Get latest general news articles → /stable/news/general-latest?page=0&limit=10
        """
        result = await self._make_request(
            "news/general-latest", params={"limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_stock_news(
        self, tickers: str, limit: int = 20, page: int = 0
    ) -> List[Dict]:
        """
        Get stock-specific news articles → /stable/news/stock?symbols=AAPL,MSFT

        Args:
            tickers: Comma-separated ticker symbols (e.g. "AAPL,MSFT")
            limit: Number of articles to return (default 20)
            page: Page number for pagination (default 0)
        """
        result = await self._make_request(
            "news/stock", params={"symbols": tickers, "limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    async def get_press_releases(
        self, symbol: str, limit: int = 10, page: int = 0
    ) -> List[Dict]:
        """
        Get company press releases → /stable/news/press-releases?symbols=AAPL

        Args:
            symbol: Stock ticker symbol
            limit: Number of press releases to return (default 10)
            page: Page number for pagination (default 0)
        """
        result = await self._make_request(
            "news/press-releases", params={"symbols": symbol, "limit": limit, "page": page}
        )
        return result[:limit] if isinstance(result, list) else result

    # ── Hot Lists ──────────────────────────────────────────────────────

    async def get_biggest_losers(self, limit: int = 50) -> List[Dict]:
        """Get biggest losers list → /stable/biggest-losers"""
        result = await self._make_request("biggest-losers")
        return result[:limit] if isinstance(result, list) else result

    async def get_most_actives(self, limit: int = 50) -> List[Dict]:
        """Get most actives list → /stable/most-actives"""
        result = await self._make_request("most-actives")
        return result[:limit] if isinstance(result, list) else result

    async def get_biggest_gainers(self, limit: int = 50) -> List[Dict]:
        """Get biggest gainers list → /stable/biggest-gainers"""
        result = await self._make_request("biggest-gainers")
        return result[:limit] if isinstance(result, list) else result

    # ── Company Screener ───────────────────────────────────────────────

    async def get_company_screener(self, **filters) -> List[Dict]:
        """Screen stocks using FMP company screener → /stable/company-screener"""
        params = {k: v for k, v in filters.items() if v is not None}
        return await self._make_request("company-screener", params=params)

    # ── Technical Indicators ───────────────────────────────────────────

    async def get_sma(
        self,
        symbol: str,
        period_length: int,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timeframe: str = "1day",
    ) -> List[Dict]:
        """
        Get Simple Moving Average (SMA) indicator data
        → /stable/technical-indicators/sma?symbol=AAPL&periodLength=10&timeframe=1day
        """
        from datetime import date, timedelta

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {
            "symbol": symbol,
            "periodLength": period_length,
            "timeframe": timeframe,
            "from": from_date,
            "to": to_date,
        }

        return await self._make_request("technical-indicators/sma", params=params)

    async def get_technical_indicator(
        self,
        symbol: str,
        indicator: str,
        period: int = 14,
        timeframe: str = "1day",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get technical indicator data (RSI, EMA, MACD, ADX, WMA, DEMA, TEMA, Williams %R, StdDev)
        → /stable/technical-indicators/{indicator}?symbol=AAPL&periodLength=14&timeframe=1day
        """
        from datetime import date

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {
            "symbol": symbol,
            "periodLength": period,
            "timeframe": timeframe,
            "from": from_date,
            "to": to_date,
        }

        return await self._make_request(
            f"technical-indicators/{indicator}", params=params
        )

    # ── Historical Price Data ──────────────────────────────────────────

    async def get_stock_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get historical stock price data (OHLCV)
        → /stable/historical-price-eod/full?symbol=AAPL
        """
        from datetime import date, timedelta

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full", params=params)

    async def get_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get intraday stock chart data → /stable/historical-chart/{interval}?symbol=AAPL

        Retrieves historical intraday OHLCV data at various time intervals.
        """
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}", params=params)

    # ── Commodity Data ─────────────────────────────────────────────────

    async def get_commodity_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get historical commodity price data (OHLCV)
        → /stable/historical-price-eod/full?symbol=GCUSD
        """
        from datetime import date, timedelta

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full", params=params)

    async def get_commodity_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """Get intraday commodity chart data → /stable/historical-chart/{interval}?symbol=GCUSD"""
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}", params=params)

    # ── Crypto Data ────────────────────────────────────────────────────

    async def get_crypto_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get historical cryptocurrency price data (OHLCV)
        → /stable/historical-price-eod/full?symbol=BTCUSD
        """
        from datetime import date, timedelta

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full", params=params)

    async def get_crypto_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """Get intraday cryptocurrency chart data → /stable/historical-chart/{interval}?symbol=BTCUSD"""
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}", params=params)

    # ── Forex Data ─────────────────────────────────────────────────────

    async def get_forex_price(
        self,
        symbol: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get historical forex price data (OHLCV)
        → /stable/historical-price-eod/full?symbol=EURUSD
        """
        from datetime import date, timedelta

        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full", params=params)

    async def get_forex_intraday_chart(
        self,
        symbol: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[Dict]:
        """Get intraday forex chart data → /stable/historical-chart/{interval}?symbol=EURUSD"""
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}", params=params)

    # ── Stock Search ───────────────────────────────────────────────────

    async def search_stocks(self, query: str, limit: int = 50) -> List[Dict]:
        """
        Search for stocks by symbol or company name.
        → /stable/search-symbol?query=AAPL (preferred for symbol search)
        """
        return await self._make_request(
            "search-symbol",
            params={"query": query, "limit": limit},
            use_cache=True,
        )

    # ── Macro & Economic Data ──────────────────────────────────────────

    async def get_economic_indicators(self, name: str, limit: int = 50) -> List[Dict]:
        """
        Get economic indicator time series → /stable/economic-indicators?name=GDP
        """
        return await self._make_request(
            "economic-indicators", params={"name": name, "limit": limit}
        )

    async def get_economic_calendar(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        """Get upcoming economic events → /stable/economic-calendar"""
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("economic-calendar", params=params)

    async def get_treasury_rates(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        """Get treasury rates → /stable/treasury-rates"""
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("treasury-rates", params=params)

    async def get_market_risk_premium(self) -> List[Dict]:
        """Get market risk premium → /stable/market-risk-premium"""
        return await self._make_request("market-risk-premium")

    async def get_earnings_calendar_by_date(
        self, from_date: str, to_date: str
    ) -> List[Dict]:
        """
        Get earnings calendar for all companies in a date range
        → /stable/earnings-calendar?from=...&to=...
        """
        return await self._make_request(
            "earnings-calendar", params={"from": from_date, "to": to_date}
        )

    # ── Utility Methods ────────────────────────────────────────────────

    def clear_cache(self):
        """Clear all cached data"""
        self._cache.clear()
        self._cache_timestamps.clear()

    def clear_cache_for_symbol(self, symbol: str):
        """Clear cache for specific symbol"""
        keys_to_remove = [k for k in self._cache.keys() if symbol in k]
        for key in keys_to_remove:
            del self._cache[key]
            del self._cache_timestamps[key]