"""
FMP (Financial Modeling Prep) API Client
Central client for all FMP API calls with caching, rate limiting, and error handling
"""

import os
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import httpx


class FMPClient:
    """Central client for Financial Modeling Prep API (Async)"""

    BASE_URL = "https://financialmodelingprep.com/api"
    DEFAULT_VERSION = "v3"

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
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazy initialization of async client with HTTP/2"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                http2=True,
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=10)
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

    async def _make_request(self,
                            endpoint: str,
                            params: Optional[Dict[str, Any]] = None,
                            version: str = None,
                            use_cache: bool = True) -> Union[Dict, List]:
        """
        Make API request with caching and error handling

        Args:
            endpoint: API endpoint path
            params: Query parameters
            version: API version (default v3)
            use_cache: Whether to use caching

        Returns:
            API response data
        """
        params = params or {}
        params["apikey"] = self.api_key

        # Create cache key
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        # Check cache
        if use_cache and self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        # Build URL and make request
        url = self._build_url(endpoint, version)
        client = await self._get_client()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Cache successful response
            if use_cache and data:
                self._cache[cache_key] = data
                self._cache_timestamps[cache_key] = datetime.now()

            return data

        except httpx.HTTPStatusError as e:
            raise Exception(f"FMP API request failed: {str(e)}")
        except httpx.TimeoutException as e:
            raise Exception(f"FMP API request timed out: {str(e)}")
        except httpx.RequestError as e:
            raise Exception(f"FMP API request failed: {str(e)}")

    # Financial Statements
    async def get_income_statement(self,
                                   symbol: str,
                                   period: str = "annual",
                                   limit: int = 5) -> List[Dict]:
        """Get income statement data"""
        return await self._make_request("income-statement/" + symbol,
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_income_statement_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM income statement"""
        return await self._make_request("income-statement-ttm",
                                        params={
                                            "symbol": symbol,
                                            "limit": 1
                                        },
                                        version="stable")

    async def get_balance_sheet(self,
                                symbol: str,
                                period: str = "annual",
                                limit: int = 5) -> List[Dict]:
        """Get balance sheet data"""
        return await self._make_request(f"balance-sheet-statement/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_balance_sheet_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM balance sheet"""
        return await self._make_request("balance-sheet-statement-ttm",
                                        params={
                                            "symbol": symbol,
                                            "limit": 1
                                        },
                                        version="stable")

    async def get_cash_flow(self,
                            symbol: str,
                            period: str = "annual",
                            limit: int = 5) -> List[Dict]:
        """Get cash flow statement"""
        return await self._make_request(f"cash-flow-statement/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_cash_flow_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM cash flow"""
        return await self._make_request("cash-flow-statement-ttm",
                                        params={
                                            "symbol": symbol,
                                            "limit": 1
                                        },
                                        version="stable")

    # Key Metrics & Ratios
    async def get_key_metrics(self,
                              symbol: str,
                              period: str = "annual",
                              limit: int = 5) -> List[Dict]:
        """Get key financial metrics"""
        return await self._make_request(f"key-metrics/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_key_metrics_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM key metrics"""
        return await self._make_request(f"key-metrics-ttm/{symbol}")

    async def get_financial_ratios(self,
                                   symbol: str,
                                   period: str = "annual",
                                   limit: int = 5) -> List[Dict]:
        """Get financial ratios"""
        # Use stable version which has operatingCashFlowRatio
        return await self._make_request("ratios",
                                        params={
                                            "symbol": symbol,
                                            "period": period,
                                            "limit": limit
                                        },
                                        version="stable")

    async def get_ratios_ttm(self, symbol: str) -> List[Dict]:
        """Get TTM financial ratios"""
        # Use stable version for consistency
        return await self._make_request("ratios-ttm",
                                        params={"symbol": symbol},
                                        version="stable")

    # Growth Metrics
    async def get_financial_growth(self,
                                   symbol: str,
                                   period: str = "annual",
                                   limit: int = 5) -> List[Dict]:
        """Get financial statement growth"""
        return await self._make_request(f"financial-growth/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_income_statement_growth(self,
                                          symbol: str,
                                          period: str = "annual",
                                          limit: int = 5) -> List[Dict]:
        """Get income statement growth rates"""
        return await self._make_request(f"income-statement-growth/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_balance_sheet_growth(self,
                                       symbol: str,
                                       period: str = "annual",
                                       limit: int = 5) -> List[Dict]:
        """Get balance sheet growth rates"""
        return await self._make_request(f"balance-sheet-growth/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_cash_flow_growth(self,
                                   symbol: str,
                                   period: str = "annual",
                                   limit: int = 5) -> List[Dict]:
        """Get cash flow growth rates"""
        return await self._make_request(f"cash-flow-growth/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    # Valuation
    async def get_dcf(self, symbol: str) -> List[Dict]:
        """Get DCF valuation"""
        return await self._make_request(f"discounted-cash-flow/{symbol}")

    async def get_historical_dcf(self,
                                 symbol: str,
                                 period: str = "annual",
                                 limit: int = 5) -> List[Dict]:
        """Get historical DCF valuations"""
        return await self._make_request(f"historical-discounted-cash-flow/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_custom_dcf(self, symbol: str, revenue_growth_pct: float,
                             ebitda_pct: float,
                             depreciation_and_amortization_pct: float,
                             cash_and_short_term_investments_pct: float,
                             receivables_pct: float, inventories_pct: float,
                             payable_pct: float, ebit_pct: float,
                             capital_expenditure_pct: float,
                             operating_cash_flow_pct: float,
                             selling_general_and_administrative_expenses_pct: float,
                             tax_rate: float, long_term_growth_rate: float,
                             cost_of_debt: float, cost_of_equity: float,
                             market_risk_premium: float, beta: float,
                             risk_free_rate: float) -> List[Dict]:
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
            "depreciationAndAmortizationPct":
            depreciation_and_amortization_pct,
            "cashAndShortTermInvestmentsPct":
            cash_and_short_term_investments_pct,
            "receivablesPct": receivables_pct,
            "inventoriesPct": inventories_pct,
            "payablePct": payable_pct,
            "ebitPct": ebit_pct,
            "capitalExpenditurePct": capital_expenditure_pct,
            "operatingCashFlowPct": operating_cash_flow_pct,
            "sellingGeneralAndAdministrativeExpensesPct":
            selling_general_and_administrative_expenses_pct,
            "taxRate": tax_rate,
            "longTermGrowthRate": long_term_growth_rate,
            "costOfDebt": cost_of_debt,
            "costOfEquity": cost_of_equity,
            "marketRiskPremium": market_risk_premium,
            "beta": beta,
            "riskFreeRate": risk_free_rate
        }

        return await self._make_request(
            "custom-discounted-cash-flow",
            params=params,
            version="stable",
            use_cache=False  # Don't cache custom DCF results
        )

    async def get_enterprise_value(self,
                                   symbol: str,
                                   period: str = "annual",
                                   limit: int = 5) -> List[Dict]:
        """Get enterprise value"""
        return await self._make_request(f"enterprise-values/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    # Company Information
    async def get_profile(self, symbol: str) -> List[Dict]:
        """Get company profile"""
        return await self._make_request(f"profile/{symbol}")

    async def get_market_cap(self, symbol: str) -> List[Dict]:
        """Get current market capitalization"""
        return await self._make_request(f"market-capitalization/{symbol}")

    async def get_historical_market_cap(self,
                                        symbol: str,
                                        limit: int = 100) -> List[Dict]:
        """Get historical market cap"""
        return await self._make_request(f"historical-market-capitalization/{symbol}",
                                        params={"limit": limit})

    async def get_stock_peers(self, symbol: str) -> List[str]:
        """Get peer companies list"""
        response = await self._make_request(f"stock_peers",
                                            params={"symbol": symbol},
                                            version="v4")
        # Extract the actual peer list from the API response
        if response and len(response) > 0 and isinstance(response[0], dict):
            if 'peersList' in response[0]:
                return response[0]['peersList']
        return []

    # Analyst Data
    async def get_analyst_estimates(self,
                                    symbol: str,
                                    period: str = "annual",
                                    limit: int = 5) -> List[Dict]:
        """Get analyst estimates"""
        return await self._make_request(f"analyst-estimates/{symbol}",
                                        params={
                                            "period": period,
                                            "limit": limit
                                        })

    async def get_price_target(self, symbol: str) -> List[Dict]:
        """Get analyst price targets"""
        return await self._make_request(f"price-target/{symbol}", version="v4")

    async def get_price_target_summary(self, symbol: str) -> List[Dict]:
        """Get price target consensus"""
        return await self._make_request(f"price-target-summary/{symbol}",
                                        version="v4")

    async def get_rating(self, symbol: str) -> List[Dict]:
        """Get stock rating"""
        return await self._make_request(f"rating/{symbol}")

    async def get_ratings_snapshot(self, symbol: str) -> List[Dict]:
        """
        Get comprehensive financial ratings snapshot

        Provides ratings based on key financial ratios including:
        - Overall score
        - Discounted cash flow score
        - Return on equity score
        - Return on assets score
        - Debt to equity score
        - Price to earnings score
        - Price to book score

        Returns:
            List with rating snapshot data
        """
        return await self._make_request("ratings-snapshot",
                                        params={"symbol": symbol},
                                        version="stable")

    async def get_price_target_consensus(self, symbol: str) -> List[Dict]:
        """
        Get analyst price target consensus

        Provides high, low, median, and consensus price targets from analysts.

        Returns:
            List with consensus price target data
        """
        return await self._make_request("price-target-consensus",
                                        params={"symbol": symbol},
                                        version="stable")

    async def get_stock_grades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Get latest stock grades from analysts

        Track analyst grading actions (upgrades, downgrades, maintained ratings)
        from various financial institutions over time.

        Args:
            symbol: Stock ticker symbol
            limit: Number of grade records to return (default 100)

        Returns:
            List of grade records with date, grading company, previous/new grade, action
        """
        return await self._make_request("grades",
                                        params={
                                            "symbol": symbol,
                                            "limit": limit
                                        },
                                        version="stable")

    async def get_grades_summary(self, symbol: str) -> List[Dict]:
        """
        Get consolidated analyst ratings summary

        Provides a summary of analyst sentiment with counts for:
        - Strong buy
        - Buy
        - Hold
        - Sell
        - Strong sell
        - Overall consensus rating

        Returns:
            List with grades summary data
        """
        return await self._make_request("grades-consensus",
                                        params={"symbol": symbol},
                                        version="stable")

    async def get_earnings_report(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Get earnings report information

        Retrieves earnings data including:
        - Earnings report dates
        - EPS estimates and actuals
        - Revenue estimates and actuals
        - Earnings surprises

        Args:
            symbol: Stock ticker symbol
            limit: Maximum number of earnings records to return (default 100)

        Returns:
            List of earnings report data
        """
        return await self._make_request("earnings",
                                        params={
                                            "symbol": symbol,
                                            "limit": limit
                                        },
                                        version="stable")

    async def get_earnings_call_transcript(self, symbol: str, year: int,
                                           quarter: int) -> List[Dict]:
        """
        Get earnings call transcript

        Retrieves the full transcript of a company's earnings call, including
        management's prepared remarks and Q&A session. Access management's
        communication about financial performance, future plans, and strategy.

        Args:
            symbol: Stock ticker symbol
            year: Fiscal year (e.g., 2020) - REQUIRED
            quarter: Fiscal quarter (1, 2, 3, or 4) - REQUIRED

        Returns:
            List of transcript objects with:
            - symbol: Stock ticker
            - period: Fiscal period (e.g., "Q3")
            - year: Fiscal year
            - date: Earnings call date
            - content: Full transcript text

        Example:
            # Get Q3 2020 transcript for Apple
            transcript = await client.get_earnings_call_transcript("AAPL", year=2020, quarter=3)
        """
        params = {"symbol": symbol, "year": year, "quarter": quarter}

        return await self._make_request("earning-call-transcript",
                                        params=params,
                                        version="stable")

    async def get_earnings_call_dates(self, symbol: str) -> List[List]:
        """
        Get all available earnings call dates for a symbol.

        Returns a list of all earnings call transcripts with their dates,
        allowing date-based matching rather than fiscal year/quarter guessing.

        Args:
            symbol: Stock ticker symbol

        Returns:
            List of [quarter, fiscal_year, call_datetime] lists
            e.g., [[3, 2026, "2025-11-19 17:00:00"], [2, 2026, "2025-08-27 17:00:00"], ...]

        Example:
            # Get all transcript dates for NVIDIA
            dates = await client.get_earnings_call_dates("NVDA")
            # Returns: [[3, 2026, "2025-11-19 17:00:00"], ...]
        """
        return await self._make_request("earning_call_transcript",
                                        params={"symbol": symbol},
                                        version="v4")

    async def get_sec_filings(self,
                              symbol: str,
                              filing_type: Optional[str] = None,
                              limit: int = 20) -> List[Dict]:
        """
        Get SEC filings for a company.

        Retrieves SEC filings including 10-K (annual), 10-Q (quarterly),
        8-K (current reports), and other filing types.

        Args:
            symbol: Stock ticker symbol
            filing_type: Filter by filing type (e.g., "10-K", "10-Q", "8-K")
            limit: Maximum number of filings to return (default 20)

        Returns:
            List of filing objects with:
            - symbol: Stock ticker
            - fillingDate: Filing date
            - acceptedDate: SEC acceptance timestamp
            - type: Filing type (10-K, 10-Q, etc.)
            - link: SEC filing index link
            - finalLink: Direct link to filing document

        Example:
            # Get latest 10-Q filings
            filings = await client.get_sec_filings("NVDA", filing_type="10-Q", limit=5)
        """
        params = {"limit": limit}
        if filing_type:
            params["type"] = filing_type

        return await self._make_request(f"sec_filings/{symbol}",
                                        params=params,
                                        version="v3")

    async def get_historical_earnings_calendar(self,
                                               symbol: str,
                                               limit: int = 20) -> List[Dict]:
        """
        Get historical and upcoming earnings calendar for a symbol.

        Provides earnings announcement dates with fiscal period end dates,
        enabling accurate fiscal period identification.

        Args:
            symbol: Stock ticker symbol
            limit: Maximum number of records (default 20)

        Returns:
            List of earnings calendar objects with:
            - date: Earnings announcement date
            - fiscalDateEnding: Fiscal period end date
            - time: "amc" (after market close) or "bmo" (before market open)
            - eps: Actual EPS (None if not yet reported)
            - epsEstimated: Estimated EPS
            - revenue: Actual revenue (None if not yet reported)
            - revenueEstimated: Estimated revenue

        Example:
            # Get earnings calendar including next report
            calendar = await client.get_historical_earnings_calendar("NVDA")
            # First entry with eps=None is the next upcoming report
        """
        result = await self._make_request(f"historical/earning_calendar/{symbol}",
                                          version="v3")
        if result and limit:
            return result[:limit]
        return result

    # Financial Scores
    async def get_financial_score(self, symbol: str) -> List[Dict]:
        """Get financial health scores (Altman Z, Piotroski)"""
        return await self._make_request(f"score/{symbol}", version="v4")

    # Revenue Segmentation
    async def get_revenue_product_segmentation(
            self,
            symbol: str,
            period: str = "annual",
            structure: str = "flat") -> List[Dict]:
        """Get revenue breakdown by product"""
        return await self._make_request("revenue-product-segmentation",
                                        params={
                                            "symbol": symbol,
                                            "period": period,
                                            "structure": structure
                                        },
                                        version="v4")

    async def get_revenue_geographic_segmentation(
            self,
            symbol: str,
            period: str = "annual",
            structure: str = "flat") -> List[Dict]:
        """Get revenue breakdown by geography"""
        return await self._make_request("revenue-geographic-segmentation",
                                        params={
                                            "symbol": symbol,
                                            "period": period,
                                            "structure": structure
                                        },
                                        version="v4")

    # Real-Time Quotes
    async def get_quote(self, symbol: str) -> List[Dict]:
        """
        Get real-time stock quote

        Provides current market data including price, volume, bid/ask, and daily changes.
        Updated in real-time during market hours.

        Args:
            symbol: Stock ticker symbol

        Returns:
            List with real-time quote data including price, volume, dayLow, dayHigh,
            yearLow, yearHigh, marketCap, priceAvg50, priceAvg200, volume, avgVolume,
            open, previousClose, eps, pe, earningsAnnouncement, sharesOutstanding, timestamp
        """
        return await self._make_request(f"quote/{symbol}", use_cache=False)

    async def get_aftermarket_quote(self, symbol: str) -> List[Dict]:
        """
        Get after-market quote (post-market hours)

        Provides post-market trading data including price, volume, and bid/ask
        during after-hours trading sessions (typically 4:00 PM - 8:00 PM ET).

        Args:
            symbol: Stock ticker symbol

        Returns:
            List with after-market quote data
        """
        return await self._make_request("aftermarket-quote",
                                        params={"symbol": symbol},
                                        version="stable",
                                        use_cache=False)

    async def get_stock_price_change(self, symbol: str) -> List[Dict]:
        """
        Get stock price changes over multiple time periods

        Tracks stock price fluctuations in real-time across various time periods
        including daily, weekly, monthly, and long-term performance. Provides
        percentage and absolute value changes for quick growth assessment.

        Args:
            symbol: Stock ticker symbol

        Returns:
            List with price change data including:
            - symbol: Stock ticker
            - 1D: 1 day change
            - 5D: 5 day change
            - 1M: 1 month change
            - 3M: 3 month change
            - 6M: 6 month change
            - ytd: Year to date change
            - 1Y: 1 year change
            - 3Y: 3 year change
            - 5Y: 5 year change
            - 10Y: 10 year change
            - max: Maximum available period change

        Example:
            # Get price changes for Apple
            changes = await client.get_stock_price_change("AAPL")
        """
        return await self._make_request("stock-price-change",
                                        params={"symbol": symbol},
                                        version="stable")

    # Batch Operations
    async def get_batch_profiles(self, symbols: List[str]) -> List[Dict]:
        """Get profiles for multiple companies"""
        symbol_str = ",".join(symbols)
        return await self._make_request(f"profile/{symbol_str}")

    async def get_batch_quotes(self, symbols: List[str]) -> List[Dict]:
        """Get quotes for multiple companies"""
        symbol_str = ",".join(symbols)
        return await self._make_request(f"quote/{symbol_str}")

    async def get_batch_market_cap(self, symbols: List[str]) -> Dict:
        """Get market cap for multiple companies"""
        return await self._make_request("market-capitalization",
                                        params={"symbol": ",".join(symbols)},
                                        version="v4")

    # News & Press Releases
    async def get_fmp_articles(self, limit: int = 10, page: int = 0) -> List[Dict]:
        """
        Get latest FMP articles

        Args:
            limit: Number of articles to return (default 10)
            page: Page number for pagination (default 0)

        Returns:
            List of article objects with title, date, content, tickers, image, link, author, site
        """
        result = await self._make_request("fmp-articles",
                                          params={
                                              "limit": limit,
                                              "page": page
                                          },
                                          version="stable")
        # FMP API may ignore limit parameter, enforce it client-side
        return result[:limit] if isinstance(result, list) else result

    async def get_general_news(self, limit: int = 10, page: int = 0) -> List[Dict]:
        """
        Get latest general news articles from various sources

        Args:
            limit: Number of articles to return (default 10)
            page: Page number for pagination (default 0)

        Returns:
            List of news objects with symbol, publishedDate, publisher, title, image, site, text, url
        """
        result = await self._make_request("news/general-latest",
                                          params={
                                              "limit": limit,
                                              "page": page
                                          },
                                          version="stable")
        # FMP API may ignore limit parameter, enforce it client-side
        return result[:limit] if isinstance(result, list) else result

    async def get_press_releases(self,
                                 symbol: str,
                                 limit: int = 10,
                                 page: int = 0) -> List[Dict]:
        """
        Get company press releases

        Args:
            symbol: Stock ticker symbol
            limit: Number of press releases to return (default 10)
            page: Page number for pagination (default 0)

        Returns:
            List of press release objects with symbol, date, title, text
        """
        result = await self._make_request(f"press-releases/{symbol}",
                                          params={
                                              "limit": limit,
                                              "page": page
                                          })
        # FMP API may ignore limit parameter, enforce it client-side
        return result[:limit] if isinstance(result, list) else result

    # Hot lists
    async def get_biggest_losers(self, limit: int = 50) -> List[Dict]:
        """Get biggest losers list from stable endpoint"""
        result = await self._make_request("biggest-losers", version="stable")
        return result[:limit] if isinstance(result, list) else result

    async def get_most_actives(self, limit: int = 50) -> List[Dict]:
        """Get most actives list from stable endpoint"""
        result = await self._make_request("most-actives", version="stable")
        return result[:limit] if isinstance(result, list) else result

    async def get_biggest_gainers(self, limit: int = 50) -> List[Dict]:
        """Get biggest gainers list from stable endpoint"""
        result = await self._make_request("biggest-gainers", version="stable")
        return result[:limit] if isinstance(result, list) else result

    # Technical Indicators
    async def get_sma(self,
                      symbol: str,
                      period_length: int,
                      from_date: Optional[str] = None,
                      to_date: Optional[str] = None,
                      timeframe: str = "1day") -> List[Dict]:
        """
        Get Simple Moving Average (SMA) indicator data

        Args:
            symbol: Stock ticker symbol
            period_length: SMA period (e.g., 5, 20, 60)
            from_date: Start date (YYYY-MM-DD format or date object)
            to_date: End date (YYYY-MM-DD format or date object)
            timeframe: Timeframe for calculation (default "1day")

        Returns:
            List of SMA data points with date, open, high, low, close, volume, sma
        """
        from datetime import date, timedelta

        # Set default dates if not provided - increased to 500 days for better backtest coverage
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
            "to": to_date
        }

        return await self._make_request("technical-indicators/sma",
                                        params=params,
                                        version="stable")

    async def get_stock_price(self,
                              symbol: str,
                              from_date: Optional[str] = None,
                              to_date: Optional[str] = None) -> List[Dict]:
        """
        Get historical stock price data (OHLCV)

        Args:
            symbol: Stock ticker symbol
            from_date: Start date (YYYY-MM-DD format or date object)
            to_date: End date (YYYY-MM-DD format or date object)

        Returns:
            List of historical price data with date, open, high, low, close, volume
        """
        from datetime import date, timedelta

        # Set default dates if not provided - increased to 500 days for better backtest coverage
        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full",
                                        params=params,
                                        version="stable")

    async def get_intraday_chart(self,
                                 symbol: str,
                                 interval: str,
                                 from_date: Optional[str] = None,
                                 to_date: Optional[str] = None) -> List[Dict]:
        """
        Get intraday stock chart data with 1-minute intervals

        Retrieves historical intraday OHLCV data at various time intervals.
        Useful for detailed technical analysis and intraday trading patterns.

        Args:
            symbol: Stock ticker symbol
            interval: Time interval - one of: '1min', '5min', '15min', '30min', '1hour', '4hour'
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of intraday price data with date, open, high, low, close, volume

        Example:
            # Get 5-minute intraday data for Apple
            data = await client.get_intraday_chart("AAPL", "5min", from_date="2024-01-01", to_date="2024-01-31")
        """
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}",
                                        params=params,
                                        version="stable")

    async def get_commodity_price(self,
                                  symbol: str,
                                  from_date: Optional[str] = None,
                                  to_date: Optional[str] = None) -> List[Dict]:
        """
        Get historical commodity price data (OHLCV)

        Args:
            symbol: Commodity symbol (e.g., 'GCUSD' for Gold, 'SIUSD' for Silver, 'CLUSD' for Crude Oil)
            from_date: Start date (YYYY-MM-DD format or date object)
            to_date: End date (YYYY-MM-DD format or date object)

        Returns:
            List of historical price data with date, open, high, low, close, volume

        Example:
            # Get gold price history
            data = await client.get_commodity_price("GCUSD", from_date="2024-01-01", to_date="2024-12-31")
        """
        from datetime import date, timedelta

        # Set default dates if not provided - 500 days lookback for consistency with stocks
        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full",
                                        params=params,
                                        version="stable")

    async def get_commodity_intraday_chart(self,
                                           symbol: str,
                                           interval: str,
                                           from_date: Optional[str] = None,
                                           to_date: Optional[str] = None) -> List[Dict]:
        """
        Get intraday commodity chart data

        Retrieves historical intraday OHLCV data for commodities at various time intervals.
        Useful for detailed technical analysis and short-term trading patterns.

        Args:
            symbol: Commodity symbol (e.g., 'GCUSD' for Gold, 'SIUSD' for Silver)
            interval: Time interval - one of: '1min', '5min', '1hour'
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of intraday price data with date (timestamp), open, high, low, close, volume

        Example:
            # Get 5-minute intraday data for Gold
            data = await client.get_commodity_intraday_chart("GCUSD", "5min", from_date="2024-01-01", to_date="2024-01-31")
        """
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}",
                                        params=params,
                                        version="stable")

    async def get_crypto_price(self,
                               symbol: str,
                               from_date: Optional[str] = None,
                               to_date: Optional[str] = None) -> List[Dict]:
        """
        Get historical cryptocurrency price data (OHLCV)

        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTCUSD' for Bitcoin, 'ETHUSD' for Ethereum, 'SOLUSD' for Solana)
            from_date: Start date (YYYY-MM-DD format or date object)
            to_date: End date (YYYY-MM-DD format or date object)

        Returns:
            List of historical price data with date, open, high, low, close, volume

        Example:
            # Get Bitcoin price history
            data = await client.get_crypto_price("BTCUSD", from_date="2024-01-01", to_date="2024-12-31")
        """
        from datetime import date, timedelta

        # Set default dates if not provided - 500 days lookback for consistency
        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full",
                                        params=params,
                                        version="stable")

    async def get_crypto_intraday_chart(self,
                                        symbol: str,
                                        interval: str,
                                        from_date: Optional[str] = None,
                                        to_date: Optional[str] = None) -> List[Dict]:
        """
        Get intraday cryptocurrency chart data

        Retrieves historical intraday OHLCV data for cryptocurrencies at various time intervals.
        Useful for detailed technical analysis and short-term trading patterns.

        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTCUSD' for Bitcoin, 'ETHUSD' for Ethereum)
            interval: Time interval - one of: '1min', '5min', '1hour'
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of intraday price data with date (timestamp), open, high, low, close, volume

        Example:
            # Get 5-minute intraday data for Bitcoin
            data = await client.get_crypto_intraday_chart("BTCUSD", "5min", from_date="2024-01-01", to_date="2024-01-31")
        """
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}",
                                        params=params,
                                        version="stable")

    async def get_forex_price(self,
                              symbol: str,
                              from_date: Optional[str] = None,
                              to_date: Optional[str] = None) -> List[Dict]:
        """
        Get historical forex price data (OHLCV)

        Args:
            symbol: Forex pair symbol (e.g., 'EURUSD', 'GBPUSD', 'USDJPY')
            from_date: Start date (YYYY-MM-DD format or date object)
            to_date: End date (YYYY-MM-DD format or date object)

        Returns:
            List of historical price data with date, open, high, low, close, volume

        Example:
            # Get EUR/USD price history
            data = await client.get_forex_price("EURUSD", from_date="2024-01-01", to_date="2024-12-31")
        """
        from datetime import date, timedelta

        # Set default dates if not provided - 500 days lookback for consistency
        if from_date is None:
            from_date = (date.today() - timedelta(days=500)).isoformat()
        elif isinstance(from_date, date):
            from_date = from_date.isoformat()

        if to_date is None:
            to_date = date.today().isoformat()
        elif isinstance(to_date, date):
            to_date = to_date.isoformat()

        params = {"symbol": symbol, "from": from_date, "to": to_date}

        return await self._make_request("historical-price-eod/full",
                                        params=params,
                                        version="stable")

    async def get_forex_intraday_chart(self,
                                       symbol: str,
                                       interval: str,
                                       from_date: Optional[str] = None,
                                       to_date: Optional[str] = None) -> List[Dict]:
        """
        Get intraday forex chart data

        Retrieves historical intraday OHLCV data for forex pairs at various time intervals.
        Useful for detailed technical analysis and short-term trading patterns.

        Args:
            symbol: Forex pair symbol (e.g., 'EURUSD', 'GBPUSD', 'USDJPY')
            interval: Time interval - one of: '1min', '5min', '1hour'
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)

        Returns:
            List of intraday price data with date (timestamp), open, high, low, close, volume

        Example:
            # Get 5-minute intraday data for EUR/USD
            data = await client.get_forex_intraday_chart("EURUSD", "5min", from_date="2024-01-01", to_date="2024-01-31")
        """
        params = {"symbol": symbol}

        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        return await self._make_request(f"historical-chart/{interval}",
                                        params=params,
                                        version="stable")

    # Utility Methods
    def clear_cache(self):
        """Clear all cached data"""
        self._cache = {}
        self._cache_timestamps = {}

    def clear_cache_for_symbol(self, symbol: str):
        """Clear cache for specific symbol"""
        keys_to_remove = [k for k in self._cache.keys() if symbol in k]
        for key in keys_to_remove:
            del self._cache[key]
            del self._cache_timestamps[key]
