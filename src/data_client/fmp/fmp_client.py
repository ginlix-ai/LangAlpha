"""FMP (Financial Modeling Prep) async client — targets the stable API."""

import asyncio
import json
import math
import os
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, NamedTuple, Optional, Union

import httpx
from market_protocol import display_spelling, vendor_spelling

from . import walk
from .walk import Walk

_CACHE_MAX_SIZE = 512
# A dropped connection or a 5xx is retried on a fresh attempt, a refusal never:
# a walk makes up to 64 calls, and one transient failure fails the read. A
# timeout is not retried, so a hung FMP still fails a call in one timeout.
# ginlix-data's FMP client retries a dropped connection and a 5xx the same
# number of times, and a timeout too.
_RETRIES = 2
_BACKOFF_S = 0.25
_TRANSIENT = frozenset({500, 502, 503, 504})


def _vendor_symbol_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Respell the symbol params in FMP's form.

    Every FMP request passes here, so callers hold our spelling (``600519.SH``)
    and FMP still receives the ``600519.SS`` it resolves. A blank entry in a
    list (``AAPL,,MSFT``) names nothing and is dropped; a symbol that cannot be
    spelled raises :class:`FMPRequestError` rather than reaching FMP.
    """
    out = dict(params)
    try:
        if isinstance(out.get("symbol"), str):
            out["symbol"] = vendor_spelling(out["symbol"])
        if isinstance(out.get("symbols"), str):
            out["symbols"] = ",".join(
                vendor_spelling(s) for s in out["symbols"].split(",") if s.strip()
            )
    except ValueError as exc:
        # 400, the status FMP gives a malformed request: callers that map
        # statuses report it as a bad argument, with the reason.
        raise FMPRequestError(f"Invalid symbol for FMP: {exc}", status_code=400) from exc
    return out


def _own_symbol_rows(data: Any) -> Any:
    """Respell the ``symbol`` of each returned row in our spelling, in place.

    The inbound half of :func:`_vendor_symbol_params`: FMP answers a Shanghai
    listing as ``600519.SS``, and nothing past this client should see that.
    """
    rows = data if isinstance(data, list) else [data]
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("symbol"), str):
            row["symbol"] = display_spelling(row["symbol"])
    return data


class FMPRequestError(Exception):
    """FMP request failure with a sanitized message and optional HTTP status.

    ``str(exc)`` is safe to surface to callers/agents: it never embeds the
    request URL or headers; the key travels in the ``apikey`` header.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


def _retry_after(value: Optional[str]) -> Optional[float]:
    """Seconds a ``Retry-After`` header asks for, in either of its forms."""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        seconds = (when - datetime.now(timezone.utc)).total_seconds()
    return max(seconds, 0.0) if math.isfinite(seconds) else None


def _today_utc() -> date:
    """UTC equivalent of ``date.today()`` — the server runs in UTC; local-clock dates drift."""
    return datetime.now(timezone.utc).date()


class CallDate(NamedTuple):
    """One FMP earnings-call listing: the fiscal label and the day it was held."""

    quarter: int
    fiscal_year: int
    date: date


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
        params = _vendor_symbol_params(params or {})

        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"

        if use_cache and self._is_cache_valid(cache_key):
            self._cache.move_to_end(cache_key)
            return self._cache[cache_key]

        url = self._build_url(endpoint, version)
        client = await self._get_client()

        try:
            for attempt in range(_RETRIES + 1):
                try:
                    response = await client.get(
                        url, params=params, headers={"apikey": self.api_key}
                    )
                except httpx.TimeoutException:
                    raise
                except httpx.TransportError:
                    if attempt == _RETRIES:
                        raise
                else:
                    if response.status_code not in _TRANSIENT or attempt == _RETRIES:
                        break
                await asyncio.sleep(_BACKOFF_S * 2**attempt)
            response.raise_for_status()
            data = _own_symbol_rows(response.json())

            if use_cache and data:
                self._cache[cache_key] = data
                self._cache_timestamps[cache_key] = datetime.now()
                while len(self._cache) > _CACHE_MAX_SIZE:
                    oldest_key, _ = self._cache.popitem(last=False)
                    self._cache_timestamps.pop(oldest_key, None)

            return data

        except httpx.HTTPStatusError as e:
            # Defense in depth: never stringify the httpx error. Its message
            # embeds the request URL; auth must never depend on URL hygiene.
            raise FMPRequestError(
                f"FMP API request failed ({e.response.status_code})",
                status_code=e.response.status_code,
                retry_after=_retry_after(e.response.headers.get("Retry-After")),
            )
        except httpx.TimeoutException:
            raise FMPRequestError("FMP API request timed out")
        except httpx.RequestError:
            raise FMPRequestError("FMP API request failed")

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

        Stable replaces v3's ``rating`` with ``ratings-snapshot``; this
        delegates to :meth:`get_ratings_snapshot` so the two stay in sync.
        """
        return await self.get_ratings_snapshot(symbol)

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

    async def get_earnings_call_dates(self, symbol: str) -> List[CallDate]:
        """All earnings-call transcript dates for ``symbol``, one per FMP label.

        Rows missing a label or a parseable date are dropped.
        """
        data = await self._make_request(
            "earning-call-transcript-dates", params={"symbol": symbol}
        )
        if isinstance(data, dict):
            raise FMPRequestError("FMP API returned an error")
        calls: List[CallDate] = []
        for row in data or []:
            try:
                calls.append(CallDate(
                    int(row["quarter"]),
                    int(row.get("fiscalYear") or row["year"]),
                    datetime.strptime(row["date"][:10], "%Y-%m-%d").date(),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        return calls

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

        rows = await self._make_request(
            "historical-price-eod/full",
            params={"symbol": symbol, "from": from_date, "to": to_date},
        )
        return walk.eod_answer(rows, from_date)

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

        rows = await self._make_request(f"historical-chart/{interval}", params=params)
        return walk.intraday_answer(rows, interval, from_date, to_date, _today_utc())

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
        """Search across symbols AND company names, up to *limit* from each.

        v3 ``search`` matched both. Stable splits this into ``search-symbol``
        (symbol prefix match) and ``search-name`` (company name match);
        we call both in parallel and merge by symbol. The union is not cut
        back to *limit*: ``search-symbol`` fills it on a short prefix in its
        own order (``nv`` leaves out NVDA), and the name matches are what
        bring the listing back for the caller to rank.
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
        return list(merged.values())

    # =====================================================================
    # Macro & Economic Data
    # =====================================================================

    async def get_economic_indicators(self, name: str, limit: int = 50) -> List[Dict]:
        """The newest ``limit`` observations of an indicator, newest first, read
        back from today in UTC (:func:`walk.read_indicator`)."""
        return await walk.read_indicator(self._make_request, name, limit, _today_utc())

    async def _range(
        self, endpoint: str, from_date: str, to_date: str, **walk_kwargs: Any
    ) -> Union[Walk, Dict]:
        """A range read back by :func:`walk.read_range`."""

        async def request(params: Dict[str, Any]) -> Any:
            return await self._make_request(endpoint, params=params)

        return await walk.read_range(request, from_date, to_date, **walk_kwargs)

    async def get_economic_calendar(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        """Events in the range; with both bounds a :class:`Walk` (see :meth:`_range`).

        A lone bound is one read: the calendar holds upcoming dates, so where
        its range ends is FMP's to say.
        """
        if from_date and to_date:
            return await self._range(
                "economic-calendar", from_date, to_date, from_start=True
            )
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        return await self._make_request("economic-calendar", params=params)

    async def get_treasury_rates(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        """Daily rates; with a start, a :class:`Walk` to ``to_date`` or today in UTC.

        No rate lies ahead, so a lone start leaves no doubt where the range
        ends. Without a start it is one read, FMP's own recent window.
        """
        if from_date:
            to_date = to_date or _today_utc().isoformat()
            return await self._range("treasury-rates", from_date, to_date)
        params: Dict[str, Any] = {}
        if to_date:
            params["to"] = to_date
        return await self._make_request("treasury-rates", params=params)

    async def get_market_risk_premium(self) -> List[Dict]:
        return await self._make_request("market-risk-premium")

    async def get_earnings_calendar_by_date(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ) -> List[Dict]:
        """Reporters in the range; with both bounds a :class:`Walk` (see
        :meth:`_range`), paged past FMP's 4,000-row answer. A lone bound is one
        read, as the economic calendar's is, sorted oldest first as the range
        read is."""
        if from_date and to_date:
            return await self._range(
                "earnings-calendar",
                from_date,
                to_date,
                page_rows=walk.EARNINGS_PAGE_ROWS,
                window_days=walk.EARNINGS_WINDOW_DAYS,
                from_start=True,
            )
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        rows = await self._make_request("earnings-calendar", params=params)
        if not isinstance(rows, list):
            return rows
        return Walk(sorted(rows, key=lambda row: str(row.get("date"))))
