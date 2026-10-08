"""CN fundamentals, market intel and news served by ginlix-data's protocol routes.

The rows already carry langalpha's per-method shapes (FMP camelCase for
fundamentals, snake_case for intel), so each method is one route call plus
the ``symbol`` echo: ginlix-data stamps the protocol ``instrument_key``, the
callers here expect the spelling they asked with.

A name ginlix-data does not serve returns the method's empty value. A
transport error on a per-symbol kind raises instead, so an answer built from
several kinds is never half empty; the routed provider falls back to its
default on either.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from market_protocol import (
    display_spelling,
    exchange_code,
    from_instrument_key,
    market_home,
    to_canonical,
    to_legacy_api,
)

from ..financial_data_provider import PartialSearch
from .v2_routes import GinlixDataV2Routes

logger = logging.getLogger(__name__)

_CN_TZ = ZoneInfo(market_home("cn")[1])

# FMP's screener vocabulary -> the route's vendor-neutral filter names.
_SCREEN_PARAMS = {
    "marketCapMoreThan": "market_cap_min", "marketCapLowerThan": "market_cap_max",
    "priceMoreThan": "price_min", "priceLowerThan": "price_max",
    "dividendMoreThan": "dividend_min", "dividendLowerThan": "dividend_max",
    "exchange": "exchange",
}
_SCREEN_SUPPORTED = frozenset(_SCREEN_PARAMS) | {"country", "market", "limit"}
_SCREEN_COUNTRIES = {"CN", "CHN", "CHINA"}
# The exchange spellings ginlix-data's CN screen accepts, less "BSE": that is
# Beijing there but Bombay in FMP's screener vocabulary, so on its own it stays
# with the default screen and reaches the CN one only beside country CN.
_SCREEN_EXCHANGES = {
    "SSE", "SHANGHAI", "SH", "XSHG",
    "SZSE", "SHENZHEN", "SZ", "XSHE",
    "BEIJING", "BJ", "BJSE",
}
# The latest-snapshot kinds arrive in the period rows' spelling; every other
# FinancialDataSource answers get_key_metrics / get_financial_ratios with FMP's
# TTM keys, so these rows are renamed to match. Most keys just gain the suffix.
_TTM_VERBATIM = frozenset({"symbol", "marketCap", "beta", "forwardPERatio"})
_TTM_IRREGULAR = {
    "peRatio": "priceToEarningsRatioTTM",
    "debtToEquity": "debtToEquityRatioTTM",
    "debtToAssets": "debtToAssetsRatioTTM",
    "payoutRatio": "dividendPayoutRatioTTM",
    "enterpriseValueOverEBITDA": "evToEBITDATTM",
}
_ARTICLE_BUFFER_MAX = 600
_NEWS_WINDOW = 1000  # the route's cap; the market-wide window is ~500-800 articles
_LOOKUP_WINDOW_TTL = 30.0


async def _rows(
    client: GinlixDataV2Routes, symbol: str, kind: str, **params: Any
) -> list[dict[str, Any]]:
    try:
        key = to_canonical(symbol).instrument_key
    except ValueError:
        return []
    return await client.get_fundamentals_v2(key, kind, **params)


def _ttm(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {k if k in _TTM_VERBATIM else _TTM_IRREGULAR.get(k, f"{k}TTM"): v for k, v in r.items()}
        for r in rows
    ]


def _names_cn(value: Any) -> bool:
    return str(value or "").strip().upper() in _SCREEN_COUNTRIES


def screens_cn(filters: dict[str, Any]) -> bool:
    """True when screener filters explicitly target the CN market."""
    return (
        _names_cn(filters.get("country"))
        or _names_cn(filters.get("market"))
        or str(filters.get("exchange") or "").upper() in _SCREEN_EXCHANGES
    )


def has_cjk(query: str) -> bool:
    """A CJK name query, which the CN/HK directory answers before the default search."""
    return any("一" <= ch <= "鿿" for ch in query)


def is_cn_option(ticker: str) -> bool:
    """CN exchange option contract codes carry their exchange's suffix, in any spelling."""
    try:
        return to_canonical(ticker).mic in ("XSHG", "XSHE")
    except ValueError:
        return False


# Segment rows a CN filing prints beside the segments without being one: the
# classification header, which carries that classification's total (产品 /
# 地区 / 行业), totals and subtotals (合计, 小计, 总计), inter-segment
# eliminations (抵消 / 抵销) and reconciling adjustments (调整, as in
# 合计特别调整). Left in, each would take a slice of the breakdown.
_SEGMENT_HEADERS = frozenset({"产品", "地区", "行业"})
_NON_SEGMENT_MARKERS = ("合计", "小计", "总计", "抵消", "抵销", "调整")


def _is_segment(name: str) -> bool:
    return name not in _SEGMENT_HEADERS and not any(m in name for m in _NON_SEGMENT_MARKERS)


def _segments_only(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``[{date: {segment: value}}]`` rows with the non-segment items dropped.

    A period left with no segment goes too: ``{date: {}}`` would read as an
    answer and keep the routed provider from asking its default.
    """
    out = []
    for row in rows:
        kept = {}
        for d, items in row.items():
            if isinstance(items, dict):
                items = {k: v for k, v in items.items() if _is_segment(k)}
                if not items:
                    continue
            kept[d] = items
        if kept:
            out.append(kept)
    return out


def _echo(rows: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
    for r in rows:
        if "symbol" in r:
            r["symbol"] = symbol
    return rows


class GinlixDataCnFinancialSource:
    """``FinancialDataSource`` methods for CN listings over ginlix-data v2."""

    def __init__(self, client: GinlixDataV2Routes) -> None:
        self.client = client

    async def _kind(self, symbol: str, kind: str, **params: Any) -> list[dict[str, Any]]:
        return _echo(await _rows(self.client, symbol, kind, **params), symbol)

    async def get_income_statements(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await self._kind(symbol, "income", period=period, limit=limit)

    async def get_cash_flows(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await self._kind(symbol, "cashflow", period=period, limit=limit)

    async def get_balance_sheets(
        self, symbol: str, period: str = "annual", limit: int = 10
    ) -> list[dict[str, Any]]:
        return await self._kind(symbol, "balance", period=period, limit=limit)

    async def get_company_profile(self, symbol: str) -> list[dict[str, Any]]:
        return await self._kind(symbol, "profile")

    async def get_key_metrics(self, symbol: str) -> list[dict[str, Any]]:
        return _ttm(await self._kind(symbol, "key_metrics"))

    async def get_financial_ratios(self, symbol: str) -> list[dict[str, Any]]:
        return _ttm(await self._kind(symbol, "financial_ratios"))

    async def get_key_metrics_periods(
        self, symbol: str, period: str = "annual", limit: int = 10
    ) -> list[dict[str, Any]]:
        return await self._kind(symbol, "metrics", period=period, limit=limit)

    async def get_ratios_periods(
        self, symbol: str, period: str = "annual", limit: int = 10
    ) -> list[dict[str, Any]]:
        return await self._kind(symbol, "ratios", period=period, limit=limit)

    async def get_growth_periods(
        self, symbol: str, period: str = "annual", limit: int = 10
    ) -> dict[str, list[dict[str, Any]]]:
        """FMP's two growth payloads, both views of one indicator series.

        FMP keys income-statement growth ``growthRevenue`` where financial
        growth says ``revenueGrowth``.
        """
        growth = await self._kind(symbol, "growth", period=period, limit=limit)
        return {
            "financial_growth": growth,
            "income_statement_growth": [{
                "date": g.get("date"),
                "growthRevenue": g.get("revenueGrowth"),
                "growthNetIncome": g.get("netIncomeGrowth"),
                "growthEPS": g.get("epsgrowth"),
            } for g in growth],
        }

    async def get_price_performance(self, symbol: str) -> list[dict[str, Any]]:
        return await self._kind(symbol, "performance")

    async def get_analyst_price_targets(self, symbol: str) -> list[dict[str, Any]]:
        return await self._kind(symbol, "analyst_targets")

    async def get_analyst_ratings(self, symbol: str) -> list[dict[str, Any]]:
        return await self._kind(symbol, "analyst_ratings")

    async def get_earnings_history(self, symbol: str, limit: int = 10) -> list[dict[str, Any]]:
        return await self._kind(symbol, "earnings", limit=limit)

    async def get_revenue_by_segment(
        self, symbol: str, segment_type: str = "product", period: str = "annual", **_: Any
    ) -> list[dict[str, Any]]:
        kind = "segments_geography" if segment_type == "geography" else "segments_product"
        return _segments_only(await self._kind(symbol, kind, period=period))

    async def get_dividends(self, symbol: str) -> dict[str, list[dict[str, Any]]]:
        dividends, splits = await asyncio.gather(
            self._kind(symbol, "dividends"), self._kind(symbol, "splits")
        )
        return {"dividends": dividends, "splits": splits}

    async def get_shares_float(self, symbol: str) -> list[dict[str, Any]]:
        return await self._kind(symbol, "shares_float")

    async def get_key_executives(self, symbol: str) -> list[dict[str, Any]]:
        """Rows in the FMP shape the tool promises; CN filings carry no
        gender, birth year or tenure, so those keys are present and empty."""
        return [
            {"gender": None, "yearBorn": None, "titleSince": None, **row}
            for row in await self._kind(symbol, "executives")
        ]

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        """Whole-market CN screen; ``[]`` when a filter cannot be applied here.

        An empty result is a miss the routed provider re-runs on its default
        source, the only honest answer for a filter this screen cannot apply.
        """
        given = {k: v for k, v in filters.items() if v is not None}
        if unsupported := sorted(set(given) - _SCREEN_SUPPORTED):
            logger.info("ginlix_data.cn.screen_decline | unsupported=%s", ",".join(unsupported))
            return []
        for key in ("country", "market"):
            if key in given and not _names_cn(given[key]):
                return []
        params = {_SCREEN_PARAMS[k]: v for k, v in given.items() if k in _SCREEN_PARAMS}
        params["limit"] = min(int(given.get("limit", 25) or 25), 250)
        try:
            rows = await self.client.screen_v2(market="cn", **params)
        except httpx.HTTPError as exc:
            logger.info("ginlix_data.cn.screen_unavailable | error=%s", type(exc).__name__)
            return []
        for r in rows:
            key = r.pop("instrument_key", None)
            try:
                r["symbol"] = to_legacy_api(from_instrument_key(key))
            except ValueError:
                pass
        return rows

    async def search_stocks(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        """Name search over the CN and HK directories (中文 / pinyin / English / code).

        A directory that fails while the other finds nothing raises: ``[]``
        would be cached upstream as "no such name". While the other finds
        names it raises :class:`PartialSearch` with them, so they are served
        but not cached as the whole answer.
        """
        markets = ("cn", "hk")
        answers = await asyncio.gather(
            *(self.client.search_v2(query, market=m, limit=limit) for m in markets),
            return_exceptions=True,
        )
        out: list[dict[str, Any]] = []
        failure: BaseException | None = None
        for market, results in zip(markets, answers):
            if isinstance(results, BaseException):
                if not isinstance(results, httpx.HTTPError):
                    raise results
                logger.info("ginlix_data.cn.search_unavailable | market=%s error=%s",
                            market, type(results).__name__)
                failure = failure or results
                continue
            for r in results:
                try:
                    symbol = to_legacy_api(from_instrument_key(r["instrument_key"]))
                except (KeyError, ValueError):
                    continue
                exchange = exchange_code(r.get("mic"))
                out.append({
                    "symbol": symbol,
                    # Both names are optional on the wire; a search row needs one.
                    "name": r.get("name_local") or r.get("name") or symbol,
                    "nameLocal": r.get("name_local") or None,
                    "nameEn": r.get("name") if r.get("name") != r.get("name_local") else None,
                    "currency": r.get("currency"),
                    "stockExchange": exchange,
                    "exchangeShortName": exchange,
                })
        if failure is not None:
            if not out:
                raise failure
            raise PartialSearch(out[:limit])
        return out[:limit]

    async def close(self) -> None:
        pass  # the shared ginlix-data client is closed by its owner


class GinlixDataCnIntelSource:
    """``MarketIntelSource`` methods for CN listings over ginlix-data v2."""

    def __init__(self, client: GinlixDataV2Routes) -> None:
        self.client = client

    async def get_short_interest(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "settlement_date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = await _rows(self.client, symbol, "short_interest", limit=limit)
        rows.sort(key=lambda r: r.get("settlement_date") or "", reverse=sort.endswith(".desc"))
        return rows

    async def get_short_volume(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = await _rows(self.client, symbol, "short_volume", limit=limit)
        rows.sort(key=lambda r: r.get("date") or "", reverse=sort.endswith(".desc"))
        return rows

    async def get_float_shares(self, symbol: str, user_id: str | None = None) -> dict[str, Any]:
        rows = await _rows(self.client, symbol, "float_shares")
        return rows[0] if rows else {}

    async def get_options_chain(
        self, underlying: str, user_id: str | None = None, **filters: Any
    ) -> dict[str, Any]:
        """Listed contracts on a CN ETF underlying, echoing the caller's spelling."""
        try:
            key = to_canonical(underlying).instrument_key
        except ValueError:
            return {"results": []}
        try:
            rows = await self.client.get_options_chain_v2(key, **filters)
        except httpx.HTTPError as exc:
            logger.info("ginlix_data.cn.options_chain_unavailable | error=%s", type(exc).__name__)
            return {"results": []}
        for r in rows:
            r["underlying_ticker"] = underlying
        return {"results": rows}

    async def get_options_ohlcv(
        self,
        options_ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
        interval: str = "1hour",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Daily contract bars; any other interval is a miss the chain falls through."""
        if interval not in ("1day", "daily"):
            return []
        try:
            # Contract codes travel in the exchange's own spelling (``.SH``).
            return await self.client.get_options_bars_v2(
                display_spelling(options_ticker), market="cn",
                from_date=from_date, to_date=to_date,
            )
        except httpx.HTTPError as exc:
            logger.info("ginlix_data.cn.options_bars_unavailable | error=%s", type(exc).__name__)
            return []

    async def close(self) -> None:
        pass


def _instant(text: str | None) -> datetime | None:
    """ISO-8601 -> aware datetime; a naive boundary reads as CN local, like the rows."""
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).strip())
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=_CN_TZ)


class GinlixDataCnNewsSource:
    """CN market news (Chinese-language, market-wide) over ginlix-data v2.

    The feed is market-wide: a ticker-filtered request gets only articles
    tagged with one of its tickers, never the general pulse under a ticker's
    name. Paging and time filters run here over the window ginlix-data keeps
    warm; article ids are content hashes, stable across fetches, so cursors
    survive a refresh.
    """

    def __init__(self, client: GinlixDataV2Routes) -> None:
        self.client = client
        # Recently served articles, for article-by-id lookup without a refetch.
        self._buffer: dict[str, dict[str, Any]] = {}
        # (fetched at, window by id) for ids the buffer misses; see _lookup_window.
        self._lookup: tuple[float, dict[str, dict[str, Any]]] | None = None
        self._lookup_lock = asyncio.Lock()

    def _remember(self, articles: list[dict[str, Any]]) -> None:
        for a in articles:
            self._buffer[a["id"]] = a
        while len(self._buffer) > _ARTICLE_BUFFER_MAX:
            self._buffer.pop(next(iter(self._buffer)))

    async def _window(self) -> list[dict[str, Any]]:
        return await self.client.get_news_v2(market="cn", limit=_NEWS_WINDOW)

    async def get_news(
        self,
        tickers: list[str] | None = None,
        limit: int = 20,
        published_after: str | None = None,
        published_before: str | None = None,
        cursor: str | None = None,
        order: str | None = None,
        sort: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        articles = await self._window()
        if tickers:
            wanted = {display_spelling(t) for t in tickers}
            articles = [
                a for a in articles
                if wanted.intersection(display_spelling(str(t)) for t in a.get("tickers") or ())
            ]
        lo, hi = _instant(published_after), _instant(published_before)
        if lo or hi:
            def _within(article: dict[str, Any]) -> bool:
                at = _instant(article.get("published_at"))
                if at is None:
                    return True
                return (lo is None or at >= lo) and (hi is None or at <= hi)

            articles = [a for a in articles if _within(a)]
        if (order or "").lower() == "asc":
            # The window is newest first; publication time is the one order it has.
            articles = articles[::-1]
        if cursor:
            # Cursor = the id at the previous page boundary; serve the items
            # after it in this order. An id rotated out of the window ends the feed.
            ids = [a["id"] for a in articles]
            articles = articles[ids.index(cursor) + 1:] if cursor in ids else []
        page = articles[:limit]
        self._remember(page)
        next_cursor = page[-1]["id"] if page and len(articles) > len(page) else None
        return {"results": page, "count": len(page), "next_cursor": next_cursor}

    async def get_news_article(
        self, article_id: str, user_id: str | None = None
    ) -> dict[str, Any] | None:
        cached = self._buffer.get(article_id)
        if cached is not None:
            return cached
        try:
            window = await self._lookup_window()
        except httpx.HTTPError as exc:
            logger.warning("news.ginlix_data_cn.article_lookup_failed | error=%s",
                           type(exc).__name__)
            return None
        return window.get(article_id)

    async def _lookup_window(self) -> dict[str, dict[str, Any]]:
        """The window by id, fetched at most once per ``_LOOKUP_WINDOW_TTL``.

        An id no window holds (a stale link, a guessed ``ts-`` id) would
        otherwise cost the whole window on every request. Holding it briefly
        hides nothing a reader could ask for: an article newer than the held
        window can only sit on the newest page, which the feed cache answers.
        """
        async with self._lookup_lock:
            now = time.monotonic()
            if self._lookup is None or now - self._lookup[0] >= _LOOKUP_WINDOW_TTL:
                articles = await self._window()
                self._lookup = (now, {a["id"]: a for a in articles})
            return self._lookup[1]

    async def close(self) -> None:
        pass
