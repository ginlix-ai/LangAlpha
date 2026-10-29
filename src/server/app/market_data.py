"""
FastAPI router for market data proxy endpoints.

Provides cached access to FMP intraday data for stocks and indexes.
"""

import logging
from typing import NamedTuple, Optional

from fastapi import APIRouter, HTTPException, Query

from src.server.utils.api import CurrentUserId

from src.server.models.market_data import (
    IntradayDataPoint,
    IntradayResponse,
    DailyResponse,
    BatchIntradayRequest,
    BatchIntradayResponse,
    CacheMetadata,
    BatchCacheStats,
    CompanyOverviewResponse,
    StockSearchResult,
    StockSearchResponse,
    PriceTargetSummary,
    AnalystGrade,
    AnalystDataResponse,
    SnapshotData,
    SnapshotResponse,
    MarketStatusResponse,
    STOCK_INTERVALS,
    INDEX_INTERVALS,
)
from src.server.app.bars import resolve_instrument
from src.server.services.cache.intraday_cache_service import (
    IntradayCacheService,
)
from src.server.services.cache.daily_cache_service import (
    DailyCacheService,
)
from src.data_client.normalize import series_lineage
from src.server.services.cache.quote_cache_service import QuoteCacheService
from src.data_client.freshness import measure_bars, measure_daily, measure_quote
from market_protocol import (
    InstrumentRef, display_spelling, is_family_index, to_canonical, to_legacy_api,
)
from market_protocol.enums import AssetClass

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/market-data",
    tags=["market-data"],
)


def _convert_data_points(raw_data: list) -> list[IntradayDataPoint]:
    """Convert raw OHLCV data to IntradayDataPoint models."""
    return [
        IntradayDataPoint(
            time=point.get("time", 0),
            open=point.get("open", 0.0),
            high=point.get("high", 0.0),
            low=point.get("low", 0.0),
            close=point.get("close", 0.0),
            volume=point.get("volume", 0),
        )
        for point in raw_data
    ]


def _price_ref(symbol: str, *, is_index: bool = False, equity: bool = False):
    """Canonical ref for *symbol*, or None when the spelling is unparseable.

    The legacy REST models carry currency/timezone as optional fields, so an
    unresolvable symbol degrades to nulls rather than failing the request.
    """
    hint = AssetClass.INDEX if is_index else (AssetClass.EQUITY if equity else None)
    try:
        return to_canonical(symbol, asset_class=hint)
    except Exception:
        logger.debug("market_data.ref.unresolved | symbol=%r", symbol)
        return None


def _currency_of(symbol: str, *, is_index: bool = False, equity: bool = False) -> Optional[str]:
    ref = _price_ref(symbol, is_index=is_index, equity=equity)
    return ref.price_currency if ref else None


class _SeriesMeta(NamedTuple):
    fields: dict  # currency / timezone / price_treatment on the response
    source: Optional[str]  # provider that filled the series
    tier: str  # declared tier, for a freshness measurement to defer to


def _series_meta(symbol: str, result, *, is_index: bool = False, equity: bool = False) -> _SeriesMeta:
    """What a legacy bars response says about its series, resolved once.

    Currency and timezone come from the InstrumentRef. Lineage comes from the
    cached v4 envelope header the way :mod:`src.server.app.bars` reads it,
    falling back to the publisher's declared lineage. The header's
    ``publisher`` is recorded at fetch time and carried through the cache, so
    a hit credits the same provider a cold fetch would.
    """
    ref = _price_ref(symbol, is_index=is_index, equity=equity)
    header = result.header or {}
    treatment, tier = series_lineage(header.get("publisher"), ref)
    return _SeriesMeta(
        fields={
            "currency": ref.price_currency if ref else None,
            "timezone": ref.tz if ref else None,
            "price_treatment": header.get("price_treatment") or treatment,
        },
        source=header.get("publisher") or None,
        tier=str(header.get("tier") or tier.value),
    )


def _cache_meta(result) -> CacheMetadata:
    return CacheMetadata(
        cached=result.cached,
        cache_key=result.cache_key,
        ttl_remaining=result.ttl_remaining,
        refreshed_in_background=result.background_refresh_triggered,
        watermark=result.watermark,
        complete=result.complete,
        market_phase=result.market_phase,
        truncated=result.truncated,
        revision=(result.header or {}).get("revision"),
    )


def _boundary_symbol(symbol: str, *, is_index: bool = False, equity: bool = False) -> str:
    """Protocol boundary: canonicalize an inbound spelling, then map back to
    the legacy API form.

    Every spelling of one instrument (``aapl`` / ``AAPL.US`` / ``^GSPC`` /
    ``I:SPX`` / ``SPX.INDEX``) collapses here so caches and providers see a
    single spelling. ``equity=True`` (stock OHLCV endpoints) suppresses the
    bare index-family auto-detect so a real equity ticker that collides with
    an index alias (COMP) is not silently served index data; explicit ``^``/
    ``I:`` spellings still resolve as indexes. Reverse-mapped to the legacy
    form until the Phase 3 instrument_key cache cutover. Unparseable input
    passes through untouched.
    """
    ref = _price_ref(symbol, is_index=is_index, equity=equity)
    if ref is None:
        return symbol
    return _legacy_spelling(ref, equity=equity)


def _listing_symbol(symbol: str) -> str:
    """:func:`_boundary_symbol` for the routes keyed on a listing (overview, analyst data).

    A listing's alias collapses as anywhere else (600519.SS → 600519.SH), but
    an FX pair keeps the caller's spelling: its legacy form EUR-USD resolves as
    an equity ticker once the company data reads it again. A refused spelling
    is a 422, since passed through it only fails at the upstream, as a 500.
    """
    ref = resolve_instrument(symbol, AssetClass.EQUITY)
    if ref.asset_class is AssetClass.FX:
        return symbol.upper()
    return _legacy_spelling(ref, equity=True)


def _series_symbol(symbol: str, *, is_index: bool) -> str:
    """:func:`_boundary_symbol` for a single-series OHLCV route, where a refused spelling is a 422.

    The series cache keys on the canonical instrument, so a spelling passed
    through untouched would only fail there, as a 500.
    """
    ref = resolve_instrument(symbol, AssetClass.INDEX if is_index else AssetClass.EQUITY)
    return _legacy_spelling(ref, equity=not is_index)


def _legacy_spelling(ref: InstrumentRef, *, equity: bool) -> str:
    legacy = to_legacy_api(ref)
    if equity and is_family_index(ref):
        # Explicit index spelling (^GSPC / I:SPX) on an equity endpoint: keep
        # the marker, or downstream re-canonicalization (cache keys, which
        # hint EQUITY) would flip it to an equity. Only the venue-less family
        # indexes need it; an exchange-listed index (000001.SH on XSHG)
        # carries its venue and must stay bare, or the caret reaches the
        # upstream verbatim and misses.
        return f"^{legacy}"
    return legacy


def _boundary_symbols(
    symbols: list[str], *, is_index: bool = False, equity: bool = False
) -> list[str]:
    """Boundary-map a symbol list, deduping spellings that collapse."""
    return list(
        dict.fromkeys(_boundary_symbol(s, is_index=is_index, equity=equity) for s in symbols)
    )


def _requested(symbol: str) -> str:
    """The caller's own spelling, echoed back on single-instrument responses.

    The boundary collapses aliases (``AAPL.US`` → ``AAPL``) for caches and
    providers, but a client keys its rows by what it asked for, so the response
    answers in that spelling. The one exception is the venue suffix, which is
    always ours: ``600519.SS`` answers as ``600519.SH``.
    """
    return display_spelling(symbol)


async def _get_daily(
    symbol: str, user_id: str, from_date, to_date, *, is_index: bool = False,
    echo: str | None = None,
) -> DailyResponse:
    service = DailyCacheService.get_instance()
    result = await service.get_stock_daily(
        symbol=symbol, from_date=from_date, to_date=to_date,
        is_index=is_index, user_id=user_id,
    )
    if result.error:
        raise HTTPException(status_code=500, detail=result.error)
    data_points = _convert_data_points(result.data)
    meta = _series_meta(symbol, result, is_index=is_index, equity=not is_index)
    return DailyResponse(
        symbol=echo or result.symbol, data=data_points, count=len(data_points),
        **meta.fields,
        freshness=measure_daily(symbol, result.data, is_index=is_index, source=meta.source),
        cache=_cache_meta(result),
    )


# =============================================================================
# OHLCV Endpoints
# =============================================================================


def _check_interval(interval: str, is_index: bool) -> None:
    allowed, kind = (INDEX_INTERVALS, "indexes") if is_index else (STOCK_INTERVALS, "stocks")
    if interval not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid interval '{interval}' for {kind}. Supported: {', '.join(allowed)}"
        )


async def _get_intraday(
    symbol: str, user_id: str, interval: str, from_date, to_date, *, is_index: bool,
) -> IntradayResponse:
    """One intraday series. A stock answers in the caller's spelling; an index
    in the cache's, as it always has."""
    _check_interval(interval, is_index)
    requested = _requested(symbol)
    symbol = _series_symbol(symbol, is_index=is_index)
    try:
        service = IntradayCacheService.get_instance()
        fetch = service.get_index_intraday if is_index else service.get_stock_intraday
        result = await fetch(
            symbol=symbol, interval=interval, from_date=from_date, to_date=to_date, user_id=user_id,
        )
        if result.error:
            raise HTTPException(status_code=500, detail=result.error)
        data_points = _convert_data_points(result.data)
        meta = _series_meta(symbol, result, is_index=is_index, equity=not is_index)
        return IntradayResponse(
            symbol=result.symbol if is_index else requested,
            interval=result.interval,
            data=data_points,
            count=len(data_points),
            **meta.fields,
            freshness=measure_bars(
                symbol, result.interval, result.data, is_index=is_index,
                source=meta.source, tier=meta.tier,
            ),
            cache=_cache_meta(result),
        )
    except HTTPException:
        raise
    except Exception as e:
        kind = "index" if is_index else "stock"
        logger.error(f"Error fetching {kind} intraday data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _get_batch_intraday(
    request: BatchIntradayRequest, user_id: str, *, is_index: bool,
) -> BatchIntradayResponse:
    _check_interval(request.interval, is_index)
    try:
        service = IntradayCacheService.get_instance()
        fetch = service.get_batch_indexes if is_index else service.get_batch_stocks
        results, errors, cache_stats = await fetch(
            symbols=_boundary_symbols(request.symbols, is_index=is_index, equity=not is_index),
            interval=request.interval,
            from_date=request.from_date,
            to_date=request.to_date,
            user_id=user_id,
        )
        return BatchIntradayResponse(
            interval=request.interval,
            results={symbol: _convert_data_points(data) for symbol, data in results.items()},
            errors=errors,
            cache_stats=BatchCacheStats(**cache_stats),
        )
    except HTTPException:
        raise
    except Exception as e:
        kind = "index" if is_index else "stock"
        logger.error(f"Error fetching batch {kind} intraday data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/intraday/stocks/{symbol}",
    response_model=IntradayResponse,
    summary="Get stock intraday data",
    description="Retrieve intraday OHLCV data for a single stock symbol.",
)
async def get_stock_intraday(
    symbol: str,
    user_id: CurrentUserId,
    interval: str = Query("1min", description="Data interval (1min, 5min, 15min, 30min, 1hour, 4hour)"),
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> IntradayResponse:
    """Get intraday data for a single stock."""
    return await _get_intraday(symbol, user_id, interval, from_date, to_date, is_index=False)


@router.get(
    "/intraday/indexes/{symbol}",
    response_model=IntradayResponse,
    summary="Get index intraday data",
    description="Retrieve intraday OHLCV data for a single index symbol.",
)
async def get_index_intraday(
    symbol: str,
    user_id: CurrentUserId,
    interval: str = Query("1min", description="Data interval (1min, 5min, 1hour)"),
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> IntradayResponse:
    """Get intraday data for a single index."""
    return await _get_intraday(symbol, user_id, interval, from_date, to_date, is_index=True)


@router.get(
    "/daily/stocks/{symbol}",
    response_model=DailyResponse,
    summary="Get stock daily historical data",
    description="Retrieve daily EOD OHLCV data for a single stock symbol (~500 days by default).",
)
async def get_stock_daily(
    symbol: str,
    user_id: CurrentUserId,
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> DailyResponse:
    """Get daily historical data for a single stock."""
    try:
        return await _get_daily(
            _series_symbol(symbol, is_index=False), user_id, from_date, to_date,
            echo=_requested(symbol),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily stock data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/daily/indexes/{symbol}",
    response_model=DailyResponse,
    summary="Get index daily historical data",
    description="Retrieve daily EOD OHLCV data for a single index symbol (~500 days by default).",
)
async def get_index_daily(
    symbol: str,
    user_id: CurrentUserId,
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> DailyResponse:
    """Get daily historical data for a single index."""
    try:
        return await _get_daily(
            _series_symbol(symbol, is_index=True), user_id, from_date, to_date, is_index=True,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily index data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/intraday/stocks",
    response_model=BatchIntradayResponse,
    summary="Get batch stock intraday data",
    description="Retrieve intraday OHLCV data for multiple stock symbols (max 50).",
)
async def get_batch_stocks_intraday(
    request: BatchIntradayRequest,
    user_id: CurrentUserId,
) -> BatchIntradayResponse:
    """Get intraday data for multiple stocks."""
    return await _get_batch_intraday(request, user_id, is_index=False)


@router.post(
    "/intraday/indexes",
    response_model=BatchIntradayResponse,
    summary="Get batch index intraday data",
    description="Retrieve intraday OHLCV data for multiple index symbols (max 50).",
)
async def get_batch_indexes_intraday(
    request: BatchIntradayRequest,
    user_id: CurrentUserId,
) -> BatchIntradayResponse:
    """Get intraday data for multiple indexes."""
    return await _get_batch_intraday(request, user_id, is_index=True)


# =============================================================================
# Stock Search Endpoint
# =============================================================================


@router.get(
    "/search/stocks",
    response_model=StockSearchResponse,
    summary="Search stocks by keyword",
    description="Search for stocks by symbol or company name using keywords.",
)
async def search_stocks(
    user_id: CurrentUserId,
    query: str = Query(..., description="Search query (symbol or company name)", min_length=1),
    limit: int = Query(50, description="Maximum number of results to return", ge=1, le=100),
    exchange: list[str] = Query(default=[], description="Filter by exchange short names (e.g., NASDAQ, NYSE)"),
) -> StockSearchResponse:
    """
    Search for stocks by keyword.
    
    Searches both ticker symbols and company names. Returns matching stocks
    with their symbols, names, and exchange information.
    
    Example queries:
    - "AAPL" - Find by symbol
    - "Apple" - Find by company name
    - "Micro" - Partial match
    """
    if not query or not query.strip():
        raise HTTPException(status_code=422, detail="Query parameter is required and cannot be empty")

    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_financial_data_provider
        from src.data_client.financial_data_provider import PartialSearch

        cache = get_cache_client()
        cache_key = f"search:{query.strip().lower()}:{limit}"

        cached = await cache.get(cache_key)
        if cached is not None:
            results = [StockSearchResult(**r) for r in cached["results"]]
            if exchange:
                exchange_set = {e.upper() for e in exchange}
                results = [r for r in results if r.exchangeShortName and r.exchangeShortName.upper() in exchange_set]
            return StockSearchResponse(query=query.strip(), results=results, count=len(results))

        provider = await get_financial_data_provider()
        if provider.financial is None:
            raise HTTPException(status_code=503, detail="No financial data provider available")

        complete = True
        try:
            raw_results = await provider.financial.search_stocks(query=query.strip(), limit=limit)
        except PartialSearch as partial:
            # A directory failed: answer with what the others found, uncached,
            # so the next search asks it again.
            raw_results, complete = partial.rows, False

        results = []
        for item in raw_results:
            result = StockSearchResult(
                symbol=item.get("symbol", ""),
                name=item.get("name", ""),
                nameLocal=item.get("nameLocal"),
                nameEn=item.get("nameEn"),
                currency=item.get("currency"),
                stockExchange=item.get("stockExchange"),
                exchangeShortName=item.get("exchangeShortName"),
            )
            results.append(result)

        # Cache unfiltered results
        if complete:
            await cache.set(cache_key, {"results": [r.model_dump() for r in results]}, ttl=300)

        if exchange:
            exchange_set = {e.upper() for e in exchange}
            results = [r for r in results if r.exchangeShortName and r.exchangeShortName.upper() in exchange_set]

        return StockSearchResponse(query=query.strip(), results=results, count=len(results))

    except HTTPException:
        raise
    except Exception as e:
        # The query is user content and an upstream error's text carries the
        # request URL, so neither reaches the log or the response.
        logger.error("Error searching stocks: %s", type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to search stocks")


# =============================================================================
# Company Overview Endpoint
# =============================================================================


@router.get(
    "/stocks/{symbol}/overview",
    response_model=CompanyOverviewResponse,
    summary="Get company overview",
    description="Retrieve comprehensive company overview data including quote, performance, analyst ratings, financials, and revenue breakdown.",
)
async def get_company_overview(symbol: str, user_id: CurrentUserId) -> CompanyOverviewResponse:
    """Get company overview data for a stock symbol."""
    if not symbol or not symbol.strip():
        raise HTTPException(status_code=422, detail="Symbol is required")

    # Same boundary as the OHLCV and snapshot routes: an alias spelling
    # (600519.SS) must reach the cache and the upstream as the one spelling
    # they know, or the quote half of the overview comes back empty.
    symbol_upper = _listing_symbol(symbol.strip())
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        cache_key = f"overview:{symbol_upper}"

        cached = await cache.get(cache_key)
        if cached is not None:
            # Keyed on the instrument, so the hit may be another spelling's.
            return CompanyOverviewResponse(**{
                **cached,
                "symbol": _requested(symbol),
                "quote": _remeasured(cached.get("quote"), symbol_upper),
            })

        from src.tools.market_data.company import fetch_company_overview_data

        artifact = await fetch_company_overview_data(symbol_upper)

        response = CompanyOverviewResponse(
            symbol=_requested(symbol),
            name=artifact.get("name"),
            nameEn=artifact.get("nameEn"),
            currency=artifact.get("currency") or _currency_of(symbol_upper, equity=True),
            reportedCurrency=artifact.get("reportedCurrency"),
            assetClass=artifact.get("assetClass"),
            quote=artifact.get("quote"),
            performance=artifact.get("performance"),
            analystRatings=artifact.get("analystRatings"),
            quarterlyFundamentals=artifact.get("quarterlyFundamentals"),
            earningsSurprises=artifact.get("earningsSurprises"),
            cashFlow=artifact.get("cashFlow"),
            revenueByProduct=artifact.get("revenueByProduct"),
            revenueByGeo=artifact.get("revenueByGeo"),
        )
        await cache.set(cache_key, response.model_dump(), ttl=300)
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching company overview for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch company overview: {str(e)}")


# =============================================================================
# Analyst Data Endpoint
# =============================================================================


def _remeasured(quote: Optional[dict], symbol: str) -> Optional[dict]:
    """A cached overview quote with its freshness measured now.

    The overview caches for minutes, and a label measured at write time would
    keep calling an aging print live; the print time, tier and source it was
    measured from ride the cached row.
    """
    fresh = (quote or {}).get("freshness")
    if not isinstance(fresh, dict):
        return quote
    return {**quote, "freshness": measure_quote(
        symbol, fresh.get("actual_latest"), quote.get("tier"),
        regular_only=bool(quote.get("regular_only")), source=fresh.get("source"),
    ).model_dump(mode="json")}


@router.get(
    "/stocks/{symbol}/analyst-data",
    response_model=AnalystDataResponse,
    summary="Get analyst price targets and grades",
    description="Retrieve analyst price target consensus and recent stock grade changes.",
)
async def get_analyst_data(
    symbol: str,
    user_id: CurrentUserId,
    grade_limit: int = Query(50, description="Maximum number of grade records to return", ge=1, le=200),
) -> AnalystDataResponse:
    """Get analyst data for a stock symbol."""
    if not symbol or not symbol.strip():
        raise HTTPException(status_code=422, detail="Symbol is required")

    # Keyed and fetched on the instrument, answered in the caller's spelling.
    symbol_upper = _listing_symbol(symbol.strip())

    try:
        import asyncio
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_financial_data_provider

        cache = get_cache_client()
        cache_key = f"analyst:{symbol_upper}"

        cached = await cache.get(cache_key)
        if cached is not None:
            return AnalystDataResponse(**{**cached, "symbol": _requested(symbol)})

        provider = await get_financial_data_provider()
        if provider.financial is None:
            raise HTTPException(status_code=503, detail="No financial data provider available")

        # Price targets: via provider (works for FMP and yfinance)
        # Grades: FMP-only (per-analyst records); gracefully empty otherwise
        async def _fetch_grades() -> list:
            try:
                from src.data_client.fmp.fmp_client import FMPClient
                fmp_client = FMPClient()
                try:
                    return await fmp_client.get_stock_grades(symbol_upper, limit=grade_limit)
                finally:
                    await fmp_client.close()
            except Exception:
                logger.warning("Failed to fetch grades for %s", symbol_upper, exc_info=True)
                return []

        price_targets_raw, grades_raw = await asyncio.gather(
            provider.financial.get_analyst_price_targets(symbol_upper),
            _fetch_grades(),
            return_exceptions=True,
        )

        price_targets = None
        if isinstance(price_targets_raw, list) and len(price_targets_raw) > 0:
            pt = price_targets_raw[0]
            price_targets = PriceTargetSummary(
                targetHigh=pt.get("targetHigh"),
                targetLow=pt.get("targetLow"),
                targetConsensus=pt.get("targetConsensus"),
                targetMedian=pt.get("targetMedian"),
            )
        elif isinstance(price_targets_raw, Exception):
            logger.warning(f"Failed to fetch price targets for {symbol_upper}: {price_targets_raw}")

        grades = []
        if isinstance(grades_raw, list):
            for g in grades_raw:
                grades.append(AnalystGrade(
                    date=g.get("date", ""),
                    company=g.get("gradingCompany", ""),
                    previousGrade=g.get("previousGrade"),
                    newGrade=g.get("newGrade"),
                    action=g.get("action"),
                ))

        response = AnalystDataResponse(
            symbol=_requested(symbol),
            priceTargets=price_targets,
            grades=grades,
        )
        await cache.set(cache_key, response.model_dump(), ttl=900)
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching analyst data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch analyst data: {str(e)}")


# =============================================================================
# Snapshot Endpoints
# =============================================================================

_MARKET_STATUS_CACHE_TTL = 30  # seconds
_MARKET_STATUS_MARKETS = {"us", "cn", "hk"}

# Enforced batch width — matches the documented "max 250" in the query docs;
# bounds the Redis MGET width and upstream fan-out per request.
_MAX_BATCH_SNAPSHOT_SYMBOLS = 250


@router.get(
    "/snapshots/stocks",
    response_model=SnapshotResponse,
    summary="Get batch stock snapshots",
    description="Retrieve real-time snapshot data for multiple stock symbols.",
)
async def get_stock_snapshots(
    user_id: CurrentUserId,
    symbols: str = Query(..., description="Comma-separated stock symbols (max 250)"),
) -> SnapshotResponse:
    """Get batch snapshots for stocks."""
    return await _get_batch_snapshots(symbols, "stocks", user_id)


@router.get(
    "/snapshots/indexes",
    response_model=SnapshotResponse,
    summary="Get batch index snapshots",
    description="Retrieve real-time snapshot data for multiple index symbols.",
)
async def get_index_snapshots(
    user_id: CurrentUserId,
    symbols: str = Query(..., description="Comma-separated index symbols (e.g. GSPC,IXIC,DJI)"),
) -> SnapshotResponse:
    """Get batch snapshots for indexes."""
    return await _get_batch_snapshots(symbols, "indices", user_id)


def _snapshot(
    ref: InstrumentRef, row: dict, *, symbol: str,
    requested: Optional[list[str]] = None,
) -> SnapshotData:
    """A cached quote row, stated about the instrument it was cached under.

    Asset class comes from *ref*, never from the vendor's echo, which spells
    ``^GSPC`` as ``GSPC`` and would read as an equity. Currency is the vendor's
    when it states one, since it knows the counter it quoted (an RMB counter
    on a Hong Kong listing, a London price in pence), and the listing's
    otherwise. Freshness is measured at response time, never in the cache: a
    row's label would otherwise freeze at write time and keep claiming to be
    fresh.
    """
    return SnapshotData(**{
        **row,
        "currency": row.get("currency") or ref.price_currency,
        "asset_class": ref.asset_class.value,
        "symbol": symbol,
        "requested": requested,
        "freshness": measure_quote(
            to_legacy_api(ref), row.get("as_of"), row.get("tier"),
            is_index=ref.asset_class is AssetClass.INDEX,
            regular_only=bool(row.get("regular_only")), source=row.get("source"),
        ),
    })


async def _get_batch_snapshots(
    symbols: str, asset_type: str, user_id: str,
) -> SnapshotResponse:
    """Shared implementation for batch stock/index snapshot endpoints.

    Thin wrapper over QuoteCacheService: per-instrument cache keys, one
    batched upstream fill for misses, in-flight dedup. Unresolvable symbols
    are dropped from the response (no null-field rows).
    """
    is_index = asset_type == "indices"
    # Every requested spelling of a collapsed instrument is answered: a client
    # keyed on its input (``AAPL`` in one widget, ``AAPL.US`` in another) must
    # find both, while the upstream fetch happens once.
    instruments: dict[str, tuple[InstrumentRef, list[str]]] = {}
    for spelling in (s.strip().upper() for s in symbols.split(",") if s.strip()):
        ref = _price_ref(spelling, is_index=is_index, equity=not is_index)
        if ref is None:
            continue
        spellings = instruments.setdefault(ref.instrument_key, (ref, []))[1]
        if spelling not in spellings:
            spellings.append(spelling)
    if not instruments:
        raise HTTPException(status_code=422, detail="At least one symbol is required")
    if len(instruments) > _MAX_BATCH_SNAPSHOT_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"Too many symbols ({len(instruments)}); max {_MAX_BATCH_SNAPSHOT_SYMBOLS} per request",
        )

    try:
        quotes = await QuoteCacheService.get_instance().get_quotes(
            [ref for ref, _ in instruments.values()], user_id=user_id,
            daily_fallback=not is_index,
        )
        snapshots = []
        for ref, row in quotes:
            # One row per display spelling. ``600519.SS`` and ``600519.SH`` are
            # one row spelled ``.SH``, and ``requested`` names the caller's own
            # spellings it answers so a client keyed on its input still finds it.
            # An index answers in its legacy spelling, as it always has.
            shown: dict[str, list[str]] = {}
            for spelling in instruments[ref.instrument_key][1]:
                label = to_legacy_api(ref) if is_index else _requested(spelling)
                shown.setdefault(label, []).append(spelling)
            snapshots.extend(
                _snapshot(ref, row, symbol=symbol, requested=answers)
                for symbol, answers in shown.items()
            )
        return SnapshotResponse(snapshots=snapshots, count=len(snapshots))

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching %s snapshots: %s", asset_type, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/snapshots/stocks/{symbol}",
    response_model=SnapshotData,
    summary="Get single stock snapshot",
    description="Retrieve real-time snapshot data for a single stock symbol.",
)
async def get_single_stock_snapshot(symbol: str, user_id: CurrentUserId) -> SnapshotData:
    """Get snapshot for a single stock — same cache path as the batch endpoint."""
    requested = _requested(symbol)
    ref = _price_ref(requested, equity=True)

    try:
        service = QuoteCacheService.get_instance()
        quotes = await service.get_quotes([ref], user_id=user_id, daily_fallback=True) if ref else []
        if not quotes:
            raise HTTPException(status_code=404, detail="No snapshot data available for this symbol")
        return _snapshot(ref, quotes[0][1], symbol=requested)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching snapshot for {requested}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Market Status Endpoint
# =============================================================================


@router.get(
    "/status",
    response_model=MarketStatusResponse,
    summary="Get current market status (alias)",
    description="Alias for /market-status for backward compatibility.",
)
async def get_market_status_alias(
    user_id: CurrentUserId,
    market: str = Query("us", description="Market region: us, cn or hk"),
) -> MarketStatusResponse:
    """Alias for get_market_status."""
    return await get_market_status(user_id, market)


@router.get(
    "/market-status",
    response_model=MarketStatusResponse,
    summary="Get current market status",
    description="Retrieve the current market status (open, closed, extended hours).",
)
async def get_market_status(
    user_id: CurrentUserId,
    market: str = Query("us", description="Market region: us, cn or hk"),
) -> MarketStatusResponse:
    """Get current market status for *market*."""
    market = (market or "us").strip().lower()
    if market not in _MARKET_STATUS_MARKETS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported market '{market}'. Expected one of: "
            + ", ".join(sorted(_MARKET_STATUS_MARKETS)),
        )
    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_market_data_provider

        cache = get_cache_client()
        # Scoped per market — one clock per exchange region, never shared.
        cache_key = f"market:status:{market}"

        cached = await cache.get(cache_key)
        if cached is not None:
            return MarketStatusResponse(**cached)

        provider = await get_market_data_provider()
        raw = await provider.get_market_status(user_id=user_id, market=market)

        response = MarketStatusResponse(
            market=raw.get("market"),
            afterHours=raw.get("afterHours"),
            earlyHours=raw.get("earlyHours"),
            serverTime=raw.get("serverTime"),
            exchanges=raw.get("exchanges"),
            providers=provider.source_names_for_market(market),
        )
        await cache.set(cache_key, response.model_dump(), ttl=_MARKET_STATUS_CACHE_TTL)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching market status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
