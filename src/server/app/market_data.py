"""
FastAPI router for market data proxy endpoints.

Provides cached access to FMP intraday data for stocks and indexes.
"""

import logging
from typing import Optional

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
from src.server.services.cache.intraday_cache_service import (
    IntradayCacheService,
)
from src.server.services.cache.daily_cache_service import (
    DailyCacheService,
)
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


async def _get_daily(
    symbol: str, user_id: str, from_date, to_date, *, is_index: bool = False,
) -> DailyResponse:
    service = DailyCacheService.get_instance()
    result = await service.get_stock_daily(
        symbol=symbol, from_date=from_date, to_date=to_date,
        is_index=is_index, user_id=user_id,
    )
    if result.error:
        raise HTTPException(status_code=500, detail=result.error)
    data_points = _convert_data_points(result.data)
    return DailyResponse(
        symbol=result.symbol, data=data_points, count=len(data_points),
        cache=CacheMetadata(
            cached=result.cached, cache_key=result.cache_key,
            ttl_remaining=result.ttl_remaining,
            refreshed_in_background=result.background_refresh_triggered,
            watermark=result.watermark, complete=result.complete,
            market_phase=result.market_phase,
            truncated=result.truncated,
        ),
    )


# =============================================================================
# Single Stock Endpoints
# =============================================================================


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
    # Validate interval
    if interval not in STOCK_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid interval '{interval}' for stocks. Supported: {', '.join(STOCK_INTERVALS)}"
        )

    try:
        service = IntradayCacheService.get_instance()
        result = await service.get_stock_intraday(
            symbol=symbol,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            user_id=user_id,
        )

        if result.error:
            raise HTTPException(status_code=500, detail=result.error)

        data_points = _convert_data_points(result.data)

        return IntradayResponse(
            symbol=result.symbol,
            interval=result.interval,
            data=data_points,
            count=len(data_points),
            cache=CacheMetadata(
                cached=result.cached,
                cache_key=result.cache_key,
                ttl_remaining=result.ttl_remaining,
                refreshed_in_background=result.background_refresh_triggered,
                watermark=result.watermark,
                complete=result.complete,
                market_phase=result.market_phase,
                truncated=result.truncated,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching stock intraday data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Daily Stock Endpoints
# =============================================================================


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
        return await _get_daily(symbol, user_id, from_date, to_date)
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
        return await _get_daily(symbol, user_id, from_date, to_date, is_index=True)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily index data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Batch Stock Endpoints
# =============================================================================


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
    if request.interval == "1s":
        raise HTTPException(
            status_code=422,
            detail="1s interval is not supported for batch requests. Use the single-symbol endpoint instead.",
        )

    # Validate interval
    if request.interval not in STOCK_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid interval '{request.interval}' for stocks. Supported: {', '.join(STOCK_INTERVALS)}"
        )

    try:
        service = IntradayCacheService.get_instance()
        results, errors, cache_stats = await service.get_batch_stocks(
            symbols=request.symbols,
            interval=request.interval,
            from_date=request.from_date,
            to_date=request.to_date,
            user_id=user_id,
        )

        # Convert raw data to IntradayDataPoint models
        converted_results = {
            symbol: _convert_data_points(data)
            for symbol, data in results.items()
        }

        return BatchIntradayResponse(
            interval=request.interval,
            results=converted_results,
            errors=errors,
            cache_stats=BatchCacheStats(**cache_stats),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching batch stock intraday data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Single Index Endpoints
# =============================================================================


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
    # Validate interval
    if interval not in INDEX_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid interval '{interval}' for indexes. Supported: {', '.join(INDEX_INTERVALS)}"
        )

    try:
        service = IntradayCacheService.get_instance()
        result = await service.get_index_intraday(
            symbol=symbol,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            user_id=user_id,
        )

        if result.error:
            raise HTTPException(status_code=500, detail=result.error)

        data_points = _convert_data_points(result.data)

        return IntradayResponse(
            symbol=result.symbol,
            interval=result.interval,
            data=data_points,
            count=len(data_points),
            cache=CacheMetadata(
                cached=result.cached,
                cache_key=result.cache_key,
                ttl_remaining=result.ttl_remaining,
                refreshed_in_background=result.background_refresh_triggered,
                watermark=result.watermark,
                complete=result.complete,
                market_phase=result.market_phase,
                truncated=result.truncated,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching index intraday data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Batch Index Endpoints
# =============================================================================


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
    if request.interval == "1s":
        raise HTTPException(
            status_code=422,
            detail="1s interval is not supported for batch requests. Use the single-symbol endpoint instead.",
        )

    # Validate interval
    if request.interval not in INDEX_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid interval '{request.interval}' for indexes. Supported: {', '.join(INDEX_INTERVALS)}"
        )

    try:
        service = IntradayCacheService.get_instance()
        results, errors, cache_stats = await service.get_batch_indexes(
            symbols=request.symbols,
            interval=request.interval,
            from_date=request.from_date,
            to_date=request.to_date,
            user_id=user_id,
        )

        # Convert raw data to IntradayDataPoint models
        converted_results = {
            symbol: _convert_data_points(data)
            for symbol, data in results.items()
        }

        return BatchIntradayResponse(
            interval=request.interval,
            results=converted_results,
            errors=errors,
            cache_stats=BatchCacheStats(**cache_stats),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching batch index intraday data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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

        raw_results = await provider.financial.search_stocks(query=query.strip(), limit=limit)

        results = []
        for item in raw_results:
            result = StockSearchResult(
                symbol=item.get("symbol", ""),
                name=item.get("name", ""),
                currency=item.get("currency"),
                stockExchange=item.get("stockExchange"),
                exchangeShortName=item.get("exchangeShortName"),
            )
            results.append(result)

        # Cache unfiltered results
        await cache.set(cache_key, {"results": [r.model_dump() for r in results]}, ttl=300)

        if exchange:
            exchange_set = {e.upper() for e in exchange}
            results = [r for r in results if r.exchangeShortName and r.exchangeShortName.upper() in exchange_set]

        return StockSearchResponse(query=query.strip(), results=results, count=len(results))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching stocks for query '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search stocks: {str(e)}")


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

    symbol_upper = symbol.strip().upper()
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        cache_key = f"overview:{symbol_upper}"

        cached = await cache.get(cache_key)
        if cached is not None:
            return CompanyOverviewResponse(**cached)

        from src.tools.market_data.implementations import fetch_company_overview_data

        artifact = await fetch_company_overview_data(symbol_upper)

        response = CompanyOverviewResponse(
            symbol=artifact.get("symbol", symbol),
            name=artifact.get("name"),
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

    symbol_upper = symbol.strip().upper()

    try:
        import asyncio
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_financial_data_provider

        cache = get_cache_client()
        cache_key = f"analyst:{symbol_upper}"

        cached = await cache.get(cache_key)
        if cached is not None:
            return AnalystDataResponse(**cached)

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
            symbol=symbol_upper,
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

_SNAPSHOT_CACHE_TTL = 15  # seconds
_MARKET_STATUS_CACHE_TTL = 30  # seconds


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
    return await _get_batch_snapshots(symbols, "stocks", "stocks", user_id)


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
    return await _get_batch_snapshots(symbols, "indices", "indexes", user_id)


async def _get_batch_snapshots(
    symbols: str, asset_type: str, cache_prefix: str, user_id: str,
) -> SnapshotResponse:
    """Shared implementation for batch stock/index snapshot endpoints."""
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=422, detail="At least one symbol is required")

    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_market_data_provider

        cache = get_cache_client()
        cache_key = f"snapshot:{cache_prefix}:{','.join(sorted(symbol_list))}"

        cached = await cache.get(cache_key)
        if cached is not None:
            snapshots = [SnapshotData(**s) for s in cached]
            return SnapshotResponse(snapshots=snapshots, count=len(snapshots))

        provider = await get_market_data_provider()
        raw = await provider.get_snapshots(symbol_list, asset_type=asset_type, user_id=user_id)

        snapshots = [SnapshotData(**item) for item in raw]
        await cache.set(cache_key, [s.model_dump() for s in snapshots], ttl=_SNAPSHOT_CACHE_TTL)

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
    """Get snapshot for a single stock."""
    symbol = symbol.strip().upper()

    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_market_data_provider

        cache = get_cache_client()
        cache_key = f"snapshot:stock:{symbol}"

        cached = await cache.get(cache_key)
        if cached is not None:
            return SnapshotData(**cached)

        provider = await get_market_data_provider()
        raw = await provider.get_snapshots([symbol], asset_type="stocks", user_id=user_id)

        if not raw:
            raise HTTPException(status_code=404, detail="No snapshot data available for this symbol")
        snap = SnapshotData(**raw[0])
        await cache.set(cache_key, snap.model_dump(), ttl=_SNAPSHOT_CACHE_TTL)

        return snap

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching snapshot for {symbol}: {e}")
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
async def get_market_status_alias(user_id: CurrentUserId) -> MarketStatusResponse:
    """Alias for get_market_status."""
    return await get_market_status(user_id)


@router.get(
    "/market-status",
    response_model=MarketStatusResponse,
    summary="Get current market status",
    description="Retrieve the current market status (open, closed, extended hours).",
)
async def get_market_status(user_id: CurrentUserId) -> MarketStatusResponse:
    """Get current market status."""
    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.data_client import get_market_data_provider

        cache = get_cache_client()
        cache_key = "market:status"

        cached = await cache.get(cache_key)
        if cached is not None:
            return MarketStatusResponse(**cached)

        provider = await get_market_data_provider()
        raw = await provider.get_market_status(user_id=user_id)

        response = MarketStatusResponse(
            market=raw.get("market"),
            afterHours=raw.get("afterHours"),
            earlyHours=raw.get("earlyHours"),
            serverTime=raw.get("serverTime"),
            exchanges=raw.get("exchanges"),
            providers=provider.source_names,
        )
        await cache.set(cache_key, response.model_dump(), ttl=_MARKET_STATUS_CACHE_TTL)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching market status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
