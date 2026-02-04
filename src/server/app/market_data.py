"""
FastAPI router for market data proxy endpoints.

Provides cached access to FMP intraday data for stocks and indexes.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.server.models.market_data import (
    IntradayDataPoint,
    IntradayResponse,
    BatchIntradayRequest,
    BatchIntradayResponse,
    CacheMetadata,
    BatchCacheStats,
    StockSearchResult,
    StockSearchResponse,
    STOCK_INTERVALS,
    INDEX_INTERVALS,
)
from src.server.services.intraday_cache_service import (
    IntradayCacheService,
    IntradayCacheKeyBuilder,
)
from src.data_client.fmp.fmp_client import FMPClient

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/market-data",
    tags=["market-data"],
)


def _convert_data_points(raw_data: list) -> list[IntradayDataPoint]:
    """Convert raw FMP data to IntradayDataPoint models."""
    return [
        IntradayDataPoint(
            date=point.get("date", ""),
            open=point.get("open", 0.0),
            high=point.get("high", 0.0),
            low=point.get("low", 0.0),
            close=point.get("close", 0.0),
            volume=point.get("volume", 0),
        )
        for point in raw_data
    ]


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
        )

        if result.error:
            raise HTTPException(status_code=500, detail=result.error)

        cache_key = IntradayCacheKeyBuilder.stock_key(symbol, interval, from_date, to_date)
        data_points = _convert_data_points(result.data)

        return IntradayResponse(
            symbol=result.symbol,
            interval=result.interval,
            data=data_points,
            count=len(data_points),
            cache=CacheMetadata(
                cached=result.cached,
                cache_key=cache_key,
                ttl_remaining=result.ttl_remaining,
                refreshed_in_background=result.background_refresh_triggered,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching stock intraday data for {symbol}: {e}")
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
) -> BatchIntradayResponse:
    """Get intraday data for multiple stocks."""
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
        )

        if result.error:
            raise HTTPException(status_code=500, detail=result.error)

        cache_key = IntradayCacheKeyBuilder.index_key(symbol, interval, from_date, to_date)
        data_points = _convert_data_points(result.data)

        return IntradayResponse(
            symbol=result.symbol,
            interval=result.interval,
            data=data_points,
            count=len(data_points),
            cache=CacheMetadata(
                cached=result.cached,
                cache_key=cache_key,
                ttl_remaining=result.ttl_remaining,
                refreshed_in_background=result.background_refresh_triggered,
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
) -> BatchIntradayResponse:
    """Get intraday data for multiple indexes."""
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
        # Create FMP client instance
        fmp_client = FMPClient()
        
        try:
            # Call FMP API search endpoint
            raw_results = await fmp_client.search_stocks(query=query.strip(), limit=limit)
            
            # Convert raw results to Pydantic models
            results = []
            for item in raw_results:
                # Handle different response formats from FMP API
                result = StockSearchResult(
                    symbol=item.get("symbol", ""),
                    name=item.get("name", ""),
                    currency=item.get("currency"),
                    stockExchange=item.get("stockExchange"),
                    exchangeShortName=item.get("exchangeShortName"),
                )
                results.append(result)

            # Filter by exchange if specified
            if exchange:
                exchange_set = {e.upper() for e in exchange}
                results = [
                    r for r in results
                    if r.exchangeShortName and r.exchangeShortName.upper() in exchange_set
                ]

            return StockSearchResponse(
                query=query.strip(),
                results=results,
                count=len(results),
            )
            
        finally:
            # Always close the client
            await fmp_client.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching stocks for query '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search stocks: {str(e)}")
