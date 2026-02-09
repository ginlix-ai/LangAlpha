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
    STOCK_INTERVALS,
    INDEX_INTERVALS,
)
from src.server.services.intraday_cache_service import (
    IntradayCacheService,
    IntradayCacheKeyBuilder,
)
from src.server.services.daily_cache_service import (
    DailyCacheService,
    DailyCacheKeyBuilder,
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
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> DailyResponse:
    """Get daily historical data for a single stock."""
    try:
        service = DailyCacheService.get_instance()
        result = await service.get_stock_daily(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
        )

        if result.error:
            raise HTTPException(status_code=500, detail=result.error)

        cache_key = DailyCacheKeyBuilder.stock_key(symbol, from_date, to_date)
        data_points = _convert_data_points(result.data)

        return DailyResponse(
            symbol=result.symbol,
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
        logger.error(f"Error fetching daily stock data for {symbol}: {e}")
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


# =============================================================================
# Company Overview Endpoint
# =============================================================================


@router.get(
    "/stocks/{symbol}/overview",
    response_model=CompanyOverviewResponse,
    summary="Get company overview",
    description="Retrieve comprehensive company overview data including quote, performance, analyst ratings, financials, and revenue breakdown.",
)
async def get_company_overview(symbol: str) -> CompanyOverviewResponse:
    """Get company overview data for a stock symbol."""
    if not symbol or not symbol.strip():
        raise HTTPException(status_code=422, detail="Symbol is required")

    try:
        from src.tools.market_data.implementations import fetch_company_overview_data

        artifact = await fetch_company_overview_data(symbol.strip().upper())

        return CompanyOverviewResponse(
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
    grade_limit: int = Query(50, description="Maximum number of grade records to return", ge=1, le=200),
) -> AnalystDataResponse:
    """Get analyst data for a stock symbol."""
    if not symbol or not symbol.strip():
        raise HTTPException(status_code=422, detail="Symbol is required")

    symbol_upper = symbol.strip().upper()

    try:
        fmp_client = FMPClient()

        try:
            # Fetch price targets and grades in parallel
            import asyncio
            price_targets_raw, grades_raw = await asyncio.gather(
                fmp_client.get_price_target_summary(symbol_upper),
                fmp_client.get_stock_grades(symbol_upper, limit=grade_limit),
                return_exceptions=True,
            )

            # Process price targets
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

            # Process grades
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
            elif isinstance(grades_raw, Exception):
                logger.warning(f"Failed to fetch grades for {symbol_upper}: {grades_raw}")

            return AnalystDataResponse(
                symbol=symbol_upper,
                priceTargets=price_targets,
                grades=grades,
            )

        finally:
            await fmp_client.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching analyst data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch analyst data: {str(e)}")
