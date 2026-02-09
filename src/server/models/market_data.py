"""
Pydantic models for market data endpoints.

This module provides request and response models for FMP intraday data proxy endpoints.
"""

from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field


# Supported intervals for intraday data
STOCK_INTERVALS = ("1min", "5min", "15min", "30min", "1hour", "4hour")
INDEX_INTERVALS = ("1min", "5min", "1hour")

StockInterval = Literal["1min", "5min", "15min", "30min", "1hour", "4hour"]
IndexInterval = Literal["1min", "5min", "1hour"]


class IntradayDataPoint(BaseModel):
    """Single OHLCV data point for intraday chart data."""
    date: str = Field(..., description="Timestamp in ISO format (YYYY-MM-DD HH:MM:SS)")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Closing price")
    volume: int = Field(..., description="Trading volume")


class CacheMetadata(BaseModel):
    """Cache metadata for responses."""
    cached: bool = Field(..., description="Whether data was served from cache")
    cache_key: Optional[str] = Field(None, description="Cache key used")
    ttl_remaining: Optional[int] = Field(None, description="Remaining TTL in seconds")
    refreshed_in_background: bool = Field(False, description="Whether a background refresh was triggered")


class IntradayResponse(BaseModel):
    """Response for single symbol intraday data request."""
    symbol: str = Field(..., description="Stock/index symbol")
    interval: str = Field(..., description="Data interval (e.g., 1min, 5min, 1hour)")
    data: List[IntradayDataPoint] = Field(default_factory=list, description="Intraday OHLCV data points")
    count: int = Field(0, description="Number of data points returned")
    cache: CacheMetadata = Field(..., description="Cache metadata")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "interval": "1min",
                "data": [
                    {
                        "date": "2024-01-15 09:30:00",
                        "open": 185.50,
                        "high": 185.75,
                        "low": 185.25,
                        "close": 185.60,
                        "volume": 1500000
                    }
                ],
                "count": 1,
                "cache": {
                    "cached": True,
                    "cache_key": "fmp:intraday:stock:symbol=AAPL:interval=1min",
                    "ttl_remaining": 45,
                    "refreshed_in_background": False
                }
            }
        }


class DailyResponse(BaseModel):
    """Response for single symbol daily historical data request."""
    symbol: str = Field(..., description="Stock symbol")
    data: List[IntradayDataPoint] = Field(default_factory=list, description="Daily OHLCV data points")
    count: int = Field(0, description="Number of data points returned")
    cache: CacheMetadata = Field(..., description="Cache metadata")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "data": [
                    {
                        "date": "2024-01-15",
                        "open": 185.50,
                        "high": 187.00,
                        "low": 184.25,
                        "close": 186.60,
                        "volume": 55000000
                    }
                ],
                "count": 1,
                "cache": {
                    "cached": True,
                    "cache_key": "fmp:daily:stock:symbol=AAPL",
                    "ttl_remaining": 3200,
                    "refreshed_in_background": False
                }
            }
        }


class BatchIntradayRequest(BaseModel):
    """Request for batch intraday data."""
    symbols: List[str] = Field(
        ...,
        description="List of stock/index symbols (max 50)",
        min_length=1,
        max_length=50
    )
    interval: str = Field(
        "1min",
        description="Data interval (1min, 5min, 15min, 30min, 1hour, 4hour for stocks; 1min, 5min, 1hour for indexes)"
    )
    from_date: Optional[str] = Field(
        None,
        alias="from",
        description="Start date (YYYY-MM-DD format)"
    )
    to_date: Optional[str] = Field(
        None,
        alias="to",
        description="End date (YYYY-MM-DD format)"
    )

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "symbols": ["AAPL", "MSFT", "GOOGL"],
                "interval": "15min",
                "from": "2024-01-01",
                "to": "2024-01-15"
            }
        }


class BatchCacheStats(BaseModel):
    """Cache statistics for batch requests."""
    total_requests: int = Field(..., description="Total number of symbols requested")
    cache_hits: int = Field(0, description="Number of symbols served from cache")
    cache_misses: int = Field(0, description="Number of symbols fetched from API")
    background_refreshes: int = Field(0, description="Number of background refreshes triggered")


class BatchIntradayResponse(BaseModel):
    """Response for batch intraday data request."""
    interval: str = Field(..., description="Data interval used for the request")
    results: Dict[str, List[IntradayDataPoint]] = Field(
        default_factory=dict,
        description="Map of symbol to intraday data points"
    )
    errors: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of symbol to error message for failed requests"
    )
    cache_stats: BatchCacheStats = Field(..., description="Aggregated cache statistics")

    class Config:
        json_schema_extra = {
            "example": {
                "interval": "15min",
                "results": {
                    "AAPL": [
                        {
                            "date": "2024-01-15 09:30:00",
                            "open": 185.50,
                            "high": 185.75,
                            "low": 185.25,
                            "close": 185.60,
                            "volume": 1500000
                        }
                    ],
                    "MSFT": [
                        {
                            "date": "2024-01-15 09:30:00",
                            "open": 375.00,
                            "high": 375.50,
                            "low": 374.80,
                            "close": 375.25,
                            "volume": 800000
                        }
                    ]
                },
                "errors": {
                    "INVALID": "Symbol not found"
                },
                "cache_stats": {
                    "total_requests": 3,
                    "cache_hits": 2,
                    "cache_misses": 1,
                    "background_refreshes": 1
                }
            }
        }


class CompanyOverviewResponse(BaseModel):
    """Response for company overview endpoint."""
    symbol: str = Field(..., description="Stock ticker symbol")
    name: Optional[str] = Field(None, description="Company name")
    quote: Optional[Dict[str, Any]] = Field(None, description="Real-time quote data")
    performance: Optional[Dict[str, Any]] = Field(None, description="Price performance by period")
    analystRatings: Optional[Dict[str, Any]] = Field(None, description="Analyst rating distribution")
    quarterlyFundamentals: Optional[List[Dict[str, Any]]] = Field(None, description="Quarterly revenue/income data")
    earningsSurprises: Optional[List[Dict[str, Any]]] = Field(None, description="EPS actual vs estimate")
    cashFlow: Optional[List[Dict[str, Any]]] = Field(None, description="Quarterly cash flow data")
    revenueByProduct: Optional[Dict[str, Any]] = Field(None, description="Revenue breakdown by product")
    revenueByGeo: Optional[Dict[str, Any]] = Field(None, description="Revenue breakdown by geography")


class PriceTargetSummary(BaseModel):
    """Price target summary from analyst consensus."""
    targetHigh: Optional[float] = Field(None, description="Highest price target")
    targetLow: Optional[float] = Field(None, description="Lowest price target")
    targetConsensus: Optional[float] = Field(None, description="Consensus (average) price target")
    targetMedian: Optional[float] = Field(None, description="Median price target")


class AnalystGrade(BaseModel):
    """Single analyst grade change record."""
    date: str = Field(..., description="Date of grade change (YYYY-MM-DD)")
    company: str = Field("", description="Grading company/institution name")
    previousGrade: Optional[str] = Field(None, description="Previous grade")
    newGrade: Optional[str] = Field(None, description="New grade")
    action: Optional[str] = Field(None, description="Action (upgrade, downgrade, maintained, etc.)")


class AnalystDataResponse(BaseModel):
    """Response for analyst data endpoint."""
    symbol: str = Field(..., description="Stock ticker symbol")
    priceTargets: Optional[PriceTargetSummary] = Field(None, description="Price target summary")
    grades: List[AnalystGrade] = Field(default_factory=list, description="Recent analyst grade changes")


class StockSearchResult(BaseModel):
    """Single stock search result."""
    symbol: str = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    name: str = Field(..., description="Company name")
    currency: Optional[str] = Field(None, description="Currency code")
    stockExchange: Optional[str] = Field(None, description="Stock exchange name")
    exchangeShortName: Optional[str] = Field(None, description="Short exchange name")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "currency": "USD",
                "stockExchange": "NASDAQ Global Select",
                "exchangeShortName": "NASDAQ"
            }
        }


class StockSearchResponse(BaseModel):
    """Response for stock search request."""
    query: str = Field(..., description="Search query used")
    results: List[StockSearchResult] = Field(default_factory=list, description="List of matching stocks")
    count: int = Field(0, description="Number of results returned")

    class Config:
        json_schema_extra = {
            "example": {
                "query": "Apple",
                "results": [
                    {
                        "symbol": "AAPL",
                        "name": "Apple Inc.",
                        "currency": "USD",
                        "stockExchange": "NASDAQ Global Select",
                        "exchangeShortName": "NASDAQ"
                    }
                ],
                "count": 1
            }
        }
