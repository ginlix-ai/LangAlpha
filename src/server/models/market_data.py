"""
Pydantic models for market data endpoints.

This module provides request and response models for FMP intraday data proxy endpoints.
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict, Field, field_validator


# Supported intervals for intraday data
SUPPORTED_INTERVALS = ("1s", "1min", "5min", "15min", "30min", "1hour", "4hour")
STOCK_INTERVALS = SUPPORTED_INTERVALS
INDEX_INTERVALS = SUPPORTED_INTERVALS


class IntradayDataPoint(BaseModel):
    """Single OHLCV data point for intraday chart data."""
    time: int = Field(..., description="Timestamp in Unix milliseconds")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Closing price")
    volume: int = Field(..., description="Trading volume")

    @field_validator("volume", mode="before")
    @classmethod
    def coerce_volume_to_int(cls, v: object) -> int:
        if isinstance(v, float):
            return int(v)
        return v


class CacheMetadata(BaseModel):
    """Cache metadata for responses."""
    cached: bool = Field(..., description="Whether data was served from cache")
    cache_key: Optional[str] = Field(None, description="Cache key used")
    ttl_remaining: Optional[int] = Field(None, description="Remaining TTL in seconds")
    refreshed_in_background: bool = Field(False, description="Whether a background refresh was triggered")
    watermark: Optional[int] = Field(None, description="Timestamp of last cached bar (Unix ms)")
    complete: Optional[bool] = Field(None, description="True when all bars are immutable (market closed / historical)")
    market_phase: Optional[str] = Field(None, description="Current market phase: pre, open, post, or closed")
    truncated: Optional[bool] = Field(None, description="True when upstream fetch hit its bar limit")


class IntradayResponse(BaseModel):
    """Response for single symbol intraday data request."""

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "symbol": "AAPL",
            "interval": "1min",
            "data": [
                {
                    "time": 1705322400000,
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
                "cache_key": "ohlcv:stock:AAPL:1min",
                "ttl_remaining": 45,
                "refreshed_in_background": False
            }
        }
    })

    symbol: str = Field(..., description="Stock/index symbol")
    interval: str = Field(..., description="Data interval (e.g., 1min, 5min, 1hour)")
    data: List[IntradayDataPoint] = Field(default_factory=list, description="Intraday OHLCV data points")
    count: int = Field(0, description="Number of data points returned")
    cache: CacheMetadata = Field(..., description="Cache metadata")


class DailyResponse(BaseModel):
    """Response for single symbol daily historical data request."""

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "symbol": "AAPL",
            "data": [
                {
                    "time": 1705276800000,
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
                "cache_key": "ohlcv:stock:AAPL:day",
                "ttl_remaining": 3200,
                "refreshed_in_background": False
            }
        }
    })

    symbol: str = Field(..., description="Stock symbol")
    data: List[IntradayDataPoint] = Field(default_factory=list, description="Daily OHLCV data points")
    count: int = Field(0, description="Number of data points returned")
    cache: CacheMetadata = Field(..., description="Cache metadata")


class BatchIntradayRequest(BaseModel):
    """Request for batch intraday data."""

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "symbols": ["AAPL", "MSFT", "GOOGL"],
                "interval": "15min",
                "from": "2024-01-01",
                "to": "2024-01-15"
            }
        },
    )

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


class BatchCacheStats(BaseModel):
    """Cache statistics for batch requests."""
    total_requests: int = Field(..., description="Total number of symbols requested")
    cache_hits: int = Field(0, description="Number of symbols served from cache")
    cache_misses: int = Field(0, description="Number of symbols fetched from API")
    background_refreshes: int = Field(0, description="Number of background refreshes triggered")


class BatchIntradayResponse(BaseModel):
    """Response for batch intraday data request."""

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "interval": "15min",
            "results": {
                "AAPL": [
                    {
                        "time": 1705322400000,
                        "open": 185.50,
                        "high": 185.75,
                        "low": 185.25,
                        "close": 185.60,
                        "volume": 1500000
                    }
                ],
                "MSFT": [
                    {
                        "time": 1705322400000,
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
    })

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


class SnapshotData(BaseModel):
    """Normalized snapshot data for a single ticker."""
    symbol: str = Field(..., description="Ticker symbol")
    name: Optional[str] = Field(None, description="Ticker name")
    price: Optional[float] = Field(None, description="Current / last price")
    change: Optional[float] = Field(None, description="Day change (absolute)")
    change_percent: Optional[float] = Field(None, description="Day change %")
    previous_close: Optional[float] = Field(None, description="Previous close price")
    open: Optional[float] = Field(None, description="Day open")
    high: Optional[float] = Field(None, description="Day high")
    low: Optional[float] = Field(None, description="Day low")
    volume: Optional[int] = Field(None, description="Day volume")
    market_status: Optional[str] = Field(None, description="Per-ticker market phase")
    regular_trading_change: Optional[float] = Field(None, description="Regular session change (close - prev close)")
    early_trading_change_percent: Optional[float] = Field(None, description="Pre-market change %")
    late_trading_change_percent: Optional[float] = Field(None, description="After-hours change %")


class SnapshotResponse(BaseModel):
    """Response for batch snapshot request."""
    snapshots: List[SnapshotData] = Field(default_factory=list, description="Snapshot data per symbol")
    count: int = Field(0, description="Number of snapshots returned")


class MarketStatusResponse(BaseModel):
    """Response for market status request."""
    market: Optional[str] = Field(None, description="Market status (open / closed / extended-hours)")
    afterHours: Optional[bool] = Field(None, description="Whether after-hours trading is active")
    earlyHours: Optional[bool] = Field(None, description="Whether pre-market trading is active")
    serverTime: Optional[str] = Field(None, description="Server time (ISO)")
    exchanges: Optional[Dict[str, Any]] = Field(None, description="Per-exchange status")
    providers: List[str] = Field(default_factory=list, description="Active market data provider names")


class StockSearchResult(BaseModel):
    """Single stock search result."""

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "currency": "USD",
            "stockExchange": "NASDAQ Global Select",
            "exchangeShortName": "NASDAQ"
        }
    })

    symbol: str = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    name: str = Field(..., description="Company name")
    currency: Optional[str] = Field(None, description="Currency code")
    stockExchange: Optional[str] = Field(None, description="Stock exchange name")
    exchangeShortName: Optional[str] = Field(None, description="Short exchange name")


class StockSearchResponse(BaseModel):
    """Response for stock search request."""

    model_config = ConfigDict(json_schema_extra={
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
    })

    query: str = Field(..., description="Search query used")
    results: List[StockSearchResult] = Field(default_factory=list, description="List of matching stocks")
    count: int = Field(0, description="Number of results returned")
