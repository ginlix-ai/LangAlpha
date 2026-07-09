"""
Data Agent Tools - Market data retrieval and analysis tools.

Provides comprehensive market data tools supporting US stocks, A-shares (Chinese),
and HK stocks. Tools are organized with clear separation between LangChain interface
(@tool decorators) and business logic implementations.

Available tools:
- get_daily_prices: Historical daily OHLCV price data
- get_company_overview: Comprehensive investment intelligence overview (includes real-time quote)
- get_quote: Real-time quotes only — cheap and fast
- get_market_overview: Region-keyed index basket + US sector performance
- screen_stocks: Stock screener with filters for market cap, price, sector, etc.
"""

from .tool import (
    get_daily_prices,
    get_company_overview,
    get_market_movers,
    get_market_overview,
    get_options_chain,
    get_quote,
    screen_stocks,
)
from .implementations import fetch_market_indices, fetch_quote, fetch_sector_performance

__all__ = [
    "get_daily_prices",
    "get_company_overview",
    "get_market_movers",
    "get_market_overview",
    "get_options_chain",
    "get_quote",
    "screen_stocks",
    "fetch_market_indices",
    "fetch_quote",
    "fetch_sector_performance",
]
