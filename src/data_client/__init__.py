"""Data access layer.

This package is the single source of truth for fetching raw financial data.

Design goals:
- Unified API for both host tools and sandbox code.
- Backend can be either direct-provider (e.g. FMP) or MCP-based.
- Do not inline secrets into sandbox-uploaded code.

MCP convention:
- The price data MCP server should be named `price_data`.
  When running inside a PTC sandbox, this will be available as `tools.price_data`.
"""

from .base import (  # noqa: F401 — re-export
    FinancialDataSource,
    MarketDataSource,
    MarketIntelSource,
    NewsDataSource,
    PriceDataProvider,
)
from .financial_data_provider import FinancialDataProvider  # noqa: F401 — re-export
from .market_data_provider import is_us_symbol, symbol_timezone  # noqa: F401 — re-export
from .normalize import normalize_bars  # noqa: F401 — re-export
from .registry import (  # noqa: F401 — re-export
    get_financial_data_provider,
    get_market_data_provider,
    get_news_data_provider,
    get_news_source,
    get_price_provider,
)
