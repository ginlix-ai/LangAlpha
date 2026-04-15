"""Provider singletons and source registry.

Builds market-data, news, and financial-data provider singletons from
config + credentials.  All three use double-checked locking via
``asyncio.Lock`` to avoid redundant initialization.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from .base import (
    FinancialDataSource,
    MarketDataSource,
    MarketIntelSource,
    NewsDataSource,
)
from .financial_data_provider import FinancialDataProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Availability checks
# ---------------------------------------------------------------------------


def _ginlix_data_available() -> bool:
    from src.config.settings import GINLIX_DATA_URL

    return bool(GINLIX_DATA_URL)


def _fmp_available() -> bool:
    return bool(os.getenv("FMP_API_KEY"))


def _yfinance_available() -> bool:
    try:
        import yfinance  # noqa: F401

        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Async source constructors
# ---------------------------------------------------------------------------


async def _build_ginlix_data_source() -> MarketDataSource:
    from .ginlix_data import get_ginlix_data_client
    from .ginlix_data.data_source import GinlixDataSource

    client = await get_ginlix_data_client()
    return GinlixDataSource(client)


async def _build_fmp_source() -> MarketDataSource:
    from .fmp.data_source import FMPDataSource

    return FMPDataSource()


async def _build_ginlix_data_news_source() -> NewsDataSource:
    from .ginlix_data import get_ginlix_data_client
    from .ginlix_data.news_source import GinlixDataNewsSource

    client = await get_ginlix_data_client()
    return GinlixDataNewsSource(client)


async def _build_fmp_news_source() -> NewsDataSource:
    from .fmp.news_source import FMPNewsSource

    return FMPNewsSource()


async def _build_yfinance_source() -> MarketDataSource:
    from .yfinance.data_source import YFinanceDataSource

    return YFinanceDataSource()


async def _build_yfinance_news_source() -> NewsDataSource:
    from .yfinance.news_source import YFinanceNewsSource

    return YFinanceNewsSource()


# ---------------------------------------------------------------------------
# Source registries — map config name → (availability_check, async_constructor)
# ---------------------------------------------------------------------------

_SOURCE_REGISTRY: dict[str, tuple[Any, Any]] = {
    "ginlix-data": (_ginlix_data_available, _build_ginlix_data_source),
    "fmp": (_fmp_available, _build_fmp_source),
    "yfinance": (_yfinance_available, _build_yfinance_source),
}

_NEWS_SOURCE_REGISTRY: dict[str, tuple[Any, Any]] = {
    "ginlix-data": (_ginlix_data_available, _build_ginlix_data_news_source),
    "fmp": (_fmp_available, _build_fmp_news_source),
    "yfinance": (_yfinance_available, _build_yfinance_news_source),
}

# ---------------------------------------------------------------------------
# Market data provider factory
# ---------------------------------------------------------------------------

_market_data_provider: MarketDataSource | None = None
_market_data_provider_lock = asyncio.Lock()


async def get_market_data_provider() -> MarketDataSource:
    """Return the active :class:`MarketDataSource` singleton.

    Builds an ordered chain from ``market_data.providers`` in config.yaml.
    Each provider that passes its availability check is included.
    When multiple sources are available, requests are routed by market
    region with automatic fallback.
    """
    global _market_data_provider
    if _market_data_provider is not None:
        return _market_data_provider

    async with _market_data_provider_lock:
        if _market_data_provider is not None:
            return _market_data_provider

        from src.config.settings import get_market_data_providers
        from .market_data_provider import MarketDataProvider, ProviderEntry

        provider_configs = get_market_data_providers()
        entries: list[ProviderEntry] = []

        for cfg in provider_configs:
            name = cfg["name"]
            markets = set(cfg.get("markets", ["all"]))
            reg = _SOURCE_REGISTRY.get(name)
            if reg and reg[0]():  # availability check
                source = await reg[1]()
                entries.append(ProviderEntry(name=name, source=source, markets=markets))
                logger.debug(
                    "market_data.source.registered | name=%s markets=%s", name, markets
                )
            else:
                logger.debug("market_data.source.skipped | name=%s (unavailable)", name)

        if not entries:
            raise RuntimeError(
                "No market data source available — check config and credentials"
            )

        _market_data_provider = MarketDataProvider(entries)

        return _market_data_provider


# Backward-compatible alias
get_price_provider = get_market_data_provider

# ---------------------------------------------------------------------------
# News data provider factory
# ---------------------------------------------------------------------------

_news_data_provider = None
_news_data_provider_lock = asyncio.Lock()


async def get_news_data_provider():
    """Return the active :class:`NewsDataProvider` singleton.

    Builds an ordered chain from ``news_data.providers`` in config.yaml.
    """
    global _news_data_provider
    if _news_data_provider is not None:
        return _news_data_provider

    async with _news_data_provider_lock:
        if _news_data_provider is not None:
            return _news_data_provider

        from src.config.settings import get_news_data_providers
        from .news_data_provider import NewsDataProvider

        provider_configs = get_news_data_providers()
        sources: list[tuple[str, Any]] = []

        for cfg in provider_configs:
            name = cfg["name"]
            reg = _NEWS_SOURCE_REGISTRY.get(name)
            if reg and reg[0]():  # availability check
                source = await reg[1]()
                sources.append((name, source))
                logger.debug("news_data.source.registered | name=%s", name)
            else:
                logger.debug("news_data.source.skipped | name=%s (unavailable)", name)

        if not sources:
            raise RuntimeError(
                "No news data source available — check config and credentials"
            )

        _news_data_provider = NewsDataProvider(sources)
        return _news_data_provider


# ---------------------------------------------------------------------------
# Financial data provider factory
# ---------------------------------------------------------------------------

_financial_data_provider: FinancialDataProvider | None = None
_financial_data_provider_lock = asyncio.Lock()


async def get_financial_data_provider() -> FinancialDataProvider:
    """Return the active :class:`FinancialDataProvider` singleton.

    Builds the composite from available backends:
    - :class:`FMPFinancialSource` if ``FMP_API_KEY`` is set.
    - :class:`GinlixMarketIntelSource` if ``GINLIX_DATA_URL`` is configured.
    """
    global _financial_data_provider
    if _financial_data_provider is not None:
        return _financial_data_provider

    async with _financial_data_provider_lock:
        if _financial_data_provider is not None:
            return _financial_data_provider

        financial: FinancialDataSource | None = None
        intel: MarketIntelSource | None = None

        if _fmp_available():
            from .fmp import get_fmp_client
            from .fmp.financial_source import FMPFinancialSource

            fmp_client = await get_fmp_client()
            financial = FMPFinancialSource(fmp_client)
            logger.debug(
                "financial_data.source.registered | name=fmp (FinancialDataSource)"
            )
        elif _yfinance_available():
            from .yfinance.financial_source import YFinanceFinancialSource

            financial = YFinanceFinancialSource()
            logger.debug(
                "financial_data.source.registered | name=yfinance (FinancialDataSource)"
            )

        if _ginlix_data_available():
            from .ginlix_data import get_ginlix_data_client
            from .ginlix_data.market_intel_source import GinlixMarketIntelSource

            client = await get_ginlix_data_client()
            intel = GinlixMarketIntelSource(client)
            logger.debug(
                "financial_data.source.registered | name=ginlix-data (MarketIntelSource)"
            )

        _financial_data_provider = FinancialDataProvider(
            financial=financial, intel=intel
        )
        return _financial_data_provider
