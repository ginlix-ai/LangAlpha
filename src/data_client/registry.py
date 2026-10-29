"""Provider singletons and source registry.

Builds market-data, news, and financial-data provider singletons from
config + credentials.  All three use double-checked locking via
``asyncio.Lock`` to avoid redundant initialization.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
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


def _tickertick_available() -> bool:
    return True  # free, keyless API


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


async def _build_tushare_source() -> MarketDataSource:
    # CN bars and quotes come from ginlix-data. The entry keeps the ``tushare``
    # name because that is still the publisher.
    from .ginlix_data import get_ginlix_data_client
    from .ginlix_data.cn_source import GinlixDataCnSource

    return GinlixDataCnSource(await get_ginlix_data_client())


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


async def _build_tickertick_news_source() -> NewsDataSource:
    from .tickertick.news_source import TickerTickNewsSource

    return TickerTickNewsSource()


async def _build_tushare_news_source() -> NewsDataSource:
    from .ginlix_data import get_ginlix_data_client
    from .ginlix_data.cn_financial import GinlixDataCnNewsSource

    return GinlixDataCnNewsSource(await get_ginlix_data_client())


# ---------------------------------------------------------------------------
# Source registries — map config name → (availability_check, async_constructor)
# ---------------------------------------------------------------------------

_SOURCE_REGISTRY: dict[str, tuple[Any, Any]] = {
    "ginlix-data": (_ginlix_data_available, _build_ginlix_data_source),
    "tushare": (_ginlix_data_available, _build_tushare_source),
    "fmp": (_fmp_available, _build_fmp_source),
    "yfinance": (_yfinance_available, _build_yfinance_source),
}

@dataclass(frozen=True)
class NewsGate:
    """Who may target a news source by name: a feature flag plus a locale prefix."""

    feature: str
    locale: str


@dataclass(frozen=True)
class NewsSourceSpec:
    """A named news source, and what a caller targeting it by name may do.

    ``owns_article`` marks a source outside the default chain that can resolve
    an article id it may have issued; ``gate`` marks one whose feed is served
    only to eligible users (everyone else gets the chain).
    """

    available: Callable[[], bool]
    build: Callable[[], Awaitable[NewsDataSource]]
    owns_article: Callable[[str], bool] | None = None
    gate: NewsGate | None = None


def _any_article(_: str) -> bool:
    return True


_NEWS_SOURCE_REGISTRY: dict[str, NewsSourceSpec] = {
    "ginlix-data": NewsSourceSpec(_ginlix_data_available, _build_ginlix_data_news_source),
    "fmp": NewsSourceSpec(_fmp_available, _build_fmp_news_source),
    "yfinance": NewsSourceSpec(_yfinance_available, _build_yfinance_news_source),
    "tickertick": NewsSourceSpec(
        _tickertick_available, _build_tickertick_news_source, owns_article=_any_article
    ),
    # CN market news — targeted via ?provider=tushare (bypass path, like
    # tickertick); deliberately NOT in the default news_data.providers chain.
    # Its article ids are ``ts-`` content hashes, so no other id costs a
    # fetch of its whole window.
    "tushare": NewsSourceSpec(
        _ginlix_data_available,
        _build_tushare_news_source,
        owns_article=lambda article_id: article_id.startswith("ts-"),
        gate=NewsGate(feature="a_share_pack", locale="zh"),
    ),
}


def news_gate(name: str | None) -> NewsGate | None:
    """The eligibility gate on targeting news source *name*, if it has one."""
    spec = _NEWS_SOURCE_REGISTRY.get(name or "")
    return spec.gate if spec else None


def news_source_available(name: str) -> bool:
    """Whether news source *name* can be built here, as ``get_news_source`` would."""
    spec = _NEWS_SOURCE_REGISTRY.get(name)
    return spec is not None and spec.available()


def news_article_owners(article_id: str) -> list[str]:
    """Named sources to ask for *article_id* after the default chain misses."""
    return [
        name for name, spec in _NEWS_SOURCE_REGISTRY.items()
        if spec.owns_article is not None and spec.owns_article(article_id)
    ]

# ---------------------------------------------------------------------------
# Market data provider factory
# ---------------------------------------------------------------------------


RULESET_PATH_ENV = "DATA_ROUTING_RULESET"


def default_ruleset_path() -> Path:
    """``$DATA_ROUTING_RULESET``, else ``data_routing.yaml`` in the working directory.

    Deployment configuration, like ``.env``: generated per deployment against
    its own tokens, so it is not committed.
    """
    override = os.environ.get(RULESET_PATH_ENV)
    return Path(override) if override else Path.cwd() / "data_routing.yaml"


def _load_routing_table():
    """The generated routing ruleset, or ``None`` when this deployment has none.

    Absence is the normal case (the file is produced by the entitlement probe
    against a deployment's own tokens), but which routing is live should be
    readable from the log, so the fallback says so once per process.
    """
    from market_protocol.routing import RoutingTable, load_ruleset

    path = default_ruleset_path()
    ruleset = load_ruleset(path)
    if ruleset is None:
        logger.info(
            "market_data.routing.no_ruleset | path=%s using the static market_data "
            "chain from config.yaml", path,
        )
        return None
    logger.info(
        "market_data.routing.loaded | cells=%s generated_at=%s",
        len(ruleset.cells),
        ruleset.generated_at.isoformat(),
    )
    return RoutingTable(ruleset)


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
        sources_by_name: dict[str, Any] = {}

        for cfg in provider_configs:
            name = cfg["name"]
            markets = set(cfg.get("markets", ["all"]))
            reg = _SOURCE_REGISTRY.get(name)
            if reg and reg[0]():  # availability check
                # A provider may appear multiple times (per-capability chain
                # positions) — all its entries share one source instance.
                if name not in sources_by_name:
                    sources_by_name[name] = await reg[1]()
                entries.append(ProviderEntry(
                    name=name,
                    source=sources_by_name[name],
                    markets=markets,
                    intraday_markets=(
                        set(cfg["intraday_markets"]) if cfg.get("intraday_markets") is not None else None
                    ),
                    daily_markets=(
                        set(cfg["daily_markets"]) if cfg.get("daily_markets") is not None else None
                    ),
                    snapshot_markets=(
                        set(cfg["snapshot_markets"]) if cfg.get("snapshot_markets") is not None else None
                    ),
                ))
                logger.debug(
                    "market_data.source.registered | name=%s markets=%s", name, markets
                )
            else:
                logger.debug("market_data.source.skipped | name=%s (unavailable)", name)

        if not entries:
            raise RuntimeError(
                "No market data source available — check config and credentials"
            )

        from .ginlix_data.directory import fill_quote_names

        _market_data_provider = MarketDataProvider(
            entries, routing=_load_routing_table(), name_rows=fill_quote_names,
        )

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
            if reg and reg.available():  # availability check
                source = await reg.build()
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
# Named single-source access (for providers targeted directly, e.g. TickerTick)
# ---------------------------------------------------------------------------

_news_sources: dict[str, NewsDataSource] = {}
_news_sources_lock = asyncio.Lock()


async def get_news_source(name: str) -> NewsDataSource:
    """Return a single named :class:`NewsDataSource`, bypassing the fallback chain.

    Used when a caller wants a specific provider (e.g. ``tickertick`` for the
    dashboard's curated feed) rather than the configured fallback order.
    """
    cached = _news_sources.get(name)
    if cached is not None:
        return cached

    async with _news_sources_lock:
        cached = _news_sources.get(name)
        if cached is not None:
            return cached

        reg = _NEWS_SOURCE_REGISTRY.get(name)
        if not reg or not reg.available():
            raise ValueError(f"News source '{name}' is not available")

        source = await reg.build()
        _news_sources[name] = source
        return source


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

        if _ginlix_data_available():
            # CN fundamentals come from ginlix-data (vendor: Tushare).
            from .financial_data_provider import (
                MarketRoute,
                RoutedFinancialSource,
                RoutedMarketIntelSource,
            )
            from .ginlix_data import get_ginlix_data_client
            from .ginlix_data.cn_financial import (
                GinlixDataCnFinancialSource,
                GinlixDataCnIntelSource,
                has_cjk,
                is_cn_option,
                screens_cn,
            )
            from .ginlix_data.directory import attach_names

            client = await get_ginlix_data_client()
            financial = RoutedFinancialSource(
                default=financial,
                by_market={"cn": MarketRoute(
                    GinlixDataCnFinancialSource(client), screens=screens_cn, claims_query=has_cjk,
                )},
                name_rows=functools.partial(attach_names, local="nameLocal", english="nameEn"),
            )
            intel = RoutedMarketIntelSource(
                default=intel,
                by_market={"cn": MarketRoute(
                    GinlixDataCnIntelSource(client), claims_option=is_cn_option,
                )},
            )
            logger.debug(
                "financial_data.source.registered | name=ginlix-data (cn routing)"
            )

        _financial_data_provider = FinancialDataProvider(
            financial=financial, intel=intel
        )
        return _financial_data_provider
