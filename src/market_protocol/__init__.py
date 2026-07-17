"""Canonical Market Data Protocol (CMDP).

Single source of truth for market-data identity, time, price, and freshness
semantics across providers, caches, the REST/WS API, and the frontend.
See the decision log in the CMDP plan for the rationale behind each axis.
"""

from .enums import (
    OHLCV_SCHEMAS,
    AssetClass,
    FeedScope,
    MarketPhase,
    PriceTreatment,
    Tier,
)
from .models import (
    Coverage,
    Gap,
    InstrumentRef,
    OhlcvBar,
    Series,
    SeriesHeader,
)
from .symbology import (
    CARET_INDEX_REGIONS,
    display_decimals_for,
    parse_instrument_key,
    to_canonical,
    to_display,
    to_legacy_api,
    to_provider,
)

__all__ = [
    "CARET_INDEX_REGIONS",
    "OHLCV_SCHEMAS",
    "AssetClass",
    "Coverage",
    "FeedScope",
    "Gap",
    "InstrumentRef",
    "MarketPhase",
    "OhlcvBar",
    "PriceTreatment",
    "Series",
    "SeriesHeader",
    "Tier",
    "display_decimals_for",
    "parse_instrument_key",
    "to_canonical",
    "to_display",
    "to_legacy_api",
    "to_provider",
]
