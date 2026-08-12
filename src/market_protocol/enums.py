"""Protocol enums and schema identifiers.

All enums are ``StrEnum`` so they serialize as their wire string on
``model_dump(mode="json")`` without custom encoders.
"""

from __future__ import annotations

from enum import StrEnum


class AssetClass(StrEnum):
    EQUITY = "equity"
    INDEX = "index"
    CRYPTO = "crypto"
    FX = "fx"


class MarketPhase(StrEnum):
    """Session phase. Superset of the legacy pre/open/post/closed."""

    PRE = "pre"
    REGULAR = "regular"
    LUNCH = "lunch"
    POST = "post"
    CLOSED = "closed"
    HALTED = "halted"


class PriceTreatment(StrEnum):
    """Adjustment convention of a price series — declared, never mixed."""

    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    DIVIDEND_ADJUSTED = "dividend_adjusted"


class Tier(StrEnum):
    """Data freshness tier of a publisher feed."""

    REALTIME = "realtime"
    DELAYED_15M = "delayed_15m"
    EOD = "eod"


class FeedScope(StrEnum):
    """Whether a series reflects consolidated tape or a single venue's book.

    Our upstream feeds are consolidated, so ``composite`` is the default;
    the instrument's MIC is listing identity, not feed precision.
    """

    COMPOSITE = "composite"
    VENUE = "venue"


# Live record schema identifiers. Bar anchor is the OPEN of the aggregate
# window (ts_event = window start, Unix ms UTC) — pinned by conformance.
OHLCV_SCHEMAS: tuple[str, ...] = (
    "ohlcv-1s",
    "ohlcv-1m",
    "ohlcv-5m",
    "ohlcv-15m",
    "ohlcv-30m",
    "ohlcv-1h",
    "ohlcv-4h",
    "ohlcv-1d",
)
