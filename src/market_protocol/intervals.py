"""Canonical schema ids ⇄ legacy interval strings ⇄ bar period seconds.

The legacy strings ("1min", "1hour", …) are the FMP-style spellings used by
today's REST API, cache keys, and provider interfaces. They remain the wire
format of the legacy endpoints indefinitely; protocol endpoints speak schema
ids only.
"""

from __future__ import annotations

from .enums import OHLCV_SCHEMAS

_LEGACY_BY_SCHEMA: dict[str, str] = {
    "ohlcv-1s": "1s",
    "ohlcv-1m": "1min",
    "ohlcv-5m": "5min",
    "ohlcv-15m": "15min",
    "ohlcv-30m": "30min",
    "ohlcv-1h": "1hour",
    "ohlcv-4h": "4hour",
    "ohlcv-1d": "1day",
}

_SCHEMA_BY_LEGACY: dict[str, str] = {v: k for k, v in _LEGACY_BY_SCHEMA.items()}

# Keep in sync with src/utils/market_hours._INTERVAL_SECONDS (parity-tested).
_SECONDS_BY_SCHEMA: dict[str, int] = {
    "ohlcv-1s": 1,
    "ohlcv-1m": 60,
    "ohlcv-5m": 300,
    "ohlcv-15m": 900,
    "ohlcv-30m": 1800,
    "ohlcv-1h": 3600,
    "ohlcv-4h": 14400,
    "ohlcv-1d": 86400,
}

if set(_LEGACY_BY_SCHEMA) != set(OHLCV_SCHEMAS):
    raise RuntimeError("intervals: _LEGACY_BY_SCHEMA drifted from OHLCV_SCHEMAS")
if set(_SECONDS_BY_SCHEMA) != set(OHLCV_SCHEMAS):
    raise RuntimeError("intervals: _SECONDS_BY_SCHEMA drifted from OHLCV_SCHEMAS")


def schema_for_legacy(interval: str) -> str:
    """Map a legacy interval string ("1min") to its schema id ("ohlcv-1m")."""
    try:
        return _SCHEMA_BY_LEGACY[interval]
    except KeyError:
        raise ValueError(f"Unknown legacy interval: {interval!r}") from None


def legacy_for_schema(schema: str) -> str:
    """Map a schema id ("ohlcv-1m") to its legacy interval string ("1min")."""
    try:
        return _LEGACY_BY_SCHEMA[schema]
    except KeyError:
        raise ValueError(f"Unknown ohlcv schema: {schema!r}") from None


def schema_seconds(schema: str) -> int:
    """Bar period in seconds for a schema id."""
    try:
        return _SECONDS_BY_SCHEMA[schema]
    except KeyError:
        raise ValueError(f"Unknown ohlcv schema: {schema!r}") from None


def is_intraday_schema(schema: str) -> bool:
    """True for sub-daily schemas."""
    return schema_seconds(schema) < 86400
