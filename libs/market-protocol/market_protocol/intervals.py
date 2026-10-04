"""Canonical schema ids ⇄ legacy interval strings ⇄ bar period seconds.

The legacy strings ("1min", "1hour", ...) are the FMP-style spellings older
APIs and provider interfaces use. Protocol records speak schema ids only, so
these conversions belong at the boundary with a legacy spelling.
"""

from __future__ import annotations

import re

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

# Only the bare width: an ``ohlcv-`` id is this protocol's own spelling, where
# m is always a minute.
_MONTH_LIKE = re.compile(r"[0-9]+M")


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


def to_schema(interval: str) -> str:
    """Schema id for any interval spelling: ``ohlcv-1m``, the bare width ``1m``, or legacy ``1min``.

    The bare width is what a person types and what version-1 routing rulesets
    stored, so it is read here rather than by each caller. Case does not
    matter except in a bare width ending in ``M``: ``1M`` is the usual
    spelling of a month, so it is refused rather than lowered into a minute.
    """
    token = interval.strip()
    if _MONTH_LIKE.fullmatch(token):
        raise ValueError(f"Ambiguous interval: {interval!r} reads as months; minutes are 'm' or 'min'")
    token = token.lower()
    if token in _SECONDS_BY_SCHEMA:
        return token
    if f"ohlcv-{token}" in _SECONDS_BY_SCHEMA:
        return f"ohlcv-{token}"
    if token in _SCHEMA_BY_LEGACY:
        return _SCHEMA_BY_LEGACY[token]
    raise ValueError(f"Unknown interval: {interval!r}")


def schema_seconds(schema: str) -> int:
    """Bar period in seconds for a schema id."""
    try:
        return _SECONDS_BY_SCHEMA[schema]
    except KeyError:
        raise ValueError(f"Unknown ohlcv schema: {schema!r}") from None


def is_intraday_schema(schema: str) -> bool:
    """True for sub-daily schemas."""
    return schema_seconds(schema) < 86400
