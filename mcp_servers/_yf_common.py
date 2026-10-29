"""Shared helpers for the yfinance MCP servers.

Synced into sandboxes as a sibling module — keep imports limited to stdlib,
pandas, and market_protocol.
"""

from __future__ import annotations

import math
from datetime import tzinfo
from typing import Optional

import pandas as pd

from market_protocol import (
    AssetClass,
    InstrumentRef,
    is_family_index,
    to_canonical,
    to_display,
    vendor_symbol,
)


class _CookieCacheWithA3:
    """yfinance's cookie cache, less an entry without the ``A3`` cookie.

    yfinance reads the cached cookie's ``A3`` without checking it is there, so
    one saved without it fails every request in every later process with
    ``KeyError: 'A3'``; a sandbox keeps its home, so that is every yfinance
    tool in the workspace for good. Such an entry reads as a miss and is
    deleted, which also clears it for code that imports yfinance itself. The
    cache stays: yfinance refetches the cookie on every request without it.
    """

    checks_a3 = True

    def __init__(self, inner):
        self._inner = inner

    def lookup(self, strategy):
        entry = self._inner.lookup(strategy)
        try:
            next(iter(entry["cookie"].values()))["/"]["A3"]
        except (TypeError, KeyError, StopIteration, AttributeError):
            if entry is not None:
                self._inner.store(strategy, None)  # a None store deletes the row
            return None
        return entry

    def store(self, strategy, cookie):
        self._inner.store(strategy, cookie)


def guard_cookie_cache() -> None:
    try:
        from yfinance import cache
        current = cache.get_cookie_cache()
        if not getattr(current, "checks_a3", False):
            cache._CookieCacheManager._Cookie_cache = _CookieCacheWithA3(current)
    except (ImportError, AttributeError):
        pass  # a release that moved the cache; the load it guards moved with it


guard_cookie_cache()


def yfinance_symbol(ref: InstrumentRef) -> str:
    """Yahoo's spelling: ``^GSPC`` for a family index, ``600519.SS``, ``EURUSD=X`` for FX."""
    if is_family_index(ref):
        return f"^{vendor_symbol(ref)}"
    if ref.asset_class is AssetClass.FX:
        return f"{ref.symbol.replace('-', '')}=X"
    return vendor_symbol(ref)


def boundary(ticker: str) -> tuple[str, str, Optional[str]]:
    """Resolve an agent-supplied ticker at the protocol boundary.

    Returns ``(display_symbol, yfinance_symbol, price_currency)``. Falls back to
    the upper-cased raw ticker when the symbol cannot be canonicalized.
    """
    try:
        ref = to_canonical(ticker)
        return to_display(ref), yfinance_symbol(ref), ref.price_currency
    except Exception:  # noqa: BLE001
        cleaned = (ticker or "").strip().upper()
        return cleaned, cleaned, None


def safe_detail(action: str, exc: Exception) -> str:
    """Sanitized error detail — names the operation and exception type only."""
    return f"{action} failed via yfinance ({type(exc).__name__})."


def format_datetime(value) -> str:
    """YYYY-MM-DD for dates, YYYY-MM-DD HH:MM:SS for datetimes with a time."""
    if hasattr(value, "hour"):
        if value.hour or value.minute or value.second:
            return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def serialize_records(df: pd.DataFrame) -> list[dict]:
    """DataFrame → list of dicts with snake_case keys, preserving row order."""
    if df is None or df.empty:
        return []
    df = df.reset_index() if not isinstance(df.index, pd.RangeIndex) else df.copy()
    records = df.to_dict(orient="records")
    cleaned = []
    for rec in records:
        clean_rec = {}
        for key, value in rec.items():
            clean_key = (
                str(key)
                .lower()
                .replace(" ", "_")
                .replace("(%)", "_pct")
                .replace("%", "pct")
                .replace("(", "")
                .replace(")", "")
            )
            clean_rec[clean_key] = clean_value(value)
        cleaned.append(clean_rec)
    return cleaned


def clean_value(obj):
    """Recursively normalize datetimes, time zones and missing values (NaT/NaN/inf) for JSON."""
    if isinstance(obj, dict):
        return {k: clean_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_value(item) for item in obj]
    try:
        # NaT/NaN/None first — pd.NaT has isoformat but strftime raises on it.
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(obj, float) and math.isinf(obj):
        return None
    if hasattr(obj, "isoformat"):
        return format_datetime(obj)
    if isinstance(obj, tzinfo):
        # Yahoo's market status carries one (``tz``); its name is what JSON can hold.
        return str(obj)
    return obj
