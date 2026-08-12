"""Shared helpers for the yfinance MCP servers.

Synced into sandboxes as a sibling module — keep imports limited to stdlib,
pandas, and src.market_protocol.
"""

from __future__ import annotations

import math
from typing import Optional

import pandas as pd

from src.market_protocol import to_canonical, to_display, to_provider


def boundary(ticker: str) -> tuple[str, str, Optional[str]]:
    """Resolve an agent-supplied ticker at the protocol boundary.

    Returns ``(display_symbol, yfinance_symbol, price_currency)``. Falls back to
    the upper-cased raw ticker when the symbol cannot be canonicalized.
    """
    try:
        ref = to_canonical(ticker)
        return to_display(ref), to_provider(ref, "yfinance"), ref.price_currency
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
    """Recursively normalize datetimes and missing values (NaT/NaN/inf) for JSON."""
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
    return obj
