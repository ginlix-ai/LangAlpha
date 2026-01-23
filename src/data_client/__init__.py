"""Data access layer.

This package is the single source of truth for fetching raw financial data.

Design goals:
- Unified API for both host tools and sandbox code.
- Backend can be either direct-provider (e.g. FMP) or MCP-based.
- Do not inline secrets into sandbox-uploaded code.

MCP convention:
- The market/fundamentals MCP server should be named `financial_data`.
  When running inside a PTC sandbox, this will be available as `tools.financial_data`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import importlib
from typing import Any, Optional

from .fmp import FMPClient


MCP_SERVER_NAME = "financial_data"


class FinancialDataBackendError(RuntimeError):
    """Raised when no backend is available for a request."""


@dataclass(frozen=True)
class FinancialDataResult:
    """Standard wrapper for raw financial-data results."""

    data: Any
    source: str  # "mcp" | "direct"


def _try_import_mcp_module() -> Any | None:
    """Best-effort import of the sandbox-generated MCP module."""

    try:
        return importlib.import_module(f"tools.{MCP_SERVER_NAME}")
    except Exception:
        return None


async def _direct_get_stock_data(
    symbol: str,
    interval: str = "1day",
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    interval_lower = interval.lower()

    # Default date window for intraday queries (FMP requires dates)
    if interval_lower not in {"1day", "daily", "1d", "day"}:
        if end_date is None:
            end_date = date.today().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    async with FMPClient() as client:
        if interval_lower in {"1day", "daily", "1d", "day"}:
            rows = await client.get_stock_price(symbol, from_date=start_date, to_date=end_date)
        else:
            rows = await client.get_intraday_chart(symbol, interval_lower, from_date=start_date, to_date=end_date)

    return rows or []


async def get_stock_data(
    symbol: str,
    interval: str = "1day",
    start_date: str | None = None,
    end_date: str | None = None,
) -> FinancialDataResult:
    """Unified OHLCV fetch.

    Returns raw rows (list of dicts). In sandbox, prefers MCP (`tools.financial_data`).
    In host-only mode, falls back to direct provider access.
    """

    mcp_module = _try_import_mcp_module()
    if mcp_module is not None and hasattr(mcp_module, "get_stock_data"):
        # MCP tool modules are generated as sync functions.
        data = mcp_module.get_stock_data(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
        return FinancialDataResult(data=data, source="mcp")

    data = await _direct_get_stock_data(symbol, interval, start_date, end_date)
    return FinancialDataResult(data=data, source="direct")


async def get_quote(symbol: str) -> FinancialDataResult:
    """Get real-time quote (raw provider response)."""

    mcp_module = _try_import_mcp_module()
    if mcp_module is not None and hasattr(mcp_module, "get_quote"):
        data = mcp_module.get_quote(symbol=symbol)
        return FinancialDataResult(data=data, source="mcp")

    async with FMPClient() as client:
        data = await client.get_quote(symbol)
    return FinancialDataResult(data=data, source="direct")


async def get_profile(symbol: str) -> FinancialDataResult:
    """Get company profile (raw provider response)."""

    mcp_module = _try_import_mcp_module()
    if mcp_module is not None and hasattr(mcp_module, "get_profile"):
        data = mcp_module.get_profile(symbol=symbol)
        return FinancialDataResult(data=data, source="mcp")

    async with FMPClient() as client:
        data = await client.get_profile(symbol)
    return FinancialDataResult(data=data, source="direct")


async def _direct_get_asset_data(
    symbol: str,
    asset_type: str,
    interval: str = "daily",
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, Any]]:
    at = asset_type.lower().strip()
    interval_lower = interval.lower()

    if at not in {"stock", "commodity", "crypto", "forex"}:
        raise ValueError("Invalid asset_type. Must be one of: stock, commodity, crypto, forex")

    # Default date window for intraday queries (FMP requires dates)
    if interval_lower not in {"1day", "daily", "1d", "day"}:
        if to_date is None:
            to_date = date.today().strftime("%Y-%m-%d")
        if from_date is None:
            from_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    async with FMPClient() as client:
        if at == "stock":
            if interval_lower in {"1day", "daily", "1d", "day"}:
                rows = await client.get_stock_price(symbol, from_date=from_date, to_date=to_date)
            else:
                rows = await client.get_intraday_chart(symbol, interval_lower, from_date=from_date, to_date=to_date)
            return rows or []

        # commodity/crypto/forex
        if interval_lower in {"1day", "daily", "1d", "day"}:
            if at == "commodity":
                rows = await client.get_commodity_price(symbol, from_date=from_date, to_date=to_date)
            elif at == "crypto":
                rows = await client.get_crypto_price(symbol, from_date=from_date, to_date=to_date)
            else:
                rows = await client.get_forex_price(symbol, from_date=from_date, to_date=to_date)
            return rows or []

        if interval_lower not in {"1min", "5min", "1hour"}:
            raise ValueError("Unsupported interval for commodity/crypto/forex")

        if at == "commodity":
            rows = await client.get_commodity_intraday_chart(symbol, interval_lower, from_date=from_date, to_date=to_date)
        elif at == "crypto":
            rows = await client.get_crypto_intraday_chart(symbol, interval_lower, from_date=from_date, to_date=to_date)
        else:
            rows = await client.get_forex_intraday_chart(symbol, interval_lower, from_date=from_date, to_date=to_date)

    return rows or []


async def get_asset_data(
    symbol: str,
    asset_type: str,
    interval: str = "daily",
    from_date: str | None = None,
    to_date: str | None = None,
) -> FinancialDataResult:
    """Unified OHLCV fetch for stock/commodity/crypto/forex.

    In sandbox, prefers MCP (`tools.financial_data.get_asset_data`).
    """

    mcp_module = _try_import_mcp_module()
    if mcp_module is not None and hasattr(mcp_module, "get_asset_data"):
        data = mcp_module.get_asset_data(
            symbol=symbol,
            asset_type=asset_type,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
        )
        return FinancialDataResult(data=data, source="mcp")

    data = await _direct_get_asset_data(symbol, asset_type, interval, from_date, to_date)
    return FinancialDataResult(data=data, source="direct")
