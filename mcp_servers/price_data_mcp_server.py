#!/usr/bin/env python3
"""Price Data MCP Server.

Provides normalized OHLCV time series data and short sale analytics via MCP.

Design goals:
- Small, stable tool surface (high PTC value)
- Standard agent-facing envelope (see mcp_servers/AGENT_CONTRACT.md)
- Can run in sandbox (stdio) for OSS/dev
- Can be deployed externally (http/sse) for production

Tools:
- get_stock_data: stock OHLCV
- get_asset_data: stock/commodity/crypto/forex OHLCV
- get_short_data: short interest (bi-monthly) and short volume (daily)

Symbols are resolved at the boundary through src.market_protocol (canonical
identity, currency, timezone); prices are returned in major currency units
(GBX/pence venues converted to pounds).
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    import _bootstrap  # noqa: F401  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers import _bootstrap  # noqa: F401

from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

from mcp.server.fastmcp import FastMCP

from data_client.fmp import close_fmp_client, get_fmp_client
from data_client.ginlix_data import (
    DAILY_INTERVALS,
    close_ginlix_mcp_client,
    get_ginlix_mcp_client,
)
from data_client.normalize import minor_unit_scale, normalize_bars, scale_price
from src.market_protocol import to_canonical, to_display, to_legacy_api
from src.market_protocol.enums import AssetClass

try:
    from _envelope import (
        error_from_exception,
        error_from_upstream,
        make_error,
        make_response,
        normalize_interval,
    )
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._envelope import (
        error_from_exception,
        error_from_upstream,
        make_error,
        make_response,
        normalize_interval,
    )


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

_ginlix = get_ginlix_mcp_client()

# Intervals served by each family. Stock/index route through the ginlix-data
# (US) + FMP (global) fetch clients, which cover intraday + daily only.
_STOCK_INTRADAY = {"1min", "5min", "15min", "30min", "1hour", "4hour"}
_STOCK_INTERVALS = _STOCK_INTRADAY | {"1day"}
# FMP commodity/crypto/forex intraday coverage is narrower.
_ASSET_INTRADAY = {"1min", "5min", "1hour"}
_ASSET_INTERVALS = _ASSET_INTRADAY | {"1day"}


@asynccontextmanager
async def _lifespan(app):
    try:
        yield
    finally:
        await close_ginlix_mcp_client()
        await close_fmp_client()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve(
    symbol: str, asset_class: Optional[AssetClass] = None
) -> tuple[str, str, Optional[str], Optional[str]]:
    """Resolve a caller symbol via the protocol registry.

    Returns ``(display, legacy, currency, timezone)`` — the canonical display
    spelling, the legacy provider spelling, ISO-4217 price currency, and IANA
    timezone. Falls back to the raw symbol with unknown currency/tz.
    """
    try:
        ref = to_canonical(symbol, asset_class=asset_class)
        return to_display(ref), to_legacy_api(ref), ref.price_currency, ref.tz
    except Exception:  # noqa: BLE001
        return symbol, symbol, None, None


def _resolve_interval(
    interval: str, served: set[str], *, symbol: str
) -> tuple[Optional[str], Optional[dict]]:
    """Normalize *interval* to canonical vocab, constrained to *served*.

    Returns ``(canonical, None)`` on success or ``(None, error_envelope)`` for
    unknown spellings and vocab this tool does not serve.
    """
    canonical = normalize_interval(interval)
    if canonical is None or canonical not in served:
        return None, make_error(
            "unsupported_interval",
            f"Interval {interval!r} is not supported by this tool.",
            symbol=symbol,
            interval=interval,
            supported=sorted(served),
        )
    return canonical, None


def _display_rows(
    rows: list[dict], symbol: str, *, intraday: bool, scale: float = 1.0
) -> list[dict]:
    """Raw provider rows → ascending ``{date, open, high, low, close, volume}``.

    Timestamps are localized to *symbol*'s exchange timezone; price fields are
    scaled to major units (``scale`` = 0.01 for GBX/pence venues). Volume is a
    share count and never scaled.
    """
    normalized = normalize_bars(rows, symbol, intraday=intraday)  # descending
    normalized.reverse()  # ascending, oldest-first (contract)
    if scale != 1.0:
        for row in normalized:
            for field in ("open", "high", "low", "close"):
                row[field] = scale_price(row[field], scale)
    return normalized


async def _fetch_stock_series(
    symbol: str,
    interval: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[Optional[dict], Optional[dict]]:
    """Fetch stock OHLCV: ginlix-data (US) → FMP fallback (global).

    Returns ``(make_response_kwargs, None)`` on success or ``(None, error)``.
    The success kwargs carry canonical ``symbol``/``interval``, ``currency``,
    ``timezone``, ``data`` (ascending), and the resolved ``source``.
    """
    display, legacy, currency, timezone = _resolve(symbol)
    canonical, err = _resolve_interval(interval, _STOCK_INTERVALS, symbol=display)
    if err is not None:
        return None, err

    intraday = canonical not in DAILY_INTERVALS
    if intraday and (not start_date or not end_date):
        return None, make_error(
            "invalid_argument",
            "start_date and end_date are required for intraday intervals "
            "(YYYY-MM-DD or YYYY-MM-DD HH:MM).",
            symbol=display,
            interval=canonical,
        )

    # ginlix-data first (US equities only; returns None for anything else).
    ginlix_result = await _ginlix.fetch_stock_data(legacy, canonical, start_date, end_date)
    if isinstance(ginlix_result, dict):
        return None, error_from_upstream(
            ginlix_result.get("error", "ginlix-data request failed"),
            symbol=display,
            interval=canonical,
        )
    if ginlix_result is not None:
        # Already-normalized display rows, descending → flip to ascending.
        data = list(reversed(ginlix_result))
        return {
            "data": data,
            "source": "ginlix-data",
            "symbol": display,
            "interval": canonical,
            "currency": currency,
            "timezone": timezone,
        }, None

    # FMP fallback (all markets; converts GBX/pence to pounds).
    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return None, make_error(
            "client_unavailable",
            "FMP client unavailable (FMP_API_KEY not configured).",
            symbol=display,
        )

    scale = minor_unit_scale(legacy)
    try:
        if canonical in DAILY_INTERVALS:
            rows = await client.get_stock_price(legacy, from_date=start_date, to_date=end_date)
        else:
            rows = await client.get_intraday_chart(
                legacy, canonical, from_date=start_date, to_date=end_date
            )
    except Exception as e:  # noqa: BLE001
        return None, error_from_exception(
            e, "FMP fetch failed.", symbol=display, interval=canonical
        )

    data = _display_rows(rows or [], legacy, intraday=intraday, scale=scale)
    return {
        "data": data,
        "source": "fmp",
        "symbol": display,
        "interval": canonical,
        "currency": currency,
        "timezone": timezone,
    }, None


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = FastMCP("PriceDataMCP", lifespan=_lifespan)


@mcp.tool()
async def get_stock_data(
    symbol: str,
    interval: str = "1day",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Fetch OHLCV price bars for a stock; use for historical charts/analysis.

    Intervals: 1min|5min|15min|30min|1hour|4hour|1day (aliases like "1d"/"daily"
    accepted). start_date and end_date are required for intraday.

    Args:
        symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS", LSE "VOD.L".
        interval: Bar size; one of the intervals above.
        start_date: "YYYY-MM-DD" (append " HH:MM" for intraday time filtering).
        end_date: "YYYY-MM-DD" (append " HH:MM" for intraday time filtering).

    Returns:
        dict: {symbol, interval, currency, timezone, count, data, source}. data:
        list of {date, open, high, low, close, volume} ascending (oldest first);
        date exchange-local "YYYY-MM-DD" (daily) or "YYYY-MM-DD HH:MM:SS"
        (intraday); prices in `currency` major units. On error:
        {error: <code>, detail}.
    """
    env, err = await _fetch_stock_series(symbol, interval, start_date, end_date)
    if err is not None:
        return err
    return make_response(**env)


@mcp.tool()
async def get_asset_data(
    symbol: str,
    asset_type: str,
    interval: str = "1day",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> dict:
    """Fetch OHLCV bars for a stock, commodity, crypto, or forex pair.

    Intervals: 1min|5min|1hour|1day (stock adds 15min|30min|4hour); dates
    required for intraday.

    Args:
        symbol: Stock "AAPL", commodity "GCUSD", crypto "BTCUSD", forex "EURUSD".
        asset_type: One of stock|commodity|crypto|forex.
        interval: Bar size; see intervals above.
        from_date: "YYYY-MM-DD" (append " HH:MM" for intraday time filtering).
        to_date: "YYYY-MM-DD" (append " HH:MM" for intraday time filtering).

    Returns:
        dict: {symbol, asset_type, interval, currency, timezone, count, data,
        source}. data: list of {date, open, high, low, close, volume} ascending
        (oldest first); date exchange-local "YYYY-MM-DD[ HH:MM:SS]"; prices in
        `currency` major units. On error: {error: <code>, detail}.
    """
    at = asset_type.lower().strip()
    if at not in {"stock", "commodity", "crypto", "forex"}:
        return make_error(
            "invalid_argument",
            f"Invalid asset_type {asset_type!r}.",
            supported=["stock", "commodity", "crypto", "forex"],
        )

    if at == "stock":
        env, err = await _fetch_stock_series(symbol, interval, from_date, to_date)
        if err is not None:
            return err
        env["asset_type"] = at
        return make_response(**env)

    # commodity / crypto / forex — FMP direct (the provider chain and ginlix
    # cover equities/indexes only). Currency + timezone from canonical identity.
    hint = {"crypto": AssetClass.CRYPTO, "forex": AssetClass.FX}.get(at)
    display, _legacy, currency, timezone = _resolve(symbol, asset_class=hint)
    canonical, err = _resolve_interval(interval, _ASSET_INTERVALS, symbol=display)
    if err is not None:
        return err

    intraday = canonical not in DAILY_INTERVALS
    if intraday and (not from_date or not to_date):
        return make_error(
            "invalid_argument",
            "from_date and to_date are required for intraday intervals "
            "(YYYY-MM-DD or YYYY-MM-DD HH:MM).",
            symbol=display,
            interval=canonical,
        )

    try:
        client = await get_fmp_client()
    except Exception:  # noqa: BLE001
        return make_error(
            "client_unavailable",
            "FMP client unavailable (FMP_API_KEY not configured).",
            symbol=display,
        )

    try:
        if canonical in DAILY_INTERVALS:
            if at == "commodity":
                rows = await client.get_commodity_price(symbol, from_date=from_date, to_date=to_date)
            elif at == "crypto":
                rows = await client.get_crypto_price(symbol, from_date=from_date, to_date=to_date)
            else:
                rows = await client.get_forex_price(symbol, from_date=from_date, to_date=to_date)
        elif at == "commodity":
            rows = await client.get_commodity_intraday_chart(
                symbol, canonical, from_date=from_date, to_date=to_date
            )
        elif at == "crypto":
            rows = await client.get_crypto_intraday_chart(
                symbol, canonical, from_date=from_date, to_date=to_date
            )
        else:
            rows = await client.get_forex_intraday_chart(
                symbol, canonical, from_date=from_date, to_date=to_date
            )
    except Exception as e:  # noqa: BLE001
        return error_from_exception(
            e, "FMP fetch failed.", symbol=display, interval=canonical
        )

    data = _display_rows(rows or [], symbol, intraday=intraday)
    return make_response(
        data,
        source="fmp",
        symbol=display,
        interval=canonical,
        currency=currency,
        timezone=timezone,
        asset_type=at,
    )


@mcp.tool()
async def get_short_data(
    symbol: str,
    data_type: Literal["short_interest", "short_volume", "both"] = "both",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 20,
) -> dict:
    """Fetch short interest and/or short volume for a US stock (short-squeeze work).

    Short interest is FINRA bi-monthly (settlement_date); short volume is daily
    off-exchange (date). ginlix-data only.

    Args:
        symbol: Stock ticker, e.g. "AAPL".
        data_type: "short_interest", "short_volume", or "both" (default).
        from_date: "YYYY-MM-DD" start filter (optional).
        to_date: "YYYY-MM-DD" end filter (optional).
        limit: Max records per section (default 20, max 50000).

    Returns:
        dict: {symbol, timezone, count, data, source, data_type}. data is a dict
        of {short_interest, short_volume} lists (requested sections only), newest
        first. Record fields are vendor-native (e.g. settlement_date,
        short_volume_ratio). Partial failures appear in an `errors` map. On
        error: {error: <code>, detail}.
    """
    display, legacy, _currency, timezone = _resolve(symbol)

    if not await _ginlix.ensure():
        return make_error(
            "client_unavailable",
            "Short data requires ginlix-data (not configured).",
            symbol=display,
        )

    raw = await _ginlix.fetch_short_data(
        legacy, data_type=data_type, from_date=from_date, to_date=to_date, limit=limit,
    )

    data: dict[str, list] = {}
    errors: dict[str, str] = {}
    for section in ("short_interest", "short_volume"):
        if section in raw:
            data[section] = raw[section]
        if f"{section}_error" in raw:
            errors[section] = raw[f"{section}_error"]

    if not data:
        return make_error(
            "upstream_error",
            "; ".join(errors.values()) or "No short data returned.",
            symbol=display,
            data_type=data_type,
        )

    extra: dict[str, Any] = {"data_type": data_type}
    if errors:
        extra["errors"] = errors
    return make_response(
        data,
        source="ginlix-data",
        symbol=display,
        timezone=timezone,
        **extra,
    )


if __name__ == "__main__":
    mcp.run()
