#!/usr/bin/env python3
"""Options Data MCP Server.

Provides options contracts, OHLCV price data, and real-time snapshots via MCP.

Design goals:
- Standard agent-facing envelope (see mcp_servers/AGENT_CONTRACT.md)
- Complements native get_options_chain tool (which returns pre-formatted markdown)
- ginlix-data only (no FMP fallback)

Tools:
- get_options_chain: list options contracts with filters
- get_options_prices: historical OHLCV bars for an options contract
- get_options_snapshot: real-time bid/ask, last trade, session data

Currency and timezone are derived from the UNDERLYING instrument via
src.market_protocol (US options: USD / America/New_York).
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
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP

from data_client.ginlix_data import (
    close_ginlix_mcp_client,
    get_ginlix_mcp_client,
)
from src.market_protocol import to_canonical, to_display

try:
    from _envelope import error_from_upstream, make_error, make_response, normalize_interval
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._envelope import (
        error_from_upstream,
        make_error,
        make_response,
        normalize_interval,
    )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_ginlix = get_ginlix_mcp_client()

# The ginlix-data option aggregates endpoint serves the full canonical vocab.
_OPTION_INTERVALS = {
    "1min", "5min", "15min", "30min", "1hour", "4hour", "1day", "1week", "1month",
}


@asynccontextmanager
async def _lifespan(app):
    try:
        yield
    finally:
        await close_ginlix_mcp_client()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _underlying_context(
    options_ticker: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve the option's underlying → ``(display, currency, timezone)``.

    OCC layout is ``[O:]<root><YYMMDD><C|P><strike*8>``; the root is everything
    before the trailing 15 chars. Currency/timezone come from the underlying's
    canonical instrument (US options → USD / America/New_York). Returns Nones
    when the root cannot be parsed or resolved.
    """
    t = options_ticker.strip().upper()
    if t.startswith("O:"):
        t = t[2:]
    if len(t) <= 15:
        return None, None, None
    root = t[:-15]
    if not root.isalnum():
        return None, None, None
    try:
        ref = to_canonical(root)
        return to_display(ref), ref.price_currency, ref.tz
    except Exception:  # noqa: BLE001
        return root, None, None


def _ginlix_error(result: dict, **context: Any) -> dict:
    """Contract error envelope from a ginlix-data client error dict."""
    return error_from_upstream(
        result.get("error", "ginlix-data request failed"), **context
    )


# ---------------------------------------------------------------------------
# MCP server + tools
# ---------------------------------------------------------------------------

mcp = FastMCP("OptionsMCP", lifespan=_lifespan)


@mcp.tool()
async def get_options_chain(
    underlying_ticker: str,
    contract_type: Optional[str] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    strike_price_gte: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    limit: int = 50,
) -> dict:
    """List options contracts for an underlying; use to discover/filter contracts.

    Filter by call/put, expiration range, and strike range. ginlix-data only.

    Args:
        underlying_ticker: Underlying stock ticker, e.g. "AAPL".
        contract_type: "call" or "put" (default: both).
        expiration_date_gte: Min expiration "YYYY-MM-DD".
        expiration_date_lte: Max expiration "YYYY-MM-DD".
        strike_price_gte: Min strike price.
        strike_price_lte: Max strike price.
        limit: Max contracts (default 50, max 1000).

    Returns:
        dict: {symbol, underlying_ticker, currency, timezone, count, data,
        source}. data: list of contract dicts with ticker, contract_type,
        expiration_date, strike_price, shares_per_contract, primary_exchange;
        strike_price in `currency` major units. On error: {error: <code>, detail}.
    """
    try:
        ref = to_canonical(underlying_ticker)
        display, currency, timezone = to_display(ref), ref.price_currency, ref.tz
    except Exception:  # noqa: BLE001
        display, currency, timezone = underlying_ticker, None, None

    if not await _ginlix.ensure():
        return make_error(
            "client_unavailable",
            "Options data requires ginlix-data (not configured).",
            symbol=display,
        )

    result = await _ginlix.fetch_options_chain(
        underlying_ticker,
        contract_type=contract_type,
        expiration_date_gte=expiration_date_gte,
        expiration_date_lte=expiration_date_lte,
        strike_price_gte=strike_price_gte,
        strike_price_lte=strike_price_lte,
        limit=limit,
    )
    if "error" in result:
        return _ginlix_error(result, symbol=display)

    return make_response(
        result.get("results", []),
        source="ginlix-data",
        symbol=display,
        currency=currency,
        timezone=timezone,
        underlying_ticker=display,
    )


@mcp.tool()
async def get_options_prices(
    options_ticker: str,
    from_date: str,
    to_date: str,
    interval: str = "1day",
) -> dict:
    """Fetch OHLCV bars for one options contract; use for option price history.

    Intervals: 1min|5min|15min|30min|1hour|4hour|1day|1week|1month (aliases like
    "1d"/"daily" accepted). ginlix-data only.

    Args:
        options_ticker: OCC contract ticker, e.g. "O:AAPL260618C00220000".
        from_date: Start date "YYYY-MM-DD" (required).
        to_date: End date "YYYY-MM-DD" (required).
        interval: Bar size; one of the intervals above.

    Returns:
        dict: {symbol, interval, currency, timezone, count, data, source}. symbol
        echoes the contract ticker; currency/timezone are the underlying's. data:
        list of {date, open, high, low, close, volume} ascending (oldest first);
        date exchange-local "YYYY-MM-DD[ HH:MM:SS]"; prices in `currency` major
        units. On error: {error: <code>, detail}.
    """
    opt = options_ticker.strip()
    _underlying, currency, timezone = _underlying_context(opt)

    canonical = normalize_interval(interval)
    if canonical is None or canonical not in _OPTION_INTERVALS:
        return make_error(
            "unsupported_interval",
            f"Interval {interval!r} is not supported by this tool.",
            symbol=opt,
            interval=interval,
            supported=sorted(_OPTION_INTERVALS),
        )

    if not await _ginlix.ensure():
        return make_error(
            "client_unavailable",
            "Options data requires ginlix-data (not configured).",
            symbol=opt,
        )

    result = await _ginlix.fetch_options_prices(
        opt, from_date=from_date, to_date=to_date, interval=canonical,
    )
    if isinstance(result, dict):
        return _ginlix_error(result, symbol=opt, interval=canonical)

    data = list(reversed(result))  # normalize_bars is descending → ascending
    return make_response(
        data,
        source="ginlix-data",
        symbol=opt,
        interval=canonical,
        currency=currency,
        timezone=timezone,
    )


@mcp.tool()
async def get_options_snapshot(
    options_tickers: str,
) -> dict:
    """Fetch real-time snapshots for options contracts; use for live quotes.

    Session OHLCV is always present; bid/ask and last trade populate during
    market hours. ginlix-data only.

    Args:
        options_tickers: One or more OCC tickers, comma-separated
            (e.g. "O:AAPL260618C00220000,O:AAPL260618C00230000").

    Returns:
        dict: {currency, timezone, count, data, source}. currency/timezone are
        the underlying's, set only when all contracts share one (else omitted).
        data: list of snapshot dicts, each with ticker, name, market_status,
        session (OHLCV + change), last_quote (bid/ask/midpoint), last_trade
        (price/size); prices in `currency` major units. On error:
        {error: <code>, detail}.
    """
    tickers = [t.strip() for t in options_tickers.split(",") if t.strip()]
    if not tickers:
        return make_error("invalid_argument", "No options tickers provided.")

    if not await _ginlix.ensure():
        return make_error(
            "client_unavailable",
            "Options data requires ginlix-data (not configured).",
            tickers=options_tickers,
        )

    # Currency/timezone only when the batch shares a single underlying identity.
    contexts = [_underlying_context(t) for t in tickers]
    currencies = {c for (_d, c, _tz) in contexts if c}
    timezones = {tz for (_d, _c, tz) in contexts if tz}
    currency = next(iter(currencies)) if len(currencies) == 1 else None
    timezone = next(iter(timezones)) if len(timezones) == 1 else None

    result = await _ginlix.fetch_options_snapshot(tickers)
    if "error" in result:
        return _ginlix_error(result, tickers=options_tickers)

    return make_response(
        result.get("data", []),
        source="ginlix-data",
        currency=currency,
        timezone=timezone,
    )


if __name__ == "__main__":
    mcp.run()
