#!/usr/bin/env python3
"""Options Data MCP Server.

Provides options contracts, OHLCV price data, and delayed snapshots via MCP.

Design goals:
- Standard agent-facing envelope (see mcp_servers/AGENT_CONTRACT.md)
- Complements native get_options_chain tool (which returns pre-formatted markdown)
- ginlix-data only (no FMP fallback)

Tools:
- get_options_chain: list options contracts with filters
- get_options_prices: historical OHLCV bars for an options contract
- get_options_snapshot: session prices, greeks, IV and open interest

Currency and timezone are derived from the UNDERLYING instrument via
market_protocol (US options: USD / America/New_York).
"""

# NOTE: Tool docstrings in this file are hand-tuned agent prompt surface (parsed
# into agent prompts and generated sandbox wrappers) and are content-pinned by
# tests/unit/mcp_servers/test_agent_contract.py. Read mcp_servers/AGENT_CONTRACT.md
# before editing; intentional changes must regenerate agent_docstring_lock.json.

from __future__ import annotations

try:
    from _bootstrap import MCPServer  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._bootstrap import MCPServer

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone as _tz
from typing import Any, Optional

from data_client.ginlix_data.option_snapshots import snapshot_row
from market_protocol.calendars import get_calendar, prebuild_calendars
from data_client.normalize import listing_currency
from data_client.ginlix_data import (
    close_ginlix_mcp_client,
    get_ginlix_mcp_client,
)
from market_protocol import market_of, to_canonical, to_display

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

from mcp_servers._dates import DateBoundError, daily_days

from mcp_servers._schemas import (
    BOOL,
    RECORDS,
    STR,
    TRUNCATED,
    described,
    envelope_schema,
    output_model,
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
    # Snapshot rows are dated on the XNYS calendar, whose first build takes
    # seconds; build it off the event loop before a snapshot needs it.
    warm = asyncio.create_task(asyncio.to_thread(prebuild_calendars, ("XNYS",)))
    try:
        yield
    finally:
        warm.cancel()
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
        return to_display(ref), listing_currency(ref), ref.tz
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

mcp = MCPServer("OptionsMCP", lifespan=_lifespan)


_OUT_GET_OPTIONS_CHAIN = output_model(
    "GetOptionsChainOut",
    envelope_schema(
        RECORDS,
        frame=("symbol", "currency", "timezone"),
        echo={
            "underlying_ticker": STR,
            "truncated": described(BOOL, "Present and true if more contracts match than limit."),
        },
    ),
)


@mcp.tool()
async def get_options_chain(
    underlying_ticker: str,
    contract_type: Optional[str] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    strike_price_gte: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    limit: int = 50,
) -> _OUT_GET_OPTIONS_CHAIN:
    """List an underlying's options contracts; use to find contract tickers.

    US-listed underlyings only; a listing on a known non-US venue is refused.

    Args:
        underlying_ticker: Underlying stock ticker, e.g. "AAPL".
        contract_type: "call" or "put" (default: both).
        expiration_date_gte: Min expiration "YYYY-MM-DD".
        expiration_date_lte: Max expiration "YYYY-MM-DD".
        strike_price_gte: Min strike price.
        strike_price_lte: Max strike price.
        limit: Max contracts (default 50, max 1000).

    Returns:
        dict: {symbol, underlying_ticker, currency?, timezone, count, data,
        source, truncated?}. data: list of {ticker, contract_type,
        expiration_date, strike_price, shares_per_contract, primary_exchange};
        strike_price in `currency` major units. truncated: true if more
        contracts match than limit. On error: {error: <code>, detail}.
    """
    try:
        ref = to_canonical(underlying_ticker)
        display, currency, timezone = to_display(ref), listing_currency(ref), ref.tz
    except Exception:  # noqa: BLE001
        ref, display, currency, timezone = None, underlying_ticker, None, None

    # ginlix-data lists US contracts only, so a listing it does not cover would
    # read as one with no options. A venue the protocol does not know stays.
    if ref is not None and market_of(ref) not in ("us", "other"):
        return make_error(
            "invalid_argument",
            f"Options chains cover US-listed underlyings only; {display} is not one.",
            symbol=display,
        )

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
        truncated=result.get("truncated", False),
        underlying_ticker=display,
    )


_OUT_GET_OPTIONS_PRICES = output_model(
    "GetOptionsPricesOut",
    envelope_schema(
        RECORDS,
        frame=("symbol", "interval", "currency", "timezone"),
        echo={"truncated": TRUNCATED},
    ),
)


@mcp.tool()
async def get_options_prices(
    options_ticker: str,
    from_date: str,
    to_date: str,
    interval: str = "1day",
) -> _OUT_GET_OPTIONS_PRICES:
    """Fetch OHLCV bars for one options contract.

    Intervals: 1min|5min|15min|30min|1hour|4hour|1day|1week|1month (aliases like
    "1d"/"daily" accepted).

    Args:
        options_ticker: OCC contract ticker, e.g. "O:AAPL260618C00220000".
        from_date: Start date "YYYY-MM-DD".
        to_date: End date "YYYY-MM-DD".
        interval: One of the intervals above.

    Returns:
        dict: {symbol, interval, currency, timezone, count, data, source,
        truncated?}. symbol echoes the contract ticker; currency/timezone are the
        underlying's. data: list of {date, open, high, low, close, volume}
        ascending (oldest first); date exchange-local "YYYY-MM-DD[ HH:MM:SS]";
        prices in `currency` major units. truncated: true if a size cap cut the
        oldest bars. On error: {error: <code>, detail}.
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

    # Bounds are read as the price tools read them, so a reversed or unreadable
    # one is refused in their words rather than by the vendor.
    try:
        daily_days(from_date, to_date)
    except DateBoundError as e:
        return error_from_exception(
            e, "Options prices fetch failed.", symbol=opt, interval=canonical
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

    data = list(reversed(result.bars))  # normalize_bars is descending → ascending
    return make_response(
        data,
        source="ginlix-data",
        symbol=opt,
        interval=canonical,
        currency=currency,
        timezone=timezone,
        truncated=result.truncated,
    )


_OUT_GET_OPTIONS_SNAPSHOT = output_model(
    "GetOptionsSnapshotOut",
    envelope_schema(RECORDS, frame=("currency", "timezone")),
)


@mcp.tool()
async def get_options_snapshot(
    options_tickers: str,
) -> _OUT_GET_OPTIONS_SNAPSHOT:
    """Options snapshots: session prices, greeks, IV and open interest.

    Args:
        options_tickers: OCC tickers, comma-separated, e.g. "O:AAPL260618C00220000".

    Returns:
        dict: {currency?, timezone?, count, data, source}. data: list of
        {ticker, name, market_status, session, greeks, implied_volatility,
        open_interest, break_even_price, underlying_asset, last_quote,
        last_trade}, keys if known.
        session: OHLCV, change, timeframe ("DELAYED": 15 min), last_updated
        (when the feed showed it), stale (true: not traded this session;
        close, change, volume are the last_updated day's); an undated session
        may be from any past day. greeks {delta, gamma, theta, vega};
        implied_volatility decimal; underlying_asset {ticker, price,
        last_updated, timeframe}. Times exchange-local "YYYY-MM-DD HH:MM:SS";
        prices in `currency` major units. On error: {error: <code>, detail}.
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

    # A call during the startup warm-up must not build the calendar on the loop.
    await asyncio.to_thread(get_calendar, "XNYS")
    now = datetime.now(_tz.utc)
    return make_response(
        [snapshot_row(row, now) for row in result.get("data", [])],
        source="ginlix-data",
        currency=currency,
        timezone=timezone,
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
