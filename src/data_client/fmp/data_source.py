"""FMP implementation of MarketDataSource.

Thin wrapper around :class:`FMPClient` that conforms to the
:class:`~src.data_client.base.MarketDataSource` protocol.

FMP stamps bars with exchange-local wall-clock strings and no timezone;
timestamps are localized per symbol (``symbol_timezone``) and bars are
returned ascending. Quotes/prices for LSE symbols arrive in GBX (pence);
both interfaces convert them to major units — the legacy bar/snapshot path
via ``minor_unit_places`` and the protocol path (:func:`normalize_series`)
independently, so conversion is applied exactly once per path.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.data_client.base import IntervalNotSupported
from src.data_client.normalize import (
    build_series,
    change_from,
    minor_unit_places,
    scale_price,
    scale_snapshot_prices,
    symbol_timezone,
)
from src.utils.cache.redis_cache import get_cache_client
from market_protocol import (
    InstrumentRef,
    Series,
    has_home_venue,
    to_canonical,
    vendor_spelling,
)
from market_protocol.calendars import get_calendar
from market_protocol.enums import AssetClass

from .fmp_client import FMPClient

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def _print_time_ms(timestamp: Any) -> int | None:
    """A quote's ``timestamp`` (Unix seconds) as ``as_of``. Without it a row
    printed in the previous session cannot be told from today's."""
    if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool) and timestamp > 0:
        return int(timestamp * 1000)
    return None


def _parse_row_time(row: dict[str, Any], tz: ZoneInfo) -> int:
    """Row ``date`` string (exchange-local wall clock) → Unix ms."""
    t = row.get("time")
    if t is not None:
        return t
    date_str = row.get("date", "")
    if not date_str:
        return 0
    try:
        fmt = "%Y-%m-%d %H:%M:%S" if " " in date_str else "%Y-%m-%d"
        dt = datetime.strptime(date_str, fmt).replace(tzinfo=tz)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def normalize_series(rows: list[dict[str, Any]], *, ref: InstrumentRef, schema: str) -> Series:
    """Normalize raw FMP rows to a protocol Series (FMP parses wall-clock dates)."""
    tz = ZoneInfo(ref.tz)
    return build_series(
        rows, ref=ref, schema=schema, publisher="fmp",
        ts_of=lambda row: _parse_row_time(row, tz),
    )


# Feeds round one close differently (40194.9 in a quote, 40194.91 in history).
_SAME_CLOSE = 1e-5
# A stale previousClose has been seen naming the close two sessions back; it is
# matched against the two daily closes before the last session's.
_STALE_DEPTH = 2
# Daily history read before a session: longer than any market holiday.
_PRIOR_DAYS = 21
# The closes before a session do not change, but FMP may publish the last one
# late, so a read is reused only briefly.
_PRIOR_TTL = 600
# History reads one index batch keeps in flight.
_HISTORY_READS = 8


def _positive(value: Any) -> float | None:
    """*value* as a usable price: a finite number above zero, else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value > 0 else None


def _same(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=_SAME_CLOSE)


def settled_previous_close(
    stated: float | None, prior: date, closes: dict[date, float],
) -> float | None:
    """A replacement for a quote's previous close, or None to keep *stated*.

    *prior* is the session before the quote's on its venue calendar and
    *closes* FMP's daily closes by date. FMP's field can name the close of two
    sessions back; one equal to a close before *prior*'s is replaced by
    *prior*'s. Any other field stands: a daily bar is not every venue's official
    close (FMP's DAX bar is a late-evening level, its quote the Xetra close), so
    a field matching no bar is taken as the official one. Until *prior*'s bar is
    published there is nothing to replace it with.
    """
    last = closes.get(prior)
    if last is None:
        return None
    if stated is None:
        return last
    earlier = [closes[d] for d in sorted(closes) if d < prior][-_STALE_DEPTH:]
    if _same(stated, last) or not any(_same(stated, c) for c in earlier):
        return None
    return last


async def _daily_closes(client: FMPClient, symbol: str, session: date) -> dict[date, float]:
    """FMP's daily closes for the weeks before *session*, cached per session."""
    cache = get_cache_client()
    key = f"fmp:prior_closes:{symbol}:{session.isoformat()}"
    by_day = await cache.get(key)
    if not isinstance(by_day, dict):
        rows = await client.get_eod_closes(
            symbol,
            (session - timedelta(days=_PRIOR_DAYS)).isoformat(),
            (session - timedelta(days=1)).isoformat(),
        )
        by_day = {
            str(row.get("date"))[:10]: close
            for row in (rows if isinstance(rows, list) else [])
            if (close := _positive(row.get("price"))) is not None
        }
        await cache.set(key, by_day, ttl=_PRIOR_TTL)
    return {date.fromisoformat(day): close for day, close in by_day.items()}


async def _check_previous_close(
    client: FMPClient, quote: dict[str, Any], reads: asyncio.Semaphore,
) -> dict[str, Any]:
    """*quote* with its previous close checked against FMP's daily closes.

    The quote's session is the trading date of its ``timestamp``, the last
    print, on its venue's calendar: never the UTC date, since Sydney's Friday
    starts on Thursday there. A quote on a venue the symbology does not know,
    or one that cannot be dated or checked, stays as FMP served it.
    """
    symbol = str(quote.get("symbol") or "")
    price, printed = _positive(quote.get("price")), _print_time_ms(quote.get("timestamp"))
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.INDEX)
    except ValueError:
        return quote
    if price is None or printed is None or not has_home_venue(ref):
        return quote
    try:
        cal = get_calendar(ref.calendar_id)
        session = cal.latest_trading_date(datetime.fromtimestamp(printed / 1000, timezone.utc))
        prior = cal.previous_session(session)
        async with reads:
            closes = await _daily_closes(client, symbol, session)
    except Exception:  # noqa: BLE001 - the served quote beats no quote
        logger.warning("fmp.previous_close.unchecked | symbol=%s", symbol, exc_info=True)
        return quote
    prev = settled_previous_close(_positive(quote.get("previousClose")), prior, closes)
    if prev is None:
        return quote
    change, change_pct = change_from(price, prev)
    logger.info(
        "fmp.previous_close.settled | symbol=%s session=%s served=%s settled=%s",
        symbol, session, quote.get("previousClose"), prev,
    )
    return {**quote, "previousClose": prev, "change": change, "changePercentage": change_pct}


class FMPDataSource:
    """Market data source backed by Financial Modeling Prep."""

    # FMP supports these intraday intervals; anything else should be rejected
    # so the chain can fall through to a provider that does support it.
    _SUPPORTED_INTERVALS = frozenset({"1min", "5min", "15min", "30min", "1hour", "4hour"})

    @staticmethod
    def _api_symbol(symbol: str, is_index: bool) -> str:
        # Suffixed index symbols (000300.SH) are already valid — never caret them.
        if not is_index or symbol.startswith("^") or "." in symbol:
            return symbol
        return f"^{symbol}"

    @staticmethod
    def _normalize(row: dict[str, Any], tz: ZoneInfo, places: int) -> dict[str, Any]:
        """Normalize a raw FMP bar to the standard OHLCV shape.

        FMP returns ``date`` as an exchange-local string
        (``"2024-01-15 09:30:00"`` or ``"2024-01-15"``) — localized with the
        symbol's exchange tz, NOT assumed ET. ``time`` is Unix ms to match
        the envelope contract. ``places`` is the minor-unit shift (2 for
        GBX/pence venues, else 0); volume is a share count, never scaled.
        """
        return {
            "time": _parse_row_time(row, tz),
            "open": scale_price(row.get("open", 0.0), places),
            "high": scale_price(row.get("high", 0.0), places),
            "low": scale_price(row.get("low", 0.0), places),
            "close": scale_price(row.get("close", 0.0), places),
            "volume": int(row.get("volume") or 0),
        }

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if interval not in self._SUPPORTED_INTERVALS:
            raise IntervalNotSupported(
                f"Interval '{interval}' is not supported by this data source"
            )
        api_symbol = self._api_symbol(symbol, is_index)
        tz = symbol_timezone(symbol)
        places = minor_unit_places(symbol)
        async with FMPClient() as client:
            data = await client.get_intraday_chart(
                symbol=api_symbol,
                interval=interval,
                from_date=from_date,
                to_date=to_date,
            )
        bars = [self._normalize(bar, tz, places) for bar in (data or [])]
        bars.sort(key=lambda b: b["time"])
        return bars

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = self._api_symbol(symbol, is_index)
        tz = symbol_timezone(symbol)
        places = minor_unit_places(symbol)
        async with FMPClient() as client:
            data = await client.get_stock_price(
                symbol=api_symbol,
                from_date=from_date,
                to_date=to_date,
            )
        bars = [self._normalize(bar, tz, places) for bar in (data or [])]
        bars.sort(key=lambda b: b["time"])
        return bars

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch batch snapshots via FMP batch quote endpoint.

        Uses a single batch call. Extended-hours fields (early/late trading)
        are not available from FMP and returned as None — the frontend
        gracefully hides them when absent.
        """
        api_symbols: list[str] = []
        refused: ValueError | None = None
        for s in symbols:
            api_symbol = self._api_symbol(s, is_index=(asset_type == "indices"))
            try:
                vendor_spelling(api_symbol)
            except ValueError as exc:
                # One symbol FMP cannot be asked for must not fail the batch
                # quote for the rest; it is a miss the chain moves on.
                refused = refused or exc
            else:
                api_symbols.append(api_symbol)
        if not api_symbols:
            if refused is not None:
                raise refused
            return []
        async with FMPClient() as client:
            quotes = await client.get_batch_quotes(api_symbols) or []
            if asset_type == "indices":
                # FMP's previousClose for an index can be the close of two
                # sessions back, so a two-day move reads as one day's.
                reads = asyncio.Semaphore(_HISTORY_READS)
                quotes = await asyncio.gather(*(_check_previous_close(client, q, reads) for q in quotes))
        return [self._normalize_quote(q, asset_type) for q in quotes]

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Derive market status from current time (FMP has no dedicated endpoint)."""
        from src.utils.market_hours import current_market_phase

        phase = current_market_phase()
        return {
            "market": "open" if phase == "open" else ("extended-hours" if phase in ("pre", "post") else "closed"),
            "afterHours": phase == "post",
            "earlyHours": phase == "pre",
            "serverTime": datetime.now(_ET).isoformat(),
            "exchanges": None,
        }

    @staticmethod
    def _normalize_quote(q: dict[str, Any], asset_type: str = "stocks") -> dict[str, Any]:
        """Normalize an FMP quote response to the unified snapshot shape.

        Price-like fields are scaled to major units per the quote's own symbol
        (GBX venues → pounds), so a mixed-market batch converts each row
        correctly; change_percent is a ratio and volume a share count — both
        left untouched.
        """
        symbol = q.get("symbol", "")
        if asset_type == "indices":
            symbol = symbol.lstrip("^")
        snap = {
            "symbol": symbol,
            "name": q.get("name"),
            "price": q.get("price"),
            "change": q.get("change"),
            "change_percent": q.get("changePercentage"),
            "previous_close": q.get("previousClose"),
            "open": q.get("open"),
            "high": q.get("dayHigh"),
            "low": q.get("dayLow"),
            "volume": int(q["volume"]) if q.get("volume") is not None else None,
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
            # The regular session's print: extended hours have their own
            # endpoints, so after the close this stamp stays at the close.
            "as_of": _print_time_ms(q.get("timestamp")),
            "regular_only": True,
        }
        return scale_snapshot_prices(snap, minor_unit_places(symbol))

    async def close(self) -> None:
        pass  # FMPClient manages its own lifecycle per-request

# Backward-compatible alias
FMPPriceProvider = FMPDataSource
