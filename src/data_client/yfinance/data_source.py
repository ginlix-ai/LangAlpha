"""MarketDataSource implementation backed by yfinance.

Free fallback provider — requires no API key. Used when both ginlix-data
and FMP are unavailable (e.g. OSS / self-hosted deployments).
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from src.data_client.base import IntervalNotSupported
from src.data_client.normalize import (
    build_series,
    change_from,
    minor_unit_places,
    scale_snapshot_prices,
    symbol_timezone,
)
from market_protocol import InstrumentRef, Series, to_canonical, to_major_units
from market_protocol.calendars import get_calendar, session_bounds
from market_protocol.enums import AssetClass
from market_protocol.symbology import ALWAYS_24_7, FUTURES_MIC, WEEKDAYS_24_5
from src.utils.market_hours import interval_seconds

from .yahoo import yahoo_symbol, yahoo_ticker

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def normalize_series(rows: list[dict], *, ref: InstrumentRef, schema: str) -> Series:
    """Normalize yfinance bars (epoch-ms ``time`` already UTC-correct) to a Series."""
    return build_series(
        rows, ref=ref, schema=schema, publisher="yfinance",
        ts_of=lambda row: int(row["time"]) if row.get("time") else 0,
    )

# Map data_client interval names → yfinance interval strings.
# None means unsupported — raises ValueError so the chain can skip this source.
_INTERVAL_MAP: dict[str, str | None] = {
    "1s": None,
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1hour": "1h",
    "4hour": None,
}

# Default lookback when no from_date is given, keyed by yfinance interval.
# Two days inside Yahoo's caps (1m: 8d, sub-hour: 60d, 1h: 730d): the start is
# a date string Yahoo reads at exchange-local midnight, so a cap-boundary
# lookback overflows by the time-of-day plus any ET-to-exchange date shift and
# Yahoo rejects the whole request.
_DEFAULT_LOOKBACK: dict[str, timedelta] = {
    "1m": timedelta(days=6),
    "5m": timedelta(days=57),
    "15m": timedelta(days=57),
    "30m": timedelta(days=57),
    "1h": timedelta(days=727),
}


def _exclusive_end(end: str) -> str:
    """Advance an inclusive to_date to yfinance's exclusive ``end`` bound.

    The provider contract treats to_date as inclusive; yfinance's ``end`` is an
    exclusive exchange-local midnight, so passing it through drops the final
    trading day — including today's live session on windows ending today.
    """
    try:
        return (date.fromisoformat(end) + timedelta(days=1)).isoformat()
    except ValueError:
        return end


def _finite(v: Any) -> float | None:
    """Coerce to float, returning None for None/non-finite/unparseable values.

    None (not 0.0): a missing price coerced to zero reads as a real $0.00
    quote downstream, while None lets callers drop or omit the field.
    """
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _quoted(value: float) -> float:
    """*value* as Yahoo quoted it.

    Yahoo's chart series carry float32 prices, which widen to digits nobody
    quoted (4.62e-06 arrives as 4.619999799615471e-06). The shortest float32
    spelling restores the quote with no fixed number of places, so a sub-cent
    coin keeps its digits; a value float32 cannot hold exactly passes as is.
    """
    narrow = np.float32(value)
    return float(str(narrow)) if float(narrow) == value else value


def _normalize_bar(idx, row: Any, places: int = 0) -> dict[str, Any]:
    """Convert a yfinance history row to the standard OHLCV bar shape.

    ``places`` is the minor-unit shift (2 for GBX/pence venues, else 0);
    OHLC prices are scaled, volume (a share count) is not.
    """
    if hasattr(idx, "timestamp"):
        t = int(idx.timestamp() * 1000)
    else:
        t = 0
    return {
        "time": t,
        "open": to_major_units(_quoted(float(row["Open"])), places),
        "high": to_major_units(_quoted(float(row["High"])), places),
        "low": to_major_units(_quoted(float(row["Low"])), places),
        "close": to_major_units(_quoted(float(row["Close"])), places),
        # NaN != NaN: index/sparse rows carry NaN volume and int(NaN) raises,
        # killing the whole fetch for one bad row.
        "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
    }


def _fetch_history(
    symbol: str,
    interval: str,
    start: str | None,
    end: str | None,
    places: int = 0,
    day_tz: ZoneInfo | None = None,
) -> list[dict[str, Any]]:
    """Synchronous helper — called via ``asyncio.to_thread``.

    *day_tz* restamps each daily bar at midnight there: Yahoo stamps a day at
    midnight in its own exchange zone, London for FX, which in summer is the
    previous evening in the UTC an FX date is read in.
    """
    ticker = yahoo_ticker(symbol)

    # auto_adjust=False: Yahoo's Close is split-adjusted only (dividends live
    # in Adj Close, which we don't read) — the whole provider chain declares
    # price_treatment=split_adjusted, so conventions never blend across a
    # provider failover.
    kwargs: dict[str, Any] = {"interval": interval, "auto_adjust": False}
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = _exclusive_end(end)
    if not start and not end:
        lookback = _DEFAULT_LOOKBACK.get(interval, timedelta(days=730))
        kwargs["start"] = (datetime.now(_ET) - lookback).strftime("%Y-%m-%d")

    df = ticker.history(**kwargs)
    if df is None or df.empty:
        return []

    # A NaN-priced row (halted session, yfinance gap padding) would propagate
    # NaN into the cache and render as "nan" downstream — drop it here.
    df = df.copy().dropna(subset=["Open", "High", "Low", "Close"])
    if day_tz is not None:
        df.index = [datetime.combine(i.date(), time(), day_tz) for i in df.index]
    return [_normalize_bar(idx, row, places) for idx, row in df.iterrows()]


def _place_in_sessions(
    bars: list[dict[str, Any]], symbol: str, is_index: bool, interval: str,
) -> list[dict[str, Any]]:
    """Fit Yahoo's intraday grid around a venue's midday break.

    Yahoo pads the A-share 11:30-13:00 break with flat zero-volume bars, and
    its hourly grid, anchored at the open, starts a bar at 12:30 inside both
    the Shanghai and the Hong Kong break. A bar wholly inside a break carries no
    trading and is dropped; one that starts in it holds the reopen's trades, so
    it starts at the reopen.
    """
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.INDEX if is_index else None)
        tz = get_calendar(ref.calendar_id).tz
    except Exception:
        return bars
    span_ms = interval_seconds(interval) * 1000
    breaks: dict[date, tuple[int, int] | None] = {}
    placed = []
    for bar in bars:
        day = datetime.fromtimestamp(bar["time"] / 1000, tz).date()
        if day not in breaks:
            bounds = session_bounds(ref.calendar_id, day.isoformat())
            breaks[day] = (bounds[2], bounds[3]) if bounds and bounds[2] is not None else None
        brk = breaks[day]
        if brk is None or not brk[0] <= bar["time"] < brk[1]:
            placed.append(bar)
        elif bar["time"] + span_ms > brk[1]:
            placed.append({**bar, "time": brk[1]})
    return placed


def _print_time_ms(meta: dict[str, Any]) -> int | None:
    """The quote's print time, from the chart metadata the quote came with.
    Without it a row printed in the previous session cannot be told from
    today's."""
    t = meta.get("regularMarketTime")
    if isinstance(t, datetime):  # yfinance formats it as a pandas Timestamp
        return int(t.timestamp() * 1000)
    if isinstance(t, (int, float)) and not isinstance(t, bool) and t > 0:
        return int(t) * 1000
    return None


def _bar_field(bar: Any, field: str) -> float | None:
    value = _finite(bar[field]) if bar is not None else None
    return _quoted(value) if value is not None else None


def _sessions(
    ref: InstrumentRef | None, printed_ms: int | None, zone: str | None, dates: list[date],
) -> tuple[date | None, date | None]:
    """The trading date of the quote's last print, and the session before it.

    The venue's calendar answers when it is the one Yahoo quotes from (a
    futures evening already trades tomorrow). A venue the symbology has no
    calendar for gets the print's exchange-local date and the newest bar
    before it.
    """
    if printed_ms is None or not zone:
        return None, None
    at = datetime.fromtimestamp(printed_ms / 1000, ZoneInfo(zone))
    if ref is not None:
        cal = get_calendar(ref.calendar_id)
        if ref.mic == FUTURES_MIC or cal.tz.key == zone:
            session = cal.latest_trading_date(at)
            return session, cal.previous_session(session)
    session = at.date()
    return session, max((d for d in dates if d < session), default=None)


def _quoted_info_previous_close(ticker: Any) -> float | None:
    try:
        value = _finite((ticker.info or {}).get("regularMarketPreviousClose"))
        return _quoted(value) if value is not None else None
    except Exception:  # noqa: BLE001 - no previous close beats losing the quote
        logger.warning("yfinance.snapshot.no_previous_close | symbol=%s", ticker.ticker, exc_info=True)
        return None


def _fetch_single_snapshot(sym: str) -> dict[str, Any] | None:
    """Fetch snapshot for a single symbol. Returns None on failure.

    The previous close is the daily bar of the session before the quote's:
    its close is the official one (a closing auction, a futures settlement).
    Yahoo's daily series has holes for some A-share listings, so a missing bar
    falls back to a one-day chart's ``chartPreviousClose``, which is right
    there but runs a session stale on others (399001.SZ). A market that never
    closes takes the chart's day boundary, as other vendors quote it, where
    Yahoo's daily bar closes elsewhere (USD-JPY) or flat and rounded (a
    sub-cent coin). yfinance's ``fast_info`` takes the last hourly bar,
    extended hours included, of the previous calendar day: no session's close.
    """
    try:
        ref = to_canonical(sym)
    except ValueError:
        ref = None  # a spelling the protocol refuses still gets to ask Yahoo
    continuous = ref is not None and ref.calendar_id in (ALWAYS_24_7, WEEKDAYS_24_5)
    # Yahoo's running volume for an A-share index reads 2x to 10x low through
    # the day, and its intraday bars double-count the afternoon reopen, so no
    # count it publishes is the session's.
    a_share_index = (
        ref is not None and ref.asset_class is AssetClass.INDEX and ref.mic in ("XSHG", "XSHE")
    )
    try:
        ticker = yahoo_ticker(sym)
        days = ticker.history(period="1d" if continuous else "5d", interval="1d", auto_adjust=False)
        # A live view that the next history() call replaces, and copying it
        # would fetch its lazy tradingPeriods: read what the quote needs now.
        meta = ticker.history_metadata
        # NaN is truthy, so `nan or 0` stays NaN — route every numeric field
        # through _finite() so a snapshot can never carry non-finite floats.
        # No finite last price means no quote: a None snapshot beats a $0.00
        # phantom; missing optional fields stay None rather than becoming 0.
        price = _finite(meta.get("regularMarketPrice"))
        if price is None:
            # Yahoo has no such symbol, or yfinance swallowed the request's own
            # failure; either can be an outage, so it stays a warning.
            logger.warning("yfinance.snapshot.no_chart | symbol=%s", sym)
            return None
        printed = _print_time_ms(meta)
        if continuous:
            bar = days.iloc[-1] if not days.empty else None
            prev = _finite(meta.get("chartPreviousClose"))
        else:
            by_date = {idx.date(): row for idx, row in days.iterrows()}
            session, prior = _sessions(ref, printed, meta.get("exchangeTimezoneName"), list(by_date))
            bar = by_date.get(session)
            prev = _bar_field(by_date.get(prior), "Close")
            if (
                ref is not None
                and ref.mic == FUTURES_MIC
                and prior is not None
                and prior == max(by_date, default=None)
            ):
                # After the evening reopen Yahoo adds a partial bar and yfinance
                # folds it into the last daily bar, so that bar's Close is the
                # live price: only the quote's own field keeps the settlement.
                prev = _quoted_info_previous_close(ticker)
            elif prev is None:
                ticker.history(period="1d", interval="1d", auto_adjust=False)
                prev = _finite(ticker.history_metadata.get("chartPreviousClose"))
        volume = _finite(bar["Volume"]) if bar is not None and not a_share_index else None
        change, change_pct = change_from(price, prev)
        return {
            "symbol": sym,
            "name": None,
            "price": price,
            "change": change,
            "change_percent": change_pct,
            "previous_close": prev,
            "open": _bar_field(bar, "Open"),
            "high": _bar_field(bar, "High"),
            "low": _bar_field(bar, "Low"),
            "volume": int(volume) if volume is not None else 0,
            "market_status": None,
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
            # regularMarketTime: the regular session's last print, which
            # stands through the extended hours that follow.
            "as_of": printed,
            "regular_only": True,
        }
    except Exception:
        logger.warning("yfinance.snapshot.failed | symbol=%s", sym, exc_info=True)
        return None


class YFinanceDataSource:
    """Market data source backed by Yahoo Finance (yfinance library)."""

    @staticmethod
    def _api_symbol(symbol: str, is_index: bool) -> str:
        return yahoo_symbol(symbol, is_index=is_index)

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        yf_interval = _INTERVAL_MAP.get(interval)
        if yf_interval is None:
            raise IntervalNotSupported(
                f"Interval '{interval}' is not supported by yfinance"
            )
        api_symbol = self._api_symbol(symbol, is_index)
        places = minor_unit_places(symbol)
        bars = await asyncio.to_thread(
            _fetch_history, api_symbol, yf_interval, from_date, to_date, places
        )
        return _place_in_sessions(bars, symbol, is_index, interval)

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        api_symbol = self._api_symbol(symbol, is_index)
        places = minor_unit_places(symbol)
        return await asyncio.to_thread(
            _fetch_history, api_symbol, "1d", from_date, to_date, places, symbol_timezone(symbol)
        )

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        prepared: list[tuple[str, str]] = []  # (as requested, as Yahoo spells it)
        refused: ValueError | None = None
        for s in symbols:
            try:
                prepared.append((s, self._api_symbol(s, is_index=(asset_type == "indices"))))
            except ValueError as exc:
                # A symbol Yahoo cannot be asked for is a miss like any other,
                # not a failure of the whole batch.
                refused = refused or exc
        if not prepared:
            if refused is not None:
                raise refused
            return []
        results = await asyncio.gather(
            *(asyncio.to_thread(_fetch_single_snapshot, api) for _, api in prepared)
        )
        # gather preserves order, so results[i] maps back to prepared[i]. Restore
        # the originally-requested (bare) symbol — the caret was only for the
        # Yahoo query — so the provider chain matches on the requested ticker
        # instead of dropping "^GSPC" as unrequested. Keeps yfinance consistent
        # with FMP/ginlix-data, which already return bare index symbols.
        out: list[dict[str, Any]] = []
        for (original, _), snap in zip(prepared, results):
            if snap is None:
                continue
            snap["symbol"] = original
            # Scale price-like fields per the requested symbol (GBX → pounds).
            scale_snapshot_prices(snap, minor_unit_places(original))
            out.append(snap)
        return out

    async def get_market_status(
        self,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        from src.utils.market_hours import current_market_phase

        phase = current_market_phase()
        return {
            "market": (
                "open"
                if phase == "open"
                else ("extended-hours" if phase in ("pre", "post") else "closed")
            ),
            "afterHours": phase == "post",
            "earlyHours": phase == "pre",
            "serverTime": datetime.now(_ET).isoformat(),
            "exchanges": None,
        }

    async def close(self) -> None:
        pass
