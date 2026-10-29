"""CN A-share market data served by ginlix-data's protocol (v2) routes.

The upstream vendor is still Tushare, so this source sits in the chain under
the ``tushare`` name and every lineage, tier and envelope rule keyed on that
publisher keeps holding: prices are qfq (dividend-adjusted), quotes are the
realtime print.

A series ginlix-data does not have (an index's minutes, intraday older than
its history) returns empty, so the chain falls through exactly as before; an
outage raises, so the chain falls back rather than caching an empty answer.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from market_protocol import InstrumentRef, OhlcvBar, market_home, market_of, to_canonical
from market_protocol.intervals import schema_for_legacy

from src.data_client.base import FetchResult
from src.data_client.normalize import series_lineage

from .v2_routes import GinlixDataV2Routes

_CN_TZ = ZoneInfo(market_home("cn")[1])
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)
# The chain name this source publishes under; see the module docstring.
_PUBLISHER = "tushare"
# Widths ginlix-data aggregates CN minutes into; it has no 4-hour or seconds bars.
_INTRADAY_SCHEMAS = frozenset({"ohlcv-1m", "ohlcv-5m", "ohlcv-15m", "ohlcv-30m", "ohlcv-1h"})
# A daily fetch without a start runs back this far from its end, as on the US
# source: ginlix-data answers a missing start with the whole listed history
# (~6k bars for an old listing), all of which would land in the live cache entry.
_DAILY_LOOKBACK_DAYS = 365 * 2


def _cn_ref(symbol: str) -> InstrumentRef | None:
    """The ref of a CN listing; ``None`` for any other symbol.

    ginlix-data's v2 routes also serve other markets, so a chain or probe that
    reaches this source with a US symbol would otherwise get a US answer
    published under the ``tushare`` name.
    """
    try:
        ref = to_canonical(symbol)
    except ValueError:
        return None
    return ref if market_of(ref) == "cn" else None


def _check_lineage(header: dict[str, Any], ref: InstrumentRef) -> None:
    """Refuse a series whose header declares another publisher or price treatment.

    Every cache and envelope reads a series' lineage from the name it was
    published under, so a raw series served under ``tushare`` would be cached
    as qfq. A header without the fields keeps that declaration.
    """
    publisher = header.get("publisher")
    treatment = header.get("price_treatment")
    declared = series_lineage(_PUBLISHER, ref)[0]
    if (publisher and publisher != _PUBLISHER) or (treatment and treatment != declared):
        raise ValueError(
            f"ginlix-data served {ref.instrument_key} as {publisher}/{treatment}, "
            f"not {_PUBLISHER}/{declared.value}"
        )


def _day_bounds_ms(from_date: str | None, to_date: str | None) -> tuple[int | None, int | None]:
    """``YYYY-MM-DD`` bounds as Shanghai-day ms: start of the first, end of the last.

    The end is the day's own last millisecond (the later one, where the clock
    fell back at midnight), not the next midnight less one: 9999-12-31 has no
    next day, and its OverflowError would send the chain to another vendor.
    The route clamps a bound that far out.
    """

    def ms(d: str, *, last: bool = False) -> int:
        day = datetime.strptime(d[:10], "%Y-%m-%d").replace(tzinfo=_CN_TZ)
        if last:
            day = day.replace(hour=23, minute=59, second=59, microsecond=999_000, fold=1)
        return (day - _EPOCH) // _ONE_MS

    return (
        ms(from_date) if from_date else None,
        ms(to_date, last=True) if to_date else None,
    )


class GinlixDataCnSource:
    """``MarketDataSource`` for CN listings over ginlix-data v2."""

    def __init__(self, client: GinlixDataV2Routes) -> None:
        self.client = client

    async def _bars(
        self, symbol: str, schema: str, from_date: str | None, to_date: str | None,
        user_id: str | None,
    ) -> FetchResult:
        ref = _cn_ref(symbol)
        if ref is None:
            return FetchResult(bars=[])
        start, end = _day_bounds_ms(from_date, to_date)
        # Only a 404 (no vendor has the series) is empty. Anything else raises,
        # so a series pinned to this source falls back instead of caching [].
        wire = await self.client.get_bars_v2(
            ref.instrument_key, schema, start_ms=start, end_ms=end, user_id=user_id
        )
        if not wire:
            return FetchResult(bars=[])
        header = wire.get("header") or {}
        _check_lineage(header, ref)
        # A record the protocol refuses (a missing or null price) fails the whole
        # series, so the chain falls back instead of caching a bar every reader
        # of it would refuse.
        bars = [
            OhlcvBar.model_validate({**r, "volume": r.get("volume")})
            for r in wire.get("records") or []
        ]
        rows = [
            {
                "time": b.ts_event,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
            }
            for b in bars
        ]
        # A series short of its window (a vendor refetch that failed, a session
        # missing from the minute archive) is cached for a shorter time and
        # refilled whole rather than from its last bar.
        coverage = header.get("coverage") or {}
        return FetchResult(bars=rows, truncated=bool(coverage.get("truncated")))

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> FetchResult:
        if not from_date:
            end = (
                datetime.strptime(to_date[:10], "%Y-%m-%d") if to_date else datetime.now(_CN_TZ)
            )
            from_date = (end - timedelta(days=_DAILY_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        return await self._bars(symbol, "ohlcv-1d", from_date, to_date, user_id)

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> FetchResult:
        try:
            schema = schema_for_legacy(interval)
        except ValueError:
            schema = None
        if schema not in _INTRADAY_SCHEMAS:
            raise ValueError(f"Interval '{interval}' is not supported by this data source")
        return await self._bars(symbol, schema, from_date, to_date, user_id)

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Quotes stamped with the REQUESTED spelling: the chain drops rows it did not ask for."""
        if asset_type == "indices":
            return []
        wanted = {
            s: ref.instrument_key for s in dict.fromkeys(str(s).strip() for s in symbols)
            if s and (ref := _cn_ref(s)) is not None
        }
        if not wanted:
            return []
        # A failure raises rather than answering "no quotes": the chain then
        # records it and still moves every symbol on to the next provider.
        quotes = await self.client.get_quotes_v2(list(wanted), user_id=user_id)
        return [
            self._snapshot(sym, q) for sym, key in wanted.items()
            if (q := quotes.get(key)) is not None
        ]

    @staticmethod
    def _snapshot(symbol: str, q: dict[str, Any]) -> dict[str, Any]:
        volume = q.get("volume")
        row = {
            "symbol": symbol,
            "name": q.get("name"),
            "name_local": q.get("name_local"),
            "price": q.get("price"),
            "change": q.get("change"),
            "change_percent": q.get("change_percent"),
            "previous_close": q.get("previous_close"),
            "open": q.get("open"),
            "high": q.get("high"),
            "low": q.get("low"),
            "volume": int(volume) if volume is not None else None,
            "market_status": None,
            "as_of": q.get("as_of"),
            "early_trading_change_percent": None,
            "late_trading_change_percent": None,
        }
        # A quote without a tier gets the chain's declared tier for this source;
        # claiming one here would pre-empt it.
        if q.get("tier"):
            row["tier"] = q["tier"]
        return row

    async def close(self) -> None:
        pass  # the shared ginlix-data client is closed by its owner
