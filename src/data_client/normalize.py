"""Shared OHLCV bar normalization.

Single source of truth for converting the canonical
``{time, open, high, low, close, volume}`` shape to display format with
exchange-local timestamps.

Also holds the legacy-path minor-unit scale helpers: providers that quote a
venue in a currency's minor unit (XLON equities quote GBX/pence) shift
price-like fields by ``minor_unit_places`` on the legacy bar/snapshot path so
the wire values are major units. The protocol path applies the same rule
independently inside each provider's ``normalize_series`` — the two never
chain, so conversion happens exactly once per path.
"""

from __future__ import annotations

import calendar
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from market_protocol import (
    InstrumentRef,
    Series,
    market_home,
    to_canonical,
    to_major_units,
)
from market_protocol import build_series as _build_series
from market_protocol.calendars import get_calendar
from market_protocol.enums import AssetClass, PriceTreatment, Tier
from market_protocol.symbology import FUTURES_MIC, MINOR_UNIT_PLACES, UNKNOWN_MIC

# Snapshot fields whose values carry a price and therefore scale with the
# quote's minor-currency unit. change_percent (and pre/post percents) are
# ratios — scale-invariant; volume is a share count — never scaled.
_SNAPSHOT_PRICE_FIELDS = (
    "price", "change", "previous_close", "open", "high", "low",
)

# Realtime-quote row fields that carry a price. changePercentage is a ratio,
# volume and sharesOutstanding are counts, marketCap is already in the major
# unit, and eps is per share in the reporting currency, so none of them scale.
_QUOTE_PRICE_FIELDS = (
    "price", "change", "previousClose", "open", "dayHigh", "dayLow",
    "yearHigh", "yearLow", "priceAvg50", "priceAvg200",
)


class _Declared(NamedTuple):
    treatment: PriceTreatment
    tier: Tier  # bars
    quote_tier: Tier  # snapshots


# What each publisher's feed declares, ONE source of truth: the cache header
# builder, every boundary and the snapshot stamp resolve it through
# ``series_lineage`` / ``snapshot_tier``, so no layer re-mirrors these.
_DECLARED: dict[str, _Declared] = {
    "ginlix-data": _Declared(PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME, Tier.REALTIME),
    "fmp": _Declared(PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME, Tier.REALTIME),
    "yfinance": _Declared(PriceTreatment.SPLIT_ADJUSTED, Tier.DELAYED_15M, Tier.DELAYED_15M),
    # CN daily is qfq (前复权): raw `daily` scaled by adj_factor/latest_factor,
    # so the series is dividend/split continuous. Minute path is near-realtime
    # (rt_min); the bar tier is the conservative floor across both. Its
    # ``rt_k`` quote is a realtime print.
    "tushare": _Declared(PriceTreatment.DIVIDEND_ADJUSTED, Tier.DELAYED_15M, Tier.REALTIME),
}

# An unknown/absent publisher: bars keep the legacy hardcoded header fallback;
# a quote reads delayed, since a wrong "realtime" is the one label the header
# must never show.
_UNDECLARED = _Declared(PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME, Tier.DELAYED_15M)

# Publishers realtime only on the US consolidated tape and 15 minutes delayed on
# every other venue, bars and quotes alike.
_REALTIME_ON_US_TAPE_ONLY = frozenset({"fmp"})

# Publishers whose intraday bars cover only the regular session: FMP's
# ``historical-chart`` without ``extended`` and yfinance's history without
# ``prepost``. ginlix-data's aggregates carry pre-market and after-hours bars.
_REGULAR_SESSION_BARS = frozenset({"fmp", "yfinance"})

# Venues are keyed by :func:`declared_venue`: XNYS for the US tape, XSHG for
# all three CN exchanges. FMP's CN bars are the exchange's unadjusted prints:
# measured on 600519.SH across the June 2024 dividend, FMP's daily closes equal
# Tushare's raw ``daily``, not its qfq series.
_US_TAPE = market_home("us")[0]
_CN_CALENDAR = market_home("cn")[0]

# An index or a pair has no corporate action to adjust for.
NO_CORPORATE_ACTIONS = frozenset({AssetClass.INDEX, AssetClass.CRYPTO, AssetClass.FX})


def adjusts_nothing(asset_class: AssetClass | None, venue: str | None) -> bool:
    """Whether an instrument has no corporate action to adjust for: an index,
    a pair, or anything on CME Globex (a future, a commodity). *venue* is a
    :func:`declared_venue`."""
    return asset_class in NO_CORPORATE_ACTIONS or venue == FUTURES_MIC

# US dotted class-share suffixes (BRK.B, BF.B), kept from the pre-CMDP
# classifier. They live here, not in the instrument clock that also reads them,
# because this module ships to the sandbox, which has no ``src`` package.
_US_CLASS_SUFFIXES = {"A", "B", "C"}


def is_us_class_share(ref: InstrumentRef) -> bool:
    """A dotted US class share (BRK.B, BF.B). The protocol knows no such
    suffix, so the listing parses to the unknown venue; any other suffix there
    (NOVO-B.CO, PETR4.SA) is a foreign venue it does not know either."""
    stem, dot, suffix = ref.symbol.rpartition(".")
    return ref.mic == UNKNOWN_MIC and bool(dot and stem) and suffix.upper() in _US_CLASS_SUFFIXES


def declared_venue(ref: InstrumentRef | None) -> str | None:
    """The venue key a declaration ranks *ref* on; None for no instrument.

    A listing on a venue the protocol does not know takes XNYS as its default
    home, which would read as the US tape and earn a US-only realtime grant.
    It keys as the unknown venue instead, except a US class share.
    """
    if ref is None:
        return None
    if ref.mic == UNKNOWN_MIC and not is_us_class_share(ref):
        return UNKNOWN_MIC
    return ref.calendar_id


def _declared(publisher: str | None, venue: str | None) -> _Declared:
    declared = _DECLARED.get(publisher or "", _UNDECLARED)
    if publisher in _REALTIME_ON_US_TAPE_ONLY and venue and venue != _US_TAPE:
        return declared._replace(tier=Tier.DELAYED_15M, quote_tier=Tier.DELAYED_15M)
    return declared


def bars_regular_only(publisher: str | None) -> bool:
    """Whether *publisher*'s intraday bars stop at the regular session's edges."""
    return publisher in _REGULAR_SESSION_BARS


def snapshot_tier(publisher: str | None, venue: str | None = None) -> Tier:
    """Freshness of *publisher*'s quote on *venue* (a :func:`declared_venue`; None = publisher-wide)."""
    return _declared(publisher, venue).quote_tier


def publisher_lineage(
    publisher: str | None,
    asset_class: AssetClass | None = None,
    venue: str | None = None,
) -> tuple[PriceTreatment, Tier]:
    """(price_treatment, tier) declared for *publisher*'s bars; conservative default.

    The per-class rule behind :func:`series_lineage`, kept for a caller that
    knows an asset class but has no instrument (the data probe declaring a
    whole routing cell). Tushare's qfq join exists only for equities
    (``adj_factor``), and an index, a pair or a futures contract is unadjusted
    from every publisher. *venue* is a :func:`declared_venue`; None keeps the
    publisher-wide declaration.
    """
    treatment, tier, _ = _declared(publisher, venue)
    if adjusts_nothing(asset_class, venue) or (
        publisher == "tushare" and asset_class is AssetClass.FUND
    ):
        treatment = PriceTreatment.RAW
    elif publisher == "fmp" and venue == _CN_CALENDAR:
        treatment = PriceTreatment.RAW
    if publisher == "ginlix-data" and asset_class is AssetClass.INDEX:
        # Its index feed is the 15-minute delayed one (the live index stream is
        # delayed-only too); declared realtime, a daily series would settle at
        # 16:05 ET on a pre-close value and be held until the next open.
        tier = Tier.DELAYED_15M
    return treatment, tier


def series_lineage(
    publisher: str | None, ref: InstrumentRef | None
) -> tuple[PriceTreatment, Tier]:
    """(price_treatment, tier) of *publisher*'s series for *ref*.

    Lineage is per series, not per publisher. None (an unresolvable symbol)
    keeps the publisher-wide declaration.
    """
    if ref is None:
        return publisher_lineage(publisher)
    return publisher_lineage(publisher, ref.asset_class, declared_venue(ref))


# Widths of the intraday intervals, by their legacy spelling and yfinance's
# native 2m and 90m.
_INTRADAY_SECONDS = {
    "1s": 1, "1min": 60, "2m": 120, "5min": 300, "15min": 900, "30min": 1800,
    "1hour": 3600, "90m": 5400, "4hour": 14400,
}

# How far behind the venue a series of each tier runs.
_TIER_LAG_S = {Tier.DELAYED_15M: 900}

# Each venue's regular weekday close (exchange_calendars 4.13.2), and the
# after-hours close where it has one, for a runtime without exchange_calendars.
# Half days and holidays are not modelled. Globex closes in Chicago whatever
# zone a ref's labels use (a commodity's are New York's).
_REGULAR_CLOSE = {
    "XAMS": time(17, 30), "XASX": time(16), "XBOM": time(15, 30), "XETR": time(17, 30),
    "XHKG": time(16), "XKRX": time(15, 30), "XLON": time(16, 30), "XMAD": time(17, 30),
    "XMIL": time(17, 30), "XNYS": time(16), "XPAR": time(17, 30), "XSES": time(17),
    "XSHG": time(15), "XSWX": time(17, 30), "XTAI": time(13, 30), "XTKS": time(15, 30),
    "XTSE": time(16),
    FUTURES_MIC: time(16, tzinfo=ZoneInfo("America/Chicago")),
}
_EXTENDED_CLOSE = {"XNYS": time(20)}


def _period_last_day(first: date, interval: str) -> date:
    """The last calendar day a bar opening on *first* covers."""
    if interval in ("1week", "5d"):
        return first + timedelta(days=6)
    months = {"1month": 1, "3mo": 3}.get(interval)
    if months:
        year, month = divmod(first.month - 1 + months - 1, 12)
        year += first.year
        return date(year, month + 1, calendar.monthrange(year, month + 1)[1])
    return first


def _session_end_ms(cal: Any, day: date, regular_only: bool) -> int | None:
    """When session *day*'s bars stop changing: the after-hours close where the
    venue has one and the series takes it, else the close; None when *day* has
    no session."""
    extended = None if regular_only else getattr(cal, "extended_bounds_ms", None)
    hours = extended(day) if extended else None
    return hours[1] if hours else cal.session_close_ms(day)


def _fixed_session_end_ms(ref: InstrumentRef, day: date, regular_only: bool) -> int | None:
    """:func:`_session_end_ms` from the venue's fixed weekday hours; None on a
    weekend or for a venue without them."""
    close = None if regular_only else _EXTENDED_CLOSE.get(ref.calendar_id)
    close = close or _REGULAR_CLOSE.get(ref.calendar_id)
    if close is None or day.weekday() >= 5:
        return None
    return int(datetime.combine(day, close, close.tzinfo or ZoneInfo(ref.tz)).timestamp() * 1000)


def bar_in_progress(
    label: str | None,
    ref: InstrumentRef | None,
    interval: str,
    now: datetime | None = None,
    *,
    tier: Tier | None = None,
    regular_only: bool = False,
) -> bool:
    """Whether the bar labelled *label* is still forming at *now*.

    *label* is the exchange-local date or datetime :func:`normalize_bars`
    writes, and *interval* is canonical (``1min`` ... ``1month``). An intraday bar
    forms until its width or its session runs out, the session being the next
    day's where it opens the evening before (CME Globex); a longer bar until the last
    session it covers ends, after-hours included, since a US daily aggregate
    keeps taking after-hours prints until 20:00 ET. An index, or a
    *regular_only* series, settles at the regular close. A delayed *tier* reaches
    each of those moments its delay late. No vendor marks this, so it comes
    from the venue calendar; a runtime without ``exchange_calendars`` takes
    the venue's fixed weekday close instead. At an unknown venue a bar is
    forming while its period reaches the venue's today.
    """
    if not label or ref is None:
        return False
    now = (now or datetime.now(timezone.utc)) - timedelta(seconds=_TIER_LAG_S.get(tier, 0))
    tz = ZoneInfo(ref.tz)
    today = now.astimezone(tz).date()
    seconds = _INTRADAY_SECONDS.get(interval)
    try:
        if seconds is not None:
            opened = datetime.fromisoformat(label).replace(tzinfo=tz)
            if now >= opened + timedelta(seconds=seconds):
                return False
            first, last = opened.date(), opened.date() + timedelta(days=1)
        else:
            first = date.fromisoformat(label[:10])
            last = _period_last_day(first, interval)
    except ValueError:
        return False
    if last < today:
        return False
    if ref.mic == UNKNOWN_MIC and not is_us_class_share(ref):
        return True
    start = first if seconds is not None else max(first, today)
    regular_only = regular_only or ref.asset_class is AssetClass.INDEX
    days = [start + timedelta(days=i) for i in range((last - start).days + 1)]
    try:
        cal = get_calendar(ref.calendar_id)
        ends = [_session_end_ms(cal, day, regular_only) for day in days]
    except (ImportError, ValueError):
        ends = [_fixed_session_end_ms(ref, day, regular_only) for day in days]
    now_ms = int(now.timestamp() * 1000)
    if seconds is not None:
        opened_ms = int(opened.timestamp() * 1000)
        end = next((end for end in ends if end is not None and end > opened_ms), None)
        return end is not None and end > now_ms
    return any(end is not None and end > now_ms for end in ends)


def series_in_progress(
    publisher: str | None, ref: InstrumentRef | None, label: str | None, interval: str
) -> bool:
    """:func:`bar_in_progress` for *publisher*'s series of *ref*: late by its
    tier's delay, and ending at the regular close where its bars do."""
    return bar_in_progress(
        label, ref, interval,
        tier=series_lineage(publisher, ref)[1], regular_only=bars_regular_only(publisher),
    )


# FMP's CN intraday bars carry a single print's size on an unpredictable subset
# of bars (6x to 48x below Yahoo's on 600519.SH), and no other source holds
# whole CN minute sessions to read instead.
_CN_INTRADAY_VOLUME_NOTE = (
    "FMP reports a fraction of the real volume on some CN intraday bars; "
    "volume totals and volume-weighted figures understate."
)


def series_labels(
    publisher: str | None,
    ref: InstrumentRef | None,
    interval: str,
    newest: str | None,
) -> dict[str, str]:
    """Envelope keys for a bar series: the price treatment *publisher* declares
    for *ref*, ``in_progress`` naming the newest bar while it still forms, and
    ``volume_note`` where the volume is known to be unreliable."""
    labels = {"price_treatment": series_lineage(publisher, ref)[0].value}
    if series_in_progress(publisher, ref, newest, interval):
        labels["in_progress"] = newest
    if (
        publisher == "fmp"
        and interval in _INTRADAY_SECONDS
        and ref is not None
        and ref.calendar_id == _CN_CALENDAR
    ):
        labels["volume_note"] = _CN_INTRADAY_VOLUME_NOTE
    return labels


def ref_for_key(instrument_key: str | None) -> InstrumentRef | None:
    """The ref an instrument key names, or None when it does not parse."""
    if not instrument_key:
        return None
    try:
        return to_canonical(instrument_key)
    except ValueError:
        return None


def build_series(
    rows: list[dict[str, Any]],
    *,
    ref: InstrumentRef,
    schema: str,
    publisher: str,
    ts_of: Callable[[dict[str, Any]], int],
) -> Series:
    """Canonical raw-rows to a protocol :class:`Series`, lineage from :func:`series_lineage`.

    The builder itself is the protocol's; this layer only knows which lineage
    each langalpha publisher declares.
    """
    treatment, tier = series_lineage(publisher, ref)
    return _build_series(
        rows, ref=ref, schema=schema, publisher=publisher,
        price_treatment=treatment, tier=tier, ts_of=ts_of,
    )


def minor_unit_places(symbol: str) -> int:
    """Legacy-path decimal places to *symbol*'s major unit: 2 when quoted in GBX (pence).

    Resolved once per fetch from the canonical instrument's ``display_unit``
    (XLON equities quote in pence). Falls back to 0 for anything unresolvable.
    """
    try:
        return MINOR_UNIT_PLACES.get(to_canonical(symbol).display_unit or "", 0)
    except Exception:
        return 0


def scale_price(value: Any, places: int) -> Any:
    """*value* shifted *places* decimal places to major units; None/non-numeric pass through.

    Zero places returns the value untouched (identity, no float cast),
    preserving exact legacy behavior for non-pence instruments.
    """
    if not places or value is None:
        return value
    try:
        return to_major_units(float(value), places)
    except (TypeError, ValueError):
        return value


def scale_snapshot_prices(snap: dict, places: int) -> dict:
    """In-place shift of price-like snapshot fields to major units; percents/volume untouched."""
    if places:
        for field in _SNAPSHOT_PRICE_FIELDS:
            if snap.get(field) is not None:
                snap[field] = scale_price(snap[field], places)
    return snap


def scale_quote_prices(rows: list[dict], symbol: str) -> list[dict]:
    """Quote or profile *rows* with their price fields in *symbol*'s major unit.

    FMP and Yahoo quote London listings in pence while the snapshot and bars are
    already scaled to pounds, so a quote field read raw would sit beside them
    100x off. Returns new rows; the input is left as the provider sent it.
    """
    places = minor_unit_places(symbol)
    if not places:
        return rows
    return [
        {**row, **{f: scale_price(row[f], places) for f in _QUOTE_PRICE_FIELDS if f in row}}
        for row in rows
    ]


def change_from(
    price: float, prev_close: Any, ndigits: int | None = None
) -> tuple[float | None, float | None]:
    """``(change, change_percent)`` of *price* against *prev_close*.

    Both None without a previous close. With *ndigits* the percent derives from
    the rounded change, the way a synthesized quote row has always reported it.
    """
    if not prev_close:
        return None, None
    prev = float(prev_close)
    change = price - prev
    if ndigits is None:
        return change, change / prev * 100
    change = round(change, ndigits)
    return change, round(change / prev * 100, ndigits)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _field(bar: dict, name: str, short: str) -> Any:
    """*name*, or its one-letter form when *name* is absent; a zero is a value, not a gap."""
    value = bar.get(name)
    return bar.get(short) if value is None else value


def populated(data: Any) -> bool:
    """Whether a provider answer is worth serving; ``{"results": []}`` is a miss.

    The rule a routed source and the CN MCP branches share, so an empty answer
    falls through to the default provider the same way everywhere.
    """
    return any(data.values()) if isinstance(data, dict) else bool(data)


def symbol_timezone(symbol: str) -> ZoneInfo:
    """Exchange-local timezone for a symbol, via the canonical instrument.

    Falls back to ET for anything unresolvable — offset-identical to the old
    region map, but per-venue accurate for European suffixes.
    """
    try:
        return ZoneInfo(to_canonical(symbol).tz)
    except Exception:
        return ZoneInfo("America/New_York")


def normalize_bars(
    bars: list[dict],
    symbol: str,
    *,
    intraday: bool = False,
) -> list[dict]:
    """Convert bars from internal format (Unix ms) to display format.

    Timestamps are converted to exchange-local time for the given symbol.
    Output: ``{date, open, high, low, close, volume}``, descending by date.
    """
    tz = symbol_timezone(symbol)
    normalized = []
    for bar in bars:
        ts = bar.get("time") or bar.get("t")
        if ts is not None:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(tz)
            fmt = "%Y-%m-%d %H:%M:%S" if intraday else "%Y-%m-%d"
            date_str = dt.strftime(fmt)
        else:
            date_str = bar.get("date", "")
        normalized.append({
            "date": date_str,
            "open": _as_float(_field(bar, "open", "o")),
            "high": _as_float(_field(bar, "high", "h")),
            "low": _as_float(_field(bar, "low", "l")),
            "close": _as_float(_field(bar, "close", "c")),
            "volume": _as_float(_field(bar, "volume", "v")),
        })
    normalized.sort(key=lambda r: r["date"], reverse=True)
    return normalized
