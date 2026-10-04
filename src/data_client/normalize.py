"""Shared OHLCV bar normalization.

Single source of truth for converting the canonical
``{time, open, high, low, close, volume}`` shape to display format with
exchange-local timestamps.

Also holds the legacy-path minor-unit scale helpers: providers that quote a
venue in a currency's minor unit (XLON equities quote GBX/pence) multiply
price-like fields by ``minor_unit_scale`` on the legacy bar/snapshot path so
the wire values are major units. The protocol path applies the same rule
independently inside each provider's ``normalize_series`` — the two never
chain, so conversion happens exactly once per path.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from market_protocol import (
    InstrumentRef,
    Series,
    market_home,
    to_canonical,
)
from market_protocol import build_series as _build_series
from market_protocol.enums import AssetClass, PriceTreatment, Tier
from market_protocol.symbology import UNKNOWN_MIC

# Snapshot fields whose values carry a price and therefore scale with the
# quote's minor-currency unit. change_percent (and pre/post percents) are
# ratios — scale-invariant; volume is a share count — never scaled.
_SNAPSHOT_PRICE_FIELDS = (
    "price", "change", "previous_close", "open", "high", "low",
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
    (``adj_factor``). *venue* is a :func:`declared_venue`; None keeps the
    publisher-wide declaration.
    """
    treatment, tier, _ = _declared(publisher, venue)
    if publisher == "tushare" and asset_class in (AssetClass.INDEX, AssetClass.FUND):
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


def minor_unit_scale(symbol: str) -> float:
    """Legacy-path price scale for *symbol*: 0.01 when quoted in GBX (pence).

    Resolved once per fetch from the canonical instrument's ``display_unit``
    (XLON equities quote in pence). Falls back to 1.0 for anything unresolvable.
    """
    try:
        return 0.01 if to_canonical(symbol).display_unit == "GBX" else 1.0
    except Exception:
        return 1.0


def scale_price(value: Any, scale: float) -> Any:
    """Return ``value * scale`` for numerics; pass None/non-numeric through.

    A ``scale`` of 1.0 returns the value untouched (identity, no float cast),
    preserving exact legacy behavior for non-pence instruments.
    """
    if scale == 1.0 or value is None:
        return value
    try:
        return float(value) * scale
    except (TypeError, ValueError):
        return value


def scale_snapshot_prices(snap: dict, scale: float) -> dict:
    """In-place ×scale of price-like snapshot fields; percents/volume untouched."""
    if scale != 1.0:
        for field in _SNAPSHOT_PRICE_FIELDS:
            if snap.get(field) is not None:
                snap[field] = scale_price(snap[field], scale)
    return snap


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
            "open": _as_float(bar.get("open") or bar.get("o")),
            "high": _as_float(bar.get("high") or bar.get("h")),
            "low": _as_float(bar.get("low") or bar.get("l")),
            "close": _as_float(bar.get("close") or bar.get("c")),
            "volume": _as_float(bar.get("volume") or bar.get("v")),
        })
    normalized.sort(key=lambda r: r["date"], reverse=True)
    return normalized
