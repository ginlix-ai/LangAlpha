"""Shared envelope helpers for OHLCV cache services (daily + intraday).

Provides the envelope structure, parsing, delta-merge, and SWR staleness
check used by both DailyCacheService and IntradayCacheService.
"""

import time
from bisect import bisect_left
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config.core import get_infrastructure_config
from src.data_client.normalize import ref_for_key, series_lineage
from market_protocol import to_canonical
from market_protocol.enums import AssetClass, PriceTreatment, Tier
from market_protocol.intervals import schema_for_legacy
from src.data_client.instrument_clock import clock_for
from src.utils.market_hours import current_trading_date, interval_seconds

_ET = ZoneInfo("America/New_York")

# v4 (Phase 3): protocol Series container — header + records, keyed on
# instrument_key. v3 (legacy source-segmented keys) remains readable for
# dual-read of warm caches written before the cutover.
#
# Bump on ANY stored-contract change, additive included: closed-market TTLs
# freeze complete envelopes until the venue's next open, so an unversioned
# change keeps serving the old shape for up to a whole weekend after deploy.
# A mismatched version reads as a miss (refetch). v5: invalidates
# pre-release v4 rows written before `market_phase` joined the cache block.
# v6: caret index families (HSI, N225, FTSE, GDAXI, FCHI, STOXX50E) moved
# to their home venue, so their daily bars are stamped at venue midnight
# instead of New York midnight; a delta refresh onto a v5 envelope would
# splice the two encodings and draw the seam day twice.
# v7: the live series records the window it holds (``window``).
ENVELOPE_VERSION = 7
_ENVELOPE_V3 = 3

_SOFT_TTL_RATIO: float = get_infrastructure_config().redis.swr.soft_ttl_ratio
_TRUNCATED_TTL_RATIO = 0.25  # aggressive refresh for truncated data
_EMPTY_RESULT_TTL = 30  # short TTL for empty upstream results
# Floor between consecutive staleness-driven re-fetches. When the provider
# itself is behind (e.g. today's daily bar not yet published right after the
# open), or the listing stopped printing (a suspended stock), re-asking
# immediately can't yield newer data — without this floor every request would
# bypass the cache with a blocking fetch.
_STALE_REFETCH_COOLDOWN = 120

# Phase settledness ladder for the daily post-close settle check. A daily
# envelope written in a less-settled phase than the venue is in now holds a
# partial-day head candle frozen at fetch time; each rung climbed forces one
# refetch (cooldown-bounded) so the bar settles at the official close (post)
# and again at the consolidated close (closed).
_PHASE_SETTLEDNESS = {"pre": 0, "open": 0, "post": 1, "closed": 2}

def series_identity(symbol: str, interval: str, is_index: bool = False) -> tuple[str, str]:
    """(instrument_key, schema) for a legacy symbol + interval pair.

    ``is_index=False`` hints EQUITY (not autodetect): the endpoint already
    decided the asset class, and a bare index-family collision (COMP the
    stock) must never key equity bars under the index's series. Explicit
    ``^``/``I:`` spellings still resolve as indexes.
    """
    hint = AssetClass.INDEX if is_index else AssetClass.EQUITY
    ref = to_canonical(symbol, asset_class=hint)
    return ref.instrument_key, schema_for_legacy(interval)


def canonical_series_key(
    symbol: str,
    interval: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    is_index: bool = False,
    live: bool = True,
) -> str:
    """Phase 3 cache key: ``ohlcv:{instrument_key}:{schema}``.

    Every spelling of one instrument collapses to one key, and the publisher
    lives in the envelope header (single live data key per instrument — the
    pin decides who fills it). Historical windows append ``:{from}:{to}``.
    """
    instrument_key, schema = series_identity(symbol, interval, is_index)
    base = f"ohlcv:{instrument_key}:{schema}"
    if live:
        return base
    return f"{base}:{from_date}:{to_date}"


def pin_key(symbol: str, interval: str, is_index: bool = False) -> str:
    """``pin:{instrument_key}:{schema}`` → {"publisher": name}."""
    instrument_key, schema = series_identity(symbol, interval, is_index)
    return f"pin:{instrument_key}:{schema}"


def _build_envelope(
    bars: List[Dict[str, Any]],
    market_phase: str,
    complete: bool,
    stored_ttl: int = 0,
    truncated: bool = False,
    data_date: Optional[str] = None,
    instrument_key: Optional[str] = None,
    schema: Optional[str] = None,
    publisher: Optional[str] = None,
    revision: int = 0,
    lineage: Optional[tuple[PriceTreatment, Tier]] = None,
    window: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a v4 Series envelope (storage form).

    Protocol lineage lives in ``header``; cache-operational flags stay
    top-level. Records gain ``ts_event`` (bar-open ms UTC, aliasing the
    legacy ``time`` they already carry). A fill that already resolved the
    series' *lineage* passes it in rather than re-parsing the key.
    """
    watermark = bars[-1].get("time", 0) if bars else 0
    now = time.time()
    for b in bars:
        b.setdefault("ts_event", b.get("time"))
    treatment, tier = lineage or series_lineage(publisher, ref_for_key(instrument_key))
    return {
        "v": ENVELOPE_VERSION,
        "header": {
            "instrument_key": instrument_key,
            "schema": schema,
            "publisher": publisher,
            "price_treatment": treatment.value,
            "tier": tier.value,
            "feed_scope": "composite",
            "ts_unit": "ms",
            "latest_trading_date": data_date or current_trading_date(),
            "revision": revision,
            "asof": now,
            "coverage": {"truncated": truncated},
            "fetched_at": now,
            "watermark": watermark,
        },
        "records": bars,
        "market_phase": market_phase,
        "complete": complete,
        "stored_ttl": stored_ttl,
        "window": window or _NO_WINDOW,
    }


def adopt_v3_envelope(
    v3: Dict[str, Any],
    instrument_key: str,
    schema: str,
    publisher: Optional[str],
) -> Dict[str, Any]:
    """Translate a legacy v3 envelope into v4 storage form (dual-read adopt).

    Operational fields (fetched_at, watermark, data_date, TTL bookkeeping)
    are PRESERVED, not re-stamped — adoption is a format move, not a fresh
    fetch, so staleness cooldowns and soft-TTL windows keep their meaning.
    ``publisher`` comes from the legacy key's source segment; unknown header
    fields are filled conservatively from the publisher's declared defaults.
    The legacy live key carried no window, so the adopted series is taken as
    the default one it was read and written for.
    """
    bars = v3.get("bars") or []
    for b in bars:
        b.setdefault("ts_event", b.get("time"))
    treatment, tier = series_lineage(publisher, ref_for_key(instrument_key))
    return {
        "v": ENVELOPE_VERSION,
        "header": {
            "instrument_key": instrument_key,
            "schema": schema,
            "publisher": publisher,
            "price_treatment": treatment.value,
            "tier": tier.value,
            "feed_scope": "composite",
            "ts_unit": "ms",
            "latest_trading_date": v3.get("data_date"),
            "revision": 0,
            "asof": v3.get("fetched_at", 0),
            "coverage": {"truncated": bool(v3.get("truncated"))},
            "fetched_at": v3.get("fetched_at", 0),
            "watermark": v3.get("watermark", 0),
        },
        "records": bars,
        "market_phase": v3.get("market_phase"),
        "complete": v3.get("complete", False),
        "stored_ttl": v3.get("stored_ttl", 0),
        "window": fetch_window(None),
    }


def _parse_envelope(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize a stored envelope (v4 or legacy v3) to the working view.

    The working view is the flat dict the staleness stack reads
    (``bars/watermark/fetched_at/data_date/truncated/...``). v4 envelopes
    additionally expose ``header``; v3 envelopes (dual-read of warm caches
    written before the key cutover) pass through as-is with no header.
    Anything else → None (cache miss).
    """
    if not isinstance(raw, dict):
        return None
    if raw.get("v") == ENVELOPE_VERSION:
        header = raw.get("header")
        if not isinstance(header, dict) or "records" not in raw:
            return None
        return {
            "v": ENVELOPE_VERSION,
            "bars": raw["records"],
            "watermark": header.get("watermark", 0),
            "fetched_at": header.get("fetched_at", 0),
            "market_phase": raw.get("market_phase"),
            "complete": raw.get("complete", False),
            "stored_ttl": raw.get("stored_ttl", 0),
            "data_date": header.get("latest_trading_date"),
            "truncated": bool((header.get("coverage") or {}).get("truncated")),
            "window": raw.get("window") or _NO_WINDOW,
            "header": header,
        }
    if raw.get("v") == _ENVELOPE_V3 and "bars" in raw:
        return raw
    return None


# The window a live series holds. ``from`` is the earliest requested start
# (ISO date) the series is complete from; ``default`` says it also holds a
# fetch with no start, whose left edge each provider picks for itself. One
# live key serves every window of an instrument, so a reader serves from it
# only what the window covers, and a fill extends it rather than narrowing it.
_NO_WINDOW: Dict[str, Any] = {"from": None, "default": False}


def fetch_window(from_date: Optional[str]) -> Dict[str, Any]:
    """The window a fetch starting at *from_date* (None: provider default) fills."""
    if from_date is None:
        return {"from": None, "default": True}
    return {"from": from_date[:10], "default": False}


def union_window(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    starts = [s for s in (a.get("from"), b.get("from")) if s]
    return {"from": min(starts) if starts else None, "default": bool(a.get("default") or b.get("default"))}


def window_covers(
    envelope: Dict[str, Any], from_date: Optional[str], tz: Optional[ZoneInfo] = None,
) -> bool:
    """Whether the stored series holds everything a fetch from *from_date* returns.

    A default-window series covers any start on or after its first bar: the
    provider had nothing older to give inside that window.
    """
    window = envelope["window"]
    if from_date is None:
        return bool(window.get("default"))
    start = from_date[:10]
    if window.get("from") and start >= window["from"]:
        return True
    bars = envelope.get("bars")
    if window.get("default") and bars:
        first = watermark_to_date_str(_bar_time(bars[0]), tz=tz)
        return first is not None and start >= first
    return False


def bars_from(
    bars: List[Dict[str, Any]], from_date: Optional[str], tz: Optional[ZoneInfo] = None,
) -> List[Dict[str, Any]]:
    """The bars on or after *from_date* in the instrument's calendar.

    A live series can reach back past the window a request asked for, so the
    response is cut to that window and stays a function of the request.
    """
    if not from_date or not bars:
        return bars
    start = from_date[:10]
    for i, bar in enumerate(bars):
        day = watermark_to_date_str(_bar_time(bar), tz=tz)
        if day is not None and day >= start:
            return bars[i:] if i else bars
    return []


def extend_series(
    stored: Optional[Dict[str, Any]],
    bars: List[Dict[str, Any]],
    source: Optional[str],
    from_date: Optional[str],
    *,
    cut: bool = False,
    tz: Optional[ZoneInfo] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """``(bars, window)`` for a fetch from *from_date* landing on *stored*.

    A stored series from the same publisher that overlaps the fetch and
    agrees with it on final history keeps the bars the fetch does not reach:
    older ones in front, so a short window never narrows a longer series, and
    newer ones behind, so a lagging answer never drops what a stream added.
    Older bars need a final bar the fetch shares to check the seam against.
    The window widens to the stored one when the result reaches back as far;
    another publisher's series hands over nothing, its window included.
    *cut* marks a truncated series refilled on the default window: the older
    bars kept in front are the cut answer, so the window claims only the
    whole days they hold.
    """
    window = fetch_window(from_date)
    old = stored["bars"] if stored else []
    if not old:
        if stored and not bars:
            window = union_window(window, stored["window"])
        return bars, window
    if not bars or not _same_series(stored, bars, source):
        return bars, window
    first, last = _bar_time(bars[0]), _bar_time(bars[-1])
    watermark = stored.get("watermark")
    held = {_bar_time(b) for b in old}
    seam_checked = bool(watermark) and any(
        _bar_time(b) < watermark and _bar_time(b) in held for b in bars
    )
    older = [b for b in old if _bar_time(b) < first] if seam_checked else []
    merged = older + bars + [b for b in old if _bar_time(b) > last]
    if _bar_time(merged[0]) <= _bar_time(old[0]):
        window = union_window(window, stored["window"])
        if cut and older:
            day = date.fromisoformat(watermark_to_date_str(_bar_time(older[0]), tz=tz))
            window = {**window, "from": (day + timedelta(days=1)).isoformat()}
    return merged, window


def _same_series(stored: Dict[str, Any], bars: List[Dict[str, Any]], source: Optional[str]) -> bool:
    """Whether *bars* continue *stored*: its publisher, overlapping, same final history."""
    old = stored["bars"]
    return (
        bool(source)
        and source == (stored.get("header") or {}).get("publisher")
        and _bar_time(bars[0]) <= _bar_time(old[-1])
        and _bar_time(bars[-1]) >= _bar_time(old[0])
        and not splice_is_discontinuous(old, bars, stored.get("watermark"))
    )


def fill_keeps_stored(
    stored: Optional[Dict[str, Any]],
    fetched: List[Dict[str, Any]],
    series: List[Dict[str, Any]],
    source: Optional[str],
    head: Optional[str],
) -> bool:
    """Whether a sync fill leaves *stored* in place rather than writing *series*.

    The fill, *series* being *fetched* extended over *stored*, reaches less
    far back than the stored series and holds nothing newer: a start past
    the head's lookback cap, answered by a shorter stand-in or by nothing.
    It still replaces the series when the chain *head* answers with bars for
    a series a stand-in filled, or when the series' own publisher moved its
    final history (a split re-adjusted).
    """
    held = (stored or {}).get("bars")
    if not held:
        return False
    if series and (
        _bar_time(series[0]) <= _bar_time(held[0]) or _bar_time(series[-1]) > _bar_time(held[-1])
    ):
        return False
    publisher = (stored.get("header") or {}).get("publisher")
    if fetched and head and source == head != publisher:
        return False
    return not (source == publisher and splice_is_discontinuous(held, fetched, stored.get("watermark")))


def refill_from(envelope: Optional[Dict[str, Any]], tz: Optional[ZoneInfo] = None) -> Optional[str]:
    """Start date for refetching a stored live series whole.

    The window's own start, reached back to the first stored bar when the
    series also holds a default window (which may begin earlier), so the
    refetch never asks for less than the series holds. None for a series
    that holds only the default window.
    """
    if not envelope:
        return None
    window = envelope["window"]
    start = window.get("from")
    bars = envelope["bars"]
    if start and window.get("default") and bars:
        head = watermark_to_date_str(_bar_time(bars[0]), tz=tz)
        return min(start, head) if head else start
    return start


def _merge_bars(
    existing: List[Dict[str, Any]],
    delta: List[Dict[str, Any]],
    watermark,
) -> List[Dict[str, Any]]:
    """Merge delta bars into existing, keeping the immutable prefix intact.

    Everything before the watermark is immutable history.
    Delta replaces everything from the watermark onward.
    Delta may start earlier than the watermark (when from_date is a date
    string rather than a precise timestamp), so we filter it first.

    Gap fill: when the delta contains bars that predate the existing prefix
    (e.g. the initial load returned only recent bars), those earlier bars
    are prepended so the gap is filled on the next refresh.
    """
    if not existing:
        return delta
    if not delta:
        return existing

    # Find split point via bisect on the bar anchor (ts_event, legacy "time")
    times = [_bar_time(b) for b in existing]
    split_idx = bisect_left(times, watermark)

    # Filter delta to only bars at or after the watermark so we don't
    # re-introduce bars that are already in the immutable prefix.
    fresh = [b for b in delta if _bar_time(b) >= watermark]

    # Gap fill: delta bars that predate existing (partial initial load).
    first_existing_time = times[0] if times else 0
    gap_fill = [b for b in delta if 0 < _bar_time(b) < first_existing_time]

    if not fresh and not gap_fill:
        return existing

    return gap_fill + existing[:split_idx] + fresh


def _bar_time(bar: Dict[str, Any]) -> int:
    """Bar anchor ms: ``ts_event`` (protocol) with legacy ``time`` fallback."""
    return bar.get("ts_event") or bar.get("time", 0)


# Relative tolerance for the splice-discontinuity check. A provider
# correction or split re-adjustment moves final bars by percents (a 2:1
# split halves them); float noise across providers is ~1e-5.
_DISCONTINUITY_REL_TOL = 0.005


def splice_is_discontinuous(
    existing: List[Dict[str, Any]],
    delta: List[Dict[str, Any]],
    watermark,
) -> bool:
    """True when the delta disagrees with cached FINAL history — never splice.

    Compares overlapping bars strictly BEFORE the watermark (the bar at the
    watermark is the forming bar; its values legitimately change). A relative
    move beyond tolerance on any shared final bar means an adjustment or
    provider correction happened upstream — the cached prefix is no longer
    the same series, so the caller must discard and full-refetch (bumping
    the header ``revision``) instead of splicing mixed treatments.
    """
    if not existing or not delta or not watermark:
        return False
    by_time = {_bar_time(b): b for b in existing}
    for d in delta:
        t = _bar_time(d)
        if not t or t >= watermark:
            continue
        e = by_time.get(t)
        if e is None:
            continue
        for f in ("close", "open"):
            ev, dv = e.get(f), d.get(f)
            if ev is None or dv is None or not ev:
                continue
            if abs(dv - ev) / abs(ev) > _DISCONTINUITY_REL_TOL:
                return True
    return False


def watermark_to_date_str(watermark, tz: Optional[ZoneInfo] = None) -> Optional[str]:
    """Convert a watermark (Unix ms) to an exchange-local date string (YYYY-MM-DD).

    ``tz`` is the symbol's exchange timezone — delta-refresh from_date windows
    must be exchange-local or non-US deltas start on the wrong trading date.
    Defaults to ET for callers that are US-only by construction.
    """
    if not watermark or not isinstance(watermark, (int, float)) or watermark <= 0:
        return None
    dt_local = datetime.fromtimestamp(watermark / 1000, tz=timezone.utc).astimezone(tz or _ET)
    return dt_local.strftime("%Y-%m-%d")


def _is_stale_date(
    envelope: Dict[str, Any], now: Optional[datetime] = None, clock=None,
) -> bool:
    """Return True if the envelope's ``data_date`` is behind the current trading date.

    The trading date comes from the instrument's clock (calendar-correct for
    non-US symbols; XNYS by default) and is valid in every market
    phase, including weekends and holidays — no phase gate needed.
    """
    data_date = envelope.get("data_date")
    if not data_date:
        return True  # missing data_date — treat as stale
    return data_date != (clock or clock_for(None)).current_trading_date(now)


def is_watermark_stale(
    envelope: Dict[str, Any],
    interval: str,
    now: Optional[datetime] = None,
    symbol: Optional[str] = None,
    is_index: bool = False,
    clock=None,
) -> bool:
    """Return True if the envelope's watermark is meaningfully behind the
    most recent bar that *should* exist right now for this interval.

    Catches mid-session stagnation — when ``data_date`` still matches the
    current trading date but the watermark hasn't advanced because a prior
    delta-refresh failed or returned an empty / truncated response. Also
    catches the overnight case where the cache's last bar is from several
    trading days ago even though the date-level check alone might miss it
    (e.g. a ``data_date`` that got refreshed without the bars advancing).

    Daily (``1day``) is checked at the DATE level, plus a post-close settle
    check (an envelope written mid-session must refetch once the venue phase
    settles, or its head bar stays a frozen partial-day candle): the newest
    bar's trading date (in the instrument's exchange tz) vs the clock's
    ``expected_latest_daily_date()`` (the newest bar that should exist now —
    the previous trading day during pre-market, since today's daily bar
    doesn't appear until the session opens). This is the only backstop daily
    has against a ``data_date`` that was silently re-stamped with today's date
    by a prior refresh that fetched nothing new (``_is_stale_date`` alone can't
    catch that). Calendar-correct per instrument (Phase 3): non-US symbols
    are judged against their own exchange calendar. Fail-closed defaults
    remain — no symbol, unknown index families, and unrecognized suffixes
    (other than US class shares) skip the backstop entirely rather than risk
    reading permanently stale against the wrong calendar.

    Tolerance: ``2 * interval_period``. Absorbs provider delay plus small
    clock skew without hiding real stagnation.
    """
    if interval == "1day":
        # Date-level: stale when the newest bar's trading date is behind the
        # newest daily bar that should exist now. Uses the same watermark→date
        # conversion the daily delta-refresh trusts for its from_date.
        if symbol is None and clock is None:
            # Unclassifiable — fail closed.
            return False
        clock = clock or clock_for(symbol, is_index)
        if not getattr(clock, "daily_backstop", True):
            return False
        if not envelope.get("bars"):
            return False  # empty window — soft-TTL governs re-fetch timing
        if time.time() - envelope.get("fetched_at", 0) < _STALE_REFETCH_COOLDOWN:
            # Just fetched — the provider simply hasn't published a newer bar
            # yet; flagging stale again would re-fetch on every request.
            # Deliberately ahead of the corrupt-watermark check: a persistently
            # corrupt feed re-fetches once per cooldown, not per request.
            return False
        last_bar_date = watermark_to_date_str(envelope.get("watermark"), tz=clock.tz)
        if last_bar_date is None:
            return True  # bars present but unusable watermark — corrupt
        if last_bar_date < clock.expected_latest_daily_date(now):
            return True
        # Post-close settle: an envelope written mid-session holds a partial
        # head candle (OHLCV frozen at fetch time). Once the venue reaches a
        # more settled phase the head bar must be refetched or it serves the
        # partial values all evening.
        stored_phase = envelope.get("market_phase")
        if stored_phase is not None:
            now_phase = clock.market_phase(now)
            if _PHASE_SETTLEDNESS.get(now_phase, 0) > _PHASE_SETTLEDNESS.get(stored_phase, 0):
                return True
            # Same rung, not yet final: a fetch in the minutes after the close
            # (a venue with no post phase, or a regular-only index clock, goes
            # straight from open to closed) is stored closed but incomplete
            # while the feed's delay runs out, and no rung change follows to
            # revisit it. Once that delay has passed, refetch it final.
            if (
                stored_phase == "closed"
                and not envelope.get("complete")
                and is_settled_complete(
                    now_phase, envelope["bars"], clock, _envelope_tier(envelope), now,
                )
            ):
                return True
        return False
    # Empty envelopes (no bars in requested window) are not meaningfully stale
    # on a watermark basis — there's nothing to be behind. They're deliberately
    # cached with a short _EMPTY_RESULT_TTL to dampen fetch storms for symbols
    # with no data; the soft TTL path handles re-fetch timing.
    if not envelope.get("bars"):
        return False
    clock = clock or clock_for(symbol, is_index)
    if envelope.get("complete") and clock.is_closed(now):
        # The session's final form: no newer bar can exist before the open.
        return False
    if time.time() - envelope.get("fetched_at", 0) < _STALE_REFETCH_COOLDOWN:
        # As on daily, and ahead of the corrupt-watermark check for the same
        # reason: a listing that stopped printing answers a re-ask unchanged.
        return False
    watermark_ms = envelope.get("watermark") or 0
    if watermark_ms <= 0:
        # Bars exist but watermark is 0 — envelope is corrupt, treat as stale.
        return True
    expected_ms = clock.expected_latest_bar_ms(interval, now)
    if expected_ms <= 0:
        return False
    tolerance_ms = interval_seconds(interval) * 2 * 1000 + _tier_delay_ms(envelope)
    return watermark_ms < expected_ms - tolerance_ms


# Publication delay allowance per declared feed tier. A delayed feed's newest
# bar legitimately trails the clock by its delay; judging it against a
# realtime expectation flags every mid-session check stale and degenerates the
# cache into a full upstream refetch per request (frozen-0700.HK incident).
_TIER_DELAY_MS: Dict[str, int] = {Tier.DELAYED_15M.value: 15 * 60 * 1000}


def _tier_delay_ms(envelope: Dict[str, Any]) -> int:
    """Watermark allowance (ms) for the envelope's declared feed tier."""
    tier = (envelope.get("header") or {}).get("tier")
    return _TIER_DELAY_MS.get(tier, 0)


def _envelope_tier(envelope: Dict[str, Any]) -> Tier:
    """The envelope's declared feed tier; a headerless (v3) one reads as realtime,
    matching the strict tolerance :func:`_tier_delay_ms` gives it."""
    try:
        return Tier((envelope.get("header") or {}).get("tier"))
    except ValueError:
        return Tier.REALTIME


# How long after a venue's regular close its session may still print, before
# the feed's declared delay: a closing auction past the close the calendar
# knows (HKEX's runs to 16:10) plus its publication. Public so a quote of the
# same session settles on the same window.
CLOSE_SETTLE_GRACE = timedelta(minutes=15)
_CLOSE_SETTLE_GRACE_MS = int(CLOSE_SETTLE_GRACE.total_seconds() * 1000)


def is_settled_complete(
    phase: str,
    bars: List[Dict[str, Any]],
    clock,
    tier: Tier,
    now: Optional[datetime] = None,
) -> bool:
    """Whether a series fetched now may be stored as the session's final form.

    A complete envelope is never refreshed until the next open, so declaring it
    on the first post-close fetch freezes whatever a delayed feed had out at
    that moment: a 15-minute feed read at 15:00 stops at 14:44 and never gains
    the rest of the session. The flag waits until the feed's declared delay
    has run past the close. *tier* is the series' declared feed tier.
    """
    if phase != "closed" or not bars:
        return False
    close_ms = clock.expected_latest_bar_ms("1day", now)
    if close_ms <= 0:
        return True
    now_ms = int((now or datetime.now(timezone.utc)).timestamp() * 1000)
    return now_ms >= close_ms + _TIER_DELAY_MS.get(tier.value, 0) + _CLOSE_SETTLE_GRACE_MS


def _needs_refresh(
    envelope: Dict[str, Any],
    ttl: int,
    interval: Optional[str] = None,
    now: Optional[datetime] = None,
    is_live: bool = True,
    symbol: Optional[str] = None,
    is_index: bool = False,
    clock=None,
) -> bool:
    """Determine whether an SWR background refresh should fire.

    Priority order (live-only checks gated by ``is_live``):
    1. (live) Stale date (``data_date`` < current trading date) → always refresh.
    2. (live) Stale watermark (interval-aware) → always refresh — catches the
       case where the date is current but bars haven't advanced for N periods.
    3. (live) Complete + market reopened → refresh (day-boundary transition);
       complete + closed → no refresh, unless truncated.
    4. Truncated data → aggressive 25% soft TTL (fires for both live and
       historical so incomplete ranges get retried).
    5. Normal → 50% soft TTL.

    ``is_live=False`` skips the three live-only branches (date/watermark/market-
    phase), since historical envelopes are immutable snapshots. The truncated-
    and soft-TTL branches still fire so truncated historical ranges keep
    getting retried on subsequent hits.

    ``interval`` is optional for backward compatibility, but callers should
    pass it whenever available so the watermark check fires. Daily callers
    must also pass ``symbol`` — without it the daily watermark check is
    skipped (see :func:`is_watermark_stale`).
    """
    if is_live:
        clock = clock or clock_for(symbol, is_index)

        # 1. Stale date — strongest signal
        if _is_stale_date(envelope, now, clock=clock):
            return True

        # 2. Stale watermark — mid-session stagnation
        if interval and is_watermark_stale(
            envelope, interval, now, symbol=symbol, is_index=is_index, clock=clock,
        ):
            return True

        # 3. Complete + market reopened. A complete series still truncated
        # falls through: frozen until the next open, it would keep a missing
        # session through the whole closed stretch.
        if envelope.get("complete"):
            if not clock.is_closed(now):
                return True
            if not envelope.get("truncated"):
                return False

    elapsed = time.time() - envelope.get("fetched_at", 0)

    # 4. Truncated data — aggressive refresh (both live and historical)
    if envelope.get("truncated"):
        return elapsed > ttl * _TRUNCATED_TTL_RATIO

    # 5. Normal soft TTL
    return elapsed > ttl * _SOFT_TTL_RATIO
