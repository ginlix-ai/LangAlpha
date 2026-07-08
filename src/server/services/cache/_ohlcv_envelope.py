"""Shared envelope helpers for OHLCV cache services (daily + intraday).

Provides the envelope structure, parsing, delta-merge, and SWR staleness
check used by both DailyCacheService and IntradayCacheService.
"""

import time
from bisect import bisect_left
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config.core import get_infrastructure_config
from src.data_client.normalize import publisher_lineage
from src.market_protocol import to_canonical
from src.market_protocol.enums import AssetClass, Tier
from src.market_protocol.intervals import schema_for_legacy
from src.server.services.cache._instrument_clock import UsClock, clock_for
from src.utils.market_hours import current_trading_date, interval_seconds

_ET = ZoneInfo("America/New_York")

# Default clock: XNYS via the market_hours facade (pre-CMDP parity).
_US_CLOCK = UsClock()

# v4 (Phase 3): protocol Series container — header + records, keyed on
# instrument_key. v3 (legacy source-segmented keys) remains readable for
# dual-read of warm caches written before the cutover.
#
# Bump on ANY stored-contract change, additive included: closed-market TTLs
# freeze complete envelopes until the venue's next open, so an unversioned
# change keeps serving the old shape for up to a whole weekend after deploy.
# A mismatched version reads as a miss (refetch). v5: invalidates
# pre-release v4 rows written before `market_phase` joined the cache block.
ENVELOPE_VERSION = 5
_ENVELOPE_V3 = 3

_SOFT_TTL_RATIO: float = get_infrastructure_config().redis.swr.soft_ttl_ratio
_TRUNCATED_TTL_RATIO = 0.25  # aggressive refresh for truncated data
_EMPTY_RESULT_TTL = 30  # short TTL for empty upstream results
# Floor between consecutive staleness-driven daily re-fetches. When the
# provider itself is behind (e.g. today's daily bar not yet published right
# after the open), re-asking immediately can't yield newer data — without
# this floor every request would bypass the cache with a blocking fetch.
_DAILY_STALE_REFETCH_COOLDOWN = 120

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
) -> Dict[str, Any]:
    """Build a v4 Series envelope (storage form).

    Protocol lineage lives in ``header``; cache-operational flags stay
    top-level. Records gain ``ts_event`` (bar-open ms UTC, aliasing the
    legacy ``time`` they already carry).
    """
    watermark = bars[-1].get("time", 0) if bars else 0
    now = time.time()
    for b in bars:
        b.setdefault("ts_event", b.get("time"))
    treatment, tier = publisher_lineage(publisher)
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
    """
    bars = v3.get("bars") or []
    for b in bars:
        b.setdefault("ts_event", b.get("time"))
    treatment, tier = publisher_lineage(publisher)
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
            "header": header,
        }
    if raw.get("v") == _ENVELOPE_V3 and "bars" in raw:
        return raw
    return None


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
    non-US symbols; XNYS parity by default) and is valid in every market
    phase, including weekends and holidays — no phase gate needed.
    """
    data_date = envelope.get("data_date")
    if not data_date:
        return True  # missing data_date — treat as stale
    return data_date != (clock or _US_CLOCK).current_trading_date(now)


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
        if time.time() - envelope.get("fetched_at", 0) < _DAILY_STALE_REFETCH_COOLDOWN:
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
            now_rank = _PHASE_SETTLEDNESS.get(clock.market_phase(now), 0)
            if now_rank > _PHASE_SETTLEDNESS.get(stored_phase, 0):
                return True
        return False
    # Empty envelopes (no bars in requested window) are not meaningfully stale
    # on a watermark basis — there's nothing to be behind. They're deliberately
    # cached with a short _EMPTY_RESULT_TTL to dampen fetch storms for symbols
    # with no data; the soft TTL path handles re-fetch timing.
    if not envelope.get("bars"):
        return False
    watermark_ms = envelope.get("watermark") or 0
    if watermark_ms <= 0:
        # Bars exist but watermark is 0 — envelope is corrupt, treat as stale.
        return True
    clock = clock or clock_for(symbol, is_index)
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
    3. (live) Complete + market reopened → refresh (day-boundary transition).
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

        # 3. Complete + market reopened
        if envelope.get("complete"):
            if not clock.is_closed(now):
                return True
            return False

    elapsed = time.time() - envelope.get("fetched_at", 0)

    # 4. Truncated data — aggressive refresh (both live and historical)
    if envelope.get("truncated"):
        return elapsed > ttl * _TRUNCATED_TTL_RATIO

    # 5. Normal soft TTL
    return elapsed > ttl * _SOFT_TTL_RATIO
