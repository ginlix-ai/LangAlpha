"""Measurement: pure analysis of what a provider returned, plus the live run.

Everything above the ``Live probing`` divider is pure — given bars, a phase and
a calendar view it decides completeness, anchors and lag with no network and no
clock of its own. That split is what makes the interesting cases testable: the
CN closing-auction bar and the lunch break are calendar facts, not API facts.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

from market_protocol import AssetClass, PriceTreatment, Tier, classify_lag
from market_protocol.symbology import display_spelling
from market_protocol.intervals import legacy_for_schema, schema_seconds
from market_protocol.routing.ruleset import (
    Cell,
    CellKey,
    ProbedProvider,
    Surface,
    order_providers,
)

logger = logging.getLogger(__name__)

# Intraday schemas a probe run can measure. Seconds bars are left out: no
# chart loads them, so no cell needs them.
PROBE_SCHEMAS: tuple[str, ...] = (
    "ohlcv-1m", "ohlcv-5m", "ohlcv-15m", "ohlcv-30m", "ohlcv-1h", "ohlcv-4h",
)

# Bars stamped after the regular close but inside this window are closing-auction
# prints, not fabrications: HKEX runs a 10-minute Closing Auction Session past
# 16:00 and Shanghai stamps its auction on the 15:00 close itself. They are
# noted, never counted as bad anchors.
POST_CLOSE_AUCTION_TAIL_S = 600

# How long after the close a venue's daily bar is published, keyed by the
# calendar the series is judged against (XSHG carries the SSE, SZSE and BSE
# listings). Until it passes, the session's own daily bar is not yet owed: CN
# closes post from about 15:30, index and fund bars about two hours after the
# bell.
DAILY_PUBLICATION_GRACE_S: dict[str, int] = {"XSHG": 2 * 3600 + 30 * 60}

# Only the most recent bars are anchor-checked. Venue calendars are built from a
# bounded start, so a 2001 bar has no session to be judged against and would read
# as "non-trading day"; the recent tail is also the window a chart actually loads.
MAX_ANCHOR_BARS_INTRADAY = 2000
MAX_ANCHOR_BARS_DAILY = 60

# A provider's own error text can carry a query string with the API key in it.
# Nothing this script writes to disk or prints escapes this scrub.
_SECRET_RE = re.compile(r"[A-Za-z0-9_-]{24,}")

# Rate-limit phrasings, mapped to the divisor that turns the number into
# calls-per-minute. Tushare says 频率超限(1次/分钟); FMP says "Limit Reach ... 300/min".
_RATE_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"(\d+)\s*次\s*/\s*分钟"), 1),
    (re.compile(r"每分钟[^0-9]{0,16}?(\d+)\s*次"), 1),
    (re.compile(r"(\d+)\s*次\s*/\s*小时"), 60),
    (re.compile(r"每小时[^0-9]{0,16}?(\d+)\s*次"), 60),
    (re.compile(r"(\d+)\s*(?:api\s*)?(?:calls?|requests?|reqs?)?\s*(?:per|/)\s*min(?:ute)?\b", re.I), 1),
    (re.compile(r"(\d+)\s*(?:api\s*)?(?:calls?|requests?|reqs?)?\s*(?:per|/)\s*h(?:ou)?r\b", re.I), 60),
)

_PERMISSION_MARKERS = (
    "没有接口访问权限", "权限", "积分不足", "permission", "not authorized",
    "unauthorized", "forbidden", "subscription", "exclusive endpoint",
    "special endpoint", "premium", "upgrade your plan", "not entitled",
)
_RATE_MARKERS = (
    "每分钟", "每小时", "抽取", "频率", "调取", "rate limit", "too many",
    "limit reach", "429",
)


def scrub(text: str) -> str:
    """Redact anything long enough to be a token before it is printed or saved."""
    return _SECRET_RE.sub("<redacted>", str(text))


def _http_status(exc: BaseException) -> Optional[int]:
    """Status a client attached to its exception, directly or via an httpx response."""
    for holder, attr in ((exc, "status_code"), (getattr(exc, "response", None), "status_code")):
        value = getattr(holder, attr, None)
        if isinstance(value, int):
            return value
    return None


def classify_error(exc: BaseException) -> str:
    """``permission`` / ``rate_limit`` / ``other`` for one provider exception.

    HTTP status first (FMP's text is just "request failed (403)" and matches no
    marker), type name second (providers that model the taxonomy say so in the
    class), message markers last (providers that only raise a generic error).
    """
    status = _http_status(exc)
    if status in (401, 402, 403):
        return "permission"
    if status == 429:
        return "rate_limit"
    name = type(exc).__name__
    if "Permission" in name or "Forbidden" in name or "Unauthorized" in name:
        return "permission"
    if "RateLimit" in name or "TooManyRequests" in name:
        return "rate_limit"
    msg = str(exc)
    low = msg.lower()
    if any(m in msg or m.lower() in low for m in _PERMISSION_MARKERS):
        return "permission"
    if any(m in msg or m.lower() in low for m in _RATE_MARKERS):
        return "rate_limit"
    return "other"


def parse_calls_per_minute(message: str | BaseException) -> Optional[int]:
    """Read the provider's own quota out of its rate-limit message.

    The chain's per-minute floor is the only number that decides whether a
    provider can back a chart load, and the provider states it exactly once:
    when it refuses.
    """
    text = str(message)
    for pattern, divisor in _RATE_PATTERNS:
        m = pattern.search(text)
        if m:
            n = int(m.group(1))
            return n if divisor == 1 else round(n / divisor)
    return None


# ---------------------------------------------------------------------------
# Analysis (pure)
# ---------------------------------------------------------------------------


@dataclass
class BarAnalysis:
    session_complete: Optional[bool] = None
    anchors_ok: Optional[bool] = None
    lag_s: Optional[int] = None
    notes: list[str] = field(default_factory=list)


# (open_ms, close_ms, break_start_ms, break_end_ms) or None when unknown.
Bounds = Optional[tuple[int, int, Optional[int], Optional[int]]]


def _hhmm(ms: int, tz: ZoneInfo) -> str:
    return datetime.fromtimestamp(ms / 1000, tz).strftime("%H:%M")


def _local_date(ms: int, tz: ZoneInfo) -> str:
    return datetime.fromtimestamp(ms / 1000, tz).date().isoformat()


def bar_times(bars: Iterable[dict[str, Any]]) -> list[int]:
    return sorted(int(b["time"]) for b in bars if b.get("time") is not None)


def analyze_intraday(
    bars: list[dict[str, Any]],
    *,
    interval: str,
    phase: str,
    tz: ZoneInfo,
    close_ms: Optional[int],
    expected_latest_ms: Optional[int],
    bounds_lookup: Callable[[str], Bounds],
    is_trading_day: Callable[[str], bool] | None = None,
    extended_lookup: Callable[[str], tuple[int, int] | None] | None = None,
    asset_class: AssetClass = AssetClass.EQUITY,
    max_notes: int = 4,
) -> BarAnalysis:
    """Completeness, anchors and lag for one intraday series.

    Completeness is only answerable once the venue has closed, and lag only
    while it is open — the two measurements never coexist, which is why the
    probe wants runs in both windows to fill a market's cells.

    *asset_class* only decides how a lunch-break bar is read: an index level is
    published continuously through the HKEX and SSE breaks, so those bars are
    the publisher's behaviour, not a fabrication.

    *extended_lookup* gives the venue's published pre-market and after-hours
    window for a date: a bar inside it is legitimate extended-hours data, noted
    rather than failed. Completeness is still judged against the regular close.
    """
    res = BarAnalysis()
    times = bar_times(bars)
    if not times:
        return res

    period_ms = schema_seconds(interval) * 1000
    last = times[-1]

    if phase in ("closed", "post") and close_ms is not None:
        # The last continuous window OPENS one period before the close; a bar
        # stamped exactly at the close is a closing-auction print (Shanghai).
        final_open = close_ms - period_ms
        if last >= final_open:
            res.session_complete = True
        else:
            short = max(1, (final_open - last) // period_ms)
            res.session_complete = False
            res.notes.append(
                f"last bar {_hhmm(last, tz)}, {short} bars short of close"
            )
    elif phase in ("open", "pre") and expected_latest_ms:
        res.lag_s = max(0, (expected_latest_ms - last) // 1000)

    problems: list[str] = []
    auction = 0
    extended_bars = 0
    lunch_prints = 0
    index_prints_through_lunch = asset_class == AssetClass.INDEX
    checked = times[-MAX_ANCHOR_BARS_INTRADAY:]
    for t in checked:
        day = _local_date(t, tz)
        if is_trading_day is not None and not is_trading_day(day):
            problems.append(f"bar {day} {_hhmm(t, tz)} on a non-trading day")
            continue
        bounds = bounds_lookup(day)
        if bounds is None:
            continue  # calendar has no session bounds to judge against
        open_ms, cls_ms, brk_start, brk_end = bounds
        extended = extended_lookup(day) if extended_lookup is not None else None
        if (
            extended is not None
            and (extended[0] <= t < open_ms or cls_ms < t <= extended[1])
        ):
            extended_bars += 1
        elif cls_ms < t <= cls_ms + POST_CLOSE_AUCTION_TAIL_S * 1000:
            auction += 1
        elif not (open_ms <= t <= cls_ms):
            problems.append(f"bar {day} {_hhmm(t, tz)} outside the session window")
        elif brk_start is not None and brk_end is not None and brk_start < t < brk_end:
            # A bar stamped exactly on the break start is the morning close
            # print, not a fabrication; anything strictly inside is — unless the
            # series is an index, which keeps printing a level through the break.
            if index_prints_through_lunch:
                lunch_prints += 1
            else:
                problems.append(f"bar {day} {_hhmm(t, tz)} inside the lunch break")

    def window(t: int) -> tuple[str, bool]:
        # The interval's grid holds within one continuous trading window and
        # restarts at every open, the reopen after lunch included: Hong Kong's
        # hourly 11:30 and 13:00 bars are 90 minutes apart and both on time.
        day = _local_date(t, tz)
        bounds = bounds_lookup(day)
        return day, bool(bounds and bounds[3] is not None and t >= bounds[3])

    for a, b in zip(checked, checked[1:]):
        if window(a) == window(b) and (b - a) % period_ms:
            problems.append(
                f"gap {(b - a) // 1000}s between {_hhmm(a, tz)} and {_hhmm(b, tz)} "
                f"is not a multiple of {interval}"
            )

    res.anchors_ok = not problems
    if auction:
        res.notes.append(f"{auction} closing-auction bars after the regular close")
    if extended_bars:
        res.notes.append(f"{extended_bars} extended-hours bars outside the regular session")
    if lunch_prints:
        res.notes.append(f"{lunch_prints} bars print through the lunch break (index)")
    res.notes.extend(problems[:max_notes])
    if len(problems) > max_notes:
        res.notes.append(f"... and {len(problems) - max_notes} more anchor problems")
    return res


def analyze_daily(
    bars: list[dict[str, Any]],
    *,
    tz: ZoneInfo,
    expected_latest_date: Optional[str],
    is_trading_day: Callable[[str], bool] | None = None,
    max_notes: int = 4,
) -> BarAnalysis:
    """Daily series: reaches the expected latest session, lands on trading days."""
    res = BarAnalysis()
    times = bar_times(bars)
    if not times:
        return res

    last_date = _local_date(times[-1], tz)
    if expected_latest_date:
        if last_date >= expected_latest_date:
            res.session_complete = True
        else:
            res.session_complete = False
            res.notes.append(
                f"last bar {last_date}, expected through {expected_latest_date}"
            )

    problems: list[str] = []
    seen: set[str] = set()
    for t in times[-MAX_ANCHOR_BARS_DAILY:]:
        day = _local_date(t, tz)
        if day in seen:
            problems.append(f"duplicate daily bar for {day}")
        seen.add(day)
        if is_trading_day is not None and not is_trading_day(day):
            problems.append(f"daily bar {day} on a non-trading day")
    res.anchors_ok = not problems
    res.notes.extend(problems[:max_notes])
    if len(problems) > max_notes:
        res.notes.append(f"... and {len(problems) - max_notes} more anchor problems")
    return res


def _normalize_symbol(sym: str) -> str:
    # One spelling on both sides, as ``snapshot_key`` meets them: a ``.SS``
    # request must meet a ``.SH`` row, and only one caret drops.
    return display_spelling(str(sym)).removeprefix("^")


def snapshot_coverage(rows: list[dict[str, Any]], requested: list[str]) -> int:
    """Canaries that came back with a usable price."""
    wanted = {_normalize_symbol(s) for s in requested}
    hit: set[str] = set()
    for row in rows or []:
        sym = _normalize_symbol(row.get("symbol") or "")
        if sym in wanted and row.get("price") is not None:
            hit.add(sym)
    return len(hit)


def snapshot_as_of_ms(rows: list[dict[str, Any]]) -> Optional[int]:
    """Oldest quote timestamp across the rows, if any row carries one.

    Each canary is judged on its own and the cell takes the worst: one stale
    quote is what a watchlist shows, however fresh the rest of the batch is.
    """
    stamps: list[int] = []
    for row in rows or []:
        for key in ("as_of", "timestamp", "time"):
            raw = row.get(key)
            if raw is None:
                continue
            ms = _to_ms(raw)
            if ms is not None:
                stamps.append(ms)
                break
    return min(stamps) if stamps else None


def _to_ms(raw: Any) -> Optional[int]:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        value = int(raw)
        return value * 1000 if value < 10**11 else value
    if isinstance(raw, str):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


# Rows a single upstream call returns before the source starts paging. Only
# providers that actually page need an entry; everything else costs one call.
PAGE_SIZE: dict[str, int] = {"tushare": 8000}


def estimate_cost_calls(provider: str, rows: int) -> int:
    """Upstream calls one canary request cost, from the rows it came back with."""
    page = PAGE_SIZE.get(provider)
    if not page or rows <= page:
        return 1
    return -(-rows // page)


# ---------------------------------------------------------------------------
# Live probing
# ---------------------------------------------------------------------------


@dataclass
class MarketView:
    """The calendar facts one cell is judged against."""

    tz: ZoneInfo
    phase: str
    close_ms: Optional[int]
    expected_latest_ms: Optional[int]
    expected_latest_date: Optional[str]
    bounds_lookup: Callable[[str], Bounds]
    is_trading_day: Callable[[str], bool]
    extended_lookup: Callable[[str], tuple[int, int] | None] | None = None


def market_view(
    symbol: str, *, is_index: bool, interval: str | None, now: datetime
) -> MarketView:
    """Resolve *symbol* to its venue clock and calendar for the probe window."""
    from datetime import date as _date

    from market_protocol import to_canonical
    from market_protocol.calendars import get_calendar, session_bounds
    from src.data_client.instrument_clock import clock_for_ref

    ref = to_canonical(symbol, asset_class=AssetClass.INDEX if is_index else None)
    clock = clock_for_ref(ref)
    cal = get_calendar(ref.calendar_id)

    def bounds_lookup(iso_date: str) -> Bounds:
        try:
            return session_bounds(ref.calendar_id, iso_date)
        except Exception:
            return None

    def is_trading_day(iso_date: str) -> bool:
        try:
            return bool(cal.is_trading_day(_date.fromisoformat(iso_date)))
        except Exception:
            return True

    def extended_lookup(iso_date: str) -> tuple[int, int] | None:
        bounds_fn = getattr(cal, "extended_bounds_ms", None)
        if bounds_fn is None:
            return None
        try:
            return bounds_fn(_date.fromisoformat(iso_date))
        except Exception:
            return None

    now_ms = int(now.timestamp() * 1000)
    close_ms: Optional[int] = None
    try:
        session = cal.latest_trading_date(now)
        close_ms = cal.session_close_ms(session)
        if close_ms is not None and close_ms > now_ms:
            # The trading date rolls at the pre-market open, so an index probed
            # before the bell is dated today, whose close is still ahead; the
            # last close is the session before.
            close_ms = cal.session_close_ms(cal.previous_session(session))
    except Exception:
        close_ms = None

    src_interval = legacy_for_schema(interval or "ohlcv-1m")
    try:
        expected_latest_ms = clock.expected_latest_bar_ms(src_interval, now)
    except Exception:
        expected_latest_ms = None
    try:
        # The newest session that has opened; its daily bar is owed only once it
        # has closed and the venue's publication grace has run. Until then the
        # session before it is the mark, and a publisher that also carries the
        # forming bar is ahead of it, not merely level with it.
        latest = cal.expected_latest_daily_date(now)
        published_ms = cal.session_close_ms(latest)
        if published_ms is not None:
            published_ms += DAILY_PUBLICATION_GRACE_S.get(ref.calendar_id, 0) * 1000
            if now_ms < published_ms:
                latest = cal.previous_session(latest)
        expected_latest_date = latest.isoformat()
    except Exception:
        expected_latest_date = None

    return MarketView(
        tz=ZoneInfo(str(clock.tz)),
        phase=clock.market_phase(now),
        close_ms=close_ms,
        expected_latest_ms=expected_latest_ms,
        expected_latest_date=expected_latest_date,
        bounds_lookup=bounds_lookup,
        is_trading_day=is_trading_day,
        extended_lookup=extended_lookup,
    )


def _venue_of(symbol: str | None, asset_class: AssetClass) -> str | None:
    """``calendar_id`` of a canary, the venue key ``publisher_lineage`` ranks on."""
    if not symbol:
        return None
    from market_protocol import to_canonical

    try:
        return to_canonical(symbol, asset_class=AssetClass(asset_class)).calendar_id
    except Exception:
        return None


def declared_tier(
    provider: str, *, surface: Surface, asset_class: AssetClass, symbol: str | None = None
) -> Tier:
    from src.data_client.normalize import publisher_lineage, snapshot_tier

    venue = _venue_of(symbol, asset_class)
    if surface is Surface.SNAPSHOT:
        return Tier(snapshot_tier(provider, venue))
    return Tier(publisher_lineage(provider, asset_class, venue)[1])


def series_price_treatment(
    provider: str, result: Any, asset_class: AssetClass, symbol: str | None = None
) -> Optional[PriceTreatment]:
    """``PriceTreatment`` of the bars *provider* just returned.

    Same path the REST boundary takes (``_series_meta`` in
    ``src/server/app/market_data.py``): the CMDP series header when the source
    returned one, otherwise the publisher's declared lineage for this asset
    class — which is where the per-class split lives (Tushare equities are qfq,
    its indices and funds raw).
    """
    header = getattr(result, "header", None)
    if header is not None:
        value = (
            header.get("price_treatment") if isinstance(header, dict)
            else getattr(header, "price_treatment", None)
        )
        if value is not None:
            return PriceTreatment(value)
    from src.data_client.normalize import publisher_lineage

    return PriceTreatment(publisher_lineage(provider, asset_class, _venue_of(symbol, asset_class))[0])


def _unwrap(result: Any) -> list[dict[str, Any]]:
    bars = getattr(result, "bars", result)
    return list(bars or [])


# A refusal that says why is itself the verdict: no entitlement, or a quota.
_VERDICT_ERRORS = ("permission", "rate_limit")


class Unmeasured(Exception):
    """No canary got an answer and no refusal said why: an outage during the
    run, which says nothing about what the provider covers."""


async def _call_with_backoff(
    coro_factory: Callable[[], Any],
    *,
    backoff_s: float,
    sleeper: Callable[[float], Any] = asyncio.sleep,
) -> tuple[Any, Optional[BaseException], Optional[BaseException]]:
    """Run a provider call, retrying a rate-limit refusal exactly once.

    One retry is the whole budget: the probe is a survey, and a provider that
    is still refusing after its own stated cooldown has answered the question.
    Returns ``(result, error, throttle)``; *throttle* is the refusal that stated
    the quota, reported even when the retry then succeeded — a provider names
    its per-minute limit only when it refuses.
    """
    try:
        return await coro_factory(), None, None
    except Exception as exc:  # a provider failure must never abort the run
        if classify_error(exc) != "rate_limit":
            return None, exc, None
        first = exc
    await sleeper(backoff_s)
    try:
        return await coro_factory(), None, first
    except Exception as exc:
        return None, (exc if classify_error(exc) == "rate_limit" else first), first


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def probe_provider(
    name: str,
    source: Any,
    key: CellKey,
    symbols: list[str],
    *,
    clock: Callable[[], datetime] = _utcnow,
    backoff_s: float = 60.0,
    sleeper: Callable[[float], Any] = asyncio.sleep,
    log: Callable[[str], None] = lambda _m: None,
) -> ProbedProvider:
    """Run one provider against one cell's canary basket.

    *clock* is read after each fetch returns: a basket runs canaries in turn
    behind minute-long backoffs, and a lag judged against the moment it began
    reads every late bar as on time.
    """
    probed = ProbedProvider(name=name, coverage_total=len(symbols))

    if key.surface is Surface.SNAPSHOT:
        asset_type = "indices" if key.asset_class is AssetClass.INDEX else "stocks"
        rows, exc, throttle = await _call_with_backoff(
            lambda: source.get_snapshots(symbols, asset_type=asset_type),
            backoff_s=backoff_s, sleeper=sleeper,
        )
        _record_quota(probed, throttle)
        if exc is not None:
            kind = _record_failure(probed, exc, "snapshot")
            log(f"    {name}: {scrub(exc)}")
            if kind not in _VERDICT_ERRORS or _throttled_blind(probed, kind):
                raise Unmeasured(probed.notes[-1])
        else:
            rows = list(rows or [])
            probed.coverage_hit = snapshot_coverage(rows, symbols)
            probed.cost_calls = estimate_cost_calls(name, len(rows))
            now = clock()
            view = market_view(symbols[0], is_index=key.asset_class is AssetClass.INDEX,
                               interval=None, now=now)
            as_of = snapshot_as_of_ms(rows)
            if view.phase in ("open", "pre") and as_of is not None:
                probed.lag_s = max(0, int(now.timestamp() * 1000 - as_of) // 1000)
        probed.tier = (
            classify_lag(probed.lag_s) if probed.lag_s is not None
            else declared_tier(name, surface=key.surface, asset_class=key.asset_class,
                               symbol=symbols[0] if symbols else None)
        )
        return probed

    is_index = key.asset_class is AssetClass.INDEX
    completes: list[bool] = []
    anchors: list[bool] = []
    lags: list[int] = []
    widest_page = 0
    answered = refused = 0
    last_refusal = ""

    for symbol in symbols:
        if key.surface is Surface.INTRADAY:
            src_interval = legacy_for_schema(key.interval or "ohlcv-1m")
            factory = lambda s=symbol, i=src_interval: source.get_intraday(  # noqa: E731
                s, i, is_index=is_index
            )
        else:
            factory = lambda s=symbol: source.get_daily(s, is_index=is_index)  # noqa: E731

        result, exc, throttle = await _call_with_backoff(
            factory, backoff_s=backoff_s, sleeper=sleeper
        )
        _record_quota(probed, throttle)
        if exc is not None:
            kind = _record_failure(probed, exc, symbol)
            log(f"    {name} {symbol}: {scrub(exc)}")
            if kind in _VERDICT_ERRORS:
                refused += 1
                last_refusal = kind
                break  # the answer is the same for every remaining canary
            continue

        answered += 1
        bars = _unwrap(result)
        if not bars:
            probed.notes.append(f"{symbol}: no rows")
            continue
        probed.coverage_hit += 1
        widest_page = max(widest_page, len(bars))
        if probed.price_treatment is None:
            probed.price_treatment = series_price_treatment(name, result, key.asset_class, symbol)

        view = market_view(symbol, is_index=is_index, interval=key.interval, now=clock())
        if key.surface is Surface.INTRADAY:
            analysis = analyze_intraday(
                bars, interval=key.interval or "ohlcv-1m", phase=view.phase, tz=view.tz,
                close_ms=view.close_ms, expected_latest_ms=view.expected_latest_ms,
                bounds_lookup=view.bounds_lookup, is_trading_day=view.is_trading_day,
                extended_lookup=view.extended_lookup,
                asset_class=key.asset_class,
            )
        else:
            analysis = analyze_daily(
                bars, tz=view.tz, expected_latest_date=view.expected_latest_date,
                is_trading_day=view.is_trading_day,
            )
        if analysis.session_complete is not None:
            completes.append(analysis.session_complete)
        if analysis.anchors_ok is not None:
            anchors.append(analysis.anchors_ok)
        if analysis.lag_s is not None:
            lags.append(analysis.lag_s)
        probed.notes.extend(f"{symbol}: {n}" for n in analysis.notes)

    if symbols and not answered and not refused:
        raise Unmeasured(probed.notes[-1])
    if refused and not answered and _throttled_blind(probed, last_refusal):
        raise Unmeasured(probed.notes[-1])
    if completes:
        probed.session_complete = all(completes)
    if anchors:
        probed.anchors_ok = all(anchors)
    if lags:
        probed.lag_s = max(lags)  # the worst canary is the one a chart notices
    probed.cost_calls = estimate_cost_calls(name, widest_page)
    probed.tier = (
        classify_lag(probed.lag_s) if probed.lag_s is not None
        else declared_tier(name, surface=key.surface, asset_class=key.asset_class,
                           symbol=symbols[0] if symbols else None)
    )
    return probed


def _throttled_blind(probed: ProbedProvider, kind: str) -> bool:
    """A throttle that stated no quota and left no coverage proves nothing.

    Only an answer proves coverage; recording ``no_coverage`` here would drop a
    working provider from routing until the next probe.
    """
    return kind == "rate_limit" and probed.calls_per_minute is None and not probed.coverage_hit


def _record_quota(probed: ProbedProvider, throttle: Optional[BaseException]) -> None:
    """Keep the quota a refusal stated, whether or not the retry then worked.

    The policy reports ``quota_below_floor`` ahead of ``no_coverage``, so a
    provider throttled down to zero rows has to carry the number that explains
    it.
    """
    if throttle is None or probed.calls_per_minute is not None:
        return
    cpm = parse_calls_per_minute(scrub(throttle))
    if cpm is not None:
        probed.calls_per_minute = cpm


def _record_failure(probed: ProbedProvider, exc: BaseException, where: str) -> str:
    kind = classify_error(exc)
    message = scrub(exc) or type(exc).__name__
    if kind == "permission":
        probed.entitled = False
        probed.notes.append(f"{where}: permission denied: {message}")
    elif kind == "rate_limit":
        cpm = parse_calls_per_minute(message)
        if cpm is not None:
            probed.calls_per_minute = cpm
        probed.notes.append(f"{where}: rate limited: {message}")
    else:
        probed.notes.append(f"{where}: {type(exc).__name__}: {message}")
    return kind


async def probe_cell(
    key: CellKey,
    sources: dict[str, Any],
    symbols: list[str],
    *,
    clock: Callable[[], datetime] = _utcnow,
    backoff_s: float = 60.0,
    sleeper: Callable[[float], Any] = asyncio.sleep,
    log: Callable[[str], None] = lambda _m: None,
) -> Cell:
    """Probe every source for one cell and apply the ordering policy."""
    now = clock()
    view = market_view(symbols[0], is_index=key.asset_class is AssetClass.INDEX,
                       interval=key.interval, now=now)
    providers: list[ProbedProvider] = []
    for name, source in sources.items():
        try:
            providers.append(await probe_provider(
                name, source, key, symbols, clock=clock,
                backoff_s=backoff_s, sleeper=sleeper, log=log,
            ))
        except Exception as exc:  # never let one provider abort the run
            # Left out of the cell rather than recorded as covering nothing:
            # a merge then keeps the verdict an earlier run measured, and the
            # reader treats a provider without one as configured, not excluded.
            logger.warning(
                "data_probe.provider_unmeasured | name=%s cell=%s error=%s",
                name, key.as_str(), scrub(exc) or type(exc).__name__,
            )
    return Cell(
        key=key,
        probed_at=now,
        phase_at_probe=view.phase,
        canaries=list(symbols),
        providers=order_providers(key.surface, providers),
    )
