"""FMP's answer caps: flagging an answer a cap cut, and reading a date range or an
indicator series back past them."""

import asyncio
import json
from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Dict, Union

# ``treasury-rates`` and both calendars answer at most the 90 days ending at
# ``to``, clamping a longer range without a word, and an earnings answer at
# most 4,000 rows, newest first, the rest a ``page`` away. A range is read
# back in those windows and pages, up to a call cap per read, as ginlix-data's
# FMP vendor reads it, so both serve the same rows.
RANGE_WINDOW_DAYS = 90
EARNINGS_PAGE_ROWS = 4000
# A calendar window counts only once read whole, so a cap part-way drops it.
# Fourteen days of reporters stay within four pages (13,152 rows at the busiest
# fortnight of late 2026, measured), so the cap drops little and a first window
# always fits.
EARNINGS_WINDOW_DAYS = 14
RANGE_MAX_CALLS = 8

# Stable's ``economic-indicators`` ignores ``limit`` and answers at most a
# stretch of days ending at ``to``, clamping a longer range without a word: a
# year when measured (2026-11: 12 monthly, 4 quarterly, 52 weekly rows), 91
# days in earlier measurements. A read asks a decade first and takes the span
# that answer held as its window (a span inside it is answered whole), or 91
# days when it held nothing. The window cap counts requests; four years with
# no row is where the series begins. ginlix-data's FMP vendor walks the same
# windows with the same caps, so both serve the same rows.
INDICATOR_PROBE_DAYS = 3653
INDICATOR_WINDOW_DAYS = 91
INDICATOR_MAX_WINDOWS = 64
INDICATOR_MAX_EMPTY_DAYS = 1456
INDICATOR_MAX_BATCH = 8

# A ``historical-price-eod/full`` answer holds at most 5,000 bars, the newest of
# its range, and an intraday answer at most this many calendar days ending at
# ``to``, whole (both measured live by ginlix-data's FMP vendor, which reads a
# longer range back a span at a time). Past either, FMP leaves the oldest bars
# out without a word, so a read that reaches a cap says so.
EOD_RECORDS = 5000
INTRADAY_SPAN_DAYS = {"1min": 3, "5min": 10, "15min": 45, "30min": 30, "1hour": 90, "4hour": 180}

# A walk is many calls, and a refusal at any of them fails the read, so a call
# FMP refuses with a 429 is asked again: after its ``Retry-After`` when FMP
# sends one, else after a doubling backoff, at most twice and never past the
# total wait below for that call, after which the refusal stands. ginlix-data's FMP vendor
# retries its walks the same way.
RATE_RETRIES = 2
RATE_BACKOFF_S = 1.0
RATE_MAX_WAIT_S = 4.0


async def patiently[T](call: Callable[[], Awaitable[T]]) -> T:
    """``call()``, asked again on a 429 within the bounds above."""
    waited = 0.0
    for attempt in range(RATE_RETRIES + 1):
        try:
            return await call()
        except Exception as exc:
            if getattr(exc, "status_code", None) != 429 or attempt == RATE_RETRIES:
                raise
            delay = getattr(exc, "retry_after", None)
            delay = RATE_BACKOFF_S * 2**attempt if delay is None else delay
            if waited + delay > RATE_MAX_WAIT_S:
                raise
            waited += delay
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


class Walk(list):
    """Rows a read returned.

    ``truncated`` when a cap (FMP's answer size, or the call cap of a read
    over several calls) cut one end of the range: the oldest rows, or a
    calendar's latest dates (ginlix-data's ``Walk``).
    """

    truncated = False


async def read_range(
    request: Callable[[Dict[str, Any]], Awaitable[Any]],
    from_date: str,
    to_date: str,
    *,
    page_rows: int | None = None,
    window_days: int = RANGE_WINDOW_DAYS,
    from_start: bool = False,
) -> Union[Walk, Dict]:
    """Rows from ``from_date`` to ``to_date``, a window at a time, each paged on
    while a page comes back full, up to a call cap.

    ``request`` makes one call with the given params. Rates keep the end of
    their range: windows newest first, rows in the order one answer has them.
    A calendar is read for its nearest dates, so it keeps the start
    (``from_start``): windows oldest first, rows sorted by date. A window
    counts only once read whole, so a read the cap stops loses only the end it
    does not keep, and the rows in hand come back ``truncated``. A refusal at
    any call, an error or an answer that is not a list, fails the read as FMP
    reports it, once a 429 has been asked again (:func:`patiently`). A row
    identical to one already read is dropped (rows landing between calls shift
    a page).
    """
    floor, ceiling = date.fromisoformat(from_date), date.fromisoformat(to_date)
    out, seen, calls, truncated = Walk(), set(), 0, False
    span = timedelta(days=window_days - 1)
    while floor <= ceiling:
        if from_start:
            lo, hi = floor, min(ceiling, floor + span)
        else:
            lo, hi = max(floor, ceiling - span), ceiling
        window: list = []
        page = 0
        while True:
            if calls == RANGE_MAX_CALLS:
                truncated = True
                break
            params: Dict[str, Any] = {"from": lo.isoformat(), "to": hi.isoformat()}
            if page:
                params["page"] = page
            rows = await patiently(lambda: request(params))
            calls += 1
            if not isinstance(rows, list):
                return rows
            window.extend(rows)
            if page_rows is None or len(rows) < page_rows:
                break
            page += 1
        if truncated:
            break
        for row in window:
            key = json.dumps(row, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                out.append(row)
        if from_start:
            floor = hi + timedelta(days=1)
        else:
            ceiling = lo - timedelta(days=1)
    out.truncated = truncated
    if from_start:
        out.sort(key=lambda row: str(row.get("date")))
    return out


async def read_indicator(
    request: Callable[..., Awaitable[Any]], name: str, limit: int, end: date
) -> Union[Walk, Dict]:
    """The newest ``limit`` observations of an indicator, newest first.

    Read back from ``end`` a window at a time (see the constants above): the
    decade asked first teaches the window, and the rest go out in batches sized
    to the rows still wanted, until ``limit`` rows are in hand. A
    :class:`Walk`, ``truncated`` when the window cap ended it first. An answer
    that is not a list is FMP's refusal and is returned as it came.
    """
    if limit <= 0:
        return Walk()
    found: Dict[str, Dict] = {}

    async def read(lo: date, hi: date) -> Any:
        params = {"name": name, "from": lo.isoformat(), "to": hi.isoformat()}
        try:
            return await patiently(lambda: request("economic-indicators", params=params))
        except json.JSONDecodeError as exc:
            if exc.doc.strip() != "Invalid name":
                raise
            # FMP answers a name it lacks with a 200 whose body is the text "Invalid name".
            from .fmp_client import FMPRequestError

            raise FMPRequestError(f"FMP has no economic indicator named {name!r}", status_code=422) from exc

    def keep(lo: date, hi: date, answer: list) -> list:
        inside = [
            row
            for row in answer
            if isinstance(row, dict) and lo.isoformat() <= str(row.get("date"))[:10] <= hi.isoformat()
        ]
        for row in inside:
            found.setdefault(row["date"], row)
        return inside

    probe_lo = end - timedelta(days=INDICATOR_PROBE_DAYS - 1)
    probe = await read(probe_lo, end)
    if not isinstance(probe, list):
        return probe
    held = keep(probe_lo, end, probe)
    if held:
        oldest = date.fromisoformat(min(str(row["date"])[:10] for row in held))
        # Never narrower than 91 days, which FMP always answers whole: a sparse
        # series whose newest row sits just before ``end`` would teach a span
        # too short for the empty run to end it before the window cap.
        span = max((end - oldest).days + 1, INDICATOR_WINDOW_DAYS)
        cursor, empty_days = oldest - timedelta(days=1), 0
    else:
        span = INDICATOR_WINDOW_DAYS
        cursor, empty_days = end - timedelta(days=span), span
    per_window = max(len(held), 1)
    walked = 1
    while walked < INDICATOR_MAX_WINDOWS and empty_days < INDICATOR_MAX_EMPTY_DAYS and len(found) < limit:
        wanted = -(-(limit - len(found)) // per_window)
        spans = []
        for _ in range(min(wanted, INDICATOR_MAX_BATCH, INDICATOR_MAX_WINDOWS - walked)):
            lo = cursor - timedelta(days=span - 1)
            spans.append((lo, cursor))
            cursor = lo - timedelta(days=1)
        walked += len(spans)
        answers = await asyncio.gather(*(read(lo, hi) for lo, hi in spans), return_exceptions=True)
        # Newest first, in window order rather than completion order: a
        # window older than the rows already in hand, or past the empty run
        # that ends the series, is not needed, and neither is its failure.
        for (lo, hi), answer in zip(spans, answers):
            if len(found) >= limit or empty_days >= INDICATOR_MAX_EMPTY_DAYS:
                break
            if isinstance(answer, BaseException):
                raise answer
            if not isinstance(answer, list):
                return answer
            empty_days = 0 if keep(lo, hi, answer) else empty_days + span
    rows = sorted(found.values(), key=lambda row: row["date"], reverse=True)
    out = Walk(rows[:limit])
    out.truncated = (
        walked >= INDICATOR_MAX_WINDOWS
        and empty_days < INDICATOR_MAX_EMPTY_DAYS
        and len(found) < limit
    )
    return out


def eod_answer(rows: Any, from_date: str) -> Any:
    """A daily answer as a :class:`Walk`, ``truncated`` when it holds the record
    cap and starts after ``from_date``; a refusal is returned as it came."""
    if not isinstance(rows, list):
        return rows
    out = Walk(rows)
    out.truncated = len(rows) >= EOD_RECORDS and min(
        str(row.get("date"))[:10] for row in rows
    ) > from_date[:10]
    return out


def intraday_answer(
    rows: Any, interval: str, from_date: Any, to_date: Any, today: date
) -> Any:
    """An intraday answer as a :class:`Walk`, ``truncated`` when the window asked
    for (to ``today`` without an end) is longer than the interval's span."""
    span = INTRADAY_SPAN_DAYS.get(interval)
    if not isinstance(rows, list) or not from_date or span is None:
        return rows
    out = Walk(rows)
    try:
        first = date.fromisoformat(str(from_date)[:10])
        last = date.fromisoformat(str(to_date)[:10]) if to_date else today
    except ValueError:
        return out
    out.truncated = (last - first).days + 1 > span
    return out
