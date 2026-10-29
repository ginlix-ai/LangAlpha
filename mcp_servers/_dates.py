"""Date bounds read the way the hosted market-data tools read them.

The hosted implementation of these tools refuses a bound it cannot parse, and a
window whose start is after its end, where a read reaches FMP. FMP does
neither: it drops a bound it cannot read and serves its default window, which
can be years of rows, and answers a reversed window with nothing. Reading the
bounds here by the same rules, in the same words, gives the agent the same
answer on either deployment. Pure stdlib, like ``_envelope``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Optional
from zoneinfo import ZoneInfo

#: The FMP client's undated daily window: this many days back to the end, today (UTC) if none.
_FMP_DAILY_LOOKBACK_DAYS = 500

#: FMP labels a currency or crypto pair's intraday bars, and a commodity's, in New York time.
NEW_YORK = "America/New_York"

_LABEL = "%Y-%m-%d %H:%M:%S"


class DateBoundError(ValueError):
    """A date bound refused before FMP is asked.

    ``status_code`` marks the message as safe to show, so ``error_from_exception``
    hands it to the agent as ``invalid_argument``, as it does an FMP 4xx.
    """

    status_code = 422


def calendar_days(
    from_date: Optional[str], to_date: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """A calendar or rates read's bounds as ISO days; an absent bound stays absent."""
    days: list[Optional[str]] = []
    for value, name in ((from_date, "from"), (to_date, "to")):
        try:
            days.append(date.fromisoformat(value).isoformat() if value else None)
        except (TypeError, ValueError):
            raise DateBoundError(f"{name} must be an ISO date, not {value!r}") from None
    if days[0] and days[1] and days[0] > days[1]:
        raise DateBoundError("from is after to")
    return days[0], days[1]


def _day(value: Optional[str], what: str) -> Optional[date]:
    # Only the first ten characters count, so a time of day is dropped.
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except (TypeError, ValueError):
        raise DateBoundError(f"{what} must be YYYY-MM-DD, not {value!r}") from None


def _iso(day: Optional[date]) -> Optional[str]:
    return day.isoformat() if day else None


def _no_future_start(start: Optional[date], end: Optional[date]) -> None:
    # Against the default end, today, a lone future start would read "start is
    # after end", naming an end the caller never gave.
    today = datetime.now(timezone.utc).date()
    if start and not end and start > today:
        raise DateBoundError(f"from_date is in the future (today is {today.isoformat()} UTC)")


def _back_from(start: Optional[date], end: Optional[date]) -> Optional[date]:
    # The client's window runs back from today, which a lone old end would
    # reverse; run it back from the end the caller gave, as the CN path does.
    if end and not start:
        return end - timedelta(days=_FMP_DAILY_LOOKBACK_DAYS)
    return start


def daily_days(
    from_date: Optional[str], to_date: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """A daily FMP bar read's bounds as ISO days; with neither, the client's default stands."""
    start, end = _day(from_date, "from_date"), _day(to_date, "to_date")
    _no_future_start(start, end)
    if start and end and start > end:
        raise DateBoundError("start is after end")
    return _iso(_back_from(start, end)), _iso(end)


def commodity_daily_days(
    from_date: Optional[str], to_date: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """A daily commodity read's bounds, read as ``daily_days`` reads them; a
    reversed pair is worded as the hosted commodity route words it."""
    start, end = _day(from_date, "from_date"), _day(to_date, "to_date")
    _no_future_start(start, end)
    if start and end and start > end:
        raise DateBoundError("from is after to")
    return _iso(_back_from(start, end)), _iso(end)


def _ms(moment: datetime, zone: tzinfo) -> int:
    return int(moment.replace(tzinfo=zone).timestamp() * 1000)


def _label_ms(label: object, zone: tzinfo) -> Optional[int]:
    try:
        return _ms(datetime.strptime(str(label), _LABEL), zone)
    except ValueError:
        return None


def _zone_day(ms: int, zone: tzinfo) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=zone).date().isoformat()


@dataclass(frozen=True)
class IntradayWindow:
    """Inclusive bar-open instants, and the days to ask FMP for in the zone it labels in."""

    zone: tzinfo  # bounds are read, and labels written, here: the zone the envelope states
    labels: tzinfo  # the zone FMP labels the bars in
    start_ms: int
    end_ms: int
    from_day: str
    to_day: str

    def rows(self, rows: list) -> list:
        """FMP's rows for the bars that open inside, labelled in the window's zone.

        A label that does not read as one is dropped, and one bar is kept per
        instant (the last FMP sent), as the hosted vendor keeps them: rewritten,
        a label in the repeated autumn hour names one instant twice.
        """
        kept: dict[int, dict] = {}
        for row in rows:
            ms = _label_ms(row.get("date"), self.labels)
            if ms is None or not self.start_ms <= ms <= self.end_ms:
                continue
            if self.labels != self.zone:
                stamp = datetime.fromtimestamp(ms / 1000, tz=self.zone)
                row = {**row, "date": stamp.strftime(_LABEL)}
            kept[ms] = row
        return list(kept.values())


def _instant(value: str, zone: tzinfo, what: str) -> datetime:
    try:
        moment = datetime.fromisoformat(value.strip())
    except (AttributeError, ValueError):
        raise DateBoundError(
            f"{what} must be YYYY-MM-DD or YYYY-MM-DD HH:MM, not {value!r}"
        ) from None
    if moment.tzinfo is not None:
        moment = moment.astimezone(zone).replace(tzinfo=None)
    return moment


def intraday_window(
    from_date: str, to_date: str, zone_name: Optional[str], labels: Optional[str] = None
) -> IntradayWindow:
    """An intraday read's bounds in *zone_name*: a bare day spans the whole day, a time counts to the second.

    FMP's intraday endpoints take days and drop a bound with a time in it, so
    the read asks for the whole days the window touches in the zone FMP labels
    in (*labels*, else *zone_name*) and keeps the bars ``rows`` finds inside.
    """
    zone = ZoneInfo(zone_name or "UTC")
    label_zone = ZoneInfo(labels) if labels else zone
    start = _instant(from_date, zone, "from_date")
    end = _instant(to_date, zone, "to_date")
    bare_end = len(to_date.strip()) <= 10
    end_ms = _ms(end + (timedelta(days=1) if bare_end else timedelta(seconds=1)), zone) - 1
    start_ms = _ms(start, zone)
    if start_ms > end_ms:
        raise DateBoundError("start is after end")
    return IntradayWindow(
        zone,
        label_zone,
        start_ms,
        end_ms,
        _zone_day(start_ms, label_zone),
        _zone_day(end_ms, label_zone),
    )
