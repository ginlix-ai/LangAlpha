"""The clocks a wakeup is read on, and its times said back on them.

A stored zone is an IANA name, or ``UTC-04:00`` for an ``at`` written with an
offset, so the listing keeps reading it the way the model wrote it. Elapsed
time is always measured in UTC: subtracting two datetimes on one ZoneInfo is
wall-clock arithmetic, which a daylight-saving change makes wrong.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence
from zoneinfo import ZoneInfo

Zone = ZoneInfo | timezone

_FIXED_OFFSET = re.compile(r"UTC([+-])(\d{2}):(\d{2})")


def load_zone(name: str) -> Zone:
    """A stored zone name back to a tzinfo: an IANA name or ``UTC-04:00``."""
    m = _FIXED_OFFSET.fullmatch(name)
    if m:
        delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
        return timezone(-delta if m.group(1) == "-" else delta)
    return ZoneInfo(name)


def zone_name(zone: Zone) -> str:
    """The inverse of `load_zone`, for a column."""
    return zone.key if isinstance(zone, ZoneInfo) else str(zone)


def user_zone_of(user_tz: str) -> Zone:
    try:
        return load_zone(user_tz)
    except Exception:
        return ZoneInfo("UTC")


def zone_of(name: Optional[str], user_tz: str) -> Zone:
    """The stored zone ``name``, or the user's when there is none to read."""
    if name:
        try:
            return load_zone(name)
        except Exception:
            pass
    return user_zone_of(user_tz)


def same_clock(a: Zone, b: Zone, at: Optional[datetime] = None) -> bool:
    """Whether ``a`` and ``b`` are one clock: the same IANA name, or, given
    ``at``, a fixed offset that reads the other's time there."""
    if isinstance(a, ZoneInfo) and isinstance(b, ZoneInfo):
        return a.key == b.key
    return at is not None and at.astimezone(a).utcoffset() == at.astimezone(b).utcoffset()


def utc(at: datetime) -> datetime:
    return at.astimezone(timezone.utc)


def when(at: datetime, zone: Zone) -> str:
    """``Tue 2026-10-20 10:00 EDT``; a fixed offset prints as ``UTC-04:00``."""
    return at.astimezone(zone).strftime("%a %Y-%m-%d %H:%M %Z")


def clock(at: datetime, zone: Zone, ref: datetime) -> str:
    """``10:00 EDT`` on ``ref``'s date on that clock, else the dated form."""
    local = at.astimezone(zone)
    if local.date() == ref.astimezone(zone).date():
        return local.strftime("%H:%M %Z")
    return when(at, zone)


def for_user(at: datetime, zone: Zone, user_zone: Zone) -> Optional[str]:
    """``Tue 22:00 for the user``, or None when the user reads the same clock."""
    if same_clock(zone, user_zone, at):
        return None
    return f"{at.astimezone(user_zone):%a %H:%M} for the user"


def lead(at: datetime, now: datetime) -> str:
    """``in 3 min``, ``in under a minute``, ``in 1 d 2 h 3 min``."""
    start = utc(now).replace(second=0, microsecond=0)
    mins = int((utc(at) - start).total_seconds() // 60)
    if mins < 1:
        return "in under a minute"
    d, rem = divmod(mins, 1440)
    h, m = divmod(rem, 60)
    return "in " + " ".join(
        [f"{d} d"] * bool(d) + [f"{h} h"] * bool(h) + [f"{m} min"] * bool(m)
    )


def header(user_tz: str, now: datetime) -> str:
    """Opens a listing; the user's own clock, so day names in it read as theirs."""
    local = now.astimezone(user_zone_of(user_tz))
    return f"Pending wakeups (the user is in {user_tz}, now {local:%a %H:%M}):"


def _every(gap: timedelta) -> str:
    minutes = int(gap.total_seconds() // 60)
    if minutes == 60:
        return "every hour"
    return (
        f"every {minutes // 60} hours" if minutes % 60 == 0 else f"every {minutes} min"
    )


def _runs(day: Sequence[datetime]) -> list[list[datetime]]:
    """Maximal evenly spaced runs, taken in order."""
    runs: list[list[datetime]] = []
    for at in day:
        if runs:
            run = runs[-1]
            if len(run) == 1 or utc(at) - utc(run[-1]) == utc(run[-1]) - utc(
                run[-2]
            ):
                run.append(at)
                continue
        runs.append([at])
    return runs


def by_day(fires: Sequence[datetime], zone: Zone) -> Optional[str]:
    """``By day, EDT: Tue 10:00 to 16:00 every hour (7), ...`` for a series over more than one day.

    First and last fire alone hide a pattern that is wrong only on the middle
    days, so each day is written out on the series' own clock. A day never
    collapses to first-to-last: a split range such as 22:00-23:00 plus
    00:00-04:00 would then read as one all-day window.
    """
    days: dict[Any, list[datetime]] = {}
    for at in fires:
        local = at.astimezone(zone)
        days.setdefault(local.date(), []).append(local)
    if len(days) <= 1:
        return None
    names = [fs[0].strftime("%a") for fs in days.values()]
    parts = []
    for fs in days.values():
        runs = _runs(fs)
        items = sum(1 if len(r) >= 3 else len(r) for r in runs)
        if items > 4:
            body = ", ".join(f"{f:%H:%M}" for f in fs)
        else:
            bits = []
            for run in runs:
                if len(run) >= 3:
                    every = _every(utc(run[1]) - utc(run[0]))
                    bits.append(
                        f"{run[0]:%H:%M} to {run[-1]:%H:%M} {every} ({len(run)})"
                    )
                else:
                    bits.extend(f"{f:%H:%M}" for f in run)
            body = ", ".join(bits)
        day = fs[0].strftime("%a")
        if names.count(day) > 1:
            day = fs[0].strftime("%a %m-%d")
        parts.append(f"{day} {body}")
    return f"By day, {fires[0].astimezone(zone):%Z}: " + ", ".join(parts)
