"""Options snapshots as the agent reads them.

ginlix-data's snapshot rows carry Unix-ns times and say nothing about whether a
session is today's: a contract that last traded weeks ago keeps that day's
close, change and volume. A row here carries its times exchange-local and marks
such a session ``stale``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from market_protocol.calendars import get_calendar

# ginlix-data lists US contracts only, on the NYSE calendar.
_CALENDAR = "XNYS"

# How far behind the clock a feed of each Massive timeframe runs.
_LAG = {"DELAYED": timedelta(minutes=15)}

# Vendor fields that carry Unix ns.
_STAMPS = ("last_updated", "sip_timestamp", "fmv_last_updated")

_STAMP = "%Y-%m-%d %H:%M:%S"


def feed_lag(timeframe: str | None) -> timedelta:
    """How far behind the clock a feed of Massive *timeframe* runs."""
    return _LAG.get(timeframe, timedelta(0))


def _local(ns: int, cal: Any) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, cal.tz)


def _local_times(part: dict[str, Any], cal: Any) -> dict[str, Any]:
    return {
        k: _local(v, cal).strftime(_STAMP) if k in _STAMPS and isinstance(v, int) else v
        for k, v in part.items()
    }


def snapshot_row(raw: dict[str, Any], now: datetime) -> dict[str, Any]:
    """One contract's snapshot as ginlix-data sent it, less the contract terms
    (``details``), with every time exchange-local.

    ``session.stale`` is true when the session's last update is from a trading
    day before the one the feed has reached at *now*, so its close, change and
    volume are that older day's. A session with no time gets no mark: it may be
    from any past day, and None would read as not stale.
    """
    cal = get_calendar(_CALENDAR)
    row = _local_times({k: v for k, v in raw.items() if k != "details"}, cal)
    for key, part in row.items():
        if isinstance(part, dict):
            row[key] = _local_times(part, cal)
    session, stamp = row.get("session"), (raw.get("session") or {}).get("last_updated")
    if session is not None and isinstance(stamp, int):
        reached = now - feed_lag(session.get("timeframe"))
        traded = _local(stamp, cal).date()
        session["stale"] = traded < cal.expected_latest_daily_date(reached)
    underlying = row.get("underlying_asset")
    if underlying is not None and "value" in underlying:
        # An index underlying is priced as a value.
        underlying.setdefault("price", underlying.pop("value"))
    return row
