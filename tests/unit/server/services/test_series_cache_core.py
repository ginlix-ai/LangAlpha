"""Unit tests for the OHLCV series-cache core helpers.

Covers ``is_live_window`` — the "may this window still grow?" TTL gate. The
guard compares *to_date* against the western-most plausible venue-local date
(UTC minus 12h), NOT the server's ``date.today()``. On a UTC host that matters:
at 00:30 UTC an ET trading window is still "today" in New York (20:30 the prior
day), so the window must read live. A naive ``date.today()`` on UTC would flip
it historical at 00:00 UTC and freeze the live evening session.
"""

import datetime as dt

import pytest

from src.server.services.cache import _series_cache_core as mod
from src.server.services.cache._series_cache_core import is_live_window

# 00:30 UTC on 2026-07-07. UTC-12h floors to 2026-07-06 — i.e. ET's "today",
# since New York at this instant is 20:30 on 2026-07-06.
_FIXED_UTC = dt.datetime(2026, 7, 7, 0, 30, tzinfo=dt.timezone.utc)
_UTC_YESTERDAY = "2026-07-06"  # == the UTC-12 floor / venue-local today (ET)
_TWO_DAYS_BACK = "2026-07-05"


class _FrozenDatetime(dt.datetime):
    """`datetime` whose ``now()`` is pinned to ``_FIXED_UTC``."""

    @classmethod
    def now(cls, tz=None):
        return _FIXED_UTC if tz is None else _FIXED_UTC.astimezone(tz)


@pytest.fixture
def _frozen_clock(monkeypatch):
    monkeypatch.setattr(mod, "datetime", _FrozenDatetime)


class TestIsLiveWindow:
    def test_venue_local_today_is_live(self, _frozen_clock):
        # to_date == the UTC-12 floor (ET's current date) → still growable.
        assert is_live_window(_UTC_YESTERDAY) is True

    def test_two_days_back_is_historical(self, _frozen_clock):
        # A window ending before the floor can no longer grow.
        assert is_live_window(_TWO_DAYS_BACK) is False

    def test_none_to_date_is_live(self, _frozen_clock):
        # An open-ended window (no explicit to_date) is always live.
        assert is_live_window(None) is True

    def test_unparseable_to_date_defaults_to_live(self, _frozen_clock):
        # Fail-open: a non-ISO string is never treated as narrower than live.
        assert is_live_window("not-a-date") is True
