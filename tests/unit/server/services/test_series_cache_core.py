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


class TestExplicitLiveOverride:
    """``live=False`` pins a date-suffixed key even when the heuristic reads
    the window as live — a /bars ``before=`` page whose right edge lands in
    the UTC-12 zone must never read or fill the window-less live key."""

    def _svc(self):
        from src.server.services.cache.daily_cache_service import DailyCacheService

        DailyCacheService._instance = None
        return DailyCacheService.get_instance()

    def test_build_key_default_follows_heuristic(self, _frozen_clock):
        key = self._svc()._build_key("AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False)
        assert key == "ohlcv:AAPL.XNAS:ohlcv-1d"  # heuristic: live

    def test_build_key_live_false_forces_windowed_key(self, _frozen_clock):
        key = self._svc()._build_key(
            "AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False, live=False,
        )
        assert key == f"ohlcv:AAPL.XNAS:ohlcv-1d:2026-01-01:{_UTC_YESTERDAY}"

    @pytest.mark.asyncio
    async def test_find_cached_live_false_skips_legacy_dual_read(
        self, _frozen_clock, monkeypatch,
    ):
        """In the disagreement zone (heuristic says live, caller says
        historical) any legacy hit sits under the legacy LIVE key — adopting
        it would graft a live series onto a bounded window."""
        from src.server.services.cache import daily_cache_service as dcs

        svc = self._svc()

        class _EmptyCache:
            async def get(self, key):
                return None

            async def mget(self, keys):
                raise AssertionError("legacy dual-read must be skipped")

        monkeypatch.setattr(dcs, "get_cache_client", lambda: _EmptyCache())

        async def _no_provider():
            raise AssertionError("legacy dual-read must be skipped")

        monkeypatch.setattr(dcs, "get_market_data_provider", _no_provider)

        key, envelope = await svc._find_cached(
            "AAPL", "1day", "2026-01-01", _UTC_YESTERDAY, False, live=False,
        )
        assert (key, envelope) == (None, None)
