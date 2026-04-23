"""Staleness checks for OHLCV cache envelopes.

Focus: the two freshness primitives that gate cache serve-vs-refetch:
- ``_is_stale_date`` — date-level: envelope's trading date vs "now"'s.
- ``is_watermark_stale`` — interval-aware: watermark vs expected latest bar.

Historical bug covered here: a previous ``_is_stale_date`` short-circuited
to False whenever ``is_market_active()`` returned False. That let weekend
reads serve envelopes whose ``data_date`` was multiple trading days stale.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.server.services.cache._ohlcv_envelope import (
    _is_stale_date,
    is_watermark_stale,
)
from src.utils.market_hours import expected_latest_bar_ms

ET = ZoneInfo("America/New_York")


def _env(data_date: str, watermark_ms: int = 0) -> dict:
    return {"data_date": data_date, "watermark": watermark_ms}


# ---------------------------------------------------------------------------
# _is_stale_date
# ---------------------------------------------------------------------------

class TestIsStaleDate:
    def test_missing_data_date_is_stale(self):
        assert _is_stale_date({}) is True

    def test_weekend_envelope_from_prior_week_is_stale(self):
        # Bug fix: Saturday read of a Wednesday envelope used to return False
        # because `is_market_active()` was False. It must now return True.
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        env = _env("2026-04-15")  # Wednesday
        assert _is_stale_date(env, now=saturday) is True

    def test_weekend_envelope_from_friday_is_fresh(self):
        # The most recent trading day as seen from Saturday is Friday.
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        env = _env("2026-04-17")  # Friday
        assert _is_stale_date(env, now=saturday) is False

    def test_holiday_envelope_walks_back_to_last_trading_day(self):
        # Good Friday 2026-04-03 is a holiday. Reading on that day, the
        # most recent trading date is Thursday 2026-04-02.
        holiday = datetime(2026, 4, 3, 11, 0, tzinfo=ET)
        assert _is_stale_date(_env("2026-04-02"), now=holiday) is False
        assert _is_stale_date(_env("2026-04-01"), now=holiday) is True

    def test_midsession_same_day_is_fresh(self):
        wed = datetime(2026, 4, 15, 10, 30, tzinfo=ET)
        env = _env("2026-04-15")
        assert _is_stale_date(env, now=wed) is False

    def test_midsession_prior_day_is_stale(self):
        wed = datetime(2026, 4, 15, 10, 30, tzinfo=ET)
        env = _env("2026-04-14")
        assert _is_stale_date(env, now=wed) is True


# ---------------------------------------------------------------------------
# expected_latest_bar_ms
# ---------------------------------------------------------------------------

class TestExpectedLatestBarMs:
    def test_midsession_floors_to_interval(self):
        # Wed 10:07:30 ET, 5-minute bars → expected = 10:05 bar
        now = datetime(2026, 4, 15, 10, 7, 30, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("5min", now=now)
        expected_dt = datetime.fromtimestamp(expected_ms / 1000, tz=ET)
        assert expected_dt.hour == 10 and expected_dt.minute == 5

    def test_weekend_anchors_to_friday_close(self):
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("5min", now=saturday)
        expected_dt = datetime.fromtimestamp(expected_ms / 1000, tz=ET)
        assert expected_dt.date().isoformat() == "2026-04-17"  # Friday
        assert (expected_dt.hour, expected_dt.minute) == (16, 0)

    def test_holiday_anchors_to_prior_trading_day(self):
        # Good Friday 2026-04-03 off-hours (before pre-open)
        off_hours = datetime(2026, 4, 3, 2, 0, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("15min", now=off_hours)
        expected_dt = datetime.fromtimestamp(expected_ms / 1000, tz=ET)
        # Most recent trading day is Thursday 2026-04-02.
        assert expected_dt.date().isoformat() == "2026-04-02"

    def test_daily_interval_returns_close_of_most_recent_trading_day(self):
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("1day", now=saturday)
        expected_dt = datetime.fromtimestamp(expected_ms / 1000, tz=ET)
        assert expected_dt.date().isoformat() == "2026-04-17"


# ---------------------------------------------------------------------------
# is_watermark_stale
# ---------------------------------------------------------------------------

class TestIsWatermarkStale:
    def test_daily_is_always_false(self):
        # Daily staleness is handled at the date level.
        env = {"watermark": 0, "data_date": "1970-01-01"}
        assert is_watermark_stale(env, "1day") is False

    def test_empty_envelope_is_not_stale(self):
        # Empty-bar envelopes (no data in requested window) are deliberately
        # short-TTL'd via _EMPTY_RESULT_TTL to dampen fetch storms. Watermark
        # check must not discard them — let the TTL handle re-fetch timing.
        assert is_watermark_stale({"watermark": 0, "bars": []}, "5min") is False
        assert is_watermark_stale({"bars": []}, "5min") is False
        assert is_watermark_stale({}, "5min") is False

    def test_corrupt_envelope_with_bars_but_zero_watermark_is_stale(self):
        # Bars present but watermark is 0 — envelope is corrupt, treat as stale
        # so the next request forces a sync re-fetch.
        env = {"watermark": 0, "bars": [{"time": 1234567890}]}
        assert is_watermark_stale(env, "5min") is True

    def test_watermark_at_expected_is_fresh(self):
        now = datetime(2026, 4, 15, 10, 10, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("5min", now=now)
        env = {"watermark": expected_ms, "bars": [{"time": expected_ms}]}
        assert is_watermark_stale(env, "5min", now=now) is False

    def test_watermark_one_period_behind_is_within_tolerance(self):
        now = datetime(2026, 4, 15, 10, 10, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("5min", now=now)
        one_period_ms = 5 * 60 * 1000
        watermark = expected_ms - one_period_ms
        env = {"watermark": watermark, "bars": [{"time": watermark}]}
        # tolerance = 2 periods → 1 period behind is still fresh
        assert is_watermark_stale(env, "5min", now=now) is False

    def test_watermark_three_periods_behind_is_stale(self):
        now = datetime(2026, 4, 15, 10, 10, tzinfo=ET)
        expected_ms = expected_latest_bar_ms("5min", now=now)
        one_period_ms = 5 * 60 * 1000
        watermark = expected_ms - 3 * one_period_ms
        env = {"watermark": watermark, "bars": [{"time": watermark}]}
        assert is_watermark_stale(env, "5min", now=now) is True

    def test_overnight_stagnation_detected(self):
        # Monday 10:00 ET, but watermark is from Friday 15:00 ET (3+ days old).
        monday = datetime(2026, 4, 20, 10, 0, tzinfo=ET)
        friday_1500 = datetime(2026, 4, 17, 15, 0, tzinfo=ET)
        watermark = int(friday_1500.timestamp() * 1000)
        env = {"watermark": watermark, "bars": [{"time": watermark}]}
        assert is_watermark_stale(env, "5min", now=monday) is True

    def test_weekend_envelope_last_bar_is_friday_close_fresh(self):
        # Saturday read; watermark is Friday 16:00 ET. Fresh.
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        friday_close = datetime(2026, 4, 17, 16, 0, tzinfo=ET)
        watermark = int(friday_close.timestamp() * 1000)
        env = {"watermark": watermark, "bars": [{"time": watermark}]}
        assert is_watermark_stale(env, "5min", now=saturday) is False


# ---------------------------------------------------------------------------
# Integration: _should_discard_envelope on the exact screenshot scenario
# ---------------------------------------------------------------------------

class TestDiscardEnvelopeScreenshotScenario:
    """Reproduce the exact staleness the user saw in the dashboard screenshot.

    Context: 2026-04-22 evening, market closed. Widget shows NVDA 5m / 15m /
    1H with bars from late March (~3 weeks stale). If ``_should_discard_envelope``
    returns True for these envelopes, the live backend will discard and
    sync-refetch. If it returns False, the fix is missing something.
    """

    def _screenshot_envelope(self, watermark_dt: datetime) -> dict:
        # Mimics an envelope that got silently marked with today's trading
        # date by a prior delta-refresh despite bars not advancing.
        return {
            "v": 3,
            "bars": [{"time": int(watermark_dt.timestamp() * 1000)}],
            "watermark": int(watermark_dt.timestamp() * 1000),
            "fetched_at": 0,
            "market_phase": "closed",
            "complete": False,
            "stored_ttl": 0,
            "data_date": "2026-04-22",  # rewritten by a delta that fetched nothing new
            "truncated": False,
        }

    def test_five_min_3_weeks_stale_is_discarded(self, monkeypatch):
        from src.server.services.cache import intraday_cache_service as ic

        march_30 = datetime(2026, 3, 30, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        env = self._screenshot_envelope(march_30)
        assert ic._should_discard_envelope(env, interval="5min") is True

    def test_fifteen_min_3_weeks_stale_is_discarded(self, monkeypatch):
        from src.server.services.cache import intraday_cache_service as ic

        march_1 = datetime(2026, 3, 1, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        env = self._screenshot_envelope(march_1)
        assert ic._should_discard_envelope(env, interval="15min") is True

    def test_one_hour_3_weeks_stale_is_discarded(self, monkeypatch):
        from src.server.services.cache import intraday_cache_service as ic

        march_31 = datetime(2026, 3, 31, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        env = self._screenshot_envelope(march_31)
        assert ic._should_discard_envelope(env, interval="1hour") is True

    def test_historical_envelope_is_not_discarded_despite_stale_watermark(self, monkeypatch):
        # Regression guard: historical cache keys (with :{from_date}:{to_date}
        # suffix) intentionally carry watermarks in the past. Passing is_live=False
        # must skip both the stale-date check and the stale-watermark check so
        # historical cache hits are preserved across day boundaries.
        from src.server.services.cache import intraday_cache_service as ic

        # Historical envelope: bars from March 2026, read on April 22
        march_30 = datetime(2026, 3, 30, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        watermark = int(march_30.timestamp() * 1000)
        env = {
            "v": 3,
            "bars": [{"time": watermark}],
            "watermark": watermark,
            "fetched_at": 0,
            "market_phase": "closed",
            "complete": True,
            "stored_ttl": 86400,
            "data_date": "2026-03-30",  # historical date, from when the range was fetched
            "truncated": False,
        }
        # Default (is_live=True) discards as expected for the screenshot repro
        assert ic._should_discard_envelope(env, interval="5min") is True
        # Historical path (is_live=False) preserves the envelope
        assert ic._should_discard_envelope(env, interval="5min", is_live=False) is False

    def test_empty_envelope_within_ttl_is_not_discarded(self, monkeypatch):
        # Regression guard: symbols with genuinely no data in the requested
        # window get cached with _EMPTY_RESULT_TTL (short TTL) to dampen fetch
        # storms. The watermark-stale check must NOT force discard here —
        # otherwise every repeat request within the 30s TTL re-hits upstream.
        from src.server.services.cache import intraday_cache_service as ic

        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        # Empty-bars envelope (no data for the symbol/window)
        env = {
            "v": 3,
            "bars": [],
            "watermark": 0,
            "fetched_at": 0,
            "market_phase": "closed",
            "complete": False,
            "stored_ttl": 30,
            "data_date": "2026-04-22",
            "truncated": False,
        }
        assert ic._should_discard_envelope(env, interval="5min") is False

    def test_one_min_with_recent_watermark_is_not_discarded(self, monkeypatch):
        # Control: a 1min envelope with bars covering today's full session
        # (first bar at open, last bar at close) must NOT be discarded —
        # proves the fix doesn't over-fire. Includes a bar at open so the
        # separate coverage-gap check doesn't trip.
        from src.server.services.cache import intraday_cache_service as ic

        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        open_dt = datetime(2026, 4, 22, 9, 30, tzinfo=ET)
        close_dt = datetime(2026, 4, 22, 16, 0, tzinfo=ET)
        monkeypatch.setattr(ic, "is_market_closed", lambda _now=None: True)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))
        # today_market_open_ms uses the real datetime — mock it directly too.
        monkeypatch.setattr(
            ic,
            "today_market_open_ms",
            lambda: int(open_dt.timestamp() * 1000),
        )

        env = {
            "v": 3,
            "bars": [
                {"time": int(open_dt.timestamp() * 1000)},
                {"time": int(close_dt.timestamp() * 1000)},
            ],
            "watermark": int(close_dt.timestamp() * 1000),
            "fetched_at": 0,
            "market_phase": "closed",
            "complete": False,
            "stored_ttl": 0,
            "data_date": "2026-04-22",
            "truncated": False,
        }
        assert ic._should_discard_envelope(env, interval="1min") is False


class _FrozenDatetime:
    """Minimal ``datetime`` shim so market_hours' ``datetime.now(ET)`` returns
    a fixed moment under monkeypatch. Using ``freezegun`` would be cleaner but
    isn't already a dep here; this stub is enough for two call sites."""

    def __init__(self, now: datetime):
        self._now = now

    def now(self, tz=None):
        if tz is None:
            return self._now
        return self._now.astimezone(tz)

    def combine(self, *args, **kwargs):
        from datetime import datetime as _dt

        return _dt.combine(*args, **kwargs)

    def fromtimestamp(self, *args, **kwargs):
        from datetime import datetime as _dt

        return _dt.fromtimestamp(*args, **kwargs)
