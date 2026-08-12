"""Staleness checks for OHLCV cache envelopes.

Focus: the two freshness primitives that gate cache serve-vs-refetch:
- ``_is_stale_date`` — date-level: envelope's trading date vs "now"'s.
- ``is_watermark_stale`` — interval-aware: watermark vs expected latest bar.

Historical bug covered here: a previous ``_is_stale_date`` short-circuited
to False whenever ``is_market_active()`` returned False. That let weekend
reads serve envelopes whose ``data_date`` was multiple trading days stale.
"""

from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.data_client.market_data_provider import _SUFFIX_MAP
from src.server.services.cache._instrument_clock import (
    _US_CLASS_SUFFIXES,
    CalendarClock,
    UsClock,
    clock_for,
)
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
    def test_empty_daily_envelope_is_not_stale(self):
        # An empty daily window has nothing to be behind — soft-TTL handles it.
        env = {"watermark": 0, "data_date": "1970-01-01", "bars": []}
        assert is_watermark_stale(env, "1day", symbol="AAPL") is False
        assert is_watermark_stale({"watermark": 0, "data_date": "1970-01-01"}, "1day", symbol="AAPL") is False

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
# is_watermark_stale — delayed-tier allowance
# ---------------------------------------------------------------------------

class TestDelayedTierWatermark:
    """A delayed feed's watermark legitimately trails the clock by its delay.

    Frozen-0700.HK incident: yfinance HK is delayed_15m, so mid-session the
    watermark sat ~15 min behind ``expected_latest_bar_ms``, tripping the
    2-bar tolerance on every request → discard + full upstream refetch storm.
    The declared header tier must widen the allowance.
    """

    HKT = ZoneInfo("Asia/Hong_Kong")

    def _mid_session_env(self, behind_minutes: int, tier: str | None) -> tuple[dict, datetime]:
        # Monday 2026-07-06 11:32 HKT — XHKG morning session is live.
        now = datetime(2026, 7, 6, 11, 32, tzinfo=self.HKT)
        clock = clock_for("0700.HK")
        expected_ms = clock.expected_latest_bar_ms("1min", now)
        assert expected_ms > 0  # sanity: session must be open at this moment
        watermark = expected_ms - behind_minutes * 60 * 1000
        env: dict = {"watermark": watermark, "bars": [{"time": watermark}]}
        if tier is not None:
            env["header"] = {"tier": tier}
        return env, now

    def test_delayed_feed_within_its_delay_is_fresh(self):
        env, now = self._mid_session_env(behind_minutes=15, tier="delayed_15m")
        assert is_watermark_stale(env, "1min", now=now, symbol="0700.HK") is False

    def test_realtime_feed_15_minutes_behind_is_stale(self):
        # Same lag without the delayed tier stays stale — the allowance is
        # opt-in per declared lineage, not a blanket loosening.
        env, now = self._mid_session_env(behind_minutes=15, tier="realtime")
        assert is_watermark_stale(env, "1min", now=now, symbol="0700.HK") is True

    def test_headerless_envelope_keeps_strict_tolerance(self):
        env, now = self._mid_session_env(behind_minutes=15, tier=None)
        assert is_watermark_stale(env, "1min", now=now, symbol="0700.HK") is True

    def test_delayed_feed_stagnating_past_its_delay_is_stale(self):
        # Delay allowance must not mask genuine mid-session stagnation.
        env, now = self._mid_session_env(behind_minutes=40, tier="delayed_15m")
        assert is_watermark_stale(env, "1min", now=now, symbol="0700.HK") is True


# ---------------------------------------------------------------------------
# is_watermark_stale — daily (1day) date-level backstop
# ---------------------------------------------------------------------------

class TestDailyWatermarkStale:
    """Daily staleness is judged DATE-level: the newest bar's ET trading date
    vs ``current_trading_date()``. This is the backstop that catches an
    envelope whose ``data_date`` was silently stamped with today's date by a
    prior refresh even though the bars never advanced (the daily analogue of
    the intraday screenshot bug). Daily bars are anchored to ET midnight, so a
    date-level comparison sidesteps the provider timestamp-anchor brittleness
    that motivated the old ``1day`` no-op.
    """

    @staticmethod
    def _daily_env(bar_date: datetime, *, data_date: str) -> dict:
        # Daily bars anchor to ET midnight of the trading day.
        wm = int(datetime(bar_date.year, bar_date.month, bar_date.day, 0, 0, tzinfo=ET).timestamp() * 1000)
        return {"watermark": wm, "bars": [{"time": wm}], "data_date": data_date}

    def test_behind_today_is_stale_even_when_data_date_lies(self):
        # The reported bug: Tue 2026-05-26 market open, newest bar is 2026-05-21,
        # but a prior refresh stamped data_date="2026-05-26". The date-level
        # check must still flag it stale so the cache re-fetches.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="AAPL") is True

    def test_today_bar_present_is_fresh(self):
        # Provider returns the in-progress current-day daily bar → not stale.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="AAPL") is False

    def test_friday_bar_on_saturday_is_fresh(self):
        # Saturday's current trading date is Friday; a Friday bar is current.
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 4, 17), data_date="2026-04-17")
        assert is_watermark_stale(env, "1day", now=saturday, symbol="AAPL") is False

    def test_missing_last_session_is_stale(self):
        # Saturday read, newest bar is Thursday — Friday's completed bar is
        # missing → stale.
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 4, 16), data_date="2026-04-17")
        assert is_watermark_stale(env, "1day", now=saturday, symbol="AAPL") is True

    def test_holiday_walks_back_to_last_trading_day(self):
        # Memorial Day 2026-05-25 is a holiday; current trading date is Fri 05-22.
        holiday = datetime(2026, 5, 25, 11, 0, tzinfo=ET)
        fresh = self._daily_env(datetime(2026, 5, 22), data_date="2026-05-22")
        stale = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-22")
        assert is_watermark_stale(fresh, "1day", now=holiday, symbol="AAPL") is False
        assert is_watermark_stale(stale, "1day", now=holiday, symbol="AAPL") is True

    def test_empty_daily_window_is_not_stale(self):
        # No bars → nothing to be behind; soft-TTL governs re-fetch.
        assert is_watermark_stale({"bars": [], "watermark": 0}, "1day", symbol="AAPL") is False

    def test_corrupt_daily_watermark_is_stale(self):
        # Bars present but watermark is 0 → corrupt, force re-fetch.
        assert is_watermark_stale({"bars": [{"time": 123}], "watermark": 0}, "1day", symbol="AAPL") is True

    # -- pre-market: today's daily bar doesn't exist yet -----------------
    # The freshest bar that can legitimately exist before 09:30 ET is the
    # PREVIOUS trading day's. Comparing against current_trading_date() (which
    # advances to today at 04:00 ET) falsely flagged a healthy cache stale and
    # caused a blocking sync re-fetch on every pre-market request.

    def test_premarket_yesterday_bar_is_fresh(self):
        # Wed 08:00 ET pre-market; cache holds Tue's completed bar. Wed's daily
        # bar can't exist yet → yesterday's bar is fresh, NOT stale.
        wed_premkt = datetime(2026, 5, 27, 8, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), data_date="2026-05-27")
        assert is_watermark_stale(env, "1day", now=wed_premkt, symbol="AAPL") is False

    def test_premarket_gross_staleness_still_caught(self):
        # Pre-market does NOT mean "never stale": a week-old bar (with a lying
        # data_date) is still behind the previous trading day → stale.
        wed_premkt = datetime(2026, 5, 27, 8, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 20), data_date="2026-05-27")
        assert is_watermark_stale(env, "1day", now=wed_premkt, symbol="AAPL") is True

    def test_premarket_after_weekend_walks_back(self):
        # Mon 08:00 ET pre-market; freshest possible bar is Fri's (weekend has
        # no sessions). A Friday bar is fresh.
        mon_premkt = datetime(2026, 5, 18, 8, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 15), data_date="2026-05-18")
        assert is_watermark_stale(env, "1day", now=mon_premkt, symbol="AAPL") is False

    def test_at_open_today_bar_expected(self):
        # At 09:30 ET the session has opened — today's in-progress bar should
        # exist. A cache still on yesterday is now stale (chases today's bar).
        wed_open = datetime(2026, 5, 27, 9, 30, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), data_date="2026-05-27")
        assert is_watermark_stale(env, "1day", now=wed_open, symbol="AAPL") is True

    # -- re-fetch cooldown: a just-fetched envelope is never stale ---------
    # Right after the open the provider may not have published today's daily
    # bar yet. A fresh fetch that still ends on yesterday must not be
    # re-flagged stale immediately, or every request would bypass the cache
    # with a blocking fetch until the provider catches up.

    def test_just_fetched_envelope_is_not_stale(self):
        wed_open = datetime(2026, 5, 27, 9, 35, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), data_date="2026-05-27")
        env["fetched_at"] = time.time()
        assert is_watermark_stale(env, "1day", now=wed_open, symbol="AAPL") is False

    def test_cooldown_expiry_restores_staleness(self):
        wed_open = datetime(2026, 5, 27, 9, 35, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), data_date="2026-05-27")
        env["fetched_at"] = time.time() - 300  # past the 120s cooldown
        assert is_watermark_stale(env, "1day", now=wed_open, symbol="AAPL") is True

    # -- non-US symbols: calendar-correct backstop (FLIPPED, Phase 3) ------
    # Pre-CMDP the backstop was DISABLED for non-US symbols (their daily bars
    # would read permanently stale against the ET calendar). Now each symbol
    # is judged against its own exchange calendar in its own timezone.

    def test_non_us_symbol_backstop_calendar_correct(self):
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        hkt = ZoneInfo("Asia/Hong_Kong")
        clock = clock_for("0700.HK")
        # A bar several sessions behind is stale by the XHKG calendar.
        behind = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(behind, "1day", now=tue_open, symbol="0700.HK") is True
        # A bar on the newest expected XHKG session (midnight HKT anchor,
        # matching the fixed FMP localization) is fresh.
        expected = clock.expected_latest_daily_date(tue_open)
        y, m, d = (int(x) for x in expected.split("-"))
        wm = int(datetime(y, m, d, 0, 0, tzinfo=hkt).timestamp() * 1000)
        fresh = {"watermark": wm, "bars": [{"time": wm}], "data_date": expected}
        assert is_watermark_stale(fresh, "1day", now=tue_open, symbol="0700.HK") is False

    def test_unknown_index_family_skips_backstop(self):
        # Index symbols aren't suffix-classifiable; unknown families fail
        # closed (no backstop) rather than guessing a calendar.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="HSI", is_index=True) is False

    def test_us_symbol_and_index_keep_backstop(self):
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="AAPL") is True
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="GSPC", is_index=True) is True

    def test_us_dotted_class_share_keeps_backstop(self):
        # BRK.B / BF.B classify as "other" (unmapped suffix), not a foreign
        # region, so the US backstop must still engage for them.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="BRK.B") is True
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="BF.B") is True

    def test_expanded_index_allowlist_keeps_backstop(self):
        # An index in the clock's US-calendar allowlist gets the backstop.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open, symbol="SOX", is_index=True) is True

    def test_no_symbol_fails_closed(self):
        # No symbol → can't classify the calendar anchor → the backstop is
        # skipped entirely, even for a grossly behind watermark. Guards the
        # symbol-less intraday call sites if "1day" ever routes through them.
        tue_open = datetime(2026, 5, 26, 11, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 21), data_date="2026-05-26")
        assert is_watermark_stale(env, "1day", now=tue_open) is False


# ---------------------------------------------------------------------------
# is_watermark_stale — daily post-close settle
# ---------------------------------------------------------------------------

class TestDailyPostCloseSettle:
    """An envelope written mid-session holds a partial-day head candle — its
    OHLCV froze at fetch time. Crossing a settledness rung (open→post at the
    bell, post→closed after hours) must flag it stale so the head bar settles
    at the official close; equal or descending rungs must not (weekend cache
    hits stay byte-stable).
    """

    @staticmethod
    def _daily_env(bar_date: datetime, *, stored_phase: str | None, fetched_at: float = 0) -> dict:
        wm = int(datetime(bar_date.year, bar_date.month, bar_date.day, 0, 0, tzinfo=ET).timestamp() * 1000)
        env = {"watermark": wm, "bars": [{"time": wm}], "market_phase": stored_phase}
        if fetched_at:
            env["fetched_at"] = fetched_at
        return env

    # Tue 2026-05-26 is a regular XNYS session.
    MID_SESSION = datetime(2026, 5, 26, 12, 0, tzinfo=ET)
    POST = datetime(2026, 5, 26, 17, 0, tzinfo=ET)
    NIGHT = datetime(2026, 5, 26, 21, 0, tzinfo=ET)

    def test_open_envelope_settles_at_the_bell(self):
        env = self._daily_env(datetime(2026, 5, 26), stored_phase="open")
        assert is_watermark_stale(env, "1day", now=self.POST, symbol="AAPL") is True

    def test_open_envelope_settles_when_fully_closed(self):
        env = self._daily_env(datetime(2026, 5, 26), stored_phase="open")
        assert is_watermark_stale(env, "1day", now=self.NIGHT, symbol="AAPL") is True

    def test_post_envelope_resettles_at_consolidated_close(self):
        # A bar fetched right after the bell may predate the consolidated
        # close — one more refetch when the venue fully closes.
        env = self._daily_env(datetime(2026, 5, 26), stored_phase="post")
        assert is_watermark_stale(env, "1day", now=self.NIGHT, symbol="AAPL") is True

    def test_same_rung_is_fresh(self):
        post_env = self._daily_env(datetime(2026, 5, 26), stored_phase="post")
        assert is_watermark_stale(post_env, "1day", now=self.POST, symbol="AAPL") is False
        open_env = self._daily_env(datetime(2026, 5, 26), stored_phase="open")
        assert is_watermark_stale(open_env, "1day", now=self.MID_SESSION, symbol="AAPL") is False

    def test_closed_envelope_stays_byte_stable_on_weekend(self):
        saturday = datetime(2026, 5, 30, 10, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 29), stored_phase="closed")
        assert is_watermark_stale(env, "1day", now=saturday, symbol="AAPL") is False

    def test_closed_envelope_next_premarket_is_fresh(self):
        # Descending rung (closed → pre) must not refetch; the date-level
        # checks own new-day transitions.
        wed_premkt = datetime(2026, 5, 27, 8, 0, tzinfo=ET)
        env = self._daily_env(datetime(2026, 5, 26), stored_phase="closed")
        assert is_watermark_stale(env, "1day", now=wed_premkt, symbol="AAPL") is False

    def test_cooldown_shields_fresh_fetches(self):
        env = self._daily_env(datetime(2026, 5, 26), stored_phase="open", fetched_at=time.time())
        assert is_watermark_stale(env, "1day", now=self.POST, symbol="AAPL") is False

    def test_phaseless_envelope_skips_the_settle_check(self):
        env = self._daily_env(datetime(2026, 5, 26), stored_phase=None)
        assert is_watermark_stale(env, "1day", now=self.NIGHT, symbol="AAPL") is False

    def test_hk_lunch_envelope_settles_after_hk_close(self):
        # Non-US venue on its own calendar: an envelope written during the
        # XHKG session (legacy phase "open") settles once HK closes.
        hkt = ZoneInfo("Asia/Hong_Kong")
        hk_evening = datetime(2026, 5, 26, 22, 0, tzinfo=hkt)
        wm = int(datetime(2026, 5, 26, 0, 0, tzinfo=hkt).timestamp() * 1000)
        env = {"watermark": wm, "bars": [{"time": wm}], "market_phase": "open"}
        assert is_watermark_stale(env, "1day", now=hk_evening, symbol="0700.HK") is True


# ---------------------------------------------------------------------------
# clock_for — instrument → market clock classification
# ---------------------------------------------------------------------------

class TestClockClassification:
    def test_us_class_suffixes_disjoint_from_foreign_suffix_map(self):
        # Trusting the class-share suffixes as US-calendar is safe only while
        # none of them doubles as a foreign region suffix in _SUFFIX_MAP
        # (which already carries single-letter codes like L/T). A future
        # addition must fail here, not silently misclassify foreign symbols.
        assert _US_CLASS_SUFFIXES.isdisjoint(_SUFFIX_MAP)

    def test_bare_us_ticker_gets_us_clock_with_backstop(self):
        clock = clock_for("AAPL")
        assert isinstance(clock, UsClock) and clock.daily_backstop is True
        assert clock_for("AAPL.US").daily_backstop is True

    def test_us_dotted_class_shares_keep_backstop(self):
        for sym in ("BRK.B", "BF.B", "HEI.A"):
            clock = clock_for(sym)
            assert isinstance(clock, UsClock) and clock.daily_backstop is True, sym

    def test_unknown_dotted_suffix_fails_closed(self):
        clock = clock_for("FOO.XYZ")
        assert isinstance(clock, UsClock) and clock.daily_backstop is False

    def test_foreign_suffixes_get_their_calendar(self):
        cases = {
            "0700.HK": "Asia/Hong_Kong",
            "600519.SS": "Asia/Shanghai",
            "RDS.L": "Europe/London",
            "7203.T": "Asia/Tokyo",
        }
        for sym, tz_key in cases.items():
            clock = clock_for(sym)
            assert isinstance(clock, CalendarClock), sym
            assert str(clock.tz) == tz_key, sym
            assert clock.daily_backstop is True, sym

    def test_index_allowlist_membership(self):
        assert clock_for("GSPC", True).daily_backstop is True
        assert clock_for("^SOX", True).daily_backstop is True
        assert clock_for("I:SPX", True).daily_backstop is True
        assert clock_for("HSI", True).daily_backstop is False

    def test_none_symbol_is_us_parity(self):
        assert isinstance(clock_for(None), UsClock)


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
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        env = self._screenshot_envelope(march_30)
        assert ic._should_discard_envelope(env, interval="5min") is True

    def test_fifteen_min_3_weeks_stale_is_discarded(self, monkeypatch):
        from src.server.services.cache import intraday_cache_service as ic

        march_1 = datetime(2026, 3, 1, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

        env = self._screenshot_envelope(march_1)
        assert ic._should_discard_envelope(env, interval="15min") is True

    def test_one_hour_3_weeks_stale_is_discarded(self, monkeypatch):
        from src.server.services.cache import intraday_cache_service as ic

        march_31 = datetime(2026, 3, 31, 15, 0, tzinfo=ET)
        now = datetime(2026, 4, 22, 20, 49, tzinfo=ET)
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
        monkeypatch.setattr("src.utils.market_hours.datetime", _FrozenDatetime(now))

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
