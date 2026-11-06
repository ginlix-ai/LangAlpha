"""Date bounds the stdio market-data tools read as the hosted ones do (``mcp_servers/_dates.py``).

The refusals and their wording are pinned to the hosted implementation's, so an
agent sees the same error whichever deployment answers it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mcp_servers._dates import (
    NEW_YORK,
    DateBoundError,
    calendar_days,
    commodity_daily_days,
    daily_days,
    intraday_window,
    rates_days,
    upcoming_days,
    whole_daily_days,
)
from mcp_servers._envelope import error_from_exception


def _refusal(fn, *args) -> str:
    with pytest.raises(DateBoundError) as info:
        fn(*args)
    return str(info.value)


def _future_start() -> tuple[str, str]:
    """A start after today, and the refusal it gets."""
    today = datetime.now(timezone(timedelta(hours=14))).date()
    return (
        (today + timedelta(days=30)).isoformat(),
        f"from_date is in the future (the latest date anywhere is {today.isoformat()})",
    )


def _kept(window, *labels: str) -> list[str]:
    return [row["date"] for row in window.rows([{"date": label} for label in labels])]


def test_refusal_reaches_the_agent_as_invalid_argument():
    err = error_from_exception(DateBoundError("from is after to"), "FMP request failed")
    assert err == {"error": "invalid_argument", "detail": "from is after to"}


class TestCalendarDays:
    def test_absent_bounds_stay_absent(self):
        assert calendar_days(None, "") == (None, None)

    def test_one_bound_is_enough(self):
        assert calendar_days("2026-10-12", None) == ("2026-10-12", None)

    @pytest.mark.parametrize(
        "lo, hi, detail",
        [
            ("not-a-date", None, "from must be an ISO date, not 'not-a-date'"),
            (None, "2026-10-16 10:00", "to must be an ISO date, not '2026-10-16 10:00'"),
            ("2026-10-16", "2026-10-09", "from is after to"),
        ],
    )
    def test_refused(self, lo, hi, detail):
        assert _refusal(calendar_days, lo, hi) == detail


class TestUpcomingDays:
    def test_absent_bounds_are_yesterday_and_a_week_after_it(self):
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        assert upcoming_days(None, "") == (
            yesterday.isoformat(),
            (yesterday + timedelta(days=7)).isoformat(),
        )

    def test_a_lone_start_ends_a_week_after_itself(self):
        today = datetime.now(timezone.utc).date()
        far = (today + timedelta(days=90)).isoformat()
        assert upcoming_days("2026-10-01", None) == ("2026-10-01", "2026-10-08")
        assert upcoming_days(far, None) == (far, (today + timedelta(days=97)).isoformat())

    def test_a_lone_end_from_yesterday_on_keeps_the_default_start(self):
        today = datetime.now(timezone.utc).date()
        yesterday = (today - timedelta(days=1)).isoformat()
        end = (today + timedelta(days=30)).isoformat()
        assert upcoming_days(None, end) == (yesterday, end)
        assert upcoming_days(None, yesterday) == (yesterday, yesterday)

    def test_a_lone_end_before_yesterday_starts_a_week_before_itself(self):
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        day_before = yesterday - timedelta(days=1)
        assert upcoming_days(None, "2020-01-31") == ("2020-01-24", "2020-01-31")
        assert upcoming_days(None, day_before.isoformat()) == (
            (day_before - timedelta(days=7)).isoformat(),
            day_before.isoformat(),
        )
        assert upcoming_days(None, "2024-03-02") == ("2024-02-24", "2024-03-02")

    def test_a_week_after_a_start_rolls_over_the_year(self):
        assert upcoming_days("2026-12-28", None) == ("2026-12-28", "2027-01-04")

    def test_a_week_beyond_the_calendar_is_refused(self):
        assert _refusal(upcoming_days, "9999-12-30", None) == "date is out of range"
        assert _refusal(upcoming_days, None, "0001-01-02") == "date is out of range"

    def test_reversed_and_unreadable_bounds_are_refused_as_for_a_calendar(self):
        assert _refusal(upcoming_days, "2026-10-16", "2026-10-09") == "from is after to"
        assert _refusal(upcoming_days, "not-a-date", None) == (
            "from must be an ISO date, not 'not-a-date'"
        )


class TestRatesDays:
    def test_an_absent_end_is_today_and_an_absent_start_stays_absent(self):
        today = datetime.now(timezone.utc).date().isoformat()
        assert rates_days(None, None) == (None, today)
        assert rates_days("2026-10-12", "") == ("2026-10-12", today)

    def test_a_given_end_stands(self):
        assert rates_days("2026-10-12", "2026-10-16") == ("2026-10-12", "2026-10-16")

    def test_a_lone_future_start_is_refused_against_the_default_end(self):
        today = datetime.now(timezone.utc).date()
        start = (today + timedelta(days=30)).isoformat()
        assert _refusal(rates_days, start, None) == (
            f"from is after to, which is today ({today.isoformat()}) unless given"
        )

    def test_reversed_bounds_are_refused(self):
        assert _refusal(rates_days, "2026-10-16", "2026-10-09") == "from is after to"


class TestDailyDays:
    def test_time_of_day_is_dropped(self):
        assert daily_days("2026-10-14 10:00", "2026-10-16") == ("2026-10-14", "2026-10-16")

    def test_absent_bounds_left_to_the_client(self):
        assert daily_days(None, "") == (None, None)

    @pytest.mark.parametrize(
        "lo, hi, detail",
        [
            ("not-a-date", None, "from_date must be YYYY-MM-DD, not 'not-a-date'"),
            (None, "2026-13-01", "to_date must be YYYY-MM-DD, not '2026-13-01'"),
            ("2026-09-30", "2026-09-01", "start is after end"),
        ],
    )
    def test_refused(self, lo, hi, detail):
        assert _refusal(daily_days, lo, hi) == detail

    def test_a_lone_end_runs_the_client_window_back_from_it(self):
        # From today, an end over 500 days old would be a reversed window.
        assert daily_days(None, "2020-01-31") == ("2018-09-18", "2020-01-31")

    def test_a_lone_start_is_left_to_run_to_today(self):
        assert daily_days("2026-01-02", None) == ("2026-01-02", None)

    def test_a_lone_future_start_is_refused_as_in_the_future(self):
        start, detail = _future_start()
        assert _refusal(daily_days, start, None) == detail


class TestCommodityDailyDays:
    def test_only_the_day_is_sent(self):
        assert commodity_daily_days("2026-10-16 10:00", "2026-10-16 12:00") == (
            "2026-10-16",
            "2026-10-16",
        )

    def test_reversed_refused(self):
        assert _refusal(commodity_daily_days, "2026-10-16", "2026-10-01") == "from is after to"

    def test_lone_bounds_read_as_for_daily_bars(self):
        assert commodity_daily_days(None, "2020-01-31") == ("2018-09-18", "2020-01-31")
        start, detail = _future_start()
        assert _refusal(commodity_daily_days, start, None) == detail


class TestIntradayWindow:
    def test_time_of_day_bounds_are_inclusive(self):
        window = intraday_window("2026-10-16 10:00", "2026-10-16 11:00", "Asia/Hong_Kong")
        assert (window.from_day, window.to_day) == ("2026-10-16", "2026-10-16")
        assert _kept(
            window,
            "2026-10-15 10:30:00",
            "2026-10-16 09:55:00",
            "2026-10-16 10:00:00",
            "2026-10-16 11:00:00",
            "2026-10-16 11:05:00",
        ) == ["2026-10-16 10:00:00", "2026-10-16 11:00:00"]

    def test_bare_end_day_spans_the_day(self):
        window = intraday_window("2026-10-15", "2026-10-16", "America/New_York")
        assert _kept(
            window, "2026-10-15 00:00:00", "2026-10-16 23:59:00", "2026-10-17 00:00:00"
        ) == ["2026-10-15 00:00:00", "2026-10-16 23:59:00"]

    def test_offset_bound_read_in_the_venue_zone(self):
        window = intraday_window(
            "2026-10-16T02:00:00Z", "2026-10-16T03:00:00Z", "Asia/Hong_Kong"
        )
        assert _kept(
            window, "2026-10-16 09:55:00", "2026-10-16 10:00:00", "2026-10-16 11:05:00"
        ) == ["2026-10-16 10:00:00"]

    def test_one_instant_is_a_window(self):
        window = intraday_window("2026-10-16 10:00", "2026-10-16 10:00", "Asia/Hong_Kong")
        assert _kept(window, "2026-10-16 10:00:00", "2026-10-16 10:01:00") == [
            "2026-10-16 10:00:00"
        ]

    def test_a_label_that_does_not_read_is_dropped(self):
        window = intraday_window("2026-10-16", "2026-10-16", "America/New_York")
        assert _kept(window, "2026-10-16", "", "2026-10-16 10:00:00") == ["2026-10-16 10:00:00"]

    def test_new_york_labels_are_read_and_written_in_the_windows_zone(self):
        # A UTC hour just after midnight is the previous New York evening,
        # so that is the day FMP is asked for.
        window = intraday_window("2026-10-16 00:00", "2026-10-16 01:00", "UTC", labels=NEW_YORK)
        assert (window.from_day, window.to_day) == ("2026-10-15", "2026-10-15")
        assert _kept(
            window,
            "2026-10-15 19:55:00",
            "2026-10-15 20:00:00",
            "2026-10-15 21:00:00",
            "2026-10-15 21:05:00",
            "2026-10-16 00:00:00",
        ) == ["2026-10-16 00:00:00", "2026-10-16 01:00:00"]

    def test_one_bar_per_instant_survives_the_repeated_autumn_hour(self):
        window = intraday_window("2026-11-01", "2026-11-01", "UTC", labels=NEW_YORK)
        rows = [
            {"date": "2026-11-01 01:30:00", "close": 2},
            {"date": "2026-11-01 01:30:00", "close": 1},
        ]
        assert window.rows(rows) == [{"date": "2026-11-01 05:30:00", "close": 1}]

    @pytest.mark.parametrize(
        "lo, hi, detail",
        [
            (
                "not-a-date",
                "2026-10-16",
                "from_date must be YYYY-MM-DD or YYYY-MM-DD HH:MM, not 'not-a-date'",
            ),
            (
                "2026-10-16 10:00",
                "16/10/2026",
                "to_date must be YYYY-MM-DD or YYYY-MM-DD HH:MM, not '16/10/2026'",
            ),
            ("2026-10-16 11:00", "2026-10-16 10:00", "start is after end"),
        ],
    )
    def test_refused(self, lo, hi, detail):
        assert _refusal(intraday_window, lo, hi, "Asia/Hong_Kong") == detail


class TestWholeDailyDays:
    def test_absent_bounds_get_the_fmp_clients_window(self):
        # FMP's 500 days back from today (UTC), to today where the date begins
        # first, so no venue's session is cut.
        utc_today = datetime.now(timezone.utc).date()
        today = datetime.now(timezone(timedelta(hours=14))).date()
        assert whole_daily_days(None, "") == (
            (utc_today - timedelta(days=500)).isoformat(),
            today.isoformat(),
        )
        assert whole_daily_days("2026-01-02", None) == ("2026-01-02", today.isoformat())
        assert whole_daily_days(None, "2020-01-31") == ("2018-09-18", "2020-01-31")

    def test_refusals_quote_the_names_given(self):
        names = ("start_date", "end_date")
        start, _ = _future_start()
        assert _refusal(whole_daily_days, "x", None, names) == "start_date must be YYYY-MM-DD, not 'x'"
        assert _refusal(whole_daily_days, None, "x", names) == "end_date must be YYYY-MM-DD, not 'x'"
        assert _refusal(whole_daily_days, start, None, names).startswith("start_date is in the future")


def test_a_lone_end_too_early_for_the_lookback_is_refused_not_raised():
    for read in (daily_days, whole_daily_days):
        with pytest.raises(DateBoundError, match="date is out of range"):
            read(None, "0001-01-01")
