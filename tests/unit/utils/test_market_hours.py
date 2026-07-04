"""Direct tests for market_hours trading-date helpers.

``expected_latest_daily_date`` differs from ``current_trading_date`` only
during pre-market (04:00–09:30 ET), when today's daily bar doesn't exist yet.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.utils.market_hours import current_trading_date, expected_latest_daily_date

ET = ZoneInfo("America/New_York")


class TestCurrentTradingDate:
    def test_midsession_is_today(self):
        wed = datetime(2026, 5, 27, 11, 0, tzinfo=ET)
        assert current_trading_date(wed) == "2026-05-27"

    def test_before_premarket_open_is_previous_day(self):
        wed_early = datetime(2026, 5, 27, 3, 0, tzinfo=ET)
        assert current_trading_date(wed_early) == "2026-05-26"

    def test_weekend_walks_back_to_friday(self):
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        assert current_trading_date(saturday) == "2026-04-17"

    def test_holiday_walks_back(self):
        # Memorial Day 2026-05-25 → most recent trading day is Fri 05-22.
        holiday = datetime(2026, 5, 25, 11, 0, tzinfo=ET)
        assert current_trading_date(holiday) == "2026-05-22"


class TestExpectedLatestDailyDate:
    def test_midsession_is_today(self):
        wed = datetime(2026, 5, 27, 11, 0, tzinfo=ET)
        assert expected_latest_daily_date(wed) == "2026-05-27"

    def test_at_open_boundary_is_today(self):
        wed_open = datetime(2026, 5, 27, 9, 30, tzinfo=ET)
        assert expected_latest_daily_date(wed_open) == "2026-05-27"

    def test_premarket_is_previous_trading_day(self):
        # 08:00 ET: pre-market is active (current_trading_date says today)
        # but today's daily bar can't exist yet.
        wed_premkt = datetime(2026, 5, 27, 8, 0, tzinfo=ET)
        assert expected_latest_daily_date(wed_premkt) == "2026-05-26"
        assert current_trading_date(wed_premkt) == "2026-05-27"

    def test_premarket_monday_walks_back_to_friday(self):
        mon_premkt = datetime(2026, 5, 18, 8, 0, tzinfo=ET)
        assert expected_latest_daily_date(mon_premkt) == "2026-05-15"

    def test_weekend_is_friday(self):
        saturday = datetime(2026, 4, 18, 10, 0, tzinfo=ET)
        assert expected_latest_daily_date(saturday) == "2026-04-17"

    def test_holiday_walks_back(self):
        holiday = datetime(2026, 5, 25, 11, 0, tzinfo=ET)
        assert expected_latest_daily_date(holiday) == "2026-05-22"
