"""Direct tests for market_hours trading-date helpers.

``expected_latest_daily_date`` differs from ``current_trading_date`` only
during pre-market (04:00–09:30 ET), when today's daily bar doesn't exist yet.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.utils.market_hours import (
    current_trading_date,
    expected_latest_daily_date,
    next_phase_change_ms,
)

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


class TestNextPhaseChange:
    @staticmethod
    def _ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    def test_walks_the_session_boundaries(self):
        # (now, next boundary) across every phase of one trading day.
        cases = [
            (datetime(2026, 7, 2, 3, 0, tzinfo=ET), datetime(2026, 7, 2, 4, 0, tzinfo=ET)),
            (datetime(2026, 7, 2, 8, 0, tzinfo=ET), datetime(2026, 7, 2, 9, 30, tzinfo=ET)),
            (datetime(2026, 7, 2, 12, 0, tzinfo=ET), datetime(2026, 7, 2, 16, 0, tzinfo=ET)),
            (datetime(2026, 7, 2, 17, 0, tzinfo=ET), datetime(2026, 7, 2, 20, 0, tzinfo=ET)),
        ]
        for now, expected in cases:
            assert next_phase_change_ms(now) == self._ms(expected), now.isoformat()

    def test_boundary_instant_advances_to_the_next_edge(self):
        # Exactly at 16:00 the phase is already post — next change is 20:00.
        at_close = datetime(2026, 7, 2, 16, 0, tzinfo=ET)
        assert next_phase_change_ms(at_close) == self._ms(datetime(2026, 7, 2, 20, 0, tzinfo=ET))

    def test_skips_holiday_and_weekend(self):
        # Post-close Thu 2026-07-02; Fri is the observed July 4th holiday →
        # next boundary is Monday's 04:00 pre-open.
        after_hours = datetime(2026, 7, 2, 21, 0, tzinfo=ET)
        assert next_phase_change_ms(after_hours) == self._ms(datetime(2026, 7, 6, 4, 0, tzinfo=ET))
