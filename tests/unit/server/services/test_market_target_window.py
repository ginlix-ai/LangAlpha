"""A market-watch target lasts through the first full regular session of its
venue that opens after it is set; the venue calendar decides, with a 24h
fallback when it cannot."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from src.server.models.automation import MarketType
from src.server.services.market_targets.window import (
    FALLBACK_LIFETIME,
    session_expiry,
    target_window,
)

NY = ZoneInfo("America/New_York")
HK = ZoneInfo("Asia/Hong_Kong")


def _ny(*args) -> datetime:
    return datetime(*args, tzinfo=NY)


@pytest.mark.parametrize(
    "now, expected",
    [
        # Weekday intraday: today's session is under way, so tomorrow's is the full one.
        (_ny(2026, 10, 6, 10, 0), _ny(2026, 10, 7, 16, 0)),
        # After the close: the next session.
        (_ny(2026, 10, 6, 17, 0), _ny(2026, 10, 7, 16, 0)),
        # Pre-market: today's session closes in 8h, under the floor, so tomorrow's.
        (_ny(2026, 10, 6, 8, 0), _ny(2026, 10, 7, 16, 0)),
        # Before dawn: today's close is 12h out, which meets the floor.
        (_ny(2026, 10, 6, 4, 0), _ny(2026, 10, 6, 16, 0)),
        # Friday afternoon carries over the weekend instead of lapsing on Saturday.
        (_ny(2026, 10, 9, 14, 0), _ny(2026, 10, 12, 16, 0)),
        # Thanksgiving is skipped and the half day's early close is the expiry.
        (_ny(2026, 11, 25, 11, 0), _ny(2026, 11, 27, 13, 0)),
    ],
)
def test_us_stock_window(now, expected):
    expires_at, warn_at = target_window("NVDA", MarketType.STOCK, now, warning_minutes=60)
    assert expires_at == expected
    assert expires_at.tzinfo is not None
    assert warn_at == expected - timedelta(minutes=60)


def test_venue_calendar_is_the_listings_own():
    """Hong Kong's National Day closes HKEX while New York trades."""
    now = datetime(2026, 9, 30, 10, 0, tzinfo=HK)
    expires_at, _ = target_window("0700.HK", MarketType.STOCK, now, warning_minutes=60)
    assert expires_at == datetime(2026, 10, 2, 16, 0, tzinfo=HK)


def test_index_reads_its_index_calendar():
    """HSI on an index target is the Hang Seng, on Hong Kong's calendar."""
    now = datetime(2026, 9, 30, 10, 0, tzinfo=HK)
    expires_at, _ = target_window("HSI", MarketType.INDEX, now, warning_minutes=60)
    assert expires_at == datetime(2026, 10, 2, 16, 0, tzinfo=HK)


def test_unreadable_symbol_falls_back_to_a_day():
    now = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)
    assert session_expiry("!!!", MarketType.STOCK, now) is None
    expires_at, warn_at = target_window("!!!", MarketType.STOCK, now, warning_minutes=60)
    assert expires_at == now + FALLBACK_LIFETIME
    assert warn_at == expires_at - timedelta(minutes=60)


def test_calendar_failure_falls_back_to_a_day():
    now = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)
    with patch(
        "src.server.services.market_targets.window.get_calendar",
        side_effect=RuntimeError("no calendar"),
    ):
        expires_at, _ = target_window("NVDA", "stock", now, warning_minutes=60)
    assert expires_at == now + FALLBACK_LIFETIME


def test_warning_never_before_now():
    now = _ny(2026, 10, 6, 8, 0)
    expires_at, warn_at = target_window("NVDA", MarketType.STOCK, now, warning_minutes=48 * 60)
    assert expires_at == _ny(2026, 10, 7, 16, 0)
    assert warn_at == now


def test_naive_now_reads_as_utc():
    naive = datetime(2026, 10, 6, 14, 0)  # 10:00 New York
    expires_at, _ = target_window("NVDA", MarketType.STOCK, naive, warning_minutes=60)
    assert expires_at == _ny(2026, 10, 7, 16, 0)


def test_warning_defaults_to_config():
    now = _ny(2026, 10, 6, 10, 0)
    expires_at, warn_at = target_window("NVDA", MarketType.STOCK, now)
    assert expires_at - warn_at == timedelta(minutes=60)
