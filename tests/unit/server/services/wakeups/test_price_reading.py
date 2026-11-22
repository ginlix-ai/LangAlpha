"""What a listed price wakeup reads off the market: the level the monitor
fires it at, and a listing a slow vendor cannot hold."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.server.services.wakeups import listing, price_reading
from tests.unit.server.services.wakeups.rows import base_row

NY = ZoneInfo("America/New_York")
CLOSED_AT = datetime(2026, 11, 5, 16, 0, tzinfo=NY)
# The delayed closing value, still inside the grace after the close.
NOW = CLOSED_AT + timedelta(minutes=10)
SPX = {"price": 5900.0, "regular_close": 5900.0, "previous_close": 6000.0,
       "as_of": int(CLOSED_AT.timestamp() * 1000)}


def _price(set_at: datetime, kind: str = "pct_up", value: float = 1.0) -> dict:
    return base_row(kind="price", symbol="SPX", market="index", price_kind=kind, price_value=value,
                    pct_from="prev_close" if kind.startswith("pct") else None, created_at=set_at)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "set_at, level",
    [
        # Measured on the closing value, from the ended session's previous close.
        (CLOSED_AT - timedelta(minutes=30), 6060.0),
        # Set since it printed, so never measured on it: from the session's own close.
        (CLOSED_AT + timedelta(minutes=5), 5959.0),
    ],
    ids=["set-before-the-closing-value", "set-after-it"],
)
async def test_a_wakeup_set_after_a_delayed_closing_value_lists_the_level_the_monitor_fires_at(
    monkeypatch, set_at, level
):
    async def snap(listings, user_id, **_):
        return {lst: SPX for lst in listings}

    monkeypatch.setattr(price_reading, "snapshots", snap)
    (reading,) = (await price_reading.price_readings([_price(set_at)], "u_test", NOW)).values()
    assert reading.last == 5900.0
    assert reading.level == pytest.approx(level)


@pytest.mark.asyncio
async def test_a_slow_vendor_lists_the_wakeups_without_prices(monkeypatch):
    async def provider():
        return object()

    async def stalls(*_, **__):
        await asyncio.sleep(60)

    monkeypatch.setattr(price_reading, "get_market_data_provider", provider)
    monkeypatch.setattr(price_reading, "listing_snapshots", stalls)
    monkeypatch.setattr(listing, "PRICE_READ_TIMEOUT_S", 0.05)
    rows = [{**_price(NOW), "is_open": True}]

    async def listed(*_, **__):
        return rows

    async def no_legacy(_):
        return []

    monkeypatch.setattr(listing.wakeups_read, "list_thread_wakeups", listed)
    monkeypatch.setattr(listing, "_legacy_targets", no_legacy)
    started = time.monotonic()
    (listed_one,) = await listing.thread_wakeups("thread", "u_test")
    assert time.monotonic() - started < 5
    assert listed_one.cancellable
    assert listed_one.price.last is None and listed_one.price.level is None
