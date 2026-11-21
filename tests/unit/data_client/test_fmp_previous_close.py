"""An FMP index quote's previous close, settled against FMP's daily history.

FMP's batch quote can name the close of two sessions back as ``previousClose``
(``change`` and ``changePercentage`` with it), per index and per day. The
numbers below are FMP's own for the 2026-11-20 close.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from src.data_client.fmp.data_source import FMPDataSource, settled_previous_close

_MOD = "src.data_client.fmp.data_source"


def _stamp(local: str, tz: str) -> int:
    return int(datetime.strptime(local, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(tz)).timestamp())


def _quote(symbol: str, price: float, prev: Any, stamp: int) -> dict[str, Any]:
    change = price - prev if isinstance(prev, (int, float)) and prev else None
    return {
        "symbol": symbol, "name": symbol, "price": price, "previousClose": prev,
        "change": change, "changePercentage": None if change is None else change / prev * 100,
        "open": price, "dayHigh": price, "dayLow": price, "volume": 0, "timestamp": stamp,
    }


class _Client:
    def __init__(self, quotes: list[dict], history: dict[str, dict[str, float]], fail: bool = False):
        self.quotes, self.history, self.fail = quotes, history, fail
        self.eod_calls: list[tuple[str, str, str]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get_batch_quotes(self, symbols):
        return [q for q in self.quotes if q["symbol"] in symbols]

    async def get_eod_closes(self, symbol, from_date, to_date):
        self.eod_calls.append((symbol, from_date, to_date))
        if self.fail:
            raise RuntimeError("history down")
        # Newest first, as FMP answers; the window is not trusted to exclude
        # the session's own (in-progress) bar.
        return [{"symbol": symbol, "date": d, "price": c} for d, c in sorted(self.history[symbol].items(), reverse=True)]


class _Cache:
    def __init__(self):
        self.data: dict[str, Any] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, ttl=None):
        self.data[key], self.ttls[key] = value, ttl
        return True


async def _snapshots(client: _Client, symbols: list[str], cache: _Cache | None = None, asset_type="indices"):
    with patch(f"{_MOD}.FMPClient", return_value=client), \
         patch(f"{_MOD}.get_cache_client", return_value=cache or _Cache()):
        rows = await FMPDataSource().get_snapshots(symbols, asset_type=asset_type)
    return {r["symbol"]: r for r in rows}


TWII_HISTORY = {
    "2026-11-16": 41040.08, "2026-11-17": 40092.11, "2026-11-18": 40194.91,
    "2026-11-19": 38426.89, "2026-11-20": 36033.29,
}


@pytest.mark.asyncio
async def test_a_previous_close_two_sessions_back_is_replaced_by_the_last_session():
    client = _Client(
        [_quote("^TWII", 36033.29, 40194.9, _stamp("2026-11-20 13:33", "Asia/Taipei"))],
        {"^TWII": TWII_HISTORY},
    )
    snap = (await _snapshots(client, ["TWII"]))["TWII"]
    assert snap["previous_close"] == 38426.89
    assert snap["change"] == pytest.approx(-2393.6)
    assert snap["change_percent"] == pytest.approx(-6.229, abs=1e-3)


@pytest.mark.asyncio
async def test_a_previous_close_naming_the_last_session_stands():
    history = {"2026-11-18": 59024.28, "2026-11-19": 59444.21, "2026-11-20": 56436.65}
    quote = _quote("^N225", 56436.65, 59444.21, _stamp("2026-11-20 15:45", "Asia/Tokyo"))
    snap = (await _snapshots(_Client([quote], {"^N225": history}), ["N225"]))["N225"]
    assert (snap["previous_close"], snap["change_percent"]) == (59444.21, quote["changePercentage"])


@pytest.mark.asyncio
async def test_a_previous_close_matching_no_daily_bar_is_the_official_one():
    """FMP's DAX bar is a late-evening level; the quote's field is the Xetra close."""
    history = {"2026-11-17": 26064.0, "2026-11-18": 25743.5, "2026-11-19": 25259.5}
    quote = _quote("^GDAXI", 24802.78, 25463.81, _stamp("2026-11-20 18:00", "Europe/Berlin"))
    snap = (await _snapshots(_Client([quote], {"^GDAXI": history}), ["GDAXI"]))["GDAXI"]
    assert snap["previous_close"] == 25463.81
    assert snap["change_percent"] == pytest.approx(-2.596, abs=1e-3)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", [None, 0, -1.0, float("nan"), "n/a"])
async def test_a_missing_or_implausible_previous_close_falls_back_to_history(field):
    client = _Client(
        [_quote("^TWII", 36033.29, field, _stamp("2026-11-20 13:33", "Asia/Taipei"))],
        {"^TWII": TWII_HISTORY},
    )
    snap = (await _snapshots(client, ["TWII"]))["TWII"]
    assert snap["previous_close"] == 38426.89


@pytest.mark.asyncio
async def test_the_session_is_the_exchange_local_date_not_the_utc_one():
    """Sydney's Friday session opens on Thursday in UTC. Dated by UTC, the stale
    field would match the newest close before Thursday and stand."""
    history = {"2026-11-17": 8883.2, "2026-11-18": 8821.8, "2026-11-19": 8730.4}
    stamp = int(datetime(2026, 11, 19, 23, 30, tzinfo=ZoneInfo("UTC")).timestamp())
    client = _Client([_quote("^AXJO", 8650.0, 8821.8, stamp)], {"^AXJO": history})
    snap = (await _snapshots(client, ["AXJO"]))["AXJO"]
    assert snap["previous_close"] == 8730.4
    assert client.eod_calls == [("^AXJO", "2026-10-30", "2026-11-19")]


@pytest.mark.asyncio
async def test_an_in_progress_bar_for_the_session_is_not_a_previous_close():
    """Mid-session the daily feed may already carry a bar for today."""
    history = {**TWII_HISTORY, "2026-11-20": 36500.0}
    client = _Client(
        [_quote("^TWII", 36500.0, 38426.89, _stamp("2026-11-20 10:15", "Asia/Taipei"))],
        {"^TWII": history},
    )
    snap = (await _snapshots(client, ["TWII"]))["TWII"]
    assert snap["previous_close"] == 38426.89


@pytest.mark.asyncio
async def test_a_stamp_after_the_session_belongs_to_the_session_it_closed():
    """A ``timestamp`` that is the last update, not the last trade (a weekend
    stamp), still dates the quote to Friday: Friday's own bar is never the
    previous close, so a correct field is not zeroed."""
    stamp = _stamp("2026-11-21 10:00", "Asia/Taipei")
    kept = _Client([_quote("^TWII", 36033.29, 38426.89, stamp)], {"^TWII": TWII_HISTORY})
    assert (await _snapshots(kept, ["TWII"]))["TWII"]["previous_close"] == 38426.89
    stale = _Client([_quote("^TWII", 36033.29, 40194.9, stamp)], {"^TWII": TWII_HISTORY})
    assert (await _snapshots(stale, ["TWII"]))["TWII"]["previous_close"] == 38426.89


@pytest.mark.asyncio
@pytest.mark.parametrize("field", [38426.89, 40194.9, None])
async def test_before_the_last_session_is_published_the_field_stands(field):
    """With no bar for the session before the quote's, an older close is not a
    better previous close than whatever FMP served."""
    history = {k: v for k, v in TWII_HISTORY.items() if k < "2026-11-19"}
    client = _Client(
        [_quote("^TWII", 36033.29, field, _stamp("2026-11-20 13:33", "Asia/Taipei"))],
        {"^TWII": history},
    )
    assert (await _snapshots(client, ["TWII"]))["TWII"]["previous_close"] == field


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "settled"), [(None, 38426.89), (40194.9, 38426.89), (38426.89, 38426.89)])
async def test_a_flat_index_keeps_the_last_session_as_its_base(field, settled):
    client = _Client(
        [_quote("^TWII", 38426.89, field, _stamp("2026-11-20 13:33", "Asia/Taipei"))],
        {"^TWII": TWII_HISTORY},
    )
    snap = (await _snapshots(client, ["TWII"]))["TWII"]
    assert (snap["previous_close"], snap["change"]) == (settled, 0.0)


@pytest.mark.asyncio
async def test_daily_closes_are_cached_per_session():
    cache = _Cache()
    quote = _quote("^TWII", 36033.29, 40194.9, _stamp("2026-11-20 13:33", "Asia/Taipei"))
    first = _Client([quote], {"^TWII": TWII_HISTORY})
    await _snapshots(first, ["TWII"], cache)
    assert cache.ttls == {"fmp:prior_closes:^TWII:2026-11-20": 600}
    second = _Client([quote], {"^TWII": TWII_HISTORY})
    snap = (await _snapshots(second, ["TWII"], cache))["TWII"]
    assert (snap["previous_close"], second.eod_calls) == (38426.89, [])


@pytest.mark.asyncio
async def test_a_suffixed_index_listing_is_checked_on_its_venue():
    history = {"2026-11-18": 3587.83, "2026-11-19": 3577.62, "2026-11-20": 3533.793}
    quote = _quote("000001.SH", 3533.793, 3587.83, _stamp("2026-11-20 15:00", "Asia/Shanghai"))
    snap = (await _snapshots(_Client([quote], {"000001.SH": history}), ["000001.SH"]))["000001.SH"]
    assert snap["previous_close"] == 3577.62


@pytest.mark.asyncio
async def test_an_index_whose_venue_is_unknown_is_left_as_served():
    """The symbology files ^BVSP under New York; its session cannot be dated."""
    quote = _quote("^BVSP", 166376.43, 167537.95, _stamp("2026-11-19 22:10", "America/Sao_Paulo"))
    client = _Client([quote], {})
    snap = (await _snapshots(client, ["BVSP"]))["BVSP"]
    assert (snap["previous_close"], client.eod_calls) == (167537.95, [])


@pytest.mark.asyncio
async def test_unreadable_history_leaves_the_quote_as_served():
    client = _Client(
        [_quote("^TWII", 36033.29, 40194.9, _stamp("2026-11-20 13:33", "Asia/Taipei"))],
        {"^TWII": TWII_HISTORY}, fail=True,
    )
    assert (await _snapshots(client, ["TWII"]))["TWII"]["previous_close"] == 40194.9


@pytest.mark.asyncio
async def test_stock_quotes_are_not_checked():
    client = _Client([_quote("AAPL", 200.0, 190.0, _stamp("2026-11-20 16:00", "America/New_York"))], {})
    snap = (await _snapshots(client, ["AAPL"], asset_type="stocks"))["AAPL"]
    assert (snap["previous_close"], client.eod_calls) == (190.0, [])


class TestSettledPreviousClose:
    PRIOR = date(2026, 11, 19)
    CLOSES = {
        date(2026, 11, 16): 41040.08, date(2026, 11, 17): 40092.11,
        date(2026, 11, 18): 40194.91, date(2026, 11, 19): 38426.89,
    }

    def test_feeds_rounding_one_close_differently_still_match(self):
        assert settled_previous_close(40194.9, self.PRIOR, self.CLOSES) == 38426.89
        assert settled_previous_close(38426.9, self.PRIOR, self.CLOSES) is None

    def test_only_the_two_closes_before_the_last_count_as_stale(self):
        assert settled_previous_close(40092.11, self.PRIOR, self.CLOSES) == 38426.89
        assert settled_previous_close(41040.08, self.PRIOR, self.CLOSES) is None

    def test_a_close_after_the_last_session_is_never_a_base(self):
        closes = {**self.CLOSES, date(2026, 11, 20): 36033.29}
        assert settled_previous_close(36033.29, self.PRIOR, closes) is None
        assert settled_previous_close(None, self.PRIOR, closes) == 38426.89

    def test_no_bar_for_the_last_session_keeps_the_field(self):
        assert settled_previous_close(40194.9, self.PRIOR, {}) is None
        assert settled_previous_close(None, self.PRIOR, {}) is None


@pytest.mark.asyncio
async def test_a_stamp_no_calendar_can_place_leaves_that_quote_and_the_batch_as_served():
    good = _quote("^TWII", 36033.29, 40194.9, _stamp("2026-11-20 13:33", "Asia/Taipei"))
    odd = _quote("^HSI", 22410.8, 23294.8, 1)  # 1970: before any session the calendar holds
    client = _Client([good, odd], {"^TWII": TWII_HISTORY})
    snaps = await _snapshots(client, ["TWII", "HSI"])
    assert (snaps["TWII"]["previous_close"], snaps["HSI"]["previous_close"]) == (38426.89, 23294.8)


@pytest.mark.asyncio
async def test_one_batch_keeps_a_bounded_number_of_history_reads_in_flight():
    stamp = _stamp("2026-11-20 13:33", "Asia/Taipei")
    quotes = [_quote(f"^TWII{i}", 36033.29, 40194.9, stamp) for i in range(30)]

    class _Slow(_Client):
        flying = peak = 0

        async def get_eod_closes(self, symbol, from_date, to_date):
            _Slow.flying += 1
            _Slow.peak = max(_Slow.peak, _Slow.flying)
            await asyncio.sleep(0.001)
            _Slow.flying -= 1
            return []

    with patch(f"{_MOD}.has_home_venue", return_value=True):
        await _snapshots(_Slow(quotes, {}), [q["symbol"].lstrip("^") for q in quotes])
    assert _Slow.peak == 8
