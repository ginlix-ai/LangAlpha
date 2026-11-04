"""``bar_in_progress``: a bar is forming until its width or its last session ends."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from market_protocol import AssetClass, Tier, to_canonical
from market_protocol.symbology import FUTURES_MIC

import src.data_client.normalize as normalize
from src.data_client.normalize import bar_in_progress, series_labels

NY = ZoneInfo("America/New_York")
CN = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")
AAPL = to_canonical("AAPL")
MOUTAI = to_canonical("600519.SH")
BTC = to_canonical("BTCUSD", asset_class=AssetClass.CRYPTO)
BRK = to_canonical("BRK.B")


def at(tz: ZoneInfo, *parts: int) -> datetime:
    return datetime(*parts, tzinfo=tz)


@pytest.mark.parametrize(
    "label, ref, interval, now, forming",
    [
        # A US daily aggregate takes after-hours prints until 20:00 ET.
        ("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 8, 0), True),
        ("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 16, 30), True),
        ("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 20, 1), False),
        ("2026-11-02", AAPL, "1day", at(NY, 2026, 11, 3, 11, 0), False),
        ("2026-11-03 10:00:00", AAPL, "5min", at(NY, 2026, 11, 3, 10, 4), True),
        ("2026-11-03 10:00:00", AAPL, "5min", at(NY, 2026, 11, 3, 10, 5), False),
        # Shanghai has no after-hours: the session ends at 15:00, and a bar
        # whose width runs past the close ends with it.
        ("2026-11-04", MOUTAI, "1day", at(CN, 2026, 11, 4, 10, 0), True),
        ("2026-11-04", MOUTAI, "1day", at(CN, 2026, 11, 4, 15, 1), False),
        ("2026-11-04 14:30:00", MOUTAI, "1hour", at(CN, 2026, 11, 4, 14, 50), True),
        ("2026-11-04 14:30:00", MOUTAI, "1hour", at(CN, 2026, 11, 4, 15, 10), False),
        # FMP's technicals label a daily bar with a midnight time.
        ("2026-11-04 00:00:00", MOUTAI, "1day", at(CN, 2026, 11, 4, 10, 0), True),
        # A week forms until its last session ends, a month likewise.
        ("2026-11-02", AAPL, "1week", at(NY, 2026, 11, 5, 21, 0), True),
        ("2026-11-02", AAPL, "1week", at(NY, 2026, 11, 6, 21, 0), False),
        ("2026-11-01", AAPL, "1month", at(NY, 2026, 11, 3, 12, 0), True),
        # A 24x7 day ends at the next UTC midnight.
        ("2026-11-03", BTC, "1day", at(UTC, 2026, 11, 3, 23, 0), True),
        ("2026-11-03", BTC, "1day", at(UTC, 2026, 11, 4, 0, 1), False),
        # A US class share sits on no venue the protocol knows but keeps XNYS hours.
        ("2026-11-03", BRK, "1day", at(NY, 2026, 11, 3, 19, 0), True),
        ("2026-11-03", BRK, "1day", at(NY, 2026, 11, 3, 20, 1), False),
    ],
)
def test_forming_until_its_session_ends(label, ref, interval, now, forming):
    assert bar_in_progress(label, ref, interval, now) is forming


HK = to_canonical("0700.HK")
HKT = ZoneInfo("Asia/Hong_Kong")


@pytest.mark.parametrize(
    "label, ref, interval, now, tier, forming",
    [
        # A 15-minute delayed series reaches the 16:00 close at 16:15.
        ("2026-11-04", HK, "1day", at(HKT, 2026, 11, 4, 16, 10), Tier.DELAYED_15M, True),
        ("2026-11-04", HK, "1day", at(HKT, 2026, 11, 4, 16, 16), Tier.DELAYED_15M, False),
        ("2026-11-04", HK, "1day", at(HKT, 2026, 11, 4, 16, 10), Tier.REALTIME, False),
        # yfinance's native widths and periods.
        ("2026-11-03 14:30:00", AAPL, "90m", at(NY, 2026, 11, 3, 15, 30), None, True),
        ("2026-11-03 14:30:00", AAPL, "90m", at(NY, 2026, 11, 3, 16, 1), None, False),
        ("2026-10-01", AAPL, "3mo", at(NY, 2026, 11, 3, 12, 0), None, True),
        ("2026-07-01", AAPL, "3mo", at(NY, 2026, 11, 3, 12, 0), None, False),
        ("2026-10-29", AAPL, "5d", at(NY, 2026, 11, 3, 12, 0), None, True),
    ],
)
def test_delay_and_native_intervals(label, ref, interval, now, tier, forming):
    assert bar_in_progress(label, ref, interval, now, tier=tier) is forming


SPX = to_canonical("^GSPC", asset_class=AssetClass.INDEX)


@pytest.mark.parametrize(
    "label, ref, interval, now, tier, regular_only, forming",
    [
        # An index and a regular-session series settle at the 16:00 close.
        ("2026-11-03", SPX, "1day", at(NY, 2026, 11, 3, 15, 59), None, False, True),
        ("2026-11-03", SPX, "1day", at(NY, 2026, 11, 3, 16, 1), None, False, False),
        ("2026-11-03", SPX, "1day", at(NY, 2026, 11, 3, 16, 10), Tier.DELAYED_15M, False, True),
        ("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 16, 1), None, True, False),
        ("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 16, 1), None, False, True),
    ],
)
def test_index_and_regular_session_series_settle_at_the_close(
    label, ref, interval, now, tier, regular_only, forming
):
    assert bar_in_progress(label, ref, interval, now, tier=tier, regular_only=regular_only) is forming


@pytest.mark.parametrize("label, ref", [(None, AAPL), ("", AAPL), ("2026-11-03", None), ("n/a", AAPL)])
def test_nothing_to_judge_is_settled(label, ref):
    assert bar_in_progress(label, ref, "1day", at(NY, 2026, 11, 3, 11, 0)) is False


# A futures contract labelled in Chicago time, and an FMP commodity on Globex
# hours labelled in New York time.
GLOBEX = [
    to_canonical("GC=F"),
    to_canonical("GCUSD").model_copy(update={"mic": FUTURES_MIC, "calendar_id": FUTURES_MIC}),
]


def label_at(ref, moment: datetime) -> str:
    return moment.astimezone(ZoneInfo(ref.tz)).strftime("%Y-%m-%d %H:%M:%S")


@pytest.mark.parametrize("calendar", [True, False])
@pytest.mark.parametrize("ref", GLOBEX)
def test_a_globex_day_ends_at_the_halt_and_its_evening_opens_the_next(monkeypatch, calendar, ref):
    # CME Globex halts at 16:00 Chicago (17:00 New York), with or without
    # exchange_calendars, and reopens at 17:00 Chicago on the next day's session.
    if not calendar:
        def missing(_calendar_id):
            raise ModuleNotFoundError("exchange_calendars")

        monkeypatch.setattr(normalize, "get_calendar", missing)
    assert bar_in_progress("2026-11-03", ref, "1day", at(NY, 2026, 11, 3, 16, 59)) is True
    assert bar_in_progress("2026-11-03", ref, "1day", at(NY, 2026, 11, 3, 17, 1)) is False
    afternoon = label_at(ref, at(NY, 2026, 11, 3, 16, 30))
    assert bar_in_progress(afternoon, ref, "1hour", at(NY, 2026, 11, 3, 17, 10)) is False
    evening = label_at(ref, at(NY, 2026, 11, 3, 19, 0))
    assert bar_in_progress(evening, ref, "1hour", at(NY, 2026, 11, 3, 19, 20)) is True
    assert bar_in_progress(evening, ref, "1hour", at(NY, 2026, 11, 3, 20, 1)) is False


def test_without_a_calendar_the_fixed_close_ends_the_session(monkeypatch):
    # A runtime without exchange_calendars takes each venue's fixed weekday close.
    def missing(_calendar_id):
        raise ModuleNotFoundError("exchange_calendars")

    monkeypatch.setattr(normalize, "get_calendar", missing)
    assert bar_in_progress("2026-11-04", MOUTAI, "1day", at(CN, 2026, 11, 4, 14, 0)) is True
    assert bar_in_progress("2026-11-04", MOUTAI, "1day", at(CN, 2026, 11, 4, 15, 1)) is False
    assert bar_in_progress("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 19, 0)) is True
    assert bar_in_progress("2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 20, 1)) is False
    assert bar_in_progress(
        "2026-11-03", AAPL, "1day", at(NY, 2026, 11, 3, 16, 1), regular_only=True
    ) is False


def test_series_labels_name_the_treatment_and_the_forming_bar(monkeypatch):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return at(UTC, 2026, 11, 3, 23, 0)

    monkeypatch.setattr(normalize, "datetime", _Frozen)
    forming = series_labels("fmp", BTC, "1day", "2026-11-03")
    assert forming == {"price_treatment": "raw", "in_progress": "2026-11-03"}
    assert series_labels("tushare", MOUTAI, "1day", "2025-11-04") == {
        "price_treatment": "dividend_adjusted"
    }
    assert series_labels("fmp", MOUTAI, "1day", None) == {"price_treatment": "raw"}
    assert "volume" in series_labels("fmp", MOUTAI, "1hour", None)["volume_note"]
