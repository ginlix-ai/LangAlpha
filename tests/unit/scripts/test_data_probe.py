"""Unit tests for the entitlement probe's pure analysis and CLI plumbing.

Everything here runs against fake sources and fixed ``now`` values — the probe's
whole point is measuring live upstreams, so the parts worth locking are the ones
that turn a provider's answer into a routing decision.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts.data_probe import cli
from scripts.data_probe.canaries import canaries_for
from scripts.data_probe.probe import (
    analyze_daily,
    analyze_intraday,
    classify_error,
    Unmeasured,
    estimate_cost_calls,
    parse_calls_per_minute,
    probe_cell,
    probe_provider,
    scrub,
    series_price_treatment,
    snapshot_as_of_ms,
    snapshot_coverage,
)
from scripts.data_probe.report import render_table
from scripts.data_probe.store import credential_fingerprint, merge_rulesets, save_ruleset
from market_protocol import Tier, classify_lag
from market_protocol.routing.ruleset import (
    Cell,
    CellKey,
    ProbedProvider,
    Ruleset,
    Surface,
    load_ruleset,
    order_providers,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _ms(iso: str, tz: ZoneInfo = SHANGHAI) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=tz).timestamp() * 1000)


# A Shanghai session: 09:30 open, 15:00 close, 11:30-13:00 lunch.
CN_OPEN = _ms("2026-09-08T09:30")
CN_CLOSE = _ms("2026-09-08T15:00")
CN_LUNCH = (_ms("2026-09-08T11:30"), _ms("2026-09-08T13:00"))
CN_BOUNDS = (CN_OPEN, CN_CLOSE, CN_LUNCH[0], CN_LUNCH[1])


def cn_bounds(_iso_date: str):
    return CN_BOUNDS


def bars(*local_times: str) -> list[dict]:
    return [{"time": _ms(f"2026-09-08T{t}"), "close": 1.0} for t in local_times]


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


def test_series_short_of_the_close_is_incomplete_and_says_by_how_much():
    series = bars("14:54", "14:55", "14:56")
    result = analyze_intraday(
        series, interval="ohlcv-1m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.session_complete is False
    assert "last bar 14:56, 3 bars short of close" in result.notes


def test_series_reaching_the_closing_auction_bar_is_complete():
    result = analyze_intraday(
        bars("14:58", "14:59", "15:00"), interval="ohlcv-1m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.session_complete is True
    assert result.anchors_ok is True


def test_last_continuous_bar_counts_as_complete_without_the_auction_print():
    result = analyze_intraday(
        bars("14:58", "14:59"), interval="ohlcv-1m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.session_complete is True


def test_completeness_is_unanswerable_while_the_venue_is_open():
    result = analyze_intraday(
        bars("10:00", "10:01"), interval="ohlcv-1m", phase="open", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=_ms("2026-09-08T10:03"),
        bounds_lookup=cn_bounds,
    )
    assert result.session_complete is None
    assert result.lag_s == 120


def test_lag_is_not_measured_once_the_venue_has_closed():
    result = analyze_intraday(
        bars("14:59"), interval="ohlcv-1m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=CN_CLOSE, bounds_lookup=cn_bounds,
    )
    assert result.lag_s is None


# ---------------------------------------------------------------------------
# Anchors
# ---------------------------------------------------------------------------


def test_a_bar_inside_the_lunch_break_fails_the_anchor_check():
    result = analyze_intraday(
        bars("11:25", "11:30", "12:00", "13:00"), interval="ohlcv-5m", phase="closed",
        tz=SHANGHAI, close_ms=CN_CLOSE, expected_latest_ms=None,
        bounds_lookup=cn_bounds,
    )
    assert result.anchors_ok is False
    assert any("12:00 inside the lunch break" in n for n in result.notes)


def test_an_index_printing_through_lunch_is_a_note_not_an_anchor_failure():
    # Index levels are published continuously through the SSE/HKEX break; only
    # a tradable series can have a fabricated bar there.
    result = analyze_intraday(
        bars("11:25", "11:30", "12:00", "13:00"), interval="ohlcv-5m", phase="closed",
        tz=SHANGHAI, close_ms=CN_CLOSE, expected_latest_ms=None,
        bounds_lookup=cn_bounds, asset_class="index",
    )
    assert result.anchors_ok is True
    assert any("print through the lunch break (index)" in n for n in result.notes)


def test_a_fund_printing_through_lunch_still_fails_anchors():
    result = analyze_intraday(
        bars("11:25", "12:00"), interval="ohlcv-5m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
        asset_class="fund",
    )
    assert result.anchors_ok is False


def test_the_bar_stamped_on_the_break_start_is_the_morning_close_not_lunch():
    result = analyze_intraday(
        bars("11:25", "11:30"), interval="ohlcv-5m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.anchors_ok is True


def test_hourly_bars_restart_their_grid_after_lunch():
    # Hong Kong's hourly bars: the morning ends at 11:30 and the afternoon
    # reopens at 13:00, 90 minutes on.
    hk_bounds = (
        _ms("2026-09-08T09:30"), _ms("2026-09-08T16:00"),
        _ms("2026-09-08T12:00"), _ms("2026-09-08T13:00"),
    )
    result = analyze_intraday(
        bars("09:30", "10:30", "11:30", "13:00", "14:00"), interval="ohlcv-1h",
        phase="closed", tz=SHANGHAI, close_ms=hk_bounds[1], expected_latest_ms=None,
        bounds_lookup=lambda _d: hk_bounds,
    )
    assert result.anchors_ok is True


def test_a_gap_off_the_grid_within_one_window_still_fails():
    result = analyze_intraday(
        bars("09:30", "10:30", "11:00"), interval="ohlcv-1h", phase="closed",
        tz=SHANGHAI, close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.anchors_ok is False


def test_a_bar_before_the_open_is_flagged_as_outside_the_session():
    result = analyze_intraday(
        bars("09:25", "09:30"), interval="ohlcv-5m", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.anchors_ok is False
    assert any("09:25 outside the session window" in n for n in result.notes)


def test_a_gap_that_is_not_a_multiple_of_the_interval_is_flagged():
    series = [
        {"time": _ms("2026-09-08T10:00")},
        {"time": _ms("2026-09-08T10:00") + 90_000},
    ]
    result = analyze_intraday(
        series, interval="ohlcv-1m", phase="closed", tz=SHANGHAI, close_ms=CN_CLOSE,
        expected_latest_ms=None, bounds_lookup=cn_bounds,
    )
    assert result.anchors_ok is False
    assert any("not a multiple of ohlcv-1m" in n for n in result.notes)


def test_a_closing_auction_bar_past_the_close_is_noted_not_failed():
    hk_close = _ms("2026-09-08T16:00", ZoneInfo("Asia/Hong_Kong"))
    hk_open = _ms("2026-09-08T09:30", ZoneInfo("Asia/Hong_Kong"))
    series = [
        {"time": hk_close - 300_000},
        {"time": hk_close + 300_000},  # HKEX closing auction session
    ]
    result = analyze_intraday(
        series, interval="ohlcv-5m", phase="closed", tz=ZoneInfo("Asia/Hong_Kong"),
        close_ms=hk_close, expected_latest_ms=None,
        bounds_lookup=lambda _d: (hk_open, hk_close, None, None),
    )
    assert result.anchors_ok is True
    assert any("closing-auction bars" in n for n in result.notes)


NEW_YORK = ZoneInfo("America/New_York")


def _us_view(extended: bool = True):
    from market_protocol.calendars import get_calendar
    from datetime import date

    cal = get_calendar("XNYS")
    d = date(2026, 9, 8)
    from market_protocol.calendars import session_bounds

    b = session_bounds("XNYS", d.isoformat())
    return b, (lambda _d: cal.extended_bounds_ms(d)) if extended else None


def _us_analyze(times: list[str]):
    b, ext = _us_view()
    series = [{"time": _ms(f"2026-09-08T{t}", NEW_YORK)} for t in times]
    return analyze_intraday(
        series, interval="ohlcv-1h", phase="closed", tz=NEW_YORK,
        close_ms=b[1], expected_latest_ms=None,
        bounds_lookup=lambda _d: b, extended_lookup=ext,
    )


def test_us_extended_hours_bars_are_a_note_not_an_anchor_failure():
    result = _us_analyze(["04:00", "10:00", "15:00", "19:00"])
    assert result.anchors_ok is True
    assert result.session_complete is True
    assert any("2 extended-hours bars" in n for n in result.notes)


def test_us_bar_outside_the_extended_window_is_still_a_problem():
    result = _us_analyze(["10:00", "21:00"])
    assert result.anchors_ok is False
    assert any("21:00 outside the session window" in n for n in result.notes)


def test_cn_series_without_extended_hours_is_unchanged():
    result = analyze_intraday(
        bars("09:00", "10:00"), interval="ohlcv-1h", phase="closed", tz=SHANGHAI,
        close_ms=CN_CLOSE, expected_latest_ms=None, bounds_lookup=cn_bounds,
        extended_lookup=lambda _d: None,
    )
    assert result.anchors_ok is False


def test_market_view_exposes_extended_window_for_us_only():
    now = datetime(2026, 9, 9, 22, 0, tzinfo=NEW_YORK)
    us = market_view_for("AAPL", now)
    assert us.extended_lookup("2026-09-08") is not None
    cn = market_view_for("600519.SS", now)
    assert cn.extended_lookup("2026-09-08") is None


def market_view_for(symbol: str, now: datetime):
    from scripts.data_probe.probe import market_view

    return market_view(symbol, is_index=False, interval=None, now=now)


def test_daily_series_short_of_the_expected_session_is_incomplete():
    series = [{"time": _ms("2026-09-04T15:00")}, {"time": _ms("2026-09-07T15:00")}]
    result = analyze_daily(series, tz=SHANGHAI, expected_latest_date="2026-09-08")
    assert result.session_complete is False
    assert "last bar 2026-09-07, expected through 2026-09-08" in result.notes


def test_daily_completeness_is_owed_through_the_last_closed_session_only():
    from scripts.data_probe.probe import market_view

    # Wednesday 2026-09-09, 10:30 Shanghai: the session is running, so a
    # publisher that posts closes after the bell is complete through Tuesday.
    open_now = datetime(2026, 9, 9, 10, 30, tzinfo=SHANGHAI)
    assert market_view("600519.SS", is_index=False, interval=None, now=open_now).expected_latest_date == "2026-09-08"
    # Shanghai posts its closes from about 15:30 (index and fund bars about two
    # hours on), so the day's bar is owed only once the publication grace has run.
    publishing = datetime(2026, 9, 9, 16, 30, tzinfo=SHANGHAI)
    assert market_view("600519.SS", is_index=False, interval=None, now=publishing).expected_latest_date == "2026-09-08"
    published = datetime(2026, 9, 9, 17, 45, tzinfo=SHANGHAI)
    assert market_view("600519.SS", is_index=False, interval=None, now=published).expected_latest_date == "2026-09-09"
    # Monday morning reaches back over the weekend to Friday.
    monday = datetime(2026, 9, 14, 10, 30, tzinfo=SHANGHAI)
    assert market_view("600519.SS", is_index=False, interval=None, now=monday).expected_latest_date == "2026-09-11"


def test_us_pre_market_owes_the_previous_session_not_the_one_before():
    from scripts.data_probe.probe import market_view

    # Wednesday 08:00 New York: the clock already names Tuesday, whose bar is owed.
    pre = datetime(2026, 9, 9, 8, 0, tzinfo=NEW_YORK)
    assert market_view("AAPL", is_index=False, interval=None, now=pre).expected_latest_date == "2026-09-08"


def test_a_closed_index_before_the_bell_is_judged_against_the_last_close():
    from scripts.data_probe.probe import market_view

    # Tuesday 08:00 New York is dated Tuesday, whose close is still ahead; the
    # session to judge is Friday's (Monday is Labor Day).
    view = market_view("^GSPC", is_index=True, interval="ohlcv-1m",
                       now=datetime(2026, 9, 8, 8, 0, tzinfo=NEW_YORK))
    assert view.phase == "closed"
    assert view.close_ms == _ms("2026-09-04T16:00", NEW_YORK)


def test_daily_anchors_ignore_bars_older_than_the_calendar_can_judge():
    # Venue calendars are built from a bounded start, so deep history reads as
    # "non-trading day"; only the recent tail is checked.
    from datetime import date, timedelta

    old = date(2001, 1, 1)
    recent = date(2026, 6, 22)
    series = [
        {"time": _ms(f"{old + timedelta(days=n):%Y-%m-%d}T15:00")} for n in range(40)
    ] + [
        {"time": _ms(f"{recent + timedelta(days=n):%Y-%m-%d}T15:00")} for n in range(60)
    ]
    result = analyze_daily(
        series, tz=SHANGHAI, expected_latest_date="2026-08-19",
        is_trading_day=lambda d: d.startswith("2026"),
    )
    assert result.anchors_ok is True


def test_daily_bar_on_a_non_trading_day_fails_anchors():
    series = [{"time": _ms("2026-09-05T15:00")}]  # a Saturday
    result = analyze_daily(
        series, tz=SHANGHAI, expected_latest_date="2026-09-04",
        is_trading_day=lambda d: d != "2026-09-05",
    )
    assert result.anchors_ok is False


# ---------------------------------------------------------------------------
# Provider errors and quotas
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message,expected",
    [
        ("抽取业务数据频率超限(1次/分钟)", 1),
        ("每分钟最多访问该接口500次", 500),
        ("频率超限(120次/小时)", 2),
        ("Limit Reach: 300 requests per minute", 300),
        ("rate limit exceeded: 60/min", 60),
        ("something else entirely", None),
    ],
)
def test_rate_limit_message_yields_calls_per_minute(message, expected):
    assert parse_calls_per_minute(message) == expected


class _Permission(Exception):
    pass


def test_permission_error_marks_the_provider_unentitled():
    assert classify_error(Exception("没有接口访问权限")) == "permission"
    assert classify_error(Exception("HTTP 403: Exclusive Endpoint")) == "permission"
    assert classify_error(Exception("抽取数据频率超限")) == "rate_limit"
    assert classify_error(Exception("connection reset by peer")) == "other"


def test_http_status_classifies_an_error_whose_text_names_no_cause():
    class FMPRequestError(Exception):
        def __init__(self, message, status_code=None):
            super().__init__(message)
            self.status_code = status_code

    for code in (401, 402, 403):
        assert classify_error(FMPRequestError(f"FMP API request failed ({code})", code)) == "permission"
    assert classify_error(FMPRequestError("FMP API request failed", 429)) == "rate_limit"
    assert classify_error(FMPRequestError("FMP API request failed (500)", 500)) == "other"

    class _Resp:
        status_code = 403

    err = Exception("request failed")
    err.response = _Resp()
    assert classify_error(err) == "permission"


def test_error_class_name_classifies_before_the_message():
    class TusharePermissionError(Exception):
        pass

    class TushareRateLimitError(Exception):
        pass

    assert classify_error(TusharePermissionError("boom")) == "permission"
    assert classify_error(TushareRateLimitError("boom")) == "rate_limit"


def test_scrub_redacts_anything_token_shaped():
    text = scrub("https://api.example.com/quote?apikey=" + "a" * 32)
    assert "a" * 32 not in text
    assert "<redacted>" in text


def test_classify_lag_buckets():
    assert classify_lag(10) is Tier.REALTIME
    assert classify_lag(600) is Tier.DELAYED_15M
    assert classify_lag(4000) is Tier.EOD


def test_cost_calls_counts_pagination_only_where_a_source_pages():
    assert estimate_cost_calls("yfinance", 50_000) == 1
    assert estimate_cost_calls("tushare", 100) == 1
    assert estimate_cost_calls("tushare", 8001) == 2


def test_snapshot_coverage_matches_carets_and_ignores_priceless_rows():
    rows = [{"symbol": "GSPC", "price": 5000.0}, {"symbol": "AAPL", "price": None}]
    assert snapshot_coverage(rows, ["^GSPC", "AAPL"]) == 1


def test_snapshot_lag_is_judged_on_the_stalest_canary():
    rows = [
        {"symbol": "600519.SH", "as_of": _ms("2026-09-08T10:00")},
        {"symbol": "000858.SZ", "as_of": _ms("2026-09-08T09:40")},
    ]
    assert snapshot_as_of_ms(rows) == _ms("2026-09-08T09:40")


def test_snapshot_coverage_reconciles_vendor_and_display_spellings():
    # Shanghai is .SS to the vendors and .SH in our spelling.
    assert snapshot_coverage([{"symbol": "600519.SH", "price": 1.0}], ["600519.SS"]) == 1
    assert snapshot_coverage([{"symbol": "600519.SS", "price": 1.0}], ["600519.SH"]) == 1


# ---------------------------------------------------------------------------
# Price treatment
# ---------------------------------------------------------------------------


class _Header:
    price_treatment = "raw"


class _Series:
    header = _Header()


def test_price_treatment_comes_from_the_series_header_when_there_is_one():
    assert series_price_treatment("tushare", _Series(), "equity") == "raw"
    assert series_price_treatment("fmp", {"header": {}}, "equity") == "split_adjusted"


def test_legacy_bars_fall_back_to_the_publisher_lineage_per_asset_class():
    # Tushare joins adj_factor for equities only; its index endpoint is raw.
    assert series_price_treatment("tushare", [], "equity") == "dividend_adjusted"
    assert series_price_treatment("tushare", [], "index") == "raw"
    assert series_price_treatment("yfinance", [], "equity") == "split_adjusted"


def test_fmp_cn_daily_is_declared_raw_by_venue():
    # FMP's Shanghai closes equal the exchange's unadjusted prints, so the
    # split-adjusted declaration holds for the US tape only.
    assert series_price_treatment("fmp", [], "equity", "600519.SS") == "raw"
    assert series_price_treatment("fmp", [], "equity", "000001.SZ") == "raw"
    assert series_price_treatment("fmp", [], "equity", "AAPL") == "split_adjusted"


def test_adjusted_daily_series_outranks_a_raw_one_that_is_cheaper():
    ordered = order_providers(Surface.DAILY, [
        ProbedProvider(name="fmp", coverage_hit=2, coverage_total=2, cost_calls=1,
                       tier="delayed_15m", price_treatment="raw"),
        ProbedProvider(name="tushare", coverage_hit=2, coverage_total=2, cost_calls=3,
                       tier="delayed_15m", price_treatment="dividend_adjusted"),
    ])
    assert [p.name for p in ordered] == ["tushare", "fmp"]


# ---------------------------------------------------------------------------
# probe_provider against fake sources
# ---------------------------------------------------------------------------


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
KEY = CellKey(market="cn", asset_class="equity", surface=Surface.INTRADAY, interval="1m")


class _FakeSource:
    def __init__(self, *, error=None, rows=None):
        self._error = error
        self._rows = rows if rows is not None else bars("14:59")
        self.calls = 0

    async def get_intraday(self, symbol, interval, **kwargs):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._rows

    async def get_daily(self, symbol, **kwargs):
        return self._rows

    async def get_snapshots(self, symbols, asset_type="stocks", **kwargs):
        if self._error is not None:
            raise self._error
        return [{"symbol": s, "price": 1.0} for s in symbols]


def test_permission_denial_sets_entitled_false_and_stops_the_basket():
    class TusharePermissionError(Exception):
        pass

    source = _FakeSource(error=TusharePermissionError("没有接口访问权限"))
    probed = asyncio.run(probe_provider(
        "tushare", source, KEY, ["600519.SS", "000858.SZ"], clock=lambda: NOW,
    ))
    assert probed.entitled is False
    assert probed.coverage_hit == 0
    assert source.calls == 1  # no point asking the next canary


def test_rate_limit_retries_once_then_records_the_stated_quota():
    class TushareRateLimitError(Exception):
        pass

    slept: list[float] = []

    async def sleeper(seconds):
        slept.append(seconds)

    source = _FakeSource(error=TushareRateLimitError("抽取业务数据频率超限(1次/分钟)"))
    probed = asyncio.run(probe_provider(
        "tushare", source, KEY, ["600519.SS"], clock=lambda: NOW,
        backoff_s=60.0, sleeper=sleeper,
    ))
    assert slept == [60.0]
    assert source.calls == 2
    assert probed.calls_per_minute == 1
    assert probed.entitled is True
    # The quota, not the empty basket, is what the file must say.
    assert probed.coverage_hit == 0
    ordered = order_providers(KEY.surface, [probed])
    assert ordered[0].excluded_reason == "quota_below_floor"


def test_a_quota_stated_on_a_refusal_survives_a_retry_that_then_worked():
    class TushareRateLimitError(Exception):
        pass

    class _ThrottledOnce(_FakeSource):
        async def get_intraday(self, symbol, interval, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TushareRateLimitError("抽取业务数据频率超限(1次/分钟)")
            return bars("14:59")

    async def sleeper(_s):
        return None

    probed = asyncio.run(probe_provider(
        "tushare", _ThrottledOnce(), KEY, ["600519.SS"], clock=lambda: NOW, sleeper=sleeper,
    ))
    assert probed.coverage_hit == 1
    assert probed.calls_per_minute == 1
    assert order_providers(KEY.surface, [probed])[0].excluded_reason == "quota_below_floor"


def test_a_throttle_that_states_no_quota_is_unmeasured_not_uncovered():
    """Only an answer proves coverage: writing no_coverage after a bare 429
    would drop a working provider from the chain until the next probe."""
    class RateLimitError(Exception):
        pass

    async def sleeper(_s):
        return None

    source = _FakeSource(error=RateLimitError("too many requests"))
    with pytest.raises(Unmeasured):
        asyncio.run(probe_provider(
            "fmp", source, KEY, ["600519.SS", "000858.SZ"], clock=lambda: NOW, sleeper=sleeper,
        ))
    assert source.calls == 2  # one retry, then the basket stops


def test_bar_probes_record_the_price_treatment_of_what_came_back():
    probed = asyncio.run(probe_provider(
        "tushare", _FakeSource(), KEY, ["600519.SS"], clock=lambda: NOW,
    ))
    assert probed.price_treatment == "dividend_adjusted"


def test_lag_is_measured_when_the_fetch_returns_not_when_the_basket_began():
    # The fetch takes until 10:05:20 (a backoff, a slow page) and its newest
    # bar is 10:02: three minutes behind, not "ahead of" a 10:00 start.
    clock = {"now": datetime(2026, 9, 8, 10, 0, tzinfo=SHANGHAI)}

    class _Slow(_FakeSource):
        async def get_intraday(self, symbol, interval, **kwargs):
            clock["now"] = datetime(2026, 9, 8, 10, 5, 20, tzinfo=SHANGHAI)
            return bars("10:01", "10:02")

    probed = asyncio.run(probe_provider(
        "fmp", _Slow(), KEY, ["600519.SS"], clock=lambda: clock["now"],
    ))
    assert probed.lag_s == 180


def test_an_unexpected_provider_error_never_aborts_the_basket():
    class _Boom(_FakeSource):
        async def get_intraday(self, symbol, interval, **kwargs):
            self.calls += 1
            if symbol == "600519.SS":
                raise RuntimeError("connection reset by peer")
            return bars("14:59")

    source = _Boom()
    probed = asyncio.run(probe_provider(
        "yfinance", source, KEY, ["600519.SS", "000858.SZ"], clock=lambda: NOW,
    ))
    assert source.calls == 2
    assert probed.coverage_hit == 1
    assert probed.coverage_total == 2


def test_a_provider_no_canary_reached_is_unmeasured_not_uncovered():
    """An outage during the run is no verdict on coverage: excluding the
    provider as no_coverage would drop it from the cell until the next run."""
    down = _FakeSource(error=TimeoutError("read timed out"))
    with pytest.raises(Unmeasured):
        asyncio.run(probe_provider("fmp", down, KEY, ["600519.SS", "000858.SZ"], clock=lambda: NOW))
    snap = CellKey(market="cn", asset_class="equity", surface=Surface.SNAPSHOT)
    with pytest.raises(Unmeasured):
        asyncio.run(probe_provider("fmp", down, snap, ["600519.SS"], clock=lambda: NOW))

    cell = asyncio.run(probe_cell(
        KEY, {"tushare": _FakeSource(), "fmp": down}, ["600519.SS"], clock=lambda: NOW,
    ))
    assert [p.name for p in cell.providers] == ["tushare"]


# ---------------------------------------------------------------------------
# Ruleset merge
# ---------------------------------------------------------------------------


def _cell(market: str, provider: str) -> Cell:
    return Cell(
        key=CellKey(market=market, asset_class="equity", surface=Surface.DAILY),
        probed_at=NOW,
        canaries=["X"],
        providers=[ProbedProvider(name=provider, coverage_hit=1, coverage_total=1)],
    )


def test_merge_keeps_cells_the_new_run_did_not_touch(tmp_path: Path):
    path = tmp_path / "data_routing.yaml"
    save_ruleset(Ruleset(generated_at=NOW, cells=[_cell("us", "fmp"), _cell("cn", "tushare")]), path)

    update = Ruleset(generated_at=NOW, cells=[_cell("cn", "yfinance")])
    merged = merge_rulesets(load_ruleset(path), update)
    save_ruleset(merged, path)

    reloaded = load_ruleset(path)
    keys = {c.key.as_str(): c.providers[0].name for c in reloaded.cells}
    assert keys == {"us/equity/daily": "fmp", "cn/equity/daily": "yfinance"}


def test_merge_keeps_the_verdicts_of_providers_the_run_did_not_measure():
    base = Ruleset(generated_at=NOW, cells=[Cell(
        key=CellKey(market="cn", asset_class="equity", surface=Surface.DAILY),
        probed_at=NOW,
        providers=[
            ProbedProvider(name="tushare", coverage_hit=1, coverage_total=1),
            ProbedProvider(name="fmp", coverage_hit=1, coverage_total=1),
        ],
    )])
    # A ``--provider fmp`` run that found FMP short of the latest session.
    update = Ruleset(generated_at=NOW, cells=[Cell(
        key=CellKey(market="cn", asset_class="equity", surface=Surface.DAILY),
        probed_at=NOW,
        providers=[ProbedProvider(name="fmp", coverage_hit=1, coverage_total=1,
                                  session_complete=False)],
    )])
    (cell,) = merge_rulesets(base, update).cells
    assert {p.name: p.excluded_reason for p in cell.providers} == {
        "tushare": None, "fmp": "incomplete_session",
    }
    assert cell.routable_names() == ["tushare"]


def test_save_writes_through_a_symlink_and_keeps_the_file_readable(tmp_path: Path):
    import os
    import stat

    target = tmp_path / "deploy" / "data_routing.yaml"
    save_ruleset(Ruleset(generated_at=NOW, cells=[_cell("us", "fmp")]), target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o644  # not mkstemp's 0600
    os.chmod(target, 0o640)
    link = tmp_path / "data_routing.yaml"
    link.symlink_to(target)

    save_ruleset(Ruleset(generated_at=NOW, cells=[_cell("cn", "tushare")]), link)
    assert link.is_symlink()
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert load_ruleset(target).cells[0].key.market == "cn"


def test_an_unreadable_ruleset_is_refused_rather_than_overwritten(tmp_path: Path):
    from scripts.data_probe.store import load_for_merge

    path = tmp_path / "data_routing.yaml"
    path.write_text("cells: [ this is not a ruleset\n")
    with pytest.raises(SystemExit, match="not a ruleset this probe can read"):
        load_for_merge(path)
    assert load_for_merge(tmp_path / "absent.yaml") is None


@pytest.mark.parametrize(
    ("damage", "reason"), [("cell", "cell 1"), ("version", "unknown ruleset version")]
)
def test_a_ruleset_a_worker_would_only_partly_read_is_refused(
    tmp_path: Path, damage: str, reason: str
):
    # A worker skips a cell it cannot validate; a merge that did the same would
    # write the file back without it.
    import yaml

    from scripts.data_probe.store import load_for_merge

    path = tmp_path / "data_routing.yaml"
    save_ruleset(Ruleset(generated_at=NOW, cells=[_cell("us", "fmp")]), path)
    raw = yaml.safe_load(path.read_text())
    if damage == "cell":
        raw["cells"].append({"key": {"market": "cn"}, "probed_at": NOW.isoformat()})
    else:
        raw["version"] += 1
    path.write_text(yaml.safe_dump(raw))

    with pytest.raises(SystemExit, match=reason):
        load_for_merge(path)


def test_saved_ruleset_never_contains_a_raw_credential(tmp_path: Path):
    from market_protocol.routing.ruleset import ProviderInfo

    secret = "s3cret" * 8
    path = tmp_path / "data_routing.yaml"
    save_ruleset(Ruleset(
        generated_at=NOW,
        providers={"fmp": ProviderInfo(fingerprint=credential_fingerprint(secret))},
    ), path)
    assert secret not in path.read_text()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_plan_for_a_closed_market_names_the_next_open():
    # A Tuesday 22:00 in Shanghai — the session closed hours ago.
    now = datetime(2026, 9, 8, 14, 0, tzinfo=timezone.utc)
    rows = cli.plan_rows(["cn"], now, timezone.utc)
    market, phase, lag_now, measures, venue_open, _local_open, _in = rows[0]
    assert (market, phase, lag_now, measures) == ("cn", "closed", "no", "completeness")
    assert venue_open.startswith("2026-09-09 09:30")


def test_plan_for_an_open_market_measures_lag():
    # 02:00 UTC on a Tuesday is 10:00 in Shanghai — mid-session.
    now = datetime(2026, 9, 8, 2, 0, tzinfo=timezone.utc)
    rows = cli.plan_rows(["cn"], now, timezone.utc)
    assert rows[0][1] == "open"
    assert rows[0][2] == "yes"
    assert rows[0][3] == "lag"


def test_cell_keys_expand_markets_asset_classes_surfaces_and_intervals():
    keys = cli.cell_keys(["hk"], [Surface.INTRADAY, Surface.SNAPSHOT], ["ohlcv-1m", "ohlcv-5m"])
    assert [k.as_str() for k in keys] == [
        "hk/equity/intraday/ohlcv-1m",
        "hk/equity/intraday/ohlcv-5m",
        "hk/equity/snapshot",
        "hk/index/intraday/ohlcv-1m",
        "hk/index/intraday/ohlcv-5m",
        "hk/index/snapshot",
    ]


def test_unknown_market_is_rejected_rather_than_silently_probed():
    with pytest.raises(SystemExit):
        cli.resolve_markets("cn,atlantis")


def test_auto_resolves_to_every_market_with_a_canary_basket():
    assert cli.resolve_markets("auto") == ["us", "cn", "hk", "jp", "uk", "eu"]
    assert canaries_for("cn", "equity") == ["600519.SS", "000858.SZ", "920300.BJ"]


def test_every_canary_lands_in_the_cell_it_measures():
    """A canary the reader routes to another cell writes a verdict no request reads."""
    from market_protocol import market_of

    from scripts.data_probe.canaries import CANARIES
    from src.data_client.market_data_provider import _route_ref

    for market, basket in CANARIES.items():
        for asset_class, symbols in basket.items():
            for symbol in symbols:
                ref = _route_ref(symbol.lstrip("^"), asset_class == "index")
                assert ref is not None, symbol
                assert (market_of(ref), ref.asset_class.value) == (market, asset_class), symbol


def test_wait_for_open_returns_immediately_when_the_market_is_open():
    printed: list[str] = []
    now = datetime(2026, 9, 8, 2, 0, tzinfo=timezone.utc)

    async def sleeper(_s):  # pragma: no cover - must never run
        raise AssertionError("should not sleep while the market is open")

    assert asyncio.run(cli.wait_for_open(
        "cn", now_fn=lambda: now, sleeper=sleeper, printer=printed.append
    )) is True
    assert "already open" in printed[0]


def test_wait_for_open_polls_then_gives_up_at_the_cap():
    printed: list[str] = []
    closed = datetime(2026, 9, 8, 14, 0, tzinfo=timezone.utc)
    slept: list[float] = []

    async def sleeper(seconds):
        slept.append(seconds)

    assert asyncio.run(cli.wait_for_open(
        "cn", now_fn=lambda: closed, poll_s=30, cap_s=120,
        sleeper=sleeper, printer=printed.append,
    )) is False
    assert slept == [30, 30, 30, 30]
    assert "gave up waiting for cn" in printed[-1]


def test_wait_for_open_in_pre_market_counts_down_to_the_bell():
    printed: list[str] = []
    pre = datetime(2026, 9, 8, 8, 0, tzinfo=NEW_YORK)

    async def sleeper(_s):
        return None

    asyncio.run(cli.wait_for_open(
        "us", now_fn=lambda: pre, cap_s=0, sleeper=sleeper, printer=printed.append,
    ))
    assert printed[0].startswith("us opens in 1h30m (2026-09-08 09:30 EDT)")


def test_a_cell_is_probed_only_with_providers_configured_for_its_market(monkeypatch):
    monkeypatch.setattr("src.config.settings.get_market_data_providers", lambda: [
        {"name": "tushare", "markets": ["cn"], "intraday_markets": [], "snapshot_markets": ["cn"]},
        {"name": "yfinance", "markets": [], "intraday_markets": ["non-us"]},
        {"name": "fmp", "markets": ["all"]},
    ])
    sources = {"tushare": object(), "yfinance": object(), "fmp": object(), "unlisted": object()}
    scopes = cli.configured_markets()
    assert set(cli.sources_for_market(sources, scopes, "us")) == {"fmp", "unlisted"}
    assert set(cli.sources_for_market(sources, scopes, "cn")) == set(sources)


def test_plain_table_is_aligned_when_rich_is_unavailable():
    text = render_table(("cell", "provider"), [["cn/equity/daily", "tushare"]])
    lines = text.splitlines()
    assert lines[0].startswith("cell")
    assert "cn/equity/daily  tushare" in lines[2]


def test_declared_bar_tier_is_venue_aware_for_fmp():
    from scripts.data_probe.probe import declared_tier

    assert declared_tier("fmp", surface=Surface.INTRADAY, asset_class="equity",
                         symbol="AAPL") == "realtime"
    assert declared_tier("fmp", surface=Surface.INTRADAY, asset_class="equity",
                         symbol="600519.SH") == "delayed_15m"
    assert declared_tier("fmp", surface=Surface.INTRADAY, asset_class="index",
                         symbol="^HSI") == "delayed_15m"


def test_declared_snapshot_tier_is_venue_aware_for_fmp():
    from scripts.data_probe.probe import declared_tier

    assert declared_tier("fmp", surface=Surface.SNAPSHOT, asset_class="equity",
                         symbol="AAPL") == "realtime"
    assert declared_tier("fmp", surface=Surface.SNAPSHOT, asset_class="equity",
                         symbol="0700.HK") == "delayed_15m"
