"""chartkit's settled contract: the layer JSON it writes, its caps and errors, times, publishing.

The sandbox imports the package as ``chartkit``; here it is the repo module. Indicator values are
cross-checked against loops outside the suite (tmp/chart-script/check_ta.py).
"""

from __future__ import annotations

import json
import subprocess
import sys
import types

import numpy as np
import pandas as pd
import pytest

from chartkit import Chart, ChartError, analysis, bars, ta
from chartkit._util import tz_for_symbol
from chartkit.schema import parse_layer


def _daily(n: int = 60, start: str = "2026-01-02") -> pd.DataFrame:
    idx = pd.bdate_range(start, periods=n)
    close = 100 + np.sin(np.arange(n) / 4) * 5 + np.arange(n) * 0.2
    return pd.DataFrame(
        {"open": close - 0.5, "high": close + 1, "low": close - 1, "close": close, "volume": 1e6},
        index=idx,
    )


@pytest.fixture
def chart_file(tmp_path, monkeypatch):
    path = tmp_path / "job" / "charts.jsonl"
    path.parent.mkdir()
    monkeypatch.setenv("PTC_CHART_FILE", str(path))
    return path


# ---------------------------------------------------------------- serialization shape


def test_layer_shape_and_defaults(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="NVDA Rising Channel!")
    sid = c.plot(ta.sma(df.close, 5), "SMA 5", note="trend")
    c.level(110, "Resistance")
    c.marker(df.index[10], "Earnings", position="aboveBar")
    c.alert(112.5, "Breakout", "above")
    layer = c.publish(summary="thesis")

    assert list(layer) == [
        "version", "id", "title", "summary", "symbol", "timeframe", "as_of", "source",
        "panes", "series", "fills", "levels", "markers", "regimes", "drawings", "alerts",
    ]
    assert layer["version"] == 1
    assert layer["id"] == "nvda-rising-channel"
    assert layer["source"] == {"kind": "python"}
    assert layer["as_of"] == df.index[-1].strftime("%Y-%m-%d")
    s = layer["series"][0]
    assert s["id"] == sid == "sma-5"
    assert {k: s[k] for k in ("pane", "type", "width", "style", "note")} == {
        "pane": "price", "type": "line", "width": 2, "style": "solid", "note": "trend"
    }
    # warmup NaNs are dropped, not serialized
    assert len(s["data"]) == len(df) - 4 and set(s["data"][0]) == {"time", "value"}
    assert layer["levels"][0] == {
        "price": 110.0, "pane": "price", "style": "dashed", "title": "Resistance", "color": "#787b86"
    }
    assert layer["markers"][0] == {
        "time": df.index[10].strftime("%Y-%m-%d"), "position": "aboveBar", "shape": "arrowDown",
        "color": "#ef5350", "text": "Earnings",
    }
    assert layer["alerts"][0]["condition"] == {"kind": "price_cross", "level": 112.5, "direction": "above"}
    parse_layer(json.dumps(layer))


def test_every_drawing_type_serializes_to_the_contract(chart_file):
    df = _daily(80)
    c = Chart(df, "AAPL", title="all drawings")
    t = [d.strftime("%Y-%m-%d") for d in df.index]
    a, b, d3 = (df.index[10], 100.0), (df.index[40], 110.0), (df.index[25], 104.0)
    fut = c.future(10)
    c.segment(a, b, extend="right")
    c.vline(fut[5], label="Earnings")
    c.channel(**analysis.regression_channel(df, 40))
    c.pitchfork(a, b, d3)
    c.box(a, b)
    c.label(b, text="Top", position="right")
    c.arrow(b, (fut[-1], 120.0))
    c.path([a, d3, b], point_labels=["A", None, "C"])
    c.fib(a, b, levels=[0, 0.5, 1])
    c.cone(**analysis.vol_cone(df, 10))
    c.forecast(130, 10, probability=0.6, label="Breakout")
    c.analog(df.index[10], df.index[14])
    c.position("long", b, 105, [120, 115], until=fut[-1])
    c.measure(a, b)
    c.profile(**analysis.volume_profile(df, df.index[20], None, bins=12))
    layer = c.publish()

    by_type = {d["type"]: d for d in layer["drawings"]}
    assert list(by_type) == [
        "segment", "vline", "channel", "pitchfork", "box", "label", "arrow", "path", "fib", "cone",
        "forecast", "analog", "position", "measure", "profile",
    ]
    assert by_type["segment"]["p1"] == {"time": t[10], "price": 100.0}
    assert by_type["vline"]["time"] == fut[5].strftime("%Y-%m-%d")  # future times allowed
    assert set(by_type["channel"]) >= {"p1", "p2", "p3", "extend", "fill"}
    assert by_type["label"]["text"] == "Top" and by_type["label"]["position"] == "right"
    assert by_type["path"]["point_labels"] == ["A", None, "C"] and by_type["path"]["closed"] is False
    assert by_type["fib"]["levels"] == [0.0, 0.5, 1.0]
    cone = by_type["cone"]
    assert [b_["label"] for b_ in cone["bands"]] == ["±2σ", "±1σ"]  # widest first
    assert len(cone["bands"][0]["upper"]) == 10
    assert by_type["forecast"]["probability"] == 0.6 and by_type["forecast"]["target"]["price"] == 130.0
    assert len(by_type["analog"]["bars"]) == 5 and by_type["analog"]["source_end"] == t[14]
    pos = by_type["position"]
    assert pos["side"] == "long" and pos["stop"] == 105.0 and pos["targets"] == [115.0, 120.0]
    prof = by_type["profile"]
    assert prof["from"] == t[20] and prof["to"] == t[-1] and len(prof["rows"]) == 12
    assert set(prof["rows"][0]) == {"low", "high", "volume", "up_volume"}
    assert prof["value_area"][0] <= prof["poc"] <= prof["value_area"][1]
    assert "info" not in json.dumps(layer)  # helper stats never reach the layer
    assert len({d["id"] for d in layer["drawings"]}) == 15  # ids unique per kind
    # the server's model is the authority: what chartkit writes, it accepts
    parsed = parse_layer(chart_file.read_text().splitlines()[-1])
    assert [d.type for d in parsed.drawings] == list(by_type)


def test_panes_fills_and_histogram_colors(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="panes")
    mid, up, lo = ta.bbands(df.close, 10, 2)
    u, lw = c.plot(up, "Upper"), c.plot(lo, "Lower")
    c.fill(u, lw)
    c.pane("macd", "MACD", height=0.2)
    c.plot(ta.macd(df.close, 3, 6, 3).hist, "Hist", pane="macd", type="histogram")
    c.level(0, pane="macd")
    layer = c.publish()
    assert layer["panes"] == [{"id": "macd", "height": 0.2, "title": "MACD"}]
    assert layer["fills"] == [{"a": "upper", "b": "lower", "color": "rgba(41,98,255,0.1)"}]
    hist = layer["series"][2]
    assert {p["color"] for p in hist["data"]} <= {"rgba(38,166,154,0.6)", "rgba(239,83,80,0.6)"}


# ---------------------------------------------------------------- times


def test_intraday_times_are_exchange_local_from_an_aware_index(chart_file):
    # 14:30 UTC is 09:30 in New York in winter and 10:30 in summer (DST).
    idx = pd.DatetimeIndex(["2026-01-05 14:30", "2026-01-05 14:35", "2026-07-06 14:30"], tz="UTC")
    df = pd.DataFrame({"close": [1.0, 2.0, 3.0]}, index=idx)
    c = Chart(df, "NVDA", "5min", title="intraday")
    c.vline(pd.Timestamp("2026-01-05 15:00", tz="UTC"))  # aware point: converted
    c.vline("2026-01-05 10:00")  # naive point: already exchange-local
    layer = c.publish()
    assert layer["as_of"] == "2026-07-06T10:30:00"
    assert c._times[:2] == ["2026-01-05T09:30:00", "2026-01-05T09:35:00"]
    assert [d["time"] for d in layer["drawings"]] == ["2026-01-05T10:00:00", "2026-01-05T10:00:00"]


@pytest.mark.parametrize(
    ("symbol", "tz"),
    [("NVDA", "America/New_York"), ("0700.HK", "Asia/Hong_Kong"), ("600519.SS", "Asia/Shanghai"),
     ("000001.SZ", "Asia/Shanghai"), ("VOD.L", "Europe/London"), ("7203.T", "Asia/Tokyo")],
)
def test_zone_defaults_from_the_symbol_suffix(symbol, tz):
    df = pd.DataFrame({"close": [1.0]}, index=pd.DatetimeIndex(["2026-03-02 01:30"], tz="UTC"))
    c = Chart(df, symbol, "30min", title="z")
    assert c.tz == tz
    assert c._times[0] == pd.Timestamp("2026-03-02 01:30", tz="UTC").tz_convert(tz).strftime("%Y-%m-%dT%H:%M:%S")


def test_explicit_tz_and_naive_index():
    naive = pd.DataFrame({"close": [1.0]}, index=pd.DatetimeIndex(["2026-03-02 09:30"]))
    assert Chart(naive, "0700.HK", "30min", title="n")._times == ["2026-03-02T09:30:00"]
    aware = naive.tz_localize("UTC")
    assert Chart(aware, "BTCUSD", "30min", title="n", tz="UTC")._times == ["2026-03-02T09:30:00"]


def test_timeframe_mismatches_are_caught():
    intraday = pd.DataFrame({"close": [1.0, 2.0]}, index=pd.DatetimeIndex(["2026-03-02 09:30", "2026-03-02 09:35"]))
    with pytest.raises(ChartError, match=r"Chart\(timeframe=...\): is '1day' but df has intraday bars"):
        Chart(intraday, "NVDA", title="x")
    with pytest.raises(ChartError, match="one bar per day"):
        Chart(_daily(), "NVDA", "5min", title="x")
    with pytest.raises(ChartError, match="looks like a title"):
        Chart(_daily(), "NVDA", "NVDA rising channel")
    assert Chart(_daily(), "NVDA", "1d", title="x").timeframe == "1day"


def test_future_times_follow_sessions():
    df = _daily(5, "2026-10-26")  # Mon..Fri
    assert [t.strftime("%a") for t in analysis.future_times(df, 3)] == ["Mon", "Tue", "Wed"]
    # HK 30-min bars with a lunch break carry their session slots forward.
    times = ["09:30", "10:00", "10:30", "11:00", "11:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:30"]
    idx = pd.DatetimeIndex([f"2026-10-29 {t}" for t in times] + ["2026-10-30 09:30", "2026-10-30 10:00",
                                                                 "2026-10-30 10:30", "2026-10-30 11:00"])
    out = analysis.future_times(pd.DataFrame({"close": 1.0}, index=idx), 4)
    assert [t.strftime("%a %H:%M") for t in out] == ["Fri 11:30", "Fri 13:00", "Fri 13:30", "Fri 14:00"]


def _one_day(times: list[str], day: str = "2026-10-29") -> pd.DataFrame:
    return pd.DataFrame({"close": 1.0}, index=pd.DatetimeIndex([f"{day} {t}" for t in times]))


def _hm(times) -> list[str]:
    return [t.strftime("%a %H:%M") for t in times]


def test_one_day_of_bars_runs_on_through_the_venues_session_hours(chart_file):
    # A session in progress steps on by the interval, not to tomorrow's first seen slot.
    partial = _one_day(["09:30", "09:35", "09:40"])
    assert _hm(Chart(partial, "NVDA", "5min", title="t").future(2)) == ["Thu 09:45", "Thu 09:50"]
    # A finished session (the 15:55 bar closes at 16:00) rolls to the next session's open.
    full = _one_day([t.strftime("%H:%M") for t in pd.date_range("09:30", "15:55", freq="5min")])
    assert _hm(Chart(full, "NVDA", "5min", title="t").future(2)) == ["Fri 09:30", "Fri 09:35"]
    # One bar has no spacing of its own; the declared interval supplies it.
    assert _hm(Chart(_one_day(["09:30"]), "NVDA", "5min", title="t").future(2)) == ["Thu 09:35", "Thu 09:40"]
    # Friday's last bar goes on to Monday's open, across the weekend.
    fri = _one_day(["15:50", "15:55"], "2026-10-30")
    assert _hm(Chart(fri, "NVDA", "5min", title="t").future(1)) == ["Mon 09:30"]
    # Hong Kong's lunch break holds no bars.
    hk = _one_day(["11:50", "11:55"])
    assert _hm(Chart(hk, "0700.HK", "5min", title="t").future(3)) == ["Thu 13:00", "Thu 13:05", "Thu 13:10"]
    # The symbol and interval can come from df.attrs, as chartkit.bars() sets them.
    partial.attrs.update(symbol="NVDA", timeframe="5min")
    assert _hm(analysis.future_times(partial, 1)) == ["Thu 09:45"]


def test_one_day_of_bars_with_no_known_venue_rolls_to_the_next_day(chart_file):
    partial = _one_day(["09:30", "09:35", "09:40"])
    assert _hm(analysis.future_times(partial, 2)) == ["Fri 09:30", "Fri 09:35"]
    assert _hm(analysis.future_times(partial, 2, symbol="NOT A SYMBOL")) == ["Fri 09:30", "Fri 09:35"]


def _waves(close) -> pd.DataFrame:
    close = np.asarray(close, dtype="float64")
    return pd.DataFrame({"open": close, "high": close + 1, "low": close - 1, "close": close},
                        index=pd.bdate_range("2026-01-02", periods=len(close)))


@pytest.mark.parametrize("name, n, center, amp, want, anchor", [
    ("rising", 120, lambda i: 100 + 0.5 * i, lambda i: 5, "up", "lows"),
    ("falling", 120, lambda i: 200 - 0.5 * i, lambda i: 5, "down", "highs"),
    # Broadening: lows fall while highs rise, so the closes pick the direction and the pair
    # moving that way has to carry the line (once drawn on the other pair, sloping against it).
    ("broadening, closes falling", 100, lambda i: 150 - 0.1 * i, lambda i: 2 + 0.2 * i, "down", "lows"),
    ("broadening, closes rising", 110, lambda i: 150 - 0.1 * i, lambda i: 2 + 0.2 * i, "up", "highs"),
])
def test_trend_channel_direction_matches_the_slope_it_draws(name, n, center, amp, want, anchor):
    i = np.arange(n)
    df = _waves(center(i) + amp(i) * np.sin(2 * np.pi * i / 20))
    ch = analysis.trend_channel(df)
    info = ch["info"]
    assert (info["direction"], info["anchor"]) == (want, anchor)
    assert (info["slope_per_bar"] > 0) == (want == "up") and info["slope_per_bar"] != 0
    assert (ch["p2"].price > ch["p1"].price) == (want == "up")
    assert info["upper"] > info["lower"]
    assert analysis.trend_channel(df, direction=want)["info"]["slope_per_bar"] == info["slope_per_bar"]


def test_trend_channel_refuses_a_direction_neither_pivot_pair_moves():
    i = np.arange(120)
    df = _waves(100 + 0.5 * i + 5 * np.sin(2 * np.pi * i / 20))
    with pytest.raises(ChartError, match=r"is 'down' but neither the last two swing lows nor the last two swing highs fall"):
        analysis.trend_channel(df, direction="down")


def test_trend_channel_passes_over_a_bar_with_no_high():
    i = np.arange(120)
    df = _waves(100 + 0.5 * i + 5 * np.sin(2 * np.pi * i / 20))
    gapped = df.copy()
    gapped.iloc[-2, gapped.columns.get_loc("high")] = np.nan
    got = analysis.trend_channel(gapped)
    assert got["info"]["anchor"] == "lows"
    assert np.isfinite(got["p3"].price) and np.isfinite(got["info"]["upper"])
    assert got["p3"].time != df.index[-2]


# ---------------------------------------------------------------- regimes and masks


def test_mask_compresses_to_inclusive_ranges(chart_file):
    df = _daily(10)
    t = [d.strftime("%Y-%m-%d") for d in df.index]
    mask = pd.Series([False, True, True, False, True, False, False, True, True, True], index=df.index)
    c = Chart(df, "NVDA", title="regimes")
    assert c.regime(mask, "rgba(0,0,0,0.1)", label="up") == 3
    assert c.regime((df.index[0], df.index[2])) == 1
    assert c.regime(pd.Series(False, index=df.index)) == 0
    layer = c.publish()
    assert [(r["from"], r["to"]) for r in layer["regimes"]] == [
        (t[1], t[2]), (t[4], t[4]), (t[7], t[9]), (t[0], t[2])
    ]
    assert layer["regimes"][0] == {"from": t[1], "to": t[2], "color": "rgba(0,0,0,0.1)", "label": "up"}


def test_series_align_by_time_and_keep_a_future_tail(chart_file):
    df = _daily(10)
    c = Chart(df, "NVDA", title="align")
    fut = c.future(2)
    s = pd.Series([1.0, 2.0, 3.0, 4.0], index=[df.index[0], df.index[5], fut[0], fut[1]])
    c.plot(s, "sparse")
    c.markers(np.array([True] + [False] * 9), "first")  # arrays align by position
    layer = c.publish()
    assert [p["time"] for p in layer["series"][0]["data"]] == [
        df.index[0].strftime("%Y-%m-%d"), df.index[5].strftime("%Y-%m-%d"),
        fut[0].strftime("%Y-%m-%d"), fut[1].strftime("%Y-%m-%d"),
    ]
    assert len(layer["markers"]) == 1


def test_a_projected_histogram_keeps_its_sign_colours(chart_file):
    df = _daily(10)
    c = Chart(df, "NVDA", title="hist")
    fut = c.future(2)
    s = pd.Series([1.0, -1.0, 2.0, -2.0], index=[df.index[8], df.index[9], fut[0], fut[1]])
    c.pane("osc")
    c.plot(s, "macd hist", pane="osc", type="histogram")
    colors = [p.get("color") for p in c.to_dict()["series"][0]["data"]]
    up, down = colors[0], colors[1]
    assert up != down and colors[2:] == [up, down]


def test_a_colour_series_colours_the_projected_points_too(chart_file):
    df = _daily(10)
    c = Chart(df, "NVDA", title="hues")
    fut = c.future(2)
    at = [df.index[8], df.index[9], fut[0], fut[1]]
    s = pd.Series([1.0, 2.0, 3.0, 4.0], index=at)
    hues = pd.Series(["#ff0000", "#00ff00", "#0000ff", "#123456"], index=at)
    c.plot(s, "projection", color=hues)
    colors = [p.get("color") for p in c.to_dict()["series"][0]["data"]]
    assert colors == ["#ff0000", "#00ff00", "#0000ff", "#123456"]
    with pytest.raises(ChartError, match="color"):
        c.plot(s, "bad", color=pd.Series(["#ff0000"] * 3 + ["not a colour"], index=at))


# ---------------------------------------------------------------- caps and errors


def test_forecast_reaches_by_bars_or_by_time_from_the_last_close(chart_file):
    df = _daily()
    c = Chart(df, "AAPL", title="t")
    c.forecast(130, 3, probability=0.4)
    fut = c.future(3)
    c.forecast(90, fut[-1], origin=(df.index[-5], 100.0))
    first, second = c.publish()["drawings"]
    assert first["origin"] == {"time": df.index[-1].strftime("%Y-%m-%d"), "price": float(df.close.iloc[-1])}
    assert first["target"] == {"time": fut[-1].strftime("%Y-%m-%d"), "price": 130.0} and first["probability"] == 0.4
    assert "probability" not in second and second["target"]["time"] == fut[-1].strftime("%Y-%m-%d")
    assert first["color"] != second["color"]  # up and down default to different colours


def test_a_future_origin_counts_its_bars_from_the_origin_not_from_the_last_bar(chart_file):
    df = _daily()
    c = Chart(df, "AAPL", title="t")
    fut = [t.strftime("%Y-%m-%d") for t in c.future(12)]
    origin = fut[3]
    c.forecast(130, 2, origin=(origin, 100.0))
    c.analog(df.index[10], df.index[12], origin=(origin, 150.0))
    f, a = c.publish()["drawings"]
    assert f["origin"]["time"] == origin and f["target"]["time"] == fut[5]
    assert [b["time"] for b in a["bars"]] == fut[4:7]


def test_analog_replays_the_stretch_scaled_to_the_origin_into_the_future(chart_file):
    df = _daily()
    c = Chart(df, "AAPL", title="t")
    c.analog(df.index[10], df.index[14], origin=(df.index[-3], 150.0))
    d = c.publish()["drawings"][0]
    k = 150.0 / float(df.close.iloc[9])
    assert d["source_start"] == df.index[10].strftime("%Y-%m-%d") and len(d["bars"]) == 5
    assert d["bars"][0]["close"] == pytest.approx(float(df.close.iloc[10]) * k)
    assert d["bars"][-1]["high"] == pytest.approx(float(df.high.iloc[14]) * k)
    # two df bars remain after the origin; the other three spill into the future
    assert [b["time"] for b in d["bars"][:2]] == [t.strftime("%Y-%m-%d") for t in df.index[-2:]]
    assert d["bars"][2]["time"] > df.index[-1].strftime("%Y-%m-%d")
    assert [b["time"] for b in d["bars"]] == sorted(b["time"] for b in d["bars"])


def test_analog_refuses_too_long_a_stretch_and_a_df_without_ohlc(chart_file):
    df = _daily(600)
    with pytest.raises(ChartError, match=r"c\.analog\(end=...\): .* holds 501 bars; the limit is 500"):
        Chart(df, "AAPL", title="t").analog(df.index[0], df.index[500], origin=(df.index[-1], 100))
    with pytest.raises(ChartError, match=r"c\.analog\(df=...\): replaying bars needs open, high, low and close"):
        Chart(df.drop(columns="high"), "AAPL", title="t").analog(df.index[5], df.index[8])


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda c, df: c.segment((150.0, "2026-01-02"), ("2026-01-05", 1.0)),
         r"c\.segment\(p1\[0\] \(time\)=...\): .*points are \(time, price\)"),
        (lambda c, df: c.level(df.close), r"c\.level\(price=...\): expected a single number"),
        (lambda c, df: c.plot(ta.bbands(df.close, 5, 2), "BB"), r"c\.plot\(series=...\): .*unpack them first"),
        (lambda c, df: c.plot(df.close, "x", pane="rsi"), r"c\.plot\(pane=...\): no pane named 'rsi'"),
        (lambda c, df: c.markers(df.close > 0, note="x" * 601), r"c\.markers\(note=...\): is 601 characters; the limit is 600"),
        (lambda c, df: c.level(1.0, color="cornflowerblue"), r"c\.level\(color=...\): expected a hex or rgb"),
        (lambda c, df: c.position("long", (df.index[-1], 100), 101, [110]), r"c\.position\(\): a long's stop 101 must be below its entry 100"),
        (lambda c, df: c.position("short", (df.index[-1], 100), 105, [110]), r"c\.position\(\): a short's targets must be below its entry 100"),
        (lambda c, df: c.path([(df.index[0], 1)] * 501), r"c\.path\(points=...\): has 501 points; the limit is 500"),
        (lambda c, df: c.markers((df.close > 0).astype(int)), r"c\.markers\(mask=...\): expected a boolean Series"),
        (lambda c, df: c.forecast(110, 5, probability=1.5), r"c\.forecast\(probability=...\): must be from 0 to 1, got 1\.5"),
        (lambda c, df: c.forecast(110, df.index[-3]), r"c\.forecast\(\): target\.time .* must be after origin\.time"),
        (lambda c, df: c.forecast(110, 0), r"c\.forecast\(by=...\): a number of bars must be at least 1"),
        (lambda c, df: c.analog(df.index[5], df.index[-1]), r"c\.analog\(origin=...\): .* falls inside the stretch"),
        (lambda c, df: c.analog("2025-01-01", "2025-01-10"), r"c\.analog\(start=...\): no bars of df fall from"),
        (lambda c, df: c.cone((df.index[-1], 1), [{"label": "x", "upper": [(df.index[-1], 2)], "lower": [(df.index[-1], 0)]}] * 5),
         r"c\.cone\(bands=...\): has 5 bands; the limit is 4"),
        (lambda c, df: c.profile(df.index[0], df.index[-1], [{"low": 1, "high": 2, "volume": 1}] * 201),
         r"c\.profile\(rows=...\): has 201 rows; the limit is 200"),
        (lambda c, df: [c.pane(f"p{i}", height=0.1) for i in range(5)], r"c\.pane\(id=...\): a layer holds at most 4"),
        (lambda c, df: [c.plot(df.close, f"s{i}") for i in range(25)], r"c\.plot\(series=...\): a layer holds at most 24 series"),
        (lambda c, df: [c.vline(df.index[0]) for _ in range(301)], r"a layer holds at most 300 drawings"),
        (lambda c, df: [c.markers(df.close > 0) for _ in range(51)], r"c\.markers\(mask=...\): would bring the layer to 3060 markers; the limit is 3000"),
        (lambda c, df: c.plot(np.arange(5.0), "x"), r"needs exactly one value per bar of df \(60\)"),
        (lambda c, df: c.fill("a", "b"), r"c\.fill\(a=...\): no series with id 'a'"),
    ],
)
def test_errors_name_the_call_and_argument(build, message):
    df = _daily()
    with pytest.raises(ChartError, match=message):
        build(Chart(df, "NVDA", title="errors"), df)


def test_layer_level_caps():
    df = _daily()
    with pytest.raises(ChartError, match=r"Chart\(title=...\): is 121 characters; the limit is 120"):
        Chart(df, "NVDA", title="x" * 121)
    with pytest.raises(ChartError, match="must be 1-48 characters"):
        Chart(df, "NVDA", title="x", id="Not A Slug")
    c = Chart(df, "NVDA", title="x")
    c.level(1)
    with pytest.raises(ChartError, match=r"c\.publish\(summary=...\): is 1201 characters; the limit is 1200"):
        c.publish(summary="s" * 1201)
    with pytest.raises(ChartError, match="nothing to draw"):
        Chart(df, "NVDA", title="empty").publish()


def test_size_cap_rejects_at_publish(chart_file):
    idx = pd.date_range("2026-01-01 09:30", periods=20_000, freq="1min")
    df = pd.DataFrame({"close": np.linspace(1, 2, len(idx))}, index=idx)
    c = Chart(df, "NVDA", "1min", title="big")
    for i in range(5):
        c.plot(df.close + i * 0.123456789, f"s{i}", note="n" * 600)
    with pytest.raises(ChartError, match=r"the layer is \d\.\d\d MB serialized, over the 1\.5 MB cap"):
        c.publish()
    assert not chart_file.exists()


def test_publish_refuses_what_the_server_would_with_its_words(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="x")
    c.plot(df.close, "a")
    # Past the builder's own checks, as a script editing the layer's lists could get.
    c._series[0]["pane"] = "rsi"
    c._fills.append({"a": "a", "b": "nope", "color": "#000"})
    with pytest.raises(ChartError) as err:
        c.publish()
    msg = str(err.value)
    assert msg.startswith("c.publish(): the layer is not valid:\n  - ")
    assert "series[0].pane: 'rsi' is not a declared pane" in msg
    assert "fills[0].b: 'nope' is not a series id in this layer" in msg
    c._drawings.append({"type": "segment", "p1": {"time": "2026/01/02", "price": 1},
                        "p2": {"time": "2026-01-03", "price": 2}})
    with pytest.raises(ChartError, match=r"drawings\[0\] \(segment\)\.p1\.time: time '2026/01/02' must be"):
        c.publish()
    assert not chart_file.exists()


def test_the_builder_accepts_what_the_server_accepts(chart_file):
    # Element ids and colours take the layer schema's own patterns, not a stricter copy.
    df = _daily()
    c = Chart(df, "NVDA", title="x")
    c.pane("RSI_14", height=0.2)
    c.plot(df.close, "close", pane="RSI_14", id="Close.v2", color="rgb(10%, 20%, 30%)")
    c.vline(df.index[3], id="Earnings:Q3")
    parse_layer(json.dumps(c.publish()))
    with pytest.raises(ChartError, match=r"c\.vline\(id=...\): must be 1-48 characters of letters"):
        c.vline(df.index[4], id="-leading-dash")
    with pytest.raises(ChartError, match=r"c\.fib\(levels=...\): has 25 levels; the limit is 24"):
        c.fib((df.index[0], 1), (df.index[1], 2), levels=[0.5] * 25)


def test_symbol_zones_come_from_the_market_data_layer():
    assert tz_for_symbol("0700.HK") == "Asia/Hong_Kong"
    assert tz_for_symbol("600519.SS") == "Asia/Shanghai"
    assert tz_for_symbol("NVDA") == "America/New_York"
    assert tz_for_symbol("EURUSD=X") == "UTC"
    assert tz_for_symbol("not/a symbol") == "America/New_York"


def test_the_server_reads_the_schema_without_pandas():
    code = ("import sys; import chartkit.schema; "
            "sys.exit(int(any(m in sys.modules for m in ('pandas', 'numpy'))))")
    assert subprocess.run([sys.executable, "-c", code], capture_output=True).returncode == 0


# ---------------------------------------------------------------- publishing


def test_publish_appends_one_json_line_per_call(chart_file, capsys):
    df = _daily()
    c = Chart(df, "NVDA", title="NVDA setup")
    c.level(100)
    c.vline(df.index[3])
    first = c.publish()
    c.level(105)
    second = c.publish()  # same id: the server replaces the stored layer
    lines = chart_file.read_text().splitlines()
    assert [json.loads(x) for x in lines] == [first, second]
    assert first["id"] == second["id"] == "nvda-setup"
    out = capsys.readouterr().out
    assert 'chart layer "NVDA setup" queued for NVDA 1day (1 level, 1 drawing)' in out
    assert "(2 levels, 1 drawing)" in out


def test_publish_without_the_env_var_explains_where_it_works(monkeypatch):
    monkeypatch.delenv("PTC_CHART_FILE", raising=False)
    c = Chart(_daily(), "NVDA", title="x")
    c.level(1)
    with pytest.raises(RuntimeError, match="charts publish only from ExecuteCode/Bash runs in the sandbox"):
        c.publish()


def test_non_ascii_title_gets_a_stable_id(chart_file):
    a = Chart(_daily(), "0700.HK", title="腾讯 上升通道")
    b = Chart(_daily(), "0700.HK", title="腾讯 上升通道")
    assert a.id == b.id and a.id.startswith("layer-")


# ---------------------------------------------------------------- bars()


def _fake_price_data(monkeypatch, result):
    calls = []

    def get_stock_data(**kwargs):
        calls.append(kwargs)
        return result

    pkg = types.ModuleType("tools")
    mod = types.ModuleType("tools.price_data")
    mod.get_stock_data = get_stock_data
    pkg.price_data = mod
    monkeypatch.setitem(sys.modules, "tools", pkg)
    monkeypatch.setitem(sys.modules, "tools.price_data", mod)
    return calls


def test_bars_builds_a_local_frame_with_attrs(monkeypatch, chart_file):
    calls = _fake_price_data(monkeypatch, {
        "symbol": "0700.HK", "interval": "30min", "timezone": "Asia/Hong_Kong", "currency": "HKD", "count": 2,
        "data": [
            {"date": "2026-10-29 09:30:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10},
            {"date": "2026-10-29 10:00:00", "open": 1.5, "high": 2.5, "low": 1, "close": 2, "volume": "inf"},
        ],
    })
    df = bars("0700.HK", "30min", start="2026-10-29")
    assert np.isnan(df["volume"].iloc[1])  # a value that is not a finite number is missing
    # intraday needs both dates, so a missing end is filled in
    assert calls[0]["interval"] == "30min" and calls[0]["start_date"] == "2026-10-29"
    assert calls[0]["end_date"] > "2026-10-29"
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.attrs == {"symbol": "0700.HK", "timeframe": "30min", "tz": "Asia/Hong_Kong", "currency": "HKD"}
    c = Chart(df, title="from bars")
    assert (c.symbol, c.timeframe, c.tz, c.as_of) == ("0700.HK", "30min", "Asia/Hong_Kong", "2026-10-29T10:00:00")


def test_bars_surfaces_tool_errors(monkeypatch):
    _fake_price_data(monkeypatch, {"error": "upstream_error", "detail": "FMP fetch failed."})
    with pytest.raises(ChartError, match="upstream_error: FMP fetch failed"):
        bars("NVDA")


def test_bars_without_the_tools_package(monkeypatch):
    monkeypatch.setitem(sys.modules, "tools", None)
    with pytest.raises(ChartError, match=r"tools\.price_data\) are not importable"):
        bars("NVDA")


# ---------------------------------------------------------------- one contract, checked once


def test_pane_and_fill_rules_are_the_schemas_and_surface_at_publish(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="panes")
    with pytest.raises(ChartError, match=r"c\.pane\(height=...\): must be a fraction of the chart height from 0\.05 to 0\.5, got 0\.6"):
        c.pane("x", height=0.6)
    c.pane("a", height=0.4)
    c.pane("b", height=0.4)  # each is fine on its own; the total is the layer's rule
    up = c.plot(df.close, "up")
    lo = c.plot(df.close - 1, "lo", pane="b")
    c.fill(up, lo)
    with pytest.raises(ChartError) as err:
        c.publish()
    assert "panes: lower panes take 0.80 of the chart height" in str(err.value)
    assert f"fills[0]: series {up!r} is in pane 'price' but {lo!r} is in 'b'" in str(err.value)
    assert not chart_file.exists()


def test_an_element_is_refused_with_the_schemas_words_located_by_the_call(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="t")
    t0, t1 = df.index[1], df.index[5]
    with pytest.raises(ChartError, match=r"c\.path\(\): point_labels has 1 entries but points has 3"):
        c.path([(t0, 1), (t1, 2), (t1 + pd.Timedelta(days=1), 3)], point_labels=["a"])
    with pytest.raises(ChartError, match=r"c\.cone\(bands\[0\]\.foo=...\): is not a field of this element"):
        c.cone((t0, 1), [{"label": "x", "upper": [(t1, 2)], "lower": [(t1, 0)], "foo": 1}])
    with pytest.raises(ChartError, match=r"c\.cone\(bands=...\): has 0 bands; it needs at least 1\. Pass \[\{'label'"):
        c.cone((t0, 1), [])
    with pytest.raises(ChartError, match=r"c\.profile\(\): it ends at .*, before it starts at"):
        c.profile(t1, t0, [{"low": 1, "high": 2, "volume": 1}])
    with pytest.raises(ChartError, match=r"c\.alert\(direction=...\): must be 'above', 'below' or 'either', got 'sideways'"):
        c.alert(1, "t", "sideways")
    with pytest.raises(ChartError, match=r"c\.plot\(series=...\): has 20001 points; the limit is 20000\. Plot a shorter"):
        idx = pd.date_range("2026-01-01 09:30", periods=20_001, freq="1min")
        big = pd.DataFrame({"close": np.arange(20_001.0)}, index=idx)
        Chart(big, "NVDA", "1min", title="big").plot(big.close, "x")
    # Every problem of one element comes back together, up to a handful.
    with pytest.raises(ChartError) as err:
        c.segment((t0, 1), (t1, 2), extend="up", style="wavy", width=9)
    assert str(err.value).count("\n") == 2
    assert not c._drawings


def test_a_marker_style_is_checked_even_when_the_mask_selects_nothing():
    df = _daily()
    with pytest.raises(ChartError, match=r"c\.markers\(position=...\)"):
        Chart(df, "NVDA", title="t").markers(df.close < 0, position="sideways")


def test_a_dict_point_may_carry_other_keys_in_chart_and_analysis_alike():
    df = _daily()
    p = {"time": df.index[3], "price": 101.5, "volume": 7}
    c = Chart(df, "NVDA", title="t")
    c.measure(p, (df.index[9], 99.0))
    assert c._drawings[0]["p1"] == {"time": df.index[3].strftime("%Y-%m-%d"), "price": 101.5}
    analysis.measured_move(df, p, {"time": df.index[8], "price": 103.0}, (df.index[12], 100.0))
    with pytest.raises(ChartError, match=r"a dict point needs the keys 'time' and 'price', got \['time'\]"):
        c.measure({"time": df.index[3]}, p)
    with pytest.raises(ChartError, match=r"analysis\.measured_move\(a=...\): a dict point needs the keys"):
        analysis.measured_move(df, {"price": 1.0}, p, p)


def test_chart_and_snapshot_read_the_same_bars_the_same_way(tmp_path):
    from chartkit import snapshot

    df = _daily()
    for bad, match in (
        (df.iloc[::-1], r"bars must be in time order"),
        (pd.concat([df, df.iloc[:1]]).sort_index(), r"more than one bar at"),
        (df.set_axis(df.index + pd.Timedelta(hours=9)), r"is '1day' but df has intraday bars"),
    ):
        with pytest.raises(ChartError, match=match):
            Chart(bad, "NVDA", title="t")
        with pytest.raises(ChartError, match=match):
            snapshot(bad, symbol="NVDA")
    with pytest.raises(ChartError, match=r"must be one ticker with no spaces"):
        snapshot(df, symbol="N VDA")
    with pytest.raises(ChartError, match=r"must be one ticker with no spaces"):
        Chart(df, "N VDA", title="t")


def test_vol_cone_leaves_the_band_fill_to_the_cone():
    df = _daily()
    cone = analysis.vol_cone(df, 5)
    assert all("fill" not in b for b in cone["bands"])
    c = Chart(df, "NVDA", title="t")
    c.cone(**cone)
    assert [b["fill"] for b in c._drawings[0]["bands"]] == ["rgba(41,98,255,0.08)", "rgba(41,98,255,0.14)"]


def test_bars_windows_follow_the_interval(monkeypatch):
    rows = [{"date": "2026-10-29", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}]
    calls = _fake_price_data(monkeypatch, {"symbol": "NVDA", "timezone": "America/New_York", "data": rows})
    bars("NVDA")
    bars("NVDA", start="2026-10-01 09:30", end="2026-10-29")
    bars("NVDA", "5min", start="2026-10-28 09:30", end="2026-10-29 16:00")
    bars("NVDA", "5min", end="2026-10-29 16:00")
    assert calls[0] == {"symbol": "NVDA", "interval": "1day"}
    assert (calls[1]["start_date"], calls[1]["end_date"]) == ("2026-10-01", "2026-10-29")
    # Both dates given: the times of day go through; either missing: both fall back to whole days.
    assert (calls[2]["start_date"], calls[2]["end_date"]) == ("2026-10-28 09:30", "2026-10-29 16:00")
    assert (calls[3]["start_date"], calls[3]["end_date"]) == ("2026-10-09", "2026-10-29")


def test_a_fill_from_an_rgb_color_is_translucent():
    # A channel's default fill comes from its line color; an opaque rgb() must not cover the candles.
    from chartkit._util import with_alpha
    assert with_alpha("rgb(41, 98, 255)", 0.08) == "rgba(41,98,255,0.08)"
    assert with_alpha("#2962ff", 0.08) == "rgba(41,98,255,0.08)"
    assert with_alpha("rgba(41,98,255,0.5)", 0.08) == "rgba(41,98,255,0.5)"
    df = _daily()
    c = Chart(df, "NVDA", title="t", summary="s")
    c.channel((df.index[0], 100), (df.index[10], 110), (df.index[5], 104), color="rgb(41, 98, 255)")
    assert c.to_dict()["drawings"][0]["fill"] == "rgba(41,98,255,0.08)"


def test_capitalised_ohlc_columns_read_as_the_lowercase_ones():
    # A snapshot reads Close as close; the builders that fall back to the last close must too.
    df = _daily().rename(columns=str.capitalize)
    c = Chart(df, "NVDA", title="t", summary="s")
    c.forecast(float(df["Close"].iloc[-1]) * 1.1, 5)
    assert c.to_dict()["drawings"][0]["origin"]["price"] == float(df["Close"].iloc[-1])


def test_a_level_alone_in_a_lower_pane_is_refused_at_publish(chart_file):
    df = _daily()
    c = Chart(df, "NVDA", title="t", summary="s")
    c.pane("osc")
    c.level(70, "overbought", pane="osc")
    with pytest.raises(ChartError) as err:
        c.publish()
    assert "levels[0].pane: pane 'osc' plots no series" in str(err.value)
    assert not chart_file.exists()
    c.plot(ta.rsi(df.close, 14), "RSI", pane="osc")
    c.publish()
    assert chart_file.exists()


@pytest.mark.parametrize("fn", [ta.ema, ta.rma])
def test_a_recursive_average_carries_its_state_over_a_gap(fn):
    # Pine's ta.ema and ta.rma ignore an na input: the gap bar stays empty and the
    # average resumes from where it was, as if the bar were not there.
    x = pd.Series([1.0, 2.0, 3.0, np.nan, 4.0, 5.0, np.nan, 6.0, 7.0])
    got = fn(x, 3)
    want = fn(x.dropna().reset_index(drop=True), 3).to_numpy()
    assert np.isnan(got.iloc[3]) and np.isnan(got.iloc[6])
    np.testing.assert_allclose(got.dropna().to_numpy(), want[~np.isnan(want)])
    # The seed is the first n valid values, wherever the gap falls among them.
    seeded = fn(pd.Series([1.0, np.nan, 2.0, 3.0]), 3)
    assert seeded.iloc[3] == pytest.approx(2.0)


@pytest.mark.parametrize("fn", [ta.sma, ta.stdev, ta.highest, ta.lowest, ta.wma])
def test_a_rolling_window_closes_over_a_gap(fn):
    # Pine's windowed built-ins take the last n valid values: a gap leaves its own bar
    # empty instead of emptying the n bars whose window holds it.
    x = pd.Series([1.0, 4.0, 2.0, np.nan, 5.0, 3.0, 6.0, np.nan, 2.0])
    got = fn(x, 3)
    want = fn(x.dropna().reset_index(drop=True), 3).to_numpy()
    assert np.isnan(got.iloc[3]) and np.isnan(got.iloc[7])
    np.testing.assert_allclose(got.dropna().to_numpy(), want[~np.isnan(want)])
    assert not np.isnan(got.iloc[4])


def test_rsi_counts_the_move_across_a_gap():
    # Like the averages under it: after a gap, RSI is the RSI of the series with the gap dropped.
    x = pd.Series([10.0, 11.0, 10.5, np.nan, 12.0, 11.0, 11.5, np.nan, np.nan, 13.0, 12.5])
    got = ta.rsi(x, 3)
    want = ta.rsi(x.dropna().reset_index(drop=True), 3).to_numpy()
    assert got[x.isna()].isna().all()
    np.testing.assert_allclose(got.dropna().to_numpy(), want[~np.isnan(want)])


def test_supertrend_goes_on_from_the_last_bar_after_a_gap():
    # A steady rise is an uptrend throughout; a missing bar leaves its own bar empty and the
    # trend resumes, instead of restarting as the downtrend the first defined bar is.
    i = np.arange(60, dtype="float64")
    df = _waves(100 + i)
    gapped = df.copy()
    gapped.iloc[40] = np.nan
    want = ta.supertrend(df, 10, 3)
    got = ta.supertrend(gapped, 10, 3)
    assert want.direction.iloc[39:].eq(1).all()
    assert np.isnan(got.direction.iloc[40]) and np.isnan(got.line.iloc[40])
    assert got.direction.iloc[41:].eq(1).all()


def test_future_times_refuses_more_bars_than_a_layer_reaches():
    with pytest.raises(ChartError, match="at most 20000 bars past the last one"):
        analysis.future_times(_daily(20), 100_000_000)


def test_zigzag_starts_from_the_first_bar_with_a_range():
    df = _daily()
    gapped = df.copy()
    gapped.iloc[0, gapped.columns.get_loc("high")] = np.nan
    want = analysis.zigzag(df.iloc[1:], 3)
    assert want and analysis.zigzag(gapped, 3) == want


def test_a_future_time_between_slots_is_the_slot_before_it():
    df = _daily(20)  # ends on a weekday; future slots skip the weekend
    fut = analysis.future_times(df, 10)
    fri = next(t for t in fut if t.weekday() == 4)
    sat = fri + pd.Timedelta(days=1)
    a, b = (df.index[5], 100.0), (df.index[10], 105.0)
    on_fri = analysis.measured_move(df, a, b, (fri, 103.0))
    on_sat = analysis.measured_move(df, a, b, (sat, 103.0))
    assert on_sat["p1"].time == on_fri["p1"].time == fri
    assert on_sat["p2"].time == on_fri["p2"].time


def test_a_future_time_past_the_first_500_slots_lands_on_its_own_slot():
    df = _daily(20)
    far = analysis.future_times(df, 800)[-1]
    a, b = (df.index[5], 100.0), (df.index[10], 105.0)
    assert analysis.measured_move(df, a, b, (far, 103.0))["p1"].time == far


def test_trend_channel_reads_the_closes_it_has_when_the_swings_disagree():
    i = np.arange(110)
    df = _waves(150 - 0.1 * i + (2 + 0.2 * i) * np.sin(2 * np.pi * i / 20))
    df.iloc[-3, df.columns.get_loc("close")] = np.nan
    assert analysis.trend_channel(df)["info"]["direction"] == "up"


def test_forecast_by_reaches_the_last_future_slot_and_no_further(chart_file):
    c = Chart(_daily(), "NVDA", title="far")
    c.forecast(130, 20_000)
    with pytest.raises(ChartError, match=r"c\.forecast\(by=...\): reaches past 20000 bars"):
        c.forecast(130, 20_001)


def test_markers_count_only_bars_with_a_price(chart_file):
    df = _daily(3_500)
    price = pd.Series(np.nan, index=df.index)
    price.iloc[::500] = df["low"].iloc[::500]
    c = Chart(df, "NVDA", title="sparse")
    assert c.markers(pd.Series(True, index=df.index), position="atPrice", price=price) == 7


def test_anchored_vwap_goes_on_past_a_missing_bar():
    idx = pd.date_range("2026-03-02 09:30", periods=8, freq="h")
    df = pd.DataFrame({"high": np.arange(8.0) + 11, "low": np.arange(8.0) + 9,
                       "close": np.arange(8.0) + 10, "volume": 100.0}, index=idx)
    gapped = df.copy()
    gapped.iloc[3, gapped.columns.get_loc("volume")] = np.nan
    got = ta.vwap(gapped, anchor=idx[1])
    want = ta.vwap(df.drop(idx[3]), anchor=idx[1])
    assert np.isnan(got.iloc[3]) and got.iloc[4:].notna().all()
    np.testing.assert_allclose(got.drop(idx[3]).dropna().to_numpy(), want.dropna().to_numpy())


def test_vol_cone_horizon_stops_at_the_band_limit():
    df = _daily(60)
    assert len(analysis.vol_cone(df, 500)["bands"][0]["upper"]) == 500
    with pytest.raises(ChartError, match="at most 500"):
        analysis.vol_cone(df, 501)


def test_fib_from_swing_refuses_a_window_with_no_highs():
    df = _daily(60)
    df["high"] = np.nan
    with pytest.raises(ChartError, match=r"analysis\.fib_from_swing\(df=...\): has no high values"):
        analysis.fib_from_swing(df, 30)


def test_forecast_asks_for_an_origin_when_df_has_no_close(chart_file):
    df = _daily()[["volume"]]
    df.attrs.update(_daily().attrs)
    c = Chart(df, "NVDA", title="volume only")
    with pytest.raises(ChartError, match=r"c\.forecast\(origin=...\): defaults to the last close"):
        c.forecast(130, 5)
    c.forecast(130, 5, origin=(df.index[-1], 120.0))


def test_regression_channel_refuses_an_infinite_close():
    df = _daily(60)
    df.iloc[-3, df.columns.get_loc("close")] = np.inf
    with pytest.raises(ChartError, match="NaN or infinite values in the last 30 bars"):
        analysis.regression_channel(df, 30)


def test_volume_profile_refuses_a_range_with_no_high_or_low():
    df = _daily(60)
    df.loc[df.index[40]:, "low"] = np.nan
    with pytest.raises(ChartError, match=r"analysis\.volume_profile\(df=...\): has no bar with a high and a low"):
        analysis.volume_profile(df, df.index[40], df.index[-1])
    rows = analysis.volume_profile(df, df.index[30], df.index[-1])["rows"]
    assert all(np.isfinite([r["low"], r["high"]]).all() for r in rows)


def test_vol_cone_refuses_a_missing_close_in_its_window():
    df = _daily(60)
    df.iloc[-5, df.columns.get_loc("close")] = np.nan
    with pytest.raises(ChartError, match=r"analysis\.vol_cone\(df=...\): needs 21 positive closes"):
        analysis.vol_cone(df, 10)


def test_volume_profile_refuses_a_range_with_no_volume():
    df = _daily(60)
    df["volume"] = np.nan
    with pytest.raises(ChartError, match=r"analysis\.volume_profile\(df=...\): has no volume from"):
        analysis.volume_profile(df)
    df["volume"] = 1e6
    df.iloc[5, df.columns.get_loc("volume")] = np.inf
    df.iloc[6, df.columns.get_loc("volume")] = -5.0
    prof = analysis.volume_profile(df)
    assert np.isfinite(prof["info"]["total_volume"]) and prof["info"]["total_volume"] == pytest.approx(58e6)


def test_trend_channel_passes_over_an_infinite_high():
    i = np.arange(120)
    df = _waves(100 + 0.5 * i + 5 * np.sin(2 * np.pi * i / 20))
    df.iloc[-2, df.columns.get_loc("high")] = np.inf
    got = analysis.trend_channel(df)
    assert np.isfinite(got["p3"].price) and np.isfinite(got["info"]["upper"])


def test_an_infinite_value_is_read_as_missing():
    # fib_from_swing on a window with one infinite high takes the finite swing.
    df = _daily(60)
    gapped = df.copy()
    gapped.iloc[-3, gapped.columns.get_loc("high")] = np.inf
    gapped.iloc[-4, gapped.columns.get_loc("low")] = -np.inf
    fib = analysis.fib_from_swing(gapped, 30)
    assert np.isfinite(fib["p1"].price) and np.isfinite(fib["p2"].price)
    # A recursive indicator passes over it and carries its state, as over a NaN.
    x = pd.Series([1.0, 2.0, 3.0, np.inf, 4.0, 5.0])
    np.testing.assert_allclose(ta.ema(x, 3).dropna().to_numpy(),
                               ta.ema(x.replace(np.inf, np.nan), 3).dropna().to_numpy())
    assert np.isnan(ta.ema(x, 3).iloc[3])
    assert ta.atr(gapped, 14).iloc[-1] == pytest.approx(ta.atr(gapped.replace([np.inf, -np.inf], np.nan), 14).iloc[-1])


def test_regression_channel_on_a_line_ending_at_zero_has_no_percent_slope():
    df = _daily(40)
    df["close"] = 0.0
    assert analysis.regression_channel(df, 20)["info"]["slope_pct_per_bar"] is None


def test_vol_cone_refuses_an_origin_that_is_not_positive():
    for price in (0, -5):
        with pytest.raises(ChartError, match=r"analysis\.vol_cone\(price=...\): .*positive price"):
            analysis.vol_cone(_daily(60), 10, price=price)


def test_measured_move_from_a_zero_origin_has_no_percent():
    df = _daily(20)
    mm = analysis.measured_move(df, (df.index[2], 10.0), (df.index[5], 12.0), (df.index[8], 0.0))
    assert mm["p2"].price == 2.0 and mm["info"]["pct"] is None
