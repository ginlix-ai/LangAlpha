"""chartkit snapshots' settled contract, short of a browser: the render page ships with chartkit,
a computer without it degrades to a printed line (never an exception that would skip a publish),
the routed API answers the chart's requests in the backend's shapes from the market-data tool (the
agent's DataFrame only when the tool fails), and the user's chart view sets the look and the window.

The render itself is proven against the live page in a real sandbox (tmp/chart-script/snapshot_drv).
"""

from __future__ import annotations

import dataclasses
import json

import numpy as np
import pandas as pd
import pytest

from chartkit import Chart, ChartError, snapshot
from chartkit import protocol, _renderer, _report, _view
from chartkit._bars_source import ChartBars, bar_rows
from chartkit._renderer import SnapshotJob, SnapshotUnavailable, api



def _daily(n: int = 30) -> pd.DataFrame:
    idx = pd.bdate_range("2026-03-02", periods=n)
    close = 100 + np.arange(n) * 0.5
    df = pd.DataFrame(
        {"open": close - 0.5, "high": close + 1, "low": close - 1, "close": close, "volume": 1e6}, index=idx
    )
    df.attrs.update(symbol="NVDA", timeframe="1day")
    return df


def test_without_a_renderer_snapshot_says_so_and_publish_still_runs(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(_renderer, "RENDER_DIR", tmp_path / "missing")
    monkeypatch.setenv(protocol.CHART_FILE_ENV, str(tmp_path / "charts.jsonl"))
    monkeypatch.setenv(protocol.SNAPSHOT_DIR_ENV, str(tmp_path / "charts"))
    c = Chart(_daily(), title="NVDA check")
    c.level(110, "R")
    r = c.snapshot()
    assert r["path"] is None
    assert capsys.readouterr().out.strip() == r["report"]
    assert "\n" not in r["report"] and "no chart renderer" in r["report"]
    c.publish()
    assert (tmp_path / "charts.jsonl").read_text().count("\n") == 1
    assert snapshot(_daily())["path"] is None


def test_snapshot_arguments_fail_like_any_chartkit_call(tmp_path):
    c = Chart(_daily(), title="NVDA check")
    with pytest.raises(ChartError, match=r"c\.snapshot\(theme=\.\.\.\)"):
        c.snapshot(tmp_path / "x.png", theme="sepia")
    with pytest.raises(ChartError, match=r"c\.snapshot\(path=\.\.\.\): must end in \.png"):
        c.snapshot(tmp_path / "x.jpg")
    bare = _daily()
    bare.attrs.clear()
    with pytest.raises(ChartError, match=r"chartkit\.snapshot\(symbol=\.\.\.\)"):
        snapshot(bare)


def _job(layer=None, tool=None, df=None, tf="1day"):
    df = _daily() if df is None else df
    rows = bar_rows(df, df.index, "America/New_York")
    bars = ChartBars("NVDA", tf, "America/New_York", rows,
                     tool=tool or (lambda **kw: {"error": "down", "detail": "x"}))
    return SnapshotJob(symbol="NVDA", timeframe=tf, tz="America/New_York", layer=layer, path="/tmp/x.png",
                       theme="light", width=740, height=762, bars=bars, frame=_view.Frame("default"))


def _tool_answer(dates, close=200.0, **extra):
    return {"symbol": "NVDA", "interval": "1min", "currency": "USD", "timezone": "America/New_York",
            "source": "ginlix-data", "data": [{"date": d, "open": close, "high": close + 1, "low": close - 1,
                                                "close": close, "volume": 1234.9} for d in dates], **extra}


def test_bars_route_answers_from_the_market_data_tool_for_the_window_the_chart_asks():
    asked = []

    def tool(**kwargs):
        asked.append(kwargs)
        return _tool_answer(["2026-10-30 04:00:00", "2026-10-30 19:59:00"])

    job = _job(tool=tool, tf="1min")
    status, body = api(job, "/api/v1/market-data/intraday/stocks/NVDA",
                       {"interval": ["1min"], "from": ["2026-10-28"], "to": ["2026-10-31"]}, [])
    assert status == 200 and asked == [
        {"symbol": "NVDA", "interval": "1min", "start_date": "2026-10-28", "end_date": "2026-10-31"}]
    assert [r["time"] for r in body["data"]] == [
        int(pd.Timestamp("2026-10-30 04:00", tz="America/New_York").value // 1_000_000),
        int(pd.Timestamp("2026-10-30 19:59", tz="America/New_York").value // 1_000_000),
    ]
    # Whole shares, as the backend sends volume; the tool's currency; a long-settled tape reads closed.
    assert body["data"][0]["volume"] == 1234 and body["currency"] == "USD"
    assert body["cache"]["market_phase"] == "closed" and body["interval"] == "1min"
    assert job.bars.mode == "tool" and job.bars.source == "ginlix-data"


def test_a_failed_tool_falls_back_to_the_dataframe_and_a_later_miss_loads_nothing():
    job = _job()
    status, body = api(job, "/api/v1/market-data/daily/stocks/NVDA", {"to": ["2026-03-10"]}, [])
    assert status == 200 and len(body["data"]) == 7  # every df bar up to `to`
    assert set(body["data"][0]) == {"time", "open", "high", "low", "close", "volume"}
    assert job.bars.mode == "df" and job.bars.failure == "down: x"
    assert body["cache"]["market_phase"] is None
    # An earlier page before the first bar finds nothing new.
    assert api(job, "/api/v1/market-data/daily/stocks/NVDA", {"to": ["2026-02-27"]}, [])[1]["data"] == []

    answers = iter([_tool_answer(["2026-10-30"]), "boom"])

    def flaky(**kw):
        got = next(answers)
        if got == "boom":
            raise RuntimeError("connection reset\n<html>")
        return got

    job = _job(tool=flaky)
    assert len(api(job, "/api/v1/market-data/daily/stocks/NVDA", {}, [])[1]["data"]) == 1
    assert api(job, "/api/v1/market-data/daily/stocks/NVDA", {"from": ["2025-01-01"], "to": ["2025-12-31"]},
               [])[1]["data"] == []
    assert job.bars.mode == "tool"
    assert job.bars.misses == ["2025-01-01 to 2025-12-31 (RuntimeError: connection reset)"]


def test_the_users_view_sets_theme_and_size_unless_the_call_says_otherwise(monkeypatch):
    monkeypatch.setenv(protocol.CHART_VIEWS_ENV, json.dumps([
        {"type": "chart_view", "symbol": "NVDA", "timeframe": "1day", "theme": "light", "from": "2026-01-02",
         "to": "2026-03-01", "width": 900, "height": 500},
        {"type": "chart_view", "symbol": "goog", "timeframe": "1min", "theme": "dark", "from": "2026-10-30T04:00",
         "to": "2026-10-30T19:40", "width": 1180, "height": 760},
    ]))
    user, _ = _view.user_view("GOOG", "1min")
    look = _view.choose_look("c.snapshot", "GOOG", "1min", user, [], None, None, None)
    assert (look.theme, look.width, look.height) == ("dark", 1180, 760)
    assert look.line == "Matches the user's chart: dark, 1180x760, GOOG 1min."
    assert _view.choose_frame("c.snapshot", None, None, user, "1min", "America/New_York") == _view.Frame(
        "user", start="2026-10-30T04:00", end="2026-10-30T19:40")
    # With a layer, the layer's frame; an asked view wins over both.
    assert _view.choose_frame("c.snapshot", None, {"id": "x"}, user, "1min", "America/New_York") == _view.Frame(
        "layer", layer_id="x")
    assert _view.choose_frame("c.snapshot", "ytd", {"id": "x"}, user, "1min", "America/New_York") == _view.Frame(
        "preset", preset="YTD")
    with pytest.raises(ChartError, match=r"c\.snapshot\(view=\.\.\.\)"):
        _view.choose_frame("c.snapshot", "2W", None, None, "1day", "America/New_York")

    look = _view.choose_look("c.snapshot", "GOOG", "1min", user, [], "light", 1400, None)
    assert look.line == "The user's GOOG 1min chart is dark, 1180x760; drawn light 1400x760 as you asked."
    user, others = _view.user_view("AAPL", "1day")
    look = _view.choose_look("c.snapshot", "AAPL", "1day", user, others, None, None, None)
    assert user is None and (look.theme, look.width, look.height) == ("light", *_view.DEFAULT_SIZE)
    assert look.line == ("No chart view from the user for AAPL 1day (on their screen: NVDA 1day, GOOG 1min); "
                         "using light 740x762.")


def test_a_snapshot_draws_in_the_readers_language_from_any_chart_they_sent(monkeypatch, tmp_path):
    jobs = []

    def run(job):
        jobs.append(job)
        raise SnapshotUnavailable("test")

    monkeypatch.setattr(_renderer.RENDERER, "run", run)
    monkeypatch.setenv(protocol.SNAPSHOT_DIR_ENV, str(tmp_path))
    c = Chart(_daily(), title="NVDA check")
    c.snapshot()
    monkeypatch.setenv(protocol.CHART_VIEWS_ENV, json.dumps([
        {"type": "chart_view", "symbol": "AAPL", "timeframe": "1day", "locale": "zh-CN"}]))
    c.snapshot()  # NVDA has no view of its own; the language is still the reader's
    assert [j.locale for j in jobs] == ["en-US", "zh-CN"]


def test_a_view_matches_the_symbol_in_the_spelling_the_server_stores(monkeypatch):
    monkeypatch.setenv(protocol.CHART_VIEWS_ENV, json.dumps([
        {"type": "chart_view", "symbol": "0700.HK", "timeframe": "1day", "theme": "dark"},
        {"type": "chart_view", "symbol": "600519.ss", "timeframe": "1day"},
    ]))
    assert _view.user_view("700.HK", "1day")[0].theme == "dark"
    assert _view.user_view("600519.SH", "1day")[0].symbol == "600519.SH"
    assert _view.user_view("9988.HK", "1day")[1] == ["0700.HK 1day", "600519.SH 1day"]


def test_the_report_names_bars_the_dataframe_lacks():
    days = ["2026-10-29", "2026-10-30"]
    regular = pd.DatetimeIndex([f"{d} {h:02d}:{m:02d}" for d in days for h in range(9, 16) for m in range(60)
                                if (h, m) >= (9, 30)])
    df = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 100.0}, index=regular)
    chart_times = pd.DatetimeIndex([f"{d} {h:02d}:{m:02d}" for d in days for h in range(4, 20) for m in range(60)])
    chart = pd.Series(100.0, index=chart_times)
    lines = _report.compare("1min", df, regular, chart)
    assert lines[0] == ("Your DataFrame has no bars outside 09:30-15:59; the chart has 1,140 there on the same "
                        "days (04:00-09:29 before, 16:00-19:59 after).")
    assert lines[1] == "Your DataFrame ends 2026-10-30 15:59; the chart's last bar is 2026-10-30 19:59, 240 bars later."
    assert lines[-1].startswith("Compute on chartkit.bars(")
    assert _report.compare("1min", df, regular, chart[chart.index.isin(regular)]) == []


def test_chart_annotations_route_serves_the_draft_layer_as_a_stored_row():
    unanswered: list[str] = []
    status, body = api(_job({"id": "nvda-check", "title": "t", "symbol": "nvda"}),
                       "/api/v1/workspaces/snapshot/chart-annotations", {"symbol": ["NVDA"]}, unanswered)
    assert status == 200 and not unanswered
    (chart,) = body["charts"]
    assert chart["chart_id"] == "NVDA:1day"
    (row,) = chart["layers"]
    assert row["layer_id"] == "nvda-check" and row["layer"]["symbol"] == "NVDA"


def test_unknown_requests_are_flagged():
    unanswered: list[str] = []
    assert api(_job(), "/api/v1/market-data/stocks/NVDA/overview", {}, unanswered)[0] == 404
    assert unanswered == ["/api/v1/market-data/stocks/NVDA/overview"]
    # The live chart's delta poll is expected to miss: it falls back to the bars route.
    quiet: list[str] = []
    assert api(_job(), "/api/v1/market-data/bars/NVDA", {"schema": ["ohlcv-1d"]}, quiet)[0] == 404
    assert quiet == []


def test_the_report_names_what_the_picture_cuts_short_covers_or_judges():
    job = _job(layer={"id": "setup", "title": "Forward setup for the November earnings run"})
    rep = {
        "view": {}, "layers": [{"id": "setup", "title": "t"}], "droppedLabels": [], "offscreen": [], "notes": [],
        "cutText": [
            {"layer": "setup", "element": "Prior swing high resistance", "where": "axis",
             "text": "Prior swing high resistance", "shown": "Prior swing hig…"},
            {"layer": "setup", "element": "title", "where": "title",
             "text": "Forward setup for the November earnings run", "shown": "Forward setup for the Nov…"},
        ],
        "coveredLabels": [
            {"layer": "setup", "element": "fomc", "kind": "vline", "by": "chip", "byText": "Forward setup"},
            {"layer": "setup", "element": "note", "kind": "label", "by": "tag", "byText": "Close"},
        ],
        "markersOverCandles": [{"text": "Q3 earnings beat and raise"}],
        "forecasts": [
            {"layer": "setup", "element": "up", "label": "Retest high", "state": "hit", "at": "2026-10-13"},
            {"layer": "setup", "element": "moon", "label": None, "state": "missed", "at": "2026-10-12"},
            {"layer": "setup", "element": "later", "label": "Run", "state": "pending", "at": "2026-11-05T09:30"},
            {"layer": "setup", "element": "old", "label": "Base", "state": "unknown", "at": "2026-01-02"},
        ],
    }
    rendered = _renderer.Rendered(report=rep, view=None, framed="default", size=(740, 762), unanswered=[],
                                  page_errors=[])
    job = dataclasses.replace(job, look_line="Look.", counts="1 level")
    lines = _report.report(job, rendered).splitlines()
    assert ('Text cut short (2), shorten it: price-axis tag "Prior swing high resistance" shows "Prior swing hig…"; '
            'layer title "Forward setup for the November earnings run" shows "Forward setup for the Nov…"') in lines
    assert ("Labels under the chart's own text (2), move them: fomc (vline) under the layer title chip; "
            'note (label) under the price tag "Close"') in lines
    assert 'Marker text over candles (1), shorten it or use c.label: "Q3 earnings beat and raise"' in lines
    assert ('Forecasts (4): "Retest high" hit on 2026-10-13; moon missed (target 2026-10-12 passed); '
            '"Run" pending until 2026-11-05 09:30; "Base" not judged: the chart\'s bars start after its origin') in lines


def test_a_driver_started_for_a_failed_chromium_launch_is_stopped(monkeypatch):
    # Each later snapshot retries the launch; a driver left running per try would pile up.
    class Error(Exception):
        pass

    stopped = []

    class Driver:
        class chromium:
            @staticmethod
            def launch(args):
                raise Error("Executable doesn't exist at /ms-playwright/chromium")

        def stop(self):
            stopped.append(True)

    class SyncApi:
        @staticmethod
        def sync_playwright():
            class Starter:
                def start(self):
                    return Driver()
            return Starter()

    SyncApi.Error = Error
    monkeypatch.setattr(_renderer, "_playwright", lambda: SyncApi)
    with pytest.raises(SnapshotUnavailable, match="Chromium did not start"):
        _renderer._launch(None)
    assert stopped == [True]
