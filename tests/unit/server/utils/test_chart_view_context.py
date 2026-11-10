"""The chart views a request carries, as its turn's jobs get them: the latest
view of each chart (SYMBOL:timeframe), at most eight, as compact JSON in
``PTC_CHART_VIEWS``."""

import json

from chartkit.protocol import CHART_VIEWS_ENV
from src.server.models.chat import ChatRequest
from src.server.utils.chart_view_context import chart_views_env


def _view(symbol: str = "NVDA", timeframe: str = "1day", **extra) -> dict:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "theme": "dark",
        "from": "2026-01-02",
        "to": "2026-10-30",
        "width": 1200,
        "height": 600,
        **extra,
    }


def _request(*views: dict) -> ChatRequest:
    return ChatRequest(
        workspace_id="w",
        messages=[{"role": "user", "content": "hi"}],
        chart_views=list(views),
    )


def _views(*views: dict) -> list[dict]:
    env = chart_views_env(_request(*views).chart_views)
    assert list(env) == [CHART_VIEWS_ENV]
    return json.loads(env[CHART_VIEWS_ENV])


class TestChartViewsEnv:
    def test_the_latest_view_of_each_chart_in_the_order_last_seen(self):
        views = _views(
            _view("NVDA", theme="dark"),
            _view("AAPL"),
            _view("nvda", theme="light"),
        )

        assert [(v["symbol"], v["theme"]) for v in views] == [
            ("AAPL", "dark"),
            ("nvda", "light"),
        ]
        assert views[0] == _view("AAPL")

    def test_compact_json(self):
        env = chart_views_env(_request(_view()).chart_views)
        assert " " not in env[CHART_VIEWS_ENV]

    def test_the_language_is_passed_when_sent(self):
        assert _views(_view(locale="zh-CN"))[0]["locale"] == "zh-CN"
        assert "locale" not in _views(_view())[0]

    def test_the_size_is_passed_as_sent(self):
        """chartkit bounds it, where the variable is read."""
        assert _views(_view(width=9000, height=50))[0]["width"] == 9000

    def test_one_chart_per_interval(self):
        assert len(_views(_view("NVDA", "1day"), _view("NVDA", "5min"))) == 2

    def test_one_chart_per_listing_whichever_spelling(self):
        """Keyed as the chart is (``make_chart_id``), so two spellings of one
        listing are one chart."""
        views = _views(_view("600519.SS"), _view("600519.sh", theme="light"))
        assert [(v["symbol"], v["theme"]) for v in views] == [("600519.sh", "light")]

    def test_at_most_the_latest_eight(self):
        views = _views(*(_view(f"S{i}") for i in range(12)))
        assert [v["symbol"] for v in views] == [f"S{i}" for i in range(4, 12)]

    def test_nothing_without_views(self):
        assert chart_views_env(None) == {}
        assert chart_views_env(_request().chart_views) == {}
