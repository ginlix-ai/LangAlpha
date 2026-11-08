"""Tests for the TickerTick news source: query building + normalization."""

from __future__ import annotations

import httpx
import pytest

from src.data_client.tickertick.client import TickerTickClient, _convert_timestamps
from src.data_client.tickertick.news_source import (
    TickerTickNewsSource,
    _build_query,
    _normalize_story,
)

RAW_STORY = {
    "id": 8045485333109595178,
    "title": "Tesla sets a record",
    "url": "https://example.com/a",
    "site": "investing.com",
    "time": "2025-12-01T09:12:29+00:00",  # already ISO (client converts ms upstream)
    "favicon_url": "https://static.example.com/f.ico",
    "tags": ["tsla", "ev"],
    "tickers": ["tsla"],
    "description": "Full summary.",
}


class TestBuildQuery:
    def test_no_tickers_is_curated(self):
        assert _build_query(None) == "T:curated"
        assert _build_query([]) == "T:curated"

    def test_single_ticker_is_broad(self):
        assert _build_query(["AAPL"]) == "tt:aapl"

    def test_multi_ticker_is_or(self):
        assert _build_query(["AAPL", "MSFT"]) == "(or tt:aapl tt:msft)"

    def test_injection_tokens_are_dropped(self):
        # Tokens carrying DSL operators are ignored, not interpolated into the query.
        assert _build_query(["AAPL) (or s:cnn"]) == "T:curated"
        assert _build_query(["AAPL) x", "MSFT"]) == "tt:msft"
        # Legitimate dotted/hyphenated tickers still pass through.
        assert _build_query(["BRK.B", "BF-B"]) == "(or tt:brk.b tt:bf-b)"


class TestNormalize:
    def test_field_mapping(self):
        n = _normalize_story(RAW_STORY)
        assert n["id"] == "8045485333109595178"  # numeric id stringified
        assert n["title"] == "Tesla sets a record"
        assert n["article_url"] == "https://example.com/a"
        assert n["published_at"] == "2025-12-01T09:12:29+00:00"
        assert n["description"] == "Full summary."
        assert n["source"] == {
            "name": "investing.com",
            "logo_url": None,
            "homepage_url": None,
            "favicon_url": "https://static.example.com/f.ico",
        }
        assert n["tickers"] == ["TSLA"]  # upper-cased
        assert n["keywords"] == ["tsla", "ev"]  # tags → keywords

    def test_no_image_or_sentiment(self):
        n = _normalize_story(RAW_STORY)
        assert n["image_url"] is None
        assert n["sentiments"] is None

    def test_missing_optional_fields(self):
        n = _normalize_story({"id": 1, "title": "x"})
        assert n["id"] == "1"
        assert n["description"] is None
        assert n["tickers"] == []
        assert n["keywords"] == []
        assert n["source"]["favicon_url"] is None


class TestConvertTimestamps:
    def test_ms_to_iso(self):
        out = _convert_timestamps({"stories": [{"time": 1764580349000}]})
        assert out["stories"][0]["time"].startswith("2025-12-01T")

    def test_already_iso_untouched(self):
        out = _convert_timestamps({"stories": [{"time": "2025-12-01T00:00:00+00:00"}]})
        assert out["stories"][0]["time"] == "2025-12-01T00:00:00+00:00"


class TestGetNews:
    @pytest.mark.asyncio
    async def test_get_news_curated(self, monkeypatch):
        captured: dict = {}

        async def fake_get_feed(self, query, limit=30, last_id=None):
            captured["query"] = query
            captured["limit"] = limit
            return {"stories": [RAW_STORY]}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )

        result = await TickerTickNewsSource().get_news(limit=10)
        assert captured["query"] == "T:curated"
        assert captured["limit"] == 10
        assert result["count"] == 1
        assert result["next_cursor"] is None
        assert result["results"][0]["id"] == "8045485333109595178"

    @pytest.mark.asyncio
    async def test_get_news_multi_ticker(self, monkeypatch):
        captured: dict = {}

        async def fake_get_feed(self, query, limit=30, last_id=None):
            captured["query"] = query
            return {"stories": []}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )

        await TickerTickNewsSource().get_news(tickers=["AAPL", "MSFT"], limit=5)
        assert captured["query"] == "(or tt:aapl tt:msft)"

    @pytest.mark.asyncio
    async def test_full_page_sets_next_cursor(self, monkeypatch):
        """A full page hands back the last story id as the pagination cursor."""

        async def fake_get_feed(self, query, limit=30, last_id=None):
            stories = [{**RAW_STORY, "id": 100 + i} for i in range(limit)]
            return {"stories": stories}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )
        result = await TickerTickNewsSource().get_news(limit=3)
        assert result["count"] == 3
        assert result["next_cursor"] == "102"  # last of ids 100,101,102 stringified

    @pytest.mark.asyncio
    async def test_cursor_forwarded_as_last_id(self, monkeypatch):
        """A provided cursor is sent to TickerTick as ``last`` for the next page."""
        captured: dict = {}

        async def fake_get_feed(self, query, limit=30, last_id=None):
            captured["last_id"] = last_id
            return {"stories": []}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )
        await TickerTickNewsSource().get_news(limit=5, cursor="abc-cursor")
        assert captured["last_id"] == "abc-cursor"


class TestDateWindow:
    """published_after/published_before cut a newest-first feed paged with `last`."""

    @staticmethod
    def _feed(monkeypatch, hours_ago: list[int]):
        """Install a fake feed of stories `hours_ago` (newest first); returns the call log."""
        calls: list[str | None] = []
        stories = [
            {
                **RAW_STORY,
                "id": 1000 + i,
                "time": f"2026-03-10T{23 - h:02d}:00:00+00:00",
            }
            for i, h in enumerate(hours_ago)
        ]

        async def fake_get_feed(self, query, limit=30, last_id=None):
            calls.append(last_id)
            start = 0
            if last_id:
                start = (
                    next(i for i, s in enumerate(stories) if str(s["id"]) == last_id)
                    + 1
                )
            return {"stories": stories[start : start + limit]}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )
        return calls

    @pytest.mark.asyncio
    async def test_window_keeps_only_in_range_stories_and_stops_at_its_start(
        self, monkeypatch
    ):
        calls = self._feed(monkeypatch, [0, 1, 2, 3, 4, 5, 6, 7])
        result = await TickerTickNewsSource().get_news(
            limit=10,
            published_after="2026-03-10T18:00:00Z",
            published_before="2026-03-10T21:00:00Z",
        )
        assert [r["published_at"][11:13] for r in result["results"]] == [
            "21",
            "20",
            "19",
            "18",
        ]
        assert result["count"] == 4
        assert result["next_cursor"] is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_scan_stops_without_fetching_another_page_past_the_window(
        self, monkeypatch
    ):
        from src.data_client.tickertick import news_source

        calls = self._feed(
            monkeypatch, [0] * 5 + [10] * (news_source._WINDOW_PAGE_SIZE + 15)
        )
        result = await TickerTickNewsSource().get_news(
            limit=50, published_after="2026-03-10T20:00:00Z"
        )
        assert result["count"] == 5
        assert result["next_cursor"] is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_window_cursor_is_the_last_returned_story_when_limit_fills(
        self, monkeypatch
    ):
        self._feed(monkeypatch, [0, 1, 2, 3, 4, 5])
        result = await TickerTickNewsSource().get_news(
            limit=2, published_after="2026-03-10T00:00:00Z"
        )
        assert [r["id"] for r in result["results"]] == ["1000", "1001"]
        assert result["next_cursor"] == "1001"

    @pytest.mark.asyncio
    async def test_scan_budget_hands_back_a_cursor_inside_a_deep_window(
        self, monkeypatch
    ):
        from src.data_client.tickertick import news_source

        scanned = news_source._WINDOW_PAGE_SIZE * news_source._WINDOW_MAX_PAGES
        calls = self._feed(monkeypatch, [0] * (scanned + 50))
        result = await TickerTickNewsSource().get_news(
            limit=5, published_before="2026-03-10T00:00:00Z"
        )
        assert result["results"] == []
        assert len(calls) == news_source._WINDOW_MAX_PAGES
        assert result["next_cursor"] == str(1000 + scanned - 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bound", ["yesterday", "2026-13-01"])
    async def test_malformed_bound_is_ignored(self, monkeypatch, bound):
        self._feed(monkeypatch, [0, 1])
        result = await TickerTickNewsSource().get_news(
            limit=5, published_after=bound, published_before=bound
        )
        assert result["count"] == 2
        assert result["next_cursor"] is None

    @pytest.mark.asyncio
    async def test_failed_later_page_returns_what_was_gathered(self, monkeypatch):
        from src.data_client.tickertick import news_source

        size = news_source._WINDOW_PAGE_SIZE
        stories = [
            {**RAW_STORY, "id": 1000 + i, "time": "2026-03-10T12:00:00+00:00"}
            for i in range(size)
        ]
        calls: list[str | None] = []

        async def fake_get_feed(self, query, limit=30, last_id=None):
            calls.append(last_id)
            if last_id:
                raise Exception("TickerTick request failed")
            return {"stories": stories}

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )
        result = await TickerTickNewsSource().get_news(
            limit=size + 1, published_after="2026-03-10T00:00:00Z"
        )
        assert result["count"] == size
        assert result["next_cursor"] == str(1000 + size - 1)
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_failed_first_page_raises(self, monkeypatch):
        async def fake_get_feed(self, query, limit=30, last_id=None):
            raise Exception("TickerTick request failed")

        monkeypatch.setattr(
            "src.data_client.tickertick.news_source.TickerTickClient.get_feed",
            fake_get_feed,
        )
        with pytest.raises(Exception, match="TickerTick request failed"):
            await TickerTickNewsSource().get_news(
                limit=5, published_after="2026-03-10T00:00:00Z"
            )

    @pytest.mark.asyncio
    async def test_short_page_ends_the_window(self, monkeypatch):
        self._feed(monkeypatch, [0, 1])
        result = await TickerTickNewsSource().get_news(
            limit=5, published_after="2026-03-10T00:00:00Z"
        )
        assert result["count"] == 2
        assert result["next_cursor"] is None


class TestGetFeedErrors:
    """The HTTP client wraps transport/status failures in a clear Exception."""

    @pytest.mark.asyncio
    async def test_http_status_error_wrapped(self, monkeypatch):
        async def fake_get(self, url, params=None):
            return httpx.Response(500, request=httpx.Request("GET", url))

        monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
        async with TickerTickClient() as client:
            with pytest.raises(Exception, match="TickerTick request failed"):
                await client.get_feed("T:curated")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exc", "match"),
        [
            (httpx.TimeoutException("slow"), "TickerTick request timed out"),
            (httpx.ConnectError("down"), "TickerTick request failed"),
        ],
    )
    async def test_transport_error_wrapped(self, monkeypatch, exc, match):
        async def boom(self, url, params=None):
            raise exc

        monkeypatch.setattr("httpx.AsyncClient.get", boom)
        async with TickerTickClient() as client:
            with pytest.raises(Exception, match=match):
                await client.get_feed("T:curated")
