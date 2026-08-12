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
