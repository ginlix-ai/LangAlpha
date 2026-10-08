"""Article-by-id lookup on the CN news feed over ginlix-data's window."""

from __future__ import annotations

from typing import Any

import pytest

from src.data_client.ginlix_data import cn_financial
from src.data_client.ginlix_data.cn_financial import GinlixDataCnNewsSource


def _article(i: int) -> dict[str, Any]:
    return {"id": f"ts-{i:04d}", "title": f"headline {i}",
            "published_at": f"2026-03-02T{i % 24:02d}:00:00+08:00"}


class _Client:
    def __init__(self, window: list[dict[str, Any]]) -> None:
        self.window = window
        self.fetches = 0

    async def get_news_v2(self, *, market: str, limit: int = 20) -> list[dict[str, Any]]:
        self.fetches += 1
        return list(self.window)


@pytest.mark.asyncio
async def test_an_article_deep_in_a_full_window_is_found():
    # The window is newest first and larger than the article buffer, so the
    # oldest articles are the ones a buffer would have evicted first.
    window = [_article(i) for i in range(800)]
    source = GinlixDataCnNewsSource(_Client(window))

    assert await source.get_news_article("ts-0000") == window[0]
    assert await source.get_news_article("ts-0799") == window[799]


@pytest.mark.asyncio
async def test_unknown_ids_reuse_one_window_fetch(monkeypatch):
    client = _Client([_article(i) for i in range(5)])
    source = GinlixDataCnNewsSource(client)

    for i in range(20):
        assert await source.get_news_article(f"ts-guess-{i}") is None
    assert client.fetches == 1

    # Past the hold, a lookup sees the current window again.
    monkeypatch.setattr(cn_financial, "_LOOKUP_WINDOW_TTL", 0.0)
    client.window = [_article(9)]
    assert await source.get_news_article("ts-0009") == _article(9)
    assert client.fetches == 2


@pytest.mark.asyncio
async def test_a_served_page_answers_without_a_fetch():
    client = _Client([_article(i) for i in range(5)])
    source = GinlixDataCnNewsSource(client)

    await source.get_news(limit=2)
    assert await source.get_news_article("ts-0001") == _article(1)
    assert client.fetches == 1


@pytest.mark.asyncio
async def test_a_ticker_request_never_gets_the_market_wide_feed():
    # ginlix-data's CN articles are market-wide and carry no ticker tags today,
    # so a ticker-scoped request comes back empty rather than mislabelled.
    untagged = [{**_article(i), "tickers": []} for i in range(5)]
    source = GinlixDataCnNewsSource(_Client(untagged))

    page = await source.get_news(tickers=["600519.SH"], limit=3)

    assert page == {"results": [], "count": 0, "next_cursor": None}


@pytest.mark.asyncio
async def test_a_ticker_request_matches_tags_in_any_spelling():
    window = [
        {**_article(0), "tickers": ["000001.SZ"]},
        {**_article(1), "tickers": ["600519.ss"]},
        {**_article(2), "tickers": []},
    ]
    source = GinlixDataCnNewsSource(_Client(window))

    page = await source.get_news(tickers=["600519.SH"])

    assert [a["id"] for a in page["results"]] == ["ts-0001"]


@pytest.mark.asyncio
async def test_ascending_order_pages_from_the_oldest_article():
    # The window is newest first, so its last article is the oldest.
    window = [_article(i) for i in range(5)]
    source = GinlixDataCnNewsSource(_Client(window))

    first = await source.get_news(limit=2, order="asc", sort="published_utc")
    assert [a["id"] for a in first["results"]] == ["ts-0004", "ts-0003"]
    second = await source.get_news(limit=2, order="asc", cursor=first["next_cursor"])
    assert [a["id"] for a in second["results"]] == ["ts-0002", "ts-0001"]
