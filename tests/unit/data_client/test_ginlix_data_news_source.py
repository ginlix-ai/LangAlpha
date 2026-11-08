"""Multi-ticker merge on the ginlix-data news source."""

from __future__ import annotations

from typing import Any

import pytest

from src.data_client.ginlix_data.news_source import GinlixDataNewsSource


def _raw(article_id: str, hour: int) -> dict[str, Any]:
    return {
        "id": article_id,
        "title": f"headline {article_id}",
        "published_utc": f"2026-03-02T{hour:02d}:00:00Z",
    }


class _Client:
    """Per-ticker feeds (newest first) that honor `limit` and `order` like the real endpoint."""

    def __init__(self, feeds: dict[str, list[dict[str, Any]]]) -> None:
        self.feeds = feeds
        self.calls: list[dict[str, Any]] = []

    async def get_news(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        feed = self.feeds[kwargs["ticker"]]
        if kwargs.get("order") == "asc":
            feed = feed[::-1]
        return {"results": feed[: kwargs["limit"]]}


@pytest.mark.asyncio
async def test_merged_newest_n_is_exact_when_one_ticker_dominates():
    feeds = {
        "NVDA": [_raw(f"n{h}", h) for h in range(23, 13, -1)],
        "AAPL": [_raw(f"a{h}", h) for h in range(12, 2, -1)],
    }
    client = _Client(feeds)

    out = await GinlixDataNewsSource(client).get_news(tickers=["AAPL", "NVDA"], limit=6)

    assert [a["id"] for a in out["results"]] == [f"n{h}" for h in range(23, 17, -1)]
    assert out["count"] == 6
    assert out["next_cursor"] is None


@pytest.mark.asyncio
async def test_ascending_order_returns_the_exact_oldest_n():
    feeds = {
        "NVDA": [_raw(f"n{h}", h) for h in range(23, 13, -1)],
        "AAPL": [_raw(f"a{h}", h) for h in range(12, 2, -1)],
    }
    client = _Client(feeds)

    out = await GinlixDataNewsSource(client).get_news(
        tickers=["AAPL", "NVDA"], limit=6, order="asc", sort="published_utc"
    )

    assert [a["id"] for a in out["results"]] == [f"a{h}" for h in range(3, 9)]
    assert all(
        c["order"] == "asc" and c["sort"] == "published_utc" for c in client.calls
    )


@pytest.mark.asyncio
async def test_story_shared_by_two_tickers_is_returned_once():
    shared = _raw("shared", 20)
    feeds = {
        "AAPL": [shared, _raw("a1", 10)],
        "NVDA": [shared, _raw("n1", 11)],
    }

    out = await GinlixDataNewsSource(_Client(feeds)).get_news(
        tickers=["AAPL", "NVDA"], limit=4
    )

    assert [a["id"] for a in out["results"]] == ["shared", "n1", "a1"]


@pytest.mark.asyncio
async def test_window_bounds_reach_every_ticker_request():
    client = _Client({"AAPL": [], "NVDA": []})

    await GinlixDataNewsSource(client).get_news(
        tickers=["AAPL", "NVDA"],
        limit=7,
        published_after="2026-03-01T00:00:00Z",
        published_before="2026-03-02T00:00:00Z",
    )

    assert len(client.calls) == 2
    for call in client.calls:
        assert call["limit"] == 7
        assert call["published_after"] == "2026-03-01T00:00:00Z"
        assert call["published_before"] == "2026-03-02T00:00:00Z"
