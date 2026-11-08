"""Article-by-id lookup on the CN news feed over ginlix-data's window."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from src.data_client.ginlix_data import cn_financial
from src.data_client.ginlix_data.cn_financial import GinlixDataCnNewsSource


def _article(i: int) -> dict[str, Any]:
    return {"id": f"ts-{i:04d}", "title": f"headline {i}",
            "published_at": f"2026-03-02T{i % 24:02d}:00:00+08:00"}


class _Client:
    def __init__(
        self, window: list[dict[str, Any]], listed: dict[str, dict[str, Any]] | None = None
    ) -> None:
        self.window = window
        self.listed = listed or {}
        self.fetches = 0

    async def get_news_v2(self, *, market: str, limit: int = 20) -> list[dict[str, Any]]:
        self.fetches += 1
        return list(self.window)

    async def get_instruments_v2(self, keys: list[str]) -> dict[str, dict[str, Any]]:
        return {k: self.listed[k] for k in keys if k in self.listed}


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
    # The CN feed's articles are market-wide and untagged, so articles that
    # never name the ticker stay out rather than fill its feed.
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


_LISTED = {
    "600519.XSHG": {"name_local": "某某酒业", "industry": "白酒"},
    "600000.XSHG": {"name_local": "某某银行", "industry": "银行"},
    "600030.XSHG": {"name_local": "某某证券", "industry": "证券"},
    "300024.XSHE": {"name_local": "机器人", "industry": "专用机械"},
}


@pytest.mark.asyncio
@pytest.mark.parametrize(("ticker", "title", "description", "named"), [
    ("600519.ss", "板块走强", "某某酒业涨3%，带动指数", True),
    ("600519.SH", "公告速递", "某某酒业（600519）发布公告", True),
    ("600519.SH", "公告速递", "代码SH600519、600519.SH收涨", True),
    ("600519.SH", "增持", "股东拟增持600519股", False),
    ("600519.SH", "指数", "上证某指数(600519)收涨", False),
    ("600519.SH", "跨市场", "600519.SZ收涨", False),
    ("600519.SH", "链接", '<a href="//q/600519.SH/" title="某某酒业">点此</a>查看', False),
    ("600000.SH", "行业", "某某银行业协会发布数据，某某银行间市场活跃", False),
    ("600000.SH", "业绩", "某某银行发布三季报", True),
    ("600000.SH", "财报", "某某银行业绩增长，某某银行保持增长", True),
    ("600519.SH", "回购", "某某酒业集中竞价回购股份，某某酒业业务收入增长", True),
    ("600519.SH", "选股", "PE<20倍的标的中，某某酒业领涨", True),
    ("600030.SH", "某某证券：电商板块估值处于低位", "", False),
    ("600030.SH", "市场观察", "某某证券三季度净利增长", False),
    ("600030.SH", "某某证券三季度净利增长", "", True),
    ("600030.SH", "某某证券研究部：看好白酒", "", False),
    ("600030.SH", "某某证券预计明年A股盈利改善", "", False),
    ("600030.SH", "某某证券最新研报：看好白酒板块", "", False),
    ("300024.SZ", "机器人公司发布新品", "", True),
    ("300024.SZ", "人工智能", "工业机器人订单增长", False),
    ("300024.SZ", "公告", "机器人（300024）中标", True),
])
async def test_a_ticker_request_matches_articles_naming_the_ticker(ticker, title, description, named):
    window = [{**_article(0), "title": title, "description": description}]
    source = GinlixDataCnNewsSource(_Client(window, _LISTED))

    page = await source.get_news(tickers=[ticker])

    assert page["count"] == int(named)


@pytest.mark.asyncio
async def test_a_ticker_the_directory_does_not_list_matches_its_tagged_code_only():
    window = [
        {**_article(0), "title": "公告", "description": "某公司（600519）发布公告"},
        {**_article(1), "title": "收盘", "description": "600519.SH收涨"},
    ]
    source = GinlixDataCnNewsSource(_Client(window))

    page = await source.get_news(tickers=["600519.SH"])

    assert [a["id"] for a in page["results"]] == ["ts-0001"]


@pytest.mark.asyncio
async def test_a_failed_name_lookup_fails_the_ticker_request():
    class _DirectoryDown(_Client):
        async def get_instruments_v2(self, keys: list[str]) -> dict[str, dict[str, Any]]:
            raise httpx.ConnectError("directory unreachable")

    source = GinlixDataCnNewsSource(_DirectoryDown([_article(0)]))

    with pytest.raises(httpx.ConnectError):
        await source.get_news(tickers=["600519.SH"])


@pytest.mark.asyncio
async def test_ascending_order_pages_from_the_oldest_article():
    # The window is newest first, so its last article is the oldest.
    window = [_article(i) for i in range(5)]
    source = GinlixDataCnNewsSource(_Client(window))

    first = await source.get_news(limit=2, order="asc", sort="published_utc")
    assert [a["id"] for a in first["results"]] == ["ts-0004", "ts-0003"]
    second = await source.get_news(limit=2, order="asc", cursor=first["next_cursor"])
    assert [a["id"] for a in second["results"]] == ["ts-0002", "ts-0001"]
