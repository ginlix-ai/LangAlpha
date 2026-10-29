"""yfinance news: search is the fallback when Ticker.news comes back empty."""

from unittest.mock import MagicMock, patch

from src.data_client.yfinance.news_source import _fetch_news

_SEARCH_ITEM = {
    "uuid": "abc",
    "title": "Headline",
    "publisher": "Wire",
    "link": "https://example.com/a",
    "providerPublishTime": 1700000000,
}


def _patched(ticker_news, search_news):
    ticker = MagicMock()
    ticker.news = ticker_news
    search = MagicMock()
    search.news = search_news
    return (
        patch("src.data_client.yfinance.yahoo.yf.Ticker", return_value=ticker),
        patch("src.data_client.yfinance.yahoo.yf.Search", return_value=search),
    )


def test_empty_ticker_news_falls_back_to_search():
    ticker_patch, search_patch = _patched([], [_SEARCH_ITEM])
    with ticker_patch, search_patch:
        result = _fetch_news(["AAPL"], limit=5)
    assert result["count"] == 1
    article = result["results"][0]
    assert article["id"] == "abc"
    assert article["title"] == "Headline"
    assert article["article_url"] == "https://example.com/a"
    assert article["source"]["name"] == "Wire"
    assert article["published_at"].startswith("2023-11-14")
    assert article["tickers"] == ["AAPL"]


def test_ticker_news_wins_over_search():
    primary = {"id": "p1", "content": {"title": "Primary"}}
    ticker_patch, search_patch = _patched([primary], [_SEARCH_ITEM])
    with ticker_patch, search_patch as search:
        result = _fetch_news(["AAPL"], limit=5)
    assert [a["id"] for a in result["results"]] == ["p1"]
    search.assert_not_called()


def test_a_failing_search_skips_the_symbol():
    ticker_patch, _ = _patched([], [])
    with ticker_patch, patch(
        "src.data_client.yfinance.yahoo.yf.Search", side_effect=Exception("down")
    ):
        assert _fetch_news(["AAPL"], limit=5)["count"] == 0
