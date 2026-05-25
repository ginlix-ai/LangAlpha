from __future__ import annotations

import pytest

from src.tools.market_data.adanos_sentiment import fetch_adanos_market_sentiment


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeAsyncClient:
    calls: list[dict] = []

    def __init__(self, *, timeout: float):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url: str, *, headers: dict, params: dict):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "params": params,
                "timeout": self.timeout,
            }
        )
        return _FakeResponse({"sentiment_score": 0.42, "mentions": 12})


@pytest.fixture(autouse=True)
def reset_fake_client():
    _FakeAsyncClient.calls = []


@pytest.mark.asyncio
async def test_fetch_adanos_stock_sentiment_calls_requested_sources(monkeypatch):
    monkeypatch.setenv("ADANOS_API_KEY", "test-key")
    monkeypatch.setattr(
        "src.tools.market_data.adanos_sentiment.httpx.AsyncClient",
        _FakeAsyncClient,
    )

    content, artifact = await fetch_adanos_market_sentiment(
        "aapl",
        sources=["reddit", "news"],
        days=5,
        end_date="2026-05-20",
        base_url="https://adanos.test",
    )

    assert "Adanos Market Sentiment: AAPL" in content
    assert "| reddit | 0.42 | 12 | ok |" in content
    assert artifact["type"] == "adanos_market_sentiment"
    assert artifact["symbol"] == "AAPL"
    assert artifact["sources"] == ["reddit", "news"]
    assert _FakeAsyncClient.calls[0]["url"] == "https://adanos.test/reddit/stocks/v1/stock/AAPL"
    assert _FakeAsyncClient.calls[0]["headers"] == {"X-API-Key": "test-key"}
    assert _FakeAsyncClient.calls[0]["params"] == {"days": 5, "to": "2026-05-20"}


@pytest.mark.asyncio
async def test_fetch_adanos_market_sentiment_uses_market_endpoint(monkeypatch):
    monkeypatch.setenv("ADANOS_API_KEY", "test-key")
    monkeypatch.setattr(
        "src.tools.market_data.adanos_sentiment.httpx.AsyncClient",
        _FakeAsyncClient,
    )

    content, artifact = await fetch_adanos_market_sentiment(
        mode="market",
        sources="polymarket",
        days=3,
        end_date="2026-05-20",
        base_url="https://adanos.test",
    )

    assert "Adanos Market Sentiment: Market" in content
    assert artifact["mode"] == "market"
    assert artifact["symbol"] is None
    assert _FakeAsyncClient.calls[0]["url"] == "https://adanos.test/polymarket/stocks/v1/market-sentiment"
    assert _FakeAsyncClient.calls[0]["params"] == {"days": 3, "to": "2026-05-20"}


@pytest.mark.asyncio
async def test_fetch_adanos_sentiment_requires_api_key(monkeypatch):
    monkeypatch.delenv("ADANOS_API_KEY", raising=False)

    with pytest.raises(ValueError, match="ADANOS_API_KEY is required"):
        await fetch_adanos_market_sentiment("MSFT", sources="x")


@pytest.mark.asyncio
async def test_fetch_adanos_sentiment_rejects_unknown_sources(monkeypatch):
    monkeypatch.setenv("ADANOS_API_KEY", "test-key")

    with pytest.raises(ValueError, match="Unsupported Adanos source"):
        await fetch_adanos_market_sentiment("MSFT", sources="crypto")
