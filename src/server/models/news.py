"""Pydantic models for the news API."""

from __future__ import annotations

from pydantic import BaseModel


class NewsPublisher(BaseModel):
    name: str
    logo_url: str | None = None
    homepage_url: str | None = None
    favicon_url: str | None = None


class NewsSentiment(BaseModel):
    ticker: str
    sentiment: str | None = None
    reasoning: str | None = None


class NewsArticle(BaseModel):
    id: str
    title: str
    author: str | None = None
    description: str | None = None
    published_at: str  # ISO 8601
    article_url: str
    image_url: str | None = None
    source: NewsPublisher
    tickers: list[str] = []
    keywords: list[str] = []
    sentiments: list[NewsSentiment] | None = None


class NewsResponse(BaseModel):
    results: list[NewsArticle]
    count: int
    next_cursor: str | None = None


class NewsArticleCompact(BaseModel):
    id: str
    title: str
    published_at: str
    image_url: str | None = None
    article_url: str | None = None
    source: NewsPublisher
    tickers: list[str] = []
    has_sentiment: bool = False


class NewsCompactResponse(BaseModel):
    results: list[NewsArticleCompact]
    count: int
    next_cursor: str | None = None
