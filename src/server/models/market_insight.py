"""Pydantic response models for market insights."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class InsightTopic(BaseModel):
    text: str
    trend: str  # "up", "down", "neutral"


class InsightNewsItem(BaseModel):
    title: str
    body: str
    url: Optional[str] = None


class InsightOutputSchema(BaseModel):
    """Schema for structured extraction from the research agent's output."""

    headline: str = Field(description="Concise headline capturing the dominant market theme (max 120 chars)")
    summary: str = Field(description="2-3 sentence overview of the most important developments")
    news_items: List[InsightNewsItem] = Field(description="Curated list of significant news stories")
    topics: List[InsightTopic] = Field(description="3-5 key topic tags with trend direction")


class MarketInsightLatestResponse(BaseModel):
    market_insight_id: str
    type: str
    status: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    topics: Optional[List[InsightTopic]] = None
    model: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class MarketInsightListResponse(BaseModel):
    insights: List[MarketInsightLatestResponse]
    count: int


class MarketInsightDetailResponse(MarketInsightLatestResponse):
    content: Optional[List[InsightNewsItem]] = None
    sources: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
