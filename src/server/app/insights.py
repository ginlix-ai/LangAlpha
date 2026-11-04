"""API routes for AI market insights."""

import logging

from fastapi import APIRouter, HTTPException

from src.server.database import market_insight as market_insight_db
from src.server.dependencies.usage_limits import enforce_credit_limit
from src.server.models.market_insight import (
    MarketInsightDetailResponse,
    MarketInsightListResponse,
)
from src.server.services.insight_service import (
    InsightAlreadyGeneratingError,
    InsightService,
)
from src.server.dependencies.principal import DataReadUserId
from src.server.utils.api import (
    CurrentUserId,
    handle_api_exceptions,
    raise_not_found,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Insights"])


@router.get("/insights/today", response_model=MarketInsightListResponse)
@handle_api_exceptions("get today's insights", logger)
async def get_todays_insights(user_id: DataReadUserId):
    rows = await market_insight_db.get_todays_market_insights(user_id=user_id)
    return {"insights": rows, "count": len(rows)}


@router.get(
    "/insights/{market_insight_id}",
    response_model=MarketInsightDetailResponse,
)
@handle_api_exceptions("get insight detail", logger)
async def get_insight_detail(market_insight_id: str, user_id: DataReadUserId):
    row = await market_insight_db.get_market_insight(market_insight_id)
    if not row:
        raise_not_found("Market Insight")

    # Ownership check: system insights (user_id IS NULL) are public,
    # per-user insights are private to the owner
    if row.get("user_id") is not None and str(row["user_id"]) != user_id:
        raise_not_found("Market Insight")

    return row


@router.post(
    "/insights/generate",
    response_model=MarketInsightDetailResponse,
    status_code=202,
)
@handle_api_exceptions("generate personalized insight", logger)
async def generate_personalized_insight(user_id: CurrentUserId):
    """Request personalized insight generation.

    Returns 202 immediately with the generating row.
    The actual agent work runs in the background.
    Poll GET /insights/{id} to check for completion.
    """
    await enforce_credit_limit(user_id)

    service = InsightService.get_instance()

    try:
        result = await service.generate_for_user(user_id)
    except InsightAlreadyGeneratingError as e:
        if e.existing_insight.get("retry"):
            raise HTTPException(status_code=409, detail="Please try again")
        # Return the existing in-progress row so frontend can poll it
        return e.existing_insight

    if not result:
        raise HTTPException(
            status_code=500,
            detail="Insight generation failed",
        )

    return result
