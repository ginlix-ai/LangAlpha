"""Portfolio API Router — CRUD endpoints for /api/v1/users/me/portfolio."""

import logging

from fastapi import APIRouter
from fastapi.responses import Response

from src.server.database.portfolio import (
    delete_portfolio_holding as db_delete_portfolio_holding,
    get_portfolio_holding as db_get_portfolio_holding,
    get_user_portfolio as db_get_user_portfolio,
    update_portfolio_holding as db_update_portfolio_holding,
    upsert_portfolio_holding as db_upsert_portfolio_holding,
)
from src.server.services.onboarding import maybe_complete_onboarding
from src.server.models.user import (
    PortfolioHoldingCreate,
    PortfolioHoldingResponse,
    PortfolioHoldingUpdate,
    PortfolioResponse,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions, raise_not_found

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/users/me/portfolio", tags=["Portfolio"])


@router.get("", response_model=PortfolioResponse)
@handle_api_exceptions("list portfolio", logger)
async def list_portfolio(user_id: CurrentUserId):
    holdings = await db_get_user_portfolio(user_id)

    return PortfolioResponse(
        holdings=[PortfolioHoldingResponse.model_validate(h) for h in holdings],
        total=len(holdings),
    )


@router.post("", response_model=PortfolioHoldingResponse, status_code=201)
@handle_api_exceptions("add portfolio holding", logger)
async def add_portfolio_holding(
    request: PortfolioHoldingCreate,
    user_id: CurrentUserId,
    response: Response,
):
    """
    Add a holding to the portfolio. If the same symbol + instrument_type + account_name
    already exists, merges the position (sums quantity, computes weighted average cost).

    Args:
        request: Portfolio holding data
        user_id: User ID from authentication header
        response: FastAPI response for setting status code

    Returns:
        Created or merged portfolio holding (201 for new, 200 for merged)
    """
    holding, merge_details = await db_upsert_portfolio_holding(
        user_id=user_id,
        symbol=request.symbol,
        instrument_type=request.instrument_type,
        quantity=request.quantity,
        exchange=request.exchange,
        name=request.name,
        average_cost=request.average_cost,
        currency=request.currency,
        account_name=request.account_name,
        notes=request.notes,
        metadata=request.metadata,
        first_purchased_at=request.first_purchased_at,
    )

    await maybe_complete_onboarding(user_id)

    if merge_details:
        response.status_code = 200
        logger.info(f"Merged portfolio holding {holding['user_portfolio_id']} for user {user_id}")
    else:
        logger.info(f"Added portfolio holding {holding['user_portfolio_id']} for user {user_id}")

    return PortfolioHoldingResponse.model_validate(holding)


@router.get("/{holding_id}", response_model=PortfolioHoldingResponse)
@handle_api_exceptions("get portfolio holding", logger)
async def get_portfolio_holding(
    holding_id: str,
    user_id: CurrentUserId,
):
    """Get a single holding. 404 if not found or not owned by the caller."""
    holding = await db_get_portfolio_holding(holding_id, user_id)

    if not holding:
        raise_not_found("Portfolio holding")

    return PortfolioHoldingResponse.model_validate(holding)


@router.put("/{holding_id}", response_model=PortfolioHoldingResponse)
@handle_api_exceptions("update portfolio holding", logger)
async def update_portfolio_holding(
    holding_id: str,
    request: PortfolioHoldingUpdate,
    user_id: CurrentUserId,
):
    """Partial update. 404 if not found or not owned by the caller."""
    holding = await db_update_portfolio_holding(
        user_portfolio_id=holding_id,
        user_id=user_id,
        name=request.name,
        quantity=request.quantity,
        average_cost=request.average_cost,
        currency=request.currency,
        account_name=request.account_name,
        notes=request.notes,
        metadata=request.metadata,
        first_purchased_at=request.first_purchased_at,
    )

    if not holding:
        raise_not_found("Portfolio holding")

    logger.info(f"Updated portfolio holding {holding_id} for user {user_id}")
    return PortfolioHoldingResponse.model_validate(holding)


@router.delete("/{holding_id}", status_code=204)
@handle_api_exceptions("delete portfolio holding", logger)
async def delete_portfolio_holding(
    holding_id: str,
    user_id: CurrentUserId,
):
    """Delete a holding. 404 if not found or not owned by the caller."""
    deleted = await db_delete_portfolio_holding(holding_id, user_id)

    if not deleted:
        raise_not_found("Portfolio holding")

    logger.info(f"Deleted portfolio holding {holding_id} for user {user_id}")
    return Response(status_code=204)
