"""
Portfolio API Router.

Provides REST endpoints for portfolio management.

Endpoints:
- GET /api/v1/users/me/portfolio - List all holdings
- POST /api/v1/users/me/portfolio - Add holding
- GET /api/v1/users/me/portfolio/{holding_id} - Get single holding
- PUT /api/v1/users/me/portfolio/{holding_id} - Update holding
- DELETE /api/v1/users/me/portfolio/{holding_id} - Remove holding
"""

import logging

from fastapi import APIRouter
from fastapi.responses import Response

from src.server.database.portfolio import (
    create_portfolio_holding as db_create_portfolio_holding,
    delete_portfolio_holding as db_delete_portfolio_holding,
    get_portfolio_holding as db_get_portfolio_holding,
    get_user_portfolio as db_get_user_portfolio,
    update_portfolio_holding as db_update_portfolio_holding,
)
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
    """
    List all portfolio holdings for the current user.

    Args:
        user_id: User ID from authentication header

    Returns:
        List of portfolio holdings with total count
    """
    holdings = await db_get_user_portfolio(user_id)

    return PortfolioResponse(
        holdings=[PortfolioHoldingResponse.model_validate(h) for h in holdings],
        total=len(holdings),
    )


@router.post("", response_model=PortfolioHoldingResponse, status_code=201)
@handle_api_exceptions("add portfolio holding", logger, conflict_on_value_error=True)
async def add_portfolio_holding(
    request: PortfolioHoldingCreate,
    user_id: CurrentUserId,
):
    """
    Add a holding to the portfolio.

    Args:
        request: Portfolio holding data
        user_id: User ID from authentication header

    Returns:
        Created portfolio holding

    Raises:
        409: Holding already exists (same symbol + instrument_type + account_name)
    """
    holding = await db_create_portfolio_holding(
        user_id=user_id,
        symbol=request.symbol,
        instrument_type=request.instrument_type.value,
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

    logger.info(f"Added portfolio holding {holding['holding_id']} for user {user_id}")
    return PortfolioHoldingResponse.model_validate(holding)


@router.get("/{holding_id}", response_model=PortfolioHoldingResponse)
@handle_api_exceptions("get portfolio holding", logger)
async def get_portfolio_holding(
    holding_id: str,
    user_id: CurrentUserId,
):
    """
    Get a single portfolio holding.

    Args:
        holding_id: Portfolio holding ID
        user_id: User ID from authentication header

    Returns:
        Portfolio holding details

    Raises:
        404: Holding not found or not owned by user
    """
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
    """
    Update a portfolio holding.

    Partial update supported - only provided fields are updated.

    Args:
        holding_id: Portfolio holding ID
        request: Fields to update
        user_id: User ID from authentication header

    Returns:
        Updated portfolio holding

    Raises:
        404: Holding not found or not owned by user
    """
    holding = await db_update_portfolio_holding(
        holding_id=holding_id,
        user_id=user_id,
        name=request.name,
        quantity=request.quantity,
        average_cost=request.average_cost,
        currency=request.currency,
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
    """
    Remove a holding from the portfolio.

    Args:
        holding_id: Portfolio holding ID
        user_id: User ID from authentication header

    Returns:
        204 No Content on success

    Raises:
        404: Holding not found or not owned by user
    """
    deleted = await db_delete_portfolio_holding(holding_id, user_id)

    if not deleted:
        raise_not_found("Portfolio holding")

    logger.info(f"Deleted portfolio holding {holding_id} for user {user_id}")
    return Response(status_code=204)
