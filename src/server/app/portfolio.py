"""
Portfolio API Router.

Proxies portfolio holdings from Sharesight.

Endpoints:
- GET /api/v1/users/me/portfolio - List holdings from Sharesight
"""

import logging

from fastapi import APIRouter, HTTPException

from src.server.services.sharesight import sharesight_client
from src.server.models.user import PortfolioHoldingResponse, PortfolioResponse
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/users/me/portfolio", tags=["Portfolio"])


@router.get("", response_model=PortfolioResponse)
@handle_api_exceptions("list portfolio", logger)
async def list_portfolio(user_id: CurrentUserId):
    """
    List portfolio holdings from Sharesight.

    Fetches holdings from the configured Sharesight portfolio
    and maps them to the standard PortfolioHolding schema.
    """
    if not sharesight_client.is_configured:
        raise HTTPException(
            status_code=503,
            detail="Sharesight integration not configured. Set SHARESIGHT_CLIENT_ID and SHARESIGHT_CLIENT_SECRET.",
        )

    try:
        holdings = await sharesight_client.get_portfolio_holdings()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        if "authentication failed" in str(exc).lower():
            raise HTTPException(
                status_code=502,
                detail="Sharesight authentication failed. Check API credentials.",
            )
        raise HTTPException(status_code=502, detail="Unable to reach Sharesight. Try again later.")
    except Exception:
        logger.exception("Sharesight API error")
        raise HTTPException(status_code=502, detail="Unable to reach Sharesight. Try again later.")

    return PortfolioResponse(
        holdings=[PortfolioHoldingResponse.model_validate(h) for h in holdings],
        total=len(holdings),
    )
