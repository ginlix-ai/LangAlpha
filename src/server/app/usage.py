"""
Usage Limits and Code Redemption API Router.

Endpoints:
- GET  /api/v1/usage         — Current usage status (credits, workspaces, plan)
- POST /api/v1/usage/redeem  — Redeem a code to upgrade plan
"""

import logging

from fastapi import APIRouter, HTTPException

from src.server.utils.api import CurrentUserId
from src.server.services.usage_limiter import UsageLimiter
from src.server.database.redemption import redeem_code
from src.server.models.user import RedeemCodeRequest, RedeemCodeResponse
from src.server.services.plan_service import PlanService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/usage", tags=["Usage"])


@router.get("")
async def get_usage_status(user_id: CurrentUserId):
    """
    Get current usage status for the authenticated user.

    Returns plan info, credit usage, and workspace usage.
    When limits are disabled, returns limits_enabled=false.
    """
    svc = PlanService.get_instance()
    await svc.ensure_loaded()

    def _plan_obj(plan_info):
        return {
            'id': plan_info.id,
            'name': plan_info.name,
            'display_name': plan_info.display_name,
            'rank': plan_info.rank,
        }

    from src.server.database.api_keys import is_byok_active

    byok_enabled = await is_byok_active(user_id)

    if not UsageLimiter.is_enabled():
        return {
            'limits_enabled': False,
            'plan': _plan_obj(svc.get_default_plan()),
            'credits': {'used': 0.0, 'limit': -1, 'remaining': -1},
            'workspaces': {'active': 0, 'limit': -1, 'remaining': -1},
            'byok_enabled': byok_enabled,
        }

    plan = await UsageLimiter.get_user_plan(user_id)
    daily_credit_limit = plan.daily_credits
    workspace_limit = plan.max_active_workspaces

    used_credits = await UsageLimiter.get_daily_credit_usage(user_id)
    active_workspaces = await UsageLimiter.get_active_workspace_count(user_id)

    credits_remaining = max(0.0, daily_credit_limit - used_credits) if daily_credit_limit != -1 else -1
    workspace_remaining = max(0, workspace_limit - active_workspaces) if workspace_limit != -1 else -1

    return {
        'limits_enabled': True,
        'plan': _plan_obj(plan),
        'credits': {
            'used': round(used_credits, 2),
            'limit': daily_credit_limit,
            'remaining': round(credits_remaining, 2) if credits_remaining != -1 else -1,
        },
        'workspaces': {
            'active': active_workspaces,
            'limit': workspace_limit,
            'remaining': workspace_remaining,
        },
        'byok_enabled': byok_enabled,
    }


@router.post("/redeem", response_model=RedeemCodeResponse)
async def redeem_usage_code(request: RedeemCodeRequest, user_id: CurrentUserId):
    """
    Redeem a code to upgrade the user's plan.

    The code is validated and applied in a single database transaction.
    On success, the Redis plan cache is flushed so new limits take effect immediately.
    """
    try:
        result = await redeem_code(user_id, request.code)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Flush Redis plan cache so new limits take effect immediately
    await UsageLimiter.flush_plan_cache(user_id)

    return RedeemCodeResponse(
        previous_plan=result['previous_plan'],
        new_plan=result['new_plan'],
        message=f"Plan upgraded to {result['new_plan']}",
    )
