"""
FastAPI dependencies for usage limit enforcement.

Provides two dependencies that compose with get_current_user_id:
- ChatRateLimited: Enforces daily credit limit + burst guard
- WorkspaceLimitCheck: Enforces active workspace limits

Both are complete no-ops when auth is disabled.
"""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException

from src.server.utils.api import get_current_user_id
from src.server.services.usage_limiter import UsageLimiter


@dataclass
class ChatAuthResult:
    """Result from chat rate-limit dependency, carrying BYOK status to avoid re-querying."""
    user_id: str
    byok_active: bool = False


async def enforce_chat_limit(
    user_id: str = Depends(get_current_user_id),
) -> ChatAuthResult:
    """
    FastAPI dependency: enforce daily credit limit + burst guard.

    Layer 1: DB credit check (SUM total_credits today vs tier daily_credits)
    Layer 2: Redis burst guard (concurrent in-flight request cap)

    BYOK users bypass the credit check but still face burst guard.

    Returns ChatAuthResult on success, raises HTTPException(429) if over limit.
    """
    if not UsageLimiter.is_enabled():
        return ChatAuthResult(user_id=user_id)

    # Check BYOK status once — reused downstream via ChatAuthResult
    from src.server.database.api_keys import is_byok_active

    byok = await is_byok_active(user_id)

    if byok:
        # BYOK bypasses credit limit, but still enforce burst guard
        plan = await UsageLimiter.get_user_plan(user_id)
        if plan.max_concurrent_requests != -1:
            burst_result = await UsageLimiter._check_burst_guard(
                user_id, plan.max_concurrent_requests
            )
            if not burst_result['allowed']:
                raise HTTPException(
                    status_code=429,
                    detail={
                        'message': 'Too many concurrent requests',
                        'type': 'burst_limit',
                        'retry_after': 5,
                    },
                    headers={'Retry-After': '5'},
                )
        return ChatAuthResult(user_id=user_id, byok_active=True)

    result = await UsageLimiter.check_chat_limit(user_id)

    if not result['allowed']:
        # Determine which limit was hit for the message
        is_credit_limit = result['remaining_credits'] == 0.0 and result['credit_limit'] != -1
        if is_credit_limit:
            message = 'Daily credit limit reached'
            limit_type = 'credit_limit'
        else:
            message = 'Too many concurrent requests, please wait'
            limit_type = 'burst_limit'

        raise HTTPException(
            status_code=429,
            detail={
                'message': message,
                'type': limit_type,
                'used_credits': result['used_credits'],
                'credit_limit': result['credit_limit'],
                'remaining_credits': result['remaining_credits'],
                'retry_after': result['retry_after'],
            },
            headers={
                'Retry-After': str(result['retry_after'] or 30),
                'X-RateLimit-Limit': str(result['credit_limit']),
                'X-RateLimit-Remaining': str(result['remaining_credits']),
            },
        )

    return ChatAuthResult(user_id=user_id)


async def enforce_workspace_limit(
    user_id: str = Depends(get_current_user_id),
) -> str:
    """
    FastAPI dependency: enforce active workspace limit.

    Queries DB for active workspace count.
    Returns user_id on success, raises HTTPException(429) if at limit.
    """
    if not UsageLimiter.is_enabled():
        return user_id

    result = await UsageLimiter.check_workspace_limit(user_id)

    if not result['allowed']:
        raise HTTPException(
            status_code=429,
            detail={
                'message': 'Active workspace limit reached',
                'type': 'workspace_limit',
                'current': result['current'],
                'limit': result['limit'],
                'remaining': result['remaining'],
            },
            headers={
                'X-RateLimit-Limit': str(result['limit']),
                'X-RateLimit-Remaining': '0',
            },
        )

    return user_id


# Annotated types for cleaner endpoint signatures
ChatRateLimited = Annotated[ChatAuthResult, Depends(enforce_chat_limit)]
WorkspaceLimitCheck = Annotated[str, Depends(enforce_workspace_limit)]
