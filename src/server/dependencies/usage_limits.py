"""
FastAPI dependencies for usage limit enforcement.

Gate hierarchy:
  HOST_MODE ("oss" | "platform")        — master switch; OSS mode skips all gates.
  AUTH_SERVICE_URL                       — platform quota service; guards
                                           credit/workspace limits and access tier
                                           checks.  Can be absent even when
                                           HOST_MODE is "platform" (partial deploy).

Fail-open: when the platform service is unreachable, requests are allowed.
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Annotated, Optional

import httpx
from fastapi import Depends, HTTPException

from src.config.settings import HOST_MODE, AUTH_SERVICE_URL
from src.server.utils.api import get_current_user_id

logger = logging.getLogger(__name__)

# Default burst limit when ginlix-auth doesn't specify one
_DEFAULT_MAX_CONCURRENT = int(os.getenv("BURST_MAX_CONCURRENT") or "10")
_BURST_COUNTER_TTL = int(os.getenv("BURST_COUNTER_TTL") or "300")  # seconds

# Shared httpx client (created lazily, async-safe)
_http_client: Optional[httpx.AsyncClient] = None
_http_client_lock = asyncio.Lock()


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    async with _http_client_lock:
        if _http_client is None:
            _http_client = httpx.AsyncClient(timeout=5.0)
        return _http_client


async def close_http_client() -> None:
    """Close the shared httpx client. Call during application shutdown."""
    global _http_client
    async with _http_client_lock:
        if _http_client is not None:
            await _http_client.aclose()
            _http_client = None


@dataclass
class ChatAuthResult:
    """Auth + tier data collected by enforce_chat_limit for downstream gates."""
    user_id: str
    is_byok: bool = False
    has_oauth: bool = False
    access_tier: int = -1  # -1 = no platform access, 0+ = tier level



# ---------------------------------------------------------------------------
# Burst guard (local Redis INCR/DECR — stays in langalpha)
# ---------------------------------------------------------------------------

async def _check_burst_guard(user_id: str, max_concurrent: int) -> dict:
    """Redis-based burst guard: INCR on entry, DECR on release."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return {"allowed": True}

    key = f"usage:burst:{user_id}"
    try:
        pipe = cache.client.pipeline()
        pipe.incr(key)
        pipe.expire(key, _BURST_COUNTER_TTL)
        results = await pipe.execute()
        current = results[0]

        if current > max_concurrent:
            # Roll back
            await cache.client.decr(key)
            return {"allowed": False, "current": current - 1, "limit": max_concurrent}

        return {"allowed": True, "current": current, "limit": max_concurrent}
    except Exception as e:
        logger.warning("Burst guard Redis error, allowing request: %s", e)
        return {"allowed": True}


async def release_burst_slot(user_id: str) -> None:
    """Release a burst slot (DECR) after request completes."""
    if HOST_MODE == "oss":
        return  # No burst guard in OSS mode

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return

    key = f"usage:burst:{user_id}"
    try:
        current = await cache.client.decr(key)
        if current < 0:
            await cache.client.set(key, 0, ex=_BURST_COUNTER_TTL)
    except Exception as e:
        logger.warning("Burst guard release error: %s", e)


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

async def enforce_chat_limit(
    user_id: str = Depends(get_current_user_id),
) -> ChatAuthResult:
    """
    FastAPI dependency: burst guard + BYOK/OAuth/tier collection.

    In OSS mode (HOST_MODE="oss"), BYOK is still checked so custom models work;
    burst guard and platform tier checks are skipped.
    """
    from src.server.database.api_keys import is_byok_active

    if HOST_MODE == "oss":
        byok = await is_byok_active(user_id)
        return ChatAuthResult(user_id=user_id, is_byok=byok)

    from src.server.database.oauth_tokens import has_any_oauth_token

    # Two independent DB queries — run in parallel to cut TTFT latency.
    is_byok, has_oauth = await asyncio.gather(
        is_byok_active(user_id),
        has_any_oauth_token(user_id),
    )

    # Burst guard runs after DB queries succeed so the INCR'd slot
    # isn't leaked if a DB connection error propagates above.
    burst_result = await _check_burst_guard(user_id, _DEFAULT_MAX_CONCURRENT)
    if not burst_result["allowed"]:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Too many concurrent requests",
                "type": "burst_limit",
                "retry_after": 5,
            },
            headers={"Retry-After": "5"},
        )

    # Platform access tier — only when quota service is available and user
    # has no own-key path (BYOK or OAuth already grants access).
    tier = -1
    if HOST_MODE != "oss" and AUTH_SERVICE_URL and not is_byok and not has_oauth:
        tier = await _fetch_platform_tier(user_id)

    return ChatAuthResult(
        user_id=user_id,
        is_byok=is_byok,
        has_oauth=has_oauth,
        access_tier=tier,
    )


_BYOK_BALANCE_CACHE_TTL = 60  # seconds — negative balance changes slowly


async def enforce_credit_limit(user_id: str, *, byok: bool = False) -> None:
    """
    Check credit quota via ginlix-auth. Raises HTTPException(429) if exceeded.
    No-op in OSS mode.

    BYOK path: blocks only on negative balance; cached 60 s (balance changes
    slowly — only on platform fallback completion).
    Platform path: uncached real-time daily-credit check.
    """
    if HOST_MODE == "oss" or not AUTH_SERVICE_URL:
        return

    # BYOK fast path: cached negative-balance check (Redis, 60 s TTL).
    if byok:
        await _enforce_byok_negative_balance(user_id)
        return

    # Platform-served: uncached real-time quota check.
    result = await _call_validate_for_user(user_id, check_quota="chat")

    if result is None:
        return  # Fail-open

    quota = result.get("quota")
    if not quota:
        return

    if not quota.get("allowed", True):
        limit_type = quota.get("limit_type", "credit_limit")
        if limit_type == "credit_limit":
            message = "Daily credit limit reached"
        else:
            message = "Too many concurrent requests, please wait"

        raise HTTPException(
            status_code=429,
            detail={
                "message": message,
                "type": limit_type,
                "used_credits": quota.get("used_credits"),
                "credit_limit": quota.get("credit_limit"),
                "remaining_credits": quota.get("remaining_credits"),
                "retry_after": quota.get("retry_after", 30),
            },
            headers={
                "Retry-After": str(quota.get("retry_after") or 30),
                "X-RateLimit-Limit": str(quota.get("credit_limit", "")),
                "X-RateLimit-Remaining": str(quota.get("remaining_credits", "")),
            },
        )


async def _enforce_byok_negative_balance(user_id: str) -> None:
    """Raise 429 when remaining_credits < 0 (outstanding debt from past platform usage). Cached 60 s."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    cache_key = f"byok_balance:{user_id}"

    if cache.enabled and cache.client:
        try:
            cached = await cache.get(cache_key)
            if cached is not None:
                if cached == "negative":
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "message": "Outstanding credit balance. Please add credits to continue.",
                            "type": "negative_balance",
                            "retry_after": 30,
                        },
                        headers={"Retry-After": "30"},
                    )
                return  # cached "ok"
        except HTTPException:
            raise
        except Exception as e:
            logger.warning("BYOK balance cache read error, falling through: %s", e)

    result = await _call_validate_for_user(user_id, check_quota="chat", byok=True)

    if result is None:
        return  # Fail-open

    quota = result.get("quota")
    remaining = quota.get("remaining_credits") if quota else None

    is_negative = remaining is not None and remaining < 0

    if cache.enabled and cache.client:
        try:
            await cache.set(
                cache_key,
                "negative" if is_negative else "ok",
                ttl=_BYOK_BALANCE_CACHE_TTL,
            )
        except Exception as e:
            logger.warning("BYOK balance cache write error: %s", e)

    if is_negative:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Outstanding credit balance. Please add credits to continue.",
                "type": "negative_balance",
                "used_credits": quota.get("used_credits"),
                "credit_limit": quota.get("credit_limit"),
                "remaining_credits": remaining,
                "retry_after": quota.get("retry_after", 30),
            },
            headers={
                "Retry-After": str(quota.get("retry_after") or 30),
                "X-RateLimit-Limit": str(quota.get("credit_limit", "")),
                "X-RateLimit-Remaining": str(remaining),
            },
        )


async def _call_validate_for_user(
    user_id: str,
    check_quota: Optional[str] = None,
    byok: bool = False,
) -> Optional[dict]:
    """POST to ginlix-auth /api/auth/validate. Returns None in OSS mode or on failure."""
    if HOST_MODE == "oss" or not AUTH_SERVICE_URL:
        return None

    client = await _get_http_client()
    headers = {"X-User-Id": user_id}

    internal_token = os.getenv("INTERNAL_SERVICE_TOKEN", "")  # shared secret, not a JWT
    if internal_token:
        headers["X-Service-Token"] = internal_token

    body = {}
    if check_quota:
        body["check_quota"] = check_quota
    if byok:
        body["byok"] = True

    try:
        resp = await client.post(
            f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/validate",
            json=body if body else None,
            headers=headers,
        )
        if resp.status_code == 200:
            return resp.json()
        logger.warning(
            "ginlix-auth validate returned %d: %s", resp.status_code, resp.text[:200]
        )
        return None
    except Exception as e:
        logger.warning("ginlix-auth unreachable, failing open: %s", e)
        return None


async def enforce_workspace_limit(
    user_id: str = Depends(get_current_user_id),
) -> str:
    """FastAPI dependency: enforce active workspace limit via ginlix-auth. No-op in OSS mode."""
    if HOST_MODE == "oss" or not AUTH_SERVICE_URL:
        return user_id

    result = await _call_validate_for_user(user_id, check_quota="workspace")

    if result is None:
        return user_id  # Fail-open

    quota = result.get("quota")
    if not quota:
        return user_id

    if not quota.get("allowed", True):
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Active workspace limit reached",
                "type": "workspace_limit",
                "current": quota.get("active_workspaces"),
                "limit": quota.get("workspace_limit"),
                "remaining": 0,
            },
            headers={
                "X-RateLimit-Limit": str(quota.get("workspace_limit", "")),
                "X-RateLimit-Remaining": "0",
            },
        )

    return user_id


# ---------------------------------------------------------------------------
# Platform membership (access tier + plan display name)
# ---------------------------------------------------------------------------

_PLATFORM_MEMBERSHIP_CACHE_TTL = 300  # 5 minutes


def platform_membership_cache_key(user_id: str) -> str:
    return f"platform_membership:{user_id}"


async def _fetch_platform_membership(user_id: str) -> dict:
    """Fetch the user's platform membership (access tier + plan display name).

    Returns ``{"access_tier": int, "plan_display_name": Optional[str]}``.
    ``access_tier`` is -1 when the user has no platform access;
    ``plan_display_name`` is ``None`` when the user has no active subscription.
    Cached in Redis for 5 minutes. No-op in OSS mode.
    """
    if HOST_MODE == "oss" or not AUTH_SERVICE_URL:
        return {"access_tier": -1, "plan_display_name": None}

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    cache_key = platform_membership_cache_key(user_id)
    cached = await cache.get(cache_key)
    if isinstance(cached, dict) and "access_tier" in cached:
        return cached

    result = await _call_validate_for_user(user_id)
    if result is not None:
        membership = {
            "access_tier": int(result.get("access_tier", -1)),
            "plan_display_name": result.get("plan_display_name"),
        }
        await cache.set(cache_key, membership, ttl=_PLATFORM_MEMBERSHIP_CACHE_TTL)
        return membership

    # Brief negative cache prevents thundering herd against a down service.
    fallback = {"access_tier": -1, "plan_display_name": None}
    await cache.set(cache_key, fallback, ttl=15)
    return fallback


async def _fetch_platform_tier(user_id: str) -> int:
    """Fetch only the user's platform access tier. Shares cache with membership."""
    membership = await _fetch_platform_membership(user_id)
    return int(membership.get("access_tier", -1))


# ---------------------------------------------------------------------------
# Scope-based feature gating
# ---------------------------------------------------------------------------

_scope_cache: dict[str, tuple[list[str], float]] = {}  # {user_id: (scopes, expiry_ts)}
_SCOPE_CACHE_TTL = 300  # 5 minutes


async def _get_user_scopes(user_id: str) -> list[str]:
    """Return user's scopes from ginlix-auth; in-process cache with 5 min TTL."""
    import time

    now = time.time()
    cached = _scope_cache.get(user_id)
    if cached and cached[1] > now:
        return cached[0]

    result = await _call_validate_for_user(user_id)
    if result and "scopes" in result:
        scopes = result["scopes"]
    else:
        scopes = []  # Fail-open: no scopes restriction

    _scope_cache[user_id] = (scopes, now + _SCOPE_CACHE_TTL)
    return scopes


def require_scope(scope: str):
    """FastAPI dependency factory — checks user has scope. No-op in OSS mode."""
    async def check(user_id: str = Depends(get_current_user_id)):
        if HOST_MODE == "oss" or not AUTH_SERVICE_URL:
            return user_id  # OSS mode: everything allowed
        scopes = await _get_user_scopes(user_id)
        if scopes and scope not in scopes:
            raise HTTPException(403, detail=f"Requires scope: {scope}")
        return user_id
    return Depends(check)


# Annotated types for cleaner endpoint signatures
ChatRateLimited = Annotated[ChatAuthResult, Depends(enforce_chat_limit)]
WorkspaceLimitCheck = Annotated[str, Depends(enforce_workspace_limit)]
