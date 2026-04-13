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
    """Result from enforce_chat_limit. Downstream gates read these flags
    instead of re-querying the DB."""
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
    FastAPI dependency: burst guard + auth status collection.

    Populates ChatAuthResult with is_byok / has_oauth / access_tier so
    downstream gates (403 in threads.py, credit check) can decide without
    re-querying the DB.

    OSS mode (HOST_MODE="oss"): still check BYOK so custom models
    (Ollama, LM Studio, etc.) work. Skip burst guard + platform tier.
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
    if AUTH_SERVICE_URL and not is_byok and not has_oauth:
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

    When ``byok=False`` (platform-served): blocks when daily credit limit is
    reached (``allowed=False``).  Not cached — credits deplete per-message and
    users need real-time feedback.

    When ``byok=True`` (user's own key): blocks only when credits are negative.
    Cached in Redis (60 s) to avoid an HTTP round-trip on every message.
    Negative balance changes slowly (only on platform fallback completion).
    """
    if not AUTH_SERVICE_URL:
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
    """Check BYOK user for negative credit balance (cached, 60 s TTL).

    Only blocks when ``remaining_credits < 0`` (outstanding debt from past
    platform usage).  The result is cached in Redis to avoid an HTTP
    round-trip to the platform service on every BYOK message.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    cache_key = f"byok_balance:{user_id}"

    # Fast path: check cache
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

    # Cache miss or no Redis — fetch from platform
    result = await _call_validate_for_user(user_id, check_quota="chat", byok=True)

    if result is None:
        return  # Fail-open

    quota = result.get("quota")
    remaining = quota.get("remaining_credits") if quota else None

    is_negative = remaining is not None and remaining < 0

    # Cache the result
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
    """Call ginlix-auth validate using internal service token or user_id header."""
    if not AUTH_SERVICE_URL:
        return None

    client = await _get_http_client()
    headers = {"X-User-Id": user_id}

    # Use internal service token if available (shared secret, not a JWT)
    internal_token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
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
    """
    FastAPI dependency: enforce active workspace limit.

    Open-source mode: no limits.
    Commercial mode: calls ginlix-auth for workspace quota check.

    Returns user_id on success, raises HTTPException(429) if at limit.
    """
    if not AUTH_SERVICE_URL:
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
# Platform access tier
# ---------------------------------------------------------------------------

_PLATFORM_TIER_CACHE_TTL = 300  # 5 minutes


def platform_tier_cache_key(user_id: str) -> str:
    return f"platform_tier:{user_id}"


async def _fetch_platform_tier(user_id: str) -> int:
    """Fetch the user's platform access tier.

    Returns the numeric tier (0+) on success, or -1 when the user has no
    platform access, the service is unavailable, or AUTH_SERVICE_URL is unset.
    Results are cached in Redis for 5 minutes.
    """
    if not AUTH_SERVICE_URL:
        return -1

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    cache_key = platform_tier_cache_key(user_id)
    cached = await cache.get(cache_key)
    if cached is not None:
        return int(cached)

    result = await _call_validate_for_user(user_id)
    if result is not None:
        tier = result.get("access_tier", -1)
        await cache.set(cache_key, tier, ttl=_PLATFORM_TIER_CACHE_TTL)
        return tier

    # Brief negative cache prevents thundering herd against a down service.
    await cache.set(cache_key, -1, ttl=15)
    return -1


# ---------------------------------------------------------------------------
# Scope-based feature gating (Phase 2)
# ---------------------------------------------------------------------------

# Cache for user scopes: {user_id: (scopes_list, expiry_timestamp)}
_scope_cache: dict[str, tuple[list[str], float]] = {}
_SCOPE_CACHE_TTL = 300  # 5 minutes


async def _get_user_scopes(user_id: str) -> list[str]:
    """Get user's scopes from ginlix-auth (cached)."""
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
    """FastAPI dependency factory — checks user has scope. No-op when AUTH_SERVICE_URL unset."""
    async def check(user_id: str = Depends(get_current_user_id)):
        if not AUTH_SERVICE_URL:
            return user_id  # Open-source: everything allowed
        scopes = await _get_user_scopes(user_id)
        if scopes and scope not in scopes:
            raise HTTPException(403, detail=f"Requires scope: {scope}")
        return user_id
    return Depends(check)


# Annotated types for cleaner endpoint signatures
ChatRateLimited = Annotated[ChatAuthResult, Depends(enforce_chat_limit)]
WorkspaceLimitCheck = Annotated[str, Depends(enforce_workspace_limit)]
