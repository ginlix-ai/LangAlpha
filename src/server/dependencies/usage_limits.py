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

# Default burst limit when the auth/quota service doesn't specify one
_DEFAULT_MAX_CONCURRENT = int(os.getenv("BURST_MAX_CONCURRENT") or "10")
# Margin past the workflow timeout before an unreleased slot is presumed leaked
_BURST_REAP_MARGIN = 300  # seconds


def _burst_reap_horizon() -> int:
    """Age after which an unreleased slot member is reaped as leaked.

    Must exceed the longest legitimate run (the workflow timeout) — a shorter
    horizon reaps slots of still-running turns, so long runs silently stop
    counting against the burst limit. BURST_COUNTER_TTL overrides for tests.
    """
    override = os.getenv("BURST_COUNTER_TTL")
    if override:
        return int(override)
    from src.config.settings import get_workflow_timeout

    return get_workflow_timeout() + _BURST_REAP_MARGIN

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


def _platform_gating_active() -> bool:
    """True when platform scope/quota gates should run (not OSS and an auth URL set)."""
    return HOST_MODE != "oss" and bool(AUTH_SERVICE_URL)


@dataclass
class ChatAuthResult:
    """Auth + tier data collected by enforce_chat_limit for downstream gates."""
    user_id: str
    is_byok: bool = False
    has_oauth: bool = False
    access_tier: int = -1  # -1 = no platform access, 0+ = tier level
    burst_slot_id: Optional[str] = None  # ZSET member held for this request



# ---------------------------------------------------------------------------
# Burst guard (local Redis ZSET of slot ids — stays in langalpha)
#
# v4 (1.7): each admission holds a uuid member in a per-user ZSET scored by
# admission time. Release is ZREM — idempotent, so the finalize-time release
# can ride the hook outbox (effect-before-ack retries are harmless), unlike
# the old INCR/DECR counter where a retried DECR freed slots never held.
# Stale members (crashed before release) are reaped by score on each check,
# so a leak self-heals after the reap horizon even for a busy user who keeps
# the key alive (the old counter never healed under load). The horizon is
# workflow-timeout-based so a long-running turn is never reaped while live.
# ---------------------------------------------------------------------------

def _burst_key(user_id: str) -> str:
    # New key name: the legacy key held a counter STRING; ZADD on it would
    # WRONGTYPE during a rolling deploy. Old keys expire on their own TTL.
    return f"usage:burst:slots:{user_id}"


async def _check_burst_guard(user_id: str, max_concurrent: int) -> dict:
    """Admit by ZSET cardinality; returns the held slot_id on success."""
    import time
    from uuid import uuid4

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return {"allowed": True}

    key = _burst_key(user_id)
    slot_id = str(uuid4())
    now = time.time()
    horizon = _burst_reap_horizon()
    try:
        pipe = cache.client.pipeline()
        pipe.zremrangebyscore(key, "-inf", now - horizon)  # reap leaked
        pipe.zadd(key, {slot_id: now})
        pipe.zcard(key)
        pipe.expire(key, horizon)
        results = await pipe.execute()
        current = results[2]

        if current > max_concurrent:
            # Roll back our own member only.
            await cache.client.zrem(key, slot_id)
            return {"allowed": False, "current": current - 1, "limit": max_concurrent}

        return {
            "allowed": True,
            "current": current,
            "limit": max_concurrent,
            "slot_id": slot_id,
        }
    except Exception as e:
        logger.warning("Burst guard Redis error, allowing request: %s", e)
        return {"allowed": True}


async def release_burst_slot(
    user_id: str, slot_id: Optional[str] = None, *, strict: bool = False
) -> None:
    """Release a held burst slot (ZREM). Idempotent — safe to retry.

    slot_id None is a no-op (fail-open admission held no member; legacy
    in-flight requests from before the ZSET cutover release nothing and
    their old counter key simply expires). strict=True re-raises Redis
    errors — the hook-outbox executor's nack/backoff is the retry path,
    and a swallowed ZREM would ack a still-held slot.
    """
    if HOST_MODE == "oss":
        return  # No burst guard in OSS mode
    if not slot_id:
        return

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        # `enabled` flips off on a FAILED startup connect too, so a held
        # slot must not be acked as released in this state — raise so the
        # outbox drainer nacks and retries once Redis is back.
        if strict:
            raise RuntimeError("Redis cache unavailable — burst slot not released")
        return

    try:
        await cache.client.zrem(_burst_key(user_id), slot_id)
    except Exception as e:
        if strict:
            raise
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
        burst_slot_id=burst_result.get("slot_id"),
    )


_BYOK_BALANCE_CACHE_TTL = 60  # seconds — negative balance changes slowly


async def enforce_credit_limit(user_id: str, *, byok: bool = False) -> None:
    """
    Check credit quota via the auth/quota service. Raises HTTPException(429) if exceeded.
    No-op in OSS mode.

    BYOK path: blocks only on negative balance; cached 60 s (balance changes
    slowly — only on platform fallback completion).
    Platform path: uncached real-time daily-credit check.
    """
    if not _platform_gating_active():
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
        # Forward platform's `message` and `limit_type` verbatim; no copy authored here.
        raise HTTPException(
            status_code=429,
            detail={
                "message": quota.get("message"),
                "type": quota.get("limit_type", "credit_limit"),
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
    """Raise 429 when ``outstanding_debt > 0``. Cached 60 s.

    Gates on ``outstanding_debt`` rather than ``remaining_credits`` because the
    latter uses ``-1`` / ``-2`` as unlimited-tier sentinels.
    """
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

    quota = result.get("quota") or {}
    debt = int(quota.get("outstanding_debt") or 0)
    is_negative = debt > 0

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
                "outstanding_debt": debt,
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


async def _call_validate_for_user(
    user_id: str,
    check_quota: Optional[str] = None,
    byok: bool = False,
) -> Optional[dict]:
    """POST to the auth/quota service at /api/auth/validate. Returns None in OSS mode or on failure."""
    if not _platform_gating_active():
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
            "auth/quota service validate returned %d: %s", resp.status_code, resp.text[:200]
        )
        return None
    except Exception as e:
        logger.warning("auth/quota service unreachable, failing open: %s", e)
        return None


async def enforce_workspace_limit(
    user_id: str = Depends(get_current_user_id),
) -> str:
    """FastAPI dependency: enforce active workspace limit via the auth/quota service. No-op in OSS mode."""
    if not _platform_gating_active():
        return user_id

    result = await _call_validate_for_user(user_id, check_quota="workspace")

    if result is None:
        return user_id  # Fail-open

    quota = result.get("quota")
    if not quota:
        return user_id

    if not quota.get("allowed", True):
        # Forward platform's `message` and `limit_type` verbatim; no copy authored here.
        raise HTTPException(
            status_code=429,
            detail={
                "message": quota.get("message"),
                "type": quota.get("limit_type", "workspace_limit"),
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
    if not _platform_gating_active():
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


async def get_platform_access_tier(user_id: str) -> int | None:
    """Public access tier for plan gates: the tier, or None for no access.

    Normalizes the ``-1`` no-access sentinel to None so callers can fail closed
    on ``tier is None`` instead of leaking the sentinel into tier comparisons.
    """
    tier = await _fetch_platform_tier(user_id)
    return tier if tier >= 0 else None


# ---------------------------------------------------------------------------
# Scope-based feature gating
# ---------------------------------------------------------------------------

# {user_id: (scopes, expiry_ts)}; scopes is None when the platform is
# unreachable / omits them (fail-open), an explicit list (possibly empty) when
# it answered definitively.
_scope_cache: dict[str, tuple[list[str] | None, float]] = {}
_SCOPE_CACHE_TTL = 300  # 5 minutes


async def _get_user_scopes(user_id: str) -> list[str] | None:
    """Return the user's scopes from the auth/quota service; in-process cache
    (5 min, but only 15 s for a fail-open ``None``).

    Returns None when the platform is unreachable or omits ``scopes`` (the
    fail-open signal — callers allow). An explicit list, *including an empty
    one*, is the platform's definitive answer and is enforced, so a user the
    platform grants no scopes can't slip through as if the service were down.
    """
    import time

    now = time.time()
    cached = _scope_cache.get(user_id)
    if cached and cached[1] > now:
        return cached[0]

    result = await _call_validate_for_user(user_id)
    scopes = result["scopes"] if result and "scopes" in result else None

    # Brief negative TTL (mirrors _fetch_platform_membership) so a platform
    # blip doesn't leave gating disabled for the full 5 minutes.
    ttl = _SCOPE_CACHE_TTL if scopes is not None else 15
    _scope_cache[user_id] = (scopes, now + ttl)
    return scopes


def require_scope(scope: str):
    """FastAPI dependency factory — checks user has scope. No-op in OSS mode."""
    async def check(user_id: str = Depends(get_current_user_id)):
        if not _platform_gating_active():
            return user_id  # OSS mode: everything allowed
        scopes = await _get_user_scopes(user_id)
        if scopes is not None and scope not in scopes:
            raise HTTPException(403, detail=f"Requires scope: {scope}")
        return user_id
    return Depends(check)


# ---------------------------------------------------------------------------
# Workspace entitlement enforcement (hybrid scope + count quota; OSS no-op)
# ---------------------------------------------------------------------------

async def require_workspace_scope(user_id: str, scope: str) -> None:
    """Raise 403 when the platform's definitive scope list lacks ``scope``.

    Fail-open (allow) only in OSS mode or when the platform is unreachable /
    omits scopes (``_get_user_scopes`` returns None). A definitive list —
    including an empty one — is enforced.
    """
    if not _platform_gating_active():
        return
    scopes = await _get_user_scopes(user_id)
    if scopes is not None and scope not in scopes:
        raise HTTPException(403, detail=f"Requires scope: {scope}")


def _extract_capacity(quota: dict) -> tuple[int | None, int | None]:
    """Extract ``(used, limit)`` counts from a platform quota object.

    Prefers the ``capacity_used``/``capacity_limit`` names (see ginlix-platform
    QuotaInfo), falling back to the legacy ``active``/``limit`` and
    ``active_workspaces``/``workspace_limit`` aliases.
    """
    used = quota.get("capacity_used", quota.get("active", quota.get("active_workspaces")))
    limit = quota.get("capacity_limit", quota.get("limit", quota.get("workspace_limit")))
    return used, limit


async def enforce_capacity(user_id: str, check_quota: str) -> None:
    """Raise 429 when the platform reports the named count quota is exhausted.

    Generalizes ``enforce_workspace_limit`` over ``check_quota`` (``always_on``,
    ``spec_performance``, ``spec_max``). No-op in OSS mode and fail-open when the
    platform is unreachable or omits the quota object.
    """
    if not _platform_gating_active():
        return

    result = await _call_validate_for_user(user_id, check_quota=check_quota)
    if result is None:
        return  # Fail-open

    quota = result.get("quota")
    if not quota:
        return

    if quota.get("allowed") is False:
        current, limit = _extract_capacity(quota)
        headers = {"X-RateLimit-Remaining": "0"}
        if limit is not None:
            headers["X-RateLimit-Limit"] = str(limit)
        # Forward platform's `message` and `limit_type` verbatim; no copy authored here.
        raise HTTPException(
            status_code=429,
            detail={
                "message": quota.get("message"),
                "type": quota.get("limit_type", check_quota),
                "current": current,
                "limit": limit,
                "remaining": 0,
            },
            headers=headers,
        )


async def get_capacity_status(user_id: str, check_quota: str) -> Optional[dict]:
    """Read-only count-quota status for display: ``{"used": int, "limit": int}`` or None.

    ``limit == -1`` means unlimited. Returns None in OSS mode, when the platform is
    unreachable, or when it reports no counts for ``check_quota``. Never raises — this
    backs a UI hint, not a gate.
    """
    if not _platform_gating_active():
        return None

    result = await _call_validate_for_user(user_id, check_quota=check_quota)
    if not result:
        return None

    quota = result.get("quota")
    if not quota:
        return None

    used, limit = _extract_capacity(quota)
    if limit is None:
        return None
    # Unlimited tiers report limit == -1 and omit the count, so don't require it.
    if int(limit) == -1:
        return {"used": int(used) if used is not None else 0, "limit": -1}
    if used is None:
        return None
    return {"used": int(used), "limit": int(limit)}


# Always-on entitlement identifiers — single source for the gate, the
# reconciler probe, and the quota route.
ALWAYS_ON_SCOPE = "workspace:always_on"
ALWAYS_ON_QUOTA = "always_on"

# spec tier -> (required scope, count-quota name). Absent tiers (standard,
# unknown) are ungated.
_SPEC_ENTITLEMENTS: dict[str, tuple[str, str]] = {
    "performance": ("workspace:spec:performance", "spec_performance"),
    "max": ("workspace:spec:max", "spec_max"),
}

# tier -> count-quota name, for callers that only need the quota identifier
# (e.g. the /workspaces/quota route).
SPEC_QUOTAS: dict[str, str] = {
    tier: quota for tier, (_scope, quota) in _SPEC_ENTITLEMENTS.items()
}

# Ordering for upgrade-vs-downgrade decisions. Unknown tiers rank lowest so a
# move from one is treated as an upgrade (counted).
_TIER_RANK: dict[str, int] = {"standard": 0, "performance": 1, "max": 2}


async def _assert_hybrid_gate(
    user_id: str, scope: str, check_quota: str, *, count: bool = True
) -> None:
    """Shared workspace-entitlement gate: scope 403 then optional count 429."""
    await require_workspace_scope(user_id, scope)
    if count:
        await enforce_capacity(user_id, check_quota)


async def assert_spec_allowed(
    user_id: str, tier: str, *, current_tier: Optional[str] = None
) -> None:
    """Gate an upgrade to ``tier`` (scope 403 + count 429). Standard is never gated.

    The count quota is skipped when ``current_tier == tier`` (no new slot) and on
    downgrades — a user must always be able to step down-tier, even if that
    briefly reads over the target tier's count (the freed higher slot
    compensates, and new allocations stay gated). No-op in OSS mode.
    """
    entitlement = _SPEC_ENTITLEMENTS.get(tier)
    if entitlement is None:
        return  # standard / unknown tiers are ungated
    scope, check_quota = entitlement
    # Same-tier and downward moves never count; equal ranks make the former a
    # non-upgrade automatically.
    is_upgrade = _TIER_RANK.get(tier, -1) > _TIER_RANK.get(current_tier or "", -1)
    await _assert_hybrid_gate(user_id, scope, check_quota, count=is_upgrade)


async def assert_always_on_allowed(user_id: str) -> None:
    """Gate enabling always-on (scope 403 + count 429). No-op in OSS mode."""
    await _assert_hybrid_gate(user_id, ALWAYS_ON_SCOPE, ALWAYS_ON_QUOTA)


async def spec_grantable(
    user_id: str, tier: str, *, current_tier: Optional[str] = None
) -> bool:
    """Non-raising form of :func:`assert_spec_allowed` (full scope 403 + count 429
    gate) for service-layer callers that must not import/catch ``HTTPException``.

    Returns True when the user may hold a workspace at ``tier`` and False when the
    gate would reject (missing scope or exhausted per-tier count). ``standard``/
    unknown tiers and OSS mode fail open to True. Confines HTTP semantics to this
    dependency layer.
    """
    try:
        await assert_spec_allowed(user_id, tier, current_tier=current_tier)
        return True
    except HTTPException:
        return False


async def _scope_entitlement_lost(user_id: str, scope: str) -> bool:
    """True only when the platform confirms ``scope`` is no longer granted.

    Fail-safe for unattended reconciliation: returns False (keep the
    entitlement) in OSS mode, on any validate failure/unreachable, and on an
    ambiguous response, so an outage can never mass-revoke. Uses the raw
    validate call directly (not the cached ``_get_user_scopes``) so a failed
    fetch is distinguishable from a 200 that simply lacks the scope.
    """
    if not _platform_gating_active():
        return False
    result = await _call_validate_for_user(user_id)
    if result is None:
        return False  # validate failed / unreachable → keep
    scopes = result.get("scopes")
    if not isinstance(scopes, list):
        return False  # ambiguous response → keep
    return scope not in scopes


async def always_on_entitlement_lost(user_id: str) -> bool:
    """True only when the platform confirms the user no longer holds always-on.

    Drives the idle-cleanup reconciler; fail-safe semantics per
    :func:`_scope_entitlement_lost`.
    """
    return await _scope_entitlement_lost(user_id, ALWAYS_ON_SCOPE)


async def spec_entitlement_lost(user_id: str, tier: str) -> bool:
    """True only when the platform confirms the user no longer holds ``tier``'s scope.

    Drives lazy spec reclaim at sandbox (re)provision time. Standard/unknown
    tiers are never reclaimed; fail-safe semantics per
    :func:`_scope_entitlement_lost`, deliberately uncached so a stale scope
    snapshot can't trigger a wrong rebuild.
    """
    entitlement = _SPEC_ENTITLEMENTS.get(tier)
    if entitlement is None:
        return False
    return await _scope_entitlement_lost(user_id, entitlement[0])


# Annotated types for cleaner endpoint signatures
ChatRateLimited = Annotated[ChatAuthResult, Depends(enforce_chat_limit)]
WorkspaceLimitCheck = Annotated[str, Depends(enforce_workspace_limit)]
