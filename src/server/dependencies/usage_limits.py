"""
FastAPI dependencies for usage limit enforcement.

Gate hierarchy:
  HOST_MODE ("oss" | "platform")        — master switch; OSS mode skips all gates.
  PLATFORM_SERVICE_URL                  — platform quota service; guards
                                           credit/workspace limits and access tier
                                           checks.  Can be absent even when
                                           HOST_MODE is "platform" (partial deploy).

Fail-open: when the platform service is unreachable, requests are allowed,
except by the credit gate and by a plan that is on record nowhere, which both
answer 503 rather than guess.
"""

import asyncio
import logging
import math
import os
import time
from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass
from typing import Annotated, Optional, get_args

from fastapi import Depends, HTTPException

from src.config.settings import HOST_MODE, PLATFORM_SERVICE_URL
from src.server.models.computer import ResourceTierId
from src.server.services import membership, platform_client
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



@dataclass
class ChatAuthResult:
    """Auth + tier data collected by ``chat_limit_for`` for downstream gates."""
    user_id: str
    is_byok: bool = False
    has_oauth: bool = False
    # -1 = no platform access, 0+ = tier level, None = the platform could not
    # be asked and no membership is on record for this user.
    access_tier: Optional[int] = -1
    burst_slot_id: Optional[str] = None  # ZSET member held for this request
    # Whether the user's policy lets their own sign-ins count, which
    # ``has_oauth`` already reflects; a refusal reads it to not offer a
    # reconnect the turn would not use.
    own_subscriptions: bool = True



# ---------------------------------------------------------------------------
# Burst guard (the shared ZSET slot primitive, keyed per user — stays in
# langalpha)
#
# v4 (1.7): each admission holds a uuid member in a per-user ZSET scored by
# admission time. Release is ZREM — idempotent, so the finalize-time release
# can ride the hook outbox (effect-before-ack retries are harmless), unlike
# the old INCR/DECR counter where a retried DECR freed slots never held.
# The horizon is workflow-timeout-based so a long-running turn is never reaped
# while live.
# ---------------------------------------------------------------------------

def _burst_key(user_id: str) -> str:
    # New key name: the legacy key held a counter STRING; ZADD on it would
    # WRONGTYPE during a rolling deploy. Old keys expire on their own TTL.
    return f"usage:burst:slots:{user_id}"


async def _check_burst_guard(user_id: str, max_concurrent: int) -> dict:
    """Admit by ZSET cardinality; returns the held slot_id on success."""
    from src.server.utils.slot_guard import acquire_slot_member
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return {"allowed": True}

    try:
        admitted = await acquire_slot_member(
            cache.client,
            _burst_key(user_id),
            limit=max_concurrent,
            stale_after=_burst_reap_horizon(),
        )
    except Exception as e:
        logger.warning("Burst guard Redis error, allowing request: %s", e)
        return {"allowed": True}

    result = {
        "allowed": admitted.allowed,
        "current": admitted.current,
        "limit": admitted.limit,
    }
    if admitted.allowed:
        result["slot_id"] = admitted.slot_id
    return result


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

    from src.server.utils.slot_guard import release_slot_member
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
        await release_slot_member(cache.client, _burst_key(user_id), slot_id)
    except Exception as e:
        if strict:
            raise
        logger.warning("Burst guard release error: %s", e)


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

async def chat_limit_for(user_id: str) -> ChatAuthResult:
    """
    Burst guard + BYOK/OAuth/tier collection.

    In OSS mode (HOST_MODE="oss"), BYOK is still checked so custom models work;
    burst guard and platform tier checks are skipped.

    Own keys and sign-ins the user's policy denies read as absent, so such a
    turn is admitted as one on the deployment's credential, plan check and
    all. An account whose policy is unknown is refused here.
    """
    from src.server.services.policy import OWN_SUBSCRIPTIONS
    from src.server.services.account import own_access

    if HOST_MODE == "oss":
        own = await own_access(user_id, sign_ins=False)
        return ChatAuthResult(
            user_id=user_id, is_byok=own.api_keys,
            own_subscriptions=own.policy.allows(OWN_SUBSCRIPTIONS),
        )

    own = await own_access(user_id)
    is_byok, has_oauth = own.api_keys, bool(own.subscriptions)
    policy = own.policy

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
    tier: Optional[int] = -1
    if HOST_MODE != "oss" and PLATFORM_SERVICE_URL and not is_byok and not has_oauth:
        tier = await membership.access_tier(user_id)

    return ChatAuthResult(
        user_id=user_id,
        is_byok=is_byok,
        has_oauth=has_oauth,
        access_tier=tier,
        burst_slot_id=burst_result.get("slot_id"),
        own_subscriptions=policy.allows(OWN_SUBSCRIPTIONS),
    )


async def enforce_credit_limit(
    user_id: str, *, byok: bool = False, surface: str | None = None
) -> None:
    """Check credit quota via the auth/quota service. Raises 429 if denied.

    No-op in OSS mode. ``byok`` is forwarded, not branched on: which pools apply
    to a key the user pays for themselves is the platform's rule to state, and
    this service only relays the verdict it gets back. ``surface`` is the same:
    where the turn came from (the request's ``platform``, e.g. ``web`` or
    ``slack``), sent only when the turn names one. The auth service may apply
    its own policy to it, and its refusal comes back as any other does, its
    message and type relayed as sent.

    This is the one gate that fails **closed** (503). The others admit on an
    unanswered check because the worst case is bounded and self-correcting; here
    it is an unbounded spend by a user the service may well have been about to
    refuse, landing on them later as debt they never agreed to.
    """
    await _admitted_quota(user_id, byok=byok, surface=surface)


async def spendable_credits(user_id: str) -> float | None:
    """Enforce the credit limit as ``enforce_credit_limit`` does, then return
    what the user can still spend as the platform reports it; None for no
    limit, and in OSS mode.

    A platform too old to send the figure is read as no limit rather than
    refused: the caller's own bound on uncharged spend still holds, and failing
    closed would stop every paid call until the platform is upgraded. A figure
    that is not a finite number is no verdict at all, refused as the 503 the
    gate gives an outage: NaN fails every comparison, so read as a balance it
    would bound nothing.
    """
    quota = await _admitted_quota(user_id)
    if not quota:
        return None
    if "spendable_credits" not in quota:
        logger.warning("auth/quota service sent no spendable_credits; treating it as unlimited")
        return None
    spendable = quota["spendable_credits"]
    if spendable is None:
        return None
    try:
        value = float(spendable)
    except (TypeError, ValueError):
        value = math.nan
    if not math.isfinite(value):
        logger.warning("auth/quota service sent spendable_credits %r; refusing", spendable)
        raise platform_client.quota_service_unavailable()
    return value


async def _admitted_quota(
    user_id: str, *, byok: bool = False, surface: str | None = None
) -> dict | None:
    """The chat quota the platform allowed, raising 429 on a denial and 503
    with no verdict; None when nothing was checked."""
    if not platform_client.platform_gating_active():
        return None

    try:
        result = await platform_client.call_validate(
            user_id, check_quota="chat", byok=byok, surface=surface, strict=True
        )
    except platform_client.QuotaServiceUnavailable as e:
        logger.error("Credit gate closed — quota service gave no verdict (%s)", e)
        raise platform_client.quota_service_unavailable() from e

    if result is None:
        return None  # OSS mode reached this only via a race on HOST_MODE; nothing to check.

    quota = result.get("quota")
    if not quota or quota.get("allowed", True):
        return quota

    # Forward platform's `message` and `limit_type` verbatim; no copy authored here.
    raise HTTPException(
        status_code=429,
        detail={
            "message": quota.get("message"),
            "type": quota.get("limit_type", "credit_limit"),
            "used_credits": quota.get("used_credits"),
            "credit_limit": quota.get("credit_limit"),
            "remaining_credits": quota.get("remaining_credits"),
            "outstanding_debt": quota.get("outstanding_debt"),
            "retry_after": quota.get("retry_after", 30),
        },
        headers={
            "Retry-After": str(quota.get("retry_after") or 30),
            "X-RateLimit-Limit": str(quota.get("credit_limit", "")),
            "X-RateLimit-Remaining": str(quota.get("remaining_credits", "")),
        },
    )




async def enforce_computer_limit(
    user_id: str = Depends(get_current_user_id),
) -> str:
    """FastAPI dependency: enforce the active computer limit. No-op in OSS mode.

    The plan meters computers, so this is what a route allocating one asks.
    ``check_quota``, the response fields and the 429 body still say workspace:
    the platform already counts computers behind those names, and the strings
    rename on their own release once both sides speak the new one.
    """
    await _enforce_computer_quota(user_id, "workspace")
    return user_id


# Every managed computer a user holds, stopped ones included.
COMPUTER_COUNT_QUOTA = "computer_count"


async def enforce_computer_count(user_id: str) -> None:
    """Enforce how many computers the plan lets a user hold. No-op in OSS mode.

    The platform's ``computer_count`` counts stopped computers too, so a user at
    the cap can't create another by stopping one. Ask it on a create only: on a
    start it would count the computer being started and refuse a user exactly
    at their cap. A platform that predates the value answers 422 and this fails
    open, so a create still asks :func:`enforce_computer_limit` as well.
    """
    await _enforce_computer_quota(user_id, COMPUTER_COUNT_QUOTA)


async def _enforce_computer_quota(user_id: str, check_quota: str) -> None:
    """Raise the platform's 429 when ``check_quota``'s computer count is full."""
    if not platform_client.platform_gating_active():
        return

    result = await platform_client.call_validate(user_id, check_quota=check_quota)

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




# ---------------------------------------------------------------------------
# Scope-based feature gating
# ---------------------------------------------------------------------------

# {user_id: (scopes, expiry_ts)}; scopes is None when the platform is
# unreachable / omits them (fail-open), an explicit list (possibly empty) when
# it answered definitively.
_scope_cache: dict[str, tuple[list[str] | None, float]] = {}
_SCOPE_CACHE_TTL = 300  # 5 minutes
_SCOPE_NONE_TTL = 15
# How old a list may be where a plan just changed would show: a refusal, and
# what the quota read says a plan opens.
_SCOPE_REREAD_AGE = 15
# The same, for a client back from the plan page (``plan_changed``): younger
# than any trip through it, and still a floor, so a client that says so on
# every request costs one platform read per user in that time on each worker.
_SCOPE_PLAN_CHANGED_AGE = 2


def _reread_age(plan_changed: bool) -> float:
    return _SCOPE_PLAN_CHANGED_AGE if plan_changed else _SCOPE_REREAD_AGE


def _scope_ttl(scopes: list[str] | None) -> int:
    return _SCOPE_CACHE_TTL if scopes is not None else _SCOPE_NONE_TTL


async def _get_user_scopes(user_id: str, *, max_age: float = _SCOPE_CACHE_TTL) -> list[str] | None:
    """Return the user's scopes from the auth/quota service; in-process cache
    (5 min, but only 15 s for a fail-open ``None``), read again past
    ``max_age`` unless the platform cannot answer.

    Returns None when the platform is unreachable or omits ``scopes`` (the
    fail-open signal — callers allow). An explicit list, *including an empty
    one*, is the platform's definitive answer and is enforced, so a user the
    platform grants no scopes can't slip through as if the service were down.
    """

    now = time.time()
    cached = _scope_cache.get(user_id)
    if cached and cached[1] > now and now - (cached[1] - _scope_ttl(cached[0])) <= max_age:
        return cached[0]

    result = await platform_client.call_validate(user_id)
    scopes = result["scopes"] if result and "scopes" in result else None
    # A read again that the platform cannot answer keeps the list still
    # cached: reading past the cache must not turn a definitive answer into
    # fail-open.
    if scopes is None and cached and cached[0] is not None and cached[1] > now:
        return cached[0]

    # Brief negative TTL (mirrors read_membership) so a platform
    # blip doesn't leave gating disabled for the full 5 minutes.
    _scope_cache[user_id] = (scopes, now + _scope_ttl(scopes))
    return scopes


async def _lacks_scope(user_id: str, scope: str, *, plan_changed: bool = False) -> bool:
    """Whether the platform's definitive scope list lacks ``scope``. A cached
    list that lacks it is read again once it is older than ``_reread_age``:
    otherwise a user who just upgraded their plan is refused on the one they
    had for as long as the cache holds it."""
    scopes = await _get_user_scopes(user_id)
    if scopes is None or scope in scopes:
        return False
    scopes = await _get_user_scopes(user_id, max_age=_reread_age(plan_changed))
    return scopes is not None and scope not in scopes


def require_scope(scope: str):
    """FastAPI dependency factory — checks user has scope. No-op in OSS mode."""
    async def check(user_id: str = Depends(get_current_user_id)):
        if not platform_client.platform_gating_active():
            return user_id  # OSS mode: everything allowed
        if await _lacks_scope(user_id, scope):
            raise HTTPException(403, detail=f"Requires scope: {scope}")
        return user_id
    return Depends(check)


# ---------------------------------------------------------------------------
# Workspace entitlement enforcement (hybrid scope + count quota; OSS no-op)
# ---------------------------------------------------------------------------

async def require_workspace_scope(user_id: str, scope: str, *, plan_changed: bool = False) -> None:
    """Raise 403 when the platform's definitive scope list lacks ``scope``.

    Fail-open (allow) only in OSS mode or when the platform is unreachable /
    omits scopes (``_get_user_scopes`` returns None). A definitive list —
    including an empty one — is enforced.
    """
    if not platform_client.platform_gating_active():
        return
    if await _lacks_scope(user_id, scope, plan_changed=plan_changed):
        raise HTTPException(403, detail=f"Requires scope: {scope}")


def _first(quota: dict, *names: str) -> int | None:
    """Read the first of ``names`` the quota object carries a count for.

    The platform serializes every field of its quota object, so a name it has
    no count for arrives as null: a computer quota sends ``capacity_used`` and
    ``capacity_limit`` as null beside ``active_workspaces`` and
    ``workspace_limit``. A null falls through to the next name, or the
    computer counts would never be read.
    """
    for name in names:
        value = quota.get(name)
        if value is not None:
            return value
    return None


def _extract_capacity(quota: dict) -> tuple[int | None, int | None]:
    """Extract ``(used, limit)`` counts from a platform quota object.

    Prefers the ``capacity_used``/``capacity_limit`` names, then ``active``/
    ``limit``, then the per-subject aliases.

    The ``active_computers``/``computer_limit`` pair reads the names the platform
    moves to when the capacity subject becomes the machine. It is accepted here
    first so the two sides can be deployed in either order: this build keeps
    reading the workspace names until the platform emits the computer ones, and
    keeps working after it does.
    """
    used = _first(
        quota, "capacity_used", "active", "active_computers", "active_workspaces"
    )
    limit = _first(
        quota, "capacity_limit", "limit", "computer_limit", "workspace_limit"
    )
    return used, limit


def _returned_slot_verdict(quota: dict) -> bool | None:
    """Whether the quota allows one more once a slot the caller holds is given back.

    The platform's ``allowed`` counts that slot as taken, so the counts decide
    here instead: one fewer used, a limit of 0 still the plan leaving it out,
    and -1 unlimited. None without both counts.
    """
    used, limit = _extract_capacity(quota)
    if used is None or limit is None:
        return None
    limit = int(limit)
    return limit == -1 or (limit > 0 and int(used) - 1 < limit)


def _allowed_returning_a_slot(quota: dict) -> bool:
    """The gate's reading of :func:`_returned_slot_verdict`: without both
    counts the platform's own answer stands, fail-open."""
    verdict = _returned_slot_verdict(quota)
    return quota.get("allowed") is not False if verdict is None else verdict


async def enforce_capacity(
    user_id: str, check_quota: str, *, returning_slot: bool = False
) -> None:
    """Raise 429 when the platform reports the named count quota is exhausted.

    Generalizes ``enforce_computer_limit`` over ``check_quota`` (``always_on``,
    ``spec_performance``, ``spec_max``, ``spec_xlarge``). ``returning_slot``
    is for a caller that already holds one of the slots the quota counts and
    gives it up for the one it asks for. No-op in OSS mode and fail-open when
    the platform is unreachable or omits the quota object.
    """
    if not platform_client.platform_gating_active():
        return

    result = await platform_client.call_validate(user_id, check_quota=check_quota)
    if result is None:
        return  # Fail-open

    quota = result.get("quota")
    if not quota:
        return

    allowed = (
        _allowed_returning_a_slot(quota)
        if returning_slot
        else quota.get("allowed") is not False
    )
    if not allowed:
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
    if not platform_client.platform_gating_active():
        return None

    result = await platform_client.call_validate(user_id, check_quota=check_quota)
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


async def get_entitlement_statuses(
    user_id: str, entitlements: dict[str, tuple[str, str]], *, plan_changed: bool = False
) -> dict[str, Optional[dict]]:
    """Display status per entitlement, where a scope the plan lacks reads as ``limit 0``.

    ``entitlements`` maps a caller's key to ``(scope, count-quota name)``. The
    count quota alone cannot say "not on your plan": the platform reports the
    tier's count limit whether or not the scope is granted, so a UI reading only
    counts offers an upgrade the gate then refuses with 403. Scopes are read
    once for all keys; None from the platform stays unknown (fail-open), the
    same reading the gate gives it. Scopes older than ``_reread_age`` are
    read again, as the gate reads a scope a cached list lacks, so a plan just
    upgraded shows what it opens.
    """
    if not platform_client.platform_gating_active():
        return {key: None for key in entitlements}
    scopes = await _get_user_scopes(user_id, max_age=_reread_age(plan_changed))

    async def status(scope: str, quota: str) -> Optional[dict]:
        if scopes is not None and scope not in scopes:
            return {"used": 0, "limit": 0}
        return await get_capacity_status(user_id, quota)

    keys = list(entitlements)
    results = await asyncio.gather(*(status(*entitlements[k]) for k in keys))
    return dict(zip(keys, results))


# Always-on entitlement identifiers — single source for the gate, the
# reconciler probe, and the quota route.
ALWAYS_ON_SCOPE = "workspace:always_on"
ALWAYS_ON_QUOTA = "always_on"

# spec tier -> (required scope, count-quota name). Absent tiers (standard,
# unknown) are ungated. A plan has one slot for a computer bigger than
# standard, at any size its scopes reach, so the platform counts all three
# quotas over the same computers: every one held at any of these tiers.
_SPEC_ENTITLEMENTS: dict[str, tuple[str, str]] = {
    "performance": ("workspace:spec:performance", "spec_performance"),
    "max": ("workspace:spec:max", "spec_max"),
    "xlarge": ("workspace:spec:xlarge", "spec_xlarge"),
}

#: Every size a computer may have, smallest first.
SPEC_TIERS_BY_SIZE: tuple[str, ...] = get_args(ResourceTierId)


def tier_rank(tier: Optional[str]) -> Optional[int]:
    """A spec tier's place by size; None for a tier that has none."""
    return SPEC_TIERS_BY_SIZE.index(tier) if tier in SPEC_TIERS_BY_SIZE else None


async def computer_slot_granted(user_id: str, *, running: bool = False) -> bool:
    """Whether the plan definitively lets the user hold one more computer the
    deployment builds, or with ``running`` run one more, for one made for
    them rather than by them (Home's stand-in's). Reads the other way from the
    gate: unreachable, ambiguous or OSS is no, so a blip never builds or
    starts a machine nobody asked for.
    """
    if not platform_client.platform_gating_active():
        return False
    try:
        result = await platform_client.call_validate(
            user_id, check_quota="workspace" if running else COMPUTER_COUNT_QUOTA
        )
    except Exception:
        logger.warning("Could not read the computer quota for user %s", user_id, exc_info=True)
        return False
    quota = (result or {}).get("quota")
    return bool(quota) and quota.get("allowed") is True


async def plan_spec_tier(
    user_id: str, offered: Collection[str], *, current_tier: Optional[str] = None
) -> Optional[str]:
    """The largest size of ``offered`` the user's plan gives a computer now.

    For a computer sized for the user rather than by them (their Home), so it
    reads the other way from the gate: only a definitive platform answer opens
    a size. None when the plan could not be read (unreachable platform, an
    ambiguous answer, a missing quota, any error), which a caller must not take
    for standard: that would shrink a machine on a blip. Scopes are read past
    the cache, since the plan may have just changed, and never written to it:
    two reads can overlap a plan change, and the earlier one landing last
    would hand the gate a grant the plan has withdrawn. A quota with neither
    a boolean ``allowed`` nor both counts for a returned slot is ambiguous.
    ``current_tier`` is the size the computer holds now, whose slot it gives
    back for a bigger one (as ``assert_spec_allowed``). Standard in OSS mode.
    """
    if not platform_client.platform_gating_active():
        return "standard"
    try:
        result = await platform_client.call_validate(user_id)
        scopes = (result or {}).get("scopes")
        if not isinstance(scopes, list):
            return None
        for tier in reversed(SPEC_TIERS_BY_SIZE):
            entitlement = _SPEC_ENTITLEMENTS.get(tier)
            if entitlement is None or tier not in offered or entitlement[0] not in scopes:
                continue
            if tier == current_tier:
                return tier
            counted = await platform_client.call_validate(user_id, check_quota=entitlement[1])
            later = (counted or {}).get("scopes")
            if isinstance(later, list):
                # The newer list decides the smaller sizes tried after this.
                scopes = later
            quota = (counted or {}).get("quota")
            if not quota:
                return None
            allowed = (
                _returned_slot_verdict(quota) if current_tier in _SPEC_ENTITLEMENTS else None
            )
            if allowed is None:
                allowed = quota.get("allowed")
            if allowed is True:
                # The count says nothing of the scope, which may have gone
                # since the read above: the answer approving it must carry
                # it, and one whose newer list leaves it out tries smaller.
                if not isinstance(later, list):
                    return None
                if entitlement[0] in later:
                    return tier
                continue
            if allowed is not False:
                return None
    except Exception:
        logger.warning("Could not read the plan's computer size for user %s", user_id, exc_info=True)
        return None
    return "standard"


# key -> (scope, count-quota name) for every entitlement the quota route shows.
DISPLAYED_ENTITLEMENTS: dict[str, tuple[str, str]] = {
    **_SPEC_ENTITLEMENTS,
    "always_on": (ALWAYS_ON_SCOPE, ALWAYS_ON_QUOTA),
}



async def _assert_hybrid_gate(
    user_id: str,
    scope: str,
    check_quota: str,
    *,
    count: bool = True,
    returning_slot: bool = False,
    plan_changed: bool = False,
) -> None:
    """Shared workspace-entitlement gate: scope 403 then optional count 429."""
    await require_workspace_scope(user_id, scope, plan_changed=plan_changed)
    if count:
        await enforce_capacity(user_id, check_quota, returning_slot=returning_slot)


async def assert_spec_allowed(
    user_id: str, tier: str, *, current_tier: Optional[str] = None, plan_changed: bool = False
) -> None:
    """Gate a move to ``tier`` (scope 403 + count 429). Standard is never gated.

    The count quota is skipped when ``current_tier == tier`` (no new slot). A
    move from one elevated tier to another keeps the slot the computer already
    holds, which the platform counts as used, so that one is handed back
    before the count decides: Large to Medium is open on a plan whose one
    slot the Large fills, while a limit of 0 still refuses. A move up from
    standard takes a new slot. ``plan_changed``: the client is back from the
    plan page, so a list lacking the scope is read again past
    ``_SCOPE_PLAN_CHANGED_AGE``. No-op in OSS mode.
    """
    entitlement = _SPEC_ENTITLEMENTS.get(tier)
    if entitlement is None:
        return  # standard / unknown tiers are ungated
    scope, check_quota = entitlement
    await _assert_hybrid_gate(
        user_id,
        scope,
        check_quota,
        count=tier != current_tier,
        returning_slot=current_tier in _SPEC_ENTITLEMENTS,
        plan_changed=plan_changed,
    )


async def assert_always_on_allowed(user_id: str) -> None:
    """Gate enabling always-on (scope 403 + count 429). No-op in OSS mode."""
    await _assert_hybrid_gate(user_id, ALWAYS_ON_SCOPE, ALWAYS_ON_QUOTA)


def provider_scope(kind: str) -> str:
    """The scope a new computer on a non-default sandbox provider needs."""
    return f"workspace:provider:{kind}"


async def assert_provider_allowed(
    user_id: str, kind: str, *, default_kind: str
) -> None:
    """Gate creating a computer on ``kind`` (scope 403). The default kind is never gated.

    Scope only, with no count quota: the computer cap already meters every
    machine the deployment provides, and none its owner brings (``metered``).
    No-op in OSS mode, fail-open when the platform is unreachable. The
    policy's ``own_machines`` rule is asked first, of every kind including
    the default, and never fails open.
    """
    from src.server.services.account import refuse_machine_use

    await refuse_machine_use(kind, user_id)
    if kind == default_kind:
        return
    await require_workspace_scope(user_id, provider_scope(kind))


async def get_provider_allowances(
    user_id: str, kinds: Sequence[str], *, default_kind: str
) -> dict[str, Optional[bool]]:
    """Whether the user may create a computer on each kind, for display.

    None is the gate's own fail-open reading (OSS mode, an unreachable
    platform), so a picker built on this never disables a kind the gate would
    take. Scopes are read once, from the same cache the gate uses. A kind of
    machine its owner brings is False while the policy keeps work off one,
    or cannot be read, since the gate refuses both.
    """
    from ptc_agent.core.sandbox.providers import BROUGHT_KINDS
    from src.server.services.policy import OWN_MACHINES
    from src.server.services.account import policy_for

    own_refused = False
    if any(k in BROUGHT_KINDS for k in kinds):
        policy = await policy_for(user_id)
        own_refused = policy is None or not policy.allows(OWN_MACHINES)
    scopes = (
        await _get_user_scopes(user_id)
        if platform_client.platform_gating_active() and any(k != default_kind for k in kinds)
        else None
    )

    def allowed(kind: str) -> Optional[bool]:
        if own_refused and kind in BROUGHT_KINDS:
            return False
        if kind == default_kind:
            return True
        if scopes is None:
            return None
        return provider_scope(kind) in scopes

    return {kind: allowed(kind) for kind in kinds}


async def spec_grantable(
    user_id: str, tier: str, *, current_tier: Optional[str] = None
) -> bool:
    """Non-raising form of :func:`assert_spec_allowed` (full scope 403 + count 429
    gate) for service-layer callers that must not import/catch ``HTTPException``.

    Returns True when the user may hold a workspace at ``tier`` and False when the
    gate would reject (missing scope or exhausted count). ``standard``/
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
    if not platform_client.platform_gating_active():
        return False
    result = await platform_client.call_validate(user_id)
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


ChatLimit = Callable[[str], Awaitable[ChatAuthResult]]


def chat_limiter() -> ChatLimit:
    """``chat_limit_for``, handed to a turn's route rather than taken as a
    sub-dependency: the route takes it only once the caller's body is checked,
    so a refused body never holds a burst slot, and FastAPI resolves every
    sub-dependency before the dependent's own checks run."""
    return chat_limit_for


# Annotated types for cleaner endpoint signatures
ChatLimiter = Annotated[ChatLimit, Depends(chat_limiter)]
ComputerLimitCheck = Annotated[str, Depends(enforce_computer_limit)]
