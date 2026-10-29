"""
FastAPI dependencies for usage limit enforcement.

Gate hierarchy:
  HOST_MODE ("oss" | "platform")        — master switch; OSS mode skips all gates.
  AUTH_SERVICE_URL                       — platform quota service; guards
                                           credit/workspace limits and access tier
                                           checks.  Can be absent even when
                                           HOST_MODE is "platform" (partial deploy).

Fail-open: when the platform service is unreachable, requests are allowed,
except by the credit gate and by a plan that is on record nowhere, which both
answer 503 rather than guess.
"""

import asyncio
import logging
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Optional

import httpx
from fastapi import Depends, HTTPException

from src.config.settings import HOST_MODE, AUTH_SERVICE_URL
from src.server.utils.api import get_current_user_id

logger = logging.getLogger(__name__)


class QuotaServiceUnavailable(Exception):
    """The quota service could not be asked at all.

    Distinct from it answering "no": a denial arrives as a 200 whose ``quota``
    says ``allowed: false``. Everything else (transport failure, 5xx, a token
    the service rejects) means we have no verdict, and only the caller knows
    whether proceeding without one is acceptable.
    """


# The quota service deploys in place and refuses connections while it restarts,
# for about five seconds. Strict callers retry on this schedule, 7.5 s of waiting
# in all, so a check that lands in that gap waits it out; an outage that outlasts
# it is real, and they fail closed. Deterministic 4xx answers (a rejected service
# token, a malformed request) are not retried: they will not change.
#
# Only ``strict`` callers retry. For everyone else an unanswered check already
# means "carry on", so more attempts buy a slightly better answer at the cost of
# seconds per call, including inside the always-on reconciler, which probes once
# per owner. The caller that pays for accuracy is the one that blocks on it.
_VALIDATE_RETRY_DELAYS = (0.5, 1.0, 2.0, 2.0, 2.0)  # seconds before each retry
# The schedule assumes a refused connection, which fails at once. A service that
# accepts and then hangs costs the client timeout per attempt, so no wait starts
# that would end past this, and only the attempt already under way runs over.
_VALIDATE_RETRY_WINDOW = 8.0  # seconds from the first attempt

# Long enough that a retry lands after an in-place restart, short enough that a
# client which honours it does not look hung.
_SERVICE_UNAVAILABLE_RETRY_AFTER = 15  # seconds

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

# -- the declared surface other modules call this one through ------------------
# The three below are the whole of how this service talks to the quota service:
# whose client, whether to talk at all, and what it authenticates with. They are
# public because the credit gate's port needs the same three, and a borrowed
# underscore name is a dependency no refactor here can see.

# Shared httpx client (created lazily, async-safe)
_http_client: Optional[httpx.AsyncClient] = None
_http_client_lock = asyncio.Lock()


async def get_http_client() -> httpx.AsyncClient:
    """The shared client. One per process, closed at shutdown."""
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


def platform_gating_active() -> bool:
    """True when platform scope/quota gates should run (not OSS and an auth URL set)."""
    return HOST_MODE != "oss" and bool(AUTH_SERVICE_URL)


def service_headers(user_id: Optional[str] = None) -> dict:
    """Headers for a service-to-service call to the quota service.

    The token is a shared secret, not a JWT, and is omitted when unset — which
    is a deployment fault rather than a mode: every gated endpoint answers 401
    without it. Callers that fail open on a non-200 cannot tell that apart from
    an outage, so they check for it themselves.
    """
    headers = {}
    if user_id:
        headers["X-User-Id"] = user_id
    token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
    if token:
        headers["X-Service-Token"] = token
    return headers


def service_token_missing() -> bool:
    """Platform gating is on but nothing is configured to authenticate with."""
    return platform_gating_active() and not os.getenv(
        "INTERNAL_SERVICE_TOKEN", ""
    ).strip()


def quota_service_unavailable() -> HTTPException:
    """The 503 for a request the quota service gave no verdict on.

    One shape for every gate that refuses rather than guess, so a client shows
    an outage and retries instead of reading the status as a quota denial or a
    missing plan.
    """
    return HTTPException(
        status_code=503,
        detail={
            # Same wording the quota service uses for its own guard, so the
            # user reads one sentence whichever side went dark.
            "message": "Service temporarily unavailable. Please try again shortly.",
            "type": "service_unavailable",
            "retry_after": _SERVICE_UNAVAILABLE_RETRY_AFTER,
        },
        headers={"Retry-After": str(_SERVICE_UNAVAILABLE_RETRY_AFTER)},
    )


@dataclass
class ChatAuthResult:
    """Auth + tier data collected by enforce_chat_limit for downstream gates."""
    user_id: str
    is_byok: bool = False
    has_oauth: bool = False
    # -1 = no platform access, 0+ = tier level, None = the platform could not
    # be asked and no membership is on record for this user.
    access_tier: Optional[int] = -1
    burst_slot_id: Optional[str] = None  # ZSET member held for this request



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
    tier: Optional[int] = -1
    if HOST_MODE != "oss" and AUTH_SERVICE_URL and not is_byok and not has_oauth:
        tier = await _fetch_platform_tier(user_id)

    return ChatAuthResult(
        user_id=user_id,
        is_byok=is_byok,
        has_oauth=has_oauth,
        access_tier=tier,
        burst_slot_id=burst_result.get("slot_id"),
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
    if not platform_gating_active():
        return

    try:
        result = await _call_validate_for_user(
            user_id, check_quota="chat", byok=byok, surface=surface, strict=True
        )
    except QuotaServiceUnavailable as e:
        logger.error("Credit gate closed — quota service gave no verdict (%s)", e)
        raise quota_service_unavailable() from e

    if result is None:
        return  # OSS mode reached this only via a race on HOST_MODE; nothing to check.

    quota = result.get("quota")
    if not quota or quota.get("allowed", True):
        return

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


async def _call_validate_for_user(
    user_id: str,
    check_quota: Optional[str] = None,
    byok: bool = False,
    strict: bool = False,
    surface: str | None = None,
) -> Optional[dict]:
    """POST to the auth/quota service at /api/auth/validate.

    Returns None in OSS mode. When the service cannot be reached, ``strict``
    callers get :class:`QuotaServiceUnavailable` once the retry schedule runs
    out, and everyone else gets None on the first failure, which most of them
    read as "carry on". See the fail-open notes at those call sites for why that
    is safe there and not at the credit gate.
    """
    if not platform_gating_active():
        return None

    client = await get_http_client()
    headers = service_headers(user_id)

    body = {}
    if check_quota:
        body["check_quota"] = check_quota
    if byok:
        body["byok"] = True
    if surface:
        body["surface"] = surface

    url = f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/validate"
    reason = "unknown"
    delays = _VALIDATE_RETRY_DELAYS if strict else ()
    give_up_at = time.monotonic() + _VALIDATE_RETRY_WINDOW

    for attempt in range(len(delays) + 1):
        if attempt:
            delay = delays[attempt - 1]
            if time.monotonic() + delay > give_up_at:
                break
            await asyncio.sleep(delay)
        try:
            resp = await client.post(url, json=body if body else None, headers=headers)
        except Exception as e:
            reason = f"unreachable: {e}"
            logger.warning("auth/quota service unreachable (attempt %d): %s", attempt + 1, e)
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                # A 200 we cannot read is still no verdict: an interposed proxy
                # serving its own HTML, a truncated body. It has to rejoin the
                # no-verdict path here, or it escapes as an unhandled 500 and
                # the gate that exists to fail closed fails differently instead.
                reason = f"unparseable 200: {e}"
                logger.warning(
                    "auth/quota service returned an unreadable 200 (attempt %d): %s",
                    attempt + 1, e,
                )
                continue

        reason = f"HTTP {resp.status_code}"
        logger.warning(
            "auth/quota service validate returned %d (attempt %d): %s",
            resp.status_code, attempt + 1, resp.text[:200],
        )
        if resp.status_code < 500:
            break  # deterministic answer; a retry returns the same thing

    if strict:
        raise QuotaServiceUnavailable(reason)
    return None


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
    if not platform_gating_active():
        return

    result = await _call_validate_for_user(user_id, check_quota=check_quota)

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
# Platform membership (access tier + plan display name)
# ---------------------------------------------------------------------------

_PLATFORM_MEMBERSHIP_CACHE_TTL = 300  # 5 minutes

# The last membership the platform gave, kept to stand in while it cannot be
# asked. Long, because it is read only during an outage and the credit gate
# still asks about every turn: a plan that changed meanwhile is wrong for the
# length of the outage at most, and spends nothing the gate did not allow.
_PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL = 30 * 24 * 3600  # 30 days

# After a failed ask, requests for that user stop asking for this long and read
# the last-known membership instead, so a down or hanging platform is not called
# on every request. Matches the Retry-After an unknown membership answers with,
# so a client that honours it asks again once the pause is over.
_PLATFORM_MEMBERSHIP_PAUSE_TTL = _SERVICE_UNAVAILABLE_RETRY_AFTER


def platform_membership_cache_key(user_id: str) -> str:
    return f"platform_membership:{user_id}"


def _membership_last_known_key(user_id: str) -> str:
    return f"platform_membership:last_known:{user_id}"


def _membership_pause_key(user_id: str) -> str:
    return f"platform_membership:unreachable:{user_id}"


def _is_membership(value: object) -> bool:
    return isinstance(value, dict) and "access_tier" in value


async def _fetch_platform_membership(
    user_id: str, *, refresh: bool = False, recheck: bool = False
) -> Optional[dict]:
    """The user's platform membership: ``{"access_tier", "plan_display_name"}``.

    ``access_tier`` -1 is the platform's own answer that the user has no access.
    None means there is no answer: the platform could not be asked and has not
    answered for this user within the last-known TTL. An outage is not a plan,
    so no caller may read None as -1. While the platform is unreachable the last
    membership it gave stands in. ``refresh`` asks now, past the 5-minute cache
    and any pause, for a plan known to have changed. ``recheck`` asks past the
    cache only, for one that may have, and leaves the cache to the user's other
    reads meanwhile. No-op in OSS mode.
    """
    if not platform_gating_active():
        return {"access_tier": -1, "plan_display_name": None}

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    cached, last_known, paused = await cache.mget([
        platform_membership_cache_key(user_id),
        _membership_last_known_key(user_id),
        _membership_pause_key(user_id),
    ])
    if not _is_membership(last_known):
        last_known = None
    if refresh:
        # Dropped, not only skipped: should this ask fail, later requests must
        # not take the plan it was asked to replace back from the cache.
        await cache.delete(platform_membership_cache_key(user_id))
    else:
        if not recheck and _is_membership(cached):
            # An entry an older worker wrote has no last-known record beside it.
            # Not from a -1, which an older worker also caches for a failed ask.
            if last_known is None and int(cached.get("access_tier", -1)) >= 0:
                await cache.set(
                    _membership_last_known_key(user_id),
                    cached,
                    ttl=_PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL,
                )
            return cached
        if paused:
            return last_known

    result = await _call_validate_for_user(user_id)
    if result is not None:
        membership = {
            "access_tier": int(result.get("access_tier", -1)),
            "plan_display_name": result.get("plan_display_name"),
        }
        await cache.set_many([
            (platform_membership_cache_key(user_id), membership,
             _PLATFORM_MEMBERSHIP_CACHE_TTL),
            (_membership_last_known_key(user_id), membership,
             _PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL),
        ])
        return membership

    # The failure is recorded beside the membership, never in its place: a
    # cached -1 here is what used to send a paying user to the setup wizard.
    await cache.set(
        _membership_pause_key(user_id), True, ttl=_PLATFORM_MEMBERSHIP_PAUSE_TTL
    )
    return last_known


async def _fetch_platform_tier(user_id: str, *, recheck: bool = False) -> Optional[int]:
    """Only the access tier, from the same cache; None when it is unknown."""
    membership = await _fetch_platform_membership(user_id, recheck=recheck)
    if membership is None:
        return None
    return int(membership.get("access_tier", -1))


async def get_platform_access_tier(user_id: str) -> int | None:
    """Public access tier for plan gates: the tier, or None for no access.

    Normalizes the ``-1`` no-access sentinel to None so callers can fail closed
    on ``tier is None`` instead of leaking the sentinel into tier comparisons.
    An unknown tier is None too: a plan gate stays shut on a plan it cannot
    confirm, which costs a feature for the length of an outage, not a turn.
    """
    tier = await _fetch_platform_tier(user_id)
    return tier if tier is not None and tier >= 0 else None


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
        if not platform_gating_active():
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
    if not platform_gating_active():
        return
    scopes = await _get_user_scopes(user_id)
    if scopes is not None and scope not in scopes:
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


def _allowed_returning_a_slot(quota: dict) -> bool:
    """Whether the quota allows one more once a slot the caller holds is given back.

    The platform's ``allowed`` counts that slot as taken, so the counts decide
    here instead: one fewer used, a limit of 0 still the plan leaving it out,
    and -1 unlimited. Without both counts the platform's own answer stands.
    """
    used, limit = _extract_capacity(quota)
    if used is None or limit is None:
        return quota.get("allowed") is not False
    limit = int(limit)
    return limit == -1 or (limit > 0 and int(used) - 1 < limit)


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
    if not platform_gating_active():
        return

    result = await _call_validate_for_user(user_id, check_quota=check_quota)
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
    if not platform_gating_active():
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


async def get_entitlement_statuses(
    user_id: str, entitlements: dict[str, tuple[str, str]]
) -> dict[str, Optional[dict]]:
    """Display status per entitlement, where a scope the plan lacks reads as ``limit 0``.

    ``entitlements`` maps a caller's key to ``(scope, count-quota name)``. The
    count quota alone cannot say "not on your plan": the platform reports the
    tier's count limit whether or not the scope is granted, so a UI reading only
    counts offers an upgrade the gate then refuses with 403. Scopes are read
    once for all keys; None from the platform stays unknown (fail-open), the
    same reading the gate gives it.
    """
    if not platform_gating_active():
        return {key: None for key in entitlements}
    scopes = await _get_user_scopes(user_id)

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
) -> None:
    """Shared workspace-entitlement gate: scope 403 then optional count 429."""
    await require_workspace_scope(user_id, scope)
    if count:
        await enforce_capacity(user_id, check_quota, returning_slot=returning_slot)


async def assert_spec_allowed(
    user_id: str, tier: str, *, current_tier: Optional[str] = None
) -> None:
    """Gate a move to ``tier`` (scope 403 + count 429). Standard is never gated.

    The count quota is skipped when ``current_tier == tier`` (no new slot). A
    move from one elevated tier to another keeps the slot the computer already
    holds, which the platform counts as used, so that one is handed back
    before the count decides: Large to Medium is open on a plan whose one
    slot the Large fills, while a limit of 0 still refuses. A move up from
    standard takes a new slot. No-op in OSS mode.
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
    No-op in OSS mode, fail-open when the platform is unreachable.
    """
    if kind == default_kind:
        return
    await require_workspace_scope(user_id, provider_scope(kind))


async def get_provider_allowances(
    user_id: str, kinds: Sequence[str], *, default_kind: str
) -> dict[str, Optional[bool]]:
    """Whether the user may create a computer on each kind, for display.

    None is the gate's own fail-open reading (OSS mode, an unreachable
    platform), so a picker built on this never disables a kind the gate would
    take. Scopes are read once, from the same cache the gate uses.
    """
    scopes = (
        await _get_user_scopes(user_id)
        if platform_gating_active() and any(k != default_kind for k in kinds)
        else None
    )

    def allowed(kind: str) -> Optional[bool]:
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
    if not platform_gating_active():
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
ComputerLimitCheck = Annotated[str, Depends(enforce_computer_limit)]
