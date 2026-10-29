"""Keeping a subscription connection (Codex, Claude, Grok) usable, and saying
so when it is not.

A refresh failure is one of two things. Permanent: the provider will never
accept this refresh token again (revoked, expired, or, for Codex's single-use
tokens, already spent), and only the user signing in again fixes it; the row
is flagged so the status endpoint and every turn say so. Transient: the
network, the provider's 5xx or 429, a refresh another request holds the lock
for; nothing is written and the next use tries again. A third, xAI's: the
plan has no API access (``OAuthPlanNoApiAccess``), which no sign-in fixes, so
nothing is flagged and the refusal says so instead.
"""

import asyncio
import logging
import secrets
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Mapping, Optional

import httpx

from src.server.database.oauth_tokens import (
    expire_oauth_access_token,
    get_oauth_tokens,
    mark_oauth_needs_reauth,
    replace_refreshed_tokens,
    upsert_oauth_tokens,
)
from src.server.services.llm.provider_models import invalidate_available_models
from src.server.services.oauth_protocol import (
    OAuthPlanNoApiAccess,
    TokenSet,
    upstream_error_code,
)
from src.server.services.oauth_providers import SUBSCRIPTIONS
from src.server.services.oauth_refresh_wait import (
    REFRESH_DEADLINE_S,
    REFRESH_LOCK_TTL_MS,
    REFRESH_SAVE_DEADLINE_S,
    await_refreshed_tokens,
)

logger = logging.getLogger(__name__)

class OAuthReauthRequired(Exception):
    """The stored connection cannot be refreshed; the user must sign in again."""

    def __init__(self, provider: str, code: Optional[str]):
        super().__init__(f"{provider} needs sign-in ({code})")
        self.provider = provider
        self.code = code


class OAuthRefreshUnavailable(Exception):
    """The access token has expired and could not be refreshed right now."""

    def __init__(self, provider: str, reason: str):
        super().__init__(f"{provider} refresh unavailable ({reason})")
        self.provider = provider
        self.reason = reason


ConnectionState = Literal["connected", "needs_reauth", "disconnected"]


def connection_state(row: Optional[Mapping[str, Any]]) -> ConnectionState:
    """Whether a stored connection (a ``user_oauth_tokens`` row or None) still
    works. Its access token's expiry is no part of it: that is refreshed on
    use, so a past one alone is not a fault."""
    if not row:
        return "disconnected"
    return "needs_reauth" if row.get("needs_reauth_at") else "connected"


async def store_tokens(user_id: str, provider: str, tokens: TokenSet, conn=None) -> None:
    await upsert_oauth_tokens(
        user_id=user_id,
        provider=provider,
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        account_id=tokens.account_id,
        email=tokens.email,
        plan_type=tokens.plan_type,
        expires_at=tokens.expires_at,
        conn=conn,
    )


async def connection_changed(user_id: str) -> None:
    """After a connect or disconnect: the model lists the user's credentials
    were read with are stale."""
    await invalidate_available_models(user_id)


def _refresh_failure_label(exc: BaseException) -> str:
    """A log label for a refresh failure that never carries provider text."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = upstream_error_code(exc.response)
        return f"status={exc.response.status_code} code={code}"
    return type(exc).__name__


def _refresh_due(subscription, held: TokenSet) -> bool:
    return held.expires_at <= datetime.now(timezone.utc) + subscription.refresh_buffer(held)


async def _what_replaced_it(user_id: str, provider: str) -> Optional[TokenSet]:
    """The connection stored now, after it changed under a refresh: None once
    disconnected, the new tokens while they last."""
    current = await get_oauth_tokens(user_id, provider)
    if not current:
        return None
    if current.get("needs_reauth_at"):
        raise OAuthReauthRequired(provider, current.get("last_error_code"))
    tokens = TokenSet.from_row(current)
    if tokens.expires_at > datetime.now(timezone.utc):
        return tokens
    raise OAuthRefreshUnavailable(provider, "refresh_failed")


async def valid_token(user_id: str, provider: str) -> Optional[TokenSet]:
    """A usable token for ``provider``, refreshing it if needed.

    None when the user has no connection. Raises ``OAuthReauthRequired`` for
    a flagged connection, or one whose refresh fails for good (flagging it),
    ``OAuthRefreshUnavailable`` when the token has expired and a refresh
    could not be had right now, and passes ``OAuthPlanNoApiAccess`` from the
    refresh through unflagged. A Redis lock keeps one refresh per connection
    at a time, since Codex refresh tokens are single use; without Redis the
    held token serves until it expires, and only then is refreshed
    uncoordinated.
    """
    subscription = SUBSCRIPTIONS[provider]
    row = await get_oauth_tokens(user_id, provider)
    if not row:
        return None
    if row.get("needs_reauth_at"):
        raise OAuthReauthRequired(provider, row.get("last_error_code"))
    held = TokenSet.from_row(row)
    if not _refresh_due(subscription, held):
        return held

    if not held.refresh_token:
        await mark_oauth_needs_reauth(
            user_id, provider, refresh_token="", error_code="missing_refresh_token"
        )
        raise OAuthReauthRequired(provider, "missing_refresh_token")

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    lock_key = f"oauth:refresh:{user_id}:{provider}"
    # Released only by its owner, so a refresh that outlives the lock cannot
    # free the next holder's.
    lock_token = secrets.token_hex(16)
    acquired = await cache.acquire_lock(lock_key, lock_token, REFRESH_LOCK_TTL_MS)
    if acquired is None and held.expires_at > datetime.now(timezone.utc):
        # Redis can't coordinate, and the early window is when concurrent
        # turns would spend the same single-use refresh token: the held
        # token still works, so none is spent until it no longer does.
        return held
    if acquired is False:
        # Another request is refreshing: use what it stores.
        waited = await await_refreshed_tokens(user_id, provider)
        if waited and waited.get("needs_reauth_at"):
            raise OAuthReauthRequired(provider, waited.get("last_error_code"))
        if waited:
            return TokenSet.from_row(waited)
        raise OAuthRefreshUnavailable(provider, "refresh_in_progress")

    try:
        if acquired:
            # A refresh that landed between the read above and the lock has
            # spent the refresh token read there (single use for Codex), and
            # left a token that needs none.
            row = await get_oauth_tokens(user_id, provider)
            if not row:
                return None
            if row.get("needs_reauth_at"):
                raise OAuthReauthRequired(provider, row.get("last_error_code"))
            held = TokenSet.from_row(row)
            if not _refresh_due(subscription, held):
                return held
        try:
            async with asyncio.timeout(REFRESH_DEADLINE_S):
                fresh = await subscription.refresh(held.refresh_token)
            # A refresh answer may name no identity; the stored one stands.
            fresh = replace(
                fresh,
                account_id=fresh.account_id or held.account_id,
                email=fresh.email or held.email,
                plan_type=fresh.plan_type or held.plan_type,
            )
        except OAuthPlanNoApiAccess:
            logger.warning(
                f"[oauth] {provider} refresh refused for user_id={user_id}: "
                f"plan has no API access"
            )
            raise
        except Exception as e:
            label = _refresh_failure_label(e)
            code = subscription.permanent_failure(e)
            if code is None:
                logger.warning(
                    f"[oauth] {provider} refresh failed (transient) "
                    f"for user_id={user_id}: {label}"
                )
                # The stored token may still have a few minutes in it.
                if held.expires_at > datetime.now(timezone.utc):
                    return held
                raise OAuthRefreshUnavailable(provider, "refresh_failed") from e
            logger.warning(
                f"[oauth] {provider} refresh rejected for user_id={user_id}: {label}"
            )
            if await mark_oauth_needs_reauth(
                user_id, provider, refresh_token=held.refresh_token, error_code=code
            ):
                raise OAuthReauthRequired(provider, code) from e
            # The stored token changed under us (a refresh or a reconnect
            # won the race), so the rejection was of a token no longer held.
            return await _what_replaced_it(user_id, provider)
        # Apart from the refresh: the provider has spent the old refresh token
        # by now (single use for Codex), so a failed save is not a failed
        # refresh, and the fresh token still serves this turn.
        try:
            saved = await _save_refreshed(user_id, provider, held, fresh)
        except Exception:
            logger.exception(f"[oauth] {provider} refreshed but not saved for user_id={user_id}")
            return fresh
        if not saved:
            # Disconnected or reconnected while the refresh ran: what the
            # user did since stands, so this turn uses that.
            logger.info(f"[oauth] {provider} connection changed during refresh for user_id={user_id}")
            return await _what_replaced_it(user_id, provider)
        logger.debug(f"[oauth] {provider} refreshed tokens for user_id={user_id}")
        return fresh
    finally:
        if acquired:
            await cache.release_lock(lock_key, lock_token)


_SAVE_RETRY_DELAYS_S = (0.5, 1.5)


async def _save_refreshed(user_id: str, provider: str, held: TokenSet, fresh: TokenSet) -> bool:
    """Store a refresh's tokens, retrying a failed write while the lease holds.

    The refresh token left stored otherwise is the one just spent, which the
    next refresh would present and be refused for good. A write that landed
    but reported failure is safe to repeat: its guard no longer matches, so
    the retry answers False and the caller reads back what was stored.
    """
    def replace():
        return replace_refreshed_tokens(
            user_id, provider,
            spent_refresh_token=held.refresh_token,
            access_token=fresh.access_token,
            refresh_token=fresh.refresh_token,
            account_id=fresh.account_id,
            email=fresh.email,
            plan_type=fresh.plan_type,
            expires_at=fresh.expires_at,
        )

    async with asyncio.timeout(REFRESH_SAVE_DEADLINE_S):
        for delay in _SAVE_RETRY_DELAYS_S:
            try:
                return await replace()
            except Exception:
                logger.warning(f"[oauth] {provider} refreshed tokens not saved, retrying for user_id={user_id}")
                await asyncio.sleep(delay)
        return await replace()


# --- A model call that refused a subscription's token ---------------------------


def _model_provider(model: Any) -> Optional[str]:
    if not isinstance(model, str) or not model:
        return None
    from src.llms.llm import LLM as LLMFactory

    return (LLMFactory.get_model_config().get_model_config(model) or {}).get("provider")


def _token_refusal(attempt: Any, provider: Optional[str]) -> Optional[tuple[str, int]]:
    """``(provider, status)`` when one model call ran on a subscription and was
    refused for its access token itself, which a refresh may fix, rather than
    for what the account may do; else None. ``provider`` names the call's
    provider where the manifest does not list its model (a custom model on a
    subscription)."""
    if not isinstance(attempt, dict):
        return None
    status = attempt.get("status_code")
    if status not in (401, 403):
        return None
    subscription = SUBSCRIPTIONS.get(provider or _model_provider(attempt.get("model")))
    if subscription is None or not subscription.token_refused(status, str(attempt.get("error") or "")):
        return None
    return subscription.id, status


def subscription_token_refusal(trace: Optional[dict]) -> Optional[tuple[str, int]]:
    """The token refusal of a turn's primary model call, from the attempt trace
    ``ModelResilienceMiddleware`` attaches to the failure
    (``stream_errors.find_resilience_trace``), whose ``provider``, when set,
    names the primary's provider."""
    attempted = trace.get("attempted_models") if trace else None
    if not (isinstance(attempted, list) and attempted):
        return None
    return _token_refusal(attempted[0], trace.get("provider"))


def failed_model_calls(trace: Optional[dict], fallbacks: Any) -> list[dict]:
    """Every model call of a turn that failed, as ``{model, status_code, error}``.

    A turn that failed carries them all on its trace. One a fallback answered
    raised nothing, so its failures are read off the main lane's
    ``model_fallback`` ui records as the run streamed them, each naming the
    model it left and why.
    """
    attempted = trace.get("attempted_models") if trace else None
    calls = [a for a in attempted if isinstance(a, dict)] if isinstance(attempted, list) else []
    for record in fallbacks if isinstance(fallbacks, list) else ():
        props = record.get("props") if isinstance(record, dict) else None
        if isinstance(props, dict):
            calls.append({
                "model": props.get("from_model"),
                "status_code": props.get("status_code"),
                "error": props.get("error"),
            })
    return calls


async def note_token_refusals(
    user_id: Optional[str],
    calls: list[dict],
    provider_of: Callable[[Any], Optional[str]] = lambda model: None,
) -> None:
    """After a turn's model calls, expire the stored access token of every
    subscription that refused its token, so the next use refreshes it.

    Any call counts, a fallback's as much as the primary's: a subscription
    later in the fallback list is held on its own token. A refusal alone does
    not say the connection is gone: the access token may have been revoked or
    outlived while the refresh token still works, which is how the official
    Codex CLI recovers from a 401. So the row is not flagged here; the refresh
    that follows asks the token endpoint, and only its answer flags. Without
    this, a revoked token whose stated expiry is days away would be handed to
    every turn until then.
    """
    if not user_id:
        return
    refused = {}
    for call in calls:
        refusal = _token_refusal(call, provider_of(call.get("model")))
        if refusal is not None:
            refused.setdefault(*refusal)
    for provider, status in refused.items():
        if await expire_oauth_access_token(user_id, provider):
            logger.info(
                f"[oauth] {provider} access token refused ({status}) for user_id={user_id}; "
                f"the next turn refreshes it"
            )
