"""The setup token a user's Claude Code turns sign in with: saving, removing,
and whether it works, for Settings and the model picker.

- GET    /api/v1/claude-code/status          Whether the user's Claude Code models can run
- POST   /api/v1/claude-code/sign-in         Start a sign-in that makes a setup token
- POST   /api/v1/claude-code/sign-in/code    Finish it with the code Claude's page shows
- PUT    /api/v1/claude-code/token           Save a setup token the user made themselves
- DELETE /api/v1/claude-code/token           Remove the saved token

Either way a token is checked with one minimal turn before it is saved, and
saved the same way: encrypted in the OAuth token store, never sent back.
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Response
from pydantic import BaseModel, SecretStr

from src.llms.claude_code import FEATURE, runtime_available, token_check_model
from src.server.app.oauth import OAuthRefusal, PendingSignIn, exchange_failure
from src.server.database.oauth_tokens import (
    CLAUDE_CODE_PROVIDER,
    connection_change,
    delete_oauth_tokens,
    upsert_oauth_tokens,
)
from src.server.services import claude_oauth
from src.server.services.features import user_feature_enabled
from src.server.services.llm.claude_code import TOKEN_CHECK_TTL_S, stored_row, stored_token, token_check_key
from src.server.utils.api import CurrentUserId
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/claude-code", tags=["claude-code"])

#: How long a check that could not tell stands before a read checks again.
UNKNOWN_TTL_S = 60
#: ``claude setup-token`` makes a token that lasts a year, so a saved one
#: expires within this long of being saved; when exactly, the token does not say.
TOKEN_LIFETIME = timedelta(days=365)
#: Longer than a token anyone pastes, short of one that would cost a check to refuse.
TOKEN_MAX_CHARS = 1024
#: How long a finished sign-in still answers a second paste of its code.
SAVED_MARKER_TTL_S = 120
#: The whole code exchange, which httpx bounds only per phase; well inside the change hold.
MINT_DEADLINE_S = 30
#: Strong refs to running checks: asyncio keeps only weak ones.
_checks: set[asyncio.Task[None]] = set()

#: Where the user's Claude Code models stand. ``unverified``: a token is saved
#: but the check could not reach Anthropic; its turns run, as admission lets them.
State = Literal["off", "no_token", "checking", "unverified", "signed_in", "rejected"]
#: A token's kept check, as the state it reads as.
_CHECKED: dict[object, State] = {True: "signed_in", False: "rejected", "unknown": "unverified"}


class ClaudeCodeStatus(BaseModel):
    state: State
    #: The Claude account a token made through sign-in belongs to.
    email: str | None = None
    #: When the token stops working; for a pasted one, the latest it can.
    expires_at: datetime | None = None


class TokenBody(BaseModel):
    token: SecretStr


class CodeBody(BaseModel):
    code: SecretStr


async def _check(token: str, key: str) -> State:
    """Ask Anthropic and keep the answer per token; one that could not tell is kept briefly."""
    from ptc_agent.agent.claude_code.launcher import LocalLauncher
    from ptc_agent.agent.claude_code.runtime import token_works

    cache = get_cache_client()
    try:
        works = await token_works(LocalLauncher(token=token), token_check_model())
    except Exception:
        logger.warning("[claude_code] token check failed", exc_info=True)
        await cache.set(key, "unknown", ttl=UNKNOWN_TTL_S)
        return "unverified"
    await cache.set(key, works, ttl=TOKEN_CHECK_TTL_S)
    return _CHECKED[works]


async def _check_then_release(token: str, key: str, holder: str) -> None:
    try:
        await _check(token, key)
    finally:
        await get_cache_client().release_lock(f"{key}:lock", holder)


async def _token_state(token: str) -> State:
    """The token's kept answer. On a miss one worker starts the check, and reads answer checking until it lands."""
    from ptc_agent.agent.claude_code.runtime import TOKEN_CHECK_TIMEOUT_S

    key = token_check_key(token)
    cache = get_cache_client()
    cached = await cache.get(key)
    if cached in _CHECKED:
        return _CHECKED[cached]
    holder = uuid.uuid4().hex
    leader = await cache.acquire_lock(f"{key}:lock", holder, (TOKEN_CHECK_TIMEOUT_S + 15) * 1000)
    if leader is None:
        # No Redis to keep an answer, so a check here would spend a request on
        # the user's plan at every read. The token was checked when saved.
        return "unverified"
    if leader:
        task = asyncio.create_task(_check_then_release(token, key, holder))
        _checks.add(task)
        task.add_done_callback(_checks.discard)
    return "checking"


async def _enabled(user_id: str) -> bool:
    return runtime_available() and await user_feature_enabled(user_id, FEATURE)


async def _forget(token: str | None) -> None:
    """Drop a token's kept answer, once no saved token is that token."""
    if token is not None:
        await get_cache_client().delete(token_check_key(token))


def _status(row: dict, state: State) -> ClaudeCodeStatus:
    return ClaudeCodeStatus(state=state, email=row.get("email"), expires_at=row.get("expires_at"))


async def _require_enabled(user_id: str) -> None:
    if not await _enabled(user_id):
        raise OAuthRefusal(404, "unavailable", "Claude Code is not on for your account.")


@asynccontextmanager
async def _changing(user_id: str):
    """One change to the user's token at a time: a save costs a turn on the
    plan to check, and a removal landing mid-save would be undone by it."""
    from ptc_agent.agent.claude_code.runtime import TOKEN_CHECK_TIMEOUT_S

    cache = get_cache_client()
    lock, holder = f"claude_code:token_save:{user_id}", uuid.uuid4().hex
    # A sign-in exchanges its code first, inside the same hold.
    acquired = await cache.acquire_lock(lock, holder, (TOKEN_CHECK_TIMEOUT_S + MINT_DEADLINE_S + 15) * 1000)
    if acquired is False:
        raise OAuthRefusal(409, "check_in_progress", "A token is already being checked. Try again shortly.", retryable=True)
    if acquired is None:
        # Redis did not answer: without the hold a removal could land mid-save.
        raise OAuthRefusal(
            503, "redis_unavailable", "Connecting an account is unavailable right now. Try again shortly.",
            retryable=True,
        )

    async def held() -> bool:
        """Whether the hold is still this change's. A save reads it under the
        writer lock a removal also takes, so a save whose hold lapsed while
        it ran never undoes a removal made after."""
        try:
            owner = await cache.client.get(lock)
        except Exception:
            return False
        return (owner.decode() if isinstance(owner, bytes) else owner) == holder

    try:
        yield held
    finally:
        await cache.release_lock(lock, holder)


async def _require_works(token: str, *, rejected: OAuthRefusal, retryable: bool) -> None:
    """Refuse a token Anthropic does not serve, and one the check could not
    reach Anthropic for: a token is never saved unchecked."""
    from ptc_agent.agent.claude_code.launcher import LocalLauncher
    from ptc_agent.agent.claude_code.runtime import token_works

    try:
        works = await token_works(LocalLauncher(token=token), token_check_model())
    except Exception:
        logger.warning("[claude_code] token check on save failed", exc_info=True)
        raise OAuthRefusal(
            503, "check_unavailable", "Claude could not be reached to check the token. Try again.", retryable=retryable,
        )
    if not works:
        raise rejected


async def _store(
    user_id: str, token: str, expires_at: datetime, *, held, email: str | None = None, token_uuid: str = "",
) -> ClaudeCodeStatus:
    """Save a checked token over any saved before; the row's ``account_id``
    holds the token's uuid, what logs and the UI know it by."""
    previous = await stored_row(user_id)
    if previous is not None and previous["access_token"] == token:
        # The same token saved again lasts no longer than it did.
        expires_at, email = previous["expires_at"], email or previous.get("email")
    # Before the row, so a read that finds the row finds its answer too.
    await get_cache_client().set(token_check_key(token), True, ttl=TOKEN_CHECK_TTL_S)
    async with connection_change(user_id, CLAUDE_CODE_PROVIDER) as conn:
        if not await held():
            raise OAuthRefusal(409, "check_in_progress", "A token is already being checked. Try again shortly.", retryable=True)
        await upsert_oauth_tokens(
            user_id, CLAUDE_CODE_PROVIDER, access_token=token, refresh_token="", account_id=token_uuid,
            email=email, expires_at=expires_at, conn=conn,
        )
    if previous is not None and previous["access_token"] != token:
        await _forget(previous["access_token"])
    logger.info(f"[claude_code] setup token saved for user_id={user_id} token_uuid={token_uuid or '-'}")
    return _status({"email": email, "expires_at": expires_at}, "signed_in")


def _sign_in(user_id: str) -> PendingSignIn:
    return PendingSignIn(f"claude_code:sign_in:{user_id}")


@router.get("/status", response_model=ClaudeCodeStatus)
async def get_status(x_user_id: CurrentUserId):
    if not await _enabled(x_user_id):
        return ClaudeCodeStatus(state="off")
    row = await stored_row(x_user_id)
    if row is None:
        return ClaudeCodeStatus(state="no_token")
    return _status(row, await _token_state(row["access_token"]))


@router.post("/sign-in")
async def start_sign_in(x_user_id: CurrentUserId):
    """Start a sign-in as ``claude setup-token`` does; the page Claude shows
    after it gives the code ``/sign-in/code`` takes."""
    await _require_enabled(x_user_id)
    started = await _sign_in(x_user_id).start(claude_oauth.authorize_setup_token)
    logger.info(f"[claude_code] sign-in started for user_id={x_user_id}")
    return started


@router.post("/sign-in/code", response_model=ClaudeCodeStatus)
async def finish_sign_in(body: CodeBody, x_user_id: CurrentUserId):
    """Exchange the pasted code for a setup token, check it, and save it.

    The grant also carries a refresh token, dropped as ``claude setup-token``
    drops it. A code works once: one Claude refused, or one spent on a token
    that then failed its check, ends the sign-in, and the user starts again.
    An exchange failure is read as claude-oauth reads it (``exchange_failure``):
    one asking again can fix keeps the sign-in, any other ends it.
    """
    await _require_enabled(x_user_id)
    raw = body.code.get_secret_value().strip()
    if not raw:
        raise OAuthRefusal(400, "code_missing", "Paste the code from Claude's page first.", retryable=True)
    flow = _sign_in(x_user_id)
    async with _changing(x_user_id) as held:
        try:
            stored, verifier, state = await flow.read()
        except OAuthRefusal:
            # A second paste of the code the first one saved with.
            if await flow.saved(raw):
                return await get_status(x_user_id)
            raise
        code = flow.code(raw, state)
        try:
            async with asyncio.timeout(MINT_DEADLINE_S):
                minted = await claude_oauth.mint_setup_token(code, state, verifier)
        except claude_oauth.UnexpectedGrant as e:
            await flow.claim(stored)
            logger.error(f"[claude_code] sign-in got an unexpected grant for user_id={x_user_id}: {e}")
            raise OAuthRefusal(502, "unexpected_grant", "Claude answered in a way LangAlpha did not expect. Sign in again.", retryable=False)
        except Exception as e:
            logger.warning(f"[claude_code] sign-in exchange failed for user_id={x_user_id}: {type(e).__name__}")
            if exchange_failure("Claude", e).error["retryable"]:
                raise OAuthRefusal(503, "check_unavailable", "Claude could not be reached. Try again.", retryable=True)
            await flow.claim(stored)
            raise OAuthRefusal(
                400, "code_rejected", "Claude did not accept this code. Codes work once, so sign in again.",
                retryable=False,
            )
        # The code is spent: whatever happens next, only a new sign-in helps.
        if not await flow.claim(stored):
            raise PendingSignIn.expired()
        await _require_works(
            minted.access_token, retryable=False, rejected=OAuthRefusal(
                400, "token_rejected", "Claude made a token but refused a request on it. Check that your plan includes Claude Code.",
                retryable=False,
            ),
        )
        status = await _store(
            x_user_id, minted.access_token, minted.expires_at, held=held, email=minted.email, token_uuid=minted.token_uuid,
        )
        await flow.mark_saved(state, SAVED_MARKER_TTL_S)
    return status


@router.put("/token", response_model=ClaudeCodeStatus)
async def save_token(body: TokenBody, x_user_id: CurrentUserId):
    """Save a setup token the user made with ``claude setup-token``, once
    Anthropic serves it."""
    await _require_enabled(x_user_id)
    token = body.token.get_secret_value().strip()
    if not token:
        raise OAuthRefusal(400, "token_missing", "Paste the setup token first.", retryable=True)
    rejected = OAuthRefusal(
        400, "token_rejected", "Claude did not accept this token. Make a new one and paste it again.", retryable=True,
    )
    if len(token) > TOKEN_MAX_CHARS or not token.isprintable() or " " in token:
        raise rejected
    async with _changing(x_user_id) as held:
        await _require_works(token, rejected=rejected, retryable=True)
        return await _store(x_user_id, token, datetime.now(timezone.utc) + TOKEN_LIFETIME, held=held)


@router.delete("/token", status_code=204)
async def remove_token(x_user_id: CurrentUserId):
    """Remove the saved token, whether or not Claude Code is still on for the user."""
    async with _changing(x_user_id):
        try:
            previous = await stored_token(x_user_id)
        except Exception:
            # Unreadable without the key it was saved under; the row goes all
            # the same, and its check answer lapses on its own.
            logger.warning(f"[claude_code] saved token unreadable on removal for user_id={x_user_id}", exc_info=True)
            previous = None
        async with connection_change(x_user_id, CLAUDE_CODE_PROVIDER) as conn:
            await delete_oauth_tokens(x_user_id, CLAUDE_CODE_PROVIDER, conn=conn)
        await _forget(previous)
    return Response(status_code=204)
