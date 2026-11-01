"""Changing a setup token, wherever it is kept: one flow over a ``TokenHome``.

A token reaches the backend from a sign-in (the code Claude's page shows,
exchanged as ``claude setup-token`` exchanges it) or a paste. Either way it
is checked with one minimal turn before it is kept, and one change runs at a
time per home, under a hold: a check costs a request on the user's plan, and
a removal landing mid-save would be undone by it. A home differs only in
where the token goes and when it is checked: the saved token is checked
first and then stored, a machine's is staged beside the computer's token
file, checked there, and put in its place only once it serves.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel

from src.llms.claude_code import FEATURE, backend_available, is_token_text, rows_available
from src.server.services.oauth_signin import OAuthRefusal, PendingSignIn, exchange_failure
from src.server.models.claude_code import CodeBody, TokenBody
from src.server.services import claude_oauth
from src.server.services.features import user_feature_enabled
from src.utils.cache.redis_cache import get_cache_client

from .checks import CHECK_CLEANUP_S, CHECK_S, MARGIN_S, Held, holds, require_works

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.launcher import Launcher

logger = logging.getLogger(__name__)

#: ``claude setup-token`` makes a token that lasts a year, so a saved one
#: expires within this long of being saved; when exactly, the token does not say.
TOKEN_LIFETIME = timedelta(days=365)
#: Longer than a token anyone pastes, short of one that would cost a check to refuse.
TOKEN_MAX_CHARS = 1024
#: How long a finished sign-in still answers a second paste of its code.
SAVED_MARKER_TTL_S = 120
#: The whole code exchange, which httpx bounds only per phase.
MINT_S = 30
#: The longest a dial, a write to or a removal from a machine may take, and a
#: removal of the saved token.
MACHINE_IO_S = 30
#: Why Claude Code is refused where an organization's managed Claude Code settings are installed.
POLICY_BLOCKED = (
    "Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here."
)


class TokenHome[Status: BaseModel](Protocol):
    """Where a token is kept, as the flow changes it, and ``Status`` how
    Settings shows where it stands.

    ``lock`` keeps changes to one at a time, ``sign_in`` is the sign-in
    whose token comes here, ``who`` names it in logs, ``place_s`` is
    what placing and taking a token back spend beyond its check, and
    ``remove_s`` what a removal spends.
    """

    lock: str
    sign_in: PendingSignIn
    who: str
    place_s: int
    remove_s: int

    async def ready(self, *, install: bool = True) -> None:
        """Anything the change needs before its hold: reaching a machine, and
        with ``install`` its CLI."""

    def placed(self, token: str, *, held: Held, retryable: bool) -> AbstractAsyncContextManager[Launcher]:
        """The token where its check can sign in with it, yielding that check's
        launcher; it is kept in place only once the check passes, and one the
        check refuses or cannot vouch for never replaces the token in use."""

    async def keep(
        self, token: str, *, held: Held, email: str | None, expires_at: datetime, token_uuid: str,
    ) -> Status:
        """Keep a checked token, unless the hold lapsed; where it stands now."""

    async def current(self) -> Status:
        """Where the token stands as last known, for a second paste of a sign-in already saved."""

    async def remove(self, *, held: Held) -> None:
        """Remove the token, under the hold, asked before what it keeps of the removal."""


async def rows_on(user_id: str) -> bool:
    """Whether the user's Claude Code models are on: the server offers them and the user opted in."""
    return rows_available() and await user_feature_enabled(user_id, FEATURE)


async def backend_on(user_id: str) -> bool:
    """Whether they are on and run on the backend's own CLI, on the saved token."""
    return backend_available() and await user_feature_enabled(user_id, FEATURE)


def require(on: bool, why: str = "Claude Code is not on for your account.") -> None:
    if not on:
        raise OAuthRefusal(404, "unavailable", why)


def busy() -> OAuthRefusal:
    return OAuthRefusal(409, "check_in_progress", "A token is already being checked. Try again shortly.", retryable=True)


@asynccontextmanager
async def changing(lock: str, *, hold_s: int, wait_s: float = 0, required: bool = True) -> AsyncIterator[Held]:
    """One change to a token at a time, held for ``hold_s``, what the change
    spends at most, and ``MARGIN_S`` more; yields whether the hold still
    stands, which a change asks before each write.

    The hold is waited for up to ``wait_s``. One not taken by then refuses
    the change, unless it is not ``required``: a change that must happen
    whatever runs beside it goes on without the hold.
    """
    cache = get_cache_client()
    holder = uuid.uuid4().hex
    deadline = time.monotonic() + wait_s
    while (acquired := await cache.acquire_lock(lock, holder, (hold_s + MARGIN_S) * 1000)) is False:
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(1)
    if required and acquired is False:
        raise busy()
    if required and acquired is None:
        # Redis did not answer: without the hold a removal could land mid-save.
        raise OAuthRefusal(
            503, "redis_unavailable", "Connecting an account is unavailable right now. Try again shortly.",
            retryable=True,
        )
    try:
        yield partial(holds, lock, holder)
    finally:
        if acquired:
            await cache.release_lock(lock, holder)


def pasted_token(body: TokenBody) -> tuple[str, OAuthRefusal]:
    """The token a user pasted, and the refusal it gets if Claude does not
    serve it; one that cannot be a token is refused without a check."""
    token = body.token.get_secret_value().strip()
    if not token:
        raise OAuthRefusal(400, "token_missing", "Paste the setup token first.", retryable=True)
    rejected = OAuthRefusal(
        400, "token_rejected", "Claude did not accept this token. Make a new one and paste it again.", retryable=True,
    )
    if len(token) > TOKEN_MAX_CHARS or not is_token_text(token):
        raise rejected
    return token, rejected


def pasted_code(body: CodeBody) -> str:
    raw = body.code.get_secret_value().strip()
    if not raw:
        raise OAuthRefusal(400, "code_missing", "Paste the code from Claude's page first.", retryable=True)
    return raw


def _minted_rejected() -> OAuthRefusal:
    return OAuthRefusal(
        400, "token_rejected", "Claude made a token but refused a request on it. Check that your plan includes Claude Code.",
        retryable=False,
    )


async def _mint(flow: PendingSignIn, raw: str, who: str) -> tuple[claude_oauth.SetupToken, str] | None:
    """Exchange the code pasted for ``flow`` for a setup token, and the
    sign-in's state; None when a paste just ahead of this one already saved
    this sign-in.

    The grant also carries a refresh token, dropped as ``claude setup-token``
    drops it. A code works once: one Claude refused ends the sign-in, and so
    does one spent, whatever happens to the token next. An exchange failure
    is read as claude-oauth reads it (``exchange_failure``): one asking again
    can fix keeps the sign-in, any other ends it.
    """
    try:
        stored, verifier, state = await flow.read()
    except OAuthRefusal:
        if await flow.saved(raw):
            return None
        raise
    code = flow.code(raw, state)
    try:
        async with asyncio.timeout(MINT_S):
            minted = await claude_oauth.mint_setup_token(code, state, verifier)
    except claude_oauth.UnexpectedGrant as e:
        await flow.claim(stored)
        logger.error(f"[claude_code] sign-in got an unexpected grant for {who}: {e}")
        raise OAuthRefusal(502, "unexpected_grant", "Claude answered in a way LangAlpha did not expect. Sign in again.", retryable=False) from None
    except Exception as e:
        logger.warning(f"[claude_code] sign-in exchange failed for {who}: {type(e).__name__}")
        if exchange_failure("Claude", e).error["retryable"]:
            raise OAuthRefusal(503, "check_unavailable", "Claude could not be reached. Try again.", retryable=True) from None
        await flow.claim(stored)
        raise OAuthRefusal(
            400, "code_rejected", "Claude did not accept this code. Codes work once, so sign in again.",
            retryable=False,
        ) from None
    # The code is spent: whatever happens next, only a new sign-in helps.
    if not await flow.claim(stored):
        raise PendingSignIn.expired()
    return minted, state


async def _save[S: BaseModel](
    home: TokenHome[S], token: str, *, held: Held, rejected: OAuthRefusal, retryable: bool, expires_at: datetime,
    email: str | None = None, token_uuid: str = "",
) -> S:
    async with home.placed(token, held=held, retryable=retryable) as launcher:
        await require_works(launcher, rejected=rejected, retryable=retryable)
    return await home.keep(token, held=held, email=email, expires_at=expires_at, token_uuid=token_uuid)


async def start_sign_in(home: TokenHome[BaseModel]) -> dict:
    """Start a sign-in as ``claude setup-token`` does; the page Claude shows
    after it gives the code ``finish_sign_in`` takes."""
    started = await home.sign_in.start(claude_oauth.authorize_setup_token)
    logger.info(f"[claude_code] sign-in started for {home.who}")
    return started


async def finish_sign_in[S: BaseModel](home: TokenHome[S], raw: str) -> S:
    """Exchange the pasted code for a setup token, check it, and keep it.

    The home is readied before the code is spent, so a machine that is off
    leaves the sign-in standing for another try.
    """
    await home.ready()
    async with changing(home.lock, hold_s=MINT_S + CHECK_S + CHECK_CLEANUP_S + home.place_s) as held:
        done = await _mint(home.sign_in, raw, home.who)
        if done is None:
            return await home.current()
        minted, state = done
        kept = await _save(
            home, minted.access_token, held=held, rejected=_minted_rejected(), retryable=False,
            expires_at=minted.expires_at, email=minted.email, token_uuid=minted.token_uuid,
        )
        await home.sign_in.mark_saved(state, SAVED_MARKER_TTL_S)
    return kept


async def paste[S: BaseModel](home: TokenHome[S], token: str, rejected: OAuthRefusal) -> S:
    """Keep a setup token the user made with ``claude setup-token``, once Claude serves it."""
    await home.ready()
    async with changing(home.lock, hold_s=CHECK_S + CHECK_CLEANUP_S + home.place_s) as held:
        return await _save(
            home, token, held=held, rejected=rejected, retryable=True, expires_at=datetime.now(timezone.utc) + TOKEN_LIFETIME,
        )


async def remove(home: TokenHome[BaseModel]) -> None:
    """Remove the token, whether or not Claude Code is still on for the user."""
    await home.ready(install=False)
    async with changing(home.lock, hold_s=home.remove_s) as held:
        await home.remove(held=held)
