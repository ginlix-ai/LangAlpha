"""The setup token a user saves in Settings.

The backend's own CLI signs in with it, only on a self-hosted server
(``backend_available``), which also relays it to a computer of the user's
own that has no token of its own. It is checked before it is saved, and
saved encrypted in the OAuth token store, never sent back. What the check
found is kept by the token's hash, never the token.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.llms.claude_code import backend_available
from src.server.services.oauth_signin import PendingSignIn
from src.server.database.oauth_tokens import (
    CLAUDE_CODE_PROVIDER,
    connection_change,
    delete_oauth_tokens,
    get_oauth_connection,
    get_oauth_tokens,
    upsert_oauth_tokens,
)
from src.server.models.claude_code import ClaudeCodeStatus, State
from src.utils.cache.redis_cache import get_cache_client

from .checks import CHECK_CLEANUP_S, CHECK_S, CHECKED, Held, in_background, keep_check, read_check, run_check
from .flow import busy, rows_on

logger = logging.getLogger(__name__)


def token_check_key(token: str) -> str:
    """Where a token's kept check is: by hash, never the token itself."""
    return f"claude_code:token_check:{hashlib.sha256(token.encode()).hexdigest()}"


async def stored_row(user_id: str) -> dict | None:
    """The token store's row for the setup token ``user_id`` saved, or None."""
    row = await get_oauth_tokens(user_id, CLAUDE_CODE_PROVIDER)
    return row if row and row["access_token"] else None


async def stored_token(user_id: str) -> str | None:
    """The setup token ``user_id`` saved for Claude Code, or None."""
    row = await stored_row(user_id)
    return row["access_token"] if row else None


async def usable_token(user_id: str) -> str | None:
    """The saved token, unless Claude is known to refuse it."""
    token = await stored_token(user_id)
    if token is None or await read_check(token_check_key(token)) == "refused":
        return None
    return token


@dataclass(frozen=True)
class SavedToken:
    """The setup token ``user_id`` saved, as a backend turn's launcher reads
    it (``launcher.TokenStore``): as each run starts, and recorded as refused
    when Anthropic refuses it, so Settings and admission say so from then on."""

    user_id: str

    async def read(self) -> str | None:
        return await stored_token(self.user_id)

    async def refused(self, token: str) -> None:
        await keep_check(token_check_key(token), "refused")


def _launcher(token: str) -> Any:
    """The backend's own CLI, signed in with ``token`` alone, as its check needs."""
    from ptc_agent.agent.claude_code.launcher import LocalLauncher

    return LocalLauncher(token=token)


async def _check(token: str, key: str, held: Held) -> None:
    """Ask Anthropic, and keep the answer by the token's hash."""
    try:
        works = await run_check(_launcher(token))
    except Exception as e:
        logger.warning(f"[claude_code] token check failed: {type(e).__name__}")
        await keep_check(key, "unknown")
        return
    await keep_check(key, "works" if works else "refused")


async def token_state(token: str) -> State:
    """The token's kept answer. On a miss one worker starts the check, and reads answer checking until it lands."""
    key = token_check_key(token)
    if (answer := await read_check(key)) is not None:
        return CHECKED[answer]
    started = await in_background(f"{key}:lock", CHECK_S + CHECK_CLEANUP_S, lambda held: _check(token, key, held))
    # With no Redis to keep an answer, a check here would spend a request on
    # the user's plan at every read; the token was checked when saved.
    return "unverified" if started is None else "checking"


def _status(row: dict, state: State) -> ClaudeCodeStatus:
    return ClaudeCodeStatus(
        state=state, email=row.get("email"), expires_at=row.get("expires_at"), saved=True, enabled=True,
    )


async def _current(row: dict | None) -> ClaudeCodeStatus:
    if row is None:
        return ClaudeCodeStatus(state="no_token", enabled=True)
    return _status(row, await token_state(row["access_token"]))


async def status(user_id: str) -> ClaudeCodeStatus:
    """Whether the user's Claude Code models can run on the backend, for Settings and the picker."""
    enabled = await rows_on(user_id)
    if not (enabled and backend_available()):
        saved = await get_oauth_connection(user_id, CLAUDE_CODE_PROVIDER) is not None
        return ClaudeCodeStatus(state="off", saved=saved, enabled=enabled)
    return await _current(await stored_row(user_id))


class SavedHome:
    """The token store's row, as the flow changes it: a token is checked before it is stored."""

    place_s = 0

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.lock = f"claude_code:token_save:{user_id}"
        self.sign_in = PendingSignIn(f"claude_code:sign_in:{user_id}")
        self.who = f"user_id={user_id}"

    async def ready(self, *, install: bool = True) -> None:
        return None

    @asynccontextmanager
    async def placed(self, token: str, *, held: Held, retryable: bool) -> AsyncIterator[Any]:
        yield _launcher(token)

    async def keep(
        self, token: str, *, held: Held, email: str | None, expires_at: datetime, token_uuid: str,
    ) -> ClaudeCodeStatus:
        """Save a checked token over any saved before; the row's ``account_id``
        holds the token's uuid, what logs and the UI know it by."""
        previous = await stored_row(self.user_id)
        if previous is not None and previous["access_token"] == token:
            # The same token saved again lasts no longer than it did.
            expires_at, email = previous["expires_at"], email or previous.get("email")
        # Before the row, so a read that finds the row finds its answer too.
        await keep_check(token_check_key(token), "works")
        # Read under the writer lock a removal also takes, so a save whose
        # hold lapsed never undoes a removal made after.
        async with connection_change(self.user_id, CLAUDE_CODE_PROVIDER) as conn:
            if not await held():
                raise busy()
            await upsert_oauth_tokens(
                self.user_id, CLAUDE_CODE_PROVIDER, access_token=token, refresh_token="", account_id=token_uuid,
                email=email, expires_at=expires_at, conn=conn,
            )
        if previous is not None and previous["access_token"] != token:
            await get_cache_client().delete(token_check_key(previous["access_token"]))
        logger.info(f"[claude_code] setup token saved for user_id={self.user_id} token_uuid={token_uuid or '-'}")
        return _status({"email": email, "expires_at": expires_at}, "signed_in")

    async def current(self) -> ClaudeCodeStatus:
        return await _current(await stored_row(self.user_id))

    async def remove(self) -> None:
        try:
            previous = await stored_token(self.user_id)
        except Exception:
            # Unreadable without the key it was saved under; the row goes all
            # the same, and its check answer lapses on its own.
            logger.warning(f"[claude_code] saved token unreadable on removal for user_id={self.user_id}", exc_info=True)
            previous = None
        async with connection_change(self.user_id, CLAUDE_CODE_PROVIDER) as conn:
            await delete_oauth_tokens(self.user_id, CLAUDE_CODE_PROVIDER, conn=conn)
        if previous is not None:
            await get_cache_client().delete(token_check_key(previous))
