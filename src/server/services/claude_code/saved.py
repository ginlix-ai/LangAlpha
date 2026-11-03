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
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from src.llms.claude_code import ClaudeTurnFailed, backend_available
from ptc_agent.core.claude_code_managed import local_managed
from src.server.services.oauth_signin import OAuthRefusal, PendingSignIn
from src.server.database.oauth_tokens import (
    CLAUDE_CODE_PROVIDER,
    clear_access_token_refused,
    connection_change,
    delete_oauth_tokens,
    get_oauth_connection,
    get_oauth_tokens,
    mark_access_token_refused,
    upsert_oauth_tokens,
)
from src.server.models.claude_code import ClaudeCodeStatus, State
from src.utils.cache.redis_cache import get_cache_client

from .checks import CHECK_CLEANUP_S, CHECK_S, CHECKED, Held, in_background, keep_check, read_check, run_check
from .flow import MACHINE_IO_S, POLICY_BLOCKED, busy, rows_on

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.launcher import LocalLauncher

logger = logging.getLogger(__name__)


def token_hash(token: str) -> str:
    """What a token is known by wherever it is not kept: its sha256, in hex."""
    return hashlib.sha256(token.encode()).hexdigest()


def token_check_key(token: str) -> str:
    """Where a token's kept check is: by hash, never the token itself."""
    return _check_key(token_hash(token))


def _check_key(digest: str) -> str:
    return f"claude_code:token_check:{digest}"


async def stored_row(user_id: str) -> dict | None:
    """The token store's row for the setup token ``user_id`` saved, or None."""
    row = await get_oauth_tokens(user_id, CLAUDE_CODE_PROVIDER)
    return row if row and row["access_token"] else None


async def stored_token(user_id: str) -> str | None:
    """The setup token ``user_id`` saved for Claude Code, or None."""
    row = await stored_row(user_id)
    return row["access_token"] if row else None


#: What the token store's flag records when Claude refused the saved token.
REFUSED_CODE = "token_refused"


def needs_new(row: dict) -> bool:
    """Whether the saved token needs replacing, whatever its kept check says:
    Claude refused it (the row's flag, which outlasts the check), or it is
    past its expiry, which only lapses and is never written."""
    return row.get("needs_reauth_at") is not None or _expired(row)


async def usable(row: dict) -> bool:
    """Whether a turn may run on the saved token in ``row``: not one that needs
    replacing, nor one Claude is known to refuse."""
    return not needs_new(row) and await read_check(token_check_key(row["access_token"])) != "refused"


async def usable_token(user_id: str) -> str | None:
    """The saved token, unless it needs replacing or Claude is known to refuse it."""
    row = await stored_row(user_id)
    return row["access_token"] if row is not None and await usable(row) else None


async def saved_refusal(user_id: str) -> Literal["signed_out", "token_refused"] | None:
    """Why no turn may run on the saved token, as admission refuses one: none
    is saved, or it is not ``usable``; None when one may."""
    row = await stored_row(user_id)
    if row is None:
        return "signed_out"
    return None if await usable(row) else "token_refused"


async def refuse(user_id: str, digest: str, saved_at: datetime) -> None:
    """Record that Claude refused the saved token hashing to ``digest``
    (``token_hash``), as saved at ``saved_at``: flagged on its row until a
    check finds it served or a new save replaces it, and kept by the hash for
    the hour a check stands. Neither once the row was saved again since,
    which a refusal that raced the save must not undo; a flag that could not
    be written leaves the hour."""
    try:
        flagged = await mark_access_token_refused(
            user_id, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, error_code=REFUSED_CODE,
        )
    except Exception:
        logger.warning(f"[claude_code] could not flag the refused setup token for user_id={user_id}", exc_info=True)
        flagged = True
    if flagged:
        await keep_check(_check_key(digest), "refused")


class SavedToken:
    """The setup token ``user_id`` saved, as a backend turn's launcher reads
    it (``launcher.TokenStore``): as each run starts, and recorded as refused
    when Anthropic refuses it, so Settings and admission say so from then on,
    as the turn it was refused on already does (``token_refused``).
    A server whose backend runs no CLI refuses the run as admission does: the
    workspace may have moved off the own computer admission found it on."""

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self._read: dict | None = None

    async def read(self) -> str | None:
        if not backend_available():
            raise ClaudeTurnFailed("needs_own_computer", "this server runs Claude Code only on the user's own computers")
        self._read = await stored_row(self.user_id)
        return self._read["access_token"] if self._read else None

    async def refused(self, token: str) -> None:
        if self._read is not None:
            await refuse(self.user_id, token_hash(token), self._read["updated_at"])
        raise ClaudeTurnFailed("token_refused", "Claude refused the saved setup token")


def _launcher(token: str) -> LocalLauncher:
    """The backend's own CLI, signed in with ``token`` alone, as its check needs."""
    from ptc_agent.agent.claude_code.launcher import LocalLauncher

    return LocalLauncher(token=token)


async def _check(user_id: str, row: dict, key: str, held: Held) -> None:
    """Ask Anthropic, and keep the answer by the token's hash. A refusal is
    flagged on the row too, and a token served clears a flag a refusal
    left, one a turn met for a reason other than the token included, unless
    a refusal stamped the flag again since the row was read. A refusal a
    turn met while the check ran is newer than its answer, so it stands:
    the answer is kept before the row is read again, and a refusal flags the
    row before it keeps its own, so one of the two sees the other."""
    digest = token_hash(row["access_token"])
    try:
        works = await run_check(_launcher(row["access_token"]))
    except Exception as e:
        logger.warning(f"[claude_code] token check failed: {type(e).__name__}")
        await keep_check(key, "unknown")
        return
    if not works:
        await refuse(user_id, digest, row["updated_at"])
        return
    await keep_check(key, "works")
    if row.get("needs_reauth_at") is not None:
        await clear_access_token_refused(
            user_id, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=row["updated_at"],
            flagged_at=row["needs_reauth_at"],
        )
    current = await stored_row(user_id)
    if current is not None and current.get("needs_reauth_at") is not None and token_hash(current["access_token"]) == digest:
        await keep_check(key, "refused")


def _expired(row: dict) -> bool:
    expires_at = row.get("expires_at")
    return expires_at is not None and expires_at <= datetime.now(timezone.utc)


async def token_state(user_id: str, row: dict) -> State:
    """The saved token's state: refused, with no check, once it expired,
    else its kept answer. On a miss one worker starts the check, and reads
    answer checking until it lands; a token flagged as refused is checked
    again so, as Settings is read, the one way back from a refusal that was
    not the token's."""
    if _expired(row):
        return "rejected"
    token = row["access_token"]
    key = token_check_key(token)
    if (answer := await read_check(key)) is not None:
        return CHECKED[answer]
    started = await in_background(f"{key}:lock", CHECK_S + CHECK_CLEANUP_S, lambda held: _check(user_id, row, key, held))
    if started is not None:
        return "checking"
    # With no Redis to keep an answer, a check here would spend a request on
    # the user's plan at every read; the token was checked when saved, and a
    # refusal since is on its row.
    return "rejected" if row.get("needs_reauth_at") is not None else "unverified"


def _status(row: dict, state: State) -> ClaudeCodeStatus:
    return ClaudeCodeStatus(
        state=state, email=row.get("email"), expires_at=row.get("expires_at"), saved=True, enabled=True,
    )


async def _current(user_id: str, row: dict | None) -> ClaudeCodeStatus:
    if row is None:
        return ClaudeCodeStatus(state="no_token", enabled=True)
    return _status(row, await token_state(user_id, row))


async def status(user_id: str) -> ClaudeCodeStatus:
    """Whether the user's Claude Code models can run on the backend, for
    Settings and the picker. Where an organization's policy is installed on
    the backend's machine, none can, whatever the token, which is not checked."""
    enabled = await rows_on(user_id)
    if not (enabled and backend_available()):
        saved = await get_oauth_connection(user_id, CLAUDE_CODE_PROVIDER) is not None
        return ClaudeCodeStatus(state="off", saved=saved, enabled=enabled)
    if await local_managed():
        saved = await get_oauth_connection(user_id, CLAUDE_CODE_PROVIDER) is not None
        return ClaudeCodeStatus(state="policy_blocked", saved=saved, enabled=True)
    return await _current(user_id, await stored_row(user_id))


class SavedHome:
    """The token store's row, as the flow changes it: a token is checked before it is stored."""

    place_s = 0
    remove_s = MACHINE_IO_S

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.lock = f"claude_code:token_save:{user_id}"
        self.sign_in = PendingSignIn(f"claude_code:sign_in:{user_id}")
        self.who = f"user_id={user_id}"

    async def ready(self, *, install: bool = True) -> None:
        """Refuse a sign-in or a paste, before its code is spent or its token
        checked, where an organization's policy is installed on the backend's
        machine; a removal goes on."""
        if install and await local_managed():
            raise OAuthRefusal(400, "policy_blocked", POLICY_BLOCKED, retryable=False)

    @asynccontextmanager
    async def placed(self, token: str, *, held: Held, retryable: bool) -> AsyncIterator[LocalLauncher]:
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
        return await _current(self.user_id, await stored_row(self.user_id))

    async def remove(self, *, held: Held) -> None:
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
