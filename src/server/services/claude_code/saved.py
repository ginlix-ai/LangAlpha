"""The setup token a user saves in Settings.

The backend's own CLI signs in with it where it signs in the user's turns
(``hosted.saved_token_runs``): on a self-hosted server, and on the platform
while the user chose to store it there. It is also relayed to a computer of
the user's own that has no token of its own. It is checked before it is saved, and
saved encrypted in the OAuth token store, never sent back. What the check
found is kept by the token's hash, never the token.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from .hosted import TurnsNotStopped, saved_token_runs, stop_stored_turns
from ptc_agent.core.claude_code_managed import local_managed
from src.server.services.oauth_signin import OAuthRefusal, PendingSignIn
from src.server.database import claude_code_plan
from src.server.database.claude_code_plan import SAVED
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

from .check_now import CHECK_UNAVAILABLE, PLAN_WRITTEN_S, held_check, limit_held, not_checkable, unreached, unsaved
from .checks import (
    CHECK_CLEANUP_S,
    CHECK_S,
    CHECKED,
    Check,
    Held,
    check_would_bill,
    in_background,
    keep_check,
    read_check,
    run_check,
)
from .flow import MACHINE_IO_S, POLICY_BLOCKED, busy, changing, require, rows_on, turns_running
from .plan import CheckReports, CredentialPlan, UserPlan, plan_views

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.launcher import LocalLauncher
    from ptc_agent.agent.claude_code.plan import PlanLog

logger = logging.getLogger(__name__)


def token_hash(token: str) -> str:
    """What a token is known by wherever it is not kept: its sha256, in hex."""
    return hashlib.sha256(token.encode()).hexdigest()


def refusal_stamp(row: Mapping[str, Any]) -> tuple[str, Any]:
    """What a refusal of a stored token is recorded against (``refuse``): its
    hash and when it was saved, never the token itself."""
    return token_hash(row["access_token"]), row["updated_at"]


def token_check_key(token: str) -> str:
    """Where a token's kept check is: by hash, never the token itself."""
    return _check_key(token_hash(token))


def _check_key(digest: str) -> str:
    return f"claude_code:token_check:{digest}"


def check_lock(token: str) -> str:
    """One check of a token at a time, wherever it starts."""
    return f"{token_check_key(token)}:lock"


async def stored_row(user_id: str, *, conn=None) -> dict | None:
    """The token store's row for the setup token ``user_id`` saved, or None;
    read on ``conn`` where a caller holding locks on it passes one."""
    row = await get_oauth_tokens(user_id, CLAUDE_CODE_PROVIDER, conn=conn)
    return row if row and row["access_token"] else None


async def stored_token(user_id: str) -> str | None:
    """The setup token ``user_id`` saved for Claude Code, or None."""
    row = await stored_row(user_id)
    return row["access_token"] if row else None


#: Why a token checked as the user stopped storing theirs is not saved.
STORE_ENDED = "You stopped storing your setup token while this one was checked, so it wasn't saved."
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


async def saved_refusal(user_id: str, *, conn=None) -> Literal["signed_out", "token_refused"] | None:
    """Why no turn may run on the saved token, as admission refuses one: none
    is saved, or it is not ``usable``; None when one may. Read on ``conn``
    where a caller passes one."""
    row = await stored_row(user_id, conn=conn)
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


def _launcher(token: str, plan: PlanLog | None = None) -> LocalLauncher:
    """The backend's own CLI, signed in with ``token`` alone, as its check
    needs, keeping what it reports of the plan behind the saved token where
    ``plan`` is given: ``token`` is that one."""
    from ptc_agent.agent.claude_code.launcher import LocalLauncher

    return LocalLauncher(token=token, plan=plan)


async def check_token(
    user_id: str, row: dict, key: str, held: Held, *, answered: Callable[[], None] = lambda: None,
    skipped: Callable[[], None] = lambda: None, plan: UserPlan | None = None,
) -> Check:
    """Ask Anthropic, keep the answer by the token's hash, and return it. A refusal is
    flagged on the row too, and a token served clears a flag a refusal
    left, one a turn met for a reason other than the token included, unless
    a refusal stamped the flag again since the row was read. A refusal a
    turn met while the check ran is newer than its answer, so it stands:
    the answer is kept before the row is read again, and a refusal flags the
    row before it keeps its own, so one of the two sees the other. One that
    would be billed to usage credits (``check_would_bill``) sends nothing
    and keeps the answer the row stands on, which ``skipped`` hears.
    ``answered`` hears when Claude has answered, the request then spent;
    ``plan`` keeps what the check reports of the plan, for a caller to wait on."""
    digest = token_hash(row["access_token"])
    if await check_would_bill(user_id, SAVED):
        answer = "refused" if row.get("needs_reauth_at") is not None else "works"
        await keep_check(key, answer)
        skipped()
        return answer
    plan = plan or UserPlan(user_id, token_digest=digest)
    try:
        works = await run_check(_launcher(row["access_token"], plan))
        answered()
        await plan.written()
    except Exception as e:
        logger.warning(f"[claude_code] token check failed: {type(e).__name__}")
        await keep_check(key, "unknown")
        return "unknown"
    if not works:
        await refuse(user_id, digest, row["updated_at"])
        return "refused"
    await keep_check(key, "works")
    if row.get("needs_reauth_at") is not None:
        await clear_access_token_refused(
            user_id, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=row["updated_at"],
            flagged_at=row["needs_reauth_at"],
        )
    current = await stored_row(user_id)
    if current is not None and current.get("needs_reauth_at") is not None and token_hash(current["access_token"]) == digest:
        await keep_check(key, "refused")
        return "refused"
    return "works"


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
    started = await in_background(check_lock(token), CHECK_S + CHECK_CLEANUP_S, lambda held: check_token(user_id, row, key, held))
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
    if not (enabled and await saved_token_runs(user_id)):
        saved = await get_oauth_connection(user_id, CLAUDE_CODE_PROVIDER) is not None
        return ClaudeCodeStatus(state="off", saved=saved, enabled=enabled)
    if await local_managed():
        saved = await get_oauth_connection(user_id, CLAUDE_CODE_PROVIDER) is not None
        return ClaudeCodeStatus(state="policy_blocked", saved=saved, enabled=True)
    current, plans = await asyncio.gather(_current(user_id, await stored_row(user_id)), plan_views(user_id, [SAVED]))
    return _with_plan(current, plans)


def _with_plan(status: ClaudeCodeStatus, plans: Mapping[str, CredentialPlan]) -> ClaudeCodeStatus:
    """The status with the plan behind the saved token and its Check now's hold."""
    found = plans.get(SAVED)
    return status.model_copy(update=found._asdict()) if status.saved and found else status


async def check_now(user_id: str) -> None:
    """Check now on the saved token, where this server runs turns on it
    (``check_now``): under the hold its saves and removals take, so the
    token checked is the one saved and its hold the one taken, then under
    its own check's, so it never runs beside the check a read starts. Where
    an organization's policy is installed on the backend's machine, it is
    refused as a save is, the CLI never run, and so is one that would be
    billed to usage credits (``check_would_bill``), before its hold."""
    require(await rows_on(user_id) and await saved_token_runs(user_id))
    home = SavedHome(user_id)
    await home.ready()
    async with changing(home.lock, hold_s=CHECK_S + CHECK_CLEANUP_S, unavailable=CHECK_UNAVAILABLE):
        row = await stored_row(user_id)
        if row is None or not await usable(row):
            raise not_checkable("Add a setup token that works first.")
        if await check_would_bill(user_id, SAVED):
            raise limit_held()
        token = row["access_token"]
        async with (
            changing(check_lock(token), hold_s=CHECK_S + CHECK_CLEANUP_S, unavailable=CHECK_UNAVAILABLE) as held,
            held_check(user_id, SAVED) as spend,
        ):
            plan, skips, spent = UserPlan(user_id, token_digest=token_hash(token)), [], []

            def answered() -> None:
                spend()
                spent.append(True)

            try:
                async with asyncio.timeout(CHECK_S + CHECK_CLEANUP_S):
                    answer = await check_token(
                        user_id, row, token_check_key(token), held, answered=answered, skipped=lambda: skips.append(True), plan=plan,
                    )
            except TimeoutError:
                if spent:
                    raise unsaved() from None
                answer = "unknown"
            if skips:
                raise limit_held()
            if answer == "unknown":
                raise unreached()
            if not await plan.written(PLAN_WRITTEN_S):
                raise unsaved()


class SavedHome:
    """The token store's row, as the flow changes it: a token is checked before it is stored."""

    place_s = 0
    remove_s = MACHINE_IO_S

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        #: What the check of the token being saved reported of its plan.
        self.checked = CheckReports()
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
        yield _launcher(token, self.checked)

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
        # hold lapsed never undoes a removal made after, nor one that ends the
        # user's choice to store it, made while the token was checked.
        async with connection_change(self.user_id, CLAUDE_CODE_PROVIDER) as conn:
            if not await held():
                raise busy()
            if not await saved_token_runs(self.user_id, conn=conn):
                raise OAuthRefusal(409, "unavailable", STORE_ENDED, retryable=False)
            await upsert_oauth_tokens(
                self.user_id, CLAUDE_CODE_PROVIDER, access_token=token, refresh_token="", account_id=token_uuid,
                email=email, expires_at=expires_at, conn=conn,
            )
            # The token saved may be another Claude account's.
            await claude_code_plan.forget_plan(self.user_id, SAVED, token_digest=token_hash(token), conn=conn)
        await self.checked.keep(UserPlan(self.user_id, token_digest=token_hash(token)))
        if previous is not None and previous["access_token"] != token:
            await get_cache_client().delete(token_check_key(previous["access_token"]))
        logger.info(f"[claude_code] setup token saved for user_id={self.user_id} token_uuid={token_uuid or '-'}")
        # The same token saved again keeps its plan and its Check now's hold.
        return _with_plan(_status({"email": email, "expires_at": expires_at}, "signed_in"), await plan_views(self.user_id, [SAVED]))

    async def current(self) -> ClaudeCodeStatus:
        current, plans = await asyncio.gather(_current(self.user_id, await stored_row(self.user_id)), plan_views(self.user_id, [SAVED]))
        return _with_plan(current, plans)

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
            await claude_code_plan.forget_plan(self.user_id, SAVED, conn=conn)
        if previous is not None:
            await get_cache_client().delete(token_check_key(previous))
        try:
            await stop_stored_turns(self.user_id)
        except TurnsNotStopped:
            raise turns_running() from None
