"""Check now's hold: one minimal request on a Claude Code credential's plan,
on demand, at most once per credential every ``CHECK_EVERY``.

The plan's windows are as fresh as the last request on the credential, so
Settings and the model picker offer to read them now. The request is the
token check (``runtime.token_works``) run where that credential's own check
runs, by the module that owns it: ``saved.check_now`` on the backend's CLI
for the saved token, ``machine.check_now`` on the computer for its own. The
CLI reports the plan as it does on a turn, and the run keeps that in the
credential's row (``UserPlan``), written before the check answers so the
read after it shows it, waited for up to ``PLAN_WRITTEN_S``, and a check
whose plan was not written says so (``unsaved``) rather than answering
over the plan before it; nothing sends the token to Anthropic itself.

A check spends a request on the user's plan, so the hold is taken on the
credential's row (``hold_check``), where every worker counts alike. A check
that sent nothing, or could not tell whether it reached Claude, gives the
hold back, since its refusal asks the user to try again; one Claude answered
keeps it, whatever fails after.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import timedelta

from src.server.database import claude_code_plan as store
from src.server.services.oauth_signin import OAuthRefusal

logger = logging.getLogger(__name__)

#: How often Check now may spend a request on one credential's plan.
CHECK_EVERY = timedelta(minutes=5)
#: How long a check waits for the plan it read to be written: the user waits
#: on that read, so well past the ``WRITTEN_WAIT_S`` a stopped turn waits.
PLAN_WRITTEN_S = 30.0
#: What a check refused for want of Redis to hold it says.
CHECK_UNAVAILABLE = "Checking usage is unavailable right now. Try again shortly."


def unreached() -> OAuthRefusal:
    return OAuthRefusal(503, "check_unavailable", "Couldn't reach Claude to check usage. Try again later.", retryable=True)


def computer_unreached() -> OAuthRefusal:
    """The computer itself was not reached, so no CLI ran there; ``unreached`` is one that ran and could not reach Claude."""
    return OAuthRefusal(
        503, "computer_unreachable", "Couldn't reach this computer to check usage. Try again later.", retryable=True,
    )


def unsaved() -> OAuthRefusal:
    """Claude answered, but what it said of the plan was not kept, so the read after would show the plan before."""
    return OAuthRefusal(503, "plan_unsaved", "Couldn't save the usage Claude reported. Try again later.", retryable=True)


def limit_held() -> OAuthRefusal:
    """A check that would be billed to usage credits the user chose not to
    spend (``check_would_bill``) sends nothing: the row's limit is as current
    as a check would make it."""
    return OAuthRefusal(409, "limit_held", "The Claude plan's limit holds until it resets, so its usage is already current.")


def not_checkable(message: str) -> OAuthRefusal:
    return OAuthRefusal(409, "not_checkable", message)


@asynccontextmanager
async def held_check(user_id: str, credential: str) -> AsyncIterator[Callable[[], None]]:
    """Take the credential's check for the block, or refuse it with how many
    seconds until the next may go (``check_in``). The block gets what marks
    the request spent, which it calls once Claude has answered. A block that
    raises, one cancelled with the worker included, gives the hold back
    unless it was marked: only a check whose outcome is unknown frees it.
    The give-back outlasts a second cancel, and one that fails leaves the
    block's own exception for the caller to hear."""
    stamp, check_in = await store.hold_check(user_id, credential, CHECK_EVERY)
    if stamp is None:
        raise OAuthRefusal(429, "check_held", "This token's usage was checked less than 5 minutes ago.", check_in=check_in)
    spent = False

    def spend() -> None:
        nonlocal spent
        spent = True

    try:
        yield spend
    except BaseException:
        if not spent:
            try:
                await asyncio.shield(store.give_back_check(user_id, credential, stamp))
            except Exception as e:  # noqa: BLE001 - the check's own outcome is what the caller hears
                logger.warning(f"[claude_code] could not give back a check's hold for user_id={user_id} {credential}: {type(e).__name__}")
        raise
