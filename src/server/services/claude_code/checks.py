"""Whether Claude serves a token, kept so a read never waits on Anthropic.

A check is one minimal turn on the user's plan (``runtime.token_works``), so
its answer is kept in Redis, and a read that finds none starts one in the
background on one worker (``in_background``) while every read answers
checking until it lands. The answer is one of three words wherever the token
is kept: ``works``, ``refused``, or ``unknown`` when Anthropic could not be
reached to tell.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

from src.llms.claude_code import token_check_model
from src.server.services.oauth_signin import OAuthRefusal
from src.utils.cache.redis_cache import get_cache_client

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.launcher import Launcher

logger = logging.getLogger(__name__)

Check = Literal["works", "refused", "unknown"]
#: The state each answer reads as in Settings.
CHECKED: dict[str, Literal["signed_in", "rejected", "unverified"]] = {
    "works": "signed_in", "refused": "rejected", "unknown": "unverified",
}
#: How long an answer that Claude served or refused the token stands. A turn
#: Anthropic refuses the token on records that at once, so this bounds only a
#: token revoked while no turn runs on it.
TOKEN_CHECK_TTL_S = 3600
#: How long any other answer stands: one that could not tell, or one finding
#: no usable token on a machine, where one may be placed by hand.
SHORT_TTL_S = 60
#: The longest one check may run, wherever it runs: the CLI's own conversation
#: (``runtime.TOKEN_CHECK_TIMEOUT_S``) and laying out its config dir.
CHECK_S = 120
#: What closing the CLI and removing its dir take once a check is cut off.
CHECK_CLEANUP_S = 60
#: What a hold lasts beyond the work it bounds, for releasing it.
MARGIN_S = 15
#: Strong refs to running checks: asyncio keeps only weak ones.
_checks: set[asyncio.Task[None]] = set()

#: Whether a hold still stands, asked before writing what it guards.
Held = Callable[[], Awaitable[bool]]


def ttl(answer: str) -> int:
    return TOKEN_CHECK_TTL_S if answer in ("works", "refused") else SHORT_TTL_S


#: A saved token's answer as it is kept: in the encoding workers from before
#: these words read (True, False, "unknown"), so a deploy rolling across both
#: reads every answer either kept. A machine's answers are new keys, in words.
_KEPT: dict[Check, bool | str] = {"works": True, "refused": False, "unknown": "unknown"}


def as_check(raw: object) -> Check | None:
    """A kept answer, or None for none: True or False as they mean, the
    words as they are, and anything else as none."""
    if isinstance(raw, bool):
        return "works" if raw else "refused"
    return raw if raw in ("works", "refused", "unknown") else None  # type: ignore[return-value]


async def read_check(key: str) -> Check | None:
    return as_check(await get_cache_client().get(key))


async def keep_check(key: str, answer: Check) -> None:
    await get_cache_client().set(key, _KEPT[answer], ttl=ttl(answer))


async def run_check(launcher: Launcher) -> bool:
    """Whether Claude serves the token ``launcher`` signs in with, wherever it
    runs, cut off at ``CHECK_S``; raises when it could not tell."""
    from ptc_agent.agent.claude_code.runtime import token_works

    async with asyncio.timeout(CHECK_S):
        return await token_works(launcher, token_check_model())


async def require_works(launcher: Launcher, *, rejected: OAuthRefusal, retryable: bool) -> None:
    """Refuse a token Claude does not serve, and one the check could not reach
    Claude for: a token is never kept unchecked."""
    try:
        works = await run_check(launcher)
    except Exception as e:
        # The failure's name only: its text is the CLI's, which no log keeps.
        logger.warning(f"[claude_code] token check on save failed: {type(e).__name__}")
        raise OAuthRefusal(
            503, "check_unavailable", "Claude could not be reached to check the token. Try again.", retryable=retryable,
        ) from None
    if not works:
        raise rejected


async def holds(lock: str, holder: str) -> bool:
    """Whether ``holder`` still holds ``lock``: a hold that lapsed while its
    work ran may have let another change in, which its writes would undo."""
    try:
        owner = await get_cache_client().client.get(lock)
    except Exception:
        return False
    return (owner.decode() if isinstance(owner, bytes) else owner) == holder


async def in_background(
    lock: str, budget_s: int, work: Callable[[Held], Coroutine[Any, Any, None]],
) -> bool | None:
    """Start ``work`` on this worker unless another holds ``lock``: True when
    it started here, False when another's is running, None with no Redis to
    keep its answer, where a check at every read would spend a request on
    the plan each time.

    ``work`` runs for at most ``budget_s``, inside a hold lasting
    ``MARGIN_S`` longer, so the hold always outlives it; it is handed whether
    the hold still stands, to ask before it keeps what it found.
    """
    cache = get_cache_client()
    holder = uuid.uuid4().hex
    leader = await cache.acquire_lock(lock, holder, (budget_s + MARGIN_S) * 1000)
    if leader:
        task = asyncio.create_task(_held(work(partial(holds, lock, holder)), lock, holder, budget_s))
        _checks.add(task)
        task.add_done_callback(_checks.discard)
    return leader


async def _held(work: Coroutine[Any, Any, None], lock: str, holder: str, budget_s: int) -> None:
    try:
        async with asyncio.timeout(budget_s):
            await work
    except TimeoutError:
        logger.warning(f"[claude_code] a token check ran past its {budget_s}s and was stopped")
    finally:
        await get_cache_client().release_lock(lock, holder)
