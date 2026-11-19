"""Stopping what a suspended account has running, when the notice arrives.

A suspension is learned from the auth service's ``membership-changed``
notice. What the account already has running is then stopped here, through
the paths a user's own stop takes (a turn's cancel, a task's cancel, a
command's stop mark, a preview's revocation, a computer's stop), each one
durable or idempotent on its own, so a worker that dies part way leaves only
what those paths already recover. Nothing saved is changed.

Best effort: the notice is answered first, and a step that fails is logged.
What this misses, a running turn's own check at its next model call
(``CreditGateMiddleware``) and the idle sweep still stop.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from src.server.services.membership import membership_epoch

logger = logging.getLogger(__name__)

_LOG = "[account_suspension]"

# A computer cannot stop while a turn on it is still winding down from its
# cancel, so its stop is asked again a few times before the idle sweep is
# left to it.
_STOP_ATTEMPTS = 4
_STOP_RETRY_S = 15.0

# Strong references to the stops in flight. Process-local by design: nothing
# reads this set as state, and a stop lost with its process is covered by the
# checks above.
_stops: set[asyncio.Task] = set()


def stop_if_suspended_soon(user_id: str, epoch: Optional[int]) -> None:
    """If the notice that moved the user's membership to ``epoch`` suspended
    the account, stop what it has running, in the background."""
    # Only work started before now is stopped. A turn of the same account
    # started later began after a later notice (a reinstatement) or was
    # admitted against this one; the second is stopped at its first model
    # call, and the first must not be stopped by this one at all.
    notice_at = datetime.now(timezone.utc)
    task = asyncio.create_task(
        _stop_if_suspended(user_id, epoch, notice_at),
        name=f"account_suspension_{user_id[:12]}",
    )
    _stops.add(task)
    task.add_done_callback(_stops.discard)


async def _stop_if_suspended(
    user_id: str, epoch: Optional[int], notice_at: Optional[datetime] = None
) -> None:
    from src.server.database.runs.lifecycle import user_has_running_work
    from src.server.services.account import read_standing

    # Most notices are for an account with nothing running, whose suspension
    # the gate and the refusals already cover; those ask the auth service
    # nothing.
    try:
        if not await user_has_running_work(user_id):
            return
    except Exception:
        logger.warning(f"{_LOG} reading the running work failed user={user_id}", exc_info=True)
        return

    async def still_suspended() -> bool:
        # A later notice (a reinstatement, say) moves the epoch, and its own
        # task reads the account afresh; this one, holding an answer from
        # before it, stands down. Unchanged, the answer below still holds.
        if await membership_epoch(user_id) == epoch:
            return True
        logger.info(f"{_LOG} a later notice arrived; not stopping user={user_id}")
        return False

    try:
        standing = await read_standing(user_id, unknown_on_cache_error=True)
    except Exception:
        logger.warning(f"{_LOG} reading the account failed user={user_id}", exc_info=True)
        return
    # Unknown stops nothing, unless the notice said suspended: an outage must
    # not stop every member's work. Checked after the read too, which may
    # have been asked before a notice that came in while it waited.
    if not standing.suspended or not await still_suspended():
        return
    logger.info(f"{_LOG} the account is suspended; stopping its work user={user_id}")
    await stop_account_work(user_id, still_suspended, started_before=notice_at)


async def stop_account_work(
    user_id: str,
    still_suspended: Callable[[], Awaitable[bool]],
    *,
    started_before: Optional[datetime] = None,
) -> None:
    """Stop the account's turns, tasks, commands, previews and computers,
    in that order, so nothing stopped first starts what comes after. Each
    step goes ahead only while ``still_suspended``, and the first three stop
    only what started before ``started_before``: a reinstatement that lands
    after a step's check cannot have its own new work stopped by it."""
    steps: list[tuple[str, Callable[[], Awaitable[None]]]] = [
        ("turns", lambda: _cancel_turns(user_id, started_before)),
        ("tasks", lambda: _cancel_tasks(user_id, started_before)),
        ("commands", lambda: _stop_commands(user_id, started_before)),
    ]
    for name, work in steps:
        if not await still_suspended():
            return
        await _step(name, work(), user_id)
    computers = await _computers(user_id)
    if not await still_suspended():
        return
    await _step("previews", _revoke_previews(computers), user_id)
    if not await still_suspended():
        return
    await _step("computers", _stop_computers(computers, still_suspended), user_id)


async def _step(name: str, work, user_id: str) -> None:
    try:
        await work
    except Exception:
        logger.warning(f"{_LOG} stopping {name} failed user={user_id}", exc_info=True)


async def _cancel_turns(user_id: str, started_before: Optional[datetime]) -> None:
    from src.server.database.runs.lifecycle import open_runs_of_user
    from src.server.services.cancel_dispatch import cancel_workflow

    for run in await open_runs_of_user(user_id, started_before=started_before):
        # Not the user's own stop: steers stay queued, nothing is handed
        # back to a caller who never asked, and the run settles as suspended.
        await cancel_workflow(
            str(run["conversation_thread_id"]),
            str(run["conversation_response_id"]),
            by="suspension",
        )


async def _cancel_tasks(user_id: str, started_before: Optional[datetime]) -> None:
    from src.server.database.runs.subagent_runs import open_task_runs_of_user
    from src.server.services.cancel_dispatch import cancel_subagent_task

    for run in await open_task_runs_of_user(user_id, started_before=started_before):
        await cancel_subagent_task(
            str(run["thread_id"]), str(run["task_id"]), task_run_id=str(run["task_run_id"])
        )


async def _stop_commands(user_id: str, started_before: Optional[datetime]) -> None:
    from src.server.database.runs import command_runs as cr_db

    # The watcher's next pass ends each one and settles it with no report.
    marked = await cr_db.request_stops(
        user_id=user_id, by="suspended", started_before=started_before
    )
    if marked:
        logger.info(f"{_LOG} marked {len(marked)} running commands to stop user={user_id}")


async def _computers(user_id: str) -> list[dict]:
    from src.server.database.computer import get_computers_for_user

    try:
        return await get_computers_for_user(user_id)
    except Exception:
        logger.warning(f"{_LOG} reading computers failed user={user_id}", exc_info=True)
        return []


async def _revoke_previews(computers: list[dict]) -> None:
    from src.server.services.preview_capabilities import revoke

    for computer in computers:
        await revoke(computer.get("provider_ref"))


async def _stop_computers(
    computers: list[dict], still_suspended: Callable[[], Awaitable[bool]]
) -> None:
    from src.server.models.computer import ComputerStatus
    from src.server.services.computer_manager import ComputerManager

    manager = ComputerManager.get_instance()
    running = [
        str(c["computer_id"]) for c in computers if c.get("status") == ComputerStatus.RUNNING
    ]
    await asyncio.gather(*(_stop_computer(manager, cid, still_suspended) for cid in running))


async def _stop_computer(
    manager, computer_id: str, still_suspended: Callable[[], Awaitable[bool]]
) -> None:
    for attempt in range(_STOP_ATTEMPTS):
        # The first try follows the step's own check; a retry, up to a
        # minute on, checks again, or it could stop a computer a reinstated
        # account has just started.
        if attempt and not await still_suspended():
            return
        try:
            await manager.stop_computer(computer_id)
            return
        except Exception as e:
            if attempt == _STOP_ATTEMPTS - 1:
                logger.warning(
                    f"{_LOG} computer {computer_id} not stopped, left to the idle sweep: {e}"
                )
                return
            await asyncio.sleep(_STOP_RETRY_S)
