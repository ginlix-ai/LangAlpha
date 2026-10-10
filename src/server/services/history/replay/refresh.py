"""Project turns as they settle, so a reader streams stored lines.

Two triggers: a turn's run finalizing (the newest two turns, since the
previous turn's answered interrupts only become known at this turn's resume
boundary) and a background task run finalizing (the turn that launched it,
whose lines could not be stored while the run still wrote). Both are
optimizations: a read projects whatever these left undone.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class _Wanted:
    """A thread's next pass, as ``refresh_thread`` takes it."""

    newest: bool = False
    response_ids: set[str] = field(default_factory=set)


# Execution context only: coalesces this process's triggers per thread. Truth
# stays in the stored rows, so a trigger landing on another worker, or lost
# to a restart, only costs the next read a projection.
_runners: dict[str, asyncio.Task[None]] = {}
_pending: dict[str, _Wanted] = {}


def schedule_replay_refresh(thread_id: str, response_id: str | None = None) -> None:
    """Fire-and-forget. At most one runner per thread; a trigger landing
    while it runs is folded into its next pass."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # no running loop (sync test contexts)
        return
    wanted = _pending.setdefault(thread_id, _Wanted())
    if response_id is None:
        wanted.newest = True
    else:
        wanted.response_ids.add(response_id)
    current = _runners.get(thread_id)
    if current is not None and not current.done():
        return
    task = loop.create_task(_run(thread_id))
    _runners[thread_id] = task
    task.add_done_callback(lambda done, tid=thread_id: _runner_done(tid, done))


def _runner_done(thread_id: str, task: asyncio.Task[None]) -> None:
    """Drop a finished runner without clobbering a newer one."""
    if _runners.get(thread_id) is task:
        _runners.pop(thread_id, None)


async def _run(thread_id: str) -> None:
    while wanted := _pending.pop(thread_id, None):
        await refresh_thread(thread_id, wanted.response_ids, newest=wanted.newest)


async def refresh_thread(
    thread_id: str,
    response_ids: Collection[str],
    *,
    newest: bool = False,
    claims_pass: bool = True,
) -> None:
    """Project the turns that launched ``response_ids``, and with ``newest``
    the newest two. Without ``claims_pass`` a caller that waits on this never
    reads the whole thread: turns that need it are left to this process's
    runner."""
    from src.server.services.history.replay import (
        CheckpointReplayUnavailable,
        ClaimsPassNeeded,
        load_thread_inputs,
        project_turns,
    )

    try:
        loaded = await load_thread_inputs(thread_id)
        if loaded is None:
            return
        rows, branch_tip = loaded
        launching = {
            ti
            for ti, r in rows.responses_by_turn.items()
            if str(r.get("conversation_response_id")) in response_ids
        }
        await project_turns(
            rows,
            branch_tip,
            turn_indexes=sorted(launching),
            last_n_turns=2 if newest else None,
            claims_pass=claims_pass,
        )
    except ClaimsPassNeeded:
        if newest:
            schedule_replay_refresh(thread_id)
        for response_id in response_ids:
            schedule_replay_refresh(thread_id, response_id)
    except CheckpointReplayUnavailable as e:
        logger.debug(f"[REPLAY] refresh skipped for {thread_id}: {e}")
    except Exception as e:
        logger.warning(f"[REPLAY] refresh failed for {thread_id}: {e}")
