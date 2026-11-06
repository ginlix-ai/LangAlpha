"""The drain of a finished run's surviving subagent writers.

A run's guard session stays pinned while a subagent writer it admitted can
still checkpoint. Each writer stops itself at its own clock, so the drain
waits that long; a writer still alive past every clock, or still unwinding
long after its task settled, is stuck where no clock reaches it and is
stopped as past its time limit.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable

import structlog

from ptc_agent.agent.middleware.background_subagent.registry import (
    CANCEL_UNWIND_TIMEOUT,
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.subagent import (
    SUBAGENT_CLOSE_TIMEOUT_S,
)

logger = structlog.get_logger(__name__)

# Past a tail writer's clock: the stream close a subagent gets at its clock's
# end, with room for a workflow run's terminal bookkeeping.
TAIL_DRAIN_SLACK_S = 300.0
# How long a settled task's writer may go on unwinding, and so how often the
# drain looks again for tasks that settled while it waited.
SETTLED_UNWIND_S = 1800.0


def _longest_writer_clock() -> float | None:
    """How long past the run's end a tail writer can still be inside its clock,
    or None with no subagent clock. Both clocks started before the run ended:
    a subagent's (``_with_own_deadline``) and a workflow run's ``run_timeout``."""
    from src.config.settings import (
        get_workflow_orchestration_config,
        get_workflow_timeout,
    )

    clock = get_workflow_timeout()
    if clock <= 0:
        return None
    return max(clock, get_workflow_orchestration_config().run_timeout) + TAIL_DRAIN_SLACK_S


async def drain_tail_writers(
    get_registry: Callable[[], Awaitable[BackgroundTaskRegistry | None]],
    owns: Callable[[BackgroundTask], bool],
    *,
    run_id: str,
) -> None:
    """Return once no writer of a task ``owns`` matches can still write.

    Raises TimeoutError when a stopped writer will not unwind, but only once
    every other writer has drained, so the caller discards the session rather
    than release it under a writer it cannot account for, without failing a
    writer still inside its clock. Re-reads the registry after each wait to
    catch writers registered meanwhile.
    """
    loop = asyncio.get_running_loop()
    clock = _longest_writer_clock()
    started = loop.time()
    clock_end = None if clock is None else started + clock
    first_settled: dict[str, float] = {}
    stuck: set[asyncio.Task] = set()
    while (registry := await get_registry()) is not None:
        now = loop.time()
        stuck = {writer for writer in stuck if not writer.done()}
        ends: dict[str, float | None] = {}
        waiting: list[asyncio.Task] = []
        for task, writers in await registry.live_writers_by_task(owns):
            live = [writer for writer in writers if writer not in stuck]
            if not live:
                continue
            waiting += live
            if task.completed:
                # Settling stamps last_updated_at, so the bound runs from the
                # settle, not from whenever this loop first sees it, and never
                # from before the drain began.
                settled_at = max(
                    started, now - max(0.0, time.time() - task.last_updated_at)
                )
                own_end = first_settled.setdefault(task.task_id, settled_at) + SETTLED_UNWIND_S
            elif task.stop_at is not None:
                own_end = now + task.stop_at - time.time() + TAIL_DRAIN_SLACK_S
            else:
                own_end = None
            ends[task.task_id] = min(
                (end for end in (clock_end, own_end) if end is not None),
                default=None,
            )
        if not waiting:
            break
        overdue = {task_id for task_id, end in ends.items() if end is not None and end <= now}
        if not overdue:
            next_end = min((end for end in ends.values() if end is not None), default=None)
            logger.info("Tail drain awaiting subagent writers", run_id=run_id, writers=len(waiting))
            await asyncio.wait(
                waiting,
                timeout=SETTLED_UNWIND_S
                if next_end is None
                else min(next_end - now, SETTLED_UNWIND_S),
            )
            continue
        stopped = await registry.time_out_writers_where(
            lambda task: owns(task) and task.task_id in overdue
        )
        logger.warning(
            "Stopping subagent writers past their clocks or their unwind",
            run_id=run_id,
            writers=len(stopped),
        )
        if stopped:
            await asyncio.wait(
                stopped, timeout=SUBAGENT_CLOSE_TIMEOUT_S + CANCEL_UNWIND_TIMEOUT
            )
        stuck |= {writer for writer in stopped if not writer.done()}
    if stuck:
        raise TimeoutError(
            f"tail drain deadline: {len(stuck)} subagent writer(s) still running "
            f"for run={run_id} after a stop"
        )
