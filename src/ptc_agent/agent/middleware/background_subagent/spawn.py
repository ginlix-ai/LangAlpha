"""The canonical pre-spawn pipeline: admission refusals and the writer spawn.

Every background task — Task tool init, resume, direct dispatch, workflow run —
is admitted and spawned through here, so the step order below is written once.
This is a leaf on purpose: the Task-tool path lives *under* the middleware and
the dispatch path *over* it, and only a module both can import without closing
the cycle can hold the shared pipeline.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.agent.middleware.background_subagent import run_executor
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
)
from src.observability.tracing import (
    create_task_with_context,
    emit_subagent_launch,
)
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        BackgroundSubagentMiddleware,
    )

logger = structlog.get_logger(__name__)


class TaskRunRefused(Exception):
    """Admission refused: nothing is held and no run is live.

    ``reason`` completes the agent-facing ``could not start <task> — <reason>.``
    The settle stays with the caller because it differs by cause — a refused
    launch goes inert, a refused resume has to stay resumable.
    """

    settle_reason = "run admission rejected"

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class NamespaceUnfenced(TaskRunRefused):
    """Refused at the fence rather than the ledger: no row was ever born."""

    settle_reason = "namespace fence unavailable"


class SpawnError(RuntimeError):
    """A writer could not be spawned; the task was settled before this was
    raised. Callers that need the two causes apart catch the subclasses."""


class SpawnSetupError(SpawnError):
    """Setup raised before the writer was published."""


class SpawnStoppedError(SpawnError):
    """A stop stamped the task mid-setup, so the publish fence refused it."""


def settle_never_started(task: BackgroundTask, exc: Exception) -> None:
    """Default pre-spawn settle: the entry goes inert and is not retried."""
    task.mark_never_started(f"setup failed before spawn: {exc}")


async def spawn_task_writer(
    mw: "BackgroundSubagentMiddleware",
    task: BackgroundTask,
    runner: Callable[[Any], Awaitable[Any]],
    *,
    prompt: str,
    label: str,
    name: str,
    action: str,
    request: Any = None,
    description: str | None = None,
    on_setup_failure: Callable[[BackgroundTask, Exception], None] = (
        settle_never_started
    ),
) -> None:
    """The one writer spawn — every background task goes through here.

    The step order is load-bearing and must not be reordered: launch
    telemetry → pre-spawn "running" meta → run-opener event → fenced writer
    publish → done-callback. The meta precedes the publish because a fast
    writer's terminal meta (written at settle) must never be overwritten by
    a late "running", and the run-opener precedes it so the event exists
    before any captured child event can reference it.

    Failure raises — ``SpawnSetupError`` or ``SpawnStoppedError`` — always
    after settling the task, so no caller can leak a stranded in_progress
    row. ``on_setup_failure`` chooses how it settles: inert by default,
    which is wrong only for a resume, whose durable result survives in the
    ``task:{id}`` checkpoint and must stay resumable.
    """
    tracker = PerCallTokenTracker()
    try:
        emit_subagent_launch(
            task.subagent_type,
            action=action,
            description_len=len(
                task.description or "" if description is None else description
            ),
        )
        await mw.registry.write_task_meta(task, "running")
        await mw._append_run_opener(task, prompt)
        asyncio_task = await mw.registry.publish_writer(
            task,
            lambda: create_task_with_context(
                run_executor._run_background_task(
                    task,
                    runner,
                    request,
                    tracker,
                    label,
                    registry=mw.registry,
                    namespace_owner=mw.namespace_owner,
                ),
                name=name,
            ),
        )
    except Exception as e:
        # An admitted run either spawns or terminates — never a stranded
        # in_progress row (for un-admitted children the settle still writes
        # terminal meta and releases the fence).
        await mw._abort_admitted_run(task, e)
        on_setup_failure(task, e)
        raise SpawnSetupError(f"setup failed before spawn: {e}") from e
    if asyncio_task is None:
        # A stop stamped the (handle-less) task during a setup await — the
        # publish fence refused the writer.
        await mw._finalize_cancelled_before_spawn(task)
        raise SpawnStoppedError(f"{task.display_id} was stopped before it started")
    asyncio_task.add_done_callback(
        run_executor._make_task_done_callback(
            task, mw._finalize_cancelled_before_spawn
        )
    )
