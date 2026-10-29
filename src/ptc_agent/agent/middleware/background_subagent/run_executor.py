"""Writer-side execution and terminal settle for background subagents.

``_run_background_task`` drives one admitted run's handler to completion;
``_settle_terminal_run`` is the one owner-side terminal pipeline every
admitted run funnels through (settling writer, cancelled-before-spawn,
setup abort). Free functions — the middleware hands in its registry and
namespace-owner handles, never itself.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional

import structlog
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_token_tracker,
)
from ptc_agent.agent.middleware.credit_gate import (
    CreditGateState,
    current_credit_gate,
)
from ptc_agent.agent.middleware.background_subagent.outcomes import Outcome
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)

if TYPE_CHECKING:
    from src.tools.decorators import ToolUsageTracker
    from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

logger = structlog.get_logger(__name__)


# Strong refs for the fire-and-forget pre-start finalizations below —
# asyncio holds tasks weakly, and losing one mid-cleanup would leak the
# ledger row / namespace fence it exists to settle.
_pre_start_finalizations: set[asyncio.Task] = set()


def _make_task_done_callback(
    task: BackgroundTask,
    on_never_started: Callable[[BackgroundTask], Awaitable[None]] | None = None,
) -> Callable[[asyncio.Task], None]:
    """Build a done_callback that bumps ``last_updated_at`` when the asyncio.Task finishes.

    Covers all completion paths (success, failure, cancellation) without
    having to instrument every site that settles a task.
    """

    def _on_task_done(_t: asyncio.Task) -> None:
        task.last_updated_at = time.time()
        # A writer cancelled before its coroutine's first tick never entered
        # _run_background_task, so the settle finally (ledger CAS, terminal
        # meta, fence release) never ran. ``handler_task`` is assigned before
        # the coroutine's first await, so None here is a precise
        # never-started marker — finalize the run as cancelled-before-spawn.
        if (
            on_never_started is None
            or not _t.cancelled()
            or task.handler_task is not None
        ):
            return
        cleanup = asyncio.get_running_loop().create_task(
            on_never_started(task)
        )
        _pre_start_finalizations.add(cleanup)
        cleanup.add_done_callback(_pre_start_finalizations.discard)

    return _on_task_done


def _merge_subagent_usage(
    task: BackgroundTask,
    tracker: "PerCallTokenTracker",
    tool_tracker: "ToolUsageTracker",
) -> None:
    """Merge this run's token + tool usage into any unpersisted usage on the task.

    Resume re-runs a task before the collector may have persisted the prior
    run; appending records and summing tool counts (instead of replacing) keeps
    the prior run's usage until cleanup drops it after a successful persist.
    """
    task.per_call_records = (task.per_call_records or []) + (
        tracker.get_per_call_records()
    )
    for name, count in tool_tracker.get_summary().items():
        task.tool_usage[name] = task.tool_usage.get(name, 0) + count


async def _emit_returned_steering(
    registry: "BackgroundTaskRegistry | None",
    task: BackgroundTask,
    run_row: dict[str, Any],
) -> None:
    """Stream the steering the run gave back unread as ``steering_returned``
    frames, ahead of its ``run_end``. The ledger settled them in the
    transaction that stamped the cancel or ended the run, and kept them on
    the row for replay; these frames are the live copy."""
    facts = run_row.get("replay_facts")
    returned = facts.get("steering_returned") if isinstance(facts, dict) else None
    if registry is None or not returned:
        return
    ts = time.time()
    for entry in returned:
        try:
            # Identity-exact append: this settle may run after cancel
            # teardown evicted the entry (or its tool_call_id was reused by a
            # re-registration). ``terminal`` lets the frames through a kill's
            # stream seal: they are unwind bookkeeping.
            await registry.append_event_for_task(
                task,
                {
                    "event": "steering_returned",
                    "data": {"agent": f"task:{task.task_id}", **entry},
                    "ts": ts,
                },
                terminal=True,
            )
        except Exception:
            logger.warning(
                "steering_returned frame append failed",
                task_id=task.task_id,
                exc_info=True,
            )


@asynccontextmanager
async def _child_credit_lane(
    task: BackgroundTask,
    tracker: "PerCallTokenTracker",
    tool_tracker: Any,
    label: str,
) -> AsyncIterator[Optional[CreditGateState]]:
    """This child's credit lane: its own run ref, metering and ledger heartbeat,
    sharing the turn's lease (inherited via context copy at task creation, so a
    resumed task joins whichever turn resumed it).

    The ContextVar is set on every path, including to None, so a lane that
    cannot be opened runs ungated rather than under its parent's state.
    """
    parent_gate = current_credit_gate.get()
    gate: Optional[CreditGateState] = None
    if parent_gate is not None and task.task_run_id:
        gate = parent_gate.spawn_child(
            run_ref=task.task_run_id,
            tracker=tracker,
            tool_tracker=tool_tracker,
        )
        await gate.start()
    elif parent_gate is not None:
        # Degraded admission left this task without a ledger row, so it has
        # nowhere to heartbeat and never joins the lease: it runs ungated, and
        # its spend does not reach the family total the parent is measured
        # against. Not fatal, but silent is how a hole like this stays open.
        logger.warning(
            "%s has no ledger row; running without a credit lane",
            label,
            display_id=task.display_id,
        )
    current_credit_gate.set(gate)
    try:
        yield gate
    finally:
        if gate is not None:
            await gate.aclose()


async def _run_background_task(
    task: BackgroundTask,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    request: ToolCallRequest,
    tracker: "PerCallTokenTracker",
    label: str,
    registry: BackgroundTaskRegistry | None = None,
    namespace_owner: Any | None = None,
) -> dict[str, Any]:
    """Execute a subagent handler in a background asyncio.Task.

    Shared by both the new-spawn and resume paths. On writer settle the
    finally block releases the task's namespace lock and mirrors terminal
    liveness to the Redis task meta — both only when the handler task is
    actually done, so a shielded still-live writer is never unfenced.
    """
    from src.tools.decorators import ToolUsageTracker, set_tool_tracker

    tool_tracker = ToolUsageTracker()

    async def run_handler() -> ToolMessage | Command:
        current_background_token_tracker.set(tracker)
        set_tool_tracker(tool_tracker)
        try:
            async with _child_credit_lane(task, tracker, tool_tracker, label):
                return await handler(request)
        finally:
            # The astream loop sends a held delta only once it is due, so the
            # run's last one is still held here. It goes out before the
            # outcome is judged on the stream's health: a spill that fails
            # after that would leave a "completed" run with no run_end.
            await _flush_held_delta(task, registry)

    handler_task: asyncio.Task[ToolMessage | Command] = asyncio.create_task(
        run_handler()
    )
    task.handler_task = handler_task
    # Ledger outcome, decided by whichever branch settles the writer; the
    # default only survives a path that exits without settling (double
    # cancel), where the finally skips finalize anyway.
    ledger_status = "error"
    ledger_failure: dict[str, Any] | None = {
        "error": "writer terminated without settling"
    }
    try:
        result = await asyncio.shield(handler_task)
        ledger_status, ledger_failure = "completed", None
        if isinstance(result, ToolMessage) and result.status == "error":
            # e.g. schema validation rejected the call before the subagent
            # ran. The run did not succeed — ledger, task result, and
            # terminal meta all record it as such; a success=True here would
            # let the report-back gate and TaskOutput present the failure
            # as a delivered result.
            ledger_status = "error"
            ledger_failure = {
                "error": str(result.content)[:2000],
                "error_type": "tool_error",
            }
            logger.error(
                "%s returned a tool error without running",
                label,
                display_id=task.display_id,
                error_preview=str(result.content)[:300],
            )
            return {
                "success": False,
                "error": str(result.content)[:2000],
                "error_type": "tool_error",
            }
        if task.redis_write_failed and not task.cancelled:
            # Retention contract: a run whose event spill tore mid-flight
            # must not settle "completed" — the replay archive has holes the
            # consumer can't detect. The abort loop usually raises first;
            # this covers a tear on the final events.
            torn = Outcome.torn_stream()
            ledger_status, ledger_failure = torn.status, torn.as_failure()
            logger.error(
                "%s finished with a torn event stream; finalizing transport_lost",
                label,
                display_id=task.display_id,
            )
            return torn.as_result()
        logger.debug(
            "%s completed",
            label,
            display_id=task.display_id,
            result_type=type(result).__name__,
            token_records=tracker.record_count(),
        )
        return {"success": True, "result": result}
    except asyncio.CancelledError:
        logger.info(
            "%s cancellation requested; continuing",
            label,
            display_id=task.display_id,
        )
        try:
            result = await handler_task
            ledger_status, ledger_failure = "completed", None
            if task.redis_write_failed and not task.cancelled:
                torn = Outcome.torn_stream()
                ledger_status, ledger_failure = torn.status, torn.as_failure()
                return torn.as_result()
            return {"success": True, "result": result}
        except Exception as e:
            outcome = Outcome.from_exception(e)
            ledger_status, ledger_failure = outcome.status, outcome.as_failure()
            logger.error(
                "%s failed after cancellation",
                label,
                display_id=task.display_id,
                error=str(e),
            )
            return outcome.as_result()
    except Exception as e:
        outcome = Outcome.from_exception(e)
        ledger_status, ledger_failure = outcome.status, outcome.as_failure()
        logger.error(
            "%s failed",
            label,
            display_id=task.display_id,
            error=str(e),
        )
        return outcome.as_result()
    finally:
        # A double-cancel can exit while the shielded handler still runs; in
        # that case keep the fence (the guard's teardown unlock_all reclaims
        # it) and leave the meta "running" until its TTL — never unfence a
        # possibly-live writer. A live handler is also still appending to the
        # tracker, so the merge below waits for the same condition.
        if handler_task.done():
            # The one merge for every settle path. Doing it per-branch instead
            # enumerates the ways a run can succeed, and a run force-cancelled
            # mid-flight ends via a CancelledError that reaches none of them —
            # its tokens were spent upstream and would go unbilled.
            _merge_subagent_usage(task, tracker, tool_tracker)
            await _settle_terminal_run(
                task,
                ledger_status=ledger_status,
                ledger_failure=ledger_failure,
                registry=registry,
                namespace_owner=namespace_owner,
            )


async def _flush_held_delta(
    task: BackgroundTask, registry: "BackgroundTaskRegistry | None"
) -> None:
    if registry is None:
        return
    try:
        await registry.flush_held_delta(task)
    except Exception:
        logger.warning(
            "subagent_held_delta_flush_failed",
            task_id=task.task_id,
            exc_info=True,
        )


async def _settle_terminal_run(
    task: BackgroundTask,
    *,
    ledger_status: str,
    ledger_failure: dict[str, Any] | None,
    registry: "BackgroundTaskRegistry | None",
    namespace_owner: Any | None,
) -> None:
    """Owner-side terminal pipeline — the one way an admitted run settles.

    Shared by the settling writer and the cancelled-before-spawn paths
    (publish-fence refusal, writer cancelled before its first tick) so every
    terminal honors the stream contract: ledger CAS (deferred run_end) →
    terminal meta → returned steering frames → conditional run_end → stream
    sentinel + retention → fence release. Terminal meta is written BEFORE the namespace
    releases: every meta write happens while holding N(task:id), so a
    successor (who can only acquire after the release) always writes
    "running" after this writer's terminal state — never under it.
    """
    # Ledger CAS first (durable truth), still under N(task:id) so
    # finalize-vs-successor ordering matches the meta's guarantee. A
    # failed CAS is left to orphan recovery — the row stays
    # in_progress and the released guard makes it provably dead.
    run_ledger = getattr(registry, "run_ledger", None) if registry else None
    # Local view of the outcome — superseded by the CAS-returned row
    # status below: the DB overrides a settle to 'cancelled' when a
    # durable cancel intent raced it, and a lost CAS returns the
    # survivor. Meta must mirror the row, not the local request.
    terminal_status = (
        "cancelled"
        if task.cancelled or ledger_status == "cancelled"
        else ("completed" if ledger_status == "completed" else "error")
    )
    run_finalized = False
    run_row: dict[str, Any] = {}
    # False only when this coroutine provably lost the terminal CAS
    # to a recovery finalizer (whose run_end is already appended).
    # A finalize *exception* keeps it True: the row is still open,
    # this writer still owns the stream, and recovery will append
    # run_end after whatever it writes.
    cas_applied = True
    if run_ledger is not None and task.task_run_id:
        try:
            finalized = await run_ledger.finalize_task_run(
                task.task_run_id,
                "cancelled" if task.cancelled else ledger_status,
                task_id=task.task_id,
                # A user cancel is not a failure — don't let the
                # unwind path's default failure text ride the row.
                failure=None if task.cancelled else ledger_failure,
                # run_end is appended below, AFTER the run's
                # steering_returned frames — they must precede the
                # terminal frame, and nothing may follow it.
                defer_run_end=True,
            )
            run_finalized = True
            cas_applied = bool(finalized.get("applied"))
            run_row = finalized.get("run") or {}
            row_status = run_row.get("status")
            if row_status:
                terminal_status = str(row_status)
        except Exception:
            logger.warning(
                "task-run ledger finalize failed",
                task_id=task.task_id,
                task_run_id=task.task_run_id,
                exc_info=True,
            )
    if registry is not None:
        try:
            await registry.write_task_meta(task, terminal_status)
        except Exception:
            logger.warning(
                "terminal task-meta write failed",
                task_id=task.task_id,
                exc_info=True,
            )
    # Steering the run gave back unread reaches the stream before run_end.
    # A lost CAS skips it: the recovery winner's run_end is already on the
    # stream, frames appended after it are never read, and they would bait
    # append_run_end's tail check into a second terminal frame. Replay still
    # shows them, from the row.
    if run_finalized and cas_applied:
        await _emit_returned_steering(registry, task, run_row)
    # The cursor-bearing run_end closes the v2 run stream — only
    # after a durable CAS won by THIS writer (a lost CAS means the
    # recovery finalizer already appended it; a still-open row must
    # stay probe-visible) and only on a healthy transport: a torn
    # stream resolves via the ledger backstop + replay, never reads
    # as complete.
    if run_finalized and cas_applied and not task.redis_write_failed:
        try:
            await run_ledger.append_run_end(
                task.task_run_id,
                task_id=task.task_id,
                outcome=terminal_status,
                finalized_at=run_row.get("finalized_at"),
            )
        except Exception:
            logger.warning(
                "subagent_run_end_append_failed",
                task_id=task.task_id,
                task_run_id=task.task_run_id,
                exc_info=True,
            )
    # Seal the per-task stream LAST. Content spills XADD with
    # explicit ``{seq}-0`` ids and Redis rejects ids behind the
    # sentinel's auto-generated (timestamp) id — a sentinel written
    # at astream-loop exit would make the steering_returned frames'
    # spill fail and trip the write circuit-breaker.
    if registry is not None:
        # Identity-exact stream ops: this settle may outlive the task's
        # registry entry (cancel teardown eviction, tool_call_id reuse) —
        # id-resolving variants would no-op or hit the replacement task.
        try:
            await registry.append_sentinel_for_task(task)
        except Exception:
            logger.warning(
                "subagent_sentinel_write_failed",
                task_id=task.task_id,
                exc_info=True,
            )
        # Attach-grace TTL starts at terminal (active streams carry
        # none) — stamped even when redis_write_failed skipped the
        # sentinel, so a torn stream expires instead of leaking.
        try:
            await registry.stamp_terminal_retention_for_task(task)
        except Exception:
            logger.warning(
                "subagent_terminal_ttl_stamp_failed",
                task_id=task.task_id,
                exc_info=True,
            )
    if namespace_owner is not None:
        try:
            await namespace_owner.release_task_ns(task.task_id)
        except Exception:
            logger.warning(
                "task-ns release failed",
                task_id=task.task_id,
                exc_info=True,
            )
