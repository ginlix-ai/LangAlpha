"""Writer-side execution and terminal settle for background subagents.

``_run_background_task`` drives one admitted run's handler to completion;
``_settle_terminal_run`` is the one owner-side terminal pipeline every
admitted run funnels through (settling writer, cancelled-before-spawn,
setup abort). Free functions — the middleware hands in its registry and
namespace-owner handles, never itself.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_token_tracker,
)
from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    parse_steering_payload,
    steering_queue_key,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
    TransportLostError,
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
    having to instrument every ``task.completed = True`` site.
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
        tracker.per_call_records or []
    )
    for name, count in tool_tracker.get_summary().items():
        task.tool_usage[name] = task.tool_usage.get(name, 0) + count


async def _return_unconsumed_steering(
    registry: "BackgroundTaskRegistry | None", task: BackgroundTask
) -> None:
    """Drain the run-scoped steering queue at run end into
    ``steering_returned`` events — accepted input a run never consumed is
    surfaced, not left for a later resume or a silent TTL death.

    Read → surface → delete, in that order: the queue is erased only after
    every entry made it into the event archive, so a spill failure (or a
    crash) between the two leaves the entries in Redis until TTL instead of
    silently destroying acknowledged input. No producer can slip in behind
    the read — the post-push verify sees the terminal meta (written before
    this sweep) and reclaims its own entry."""
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not cache.enabled or not cache.client:
            return
        if task.redis_write_failed or registry is None:
            # Torn transport: appends would no-op against the open circuit
            # and the delete would erase input nothing surfaced.
            return
        key = steering_queue_key(task.tool_call_id, task.task_run_id)
        raw_messages = await cache.client.lrange(key, 0, -1) or []
        if not raw_messages:
            return
        ts = time.time()
        seq_before = task.captured_event_seq
        appended = 0
        for raw in raw_messages:
            payload = parse_steering_payload(raw)
            if payload is None:
                continue
            appended += 1
            # Identity-exact append: this settle may run after cancel
            # teardown evicted the entry (or its tool_call_id was reused
            # by a re-registration) — resolving by id would drop the frame
            # or write it into the replacement task's streams.
            await registry.append_event_for_task(
                task,
                {
                    "event": "steering_returned",
                    "data": {
                        "agent": f"task:{task.task_id}",
                        "content": payload["content"],
                        "input_id": payload["input_id"],
                        "reason": "run_ended",
                    },
                    "ts": ts,
                },
                # A kill seals the task's streams; the returned-input record
                # is unwind bookkeeping that must land regardless — without
                # it the accepted instruction is neither delivered nor
                # returned anywhere durable, and the queue delete below
                # erases the last copy.
                terminal=True,
            )
        if task.redis_write_failed:
            return  # a spill tore mid-sweep — keep the queue for the record
        if task.captured_event_seq - seq_before < appended:
            # Landed-check: identity-exact appends only skip on the
            # cancelled/terminal guard, so a seq lag means the frames did
            # NOT reach the archive — keep the queue as the TTL record
            # rather than erase acknowledged input nothing surfaced.
            logger.warning(
                "unconsumed-steering sweep withheld; appends did not land",
                task_id=task.task_id,
                task_run_id=task.task_run_id,
                pending=appended,
            )
            return
        await cache.client.delete(key)
        logger.info(
            "returned unconsumed steering input",
            task_id=task.task_id,
            task_run_id=task.task_run_id,
            count=len(raw_messages),
        )
    except Exception:
        logger.warning(
            "unconsumed-steering sweep failed",
            task_id=task.task_id,
            exc_info=True,
        )


def _transport_lost_failure() -> dict[str, Any]:
    return {
        "error": (
            "transport_lost: the task's Redis event stream tore mid-run "
            "(spill failure or quota); the replay archive is incomplete"
        ),
        "error_type": "transport_lost",
    }


def _error_type(e: BaseException) -> str:
    # One spelling for the retention contract's torn-transport terminal —
    # consumers grep "transport_lost", never the class name.
    return "transport_lost" if isinstance(e, TransportLostError) else type(e).__name__


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
        return await handler(request)

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
        _merge_subagent_usage(task, tracker, tool_tracker)
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
            ledger_status = "error"
            ledger_failure = _transport_lost_failure()
            logger.error(
                "%s finished with a torn event stream; finalizing transport_lost",
                label,
                display_id=task.display_id,
            )
            return {"success": False, **_transport_lost_failure()}
        logger.debug(
            "%s completed",
            label,
            display_id=task.display_id,
            result_type=type(result).__name__,
            token_records=len(task.per_call_records),
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
            _merge_subagent_usage(task, tracker, tool_tracker)
            if task.redis_write_failed and not task.cancelled:
                ledger_status = "error"
                ledger_failure = _transport_lost_failure()
                return {"success": False, **_transport_lost_failure()}
            return {"success": True, "result": result}
        except Exception as e:
            ledger_status = "error"
            ledger_failure = {"error": str(e), "error_type": _error_type(e)}
            _merge_subagent_usage(task, tracker, tool_tracker)
            logger.error(
                "%s failed after cancellation",
                label,
                display_id=task.display_id,
                error=str(e),
            )
            return {"success": False, "error": str(e), "error_type": _error_type(e)}
    except Exception as e:
        ledger_status = "error"
        ledger_failure = {"error": str(e), "error_type": _error_type(e)}
        _merge_subagent_usage(task, tracker, tool_tracker)
        logger.error(
            "%s failed",
            label,
            display_id=task.display_id,
            error=str(e),
        )
        return {"success": False, "error": str(e), "error_type": _error_type(e)}
    finally:
        # A double-cancel can exit while the shielded handler still runs; in
        # that case keep the fence (the guard's teardown unlock_all reclaims
        # it) and leave the meta "running" until its TTL — never unfence a
        # possibly-live writer.
        if handler_task.done():
            await _settle_terminal_run(
                task,
                ledger_status=ledger_status,
                ledger_failure=ledger_failure,
                registry=registry,
                namespace_owner=namespace_owner,
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
    terminal meta → steering sweep → conditional run_end → stream sentinel +
    retention → fence release. Terminal meta is written BEFORE the namespace
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
        if task.cancelled
        else ("completed" if ledger_status == "completed" else "error")
    )
    run_finalized = False
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
                # run_end is appended below, AFTER the steering
                # sweep — its steering_returned frames must precede
                # the terminal frame, and nothing may follow it.
                defer_run_end=True,
            )
            run_finalized = True
            cas_applied = bool(finalized.get("applied"))
            row_status = (finalized.get("run") or {}).get("status")
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
    # Accepted-but-unconsumed steering must not outlive its run: the
    # sender was told "success", and the run-scoped queue would
    # otherwise sit in Redis until TTL. Sweep it into
    # steering_returned events (after the terminal meta, so producers
    # that re-check status can no longer enqueue behind the sweep).
    # A lost CAS skips the sweep: the recovery winner's run_end is
    # already on the stream, and frames appended after it are never
    # read — worse, they'd bait append_run_end's tail check into a
    # second terminal frame. That steering TTLs out with the queue.
    if task.task_run_id and cas_applied:
        await _return_unconsumed_steering(registry, task)
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
    # at astream-loop exit would make the sweep's steering_returned
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
