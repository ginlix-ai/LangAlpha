"""Background subagent execution middleware.

This middleware intercepts 'Task' tool calls and spawns them in the background,
allowing the main agent to continue working without blocking.
"""

import asyncio
import contextvars
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
    TaskRunRejected,
    TaskWriterLive,
    TransportLostError,
    parse_steering_payload,
    steering_queue_key,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    create_task_output_tool,
)
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

from src.observability.tracing import (
    create_task_with_context,
    emit_subagent_launch,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.event_capture import (
        SubagentEventCaptureMiddleware,
    )
    from src.tools.decorators import ToolUsageTracker

# This ContextVar propagates tool_call_id to subagent tool calls, used by
# SubagentEventCaptureMiddleware to track which background task a tool call
# belongs to.
current_background_tool_call_id: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("current_background_tool_call_id", default=None)
)

# This ContextVar propagates the unified agent identity (e.g., "research:uuid4")
# to subagent tool calls, for internal tool tracking.
current_background_agent_id: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("current_background_agent_id", default=None)
)

# This ContextVar propagates a dedicated PerCallTokenTracker to the subagent
# so its LLM calls are tracked separately from the parent agent's tracker.
current_background_token_tracker: contextvars.ContextVar[PerCallTokenTracker | None] = (
    contextvars.ContextVar("current_background_token_tracker", default=None)
)

logger = structlog.get_logger(__name__)


def _make_task_done_callback(task: BackgroundTask) -> Callable[[asyncio.Task], None]:
    """Build a done_callback that bumps ``last_updated_at`` when the asyncio.Task finishes.

    Covers all completion paths (success, failure, cancellation) without
    having to instrument every ``task.completed = True`` site.
    """

    def _on_task_done(_t: asyncio.Task) -> None:
        task.last_updated_at = time.time()

    return _on_task_done


def _truncate_description(description: str, max_sentences: int = 2) -> str:
    """Return the first N sentences of description (period-delimited)."""
    sentences = []
    remaining = description
    for _ in range(max_sentences):
        period_idx = remaining.find(".")
        if period_idx == -1:
            sentences.append(remaining)
            break
        sentences.append(remaining[: period_idx + 1])
        remaining = remaining[period_idx + 1 :].lstrip()
        if not remaining:
            break
    return " ".join(sentences)


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
        for raw in raw_messages:
            payload = parse_steering_payload(raw)
            if payload is None:
                continue
            await registry.append_captured_event(
                task.tool_call_id,
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
            )
        if task.redis_write_failed:
            return  # a spill tore mid-sweep — keep the queue for the record
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
        # Terminal meta is written BEFORE the namespace releases: every meta
        # write happens while holding N(task:id), so a successor (who can
        # only acquire after the release) always writes "running" after this
        # writer's terminal state — never under it.
        if handler_task.done():
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
            if run_ledger is not None and task.task_run_id:
                try:
                    finalized = await run_ledger.finalize_task_run(
                        task.task_run_id,
                        "cancelled" if task.cancelled else ledger_status,
                        task_id=task.task_id,
                        # A user cancel is not a failure — don't let the
                        # unwind path's default failure text ride the row.
                        failure=None if task.cancelled else ledger_failure,
                    )
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
            if task.task_run_id:
                await _return_unconsumed_steering(registry, task)
            # Seal the per-task stream LAST. Content spills XADD with
            # explicit ``{seq}-0`` ids and Redis rejects ids behind the
            # sentinel's auto-generated (timestamp) id — a sentinel written
            # at astream-loop exit would make the sweep's steering_returned
            # spill fail and trip the write circuit-breaker.
            if registry is not None:
                try:
                    await registry.append_sentinel_to_stream(task.tool_call_id)
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
                    await registry.stamp_terminal_retention(task.tool_call_id)
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


class BackgroundSubagentMiddleware(AgentMiddleware):
    """Intercepts Task tool calls and spawns them as background asyncio tasks.

    Returns an immediate pseudo-result to the main agent so it can continue
    working while subagents execute. The BackgroundSubagentOrchestrator
    collects pending results after the main agent finishes and re-invokes
    it for synthesis.
    """

    def __init__(
        self,
        timeout: float = 60.0,
        *,
        enabled: bool = True,
        registry: BackgroundTaskRegistry | None = None,
        event_capture_middleware: "SubagentEventCaptureMiddleware | None" = None,
        checkpointer: Any | None = None,
        namespace_owner: Any | None = None,
    ) -> None:
        """
        Args:
            checkpointer: LangGraph checkpointer used to hydrate tasks from stored
                state when the in-memory registry loses them (e.g. server restart).
            namespace_owner: optional writer fence for task checkpoint namespaces
                (``acquire_task_ns(task_id) -> bool`` / ``release_task_ns``, e.g.
                the run's WriterGuard). None = single-writer deployment; spawn and
                resume skip the fence.
        """
        super().__init__()
        self.registry = registry or BackgroundTaskRegistry()
        self.timeout = timeout
        self.enabled = enabled
        self.event_capture_middleware = event_capture_middleware
        self.checkpointer = checkpointer
        self.namespace_owner = namespace_owner
        # Task ids with a resume mid-flight: the liveness check and the
        # writer spawn are separated by awaits, so two parallel resume calls
        # in one model step could otherwise both pass and double-spawn (the
        # per-session namespace fence is deliberately idempotent for one
        # run and cannot arbitrate between coroutines sharing it).
        self._resume_claims: set[str] = set()

        # Create native tools for this middleware
        # These allow the main agent to wait for and check on background tasks
        self.tools = [
            create_task_output_tool(self),
        ]

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Sync path: no background spawn, falls back to blocking execution."""
        return handler(request)

    async def _queue_followup_to_redis(
        self, task: "BackgroundTask", description: str
    ) -> str | None:
        """Push a follow-up message onto the task's steering queue.

        Fenced by run identity: the payload stamps the run the sender
        believes it is steering, and the queue key is run-scoped so a later
        resume can never consume it. Returns the message's input_id, or None
        when queuing failed.
        """
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not cache.enabled or not cache.client:
                return None

            input_id = uuid.uuid4().hex
            key = steering_queue_key(task.tool_call_id, task.task_run_id)
            payload = json.dumps(
                {
                    "content": description,
                    "expected_task_run_id": task.task_run_id,
                    "input_id": input_id,
                }
            )
            await cache.client.rpush(key, payload)
            # 1 hour TTL — if not consumed, it's stale
            await cache.client.expire(key, 3600)

            # Push-then-verify: the terminal sweep drains this queue once,
            # so a push that lands after it would sit unread until TTL
            # while the sender reports success. Terminal meta is written
            # BEFORE the sweep, so a meta read issued AFTER our push that
            # still says "running" proves the sweep hadn't started — it
            # will collect our entry if the run ends before delivery.
            stale = task.completed or task.cancelled
            try:
                if not stale:
                    from ptc_agent.agent.middleware.background_subagent.registry import (
                        read_task_meta,
                    )

                    meta = await read_task_meta(
                        self.registry.thread_id or "", task.task_id
                    )
                    if meta is not None:
                        # "Running" must mean THIS run: a rotated epoch means
                        # our run-scoped entry sits on a queue whose one sweep
                        # has already passed.
                        stale = meta.get("status") != "running" or bool(
                            task.task_run_id
                            and (meta.get("task_run_id") or None)
                            != task.task_run_id
                        )
                    else:
                        # Meta lapsed: the injected ledger (when present) is
                        # the durable authority. An unknown task fails open —
                        # the arbitration must never be worse than admission.
                        ledger = getattr(self.registry, "run_ledger", None)
                        if ledger is not None:
                            row = await ledger.get_latest_run(task.task_id)
                            stale = row is not None and (
                                row.get("status") != "in_progress"
                                or bool(
                                    task.task_run_id
                                    and str(row.get("task_run_id"))
                                    != task.task_run_id
                                )
                            )
            except Exception:
                stale = False  # unreadable authority keeps the admission
            if stale:
                await cache.client.lrem(key, 0, payload)
                return None
            return input_id
        except Exception as e:
            logger.error(
                "Failed to queue follow-up to Redis",
                task_id=task.task_id,
                error=str(e),
            )
            return None

    async def _acquire_task_ns(self, task_id: str) -> bool:
        """Fail-closed wrapper around the namespace fence: an acquire error
        refuses the namespace rather than admitting an unfenced writer."""
        try:
            return bool(await self.namespace_owner.acquire_task_ns(task_id))
        except Exception:
            logger.warning(
                "task-ns acquire raised; refusing", task_id=task_id, exc_info=True
            )
            return False

    async def _release_task_ns(self, task_id: str) -> None:
        """Best-effort fence release for admission/setup failure paths — no
        writer will spawn, so the namespace must not stay held."""
        if self.namespace_owner is None:
            return
        try:
            await self.namespace_owner.release_task_ns(task_id)
        except Exception:
            logger.warning(
                "task-ns release failed on admission abort",
                task_id=task_id,
                exc_info=True,
            )

    async def _admit_task_run(
        self,
        task: BackgroundTask,
        *,
        cause: str,
        description: str,
        launch_tool_call_id: str,
        parent_run_id: str | None,
    ) -> str | ToolMessage:
        """Bear the ledger row for this execution (admission-authoritative).

        Called under the task's namespace guard, before any spawn side
        effect. Returns the task_run_id, or — after releasing the guard —
        an error ToolMessage: on conflict the spawn is rejected, and on
        ledger infra failure it fails closed (a run we cannot record is a
        run we do not start). No ledger injected → returns "" and the task
        keeps task_run_id=None.
        """
        ledger = getattr(self.registry, "run_ledger", None)
        if ledger is None:
            return ""
        try:
            return await ledger.start_task_run(
                task_id=task.task_id,
                cause=cause,
                description=description,
                subagent_type=task.subagent_type,
                parent_run_id=parent_run_id,
                launch_tool_call_id=launch_tool_call_id,
            )
        except TaskRunRejected as e:
            logger.warning(
                "task-run admission rejected",
                task_id=task.task_id,
                cause=cause,
                reason=e.reason,
            )
            await self._release_task_ns(task.task_id)
            return ToolMessage(
                content=f"Error: could not start {task.display_id} — {e.reason}.",
                tool_call_id=launch_tool_call_id,
                name="Task",
            )
        except Exception:
            logger.error(
                "task-run ledger unavailable; refusing spawn",
                task_id=task.task_id,
                cause=cause,
                exc_info=True,
            )
            await self._release_task_ns(task.task_id)
            return ToolMessage(
                content=(
                    f"Error: could not start {task.display_id} — its run "
                    f"could not be recorded. Try again."
                ),
                tool_call_id=launch_tool_call_id,
                name="Task",
            )

    async def _abort_admitted_run(self, task: BackgroundTask, exc: Exception) -> None:
        """Post-INSERT setup failure: finalize the just-born row as error and
        release the fence — an admitted run either spawns or terminates, it
        never strands in_progress."""
        ledger = getattr(self.registry, "run_ledger", None)
        if ledger is not None and task.task_run_id:
            try:
                await ledger.finalize_task_run(
                    task.task_run_id,
                    "error",
                    task_id=task.task_id,
                    failure={
                        "error": f"setup failed before spawn: {exc}",
                        "error_type": type(exc).__name__,
                    },
                )
            except Exception:
                logger.warning(
                    "setup-failure ledger finalize failed",
                    task_id=task.task_id,
                    task_run_id=task.task_run_id,
                    exc_info=True,
                )
        await self._release_task_ns(task.task_id)

    async def _append_run_opener(self, task: BackgroundTask, prompt: str) -> None:
        """First stream entry of a run's epoch: the instruction that launched
        it (spawn and resume alike) — the live counterpart of the checkpointed
        run-boundary HumanMessage, so live and replay share one transcript
        shape. Best-effort: the run proceeds without it."""
        try:
            await self.registry.append_captured_event(
                task.tool_call_id,
                {
                    "event": "user_message",
                    "data": {
                        "agent": f"task:{task.task_id}",
                        "role": "user",
                        "content": prompt,
                    },
                    "ts": time.time(),
                },
            )
        except Exception:
            logger.warning(
                "run-opener user_message append failed",
                task_id=task.task_id,
                exc_info=True,
            )

    async def _reset_task_for_resume(self, task: BackgroundTask) -> None:
        """Reset a completed task's state so it can be re-run.

        Steal-then-delete ordering is load-bearing: the collector claim is
        cleared (and registry membership restored) BEFORE the awaited Redis
        deletes, so an in-flight collector pass can't pass its ownership
        fence mid-reset — evicting the entry, nulling the new writer's
        handles, or claiming the resumed round's result under the prior
        round's response id. The deletes then clear the event keys so the
        resumed run starts fresh (the seq counter resets to 0; leftovers
        would collide on replay), serialized on ``redis_spill_lock`` so a
        stale cleanup delete can't land after them and erase round-2 data.
        """
        await self.registry.reclaim_for_resume(task)
        task.completed = False
        task.result = None
        task.result_seen = False
        task.error = None
        # tool_usage / per_call_records are intentionally NOT cleared here: if a
        # collector hasn't billed the prior run yet, the next completion merges
        # into them so run-1 usage survives the resume. Cleanup drops them only
        # after a successful persist.
        task.captured_event_seq = 0
        task.captured_event_count = 0
        task.captured_event_bytes = 0
        task.redis_write_failed = False
        task.sse_drain_complete = asyncio.Event()
        task.sse_consumer_count = 0
        if self.registry.thread_id:
            try:
                from src.utils.cache.redis_cache import get_cache_client

                cache = get_cache_client()
                if getattr(cache, "enabled", False):
                    async with task.redis_spill_lock:
                        await cache.delete(
                            f"subagent:events:meta:{self.registry.thread_id}:{task.task_id}"
                        )
                        await cache.delete(
                            f"subagent:stream:{self.registry.thread_id}:{task.task_id}"
                        )
                        # One-release backward-compat sweep for the legacy List
                        # key written by pre-cutover workers. Safe to drop once
                        # no worker on the old code path is in rotation.
                        await cache.delete(
                            f"subagent:events:{self.registry.thread_id}:{task.task_id}"
                        )
            except Exception:
                logger.warning(
                    "Failed to clear Redis spool on resume; replay may include stale events",
                    task_id=task.task_id,
                    exc_info=True,
                )
        # Reset timestamps so the LLM sees honest staleness for the
        # resumed run, not leftover values from the prior asyncio.Task.
        task.last_checked_at = time.time()
        task.last_updated_at = time.time()

    async def _resolve_or_error(
        self,
        target_task_id: str | None,
        parent_thread_id: str,
        tool_call_id: str,
        action_name: str | None = None,
    ) -> "BackgroundTask | ToolMessage":
        """Resolve a task by id with hydration fallback, or return a not-found ToolMessage.

        Strips whitespace from ``target_task_id`` — LLMs occasionally emit
        trailing whitespace or newlines when copying IDs from prior tool messages.

        When ``action_name`` is supplied, the "task_id required" error includes
        it for clearer output (e.g. "Error: task_id is required for 'update'
        action.").
        """
        tid = (target_task_id or "").strip()
        if not tid:
            required_msg = (
                f"Error: task_id is required for '{action_name}' action."
                if action_name
                else "Error: task_id is required."
            )
            return ToolMessage(
                content=required_msg,
                tool_call_id=tool_call_id,
                name="Task",
            )

        task = await self.registry.get_by_task_id(tid)
        if task is None:
            task = await self._hydrate_from_checkpoint(tid, parent_thread_id)
        if task is None:
            return ToolMessage(
                content=f"Error: Task-{tid} not found.",
                tool_call_id=tool_call_id,
                name="Task",
            )
        return task

    async def _hydrate_from_checkpoint(
        self, task_id: str, parent_thread_id: str
    ) -> BackgroundTask | None:
        """Reconstruct a BackgroundTask from stored checkpoint metadata.

        Called when the in-memory registry loses a task (e.g. another worker
        owns it, or a server restart). The Redis task meta supplies routing
        identity + writer liveness: when it says "running" and a namespace
        fence is wired (multi-writer deployment), the task hydrates as LIVE
        with its real tool_call_id, so 'update' routes follow-ups to the
        steering list the remote writer actually consumes. Without a fence
        this process is the only writer, so a "running" meta is necessarily
        stale — keep the completed-shape hydration.
        Returns the task inserted into the registry, or None.
        """

        if not self.checkpointer or not parent_thread_id:
            return None
        try:
            from ptc_agent.agent.middleware.background_subagent.registry import (
                read_task_meta,
            )

            meta = await read_task_meta(parent_thread_id, task_id)
            running_elsewhere = (
                self.namespace_owner is not None
                and meta is not None
                and meta.get("status") == "running"
            )

            config = {
                "configurable": {
                    "thread_id": parent_thread_id,
                    "checkpoint_ns": f"task:{task_id}",
                }
            }
            checkpoint_tuple = await self.checkpointer.aget_tuple(config)
            if not checkpoint_tuple and not running_elsewhere:
                return None

            metadata = (
                checkpoint_tuple.metadata if checkpoint_tuple else None
            ) or {}
            subagent_type = metadata.get("subagent_type") or (meta or {}).get(
                "subagent_type", "general-purpose"
            )
            description = metadata.get("description") or (meta or {}).get(
                "description", "Restored subagent"
            )

            # Reconstruct BackgroundTask and insert into registry. The run
            # fence travels with it: without task_run_id, a cross-worker
            # update would land on the legacy task-lifetime queue with
            # expected_task_run_id=null — unreclaimed by the remote run's
            # run-scoped sweep, and delivered unfenced to any later resume.
            task = BackgroundTask(
                tool_call_id=(
                    (meta or {}).get("tool_call_id") or f"hydrated-{task_id}"
                ),
                task_id=task_id,
                description=description,
                prompt=description,
                subagent_type=subagent_type,
                completed=not running_elsewhere,
                result_seen=not running_elsewhere,
                spawned_run_id=(meta or {}).get("spawned_run_id") or None,
                task_run_id=(meta or {}).get("task_run_id") or None,
            )
            async with self.registry._lock:
                # Publish-once CAS: two concurrent resolves of one lost task
                # (parallel resume/update calls racing on the same task_id)
                # both reach here with distinct fresh objects. Letting the
                # later insert win would repoint the registry at an inert
                # duplicate while the earlier object spawns the writer —
                # drains and collectors would then see no writer and release
                # the namespace under it. First insert wins; losers adopt.
                published_tcid = self.registry._task_id_to_tool_call_id.get(
                    task_id
                )
                published = (
                    self.registry._tasks.get(published_tcid)
                    if published_tcid
                    else None
                )
                if published is not None:
                    return published
                self.registry._tasks[task.tool_call_id] = task
                self.registry._task_id_to_tool_call_id[task_id] = task.tool_call_id

            logger.info(
                "Hydrated task from checkpoint",
                task_id=task_id,
                parent_thread_id=parent_thread_id,
                subagent_type=subagent_type,
                running_elsewhere=running_elsewhere,
            )
            return task
        except Exception:
            logger.exception("Failed to hydrate from checkpoint", task_id=task_id)
            return None

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Intercept task tool calls and spawn in background.

        Routing logic based on ``action`` parameter:
        1. ``action="update"`` + ``task_id`` → queue follow-up via Redis to running task
        2. ``action="resume"`` + ``task_id`` → reset completed task and respawn in background
        3. ``action="init"`` (default) → new task spawn

        For all non-Task tools, passes through to the handler normally.
        """
        # Get tool name from request
        tool_call = request.tool_call
        tool_name = tool_call.get("name", "")

        # Only intercept 'Task' tool calls when enabled
        if not self.enabled or tool_name != "Task":
            return await handler(request)

        # Extract task details
        tool_call_id = tool_call.get("id", "unknown")
        if not tool_call_id or tool_call_id == "unknown":
            raise RuntimeError("Tool call ID is required for background tasks")
        args = tool_call.get("args", {})
        description = args.get("description", "unknown task")
        prompt = args.get("prompt", "")
        action = args.get("action", "init")
        target_task_id = args.get("task_id")
        subagent_type = args.get("subagent_type")

        # Extract parent_thread_id for hydration fallback
        parent_thread_id = (
            (request.runtime.config.get("configurable") or {}).get("thread_id", "")
            if request.runtime
            else ""
        )

        # Extract the current turn's run_id from the LangGraph config so the
        # registry can stamp it on newly-spawned subagents without relying on
        # ``registry.current_run_id`` (which races when two concurrent turns
        # on the same thread share the per-thread registry). Config metadata
        # is the reliable channel: LangChain strips the top-level ``run_id``
        # from child configs by the time a tool call executes, while metadata
        # is inherited verbatim (build_graph_config stamps run_id into both).
        runtime_config = request.runtime.config if request.runtime else {}
        current_run_id = (runtime_config.get("metadata") or {}).get(
            "run_id"
        ) or runtime_config.get("run_id")

        # --- Action-based routing ---
        if action == "update":
            # --- UPDATE: Instruct a running task via Redis ---
            resolved = await self._resolve_or_error(
                target_task_id,
                parent_thread_id,
                tool_call_id,
                action_name="update",
            )
            if isinstance(resolved, ToolMessage):
                return resolved
            task = resolved

            # The agent just looked at this task — bump last_checked_at.
            task.last_checked_at = time.time()

            if task.cancelled:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} was cancelled and cannot be updated.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            # Validate subagent_type if explicitly provided
            if subagent_type and subagent_type != task.subagent_type:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} is a '{task.subagent_type}' agent, not '{subagent_type}'.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            if not task.is_pending:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} is not running. Use action='resume' to resume a completed task.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            input_id = await self._queue_followup_to_redis(task, prompt)
            if input_id:
                task.last_updated_at = time.time()
                logger.info(
                    "Queued follow-up for running task",
                    task_id=target_task_id,
                    display_id=task.display_id,
                    input_id=input_id,
                )
                return ToolMessage(
                    content=f"Follow-up sent to **{task.display_id}**. The subagent will receive your instructions before its next reasoning step.",
                    tool_call_id=tool_call_id,
                    name="Task",
                    additional_kwargs={
                        "task_artifact": {
                            "task_id": task.task_id,
                            "task_run_id": task.task_run_id,
                            "action": "update",
                            "description": description,
                            "prompt": prompt,
                            "input_id": input_id,
                        }
                    },
                )
            else:
                # The push-then-verify recheck reclaims a follow-up that
                # landed after the run settled — report the terminal race
                # honestly instead of a generic transport error.
                meta_status = None
                try:
                    from ptc_agent.agent.middleware.background_subagent.registry import (
                        read_task_meta,
                    )

                    meta = await read_task_meta(
                        parent_thread_id or "", task.task_id
                    )
                    meta_status = (meta or {}).get("status")
                except Exception:
                    pass
                if meta_status and meta_status != "running":
                    return ToolMessage(
                        content=(
                            f"Error: Task-{target_task_id} finished "
                            f"({meta_status}) before the follow-up could be "
                            f"delivered. Check its output with "
                            f"action='output', or use action='resume' to "
                            f"continue it with new instructions."
                        ),
                        tool_call_id=tool_call_id,
                        name="Task",
                    )
                return ToolMessage(
                    content=f"Error: Could not deliver follow-up to {task.display_id} -- message queue not available.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

        elif action == "resume":
            # --- RESUME: Reset a completed task and respawn ---
            resolved = await self._resolve_or_error(
                target_task_id,
                parent_thread_id,
                tool_call_id,
                action_name="resume",
            )
            if isinstance(resolved, ToolMessage):
                return resolved
            task = resolved

            # The agent just looked at this task — bump last_checked_at.
            task.last_checked_at = time.time()

            if task.cancelled:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} was cancelled and cannot be resumed.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            # Validate subagent_type if explicitly provided
            if subagent_type and subagent_type != task.subagent_type:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} is a '{task.subagent_type}' agent, not '{subagent_type}'.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            locally_live = (
                task.asyncio_task is not None and not task.asyncio_task.done()
            )
            if locally_live:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} is still running. Use action='update' to send instructions to a running task.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )
            # Claim the resume before the first await: two parallel resume
            # calls in one model step would otherwise both observe not-live
            # and double-spawn writers for one namespace (the session fence
            # is idempotent for this run and admits both).
            if task.task_id in self._resume_claims:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} is already being resumed. Use action='update' to send instructions to it.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )
            self._resume_claims.add(task.task_id)
            try:
                if self.namespace_owner is not None:
                    # Not live in THIS process: the namespace lock is the
                    # cluster-wide arbiter. Contended = a writer on another
                    # worker (or a prior run's tail) still owns it; free =
                    # safe to resume even if a stale meta/hydration said
                    # "running".
                    if not await self._acquire_task_ns(task.task_id):
                        return ToolMessage(
                            content=(
                                f"Error: Task-{target_task_id} is still running "
                                f"(its writer is live elsewhere). Use "
                                f"action='update' to send instructions to it."
                            ),
                            tool_call_id=tool_call_id,
                            name="Task",
                        )
                elif task.is_pending:
                    # No fence available (single-writer deployment): a pending
                    # task here means a registered-but-unstarted local writer.
                    return ToolMessage(
                        content=f"Error: Task-{target_task_id} is still running. Use action='update' to send instructions to a running task.",
                        tool_call_id=tool_call_id,
                        name="Task",
                    )

                logger.info(
                    "Resuming completed task in background",
                    task_id=target_task_id,
                    display_id=task.display_id,
                    checkpoint_ns=task.task_id,
                )

                # A resume is a NEW run: bear its ledger row under the fence
                # BEFORE the v1 reset destroys the prior round's streams — a
                # rejected resume must leave them intact.
                resume_run_id = current_run_id or self.registry.current_run_id
                admitted = await self._admit_task_run(
                    task,
                    cause="resume",
                    # Args aren't backfilled yet at admission time; a model
                    # that omitted description would otherwise ledger the
                    # schema default instead of the task's real one.
                    description=args.get("description") or task.description or "",
                    launch_tool_call_id=tool_call_id,
                    parent_run_id=resume_run_id,
                )
                if isinstance(admitted, ToolMessage):
                    return admitted  # fence already released by _admit_task_run
                task.task_run_id = admitted or None

                await self._reset_task_for_resume(task)

                # This run owns the writer now: rebind run ownership so its
                # drain/stop teardown and collector account for the resumed
                # writer (the original spawner may be another, long-finalized
                # run — under whose id nothing would ever await it). Stamp
                # unconditionally: None is safe (collectors treat it as
                # claimable-by-any-run), whereas keeping the stale spawner id
                # detaches the writer from every run's teardown.
                task.spawned_run_id = resume_run_id

                # Clear stale namespace mappings so new ones can be registered
                self.registry.clear_namespaces_for_task(task.tool_call_id)

                # Allow re-emission of subagent_identity event
                if self.event_capture_middleware:
                    self.event_capture_middleware.clear_identity(task.tool_call_id)

                # Set ContextVars for the resumed task
                current_background_tool_call_id.set(task.tool_call_id)
                current_background_agent_id.set(task.agent_id)

                # Backfill args the model may omit on resume (the task already
                # carries them): the downstream Task tool schema requires
                # `description`, so a missing one would fail validation inside
                # the spawned writer — an instant no-op resume that still
                # reports success.
                arg_fills = {}
                if subagent_type is None:
                    arg_fills["subagent_type"] = task.subagent_type
                if not args.get("description"):
                    arg_fills["description"] = task.description or prompt[:200]
                    description = arg_fills["description"]
                if arg_fills:
                    args = {**args, **arg_fills}
                    tool_call = {**tool_call, "args": args}
                    request = request.override(tool_call=tool_call)

                # Create a dedicated token tracker for the resumed subagent
                subagent_token_tracker = PerCallTokenTracker()

                # Spawn resumed task in background. create_task_with_context
                # propagates the current OTel context (via contextvars
                # snapshot) so spans emitted inside the subagent inherit the
                # launching chat.turn trace.
                try:
                    emit_subagent_launch(
                        task.subagent_type, action="resume", description_len=len(description),
                    )
                    # Meta before spawn: a fast writer's terminal meta (written at
                    # settle) must never be overwritten by a late "running".
                    await self.registry.write_task_meta(
                        task, "running", fenced=self.namespace_owner is not None
                    )
                    await self._append_run_opener(task, prompt)
                    asyncio_task = create_task_with_context(
                        _run_background_task(
                            task, handler, request, subagent_token_tracker,
                            "Resumed background subagent",
                            registry=self.registry,
                            namespace_owner=self.namespace_owner,
                        ),
                        name=f"background_subagent_resume_{task.display_id}",
                    )
                except Exception as e:
                    # An admitted run either spawns or terminates — never a
                    # stranded in_progress row. Re-settle the entry so the
                    # task stays resumable (its durable result survives in
                    # the task:{id} checkpoint).
                    await self._abort_admitted_run(task, e)
                    task.completed = True
                    task.error = f"resume setup failed before spawn: {e}"
                    return ToolMessage(
                        content=(
                            f"Error: could not resume {task.display_id} — setup "
                            f"failed before the subagent spawned. Try again."
                        ),
                        tool_call_id=tool_call_id,
                        name="Task",
                    )
                task.asyncio_task = asyncio_task
                asyncio_task.add_done_callback(_make_task_done_callback(task))
            finally:
                # Safe to drop once the writer is spawned (locally_live now
                # gates), and must drop on refusal/error so a later resume
                # can retry.
                self._resume_claims.discard(task.task_id)

            short_description = _truncate_description(description, max_sentences=2)
            pseudo_result = (
                f"Resumed **{task.display_id}** in background with new instructions.\n"
                f"- Type: {task.subagent_type}\n"
                f"- New task: {short_description}\n"
                f"- Status: Running (resumed with full previous context)\n\n"
                f"You can:\n"
                f"- Continue with other work\n"
                f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result\n'
                f'- Use `TaskOutput(task_id="{task.task_id}", timeout=60)` to wait until complete'
            )

            return ToolMessage(
                content=pseudo_result,
                tool_call_id=tool_call_id,
                name="Task",
                additional_kwargs={
                    "task_artifact": {
                        "task_id": task.task_id,
                        "task_run_id": task.task_run_id,
                        "action": "resume",
                        "description": description,
                        "prompt": prompt,
                        "type": task.subagent_type,
                    }
                },
            )

        else:
            # --- INIT (default): New task ---
            if subagent_type is None:
                subagent_type = "general-purpose"

            # Register the task first to get the task_id. A checkpoint
            # re-execution of an already-spawned call raises TaskWriterLive
            # (atomically, under the registry lock): re-registering would
            # displace the live writer's routing identity (events would
            # attribute to an inert replacement without its task_run_id/v2
            # stream) and ledger admission would reject the duplicate anyway
            # — answer idempotently with the live task instead.
            try:
                task = await self.registry.register(
                    tool_call_id=tool_call_id,
                    description=description,
                    prompt=prompt,
                    subagent_type=subagent_type,
                    asyncio_task=None,  # Will be set after task creation
                    run_id=current_run_id,
                )
            except TaskWriterLive as exc:
                existing = exc.task
                logger.info(
                    "Task tool call re-executed while its writer is alive; "
                    "returning the existing task",
                    tool_call_id=tool_call_id,
                    task_id=existing.task_id,
                )
                return ToolMessage(
                    content=(
                        f"Background subagent already running: "
                        f"**{existing.display_id}**\n"
                        f"- Type: {existing.subagent_type}\n"
                        f"- Status: Running in background\n\n"
                        f'Use `TaskOutput(task_id="{existing.task_id}")` to get '
                        f"progress or result"
                    ),
                    tool_call_id=tool_call_id,
                    name="Task",
                    additional_kwargs={
                        "task_artifact": {
                            "task_id": existing.task_id,
                            "task_run_id": existing.task_run_id,
                            "action": "init",
                            "description": existing.description,
                            "prompt": existing.prompt,
                            "type": existing.subagent_type,
                        }
                    },
                )
            logger.info(
                "Intercepting task tool call for background execution",
                tool_call_id=tool_call_id,
                task_id=task.task_id,
                display_id=task.display_id,
                subagent_type=subagent_type,
                description=description[:100],
            )

            if self.namespace_owner is not None and not await self._acquire_task_ns(
                task.task_id
            ):
                # A fresh task_id is practically uncontended, so this is the
                # fence itself being unusable (guard session dead) — refuse
                # rather than spawn an unfenced writer, and leave the entry
                # inert so no collector claims it.
                task.completed = True
                task.cancelled = True
                task.result_seen = True
                task.error = "namespace fence unavailable"
                return ToolMessage(
                    content=(
                        f"Error: could not start {task.display_id} — its "
                        f"checkpoint namespace could not be fenced. Try again."
                    ),
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            # Ledger row born under N(thread, task:id), before any spawn side
            # effect — admission-authoritative from day one.
            admitted = await self._admit_task_run(
                task,
                cause="init",
                description=description,
                launch_tool_call_id=tool_call_id,
                parent_run_id=task.spawned_run_id,
            )
            if isinstance(admitted, ToolMessage):
                task.completed = True
                task.cancelled = True
                task.result_seen = True
                task.error = "run admission rejected"
                return admitted
            task.task_run_id = admitted or None

            try:
                current_background_tool_call_id.set(tool_call_id)
                current_background_agent_id.set(task.agent_id)

                # Create a dedicated token tracker for this subagent
                subagent_token_tracker = PerCallTokenTracker()

                # Spawn background task. create_task_with_context propagates the
                # current OTel context (via contextvars snapshot) so spans emitted
                # inside the subagent inherit the launching chat.turn trace.
                emit_subagent_launch(
                    subagent_type, action="init", description_len=len(description),
                )
                # Meta before spawn: a fast writer's terminal meta (written at
                # settle) must never be overwritten by a late "running".
                await self.registry.write_task_meta(
                    task, "running", fenced=self.namespace_owner is not None
                )
                await self._append_run_opener(task, prompt)
                asyncio_task = create_task_with_context(
                    _run_background_task(
                        task, handler, request, subagent_token_tracker,
                        "Background subagent",
                        registry=self.registry,
                        namespace_owner=self.namespace_owner,
                    ),
                    name=f"background_subagent_{task.display_id}",
                )
            except Exception as e:
                # An admitted run either spawns or terminates — never a
                # stranded in_progress row.
                await self._abort_admitted_run(task, e)
                task.completed = True
                task.cancelled = True
                task.result_seen = True
                task.error = f"setup failed before spawn: {e}"
                return ToolMessage(
                    content=(
                        f"Error: could not start {task.display_id} — setup "
                        f"failed before the subagent spawned. Try again."
                    ),
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            # Update the task with the asyncio task reference
            task.asyncio_task = asyncio_task
            asyncio_task.add_done_callback(_make_task_done_callback(task))

            # Return immediate pseudo-result with Task-N format
            short_description = _truncate_description(description, max_sentences=2)
            pseudo_result = (
                f"Background subagent deployed: **{task.display_id}**\n"
                f"- Type: {subagent_type}\n"
                f"- Task: {short_description}\n"
                f"- Status: Running in background\n\n"
                f"You can:\n"
                f"- Continue with other work\n"
                f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result\n'
                f'- Use `TaskOutput(task_id="{task.task_id}", timeout=60)` to wait until complete\n'
                f"- Use `TaskOutput(timeout=60)` to wait for all background tasks"
            )

            return ToolMessage(
                content=pseudo_result,
                tool_call_id=tool_call_id,
                name="Task",
                additional_kwargs={
                    "task_artifact": {
                        "task_id": task.task_id,
                        "task_run_id": task.task_run_id,
                        "action": "init",
                        "description": description,
                        "prompt": prompt,
                        "type": subagent_type,
                    }
                },
            )

    def clear_registry(self) -> None:
        """Clear the task registry; called by the orchestrator after all tasks are handled."""
        self.registry.clear()
        logger.debug("Cleared background task registry")

    async def cancel_all_tasks(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks; returns the number cancelled."""
        return await self.registry.cancel_all(force=force)

    @property
    def pending_task_count(self) -> int:
        """Get the number of pending background tasks."""
        return self.registry.pending_count
