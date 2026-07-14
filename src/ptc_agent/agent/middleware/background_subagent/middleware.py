"""Background subagent execution middleware.

This middleware intercepts 'Task' tool calls and spawns them in the background,
allowing the main agent to continue working without blocking.
"""

import asyncio
import contextvars
import json
import time
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
    try:
        result = await asyncio.shield(handler_task)
        _merge_subagent_usage(task, tracker, tool_tracker)
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
            _merge_subagent_usage(task, tracker, tool_tracker)
            return {"success": True, "result": result}
        except Exception as e:
            _merge_subagent_usage(task, tracker, tool_tracker)
            logger.error(
                "%s failed after cancellation",
                label,
                display_id=task.display_id,
                error=str(e),
            )
            return {"success": False, "error": str(e), "error_type": type(e).__name__}
    except Exception as e:
        _merge_subagent_usage(task, tracker, tool_tracker)
        logger.error(
            "%s failed",
            label,
            display_id=task.display_id,
            error=str(e),
        )
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
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
            if registry is not None:
                try:
                    await registry.write_task_meta(
                        task, "cancelled" if task.cancelled else "completed"
                    )
                except Exception:
                    logger.warning(
                        "terminal task-meta write failed",
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

    async def _queue_followup_to_redis(self, task_id: str, description: str) -> bool:
        """Push a follow-up message to Redis for a running subagent. Returns True on success."""
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not cache.enabled or not cache.client:
                return False

            key = f"subagent:steering:{task_id}"
            payload = json.dumps(description)
            await cache.client.rpush(key, payload)
            # 1 hour TTL — if not consumed, it's stale
            await cache.client.expire(key, 3600)
            return True
        except Exception as e:
            logger.error(
                "Failed to queue follow-up to Redis", task_id=task_id, error=str(e)
            )
            return False

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

    async def _reset_task_for_resume(self, task: BackgroundTask) -> None:
        """Reset a completed task's state so it can be re-run.

        Clears the Redis event keys first so the resumed run starts fresh.
        Without this the resumed run's events would interleave with the prior
        run's (the seq counter resets to 0, causing seq collisions on replay).
        """
        if self.registry.thread_id:
            try:
                from src.utils.cache.redis_cache import get_cache_client

                cache = get_cache_client()
                if getattr(cache, "enabled", False):
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
        task.collector_response_id = None
        task.sse_drain_complete = asyncio.Event()
        task.sse_consumer_count = 0
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

            # Reconstruct BackgroundTask and insert into registry
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
        # on the same thread share the per-thread registry).
        current_run_id = (
            request.runtime.config.get("run_id")
            if request.runtime
            else None
        )

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

            success = await self._queue_followup_to_redis(
                task.tool_call_id, prompt
            )
            if success:
                task.last_updated_at = time.time()
                logger.info(
                    "Queued follow-up for running task",
                    task_id=target_task_id,
                    display_id=task.display_id,
                )
                return ToolMessage(
                    content=f"Follow-up sent to **{task.display_id}**. The subagent will receive your instructions before its next reasoning step.",
                    tool_call_id=tool_call_id,
                    name="Task",
                    additional_kwargs={
                        "task_artifact": {
                            "task_id": task.task_id,
                            "action": "update",
                            "description": description,
                            "prompt": prompt,
                        }
                    },
                )
            else:
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

                await self._reset_task_for_resume(task)

                # This run owns the writer now: rebind run ownership so its
                # drain/stop teardown and collector account for the resumed
                # writer (the original spawner may be another, long-finalized
                # run — under whose id nothing would ever await it).
                if current_run_id:
                    task.spawned_run_id = current_run_id

                # Clear stale namespace mappings so new ones can be registered
                self.registry.clear_namespaces_for_task(task.tool_call_id)

                # Allow re-emission of subagent_identity event
                if self.event_capture_middleware:
                    self.event_capture_middleware.clear_identity(task.tool_call_id)

                # Set ContextVars for the resumed task
                current_background_tool_call_id.set(task.tool_call_id)
                current_background_agent_id.set(task.agent_id)

                # Update args with inferred subagent_type for the handler
                if subagent_type is None:
                    args = {**args, "subagent_type": task.subagent_type}
                    tool_call = {**tool_call, "args": args}
                    request = request.override(tool_call=tool_call)

                # Create a dedicated token tracker for the resumed subagent
                subagent_token_tracker = PerCallTokenTracker()

                # Spawn resumed task in background. create_task_with_context
                # propagates the current OTel context (via contextvars
                # snapshot) so spans emitted inside the subagent inherit the
                # launching chat.turn trace.
                emit_subagent_launch(
                    task.subagent_type, action="resume", description_len=len(description),
                )
                # Meta before spawn: a fast writer's terminal meta (written at
                # settle) must never be overwritten by a late "running".
                await self.registry.write_task_meta(task, "running")
                asyncio_task = create_task_with_context(
                    _run_background_task(
                        task, handler, request, subagent_token_tracker,
                        "Resumed background subagent",
                        registry=self.registry,
                        namespace_owner=self.namespace_owner,
                    ),
                    name=f"background_subagent_resume_{task.display_id}",
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

            # Register the task first to get the task_id
            task = await self.registry.register(
                tool_call_id=tool_call_id,
                description=description,
                prompt=prompt,
                subagent_type=subagent_type,
                asyncio_task=None,  # Will be set after task creation
                run_id=current_run_id,
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
            await self.registry.write_task_meta(task, "running")
            asyncio_task = create_task_with_context(
                _run_background_task(
                    task, handler, request, subagent_token_tracker,
                    "Background subagent",
                    registry=self.registry,
                    namespace_owner=self.namespace_owner,
                ),
                name=f"background_subagent_{task.display_id}",
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
