"""Background subagent execution middleware.

This middleware intercepts 'Task' tool calls and spawns them in the background,
allowing the main agent to continue working without blocking.
"""

import asyncio
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
)
from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    steering_queue_key,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    create_task_output_tool,
)
from ptc_agent.agent.middleware.background_subagent import (
    run_executor,
    task_actions,
)


if TYPE_CHECKING:
    pass


# Re-exported for compatibility: every consumer historically imported these
# ContextVars from this module, and identity must be preserved.
from ptc_agent.agent.middleware.background_subagent.context import (  # noqa: E402, F401
    current_background_agent_id,
    current_background_token_tracker,
    current_background_tool_call_id,
)

logger = structlog.get_logger(__name__)


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
                    from ptc_agent.agent.middleware.background_subagent.redis_stream import (
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
        """Post-INSERT setup failure: settle the just-born row as error
        through the owner terminal pipeline — an admitted run either spawns
        or terminates, it never strands in_progress. Going through
        ``_settle_terminal_run`` (not a bare ledger finalize) also writes the
        terminal meta and stamps stream retention, so the aborted run's spill
        keys expire instead of leaking without a TTL."""
        await run_executor._settle_terminal_run(
            task,
            ledger_status="error",
            ledger_failure={
                "error": f"setup failed before spawn: {exc}",
                "error_type": type(exc).__name__,
            },
            registry=self.registry,
            namespace_owner=self.namespace_owner,
        )

    async def _finalize_cancelled_before_spawn(self, task: BackgroundTask) -> None:
        """A stop won the publish race: the admitted run never spawned (or
        its writer was cancelled before its first tick). Settle through the
        same owner terminal pipeline as a real writer — an abbreviated
        finalize would skip the steering sweep (acknowledged input lost
        before run_end), append run_end onto a torn stream, and leave the
        opener's stream without its sentinel/retention stamp."""
        await run_executor._settle_terminal_run(
            task,
            ledger_status="cancelled",
            ledger_failure=None,
            registry=self.registry,
            namespace_owner=self.namespace_owner,
        )

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
        # Unseal: append_captured_event drops appends while ``cancelled`` is
        # set (killed streams are final) — the resumed round is a fresh
        # writer and must not inherit the seal.
        task.cancelled = False
        # Drop the prior round's settled handles: until the publish fence
        # installs the new writer this is a STARTING task, and a stale done
        # handle would make the cancel paths misread it as a finished writer
        # awaiting its done-callback (unstampable) instead.
        task.asyncio_task = None
        task.handler_task = None
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
                    from .redis_stream import (
                        legacy_task_events_key,
                        task_meta_key,
                        task_stream_key,
                    )

                    async with task.redis_spill_lock:
                        await cache.delete(
                            task_meta_key(self.registry.thread_id, task.task_id)
                        )
                        await cache.delete(
                            task_stream_key(self.registry.thread_id, task.task_id)
                        )
                        # One-release backward-compat sweep for the legacy List
                        # key written by pre-cutover workers. Safe to drop once
                        # no worker on the old code path is in rotation.
                        await cache.delete(
                            legacy_task_events_key(
                                self.registry.thread_id, task.task_id
                            )
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
            from ptc_agent.agent.middleware.background_subagent.redis_stream import (
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
        """Intercept Task tool calls and route them by ``action``
        (init/update/resume — see ``task_actions.handle_task_action``).
        All other tools pass through to the handler."""
        if not self.enabled or request.tool_call.get("name", "") != "Task":
            return await handler(request)
        return await task_actions.handle_task_action(self, request, handler)

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
