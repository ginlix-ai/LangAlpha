"""Background subagent execution middleware.

This middleware intercepts 'Task' tool calls and spawns them in the background,
allowing the main agent to continue working without blocking.
"""

import asyncio
import contextvars
import json
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

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.counter import ToolCallCounterMiddleware

# This ContextVar propagates tool_call_id to subagent tool calls, used by
# ToolCallCounterMiddleware to track which background task a tool call
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


def _truncate_description(description: str, max_sentences: int = 2) -> str:
    """Truncate description to first N sentences.

    Args:
        description: Full task description
        max_sentences: Maximum number of sentences to keep

    Returns:
        Truncated description ending at the Nth period
    """
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


async def _run_background_task(
    task: BackgroundTask,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    request: ToolCallRequest,
    tracker: "PerCallTokenTracker",
    label: str,
) -> dict[str, Any]:
    """Execute a subagent handler in a background asyncio.Task.

    Shared by both the new-spawn and resume paths.
    """
    async def run_handler() -> ToolMessage | Command:
        current_background_token_tracker.set(tracker)
        return await handler(request)

    handler_task: asyncio.Task[ToolMessage | Command] = asyncio.create_task(
        run_handler()
    )
    task.handler_task = handler_task
    try:
        result = await asyncio.shield(handler_task)
        task.per_call_records = tracker.per_call_records or []
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
            task.per_call_records = tracker.per_call_records or []
            return {"success": True, "result": result}
        except Exception as e:
            task.per_call_records = tracker.per_call_records or []
            logger.error(
                "%s failed after cancellation",
                label,
                display_id=task.display_id,
                error=str(e),
            )
            return {"success": False, "error": str(e), "error_type": type(e).__name__}
    except Exception as e:
        task.per_call_records = tracker.per_call_records or []
        logger.error(
            "%s failed",
            label,
            display_id=task.display_id,
            error=str(e),
        )
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


class BackgroundSubagentMiddleware(AgentMiddleware):
    """Middleware that enables background subagent execution.

    This middleware intercepts 'Task' tool calls and:
    1. Spawns the subagent execution in a background asyncio task
    2. Returns an immediate pseudo-result to the main agent
    3. Tracks pending tasks in a registry
    4. Collects results after the agent ends via the waiting room pattern

    The main agent can continue with other work while subagents execute
    in the background. When the agent finishes its current work, the
    BackgroundSubagentOrchestrator will collect pending results and
    re-invoke the agent for synthesis.

    Usage:
        middleware = BackgroundSubagentMiddleware(timeout=60.0)
        agent = create_deep_agent(
            model=...,
            tools=...,
            middleware=[middleware],
        )
        orchestrator = BackgroundSubagentOrchestrator(agent, middleware)
        result = await orchestrator.ainvoke(input_state)
    """

    def __init__(
        self,
        timeout: float = 60.0,
        *,
        enabled: bool = True,
        registry: BackgroundTaskRegistry | None = None,
        counter_middleware: "ToolCallCounterMiddleware | None" = None,
        checkpointer: Any | None = None,
    ) -> None:
        """Initialize the middleware.

        Args:
            timeout: Maximum time to wait for background tasks (seconds)
            enabled: Whether background execution is enabled
            registry: Optional shared registry for background tasks
            counter_middleware: Optional counter middleware for clearing identity on resume
            checkpointer: Optional LangGraph checkpointer for hydrating tasks from stored state
        """
        super().__init__()
        self.registry = registry or BackgroundTaskRegistry()
        self.timeout = timeout
        self.enabled = enabled
        self.counter_middleware = counter_middleware
        self.checkpointer = checkpointer

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
        """Synchronous wrap_tool_call - delegates to blocking execution.

        For sync execution, we can't spawn background tasks, so we
        fall back to normal blocking execution.
        """
        return handler(request)

    async def _queue_followup_to_redis(self, task_id: str, description: str) -> bool:
        """Push a follow-up message to Redis for a running subagent.

        Args:
            task_id: The background task identifier
            description: The follow-up message content

        Returns:
            True if steering message was stored successfully
        """
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

    async def _hydrate_from_checkpoint(
        self, task_id: str, parent_thread_id: str
    ) -> BackgroundTask | None:
        """Try to reconstruct a BackgroundTask from stored checkpoint metadata.

        When the in-memory registry loses a task (e.g., server restart), this method
        queries the checkpointer for a checkpoint written under the task_id's
        checkpoint_ns, and rebuilds a minimal BackgroundTask for resume.

        Args:
            task_id: The 6-char short task ID (used as checkpoint_ns)
            parent_thread_id: The parent conversation's thread_id

        Returns:
            A reconstructed BackgroundTask inserted into the registry, or None
        """

        if not self.checkpointer or not parent_thread_id:
            return None
        try:
            config = {
                "configurable": {
                    "thread_id": parent_thread_id,
                    "checkpoint_ns": f"task:{task_id}",
                }
            }
            checkpoint_tuple = await self.checkpointer.aget_tuple(config)
            if not checkpoint_tuple:
                return None

            metadata = checkpoint_tuple.metadata or {}
            subagent_type = metadata.get("subagent_type", "general-purpose")

            # Reconstruct BackgroundTask and insert into registry
            task = BackgroundTask(
                tool_call_id=f"hydrated-{task_id}",
                task_id=task_id,
                description=metadata.get("description", "Restored subagent"),
                prompt=metadata.get("description", "Restored subagent"),
                subagent_type=subagent_type,
                completed=True,
                result_seen=True,
            )
            async with self.registry._lock:
                self.registry._tasks[task.tool_call_id] = task
                self.registry._task_id_to_tool_call_id[task_id] = task.tool_call_id

            logger.info(
                "Hydrated task from checkpoint",
                task_id=task_id,
                parent_thread_id=parent_thread_id,
                subagent_type=subagent_type,
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

        # --- Action-based routing ---
        if action == "update":
            # --- UPDATE: Instruct a running task via Redis ---
            if not target_task_id:
                return ToolMessage(
                    content="Error: task_id is required for 'update' action.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            task = await self.registry.get_by_task_id(target_task_id)

            # Hydration fallback: reconstruct from checkpoint if registry lost the task
            if task is None:
                task = await self._hydrate_from_checkpoint(
                    target_task_id, parent_thread_id
                )

            if task is None:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} not found.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

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
            if not target_task_id:
                return ToolMessage(
                    content="Error: task_id is required for 'resume' action.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

            task = await self.registry.get_by_task_id(target_task_id)

            # Hydration fallback: reconstruct from checkpoint if registry lost the task
            if task is None:
                task = await self._hydrate_from_checkpoint(
                    target_task_id, parent_thread_id
                )

            if task is None:
                return ToolMessage(
                    content=f"Error: Task-{target_task_id} not found.",
                    tool_call_id=tool_call_id,
                    name="Task",
                )

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

            if task.is_pending:
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

            # Reset task state for the new run
            task.completed = False
            task.result = None
            task.result_seen = False
            task.error = None
            task.captured_events = []
            task.collector_response_id = None  # Allow next collector to claim
            task.sse_drain_complete = asyncio.Event()  # Fresh event for new SSE stream

            # Clear stale namespace mappings so new ones can be registered
            self.registry.clear_namespaces_for_task(task.tool_call_id)

            # Allow re-emission of subagent_identity event
            if self.counter_middleware:
                self.counter_middleware.clear_identity(task.tool_call_id)

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

            # Spawn resumed task in background
            asyncio_task = asyncio.create_task(
                _run_background_task(task, handler, request, subagent_token_tracker, "Resumed background subagent"),
                name=f"background_subagent_resume_{task.display_id}",
            )
            task.asyncio_task = asyncio_task

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
            )
            logger.info(
                "Intercepting task tool call for background execution",
                tool_call_id=tool_call_id,
                task_id=task.task_id,
                display_id=task.display_id,
                subagent_type=subagent_type,
                description=description[:100],
            )

            current_background_tool_call_id.set(tool_call_id)
            current_background_agent_id.set(task.agent_id)

            # Create a dedicated token tracker for this subagent
            subagent_token_tracker = PerCallTokenTracker()

            # Spawn background task
            asyncio_task = asyncio.create_task(
                _run_background_task(task, handler, request, subagent_token_tracker, "Background subagent"),
                name=f"background_subagent_{task.display_id}",
            )

            # Update the task with the asyncio task reference
            task.asyncio_task = asyncio_task

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
        """Clear the task registry.

        Should be called by the orchestrator after handling all tasks.
        """
        self.registry.clear()
        logger.debug("Cleared background task registry")

    async def cancel_all_tasks(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks.

        Args:
            force: Cancel underlying handler tasks as well

        Returns:
            Number of tasks cancelled
        """
        return await self.registry.cancel_all(force=force)

    @property
    def pending_task_count(self) -> int:
        """Get the number of pending background tasks."""
        return self.registry.pending_count
