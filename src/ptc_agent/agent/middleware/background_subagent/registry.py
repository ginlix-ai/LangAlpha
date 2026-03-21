"""Background task registry for tracking async subagent executions.

This module provides a thread-safe registry for managing background tasks
spawned by the BackgroundSubagentMiddleware.
"""

from __future__ import annotations

import asyncio
import secrets
import time
import uuid as uuid_mod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.utils import MessageChecker

logger = structlog.get_logger(__name__)


@dataclass
class BackgroundTask:
    """Represents a background subagent task."""

    tool_call_id: str
    """The LangGraph tool_call_id that triggered this task."""

    task_id: str
    """6-char alphanumeric identifier (e.g., 'k7Xm2p')."""

    description: str
    """Short description/label of the task."""

    prompt: str
    """Detailed instructions for the subagent."""

    subagent_type: str
    """Type of subagent (e.g., 'research', 'general-purpose')."""

    asyncio_task: asyncio.Task | None = None
    """The asyncio.Task object running the background wrapper."""

    handler_task: asyncio.Task | None = None
    """The underlying tool handler task executing the subagent."""

    created_at: float = field(default_factory=time.time)
    """Timestamp when the task was created."""

    result: Any = None
    """Result from the subagent once completed."""

    error: str | None = None
    """Error message if the task failed."""

    completed: bool = False
    """Whether the task has completed."""

    result_seen: bool = False
    """Whether the agent has seen this task's result (via task_output, wait, or notification)."""

    # Tool call tracking
    tool_call_counts: dict[str, int] = field(default_factory=dict)
    """Count of tool calls by tool name."""

    total_tool_calls: int = 0
    """Total number of tool calls made."""

    current_tool: str = ""
    """Name of the tool currently being executed."""

    last_update_time: float = field(default_factory=time.time)
    """Timestamp of last metrics update."""

    agent_id: str = ""
    """Stable unique identity: '{subagent_type}:{uuid4}'."""

    captured_events: list[dict[str, Any]] = field(default_factory=list)
    """SSE-shaped events captured by middleware for post-interrupt persistence.
    Each event: {"event": "message_chunk"|"tool_calls"|"tool_call_result", "data": {...}, "ts": float}
    """

    cancelled: bool = False
    """Whether the task was explicitly cancelled (distinct from completed with error)."""

    spawned_turn_index: int = 0
    """The turn_index of the parent turn that spawned this subagent."""

    per_call_records: list[dict[str, Any]] = field(default_factory=list)
    """Token usage records collected when subagent completes."""

    collector_response_id: str | None = None
    """Response ID of the collector that claimed this task for persistence.
    Set atomically during the _mark_completed filter to prevent two collectors
    from persisting the same subagent events to different response_ids."""

    sse_drain_complete: asyncio.Event = field(default_factory=asyncio.Event)
    """Set by stream_subagent_task_events after its final drain.
    The collector awaits this before clearing captured_events so that
    live SSE consumers are guaranteed to have emitted all events."""

    @property
    def display_id(self) -> str:
        """Return Task-<id> format for display."""
        return f"Task-{self.task_id}"

    @property
    def is_pending(self) -> bool:
        """Check if this task is still pending (not yet completed).

        Returns:
            True if task is still running or waiting to start
        """
        if self.completed:
            return False
        if self.asyncio_task is None:
            return True  # Registered but not yet started
        return not self.asyncio_task.done()


class BackgroundTaskRegistry:
    """Thread-safe registry for tracking background subagent tasks.

    This registry manages the lifecycle of background tasks spawned by
    the BackgroundSubagentMiddleware. It provides methods to register
    new tasks, poll for completion, and collect results.
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._tasks: dict[str, BackgroundTask] = {}
        self._task_id_to_tool_call_id: dict[str, str] = {}  # task_id -> tool_call_id
        self._ns_uuid_to_tool_call_id: dict[
            str, str
        ] = {}  # LangGraph namespace UUID -> tool_call_id
        self._lock = asyncio.Lock()
        self._results: dict[str, Any] = {}
        self.current_turn_index: int = 0

    async def register(
        self,
        tool_call_id: str,
        description: str,
        prompt: str,
        subagent_type: str,
        asyncio_task: asyncio.Task | None = None,
    ) -> BackgroundTask:
        """Register a new background task.

        Args:
            tool_call_id: The LangGraph tool_call_id
            description: Description of the task
            prompt: Detailed instructions for the subagent
            subagent_type: Type of subagent
            asyncio_task: The asyncio.Task running the subagent (can be set later)

        Returns:
            The registered BackgroundTask
        """
        async with self._lock:
            # Generate short alphanumeric task_id
            task_id = secrets.token_urlsafe(4)[:6]

            agent_id = f"{subagent_type}:{uuid_mod.uuid4()}"
            task = BackgroundTask(
                tool_call_id=tool_call_id,
                task_id=task_id,
                description=description,
                prompt=prompt,
                subagent_type=subagent_type,
                asyncio_task=asyncio_task,
                agent_id=agent_id,
                spawned_turn_index=self.current_turn_index,
            )
            self._tasks[tool_call_id] = task
            self._task_id_to_tool_call_id[task_id] = tool_call_id

            logger.info(
                "Registered background task",
                tool_call_id=tool_call_id,
                task_id=task_id,
                display_id=task.display_id,
                subagent_type=subagent_type,
                description=description[:50],
                prompt=prompt[:50],
            )

            return task

    async def get_pending_tasks(self) -> list[BackgroundTask]:
        """Get all tasks that haven't completed yet.

        Returns:
            List of pending BackgroundTask objects
        """
        async with self._lock:
            return [task for task in self._tasks.values() if task.is_pending]

    async def get_all_tasks(self) -> list[BackgroundTask]:
        """Get all registered tasks.

        Returns:
            List of all BackgroundTask objects
        """
        async with self._lock:
            return list(self._tasks.values())

    async def get_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Get a task by its short alphanumeric task_id.

        Args:
            task_id: The 6-char task identifier (e.g., 'k7Xm2p')

        Returns:
            The BackgroundTask or None if not found
        """
        async with self._lock:
            tool_call_id = self._task_id_to_tool_call_id.get(task_id)
            if tool_call_id:
                return self._tasks.get(tool_call_id)
            return None

    async def get_task_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Alias for get_by_task_id, used by the HTTP layer."""
        return await self.get_by_task_id(task_id)

    def get_by_tool_call_id(self, tool_call_id: str) -> BackgroundTask | None:
        """Get a task by its tool_call_id (synchronous).

        This is a synchronous method for use when the lock is not needed
        (e.g., formatting results after wait_for_all has completed).

        Args:
            tool_call_id: The LangGraph tool_call_id

        Returns:
            The BackgroundTask or None if not found
        """
        return self._tasks.get(tool_call_id)

    def register_namespace(self, checkpoint_ns: str, tool_call_id: str) -> None:
        """Register LangGraph namespace UUIDs for a background task.

        Parses checkpoint_ns like "tools:uuid1|model:uuid2" and maps
        each LangGraph task UUID to our tool_call_id for streaming lookup.

        Args:
            checkpoint_ns: The checkpoint namespace string from LangGraph config
            tool_call_id: The background task's tool_call_id
        """
        for element in checkpoint_ns.split("|"):
            parts = element.split(":", 1)
            if len(parts) == 2:
                ns_uuid = parts[1]
                self._ns_uuid_to_tool_call_id[ns_uuid] = tool_call_id

    def get_task_by_namespace(self, ns_element: str) -> BackgroundTask | None:
        """Look up task from a namespace element like 'tools:uuid'.

        Args:
            ns_element: A single namespace element (e.g., "tools:4cd20fdc-...")

        Returns:
            The BackgroundTask or None if not found
        """
        parts = ns_element.split(":", 1)
        if len(parts) == 2:
            ns_uuid = parts[1]
            tool_call_id = self._ns_uuid_to_tool_call_id.get(ns_uuid)
            if tool_call_id:
                return self._tasks.get(tool_call_id)
        return None

    def clear_namespaces_for_task(self, tool_call_id: str) -> None:
        """Remove stale namespace UUID→tool_call_id mappings for a task.

        Called before resuming a completed task so that new namespace UUIDs
        from the resumed invocation can be registered fresh.

        Args:
            tool_call_id: The tool_call_id to clear mappings for
        """
        stale_keys = [
            ns
            for ns, tid in self._ns_uuid_to_tool_call_id.items()
            if tid == tool_call_id
        ]
        for key in stale_keys:
            del self._ns_uuid_to_tool_call_id[key]
        if stale_keys:
            logger.debug(
                "Cleared stale namespace mappings for task",
                tool_call_id=tool_call_id,
                cleared_count=len(stale_keys),
            )

    async def append_captured_event(
        self, tool_call_id: str, event: dict[str, Any]
    ) -> None:
        """Append a captured SSE event to a background task.

        Called by ToolCallCounterMiddleware to capture events for
        post-interrupt persistence.

        Args:
            tool_call_id: The task's tool_call_id
            event: SSE-shaped event dict
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if task:
                task.captured_events.append(event)

    async def update_metrics(self, tool_call_id: str, tool_name: str) -> None:
        """Update tool call metrics for a task.

        Called by ToolCallCounterMiddleware when a subagent makes a tool call.

        Args:
            tool_call_id: The task's tool_call_id
            tool_name: Name of the tool being called
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if task:
                task.tool_call_counts[tool_name] = (
                    task.tool_call_counts.get(tool_name, 0) + 1
                )
                task.total_tool_calls += 1
                task.current_tool = tool_name
                task.last_update_time = time.time()
                logger.debug(
                    "Updated task metrics",
                    tool_call_id=tool_call_id,
                    display_id=task.display_id,
                    tool_name=tool_name,
                    total_calls=task.total_tool_calls,
                )

    async def wait_for_specific(
        self,
        task_id: str,
        timeout: float = 60.0,
        *,
        message_checker: MessageChecker | None = None,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:
        """Wait for a specific task to complete by its task_id.

        Args:
            task_id: The 6-char task identifier (e.g., 'k7Xm2p')
            timeout: Maximum time to wait in seconds
            message_checker: Optional async callable that returns True when a
                a user steering message is pending (used to interrupt the wait early).
            poll_interval: Seconds between message-checker polls (ignored when
                *message_checker* is None — falls back to a single wait).

        Returns:
            Dict with task result or error
        """
        tool_call_id = self._task_id_to_tool_call_id.get(task_id)
        if not tool_call_id:
            return {"success": False, "error": f"Task-{task_id} not found"}

        task = self._tasks.get(tool_call_id)
        if not task:
            return {"success": False, "error": f"Task-{task_id} not found"}

        if task.completed:
            return task.result or {"success": True, "result": None}

        if task.asyncio_task is None:
            return {
                "success": False,
                "error": f"Task-{task_id} has no asyncio task",
            }

        logger.info(
            "Waiting for specific task",
            task_id=task_id,
            display_id=task.display_id,
            timeout=timeout,
        )

        # --- polling loop (or single wait when no checker) ---------------
        start = time.monotonic()

        if message_checker is None:
            # Original single-wait behaviour
            await asyncio.wait(
                [task.asyncio_task],
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED,
            )
        else:
            while True:
                remaining = timeout - (time.monotonic() - start)
                if remaining <= 0:
                    break

                await asyncio.wait(
                    [task.asyncio_task],
                    timeout=min(poll_interval, remaining),
                    return_when=asyncio.ALL_COMPLETED,
                )

                if task.asyncio_task.done():
                    break

                # Check for pending user steering
                try:
                    if await message_checker():
                        logger.info(
                            "Wait interrupted by user steering",
                            task_id=task_id,
                            display_id=task.display_id,
                            elapsed=f"{time.monotonic() - start:.1f}s",
                        )
                        return {
                            "success": False,
                            "status": "interrupted",
                            "reason": "user_steering",
                        }
                except Exception:
                    # Redis glitch — continue waiting normally
                    pass

        # --- collect result ----------------------------------------------
        async with self._lock:
            if task.asyncio_task.done():
                task.completed = True
                try:
                    result = task.asyncio_task.result()
                    task.result = result
                    self._results[tool_call_id] = result
                    logger.info(
                        "Specific task completed",
                        task_id=task_id,
                        display_id=task.display_id,
                    )
                    return result
                except Exception as e:
                    task.error = str(e)
                    error_result = {"success": False, "error": str(e)}
                    self._results[tool_call_id] = error_result
                    return error_result
            else:
                return {
                    "success": False,
                    "error": f"Wait timed out after {timeout}s - task may still be running",
                    "status": "timeout",
                }

    async def wait_for_all(
        self,
        timeout: float = 60.0,
        *,
        message_checker: MessageChecker | None = None,
        poll_interval: float = 2.0,
    ) -> dict[str, Any]:
        """Wait for all background tasks to complete.

        Args:
            timeout: Maximum time to wait in seconds
            message_checker: Optional async callable that returns True when a
                a user steering message is pending (used to interrupt the wait early).
            poll_interval: Seconds between message-checker polls (ignored when
                *message_checker* is None — falls back to a single wait).

        Returns:
            Dict mapping tool_call_id to result (success dict or error dict).
            When interrupted, still-running tasks get ``status="interrupted"``.
        """
        async with self._lock:
            tasks_to_wait = {
                tool_call_id: task.asyncio_task
                for tool_call_id, task in self._tasks.items()
                if not task.completed and task.asyncio_task is not None
            }

        if not tasks_to_wait:
            logger.debug("No background tasks to wait for")
            return self._results.copy()

        logger.info(
            "Waiting for background tasks",
            task_count=len(tasks_to_wait),
            timeout=timeout,
        )

        interrupted = False
        start = time.monotonic()

        if message_checker is None:
            await asyncio.wait(
                tasks_to_wait.values(),
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED,
            )
        else:
            remaining_tasks = set(tasks_to_wait.values())
            while remaining_tasks:
                remaining = timeout - (time.monotonic() - start)
                if remaining <= 0:
                    break

                done, remaining_tasks = await asyncio.wait(
                    remaining_tasks,
                    timeout=min(poll_interval, remaining),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not remaining_tasks:
                    break  # all done

                try:
                    if await message_checker():
                        logger.info(
                            "wait_for_all interrupted by user steering",
                            elapsed=f"{time.monotonic() - start:.1f}s",
                            pending=len(remaining_tasks),
                        )
                        interrupted = True
                        break
                except Exception:
                    pass

        # Collect results
        results = {}
        async with self._lock:
            for tool_call_id, asyncio_task in tasks_to_wait.items():
                task = self._tasks.get(tool_call_id)
                if task is None:
                    continue

                if asyncio_task.done():
                    task.completed = True
                    try:
                        result = asyncio_task.result()
                        task.result = result
                        results[tool_call_id] = result
                        logger.info(
                            "Background task completed",
                            tool_call_id=tool_call_id,
                            success=result.get("success", False)
                            if isinstance(result, dict)
                            else True,
                        )
                    except Exception as e:
                        task.error = str(e)
                        results[tool_call_id] = {"success": False, "error": str(e)}
                        logger.error(
                            "Background task failed",
                            tool_call_id=tool_call_id,
                            error=str(e),
                        )
                elif interrupted:
                    results[tool_call_id] = {
                        "success": False,
                        "status": "interrupted",
                        "reason": "user_steering",
                    }
                else:
                    # Task didn't complete within timeout
                    results[tool_call_id] = {
                        "success": False,
                        "error": f"Wait timed out after {timeout}s - task may still be running",
                        "status": "timeout",
                    }
                    logger.warning(
                        "Wait timed out for background task",
                        tool_call_id=tool_call_id,
                        timeout=timeout,
                    )

            self._results.update(results)

        return results

    async def cancel_all(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks.

        Args:
            force: Cancel underlying handler tasks as well

        Returns:
            Number of tasks cancelled
        """
        cancelled = 0
        async with self._lock:
            for task in self._tasks.values():
                if task.asyncio_task is None:
                    continue
                if not task.completed and not task.asyncio_task.done():
                    if force and task.handler_task and not task.handler_task.done():
                        task.handler_task.cancel()
                    task.asyncio_task.cancel()
                    task.completed = True
                    task.cancelled = True
                    task.error = "Cancelled"
                    task.result = {
                        "success": False,
                        "error": "Cancelled",
                        "status": "cancelled",
                    }
                    cancelled += 1

        if cancelled > 0:
            logger.info("Cancelled background tasks", count=cancelled, force=force)

        return cancelled

    def clear(self) -> None:
        """Clear all tasks and results from the registry.

        Note: This does NOT cancel running tasks. Call cancel_all() first
        if you want to stop running tasks.

        This method is intentionally synchronous and does not acquire the async lock
        because it is called by the orchestrator after wait_for_all() completes,
        when no concurrent modifications are possible.
        """
        self._tasks.clear()
        self._task_id_to_tool_call_id.clear()
        self._ns_uuid_to_tool_call_id.clear()
        self._results.clear()
        logger.debug("Cleared background task registry")

    def has_pending_tasks(self) -> bool:
        """Check if there are any pending tasks (sync version).

        Returns:
            True if there are pending tasks
        """
        return any(task.is_pending for task in self._tasks.values())

    @property
    def task_count(self) -> int:
        """Get the number of registered tasks."""
        return len(self._tasks)

    @property
    def pending_count(self) -> int:
        """Get the number of pending tasks."""
        return sum(1 for task in self._tasks.values() if task.is_pending)
