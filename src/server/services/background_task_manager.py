"""
Background Task Manager

Manages workflow execution as background tasks that continue running
independently of SSE client connections.

Key Features:
- Decouples workflow execution from HTTP connections
- Uses asyncio.shield() to protect tasks from client disconnect cancellation
- Stores intermediate results during execution for reconnection support
- Automatic cleanup of abandoned workflows
- Thread-safe task registry with async locks
- Supports concurrent workflow executions

Architecture:
- Background tasks run independently and persist in task registry
- SSE connections become "viewers" that attach/detach from running tasks
- Results are buffered in-memory during execution
- Cleanup task runs periodically to remove stale workflows

Usage:
    manager = BackgroundTaskManager.get_instance()

    # Start a workflow in background
    task_info = await manager.start_workflow(
        thread_id="uuid",
        workflow_coro=graph.astream(input, config)
    )

    # Attach SSE connection to consume results
    async for event in manager.stream_results(thread_id):
        yield event

    # Later: reconnect to same workflow
    async for event in manager.stream_results(thread_id, from_beginning=True):
        yield event
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, Coroutine
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
from contextlib import suppress

from src.config.settings import (
    get_max_concurrent_workflows,
    get_workflow_result_ttl,
    get_abandoned_workflow_timeout,
    get_cleanup_interval,
    is_intermediate_storage_enabled,
    get_max_stored_messages_per_agent,
    get_event_storage_backend,
    is_event_storage_fallback_enabled,
    get_redis_ttl_workflow_events,
)
from src.utils.cache.redis_cache import get_cache_client
from src.server.utils.persistence_utils import (
    get_token_usage_from_callback,
    get_tool_usage_from_handler,
    get_sse_events_from_handler,
    calculate_execution_time,
)

logger = logging.getLogger(__name__)


# ========== Shared Helpers (DRY) ==========


def drain_task_captured_events(task, cursor: int):
    """Yield new captured_events from a single task since cursor position.

    Generator that yields (event_dict, agent_id) tuples for events
    that have accumulated since the given cursor position.

    Handles cursor reset when captured_events is cleared (len < cursor).

    Args:
        task: A BackgroundTask with captured_events list
        cursor: Last-read position in captured_events

    Yields:
        (event_dict, agent_id) tuples for each new event
    """
    events = task.captured_events
    # Reset cursor if captured_events was cleared (e.g. by collector)
    if cursor > 0 and len(events) < cursor:
        cursor = 0
    if len(events) > cursor:
        agent_id = f"task:{task.task_id}"
        for ev in events[cursor:]:
            yield ev, agent_id


class TaskStatus(str, Enum):
    """Background task execution status."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SOFT_INTERRUPTED = "soft_interrupted"


@dataclass
class TaskInfo:
    """Information about a background workflow task."""
    thread_id: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_access_at: datetime = field(default_factory=datetime.now)

    # Task execution
    task: Optional[asyncio.Task] = None
    inner_task: Optional[asyncio.Task] = None  # Reference to consume_workflow task
    error: Optional[str] = None

    # Cancellation control
    explicit_cancel: bool = False  # True if user explicitly cancelled
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)  # Cooperative cancellation signal

    # Soft interrupt control (pause main agent, keep subagents running)
    soft_interrupt_event: asyncio.Event = field(default_factory=asyncio.Event)
    soft_interrupted: bool = False

    # Result storage
    result_buffer: deque = field(default_factory=deque)  # Stores SSE events
    final_result: Optional[Any] = None

    # Connection tracking
    active_connections: int = 0

    # Live event broadcasting for reconnection support
    live_queues: list = field(default_factory=list)  # List[asyncio.Queue]

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Completion callback
    completion_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None

    # LangGraph compiled graph for state queries (stored per-task, not global)
    graph: Optional[Any] = None


class SoftInterruptError(Exception):
    """Internal control-flow exception for user ESC soft-interrupt."""


class BackgroundTaskManager:
    """
    Manages background workflow task execution.

    Singleton service that handles:
    - Task lifecycle (create, execute, complete, cleanup)
    - Result buffering and streaming
    - Connection management
    - Automatic cleanup
    """

    # Singleton instance
    _instance: Optional['BackgroundTaskManager'] = None

    def __init__(self):
        """Initialize background task manager."""
        self.tasks: Dict[str, TaskInfo] = {}
        self.task_lock = asyncio.Lock()

        # Configuration
        self.max_concurrent = get_max_concurrent_workflows()
        self.result_ttl = get_workflow_result_ttl()
        self.abandoned_timeout = get_abandoned_workflow_timeout()
        self.cleanup_interval = get_cleanup_interval()
        self.enable_storage = is_intermediate_storage_enabled()
        self.max_stored_messages = get_max_stored_messages_per_agent()

        # Event storage configuration
        self.event_storage_backend = get_event_storage_backend()
        self.event_storage_fallback = is_event_storage_fallback_enabled()
        self.redis_event_ttl = get_redis_ttl_workflow_events()

        # Cleanup task
        self.cleanup_task: Optional[asyncio.Task] = None

    @classmethod
    def get_instance(cls) -> 'BackgroundTaskManager':
        """
        Get singleton instance of BackgroundTaskManager.

        Returns:
            BackgroundTaskManager instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _get_task_info_locked(self, thread_id: str) -> Optional[TaskInfo]:
        """
        Acquire lock and get task info.

        Helper method to reduce boilerplate of locking + dict lookup pattern.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            TaskInfo or None if not found
        """
        async with self.task_lock:
            return self.tasks.get(thread_id)

    async def start_cleanup_task(self):
        """Start periodic cleanup background task."""
        if self.cleanup_task is None or self.cleanup_task.done():
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info(
                f"BackgroundTaskManager: Cleanup task started "
                f"(max_concurrent={self.max_concurrent}, "
                f"result_ttl={self.result_ttl}s, "
                f"abandoned_timeout={self.abandoned_timeout}s)"
            )

    async def stop_cleanup_task(self):
        """Stop periodic cleanup background task."""
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
            logger.info("[BackgroundTaskManager] Stopped cleanup task")

    async def shutdown(self, timeout: float = 25.0):
        """
        Gracefully shutdown background task manager.

        Cancels all running workflows and waits for them to complete
        before database pools are closed.

        Args:
            timeout: Maximum time to wait for tasks to complete (seconds)
        """
        logger.info("[BackgroundTaskManager] Starting graceful shutdown...")

        # Stop cleanup task first
        await self.stop_cleanup_task()

        # Get list of running workflows
        async with self.task_lock:
            running_tasks = [
                (thread_id, info)
                for thread_id, info in self.tasks.items()
                if info.status in [TaskStatus.RUNNING, TaskStatus.QUEUED]
            ]

        if not running_tasks:
            logger.info("[BackgroundTaskManager] No running workflows to cancel")
            return

        logger.info(
            f"[BackgroundTaskManager] Cancelling {len(running_tasks)} running workflows"
        )

        # Cancel all running workflows
        for thread_id, info in running_tasks:
            await self.cancel_workflow(thread_id)

        # Wait for tasks to complete with timeout
        try:
            async with asyncio.timeout(timeout):
                for thread_id, info in running_tasks:
                    if info.task and not info.task.done():
                        try:
                            await info.task
                        except (asyncio.CancelledError, Exception):
                            pass  # Expected during shutdown
        except asyncio.TimeoutError:
            logger.warning(
                f"[BackgroundTaskManager] Shutdown timeout after {timeout}s, "
                f"forcing cancellation of stuck tasks"
            )

            # Aggressive cancellation: force cancel stuck tasks
            stuck_tasks = []
            for thread_id, info in running_tasks:
                if info.task and not info.task.done():
                    logger.warning(
                        f"[BackgroundTaskManager] Force-cancelling stuck task: {thread_id}"
                    )
                    info.task.cancel()
                    stuck_tasks.append(info.task)

            # Wait briefly for forced cancellations to complete
            if stuck_tasks:
                try:
                    async with asyncio.timeout(5.0):
                        await asyncio.gather(*stuck_tasks, return_exceptions=True)
                    logger.info(
                        f"[BackgroundTaskManager] Force-cancelled {len(stuck_tasks)} stuck tasks"
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"[BackgroundTaskManager] {len(stuck_tasks)} tasks did not respond "
                        f"to force cancellation after 5s"
                    )

        logger.info("[BackgroundTaskManager] Shutdown complete")

    async def _cleanup_loop(self):
        """Periodic cleanup loop for stale tasks."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_abandoned_tasks()
            except asyncio.CancelledError:
                logger.info("[BackgroundTaskManager] Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"[BackgroundTaskManager] Error in cleanup loop: {e}")

    async def _cleanup_abandoned_tasks(self):
        """Clean up abandoned and completed tasks based on TTL."""
        now = datetime.now()
        abandoned_threshold = now - timedelta(seconds=self.abandoned_timeout)
        completed_threshold = now - timedelta(seconds=self.result_ttl)

        to_remove = []

        async with self.task_lock:
            for thread_id, info in self.tasks.items():
                # Remove completed tasks after TTL
                if info.status in [
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                    TaskStatus.SOFT_INTERRUPTED,
                ]:
                    if info.completed_at and info.completed_at < completed_threshold:
                        to_remove.append(thread_id)
                        logger.info(
                            f"[BackgroundTaskManager] Cleanup: removing completed task "
                            f"{thread_id} (age: {now - info.completed_at})"
                        )

                # Remove abandoned running tasks
                elif info.status == TaskStatus.RUNNING:
                    if info.active_connections == 0 and info.last_access_at < abandoned_threshold:
                        to_remove.append(thread_id)
                        logger.warning(
                            f"[BackgroundTaskManager] Cleanup: removing abandoned task "
                            f"{thread_id} (no connections for {now - info.last_access_at})"
                        )
                        # Cancel the task
                        if info.task and not info.task.done():
                            info.task.cancel()

            # Remove from registry
            for thread_id in to_remove:
                del self.tasks[thread_id]

        if to_remove:
            logger.info(
                f"[BackgroundTaskManager] Cleaned up {len(to_remove)} tasks: {to_remove}"
            )

    async def start_workflow(
        self,
        thread_id: str,
        workflow_generator: Any,
        metadata: Optional[Dict[str, Any]] = None,
        completion_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        graph: Optional[Any] = None,
    ) -> TaskInfo:
        """
        Start a workflow as a background task.

        Args:
            thread_id: Workflow thread identifier
            workflow_generator: Async generator from graph.astream()
            metadata: Optional metadata about the workflow
            completion_callback: Optional callback to invoke when workflow completes
            graph: Optional LangGraph compiled graph for state queries during completion/error handling

        Returns:
            TaskInfo object tracking the background task

        Raises:
            ValueError: If max concurrent workflows exceeded
            RuntimeError: If workflow already exists for thread_id
        """
        async with self.task_lock:
            # Check if already exists
            if thread_id in self.tasks:
                existing = self.tasks[thread_id]
                if existing.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                    raise RuntimeError(
                        f"Workflow {thread_id} already running with status {existing.status}"
                    )
                # Also block on SOFT_INTERRUPTED - the workflow just stopped and
                # wait_for_soft_interrupted should be given time to see it
                if existing.status == TaskStatus.SOFT_INTERRUPTED:
                    raise RuntimeError(
                        f"Workflow {thread_id} was just soft-interrupted. "
                        f"Use wait_for_soft_interrupted() before starting a new workflow."
                    )
                # Remove completed task to allow re-run
                logger.info(
                    f"[BackgroundTaskManager] Removing completed task {thread_id} "
                    f"to start new execution"
                )
                del self.tasks[thread_id]

            # Check concurrent limit
            running_count = sum(
                1 for t in self.tasks.values()
                if t.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]
            )
            if running_count >= self.max_concurrent:
                raise ValueError(
                    f"Max concurrent workflows reached ({self.max_concurrent}). "
                    f"Currently running: {running_count}"
                )

            # Create task info
            task_info = TaskInfo(
                thread_id=thread_id,
                status=TaskStatus.QUEUED,
                created_at=datetime.now(),
                metadata=metadata or {},
                completion_callback=completion_callback,
                graph=graph,
            )

            # Start background task
            task_info.task = asyncio.create_task(
                self._run_workflow_shielded(thread_id, workflow_generator)
            )
            task_info.status = TaskStatus.RUNNING
            task_info.started_at = datetime.now()

            # Register task
            self.tasks[thread_id] = task_info

            logger.info(
                f"[BackgroundTaskManager] Started workflow {thread_id} "
                f"(running: {running_count + 1}/{self.max_concurrent})"
            )

            return task_info

    async def _run_workflow_shielded(
        self,
        thread_id: str,
        workflow_generator: Any
    ):
        """
        Run workflow with shield protection and cooperative cancellation.

        Uses asyncio.shield() to protect from accidental disconnects, while
        supporting explicit cancellation via cooperative event signaling.

        Args:
            thread_id: Workflow thread identifier
            workflow_generator: Async generator from graph.astream()
        """
        try:
            # Define the workflow consumer coroutine with cooperative cancellation
            async def consume_workflow(wf_gen):
                """Consume workflow generator with cancellation/soft-interrupt checks."""
                # Get cancellation + soft-interrupt event references
                async with self.task_lock:
                    task_info = self.tasks.get(thread_id)
                    cancel_event = task_info.cancel_event if task_info else None
                    soft_interrupt_event = task_info.soft_interrupt_event if task_info else None

                if not cancel_event:
                    logger.warning(
                        f"[BackgroundTaskManager] No cancel_event found for {thread_id}, "
                        f"running without cancellation support"
                    )
                    async for event in wf_gen:
                        if soft_interrupt_event and soft_interrupt_event.is_set():
                            with suppress(Exception):
                                await wf_gen.aclose()
                            raise SoftInterruptError("Soft-interrupted by user")

                        if self.enable_storage:
                            await self._buffer_event_redis(thread_id, event)
                    return

                async for event in wf_gen:
                    if cancel_event.is_set():
                        with suppress(Exception):
                            await wf_gen.aclose()
                        raise asyncio.CancelledError("Explicitly cancelled by user")

                    if soft_interrupt_event and soft_interrupt_event.is_set():
                        with suppress(Exception):
                            await wf_gen.aclose()
                        raise SoftInterruptError("Soft-interrupted by user")

                    if self.enable_storage:
                        await self._buffer_event_redis(thread_id, event)

            # ----------------------------------------------------------
            # First graph turn
            # ----------------------------------------------------------
            inner_task = asyncio.create_task(consume_workflow(workflow_generator))

            async with self.task_lock:
                task_info = self.tasks.get(thread_id)
                if task_info:
                    task_info.inner_task = inner_task

            # ALWAYS use shield - cancellation handled cooperatively inside task
            await asyncio.shield(inner_task)

            # Main graph finished — mark completed unconditionally.
            # _mark_completed handles both cases:
            # - No subagents: completion_callback persists, done.
            # - Subagents pending: completion_callback persists main turn,
            #   collector spawned to wait for subagents and persist their events.
            await self._mark_completed(thread_id)

        except SoftInterruptError:
            # User pressed ESC: flush whatever state we have so follow-up queries
            # can restore maximum progress.
            await self._flush_checkpoint(thread_id)
            await self._mark_soft_interrupted(thread_id)
            return

        except asyncio.CancelledError:
            await self._mark_cancelled(thread_id)
            raise

        except Exception as e:
            # Workflow failed
            logger.error(
                f"[BackgroundTaskManager] Workflow {thread_id} failed: {e}",
                exc_info=True
            )
            await self._mark_failed(thread_id, str(e))

    async def _flush_checkpoint(self, thread_id: str) -> None:
        """Force a checkpoint write for the current thread state.

        The agent/checkpointer normally writes checkpoints at safe boundaries.
        If the user presses ESC mid-run, this explicit flush makes sure the
        latest available state is persisted so the next request can restore it.
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            graph = task_info.graph if task_info else None

        if not graph:
            return

        config = {"configurable": {"thread_id": thread_id}}

        try:
            graph_any: Any = graph

            snapshot = await asyncio.wait_for(
                graph_any.aget_state(config), timeout=10.0
            )
            values = getattr(snapshot, "values", None)
            if not values:
                return

            await asyncio.wait_for(
                graph_any.aupdate_state(config, values), timeout=10.0
            )
            logger.info(f"[BackgroundTaskManager] Flushed checkpoint for {thread_id}")
        except asyncio.TimeoutError:
            logger.warning(
                f"[BackgroundTaskManager] Checkpoint flush timed out for {thread_id}"
            )
        except Exception as e:
            logger.warning(
                f"[BackgroundTaskManager] Failed to flush checkpoint for {thread_id}: {e}"
            )

    async def _buffer_event_redis(self, thread_id: str, event: str):
        """
        Buffer workflow event to Redis (or in-memory fallback) and broadcast to live subscribers.

        Args:
            thread_id: Workflow thread identifier
            event: SSE-formatted event string
        """
        # First, broadcast to live subscribers (in-memory, unchanged)
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return

            # Broadcast to live queues
            dead_queues = []
            for queue in task_info.live_queues:
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    logger.warning(
                        f"[BackgroundTaskManager] Queue full for subscriber "
                        f"on {thread_id}, dropping event"
                    )
                except Exception as e:
                    logger.error(
                        f"[BackgroundTaskManager] Error broadcasting to queue: {e}"
                    )
                    dead_queues.append(queue)

            # Remove dead queues
            for queue in dead_queues:
                if queue in task_info.live_queues:
                    task_info.live_queues.remove(queue)

        # Store event to Redis (if configured) or fallback to in-memory
        try:
            cache = get_cache_client()

            # Check if Redis backend is enabled and Redis is available
            use_redis = (
                self.event_storage_backend == "redis"
                and cache.enabled
            )

            if not use_redis:
                # Use in-memory storage
                if self.event_storage_backend == "redis":
                    logger.warning(
                        f"[EventBuffer] Redis unavailable, using in-memory buffer for {thread_id}"
                    )

                async with self.task_lock:
                    task_info = self.tasks.get(thread_id)
                    if task_info:
                        task_info.result_buffer.append(event)
                        if len(task_info.result_buffer) > self.max_stored_messages:
                            task_info.result_buffer.popleft()
                return

            # Redis storage path
            events_key = f"workflow:events:{thread_id}"
            meta_key = f"workflow:events:meta:{thread_id}"

            # Parse event ID from SSE format
            event_id = None
            try:
                event_id_str = event.split("\n")[0].replace("id: ", "").strip()
                event_id = int(event_id_str)
            except (ValueError, IndexError):
                logger.debug(f"[EventBuffer] Could not parse event ID from SSE string")

            # Append to Redis list with automatic FIFO trimming
            success = await cache.list_append(
                events_key,
                event,  # Store raw SSE string
                max_size=self.max_stored_messages,
                ttl=self.redis_event_ttl
            )

            # Check buffer size and warn if near capacity
            if success:
                buffer_size = await cache.list_length(events_key)
                capacity_threshold = int(self.max_stored_messages * 0.9)  # 90% threshold

                if buffer_size >= capacity_threshold:
                    logger.warning(
                        f"[EventBuffer] Buffer near capacity for {thread_id}: "
                        f"{buffer_size}/{self.max_stored_messages} events. "
                        f"Oldest events will be dropped (FIFO)."
                    )

            if not success:
                # Fallback to in-memory if Redis write fails
                if self.event_storage_fallback:
                    logger.warning(
                        f"[EventBuffer] Failed to buffer event to Redis for {thread_id}, "
                        f"falling back to in-memory"
                    )
                    async with self.task_lock:
                        task_info = self.tasks.get(thread_id)
                        if task_info:
                            task_info.result_buffer.append(event)
                            if len(task_info.result_buffer) > self.max_stored_messages:
                                task_info.result_buffer.popleft()
                else:
                    logger.error(
                        f"[EventBuffer] Failed to buffer event to Redis for {thread_id}, "
                        f"fallback disabled"
                    )
                return

            # Update metadata in Redis
            now = datetime.now().isoformat()
            meta_updates: dict[str, Any] = {
                "updated_at": now,
            }

            if event_id:
                meta_updates["last_event_id"] = event_id

            # Check if this is first event
            current_meta = await cache.hash_get_all(meta_key)
            if not current_meta or "created_at" not in current_meta:
                meta_updates["created_at"] = now

            # Increment event count
            current_count = int(current_meta.get("event_count", 0)) if current_meta else 0
            meta_updates["event_count"] = current_count + 1

            # Save all metadata fields
            for field, value in meta_updates.items():
                await cache.hash_set(meta_key, field, str(value), ttl=self.redis_event_ttl)

            logger.debug(
                f"[EventBuffer] Buffered event to Redis: {thread_id} "
                f"(id={event_id}, total={meta_updates['event_count']})"
            )

        except Exception as e:
            logger.error(
                f"[EventBuffer] Error buffering event to Redis for {thread_id}: {e}",
                exc_info=True
            )
            # Fallback to in-memory on error
            if self.event_storage_fallback:
                async with self.task_lock:
                    task_info = self.tasks.get(thread_id)
                    if task_info:
                        task_info.result_buffer.append(event)
                        if len(task_info.result_buffer) > self.max_stored_messages:
                            task_info.result_buffer.popleft()

    async def _collect_subagent_results_for_turn(
        self,
        thread_id: str,
        response_id: str,
        original_chunks: list[dict[str, Any]],
        tasks: list,
        workspace_id: str,
        user_id: str,
        timeout: float = 120.0,
        is_byok: bool = False,
    ) -> None:
        """Collect subagent results for a specific turn's tasks.

        Similar to _collect_subagent_results_after_interrupt but operates on
        a specific list of tasks (filtered by spawned_turn_index).
        """
        import copy

        try:
            # Sync completion status: asyncio_task may be done but completed flag
            # not yet set (e.g., after tail phase which checks asyncio_task.done()
            # but doesn't set task.completed)
            for task in tasks:
                if not task.completed and task.asyncio_task and task.asyncio_task.done():
                    task.completed = True
                    try:
                        task.result = task.asyncio_task.result()
                    except Exception as e:
                        task.error = str(e)
                        task.result = {"success": False, "error": str(e)}

            subagent_agent_ids = {f"task:{t.task_id}" for t in tasks}
            main_chunks = [
                c for c in original_chunks
                if c.get("data", {}).get("agent", "") not in subagent_agent_ids
            ]

            all_subagent_events: list[dict] = []

            # Collect from already-completed tasks
            for task in tasks:
                if task.completed and task.captured_events:
                    for event in task.captured_events:
                        enriched = copy.deepcopy(event)
                        enriched["data"]["thread_id"] = thread_id
                        all_subagent_events.append(enriched)

            # Get pending tasks
            pending = {
                t.asyncio_task: t for t in tasks
                if t.is_pending and t.asyncio_task
            }

            # Persist initial batch if any
            if all_subagent_events:
                await self._persist_collected_events(
                    main_chunks, all_subagent_events, response_id,
                    thread_id, workspace_id, user_id,
                )

            if not pending:
                # Persist subagent token usage as separate rows
                await self._persist_subagent_usage(
                    response_id, tasks, thread_id, workspace_id, user_id,
                    is_byok=is_byok,
                )
                # Deferred cleanup: wait for per-task SSE streams to finish their
                # final drain before clearing captured_events and Redis buffers.
                await self._await_drain_and_cleanup_tasks(tasks, thread_id)
                return

            # Wait for remaining tasks one-by-one
            deadline = time.time() + timeout

            while pending:
                remaining_timeout = deadline - time.time()
                if remaining_timeout <= 0:
                    logger.warning(
                        f"[SubagentCollector] Turn collector timeout for {thread_id}, "
                        f"{len(pending)} tasks still pending"
                    )
                    break

                done, _ = await asyncio.wait(
                    pending.keys(),
                    timeout=remaining_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    break

                for asyncio_task in done:
                    task = pending.pop(asyncio_task)
                    if not task.completed:
                        task.completed = True
                        try:
                            task.result = asyncio_task.result()
                        except Exception as e:
                            task.error = str(e)
                            task.result = {"success": False, "error": str(e)}

                    if task.captured_events:
                        for event in task.captured_events:
                            enriched = copy.deepcopy(event)
                            enriched["data"]["thread_id"] = thread_id
                            all_subagent_events.append(enriched)

                if all_subagent_events:
                    await self._persist_collected_events(
                        main_chunks, all_subagent_events, response_id,
                        thread_id, workspace_id, user_id,
                    )

            # Release claim on tasks that timed out so a future collector can retry
            if pending:
                for asyncio_task, task in pending.items():
                    task.collector_response_id = None
                    logger.warning(
                        f"[SubagentCollector] Released claim on timed-out task "
                        f"{task.display_id} for thread_id={thread_id}"
                    )

            # Persist subagent token usage as separate rows
            # (only for tasks that were actually collected, not timed-out ones)
            collected_tasks = [t for t in tasks if t not in pending.values()]
            await self._persist_subagent_usage(
                response_id, collected_tasks, thread_id, workspace_id, user_id,
                is_byok=is_byok,
            )
            # Deferred cleanup: wait for per-task SSE streams to finish their
            # final drain before clearing captured_events and Redis buffers.
            await self._await_drain_and_cleanup_tasks(collected_tasks, thread_id)

        except Exception as e:
            logger.error(
                f"[SubagentCollector] Turn collector failed for {thread_id}: {e}",
                exc_info=True,
            )

    async def _await_drain_and_cleanup_tasks(
        self, tasks: list, thread_id: str, timeout: float = 10.0
    ) -> None:
        """Wait for per-task SSE streams to finish emitting, then clear buffers.

        Each task carries an ``sse_drain_complete`` event that is set by
        ``stream_subagent_task_events`` after its final drain.  We await all of
        them concurrently so the collector only clears captured_events once every
        live SSE consumer has delivered the full event sequence.

        If no SSE consumer is connected (event never set), the timeout ensures
        cleanup still happens — persistence has already succeeded at this point.
        """
        async def _wait_one(event: "asyncio.Event") -> None:
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass  # No active SSE consumer, or too slow — safe to clear

        await asyncio.gather(*[_wait_one(t.sse_drain_complete) for t in tasks])

        cache = get_cache_client()
        for task in tasks:
            task.captured_events = []
            task.per_call_records = []
            try:
                await cache.delete(f"subagent:events:{thread_id}:{task.task_id}")
            except Exception:
                pass

    # ========== Workflow Completion & Error Handlers ==========

    async def _mark_completed(self, thread_id: str):
        """Mark workflow as completed and notify live subscribers.

        Split into two phases to avoid holding the lock during heavy async I/O:
        - Phase 1 (under lock): status update, sentinels, copy refs
        - Phase 2 (outside lock): aget_state, persistence, callbacks, collector
        """
        # Phase 1: Quick state update under lock
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()

            # Send completion sentinel to all live subscribers
            for queue in task_info.live_queues:
                try:
                    queue.put_nowait(None)  # None signals completion
                except Exception as e:
                    logger.error(f"Error sending completion signal: {e}")

            # Copy refs needed for persistence phase
            graph = task_info.graph
            metadata = task_info.metadata
            completion_callback = task_info.completion_callback

        # Phase 2: Heavy I/O outside lock (with timeout protection)
        is_interrupted = False
        try:
            if graph:
                snapshot = await asyncio.wait_for(
                    graph.aget_state({"configurable": {"thread_id": thread_id}}),
                    timeout=10.0,
                )
                if snapshot and snapshot.next:
                    # Workflow has pending nodes = interrupted, not completed
                    is_interrupted = True
        except asyncio.TimeoutError:
            logger.error(
                f"[BackgroundTaskManager] aget_state timed out for {thread_id} in _mark_completed"
            )
        except Exception as state_error:
            logger.warning(
                f"[BackgroundTaskManager] Could not check workflow state for {thread_id}: {state_error}"
            )

        # Database status will be updated by persistence service in transaction
        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        # Persist workflow state based on completion vs interrupt
        if is_interrupted:
            # Workflow interrupted - persist interrupt state with all required fields
            if workspace_id and user_id:
                try:
                    from src.server.services.conversation_persistence_service import ConversationPersistenceService

                    persistence_service = ConversationPersistenceService.get_instance(thread_id)
                    persistence_service._on_pair_persisted = lambda: self.clear_event_buffer(thread_id)

                    # Get token usage and per_call_records from token_callback
                    _, per_call_records = get_token_usage_from_callback(
                        metadata, "interrupt", thread_id
                    )

                    # Get tool usage from handler (has cached result from SSE emission)
                    tool_usage = get_tool_usage_from_handler(
                        metadata, "interrupt", thread_id
                    )

                    # Get SSE events for persistence (plan description, reasoning, etc.)
                    sse_events = get_sse_events_from_handler(
                        metadata, "interrupt", thread_id
                    )

                    # Determine actual interrupt reason from SSE events
                    interrupt_reason = "plan_review_required"  # default fallback
                    if sse_events:
                        for chunk in sse_events:
                            if chunk.get("event") == "interrupt":
                                chunk_data = chunk.get("data", {})
                                action_requests = chunk_data.get("action_requests", [])
                                if action_requests:
                                    action_type = action_requests[0].get("type")
                                    if action_type == "ask_user_question":
                                        interrupt_reason = "user_question"
                                break

                    # Calculate execution time from start_time
                    execution_time = calculate_execution_time(metadata)

                    # Build metadata with all context
                    persist_metadata = {
                        "msg_type": metadata.get("msg_type"),
                        "stock_code": metadata.get("stock_code"),
                        "deepthinking": metadata.get("deepthinking", False),
                        "byok_active": metadata.get("byok_active", False)
                    }

                    await persistence_service.persist_interrupt(
                        interrupt_reason=interrupt_reason,
                        execution_time=execution_time,
                        metadata=persist_metadata,
                        per_call_records=per_call_records,
                        tool_usage=tool_usage,
                        sse_events=sse_events
                    )
                    logger.info(f"[WorkflowPersistence] Workflow {thread_id} paused for human feedback")

                    # Update Redis workflow tracker to interrupted
                    # (prevents frontend from reconnecting to a paused workflow)
                    from src.server.services.workflow_tracker import WorkflowTracker
                    tracker = WorkflowTracker.get_instance()
                    await tracker.mark_interrupted(
                        thread_id=thread_id,
                        metadata={"interrupt_reason": interrupt_reason},
                    )
                except Exception as persist_error:
                    logger.error(
                        f"[WorkflowPersistence] Failed to persist interrupt for thread_id={thread_id}: {persist_error}",
                        exc_info=True
                    )
        else:
            # Workflow completed - invoke completion callback for full persistence
            if completion_callback:
                try:
                    await completion_callback()
                except Exception as e:
                    logger.error(
                        f"[BackgroundTaskManager] Completion callback failed for {thread_id}: {e}",
                        exc_info=True
                    )
                    # Update workflow status to error when callback fails
                    await self._mark_failed(thread_id, f"Completion callback failed: {str(e)}")

        # Spawn collector for subagent events + usage merge
        from src.server.services.conversation_persistence_service import ConversationPersistenceService
        ps = ConversationPersistenceService.get_instance(thread_id)
        response_id = ps._current_response_id

        if response_id:
            from src.server.services.background_registry_store import BackgroundRegistryStore
            bg_store = BackgroundRegistryStore.get_instance()
            bg_registry = await bg_store.get_registry(thread_id)
            if bg_registry:
                # Atomically claim uncollected tasks to prevent double-persist
                # when two collectors run concurrently (e.g. subagent from turn 0
                # still pending when turn 1 completes).
                tasks_to_collect = []
                for t in bg_registry._tasks.values():
                    if t.collector_response_id:
                        continue  # Already claimed by another turn's collector
                    if t.is_pending or t.captured_events or t.per_call_records:
                        t.collector_response_id = response_id
                        tasks_to_collect.append(t)
                if tasks_to_collect:
                    workspace_id = metadata.get("workspace_id")
                    user_id = metadata.get("user_id")
                    handler = metadata.get("handler")
                    sse_events = handler.get_sse_events() if handler else []
                    if workspace_id and user_id:
                        asyncio.create_task(
                            self._collect_subagent_results_for_turn(
                                thread_id=thread_id,
                                response_id=response_id,
                                original_chunks=sse_events or [],
                                tasks=tasks_to_collect,
                                workspace_id=workspace_id,
                                user_id=user_id,
                                is_byok=metadata.get("byok_active", False),
                            ),
                            name=f"subagent-collector-{thread_id}-post-tail",
                        )

    async def _mark_failed(self, thread_id: str, error: str):
        """Mark workflow as failed and notify live subscribers.

        Split into two phases to avoid holding the lock during heavy async I/O:
        - Phase 1 (under lock): status update, sentinels, copy refs
        - Phase 2 (outside lock): aget_state, persistence
        """
        # Phase 1: Quick state update under lock
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return

            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = error

            # Send completion sentinel to all live subscribers
            for queue in task_info.live_queues:
                try:
                    queue.put_nowait(None)  # None signals completion
                except Exception as e:
                    logger.error(f"Error sending completion signal: {e}")

            # Copy refs needed for persistence phase
            graph = task_info.graph
            metadata = task_info.metadata

        # Phase 2: Heavy I/O outside lock
        logger.error(
            f"[BackgroundTaskManager] Workflow {thread_id} failed: {error}"
        )

        # Persist error with full details
        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if workspace_id and user_id:
            try:
                from src.server.services.conversation_persistence_service import ConversationPersistenceService

                persistence_service = ConversationPersistenceService.get_instance(thread_id)
                persistence_service._on_pair_persisted = lambda: self.clear_event_buffer(thread_id)

                # Calculate execution time
                execution_time = calculate_execution_time(metadata)

                # Get token usage and per_call_records from token_callback
                _, per_call_records = get_token_usage_from_callback(
                    metadata, "error", thread_id
                )

                # Get tool usage from handler (has cached result from SSE emission)
                tool_usage = get_tool_usage_from_handler(
                    metadata, "error", thread_id
                )

                sse_events = get_sse_events_from_handler(
                    metadata, "error", thread_id
                )

                # Build metadata with all context
                persist_metadata = {
                    "msg_type": metadata.get("msg_type"),
                    "stock_code": metadata.get("stock_code"),
                    "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
                    "deepthinking": metadata.get("deepthinking", False),
                    "byok_active": metadata.get("byok_active", False)
                }

                await persistence_service.persist_error(
                    error_message=error,
                    errors=[error],
                    execution_time=execution_time,
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events,
                    metadata=persist_metadata
                )
                logger.info(f"[WorkflowPersistence] Error persisted for thread_id={thread_id}")
            except Exception as persist_error:
                logger.error(
                    f"[WorkflowPersistence] Failed to persist error for {thread_id}: {persist_error}",
                    exc_info=True
                )

    async def _mark_soft_interrupted(self, thread_id: str) -> None:
        """Mark workflow as soft-interrupted (ESC).

        This ends the foreground workflow execution so the user can immediately
        send a follow-up message on the same `thread_id`, while leaving any
        independently running background subagent tasks alone.

        Split into two phases to avoid holding the lock during heavy async I/O:
        - Phase 1 (under lock): status update, sentinels, copy refs
        - Phase 2 (outside lock): aget_state, persistence, subagent collector
        """
        # Phase 1: Quick state update under lock
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return

            task_info.status = TaskStatus.SOFT_INTERRUPTED
            task_info.completed_at = datetime.now()

            # Notify all live subscribers that the workflow stream ended
            for queue in task_info.live_queues:
                with suppress(Exception):
                    queue.put_nowait(None)

            # Copy refs needed for persistence phase
            graph = task_info.graph
            metadata = task_info.metadata

        # Phase 2: Heavy I/O outside lock
        logger.info(f"[BackgroundTaskManager] Marked as soft-interrupted: {thread_id}")

        # Persist soft interrupt so query/response pair is saved
        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if workspace_id and user_id:
            try:
                from src.server.services.conversation_persistence_service import ConversationPersistenceService

                persistence_service = ConversationPersistenceService.get_instance(
                    thread_id,
                    workspace_id=workspace_id,
                    user_id=user_id
                )
                persistence_service._on_pair_persisted = lambda: self.clear_event_buffer(thread_id)

                _, per_call_records = get_token_usage_from_callback(
                    metadata, "interrupt", thread_id
                )

                tool_usage = get_tool_usage_from_handler(
                    metadata, "interrupt", thread_id
                )

                sse_events = get_sse_events_from_handler(
                    metadata, "interrupt", thread_id
                )

                execution_time = calculate_execution_time(metadata)

                persist_metadata = {
                    "msg_type": metadata.get("msg_type"),
                    "stock_code": metadata.get("stock_code"),
                    "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
                    "deepthinking": metadata.get("deepthinking", False),
                    "byok_active": metadata.get("byok_active", False),
                    "soft_interrupted": True
                }

                response_id = await persistence_service.persist_interrupt(
                    interrupt_reason="soft_interrupt",
                    execution_time=execution_time,
                    metadata=persist_metadata,
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events
                )
                logger.info(f"[WorkflowPersistence] Soft interrupt persisted for thread_id={thread_id}")

                # Spawn collector if subagents are still running
                from src.server.services.background_registry_store import BackgroundRegistryStore
                bg_store = BackgroundRegistryStore.get_instance()
                bg_registry = await bg_store.get_registry(thread_id)

                if bg_registry and bg_registry.has_pending_tasks():
                    logger.info(
                        f"[WorkflowPersistence] {bg_registry.pending_count} subagents still running, "
                        f"spawning result collector for thread_id={thread_id}"
                    )
                    asyncio.create_task(
                        self._collect_subagent_results_after_interrupt(
                            thread_id=thread_id,
                            response_id=response_id,
                            original_chunks=sse_events or [],
                            bg_registry=bg_registry,
                            workspace_id=workspace_id,
                            user_id=user_id,
                            timeout=120.0,
                            is_byok=metadata.get("byok_active", False),
                        ),
                        name=f"subagent-collector-{thread_id}",
                    )
            except Exception as persist_error:
                logger.error(
                    f"[WorkflowPersistence] Failed to persist soft interrupt for {thread_id}: {persist_error}",
                    exc_info=True
                )

    async def _collect_subagent_results_after_interrupt(
        self,
        thread_id: str,
        response_id: str,
        original_chunks: list[dict[str, Any]],
        bg_registry: Any,
        workspace_id: str,
        user_id: str,
        timeout: float = 120.0,
        is_byok: bool = False,
    ) -> None:
        """Wait for subagents incrementally, persist as each completes.

        Fire-and-forget task spawned by _mark_soft_interrupted() when background
        subagents are still running after the user presses ESC.

        Uses asyncio.FIRST_COMPLETED so each subagent's events are persisted
        to DB as soon as that subagent finishes, rather than waiting for all.
        """
        import copy

        try:
            # Claim uncollected tasks atomically to prevent double-persist
            all_tasks = [
                t for t in await bg_registry.get_all_tasks()
                if not t.collector_response_id
            ]
            for t in all_tasks:
                t.collector_response_id = response_id

            # Sync completion status: asyncio_task may be done but completed flag
            # not yet set (same gap as in _collect_subagent_results_for_turn)
            for task in all_tasks:
                if not task.completed and task.asyncio_task and task.asyncio_task.done():
                    task.completed = True
                    try:
                        task.result = task.asyncio_task.result()
                    except Exception as e:
                        task.error = str(e)
                        task.result = {"success": False, "error": str(e)}

            subagent_agent_ids = {f"task:{t.task_id}" for t in all_tasks}

            logger.info(
                f"[SubagentCollector] Starting incremental collection for "
                f"thread_id={thread_id}, total_tasks={len(all_tasks)}, "
                f"pending={bg_registry.pending_count}"
            )

            # Main agent events (unchanged throughout)
            main_chunks = [
                c for c in original_chunks
                if c.get("data", {}).get("agent", "") not in subagent_agent_ids
            ]

            # Accumulate subagent events across iterations
            all_subagent_events: list[dict] = []

            # Collect from already-completed tasks
            for task in all_tasks:
                if task.completed and task.captured_events:
                    for event in task.captured_events:
                        enriched = copy.deepcopy(event)
                        enriched["data"]["thread_id"] = thread_id
                        all_subagent_events.append(enriched)

            # Get pending tasks
            pending = {
                t.asyncio_task: t for t in all_tasks
                if t.is_pending and t.asyncio_task
            }

            # Persist initial batch if any already-completed tasks had events
            if all_subagent_events:
                await self._persist_collected_events(
                    main_chunks, all_subagent_events, response_id,
                    thread_id, workspace_id, user_id,
                )

            if not pending:
                if not all_subagent_events:
                    logger.info(
                        f"[SubagentCollector] No subagent events captured "
                        f"for thread_id={thread_id}"
                    )
                # Persist subagent token usage as separate rows
                await self._persist_subagent_usage(
                    response_id, all_tasks, thread_id, workspace_id, user_id,
                    is_byok=is_byok,
                )
                await self._await_drain_and_cleanup_tasks(all_tasks, thread_id)
                return

            # Wait for remaining tasks one-by-one
            deadline = time.time() + timeout

            while pending:
                remaining_timeout = deadline - time.time()
                if remaining_timeout <= 0:
                    logger.warning(
                        f"[SubagentCollector] Timeout for thread_id={thread_id}, "
                        f"{len(pending)} tasks still pending"
                    )
                    break

                done, _ = await asyncio.wait(
                    pending.keys(),
                    timeout=remaining_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    break  # timeout

                for asyncio_task in done:
                    task = pending.pop(asyncio_task)

                    # Mark task completed
                    async with bg_registry._lock:
                        task.completed = True
                        try:
                            task.result = asyncio_task.result()
                        except Exception as e:
                            task.error = str(e)
                            task.result = {"success": False, "error": str(e)}

                    # Collect this task's captured events
                    if task.captured_events:
                        for event in task.captured_events:
                            enriched = copy.deepcopy(event)
                            enriched["data"]["thread_id"] = thread_id
                            all_subagent_events.append(enriched)

                    logger.info(
                        f"[SubagentCollector] {task.display_id} completed, "
                        f"persisting {len(all_subagent_events)} total events"
                    )

                # Persist after each batch of completions
                if all_subagent_events:
                    await self._persist_collected_events(
                        main_chunks, all_subagent_events, response_id,
                        thread_id, workspace_id, user_id,
                    )

            # Release claim on tasks that timed out so a future collector can retry
            if pending:
                for asyncio_task, task in pending.items():
                    task.collector_response_id = None
                    logger.warning(
                        f"[SubagentCollector] Released claim on timed-out task "
                        f"{task.display_id} for thread_id={thread_id}"
                    )

            # Persist subagent token usage as separate rows
            # (only for tasks that were actually collected, not timed-out ones)
            collected_tasks = [t for t in all_tasks if t not in pending.values()]
            await self._persist_subagent_usage(
                response_id, collected_tasks, thread_id, workspace_id, user_id,
                is_byok=is_byok,
            )
            await self._await_drain_and_cleanup_tasks(collected_tasks, thread_id)

        except Exception as e:
            logger.error(
                f"[SubagentCollector] Failed for thread_id={thread_id}: {e}",
                exc_info=True,
            )

    async def _persist_collected_events(
        self,
        main_chunks: list[dict],
        subagent_events: list[dict],
        response_id: str,
        thread_id: str,
        workspace_id: str,
        user_id: str,
    ) -> None:
        """Clean and persist main + subagent events to DB.

        Subagent events are already in correct sequential order from
        counter.py's await-based capture (append_captured_event under lock).
        We preserve this insertion order rather than sorting by timestamp,
        which can reorder events captured in tight loops with identical
        time.time() values.
        """
        import copy

        cleaned = []
        for event in subagent_events:
            e = copy.deepcopy(event)
            e.pop("ts", None)
            cleaned.append(e)

        updated_chunks = main_chunks + cleaned

        from src.server.services.conversation_persistence_service import (
            ConversationPersistenceService,
        )
        persistence_service = ConversationPersistenceService.get_instance(
            thread_id, workspace_id=workspace_id, user_id=user_id,
        )
        await persistence_service.update_sse_events(
            response_id=response_id, sse_events=updated_chunks,
        )

    async def _persist_subagent_usage(
        self,
        response_id: str,
        tasks: list,
        thread_id: str,
        workspace_id: str,
        user_id: str,
        is_byok: bool = False,
    ) -> None:
        """Persist each subagent's token usage as a separate row with msg_type='task'.

        Instead of merging subagent costs into the parent turn's record, each
        subagent gets its own conversation_usages row linked to the same
        response_id. This avoids complex read-merge-write and keeps subagent
        costs independently queryable.

        Args:
            response_id: The parent conversation_response_id (for association)
            tasks: List of BackgroundTask objects with per_call_records
            thread_id: Thread ID for logging
            workspace_id: Workspace ID for the usage record
            user_id: User ID for the usage record
        """
        from src.server.services.usage_persistence_service import UsagePersistenceService

        tasks_with_records = [t for t in tasks if t.per_call_records]
        if not tasks_with_records:
            return

        persisted_count = 0

        for task in tasks_with_records:
            try:
                usage_service = UsagePersistenceService(
                    thread_id=thread_id,
                    workspace_id=workspace_id,
                    user_id=user_id,
                )
                await usage_service.track_llm_usage(task.per_call_records)

                # Enrich the computed token_usage with subagent identity
                # (track_llm_usage already aggregated tokens + costs correctly)
                if usage_service._token_usage is not None:
                    usage_service._token_usage["task_id"] = task.task_id
                    usage_service._token_usage["agent_id"] = task.agent_id
                    usage_service._token_usage["subagent_type"] = task.subagent_type

                await usage_service.persist_usage(
                    response_id=response_id,
                    msg_type="task",
                    status="completed",
                    is_byok=is_byok,
                )
                persisted_count += 1

            except Exception as e:
                logger.error(
                    f"[SubagentUsage] Failed to persist usage for task {task.task_id} "
                    f"in thread_id={thread_id}: {e}",
                    exc_info=True,
                )

        if persisted_count:
            total_records = sum(len(t.per_call_records) for t in tasks_with_records)
            logger.info(
                f"[SubagentUsage] Persisted {persisted_count} subagent usage row(s) "
                f"({total_records} LLM calls) for response_id={response_id} "
                f"thread_id={thread_id}"
            )

    async def _mark_cancelled(self, thread_id: str):
        """Mark workflow as cancelled and notify live subscribers.

        Split into two phases to avoid holding the lock during heavy async I/O:
        - Phase 1 (under lock): status update, sentinels, copy refs
        - Phase 2 (outside lock): aget_state, persistence
        """
        # Phase 1: Quick state update under lock
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return

            task_info.status = TaskStatus.CANCELLED
            task_info.completed_at = datetime.now()

            for queue in task_info.live_queues:
                try:
                    queue.put_nowait(None)
                except Exception as e:
                    logger.error(f"Error sending completion signal: {e}")

            # Copy refs needed for persistence phase
            graph = task_info.graph
            metadata = task_info.metadata

        # Phase 2: Heavy I/O outside lock
        logger.debug(f"[BackgroundTaskManager] Marked as cancelled: {thread_id}")

        # Persist cancellation with full details
        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if workspace_id and user_id:
            try:
                from src.server.services.conversation_persistence_service import ConversationPersistenceService

                persistence_service = ConversationPersistenceService.get_instance(thread_id)
                persistence_service._on_pair_persisted = lambda: self.clear_event_buffer(thread_id)

                # Calculate token usage AND keep per_call_records
                _, per_call_records = get_token_usage_from_callback(
                    metadata, "cancellation", thread_id
                )

                # Get tool usage from handler (has cached result from SSE emission)
                tool_usage = get_tool_usage_from_handler(
                    metadata, "cancellation", thread_id
                )

                sse_events = get_sse_events_from_handler(
                    metadata, "cancellation", thread_id
                )

                # Calculate execution time
                execution_time = calculate_execution_time(metadata)

                # Build persist metadata (include deepthinking for usage tracking)
                persist_metadata = {
                    "msg_type": metadata.get("msg_type"),
                    "stock_code": metadata.get("stock_code"),
                    "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
                    "deepthinking": metadata.get("deepthinking", False),
                    "byok_active": metadata.get("byok_active", False),
                    "cancelled_by_user": True
                }

                await persistence_service.persist_cancelled(
                    execution_time=execution_time,
                    metadata=persist_metadata,
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events
                )
                logger.info(f"[WorkflowPersistence] Cancellation persisted for thread_id={thread_id}")
            except Exception as persist_error:
                logger.error(
                    f"[WorkflowPersistence] Failed to persist cancellation for {thread_id}: {persist_error}",
                    exc_info=True
                )

    async def get_task_status(self, thread_id: str) -> Optional[TaskStatus]:
        """
        Get status of a background task.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            TaskStatus or None if not found
        """
        task_info = await self._get_task_info_locked(thread_id)
        return task_info.status if task_info else None

    async def get_task_info(self, thread_id: str) -> Optional[TaskInfo]:
        """
        Get full task information.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            TaskInfo or None if not found
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if task_info:
                # Update last access time
                task_info.last_access_at = datetime.now()
            return task_info

    async def increment_connection(self, thread_id: str) -> bool:
        """
        Increment active connection count for a workflow.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            True if successful, False if task not found
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if task_info:
                task_info.active_connections += 1
                task_info.last_access_at = datetime.now()
                logger.debug(
                    f"[BackgroundTaskManager] Connection attached to {thread_id} "
                    f"(active: {task_info.active_connections})"
                )
                return True
            return False

    async def decrement_connection(self, thread_id: str) -> bool:
        """
        Decrement active connection count for a workflow.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            True if successful, False if task not found
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if task_info:
                task_info.active_connections = max(0, task_info.active_connections - 1)
                logger.debug(
                    f"[BackgroundTaskManager] Connection detached from {thread_id} "
                    f"(active: {task_info.active_connections})"
                )
                return True
            return False

    async def get_buffered_events_redis(
        self,
        thread_id: str,
        from_beginning: bool = False,
        after_event_id: Optional[int] = None
    ) -> list:
        """
        Get buffered events from Redis (or in-memory fallback).

        Args:
            thread_id: Workflow thread identifier
            from_beginning: If True, return all buffered events
            after_event_id: Optional event ID to filter events (return events > this ID)

        Returns:
            List of SSE-formatted event strings
        """
        try:
            cache = get_cache_client()

            # Check if Redis backend is enabled and Redis is available
            use_redis = (
                self.event_storage_backend == "redis"
                and cache.enabled
            )

            if not use_redis:
                # Fallback to in-memory
                if self.event_storage_backend == "redis":
                    logger.warning(
                        f"[EventBuffer] Redis unavailable, using in-memory buffer for {thread_id}"
                    )

                async with self.task_lock:
                    task_info = self.tasks.get(thread_id)
                    if not task_info or not task_info.result_buffer:
                        return []

                    events = list(task_info.result_buffer)

                    # Filter by event ID if requested
                    if after_event_id is not None:
                        filtered_events = []
                        for event in events:
                            try:
                                event_id_str = event.split("\n")[0].replace("id: ", "").strip()
                                event_id = int(event_id_str)
                                if event_id > after_event_id:
                                    filtered_events.append(event)
                            except (ValueError, IndexError):
                                # Can't parse ID, include it to be safe
                                filtered_events.append(event)
                        return filtered_events

                    return events

            # Redis retrieval path
            events_key = f"workflow:events:{thread_id}"

            # Get all events from list
            events = await cache.list_range(events_key, start=0, end=-1)

            if not events:
                logger.debug(f"[EventBuffer] No buffered events for {thread_id}")
                return []

            # Filter by event ID if requested
            if after_event_id is not None:
                filtered_events = []
                for event in events:
                    try:
                        # Parse event ID from SSE format
                        event_id_str = event.split("\n")[0].replace("id: ", "").strip()
                        event_id = int(event_id_str)

                        if event_id > after_event_id:
                            filtered_events.append(event)

                    except (ValueError, IndexError):
                        # Can't parse ID, include it to be safe
                        filtered_events.append(event)

                logger.info(
                    f"[EventBuffer] Retrieved {len(filtered_events)} events "
                    f"(after_event_id={after_event_id}) for {thread_id}"
                )
                return filtered_events

            logger.info(f"[EventBuffer] Retrieved {len(events)} events for {thread_id}")
            return events

        except Exception as e:
            logger.error(
                f"[EventBuffer] Error retrieving events from Redis for {thread_id}: {e}",
                exc_info=True
            )

            # Fallback to in-memory on error
            if self.event_storage_fallback:
                async with self.task_lock:
                    task_info = self.tasks.get(thread_id)
                    if not task_info or not task_info.result_buffer:
                        return []
                    return list(task_info.result_buffer)

            return []

    async def clear_event_buffer(self, thread_id: str):
        """
        Clear event buffer for a thread (both Redis and in-memory).

        This should be called when resuming a workflow from interrupt to prevent
        old interrupt events from persisting in the buffer.

        Args:
            thread_id: Workflow thread identifier
        """
        try:
            cache = get_cache_client()

            # Clear Redis buffer if using Redis backend
            if self.event_storage_backend == "redis" and cache.enabled:
                events_key = f"workflow:events:{thread_id}"
                meta_key = f"workflow:events:meta:{thread_id}"

                # Delete both the event list and metadata
                await cache.delete(events_key)
                await cache.delete(meta_key)

                logger.info(f"[EventBuffer] Cleared Redis event buffer for {thread_id}")

            # Also clear in-memory buffer (fallback or dual-mode)
            async with self.task_lock:
                task_info = self.tasks.get(thread_id)
                if task_info and task_info.result_buffer:
                    task_info.result_buffer.clear()
                    logger.debug(f"[EventBuffer] Cleared in-memory buffer for {thread_id}")

        except Exception as e:
            logger.error(
                f"[EventBuffer] Error clearing event buffer for {thread_id}: {e}",
                exc_info=True
            )

    async def subscribe_to_live_events(self, thread_id: str, event_queue: asyncio.Queue) -> bool:
        """
        Subscribe to live events from a running workflow.

        Args:
            thread_id: Workflow thread identifier
            event_queue: Queue to receive live events

        Returns:
            True if subscribed successfully, False if workflow not found
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return False

            if event_queue not in task_info.live_queues:
                task_info.live_queues.append(event_queue)
                logger.debug(
                    f"[BackgroundTaskManager] Subscribed to live events for {thread_id} "
                    f"(subscribers: {len(task_info.live_queues)})"
                )
            return True

    async def unsubscribe_from_live_events(self, thread_id: str, event_queue: asyncio.Queue) -> bool:
        """
        Unsubscribe from live events.

        Args:
            thread_id: Workflow thread identifier
            event_queue: Queue to unsubscribe

        Returns:
            True if unsubscribed successfully, False if workflow not found
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return False

            if event_queue in task_info.live_queues:
                task_info.live_queues.remove(event_queue)
                logger.debug(
                    f"[BackgroundTaskManager] Unsubscribed from live events for {thread_id} "
                    f"(subscribers: {len(task_info.live_queues)})"
                )
            return True

    async def cancel_workflow(self, thread_id: str) -> bool:
        """
        Cancel a running workflow using cooperative event signaling.

        Sets the cancel_event flag which will be detected on the next event
        iteration inside the shielded task, allowing graceful cancellation.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            True if cancellation signaled, False if not found or already completed
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                logger.warning(
                    f"[BackgroundTaskManager] Cannot cancel {thread_id}: "
                    f"workflow not found"
                )
                return False

            if task_info.status not in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                logger.info(
                    f"[BackgroundTaskManager] Cannot cancel {thread_id}: "
                    f"status={task_info.status}"
                )
                return False

            task_info.cancel_event.set()
            task_info.explicit_cancel = True
            logger.debug(f"[BackgroundTaskManager] Cancellation signaled: {thread_id}")
            return True

    async def soft_interrupt_workflow(self, thread_id: str) -> Dict[str, Any]:
        """
        Soft interrupt a running workflow - pause main agent, keep subagents running.

        Unlike cancel_workflow which stops everything, soft interrupt:
        - Signals the main agent to pause at the next safe point
        - Background subagents continue execution
        - Workflow can be resumed with new input

        Args:
            thread_id: Workflow thread identifier

        Returns:
            Dict with status, can_resume, and active_subagents
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                logger.warning(
                    f"[BackgroundTaskManager] Cannot soft interrupt {thread_id}: "
                    f"workflow not found"
                )
                return {
                    "status": "not_found",
                    "thread_id": thread_id,
                    "can_resume": False,
                    "background_tasks": [],
                    "active_subagents": [],
                    "completed_subagents": [],
                }

            # Query registry for active/completed task IDs
            active_tasks: list[str] = []
            completed_tasks: list[str] = []
            try:
                from src.server.services.background_registry_store import BackgroundRegistryStore
                registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)
                if registry:
                    for task in await registry.get_all_tasks():
                        if task.is_pending:
                            active_tasks.append(task.task_id)
                        else:
                            completed_tasks.append(task.task_id)
            except Exception:
                pass

            if task_info.status not in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                logger.info(
                    f"[BackgroundTaskManager] Cannot soft interrupt {thread_id}: "
                    f"status={task_info.status}"
                )
                return {
                    "status": task_info.status.value,
                    "thread_id": thread_id,
                    "can_resume": False,
                    # Backward-compatible key
                    "background_tasks": active_tasks,
                    # Preferred keys (used by CLI)
                    "active_subagents": active_tasks,
                    "completed_subagents": completed_tasks,
                }

            # Set soft interrupt flag (different from cancel)
            task_info.soft_interrupt_event.set()
            task_info.soft_interrupted = True
            logger.info(
                f"[BackgroundTaskManager] Soft interrupt signaled: {thread_id}, "
                f"active_subagents={active_tasks}"
            )

            return {
                "status": "soft_interrupted",
                "thread_id": thread_id,
                "can_resume": True,
                # Backward-compatible key
                "background_tasks": active_tasks,
                # Preferred keys (used by CLI)
                "active_subagents": active_tasks,
                "completed_subagents": completed_tasks,
            }

    async def get_workflow_status(self, thread_id: str) -> Dict[str, Any]:
        """
        Get detailed workflow status including subagent information.

        Args:
            thread_id: Workflow thread identifier

        Returns:
            Dict with status, subagent info, timestamps
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return {
                    "status": "not_found",
                    "thread_id": thread_id,
                }

            # Query registry for actual task IDs (not just agent names)
            active_tasks: list[str] = []
            try:
                from src.server.services.background_registry_store import BackgroundRegistryStore
                registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)
                if registry:
                    for task in await registry.get_all_tasks():
                        if task.is_pending:
                            active_tasks.append(task.task_id)
            except Exception:
                pass

            return {
                "status": task_info.status.value,
                "thread_id": thread_id,
                "soft_interrupted": task_info.soft_interrupted,
                "active_tasks": active_tasks,
                "created_at": task_info.created_at.isoformat() if task_info.created_at else None,
                "started_at": task_info.started_at.isoformat() if task_info.started_at else None,
                "completed_at": task_info.completed_at.isoformat() if task_info.completed_at else None,
                "active_connections": task_info.active_connections,
            }

    async def wait_for_soft_interrupted(
        self,
        thread_id: str,
        timeout: float = 30.0
    ) -> bool:
        """
        Wait for a soft-interrupted workflow to complete.

        Called before starting a new workflow on the same thread_id to ensure
        seamless continuation after ESC interrupt.

        Args:
            thread_id: Workflow thread identifier
            timeout: Maximum time to wait in seconds

        Returns:
            True if workflow completed (or wasn't running), False if timed out
        """
        async with self.task_lock:
            task_info = self.tasks.get(thread_id)
            if not task_info:
                return True  # No workflow to wait for

            # Include SOFT_INTERRUPTED - the task may still be wrapping up
            if task_info.status not in [
                TaskStatus.QUEUED, TaskStatus.RUNNING,
                TaskStatus.SOFT_INTERRUPTED,
            ]:
                return True  # Already fully completed

            if not task_info.soft_interrupted and task_info.status != TaskStatus.SOFT_INTERRUPTED:
                # Workflow is running but wasn't soft-interrupted
                # This is an unexpected state - user might be trying to send
                # concurrent messages. We'll wait briefly but not block too long.
                timeout = min(timeout, 5.0)

            task = task_info.task

        if not task:
            return True

        logger.info(
            f"[BackgroundTaskManager] Waiting for soft-interrupted workflow "
            f"{thread_id} to complete (timeout={timeout}s)"
        )

        try:
            # Wait for the task to complete with timeout
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)

            # Clean up the finished task so start_workflow can proceed
            async with self.task_lock:
                task_info = self.tasks.get(thread_id)
                if task_info and task_info.status in (
                    TaskStatus.SOFT_INTERRUPTED, TaskStatus.COMPLETED,
                ):
                    logger.info(
                        f"[BackgroundTaskManager] Cleaning up {task_info.status.value} task {thread_id}"
                    )
                    del self.tasks[thread_id]

            logger.info(
                f"[BackgroundTaskManager] Previous workflow {thread_id} "
                f"completed, ready for new request"
            )
            return True
        except asyncio.TimeoutError:
            logger.warning(
                f"[BackgroundTaskManager] Timeout waiting for soft-interrupted "
                f"workflow {thread_id} after {timeout}s"
            )
            return False
        except asyncio.CancelledError:
            # Task was cancelled, which is fine - we can proceed
            return True
        except Exception as e:
            logger.warning(
                f"[BackgroundTaskManager] Error waiting for soft-interrupted "
                f"workflow {thread_id}: {e}"
            )
            return True  # Proceed anyway

    async def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about background tasks.

        Returns:
            Dictionary with task statistics
        """
        async with self.task_lock:
            total = len(self.tasks)
            by_status = {}
            for status in TaskStatus:
                by_status[status.value] = sum(
                    1 for t in self.tasks.values() if t.status == status
                )

            return {
                "total_tasks": total,
                "by_status": by_status,
                "max_concurrent": self.max_concurrent,
                "active_connections": sum(
                    t.active_connections for t in self.tasks.values()
                )
            }
