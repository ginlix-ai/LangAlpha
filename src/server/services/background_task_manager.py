"""
Background Task Manager

Manages workflow execution as background asyncio tasks that continue running
independently of SSE client connections. Workflows write events to per-run
Redis Streams (``workflow:stream:{thread_id}:{run_id}``); consumers attach by
stream key and read via XREAD BLOCK, sharing no in-process state with the
workflow. Cleanup runs periodically to evict stale tasks.

State is keyed by ``(thread_id, run_id)`` — each POST gets a fresh ``run_id``
at the handler entry, so cross-turn state aliasing is impossible by
construction. Per-thread admission locks still serialize the
``wait_or_steer → persist_query_start → start_workflow`` window because
Pregel doesn't serialize concurrent ``astream`` on the same thread, and the
admission policy lives in our layer.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, AsyncIterator, Literal, Optional, Callable, Coroutine
from enum import Enum
from dataclasses import dataclass, field
from contextlib import suppress

from src.config.settings import (
    get_max_concurrent_workflows,
    get_workflow_result_ttl,
    get_abandoned_workflow_timeout,
    get_cleanup_interval,
    is_intermediate_storage_enabled,
    get_max_stored_messages_per_agent,
    get_event_storage_backend,
    get_redis_ttl_workflow_events,
    get_shutdown_timeout,
    get_checkpoint_flush_timeout,
    get_sse_drain_timeout,
    get_wait_for_persistence_timeout,
    get_stop_drain_timeout,
    get_subagent_collector_timeout,
    get_subagent_orphan_collector_timeout,
)
from src.utils.cache.redis_cache import get_cache_client
from src.server.dependencies.usage_limits import release_burst_slot
from src.server.services.workflow_tracker import WorkflowTracker
from src.server.utils.persistence_utils import (
    get_token_usage_from_callback,
    get_tool_usage_from_handler,
    get_sse_events_from_handler,
    calculate_execution_time,
)

logger = logging.getLogger(__name__)


# ========== Redis key helpers ==========


def stream_key(thread_id: str, run_id: str) -> str:
    """Per-run workflow event stream."""
    return f"workflow:stream:{thread_id}:{run_id}"


def stream_meta_key(thread_id: str, run_id: str) -> str:
    """Per-run event-buffer metadata (HSET counter)."""
    return f"workflow:events:meta:{thread_id}:{run_id}"


# ========== Shared Helpers (DRY) ==========


async def iter_subagent_events_full(
    thread_id: str, task
) -> AsyncIterator[dict]:
    """Yield every captured record for a subagent in seq order."""
    if task is None or not thread_id:
        return

    high_water = int(getattr(task, "captured_event_seq", 0) or 0)
    if high_water <= 0:
        return

    try:
        cache = get_cache_client()
    except Exception as exc:
        logger.warning(
            "[SubagentCollector] Failed to obtain cache client for "
            f"task {getattr(task, 'task_id', '?')}: {exc}"
        )
        return
    if cache is None or not getattr(cache, "enabled", False) or cache.client is None:
        return

    sa_stream_key = f"subagent:stream:{thread_id}:{task.task_id}"
    try:
        entries = await cache.client.xrange(sa_stream_key, min="-", max="+")
    except Exception as exc:
        logger.warning(
            f"[SubagentCollector] XRANGE failed for {sa_stream_key}: {exc}"
        )
        return

    yielded = 0
    for entry_id, fields in entries or []:
        try:
            seq_part = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id
            seq = int(seq_part.split("-", 1)[0])
        except (ValueError, AttributeError):
            continue
        if seq <= 0 or seq > high_water:
            continue
        raw = fields.get(b"record")
        if raw is None:
            continue
        try:
            payload = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            record = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        yielded += 1
        yield record

    expected = high_water
    if yielded < expected:
        logger.warning(
            "subagent_history_truncated",
            extra={
                "thread_id": thread_id,
                "task_id": getattr(task, "task_id", None),
                "expected": expected,
                "recovered": yielded,
                "missing": expected - yielded,
                "redis_write_failed": bool(getattr(task, "redis_write_failed", False)),
            },
        )


def _record_to_persist_event(record: dict, thread_id: str) -> dict:
    """Convert a captured-event record to persistence shape ``{event, data}``."""
    data = dict(record.get("data") or {})
    data["thread_id"] = thread_id
    out: dict = {
        "event": record.get("event"),
        "data": data,
    }
    ts = record.get("ts")
    if ts is not None:
        out["ts"] = ts
    return out


class TaskStatus(str, Enum):
    """Background task execution status."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskInfo:
    """Information about a background workflow task."""
    thread_id: str
    run_id: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_access_at: datetime = field(default_factory=datetime.now)

    task: Optional[asyncio.Task] = None
    inner_task: Optional[asyncio.Task] = None
    error: Optional[str] = None

    explicit_cancel: bool = False
    # True only when the user pressed Stop (HTTP /cancel). System cancels
    # (graceful shutdown, stale-sandbox recovery) set ``explicit_cancel`` for
    # the flush+teardown gate but leave this False so they are NOT persisted as
    # user-cancelled "Stopped" turns. See ``_mark_cancelled``.
    user_stop: bool = False
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    final_result: Optional[Any] = None

    active_connections: int = 0

    metadata: Dict[str, Any] = field(default_factory=dict)

    completion_callback: Optional[Callable[["TaskInfo"], Coroutine[Any, Any, None]]] = None

    persistence_complete: asyncio.Event = field(default_factory=asyncio.Event)

    graph: Optional[Any] = None


# Type alias for the key used throughout the manager.
TaskKey = tuple[str, str]


class BackgroundTaskManager:
    """Manages background workflow task execution.

    Singleton. State keyed by ``(thread_id, run_id)`` — each POST gets a
    fresh ``run_id`` so concurrent turns on the same thread are isolated
    by construction.
    """

    _instance: Optional['BackgroundTaskManager'] = None

    def __init__(self):
        # Keyed by (thread_id, run_id). One slot per turn; no cross-turn
        # aliasing because run_id is fresh per POST.
        self.tasks: Dict[TaskKey, TaskInfo] = {}
        self.task_lock = asyncio.Lock()
        # Per-thread admission locks remain thread-keyed: admission policy
        # (wait_or_steer / one foreground turn at a time) is a thread-level
        # invariant, independent of the per-turn key.
        self._admission_locks: Dict[str, asyncio.Lock] = {}

        # Configuration
        self.max_concurrent = get_max_concurrent_workflows()
        self.result_ttl = get_workflow_result_ttl()
        self.abandoned_timeout = get_abandoned_workflow_timeout()
        self.cleanup_interval = get_cleanup_interval()
        self.enable_storage = is_intermediate_storage_enabled()
        self.max_stored_messages = get_max_stored_messages_per_agent()

        self.event_storage_backend = get_event_storage_backend()
        self.redis_event_ttl = get_redis_ttl_workflow_events()

        self.cleanup_task: Optional[asyncio.Task] = None

        # Per-thread set of live orphan-collector tasks. Tracked so the stop
        # teardown can cancel any collector that would otherwise mutate the
        # persisted response after the user has stopped the turn.
        self._orphan_collectors: Dict[str, set[asyncio.Task]] = {}

    @classmethod
    def get_instance(cls) -> 'BackgroundTaskManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ---------- helpers ----------

    def _find_latest_for_thread(self, thread_id: str) -> Optional[TaskInfo]:
        """Return the most-recently-created TaskInfo for ``thread_id`` or None.

        Used for thread-scoped lookups (e.g., /status?thread_id=...) where
        the caller didn't provide a run_id.
        """
        best: Optional[TaskInfo] = None
        for (tid, _rid), info in self.tasks.items():
            if tid != thread_id:
                continue
            if best is None or info.created_at > best.created_at:
                best = info
        return best

    def _find_active_for_thread(
        self,
        thread_id: str,
        exclude_run_id: Optional[str] = None,
    ) -> Optional[TaskInfo]:
        """Return the most-recently-created active (non-terminal) TaskInfo.

        ``exclude_run_id`` skips a specific run — used by dispatched flows
        that want to check for OTHER active runs on the thread while
        ignoring their own pre-registered placeholder.
        """
        best: Optional[TaskInfo] = None
        live = (TaskStatus.QUEUED, TaskStatus.RUNNING)
        for (tid, rid), info in self.tasks.items():
            if tid != thread_id or info.status not in live:
                continue
            if exclude_run_id is not None and rid == exclude_run_id:
                continue
            if best is None or info.created_at > best.created_at:
                best = info
        return best

    async def has_active_tasks_for_workspace(self, workspace_id: str) -> bool:
        """Check if any active tasks exist for a workspace."""
        async with self.task_lock:
            active = (TaskStatus.RUNNING, TaskStatus.QUEUED)
            for info in self.tasks.values():
                if (
                    info.metadata.get("workspace_id") == workspace_id
                    and info.status in active
                ):
                    return True
        return False

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

    async def shutdown(self, timeout: float | None = None):
        """Gracefully shutdown background task manager."""
        if timeout is None:
            timeout = get_shutdown_timeout()
        logger.info("[BackgroundTaskManager] Starting graceful shutdown...")

        await self.stop_cleanup_task()

        async with self.task_lock:
            running_tasks = [
                (key, info)
                for key, info in self.tasks.items()
                if info.status in [TaskStatus.RUNNING, TaskStatus.QUEUED]
            ]

        if not running_tasks:
            logger.info("[BackgroundTaskManager] No running workflows to cancel")
            return

        logger.info(
            f"[BackgroundTaskManager] Cancelling {len(running_tasks)} running workflows"
        )

        for (thread_id, run_id), _info in running_tasks:
            # System shutdown, NOT a user stop: flush + kill subagents, but do
            # not persist the interrupted turn as a user-cancelled "Stopped".
            await self.cancel_workflow(thread_id, run_id, user_initiated=False)

        try:
            async with asyncio.timeout(timeout):
                for _key, info in running_tasks:
                    if info.task and not info.task.done():
                        try:
                            await info.task
                        except (asyncio.CancelledError, Exception):
                            pass
        except asyncio.TimeoutError:
            logger.warning(
                f"[BackgroundTaskManager] Shutdown timeout after {timeout}s, "
                f"forcing cancellation of stuck tasks"
            )
            stuck_tasks = []
            for key, info in running_tasks:
                if info.task and not info.task.done():
                    logger.warning(
                        f"[BackgroundTaskManager] Force-cancelling stuck task: {key}"
                    )
                    info.task.cancel()
                    stuck_tasks.append(info.task)
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

        to_remove: list[TaskKey] = []

        async with self.task_lock:
            for key, info in self.tasks.items():
                if info.status in [
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                ]:
                    if info.completed_at and info.completed_at < completed_threshold:
                        to_remove.append(key)
                        logger.info(
                            f"[BackgroundTaskManager] Cleanup: removing completed task "
                            f"{key} (age: {now - info.completed_at})"
                        )

                elif info.status == TaskStatus.RUNNING:
                    if info.active_connections == 0 and info.last_access_at < abandoned_threshold:
                        to_remove.append(key)
                        logger.warning(
                            f"[BackgroundTaskManager] Cleanup: removing abandoned task "
                            f"{key} (no connections for {now - info.last_access_at})"
                        )
                        if info.task and not info.task.done():
                            info.task.cancel()

                elif info.status == TaskStatus.QUEUED and info.task is None:
                    if info.created_at < abandoned_threshold:
                        to_remove.append(key)
                        logger.warning(
                            f"[BackgroundTaskManager] Cleanup: removing orphaned QUEUED "
                            f"placeholder {key} (age: {now - info.created_at})"
                        )

            for key in to_remove:
                del self.tasks[key]

            # Admission locks are NOT reclaimed here. ``get_admission_lock``
            # returns the Lock object under ``task_lock`` but the caller
            # then awaits ``acquire()`` outside the lock — if cleanup were
            # to delete the entry in that gap, a concurrent ``get_admission_lock``
            # would create a fresh Lock and both POSTs would acquire
            # different lock objects, defeating admission. The dict is
            # tiny (one entry per thread that has ever seen traffic);
            # leave it.

        if to_remove:
            logger.info(
                f"[BackgroundTaskManager] Cleaned up {len(to_remove)} tasks: {to_remove}"
            )

    async def get_admission_lock(self, thread_id: str) -> asyncio.Lock:
        """Return the per-thread admission lock, creating it on first use.

        Serializes ``wait_or_steer → persist_query_start → start_workflow``
        on a given thread so two simultaneous cold POSTs can't both pass
        ``wait_or_steer`` and race on the same ``turn_index``.
        """
        async with self.task_lock:
            lock = self._admission_locks.get(thread_id)
            if lock is None:
                lock = asyncio.Lock()
                self._admission_locks[thread_id] = lock
        return lock

    # ---------- workflow lifecycle ----------

    async def pre_register(
        self,
        thread_id: str,
        run_id: str,
    ) -> bool:
        """Pre-register a turn as QUEUED before the workflow generator starts.

        Used by background dispatch (X-Dispatch: background) to close the
        timing gap between dispatcher return and ``start_workflow``.
        Reconnecting clients see a QUEUED TaskInfo and attach to the per-run
        stream key (initially empty) instead of a 404.

        Returns True if the placeholder was created, False if a record for
        this exact ``(thread_id, run_id)`` already existed.
        """
        async with self.task_lock:
            key = (thread_id, run_id)
            if key in self.tasks:
                return False

            self.tasks[key] = TaskInfo(
                thread_id=thread_id,
                run_id=run_id,
                status=TaskStatus.QUEUED,
                created_at=datetime.now(),
            )
            logger.info(
                f"[BackgroundTaskManager] Pre-registered dispatch placeholder "
                f"for thread_id={thread_id} run_id={run_id}"
            )
            return True

    async def start_workflow(
        self,
        thread_id: str,
        run_id: str,
        workflow_generator: Any,
        metadata: Optional[Dict[str, Any]] = None,
        completion_callback: Optional[Callable[["TaskInfo"], Coroutine[Any, Any, None]]] = None,
        graph: Optional[Any] = None,
    ) -> TaskInfo:
        """Start a workflow as a background task."""
        key = (thread_id, run_id)
        async with self.task_lock:
            if key in self.tasks:
                existing = self.tasks[key]
                if existing.status == TaskStatus.QUEUED and existing.task is None:
                    # Upgrade pre-registered placeholder in-place.
                    existing.metadata = metadata or {}
                    existing.completion_callback = completion_callback
                    existing.graph = graph
                    existing.task = asyncio.create_task(
                        self._run_workflow(
                            thread_id, run_id, workflow_generator,
                            cancel_event=existing.cancel_event,
                        )
                    )
                    existing.status = TaskStatus.RUNNING
                    existing.started_at = datetime.now()
                    logger.info(
                        f"[BackgroundTaskManager] Upgraded pre-registered "
                        f"workflow thread_id={thread_id} run_id={run_id} to RUNNING"
                    )
                    return existing
                raise RuntimeError(
                    f"Workflow {key} already exists with status {existing.status}"
                )

            running_count = sum(
                1 for t in self.tasks.values()
                if t.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]
            )
            if running_count >= self.max_concurrent:
                raise ValueError(
                    f"Max concurrent workflows reached ({self.max_concurrent}). "
                    f"Currently running: {running_count}"
                )

            task_info = TaskInfo(
                thread_id=thread_id,
                run_id=run_id,
                status=TaskStatus.QUEUED,
                created_at=datetime.now(),
                metadata=metadata or {},
                completion_callback=completion_callback,
                graph=graph,
            )

            task_info.task = asyncio.create_task(
                self._run_workflow(
                    thread_id, run_id, workflow_generator,
                    cancel_event=task_info.cancel_event,
                )
            )
            task_info.status = TaskStatus.RUNNING
            task_info.started_at = datetime.now()

            self.tasks[key] = task_info

            logger.info(
                f"[BackgroundTaskManager] Started workflow thread_id={thread_id} "
                f"run_id={run_id} (running: {running_count + 1}/{self.max_concurrent})"
            )

            return task_info

    async def _run_workflow(
        self,
        thread_id: str,
        run_id: str,
        workflow_generator: Any,
        cancel_event: asyncio.Event,
    ):
        """Drive the workflow generator with cooperative + forced cancellation.

        Lifecycle is driven solely by ``cancel_event``; no SSE consumer holds a
        reference to this task post-Streams cutover, so disconnect cannot
        cascade and the inner task is awaited directly. A user stop force-cancels
        only ``inner_task`` (see ``cancel_workflow``), so the ``CancelledError``
        handler below runs in a non-cancelled context and can ``await`` the
        single-owner teardown.
        """
        key = (thread_id, run_id)
        try:
            async def consume_workflow(wf_gen):
                async for event in wf_gen:
                    if cancel_event.is_set():
                        with suppress(Exception):
                            await wf_gen.aclose()
                        raise asyncio.CancelledError("Explicitly cancelled by user")

                    if self.enable_storage:
                        await self._buffer_event_redis(thread_id, run_id, event)

            inner_task = asyncio.create_task(consume_workflow(workflow_generator))

            async with self.task_lock:
                task_info = self.tasks.get(key)
                if task_info:
                    task_info.inner_task = inner_task

            # A stop that landed before inner_task was published set cancel_event
            # but couldn't cancel the not-yet-created task; honor it now so a long
            # first step doesn't run to its next event boundary uncancelled.
            if cancel_event.is_set() and not inner_task.done():
                inner_task.cancel()

            await inner_task

            await self._mark_completed(thread_id, run_id)

        # =====================================================================
        # Single-owner stop teardown (decision 1A). On a user stop only
        # ``inner_task`` is force-cancelled, so this handler runs uncancelled
        # and owns the entire deterministic sequence:
        #
        #   except asyncio.CancelledError (consume_workflow):
        #     1. _flush_checkpoint(thread_id)        # if explicit_cancel
        #     2. drain killed-subagent events        # bounded (~stop_drain_timeout)
        #     3. cancel orphan collector tasks       # no post-stop mutation
        #     4. cancel_and_clear(force=True)        # kill subagents, wipe registry
        #     5. _mark_cancelled(thread_id)          # persist merged sse_events + SSE sentinel
        #     6. raise
        #
        # Drain MUST run before cancel_and_clear wipes the registry, and
        # cancel_and_clear must run before _mark_cancelled so the merged
        # subagent events are in place before persistence reads them.
        # =====================================================================
        except asyncio.CancelledError:
            async with self.task_lock:
                ti = self.tasks.get(key)
                explicit = bool(ti.explicit_cancel) if ti else False

            try:
                # NB: suppress(Exception) below catches flush/teardown FAILURES
                # only — NOT CancelledError (a BaseException). A second external
                # cancel landing mid-teardown still propagates to `finally`; the
                # asyncio.shield wrappers, not suppress, are what let these awaits
                # finish across that re-cancel. Don't drop a shield assuming
                # suppress already covers the cancellation case.
                if explicit:
                    # 1. Flush the LangGraph checkpoint so the next message
                    #    resumes from the last committed boundary. Gated on
                    #    explicit_cancel (set by the user stop, graceful
                    #    shutdown, and stale-sandbox recovery — all of which
                    #    cancel the INNER task, leaving this handler live to
                    #    flush). Abandoned-task cleanup cancels the OUTER task
                    #    with the flag unset and skips this. Best-effort: a
                    #    flush failure must not block persistence (step 5).
                    with suppress(Exception):
                        await asyncio.shield(self._flush_checkpoint(thread_id, run_id))

                    # 2-4. Drain killed-subagent events, cancel orphan
                    #      collectors, then kill subagents + wipe the registry.
                    #      Merged events are stashed on metadata so
                    #      _mark_cancelled persists them.
                    with suppress(Exception):
                        await asyncio.shield(
                            self._teardown_subagents_on_stop(thread_id, run_id)
                        )
            finally:
                # 5. Persist the cancellation. In a ``finally`` + ``shield`` so a
                #    SECOND cancel (graceful shutdown force-cancelling the OUTER
                #    task at its timeout, or abandoned cleanup) lands DURING
                #    teardown can't skip or tear a mid-write: burst-slot release,
                #    tracker status, and registry cleanup always run to
                #    completion rather than leaving half-state.
                await asyncio.shield(self._mark_cancelled(thread_id, run_id))
            raise

        except Exception as e:
            logger.error(
                f"[BackgroundTaskManager] Workflow {key} failed: {e}",
                exc_info=True
            )
            await self._mark_failed(thread_id, run_id, str(e))

    async def _flush_checkpoint(self, thread_id: str, run_id: str) -> None:
        """Force a checkpoint write for the current thread state on user stop.

        Persists state up to the last completed step so the next message
        resumes from it. The in-flight step is discarded and re-run on resume.
        """
        async with self.task_lock:
            task_info = self.tasks.get((thread_id, run_id))
            graph = task_info.graph if task_info else None

        if not graph:
            return

        config = {"configurable": {"thread_id": thread_id}}

        try:
            graph_any: Any = graph

            snapshot = await asyncio.wait_for(
                graph_any.aget_state(config), timeout=get_checkpoint_flush_timeout()
            )
            values = getattr(snapshot, "values", None)
            if not values:
                return

            await asyncio.wait_for(
                graph_any.aupdate_state(config, values), timeout=get_checkpoint_flush_timeout()
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

    def _track_orphan_collector(self, thread_id: str, task: asyncio.Task) -> None:
        """Register a live orphan-collector task for stop-time cancellation."""
        bucket = self._orphan_collectors.setdefault(thread_id, set())
        bucket.add(task)
        task.add_done_callback(lambda t: bucket.discard(t))

    async def _teardown_subagents_on_stop(self, thread_id: str, run_id: str) -> None:
        """Single-owner subagent teardown on a user stop.

        Order (decision 1A): drain killed-subagent events (bounded) → cancel
        orphan collectors → cancel_and_clear(force) → stash merged events on
        metadata for _mark_cancelled to persist. Drain MUST precede
        cancel_and_clear so the registry still holds the captured events.
        """
        from src.server.services.background_registry_store import BackgroundRegistryStore

        registry_store = BackgroundRegistryStore.get_instance()
        registry = await registry_store.get_registry(thread_id)

        # --- 2. Drain killed-subagent events (best-effort, hard timeout) ---
        merged_subagent_events: list[dict] = []
        if registry is not None:
            try:
                tasks = await registry.get_all_tasks()
            except Exception:
                tasks = []
            try:
                merged_subagent_events = await asyncio.wait_for(
                    self._drain_killed_subagent_events(thread_id, tasks),
                    timeout=get_stop_drain_timeout(),
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"[StopTeardown] Subagent drain exceeded "
                    f"{get_stop_drain_timeout()}s for thread_id={thread_id}; "
                    "proceeding without drained events"
                )
            except Exception as exc:
                logger.warning(
                    f"[StopTeardown] Subagent drain failed for "
                    f"thread_id={thread_id}: {exc}"
                )

        # --- 3. Cancel orphan collectors so they can't mutate the response ---
        collectors = list(self._orphan_collectors.get(thread_id, set()))
        for collector in collectors:
            if not collector.done():
                collector.cancel()
        if collectors:
            with suppress(Exception):
                await asyncio.gather(*collectors, return_exceptions=True)
        self._orphan_collectors.pop(thread_id, None)

        # --- 4. Kill subagents + wipe the registry ---
        with suppress(Exception):
            await registry_store.cancel_and_clear(thread_id, force=True)

        # Stash merged events for _mark_cancelled to fold into persisted sse_events.
        if merged_subagent_events:
            async with self.task_lock:
                ti = self.tasks.get((thread_id, run_id))
                if ti is not None:
                    ti.metadata["_stop_subagent_events"] = merged_subagent_events

    async def _drain_killed_subagent_events(
        self, thread_id: str, tasks: list
    ) -> list[dict]:
        """Synchronously read each task's already-captured events (non-blocking).

        Reads the in-memory tail + Redis spill and marks each killed subagent
        "stopped". No wait loop — never starts new work.
        """
        merged: list[dict] = []
        for task in tasks:
            if getattr(task, "captured_event_count", 0) <= 0:
                continue
            async for record in iter_subagent_events_full(thread_id, task):
                enriched = _record_to_persist_event(record, thread_id)
                merged.append(enriched)
            # Mark the killed subagent's stream "stopped" for replay.
            agent_id = f"task:{getattr(task, 'task_id', '')}"
            merged.append(
                {
                    "event": "message_chunk",
                    "data": {
                        "thread_id": thread_id,
                        "agent": agent_id,
                        "id": f"{agent_id}:stopped",
                        "role": "assistant",
                        "finish_reason": "stopped",
                    },
                }
            )
        return merged

    async def _buffer_event_redis(self, thread_id: str, run_id: str, event: str):
        """Append a workflow event to the per-run Redis Stream."""
        key = (thread_id, run_id)
        async with self.task_lock:
            if key not in self.tasks:
                return

        try:
            cache = get_cache_client()
        except Exception as e:
            logger.warning(
                f"[EventBuffer] get_cache_client() failed for {key}: {e}; dropping event"
            )
            return
        use_redis = self.event_storage_backend == "redis" and cache.enabled

        if not use_redis:
            logger.warning(
                f"[EventBuffer] Redis unavailable for {key}; "
                "consumers attached to workflow:stream:* will see no events"
            )
            return

        event_id = None
        try:
            first_line, _, _ = event.partition("\n")
            event_id = int(first_line.replace("id: ", "").strip())
        except (ValueError, IndexError):
            pass

        if event_id is None:
            logger.warning(
                "[EventBuffer] Could not parse event ID from SSE string for "
                f"{key}; event dropped"
            )
            return

        meta_k = stream_meta_key(thread_id, run_id)
        stream_k = stream_key(thread_id, run_id)

        success, seq = await cache.pipelined_event_buffer(
            meta_key=meta_k,
            event=event,
            max_size=self.max_stored_messages,
            ttl=self.redis_event_ttl,
            last_event_id=event_id,
            stream_key=stream_k,
        )

        if not success:
            logger.error(
                f"[EventBuffer] Redis pipeline failed for {key}; "
                "event dropped from workflow:stream:*"
            )
            return

        logger.debug(f"[EventBuffer] Buffered event to Redis: {key} (id={event_id}, seq={seq})")

        capacity_threshold = int(self.max_stored_messages * 0.9)
        if seq >= capacity_threshold and (seq - capacity_threshold) % 1000 == 0:
            logger.warning(
                f"[EventBuffer] Buffer near capacity for {key}: "
                f"{seq}/{self.max_stored_messages} events. "
                "Oldest events will be dropped (FIFO)."
            )

    # ========== Subagent collection ==========

    async def _collect_subagent_results_for_turn(
        self,
        thread_id: str,
        response_id: str,
        original_chunks: list[dict[str, Any]],
        tasks: list,
        workspace_id: str,
        user_id: str,
        timeout: float | None = None,
        is_byok: bool = False,
        sandbox=None,
    ) -> None:
        if timeout is None:
            timeout = get_subagent_collector_timeout()

        try:
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

            for task in tasks:
                if task.completed and task.captured_event_count > 0:
                    async for record in iter_subagent_events_full(thread_id, task):
                        enriched = _record_to_persist_event(record, thread_id)
                        all_subagent_events.append(enriched)

            pending = {
                t.asyncio_task: t for t in tasks
                if t.is_pending and t.asyncio_task
            }

            if all_subagent_events:
                await self._persist_collected_events(
                    main_chunks, all_subagent_events, response_id,
                    thread_id, workspace_id, user_id, sandbox=sandbox,
                )

            if not pending:
                await self._persist_subagent_usage(
                    response_id, tasks, thread_id, workspace_id, user_id,
                    is_byok=is_byok,
                )
                await self._await_drain_and_cleanup_tasks(tasks, thread_id)
                return

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

                    if task.captured_event_count > 0:
                        async for record in iter_subagent_events_full(thread_id, task):
                            enriched = _record_to_persist_event(record, thread_id)
                            all_subagent_events.append(enriched)

                if all_subagent_events:
                    await self._persist_collected_events(
                        main_chunks, all_subagent_events, response_id,
                        thread_id, workspace_id, user_id, sandbox=sandbox,
                    )

            if pending:
                orphaned_tasks = list(pending.values())
                logger.info(
                    f"[SubagentCollector] Spawning orphan collector for "
                    f"{len(orphaned_tasks)} timed-out task(s), thread_id={thread_id}"
                )
                orphan_task = asyncio.create_task(
                    self._collect_orphaned_subagent_results(
                        thread_id=thread_id,
                        response_id=response_id,
                        main_chunks=main_chunks,
                        prior_subagent_events=list(all_subagent_events),
                        tasks=orphaned_tasks,
                        workspace_id=workspace_id,
                        user_id=user_id,
                        is_byok=is_byok,
                        sandbox=sandbox,
                    ),
                    name=f"subagent-orphan-collector-{thread_id}",
                )
                self._track_orphan_collector(thread_id, orphan_task)

            collected_tasks = [t for t in tasks if t not in pending.values()]
            await self._persist_subagent_usage(
                response_id, collected_tasks, thread_id, workspace_id, user_id,
                is_byok=is_byok,
            )
            await self._await_drain_and_cleanup_tasks(collected_tasks, thread_id)

        except Exception as e:
            logger.error(
                f"[SubagentCollector] Turn collector failed for {thread_id}: {e}",
                exc_info=True,
            )

    async def _await_drain_and_cleanup_tasks(
        self, tasks: list, thread_id: str, timeout: float | None = None
    ) -> None:
        if timeout is None:
            timeout = get_sse_drain_timeout()

        async def _wait_one(event: "asyncio.Event") -> None:
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

        await asyncio.gather(*[_wait_one(t.sse_drain_complete) for t in tasks])

        try:
            cache = get_cache_client()
        except Exception as exc:
            cache = None
            logger.warning(
                f"[SubagentCleanup] Cache client unavailable during cleanup "
                f"for thread_id={thread_id}: {exc}"
            )

        # Look up the per-thread registry once so we can evict each task's
        # dict entry after its cleanup completes. Without this, _tasks grows
        # unboundedly across turns on a long-lived thread (every subagent
        # ever spawned stays referenced forever).
        from src.server.services.background_registry_store import BackgroundRegistryStore
        bg_registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)

        for task in tasks:
            task.per_call_records = []
            task.tool_usage = {}
            task.asyncio_task = None
            task.handler_task = None
            if cache is not None:
                try:
                    await cache.delete(
                        f"subagent:events:meta:{thread_id}:{task.task_id}"
                    )
                except Exception:
                    pass
                try:
                    await cache.delete(
                        f"subagent:stream:{thread_id}:{task.task_id}"
                    )
                except Exception:
                    pass
                try:
                    await cache.delete(
                        f"subagent:events:{thread_id}:{task.task_id}"
                    )
                except Exception:
                    pass
            logger.info(
                "task_heavy_refs_released",
                extra={
                    "thread_id": thread_id,
                    "task_id": task.task_id,
                    "tool_call_id": task.tool_call_id,
                    "captured_event_count": getattr(task, "captured_event_count", 0),
                    "captured_event_bytes": getattr(task, "captured_event_bytes", 0),
                    "redis_write_failed": getattr(task, "redis_write_failed", False),
                },
            )

            if bg_registry is not None:
                try:
                    await bg_registry.remove_task(task.tool_call_id)
                except Exception as exc:
                    logger.warning(
                        f"[SubagentCleanup] remove_task failed for "
                        f"thread_id={thread_id} task_id={task.task_id}: {exc}"
                    )

    async def _collect_orphaned_subagent_results(
        self,
        thread_id: str,
        response_id: str,
        main_chunks: list[dict[str, Any]],
        prior_subagent_events: list[dict],
        tasks: list,
        workspace_id: str,
        user_id: str,
        is_byok: bool = False,
        sandbox=None,
    ) -> None:
        idle_timeout = get_subagent_orphan_collector_timeout()
        poll_interval = min(30.0, idle_timeout)

        try:
            all_subagent_events = list(prior_subagent_events)

            for task in tasks:
                if not task.completed and task.asyncio_task and task.asyncio_task.done():
                    task.completed = True
                    try:
                        task.result = task.asyncio_task.result()
                    except Exception as e:
                        task.error = str(e)
                        task.result = {"success": False, "error": str(e)}

            pending = {
                t.asyncio_task: t for t in tasks
                if t.is_pending and t.asyncio_task
            }

            for task in tasks:
                if (
                    task.completed
                    and task.captured_event_count > 0
                    and task not in pending.values()
                ):
                    async for record in iter_subagent_events_full(thread_id, task):
                        enriched = _record_to_persist_event(record, thread_id)
                        all_subagent_events.append(enriched)

            if not pending:
                if all_subagent_events:
                    await self._persist_collected_events(
                        main_chunks, all_subagent_events, response_id,
                        thread_id, workspace_id, user_id, sandbox=sandbox,
                    )
                await self._persist_subagent_usage(
                    response_id, tasks, thread_id, workspace_id, user_id,
                    is_byok=is_byok,
                )
                await self._await_drain_and_cleanup_tasks(tasks, thread_id)
                logger.info(
                    f"[OrphanCollector] All tasks already completed for "
                    f"thread_id={thread_id}"
                )
                return

            logger.info(
                f"[OrphanCollector] Waiting for {len(pending)} task(s) with "
                f"{idle_timeout}s idle timeout, thread_id={thread_id}"
            )

            last_activity: dict[asyncio.Task, tuple[float, int]] = {
                at: (t.last_updated_at, t.captured_event_count)
                for at, t in pending.items()
            }
            last_progress_time = time.time()

            while pending:
                if time.time() - last_progress_time > idle_timeout:
                    logger.warning(
                        f"[OrphanCollector] Idle timeout ({idle_timeout}s) for "
                        f"thread_id={thread_id}, {len(pending)} tasks still pending"
                    )
                    break

                done, _ = await asyncio.wait(
                    pending.keys(),
                    timeout=poll_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if done:
                    last_progress_time = time.time()

                    for asyncio_task in done:
                        task = pending.pop(asyncio_task)
                        last_activity.pop(asyncio_task, None)
                        if not task.completed:
                            task.completed = True
                            try:
                                task.result = asyncio_task.result()
                            except Exception as e:
                                task.error = str(e)
                                task.result = {"success": False, "error": str(e)}

                        if task.captured_event_count > 0:
                            async for record in iter_subagent_events_full(thread_id, task):
                                enriched = _record_to_persist_event(record, thread_id)
                                all_subagent_events.append(enriched)

                        logger.info(
                            f"[OrphanCollector] {task.display_id} completed, "
                            f"persisting events for thread_id={thread_id}"
                        )

                    if all_subagent_events:
                        await self._persist_collected_events(
                            main_chunks, all_subagent_events, response_id,
                            thread_id, workspace_id, user_id, sandbox=sandbox,
                        )
                else:
                    for asyncio_task, task in pending.items():
                        prev_update, prev_events = last_activity.get(
                            asyncio_task, (0.0, 0)
                        )
                        cur_update = task.last_updated_at
                        cur_events = task.captured_event_count
                        if cur_update > prev_update or cur_events > prev_events:
                            last_progress_time = time.time()
                            last_activity[asyncio_task] = (cur_update, cur_events)

            if pending:
                for asyncio_task, task in pending.items():
                    task.collector_response_id = None
                    logger.warning(
                        f"[OrphanCollector] Giving up on idle task "
                        f"{task.display_id} for thread_id={thread_id} "
                        f"(no progress for {idle_timeout}s)"
                    )

            collected_tasks = [t for t in tasks if t not in pending.values()]
            if collected_tasks:
                await self._persist_subagent_usage(
                    response_id, collected_tasks, thread_id, workspace_id, user_id,
                    is_byok=is_byok,
                )
                await self._await_drain_and_cleanup_tasks(collected_tasks, thread_id)

        except Exception as e:
            logger.error(
                f"[OrphanCollector] Failed for thread_id={thread_id}: {e}",
                exc_info=True,
            )
            for task in tasks:
                if task.collector_response_id == response_id:
                    task.collector_response_id = None

    # ========== Terminal handlers ==========

    def _release_terminal_refs(
        self,
        thread_id: str,
        run_id: str,
    ) -> None:
        """Drop heavy in-process refs once a TaskInfo is in terminal state."""
        info = self.tasks.get((thread_id, run_id))
        if not info:
            return
        info.graph = None
        info.completion_callback = None
        if info.inner_task is not None and info.inner_task.done():
            info.inner_task = None
        info.metadata.pop("handler", None)
        info.metadata.pop("token_callback", None)
        info.metadata.pop("sandbox", None)
        info.metadata.pop("persistence_service", None)

    async def _mark_completed(self, thread_id: str, run_id: str):
        """Mark workflow as completed and notify live subscribers."""
        key = (thread_id, run_id)
        async with self.task_lock:
            task_info = self.tasks.get(key)
            if not task_info:
                return

            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = datetime.now()

            graph = task_info.graph
            metadata = task_info.metadata
            completion_callback = task_info.completion_callback

        is_interrupted = False
        try:
            if graph:
                snapshot = await asyncio.wait_for(
                    graph.aget_state({"configurable": {"thread_id": thread_id}}),
                    timeout=get_checkpoint_flush_timeout(),
                )
                if snapshot and snapshot.next:
                    is_interrupted = True
        except asyncio.TimeoutError:
            logger.error(
                f"[BackgroundTaskManager] aget_state timed out for {key} in _mark_completed"
            )
        except Exception as state_error:
            logger.warning(
                f"[BackgroundTaskManager] Could not check workflow state for {key}: {state_error}"
            )

        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if is_interrupted:
            if workspace_id and user_id:
                try:
                    from src.server.services.persistence.conversation import ConversationPersistenceService

                    persistence_service = metadata.get("persistence_service")
                    if persistence_service is None:
                        persistence_service = ConversationPersistenceService.get_instance(
                            thread_id, run_id,
                            workspace_id=workspace_id, user_id=user_id,
                        )
                    persistence_service._on_pair_persisted = (
                        lambda: self.clear_event_buffer(thread_id, run_id)
                    )

                    _, per_call_records = get_token_usage_from_callback(
                        metadata, "interrupt", thread_id
                    )
                    tool_usage = get_tool_usage_from_handler(
                        metadata, "interrupt", thread_id
                    )
                    sse_events = get_sse_events_from_handler(
                        metadata, "interrupt", thread_id
                    )

                    interrupt_reason = "plan_review_required"
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

                    execution_time = calculate_execution_time(metadata)

                    persist_metadata = {
                        "msg_type": metadata.get("msg_type"),
                        "stock_code": metadata.get("stock_code"),
                        "deepthinking": metadata.get("deepthinking", False),
                        "is_byok": metadata.get("is_byok", False),
                    }

                    await persistence_service.persist_interrupt(
                        interrupt_reason=interrupt_reason,
                        execution_time=execution_time,
                        metadata=persist_metadata,
                        per_call_records=per_call_records,
                        tool_usage=tool_usage,
                        sse_events=sse_events,
                    )
                    logger.info(f"[WorkflowPersistence] Workflow {key} paused for human feedback")

                    tracker = WorkflowTracker.get_instance()
                    await tracker.mark_interrupted(
                        thread_id=thread_id,
                        run_id=run_id,
                        metadata={"interrupt_reason": interrupt_reason},
                    )
                except Exception as persist_error:
                    logger.error(
                        f"[WorkflowPersistence] Failed to persist interrupt for {key}: {persist_error}",
                        exc_info=True,
                    )
        else:
            if completion_callback:
                try:
                    await completion_callback(task_info)
                except Exception as e:
                    logger.error(
                        f"[BackgroundTaskManager] Completion callback failed for {key}: {e}",
                        exc_info=True,
                    )
                    await self._mark_failed(
                        thread_id, run_id,
                        f"Completion callback failed: {str(e)}",
                    )
                    return

        # Spawn collector for subagent events. response_id == run_id by 1:1 contract.
        response_id = run_id

        from src.server.services.background_registry_store import BackgroundRegistryStore
        bg_store = BackgroundRegistryStore.get_instance()
        bg_registry = await bg_store.get_registry(thread_id)
        if bg_registry:
            tasks_to_collect = []
            # Hold the registry lock during claim so two concurrent collectors
            # (e.g., orphan from prior turn + current turn) can't both observe
            # collector_response_id is None for the same task and double-claim.
            async with bg_registry._lock:
                for t in bg_registry._tasks.values():
                    if t.collector_response_id:
                        continue
                    # Filter by spawned_run_id: only claim subagents spawned
                    # by THIS turn. None matches as a compat shim for tasks
                    # registered before run_id stamping shipped — remove the
                    # None branch in the next deploy.
                    if t.spawned_run_id is not None and t.spawned_run_id != run_id:
                        continue
                    if (
                        t.is_pending
                        or t.captured_event_count > 0
                        or t.per_call_records
                        or t.tool_usage
                    ):
                        t.collector_response_id = response_id
                        tasks_to_collect.append(t)
            if tasks_to_collect:
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
                            is_byok=metadata.get("is_byok", False),
                            sandbox=metadata.get("sandbox"),
                        ),
                        name=f"subagent-collector-{thread_id}-{run_id}-post-tail",
                    )

        if user_id:
            await release_burst_slot(user_id)

        task_info.persistence_complete.set()
        async with self.task_lock:
            self._release_terminal_refs(thread_id, run_id)

    async def wait_for_persistence(
        self, thread_id: str, run_id: str, timeout: float | None = None
    ) -> bool:
        """Wait until _mark_completed has finished persisting for the given turn.

        Captures the ``persistence_complete`` event reference under the lock
        so a concurrent admission deletion of the entry doesn't make us drop a
        still-pending wait on the floor.
        """
        if timeout is None:
            timeout = get_wait_for_persistence_timeout()
        async with self.task_lock:
            task_info = self.tasks.get((thread_id, run_id))
            event = task_info.persistence_complete if task_info else None
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(
                f"[BackgroundTaskManager] wait_for_persistence timed out for "
                f"thread_id={thread_id} run_id={run_id} after {timeout}s"
            )
            return False

    async def _mark_failed(
        self,
        thread_id: str,
        run_id: str,
        error: str,
    ):
        """Mark workflow as failed and notify live subscribers."""
        key = (thread_id, run_id)
        async with self.task_lock:
            task_info = self.tasks.get(key)
            if not task_info:
                return

            task_info.status = TaskStatus.FAILED
            task_info.completed_at = datetime.now()
            task_info.error = error
            metadata = task_info.metadata

        logger.error(
            f"[BackgroundTaskManager] Workflow {key} failed: {error}"
        )

        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if workspace_id and user_id:
            try:
                from src.server.services.persistence.conversation import ConversationPersistenceService

                persistence_service = metadata.get("persistence_service")
                if persistence_service is None:
                    persistence_service = ConversationPersistenceService.get_instance(
                        thread_id, run_id,
                        workspace_id=workspace_id, user_id=user_id,
                    )
                persistence_service._on_pair_persisted = (
                    lambda: self.clear_event_buffer(thread_id, run_id)
                )

                execution_time = calculate_execution_time(metadata)
                _, per_call_records = get_token_usage_from_callback(
                    metadata, "error", thread_id
                )
                tool_usage = get_tool_usage_from_handler(
                    metadata, "error", thread_id
                )
                sse_events = get_sse_events_from_handler(
                    metadata, "error", thread_id
                )

                persist_metadata = {
                    "msg_type": metadata.get("msg_type"),
                    "stock_code": metadata.get("stock_code"),
                    "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
                    "deepthinking": metadata.get("deepthinking", False),
                    "is_byok": metadata.get("is_byok", False),
                }

                await persistence_service.persist_error(
                    error_message=error,
                    errors=[error],
                    execution_time=execution_time,
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events,
                    metadata=persist_metadata,
                )
                logger.info(f"[WorkflowPersistence] Error persisted for {key}")
            except Exception as persist_error:
                logger.error(
                    f"[WorkflowPersistence] Failed to persist error for {key}: {persist_error}",
                    exc_info=True,
                )

        if user_id:
            await release_burst_slot(user_id)

        try:
            tracker = WorkflowTracker.get_instance()
            await tracker.mark_failed(thread_id, error=error, run_id=run_id)
        except Exception as tracker_err:
            logger.warning(
                f"[BackgroundTaskManager] tracker.mark_failed failed for {key}: {tracker_err}"
            )

        task_info.persistence_complete.set()
        async with self.task_lock:
            self._release_terminal_refs(thread_id, run_id)

    async def _persist_collected_events(
        self,
        main_chunks: list[dict],
        subagent_events: list[dict],
        response_id: str,
        thread_id: str,
        workspace_id: str,
        user_id: str,
        sandbox=None,
    ) -> None:
        """Clean and persist main + subagent events to DB."""
        import copy

        cleaned = []
        for event in subagent_events:
            e = copy.deepcopy(event)
            e.pop("ts", None)
            cleaned.append(e)

        updated_chunks = main_chunks + cleaned

        if sandbox:
            try:
                from src.server.services.persistence.image_capture import (
                    capture_and_rewrite_images,
                )

                await capture_and_rewrite_images(
                    updated_chunks, sandbox, thread_id=thread_id,
                )
            except Exception:
                logger.warning(
                    "[IMAGE_CAPTURE] Hook B failed", exc_info=True,
                )

        # Direct DB update — we know the response_id, no need to go through
        # the persistence-service singleton (which would key by run_id and
        # might not match a subagent collector running across turns).
        from src.server.database import conversation as qr_db
        try:
            await qr_db.update_sse_events(
                conversation_response_id=response_id,
                sse_events=updated_chunks,
            )
            logger.info(
                f"[SubagentCollector] Updated sse_events for "
                f"response_id={response_id} ({len(updated_chunks)} events)"
            )
        except Exception as e:
            logger.error(
                f"[SubagentCollector] Failed to update sse_events "
                f"response_id={response_id}: {e}",
                exc_info=True,
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
        """Persist each subagent's token usage as a separate row with msg_type='task'."""
        from src.server.services.persistence.usage import UsagePersistenceService
        from src.server.services.background_registry_store import BackgroundRegistryStore

        # Snapshot-and-clear usage under the registry lock, gated on still
        # owning the task (collector_response_id == response_id). A resume
        # clears that field, so a stale collector that re-claimed the same task
        # at turn-N end skips here while turn-N+1's collector bills the merged
        # usage exactly once — no double-persist across the resume window.
        bg_registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)

        def _claim_owned_usage() -> list[tuple[Any, list, dict]]:
            out: list[tuple[Any, list, dict]] = []
            for task in tasks:
                if task.collector_response_id != response_id:
                    continue
                if not (task.per_call_records or task.tool_usage):
                    continue
                records = task.per_call_records
                tool_usage = task.tool_usage
                task.per_call_records = []
                task.tool_usage = {}
                out.append((task, records, tool_usage))
            return out

        if bg_registry is not None:
            async with bg_registry._lock:
                claimed = _claim_owned_usage()
        else:
            # Registry gone (thread teardown) — tasks still carry their claim,
            # and the claim body has no awaits, so it's atomic without the lock.
            claimed = _claim_owned_usage()

        if not claimed:
            return

        persisted_count = 0
        persisted_records = 0

        for task, records, tool_usage in claimed:
            try:
                usage_service = UsagePersistenceService(
                    thread_id=thread_id,
                    workspace_id=workspace_id,
                    user_id=user_id,
                )
                await usage_service.track_llm_usage(records)

                if tool_usage:
                    usage_service.record_tool_usage_batch(tool_usage)

                # track_llm_usage([]) initializes _token_usage to a zeroed
                # dict, so tool-only tasks still get stamped; None only on its
                # internal cost-calculation error path, where skipping is the
                # documented is_byok fallback contract.
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
                persisted_records += len(records)

            except Exception as e:
                logger.error(
                    f"[SubagentUsage] Failed to persist usage for task {task.task_id} "
                    f"in thread_id={thread_id}: {e}",
                    exc_info=True,
                )

        if persisted_count:
            logger.info(
                f"[SubagentUsage] Persisted {persisted_count} subagent usage row(s) "
                f"({persisted_records} LLM calls) for response_id={response_id} "
                f"thread_id={thread_id}"
            )

    async def _mark_cancelled(self, thread_id: str, run_id: str):
        """Mark workflow as cancelled and notify live subscribers."""
        key = (thread_id, run_id)
        async with self.task_lock:
            task_info = self.tasks.get(key)
            if not task_info:
                return

            task_info.status = TaskStatus.CANCELLED
            task_info.completed_at = datetime.now()
            metadata = task_info.metadata
            # Persist as a user action ONLY for a user-pressed Stop. Both user
            # stops and system cancels (graceful shutdown via cancel_workflow,
            # stale-sandbox recovery via cancel_stale_workflow) set
            # ``explicit_cancel``, so keying off that would mislabel a pod-roll
            # or workspace eviction as a user "Stopped" turn. ``user_stop`` is
            # set only by the HTTP /cancel path.
            cancelled_by_user = bool(task_info.user_stop)

        logger.debug(f"[BackgroundTaskManager] Marked as cancelled: {key}")

        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        if workspace_id and user_id:
            try:
                from src.server.services.persistence.conversation import ConversationPersistenceService

                persistence_service = metadata.get("persistence_service")
                if persistence_service is None:
                    persistence_service = ConversationPersistenceService.get_instance(
                        thread_id, run_id,
                        workspace_id=workspace_id, user_id=user_id,
                    )
                persistence_service._on_pair_persisted = (
                    lambda: self.clear_event_buffer(thread_id, run_id)
                )

                _, per_call_records = get_token_usage_from_callback(
                    metadata, "cancellation", thread_id
                )
                tool_usage = get_tool_usage_from_handler(
                    metadata, "cancellation", thread_id
                )
                # Reconcile the transcript at the stop point: close any open
                # reasoning / tool-call / artifact / message structures so replay
                # doesn't render zombies. Only for user-driven stops; system
                # cancels (shutdown/abandoned) leave the raw events untouched.
                handler = metadata.get("handler")
                if cancelled_by_user and handler is not None and hasattr(
                    handler, "finalize_stopped_events"
                ):
                    try:
                        sse_events = handler.finalize_stopped_events()
                    except Exception as recon_err:
                        logger.warning(
                            f"[WorkflowPersistence] finalize_stopped_events failed "
                            f"for {key}: {recon_err}"
                        )
                        sse_events = get_sse_events_from_handler(
                            metadata, "cancellation", thread_id
                        )
                else:
                    sse_events = get_sse_events_from_handler(
                        metadata, "cancellation", thread_id
                    )
                # Fold in killed-subagent events drained during teardown.
                stop_subagent_events = metadata.get("_stop_subagent_events")
                if stop_subagent_events:
                    sse_events = (sse_events or []) + stop_subagent_events
                execution_time = calculate_execution_time(metadata)

                persist_metadata = {
                    "msg_type": metadata.get("msg_type"),
                    "stock_code": metadata.get("stock_code"),
                    "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
                    "deepthinking": metadata.get("deepthinking", False),
                    "is_byok": metadata.get("is_byok", False),
                    "cancelled_by_user": cancelled_by_user,
                }

                await persistence_service.persist_cancelled(
                    execution_time=execution_time,
                    metadata=persist_metadata,
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events,
                )
                logger.info(f"[WorkflowPersistence] Cancellation persisted for {key}")
            except Exception as persist_error:
                logger.error(
                    f"[WorkflowPersistence] Failed to persist cancellation for {key}: {persist_error}",
                    exc_info=True,
                )

        if user_id:
            await release_burst_slot(user_id)

        try:
            tracker = WorkflowTracker.get_instance()
            await tracker.mark_cancelled(thread_id, run_id=run_id)
        except Exception as tracker_err:
            logger.warning(
                f"[BackgroundTaskManager] tracker.mark_cancelled failed for {key}: {tracker_err}"
            )

        task_info.persistence_complete.set()
        async with self.task_lock:
            self._release_terminal_refs(thread_id, run_id)

    # ---------- status & introspection ----------

    async def get_task_status(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> Optional[TaskStatus]:
        """Get status for a specific run, or latest run on thread if ``run_id`` omitted."""
        async with self.task_lock:
            if run_id is not None:
                task_info = self.tasks.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            return task_info.status if task_info else None

    async def get_task_info(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> Optional[TaskInfo]:
        """Get full task info for a specific run, or latest on thread."""
        async with self.task_lock:
            if run_id is not None:
                task_info = self.tasks.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.last_access_at = datetime.now()
            return task_info

    async def increment_connection(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> bool:
        async with self.task_lock:
            if run_id is not None:
                task_info = self.tasks.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.active_connections += 1
                task_info.last_access_at = datetime.now()
                return True
            return False

    async def decrement_connection(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> bool:
        async with self.task_lock:
            if run_id is not None:
                task_info = self.tasks.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.active_connections = max(0, task_info.active_connections - 1)
                return True
            return False

    async def clear_event_buffer(self, thread_id: str, run_id: str):
        """Drop the per-run workflow event keys after persistence.

        Per-run keying makes this trivially safe: a concurrent new POST gets
        a different ``run_id`` and therefore different keys, so this DEL can
        never wipe an in-flight workflow's live stream.
        """
        try:
            cache = get_cache_client()

            if self.event_storage_backend == "redis" and cache.enabled:
                stream_k = stream_key(thread_id, run_id)
                meta_k = stream_meta_key(thread_id, run_id)

                await cache.delete(stream_k)
                await cache.delete(meta_k)

                logger.debug(
                    f"[EventBuffer] Cleared Redis event buffer for "
                    f"thread_id={thread_id} run_id={run_id}"
                )
        except Exception as e:
            logger.error(
                f"[EventBuffer] Error clearing event buffer for "
                f"thread_id={thread_id} run_id={run_id}: {e}",
                exc_info=True,
            )

    async def cancel_workflow(
        self, thread_id: str, run_id: Optional[str] = None,
        *, user_initiated: bool = True,
    ) -> bool:
        """Cancel a running workflow immediately.

        ``user_initiated`` distinguishes a user pressing Stop (HTTP /cancel,
        the default) from a system-driven cancel (graceful shutdown). Only
        user stops are persisted as cancelled-by-user "Stopped" turns.

        ``run_id`` may be omitted — falls back to the most recent active
        run on the thread.

        Sets the cooperative cancel flag AND force-cancels the in-flight
        ``inner_task`` so a long LLM/tool/sandbox step is interrupted now
        rather than at the next event boundary. Only ``inner_task`` is
        cancelled (mirroring ``cancel_stale_workflow``), so the outer task's
        ``except asyncio.CancelledError`` teardown runs uncancelled and can
        flush + persist. Does NOT block the HTTP response on exit.
        """
        async with self.task_lock:
            if run_id is not None:
                task_info = self.tasks.get((thread_id, run_id))
            else:
                task_info = self._find_active_for_thread(thread_id)

            if not task_info:
                logger.warning(
                    f"[BackgroundTaskManager] Cannot cancel "
                    f"thread_id={thread_id} run_id={run_id}: workflow not found"
                )
                return False

            if task_info.status not in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                logger.info(
                    f"[BackgroundTaskManager] Cannot cancel "
                    f"thread_id={thread_id} run_id={task_info.run_id}: "
                    f"status={task_info.status}"
                )
                return False

            task_info.cancel_event.set()
            task_info.explicit_cancel = True
            # Only ever raise user_stop; never let a later system cancel
            # (user_initiated=False, e.g. graceful shutdown) downgrade a turn the
            # user explicitly stopped — that would mislabel it as system-cancelled.
            if user_initiated:
                task_info.user_stop = True
            if task_info.inner_task and not task_info.inner_task.done():
                task_info.inner_task.cancel()
            logger.debug(
                f"[BackgroundTaskManager] Cancellation signaled: "
                f"thread_id={thread_id} run_id={task_info.run_id}"
            )
            return True

    async def cancel_stale_workflow(
        self, thread_id: str, timeout: float = 10.0
    ) -> bool:
        """Cancel a stale workflow on the given thread."""
        async with self.task_lock:
            task_info = self._find_active_for_thread(thread_id)
            if not task_info:
                return False

            task_info.cancel_event.set()
            task_info.explicit_cancel = True

            if task_info.inner_task and not task_info.inner_task.done():
                task_info.inner_task.cancel()
            stale_task = task_info.task

        if stale_task and not stale_task.done():
            done, _ = await asyncio.wait({stale_task}, timeout=timeout)
            if not done:
                logger.warning(
                    f"[BackgroundTaskManager] Stale workflow thread_id={thread_id} "
                    f"did not exit within {timeout}s"
                )
        return True

    async def get_workflow_status(self, thread_id: str) -> Dict[str, Any]:
        """Get detailed status for the latest run on a thread."""
        async with self.task_lock:
            task_info = self._find_latest_for_thread(thread_id)
            if not task_info:
                return {
                    "status": "not_found",
                    "thread_id": thread_id,
                }
            # Snapshot under the lock, then release it BEFORE the registry/Redis
            # lookup below. Holding task_lock across that await would let a slow
            # registry path block /cancel from acquiring the lock to signal a stop.
            status = task_info.status.value
            run_id = task_info.run_id
            created_at = task_info.created_at
            started_at = task_info.started_at
            completed_at = task_info.completed_at
            active_connections = task_info.active_connections

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
            "status": status,
            "thread_id": thread_id,
            "run_id": run_id,
            "active_tasks": active_tasks,
            "created_at": created_at.isoformat() if created_at else None,
            "started_at": started_at.isoformat() if started_at else None,
            "completed_at": completed_at.isoformat() if completed_at else None,
            "active_connections": active_connections,
        }

    async def wait_for_admission(
        self,
        thread_id: str,
        exclude_run_id: Optional[str] = None,
    ) -> Literal["fresh", "running", "stopping"]:
        """Decide whether a new turn can start on ``thread_id``.

        Returns one of:
        - ``"fresh"``  — no active task (or a cancelled one finished winding
          down within the wait): start a new turn.
        - ``"running"`` — a turn is genuinely running: the caller should steer
          it (or 409 if steering fails).
        - ``"stopping"`` — a turn was explicitly cancelled and is still tearing
          down past the wait: 409 "stopping, retry" (never start a second
          writer while ``_flush_checkpoint`` may still be writing this thread).

        ``exclude_run_id`` lets dispatched callers ignore their own
        pre-registered placeholder while checking for OTHER active runs.
        """
        async with self.task_lock:
            task_info = self._find_active_for_thread(
                thread_id, exclude_run_id=exclude_run_id
            )
            if not task_info:
                return "fresh"
            if task_info.status not in (TaskStatus.QUEUED, TaskStatus.RUNNING):
                return "fresh"
            explicit = bool(task_info.explicit_cancel)
            task = task_info.task
            key = (task_info.thread_id, task_info.run_id)

        if not explicit:
            # Genuinely running — caller routes to steering.
            return "running"

        if task is None:
            return "fresh"

        # Explicitly cancelled and winding down. NEVER bare-await the task: it
        # ends via ``raise CancelledError``, and a bare await would re-raise
        # that into this (new) request handler. ``asyncio.wait`` swallows the
        # task's exception so the caller is unaffected.
        timeout = get_checkpoint_flush_timeout() + 2.0
        logger.info(
            f"[BackgroundTaskManager] Waiting for stopping workflow {key} "
            f"to finish teardown (timeout={timeout}s)"
        )
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if done:
            async with self.task_lock:
                ti = self.tasks.get(key)
                if ti and ti.status in (TaskStatus.CANCELLED, TaskStatus.COMPLETED):
                    del self.tasks[key]
            return "fresh"

        logger.warning(
            f"[BackgroundTaskManager] Stopping workflow {key} still tearing "
            f"down after {timeout}s; rejecting new turn with 409"
        )
        return "stopping"

    async def get_stats(self) -> Dict[str, Any]:
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
                ),
            }
