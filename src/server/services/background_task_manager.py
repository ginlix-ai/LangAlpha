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
``wait_or_steer → start_turn → start_workflow`` window because
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
    get_admission_compaction_wait_timeout,
    get_compaction_timeout,
    get_sse_drain_timeout,
    get_wait_for_persistence_timeout,
    get_stop_drain_timeout,
    get_subagent_collector_timeout,
    get_subagent_orphan_collector_timeout,
)
from src.utils.cache.redis_cache import get_cache_client
from src.server.dependencies.usage_limits import release_burst_slot
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


# Terminal sentinel written to the per-run workflow Stream when the run's
# event forwarding ends (mirrors SUBAGENT_STREAM_END_EVENT). Consumers close
# on sight instead of waiting out the empty-XREAD terminal handshake.
WORKFLOW_STREAM_END_EVENT = "workflow_stream_end"
# Visible end-of-run frame (I6): written only after the finalize CAS commits,
# carrying {thread_id, run_id, outcome}. Replaces the swallowed pre-finalize
# sentinel above, which survives in the consumer as a legacy swallow only.
WORKFLOW_RUN_END_EVENT = "run_end"


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


class TransportLostError(RuntimeError):
    """Mid-run event-buffer failure — fatal to the run (I6).

    Raised by the Redis buffering path; the workflow failure handler turns it
    into a ``failed(transport_lost)`` finalize instead of letting the run
    complete with silently missing events.
    """


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
    # user-cancelled "Stopped" turns. See ``_finalize_run``.
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

    # Margin added to the checkpoint-flush timeout when a new turn waits for a
    # stopping turn's teardown to finish. Teardown does more than flush (subagent
    # drain, registry clear, persist), so the wait must outlast the flush alone;
    # past it, admission returns "stopping" → 409 retry rather than racing a
    # second checkpoint writer.
    _ADMISSION_TEARDOWN_MARGIN_S = 2.0

    # Admission floors its compaction wait at compaction_timeout + this margin so
    # a healthy in-progress compaction is never 409'd before its own call budget
    # self-terminates. The margin covers the compaction's post-LLM work (state
    # write + persistence) and the except-handler cleanup that finally sets the
    # guard's Event after the call returns or times out.
    _COMPACTION_ADMISSION_MARGIN_S = 20.0

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

        # Per-thread map of live orphan-collector tasks to their owning
        # run_id. Tracked so the stop teardown can cancel the stopped run's
        # collector that would otherwise mutate the persisted response after
        # the user has stopped the turn.
        self._orphan_collectors: Dict[str, dict[asyncio.Task, str]] = {}

        # Compaction/mutation admission gating is owned by ThreadMutationRunner
        # (v4 Phase 2.4): exclusive T(thread) + a Redis op-liveness key replace
        # the old in-memory _compacting/_compaction_tasks maps, so the guard
        # holds across workers, not just this process.

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
        ignoring their own already-registered run.
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

    async def is_run_live(self, thread_id: str, run_id: str) -> bool:
        """True while this exact run's workflow task (or inner task) is still
        executing. A live executor owns the ledger row, the tracker, and all
        terminal transport — callers must not finalize a live run."""
        async with self.task_lock:
            ti = self.tasks.get((thread_id, run_id))
            for t in (ti.task if ti else None, ti.inner_task if ti else None):
                if t is not None and not t.done():
                    return True
        return False

    async def has_active_task_for_thread(self, thread_id: str) -> bool:
        """True if any QUEUED/RUNNING task exists for the thread.

        Used by the /cancel safety net so it only wipes a thread's registry
        when nothing else owns it — a run-targeted cancel that misses (the run
        already tore down) must NOT clear a *different*, still-running turn's
        subagents.
        """
        async with self.task_lock:
            return self._find_active_for_thread(thread_id) is not None

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
        dead_handles: list = []

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
                        if info.task and not info.task.done():
                            # Cancel but KEEP the entry: the cancellation
                            # teardown's _finalize_run must still find this
                            # TaskInfo to settle the durable row. The entry
                            # leaves via the terminal-TTL branch above on a
                            # later pass. cancelling() gates re-cancels so a
                            # slow teardown isn't re-interrupted every sweep.
                            if not info.task.cancelling():
                                info.task.cancel()
                                logger.warning(
                                    f"[BackgroundTaskManager] Cleanup: cancelling "
                                    f"abandoned task {key} (no connections for "
                                    f"{now - info.last_access_at}); entry retained "
                                    f"until finalize"
                                )
                        else:
                            # Task object gone or settled without ever
                            # finalizing — nothing will settle the durable
                            # row from here; fail it open outside the lock.
                            to_remove.append(key)
                            handle = info.metadata.get("run_handle")
                            if handle is not None:
                                dead_handles.append(handle)
                            logger.warning(
                                f"[BackgroundTaskManager] Cleanup: removing dead "
                                f"RUNNING task {key} (task settled without finalize)"
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

        # Dead RUNNING entries whose task died without finalizing: settle the
        # durable row (outside the lock — fail_open_run does DB I/O).
        if dead_handles:
            from src.server.services.turn_lifecycle import TurnCoordinator

            for handle in dead_handles:
                await TurnCoordinator.get_instance().fail_open_run(
                    handle, "worker task died without finalizing (abandoned cleanup)"
                )

        if to_remove:
            logger.info(
                f"[BackgroundTaskManager] Cleaned up {len(to_remove)} tasks: {to_remove}"
            )

    async def get_admission_lock(self, thread_id: str) -> asyncio.Lock:
        """Return the per-thread admission lock, creating it on first use.

        Serializes ``wait_or_steer → start_turn → start_workflow``
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
                raise RuntimeError(
                    f"Workflow {key} already exists with status "
                    f"{self.tasks[key].status}"
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
                status=TaskStatus.RUNNING,
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
            task_info.started_at = datetime.now()

            self.tasks[key] = task_info

            logger.info(
                f"[BackgroundTaskManager] Started workflow thread_id={thread_id} "
                f"run_id={run_id} (running: {running_count + 1}/{self.max_concurrent})"
            )

            started = task_info

        run_handle = (metadata or {}).get("run_handle")

        # Handoff intent recheck: a /cancel between START and this registration
        # stamped durable intent on the row but found no TaskInfo to signal
        # (the QUEUED placeholder that used to fill this gap is gone — the
        # in_progress row committed by priming is the only pre-registration
        # identity). The row is the authority — re-derive the local signal
        # from it now that a task exists.
        if run_handle is not None:
            from src.server.database import turn_lifecycle as tl_db

            run_row = None
            try:
                run_row = await tl_db.get_run(run_handle.run_id)
            except Exception:
                logger.warning(
                    f"[BackgroundTaskManager] handoff cancel-intent recheck "
                    f"failed for {key}",
                    exc_info=True,
                )
            if (
                run_row
                and run_row.get("cancel_requested_at")
                and run_row.get("status") == "in_progress"
            ):
                logger.info(
                    f"[BackgroundTaskManager] durable cancel intent found at "
                    f"handoff for {key}; signalling cancel"
                )
                await self.cancel_workflow(thread_id, run_id)

        # I2: the guard monitor aborts the run on session loss before it can
        # act on a stale view — same force path as a user stop (cooperative
        # event + inner-task cancel), classified by the guard-lost downgrade.
        if (
            started is not None
            and run_handle is not None
            and run_handle.guard is not None
        ):

            def _abort_on_session_loss() -> None:
                info = self.tasks.get(key)
                if info is None:
                    return
                info.cancel_event.set()
                if info.inner_task is not None and not info.inner_task.done():
                    info.inner_task.cancel()

            run_handle.guard.attach_abort(_abort_on_session_loss)
        return started

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
                        try:
                            await self._buffer_event_redis(thread_id, run_id, event)
                        except TransportLostError:
                            # Fatal (I6): stop the graph at this event boundary
                            # so the failure handler finalizes
                            # failed(transport_lost) instead of the run
                            # completing with holes in its archive.
                            with suppress(Exception):
                                await wf_gen.aclose()
                            raise

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

            # No pre-finalize sentinel (1.5): the visible run_end frame is
            # written by _finalize_run AFTER the CAS commits, carrying the
            # adopted outcome. Consumers close on run_end, with the
            # two-empty-round handshake as the finalize-failure fallback.
            await self._finalize_run(thread_id, run_id, kind="stream_end")

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
        #     5. _finalize_run(kind="cancelled")     # persist merged sse_events + run_end frame
        #     6. raise
        #
        # Drain MUST run before cancel_and_clear wipes the registry, and
        # cancel_and_clear must run before _finalize_run so the merged
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
                #
                # No sentinel here (1.5): consumers close on the run_end frame
                # _finalize_run writes after the CAS in step 5.
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
                    #      _finalize_run persists them.
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
                await asyncio.shield(
                    self._finalize_run(thread_id, run_id, kind="cancelled")
                )
            raise

        except Exception as e:
            logger.error(
                f"[BackgroundTaskManager] Workflow {key} failed: {e}",
                exc_info=True
            )
            await self._finalize_run(thread_id, run_id, kind="failed", error=str(e))

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

            # Exclude `messages` from the re-write. The committed messages are
            # already in this snapshot and carry forward on the DeltaChannel, so
            # re-writing the full list only re-applies every message as a delta —
            # and any still-id-less tail message appends as a duplicate (the
            # reducer keys dedup on id). The remaining keys (private compaction /
            # offload state) are last-write-wins, so re-writing them is idempotent.
            flush_values = {k: v for k, v in values.items() if k != "messages"}
            if not flush_values:
                return

            await asyncio.wait_for(
                graph_any.aupdate_state(config, flush_values),
                timeout=get_checkpoint_flush_timeout(),
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

    def _track_orphan_collector(
        self, thread_id: str, run_id: str, task: asyncio.Task
    ) -> None:
        """Register a live orphan-collector task for stop-time cancellation,
        keyed by the run whose collection it continues.

        The done-callback discards the finished task and drops the per-thread
        bucket once it empties, so threads whose collectors complete naturally
        (turn ends without a user stop) don't leak empty sets on a long-lived
        server. The ``is bucket`` guard keeps a fresh bucket from a later turn
        on the same thread from being removed by this callback.
        """
        bucket = self._orphan_collectors.setdefault(thread_id, {})
        bucket[task] = run_id

        def _discard(t: asyncio.Task) -> None:
            bucket.pop(t, None)
            if not bucket and self._orphan_collectors.get(thread_id) is bucket:
                self._orphan_collectors.pop(thread_id, None)

        task.add_done_callback(_discard)

    async def _teardown_subagents_on_stop(self, thread_id: str, run_id: str) -> None:
        """Single-owner subagent teardown on a user stop — scoped to the
        stopped run.

        Order (decision 1A): drain killed-subagent events (bounded) → cancel
        this run's orphan collectors → cancel_run_tasks(force) → stash merged
        events on metadata for the finalize to persist. Drain MUST precede
        cancel_run_tasks so the registry still holds the captured events.
        Everything is keyed by ``spawned_run_id``: a prior turn's orphan
        collector persists to ITS OWN response, so stopping the current run
        must neither kill it nor archive its tasks' events here.
        """
        from src.server.services.background_registry_store import BackgroundRegistryStore

        registry_store = BackgroundRegistryStore.get_instance()
        registry = await registry_store.get_registry(thread_id)

        # --- 2. Drain killed-subagent events (best-effort, hard timeout) ---
        merged_subagent_events: list[dict] = []
        drain_timeout = get_stop_drain_timeout()
        if registry is not None:
            try:
                tasks = [
                    t
                    for t in await registry.get_all_tasks()
                    if getattr(t, "spawned_run_id", None) == run_id
                ]
            except Exception:
                tasks = []
            try:
                merged_subagent_events = await asyncio.wait_for(
                    self._drain_killed_subagent_events(thread_id, tasks),
                    timeout=drain_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"[StopTeardown] Subagent drain exceeded "
                    f"{drain_timeout}s for thread_id={thread_id}; "
                    "proceeding without drained events"
                )
            except Exception as exc:
                logger.warning(
                    f"[StopTeardown] Subagent drain failed for "
                    f"thread_id={thread_id}: {exc}"
                )

        # --- 3. Cancel THIS run's orphan collectors (normally none: a stopped
        # run never reached collection) so they can't mutate the response.
        # Prior turns' collectors keep running — they own other responses. ---
        bucket = self._orphan_collectors.get(thread_id, {})
        collectors = [t for t, owner in bucket.items() if owner == run_id]
        for collector in collectors:
            if not collector.done():
                collector.cancel()
        if collectors:
            with suppress(Exception):
                await asyncio.gather(*collectors, return_exceptions=True)
            for collector in collectors:
                bucket.pop(collector, None)

        # --- 4. Kill this run's subagents; the registry and prior-turn tasks
        # survive for their own collectors. ---
        with suppress(Exception):
            await registry_store.cancel_run_tasks(thread_id, run_id, force=True)

        # Stash merged events for _mark_cancelled to fold into persisted sse_events.
        if merged_subagent_events:
            async with self.task_lock:
                ti = self.tasks.get((thread_id, run_id))
                if ti is not None:
                    ti.metadata["_stop_subagent_events"] = merged_subagent_events

    async def _drain_killed_subagent_events(
        self, thread_id: str, tasks: list
    ) -> list[dict]:
        """Best-effort bounded snapshot of in-flight subagent events pre-teardown.

        Async-reads each subagent's in-memory tail + Redis spill via
        ``iter_subagent_events_full`` and appends a synthetic "stopped" close per
        task. Runs BEFORE ``cancel_and_clear`` (ordering at the teardown call
        site) so the registry is still intact; the caller bounds it with
        ``asyncio.wait_for``. Starts no new agent work — it only reads and closes.
        """
        merged: list[dict] = []
        for task in tasks:
            if getattr(task, "captured_event_count", 0) <= 0:
                continue
            # Track reasoning blocks left open at the kill point so we can close
            # them, mirroring the main agent's finalize_stopped_events. Keyed by
            # the subagent's own (agent, message id) so the synthetic close
            # matches the unpaired start exactly.
            open_reasoning: dict[tuple[str, str], None] = {}
            async for record in iter_subagent_events_full(thread_id, task):
                enriched = _record_to_persist_event(record, thread_id)
                merged.append(enriched)
                data = enriched.get("data") or {}
                if data.get("content_type") == "reasoning_signal":
                    rk = (data.get("agent", ""), data.get("id", ""))
                    if data.get("content") == "start":
                        open_reasoning[rk] = None
                    elif data.get("content") == "complete":
                        open_reasoning.pop(rk, None)
            # Close any reasoning block still open when the subagent was killed,
            # else replay renders the card stuck "thinking" indefinitely.
            for r_agent, r_id in open_reasoning:
                merged.append(
                    {
                        "event": "message_chunk",
                        "data": {
                            "thread_id": thread_id,
                            "agent": r_agent,
                            "id": r_id,
                            "role": "assistant",
                            "content": "complete",
                            "content_type": "reasoning_signal",
                        },
                    }
                )
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
        """Append a workflow event to the per-run Redis Stream.

        Buffer failure is FATAL to the run (I6): a dropped event means the
        replay archive and any attached consumer silently diverge from what
        the model actually produced, so the run must finalize
        ``failed(transport_lost)`` instead of completing with holes. Only the
        memory backend (explicitly configured, no stream consumers) keeps
        best-effort semantics.
        """
        key = (thread_id, run_id)
        async with self.task_lock:
            if key not in self.tasks:
                return

        if self.event_storage_backend != "redis":
            return  # memory backend: no stream transport to lose

        try:
            cache = get_cache_client()
        except Exception as e:
            raise TransportLostError(
                f"transport_lost: cache client unavailable ({e})"
            ) from e
        if not cache.enabled:
            raise TransportLostError(
                "transport_lost: Redis event transport is disabled/unreachable"
            )

        event_id = None
        try:
            first_line, _, _ = event.partition("\n")
            event_id = int(first_line.replace("id: ", "").strip())
        except (ValueError, IndexError):
            pass

        if event_id is None:
            raise TransportLostError(
                "transport_lost: unparsable event ID in SSE frame; replay "
                "archive would silently diverge"
            )

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
            raise TransportLostError(
                f"transport_lost: Redis pipeline write failed for {key}"
            )

        logger.debug(f"[EventBuffer] Buffered event to Redis: {key} (id={event_id}, seq={seq})")

        capacity_threshold = int(self.max_stored_messages * 0.9)
        if seq >= capacity_threshold and (seq - capacity_threshold) % 1000 == 0:
            logger.warning(
                f"[EventBuffer] Buffer near capacity for {key}: "
                f"{seq}/{self.max_stored_messages} events. "
                "Oldest events will be dropped (FIFO)."
            )

    async def append_run_end_event(
        self, thread_id: str, run_id: str, outcome: str
    ) -> None:
        """Write the visible ``run_end`` frame to the per-run Stream (I6).

        Written only AFTER the finalize CAS commits, carrying the ADOPTED
        terminal status — a consumer that sees ``run_end`` may trust the
        durable row exists with that outcome. Raw auto-ID XADD, not
        ``_buffer_event_redis``: it has no seq slot, and the auto ID (ms
        timestamp) always sorts after the explicit ``seq-0`` IDs real events
        use. Best-effort: on failure the consumer's two-empty-round terminal
        handshake still closes the stream, just slower.
        """
        if self.event_storage_backend != "redis":
            return
        try:
            cache = get_cache_client()
            if not cache.enabled or not cache.client:
                return
            # Atomic exactly-once gate: the owner (possibly alive but
            # fence-lost) and a recovery scanner can both reach this after
            # the same finalize CAS — SETNX picks one emitter, so the
            # stream never carries two run_end frames.
            gate_key = f"workflow:run_end_gate:{thread_id}:{run_id}"
            acquired = await cache.client.set(
                gate_key, "1", nx=True, ex=self.redis_event_ttl
            )
            if not acquired:
                return
            stream_k = stream_key(thread_id, run_id)
            data = json.dumps(
                {"thread_id": thread_id, "run_id": run_id, "outcome": outcome},
                ensure_ascii=False,
            )
            payload = f"event: {WORKFLOW_RUN_END_EVENT}\ndata: {data}\n\n".encode(
                "utf-8"
            )
            # A pipeline failure here is AMBIGUOUS (the XADD may have landed
            # with its reply lost) and the gate is deliberately NOT released
            # — even a tail recheck can't rule out an in-flight XADD landing
            # after it. At-most-once beats retryability: run_end is
            # best-effort by contract, and the consumer's two-empty-round
            # terminal handshake covers a missing frame.
            async with cache.client.pipeline(transaction=False) as pipe:
                pipe.xadd(
                    stream_k,
                    {b"event": payload},
                    maxlen=self.max_stored_messages,
                    approximate=True,
                )
                # Refresh TTL so a run_end landing on an expired/cleared
                # key can't recreate it without an expiry.
                pipe.expire(stream_k, self.redis_event_ttl)
                await pipe.execute()
        except Exception as exc:
            logger.debug(
                f"[EventBuffer] run_end append failed for "
                f"({thread_id}, {run_id}): {exc}"
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
                await self._enqueue_task_report_backs(
                    thread_id, response_id, tasks, workspace_id, user_id,
                    all_settled=True,
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
                self._track_orphan_collector(thread_id, response_id, orphan_task)

            collected_tasks = [t for t in tasks if t not in pending.values()]
            await self._persist_subagent_usage(
                response_id, collected_tasks, thread_id, workspace_id, user_id,
                is_byok=is_byok,
            )
            await self._enqueue_task_report_backs(
                thread_id, response_id, collected_tasks, workspace_id, user_id,
                all_settled=not pending,
            )
            await self._await_drain_and_cleanup_tasks(collected_tasks, thread_id)

        except Exception as e:
            logger.error(
                f"[SubagentCollector] Turn collector failed for {thread_id}: {e}",
                exc_info=True,
            )

    async def _enqueue_task_report_backs(
        self,
        thread_id: str,
        response_id: str,
        tasks: list,
        workspace_id: str,
        user_id: str,
        *,
        all_settled: bool,
    ) -> None:
        """Claim + enqueue report-back jobs BEFORE task cleanup evicts the
        registry entries. Never raises (the helper swallows its own errors)."""
        from src.server.handlers.chat.task_report_back import (
            enqueue_task_report_backs,
        )

        await enqueue_task_report_backs(
            thread_id=thread_id,
            response_id=response_id,
            tasks=tasks,
            workspace_id=workspace_id,
            user_id=user_id,
            all_settled=all_settled,
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
                await self._enqueue_task_report_backs(
                    thread_id, response_id, tasks, workspace_id, user_id,
                    all_settled=True,
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
                await self._enqueue_task_report_backs(
                    thread_id, response_id, collected_tasks, workspace_id, user_id,
                    all_settled=not pending,
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
        info.metadata.pop("run_handle", None)
        info.metadata.pop("artifact_hook", None)

    async def _finalize_run(
        self,
        thread_id: str,
        run_id: str,
        *,
        kind: Literal["stream_end", "cancelled", "failed"],
        error: Optional[str] = None,
    ):
        """Resolve the run's outcome in-band and drive the single finalize CAS.

        Replaces the pre-v4 ``_mark_completed``/``_mark_failed``/
        ``_mark_cancelled`` trio. Interrupted-vs-completed comes from the
        streaming handler's durability barrier (the timeout-prone
        ``aget_state`` probe survives only as a no-handler fallback), and the
        terminal write is ``TurnCoordinator.finalize_turn`` — one CAS
        transaction carrying usage rows with it, so a persist failure can no
        longer be swallowed into a zombie-ACTIVE turn.
        """
        from src.server.services.turn_lifecycle import TurnOutcome

        key = (thread_id, run_id)
        async with self.task_lock:
            task_info = self.tasks.get(key)
            if not task_info:
                return
            metadata = task_info.metadata
            cancelled_by_user = bool(task_info.user_stop)
            completion_callback = task_info.completion_callback
            graph = task_info.graph

        handle = metadata.get("run_handle")
        handler = metadata.get("handler")
        workspace_id = metadata.get("workspace_id")
        user_id = metadata.get("user_id")

        status, phase, interrupt_reason, error = await self._classify_outcome(
            kind, handler=handler, graph=graph, thread_id=thread_id, error=error
        )

        (
            execution_time,
            per_call_records,
            tool_usage,
            sse_events,
            persist_metadata,
        ) = await self._assemble_finalize_artifacts(
            key,
            metadata=metadata,
            status=status,
            phase=phase,
            handler=handler,
            cancelled_by_user=cancelled_by_user,
            workspace_id=workspace_id,
            user_id=user_id,
        )

        # ---- the single terminal transition ----
        # finalize_applied gates every terminal business effect below: losers
        # of the finalize race (and failed finalizes) do nothing. The
        # handle-less legacy path keeps its local effects so it can't wedge.
        finalize_applied = True
        survivor_status: Optional[str] = None
        if handle is not None:
            # I5: terminal hooks (burst release, report-back dispatch,
            # needs-input wake, watch clear) ride the finalize transaction
            # as durable outbox jobs — finalize_run derives them from the
            # row's START-stamped metadata, so no explicit factory here.
            outcome = TurnOutcome(
                status=status,
                interrupt_reason=interrupt_reason,
                metadata=persist_metadata,
                errors=[error] if error else None,
                execution_time=execution_time,
                sse_events=sse_events,
                per_call_records=per_call_records,
                tool_usage=tool_usage,
            )
            # I2 tail mode: subagent writers that survive the turn checkpoint
            # through the run's pinned session, so its release must wait for
            # them (N(root) drops at finalize either way).
            tail_drain = None
            if handle.guard is not None:

                async def tail_drain() -> None:
                    await self._drain_run_subagent_writers(thread_id, run_id)

            finalize_applied, status, survivor_status = (
                await self._drive_finalize_cas(
                    key, handle, outcome, tail_drain=tail_drain
                )
            )
        else:
            logger.error(
                f"[BackgroundTaskManager] no run_handle for {key}; durable "
                f"finalize skipped (legacy path?)"
            )

        # ---- local terminal mark (after the CAS: a losing finalize adopts
        # the survivor's status instead of relabeling the winner's) ----
        local_status = survivor_status or status
        async with self.task_lock:
            task_info.status = {
                "cancelled": TaskStatus.CANCELLED,
                "error": TaskStatus.FAILED,
            }.get(local_status, TaskStatus.COMPLETED)
            task_info.completed_at = datetime.now()
            if error:
                task_info.error = error

        if finalize_applied:
            await self._apply_post_terminal_effects(
                thread_id,
                run_id,
                kind=kind,
                status=status,
                task_info=task_info,
                completion_callback=completion_callback,
                execution_time=execution_time,
                interrupt_reason=interrupt_reason,
                error=error,
                metadata=metadata,
                workspace_id=workspace_id,
                user_id=user_id,
            )

        if handle is None and user_id:
            # Legacy fallback only: without a durable run row there are no
            # outbox jobs, so the burst slot must release inline.
            await release_burst_slot(user_id, metadata.get("burst_slot_id"))

        task_info.persistence_complete.set()
        async with self.task_lock:
            self._release_terminal_refs(thread_id, run_id)

    async def _classify_outcome(
        self,
        kind: Literal["stream_end", "cancelled", "failed"],
        *,
        handler,
        graph,
        thread_id: str,
        error: Optional[str],
    ) -> tuple[str, str, Optional[str], Optional[str]]:
        """In-band outcome classification -> (status, phase, interrupt_reason,
        error). Interrupted-vs-completed comes from the streaming handler's
        durability barrier; the timeout-prone aget_state probe survives only
        for handler-less runs."""
        interrupt_reason: Optional[str] = None
        if kind == "cancelled":
            status, phase = "cancelled", "cancellation"
        elif kind == "failed":
            status, phase = "error", "error"
        elif handler is not None and getattr(handler, "saw_interrupt", False):
            if handler.interrupt_verified:
                status, phase = "interrupted", "interrupt"
                interrupt_reason = handler.interrupt_reason or "plan_review_required"
            else:
                # I8: a pause that never reached the checkpointer must not
                # advertise resumability.
                status, phase = "error", "error"
                error = error or (
                    "interrupt_not_durable: the pause was not checkpointed"
                )
        elif handler is not None:
            status, phase = "completed", "completion"
        else:
            # Handler-less run (defensive): legacy state probe as fallback.
            status, phase = "completed", "completion"
            try:
                if graph:
                    snapshot = await asyncio.wait_for(
                        graph.aget_state({"configurable": {"thread_id": thread_id}}),
                        timeout=get_checkpoint_flush_timeout(),
                    )
                    if snapshot and snapshot.next:
                        status, phase = "interrupted", "interrupt"
                        interrupt_reason = "plan_review_required"
            except Exception:
                logger.warning(
                    f"[BackgroundTaskManager] fallback state probe failed for "
                    f"({thread_id}, ...)",
                    exc_info=True,
                )
        return status, phase, interrupt_reason, error

    async def _assemble_finalize_artifacts(
        self,
        key: tuple,
        *,
        metadata: dict,
        status: str,
        phase: str,
        handler,
        cancelled_by_user: bool,
        workspace_id: Optional[str],
        user_id: Optional[str],
    ) -> tuple:
        """Build everything the finalize CAS archives: usage records, the
        (possibly stop-reconciled) sse_events, and the persist metadata."""
        execution_time = calculate_execution_time(metadata)
        thread_id = key[0]
        _, per_call_records = get_token_usage_from_callback(metadata, phase, thread_id)
        tool_usage = get_tool_usage_from_handler(metadata, phase, thread_id)

        # User-pressed Stop reconciles the transcript (close open reasoning /
        # tool-call / artifact structures) so replay doesn't render zombies;
        # system cancels leave raw events untouched.
        sse_events = None
        if (
            status == "cancelled"
            and cancelled_by_user
            and handler is not None
            and hasattr(handler, "finalize_stopped_events")
        ):
            try:
                sse_events = handler.finalize_stopped_events()
            except Exception as recon_err:
                logger.warning(
                    f"[BackgroundTaskManager] finalize_stopped_events failed "
                    f"for {key}: {recon_err}"
                )
        if sse_events is None:
            sse_events = get_sse_events_from_handler(metadata, phase, thread_id)
        if status == "cancelled":
            stop_subagent_events = metadata.get("_stop_subagent_events")
            if stop_subagent_events:
                sse_events = (sse_events or []) + stop_subagent_events

        persist_metadata = {
            "msg_type": metadata.get("msg_type"),
            "stock_code": metadata.get("stock_code"),
            "agent_llm_preset": metadata.get("agent_llm_preset", "default"),
            "deepthinking": metadata.get("deepthinking", False),
            "is_byok": metadata.get("is_byok", False),
        }
        for extra in ("workspace_id", "sandbox_id", "locale", "timezone"):
            if metadata.get(extra):
                persist_metadata[extra] = metadata[extra]
        if status == "cancelled":
            persist_metadata["cancelled_by_user"] = cancelled_by_user
        # Steering inputs archive on the owning response (v4 identity model:
        # steering = no run, no turn). Replaces the old backfill that
        # fabricated query rows for orphan turn indexes.
        if handler is not None and getattr(handler, "injected_steerings", None):
            persist_metadata["steering_inputs"] = [
                m.get("content")
                for m in handler.injected_steerings
                if m.get("content")
            ]
        if not (workspace_id and user_id):
            # Usage rows need both; without them the ledger transition still
            # happens, billing artifacts are simply absent.
            per_call_records = None
            tool_usage = None

        # Pre-finalize artifact hook (sandbox image capture → storage URL
        # rewrite) must run before sse_events are archived.
        artifact_hook = metadata.get("artifact_hook")
        if status == "completed" and artifact_hook and sse_events:
            try:
                await artifact_hook(sse_events)
            except Exception:
                logger.warning(
                    f"[BackgroundTaskManager] artifact hook failed for {key}",
                    exc_info=True,
                )
        return execution_time, per_call_records, tool_usage, sse_events, persist_metadata

    async def _drive_finalize_cas(
        self, key: tuple, handle, outcome, *, tail_drain=None
    ) -> tuple[bool, str, Optional[str]]:
        """One finalize CAS -> (applied, adopted_status, survivor_status).

        Winners adopt the row's final status (durable cancel intent can flip
        it); losers report the survivor's. A failed persist leaves the row
        in_progress — honest and recoverable — never a masked terminal turn.
        """
        from src.server.services.turn_lifecycle import TurnCoordinator

        status = outcome.status
        try:
            # 1.5: no post_commit DEL — the stream is retained to its
            # redis_event_ttl so post-terminal reconnects replay from
            # Redis and see the run_end frame appended after the commit.
            result = await TurnCoordinator.get_instance().finalize_turn(
                handle,
                outcome,
                tail_drain=tail_drain,
            )
            if result.applied:
                final_status = (result.run or {}).get("status")
                if final_status and final_status != status:
                    logger.info(
                        f"[BackgroundTaskManager] finalize adopted durable "
                        f"cancel for {key}: {status} -> {final_status}"
                    )
                    status = final_status
                return True, status, None
            survivor_status = (result.run or {}).get("status")
            logger.warning(
                f"[BackgroundTaskManager] lost finalize race for {key}: "
                f"row already {survivor_status} (wanted {status}); "
                f"terminal side effects skipped"
            )
            return False, status, survivor_status
        except Exception:
            logger.critical(
                f"[BackgroundTaskManager] FINALIZE FAILED for {key}: run row "
                f"remains in_progress for recovery",
                exc_info=True,
            )
            return False, status, None

    # Legit tail subagents (deep research) run 15+ min; this only bounds how
    # long a HUNG writer can pin a budget slot before the teardown discards
    # the session out from under it.
    TAIL_DRAIN_TIMEOUT = 1800.0

    async def _drain_run_subagent_writers(self, thread_id: str, run_id: str) -> None:
        """Wait until none of this run's subagents can touch the checkpointer
        anymore — they write through the run's pinned session, so the guard
        holds until the last of their asyncio tasks settles. Collectors are
        not writers (they read Redis and persist via the app pool).

        Fail-closed: raises on registry failure or deadline, so the guard
        teardown discards the session instead of clean-releasing it under
        writers it could not account for. Re-snapshots after each wait to
        catch writers registered while earlier ones were draining."""
        from src.server.services.background_registry_store import (
            BackgroundRegistryStore,
        )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.TAIL_DRAIN_TIMEOUT
        while True:
            bg_registry = await BackgroundRegistryStore.get_instance().get_registry(
                thread_id
            )
            if not bg_registry:
                return
            writers: list[asyncio.Task] = []
            async with bg_registry._lock:
                for t in bg_registry._tasks.values():
                    if t.spawned_run_id is not None and t.spawned_run_id != run_id:
                        continue
                    for writer in (t.asyncio_task, getattr(t, "handler_task", None)):
                        if writer is not None and not writer.done():
                            writers.append(writer)
            if not writers:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"tail drain deadline: {len(writers)} subagent writer(s) "
                    f"still running for run={run_id} after "
                    f"{self.TAIL_DRAIN_TIMEOUT:.0f}s"
                )
            logger.info(
                f"[BackgroundTaskManager] guard tail-drain awaiting "
                f"{len(writers)} subagent writer(s) for run={run_id}"
            )
            await asyncio.wait(writers, timeout=remaining)

    async def _apply_post_terminal_effects(
        self,
        thread_id: str,
        run_id: str,
        *,
        kind: Literal["stream_end", "cancelled", "failed"],
        status: str,
        task_info,
        completion_callback,
        execution_time,
        interrupt_reason: Optional[str],
        error: Optional[str],
        metadata: dict,
        workspace_id: Optional[str],
        user_id: Optional[str],
    ) -> None:
        """Local effects after a WON finalize CAS (never touch run status
        again). Isolated: a side-effect failure must not propagate back into
        _run_workflow's failure handler and trigger a second finalize."""
        key = (thread_id, run_id)
        try:
            # Visible run_end AFTER the commit, carrying the adopted
            # status (I6). Attached consumers yield it and close. Losers
            # of a finalize race skip it — their consumers close via the
            # terminal handshake once the local mark lands.
            await self.append_run_end_event(thread_id, run_id, status)

            if status == "completed" and completion_callback:
                try:
                    await completion_callback(task_info)
                except Exception:
                    logger.error(
                        f"[BackgroundTaskManager] completion side-effect callback "
                        f"failed for {key} (run remains completed)",
                        exc_info=True,
                    )

            # Collection follows the ADOPTED status: only a run that
            # actually ended completed/interrupted gets a collector. A
            # terminal error or (adopted) cancel instead kills this run's
            # still-pending subagents — run-scoped, so prior turns' orphan
            # collectors and claims survive. No drain: the CAS already
            # committed this run's sse_events, and a cancelled/errored
            # run's subagent output is deliberately never billed. (On the
            # explicit user-stop path the teardown already popped the
            # whole registry; the scoped call is then a no-op.)
            if kind == "stream_end" and status in ("completed", "interrupted"):
                await self._spawn_subagent_collector(
                    thread_id, run_id, metadata, workspace_id, user_id
                )
            elif status in ("error", "cancelled"):
                try:
                    from src.server.services.background_registry_store import (
                        BackgroundRegistryStore,
                    )

                    await BackgroundRegistryStore.get_instance().cancel_run_tasks(
                        thread_id, run_id, force=True
                    )
                except Exception:
                    logger.warning(
                        f"[BackgroundTaskManager] run-scoped subagent "
                        f"cleanup failed for {key}",
                        exc_info=True,
                    )
        except Exception:
            logger.error(
                f"[BackgroundTaskManager] post-terminal side effects failed for "
                f"{key} (run remains {status})",
                exc_info=True,
            )

    async def _spawn_subagent_collector(
        self,
        thread_id: str,
        run_id: str,
        metadata: dict,
        workspace_id: Optional[str],
        user_id: Optional[str],
    ) -> None:
        """Claim this run's subagents and collect their events post-terminal."""
        response_id = run_id  # 1:1 contract

        from src.server.services.background_registry_store import BackgroundRegistryStore
        bg_store = BackgroundRegistryStore.get_instance()
        bg_registry = await bg_store.get_registry(thread_id)
        if not bg_registry:
            return
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
                # registered before run_id stamping shipped.
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
        if tasks_to_collect and workspace_id and user_id:
            handler = metadata.get("handler")
            sse_events = handler.get_sse_events() if handler else []
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

    async def wait_for_persistence(
        self, thread_id: str, run_id: str, timeout: float | None = None
    ) -> bool:
        """Wait until _finalize_run has finished persisting for the given turn.

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
        self,
        thread_id: str,
        timeout: float = 10.0,
        exclude_run_id: Optional[str] = None,
    ) -> bool:
        """Cancel a stale workflow on the given thread.

        ``exclude_run_id`` skips that run when locating the stale task, so a
        dispatched flow can pass its own run_id and not cancel the very run
        it is about to start.
        """
        async with self.task_lock:
            task_info = self._find_active_for_thread(
                thread_id, exclude_run_id=exclude_run_id
            )
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

    async def get_live_task_info(self, thread_id: str) -> Dict[str, Any]:
        """Liveness snapshot ``{"live", "run_id", "active_tasks"}`` for a thread's latest run.

        ``live`` is True when the in-process manager still holds a task record.
        Since the reader cutover (v4 2.4) this is executor-locality garnish
        (subagent ids for reattach), never lifecycle truth — the ledger row is.
        Exposes no status string: a task record's lifecycle state is not a
        workflow status.
        """
        async with self.task_lock:
            task_info = self._find_latest_for_thread(thread_id)
            if not task_info:
                return {"live": False, "run_id": None, "active_tasks": []}
            # Snapshot run_id under the lock, then release it BEFORE the registry
            # lookup below. Holding task_lock across that await would let a slow
            # registry path block /cancel from acquiring the lock to signal a stop.
            run_id = task_info.run_id

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

        return {"live": True, "run_id": run_id, "active_tasks": active_tasks}

    async def wait_for_admission(
        self,
        thread_id: str,
        exclude_run_id: Optional[str] = None,
    ) -> tuple[
        Literal["fresh", "running", "stopping", "compacting"],
        Optional[Dict[str, Any]],
    ]:
        """Decide whether a new turn can start on ``thread_id``.

        Ledger-driven (v4 2.4c): the thread's in_progress row decides, so
        the answer is identical on every worker — a run live on a peer must
        route to steering, never to a doomed START. The local task registry
        is consulted only as a fast path for awaiting a stopping run's
        teardown. Returns ``(state, active_row)``:

        - ``("fresh", None)`` — no live run: start a new turn.
        - ``("running", row)`` — a run is live (any worker): steer it
          (or 409 if steering fails). The row lets the caller run-stamp.
        - ``("stopping", row)`` — durable cancel intent whose teardown
          outlived the wait: 409 "stopping, retry" (never start a second
          writer while the checkpoint flush may still be running).
        - ``("compacting", None)`` — a thread mutation outlived the wait:
          409 "compacting, retry".

        ``exclude_run_id`` lets a caller ignore its own already-committed
        run while checking for OTHER live runs.
        """
        # Hold the new turn until any in-progress mutation finishes (a local
        # op's done-Event, or another worker's advertised op key), then read
        # the slot: an auto compaction leaves the turn's row in_progress
        # (caller steers); a manual mutation leaves no row (caller starts
        # fresh).
        from src.server.services.thread_mutation import ThreadMutationRunner

        runner = ThreadMutationRunner.get_instance()
        if await runner.is_mutating(thread_id):
            # Floor the wait at compaction_timeout + margin so a healthy
            # compaction is never 409'd before its own call budget self-
            # terminates and the runner's finally closes the op.
            backstop = max(
                get_admission_compaction_wait_timeout(),
                get_compaction_timeout() + self._COMPACTION_ADMISSION_MARGIN_S,
            )
            if not await runner.wait_until_idle(thread_id, timeout=backstop):
                logger.warning(
                    f"[BackgroundTaskManager] Mutation on thread {thread_id} "
                    f"did not finish within admission wait; rejecting new turn "
                    f"with 409 (compacting)"
                )
                return "compacting", None

        from src.server.database import turn_lifecycle as tl_db

        def _relevant(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if (
                row is not None
                and exclude_run_id
                and str(row["conversation_response_id"]) == exclude_run_id
            ):
                return None
            return row

        row = _relevant(await tl_db.get_active_run(thread_id))
        if row is None:
            return "fresh", None
        if row["cancel_requested_at"] is None:
            return "running", row

        # Stopping: durable cancel intent on the live row. Wait for the
        # teardown to settle — awaiting the local task when this worker
        # hosts the run (NEVER bare-await: it ends via CancelledError, and
        # ``asyncio.wait`` swallows it), else polling the slot until a
        # peer's finalize or the recovery scanner clears it.
        run_id = str(row["conversation_response_id"])
        async with self.task_lock:
            info = self.tasks.get((thread_id, run_id))
            local_task = info.task if info else None
        timeout = get_checkpoint_flush_timeout() + self._ADMISSION_TEARDOWN_MARGIN_S
        logger.info(
            f"[BackgroundTaskManager] Waiting for stopping run "
            f"({thread_id}, {run_id}) to finish teardown (timeout={timeout}s)"
        )
        if local_task is not None:
            await asyncio.wait({local_task}, timeout=timeout)
        else:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while loop.time() < deadline:
                current = await tl_db.get_active_run(thread_id)
                if (
                    current is None
                    or str(current["conversation_response_id"]) != run_id
                ):
                    break
                await asyncio.sleep(1.0)

        row = _relevant(await tl_db.get_active_run(thread_id))
        if row is None:
            return "fresh", None
        if row["cancel_requested_at"] is None:
            # A new run raced in while the stopped one drained.
            return "running", row
        logger.warning(
            f"[BackgroundTaskManager] Stopping run on {thread_id} still live "
            f"after {timeout}s; rejecting new turn with 409"
        )
        return "stopping", row

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
