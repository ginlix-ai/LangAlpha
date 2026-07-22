"""
Local run executor

Executes runs as background asyncio tasks that continue running
independently of SSE client connections. Runs write events to per-run
Redis Streams (``workflow:stream:{thread_id}:{run_id}``); consumers attach by
stream key and read via XREAD BLOCK, sharing no in-process state with the
run. Cleanup runs periodically to evict stale executions.

State is keyed by ``(thread_id, run_id)`` — each POST gets a fresh ``run_id``
at the handler entry, so cross-turn state aliasing is impossible by
construction. Per-thread admission locks still serialize the
``wait_or_steer → coordinator.start_run → executor.start_run`` window because
Pregel doesn't serialize concurrent ``astream`` on the same thread, and the
admission policy lives in our layer.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Literal, Optional, Callable, Coroutine
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
    get_wait_for_persistence_timeout,
)
from src.server.services.runs import (
    admission,
    finalization,
    stream_writer,
    subagent_collection,
    teardown,
)
from src.server.services.runs.stream_writer import TransportLostError
from src.server.dependencies.usage_limits import release_burst_slot

logger = logging.getLogger(__name__)


class LocalRunStatus(str, Enum):
    """Background task execution status."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"

    @property
    def terminal(self) -> bool:
        return self is not LocalRunStatus.RUNNING


@dataclass
class LocalRunExecution:
    """Information about a background workflow task."""
    thread_id: str
    run_id: str
    status: LocalRunStatus
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

    active_connections: int = 0

    metadata: Dict[str, Any] = field(default_factory=dict)

    completion_callback: Optional[Callable[["LocalRunExecution"], Coroutine[Any, Any, None]]] = None

    persistence_complete: asyncio.Event = field(default_factory=asyncio.Event)

    graph: Optional[Any] = None


# Type alias for the key used throughout the manager.
TaskKey = tuple[str, str]


class LocalRunExecutor:
    """Manages background workflow task execution.

    Singleton. State keyed by ``(thread_id, run_id)`` — each POST gets a
    fresh ``run_id`` so concurrent turns on the same thread are isolated
    by construction.
    """

    _instance: Optional['LocalRunExecutor'] = None

    def __init__(self):
        # Keyed by (thread_id, run_id). One slot per turn; no cross-turn
        # aliasing because run_id is fresh per POST.
        self.executions: Dict[TaskKey, LocalRunExecution] = {}
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
    def get_instance(cls) -> 'LocalRunExecutor':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ---------- helpers ----------

    def _find_latest_for_thread(self, thread_id: str) -> Optional[LocalRunExecution]:
        """Return the most-recently-created LocalRunExecution for ``thread_id`` or None.

        Used for thread-scoped lookups (e.g., /status?thread_id=...) where
        the caller didn't provide a run_id.
        """
        best: Optional[LocalRunExecution] = None
        for (tid, _rid), info in self.executions.items():
            if tid != thread_id:
                continue
            if best is None or info.created_at > best.created_at:
                best = info
        return best

    def _find_active_for_thread(self, thread_id: str) -> Optional[LocalRunExecution]:
        """Return the most-recently-created active (non-terminal) LocalRunExecution."""
        best: Optional[LocalRunExecution] = None
        for (tid, rid), info in self.executions.items():
            if tid != thread_id or info.status is not LocalRunStatus.RUNNING:
                continue
            if best is None or info.created_at > best.created_at:
                best = info
        return best

    async def has_active_tasks_for_workspace(self, workspace_id: str) -> bool:
        """Check if any active tasks exist for a workspace.

        Local root turns first (cheap), then the durable ledgers: in_progress
        response rows cover other workers' root turns, in_progress subagent
        runs cover tail-mode subagents that outlive every root run. A probe
        failure counts as active — callers gate sandbox teardown/recreate,
        where a false "idle" is destructive and a false "active" only defers.
        """
        async with self.task_lock:
            for info in self.executions.values():
                if (
                    info.metadata.get("workspace_id") == workspace_id
                    and info.status is LocalRunStatus.RUNNING
                ):
                    return True
        try:
            from src.server.database.runs import subagent_runs as sr_db
            from src.server.database.runs import lifecycle as tl_db

            if await tl_db.workspace_has_active_run(workspace_id):
                return True
            return await sr_db.count_open_runs_for_workspace(workspace_id) > 0
        except Exception:
            logger.warning(
                f"Workspace activity probe failed for {workspace_id}; "
                "treating as active",
                exc_info=True,
            )
            return True

    async def is_run_live(self, thread_id: str, run_id: str) -> bool:
        """True while this exact run's workflow task (or inner task) is still
        executing. A live executor owns the ledger row, the tracker, and all
        terminal transport — callers must not finalize a live run."""
        async with self.task_lock:
            ti = self.executions.get((thread_id, run_id))
            for t in (ti.task if ti else None, ti.inner_task if ti else None):
                if t is not None and not t.done():
                    return True
        return False

    async def has_active_task_for_thread(self, thread_id: str) -> bool:
        """True if any RUNNING task exists for the thread.

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
                f"LocalRunExecutor: Cleanup task started "
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
            logger.info("[LocalRunExecutor] Stopped cleanup task")

    async def shutdown(self, timeout: float | None = None):
        """Gracefully shutdown background task manager."""
        if timeout is None:
            timeout = get_shutdown_timeout()
        logger.info("[LocalRunExecutor] Starting graceful shutdown...")

        await self.stop_cleanup_task()

        async with self.task_lock:
            running_tasks = [
                (key, info)
                for key, info in self.executions.items()
                if info.status is LocalRunStatus.RUNNING
            ]

        if not running_tasks:
            logger.info("[LocalRunExecutor] No running workflows to cancel")
            return

        logger.info(
            f"[LocalRunExecutor] Cancelling {len(running_tasks)} running workflows"
        )

        for (thread_id, run_id), _info in running_tasks:
            # System shutdown, NOT a user stop: flush + kill subagents, but do
            # not persist the interrupted turn as a user-cancelled "Stopped".
            await self.signal_cancel(thread_id, run_id, user_initiated=False)

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
                f"[LocalRunExecutor] Shutdown timeout after {timeout}s, "
                f"forcing cancellation of stuck tasks"
            )
            stuck_tasks = []
            for key, info in running_tasks:
                if info.task and not info.task.done():
                    logger.warning(
                        f"[LocalRunExecutor] Force-cancelling stuck task: {key}"
                    )
                    info.task.cancel()
                    stuck_tasks.append(info.task)
            if stuck_tasks:
                try:
                    async with asyncio.timeout(5.0):
                        await asyncio.gather(*stuck_tasks, return_exceptions=True)
                    logger.info(
                        f"[LocalRunExecutor] Force-cancelled {len(stuck_tasks)} stuck tasks"
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"[LocalRunExecutor] {len(stuck_tasks)} tasks did not respond "
                        f"to force cancellation after 5s"
                    )

        logger.info("[LocalRunExecutor] Shutdown complete")

    async def _cleanup_loop(self):
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_abandoned_tasks()
            except asyncio.CancelledError:
                logger.info("[LocalRunExecutor] Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"[LocalRunExecutor] Error in cleanup loop: {e}")

    async def _cleanup_abandoned_tasks(self):
        """Clean up abandoned and completed tasks based on TTL."""
        now = datetime.now()
        abandoned_threshold = now - timedelta(seconds=self.abandoned_timeout)
        completed_threshold = now - timedelta(seconds=self.result_ttl)

        to_remove: list[TaskKey] = []
        dead_handles: list = []
        cold_running: list[TaskKey] = []

        async with self.task_lock:
            for key, info in self.executions.items():
                if info.status.terminal:
                    if info.completed_at and info.completed_at < completed_threshold:
                        to_remove.append(key)
                        logger.info(
                            f"[LocalRunExecutor] Cleanup: removing completed task "
                            f"{key} (age: {now - info.completed_at})"
                        )

                elif info.status == LocalRunStatus.RUNNING:
                    if info.active_connections == 0 and info.last_access_at < abandoned_threshold:
                        # Locally cold — candidate only. The cross-worker
                        # consumer signal is consulted outside the lock
                        # before anything is cancelled.
                        cold_running.append(key)

            for key in to_remove:
                del self.executions[key]

            # Admission locks are NOT reclaimed here. ``get_admission_lock``
            # returns the Lock object under ``task_lock`` but the caller
            # then awaits ``acquire()`` outside the lock — if cleanup were
            # to delete the entry in that gap, a concurrent ``get_admission_lock``
            # would create a fresh Lock and both POSTs would acquire
            # different lock objects, defeating admission. The dict is
            # tiny (one entry per thread that has ever seen traffic);
            # leave it.

        # Redis probe outside the lock: a locally-cold run may be watched
        # entirely through a sibling worker. None (Redis unreachable) means
        # unknown — never reap on unknown.
        abandoned: list[TaskKey] = []
        for key in cold_running:
            if await stream_writer.remote_consumer_signal(*key) == 0:
                abandoned.append(key)

        dead_removed: list[TaskKey] = []
        async with self.task_lock:
            for key in abandoned:
                info = self.executions.get(key)
                # Re-validate under the lock — a watcher or a finalize may
                # have landed during the probe.
                if (
                    info is None
                    or info.status is not LocalRunStatus.RUNNING
                    or info.active_connections != 0
                    or info.last_access_at >= abandoned_threshold
                ):
                    continue
                if info.task and not info.task.done():
                    # Cancel but KEEP the entry: the cancellation
                    # teardown's _finalize_run must still find this
                    # LocalRunExecution to settle the durable row. The entry
                    # leaves via the terminal-TTL branch above on a
                    # later pass. cancelling() gates re-cancels so a
                    # slow teardown isn't re-interrupted every sweep.
                    if not info.task.cancelling():
                        # System cancel, not a user stop: explicit_cancel
                        # gates the checkpoint flush + teardown (a bare
                        # .cancel() would skip both); user_stop stays False
                        # so the turn is not persisted as user-"Stopped".
                        info.cancel_event.set()
                        info.explicit_cancel = True
                        if info.inner_task and not info.inner_task.done():
                            info.inner_task.cancel()
                        info.task.cancel()
                        logger.warning(
                            f"[LocalRunExecutor] Cleanup: cancelling "
                            f"abandoned task {key} (no connections for "
                            f"{now - info.last_access_at}); entry retained "
                            f"until finalize"
                        )
                else:
                    # Task object gone or settled without ever
                    # finalizing — nothing will settle the durable
                    # row from here; fail it open outside the lock.
                    del self.executions[key]
                    dead_removed.append(key)
                    handle = info.metadata.get("run_handle")
                    if handle is not None:
                        dead_handles.append(handle)
                    logger.warning(
                        f"[LocalRunExecutor] Cleanup: removing dead "
                        f"RUNNING task {key} (task settled without finalize)"
                    )
        to_remove.extend(dead_removed)

        # Dead RUNNING entries whose task died without finalizing: settle the
        # durable row (outside the lock — fail_open_run does DB I/O).
        if dead_handles:
            from src.server.services.runs.coordinator import RunCoordinator

            for handle in dead_handles:
                await RunCoordinator.get_instance().fail_open_run(
                    handle, "worker task died without finalizing (abandoned cleanup)"
                )

        if to_remove:
            logger.info(
                f"[LocalRunExecutor] Cleaned up {len(to_remove)} tasks: {to_remove}"
            )

    async def get_admission_lock(self, thread_id: str) -> asyncio.Lock:
        """Return the per-thread admission lock, creating it on first use.

        Serializes ``wait_or_steer → start_run → start_run``
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

    async def start_run(
        self,
        thread_id: str,
        run_id: str,
        workflow_generator: Any,
        metadata: Optional[Dict[str, Any]] = None,
        completion_callback: Optional[Callable[["LocalRunExecution"], Coroutine[Any, Any, None]]] = None,
        graph: Optional[Any] = None,
    ) -> LocalRunExecution:
        """Start a workflow as a background task."""
        key = (thread_id, run_id)
        async with self.task_lock:
            if key in self.executions:
                raise RuntimeError(
                    f"Workflow {key} already exists with status "
                    f"{self.executions[key].status}"
                )

            running_count = sum(
                1 for t in self.executions.values()
                if t.status is LocalRunStatus.RUNNING
            )
            if running_count >= self.max_concurrent:
                raise ValueError(
                    f"Max concurrent workflows reached ({self.max_concurrent}). "
                    f"Currently running: {running_count}"
                )

            task_info = LocalRunExecution(
                thread_id=thread_id,
                run_id=run_id,
                status=LocalRunStatus.RUNNING,
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

            self.executions[key] = task_info

            logger.info(
                f"[LocalRunExecutor] Started workflow thread_id={thread_id} "
                f"run_id={run_id} (running: {running_count + 1}/{self.max_concurrent})"
            )

            started = task_info

        run_handle = (metadata or {}).get("run_handle")

        # Handoff intent recheck: a /cancel between START and this registration
        # stamped durable intent on the row but found no LocalRunExecution to signal
        # (the QUEUED placeholder that used to fill this gap is gone — the
        # in_progress row committed by priming is the only pre-registration
        # identity). The row is the authority — re-derive the local signal
        # from it now that a task exists.
        if run_handle is not None:
            from src.server.database.runs import lifecycle as tl_db

            run_row = None
            try:
                run_row = await tl_db.get_run(run_handle.run_id)
            except Exception:
                logger.warning(
                    f"[LocalRunExecutor] handoff cancel-intent recheck "
                    f"failed for {key}",
                    exc_info=True,
                )
            if (
                run_row
                and run_row.get("cancel_requested_at")
                and run_row.get("status") == "in_progress"
            ):
                logger.info(
                    f"[LocalRunExecutor] durable cancel intent found at "
                    f"handoff for {key}; signalling cancel"
                )
                await self.signal_cancel(thread_id, run_id)

        # I2: the guard monitor aborts the run on session loss before it can
        # act on a stale view — same force path as a user stop (cooperative
        # event + inner-task cancel), classified by the guard-lost downgrade.
        if (
            started is not None
            and run_handle is not None
            and run_handle.guard is not None
        ):

            def _abort_on_session_loss() -> None:
                info = self.executions.get(key)
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
        only ``inner_task`` (see ``signal_cancel``), so the ``CancelledError``
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
                task_info = self.executions.get(key)
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
                ti = self.executions.get(key)
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
                f"[LocalRunExecutor] Workflow {key} failed: {e}",
                exc_info=True
            )
            await self._finalize_run(thread_id, run_id, kind="failed", error=str(e))

    async def _flush_checkpoint(self, thread_id: str, run_id: str) -> None:
        """Resolve the run's graph and flush its checkpoint on user stop."""
        async with self.task_lock:
            task_info = self.executions.get((thread_id, run_id))
            graph = task_info.graph if task_info else None

        if not graph:
            return

        await teardown.flush_checkpoint(graph, thread_id)

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
        """Run the stop teardown, lending it this executor's orphan-collector
        bucket, then stash the drained events for the finalize to persist."""
        merged_subagent_events = await teardown.teardown_subagents_on_stop(
            thread_id,
            run_id,
            orphan_collectors=self._orphan_collectors.get(thread_id, {}),
            drain=self._drain_killed_subagent_events,
        )

        # Stash merged events for _mark_cancelled to fold into persisted sse_events.
        if merged_subagent_events:
            async with self.task_lock:
                ti = self.executions.get((thread_id, run_id))
                if ti is not None:
                    ti.metadata["_stop_subagent_events"] = merged_subagent_events

    async def _drain_killed_subagent_events(
        self, thread_id: str, tasks: list
    ) -> list[dict]:
        """Snapshot a killed run's subagent events (body in runs/teardown.py)."""
        return await teardown.drain_killed_subagent_events(thread_id, tasks)

    async def _buffer_event_redis(self, thread_id: str, run_id: str, event: str):
        """Append a workflow event to the per-run Redis Stream.

        Local gate only — the transport contract (fatal-on-loss, I6) lives
        in ``stream_writer.buffer_event``. Only the memory backend
        (explicitly configured, no stream consumers) keeps best-effort
        semantics.
        """
        key = (thread_id, run_id)
        async with self.task_lock:
            if key not in self.executions:
                return

        if self.event_storage_backend != "redis":
            return  # memory backend: no stream transport to lose

        await stream_writer.buffer_event(
            thread_id, run_id, event,
            max_stored_messages=self.max_stored_messages,
        )

    async def append_run_end_event(
        self,
        thread_id: str,
        run_id: str,
        outcome: Optional[str],
        *,
        error_frame: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write the closing frames to the per-run Stream (I6), exactly once.

        Delegates to ``stream_writer.append_run_end_event`` — see there for
        the SETNX gate and retention-stamp contract.
        """
        if self.event_storage_backend != "redis":
            return
        await stream_writer.append_run_end_event(
            thread_id, run_id, outcome,
            error_frame=error_frame,
            redis_event_ttl=self.redis_event_ttl,
            max_stored_messages=self.max_stored_messages,
        )

    # ========== Subagent collection ==========

    async def _delete_task_keys_if_owned(
        self,
        cache,
        thread_id: str,
        task_id: str,
        response_id: str,
        task_run_id: Optional[str] = None,
    ) -> None:
        await subagent_collection.delete_task_keys_if_owned(
            cache, thread_id, task_id, response_id, task_run_id=task_run_id
        )

    async def _replay_owned_task_events(
        self, thread_id: str, task, response_id: str, out: list[dict]
    ) -> bool:
        return await subagent_collection.replay_owned_task_events(
            thread_id, task, response_id, out
        )

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
        await subagent_collection.collect_subagent_results_for_turn(
            thread_id, response_id, original_chunks, tasks,
            workspace_id, user_id, timeout=timeout, is_byok=is_byok,
            sandbox=sandbox,
            track_orphan_collector=self._track_orphan_collector,
        )

    async def _publish_settled_wake(self, thread_id: str) -> None:
        await subagent_collection.publish_settled_wake(thread_id)

    async def _await_drain_and_cleanup_tasks(
        self,
        tasks: list,
        thread_id: str,
        response_id: str,
        timeout: float | None = None,
        *,
        retire_streams: bool = True,
    ) -> None:
        await subagent_collection.await_drain_and_cleanup_tasks(
            tasks, thread_id, response_id, timeout=timeout,
            retire_streams=retire_streams,
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
        await subagent_collection.collect_orphaned_subagent_results(
            thread_id=thread_id,
            response_id=response_id,
            main_chunks=main_chunks,
            prior_subagent_events=prior_subagent_events,
            tasks=tasks,
            workspace_id=workspace_id,
            user_id=user_id,
            is_byok=is_byok,
            sandbox=sandbox,
        )

    # ========== Terminal handlers ==========

    def _release_terminal_refs(
        self,
        thread_id: str,
        run_id: str,
    ) -> None:
        """Drop heavy in-process refs once a LocalRunExecution is in terminal state."""
        info = self.executions.get((thread_id, run_id))
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
        terminal write is ``RunCoordinator.finalize_run`` — one CAS
        transaction carrying usage rows with it, so a persist failure can no
        longer be swallowed into a zombie-ACTIVE turn.
        """
        from src.server.services.runs.coordinator import RunOutcome

        key = (thread_id, run_id)
        async with self.task_lock:
            task_info = self.executions.get(key)
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

        status, phase, interrupt_reason, error = await finalization.classify_outcome(
            kind, handler=handler, graph=graph, thread_id=thread_id, error=error
        )

        (
            execution_time,
            per_call_records,
            tool_usage,
            sse_events,
            persist_metadata,
        ) = await finalization.assemble_finalize_artifacts(
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
            outcome = RunOutcome(
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
                guard = handle.guard

                async def tail_drain() -> None:
                    await self._drain_run_subagent_writers(thread_id, run_id, guard)

            finalize_applied, status, survivor_status = (
                await finalization.drive_finalize_cas(
                    key, handle, outcome, tail_drain=tail_drain
                )
            )
        else:
            logger.error(
                f"[LocalRunExecutor] no run_handle for {key}; durable "
                f"finalize skipped (legacy path?)"
            )

        # ---- local terminal mark (after the CAS: a losing finalize adopts
        # the survivor's status instead of relabeling the winner's). A THROWN
        # finalize (applied=False, no survivor) marks nothing: the row is
        # still in_progress, so the entry must stay RUNNING — with its
        # run_handle — for cleanup's dead-handle fail_open path. ----
        finalize_concluded = finalize_applied or survivor_status is not None
        if finalize_concluded:
            local_status = survivor_status or status
            async with self.task_lock:
                task_info.status = {
                    "cancelled": LocalRunStatus.CANCELLED,
                    "error": LocalRunStatus.FAILED,
                    "interrupted": LocalRunStatus.INTERRUPTED,
                }.get(local_status, LocalRunStatus.COMPLETED)
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
        if finalize_concluded:
            async with self.task_lock:
                self._release_terminal_refs(thread_id, run_id)

    # Legit tail subagents (deep research) run 15+ min; this only bounds how
    # long a HUNG writer can pin a budget slot before the teardown discards
    # the session out from under it.
    TAIL_DRAIN_TIMEOUT = 1800.0

    async def _drain_run_subagent_writers(
        self, thread_id: str, run_id: str, guard
    ) -> None:
        """Wait until no writer fenced by this run's guard session can touch
        the checkpointer anymore. Ownership is keyed on the guard's own
        namespace set — the same membership check_write_ns enforces — not on
        ``spawned_run_id`` bookkeeping, so any writer this session admitted
        (spawned or resumed) holds the release, and foreign writers (another
        run's tail) never do. Collectors are not writers (they read Redis
        and persist via the app pool).

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
                    if not guard.owns_task_ns(t.task_id):
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
                f"[LocalRunExecutor] guard tail-drain awaiting "
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
                        f"[LocalRunExecutor] completion side-effect callback "
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
                        f"[LocalRunExecutor] run-scoped subagent "
                        f"cleanup failed for {key}",
                        exc_info=True,
                    )
        except Exception:
            logger.error(
                f"[LocalRunExecutor] post-terminal side effects failed for "
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
        await subagent_collection.spawn_subagent_collector(
            thread_id, run_id, metadata, workspace_id, user_id,
            collect_for_turn=self._collect_subagent_results_for_turn,
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
            task_info = self.executions.get((thread_id, run_id))
            event = task_info.persistence_complete if task_info else None
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(
                f"[LocalRunExecutor] wait_for_persistence timed out for "
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
    ) -> bool:
        return await subagent_collection.persist_collected_events(
            main_chunks, subagent_events, response_id,
            thread_id, workspace_id, user_id, sandbox=sandbox,
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
        await subagent_collection.persist_subagent_usage(
            response_id, tasks, thread_id, workspace_id, user_id,
            is_byok=is_byok,
        )

    # ---------- status & introspection ----------

    async def get_local_run(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> Optional[LocalRunExecution]:
        """Get full task info for a specific run, or latest on thread."""
        async with self.task_lock:
            if run_id is not None:
                task_info = self.executions.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.last_access_at = datetime.now()
            return task_info

    async def increment_connection(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> bool:
        found = False
        async with self.task_lock:
            if run_id is not None:
                task_info = self.executions.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.active_connections += 1
                task_info.last_access_at = datetime.now()
                run_id = task_info.run_id
                found = True
        # Remote bump even without a local entry: watching a foreign
        # worker's run is exactly the case the counter exists for.
        if run_id is not None:
            await stream_writer.bump_remote_consumers(
                thread_id, run_id, 1, ttl_seconds=self.abandoned_timeout
            )
        return found

    async def decrement_connection(
        self, thread_id: str, run_id: Optional[str] = None
    ) -> bool:
        found = False
        async with self.task_lock:
            if run_id is not None:
                task_info = self.executions.get((thread_id, run_id))
            else:
                task_info = self._find_latest_for_thread(thread_id)
            if task_info:
                task_info.active_connections = max(0, task_info.active_connections - 1)
                run_id = task_info.run_id
                found = True
        if run_id is not None:
            await stream_writer.bump_remote_consumers(
                thread_id, run_id, -1, ttl_seconds=self.abandoned_timeout
            )
        return found

    async def signal_cancel(
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
                task_info = self.executions.get((thread_id, run_id))
            else:
                task_info = self._find_active_for_thread(thread_id)

            if not task_info:
                logger.warning(
                    f"[LocalRunExecutor] Cannot cancel "
                    f"thread_id={thread_id} run_id={run_id}: workflow not found"
                )
                return False

            if task_info.status is not LocalRunStatus.RUNNING:
                logger.info(
                    f"[LocalRunExecutor] Cannot cancel "
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
                f"[LocalRunExecutor] Cancellation signaled: "
                f"thread_id={thread_id} run_id={task_info.run_id}"
            )
            return True

    async def cancel_stale_workflow(
        self,
        thread_id: str,
        timeout: float = 10.0,
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
                    f"[LocalRunExecutor] Stale workflow thread_id={thread_id} "
                    f"did not exit within {timeout}s"
                )
        return True

    async def wait_for_admission(
        self,
        thread_id: str,
    ) -> tuple[
        Literal["fresh", "running", "stopping", "compacting"],
        Optional[Dict[str, Any]],
    ]:
        """Decide whether a new turn can start on ``thread_id``.

        Delegates to ``runs.admission.wait_for_admission``, lending it this
        process's task registry as the stopping-teardown fast path.
        """
        return await admission.wait_for_admission(
            thread_id, local_task_probe=self._local_task_probe
        )

    async def _local_task_probe(
        self, thread_id: str, run_id: str
    ) -> Optional[asyncio.Task]:
        """The run's writer task when THIS worker hosts it, else None."""
        async with self.task_lock:
            info = self.executions.get((thread_id, run_id))
            return info.task if info else None
