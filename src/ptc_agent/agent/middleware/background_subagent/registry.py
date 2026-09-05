"""Background task registry for tracking async subagent executions.

This module provides a thread-safe registry for managing background tasks
spawned by the BackgroundSubagentMiddleware.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
import uuid as uuid_mod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import structlog

from . import redis_stream
from .task import BackgroundTask, TerminalStatus

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.utils import MessageChecker

logger = structlog.get_logger(__name__)

# Re-exported: ``BackgroundTask`` moved to its own leaf module, and every
# caller already reaches for it here.
__all__ = [
    "CANCEL_UNWIND_TIMEOUT",
    "BackgroundTask",
    "BackgroundTaskRegistry",
    "TaskRunRejected",
    "TaskWriterLive",
    "TerminalStatus",
    "TransportLostError",
]

# Bounded wait for a cancelled task's unwind before its registry entry drops
# (normal unwind is milliseconds; see ``cancel_run_tasks``).
CANCEL_UNWIND_TIMEOUT = 15.0

# Cap on the pre-signal durable cancel-intent stamp (``stamp_cancel_intent``):
# a hung ledger call must not block the user-facing local cancel.
_CANCEL_INTENT_STAMP_TIMEOUT_S = 2.0



class TransportLostError(RuntimeError):
    """The task's Redis event transport is torn (spill failure or quota).

    Agent-layer twin of the server's root-stream ``TransportLostError`` —
    defined here so subagent code needn't import server modules. Raised by
    the astream forwarder loop once ``redis_write_failed`` flips, aborting
    the graph instead of completing a run whose replay archive has holes.
    """


class TaskRunRejected(Exception):
    """The run ledger refused this spawn/resume (admission-authoritative).

    Defined here — not in the server — so middleware code can catch it
    without importing server modules; the server-side ledger raises it.
    ``existing`` carries the conflicting run row when the rejection is a
    duplicate/slot conflict rather than an infra failure.
    """

    def __init__(self, reason: str, existing: dict[str, Any] | None = None):
        self.reason = reason
        self.existing = existing
        super().__init__(reason)


class TaskWriterLive(Exception):
    """register() refused a tool_call_id whose previous writer still runs.

    Raised atomically under the registry lock so checkpoint re-execution of
    an already-spawned Task call cannot displace the live writer's routing
    identity; ``task`` is the live entry, for an idempotent answer.
    """

    def __init__(self, task: "BackgroundTask"):
        self.task = task
        super().__init__(f"live writer already registered for {task.tool_call_id}")


def take_task_usage(
    tasks: list[BackgroundTask], response_id: str
) -> list[tuple[BackgroundTask, list, dict, str | None]]:
    """Snapshot-and-clear the usage records ``response_id`` still owns.

    Module-level so a torn-down thread with no registry left to lock can run
    the same body: it has no awaits, so it is atomic either way, and the lock
    only orders it against concurrent registry mutation.

    ``task_run_id`` is snapshotted with the records for the same reason they
    are cleared together: a resume remints it, and the caller reads it only
    after several awaits. Settling the reminted id would mark the *live* run
    billed on its predecessor's usage alone, after which its heartbeats bounce
    off the settle guard and its spend is never seen again.
    """
    taken: list[tuple[BackgroundTask, list, dict, str | None]] = []
    for task in tasks:
        if task.collector_response_id != response_id:
            continue
        if not (task.per_call_records or task.tool_usage):
            continue
        records, tool_usage = task.per_call_records, task.tool_usage
        task.per_call_records = []
        task.tool_usage = {}
        taken.append((task, records, tool_usage, task.task_run_id or None))
    return taken


def _estimate_record_bytes(record: dict[str, Any]) -> int:
    """Cheap upper-bound estimate of a captured-event record's serialized size.

    Used purely for telemetry — never on the hot path's blocking section.
    Falls back to a conservative constant if json.dumps trips on something.
    """
    try:
        return len(json.dumps(record, ensure_ascii=False, default=str))
    except Exception:
        return 256


class BackgroundTaskRegistry:
    """Thread-safe registry for background subagent tasks spawned by BackgroundSubagentMiddleware."""

    def __init__(self, thread_id: str = "") -> None:
        """
        Args:
            thread_id: Parent thread this registry serves. Used to build
                ``subagent:stream:{thread_id}:{task_id}`` keys. Empty string
                disables Redis spill (used in tests).
        """
        self._tasks: dict[str, BackgroundTask] = {}
        self._task_id_to_tool_call_id: dict[str, str] = {}  # task_id -> tool_call_id
        self._lock = asyncio.Lock()
        self._results: dict[str, Any] = {}
        self._late_removals: set[asyncio.Task] = set()
        self.current_turn_index: int = 0
        self.current_run_id: str | None = None
        self.thread_id: str = thread_id
        # (thread_id, task_id, task_run_id) -> durable result text, injected by
        # the server (checkpoint-backed). None in CLI/tests — delivery then
        # falls back to the in-memory handler result.
        self.result_resolver: (
            Callable[[str, str, str | None], Awaitable[str | None]] | None
        ) = None
        # Same read, archive-only: for callers holding a failed/unknown verdict,
        # where the transcript half of result_resolver would fabricate.
        self.archived_result_resolver: (
            Callable[[str, str, str], Awaitable[str | None]] | None
        ) = None
        # Admission-authoritative run ledger (server-injected, same pattern
        # as result_resolver): duck-typed `start_task_run`/`finalize_task_run`
        # raising TaskRunRejected on conflict. None in CLI/tests — spawn and
        # finalize then skip the ledger entirely.
        self.run_ledger: Any | None = None

    async def mark_result_delivered(self, task: BackgroundTask) -> None:
        """Stamp the durable result_delivered_at on the run's ledger row —
        the report-back executor's arbitration signal (best-effort: a missed
        stamp costs one redundant notification, never a lost result)."""
        if self.run_ledger is not None and task.task_run_id:
            try:
                await self.run_ledger.mark_result_delivered(task.task_run_id)
            except Exception:
                logger.warning(
                    "durable result_delivered stamp failed",
                    task_id=task.task_id,
                    task_run_id=task.task_run_id,
                    exc_info=True,
                )

    async def resolve_result_text(
        self, task_id: str, task_run_id: str | None = None
    ) -> str | None:
        """Derive a task's result text from its durable archive.

        The registry entry is volatile (evicted after collection, wiped on
        stop/restart, absent on other workers) while the task's answer is
        checkpointed under ``task:{task_id}`` — the resolver reads the latter,
        so delivery survives the registry. ``task_run_id`` scopes the read to
        one ledger run; without it only the transcript derivation applies.
        Never raises; None means "nothing archived / no resolver", and callers
        fall back to in-memory state.
        """
        if self.result_resolver is None:
            return None
        try:
            return await self.result_resolver(self.thread_id, task_id, task_run_id)
        except Exception:
            logger.warning(
                "Durable result resolve failed; falling back to in-memory",
                task_id=task_id,
                exc_info=True,
            )
            return None

    async def resolve_archived_result_text(
        self, task_id: str, task_run_id: str
    ) -> str | None:
        """A run's explicitly archived result text — never transcript-derived.

        The strict counterpart to ``resolve_result_text``, for callers whose
        run verdict is failed or unknown: only a run that produced a result
        archives one, so presence proves the work finished even when the
        ledger says otherwise. Never raises; None means "nothing archived".
        """
        if self.archived_result_resolver is None:
            return None
        try:
            return await self.archived_result_resolver(
                self.thread_id, task_id, task_run_id
            )
        except Exception:
            logger.warning(
                "Durable archived result resolve failed",
                task_id=task_id,
                task_run_id=task_run_id,
                exc_info=True,
            )
            return None

    async def register(
        self,
        tool_call_id: str,
        description: str,
        prompt: str,
        subagent_type: str,
        asyncio_task: asyncio.Task | None = None,
        run_id: str | None = None,
        owner_task_id: str | None = None,
    ) -> BackgroundTask:
        """Register a new background task and return it.

        Raises :class:`TaskWriterLive` when a live writer already holds
        ``tool_call_id`` (checkpoint re-execution of a spawned call).

        ``run_id`` is the LangGraph run_id of the dispatching turn, stamped on
        the task so the collector can filter prior-turn subagents. Callers
        should always pass it explicitly (read from request config) rather
        than relying on ``self.current_run_id``, which would race when two
        concurrent turns share the registry.
        """
        async with self._lock:
            # A same-id re-registration (checkpoint replay re-executing the
            # tool call) must not displace a live entry — check and refusal
            # are atomic under this lock, and the raise carries the live
            # task for an idempotent answer. "Live" includes a STARTING
            # entry (not completed, no handles yet): its spawn is awaiting
            # setup and will publish a writer; replacing it would strand
            # that writer on an unregistered task while capture routes the
            # tool_call_id to the replacement. A dead-writer retry re-
            # registers freely (settle paths stamp completed).
            existing = self._tasks.get(tool_call_id)
            if existing is not None and (
                not existing.completed
                or any(
                    t is not None and not t.done()
                    for t in (existing.asyncio_task, existing.handler_task)
                )
            ):
                raise TaskWriterLive(existing)

            # Generate short alphanumeric task_id
            task_id = secrets.token_urlsafe(4)[:6]

            agent_id = f"{subagent_type}:{uuid_mod.uuid4()}"
            task = BackgroundTask(
                tool_call_id=tool_call_id,
                task_id=task_id,
                description=description,
                prompt=prompt,
                subagent_type=subagent_type,
                owner_task_id=owner_task_id,
                asyncio_task=asyncio_task,
                agent_id=agent_id,
                spawned_turn_index=self.current_turn_index,
                spawned_run_id=run_id if run_id is not None else self.current_run_id,
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
        """Return all tasks that haven't completed yet."""
        async with self._lock:
            return [task for task in self._tasks.values() if task.is_pending]

    async def get_all_tasks(self) -> list[BackgroundTask]:
        """Return all registered tasks — infrastructure view (teardown,
        liveness, workflow child cancellation). Agent-facing aggregates must
        use ``get_turn_visible_tasks``."""
        async with self._lock:
            return list(self._tasks.values())

    async def get_turn_visible_tasks(self) -> list[BackgroundTask]:
        """The task set the agent is allowed to see in aggregate.

        The door for the TaskOutput aggregates, where applying
        ``is_turn_visible`` per call site is how a workflow's children leaked
        into one branch while the branch beside it filtered them out. The two
        other visibility-filtered readers inline the predicate instead, and
        have to: ``wait_for_all`` already holds ``self._lock``, and the
        notification scan folds it into a compound filter. Tighten the rule
        here and those two need the same edit.
        """
        async with self._lock:
            return [t for t in self._tasks.values() if t.is_turn_visible]

    def _has_collectible_work(self, task: BackgroundTask) -> bool:
        """Still running, or holding capture a collector has to persist."""
        return bool(
            task.is_pending
            or task.captured_event_count > 0
            or task.per_call_records
            or task.tool_usage
        )

    async def _claim_for_collector(
        self, response_id: str, in_scope: Callable[[BackgroundTask], bool]
    ) -> list[BackgroundTask]:
        """Atomically claim one scope's unclaimed tasks for a collector.

        Scan and claim share the registry lock: two collectors that both saw
        a task unclaimed would each persist its events under a different
        response_id, so the check and the stamp must not be separable — which
        is why every claim scope is a registry method rather than a getter the
        caller loops over.
        """
        async with self._lock:
            claimed = []
            for task in self._tasks.values():
                if task.collector_response_id or not in_scope(task):
                    continue
                if self._has_collectible_work(task):
                    task.collector_response_id = response_id
                    claimed.append(task)
            return claimed

    async def claim_owner_children(
        self, owner_task_id: str, response_id: str
    ) -> list[BackgroundTask]:
        """Claim an owner's unclaimed children for a collector."""
        return await self._claim_for_collector(
            response_id, lambda task: task.owner_task_id == owner_task_id
        )

    async def claim_run_subagents(
        self, run_id: str, response_id: str
    ) -> list[BackgroundTask]:
        """Claim the subagents a turn spawned for that turn's collector.

        An unstamped ``spawned_run_id`` matches — compat shim for tasks
        registered before run-id stamping shipped.
        """
        return await self._claim_for_collector(
            response_id,
            lambda task: task.spawned_run_id is None or task.spawned_run_id == run_id,
        )

    async def take_owned_usage(
        self, tasks: list[BackgroundTask], response_id: str
    ) -> list[tuple[BackgroundTask, list, dict, str | None]]:
        """Take the usage records this collector already owns, under the lock.

        The billing sibling of the claim family: ownership is checked rather
        than stamped, because the caller claimed these tasks earlier — and the
        snapshot and the clear are one step, or a concurrent accrual is billed
        twice or lost.
        """
        async with self._lock:
            return take_task_usage(tasks, response_id)

    async def live_writers_where(
        self, predicate: Callable[[BackgroundTask], bool]
    ) -> list[asyncio.Task]:
        """Snapshot the still-running writer handles of matching tasks.

        Under the lock so a handle can't be swapped mid-scan. The predicate is
        the caller's because the scopes that need this aren't the registry's to
        know — the guard drain matches on writer namespace, which no registry
        field records.
        """
        async with self._lock:
            writers: list[asyncio.Task] = []
            for task in self._tasks.values():
                if not predicate(task):
                    continue
                for writer in (task.asyncio_task, task.handler_task):
                    if writer is not None and not writer.done():
                        writers.append(writer)
            return writers

    def _locked_get_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Resolve a 6-char task_id. Caller must hold ``self._lock``."""
        tool_call_id = self._task_id_to_tool_call_id.get(task_id)
        return self._tasks.get(tool_call_id) if tool_call_id else None

    async def get_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Return the task for a given 6-char task_id, or None."""
        async with self._lock:
            return self._locked_get_by_task_id(task_id)

    async def reclaim_for_resume(self, task: BackgroundTask) -> None:
        """Atomically steal a task back from any collector for a resume.

        Clears the collector claim and restores registry membership in one
        lock-held section: past this point every collector mutation site
        (settle-mark, replay, report-back enqueue, cleanup, eviction) fences
        on the claim and skips the task, and an eviction that already
        happened is healed by the re-insert — the resumed writer always
        spawns onto a registered entry.
        """
        async with self._lock:
            task.collector_response_id = None
            self._tasks[task.tool_call_id] = task
            self._task_id_to_tool_call_id[task.task_id] = task.tool_call_id

    async def get_task_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Alias for get_by_task_id, used by the HTTP layer."""
        return await self.get_by_task_id(task_id)

    def get_by_tool_call_id(self, tool_call_id: str) -> BackgroundTask | None:
        """Return the task for a given tool_call_id (synchronous, no lock)."""
        return self._tasks.get(tool_call_id)

    async def publish_writer(
        self, task: "BackgroundTask", factory: "Callable[[], asyncio.Task]"
    ) -> "asyncio.Task | None":
        """Atomically create and publish a starting task's writer handle.

        The spawn path awaits setup (admission, meta, opener) between
        registration and writer creation; a cancel landing in that window
        stamps the handle-less task cancelled. Check-and-publish under the
        registry lock — the same lock the cancel loops mutate under — so the
        race has exactly two outcomes: the writer publishes and the cancel
        signals it, or the cancel wins and no writer is ever created.
        Returns None when the cancel won (entry stamped or already dropped).
        The identity check pins the caller's OWN task object: a cancel that
        removed the entry followed by a re-registration under the same
        tool_call_id must not receive the aborted spawn's writer.
        """
        async with self._lock:
            if (
                self._tasks.get(task.tool_call_id) is not task
                or task.completed
                or task.asyncio_task is not None
            ):
                return None
            asyncio_task = factory()
            task.asyncio_task = asyncio_task
            return asyncio_task

    async def append_captured_event(
        self, tool_call_id: str, event: dict[str, Any], *, terminal: bool = False
    ) -> None:
        """Append a captured SSE event to a background task.

        Called by SubagentEventCaptureMiddleware (and steering) to capture
        events for per-task SSE replay and post-interrupt persistence. The
        record is best-effort spilled to the per-task Redis Stream; failure
        leaves the seq counter advanced but flips ``redis_write_failed``.
        An unresolvable tool_call_id drops the append — an evicted task's
        retired streams must not be recreated by a late writer.
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
        if not task:
            return
        await self.append_event_for_task(task, event, terminal=terminal)

    async def append_event_for_task(
        self,
        task: "BackgroundTask",
        event: dict[str, Any],
        *,
        terminal: bool = False,
    ) -> None:
        """Identity-exact append for the terminal settle pipeline.

        The settle paths hold THEIR task object, whose registry entry may
        already be evicted (cancel teardown) or whose tool_call_id may be
        reused by a re-registration — resolving by id would drop the frames
        or write them into the replacement task's streams (stream keys are
        task_id-scoped, so operating on the object always reaches its own).
        Retired-stream recreation is safe here: the settle stamps terminal
        retention on these keys right after.
        """
        async with self._lock:
            # A killed task's streams are final: the stop drain reads the
            # high-water after the kill, so a writer surviving the bounded
            # unwind must not append past it — output beyond the snapshot
            # would be visible live yet absent from every durable store.
            # ``terminal`` exempts unwind bookkeeping (steering_returned):
            # those frames run inside the bounded unwind the drain awaits,
            # and dropping them would erase acknowledged user input.
            if task.cancelled and not terminal:
                return

            task.captured_event_seq += 1
            seq = task.captured_event_seq
            ts = event.get("ts")
            record: dict[str, Any] = {
                "seq": seq,
                "event": event.get("event"),
                "data": event.get("data") or {},
                "agent_id": task.agent_id,
            }
            if ts is not None:
                record["ts"] = ts
            # Round stamp: collectors on OTHER workers can't see this
            # process's claim state, so the record itself carries which run's
            # writer produced it — the durable replay fence a resumed round's
            # reused seq numbers would otherwise slip past.
            if task.spawned_run_id:
                record["run"] = task.spawned_run_id
            # Ledger identity: the attribution join key for replay. Every
            # captured record names the execution that produced it, so a
            # resumed task's rounds partition without content matching.
            if task.task_run_id:
                record["task_run"] = task.task_run_id

            # Counts this round's appends, NOT the seq: on a resume that could
            # not clear the spool the seq carries over from the prior round,
            # and the archive gates measure this round only.
            task.captured_event_count += 1
            task.captured_event_bytes += _estimate_record_bytes(record)
            # Bump last_updated_at only on user-visible text output.
            # reasoning_signal / reasoning / tool_calls / tool_call_result
            # events are excluded — they're pacing noise.
            if (
                event.get("event") == "message_chunk"
                and (event.get("data") or {}).get("content_type") == "text"
            ):
                now = time.time()
                task.last_updated_at = now
                # A child's progress is its owner's progress. The orphan
                # collector watches an owner alone — owner-children are not
                # claimed until the owner settles — and a workflow parent
                # emits nothing between child_started and child_done. Without
                # this, a fan-out whose children legitimately outrun the idle
                # timeout reads as abandoned, and the collector releases the
                # claim that would have persisted their events and billed
                # their usage.
                if task.owner_task_id:
                    owner = self._locked_get_by_task_id(task.owner_task_id)
                    if owner is not None:
                        owner.last_updated_at = now

        # Spill OUTSIDE the lock — Redis I/O must not block subsequent appends.
        await self._spill_record_to_redis(task, record)

    async def _spill_record_to_redis(
        self, task: BackgroundTask, record: dict[str, Any]
    ) -> None:
        """Per-instance seam over ``redis_stream.spill_task_record``."""
        await redis_stream.spill_task_record(self.thread_id, task, record)

    async def write_task_meta(self, task: BackgroundTask, status: str) -> None:
        """Mirror routing identity + writer liveness for other workers
        (``redis_stream.write_task_meta``)."""
        await redis_stream.write_task_meta(self.thread_id, task, status)

    async def append_sentinel_to_stream(self, tool_call_id: str) -> None:
        """Write a stream-end sentinel to the per-task Redis Stream.

        The forwarder calls this once when ``_arun_subagent_streaming`` exits
        its astream loop — the canonical "no more events coming" moment. The
        per-task SSE consumer recognises the record and closes immediately,
        instead of polling ``task.asyncio_task.done()`` between BLOCK timeouts.

        Bypasses the event tail and Postgres persistence — this is a
        transport-level signal, not content. Best-effort: if it fails,
        ``terminal_check`` still closes the stream once the asyncio task
        finishes (just slower).
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
        if not task:
            return
        await self.append_sentinel_for_task(task)

    async def append_sentinel_for_task(self, task: "BackgroundTask") -> None:
        """Identity-exact sentinel — see ``append_event_for_task`` for why
        the settle pipeline must not re-resolve by reusable tool_call_id."""
        await redis_stream.append_task_sentinel(self.thread_id, task)

    async def stamp_terminal_retention(self, tool_call_id: str) -> None:
        """Stamp the attach-grace TTL on the task's event keys at terminal.

        Active streams carry no TTL (retention contract), so this is the
        only place their expiry clock starts. Runs unconditionally from the
        run wrapper's finally — including when ``redis_write_failed`` skipped
        the sentinel — because a torn stream must still expire, not leak.
        Idempotent and best-effort.
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
        if not task:
            return
        await self.stamp_terminal_retention_for_task(task)

    async def stamp_terminal_retention_for_task(
        self, task: "BackgroundTask"
    ) -> None:
        """Identity-exact retention stamp — see ``append_event_for_task``
        for why the settle pipeline must not re-resolve by tool_call_id."""
        if not self.thread_id:
            return
        try:
            await redis_stream.stamp_task_retention(
                self.thread_id,
                task.task_id,
                task.task_run_id,
                timeout=redis_stream._SPILL_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            # WARNING, not debug: this is the ONLY place an active stream's
            # expiry clock starts, so a swallowed failure here is exactly how
            # a stream becomes immortal. The retention sweeper is the backstop,
            # and a trickle of these is the signal it is doing real work.
            logger.warning(
                "subagent_terminal_ttl_stamp_failed",
                tool_call_id=task.tool_call_id,
                error=str(exc),
            )

    async def update_metrics(self, tool_call_id: str, tool_name: str) -> None:
        """Increment tool-call counters for a task; called by SubagentEventCaptureMiddleware."""
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if task:
                task.tool_call_counts[tool_name] = (
                    task.tool_call_counts.get(tool_name, 0) + 1
                )
                task.total_tool_calls += 1
                task.current_tool = tool_name
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
        """Wait for a specific task to complete.

        When ``message_checker`` is provided, polls every ``poll_interval``
        seconds and returns early with ``status="interrupted"`` if a steering
        message arrives. Returns a result dict (``success``, ``result``, or
        ``error``/``status`` on timeout/interrupt).
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
                result = task.adopt_writer_outcome(task.asyncio_task)
                self._results[tool_call_id] = result
                logger.info(
                    "Specific task settled",
                    task_id=task_id,
                    display_id=task.display_id,
                    terminal_status=task.terminal_status,
                )
                return result
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

        Returns a dict mapping tool_call_id to result. Still-running tasks
        on interrupt get ``status="interrupted"``.

        Workflow-owned children (``owner_task_id`` set) are excluded: their
        driver waits on them; the turn only waits on the run task itself.
        """
        async with self._lock:
            tasks_to_wait = {
                tool_call_id: task.asyncio_task
                for tool_call_id, task in self._tasks.items()
                if not task.completed
                and task.asyncio_task is not None
                and task.is_turn_visible
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
                    results[tool_call_id] = task.adopt_writer_outcome(asyncio_task)
                    logger.info(
                        "Background task settled",
                        tool_call_id=tool_call_id,
                        terminal_status=task.terminal_status,
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

    async def stamp_cancel_intent(self, tasks: list["BackgroundTask"]) -> None:
        """Best-effort durable cancel intent for ledgered tasks, stamped
        BEFORE their writers are signalled: a worker that dies mid-unwind
        must recover as `cancelled`, not `worker_lost`. Ledger failure —
        including a hung call — never blocks the local cancellation (fail
        open, bounded wait — cancel is user-facing)."""
        ledger = self.run_ledger
        if ledger is None:
            return
        targets = [t for t in tasks if t.task_run_id]
        if not targets:
            return

        async def _stamp_one(task: "BackgroundTask") -> None:
            try:
                await ledger.request_task_run_cancel(task.task_run_id)
            except Exception:
                logger.warning(
                    "subagent_cancel_intent_stamp_failed",
                    task_id=task.task_id,
                    task_run_id=task.task_run_id,
                    exc_info=True,
                )

        try:
            await asyncio.wait_for(
                asyncio.gather(*(_stamp_one(t) for t in targets)),
                timeout=_CANCEL_INTENT_STAMP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "subagent_cancel_intent_stamp_timed_out",
                task_count=len(targets),
                timeout=_CANCEL_INTENT_STAMP_TIMEOUT_S,
            )

    def _cancellable(self, task: "BackgroundTask") -> bool:
        """Not-completed with a live writer OR no writer handle at all — the
        latter is a STARTING task (registered, spawn awaits in flight):
        stamping it seals its capture and the publish fence aborts the
        pending writer. A done handle on a not-completed task is a finished
        writer whose done-callback hasn't settled it yet — leave that to
        settle as what it actually was."""
        if task.completed:
            return False
        return task.asyncio_task is None or not task.asyncio_task.done()

    def _cancel_marked(
        self,
        intent_targets: list["BackgroundTask"],
        *,
        force: bool,
        reason: str = "Cancelled",
    ) -> int:
        """Cancel + mark exactly the re-validated stamp targets. Caller holds
        ``self._lock``. Only stamped targets: a task registered during the
        unlocked stamp window is either stamped or not cancelled — never
        locally cancelled without durable intent (a worker dying mid-unwind
        must recover as cancelled, not worker_lost)."""
        cancelled = 0
        for task in intent_targets:
            if self._tasks.get(task.tool_call_id) is not task:
                continue
            if not self._cancellable(task):
                continue
            # A STARTING task — registered but its spawn still awaiting setup
            # (admission/meta/opener) — reaches here with no writer to cancel,
            # and gets stamped anyway: the stamp seals its capture and the
            # publish fence (``publish_writer``) turns the pending spawn into
            # an abort. Without it the task would be classified writer-less,
            # dropped, and its later-spawned writer would run to completion
            # with every append silently discarded.
            task.force_cancel(reason, cancel_handler=force)
            cancelled += 1
        return cancelled

    async def cancel_all(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks; returns the count cancelled."""
        async with self._lock:
            intent_targets = [t for t in self._tasks.values() if self._cancellable(t)]
        await self.stamp_cancel_intent(intent_targets)
        async with self._lock:
            cancelled = self._cancel_marked(intent_targets, force=force)

        if cancelled > 0:
            logger.info("Cancelled background tasks", count=cancelled, force=force)

        return cancelled

    async def cancel_task(self, task_id: str, *, force: bool = False) -> bool:
        """Cancel one live task by its short id (user-targeted stop).

        Same stamp-then-cancel flow as ``cancel_all``; the entry stays
        registered — its turn collector still claims and drains it.
        """
        async with self._lock:
            intent_targets = [
                t
                for t in self._tasks.values()
                if t.task_id == task_id and self._cancellable(t)
            ]
        if not intent_targets:
            return False
        await self.stamp_cancel_intent(intent_targets)
        async with self._lock:
            cancelled = self._cancel_marked(intent_targets, force=force)
        if cancelled:
            logger.info("Cancelled background task by request", task_id=task_id)
        return cancelled > 0

    async def cancel_run_tasks(self, run_id: str, *, force: bool = False) -> int:
        """Cancel and drop only the tasks spawned by ``run_id``.

        Run-scoped teardown for a run that finalized error/cancelled with no
        collector: thread-wide ``cancel_all`` here would abort another turn's
        orphan collector mid-collection. Tasks with an unknown spawned_run_id
        are left alone — killing work whose owner is ambiguous is the failure
        mode this exists to prevent.
        """
        async with self._lock:
            intent_targets = [
                t
                for t in self._tasks.values()
                if t.spawned_run_id == run_id and self._cancellable(t)
            ]
        await self.stamp_cancel_intent(intent_targets)
        scoped: list[str] = []
        async with self._lock:
            cancelled = self._cancel_marked(intent_targets, force=force)
            scoped = [
                tool_call_id
                for tool_call_id, task in self._tasks.items()
                if task.spawned_run_id == run_id
            ]
            # Snapshot the writers before dropping entries: a cancelled task
            # keeps unwinding (checkpoint writes in cleanup sections) after
            # cancel() returns, and the writer-guard tail drain discovers
            # writers THROUGH this registry — removing a live one would let
            # the run's pinned session release out from under it.
            unwinding = [
                t
                for tool_call_id in scoped
                if (task := self._tasks.get(tool_call_id)) is not None
                for t in (task.asyncio_task, task.handler_task)
                if t is not None and not t.done()
            ]
        if unwinding:
            await asyncio.wait(unwinding, timeout=CANCEL_UNWIND_TIMEOUT)
        # No collector will ever claim these entries — drop them so the
        # registry doesn't grow across turns on a long-lived thread. A task
        # whose writers are STILL alive after the bounded wait stays
        # registered (drain-visible); the guard drain's own deadline is the
        # backstop for a writer that never dies.
        async with self._lock:
            for tool_call_id in scoped:
                task = self._tasks.get(tool_call_id)
                if task is None:
                    continue
                if not task.completed:
                    # Registered during the stamp window — deliberately left
                    # uncancelled (no durable intent), so not ours to evict.
                    continue
                if any(
                    t is not None and not t.done()
                    for t in (task.asyncio_task, task.handler_task)
                ):
                    logger.warning(
                        "Cancelled background task still unwinding; left "
                        "registered for the guard drain",
                        run_id=run_id,
                        tool_call_id=tool_call_id,
                    )
                    self._remove_when_settled(tool_call_id, task)
                    continue
                # Evict under the SAME lock as the done-check: a resume that
                # steals the entry between check and eviction would otherwise
                # have its live writer removed by this stale sweep.
                self._remove_entry_unlocked(tool_call_id)

        if cancelled > 0:
            logger.info(
                "Cancelled run-scoped background tasks",
                run_id=run_id,
                count=cancelled,
                force=force,
            )
        return cancelled

    async def cancel_owner_children(self, owner_task_id: str, *, reason: str) -> int:
        """Cancel one owner's live children — the teardown a workflow run does
        when it ends before its children do.

        Owner-scoped cancel belongs here for the reason ``claim_owner_children``
        does: the eligibility check and the act must not be separable, and the
        predicate is the registry's (a finished writer awaiting its
        done-callback settles as what it was, not as cancelled). Entries stay
        registered — unlike ``cancel_run_tasks`` the owning turn is still live
        and its collector still drains them. Always forceful: the owner is gone,
        so a shielded handler left running has nothing to report to.
        """
        async with self._lock:
            intent_targets = [
                t
                for t in self._tasks.values()
                if t.owner_task_id == owner_task_id and self._cancellable(t)
            ]
        if not intent_targets:
            return 0
        await self.stamp_cancel_intent(intent_targets)
        async with self._lock:
            cancelled = self._cancel_marked(
                intent_targets, force=True, reason=reason
            )
        if cancelled > 0:
            logger.info(
                "Cancelled owner-scoped background tasks",
                owner_task_id=owner_task_id,
                count=cancelled,
                reason=reason,
            )
        return cancelled

    async def remove_task_if_owned(
        self, tool_call_id: str, response_id: str
    ) -> bool:
        """Evict only while the caller's collector claim still holds. A
        resume steals the entry back (clears ``collector_response_id``), and
        the check must share the lock with the eviction — a stale collector
        racing the steal would otherwise evict the live resumed writer."""
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if task is None or task.collector_response_id != response_id:
                return False
            self._remove_entry_unlocked(tool_call_id)
            return True

    async def discard_unstarted(self, tool_call_id: str) -> bool:
        """Evict an entry whose id never reached a caller.

        ``mark_never_started`` keeps a refused entry registered so nothing
        re-launches it, which only means something for an id someone holds. A
        direct dispatch mints its own and raises instead of returning it, so
        the entry is unreachable — and with no writer, capture or usage no
        collector claims it away either, leaving its prompt pinned for the
        life of the thread. Anything that settled some other way is refused,
        so a stop mid-spawn keeps its entry for the guard drain.
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if task is None or task.terminal_status != "never_started":
                return False
            if self._has_collectible_work(task):
                return False
            self._remove_entry_unlocked(tool_call_id)
            return True

    def _remove_entry_unlocked(self, tool_call_id: str) -> None:
        task = self._tasks.pop(tool_call_id, None)
        if task is None:
            return
        self._task_id_to_tool_call_id.pop(task.task_id, None)
        self._results.pop(tool_call_id, None)

    def _remove_when_settled(self, tool_call_id: str, task) -> None:
        """A cancelled entry retained for the guard drain must still leave
        the registry once its writers finally settle, or a long-lived thread
        leaks one entry per slow unwind. Identity-checked under the lock so
        a re-registration of the same tool_call_id is never removed."""
        writers = [
            t for t in (task.asyncio_task, task.handler_task) if t is not None
        ]

        async def _late_remove() -> None:
            try:
                await asyncio.wait(writers)
            except Exception:
                pass
            async with self._lock:
                if self._tasks.get(tool_call_id) is task:
                    self._remove_entry_unlocked(tool_call_id)

        reaper = asyncio.create_task(
            _late_remove(), name=f"bg-task-late-remove-{tool_call_id[:8]}"
        )
        self._late_removals.add(reaper)
        reaper.add_done_callback(self._late_removals.discard)

    def _clear_unlocked(self) -> None:
        """Drop all task/result/lookup state. Caller owns concurrency control."""
        self._tasks.clear()
        self._task_id_to_tool_call_id.clear()
        self._results.clear()
        logger.debug("Cleared background task registry")

    def clear(self) -> None:
        """Clear all tasks and results from the registry (synchronous).

        Note: This does NOT cancel running tasks. Call cancel_all() first
        if you want to stop running tasks.

        Intentionally lock-free: called by the orchestrator after
        wait_for_all() completes, when no concurrent modifications are
        possible. For the stop teardown path — which CAN race concurrent
        registry reads — use ``clear_locked`` instead.
        """
        self._clear_unlocked()

    async def clear_locked(self) -> None:
        """Lock-held variant of ``clear`` for the stop teardown path.

        The single-owner teardown wipes the registry while a concurrent drain
        / collector may still be reading it, so this acquires the registry lock
        the orchestrator path can safely skip.
        """
        async with self._lock:
            self._clear_unlocked()

    def has_pending_tasks(self) -> bool:
        """Return True if any tasks are still pending (synchronous)."""
        return any(task.is_pending for task in self._tasks.values())

    @property
    def task_count(self) -> int:
        """Get the number of registered tasks."""
        return len(self._tasks)

    @property
    def pending_count(self) -> int:
        """Get the number of pending tasks."""
        return sum(1 for task in self._tasks.values() if task.is_pending)
