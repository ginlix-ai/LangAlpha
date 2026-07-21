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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.utils import MessageChecker

logger = structlog.get_logger(__name__)


# Per-call cap for the durable Redis spill on the subagent hot path. A healthy
# pipeline acks in <10ms; this cap bounds the worst case so a degraded Redis
# can't pace subagent execution. ``asyncio.wait_for`` measures wall-clock
# INCLUDING event-loop scheduling delay, so the cap must absorb loop stalls
# (checkpoint serialization, large json.dumps) — 0.5s fired on a healthy Redis
# and killed a run whose write had actually landed. On timeout the stream tail
# is verified (landed ⇒ continue) and the write retried once before the
# per-task circuit opens for the rest of the run (see ``_spill_record_to_redis``).
_SPILL_TIMEOUT_SECONDS = 3.0

# Bounded wait for a cancelled task's unwind before its registry entry drops
# (normal unwind is milliseconds; see ``cancel_run_tasks``).
CANCEL_UNWIND_TIMEOUT = 15.0

# Cap on the pre-signal durable cancel-intent stamp (``_stamp_cancel_intent``):
# a hung ledger call must not block the user-facing local cancel.
_CANCEL_INTENT_STAMP_TIMEOUT_S = 2.0

# Event-type marker for the per-task stream-end sentinel. The producer writes
# one of these via ``append_sentinel_to_stream`` when the subagent finishes
# streaming; the per-task SSE consumer treats it as "drain complete" and exits.
# Shared between producer (registry) and consumer (stream_from_log) so the
# string lives in exactly one place.
SUBAGENT_STREAM_END_EVENT = "subagent_stream_end"


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


def _estimate_record_bytes(record: dict[str, Any]) -> int:
    """Cheap upper-bound estimate of a captured-event record's serialized size.

    Used purely for telemetry — never on the hot path's blocking section.
    Falls back to a conservative constant if json.dumps trips on something.
    """
    try:
        return len(json.dumps(record, ensure_ascii=False, default=str))
    except Exception:
        return 256


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

    last_checked_at: float = field(default_factory=time.time)
    """Epoch seconds. Bumped whenever the agent inspects this task via the
    Task tool (status/list/update/resume/cancel actions) or via TaskOutput.
    Surfaced to the LLM so it can gauge how recently it polled, independent
    of whether anything changed."""

    last_updated_at: float = field(default_factory=time.time)
    """Epoch seconds. Bumped only on meaningful transitions:

    - Task completion (via asyncio done_callback, covers success / failure /
      cancellation).
    - Explicit ``cancelled = True``.
    - A follow-up message queued via the ``update`` action.
    - A user-visible text ``message_chunk`` event is captured.

    Reasoning, reasoning-signal, tool_calls, and tool_call_result events
    are deliberately excluded — they're high-volume pacing noise. The
    OrphanCollector liveness check falls back to ``cur_events > prev_events``
    for tool-only progression, so idle detection still works."""

    agent_id: str = ""
    """Stable unique identity: '{subagent_type}:{uuid4}'."""

    captured_event_seq: int = 0
    """Monotonic seq counter. Each captured event gets ``captured_event_seq + 1``;
    the value is also the XADD entry ID (``<seq>-0``) on the per-task stream."""

    captured_event_count: int = 0
    """Total events ever captured (== ``captured_event_seq`` once monotonic).
    Tracked separately so it can survive resets that re-zero ``captured_event_seq``
    if that ever happens (currently they move in lock-step)."""

    captured_event_bytes: int = 0
    """Cumulative bytes captured (telemetry only; estimated)."""

    redis_write_failed: bool = False
    """Set if any Redis spill failed for this task. Telemetry only — degraded
    mode still keeps streaming working via the in-memory tail."""

    cancelled: bool = False
    """Whether the task was explicitly cancelled (distinct from completed with error)."""

    spawned_turn_index: int = 0
    """The turn_index of the parent turn that spawned this subagent."""

    spawned_run_id: str | None = None
    """The run_id of the parent turn that spawned this subagent. Set from
    ``registry.current_run_id`` at register time. Collectors filter by this
    so subagents from prior turns can't get claimed by a later turn's
    collector after the registry is reused across turns."""

    task_run_id: str | None = None
    """This execution's ledger identity (subagent_runs row). Stamped by the
    middleware after the admission INSERT, re-stamped on every resume (a
    resume is a NEW run). None when no ledger is injected (CLI/tests) or
    for pre-ledger launches."""

    per_call_records: list[dict[str, Any]] = field(default_factory=list)
    """Token usage records collected when subagent completes."""

    tool_usage: dict[str, int] = field(default_factory=dict)
    """Billing-keyed infrastructure tool usage (e.g. "TavilySearchTool:deep" → 2),
    snapshotted from the per-task ToolUsageTracker when the subagent completes."""

    collector_response_id: str | None = None
    """Response ID of the collector that claimed this task for persistence.
    Set atomically during the _mark_completed filter to prevent two collectors
    from persisting the same subagent events to different response_ids."""

    sse_drain_complete: asyncio.Event = field(default_factory=asyncio.Event)
    """Set by stream_subagent_from_log after its final drain.
    The collector awaits this before clearing the captured-event tail so that
    live SSE consumers are guaranteed to have emitted all events."""

    sse_consumer_count: int = 0
    """Number of active SSE consumers for this task. sse_drain_complete is
    only set when the last consumer finishes, preventing the collector from
    clearing the captured-event tail while another consumer is still draining."""

    redis_spill_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    """Per-task lock that serializes XADD writes so concurrent appends to
    the same task can't interleave commands. Without this, two appends that
    release the registry-wide lock back-to-back can hit different Redis
    pool connections and land at the server in reverse order, breaking the
    explicit ``<seq>-0`` ordering on the stream. Off the registry-wide lock
    so a slow Redis blip on one task can't stall appends to other tasks."""

    @property
    def display_id(self) -> str:
        """Return Task-<id> format for display."""
        return f"Task-{self.task_id}"

    @property
    def is_pending(self) -> bool:
        """True if the task is still running or registered but not yet started."""
        if self.completed:
            return False
        if self.asyncio_task is None:
            return True  # Registered but not yet started
        return not self.asyncio_task.done()


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
        # (thread_id, task_id) -> durable result text, injected by the server
        # (checkpoint-backed). None in CLI/tests — delivery then falls back to
        # the in-memory handler result.
        self.result_resolver: (
            Callable[[str, str], Awaitable[str | None]] | None
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

    async def resolve_result_text(self, task_id: str) -> str | None:
        """Derive a task's result text from its durable archive.

        The registry entry is volatile (evicted after collection, wiped on
        stop/restart, absent on other workers) while the subagent's answer is
        checkpointed under ``task:{task_id}`` — the resolver reads the latter,
        so delivery survives the registry. Never raises; None means "nothing
        archived / no resolver", and callers fall back to in-memory state.
        """
        if self.result_resolver is None:
            return None
        try:
            return await self.result_resolver(self.thread_id, task_id)
        except Exception:
            logger.warning(
                "Durable result resolve failed; falling back to in-memory",
                task_id=task_id,
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
        """Return all registered tasks."""
        async with self._lock:
            return list(self._tasks.values())

    async def get_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Return the task for a given 6-char task_id, or None."""
        async with self._lock:
            tool_call_id = self._task_id_to_tool_call_id.get(task_id)
            if tool_call_id:
                return self._tasks.get(tool_call_id)
            return None

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
                or task.cancelled
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

            task.captured_event_count = seq
            task.captured_event_bytes += _estimate_record_bytes(record)
            # Bump last_updated_at only on user-visible text output.
            # reasoning_signal / reasoning / tool_calls / tool_call_result
            # events are excluded — they're pacing noise.
            if (
                event.get("event") == "message_chunk"
                and (event.get("data") or {}).get("content_type") == "text"
            ):
                task.last_updated_at = time.time()

        # Spill OUTSIDE the lock — Redis I/O must not block subsequent appends.
        await self._spill_record_to_redis(task, record)

    @staticmethod
    async def _stream_tail_seq(cache: Any, stream_key: str, field: bytes) -> int | None:
        """Seq of the newest entry in a spill stream, or None.

        ``wait_for`` cancels the awaiting coroutine on timeout, but the
        command may already be on the wire — the tail decides landed
        (continue) vs lost (retry, then circuit).
        """
        try:
            entries = await asyncio.wait_for(
                cache.client.xrevrange(stream_key, count=1),
                timeout=_SPILL_TIMEOUT_SECONDS,
            )
            if not entries:
                return None
            raw = (entries[0][1] or {}).get(field)
            if raw is None:
                return None
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")
            return int(json.loads(raw)["seq"])
        except Exception:
            return None

    async def _spill_record_to_redis(
        self, task: BackgroundTask, record: dict[str, Any]
    ) -> None:
        """Best-effort spill of one captured record to the per-task Stream.

        Writes a single XADD entry with two fields: ``b"event"`` (pre-rendered
        SSE wire string, consumed live by SSE clients) and ``b"record"``
        (JSON record, consumed post-turn by ``iter_subagent_events_full``
        via XRANGE). A timed-out write is verified against the stream tail
        (landed ⇒ continue) and retried once; only then does failure flip
        ``task.redis_write_failed`` (sticky circuit-break), silently logged —
        never raised. Returns
        silently when the circuit-break is set, the registry has no
        thread_id (test fixtures), the spill flag is off, or the cache
        client is unavailable.
        """
        if task.redis_write_failed:
            return

        if not self.thread_id:
            return

        # Lazy import to avoid circular imports during test collection.
        try:
            from src.config.settings import (
                get_max_stored_messages_per_agent,
                is_subagent_event_redis_spill_enabled,
            )
        except Exception:
            return

        try:
            if not is_subagent_event_redis_spill_enabled():
                return
        except Exception:
            return

        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
        except Exception as exc:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="cache_init",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                error=str(exc),
            )
            return

        if not getattr(cache, "enabled", False):
            return

        # Records are JSON-serialized ``{"seq", "event", "data", "agent_id", "ts"}`` dicts.
        meta_key = f"subagent:events:meta:{self.thread_id}:{task.task_id}"
        stream_key = f"subagent:stream:{self.thread_id}:{task.task_id}"

        try:
            payload = json.dumps(record, ensure_ascii=False, default=str)
        except Exception as exc:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="serialize",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                error=str(exc),
            )
            return

        # Pre-render the SSE wire format for the Stream so the live consumer
        # can yield bytes verbatim — no JSON-decode + re-render branch in the
        # read path. The post-turn collector (``iter_subagent_events_full``)
        # reads the parallel ``b"record"`` field via XRANGE.
        try:
            seq = int(record.get("seq") or 0)
            data = {
                **(record.get("data") or {}),
                "thread_id": self.thread_id,
                "agent": f"task:{task.task_id}",
            }
            stream_payload = (
                f"id: {seq}\n"
                f"event: {record.get('event') or 'message_chunk'}\n"
                f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
            )
        except Exception as exc:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="render_sse",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                error=str(exc),
            )
            return

        # Serialize spills per task. The registry-wide lock is released
        # before this call so multiple tasks can spill in parallel; the
        # per-task lock guarantees that for any two appends to the SAME
        # task, the second's pipeline cannot start until the first's
        # pipeline has acked at Redis. Without this, two appends that
        # acquired distinct seq numbers can race to the server via
        # different pool connections and land out of order.
        try:
            quota = get_max_stored_messages_per_agent()
            async with task.redis_spill_lock:
                # XADD carries both the pre-rendered SSE wire string
                # (``b"event"``, consumed live by ``stream_subagent_from_log``)
                # and the JSON record (``b"record"``, consumed post-turn by
                # ``iter_subagent_events_full`` via XRANGE). Active streams
                # carry no TTL (retention contract — the attach-grace TTL is
                # stamped at terminal by ``stamp_terminal_retention``), and
                # MAXLEN is a 2x backstop: the quota check below opens the
                # circuit before FIFO trim could touch the head.
                success = False
                seq_count: int | None = None
                for attempt in (1, 2):
                    try:
                        success, seq_count = await asyncio.wait_for(
                            cache.pipelined_event_buffer(
                                meta_key=meta_key,
                                max_size=quota * 2,
                                ttl=None,
                                last_event_id=record.get("seq"),
                                stream_key=stream_key,
                                stream_event=stream_payload,
                                stream_record=payload,
                            ),
                            timeout=_SPILL_TIMEOUT_SECONDS,
                        )
                        break
                    except asyncio.TimeoutError:
                        # The cancelled pipeline may already have executed
                        # server-side; a landed write must not tear the run.
                        # seq_count=None skips this record's quota check —
                        # the next spill re-evaluates it.
                        if (
                            await self._stream_tail_seq(cache, stream_key, b"record")
                            == seq
                        ):
                            success, seq_count = True, None
                            logger.info(
                                "subagent_event_spill_recovered",
                                phase="v1_timeout_landed",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                seq=seq,
                            )
                            break
                        if attempt == 1:
                            logger.warning(
                                "subagent_event_spill_retry",
                                phase="v1_timeout",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                seq=seq,
                                timeout_seconds=_SPILL_TIMEOUT_SECONDS,
                            )
                            continue
                        raise
                # v2 dual-write (STREAM_CONTRACT_V2.md): the immutable
                # per-run stream, keyed by ledger identity. Same lock hold so
                # per-run frame order matches append order; seq is the XADD
                # id (Redis-side). Contract-grade: a hole in the canonical
                # per-run stream opens the circuit like a v1 failure — the
                # run tears as error(transport_lost) rather than a reader
                # ever being served a stream with a silent gap.
                if success and task.task_run_id:
                    v2_key = f"subagent:stream:{self.thread_id}:{task.task_run_id}"
                    # No per-write TTL: the immutable per-run stream is
                    # active until terminal, when stamp_terminal_retention
                    # applies the attach-grace TTL.
                    v2_fields = {
                        b"run_id": task.task_run_id.encode(),
                        b"lane": f"task:{task.task_id}".encode(),
                        b"type": (record.get("event") or "message_chunk").encode(),
                        b"payload": payload.encode("utf-8"),
                    }
                    for attempt in (1, 2):
                        try:
                            await asyncio.wait_for(
                                cache.client.xadd(v2_key, v2_fields),
                                timeout=_SPILL_TIMEOUT_SECONDS,
                            )
                            break
                        except asyncio.TimeoutError:
                            if (
                                await self._stream_tail_seq(cache, v2_key, b"payload")
                                == seq
                            ):
                                logger.info(
                                    "subagent_event_spill_recovered",
                                    phase="v2_timeout_landed",
                                    tool_call_id=task.tool_call_id,
                                    task_id=task.task_id,
                                    task_run_id=task.task_run_id,
                                    seq=seq,
                                )
                                break
                            if attempt == 1:
                                logger.warning(
                                    "subagent_event_spill_retry",
                                    phase="v2_timeout",
                                    tool_call_id=task.tool_call_id,
                                    task_id=task.task_id,
                                    task_run_id=task.task_run_id,
                                    seq=seq,
                                    timeout_seconds=_SPILL_TIMEOUT_SECONDS,
                                )
                                continue
                            task.redis_write_failed = True
                            logger.warning(
                                "subagent_event_spill_failed",
                                phase="v2_pipeline",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                task_run_id=task.task_run_id,
                                seq=record.get("seq"),
                            )
                            break
                        except Exception:
                            task.redis_write_failed = True
                            logger.warning(
                                "subagent_event_spill_failed",
                                phase="v2_pipeline",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                task_run_id=task.task_run_id,
                                seq=record.get("seq"),
                            )
                            break
            if not success:
                task.redis_write_failed = True
                logger.warning(
                    "subagent_event_spill_failed",
                    phase="pipeline",
                    tool_call_id=task.tool_call_id,
                    task_id=task.task_id,
                    seq=record.get("seq"),
                )
            elif int(seq_count or 0) > quota:
                # Quota breach tears the transport by contract: opening the
                # circuit here (instead of trimming FIFO) makes the abort
                # loop + terminal escalation finalize error(transport_lost),
                # so a replay with a silent hole is never served.
                task.redis_write_failed = True
                logger.warning(
                    "subagent_event_spill_failed",
                    phase="quota",
                    tool_call_id=task.tool_call_id,
                    task_id=task.task_id,
                    seq=record.get("seq"),
                    quota=quota,
                )
        except asyncio.TimeoutError:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="timeout",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                timeout_seconds=_SPILL_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="exception",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                error=str(exc),
            )

    async def write_task_meta(self, task: BackgroundTask, status: str) -> None:
        """Best-effort mirror of the task's routing identity + writer liveness
        to Redis (``subagent:meta:{thread}:{task}``) so OTHER workers can
        resolve steer/update targets and gate resumes.

        ``status`` tracks the WRITER ("running" while an asyncio writer owns
        the namespace, "completed"/"cancelled"/"error" once it settled), not
        result availability. Advisory only — the N(thread, task:id) advisory
        lock, not this hash, is the write fence.
        """
        if not self.thread_id:
            return
        try:
            from src.config.settings import get_redis_ttl_workflow_events
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not getattr(cache, "enabled", False) or not cache.client:
                return
            key = f"subagent:meta:{self.thread_id}:{task.task_id}"
            pipe = cache.client.pipeline()
            pipe.hset(
                key,
                mapping={
                    "tool_call_id": task.tool_call_id,
                    "status": status,
                    "subagent_type": task.subagent_type,
                    "description": (task.description or "")[:200],
                    "spawned_run_id": task.spawned_run_id or "",
                    # Execution-scoped stream epoch: spawned_run_id is
                    # parent-turn-scoped and does NOT change on a same-turn
                    # resume, so epoch consumers prefer this field.
                    "task_run_id": task.task_run_id or "",
                    "updated_at": str(time.time()),
                },
            )
            pipe.expire(key, get_redis_ttl_workflow_events())
            await asyncio.wait_for(pipe.execute(), timeout=_SPILL_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.warning(
                "task meta write failed",
                task_id=task.task_id,
                status=status,
                error=str(exc),
            )

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
        if not self.thread_id:
            return

        if task.redis_write_failed:
            return

        # Defensive guard: settings/cache imports are stable in normal
        # operation, so a raise here means a broken deployment — bail
        # quietly rather than crash the producer's astream loop.
        try:
            from src.config.settings import (
                get_max_stored_messages_per_agent,
                get_redis_ttl_workflow_events,
                is_subagent_event_redis_spill_enabled,
            )
            if not is_subagent_event_redis_spill_enabled():
                return
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
        except Exception:
            return

        if not getattr(cache, "enabled", False) or not getattr(cache, "client", None):
            return

        stream_key = f"subagent:stream:{self.thread_id}:{task.task_id}"
        payload = json.dumps(
            {"event": SUBAGENT_STREAM_END_EVENT}, ensure_ascii=False
        ).encode("utf-8")

        # Hold redis_spill_lock across pipe.execute() so the sentinel cannot
        # land before an in-flight content spill on the same task. Auto-id
        # XADD ordering is only a server-side guarantee — once two pipelines
        # are both in flight, either can win the race. The per-task lock is
        # the issue-order guarantee: a concurrent content spill must finish
        # its XADD before the sentinel's pipeline opens; otherwise the
        # consumer exits on the sentinel and the late content event is lost.
        # _SPILL_TIMEOUT_SECONDS caps queue depth under load.
        #
        # ``wait_for`` timeout window: if the timeout fires *after*
        # ``pipe.execute()`` has already dispatched the commands but before
        # Redis ACKs, the sentinel XADD has already landed. The lock is then
        # released and a queued content spill will write its XADD *after* the
        # sentinel — at which point the consumer has already exited on the
        # sentinel and that late event is lost. Best-effort by design; the
        # sub-500-ms window makes it astronomically unlikely under normal
        # load, and the fallback (``terminal_check`` closes the stream once
        # the asyncio task finishes) still fires on the next BLOCK timeout.
        try:
            async with task.redis_spill_lock:
                async with cache.client.pipeline(transaction=False) as pipe:
                    pipe.xadd(
                        stream_key,
                        {b"event": payload},
                        maxlen=get_max_stored_messages_per_agent() * 2,
                        approximate=True,
                    )
                    pipe.expire(stream_key, get_redis_ttl_workflow_events())
                    await asyncio.wait_for(
                        pipe.execute(),
                        timeout=_SPILL_TIMEOUT_SECONDS,
                    )
        except Exception as exc:
            logger.debug(
                "subagent_stream_end_sentinel_failed",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                error=str(exc),
            )

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
            from src.config.settings import get_redis_ttl_workflow_events
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not getattr(cache, "enabled", False) or not cache.client:
                return
            ttl = get_redis_ttl_workflow_events()
            async with cache.client.pipeline(transaction=False) as pipe:
                pipe.expire(f"subagent:stream:{self.thread_id}:{task.task_id}", ttl)
                pipe.expire(
                    f"subagent:events:meta:{self.thread_id}:{task.task_id}", ttl
                )
                if task.task_run_id:
                    pipe.expire(
                        f"subagent:stream:{self.thread_id}:{task.task_run_id}", ttl
                    )
                await asyncio.wait_for(
                    pipe.execute(), timeout=_SPILL_TIMEOUT_SECONDS
                )
        except Exception as exc:
            logger.debug(
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

        Returns a dict mapping tool_call_id to result. Still-running tasks
        on interrupt get ``status="interrupted"``.
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

    async def _stamp_cancel_intent(self, tasks: list["BackgroundTask"]) -> None:
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

    async def cancel_all(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks; returns the count cancelled."""
        async with self._lock:
            intent_targets = [t for t in self._tasks.values() if self._cancellable(t)]
        await self._stamp_cancel_intent(intent_targets)
        cancelled = 0
        async with self._lock:
            # Only the re-validated stamp targets: a task registered during
            # the unlocked stamp window is either stamped or not cancelled —
            # never locally cancelled without durable intent (a worker dying
            # mid-unwind must recover as cancelled, not worker_lost).
            for task in intent_targets:
                if self._tasks.get(task.tool_call_id) is not task:
                    continue
                if not self._cancellable(task):
                    continue
                if task.asyncio_task is not None:
                    if force and task.handler_task and not task.handler_task.done():
                        task.handler_task.cancel()
                    task.asyncio_task.cancel()
                # else: STARTING task — no writer yet; the stamp seals the
                # capture and the publish fence aborts the pending spawn.
                task.completed = True
                task.cancelled = True
                task.error = "Cancelled"
                task.last_updated_at = time.time()
                task.result = {
                    "success": False,
                    "error": "Cancelled",
                    "status": "cancelled",
                }
                cancelled += 1

        if cancelled > 0:
            logger.info("Cancelled background tasks", count=cancelled, force=force)

        return cancelled

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
        await self._stamp_cancel_intent(intent_targets)
        scoped: list[str] = []
        cancelled = 0
        async with self._lock:
            # Cancel only the re-validated stamp targets: a task registered
            # during the unlocked stamp window is either stamped or not
            # cancelled — never locally cancelled without durable intent (a
            # worker dying mid-unwind must recover as cancelled, not
            # worker_lost).
            for task in intent_targets:
                if self._tasks.get(task.tool_call_id) is not task:
                    continue
                if not self._cancellable(task):
                    continue
                if task.asyncio_task is not None:
                    if force and task.handler_task and not task.handler_task.done():
                        task.handler_task.cancel()
                    task.asyncio_task.cancel()
                # else: STARTING task — registered but its spawn is still
                # awaiting setup (admission/meta/opener). There is no writer
                # to cancel, but the stamp seals its capture and the publish
                # fence (``publish_writer``) turns the pending spawn into an
                # abort. Without this the task would be classified
                # writer-less, dropped, and its later-spawned writer would
                # run to completion with every append silently discarded.
                task.completed = True
                task.cancelled = True
                task.error = "Cancelled"
                task.last_updated_at = time.time()
                task.result = {
                    "success": False,
                    "error": "Cancelled",
                    "status": "cancelled",
                }
                cancelled += 1
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


def steering_queue_key(tool_call_id: str, task_run_id: str | None = None) -> str:
    """Redis list a subagent drains for follow-up steering input.

    Run-scoped when the execution's ledger identity is known — a queued
    message can then never be consumed by a later resume of the same task;
    the run-end sweep returns whatever its own run left unconsumed. The
    task-lifetime key is the pre-ledger fallback (legacy semantics: next
    writer drains it).
    """
    if task_run_id:
        return f"subagent:steering:{tool_call_id}:{task_run_id}"
    return f"subagent:steering:{tool_call_id}"


def parse_steering_payload(raw: Any) -> dict[str, Any] | None:
    """Normalize a raw steering-queue entry to ``{content,
    expected_task_run_id, input_id}``. Legacy entries are bare JSON strings
    (no fence, no id); unparseable entries return None."""
    try:
        data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(data, str):
        return {"content": data, "expected_task_run_id": None, "input_id": None}
    if isinstance(data, dict) and data.get("content"):
        return {
            "content": str(data["content"]),
            "expected_task_run_id": data.get("expected_task_run_id") or None,
            "input_id": data.get("input_id") or None,
        }
    return None


async def read_task_meta(thread_id: str, task_id: str) -> dict[str, str] | None:
    """Read the cross-worker task meta hash written by ``write_task_meta``.

    Returns a decoded str->str dict, or None when the key is absent, Redis is
    unavailable, or the read fails (callers treat None as "no distributed
    knowledge" and fall back to local/checkpoint state).
    """
    if not thread_id or not task_id:
        return None
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or not cache.client:
            return None
        raw = await cache.client.hgetall(f"subagent:meta:{thread_id}:{task_id}")
        if not raw:
            return None
        return {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else str(v)
            )
            for k, v in raw.items()
        }
    except Exception as exc:
        logger.warning(
            "task meta read failed", thread_id=thread_id, task_id=task_id,
            error=str(exc),
        )
        return None
