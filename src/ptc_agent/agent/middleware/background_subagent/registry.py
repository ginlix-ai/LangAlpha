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
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.utils import MessageChecker

logger = structlog.get_logger(__name__)


# Default cap for the in-memory hot tail. Older events are spilled to Redis
# so the in-memory footprint of a long-running subagent stays bounded
# regardless of workflow length. Overridable via config.
DEFAULT_TAIL_MAX_EVENTS = 1000

# Per-call cap for the durable Redis spill on the subagent hot path. A healthy
# pipeline acks in <10ms; this cap bounds the worst case so a degraded Redis
# can't pace subagent execution. After one timeout/failure the per-task circuit
# stays open for the rest of the run (see ``_spill_record_to_redis``).
_SPILL_TIMEOUT_SECONDS = 0.5

# Event-type marker for the per-task stream-end sentinel. The producer writes
# one of these via ``append_sentinel_to_stream`` when the subagent finishes
# streaming; the per-task SSE consumer treats it as "drain complete" and exits.
# Shared between producer (registry) and consumer (stream_from_log) so the
# string lives in exactly one place.
SUBAGENT_STREAM_END_EVENT = "subagent_stream_end"


def _estimate_record_bytes(record: dict[str, Any]) -> int:
    """Cheap upper-bound estimate of a captured-event record's serialized size.

    Used purely for telemetry — never on the hot path's blocking section.
    Falls back to a conservative constant if json.dumps trips on something.
    """
    try:
        return len(json.dumps(record, ensure_ascii=False, default=str))
    except Exception:
        return 256


def _resolve_tail_maxlen() -> int:
    """Read the configured tail size, falling back to the module default.

    Called by ``register()`` on every task creation so the runtime tail
    cap reflects ``in_memory_event_tail_max_events`` from config. Pulled
    into a function (rather than evaluated at import time) so the registry
    doesn't force a config import at module load, and so test fixtures
    can ``monkeypatch`` the settings module between tasks.
    """
    try:
        from src.config.settings import get_in_memory_event_tail_max_events

        value = int(get_in_memory_event_tail_max_events())
        return value if value > 0 else DEFAULT_TAIL_MAX_EVENTS
    except Exception:
        return DEFAULT_TAIL_MAX_EVENTS


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

    captured_events_tail: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=DEFAULT_TAIL_MAX_EVENTS)
    )
    """Bounded in-memory hot tail of captured SSE-shaped events.

    Each entry is a self-contained record::

        {"seq": int, "event": str, "data": dict, "agent_id": str | None}

    where ``seq`` starts at 1 and is monotonic. Older events that fall off the
    tail are still available via the Redis spill — see
    ``BackgroundTaskRegistry.append_captured_event`` and the per-task Redis key
    ``subagent:events:{thread_id}:{task_id}``.
    """

    captured_event_seq: int = 0
    """High-water mark for ``captured_events_tail`` ``seq`` values. The next
    appended record gets ``captured_event_seq + 1``."""

    captured_event_count: int = 0
    """Total events ever captured (== ``captured_event_seq`` once monotonic).
    Tracked separately so cleanup can drop the tail without wiping the count
    used for sort ordering and progress checks."""

    captured_event_bytes: int = 0
    """Cumulative bytes captured (telemetry only; estimated)."""

    redis_write_failed: bool = False
    """Set if any Redis spill failed for this task. Telemetry only — degraded
    mode still keeps streaming working via the in-memory tail."""

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
    The collector awaits this before clearing the captured-event tail so that
    live SSE consumers are guaranteed to have emitted all events."""

    sse_consumer_count: int = 0
    """Number of active SSE consumers for this task. sse_drain_complete is
    only set when the last consumer finishes, preventing the collector from
    clearing the captured-event tail while another consumer is still draining."""

    redis_spill_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    """Per-task lock that serializes Redis spills so concurrent appends to
    the same task can't interleave RPUSH commands. Without this, two appends
    that release the registry-wide lock back-to-back can hit different
    Redis pool connections and land at the server in reverse order — the
    Redis list ends up out of seq order, and both the post-turn collector
    and SSE reconnect replay yield events in the wrong sequence. We keep
    this off the registry-wide lock so a slow Redis blip on one task can't
    stall appends to other tasks."""

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
                ``subagent:events:{thread_id}:{task_id}`` keys. Empty string
                disables Redis spill (used in tests).
        """
        self._tasks: dict[str, BackgroundTask] = {}
        self._task_id_to_tool_call_id: dict[str, str] = {}  # task_id -> tool_call_id
        self._ns_uuid_to_tool_call_id: dict[
            str, str
        ] = {}  # LangGraph namespace UUID -> tool_call_id
        self._lock = asyncio.Lock()
        self._results: dict[str, Any] = {}
        self.current_turn_index: int = 0
        self.thread_id: str = thread_id

    async def register(
        self,
        tool_call_id: str,
        description: str,
        prompt: str,
        subagent_type: str,
        asyncio_task: asyncio.Task | None = None,
    ) -> BackgroundTask:
        """Register a new background task and return it."""
        async with self._lock:
            # Generate short alphanumeric task_id
            task_id = secrets.token_urlsafe(4)[:6]

            agent_id = f"{subagent_type}:{uuid_mod.uuid4()}"
            tail_maxlen = _resolve_tail_maxlen()
            task = BackgroundTask(
                tool_call_id=tool_call_id,
                task_id=task_id,
                description=description,
                prompt=prompt,
                subagent_type=subagent_type,
                asyncio_task=asyncio_task,
                agent_id=agent_id,
                spawned_turn_index=self.current_turn_index,
                captured_events_tail=deque(maxlen=tail_maxlen),
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

    async def get_task_by_task_id(self, task_id: str) -> BackgroundTask | None:
        """Alias for get_by_task_id, used by the HTTP layer."""
        return await self.get_by_task_id(task_id)

    def get_by_tool_call_id(self, tool_call_id: str) -> BackgroundTask | None:
        """Return the task for a given tool_call_id (synchronous, no lock)."""
        return self._tasks.get(tool_call_id)

    def register_namespace(self, checkpoint_ns: str, tool_call_id: str) -> None:
        """Map each LangGraph UUID in checkpoint_ns to tool_call_id for streaming lookup."""
        for element in checkpoint_ns.split("|"):
            parts = element.split(":", 1)
            if len(parts) == 2:
                ns_uuid = parts[1]
                self._ns_uuid_to_tool_call_id[ns_uuid] = tool_call_id

    def get_task_by_namespace(self, ns_element: str) -> BackgroundTask | None:
        """Return the task for a namespace element like 'tools:uuid', or None."""
        parts = ns_element.split(":", 1)
        if len(parts) == 2:
            ns_uuid = parts[1]
            tool_call_id = self._ns_uuid_to_tool_call_id.get(ns_uuid)
            if tool_call_id:
                return self._tasks.get(tool_call_id)
        return None

    def clear_namespaces_for_task(self, tool_call_id: str) -> None:
        """Remove stale namespace UUID mappings so resumed invocations can register fresh ones."""
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

        Called by SubagentEventCaptureMiddleware (and steering) to capture
        events for per-task SSE replay and post-interrupt persistence. The
        record is appended to the bounded in-memory tail and (best-effort)
        spilled to Redis so older events stay reachable for reconnect /
        full-history collectors.
        """
        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if not task:
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

            task.captured_events_tail.append(record)
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

    async def _spill_record_to_redis(
        self, task: BackgroundTask, record: dict[str, Any]
    ) -> None:
        """Best-effort spill of one captured record to Redis.

        Writes a JSON record to the per-task List (consumed post-turn by
        ``iter_subagent_events_full``) and a pre-rendered SSE string to the
        per-task Stream (consumed live by SSE clients) in one atomic
        pipeline. Failure flips ``task.redis_write_failed`` (sticky
        circuit-break) and is silently logged — never raised — so live SSE
        via the in-memory tail is unaffected. Returns silently when the
        circuit-break is set, the registry has no thread_id (test
        fixtures), the spill flag is off, or the cache client is
        unavailable.
        """
        if task.redis_write_failed:
            return

        if not self.thread_id:
            return

        # Lazy import to avoid circular imports during test collection.
        try:
            from src.config.settings import (
                get_max_stored_messages_per_agent,
                get_redis_ttl_workflow_events,
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
        events_key = f"subagent:events:{self.thread_id}:{task.task_id}"
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

        # Pre-render the SSE wire format for the Stream so the consumer can
        # yield bytes verbatim — no JSON-decode + re-render branch in the read
        # path. The List still stores JSON because the post-turn collector
        # (``iter_subagent_events_full``) expects records.
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
            async with task.redis_spill_lock:
                # The spool is the durable record used by the post-turn
                # collector to rebuild the full subagent event history for
                # ``conversation_responses.sse_events``. It must use the
                # same cap and TTL as the main-workflow buffer — the
                # per-task replay buffer cap sized for SSE reconnect would
                # silently drop events for any subagent that runs longer
                # than the cap.
                success, _seq = await asyncio.wait_for(
                    cache.pipelined_event_buffer(
                        events_key=events_key,
                        meta_key=meta_key,
                        event=payload,
                        max_size=get_max_stored_messages_per_agent(),
                        ttl=get_redis_ttl_workflow_events(),
                        last_event_id=record.get("seq"),
                        stream_key=stream_key,
                        stream_event=stream_payload,
                    ),
                    timeout=_SPILL_TIMEOUT_SECONDS,
                )
            if not success:
                task.redis_write_failed = True
                logger.warning(
                    "subagent_event_spill_failed",
                    phase="pipeline",
                    tool_call_id=task.tool_call_id,
                    task_id=task.task_id,
                    seq=record.get("seq"),
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
        if not self.thread_id:
            return

        async with self._lock:
            task = self._tasks.get(tool_call_id)
            if not task:
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
                        maxlen=get_max_stored_messages_per_agent(),
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
                tool_call_id=tool_call_id,
                task_id=task.task_id,
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

    async def cancel_all(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks; returns the count cancelled."""
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
