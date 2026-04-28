"""Stream reconnection and subagent event streaming.

Provides reconnect-to-running-workflow (replays buffered events then attaches
to the live Redis queue) and per-subagent-task SSE streaming used by the
``/threads/{id}/tasks/{task_id}/events`` endpoint.
"""

from __future__ import annotations

import asyncio
import json

from fastapi import HTTPException

from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.server.services.workflow_tracker import WorkflowTracker

from src.config.settings import (
    get_live_queue_maxsize,
    get_subagent_task_max_wait,
)

from ._common import _SSE_LOG_ENABLED, _sse_logger, logger
from .steering import drain_steering_return_event


# ---------------------------------------------------------------------------
# Reconnect to a running or completed PTC workflow
# ---------------------------------------------------------------------------


async def reconnect_to_workflow_stream(
    thread_id: str,
    last_event_id: int | None = None,
):
    """
    Reconnect to a running or completed PTC workflow.

    Args:
        thread_id: Workflow thread identifier
        last_event_id: Optional last event ID for filtering duplicates

    Yields:
        SSE-formatted event strings
    """
    manager = BackgroundTaskManager.get_instance()
    tracker = WorkflowTracker.get_instance()

    # Get workflow info
    task_info = await manager.get_task_info(thread_id)
    workflow_status = await tracker.get_status(thread_id)

    if not task_info:
        if workflow_status and workflow_status.get("status") == "completed":
            raise HTTPException(
                status_code=410, detail="Workflow completed and results expired"
            )
        raise HTTPException(status_code=404, detail=f"Workflow {thread_id} not found")

    # Replay buffered events (during tailing, Redis only holds tail-phase
    # events because the buffer is cleared after pre-tail persist)
    buffered_events = await manager.get_buffered_events_redis(
        thread_id,
        from_beginning=True,
        after_event_id=last_event_id,
    )

    logger.info(
        f"[PTC_RECONNECT] Replaying {len(buffered_events)} events for {thread_id}"
    )

    for event in buffered_events:
        yield event

    # Attach to live stream if still running, tailing, or queued (dispatch
    # pre-registered but generator hasn't reached start_workflow() yet).
    # QUEUED tasks have live_queues ready — once start_workflow() upgrades
    # the TaskInfo in-place, events flow to already-subscribed queues.
    status = await manager.get_task_status(thread_id)
    if status in [TaskStatus.RUNNING, TaskStatus.QUEUED]:
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=get_live_queue_maxsize())
        await manager.subscribe_to_live_events(thread_id, live_queue)
        await manager.increment_connection(thread_id)

        try:
            while True:
                try:
                    event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                    if event is None:
                        break
                    yield event
                except asyncio.TimeoutError:
                    current_status = await manager.get_task_status(thread_id)
                    if current_status in [
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    ]:
                        break
                    continue

            # After workflow ends, return any unconsumed steering messages to the client
            steering_event = await drain_steering_return_event(thread_id)
            if steering_event:
                yield steering_event

        finally:
            await manager.unsubscribe_from_live_events(thread_id, live_queue)
            await manager.decrement_connection(thread_id)


# ---------------------------------------------------------------------------
# Per-subagent task SSE stream
# ---------------------------------------------------------------------------


async def stream_subagent_task_events(
    thread_id: str, task_id: str, last_event_id: int | None = None
):
    """SSE stream of a single subagent's content events.

    Producer-driven Redis writes: every captured event is spilled to
    ``subagent:events:{thread_id}:{task_id}`` by the registry's
    ``append_captured_event`` so this consumer never writes Redis. On
    reconnect the consumer replays the durable JSON records from Redis;
    during live streaming it drains the in-memory tail by seq cursor and
    falls back to Redis when the tail rotated past the consumer's cursor.

    Redis key: subagent:events:{thread_id}:{task_id}
    Cleared after task completion + persistence (mirrors main stream per-turn clearing).

    Args:
        thread_id: Workflow thread identifier
        task_id: The 6-char alphanumeric task identifier
        last_event_id: Last received event ID for reconnect replay

    Yields:
        SSE-formatted event strings
    """
    from src.server.services.background_task_manager import drain_task_captured_events
    from src.utils.cache.redis_cache import get_cache_client

    registry_store = BackgroundRegistryStore.get_instance()
    cache = get_cache_client()
    redis_key = f"subagent:events:{thread_id}:{task_id}"
    # ``cursor`` is the last-emitted seq (NOT a list index). 0 means
    # "haven't emitted anything yet"; first record is seq=1.
    cursor = 0
    max_wait, waited = get_subagent_task_max_wait(), 0
    # Poll cadence for the pre-registry / pre-task startup window. Once the
    # task exists the loop is event-driven via new_event_signal.
    startup_poll_interval = 0.5

    def _format_sse(seq_id: int, event_type: str, data: dict) -> str:
        result = f"id: {seq_id}\nevent: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        if _SSE_LOG_ENABLED:
            _sse_logger.info(result)
        return result

    def _record_to_sse(record: dict) -> str:
        """Render a stored record (dict) into the SSE wire format."""
        seq = int(record.get("seq") or 0)
        data = {
            "thread_id": thread_id,
            "agent": record.get("agent_id") or f"task:{task_id}",
            **(record.get("data") or {}),
        }
        return _format_sse(seq, record.get("event") or "message_chunk", data)

    async def _replay_from_redis(after_seq: int) -> tuple[list[str], int]:
        """Read the durable Redis buffer once and return formatted SSEs.

        Returns ``(sse_strings, max_seq)`` where ``max_seq`` is the highest
        seq seen in Redis (used to advance the live cursor when Redis
        already covers events the in-memory tail no longer holds).
        """
        out: list[str] = []
        max_seq = after_seq
        try:
            stored = await cache.list_range(redis_key, 0, -1) or []
        except Exception as exc:
            logger.warning(f"[SubagentStream:{task_id}] Redis replay failed: {exc}")
            return out, max_seq
        for raw in stored:
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                logger.warning(
                    f"[SubagentStream:{task_id}] Skipped malformed Redis record"
                )
                continue
            seq = record.get("seq")
            if not isinstance(seq, int) or seq <= after_seq:
                continue
            out.append(_record_to_sse(record))
            if seq > max_seq:
                max_seq = seq
        return out, max_seq

    # Phase 1: Replay from Redis on reconnect. Records are stored as JSON
    # by the producer; we render them at yield time so the wire format is
    # identical to the live path even if rendering changes shape later.
    if last_event_id is not None:
        replayed, replay_max = await _replay_from_redis(last_event_id)
        for sse in replayed:
            yield sse
        # Advance the live cursor past whatever Redis already covered so the
        # subsequent in-memory tail drain doesn't double-emit.
        if replay_max > cursor:
            cursor = replay_max

    # Phase 2: Live streaming (event-driven). The producer is the sole
    # Redis writer — this consumer only reads the in-memory tail and (on
    # cursor lag) the Redis buffer.
    consumer_registered_on = None
    try:
        while True:
            registry = await registry_store.get_registry(thread_id)
            if not registry:
                if waited >= max_wait:
                    break
                waited += startup_poll_interval
                await asyncio.sleep(startup_poll_interval)
                continue

            task = await registry.get_task_by_task_id(task_id)
            if not task:
                if waited >= max_wait:
                    break
                waited += startup_poll_interval
                await asyncio.sleep(startup_poll_interval)
                continue

            # Reset wait counter once we find the task
            waited = 0

            # Register as an active consumer on first encounter.
            if consumer_registered_on is None:
                task.sse_consumer_count += 1
                consumer_registered_on = task

            # Clear before drain: any set() during/after drain stays visible
            # to the next wait().
            task.new_event_signal.clear()

            # If the in-memory tail rotated past our cursor, fill the gap
            # from Redis before draining the tail. Live consumers normally
            # never hit this — only slow ones falling behind tail maxlen do.
            # deque[0] is O(1) and GIL-atomic, so read the front directly
            # instead of copying the whole tail to inspect one element.
            tail_front_seq: int | None = None
            if task.captured_events_tail:
                front_seq = task.captured_events_tail[0].get("seq")
                if isinstance(front_seq, int):
                    tail_front_seq = front_seq
            if (
                tail_front_seq is not None
                and tail_front_seq > cursor + 1
                and cursor < task.captured_event_seq
            ):
                replayed, replay_max = await _replay_from_redis(cursor)
                for sse in replayed:
                    yield sse
                # Advance cursor past the replay so the tail drain below only
                # emits seq > replay_max — the boundary stays gap-free without
                # any clipping in the Redis loop.
                if replay_max > cursor:
                    cursor = replay_max

            # Drain new tail records (seq > cursor) via shared helper.
            items, new_cursor = drain_task_captured_events(task, cursor)
            for record, _agent_id, _seq in items:
                yield _record_to_sse(record)
            # Advance to the high-water snapshot (not live count) so events
            # appended during the yield loop are re-drained on the next
            # iteration instead of being skipped.
            if new_cursor > cursor:
                cursor = new_cursor

            # Task done -> final drain complete -> close
            if task.completed or (task.asyncio_task and task.asyncio_task.done()):
                break

            # Wait for a new event or timeout. 5s is a safety net so we still
            # wake if the task completes without appending events.
            try:
                await asyncio.wait_for(task.new_event_signal.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
    finally:
        if consumer_registered_on is not None:
            consumer_registered_on.sse_consumer_count -= 1
            if consumer_registered_on.sse_consumer_count <= 0:
                consumer_registered_on.sse_drain_complete.set()
