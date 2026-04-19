"""Stream reconnection and subagent event streaming.

Provides reconnect-to-running-workflow (replays buffered events then attaches
to the live Redis queue) and per-subagent-task SSE streaming used by the
``/threads/{id}/tasks/{task_id}/events`` endpoint.
"""

from __future__ import annotations

import asyncio
import json

from fastapi import HTTPException

from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.server.services.workflow_tracker import WorkflowTracker

from src.config.settings import (
    get_live_queue_maxsize,
    get_subagent_event_buffer_size,
    get_subagent_event_buffer_ttl,
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

    Per-task SSE stream with its own Redis buffer. Events are
    message_chunk, tool_calls, tool_call_result, and steering_accepted.

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
    cursor = 0
    max_wait, waited = get_subagent_task_max_wait(), 0
    # Poll cadence for the pre-registry / pre-task startup window. Once the
    # task exists the loop is event-driven via new_event_signal.
    _STARTUP_POLL_INTERVAL_S = 0.5

    def _format_sse(seq_id: int, event_type: str, data: dict) -> str:
        result = f"id: {seq_id}\nevent: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        if _SSE_LOG_ENABLED:
            _sse_logger.info(result)
        return result

    def _parse_sse_id(raw_sse: str) -> int | None:
        """Extract event ID from raw SSE string."""
        try:
            first_line = raw_sse.split("\n", 1)[0]
            if first_line.startswith("id: "):
                return int(first_line[4:].strip())
        except (ValueError, IndexError):
            pass
        return None

    # Phase 1: Replay from Redis buffer on reconnect
    # Snapshot the cursor BEFORE Redis replay so events appended during the
    # replay window aren't skipped (they'd be absent from Redis and past our
    # cursor). Possible duplicates at the boundary are tolerable — clients
    # dedupe via ``last_event_id``.
    if last_event_id is not None:
        registry = await registry_store.get_registry(thread_id)
        if registry:
            task = await registry.get_task_by_task_id(task_id)
            if task:
                cursor = len(task.captured_events)

        try:
            stored = await cache.list_range(redis_key, 0, -1) or []
            for raw_sse in stored:
                eid = _parse_sse_id(raw_sse)
                if eid is not None and eid > last_event_id:
                    yield raw_sse
        except Exception as e:
            logger.warning(f"[SubagentStream:{task_id}] Redis replay failed: {e}")

    # Phase 2: Live streaming (event-driven, with consumer reference counting)
    is_writer = False
    consumer_registered_on: BackgroundTask | None = None
    try:
        while True:
            registry = await registry_store.get_registry(thread_id)
            if not registry:
                if waited >= max_wait:
                    break
                waited += _STARTUP_POLL_INTERVAL_S
                await asyncio.sleep(_STARTUP_POLL_INTERVAL_S)
                continue

            task = await registry.get_task_by_task_id(task_id)
            if not task:
                if waited >= max_wait:
                    break
                waited += _STARTUP_POLL_INTERVAL_S
                await asyncio.sleep(_STARTUP_POLL_INTERVAL_S)
                continue

            # Reset wait counter once we find the task
            waited = 0

            # Register as an active consumer on first encounter.
            if consumer_registered_on is None:
                task.sse_consumer_count += 1
                consumer_registered_on = task

            # Claim the Redis writer role — either as first consumer, or as
            # a takeover if the prior writer disconnected while the task is
            # still running. Without this reclaim, a solo-writer drop would
            # leave the Redis replay buffer stagnant for the rest of the run.
            # Check-and-set is atomic under CPython asyncio (no await between).
            if not is_writer and not task.sse_redis_writer_claimed:
                task.sse_redis_writer_claimed = True
                is_writer = True

            # Clear before drain: any set() during/after drain stays visible
            # to the next wait().
            task.new_event_signal.clear()

            # Drain new captured_events (shared helper). The snapshot_index
            # (captured_events position) doubles as the SSE ``id:`` value so
            # ordering stays globally monotonic across writer handovers —
            # different consumers writing the same Redis replay buffer cannot
            # collide on ``id:`` because index is shared state on the task.
            items, new_cursor = drain_task_captured_events(task, cursor)
            for ev, agent_id, snapshot_index in items:
                seq = snapshot_index + 1  # 1-based for SSE id:
                data = {"thread_id": thread_id, "agent": agent_id, **ev["data"]}
                sse = _format_sse(seq, ev["event"], data)
                if is_writer:
                    try:
                        await cache.list_append(redis_key, sse, max_size=get_subagent_event_buffer_size(), ttl=get_subagent_event_buffer_ttl())
                    except Exception:
                        pass  # Non-fatal: live delivery still works
                yield sse
            # Advance to the snapshot length (not live length) so events
            # appended during the yield loop are re-drained on the next
            # iteration instead of being skipped.
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
            # Release the writer claim so another active consumer can take
            # over on their next loop iteration. Without this, a writer
            # disconnect mid-stream leaves no one writing to Redis.
            if is_writer:
                consumer_registered_on.sse_redis_writer_claimed = False
            consumer_registered_on.sse_consumer_count -= 1
            if consumer_registered_on.sse_consumer_count <= 0:
                consumer_registered_on.sse_drain_complete.set()
