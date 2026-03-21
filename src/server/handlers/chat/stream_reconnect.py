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

    # Attach to live stream if still running or tailing
    status = await manager.get_task_status(thread_id)
    if status == TaskStatus.RUNNING:
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
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
    seq = 0
    cursor = 0
    max_wait, waited = 30, 0

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
    if last_event_id is not None:
        try:
            stored = await cache.list_range(redis_key, 0, -1) or []
            for raw_sse in stored:
                eid = _parse_sse_id(raw_sse)
                if eid is not None and eid > last_event_id:
                    yield raw_sse
                seq = max(seq, eid or 0)
        except Exception as e:
            logger.warning(f"[SubagentStream:{task_id}] Redis replay failed: {e}")

        # Seed cursor past already-buffered events
        registry = await registry_store.get_registry(thread_id)
        if registry:
            task = await registry.get_task_by_task_id(task_id)
            if task:
                cursor = len(task.captured_events)

    # Phase 2: Live polling
    while True:
        registry = await registry_store.get_registry(thread_id)
        if not registry:
            if waited >= max_wait:
                break
            waited += 0.5
            await asyncio.sleep(0.5)
            continue

        task = await registry.get_task_by_task_id(task_id)
        if not task:
            if waited >= max_wait:
                break
            waited += 0.5
            await asyncio.sleep(0.5)
            continue

        # Reset wait counter once we find the task
        waited = 0

        # Drain new captured_events (shared helper)
        for ev, agent_id in drain_task_captured_events(task, cursor):
            seq += 1
            data = {"thread_id": thread_id, "agent": agent_id, **ev["data"]}
            sse = _format_sse(seq, ev["event"], data)
            # Buffer to per-task Redis key
            try:
                await cache.list_append(redis_key, sse, max_size=100, ttl=3600)
            except Exception:
                pass  # Non-fatal: live delivery still works
            yield sse
        cursor = len(task.captured_events)

        # Task done -> final drain complete -> close
        if task.completed or (task.asyncio_task and task.asyncio_task.done()):
            # Signal collector that all events have been emitted to the client
            task.sse_drain_complete.set()
            break

        await asyncio.sleep(0.5)
