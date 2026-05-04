"""Stream reconnection and per-subagent SSE consumers.

Both endpoints (``/threads/{id}/messages/stream`` reconnect and
``/threads/{id}/tasks/{task_id}``) delegate to ``stream_from_log`` /
``stream_subagent_from_log`` — each a single XREAD BLOCK loop attached by
stream key + cursor.
"""

from __future__ import annotations

from fastapi import HTTPException

from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.workflow_tracker import WorkflowTracker

from ._common import logger
from .steering import drain_steering_return_event
from .stream_from_log import stream_from_log, stream_subagent_from_log


# ---------------------------------------------------------------------------
# Reconnect to a running or completed PTC workflow
# ---------------------------------------------------------------------------


async def reconnect_to_workflow_stream(
    thread_id: str,
    last_event_id: int | None = None,
):
    """Reconnect to a running or completed workflow via Redis Streams.

    ``stream_from_log`` maps both ``None`` and ``<= 0`` to cursor ``0``
    (replay from the beginning), so we pass ``last_event_id`` through
    untouched.
    """
    manager = BackgroundTaskManager.get_instance()
    tracker = WorkflowTracker.get_instance()

    task_info = await manager.get_task_info(thread_id)
    workflow_status = await tracker.get_status(thread_id)

    if not task_info:
        if workflow_status and workflow_status.get("status") == "completed":
            raise HTTPException(
                status_code=410, detail="Workflow completed and results expired"
            )
        raise HTTPException(status_code=404, detail=f"Workflow {thread_id} not found")

    async for event in stream_from_log(thread_id, last_event_id):
        yield event

    # After the workflow ends, return any unconsumed steering messages so the
    # client can re-render them instead of silently dropping them.
    steering_event = await drain_steering_return_event(thread_id)
    if steering_event:
        logger.info(
            f"[PTC_RECONNECT] Returning unconsumed steering message(s) "
            f"to client: thread_id={thread_id}"
        )
        yield steering_event


# ---------------------------------------------------------------------------
# Per-subagent task SSE stream
# ---------------------------------------------------------------------------


async def stream_subagent_task_events(
    thread_id: str, task_id: str, last_event_id: int | None = None
):
    """SSE stream of a single subagent's content events.

    Producer-driven Redis writes: ``SubagentEventCaptureMiddleware``'s spill
    path writes pre-rendered SSE wire strings to
    ``subagent:stream:{thread_id}:{task_id}`` so this consumer is a
    pass-through XREAD BLOCK loop.
    """
    async for event in stream_subagent_from_log(thread_id, task_id, last_event_id):
        yield event
