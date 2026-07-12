"""Stream reconnection and per-subagent SSE consumers.

Both endpoints (``/threads/{id}/messages/stream`` reconnect and
``/threads/{id}/tasks/{task_id}``) delegate to ``stream_from_log`` /
``stream_subagent_from_log`` — each a single XREAD BLOCK loop attached by
stream key + cursor.
"""

from __future__ import annotations

import uuid

from fastapi import HTTPException

from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.workflow_tracker import WorkflowTracker

from ._common import logger
from .steering import drain_steering_return_event
from .stream_from_log import stream_from_log, stream_subagent_from_log


# ---------------------------------------------------------------------------
# Reconnect to a running or completed PTC workflow
# ---------------------------------------------------------------------------


async def _probe_stream(thread_id: str, run_id: str | None) -> bool | None:
    """Tri-state stream presence: True/False = confirmed, None = unknowable.

    None means the transport is CONFIGURED for Redis streams but unreachable
    right now — absence must not be asserted (I6). False is only returned on
    a confirmed EXISTS=0, or when the deployment doesn't use Redis streams at
    all (then absence is permanent truth, not an outage).
    """
    from src.server.services.background_task_manager import (
        BackgroundTaskManager,
        stream_key,
    )
    from src.utils.cache.redis_cache import get_cache_client

    uses_redis_streams = (
        BackgroundTaskManager.get_instance().event_storage_backend == "redis"
    )
    try:
        cache = get_cache_client()
        if not (cache.enabled and cache.client):
            return None if uses_redis_streams else False
        return bool(await cache.client.exists(stream_key(thread_id, run_id)))
    except Exception:
        return None if uses_redis_streams else False


def _transport_unavailable(run_id: str | None) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "code": "transport_unavailable",
            "message": (
                "The event-stream transport is temporarily unreachable; "
                "retry shortly."
            ),
            "run_id": run_id,
        },
        headers={"Retry-After": "3"},
    )


async def classify_reconnect(
    thread_id: str, run_id: str | None = None
) -> str | None:
    """Pre-header reconnect classification (1.5d) — ledger-first.

    Must run BEFORE the StreamingResponse is built: a raise inside the SSE
    generator lands after HTTP 200 + headers are already on the wire.
    Returns the effective run_id to stream (None = legacy thread-key
    fallback inside ``stream_from_log``), or raises:

    - 404: run not on this thread / no runs and no local activity at all
    - 409 ``recovering``: durable in_progress row but no local executor
      (crashed worker; the startup sweep or Phase-2 scanner will settle it)
    - 410 ``stream_expired``: terminal run whose retained stream is CONFIRMED
      gone — the archived replay endpoint is the only remaining source
    - 503 ``transport_unavailable``: Redis is configured but unreachable, so
      stream absence cannot be distinguished from a transient outage (I6
      tri-state: absence ≠ terminal). Retryable, unlike the 410.
    """
    from src.server.database import turn_lifecycle as tl_db

    manager = BackgroundTaskManager.get_instance()

    run = None
    if run_id is not None:
        try:
            uuid.UUID(run_id)
        except ValueError:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        run = await tl_db.get_run(run_id)
        if run is not None and str(run["conversation_thread_id"]) != thread_id:
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found on thread {thread_id}",
            )
    else:
        run = await tl_db.get_latest_attempt(thread_id)
        if run is not None:
            run_id = str(run["conversation_response_id"])

    if run is None:
        # No ledger row (pre-v4 turn, or nothing durable yet): fall back to
        # local task / tracker knowledge, mirroring the pre-1.5 behavior.
        # An explicit run_id must be corroborated by that knowledge — the
        # task registry is keyed by (thread, run) already, and the tracker
        # blob only vouches for its OWN run_id (an unrelated blob must not
        # admit a made-up run id onto an empty stream key).
        task_info = await manager.get_task_info(thread_id, run_id)
        if task_info is not None:
            return run_id or task_info.run_id
        tracker_status = await WorkflowTracker.get_instance().get_status(thread_id)
        if tracker_status is not None:
            if run_id is not None and tracker_status.get("run_id") != run_id:
                raise HTTPException(
                    status_code=404,
                    detail=f"Run {run_id} not found on thread {thread_id}",
                )
            return run_id  # stream_from_log resolves via tracker/legacy key
        raise HTTPException(
            status_code=404, detail=f"Workflow {thread_id} not found"
        )

    if run["status"] == "in_progress":
        task_info = await manager.get_task_info(thread_id, run_id)
        if task_info is None:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "recovering",
                    "message": (
                        "The run is recorded as in progress but has no live "
                        "executor; recovery will settle it. Retry shortly."
                    ),
                    "run_id": run_id,
                },
            )
        # Live run: the stream key may legitimately not exist yet (no event
        # buffered so far — False is fine), but the transport itself must be
        # reachable or the committed 200 would attach to a stream that can
        # never be read. Without Redis event storage there is no live
        # transport at all — never admit a watch that cannot deliver.
        if manager.event_storage_backend != "redis" or not manager.enable_storage:
            raise _transport_unavailable(run_id)
        if await _probe_stream(thread_id, run_id) is None:
            raise _transport_unavailable(run_id)
        return run_id

    # Terminal run: replay from the retained stream while it survives.
    # Tri-state (I6): only a CONFIRMED miss may report permanent expiry.
    alive = await _probe_stream(thread_id, run_id)
    if alive is None:
        raise _transport_unavailable(run_id)
    if alive:
        return run_id
    raise HTTPException(
        status_code=410,
        detail={
            "code": "stream_expired",
            "message": "Run finished and its event stream expired.",
            "run_id": run_id,
            "run_status": run["status"],
            "replay_url": f"/api/v1/threads/{thread_id}/messages/replay",
        },
    )


async def reconnect_to_workflow_stream(
    thread_id: str,
    run_id: str | None = None,
    last_event_id: int | None = None,
):
    """Stream a reconnect that ``classify_reconnect`` already admitted.

    ``run_id`` should be the classifier's effective run id; this generator
    makes no admission decisions (the response is already committed by the
    time it runs).
    """
    async for event in stream_from_log(thread_id, run_id, last_event_id):
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
