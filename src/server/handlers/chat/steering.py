"""Redis steering utilities for chat workflows.

Handles queuing and draining of user steering messages that arrive
while a workflow is already running. Messages are stored in Redis and consumed by
SteeringMiddleware (main agent) or SubagentSteeringMiddleware (subagents).
"""

import json
import time
import uuid

from fastapi import HTTPException

from src.server.services.background_registry_store import BackgroundRegistryStore
from src.config.settings import get_redis_ttl_steering

from ._common import logger


async def drain_steering_return_event(thread_id: str) -> str | None:
    """Drain unconsumed steering messages and format as a ``steering_returned`` SSE event.

    Returns the SSE string ready to yield, or ``None`` if no messages were pending.
    """
    unconsumed = await drain_pending_steerings(thread_id)
    if not unconsumed:
        return None
    event_data = json.dumps({
        "thread_id": thread_id,
        "messages": [
            {"content": m["content"], "user_id": m.get("user_id")}
            for m in unconsumed
        ],
    })
    return f"event: steering_returned\ndata: {event_data}\n\n"


async def drain_pending_steerings(thread_id: str) -> list[dict] | None:
    """Drain any unconsumed steering messages from Redis after workflow completion.

    Returns the messages so they can be sent back to the client for input restoration.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    try:
        key = f"workflow:steering:{thread_id}"
        # transaction=True (MULTI/EXEC) is load-bearing, not incidental: the
        # LRANGE+DELETE must be atomic so it can never interleave with a
        # concurrent ``unsteer_thread`` LREM. That atomicity is what gives
        # wait_or_steer's reclaim its delivered-XOR-reclaimed guarantee — a
        # message is returned here or reclaimed there, never both, never lost.
        pipe = cache.client.pipeline(transaction=True)
        pipe.lrange(key, 0, -1)
        pipe.delete(key)
        results = await pipe.execute()

        raw_messages = results[0]
        if not raw_messages:
            return None

        messages = []
        for raw in raw_messages:
            try:
                data = json.loads(
                    raw.decode("utf-8") if isinstance(raw, bytes) else raw
                )
                messages.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        return messages or None
    except Exception as e:
        logger.error(f"[CHAT] Failed to drain pending steerings: {e}")
        return None


async def steer_thread(
    thread_id: str, content: str, user_id: str, run_id: str | None = None
) -> dict | None:
    """Steer a running workflow by injecting a user message via Redis.

    The SteeringMiddleware will pick these up before the next LLM call.

    Args:
        thread_id: The thread with an active workflow
        content: The user's message text
        user_id: User identifier
        run_id: The live run this steer targets (v4 2.4c). The consuming
            middleware delivers only payloads stamped with its own run (or
            legacy unstamped ones), so a message steered into a run that
            died un-drained is returned by the next turn's end-of-run drain
            instead of leaking into its context. None = unstamped.

    Returns:
        Dict with queue position and the exact queued payload (for a
        possible ``unsteer_thread`` reclaim) if successful, None if
        steering failed
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    try:
        key = f"workflow:steering:{thread_id}"
        payload = {"content": content, "user_id": user_id, "timestamp": time.time()}
        if run_id is not None:
            payload["run_id"] = run_id
        message = json.dumps(payload)
        pipe = cache.client.pipeline()
        pipe.rpush(key, message)
        pipe.llen(key)
        pipe.expire(key, get_redis_ttl_steering())
        results = await pipe.execute()
        position = results[1]
        logger.info(
            f"[CHAT] Steering for running workflow: "
            f"thread_id={thread_id} position={position}"
        )
        return {"position": position, "payload": message}
    except Exception as e:
        logger.error(f"[CHAT] Failed to steer thread: {e}")
        return None


async def unsteer_thread(thread_id: str, payload: str) -> bool:
    """Reclaim a just-pushed steering message by exact payload (``LREM``).

    Used by ``wait_or_steer`` when the workflow turned out to have exited
    between the admission snapshot and the push. True means the message was
    still queued (nothing consumed it); False means a drain got it first —
    the caller must then treat the steer as delivered-or-returned, not lost.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return False

    try:
        key = f"workflow:steering:{thread_id}"
        removed = await cache.client.lrem(key, 1, payload)
        return bool(removed)
    except Exception as e:
        logger.error(f"[CHAT] Failed to unsteer thread: {e}")
        return False


async def _steering_identity_from_ledger(
    thread_id: str, task_id: str
) -> tuple[str, str]:
    """(tool_call_id, expected_task_run_id) for a live run whose Redis meta
    has lapsed.

    The queue key the writer drains is keyed by its registry identity — the
    ORIGINAL tool_call_id, constant across resumes — which the chain's init
    run recorded as its launch call. Raises 409 for a terminal chain, 404
    when the ledger knows nothing (pre-ledger or unstamped tasks included:
    without a routing identity there is no queue to publish to).
    """
    from src.server.database import subagent_runs as sr_db

    active = await sr_db.get_active_task_run(thread_id, task_id)
    if active is None:
        statuses = await sr_db.get_latest_run_statuses(thread_id, [task_id])
        status = statuses.get(task_id)
        if status:
            raise HTTPException(
                status_code=409,
                detail=f"Task-{task_id} has already {status}",
            )
        raise HTTPException(
            status_code=404,
            detail=f"Task-{task_id} not found in thread {thread_id}",
        )
    chain = await sr_db.list_task_runs(thread_id, task_id)
    init_call = next(
        (
            r["launch_tool_call_id"]
            for r in chain
            if r.get("cause") == "init" and r.get("launch_tool_call_id")
        ),
        None,
    )
    if not init_call:
        raise HTTPException(
            status_code=404,
            detail=f"Task-{task_id} not found in thread {thread_id}",
        )
    return str(init_call), str(active["task_run_id"])


async def steer_subagent(
    thread_id: str,
    task_id: str,
    content: str,
    user_id: str,
) -> dict:
    """Steer a running subagent by injecting a user message via Redis.

    The SubagentSteeringMiddleware will pick these up before the subagent's next LLM call.

    Args:
        thread_id: The thread with an active workflow
        task_id: The subagent task ID (e.g., 'k7Xm2p')
        content: The message text to send
        user_id: User identifier

    Returns:
        Dict with success status and queue position
    """
    from src.utils.cache.redis_cache import get_cache_client

    # 1. Resolve the target: local registry first (this worker owns the
    # writer), else the cross-worker Redis task meta (v4 2.4e) — the consume
    # side (SubagentSteeringMiddleware) reads the steering list from Redis,
    # so delivery already works across workers once the target resolves.
    registry_store = BackgroundRegistryStore.get_instance()
    registry = await registry_store.get_registry(thread_id)
    task = await registry.get_by_task_id(task_id) if registry else None

    # A registry entry without a LIVE local writer is history: a hydrated
    # shadow of another worker's task, or a settled local task whose
    # namespace a later turn on another worker may since have re-acquired.
    # Neither tracks what the real writer did next, so the fresh Redis meta
    # (not the local object) is the authority.
    if task is not None and (
        task.asyncio_task is None or task.asyncio_task.done()
    ):
        task = None

    if task is not None:
        # 2a. Local task: reject if already completed or cancelled
        if task.completed or task.cancelled:
            status = "cancelled" if task.cancelled else "completed"
            raise HTTPException(
                status_code=409,
                detail=f"Task-{task_id} has already {status}",
            )
        tool_call_id = task.tool_call_id
        expected_task_run_id = task.task_run_id
    else:
        # 2b. Foreign or lost task: the meta hash carries routing identity
        # (tool_call_id) and writer liveness.
        from ptc_agent.agent.middleware.background_subagent.registry import (
            read_task_meta,
        )

        meta = await read_task_meta(thread_id, task_id)
        if meta is not None and meta.get("tool_call_id"):
            if meta.get("status") != "running":
                raise HTTPException(
                    status_code=409,
                    detail=f"Task-{task_id} has already {meta.get('status')}",
                )
            tool_call_id = meta["tool_call_id"]
            expected_task_run_id = meta.get("task_run_id") or None
        else:
            # 2c. Meta lapsed (TTL, flush): the durable ledger can still
            # name a live run — don't 404 a task that is provably running.
            tool_call_id, expected_task_run_id = (
                await _steering_identity_from_ledger(thread_id, task_id)
            )

    # 3. Queue to Redis (same pattern as _queue_followup_to_redis)
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(
            status_code=503,
            detail="Message queuing unavailable (Redis not connected)",
        )

    try:
        from ptc_agent.agent.middleware.background_subagent.registry import (
            steering_queue_key,
        )

        input_id = uuid.uuid4().hex
        key = steering_queue_key(tool_call_id, expected_task_run_id)
        payload = json.dumps(
            {
                "content": content,
                "expected_task_run_id": expected_task_run_id,
                "input_id": input_id,
            }
        )
        pipe = cache.client.pipeline()
        pipe.rpush(key, payload)
        pipe.llen(key)
        pipe.expire(key, get_redis_ttl_steering())
        results = await pipe.execute()
        position = results[1]

        logger.info(
            f"[SUBAGENT_MSG] Steering for subagent: "
            f"thread_id={thread_id} task=Task-{task_id} position={position}"
            f"{' (via task meta)' if task is None else ''}"
        )
        return {
            "success": True,
            "tool_call_id": tool_call_id,
            "display_id": f"Task-{task_id}",
            "queue_position": position,
            "input_id": input_id,
        }
    except Exception as e:
        logger.error(f"[SUBAGENT_MSG] Failed to steer subagent: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to steer subagent: {e}",
        )
