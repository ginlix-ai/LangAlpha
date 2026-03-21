"""Redis steering utilities for chat workflows.

Handles queuing, draining, and backfilling of user steering messages that arrive
while a workflow is already running. Messages are stored in Redis and consumed by
SteeringMiddleware (main agent) or SubagentSteeringMiddleware (subagents).
"""

import json
import time
from uuid import uuid4

from fastapi import HTTPException

from src.server.services.background_registry_store import BackgroundRegistryStore

from ._common import logger


async def backfill_steering_queries(
    thread_id: str, steering_messages: list[dict]
) -> None:
    """Backfill query records for steering messages that produced orphan responses.

    After a workflow completes, responses may exist at turn indices that have no
    matching query (because the user message was injected mid-workflow via
    SteeringMiddleware rather than arriving as a normal HTTP request).
    This function finds those orphan response turns and creates query records.
    """
    if not steering_messages:
        return

    from src.server.database.conversation import (
        create_query,
        get_queries_for_thread,
        get_responses_for_thread,
    )

    try:
        queries, _ = await get_queries_for_thread(thread_id)
        responses, _ = await get_responses_for_thread(thread_id)

        query_turns = {q["turn_index"] for q in queries}
        response_turns = {r["turn_index"] for r in responses}
        orphan_turns = sorted(response_turns - query_turns)

        if not orphan_turns:
            return

        # Match orphan turns with steering messages (FIFO order)
        for turn_index, msg in zip(orphan_turns, steering_messages):
            content = msg.get("content", "")
            if not content:
                continue
            await create_query(
                conversation_query_id=str(uuid4()),
                conversation_thread_id=thread_id,
                turn_index=turn_index,
                content=content,
                query_type="steering",
            )
            logger.info(
                f"[CHAT] Backfilled steering query: thread_id={thread_id} "
                f"turn_index={turn_index}"
            )
    except Exception as e:
        logger.error(f"[CHAT] Failed to backfill steering queries: {e}")


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
        pipe = cache.client.pipeline()
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
    thread_id: str, content: str, user_id: str
) -> dict | None:
    """Steer a running workflow by injecting a user message via Redis.

    The SteeringMiddleware will pick these up before the next LLM call.

    Args:
        thread_id: The thread with an active workflow
        content: The user's message text
        user_id: User identifier

    Returns:
        Dict with queue position if successful, None if steering failed
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    try:
        key = f"workflow:steering:{thread_id}"
        message = json.dumps(
            {"content": content, "user_id": user_id, "timestamp": time.time()}
        )
        pipe = cache.client.pipeline()
        pipe.rpush(key, message)
        pipe.llen(key)
        pipe.expire(key, 3600)  # 1h TTL
        results = await pipe.execute()
        position = results[1]
        logger.info(
            f"[CHAT] Steering for running workflow: "
            f"thread_id={thread_id} position={position}"
        )
        return {"position": position}
    except Exception as e:
        logger.error(f"[CHAT] Failed to steer thread: {e}")
        return None


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

    # 1. Look up the registry for this thread
    registry_store = BackgroundRegistryStore.get_instance()
    registry = await registry_store.get_registry(thread_id)
    if registry is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active workflow for thread {thread_id}",
        )

    # 2. Look up the task by ID
    task = await registry.get_by_task_id(task_id)
    if task is None:
        raise HTTPException(
            status_code=404,
            detail=f"Task-{task_id} not found in thread {thread_id}",
        )

    # 3. Reject if already completed or cancelled
    if task.completed or task.cancelled:
        status = "cancelled" if task.cancelled else "completed"
        raise HTTPException(
            status_code=409,
            detail=f"Task-{task_id} has already {status}",
        )

    # 4. Queue to Redis (same pattern as _queue_followup_to_redis)
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(
            status_code=503,
            detail="Message queuing unavailable (Redis not connected)",
        )

    try:
        key = f"subagent:steering:{task.tool_call_id}"
        payload = json.dumps(content)
        pipe = cache.client.pipeline()
        pipe.rpush(key, payload)
        pipe.llen(key)
        pipe.expire(key, 3600)  # 1h TTL
        results = await pipe.execute()
        position = results[1]

        logger.info(
            f"[SUBAGENT_MSG] Steering for subagent: "
            f"thread_id={thread_id} task={task.display_id} position={position}"
        )
        return {
            "success": True,
            "tool_call_id": task.tool_call_id,
            "display_id": task.display_id,
            "queue_position": position,
        }
    except Exception as e:
        logger.error(f"[SUBAGENT_MSG] Failed to steer subagent: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to steer subagent: {e}",
        )
