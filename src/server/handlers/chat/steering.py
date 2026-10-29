"""Steering for chat workflows.

A main-thread steer is a ``hook_outbox`` row (``admission_gate.wait_or_steer``
admits it), read by SteeringMiddleware before the thread's next model call.
A steer into a background subagent task is one too, on the task's chain
(``steer_subagent``), read by SubagentSteeringMiddleware.
"""

import logging
import time
from typing import Any

from fastapi import HTTPException

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


def fresh_input_message_id(request, input_state) -> str | None:
    """The id of a fresh turn's own input message, or None.

    Steers sent before a fresh turn started go ahead of its input, so the
    model reads them in the order they were sent. A retry, a HITL resume and
    a checkpoint replay have no input of their own: a steer they read goes
    after whatever the thread already holds, where it was sent.
    """
    if request.retry_of_run_id or request.hitl_response:
        return None
    if not isinstance(input_state, dict):
        return None
    for message in reversed(input_state.get("messages") or []):
        if message.get("role") == "user" and message.get("id"):
            return str(message["id"])
    return None


async def open_with_steers_ahead(
    thread_id: str, run_id: str, request, input_state
) -> str | None:
    """Put the steers sent before this fresh turn ahead of its input, and
    return the input's id (None for a turn with no input of its own).

    In the input itself, so the turn's input checkpoint holds them: a
    regenerate starts over from that checkpoint, and would otherwise lose
    what the model read ahead of the input."""
    input_id = fresh_input_message_id(request, input_state)
    if not input_id:
        return None
    from ptc_agent.agent.middleware.steering import steers_ahead_message
    from src.server.services.report_back.steer import take_steers_ahead

    steers = await take_steers_ahead(thread_id, run_id)
    if steers:
        messages = input_state["messages"]
        at = next(
            i
            for i, m in enumerate(messages)
            if isinstance(m, dict) and str(m.get("id")) == input_id
        )
        messages.insert(at, steers_ahead_message(steers, run_id))
    return input_id


def is_chat_app(platform: str | None) -> bool:
    """Whether a steer came through a channel gateway, which resubmits what
    its run missed itself, rather than a surface langalpha draws (``web``,
    ``market_view:<SYMBOL>``), whose missed steers become the next turn."""
    from ptc_agent.agent.middleware.runtime_context.surface import parse_surface
    from ptc_agent.agent.middleware.runtime_context.turn import BUILT_IN_SURFACES

    name = parse_surface(platform).name
    return name is not None and name not in BUILT_IN_SURFACES


def steer_payload(
    content: str,
    user_id: str,
    *,
    sender: str = "user",
    workspace_id: str | None = None,
    platform: str | None = None,
    request_key: str | None = None,
    origin: dict | None = None,
) -> dict[str, Any]:
    """A main-thread steer job's payload, before any lifecycle key."""
    # Half a surrogate pair gets through the request's JSON parser but cannot
    # be stored as JSONB or encoded for the wire.
    content = content.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    return {
        "target": "thread",
        "sender": sender,
        "content": content,
        "user_id": user_id,
        "workspace_id": workspace_id,
        "timestamp": time.time(),
        "platform": platform or None,
        "chat_app": is_chat_app(platform),
        "request_key": request_key,
        "origin": origin,
    }


async def _launch_call_id(thread_id: str, task_id: str) -> str:
    """The task's original Task call id, constant across resumes: the id the
    client knows the task's card by. 404 when the ledger knows no launch."""
    from src.server.database.runs import subagent_runs as sr_db

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
    return str(init_call)


async def steer_subagent(
    thread_id: str,
    task_id: str,
    content: str,
    user_id: str,
    input_id: str | None = None,
) -> dict:
    """Send the user's message into a running background task.

    Accepted only while the task's run takes steers, in the same transaction
    that reads it (``admit_task_steer``); SubagentSteeringMiddleware reads it
    before the run's next model call, and the run's ending settles it if it
    never does. A resend of an accepted ``input_id`` gets the original
    acceptance, whatever the task did since.

    Args:
        thread_id: The thread the task belongs to
        task_id: The subagent task ID (e.g., 'k7Xm2p')
        content: The message text to send
        user_id: User identifier
        input_id: The caller's id for this message, carried on the delivery
            or return that settles it; generated when not given

    Returns:
        Dict with success status and queue position
    """
    from src.server.database.runs import subagent_runs as sr_db
    from src.server.services.report_back.task_steer import (
        admit_task_message,
        live_task_run,
    )

    try:
        result = None
        if input_id:
            # The original acceptance first: the task may have ended since.
            result = await admit_task_message(
                thread_id=thread_id,
                task_id=task_id,
                task_run_id=None,
                content=content,
                sender="user",
                user_id=user_id,
                input_id=input_id,
            )
            if result["state"] != "duplicate":
                result = None
        if result is None:
            active = await live_task_run(thread_id, task_id)
            if active is None:
                statuses = await sr_db.get_latest_run_statuses(thread_id, [task_id])
                status = statuses.get(task_id)
                if status == "in_progress":
                    status = "cancelled"  # stopped, not yet ended
                if status:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Task-{task_id} has already {status}",
                    )
                raise HTTPException(
                    status_code=404,
                    detail=f"Task-{task_id} not found in thread {thread_id}",
                )
            tool_call_id = await _launch_call_id(thread_id, task_id)
            result = await admit_task_message(
                thread_id=thread_id,
                task_id=task_id,
                task_run_id=str(active["task_run_id"]),
                content=content,
                sender="user",
                user_id=user_id,
                input_id=input_id,
            )
            if result["state"] == "not_running":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Task-{task_id} finished before the message could be "
                        "delivered"
                    ),
                )
        else:
            tool_call_id = await _launch_call_id(thread_id, task_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[SUBAGENT_MSG] Failed to steer subagent: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to steer subagent: {e}",
        )

    logger.info(
        f"[SUBAGENT_MSG] Steering for subagent: thread_id={thread_id} "
        f"task=Task-{task_id} position={result['position']} ({result['state']})"
    )
    return {
        "success": True,
        "tool_call_id": tool_call_id,
        "display_id": f"Task-{task_id}",
        "queue_position": result["position"],
        "input_id": result["input_id"],
    }
