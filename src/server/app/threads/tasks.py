"""Background-subagent task endpoints: status, stream, steer."""

from typing import Annotated, Optional


from fastapi import Header, Path, Query
from fastapi.responses import StreamingResponse

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
)
from src.server.models.chat import SubagentMessageRequest



from ._deps import SSE_HEADERS, router


@router.get("/{thread_id}/tasks/{task_id}/status")
async def get_subagent_task_status(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: CurrentUserId,
):
    """Durable terminal state of a single subagent task from the run ledger.

    The client's in-memory card can go stale (a settled task whose live
    stream drained while the tab was backgrounded), leaving the detail view
    stuck on "Initializing"/spinner. This resolves the task's real
    ``{status, error}`` on demand so the view can hydrate without a full
    thread reload — the same ledger truth replay stamps at load time.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.services.history.task_status import resolve_task_details

    details = await resolve_task_details(thread_id, [task_id])
    detail = details.get(task_id)
    if detail is None:
        return {"task_id": task_id, "status": None, "error": None}
    return {"task_id": task_id, "status": detail["status"], "error": detail.get("error")}


@router.get("/{thread_id}/tasks/{task_id}/history")
async def get_subagent_task_history(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: CurrentUserId,
):
    """On-demand checkpoint transcript for one subagent task.

    Replay only projects lanes for Task-tool launches; workflow children
    have no launch artifact in the main transcript, so their transcripts —
    durably checkpointed under ``task:{task_id}`` — are otherwise
    unreachable after the live stream expires. Returns the same SSE-shaped
    items replay emits (empty for an unknown or still-running task, whose
    transcript belongs to its live stream).
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.services.history.replay.task_lane import (
        project_task_transcript,
    )

    items = await project_task_transcript(thread_id, task_id)
    return {"task_id": task_id, "items": items}


@router.get("/{thread_id}/tasks/{task_id}")
async def stream_subagent_task(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: CurrentUserId,
    last_event_id: Optional[int] = Query(
        None, description="Last received event ID for reconnect"
    ),
    last_event_id_header: Optional[str] = Header(None, alias="Last-Event-ID"),
):
    """Stream a single subagent's content events (message_chunk, tool_calls, etc.).

    Accepts the cursor as either ``?last_event_id=N`` or the SSE-spec
    ``Last-Event-ID`` HTTP header. Served from the v2 per-run stream
    (v1-identical wire shape); pre-ledger tasks fall back to the v1 reader.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat.task_run_sse_reader import stream_task_run_sse

    if last_event_id is None and last_event_id_header is not None:
        try:
            last_event_id = int(last_event_id_header)
        except ValueError:
            pass

    return StreamingResponse(
        stream_task_run_sse(thread_id, task_id, last_event_id),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )




@router.post("/{thread_id}/tasks/{task_id}/cancel")
async def cancel_subagent_task(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: CurrentUserId,
):
    """Stop one background task (e.g. a workflow run) without touching the turn."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.services.cancel_dispatch import cancel_subagent_task

    return await cancel_subagent_task(thread_id, task_id)


@router.post("/{thread_id}/tasks/{task_id}/messages")
async def send_subagent_message(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    request: SubagentMessageRequest,
    x_user_id: CurrentUserId,
):
    """Send a message/instruction to a running background subagent."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat import steer_subagent

    return await steer_subagent(
        thread_id=thread_id,
        task_id=task_id,
        content=request.content,
        user_id=x_user_id,
    )
