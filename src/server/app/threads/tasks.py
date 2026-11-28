"""Background-subagent task endpoints: status, stream, steer."""

from contextlib import aclosing
from typing import Annotated, Optional


from fastapi import Header, Path, Query
from fastapi.responses import StreamingResponse

from src.server.dependencies.principal import ReadUserId, RunUserId
from src.server.services.shared_workspaces.access import ThreadWatch, thread_access
from src.server.models.chat import SubagentMessageRequest



from ._deps import SSE_HEADERS, router
from src.server.services.shared_workspaces.turns import refuse_others_task


@router.get("/{thread_id}/tasks/{task_id}/status")
async def get_subagent_task_status(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: ReadUserId,
):
    """Durable terminal state of a single subagent task from the run ledger.

    The client's in-memory card can go stale (a settled task whose live
    stream drained while the tab was backgrounded), leaving the detail view
    stuck on "Initializing"/spinner. This resolves the task's real
    ``{status, error}`` on demand so the view can hydrate without a full
    thread reload — the same ledger truth replay stamps at load time.
    """
    await thread_access(thread_id, x_user_id)
    from src.server.services.history.task_status import (
        resolve_task_details,
        run_telemetry,
    )

    details = await resolve_task_details(thread_id, [task_id])
    detail = details.get(task_id)
    if detail is None:
        return {"task_id": task_id, "status": None, "error": None}
    run = run_telemetry(detail) or {}
    return {
        "task_id": task_id,
        "status": detail["status"],
        "error": detail.get("error"),
        "last_activity_ms": detail.get("last_activity_ms"),
        # The latest run and its bounds, so a stale card's timeline settles
        # with it; a client keys run facts by the id. Null for a task from
        # before the ledger.
        "run_id": detail.get("latest_run_id"),
        "started_ms": run.get("started_ms"),
        "finished_ms": run.get("finished_ms"),
        # Absent, not null, for a run whose row recorded none.
        **({"model": run["model"]} if run.get("model") else {}),
    }


@router.get("/{thread_id}/tasks/{task_id}/history")
async def get_subagent_task_history(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: ReadUserId,
):
    """On-demand checkpoint transcript for one subagent task.

    Replay only projects lanes for Task-tool launches; workflow children
    have no launch artifact in the main transcript, so their transcripts —
    durably checkpointed under ``task:{task_id}`` — are otherwise
    unreachable after the live stream expires. Returns the same SSE-shaped
    items replay emits (empty for an unknown or still-running task, whose
    transcript belongs to its live stream).
    """
    await thread_access(thread_id, x_user_id)
    from src.server.services.history.replay.run_lane import (
        project_task_transcript,
    )

    items = await project_task_transcript(thread_id, task_id)
    return {"task_id": task_id, "items": items}


@router.get("/{thread_id}/tasks/{task_id}")
async def stream_subagent_task(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: ReadUserId,
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
    watch = ThreadWatch.of(thread_id, x_user_id, await thread_access(thread_id, x_user_id))
    from src.server.handlers.chat.task_run_sse_reader import stream_task_run_sse

    if last_event_id is None and last_event_id_header is not None:
        try:
            last_event_id = int(last_event_id_header)
        except ValueError:
            pass

    frames = stream_task_run_sse(thread_id, task_id, last_event_id)

    async def watched():
        # A shared thread's task stream ends, as a dropped one does, once the
        # viewer may no longer read the thread; the client's reconnect then
        # meets the route's refusal.
        async with aclosing(frames):
            async for frame in frames:
                if not await watch.ok():
                    return
                yield frame

    return StreamingResponse(
        frames if watch is None else watched(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )




@router.post("/{thread_id}/tasks/{task_id}/cancel")
async def cancel_subagent_task(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    x_user_id: RunUserId,
):
    """Stop one background task (e.g. a workflow run) without touching the turn."""
    await thread_access(thread_id, x_user_id)
    from src.server.services.cancel_dispatch import cancel_subagent_task

    return await cancel_subagent_task(thread_id, task_id)


@router.post("/{thread_id}/tasks/{task_id}/messages")
async def send_subagent_message(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    request: SubagentMessageRequest,
    x_user_id: RunUserId,
):
    """Send a message/instruction to a running background subagent. In a
    shared workspace, only the member whose turn launched it instructs it."""
    access = await thread_access(thread_id, x_user_id)
    written_by = None
    if access.is_shared:
        await refuse_others_task(thread_id, task_id, x_user_id)
        from src.server.services.shared_workspaces.turn import member_sender

        # Named on the task's run as on the thread, so it reads as theirs.
        written_by = await member_sender(x_user_id)
    from src.server.handlers.chat import steer_subagent

    return await steer_subagent(
        thread_id=thread_id,
        task_id=task_id,
        content=request.content,
        user_id=x_user_id,
        input_id=request.input_id,
        written_by=written_by,
    )


@router.post("/{thread_id}/tasks/{task_id}/messages/{input_id}/send-now")
async def send_subagent_message_now(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    input_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,64}$")],
    x_user_id: RunUserId,
):
    """Send a queued message to a background task now: a command it waits
    on is set aside, still running, however busy it is. ``sent_now`` is
    false once the task read or settled it."""
    await thread_access(thread_id, x_user_id)
    from src.server.services.report_back.task_steer import send_task_message_now

    return {"sent_now": await send_task_message_now(thread_id, task_id, input_id, x_user_id)}
