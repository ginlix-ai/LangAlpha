"""Thread pre-creation: POST /api/v1/threads — create + auto-title.

Decouples thread creation from the message stream. The client gets the real
thread id immediately in a plain 201, then the generated title arrives on the
user lifecycle feed when the Background model's call lands. The first message
then targets POST /threads/{id}/messages, where ensure_thread_exists finds the
row and no-ops — identical to any follow-up turn. The legacy new-thread door
(POST /threads/messages) stays for channels and older clients; rows from either
door are interchangeable.

⚠️ One-release compatibility shim: bundles shipped before the 201 contract send
``Accept: text/event-stream`` and hard-reject a JSON body, so that header (and
only that header) still selects the legacy SSE frames. The follow-up release
deletes ``_legacy_sse_response`` AND the ``thread_created``/``thread_title``
event-ledger entries together.
"""

import asyncio
import json
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src.config.settings import HOST_MODE
from src.server.database.conversation import create_thread
from src.server.services.turn_runtime import requested_workspace, turn_route
from src.server.models.conversation import (
    ThreadCreateRequest,
    ThreadCreateResponse,
)
from src.server.services.thread_title import schedule_title_generation
from src.server.utils.api import CurrentUserId

from ._deps import SSE_HEADERS, logger, router

# Legacy-branch only: the generator waits slightly past the title task's own
# LLM timeout so the task's internal fallback (never raises, returns the
# stamped title) normally resolves first and this outer cap only catches a
# wedged await.
_TITLE_WAIT_S = 20.0


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def _legacy_sse_response(
    payload: ThreadCreateResponse, title_task: asyncio.Task | None
) -> StreamingResponse:
    """Pre-201 wire: ``thread_created`` → ``thread_title`` → close."""

    async def event_generator():
        yield _sse(
            "thread_created",
            {
                "thread_id": payload.thread_id,
                "workspace_id": payload.workspace_id,
                "thread_index": payload.thread_index,
                "title": payload.title,
                "msg_type": payload.msg_type,
            },
        )
        title, generated = payload.title, False
        if title_task is not None:
            try:
                # shield: a client disconnect cancels this generator, not the
                # title task — the CAS persist still lands.
                title, generated = await asyncio.wait_for(
                    asyncio.shield(title_task), timeout=_TITLE_WAIT_S
                )
            except Exception as e:
                logger.debug(
                    f"thread-title wait ended early thread_id={payload.thread_id}: {e}"
                )
        yield _sse(
            "thread_title",
            {
                "thread_id": payload.thread_id,
                "title": title,
                "generated": generated,
            },
        )

    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=SSE_HEADERS
    )


@router.post("", status_code=201, response_model=ThreadCreateResponse)
async def create_thread_endpoint(
    request: ThreadCreateRequest, http_request: Request, x_user_id: CurrentUserId
):
    """Create a thread and return it as a plain 201.

    Title generation runs detached — a client that never reads the response
    still gets the title persisted, and the durable truth is always readable
    via GET /threads/{id}.
    """
    workspace = await requested_workspace(x_user_id, request.workspace_id)

    # Same credit gate the follow-up /messages call enforces — a quota-blocked
    # client must not mint durable rows + platform title calls through this
    # door. byok and the surface are reported the same way the message path
    # reports them, so both doors get the same verdict for the same user.
    if HOST_MODE != "oss":
        from src.server.database.api_keys import is_byok_active
        from src.server.database.oauth_tokens import has_any_oauth_token
        from src.server.dependencies.usage_limits import enforce_credit_limit

        is_byok, has_oauth = await asyncio.gather(
            is_byok_active(x_user_id), has_any_oauth_token(x_user_id)
        )
        await enforce_credit_limit(
            x_user_id, byok=is_byok or has_oauth, surface=request.platform
        )

    # Decided where the message path decides it, so the thread is born in the
    # workspace its runs will actually use.
    route = turn_route(x_user_id, workspace)
    await route.bind(x_user_id, workspace)
    workspace_id = route.workspace_id

    # A title the caller named is the thread's own: nothing generates over it,
    # since the first message may be no words of the user's at all. Else the
    # same stamp ensure_thread_exists would apply, which is also the CAS
    # expectation the title generator swaps against.
    named_title = request.title
    stamped_title = named_title if named_title is not None else request.first_query[:255]

    thread_id = str(uuid4())
    try:
        row = await create_thread(
            conversation_thread_id=thread_id,
            workspace_id=workspace_id,
            current_status="completed",
            thread_index=None,
            title=stamped_title,
            platform=request.platform,
            subagents_allowed=request.subagents_allowed,
            reasoning_effort=request.reasoning_effort,
            fast_mode=request.fast_mode,
        )
    except Exception as e:
        logger.exception(f"Error pre-creating thread: {e}")
        raise HTTPException(status_code=500, detail="Failed to create thread")

    title_task = None if named_title is not None else schedule_title_generation(
        thread_id=thread_id,
        user_id=x_user_id,
        first_query=request.first_query,
        expected_title=stamped_title,
        timezone=request.timezone,
    )

    payload = ThreadCreateResponse(
        thread_id=thread_id,
        workspace_id=str(workspace_id),
        thread_index=row["thread_index"],
        title=stamped_title,
        msg_type=row["msg_type"],
        subagents_allowed=row["subagents_allowed"],
        reasoning_effort=row.get("reasoning_effort"),
        fast_mode=row.get("fast_mode"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )

    if "text/event-stream" in (http_request.headers.get("accept") or ""):
        return _legacy_sse_response(payload, title_task)

    return JSONResponse(status_code=201, content=payload.model_dump(mode="json"))
