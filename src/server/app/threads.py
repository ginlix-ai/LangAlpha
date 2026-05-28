"""
Unified Thread Router — all thread-related endpoints under /api/v1/threads.

Route definitions are thin; business logic lives in handlers/.
"""

import json
import logging
import secrets
from datetime import datetime, timezone
from typing import Annotated, Optional
from uuid import uuid4

import asyncio
import hmac
import os

from fastapi import APIRouter, Header, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src.server.utils.api import (
    CurrentUserId,
    require_thread_owner,
    require_workspace_owner,
)
from src.server.models.chat import ChatRequest, SubagentMessageRequest
from src.server.models.conversation import (
    WorkspaceThreadListItem,
    WorkspaceThreadsListResponse,
    ThreadUpdateRequest,
    ThreadDeleteResponse,
    ThreadShareRequest,
    ThreadShareResponse,
    SharePermissions,
    FeedbackRequest,
    FeedbackResponse,
)
from src.server.models.workflow import RetryRequest
from src.server.database.conversation import (
    get_workspace_threads,
    get_threads_for_user,
    delete_thread,
    update_thread_title,
    get_thread_by_id,
    update_thread_sharing,
    lookup_thread_by_external_id,
    get_next_turn_index,
    upsert_feedback,
    get_feedback_for_thread,
    delete_feedback,
    get_replay_thread_data,
)
from psycopg_pool import PoolTimeout
from src.server.dependencies.usage_limits import ChatRateLimited

from src.observability import (
    observe_background_chat_turn,
    observe_chat_stream,
    observe_replay_stream,
    safe_add,
    sse_reconnects,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)


# Strong references to background dispatch tasks to prevent GC.
# Tasks remove themselves via done callback.
_background_tasks: set[asyncio.Task] = set()


def _get_service_token() -> str:
    """Read INTERNAL_SERVICE_TOKEN at call time (not import time)."""
    return os.getenv("INTERNAL_SERVICE_TOKEN", "")


def _track_task(task: asyncio.Task) -> None:
    """Hold a strong reference to *task* until it completes."""
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _consume_background_gen(
    gen, label: str, thread_id: str, run_id: str
) -> bool:
    """Drain an async generator in the background, cleaning up Redis on failure."""
    _ok = True
    _error_text: str | None = None
    try:
        async for _ in gen:
            pass
    except Exception as exc:
        _ok = False
        _error_text = f"{type(exc).__name__}: {exc}"
        logger.error(
            f"[{label}] Background workflow failed: thread_id={thread_id} run_id={run_id}",
            exc_info=True,
        )
        try:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if cache.enabled and cache.client:
                origin = await cache.get(f"ptc_origin:{thread_id}")
                if origin:
                    flash_tid = origin.get("flash_thread_id")
                    await cache.delete(f"ptc_origin:{thread_id}")
                    if flash_tid:
                        watch_key = f"flash_watch:{flash_tid}"
                        await cache.client.srem(watch_key, thread_id)
                        remaining = await cache.client.scard(watch_key)
                        if remaining == 0:
                            await cache.client.delete(watch_key)
                        await cache.client.publish(
                            f"thread:wake:{flash_tid}",
                            '{"error": "background_workflow_failed"}',
                        )
        except Exception:
            logger.warning(f"[{label}] Redis cleanup after failure also failed", exc_info=True)
    finally:
        # When the generator raised before reaching start_workflow, the
        # frontend already received {status: dispatched, run_id} and
        # navigated to workflow:stream:{tid}:{rid} — but no events will
        # ever land. Write a terminal `error` SSE so a reconnected client
        # sees the failure instead of silently waiting on an empty stream.
        # Wrapped in its own try/except so failure to emit never blocks
        # the placeholder/tracker cleanup below.
        if not _ok:
            try:
                from src.server.services.background_task_manager import stream_key
                from src.utils.cache.redis_cache import get_cache_client

                cache = get_cache_client()
                if cache.enabled and cache.client:
                    error_payload = {
                        "thread_id": thread_id,
                        "content": "background workflow failed",
                        "error_type": "background_failure",
                        "error": _error_text or "background workflow failed",
                    }
                    sse_wire = (
                        f"event: error\n"
                        f"data: {json.dumps(error_payload, ensure_ascii=False)}\n\n"
                    )
                    await cache.client.xadd(
                        stream_key(thread_id, run_id),
                        {b"event": sse_wire.encode("utf-8")},
                    )
            except Exception:
                logger.warning(
                    f"[{label}] Failed to emit terminal error SSE for "
                    f"thread_id={thread_id} run_id={run_id}",
                    exc_info=True,
                )

        try:
            from src.server.services.background_task_manager import (
                BackgroundTaskManager,
                TaskStatus,
            )
            from src.server.services.workflow_tracker import WorkflowTracker

            manager = BackgroundTaskManager.get_instance()
            key = (thread_id, run_id)
            async with manager.task_lock:
                task_info = manager.tasks.get(key)
                if task_info and task_info.status == TaskStatus.QUEUED and task_info.task is None:
                    del manager.tasks[key]
                    logger.info(
                        f"[{label}] Cleaned up pre-registered placeholder "
                        f"for {key} (workflow never started)"
                    )

            tracker = WorkflowTracker.get_instance()
            status = await tracker.get_status(thread_id)
            if status and status.get("status") == "active":
                meta = status.get("metadata", {})
                if meta.get("dispatched") and status.get("run_id") == run_id:
                    if _ok:
                        await tracker.mark_completed(thread_id, run_id=run_id)
                    else:
                        await tracker.mark_failed(
                            thread_id, error=_error_text, run_id=run_id
                        )
        except Exception:
            pass
    return _ok


# Single router for all thread operations
router = APIRouter(prefix="/api/v1/threads", tags=["Threads"])

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}


# =============================================================================
# THREAD CRUD
# =============================================================================


@router.get("", response_model=WorkspaceThreadsListResponse)
async def list_threads(
    x_user_id: CurrentUserId,
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    limit: int = Query(20, ge=1, le=100, description="Max threads per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query(
        "updated_at", description="Sort field (created_at, updated_at)"
    ),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    platform_prefix: Optional[str] = Query(
        None,
        description="Prefix filter on platform column. 'market_view' matches "
        "'market_view:AAPL' and any future 'market_view:*' suffixes; 'web' "
        "matches exact 'web' since no suffix exists for that origin.",
    ),
):
    """
    List threads with optional workspace + platform-prefix filter.

    When workspace_id is provided, returns threads for that workspace.
    Otherwise returns all threads for the authenticated user.
    """
    try:
        if workspace_id:
            from src.server.database.workspace import get_workspace as db_get_workspace

            workspace = await db_get_workspace(workspace_id)
            require_workspace_owner(workspace, user_id=x_user_id)
            threads, total = await get_workspace_threads(
                workspace_id=workspace_id,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
                platform_prefix=platform_prefix,
            )
        else:
            threads, total = await get_threads_for_user(
                user_id=x_user_id,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
                platform_prefix=platform_prefix,
            )

        thread_items = [
            WorkspaceThreadListItem(
                thread_id=str(thread["conversation_thread_id"]),
                workspace_id=str(thread["workspace_id"]),
                thread_index=thread["thread_index"],
                current_status=thread["current_status"],
                msg_type=thread.get("msg_type"),
                title=thread.get("title"),
                first_query_content=thread.get("first_query_content"),
                platform=thread.get("platform"),
                is_shared=bool(thread.get("is_shared", False)),
                created_at=thread["created_at"],
                updated_at=thread["updated_at"],
            )
            for thread in threads
        ]

        return WorkspaceThreadsListResponse(
            threads=thread_items,
            total=total,
            limit=limit,
            offset=offset,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error listing threads: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list threads: {str(e)}",
        )


@router.get("/{thread_id}")
async def get_thread(thread_id: str, x_user_id: CurrentUserId):
    """Get thread metadata. Used by frontend to resolve workspaceId from threadId."""
    await require_thread_owner(thread_id, x_user_id)
    thread = await get_thread_by_id(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return WorkspaceThreadListItem(
        thread_id=str(thread["conversation_thread_id"]),
        workspace_id=str(thread["workspace_id"]),
        thread_index=thread["thread_index"],
        current_status=thread["current_status"],
        msg_type=thread.get("msg_type"),
        title=thread.get("title"),
        created_at=thread["created_at"],
        updated_at=thread["updated_at"],
    )


@router.delete("/{thread_id}", response_model=ThreadDeleteResponse)
async def delete_thread_endpoint(thread_id: str, x_user_id: CurrentUserId):
    """
    Delete a thread and all its queries/responses.

    Permanently deletes the thread and all associated data due to CASCADE constraints.
    """
    try:
        await require_thread_owner(thread_id, x_user_id)
        await delete_thread(thread_id)

        # Invalidate existence cache
        from src.server.database.conversation import thread_exists_key
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(thread_exists_key(thread_id))
            except Exception:
                pass

        logger.info(f"Successfully deleted thread thread_id={thread_id}")
        return ThreadDeleteResponse(
            success=True,
            thread_id=thread_id,
            message="Thread deleted successfully",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to delete thread: {str(e)}"
        )


@router.patch("/{thread_id}", response_model=WorkspaceThreadListItem)
async def update_thread_endpoint(
    thread_id: str, request: ThreadUpdateRequest, x_user_id: CurrentUserId
):
    """Update thread properties (currently only title)."""
    try:
        await require_thread_owner(thread_id, x_user_id)
        updated_thread = await update_thread_title(thread_id, request.title)

        if not updated_thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )

        return WorkspaceThreadListItem(
            thread_id=str(updated_thread["conversation_thread_id"]),
            workspace_id=str(updated_thread["workspace_id"]),
            thread_index=updated_thread["thread_index"],
            current_status=updated_thread["current_status"],
            msg_type=updated_thread.get("msg_type"),
            title=updated_thread.get("title"),
            created_at=updated_thread["created_at"],
            updated_at=updated_thread["updated_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to update thread: {str(e)}"
        )


# =============================================================================
# THREAD MESSAGES (SSE streams)
# =============================================================================


@router.post("/messages")
async def send_new_thread_message(
    request: ChatRequest, auth: ChatRateLimited, raw_request: Request
):
    """
    Create a new thread and send the first message. Returns an SSE stream.

    The server creates a new thread_id and returns it in SSE events.
    If external_thread_id + platform are provided, resolves to an existing thread first.
    """
    thread_id = None
    if request.external_thread_id and request.platform:
        thread_id = await lookup_thread_by_external_id(
            request.platform, request.external_thread_id, auth.user_id
        )
        if thread_id:
            logger.info(
                f"[CHAT] Resolved external_thread_id={request.external_thread_id} "
                f"platform={request.platform} -> thread_id={thread_id}"
            )
    if not thread_id:
        thread_id = str(uuid4())
    return await _handle_send_message(request, auth, thread_id, raw_request)


@router.post("/{thread_id}/messages")
async def send_thread_message(
    thread_id: str, request: ChatRequest, auth: ChatRateLimited,
    raw_request: Request,
):
    """
    Send a message to an existing thread. Returns an SSE stream.
    """
    return await _handle_send_message(request, auth, thread_id, raw_request)


async def _handle_send_message(
    request: ChatRequest, auth: ChatRateLimited, thread_id: str,
    raw_request: Request | None = None,
):
    """Shared logic for both POST /threads/messages and POST /threads/{id}/messages."""
    from src.server.handlers.chat import (
        astream_flash_workflow,
        astream_ptc_workflow,
    )
    from src.server.database.workspace import get_or_create_flash_workspace

    from src.server.database.workspace import get_workspace

    # Canonical run_id generation site. Each POST gets a fresh UUID that
    # flows through every downstream key: BTM ``(tid, rid)``, persistence
    # service, ``workflow:stream:{tid}:{rid}``, LangGraph ``config["run_id"]``
    # → ``CheckpointMetadata.run_id``, and the SSE ``metadata`` event the
    # frontend sees as the first event of the stream. 1:1 with
    # ``conversation_response_id``.
    run_id = str(uuid4())

    user_id = auth.user_id
    is_byok = auth.is_byok
    agent_mode = request.agent_mode or "ptc"
    workspace_id = request.workspace_id

    from src.server.dependencies.usage_limits import release_burst_slot

    try:
        # 403 guard: require BYOK, OAuth, or platform access (tier >= 0).
        # All flags are pre-checked by enforce_chat_limit — no DB calls here.
        from src.config.settings import HOST_MODE
        if HOST_MODE == "platform" and not auth.is_byok and not auth.has_oauth and auth.access_tier < 0:
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "No provider configured. Set up an API key or connect via OAuth.",
                    "type": "no_provider",
                    "link": {"url": "/setup/method", "label": "Set up provider"},
                },
            )

        # Resolve workspace_id from thread if not provided
        if not workspace_id and thread_id:
            thread_record = await get_thread_by_id(thread_id)
            if thread_record:
                workspace_id = str(thread_record["workspace_id"])
                logger.debug(
                    f"[CHAT] Resolved workspace_id={workspace_id} from thread_id={thread_id}"
                )

        # Validate that agent_config is initialized
        if not hasattr(setup, "agent_config") or setup.agent_config is None:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # Validate workspace_id for ptc mode
        if agent_mode == "ptc" and not workspace_id:
            raise HTTPException(
                status_code=400,
                detail="workspace_id is required for 'ptc' agent mode. Create workspace first via POST /workspaces, or use agent_mode='flash' for lightweight queries.",
            )

        # For flash mode, resolve workspace_id to the shared flash workspace
        if agent_mode == "flash" and not workspace_id:
            flash_ws = await get_or_create_flash_workspace(user_id)
            workspace_id = str(flash_ws["workspace_id"])

        # Auto-detect flash workspaces: if the workspace is flash, override agent_mode
        # so follow-up messages (HITL responses, etc.) route correctly even if
        # the client doesn't send agent_mode='flash'.
        # Skip the DB query when a ready session exists (PTC workspace, common path).
        if agent_mode != "flash" and workspace_id:
            from src.server.services.workspace_manager import WorkspaceManager
            _wm = WorkspaceManager.get_instance()
            if not _wm.has_ready_session(workspace_id):
                ws = await get_workspace(workspace_id)
                if ws and ws.get("status") == "flash":
                    agent_mode = "flash"
                    logger.debug(
                        f"[CHAT] Auto-detected flash workspace {workspace_id}, "
                        f"overriding agent_mode to 'flash'"
                    )

        # Extract user input
        user_input = ""
        if request.messages:
            last_msg = request.messages[-1]
            if isinstance(last_msg.content, str):
                user_input = last_msg.content
            elif isinstance(last_msg.content, list):
                for item in last_msg.content:
                    if hasattr(item, "text") and item.text:
                        user_input = item.text
                        break

        logger.info(
            f"[{'FLASH' if agent_mode == 'flash' else 'PTC'}_CHAT] New request: "
            f"workspace_id={workspace_id} thread_id={thread_id} user_id={user_id} "
            f"mode={agent_mode}"
        )

        # Resolve LLM config eagerly — credit check must happen before SSE stream starts
        from src.server.handlers.chat import resolve_llm_config
        from src.server.dependencies.usage_limits import enforce_credit_limit
        from ptc_agent.config.agent import CredentialSource

        config = await resolve_llm_config(
            setup.agent_config,
            user_id,
            request.llm_model,
            is_byok,
            mode=agent_mode,
            reasoning_effort=getattr(request, "reasoning_effort", None),
            fast_mode=getattr(request, "fast_mode", None),
            thread_id=thread_id,
            enabled_subagents=request.subagents_enabled,
        )

        # is_byok is True only when the stamped credential_source confirms the user
        # supplied their own key (OAUTH or BYOK), not merely that a client object exists.
        is_byok = config.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK)

        # Credit check: always enforce.
        # - Platform-served (is_byok=False): block when daily limit reached.
        # - BYOK/OAuth (is_byok=True): block only on negative balance (outstanding
        #   debt from past platform usage, e.g. fallback routing).
        await enforce_credit_limit(user_id, byok=is_byok)

        # Only honour X-Dispatch: background for internal service-to-service calls.
        _req_token = (raw_request.headers.get("X-Service-Token", "") if raw_request else "")
        _svc_token = _get_service_token()
        is_internal = bool(_svc_token and _req_token and hmac.compare_digest(_req_token, _svc_token))

        # Strip query_type from non-internal requests (prevent spoofing system messages)
        if not is_internal and request.query_type:
            request = request.model_copy(update={"query_type": None})
    except BaseException:
        await release_burst_slot(user_id)
        raise

    # Resolve model name for observability labels (bounded by models.json keys).
    _llm = getattr(config, "llm", None)
    _model = (getattr(_llm, "flash", None) if agent_mode == "flash" else getattr(_llm, "name", None)) or ""

    # Content-Location header advertises the reconnect URL for this run.
    # Mirrors langgraph_sdk's protocol so reconnects target the exact run.
    sse_headers_with_loc = {
        **SSE_HEADERS,
        "Content-Location": f"/api/v1/threads/{thread_id}/messages/stream?run_id={run_id}",
    }

    # Route to appropriate streaming function based on agent mode
    if agent_mode == "flash":
        is_flash_dispatch = (
            is_internal
            and raw_request
            and raw_request.headers.get("X-Dispatch") == "background"
        )
        flash_gen = astream_flash_workflow(
            request=request,
            thread_id=thread_id,
            run_id=run_id,
            user_input=user_input,
            user_id=user_id,
            is_byok=is_byok,
            config=config,
            dispatched=is_flash_dispatch,
        )

        if is_flash_dispatch:
            from src.server.services.background_task_manager import BackgroundTaskManager
            manager = BackgroundTaskManager.get_instance()
            # Fail-fast admission at the HTTP boundary: if another run is
            # still active on this thread, wait for it to settle (up to
            # the soft-interrupt timeout). If it doesn't settle, return
            # 409 here rather than dispatching a doomed background task.
            #
            # The admission_lock is held across wait_for_soft_interrupted +
            # pre_register so two concurrent dispatched POSTs on the same
            # thread can't both pass the gate and start workflows on the
            # same LangGraph thread_id (the foreground branch acquires
            # this same lock inside its handler via wait_or_steer; the
            # dispatched branch must do it here because it skips
            # wait_or_steer entirely). Released before _track_task
            # schedules the background workflow.
            admission_lock = await manager.get_admission_lock(thread_id)
            async with admission_lock:
                settled = await manager.wait_for_soft_interrupted(
                    thread_id, exclude_run_id=run_id
                )
                if not settled:
                    await release_burst_slot(user_id)
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Workflow {thread_id} is still running; dispatched "
                            "follow-up could not be admitted."
                        ),
                    )
                await manager.pre_register(thread_id, run_id)
            _track_task(asyncio.create_task(
                observe_background_chat_turn(
                    _consume_background_gen(flash_gen, "FLASH_DISPATCH", thread_id, run_id),
                    mode="flash",
                    model=_model,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    thread_id=thread_id,
                ),
                name=f"flash-dispatch-{thread_id}-{run_id[:8]}",
            ))
            logger.info(
                f"[FLASH_DISPATCH] Started background workflow: "
                f"thread_id={thread_id} run_id={run_id}"
            )
            return JSONResponse({
                "status": "dispatched",
                "thread_id": thread_id,
                "run_id": run_id,
            })

        return StreamingResponse(
            observe_chat_stream(
                flash_gen,
                mode="flash",
                model=_model,
                user_id=user_id,
                workspace_id=workspace_id,
                thread_id=thread_id,
            ),
            media_type="text/event-stream",
            headers=sse_headers_with_loc,
        )

    is_ptc_dispatch = (
        is_internal
        and raw_request
        and raw_request.headers.get("X-Dispatch") == "background"
    )
    ptc_gen = astream_ptc_workflow(
        request=request,
        thread_id=thread_id,
        run_id=run_id,
        user_input=user_input,
        user_id=user_id,
        workspace_id=workspace_id,
        is_byok=is_byok,
        config=config,
        dispatched=is_ptc_dispatch,
    )

    if is_ptc_dispatch:
        from src.server.services.background_task_manager import BackgroundTaskManager
        from src.server.services.workflow_tracker import WorkflowTracker

        tracker = WorkflowTracker.get_instance()
        manager = BackgroundTaskManager.get_instance()
        # Fail-fast admission at the HTTP boundary: if another run is still
        # active on this thread, wait for it to settle. If it doesn't,
        # return 409 here rather than dispatching a doomed background task.
        #
        # The admission_lock is held across wait_for_soft_interrupted +
        # mark_active + pre_register so two concurrent dispatched POSTs on
        # the same thread can't both pass the gate and start workflows on
        # the same LangGraph thread_id (the foreground branch acquires
        # this same lock inside its handler via wait_or_steer; the
        # dispatched branch must do it here because it skips
        # wait_or_steer entirely). Released before _track_task schedules
        # the background workflow.
        admission_lock = await manager.get_admission_lock(thread_id)
        async with admission_lock:
            settled = await manager.wait_for_soft_interrupted(
                thread_id, exclude_run_id=run_id
            )
            if not settled:
                await release_burst_slot(user_id)
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Workflow {thread_id} is still running; dispatched "
                        "follow-up could not be admitted."
                    ),
                )
            await tracker.mark_active(
                thread_id=thread_id,
                workspace_id=workspace_id,
                user_id=user_id,
                run_id=run_id,
                metadata={"type": "ptc_agent", "dispatched": True},
            )
            await manager.pre_register(thread_id, run_id)

        _track_task(asyncio.create_task(
            observe_background_chat_turn(
                _consume_background_gen(ptc_gen, "PTC_DISPATCH", thread_id, run_id),
                mode="ptc",
                model=_model,
                user_id=user_id,
                workspace_id=workspace_id,
                thread_id=thread_id,
            ),
            name=f"ptc-dispatch-{thread_id}-{run_id[:8]}",
        ))
        logger.info(
            f"[PTC_DISPATCH] Started background workflow: "
            f"thread_id={thread_id} run_id={run_id} workspace_id={workspace_id}"
        )
        return JSONResponse({
            "status": "dispatched",
            "thread_id": thread_id,
            "run_id": run_id,
            "workspace_id": workspace_id,
        })

    return StreamingResponse(
        observe_chat_stream(
            ptc_gen,
            mode="ptc",
            model=_model,
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
        ),
        media_type="text/event-stream",
        headers=sse_headers_with_loc,
    )


@router.get("/{thread_id}/messages/stream")
async def reconnect_to_stream(
    thread_id: str,
    x_user_id: CurrentUserId,
    last_event_id: Optional[int] = Query(None, description="Last received event ID"),
    last_event_id_header: Optional[str] = Header(None, alias="Last-Event-ID"),
    run_id: Optional[str] = Query(None, description="Specific run to reconnect to"),
):
    """Reconnect to a running or completed workflow's SSE stream.

    ``run_id`` targets a specific turn. If omitted, falls back to the
    latest run on the thread (matches the single-turn happy path).
    """
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat import reconnect_to_workflow_stream

    safe_add(sse_reconnects, 1)

    if last_event_id is None and last_event_id_header is not None:
        try:
            last_event_id = int(last_event_id_header)
        except ValueError:
            pass

    async def stream_reconnection():
        try:
            async for event in reconnect_to_workflow_stream(
                thread_id, run_id, last_event_id
            ):
                yield event
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[PTC_RECONNECT] Error: {e}", exc_info=True)
            yield f'event: error\ndata: {{"error": "Reconnection failed: {str(e)}"}}\n\n'

    return StreamingResponse(
        stream_reconnection(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/{thread_id}/watch")
async def watch_thread(thread_id: str, x_user_id: CurrentUserId):
    """Watch for new workflow activity on a thread via SSE + Redis pub/sub.

    Opens a lightweight SSE connection that emits a single ``workflow_started``
    event when a new workflow begins on this thread (e.g., flash report-back
    after PTC completion).  The client should then close the connection and
    reconnect via ``/messages/stream``.

    Sends keepalive pings every 45 seconds.  Auto-closes after 30 minutes
    to prevent leaked connections from abandoned browser tabs.
    """
    await require_thread_owner(thread_id, x_user_id)

    from src.utils.cache.redis_cache import get_cache_client

    CHANNEL = f"thread:wake:{thread_id}"
    KEEPALIVE_INTERVAL = 45  # seconds
    MAX_WATCH_DURATION = 30 * 60  # 30 minutes

    async def watch_generator():
        import time

        cache = get_cache_client()
        if not cache.enabled or not cache.client:
            yield 'event: error\ndata: {"error": "watch unavailable"}\n\n'
            return

        pubsub = cache.client.pubsub()
        started_at = time.monotonic()
        try:
            await pubsub.subscribe(CHANNEL)

            while True:
                if time.monotonic() - started_at > MAX_WATCH_DURATION:
                    yield 'event: timeout\ndata: {}\n\n'
                    break

                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=KEEPALIVE_INTERVAL)

                if msg and msg["type"] == "message":
                    data = msg["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    yield f'event: workflow_started\ndata: {data}\n\n'
                    break
                else:
                    yield ': ping\n\n'
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()

    return StreamingResponse(
        watch_generator(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/{thread_id}/messages/replay")
async def replay_thread_messages(thread_id: str, x_user_id: CurrentUserId):
    """Replay a thread as SSE using persisted sse_events.

    Stream includes:
    - user_message: emitted once per turn_index (query content)
    - message_chunk/tool_* events: emitted from stored sse_events
    - replay_done: terminal sentinel
    """
    try:
        owner_id, thread, queries, responses = await get_replay_thread_data(thread_id)

        # Preserve existing 404/403 semantics from require_thread_owner
        if owner_id is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        if owner_id != x_user_id:
            raise HTTPException(status_code=403, detail="Forbidden")
        if not thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )

        responses_by_turn = {
            r.get("turn_index"): r for r in responses if isinstance(r, dict)
        }

        async def event_generator():
            seq = 0

            for q in queries:
                if not isinstance(q, dict):
                    continue

                turn_index = q.get("turn_index")
                seq += 1
                payload = {
                    "thread_id": thread_id,
                    "turn_index": turn_index,
                    "content": q.get("content"),
                    "timestamp": q.get("created_at"),
                    "metadata": q.get("metadata"),
                }
                # Tag system queries so the frontend can hide the user bubble
                query_type = q.get("type")
                if query_type == "system":
                    payload["query_type"] = "system"

                yield (
                    f"id: {seq}\n"
                    f"event: user_message\n"
                    f"data: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"
                )

                response = responses_by_turn.get(turn_index)
                if not response:
                    continue

                sse_events = response.get("sse_events")
                if not (isinstance(sse_events, list) and sse_events):
                    continue

                for item in sse_events:
                    if not isinstance(item, dict):
                        continue
                    event_type = item.get("event")
                    data = item.get("data")
                    if not event_type or not isinstance(data, dict):
                        continue

                    seq += 1
                    replay_data = dict(data)
                    replay_data.setdefault("thread_id", thread_id)
                    replay_data["turn_index"] = turn_index
                    replay_data["response_id"] = str(
                        response.get("conversation_response_id")
                    )

                    yield (
                        f"id: {seq}\n"
                        f"event: {event_type}\n"
                        f"data: {json.dumps(replay_data, ensure_ascii=False, default=str)}\n\n"
                    )

            seq += 1
            yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

        return StreamingResponse(
            observe_replay_stream(event_generator(), source="private"),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    except PoolTimeout:
        raise HTTPException(
            status_code=503,
            detail="Database connection pool busy, please retry",
            headers={"Retry-After": "2"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error replaying thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to replay thread: {str(e)}"
        )


# =============================================================================
# THREAD CONTROL (was "workflow")
# =============================================================================


@router.get("/{thread_id}/status")
async def get_thread_status(thread_id: str, x_user_id: CurrentUserId):
    """Get current workflow execution status for a thread."""
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import get_workflow_status

    return await get_workflow_status(thread_id)


@router.post("/{thread_id}/cancel", status_code=200)
async def cancel_thread(thread_id: str, x_user_id: CurrentUserId):
    """Cancel a running workflow for this thread."""
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import cancel_workflow

    return await cancel_workflow(thread_id)


@router.post("/{thread_id}/interrupt", status_code=200)
async def interrupt_thread(thread_id: str, x_user_id: CurrentUserId):
    """Soft interrupt — pause main agent, keep subagents running."""
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import soft_interrupt_workflow

    return await soft_interrupt_workflow(thread_id)


@router.post("/{thread_id}/summarize", status_code=200)
async def summarize_thread(
    thread_id: str,
    x_user_id: CurrentUserId,
    keep_messages: int = Query(
        default=5, ge=1, le=20, description="Number of recent messages to preserve"
    ),
):
    """Manually trigger context compaction for a thread.

    Endpoint path ``/summarize`` and function name preserved for REST contract
    compatibility — clients may call the older URL.
    """
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import trigger_compaction

    return await trigger_compaction(thread_id, keep_messages, user_id=x_user_id)


@router.post("/{thread_id}/offload", status_code=200)
async def offload_thread(thread_id: str, x_user_id: CurrentUserId):
    """Truncate large tool arguments and offload originals to sandbox (Tier 1 only)."""
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import trigger_offload

    return await trigger_offload(thread_id)


@router.get("/{thread_id}/turns")
async def get_thread_turns(thread_id: str, x_user_id: CurrentUserId):
    """
    Get turn-boundary checkpoint IDs for edit/regenerate/retry operations.

    Returns per-turn checkpoint IDs:
    - edit_checkpoint_id: fork BEFORE the user message (for editing)
    - regenerate_checkpoint_id: fork AFTER user message, BEFORE AI response (for regenerating)
    - retry_checkpoint_id: most recent checkpoint (for retrying after failure)
    """
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.checkpoint_handler import (
        get_thread_turns as _get_thread_turns,
    )
    from src.server.database.conversation import get_thread_checkpoint_id

    branch_tip = await get_thread_checkpoint_id(thread_id)
    return await _get_thread_turns(thread_id, branch_tip_checkpoint_id=branch_tip)


@router.post("/{thread_id}/retry")
async def retry_thread(
    thread_id: str,
    auth: ChatRateLimited,
    body: Optional[RetryRequest] = None,
):
    """
    Retry a failed or interrupted thread from its last checkpoint.

    Accepts optional checkpoint_id in request body for precise control.
    If not provided, auto-detects the latest checkpoint.
    Returns an SSE stream.
    """
    await require_thread_owner(thread_id, auth.user_id)
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint

    explicit_checkpoint_id = body.checkpoint_id if body else None
    retry_checkpoint_id = await get_retry_checkpoint(thread_id, explicit_checkpoint_id)

    # Resolve workspace_id from body or from the thread record
    workspace_id = body.workspace_id if body and body.workspace_id else None
    if not workspace_id:
        thread_record = await get_thread_by_id(thread_id)
        if not thread_record:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        workspace_id = str(thread_record.get("workspace_id", ""))

    # Calculate fork_from_turn for retry: overwrite the last (failed) turn
    current_count = await get_next_turn_index(thread_id)
    fork_turn = max(0, current_count - 1)

    # Delegate to the existing message flow with checkpoint_id and empty messages
    request = ChatRequest(
        workspace_id=workspace_id,
        messages=[],
        checkpoint_id=retry_checkpoint_id,
        fork_from_turn=fork_turn,
    )

    return await _handle_send_message(request, auth, thread_id)


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
    ``Last-Event-ID`` HTTP header.
    """
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat import stream_subagent_task_events

    if last_event_id is None and last_event_id_header is not None:
        try:
            last_event_id = int(last_event_id_header)
        except ValueError:
            pass

    return StreamingResponse(
        stream_subagent_task_events(thread_id, task_id, last_event_id),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.post("/{thread_id}/tasks/{task_id}/messages")
async def send_subagent_message(
    thread_id: str,
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,12}$")],
    request: SubagentMessageRequest,
    x_user_id: CurrentUserId,
):
    """Send a message/instruction to a running background subagent."""
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat import steer_subagent

    return await steer_subagent(
        thread_id=thread_id,
        task_id=task_id,
        content=request.content,
        user_id=x_user_id,
    )


# =============================================================================
# THREAD SHARING
# =============================================================================


@router.post("/{thread_id}/share", response_model=ThreadShareResponse)
async def update_thread_share(
    thread_id: str,
    request: ThreadShareRequest,
    x_user_id: CurrentUserId,
):
    """Toggle public sharing for a thread and update permissions."""
    await require_thread_owner(thread_id, x_user_id)

    thread = await get_thread_by_id(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    # Build update kwargs
    kwargs: dict = {"is_shared": request.is_shared}

    # Generate share_token on first enable (reuse existing on re-enable)
    if request.is_shared and not thread.get("share_token"):
        kwargs["share_token"] = secrets.token_urlsafe(16)

    if request.is_shared:
        kwargs["shared_at"] = datetime.now(timezone.utc)

    # Merge permissions: start from existing, overlay provided fields
    existing_perms = thread.get("share_permissions") or {}
    if isinstance(existing_perms, str):
        existing_perms = json.loads(existing_perms)

    if request.permissions is not None:
        merged = {**existing_perms, **request.permissions.model_dump()}
        # Enforce: download requires files
        if merged.get("allow_download") and not merged.get("allow_files"):
            merged["allow_files"] = True
        kwargs["share_permissions"] = merged

    updated = await update_thread_sharing(thread_id, **kwargs)
    if not updated:
        raise HTTPException(status_code=404, detail="Thread not found")

    share_token = updated.get("share_token")
    perms = updated.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)

    return ThreadShareResponse(
        is_shared=updated["is_shared"],
        share_token=share_token if updated["is_shared"] else None,
        share_url=f"/s/{share_token}" if updated["is_shared"] and share_token else None,
        permissions=SharePermissions(**(perms if isinstance(perms, dict) else {})),
    )


@router.get("/{thread_id}/share", response_model=ThreadShareResponse)
async def get_thread_share(thread_id: str, x_user_id: CurrentUserId):
    """Get current share status and permissions for a thread."""
    await require_thread_owner(thread_id, x_user_id)

    thread = await get_thread_by_id(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    share_token = thread.get("share_token")
    is_shared = thread.get("is_shared", False)
    perms = thread.get("share_permissions") or {}
    if isinstance(perms, str):
        perms = json.loads(perms)

    return ThreadShareResponse(
        is_shared=is_shared,
        share_token=share_token if is_shared else None,
        share_url=f"/s/{share_token}" if is_shared and share_token else None,
        permissions=SharePermissions(**(perms if isinstance(perms, dict) else {})),
    )


# ==================== Feedback ====================


@router.post("/{thread_id}/feedback", response_model=FeedbackResponse)
async def submit_feedback(
    thread_id: str,
    request: FeedbackRequest,
    x_user_id: CurrentUserId,
):
    """Submit or update feedback (thumbs up/down) for a response."""
    try:
        await require_thread_owner(thread_id, x_user_id)
        result = await upsert_feedback(
            conversation_thread_id=thread_id,
            turn_index=request.turn_index,
            user_id=x_user_id,
            rating=request.rating,
            issue_categories=request.issue_categories,
            comment=request.comment,
            consent_human_review=request.consent_human_review,
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No response found at turn_index={request.turn_index}",
            )
        return FeedbackResponse(
            conversation_feedback_id=str(result["conversation_feedback_id"]),
            turn_index=result["turn_index"],
            rating=result["rating"],
            issue_categories=result.get("issue_categories"),
            comment=result.get("comment"),
            consent_human_review=result.get("consent_human_review", False),
            review_status=result.get("review_status"),
            created_at=str(result["created_at"]),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error submitting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")


@router.get("/{thread_id}/feedback", response_model=list[FeedbackResponse])
async def get_feedback(thread_id: str, x_user_id: CurrentUserId):
    """Get all feedback for a thread by the current user."""
    try:
        await require_thread_owner(thread_id, x_user_id)
        rows = await get_feedback_for_thread(thread_id, x_user_id)
        return [
            FeedbackResponse(
                conversation_feedback_id=str(row["conversation_feedback_id"]),
                turn_index=row["turn_index"],
                rating=row["rating"],
                issue_categories=row.get("issue_categories"),
                comment=row.get("comment"),
                consent_human_review=row.get("consent_human_review", False),
                review_status=row.get("review_status"),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get feedback")


@router.delete("/{thread_id}/feedback")
async def remove_feedback(
    thread_id: str,
    turn_index: int,
    x_user_id: CurrentUserId,
):
    """Remove feedback for a specific response. Query param: ?turn_index=N"""
    try:
        await require_thread_owner(thread_id, x_user_id)
        deleted = await delete_feedback(thread_id, turn_index, x_user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Feedback not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete feedback")
