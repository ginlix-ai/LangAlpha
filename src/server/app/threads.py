"""
Unified Thread Router — all thread-related endpoints under /api/v1/threads.

Route definitions are thin; business logic lives in handlers/.
"""

import hashlib
import json
import logging
import secrets
from datetime import datetime, timezone
from typing import Annotated, Optional
from uuid import uuid4

import asyncio
import os

from fastapi import APIRouter, Header, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from src.server.utils.api import (
    CurrentUserId,
    StampThreadAuth,
    require_thread_owner,
    require_workspace_owner,
    service_token_matches,
)
from src.server.models.chat import ChatRequest, SubagentMessageRequest
from src.server.models.conversation import (
    WorkspaceThreadListItem,
    WorkspaceThreadsListResponse,
    ThreadUpdateRequest,
    ThreadExternalIdRequest,
    ThreadDeleteResponse,
    ThreadShareRequest,
    ThreadShareResponse,
    SharePermissions,
    FeedbackRequest,
    FeedbackResponse,
)
from src.server.models.workflow import RetryRequest
from src.server.database.conversation import (
    ExternalIdConflictError,
    external_id_conflict_payload,
    get_workspace_threads,
    get_threads_for_user,
    delete_thread,
    update_thread_title,
    update_thread_external_id,
    get_thread_by_id,
    get_thread_owner_id,
    update_thread_sharing,
    lookup_thread_by_external_id,
    get_next_turn_index,
    upsert_feedback,
    get_feedback_for_thread,
    delete_feedback,
    get_replay_thread_data,
)
from src.server.database.provenance import (
    get_provenance_body_refs,
    get_provenance_for_thread,
    get_provenance_record,
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
    gen,
    label: str,
    thread_id: str,
    run_id: str,
    report_back_ptc_thread_id: str | None = None,
    user_id: str | None = None,
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
        # Report-back crash teardown is business logic owned by report_back —
        # it fetches its own cache client and swallows its own failures.
        from src.server.handlers.chat.report_back import clear_on_crash

        await clear_on_crash(thread_id, report_back_ptc_thread_id, user_id)
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
                from src.server.services.background_task_manager import (
                    BackgroundTaskManager,
                    stream_key,
                )
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
                    # Terminal sentinel after the error event so an attached
                    # consumer closes now instead of waiting out the
                    # empty-XREAD handshake (best-effort, swallows failures).
                    await BackgroundTaskManager.get_instance().append_stream_end_sentinel(
                        thread_id, run_id
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


def _thread_list_item(row: dict) -> WorkspaceThreadListItem:
    """Build the list-item response from an updated thread row."""
    return WorkspaceThreadListItem(
        thread_id=str(row["conversation_thread_id"]),
        workspace_id=str(row["workspace_id"]),
        thread_index=row["thread_index"],
        current_status=row["current_status"],
        msg_type=row.get("msg_type"),
        title=row.get("title"),
        platform=row.get("platform"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.patch("/{thread_id}", response_model=WorkspaceThreadListItem)
async def update_thread_endpoint(
    thread_id: str, request: ThreadUpdateRequest, x_user_id: CurrentUserId
):
    """Rename a thread's title."""
    try:
        await require_thread_owner(thread_id, x_user_id)
        updated_thread = await update_thread_title(thread_id, request.title)
        if not updated_thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )
        return _thread_list_item(updated_thread)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to update thread: {str(e)}"
        )


@router.put("/{thread_id}/external-id", response_model=WorkspaceThreadListItem)
async def stamp_thread_external_id_endpoint(
    thread_id: str, request: ThreadExternalIdRequest, user_id: StampThreadAuth
):
    """Stamp a channel identity (``platform`` + ``external_id``) onto a thread.

    A user-scoped caller must own the thread. A privileged service caller (valid
    ``X-Service-Token`` with no ``X-User-Id``) skips the ownership check and
    stamps by thread_id alone — used by the one-time external-id backfill, which
    cannot know each thread's owner. A ``(platform, external_id)`` already held by
    another thread maps to a 409 carrying the shared conflict payload.
    """
    try:
        if user_id is not None:
            await require_thread_owner(thread_id, user_id)
        try:
            updated_thread = await update_thread_external_id(
                thread_id, request.platform, request.external_id
            )
        except ExternalIdConflictError as e:
            raise HTTPException(
                status_code=409,
                detail=external_id_conflict_payload(e.platform, e.external_id),
            )
        if not updated_thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )
        return _thread_list_item(updated_thread)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error stamping thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to stamp thread: {str(e)}"
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

        # Reject an unauthenticated background dispatch up front, before the
        # owner lookup, workspace/LLM resolution, and credit check below — a
        # request that will 403 anyway shouldn't do that work first. In oss mode
        # with no INTERNAL_SERVICE_TOKEN configured there is nothing to
        # authenticate against, so the self-dispatch is trusted; a configured
        # token is enforced in every mode. This single is_internal value is
        # reused by the field strip and both dispatch branches below.
        _req_token = (raw_request.headers.get("X-Service-Token", "") if raw_request else "")
        _svc_token = _get_service_token()
        is_internal = service_token_matches(_req_token, _svc_token) or (
            HOST_MODE == "oss" and not _svc_token
        )
        if (
            not is_internal
            and raw_request
            and raw_request.headers.get("X-Dispatch") == "background"
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Background dispatch requires internal service auth. "
                    "Configure INTERNAL_SERVICE_TOKEN and send it as "
                    "X-Service-Token."
                ),
            )

        # IDOR guard: an existing thread must belong to the caller. A brand-new
        # thread_id has no owner yet -> creation proceeds. The internal report-back
        # dispatch sets X-User-Id to the owner, so it passes.
        owner_id = await get_thread_owner_id(thread_id) if thread_id else None
        if owner_id is not None and owner_id != user_id:
            raise HTTPException(status_code=403, detail="Forbidden")

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

        # For flash mode, resolve workspace_id to the shared flash workspace.
        # The upsert returns the full row, reused by the ownership guard below
        # and by the flash workflow (skipping a repeat upsert).
        workspace: dict | None = None
        flash_workspace: dict | None = None
        if agent_mode == "flash" and not workspace_id:
            workspace = await get_or_create_flash_workspace(user_id)
            workspace_id = str(workspace["workspace_id"])
            flash_workspace = workspace

        # Single workspace lookup, shared by the flash auto-detect and the
        # ownership guard below — one DB round-trip instead of two.
        if workspace is None and workspace_id:
            workspace = await get_workspace(workspace_id)

        # Auto-detect flash workspaces: if the workspace is flash, override
        # agent_mode so follow-up messages (HITL responses, etc.) route
        # correctly even if the client doesn't send agent_mode='flash'. Skip
        # the status check when a ready session exists (PTC workspace, common path).
        if agent_mode != "flash" and workspace_id:
            from src.server.services.workspace_manager import WorkspaceManager
            if not WorkspaceManager.get_instance().has_ready_session(workspace_id):
                if workspace and workspace.get("status") == "flash":
                    agent_mode = "flash"
                    logger.debug(
                        f"[CHAT] Auto-detected flash workspace {workspace_id}, "
                        f"overriding agent_mode to 'flash'"
                    )

        # IDOR guard (workspace dimension): pairs with the thread guard above so
        # a fresh thread_id cannot run inside another user's workspace/sandbox.
        # The internal report-back dispatch sets X-User-Id to the owner, so it passes.
        require_workspace_owner(workspace, user_id=user_id)

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

        # Strip internal-only fields from non-internal requests (prevent
        # spoofing system messages / forging report-back watch cleanup).
        if not is_internal:
            internal_overrides = {}
            if request.query_type:
                internal_overrides["query_type"] = None
            if request.report_back_ptc_thread_id:
                internal_overrides["report_back_ptc_thread_id"] = None
            if internal_overrides:
                request = request.model_copy(update=internal_overrides)
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
            flash_workspace=flash_workspace,
        )

        if is_flash_dispatch:
            from src.server.services.background_task_manager import BackgroundTaskManager
            manager = BackgroundTaskManager.get_instance()
            # Fail-fast admission at the HTTP boundary: if another run is
            # still active (running or stopping) on this thread, return 409
            # here rather than dispatching a doomed background task.
            #
            # The admission_lock is held across wait_for_admission +
            # pre_register so two concurrent dispatched POSTs on the same
            # thread can't both pass the gate and start workflows on the
            # same LangGraph thread_id (the foreground branch acquires
            # this same lock inside its handler via wait_or_steer; the
            # dispatched branch must do it here because it skips
            # wait_or_steer entirely). Released before _track_task
            # schedules the background workflow.
            #
            # Report-back idempotency: a lost-response retry of the drainer's
            # POST must NOT start a second summary run. The claim CM SET-NXs the
            # per-(flash, ptc) run pointer under the lock; a prior admission's
            # incumbent run_id is returned instead, and a non-consummated exit
            # (e.g. a 409 from the gate) releases the claim. No-op unless
            # report_back_ptc_thread_id is set.
            rb_ptc = request.report_back_ptc_thread_id
            rb_cache = None
            if rb_ptc:
                from src.utils.cache.redis_cache import get_cache_client
                rb_cache = get_cache_client()
            from src.server.handlers.chat import report_back
            admission_lock = await manager.get_admission_lock(thread_id)
            async with admission_lock, report_back.claim(
                rb_cache, thread_id, rb_ptc, run_id
            ) as rb_claim:
                if rb_claim.incumbent is not None:
                    await release_burst_slot(user_id)
                    logger.info(
                        f"[FLASH_DISPATCH] Idempotent report-back: returning "
                        f"in-flight run {rb_claim.incumbent} for ptc={rb_ptc} on "
                        f"flash thread {thread_id} (no second run)"
                    )
                    return JSONResponse({
                        "status": "dispatched",
                        "thread_id": thread_id,
                        "run_id": rb_claim.incumbent,
                    })
                state = await manager.wait_for_admission(
                    thread_id, exclude_run_id=run_id
                )
                if state != "fresh":
                    await release_burst_slot(user_id)
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Workflow {thread_id} is still running; dispatched "
                            "follow-up could not be admitted."
                        ),
                    )
                await manager.pre_register(thread_id, run_id)
                rb_claim.consummate()
            _track_task(asyncio.create_task(
                observe_background_chat_turn(
                    _consume_background_gen(
                        flash_gen,
                        "FLASH_DISPATCH",
                        thread_id,
                        run_id,
                        report_back_ptc_thread_id=request.report_back_ptc_thread_id,
                        user_id=user_id,
                    ),
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
        # active (running or stopping) on this thread, return 409 here rather
        # than dispatching a doomed background task.
        #
        # The admission_lock is held across wait_for_admission +
        # mark_active + pre_register so two concurrent dispatched POSTs on
        # the same thread can't both pass the gate and start workflows on
        # the same LangGraph thread_id (the foreground branch acquires
        # this same lock inside its handler via wait_or_steer; the
        # dispatched branch must do it here because it skips
        # wait_or_steer entirely). Released before _track_task schedules
        # the background workflow.
        admission_lock = await manager.get_admission_lock(thread_id)
        async with admission_lock:
            state = await manager.wait_for_admission(
                thread_id, exclude_run_id=run_id
            )
            if state != "fresh":
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
                _consume_background_gen(
                    ptc_gen, "PTC_DISPATCH", thread_id, run_id, user_id=user_id
                ),
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

    Opens a lightweight SSE connection that emits a ``workflow_started`` event
    each time a new workflow begins on this thread (e.g. a flash report-back
    after a PTC completes). The connection stays open across the whole chain so
    N concurrent PTCs' report-backs are all delivered on one subscription; the
    client reconnects via ``/messages/stream`` per event and closes the watch
    when ``/status`` reports no more pending report-backs.

    Sends keepalive pings every 45 seconds.  Auto-closes after 30 minutes
    to prevent leaked connections from abandoned browser tabs.
    """
    await require_thread_owner(thread_id, x_user_id)

    from src.utils.cache.redis_cache import get_cache_client
    from src.server.handlers.chat.report_back import watch_wakes

    async def watch_generator():
        cache = get_cache_client()
        async for frame in watch_wakes(cache, thread_id):
            yield frame

    return StreamingResponse(
        watch_generator(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/{thread_id}/messages/replay")
async def replay_thread_messages(
    thread_id: str,
    x_user_id: CurrentUserId,
    source: str = Query(
        "auto",
        pattern="^(auto|checkpoint|sse)$",
        description=(
            "Replay source: 'checkpoint' projects the transcript from LangGraph "
            "checkpoints, 'sse' replays persisted sse_events, 'auto' prefers "
            "checkpoint and falls back to sse when coverage is incomplete."
        ),
    ),
    limit: int | None = Query(
        None,
        ge=1,
        description=(
            "Windowed replay: build only the most recent N turns from "
            "checkpoints (bounds initial-load latency to the window). "
            "Checkpoint-sourced only; ignored for source='sse'."
        ),
    ),
):
    """Replay a thread as SSE.

    Stream includes:
    - user_message: emitted once per turn_index (query content)
    - message_chunk/tool_* events: projected from checkpoints or emitted from
      stored sse_events, per ``source``
    - replay_done: terminal sentinel
    """
    try:
        owner_id, thread, queries, responses, usages, provenance = (
            await get_replay_thread_data(thread_id)
        )

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

        from src.server.services.history.replay import (
            CheckpointReplayUnavailable,
            build_checkpoint_replay_items,
            build_sse_replay_items,
        )

        checkpoint_items: list[dict] | None = None
        if source in ("auto", "checkpoint"):
            try:
                if thread.get("latest_checkpoint_id") is None:
                    # The commit pointer (stamped at turn persist) is the only
                    # tip checkpoint replay may read — without it the reader
                    # would walk the newest checkpoint, which mid-run is
                    # uncommitted partial state.
                    raise CheckpointReplayUnavailable(
                        "thread has no committed checkpoint pointer"
                    )
                checkpoint_items = await build_checkpoint_replay_items(
                    thread_id,
                    queries,
                    responses_by_turn,
                    branch_tip_checkpoint_id=thread.get("latest_checkpoint_id"),
                    last_n_turns=limit,
                    usages=usages,
                    provenance=provenance,
                )
            except CheckpointReplayUnavailable as e:
                if source == "checkpoint":
                    raise HTTPException(
                        status_code=409,
                        detail=f"Checkpoint replay unavailable: {e}",
                    )
                logger.info(
                    f"[REPLAY] Checkpoint replay unavailable for {thread_id}, "
                    f"falling back to sse: {e}"
                )
            except HTTPException:
                raise
            except Exception as e:
                if source == "checkpoint":
                    raise
                logger.warning(
                    f"[REPLAY] Checkpoint replay failed for {thread_id}, "
                    f"falling back to sse: {e}",
                    exc_info=True,
                )

        replay_items = (
            checkpoint_items
            if checkpoint_items is not None
            else build_sse_replay_items(thread_id, queries, responses_by_turn)
        )

        async def event_generator():
            seq = 0
            for item in replay_items:
                seq += 1
                yield (
                    f"id: {seq}\n"
                    f"event: {item['event']}\n"
                    f"data: {json.dumps(item['data'], ensure_ascii=False, default=str)}\n\n"
                )
            seq += 1
            yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

        resolved_source = "checkpoint" if checkpoint_items is not None else "sse"
        return StreamingResponse(
            observe_replay_stream(event_generator(), source="private"),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Replay-Source": resolved_source,
            },
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
async def get_thread_status(
    thread_id: str,
    x_user_id: CurrentUserId,
    fields: Optional[str] = Query(
        None,
        description="'report_back' returns only the report-back slice (cheap path)",
    ),
):
    """Get current workflow execution status for a thread.

    ``fields=report_back`` returns just the pending-report-back slice, skipping
    the checkpoint / background-task / share reads — used by the frontend's
    event-driven catch-up pulls so a reconnect doesn't pay for the full status.
    """
    # One query authorizes the caller AND yields is_shared, so the full-status
    # path below doesn't re-fetch the thread row.
    from src.server.database.conversation import get_thread_auth_meta

    meta = await get_thread_auth_meta(thread_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    if meta["user_id"] != x_user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if fields == "report_back":
        from src.server.handlers.chat.report_back import read_report_back_status

        return await read_report_back_status(thread_id)

    from src.server.handlers.workflow_handler import get_workflow_status

    return await get_workflow_status(thread_id, is_shared=bool(meta["is_shared"]))


# Upper bound on ids per liveness request — one MGET stays cheap. Ids past the
# cap are dropped for that request and stay unresolved on the client (there is
# no per-card fallback); >100 concurrently-unresolved cards would need the
# frontend to chunk requests.
_MAX_LIVENESS_IDS = 100


@router.get("/dispatches/liveness")
async def get_dispatches_liveness(
    x_user_id: CurrentUserId,
    ids: str = Query(
        ...,
        description="Comma-separated thread ids to read liveness for (one MGET).",
    ),
):
    """Batched, client-keyed dispatch liveness — N cards in one round-trip.

    Reads the cheap ``workflow:status`` blobs via a single MGET (no checkpoint
    deserialize) and returns only threads owned by the caller — ownership comes
    from each blob's ``user_id``, so there's no per-thread DB read and no IDOR.
    Unknown or unowned ids are silently omitted.
    """
    seen: set[str] = set()
    deduped: list[str] = []
    for raw in ids.split(","):
        tid = raw.strip()
        if tid and tid not in seen:
            seen.add(tid)
            deduped.append(tid)

    if len(deduped) > _MAX_LIVENESS_IDS:
        logger.warning(
            f"[LIVENESS] {len(deduped)} ids requested by {x_user_id}; capping at "
            f"{_MAX_LIVENESS_IDS} (remainder unresolved this request)"
        )
        deduped = deduped[:_MAX_LIVENESS_IDS]

    if not deduped:
        return {"liveness": []}

    from src.server.services.workflow_tracker import WorkflowStatus, WorkflowTracker
    from src.server.handlers.workflow_handler import (
        crosscheck_btm_liveness,
        liveness_from_blob,
    )

    tracker = WorkflowTracker.get_instance()
    blobs = await tracker.get_statuses(deduped)

    liveness = []
    for tid, blob in blobs.items():
        if blob.get("user_id") != x_user_id:
            continue
        slice_ = liveness_from_blob(tid, blob)
        # A no-TTL ACTIVE blob can survive a process restart that killed its run,
        # leaving the card a zombie ({running, can_reconnect}) forever. Cross-check
        # the in-process BTM (authoritative under the single-worker invariant); a
        # stale ACTIVE with no live task heals to a terminal slice. INTERRUPTED is
        # resumable-by-design with no live task, so it is left untouched.
        if slice_["status"] == WorkflowStatus.ACTIVE:
            result = await crosscheck_btm_liveness(tid, tracker, True)
            if not result["live"]:
                slice_ = {
                    "thread_id": tid,
                    "status": WorkflowStatus.COMPLETED,
                    "run_id": None,
                    "can_reconnect": False,
                }
        liveness.append(slice_)

    # A terminal run's status blob has a ~1h TTL; once it expires the blob pass
    # omits the thread and the client re-freezes the card on 'starting'. Fall
    # back to the durable current_status so terminal cards stay resolved. A
    # still-in_progress row is left absent on purpose — its blob just hasn't been
    # written yet, so the card should keep polling as 'starting'.
    resolved = {slice_["thread_id"] for slice_ in liveness}
    absent = [tid for tid in deduped if tid not in resolved]
    if absent:
        from src.server.database.conversation import get_threads_terminal_status

        # DB current_status -> WorkflowStatus enum value so the frontend
        # mapStatus resolves it (a raw 'error' would hit its default -> 'starting'
        # and re-freeze). 'in_progress' / anything else is intentionally omitted.
        terminal_by_db_status = {
            "completed": WorkflowStatus.COMPLETED,
            "error": WorkflowStatus.FAILED,
            "cancelled": WorkflowStatus.CANCELLED,
            "interrupted": WorkflowStatus.INTERRUPTED,
        }
        statuses = await get_threads_terminal_status(absent, x_user_id)
        for tid, current_status in statuses.items():
            status = terminal_by_db_status.get(current_status)
            if status is None:
                continue
            liveness.append(
                {
                    "thread_id": tid,
                    "status": status,
                    "run_id": None,
                    "can_reconnect": False,
                }
            )

    return {"liveness": liveness}


@router.post("/{thread_id}/cancel", status_code=200)
async def cancel_thread(
    thread_id: str,
    x_user_id: CurrentUserId,
    run_id: Optional[str] = Query(None),
):
    """Cancel a running workflow for this thread.

    ``run_id`` targets a specific run so a retried stop can't cancel a newer
    turn started after the stopped one ended (defaults to latest active run).
    """
    await require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.workflow_handler import cancel_workflow

    return await cancel_workflow(thread_id, run_id)


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


# =============================================================================
# DATA PROVENANCE
# =============================================================================


@router.get("/{thread_id}/provenance")
async def get_provenance(thread_id: str, x_user_id: CurrentUserId):
    """Return the external data the agent accessed in a thread, grouped by turn.

    The aggregated shape (per-turn sources + a by_source_type count summary) is
    the structured input a post-hoc verification agent consumes.
    """
    try:
        await require_thread_owner(thread_id, x_user_id)
        rows = await get_provenance_for_thread(thread_id)

        turns: dict[int, dict] = {}
        by_source_type: dict[str, int] = {}
        for row in rows:
            turn_index = row["turn_index"]
            turn = turns.get(turn_index)
            if turn is None:
                turn = {
                    "turn_index": turn_index,
                    "conversation_response_id": str(row["conversation_response_id"]),
                    "sources": [],
                }
                turns[turn_index] = turn

            source_timestamp = row.get("source_timestamp")
            source = {
                # `record_id` matches the SSE/replay provenance record field so a
                # consumer can map streamed records to this REST shape directly.
                "record_id": str(row["provenance_record_id"]),
                "source_type": row["source_type"],
                "identifier": row.get("identifier"),
                "title": row.get("title"),
                "detail": row.get("detail"),
                "tool_call_id": row.get("tool_call_id"),
                "args_fingerprint": row.get("args_fingerprint"),
                "args": row.get("args"),
                "result_sha256": row.get("result_sha256"),
                "result_size": row.get("result_size"),
                "result_snippet": row.get("result_snippet"),
                "agent": row.get("agent"),
                "provider": row.get("provider"),
                "timestamp": (
                    source_timestamp.isoformat() if source_timestamp else None
                ),
            }
            turn["sources"].append(source)

            source_type = row["source_type"]
            by_source_type[source_type] = by_source_type.get(source_type, 0) + 1

        return {
            "thread_id": thread_id,
            "turns": [turns[i] for i in sorted(turns)],
            "by_source_type": by_source_type,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting provenance for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get provenance")


def _body_hashes_to(body: str, sha256: str | None) -> bool:
    """True iff ``body`` reproduces the content-address ``sha256``.

    The verifier's integrity check: a present body that hashes to its advertised
    sha is the exact content the agent reasoned over. A mismatch means the body was
    redacted (a secret was stripped) or is otherwise not the hashed bytes — the
    caller distinguishes the two via the ``truncated`` flag.
    """
    if not body or not sha256:
        return False
    return hashlib.sha256(body.encode("utf-8")).hexdigest() == sha256


@router.get("/{thread_id}/provenance/bodies")
async def get_provenance_bodies(
    thread_id: str,
    x_user_id: CurrentUserId,
    limit: int = Query(
        100,
        ge=1,
        le=200,
        description="Max bodies returned; a long thread is capped (see `capped`).",
    ),
):
    """Return stored result bodies (inline head only) for a thread's provenance records.

    Sibling to ``/provenance`` (which stays snippet-only): joins each record's
    ``result_sha256`` to the content-addressed body store and returns the inline
    head plus ``truncated`` and ``verified`` flags. Spilled objects are never
    fetched here — use the per-record ``/body?full=true`` endpoint for the full body.

    The response is bounded: each inline head is up to 64 KiB, so a long thread is
    capped at ``limit`` bodies (``capped: true`` when more were available) to keep
    one request from materializing tens of MB.
    """
    try:
        await require_thread_owner(thread_id, x_user_id)

        from src.server.database.conversation import get_db_connection
        from src.server.database.provenance_bodies import fetch_result_bodies

        # Eligible refs are filtered + capped in SQL (LIMIT limit+1) and the body
        # fetch shares the same connection, so a long thread doesn't transfer every
        # record (and its args JSON) just to discard all but `limit`.
        async with get_db_connection() as conn:
            eligible = await get_provenance_body_refs(conn, thread_id, limit)
            capped = len(eligible) > limit
            eligible = eligible[:limit]
            shas = [row["result_sha256"] for row in eligible]
            bodies = await fetch_result_bodies(conn, shas)

        records = []
        for row in eligible:
            sha = row["result_sha256"]
            body = bodies.get(sha)
            if body is None:
                continue
            body_inline = body["body_inline"] or ""
            byte_len = body["byte_len"]
            # byte_len is the length of the STORED (post-redaction) body, so the
            # inline head is incomplete exactly when the full stored body is longer
            # than what's inline — i.e. it spilled to an object, or a head was kept
            # with no bucket to spill to. A body redaction shrank below the cap is
            # stored whole (byte_len == len(inline)) and reads back complete.
            truncated = byte_len > len(body_inline.encode("utf-8"))
            records.append(
                {
                    "provenance_record_id": str(row["provenance_record_id"]),
                    "result_sha256": sha,
                    "body_inline": body_inline,
                    "byte_len": byte_len,
                    "truncated": truncated,
                    # The stored body hashes to result_sha256 (untruncated + not
                    # redacted). False on a truncated head or a redaction-modified
                    # body — the signal that "these bytes != the advertised hash."
                    "verified": (not truncated) and _body_hashes_to(body_inline, sha),
                }
            )

        return {"thread_id": thread_id, "records": records, "capped": capped}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            f"Error getting provenance bodies for thread {thread_id}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get provenance bodies")


@router.get("/{thread_id}/provenance/{provenance_record_id}/body")
async def get_provenance_record_body(
    thread_id: str,
    provenance_record_id: str,
    x_user_id: CurrentUserId,
    full: bool = Query(
        False,
        description="When true, read the full body (pulls the spilled object if any).",
    ),
):
    """Return the body for a single provenance record.

    With ``full=true`` the full body is read via ``fetch_full_body`` (pulling the
    spilled object when present, capped at ``FULL_BODY_READ_MAX_BYTES`` so one
    request can't serialize a ~10 MiB object — an over-cap body returns truncated);
    otherwise only the inline head is returned. The record must belong to the
    caller's thread.
    """
    try:
        await require_thread_owner(thread_id, x_user_id)
        row = await get_provenance_record(thread_id, provenance_record_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Provenance record not found")

        sha = row.get("result_sha256")
        if not sha:
            raise HTTPException(
                status_code=404, detail="Provenance record has no stored body"
            )

        from src.server.database.conversation import get_db_connection
        from src.server.database.provenance_bodies import (
            fetch_full_body,
            fetch_result_bodies,
        )

        async with get_db_connection() as conn:
            bodies = await fetch_result_bodies(conn, [sha])
        meta = bodies.get(sha)
        if meta is None:
            raise HTTPException(
                status_code=404, detail="Provenance record has no stored body"
            )

        byte_len = meta["byte_len"]
        if full:
            # meta already carries body_inline + object_key from the fetch above,
            # so pass it through — fetch_full_body skips a second connection and
            # only does the spilled-object read when there's an object_key.
            body = await fetch_full_body(sha, row=meta) or ""
            # byte_len is the full stored-body length; the read is incomplete
            # exactly when we returned fewer bytes than that — the spilled object
            # exceeded the read cap, or a head was kept with no bucket to spill to.
            truncated = byte_len > len(body.encode("utf-8"))
        else:
            body = meta["body_inline"] or ""
            # The inline head is incomplete exactly when the full stored body is
            # longer than the inline slice (spilled, or head kept with no bucket).
            # byte_len tracks the stored (post-redaction) length, so a redaction-
            # shrunk body that fits inline reads back complete.
            truncated = byte_len > len(body.encode("utf-8"))

        return {
            "provenance_record_id": str(row["provenance_record_id"]),
            "result_sha256": sha,
            "body": body,
            "byte_len": byte_len,
            "truncated": truncated,
            # With full=true and no truncation, a true value attests the body is the
            # exact bytes behind result_sha256; false means redacted or head-only.
            "verified": (not truncated) and _body_hashes_to(body, sha),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            f"Error getting provenance body for record {provenance_record_id}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get provenance body")
