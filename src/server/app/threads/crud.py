"""Thread CRUD: list, get, update, delete, external-id stamp, market watch."""

from typing import Optional


from fastapi import HTTPException, Query
from pydantic import BaseModel

from src.server.services.features import user_feature_enabled
# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
    StampThreadAuth,
    require_workspace_owner,
)
from src.utils.market_watch import get_watchlist
from src.server.models.conversation import (
    WorkspaceThreadListItem,
    WorkspaceThreadsListResponse,
    ThreadUpdateRequest,
    ThreadExternalIdRequest,
    ThreadDeleteResponse,
)
from src.server.database.conversation import (
    ExternalIdConflictError,
    external_id_conflict_payload,
    get_workspace_threads,
    get_threads_for_user,
    delete_thread,
    update_thread_title,
    update_thread_external_id,
    get_thread_by_id,
)



from ._deps import logger, router


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
    await auth_api.require_thread_owner(thread_id, x_user_id)
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


class MarketWatchResponse(BaseModel):
    thread_id: str
    symbols: list[str]


@router.get("/{thread_id}/market-watch", response_model=MarketWatchResponse)
async def get_market_watch(thread_id: str, x_user_id: CurrentUserId):
    """Current market-watch symbols for a thread (empty list when none)."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    if not await user_feature_enabled(x_user_id, "market_watch"):
        return MarketWatchResponse(thread_id=thread_id, symbols=[])
    symbols = await get_watchlist(thread_id)
    return MarketWatchResponse(thread_id=thread_id, symbols=symbols)


@router.delete("/{thread_id}", response_model=ThreadDeleteResponse)
async def delete_thread_endpoint(thread_id: str, x_user_id: CurrentUserId):
    """
    Delete a thread and all its queries/responses.

    Permanently deletes the thread and all associated data due to CASCADE constraints.
    """
    from src.server.services.thread_mutation import (
        MutationConflict,
        MutationUnavailable,
        ThreadMutationRunner,
    )

    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        # Guarded delete (v4 2.4): exclusive T(thread) refuses while a fenced
        # run or tail writer is live on ANY worker; the ledger gate refuses on
        # an in_progress row (cancel the run first). The delete statement runs
        # on the locked session so fence and effect die together.
        try:
            async with ThreadMutationRunner.get_instance().exclusive(
                thread_id, "delete"
            ) as mutation:
                await delete_thread(thread_id, conn=mutation.conn)
        except MutationConflict as e:
            raise HTTPException(status_code=409, detail=e.detail)
        except MutationUnavailable as e:
            raise HTTPException(status_code=503, detail=str(e))

        # Invalidate existence cache + the thread's market-watch list, so a
        # recreated thread id can't inherit the old symbols within the TTL.
        from src.server.database.conversation import thread_exists_key
        from src.utils.cache.redis_cache import get_cache_client
        from src.utils.market_watch import watch_key

        cache = get_cache_client()
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(thread_exists_key(thread_id), watch_key(thread_id))
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
        await auth_api.require_thread_owner(thread_id, x_user_id)
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
            await auth_api.require_thread_owner(thread_id, user_id)
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
