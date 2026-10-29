"""Thread CRUD: list, get, update, delete, external-id stamp, market watch."""

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID


from fastapi import HTTPException, Query
from pydantic import BaseModel, Field

from src.server.database.market_targets import cancel_targets, list_thread_targets
from src.server.services.features import user_feature_enabled
from src.server.models.automation import MarketType
from src.server.services.market_targets.changes import targets_changed
from src.server.services.market_targets.listing import watch_spelling
from src.server.services.wakeups import cancel as wakeups_cancel
from src.server.services.commands.control import (
    delete_thread_ending_commands,
    request_running_stops,
)
from src.server.services.llm.thread_model import require_selectable_model
from src.server.services.thread_lifecycle import project_lifecycle
from src.server.services.thread_lifecycle_feed import (
    publish_thread_archived,
    publish_thread_deleted,
    publish_thread_pinned,
    publish_thread_subagents,
    publish_thread_tuning,
    publish_thread_title,
    publish_thread_unarchived,
)
from src.server.services.workspace_manager import prune_thread_dirs_soon
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
    stamp_thread_seen,
    update_thread_fields,
    update_thread_external_id,
    get_thread_auth_meta,
    get_thread_by_id,
)

from ._deps import logger, router


def _lifecycle_fields(row: dict) -> dict:
    """Derive the lifecycle enrichment from a list row carrying the
    latest-attempt LATERAL columns; {} when the row wasn't enriched (detail,
    rename, external-id sites — the model fields are optional)."""
    if "latest_run_seq" not in row:
        return {}
    return dict(project_lifecycle(row))


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
    archived: bool = Query(
        False,
        description="List archived threads instead of active ones. Requires "
        "workspace_id — the archived view is per-workspace only.",
    ),
):
    """
    List threads with optional workspace + platform-prefix filter.

    When workspace_id is provided, returns threads for that workspace.
    Otherwise returns all threads for the authenticated user.
    """
    try:
        if archived and not workspace_id:
            raise HTTPException(
                status_code=400,
                detail="archived=true requires workspace_id",
            )
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
                archived=archived,
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
                metadata=thread.get("metadata") or {},
                is_shared=bool(thread.get("is_shared", False)),
                is_pinned=bool(thread.get("is_pinned", False)),
                archived_at=thread.get("archived_at"),
                llm_model=thread.get("llm_model"),
                subagents_allowed=thread["subagents_allowed"],
                reasoning_effort=thread.get("reasoning_effort"),
                fast_mode=thread.get("fast_mode"),
                turn_count=thread.get("turn_count"),
                created_at=thread["created_at"],
                updated_at=thread["updated_at"],
                **_lifecycle_fields(thread),
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
    return _thread_list_item(thread)


class ThreadSeenRequest(BaseModel):
    run_id: str = Field(
        ...,
        description="The run id the client actually observed (from the list "
        "row's latest_run_id or a feed event) — the causal token.",
    )


class ThreadSeenResponse(BaseModel):
    last_seen_run_seq: int
    latest_run_seq: int


@router.post("/{thread_id}/seen", response_model=ThreadSeenResponse)
async def mark_thread_seen(
    thread_id: str, request: ThreadSeenRequest, x_user_id: CurrentUserId
):
    """Causal seen stamp: advances the cursor to the OBSERVED run only.

    The server requires the run to belong to this thread and be terminal,
    then stamps ``GREATEST(last_seen_run_seq, run.run_seq)`` — a delayed
    POST can never sweep a settlement the user hasn't seen. Returns the
    authoritative cursors so the client can reconcile its store and cached
    list rows.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    result = await stamp_thread_seen(thread_id, request.run_id)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "seen_not_applicable",
                "message": "The observed run is missing, belongs to another "
                "thread, or is still live.",
            },
        )
    return ThreadSeenResponse(**result)


class MarketWatchTarget(BaseModel):
    target_id: str
    symbol: str
    kind: Literal["above", "below", "pct_up", "pct_down"]
    value: float
    # The agent's vocabulary, not storage's: "now" is stored as "armed".
    pct_from: Optional[Literal["previous_close", "now"]] = None
    armed_price: Optional[float] = None
    status: Literal["armed", "fired", "expired", "cancelled"]
    expires_at: datetime
    fired_at: Optional[datetime] = None
    fired_price: Optional[float] = None
    created_at: datetime


class MarketWatchResponse(BaseModel):
    thread_id: str
    symbols: list[str]
    targets: list[MarketWatchTarget] = Field(default_factory=list)


class MarketWatchTargetCancelResponse(BaseModel):
    cancelled: bool


def _target_out(row: dict, now: datetime) -> MarketWatchTarget:
    pct_from = row.get("pct_from")
    status = row["status"]
    if status == "armed" and row["expires_at"] <= now:
        # Lapsed but not yet swept: the price monitor no longer checks it, so
        # showing it armed would offer a cancel for a target already over.
        status = "expired"
    return MarketWatchTarget(
        target_id=str(row["target_id"]),
        # The watch list's spelling, so a target sits beside its symbol:
        # an index stored as SPX is watched as ^GSPC.
        symbol=watch_spelling(row["symbol"], MarketType(row.get("market") or "stock")),
        kind=row["kind"],
        value=row["value"],
        pct_from="now" if pct_from == "armed" else pct_from,
        armed_price=row.get("armed_price"),
        status=status,
        expires_at=row["expires_at"],
        fired_at=row.get("fired_at"),
        fired_price=row.get("fired_price"),
        created_at=row["created_at"],
    )


@router.get("/{thread_id}/market-watch", response_model=MarketWatchResponse)
async def get_market_watch(thread_id: str, x_user_id: CurrentUserId):
    """Current market-watch symbols and price targets for a thread."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    if not await user_feature_enabled(x_user_id, "market_watch"):
        return MarketWatchResponse(thread_id=thread_id, symbols=[])
    symbols = await get_watchlist(thread_id)
    try:
        now = datetime.now(timezone.utc)
        targets = [_target_out(r, now) for r in await list_thread_targets(thread_id)]
    except Exception:
        # The chip and the panel poll this; losing the targets for one read
        # must not take the watch list down with them.
        logger.warning(
            "[MarketWatch] listing targets failed thread_id=%s", thread_id, exc_info=True
        )
        targets = []
    return MarketWatchResponse(thread_id=thread_id, symbols=symbols, targets=targets)


@router.delete(
    "/{thread_id}/market-watch/targets/{target_id}",
    response_model=MarketWatchTargetCancelResponse,
)
async def cancel_market_watch_target(
    thread_id: str, target_id: UUID, x_user_id: CurrentUserId
):
    """Cancel one armed target; ``cancelled`` is False when it was not armed.

    Not gated on the feature: turning market watch off must never leave an
    owner unable to stop a target armed before.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    rows = await cancel_targets(thread_id, target_id=str(target_id))
    if rows:
        await targets_changed(thread_id)
    return MarketWatchTargetCancelResponse(cancelled=bool(rows))


@router.delete("/{thread_id}", response_model=ThreadDeleteResponse)
async def delete_thread_endpoint(thread_id: str, x_user_id: CurrentUserId):
    """
    Delete a thread and all its queries/responses.

    Permanently deletes the thread and all associated data due to CASCADE constraints.
    """
    from src.server.database.order_attempts import refuse_attempts_on_thread_delete
    from src.server.services.thread_mutation import (
        MutationConflict,
        MutationUnavailable,
    )

    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        # Read the row pre-delete: the thread_deleted feed event needs the
        # workspace_id and the row is gone afterwards.
        try:
            thread_row = await get_thread_by_id(thread_id)
        except Exception:
            thread_row = None
        # Guarded delete (v4 2.4): exclusive T(thread) refuses while a fenced
        # run or tail writer is live on ANY worker; the ledger gate refuses on
        # an in_progress row (cancel the run first). The delete statement runs
        # on the locked session so fence and effect die together, and the
        # thread's commands are ended once it has committed.
        try:
            await delete_thread_ending_commands(
                thread_id,
                # Before the delete and on its session: nothing is left that
                # could answer or spend an order still waiting in this thread,
                # and a refusal outliving a failed delete fails closed.
                before_delete=lambda conn: refuse_attempts_on_thread_delete(
                    thread_id, conn=conn
                ),
            )
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

        if thread_row:
            prune_thread_dirs_soon(str(thread_row["workspace_id"]))

        await publish_thread_deleted(
            user_id=x_user_id,
            thread_id=thread_id,
            workspace_id=(
                str(thread_row["workspace_id"]) if thread_row else None
            ),
        )

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
    """Build the list-item response from a detail/updated thread row."""
    return WorkspaceThreadListItem(
        thread_id=str(row["conversation_thread_id"]),
        workspace_id=str(row["workspace_id"]),
        thread_index=row["thread_index"],
        current_status=row["current_status"],
        msg_type=row.get("msg_type"),
        title=row.get("title"),
        platform=row.get("platform"),
        metadata=row.get("metadata") or {},
        is_shared=bool(row.get("is_shared", False)),
        is_pinned=bool(row.get("is_pinned", False)),
        archived_at=row.get("archived_at"),
        llm_model=row.get("llm_model"),
        subagents_allowed=row["subagents_allowed"],
        reasoning_effort=row.get("reasoning_effort"),
        fast_mode=row.get("fast_mode"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.patch("/{thread_id}", response_model=WorkspaceThreadListItem)
async def update_thread_endpoint(
    thread_id: str, request: ThreadUpdateRequest, x_user_id: CurrentUserId
):
    """Update user-editable thread fields: title, pin, archive, model,
    subagent switch, effort, fast mode.

    Applies only the fields explicitly present in the request body
    (``model_fields_set``) — a pin toggle can't clear the title.
    """
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        provided = request.model_fields_set
        updates: dict = {}
        if "title" in provided:
            # An explicit null clears the title (list rows fall back to the
            # first-query preview) — only absence from the body skips it.
            updates["title"] = request.title
        if "is_pinned" in provided and request.is_pinned is not None:
            updates["is_pinned"] = request.is_pinned
        if "archived" in provided and request.archived is not None:
            updates["archived"] = request.archived
        switched_from: str | None = None
        if "llm_model" in provided:
            # null returns the thread to the account default; a name has to
            # resolve to a model, in the catalog or the user's own, or every
            # later turn would fail on it. Whether a connection can serve it
            # is the turn's question, since a lapsed one comes back.
            if request.llm_model is not None:
                await require_selectable_model(x_user_id, request.llm_model)
            updates["llm_model"] = request.llm_model
            switched_from = (await get_thread_auth_meta(thread_id) or {}).get("llm_model")
        if "subagents_allowed" in provided:
            # null, or the side the owner's default is on, returns the thread
            # to following the default (decided in the UPDATE). Read by the
            # running turn on its next model call and at every subagent
            # launch, so a flip mid-turn takes effect without one.
            updates["subagents_allowed"] = request.subagents_allowed
        # Stored as sent, null returning the field to the user's Settings.
        # Read when the next turn resolves its model, so a running turn keeps
        # the level it started on.
        if "reasoning_effort" in provided:
            updates["reasoning_effort"] = request.reasoning_effort
        if "fast_mode" in provided:
            updates["fast_mode"] = request.fast_mode
        if not updates:
            raise HTTPException(
                status_code=400, detail="No updatable fields provided"
            )
        updated_thread = await update_thread_fields(thread_id, **updates)
        if not updated_thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )
        if "title" in updates:
            await publish_thread_title(
                user_id=x_user_id,
                thread_id=thread_id,
                workspace_id=str(updated_thread["workspace_id"]),
                title=updated_thread.get("title") or "",
                updated_at=updated_thread.get("updated_at"),
            )
        if "is_pinned" in updates:
            await publish_thread_pinned(
                user_id=x_user_id,
                thread_id=thread_id,
                workspace_id=str(updated_thread["workspace_id"]),
                pinned=bool(updated_thread.get("is_pinned", False)),
            )
        if "subagents_allowed" in updates:
            await publish_thread_subagents(
                user_id=x_user_id,
                thread_id=thread_id,
                workspace_id=str(updated_thread["workspace_id"]),
                allowed=updated_thread["subagents_allowed"],
            )
        if "reasoning_effort" in updates or "fast_mode" in updates:
            await publish_thread_tuning(
                user_id=x_user_id,
                thread_id=thread_id,
                workspace_id=str(updated_thread["workspace_id"]),
                reasoning_effort=updated_thread["reasoning_effort"],
                fast_mode=updated_thread["fast_mode"],
            )
        if "archived" in updates:
            # Post-commit only: the archive statement already stamped the
            # latest terminal attempt seen, so the row's absence from the
            # next snapshot is honest rather than truncation.
            publish = (
                publish_thread_archived
                if updates["archived"]
                else publish_thread_unarchived
            )
            await publish(
                user_id=x_user_id,
                thread_id=thread_id,
                workspace_id=str(updated_thread["workspace_id"]),
            )
            if updates["archived"]:
                # Archiving ends the thread's scratchpad.
                prune_thread_dirs_soon(str(updated_thread["workspace_id"]))
                await wakeups_cancel.cancel_on_archive(thread_id)
                # No turn starts here again, so a command's report would
                # only be dropped; its computer need not stay up for it.
                await request_running_stops(thread_id=thread_id, by="archive")
        item = _thread_list_item(updated_thread)
        if "llm_model" in updates and updates["llm_model"] != switched_from:
            from src.server.handlers.thread_maintenance import hand_off_on_switch

            item.summary_handoff = await hand_off_on_switch(
                thread_id, x_user_id, switched_from
            )
        return item

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
