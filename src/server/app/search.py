"""Search for the command palette: threads by title, turns by what was said
in them, files by path, and a turn's text for the preview pane.

A person's surface: a computer's token is refused whatever its scopes, since
its agent reads its own computer's threads through the file mount.
"""

import logging
from typing import Annotated, Optional, get_args
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.server.dependencies.principal import ReadPrincipal, refuse_computer
from src.server.models.search import SearchResponse, TurnPreview
from src.server.services.search import query as search_service
from src.server.services.search.text import Kind
from src.server.utils.api import handle_api_exceptions
from src.utils.cache.throttle import throttle

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/search", tags=["Search"])

MAX_QUERY_CHARS = 200
_KINDS: tuple[Kind, ...] = get_args(Kind)
_WINDOW_S = 10


def _searcher(bucket: str, per_window: int):
    """The user id of a person searching, ``per_window`` calls to a bucket
    each ``_WINDOW_S`` seconds, every worker counting together."""

    async def dependency(principal: ReadPrincipal) -> str:
        refuse_computer(principal, "Search is for people; a computer's token cannot search")
        if not await throttle(f"{bucket}:{principal.user_id}", per_window, _WINDOW_S):
            raise HTTPException(
                status_code=429,
                detail="Too many searches. Please wait a few seconds.",
                headers={"Retry-After": str(_WINDOW_S)},
            )
        return principal.user_id

    return dependency


# A palette searches as the user types, debounced. Previews count apart,
# since moving through the results asks for one per row.
SearchingUserId = Annotated[str, Depends(_searcher("search", 60))]
PreviewingUserId = Annotated[str, Depends(_searcher("search_preview", 60))]


def _kinds(raw: Optional[str]) -> list[Kind]:
    if raw is None:
        return list(_KINDS)
    named = {k.strip() for k in raw.split(",")} - {""}
    if not named or not named <= set(_KINDS):
        raise HTTPException(
            status_code=422,
            detail=f"kinds takes a comma-separated list of {', '.join(_KINDS)}",
        )
    return [k for k in _KINDS if k in named]


@router.get("", response_model=SearchResponse)
@handle_api_exceptions("search", logger)
async def search(
    user_id: SearchingUserId,
    q: str = Query(..., max_length=MAX_QUERY_CHARS * 4),
    limit: int = Query(8, ge=1, le=50),
    kinds: Optional[str] = Query(
        None, description="any of threads, turns, files, comma-separated (default all)"
    ),
    cursor: Optional[str] = Query(None, max_length=1024),
    workspace_id: Optional[UUID] = Query(None, description="search inside this workspace"),
    thread_id: Optional[UUID] = Query(
        None, description="search inside this thread (turns only)"
    ),
):
    q = q.strip()
    if not 1 <= len(q) <= MAX_QUERY_CHARS:
        raise HTTPException(
            status_code=422, detail=f"q must be 1 to {MAX_QUERY_CHARS} characters"
        )
    try:
        return await search_service.search(
            user_id,
            q,
            kinds=_kinds(kinds),
            limit=limit,
            cursor=cursor,
            workspace_id=str(workspace_id) if workspace_id else None,
            thread_id=str(thread_id) if thread_id else None,
        )
    except search_service.BadSearch as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    except search_service.SearchTimedOut:
        raise HTTPException(
            status_code=503, detail="Search took too long; try more specific words"
        ) from None


@router.get("/preview", response_model=TurnPreview)
@handle_api_exceptions("search preview", logger)
async def search_preview(
    user_id: PreviewingUserId,
    thread_id: UUID = Query(...),
    turn_index: Optional[int] = Query(None, ge=0, le=2**31 - 1),
):
    found = await search_service.preview(user_id, str(thread_id), turn_index)
    if found is None:
        raise HTTPException(status_code=404, detail="Turn not found")
    return found
