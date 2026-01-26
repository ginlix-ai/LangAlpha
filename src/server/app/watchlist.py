"""
Watchlist API Router.

Provides REST endpoints for watchlist and watchlist item management.

Watchlist Management Endpoints (/api/v1/users/me/watchlists):
- GET /watchlists - List all watchlists
- POST /watchlists - Create new watchlist
- GET /watchlists/{watchlist_id} - Get watchlist with items
- PUT /watchlists/{watchlist_id} - Update watchlist metadata
- DELETE /watchlists/{watchlist_id} - Delete watchlist (cascades items)

Watchlist Items Endpoints (/api/v1/users/me/watchlists/{watchlist_id}/items):
- GET /watchlists/{watchlist_id}/items - List items in watchlist
- POST /watchlists/{watchlist_id}/items - Add item to watchlist
- GET /watchlists/{watchlist_id}/items/{item_id} - Get single item
- PUT /watchlists/{watchlist_id}/items/{item_id} - Update item
- DELETE /watchlists/{watchlist_id}/items/{item_id} - Remove item

Note: Use "default" as watchlist_id to operate on the user's default watchlist.
"""

import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from src.server.database.watchlist import (
    create_watchlist as db_create_watchlist,
    create_watchlist_item as db_create_watchlist_item,
    delete_watchlist as db_delete_watchlist,
    delete_watchlist_item as db_delete_watchlist_item,
    get_or_create_default_watchlist as db_get_or_create_default_watchlist,
    get_user_watchlists as db_get_user_watchlists,
    get_watchlist as db_get_watchlist,
    get_watchlist_item as db_get_watchlist_item,
    get_watchlist_items as db_get_watchlist_items,
    update_watchlist as db_update_watchlist,
    update_watchlist_item as db_update_watchlist_item,
)
from src.server.models.user import (
    WatchlistCreate,
    WatchlistItemCreate,
    WatchlistItemResponse,
    WatchlistItemsResponse,
    WatchlistItemUpdate,
    WatchlistResponse,
    WatchlistsResponse,
    WatchlistUpdate,
    WatchlistWithItemsResponse,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions, raise_not_found

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/users/me", tags=["Watchlist"])


# =============================================================================
# Helper Functions
# =============================================================================


async def _resolve_watchlist_id(watchlist_id: str, user_id: str) -> str:
    """
    Resolve watchlist_id, handling "default" as a special case.

    Args:
        watchlist_id: Watchlist ID or "default"
        user_id: User ID

    Returns:
        Resolved watchlist UUID string
    """
    if watchlist_id.lower() == "default":
        default_watchlist = await db_get_or_create_default_watchlist(user_id)
        return str(default_watchlist["watchlist_id"])
    return watchlist_id


# =============================================================================
# Watchlist Management Endpoints
# =============================================================================


@router.get("/watchlists", response_model=WatchlistsResponse)
@handle_api_exceptions("list watchlists", logger)
async def list_watchlists(user_id: CurrentUserId):
    """
    List all watchlists for the current user.

    Args:
        user_id: User ID from authentication header

    Returns:
        List of watchlists with total count
    """
    watchlists = await db_get_user_watchlists(user_id)

    return WatchlistsResponse(
        watchlists=[WatchlistResponse.model_validate(w) for w in watchlists],
        total=len(watchlists),
    )


@router.post("/watchlists", response_model=WatchlistResponse, status_code=201)
@handle_api_exceptions("create watchlist", logger, conflict_on_value_error=True)
async def create_watchlist(
    request: WatchlistCreate,
    user_id: CurrentUserId,
):
    """
    Create a new watchlist.

    Args:
        request: Watchlist data
        user_id: User ID from authentication header

    Returns:
        Created watchlist

    Raises:
        409: Watchlist with same name already exists
    """
    watchlist = await db_create_watchlist(
        user_id=user_id,
        name=request.name,
        description=request.description,
        is_default=request.is_default,
        display_order=request.display_order,
    )

    logger.info(f"Created watchlist {watchlist['watchlist_id']} for user {user_id}")
    return WatchlistResponse.model_validate(watchlist)


@router.get("/watchlists/{watchlist_id}", response_model=WatchlistWithItemsResponse)
@handle_api_exceptions("get watchlist", logger)
async def get_watchlist(
    watchlist_id: str,
    user_id: CurrentUserId,
):
    """
    Get a watchlist with its items.

    Use "default" as watchlist_id to get the user's default watchlist.

    Args:
        watchlist_id: Watchlist ID or "default"
        user_id: User ID from authentication header

    Returns:
        Watchlist with items

    Raises:
        404: Watchlist not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    watchlist = await db_get_watchlist(resolved_id, user_id)

    if not watchlist:
        raise_not_found("Watchlist")

    items = await db_get_watchlist_items(resolved_id, user_id)

    return WatchlistWithItemsResponse(
        watchlist_id=watchlist["watchlist_id"],
        user_id=watchlist["user_id"],
        name=watchlist["name"],
        description=watchlist.get("description"),
        is_default=watchlist["is_default"],
        display_order=watchlist["display_order"],
        created_at=watchlist["created_at"],
        updated_at=watchlist["updated_at"],
        items=[WatchlistItemResponse.model_validate(item) for item in items],
        total=len(items),
    )


@router.put("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
@handle_api_exceptions("update watchlist", logger, conflict_on_value_error=True)
async def update_watchlist(
    watchlist_id: str,
    request: WatchlistUpdate,
    user_id: CurrentUserId,
):
    """
    Update watchlist metadata.

    Partial update supported - only provided fields are updated.

    Args:
        watchlist_id: Watchlist ID or "default"
        request: Fields to update
        user_id: User ID from authentication header

    Returns:
        Updated watchlist

    Raises:
        404: Watchlist not found or not owned by user
        409: Name conflicts with existing watchlist
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    watchlist = await db_update_watchlist(
        watchlist_id=resolved_id,
        user_id=user_id,
        name=request.name,
        description=request.description,
        display_order=request.display_order,
    )

    if not watchlist:
        raise_not_found("Watchlist")

    logger.info(f"Updated watchlist {resolved_id} for user {user_id}")
    return WatchlistResponse.model_validate(watchlist)


@router.delete("/watchlists/{watchlist_id}", status_code=204)
@handle_api_exceptions("delete watchlist", logger)
async def delete_watchlist(
    watchlist_id: str,
    user_id: CurrentUserId,
):
    """
    Delete a watchlist (items are cascade deleted).

    Args:
        watchlist_id: Watchlist ID or "default"
        user_id: User ID from authentication header

    Returns:
        204 No Content on success

    Raises:
        404: Watchlist not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    deleted = await db_delete_watchlist(resolved_id, user_id)

    if not deleted:
        raise_not_found("Watchlist")

    logger.info(f"Deleted watchlist {resolved_id} for user {user_id}")
    return Response(status_code=204)


# =============================================================================
# Watchlist Items Endpoints
# =============================================================================


@router.get("/watchlists/{watchlist_id}/items", response_model=WatchlistItemsResponse)
@handle_api_exceptions("list watchlist items", logger)
async def list_watchlist_items(
    watchlist_id: str,
    user_id: CurrentUserId,
):
    """
    List all items in a watchlist.

    Use "default" as watchlist_id to list items from the default watchlist.

    Args:
        watchlist_id: Watchlist ID or "default"
        user_id: User ID from authentication header

    Returns:
        List of watchlist items with total count

    Raises:
        404: Watchlist not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    watchlist = await db_get_watchlist(resolved_id, user_id)
    if not watchlist:
        raise_not_found("Watchlist")

    items = await db_get_watchlist_items(resolved_id, user_id)

    return WatchlistItemsResponse(
        items=[WatchlistItemResponse.model_validate(item) for item in items],
        total=len(items),
    )


@router.post(
    "/watchlists/{watchlist_id}/items",
    response_model=WatchlistItemResponse,
    status_code=201
)
@handle_api_exceptions("add watchlist item", logger)
async def add_watchlist_item(
    watchlist_id: str,
    request: WatchlistItemCreate,
    user_id: CurrentUserId,
):
    """
    Add an item to a watchlist.

    Use "default" as watchlist_id to add to the default watchlist.

    Args:
        watchlist_id: Watchlist ID or "default"
        request: Watchlist item data
        user_id: User ID from authentication header

    Returns:
        Created watchlist item

    Raises:
        404: Watchlist not found or not owned by user
        409: Item already exists in watchlist (same symbol + instrument_type)
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    try:
        item = await db_create_watchlist_item(
            user_id=user_id,
            watchlist_id=resolved_id,
            symbol=request.symbol,
            instrument_type=request.instrument_type.value,
            exchange=request.exchange,
            name=request.name,
            notes=request.notes,
            alert_settings=(
                request.alert_settings.model_dump(exclude_none=True)
                if request.alert_settings else None
            ),
            metadata=request.metadata,
        )
    except ValueError as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=409, detail=str(e))

    logger.info(
        f"Added item {item['item_id']} to watchlist {resolved_id} for user {user_id}"
    )
    return WatchlistItemResponse.model_validate(item)


@router.get(
    "/watchlists/{watchlist_id}/items/{item_id}",
    response_model=WatchlistItemResponse
)
@handle_api_exceptions("get watchlist item", logger)
async def get_watchlist_item(
    watchlist_id: str,
    item_id: str,
    user_id: CurrentUserId,
):
    """
    Get a single watchlist item.

    Args:
        watchlist_id: Watchlist ID or "default"
        item_id: Watchlist item ID
        user_id: User ID from authentication header

    Returns:
        Watchlist item details

    Raises:
        404: Item not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    item = await db_get_watchlist_item(item_id, user_id)

    if not item:
        raise_not_found("Watchlist item")

    if str(item["watchlist_id"]) != resolved_id:
        raise_not_found("Watchlist item")

    return WatchlistItemResponse.model_validate(item)


@router.put(
    "/watchlists/{watchlist_id}/items/{item_id}",
    response_model=WatchlistItemResponse
)
@handle_api_exceptions("update watchlist item", logger)
async def update_watchlist_item(
    watchlist_id: str,
    item_id: str,
    request: WatchlistItemUpdate,
    user_id: CurrentUserId,
):
    """
    Update a watchlist item.

    Partial update supported - only provided fields are updated.

    Args:
        watchlist_id: Watchlist ID or "default"
        item_id: Watchlist item ID
        request: Fields to update
        user_id: User ID from authentication header

    Returns:
        Updated watchlist item

    Raises:
        404: Item not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    existing = await db_get_watchlist_item(item_id, user_id)
    if not existing or str(existing["watchlist_id"]) != resolved_id:
        raise_not_found("Watchlist item")

    item = await db_update_watchlist_item(
        item_id=item_id,
        user_id=user_id,
        name=request.name,
        notes=request.notes,
        alert_settings=(
            request.alert_settings.model_dump(exclude_none=True)
            if request.alert_settings else None
        ),
        metadata=request.metadata,
    )

    if not item:
        raise_not_found("Watchlist item")

    logger.info(f"Updated item {item_id} in watchlist {resolved_id} for user {user_id}")
    return WatchlistItemResponse.model_validate(item)


@router.delete("/watchlists/{watchlist_id}/items/{item_id}", status_code=204)
@handle_api_exceptions("delete watchlist item", logger)
async def delete_watchlist_item(
    watchlist_id: str,
    item_id: str,
    user_id: CurrentUserId,
):
    """
    Remove an item from a watchlist.

    Args:
        watchlist_id: Watchlist ID or "default"
        item_id: Watchlist item ID
        user_id: User ID from authentication header

    Returns:
        204 No Content on success

    Raises:
        404: Item not found or not owned by user
    """
    resolved_id = await _resolve_watchlist_id(watchlist_id, user_id)
    existing = await db_get_watchlist_item(item_id, user_id)
    if not existing or str(existing["watchlist_id"]) != resolved_id:
        raise_not_found("Watchlist item")

    deleted = await db_delete_watchlist_item(item_id, user_id)

    if not deleted:
        raise_not_found("Watchlist item")

    logger.info(f"Deleted item {item_id} from watchlist {resolved_id} for user {user_id}")
    return Response(status_code=204)
