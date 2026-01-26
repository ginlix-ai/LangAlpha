"""
Database utility functions for watchlist management.

Provides functions for creating, retrieving, updating, and deleting
watchlists and watchlist items in PostgreSQL.
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.conversation_db import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


# =============================================================================
# Watchlist (list metadata) Functions
# =============================================================================


async def create_watchlist(
    user_id: str,
    name: str,
    description: Optional[str] = None,
    is_default: bool = False,
    display_order: int = 0,
) -> Dict[str, Any]:
    """
    Create a new watchlist.

    Args:
        user_id: User ID
        name: Watchlist name
        description: Watchlist description
        is_default: Whether this is the default watchlist
        display_order: Display order for sorting

    Returns:
        Created watchlist dict

    Raises:
        ValueError: If watchlist with same name already exists
    """
    watchlist_id = str(uuid4())

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check for existing watchlist with same name
            await cur.execute("""
                SELECT watchlist_id FROM watchlists
                WHERE user_id = %s AND name = %s
            """, (user_id, name))

            existing = await cur.fetchone()
            if existing:
                raise ValueError(f"Watchlist '{name}' already exists")

            # If setting as default, unset other defaults
            if is_default:
                await cur.execute("""
                    UPDATE watchlists SET is_default = FALSE, updated_at = NOW()
                    WHERE user_id = %s AND is_default = TRUE
                """, (user_id,))

            # Insert new watchlist
            await cur.execute("""
                INSERT INTO watchlists (
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
            """, (watchlist_id, user_id, name, description, is_default, display_order))

            result = await cur.fetchone()
            logger.info(f"[watchlist_db] create_watchlist user_id={user_id} name={name}")
            return dict(result)


async def get_user_watchlists(user_id: str) -> List[Dict[str, Any]]:
    """
    Get all watchlists for a user.

    Args:
        user_id: User ID

    Returns:
        List of watchlist dicts
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
                FROM watchlists
                WHERE user_id = %s
                ORDER BY is_default DESC, display_order ASC, created_at ASC
            """, (user_id,))

            results = await cur.fetchall()
            return [dict(row) for row in results]


async def get_watchlist(
    watchlist_id: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single watchlist by ID.

    Args:
        watchlist_id: Watchlist ID
        user_id: User ID (for ownership verification)

    Returns:
        Watchlist dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
                FROM watchlists
                WHERE watchlist_id = %s AND user_id = %s
            """, (watchlist_id, user_id))

            result = await cur.fetchone()
            return dict(result) if result else None


async def update_watchlist(
    watchlist_id: str,
    user_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    display_order: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update a watchlist.

    Only updates fields that are provided (not None).

    Args:
        watchlist_id: Watchlist ID
        user_id: User ID (for ownership verification)
        name: New name
        description: New description
        display_order: New display order

    Returns:
        Updated watchlist dict or None if not found

    Raises:
        ValueError: If new name conflicts with existing watchlist
    """
    builder = UpdateQueryBuilder()
    builder.add_field("name", name)
    builder.add_field("description", description)
    builder.add_field("display_order", display_order)

    if not builder.has_updates():
        return await get_watchlist(watchlist_id, user_id)

    returning_columns = [
        "watchlist_id", "user_id", "name", "description",
        "is_default", "display_order", "created_at", "updated_at",
    ]

    query, params = builder.build(
        table="watchlists",
        where_clause="watchlist_id = %s AND user_id = %s",
        where_params=[watchlist_id, user_id],
        returning_columns=returning_columns,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check for name conflict if updating name
            if name is not None:
                await cur.execute("""
                    SELECT watchlist_id FROM watchlists
                    WHERE user_id = %s AND name = %s AND watchlist_id != %s
                """, (user_id, name, watchlist_id))
                existing = await cur.fetchone()
                if existing:
                    raise ValueError(f"Watchlist '{name}' already exists")

            await cur.execute(query, params)

            result = await cur.fetchone()
            if result:
                logger.info(f"[watchlist_db] update_watchlist watchlist_id={watchlist_id}")
            return dict(result) if result else None


async def delete_watchlist(watchlist_id: str, user_id: str) -> bool:
    """
    Delete a watchlist (items are cascade deleted).

    Args:
        watchlist_id: Watchlist ID
        user_id: User ID (for ownership verification)

    Returns:
        True if watchlist was deleted, False if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM watchlists
                WHERE watchlist_id = %s AND user_id = %s
            """, (watchlist_id, user_id))

            deleted = cur.rowcount > 0
            if deleted:
                logger.info(f"[watchlist_db] delete_watchlist watchlist_id={watchlist_id}")
            return deleted


async def get_or_create_default_watchlist(user_id: str) -> Dict[str, Any]:
    """
    Get the user's default watchlist, creating one if it doesn't exist.

    Args:
        user_id: User ID

    Returns:
        Default watchlist dict
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Try to get existing default watchlist
            await cur.execute("""
                SELECT
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
                FROM watchlists
                WHERE user_id = %s AND is_default = TRUE
            """, (user_id,))

            result = await cur.fetchone()
            if result:
                return dict(result)

            # Create default watchlist
            watchlist_id = str(uuid4())
            await cur.execute("""
                INSERT INTO watchlists (
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
                )
                VALUES (%s, %s, 'Default', NULL, TRUE, 0, NOW(), NOW())
                ON CONFLICT (user_id, name) DO UPDATE SET is_default = TRUE
                RETURNING
                    watchlist_id, user_id, name, description,
                    is_default, display_order, created_at, updated_at
            """, (watchlist_id, user_id))

            result = await cur.fetchone()
            logger.info(f"[watchlist_db] created default watchlist for user_id={user_id}")
            return dict(result)


# =============================================================================
# Watchlist Item Functions
# =============================================================================


async def get_watchlist_items(
    watchlist_id: str,
    user_id: str
) -> List[Dict[str, Any]]:
    """
    Get all items in a watchlist.

    Args:
        watchlist_id: Watchlist ID
        user_id: User ID (for ownership verification)

    Returns:
        List of watchlist item dicts
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    wi.item_id, wi.watchlist_id, wi.user_id, wi.symbol,
                    wi.instrument_type, wi.exchange, wi.name, wi.notes,
                    wi.alert_settings, wi.metadata,
                    wi.created_at, wi.updated_at
                FROM watchlist_items wi
                INNER JOIN watchlists w ON wi.watchlist_id = w.watchlist_id
                WHERE wi.watchlist_id = %s AND w.user_id = %s
                ORDER BY wi.created_at DESC
            """, (watchlist_id, user_id))

            results = await cur.fetchall()
            return [dict(row) for row in results]


async def get_watchlist_item(
    item_id: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single watchlist item by ID.

    Args:
        item_id: Watchlist item ID
        user_id: User ID (for ownership verification)

    Returns:
        Watchlist item dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    item_id, watchlist_id, user_id, symbol, instrument_type,
                    exchange, name, notes, alert_settings, metadata,
                    created_at, updated_at
                FROM watchlist_items
                WHERE item_id = %s AND user_id = %s
            """, (item_id, user_id))

            result = await cur.fetchone()
            return dict(result) if result else None


async def create_watchlist_item(
    user_id: str,
    watchlist_id: str,
    symbol: str,
    instrument_type: str,
    exchange: Optional[str] = None,
    name: Optional[str] = None,
    notes: Optional[str] = None,
    alert_settings: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create a new watchlist item.

    Args:
        user_id: User ID
        watchlist_id: Parent watchlist ID
        symbol: Instrument symbol
        instrument_type: Type of instrument (stock, etf, etc.)
        exchange: Exchange name
        name: Full instrument name
        notes: User notes
        alert_settings: Alert configuration
        metadata: Additional metadata

    Returns:
        Created watchlist item dict

    Raises:
        ValueError: If item already exists in watchlist (same symbol + instrument_type)
        ValueError: If watchlist doesn't exist or doesn't belong to user
    """
    item_id = str(uuid4())

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Verify watchlist exists and belongs to user
            await cur.execute("""
                SELECT watchlist_id FROM watchlists
                WHERE watchlist_id = %s AND user_id = %s
            """, (watchlist_id, user_id))

            watchlist = await cur.fetchone()
            if not watchlist:
                raise ValueError("Watchlist not found")

            # Check for existing item with same symbol + instrument_type in this watchlist
            await cur.execute("""
                SELECT item_id FROM watchlist_items
                WHERE watchlist_id = %s AND symbol = %s AND instrument_type = %s
            """, (watchlist_id, symbol, instrument_type))

            existing = await cur.fetchone()
            if existing:
                raise ValueError(
                    f"Item already exists in watchlist for {symbol} ({instrument_type})"
                )

            # Insert new watchlist item
            await cur.execute("""
                INSERT INTO watchlist_items (
                    item_id, watchlist_id, user_id, symbol, instrument_type,
                    exchange, name, notes, alert_settings, metadata,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING
                    item_id, watchlist_id, user_id, symbol, instrument_type,
                    exchange, name, notes, alert_settings, metadata,
                    created_at, updated_at
            """, (
                item_id, watchlist_id, user_id, symbol, instrument_type,
                exchange, name, notes,
                Json(alert_settings or {}),
                Json(metadata or {}),
            ))

            result = await cur.fetchone()
            logger.info(
                f"[watchlist_db] create_watchlist_item user_id={user_id} "
                f"watchlist_id={watchlist_id} symbol={symbol}"
            )
            return dict(result)


async def update_watchlist_item(
    item_id: str,
    user_id: str,
    name: Optional[str] = None,
    notes: Optional[str] = None,
    alert_settings: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update a watchlist item.

    Only updates fields that are provided (not None).

    Args:
        item_id: Watchlist item ID
        user_id: User ID (for ownership verification)
        name: New name
        notes: New notes
        alert_settings: New alert settings
        metadata: New metadata

    Returns:
        Updated watchlist item dict or None if not found
    """
    builder = UpdateQueryBuilder()
    builder.add_field("name", name)
    builder.add_field("notes", notes)
    builder.add_field("alert_settings", alert_settings, is_json=True)
    builder.add_field("metadata", metadata, is_json=True)

    if not builder.has_updates():
        return await get_watchlist_item(item_id, user_id)

    returning_columns = [
        "item_id", "watchlist_id", "user_id", "symbol", "instrument_type",
        "exchange", "name", "notes", "alert_settings", "metadata",
        "created_at", "updated_at",
    ]

    query, params = builder.build(
        table="watchlist_items",
        where_clause="item_id = %s AND user_id = %s",
        where_params=[item_id, user_id],
        returning_columns=returning_columns,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)

            result = await cur.fetchone()
            if result:
                logger.info(f"[watchlist_db] update_watchlist_item item_id={item_id}")
            return dict(result) if result else None


async def delete_watchlist_item(item_id: str, user_id: str) -> bool:
    """
    Delete a watchlist item.

    Args:
        item_id: Watchlist item ID
        user_id: User ID (for ownership verification)

    Returns:
        True if item was deleted, False if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM watchlist_items
                WHERE item_id = %s AND user_id = %s
            """, (item_id, user_id))

            deleted = cur.rowcount > 0
            if deleted:
                logger.info(f"[watchlist_db] delete_watchlist_item item_id={item_id}")
            return deleted
