"""
Database utility functions for portfolio management.

Provides functions for creating, retrieving, updating, and deleting
portfolio holdings in PostgreSQL.
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.conversation import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


async def get_user_portfolio(user_id: str) -> List[Dict[str, Any]]:
    """
    Get all portfolio holdings for a user.

    Args:
        user_id: User ID

    Returns:
        List of portfolio holding dicts
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    user_portfolio_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                FROM user_portfolios
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))

            results = await cur.fetchall()
            return [dict(row) for row in results]


async def get_portfolio_holding(
    user_portfolio_id: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single portfolio holding by ID.

    Args:
        user_portfolio_id: Portfolio holding ID
        user_id: User ID (for ownership verification)

    Returns:
        Portfolio holding dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    user_portfolio_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                FROM user_portfolios
                WHERE user_portfolio_id = %s AND user_id = %s
            """, (user_portfolio_id, user_id))

            result = await cur.fetchone()
            return dict(result) if result else None


async def update_portfolio_holding(
    user_portfolio_id: str,
    user_id: str,
    name: Optional[str] = None,
    quantity: Optional[Decimal] = None,
    average_cost: Optional[Decimal] = None,
    currency: Optional[str] = None,
    account_name: Optional[str] = None,
    notes: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    first_purchased_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update a portfolio holding.

    Only updates fields that are provided (not None).

    Args:
        user_portfolio_id: Portfolio holding ID
        user_id: User ID (for ownership verification)
        name: New name
        quantity: New quantity
        average_cost: New average cost
        currency: New currency
        account_name: New account name
        notes: New notes
        metadata: New metadata
        first_purchased_at: New first purchase date

    Returns:
        Updated portfolio holding dict or None if not found
    """
    builder = UpdateQueryBuilder()
    builder.add_field("name", name)
    builder.add_field("quantity", quantity)
    builder.add_field("average_cost", average_cost)
    builder.add_field("currency", currency)
    builder.add_field("account_name", account_name)
    builder.add_field("notes", notes)
    builder.add_field("metadata", metadata, is_json=True)
    builder.add_field("first_purchased_at", first_purchased_at)

    if not builder.has_updates():
        return await get_portfolio_holding(user_portfolio_id, user_id)

    returning_columns = [
        "user_portfolio_id", "user_id", "symbol", "instrument_type", "exchange",
        "name", "quantity", "average_cost", "currency", "account_name",
        "notes", "metadata", "first_purchased_at", "created_at", "updated_at",
    ]

    query, params = builder.build(
        table="user_portfolios",
        where_clause="user_portfolio_id = %s AND user_id = %s",
        where_params=[user_portfolio_id, user_id],
        returning_columns=returning_columns,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)

            result = await cur.fetchone()
            if result:
                logger.info(
                    f"[portfolio_db] update_portfolio_holding "
                    f"user_portfolio_id={user_portfolio_id}"
                )
            return dict(result) if result else None


async def upsert_portfolio_holding(
    user_id: str,
    symbol: str,
    instrument_type: str,
    quantity: Decimal,
    exchange: Optional[str] = None,
    name: Optional[str] = None,
    average_cost: Optional[Decimal] = None,
    currency: str = "USD",
    account_name: Optional[str] = None,
    notes: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    first_purchased_at: Optional[datetime] = None,
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Create or merge a portfolio holding.

    If a holding already exists for the same (user_id, symbol, instrument_type, account_name),
    merges the position by summing quantities and computing weighted average cost.

    Args:
        user_id: User ID
        symbol: Instrument symbol
        instrument_type: Type of instrument (stock, etf, etc.)
        quantity: Number of units held
        exchange: Exchange name
        name: Full instrument name
        average_cost: Average cost per unit
        currency: Currency code
        account_name: Account name (e.g., 'Robinhood')
        notes: User notes
        metadata: Additional metadata
        first_purchased_at: First purchase date

    Returns:
        Tuple of (holding dict, merge_details dict or None).
        merge_details is None for fresh creates, or a dict with previous/added/result
        when an existing position was merged.
    """
    _returning = """
        RETURNING
            user_portfolio_id, user_id, symbol, instrument_type, exchange,
            name, quantity, average_cost, currency, account_name,
            notes, metadata, first_purchased_at, created_at, updated_at
    """

    async with get_db_connection() as conn:
        # Explicit transaction for atomicity (autocommit is ON by default)
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # FOR UPDATE locks the row to prevent concurrent merge races
                await cur.execute(f"""
                    SELECT
                        user_portfolio_id, user_id, symbol, instrument_type, exchange,
                        name, quantity, average_cost, currency, account_name,
                        notes, metadata, first_purchased_at, created_at, updated_at
                    FROM user_portfolios
                    WHERE user_id = %s AND symbol = %s AND instrument_type = %s
                    AND account_name IS NOT DISTINCT FROM %s
                    FOR UPDATE
                """, (user_id, symbol, instrument_type, account_name))

                existing = await cur.fetchone()

                if existing:
                    existing_qty = existing["quantity"] or Decimal("0")
                    total_qty = existing_qty + quantity

                    # Weighted average cost (guard against zero total quantity)
                    existing_cost = existing["average_cost"]
                    if total_qty == 0:
                        merged_avg_cost = None
                    elif existing_cost is not None and average_cost is not None:
                        merged_avg_cost = (
                            existing_qty * existing_cost + quantity * average_cost
                        ) / total_qty
                    elif average_cost is not None:
                        merged_avg_cost = average_cost
                    elif existing_cost is not None:
                        merged_avg_cost = existing_cost
                    else:
                        merged_avg_cost = None

                    # Keep the earlier first_purchased_at
                    existing_date = existing["first_purchased_at"]
                    if existing_date and first_purchased_at:
                        merged_date = min(existing_date, first_purchased_at)
                    else:
                        merged_date = existing_date or first_purchased_at

                    merged_name = name or existing["name"]
                    merged_notes = notes or existing["notes"]

                    def _dec_str(d):
                        return format(d.normalize(), 'f')

                    merge_details = {
                        "previous": {
                            "quantity": _dec_str(existing_qty),
                            "average_cost": _dec_str(existing_cost) if existing_cost is not None else None,
                        },
                        "added": {
                            "quantity": _dec_str(quantity),
                            "average_cost": _dec_str(average_cost) if average_cost is not None else None,
                        },
                        "result": {
                            "quantity": _dec_str(total_qty),
                            "average_cost": _dec_str(merged_avg_cost) if merged_avg_cost is not None else None,
                        },
                    }

                    await cur.execute(f"""
                        UPDATE user_portfolios
                        SET quantity = %s, average_cost = %s, name = %s, notes = %s,
                            first_purchased_at = %s, updated_at = NOW()
                        WHERE user_portfolio_id = %s AND user_id = %s
                        {_returning}
                    """, (
                        total_qty, merged_avg_cost, merged_name, merged_notes,
                        merged_date, existing["user_portfolio_id"], user_id,
                    ))

                    result = await cur.fetchone()
                    logger.info(
                        f"[portfolio_db] upsert_portfolio_holding merged "
                        f"user_id={user_id} symbol={symbol}"
                    )
                    return dict(result), merge_details

                else:
                    user_portfolio_id = str(uuid4())
                    await cur.execute(f"""
                        INSERT INTO user_portfolios (
                            user_portfolio_id, user_id, symbol, instrument_type, exchange,
                            name, quantity, average_cost, currency, account_name,
                            notes, metadata, first_purchased_at, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        {_returning}
                    """, (
                        user_portfolio_id, user_id, symbol, instrument_type, exchange,
                        name, quantity, average_cost, currency, account_name,
                        notes,
                        Json(metadata or {}),
                        first_purchased_at,
                    ))

                    result = await cur.fetchone()
                    logger.info(
                        f"[portfolio_db] upsert_portfolio_holding created "
                        f"user_id={user_id} symbol={symbol}"
                    )
                    return dict(result), None


async def delete_portfolio_holding(user_portfolio_id: str, user_id: str) -> bool:
    """
    Delete a portfolio holding.

    Args:
        user_portfolio_id: Portfolio holding ID
        user_id: User ID (for ownership verification)

    Returns:
        True if holding was deleted, False if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM user_portfolios
                WHERE user_portfolio_id = %s AND user_id = %s
            """, (user_portfolio_id, user_id))

            deleted = cur.rowcount > 0
            if deleted:
                logger.info(
                    f"[portfolio_db] delete_portfolio_holding "
                    f"user_portfolio_id={user_portfolio_id}"
                )
            return deleted
