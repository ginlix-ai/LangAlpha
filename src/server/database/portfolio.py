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

from src.server.database.conversation_db import get_db_connection
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
                    holding_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                FROM user_portfolio
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))

            results = await cur.fetchall()
            return [dict(row) for row in results]


async def get_portfolio_holding(
    holding_id: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get a single portfolio holding by ID.

    Args:
        holding_id: Portfolio holding ID
        user_id: User ID (for ownership verification)

    Returns:
        Portfolio holding dict or None if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    holding_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                FROM user_portfolio
                WHERE holding_id = %s AND user_id = %s
            """, (holding_id, user_id))

            result = await cur.fetchone()
            return dict(result) if result else None


async def create_portfolio_holding(
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
) -> Dict[str, Any]:
    """
    Create a new portfolio holding.

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
        Created portfolio holding dict

    Raises:
        ValueError: If holding already exists (same symbol + instrument_type + account_name)
    """
    holding_id = str(uuid4())

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check for existing holding with same symbol + instrument_type + account_name
            # IS NOT DISTINCT FROM handles NULL comparisons properly
            await cur.execute("""
                SELECT holding_id FROM user_portfolio
                WHERE user_id = %s AND symbol = %s AND instrument_type = %s
                AND account_name IS NOT DISTINCT FROM %s
            """, (user_id, symbol, instrument_type, account_name))

            existing = await cur.fetchone()
            if existing:
                account_str = f" in {account_name}" if account_name else ""
                raise ValueError(
                    f"Portfolio holding already exists for {symbol} "
                    f"({instrument_type}){account_str}"
                )

            # Insert new portfolio holding
            await cur.execute("""
                INSERT INTO user_portfolio (
                    holding_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING
                    holding_id, user_id, symbol, instrument_type, exchange,
                    name, quantity, average_cost, currency, account_name,
                    notes, metadata, first_purchased_at, created_at, updated_at
            """, (
                holding_id, user_id, symbol, instrument_type, exchange,
                name, quantity, average_cost, currency, account_name,
                notes,
                Json(metadata or {}),
                first_purchased_at,
            ))

            result = await cur.fetchone()
            logger.info(
                f"[portfolio_db] create_portfolio_holding user_id={user_id} "
                f"symbol={symbol}"
            )
            return dict(result)


async def update_portfolio_holding(
    holding_id: str,
    user_id: str,
    name: Optional[str] = None,
    quantity: Optional[Decimal] = None,
    average_cost: Optional[Decimal] = None,
    currency: Optional[str] = None,
    notes: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    first_purchased_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update a portfolio holding.

    Only updates fields that are provided (not None).

    Args:
        holding_id: Portfolio holding ID
        user_id: User ID (for ownership verification)
        name: New name
        quantity: New quantity
        average_cost: New average cost
        currency: New currency
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
    builder.add_field("notes", notes)
    builder.add_field("metadata", metadata, is_json=True)
    builder.add_field("first_purchased_at", first_purchased_at)

    if not builder.has_updates():
        return await get_portfolio_holding(holding_id, user_id)

    returning_columns = [
        "holding_id", "user_id", "symbol", "instrument_type", "exchange",
        "name", "quantity", "average_cost", "currency", "account_name",
        "notes", "metadata", "first_purchased_at", "created_at", "updated_at",
    ]

    query, params = builder.build(
        table="user_portfolio",
        where_clause="holding_id = %s AND user_id = %s",
        where_params=[holding_id, user_id],
        returning_columns=returning_columns,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)

            result = await cur.fetchone()
            if result:
                logger.info(
                    f"[portfolio_db] update_portfolio_holding "
                    f"holding_id={holding_id}"
                )
            return dict(result) if result else None


async def delete_portfolio_holding(holding_id: str, user_id: str) -> bool:
    """
    Delete a portfolio holding.

    Args:
        holding_id: Portfolio holding ID
        user_id: User ID (for ownership verification)

    Returns:
        True if holding was deleted, False if not found
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM user_portfolio
                WHERE holding_id = %s AND user_id = %s
            """, (holding_id, user_id))

            deleted = cur.rowcount > 0
            if deleted:
                logger.info(
                    f"[portfolio_db] delete_portfolio_holding "
                    f"holding_id={holding_id}"
                )
            return deleted
