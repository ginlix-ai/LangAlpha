"""Storage for each user's trading permission and its append-only history."""

from __future__ import annotations

import logging
from typing import Any

from psycopg.rows import dict_row

from src.server.database.mcp_config_version import bump_user_versions
from src.server.database.order_attempts import refuse_unasked_attempts
from src.server.database.pool import get_db_connection
from src.server.database.user_lock import lock_user_writes
from src.server.services.tool_binding import REAL_MONEY_MODES
from src.server.services.trading_permission import TradingPermission

logger = logging.getLogger(__name__)


async def get_trading_permission_row(
    user_id: str, *, conn=None
) -> dict[str, Any] | None:
    """The stored row as written, or None for a user who never chose."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT level, agreement_version, agreed_at, updated_at "
                "FROM trading_permissions WHERE user_id = %s",
                (user_id,),
            )
            return await cur.fetchone()


async def set_trading_permission(
    user_id: str, level: TradingPermission, agreement_version: int | None
) -> dict[str, Any]:
    """Store a level, record the change, and re-resolve every workspace's tools.

    The version bump is what carries the change into a turn: each workspace's
    tool view is frozen at the version it was resolved under, and the approval
    each order tool is stamped with comes off that view. A level that asks also
    refuses every real-money order let through unasked and not yet sent, in the
    same transaction, so no earlier answer outlives the one stored here. The
    current row names an agreement only while its level needs one; every
    acceptance stays on the history rows.
    """
    accepted = agreement_version if level.needs_agreement else None
    async with get_db_connection() as db, db.transaction():
        async with db.cursor(row_factory=dict_row) as cur:
            # The lock a workspace insert takes, held to commit: without it a
            # workspace committed after the bump's snapshot misses the bump and
            # can resolve the old level under a version nothing moves again.
            await lock_user_writes(cur, user_id)
            await cur.execute(
                """
                INSERT INTO trading_permissions
                    (user_id, level, agreement_version, agreed_at, updated_at)
                VALUES (%(user_id)s, %(level)s, %(version)s,
                        CASE WHEN %(version)s::int IS NULL THEN NULL ELSE NOW() END,
                        NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    level = EXCLUDED.level,
                    agreement_version = EXCLUDED.agreement_version,
                    agreed_at = EXCLUDED.agreed_at,
                    updated_at = NOW()
                RETURNING level, agreement_version, agreed_at, updated_at
                """,
                {"user_id": user_id, "level": level.value, "version": accepted},
            )
            row = await cur.fetchone()
            await cur.execute(
                "INSERT INTO trading_permission_events "
                "(user_id, level, agreement_version) VALUES (%s, %s, %s)",
                (user_id, level.value, accepted),
            )
            # Before the bump: a connection's approval switch takes the
            # approval lock before its own bump, so with one order neither
            # write holds a workspace row while waiting on the other's lock.
            if level.asks:
                await refuse_unasked_attempts(user_id, REAL_MONEY_MODES, conn=db)
            await bump_user_versions(cur, user_id)
    logger.info(
        "[TRADING] user %s trading permission -> %s (agreement %s)",
        user_id,
        level.value,
        accepted,
    )
    return row
