"""
Database utility functions for automation management.

Provides functions for creating, retrieving, updating, and deleting
automations and automation executions in PostgreSQL.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.conversation import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


# =============================================================================
# Automation CRUD
# =============================================================================


AUTOMATION_COLUMNS = """
    automation_id, user_id, name, description,
    trigger_type, cron_expression, timezone, trigger_config,
    next_run_at, last_run_at,
    agent_mode, instruction, workspace_id, llm_model, additional_context,
    thread_strategy, conversation_thread_id,
    status, max_failures, failure_count,
    delivery_config, metadata,
    created_at, updated_at
"""


async def create_automation(
    user_id: str,
    name: str,
    trigger_type: str,
    instruction: str,
    *,
    description: Optional[str] = None,
    cron_expression: Optional[str] = None,
    timezone: str = "UTC",
    trigger_config: Optional[Dict[str, Any]] = None,
    next_run_at: Optional[datetime] = None,
    agent_mode: str = "flash",
    workspace_id: Optional[str] = None,
    llm_model: Optional[str] = None,
    additional_context: Optional[List[Dict[str, Any]]] = None,
    thread_strategy: str = "new",
    conversation_thread_id: Optional[str] = None,
    max_failures: int = 3,
    delivery_config: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a new automation."""
    automation_id = str(uuid4())

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                INSERT INTO automations (
                    automation_id, user_id, name, description,
                    trigger_type, cron_expression, timezone, trigger_config,
                    next_run_at,
                    agent_mode, instruction, workspace_id, llm_model, additional_context,
                    thread_strategy, conversation_thread_id,
                    status, max_failures, failure_count,
                    delivery_config, metadata,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    'active', %s, 0,
                    %s, %s,
                    NOW(), NOW()
                )
                RETURNING {AUTOMATION_COLUMNS}
            """, (
                automation_id, user_id, name, description,
                trigger_type, cron_expression, timezone,
                Json(trigger_config or {}),
                next_run_at,
                agent_mode, instruction, workspace_id, llm_model,
                Json(additional_context) if additional_context else None,
                thread_strategy, conversation_thread_id,
                max_failures,
                Json(delivery_config or {}),
                Json(metadata or {}),
            ))

            result = await cur.fetchone()
            logger.info(
                f"[automation_db] create_automation user_id={user_id} "
                f"name={name} trigger_type={trigger_type}"
            )
            return dict(result)


async def get_automation(
    automation_id: str,
    user_id: str,
) -> Optional[Dict[str, Any]]:
    """Get a single automation by ID, verifying ownership."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}
                FROM automations
                WHERE automation_id = %s AND user_id = %s
            """, (automation_id, user_id))

            result = await cur.fetchone()
            return dict(result) if result else None


async def list_automations(
    user_id: str,
    *,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[List[Dict[str, Any]], int]:
    """List automations for a user with optional status filter.

    Returns:
        Tuple of (list of automation dicts, total count).
    """
    where_parts = ["user_id = %s"]
    params: list = [user_id]

    if status:
        where_parts.append("status = %s")
        params.append(status)

    where_clause = " AND ".join(where_parts)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get total count
            await cur.execute(
                f"SELECT COUNT(*) as cnt FROM automations WHERE {where_clause}",
                tuple(params),
            )
            total = (await cur.fetchone())["cnt"]

            # Get page
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}
                FROM automations
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (*params, limit, offset))

            results = await cur.fetchall()
            return [dict(row) for row in results], total


async def update_automation(
    automation_id: str,
    user_id: str,
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """Partial update of an automation. Only provided kwargs are applied.

    Fields in ``nullable_fields`` are set even when their value is None
    (i.e. SET column = NULL).  All other fields are skipped when None.
    """
    nullable_fields = {"next_run_at", "last_run_at", "conversation_thread_id"}
    builder = UpdateQueryBuilder()

    # Simple text/enum fields
    for field in [
        "name", "description", "cron_expression", "timezone",
        "agent_mode", "instruction", "workspace_id", "llm_model",
        "thread_strategy", "conversation_thread_id",
        "status", "max_failures", "failure_count",
        "next_run_at", "last_run_at",
    ]:
        if field not in kwargs:
            continue
        if kwargs[field] is None and field not in nullable_fields:
            continue
        builder.add_field(field, kwargs[field], nullable=field in nullable_fields)

    # JSONB fields
    for field in [
        "trigger_config", "additional_context",
        "delivery_config", "metadata",
    ]:
        if field in kwargs and kwargs[field] is not None:
            builder.add_field(field, kwargs[field], is_json=True)

    if not builder.has_updates():
        return await get_automation(automation_id, user_id)

    returning = AUTOMATION_COLUMNS.strip().split(",")
    returning = [c.strip() for c in returning]

    query, params = builder.build(
        table="automations",
        where_clause="automation_id = %s AND user_id = %s",
        where_params=[automation_id, user_id],
        returning_columns=returning,
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            result = await cur.fetchone()
            if result:
                logger.info(f"[automation_db] update_automation automation_id={automation_id}")
            return dict(result) if result else None


async def delete_automation(automation_id: str, user_id: str) -> bool:
    """Delete an automation (executions cascade deleted)."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                DELETE FROM automations
                WHERE automation_id = %s AND user_id = %s
            """, (automation_id, user_id))

            deleted = cur.rowcount > 0
            if deleted:
                logger.info(f"[automation_db] delete_automation automation_id={automation_id}")
            return deleted


# =============================================================================
# Scheduler queries (used by AutomationScheduler)
# =============================================================================


async def claim_due_automations(
    now: datetime,
    server_id: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Atomically claim automations whose next_run_at <= now.

    Uses FOR UPDATE SKIP LOCKED so multiple server instances
    won't double-claim the same automation.

    For each claimed row:
    - Sets next_run_at to NULL (will be recalculated externally)
    - Sets last_run_at to now
    - Inserts a pending execution record

    Returns the claimed automation rows together with the new execution_id.
    """
    async with get_db_connection() as conn:
        # Need an explicit transaction (autocommit is ON by default)
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Lock and fetch due automations
                await cur.execute(f"""
                    SELECT {AUTOMATION_COLUMNS}
                    FROM automations
                    WHERE status = 'active'
                      AND next_run_at IS NOT NULL
                      AND next_run_at <= %s
                    ORDER BY next_run_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (now, limit))

                rows = await cur.fetchall()
                if not rows:
                    return []

                claimed = []
                for row in rows:
                    automation_id = str(row["automation_id"])
                    execution_id = str(uuid4())

                    # Advance next_run_at to NULL (scheduler will recalculate)
                    await cur.execute("""
                        UPDATE automations
                        SET next_run_at = NULL, last_run_at = %s
                        WHERE automation_id = %s
                    """, (now, automation_id))

                    # Insert pending execution
                    await cur.execute("""
                        INSERT INTO automation_executions (
                            automation_execution_id, automation_id,
                            status, scheduled_at, server_id, created_at
                        )
                        VALUES (%s, %s, 'pending', %s, %s, NOW())
                    """, (execution_id, automation_id, row["next_run_at"], server_id))

                    entry = dict(row)
                    entry["_execution_id"] = execution_id
                    claimed.append(entry)

                logger.info(
                    f"[automation_db] claimed {len(claimed)} due automations "
                    f"(server_id={server_id})"
                )
                return claimed


async def update_automation_next_run(
    automation_id: str,
    next_run_at: Optional[datetime],
    *,
    status: Optional[str] = None,
) -> None:
    """Update next_run_at (and optionally status) after claiming."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            if status:
                await cur.execute("""
                    UPDATE automations
                    SET next_run_at = %s, status = %s
                    WHERE automation_id = %s
                """, (next_run_at, status, automation_id))
            else:
                await cur.execute("""
                    UPDATE automations
                    SET next_run_at = %s
                    WHERE automation_id = %s
                """, (next_run_at, automation_id))


async def increment_failure_count(automation_id: str) -> int:
    """Increment failure_count and return new value. Auto-disables if max reached."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                UPDATE automations
                SET failure_count = failure_count + 1
                WHERE automation_id = %s
                RETURNING failure_count, max_failures
            """, (automation_id,))
            row = await cur.fetchone()
            if not row:
                return 0
            # Auto-disable if exceeded max
            if row["failure_count"] >= row["max_failures"]:
                await cur.execute("""
                    UPDATE automations
                    SET status = 'disabled', next_run_at = NULL
                    WHERE automation_id = %s
                """, (automation_id,))
                logger.warning(
                    f"[automation_db] Auto-disabled automation {automation_id} "
                    f"after {row['failure_count']} failures"
                )
            return row["failure_count"]


async def reset_failure_count(automation_id: str) -> None:
    """Reset failure_count to 0 (called on successful execution)."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automations SET failure_count = 0
                WHERE automation_id = %s
            """, (automation_id,))


async def restore_executing_to_active(automation_id: str) -> None:
    """Restore status from 'executing' back to 'active' (only if still 'executing')."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automations SET status = 'active'
                WHERE automation_id = %s AND status = 'executing'
            """, (automation_id,))


async def get_active_price_automations() -> List[Dict[str, Any]]:
    """Get all active price-triggered automations (for PriceMonitorService)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}
                FROM automations
                WHERE trigger_type = 'price'
                  AND status = 'active'
            """)
            results = await cur.fetchall()
            return [dict(row) for row in results]


# =============================================================================
# Execution record queries
# =============================================================================


async def update_execution_status(
    execution_id: str,
    status: str,
    *,
    conversation_thread_id: Optional[str] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    delivery_result: Optional[list] = None,
) -> None:
    """Update an execution record's status and optional fields."""
    builder = UpdateQueryBuilder()
    builder.add_field("status", status)
    builder.add_field("conversation_thread_id", conversation_thread_id)
    builder.add_field("error_message", error_message)
    builder.add_field("started_at", started_at)
    builder.add_field("completed_at", completed_at)
    builder.add_field("delivery_result", delivery_result, is_json=True)

    query, params = builder.build(
        table="automation_executions",
        where_clause="automation_execution_id = %s",
        where_params=[execution_id],
        include_updated_at=False,  # no updated_at column on executions
    )

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)


async def list_executions(
    automation_id: str,
    user_id: str,
    *,
    limit: int = 20,
    offset: int = 0,
) -> tuple[List[Dict[str, Any]], int]:
    """List executions for an automation (with ownership check).

    Returns:
        Tuple of (list of execution dicts, total count).
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Verify ownership
            await cur.execute("""
                SELECT automation_id FROM automations
                WHERE automation_id = %s AND user_id = %s
            """, (automation_id, user_id))
            if not await cur.fetchone():
                return [], 0

            # Count
            await cur.execute("""
                SELECT COUNT(*) as cnt FROM automation_executions
                WHERE automation_id = %s
            """, (automation_id,))
            total = (await cur.fetchone())["cnt"]

            # Fetch page
            await cur.execute("""
                SELECT
                    automation_execution_id, automation_id,
                    status, conversation_thread_id,
                    scheduled_at, started_at, completed_at,
                    error_message, server_id, created_at
                FROM automation_executions
                WHERE automation_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (automation_id, limit, offset))

            results = await cur.fetchall()
            return [dict(row) for row in results], total


async def mark_stale_executions_failed(server_id: str) -> int:
    """Mark pending/running executions from a specific server_id as failed.

    Called on startup to recover from crashed server instances.

    Returns:
        Number of executions marked as failed.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automation_executions
                SET status = 'failed',
                    error_message = 'Server restarted during execution',
                    completed_at = NOW()
                WHERE server_id = %s
                  AND status IN ('pending', 'running')
            """, (server_id,))
            count = cur.rowcount
            if count > 0:
                logger.info(
                    f"[automation_db] Marked {count} stale executions as failed "
                    f"(server_id={server_id})"
                )
            return count


async def create_execution(
    automation_id: str,
    scheduled_at: datetime,
    server_id: str,
) -> str:
    """Create a new execution record (for manual triggers).

    Returns:
        The new execution_id.
    """
    execution_id = str(uuid4())
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO automation_executions (
                    automation_execution_id, automation_id,
                    status, scheduled_at, server_id, created_at
                )
                VALUES (%s, %s, 'pending', %s, %s, NOW())
            """, (execution_id, automation_id, scheduled_at, server_id))
    return execution_id
