"""
Database functions for automations: creating, reading, updating and
deleting the rows, and the scheduler's claims on them.

Run history lives in ``automation_executions``. The execution columns and
status filters stay here because every automation read carries its newest run.
"""

import logging
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


# =============================================================================
# Automation CRUD
# =============================================================================


AUTOMATION_COLUMNS = """
    automation_id, user_id, name, file_name, description,
    trigger_type, cron_expression, timezone, trigger_config,
    next_run_at, last_run_at,
    instruction, workspace_id, llm_model, additional_context,
    thread_strategy, conversation_thread_id,
    status, max_failures, failure_count, disable_reason,
    delivery_config, metadata,
    created_at, updated_at
"""

# One execution as every reader returns it: the history, the run feed and an
# automation's newest run. ``e`` is automation_executions.
EXECUTION_COLUMNS = """
    e.automation_execution_id, e.automation_id,
    e.status, e.conversation_thread_id,
    e.scheduled_at, e.started_at, e.completed_at,
    e.error_message, e.skip_reason, e.failure_reason, e.server_id,
    e.delivery_result, e.created_at, e.dismissed_at,
    e.result_excerpt AS excerpt
"""

UNSETTLED_STATUSES = "('pending', 'waiting', 'running')"


def not_a_server_skip(alias: str) -> str:
    """A firing that stands for its automation's last run: anything but one
    the server skipped because the thread stayed busy or it stopped."""
    return (
        f"({alias}.status <> 'skipped' OR {alias}.skip_reason IS NULL"
        f" OR {alias}.skip_reason NOT IN ('thread_busy', 'interrupted'))"
    )


def _last_execution_join(outer: str) -> str:
    """A LEFT JOIN LATERAL giving the automation row ``outer`` its
    ``last_execution``: the newest firing still in flight, else the newest
    that is not a skip the user never chose, else the newest.

    A newer skipped firing must not hide a run that is still going, or the
    automation reads idle and offers to run again. Nor may a skip the server
    made (its thread stayed busy, or it stopped) hide the failed run before
    it, which is what asks the user for attention. COALESCE runs each probe
    only when the ones before it find nothing, and each is one index descent.
    """
    return f"""
        LEFT JOIN LATERAL (
            SELECT to_jsonb(le_row) AS last_execution
            FROM (
                SELECT {EXECUTION_COLUMNS}
                FROM automation_executions e
                WHERE e.automation_execution_id = COALESCE(
                    (SELECT u.automation_execution_id
                     FROM automation_executions u
                     WHERE u.automation_id = {outer}.automation_id
                       AND u.status IN {UNSETTLED_STATUSES}
                     ORDER BY u.created_at DESC, u.automation_execution_id DESC
                     LIMIT 1),
                    (SELECT c.automation_execution_id
                     FROM automation_executions c
                     WHERE c.automation_id = {outer}.automation_id
                       AND {not_a_server_skip("c")}
                     ORDER BY c.created_at DESC, c.automation_execution_id DESC
                     LIMIT 1),
                    (SELECT n.automation_execution_id
                     FROM automation_executions n
                     WHERE n.automation_id = {outer}.automation_id
                     ORDER BY n.created_at DESC, n.automation_execution_id DESC
                     LIMIT 1)
                )
            ) le_row
        ) le ON TRUE
    """


# How many times a create picks a file name again after another create took
# the one it picked.
_FILE_NAME_ATTEMPTS = 5


def file_name_stem(name: str) -> str:
    """The file name an automation called ``name`` gets when nobody picks
    one, before ``.json`` and any ``-2`` that keeps it apart from another of
    the user's. Migration 058 named existing rows by the same rule, in SQL."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    stem = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    return stem[:48].strip("-") or "automation"


async def _free_file_name(user_id: str, name: str, conn) -> str:
    stem = file_name_stem(name)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT file_name FROM automations
            WHERE user_id = %s AND (file_name = %s OR file_name LIKE %s)
            """,
            (user_id, f"{stem}.json", f"{stem}-%.json"),
        )
        taken = {row[0] for row in await cur.fetchall()}
    candidate, n = f"{stem}.json", 1
    while candidate in taken:
        n += 1
        candidate = f"{stem}-{n}.json"
    return candidate


async def create_automation(
    user_id: str,
    name: str,
    trigger_type: str,
    instruction: str,
    *,
    file_name: Optional[str] = None,
    description: Optional[str] = None,
    cron_expression: Optional[str] = None,
    timezone: str = "UTC",
    trigger_config: Optional[Dict[str, Any]] = None,
    next_run_at: Optional[datetime] = None,
    workspace_id: Optional[str] = None,
    llm_model: Optional[str] = None,
    additional_context: Optional[List[Dict[str, Any]]] = None,
    thread_strategy: str = "new",
    conversation_thread_id: Optional[str] = None,
    max_failures: int = 3,
    delivery_config: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Create a new automation.

    Without a ``file_name`` it gets the first free one derived from ``name``.
    A create elsewhere can take that name between the read and the insert,
    so a clash reads the names again; a ``file_name`` the caller picked is
    its own, and a clash on it raises ``UniqueViolation``.
    """
    automation_id = str(uuid4())

    clashes = 0
    async with get_db_connection(conn) as conn:
        while True:
            chosen = file_name or await _free_file_name(user_id, name, conn)
            try:
                # A savepoint inside a caller's transaction, so a clash
                # leaves it usable for the next attempt.
                async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(f"""
                        INSERT INTO automations (
                            automation_id, user_id, name, file_name, description,
                            trigger_type, cron_expression, timezone, trigger_config,
                            next_run_at,
                            instruction, workspace_id, llm_model, additional_context,
                            thread_strategy, conversation_thread_id,
                            status, max_failures, failure_count,
                            delivery_config, metadata,
                            created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s,
                            %s, %s, %s, %s,
                            %s, %s,
                            'active', %s, 0,
                            %s, %s,
                            NOW(), NOW()
                        )
                        RETURNING {AUTOMATION_COLUMNS}
                    """, (
                        automation_id, user_id, name, chosen, description,
                        trigger_type, cron_expression, timezone,
                        Json(trigger_config or {}),
                        next_run_at,
                        instruction, workspace_id, llm_model,
                        Json(additional_context) if additional_context else None,
                        thread_strategy, conversation_thread_id,
                        max_failures,
                        Json(delivery_config or {}),
                        Json(metadata or {}),
                    ))
                    result = await cur.fetchone()
            except UniqueViolation:
                clashes += 1
                if file_name is not None or clashes == _FILE_NAME_ATTEMPTS:
                    raise
                continue
            logger.info(
                f"[automation_db] create_automation automation_id={automation_id} "
                f"user_id={user_id} trigger_type={trigger_type}"
            )
            return dict(result)


# Whether the pinned thread is one a run of this automation created, which is
# how a persistent thread is told from a pinned conversation once the first
# run has pinned it (``models.automation.in_own_thread``).
_OWNS_THREAD = """
    EXISTS (
        SELECT 1 FROM conversation_threads t
        WHERE t.conversation_thread_id = automations.conversation_thread_id
          AND t.metadata->'origin'->>'type' = 'automation'
          AND t.metadata->'origin'->>'id' = automations.automation_id::text
    ) AS owns_thread
"""


async def get_automation(
    automation_id: str,
    user_id: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Get a single automation by ID, verifying ownership, with its newest
    run and ``owns_thread``."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}, le.last_execution, {_OWNS_THREAD}
                FROM automations
                {_last_execution_join("automations")}
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

            # Get page. The lateral exposes only ``last_execution``, so the
            # automation columns stay unambiguous without a prefix.
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}, le.last_execution
                FROM automations
                {_last_execution_join("automations")}
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (*params, limit, offset))

            results = await cur.fetchall()
            return [dict(row) for row in results], total


async def list_all_automations(user_id: str, *, conn=None) -> List[Dict[str, Any]]:
    """Every automation of the user, each with its newest run and
    ``owns_thread``: the automations folder, one file per row."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}, le.last_execution, {_OWNS_THREAD}
                FROM automations
                {_last_execution_join("automations")}
                WHERE user_id = %s
                ORDER BY file_name
            """, (user_id,))
            return [dict(row) for row in await cur.fetchall()]


async def get_automation_file(
    user_id: str, file_name: str, *, conn=None, lock: bool = False
) -> Optional[Dict[str, Any]]:
    """The automation filed as ``file_name``, as ``list_all_automations``
    reads each. ``lock`` holds the row for the rest of ``conn``'s
    transaction."""
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {AUTOMATION_COLUMNS}, le.last_execution, {_OWNS_THREAD}
                FROM automations
                {_last_execution_join("automations")}
                WHERE user_id = %s AND file_name = %s
                {"FOR UPDATE OF automations" if lock else ""}
            """, (user_id, file_name))
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_automation_file_names(user_id: str) -> List[str]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT file_name FROM automations WHERE user_id = %s ORDER BY file_name",
                (user_id,),
            )
            return [row[0] for row in await cur.fetchall()]


async def rename_automation_file(
    automation_id: str, user_id: str, file_name: str, *, conn
) -> bool:
    """File the automation under another name; nothing else about it
    changes. Raises ``UniqueViolation`` when the user has one there."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE automations SET file_name = %s, updated_at = NOW()
            WHERE automation_id = %s AND user_id = %s
            """,
            (file_name, automation_id, user_id),
        )
        return cur.rowcount > 0


async def get_user_timezone(user_id: str) -> Optional[str]:
    """The zone the user keeps, which a new automation runs on when nothing
    else names one. Only the column: the user row is wide."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT timezone FROM users WHERE user_id = %s", (user_id,))
            row = await cur.fetchone()
            return row[0] if row else None


_FILE_LOCK_KEY_PREFIX = "automations:file:"


async def lock_user_automations(user_id: str, *, conn) -> None:
    """Serialize the writers of the user's automation files for the rest of
    ``conn``'s transaction, even where there is no row to lock: a create, or
    a rename onto a free name.

    The row itself is locked only by a save that changes it
    (``get_automation_file(lock=True)``), which holds off REST edits
    until it commits: a save that changes nothing leaves it alone.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (_FILE_LOCK_KEY_PREFIX + user_id,),
        )


async def update_automation(
    automation_id: str,
    user_id: str,
    *,
    conn=None,
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """Partial update of an automation. Only provided kwargs are applied.

    Fields in ``nullable_fields`` are set even when their value is None
    (i.e. SET column = NULL).  All other fields are skipped when None.
    """
    nullable_fields = {
        "next_run_at", "last_run_at", "conversation_thread_id", "disable_reason",
        "description", "llm_model",
    }
    builder = UpdateQueryBuilder()

    # Simple text/enum fields
    for field in [
        "name", "description", "cron_expression", "timezone",
        "instruction", "workspace_id", "llm_model",
        "thread_strategy", "conversation_thread_id",
        "status", "max_failures", "failure_count", "disable_reason",
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
        return await get_automation(automation_id, user_id, conn=conn)

    returning = AUTOMATION_COLUMNS.strip().split(",")
    returning = [c.strip() for c in returning]

    query, params = builder.build(
        table="automations",
        where_clause="automation_id = %s AND user_id = %s",
        where_params=[automation_id, user_id],
        returning_columns=returning,
    )
    # Answer with the automation as get_automation reads it, newest run
    # included, so a PATCH, pause or resume response matches a GET.
    query = f"""
        WITH updated AS ({query})
        SELECT updated.*, le.last_execution
        FROM updated
        {_last_execution_join("updated")}
    """

    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            result = await cur.fetchone()
            if result:
                logger.info(f"[automation_db] update_automation automation_id={automation_id}")
            return dict(result) if result else None


async def delete_automation(
    automation_id: str, user_id: str, *, conn=None
) -> bool:
    """Delete an automation (executions cascade deleted)."""
    async with get_db_connection(conn) as conn:
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
    - Sets last_run_at to the claim's transaction time
    - Inserts a pending execution record

    ``last_run_at`` and the execution's ``created_at`` are the same NOW(),
    which is how settling a one-time firing tells the claimed firing from a
    manual run of the same automation.

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
                        SET next_run_at = NULL, last_run_at = NOW()
                        WHERE automation_id = %s
                    """, (automation_id,))

                    # Insert pending execution
                    await cur.execute("""
                        INSERT INTO automation_executions (
                            automation_execution_id, automation_id,
                            status, scheduled_at, server_id, created_at,
                            heartbeat_at
                        )
                        VALUES (%s, %s, 'pending', %s, %s, NOW(), NOW())
                    """, (execution_id, automation_id, row["next_run_at"], server_id))

                    entry = dict(row)
                    entry["_execution_id"] = execution_id
                    claimed.append(entry)

                logger.info(
                    f"[automation_db] claimed {len(claimed)} due automations "
                    f"(server_id={server_id})"
                )
                return claimed


async def claim_price_firing(automation_id: str, server_id: str) -> Optional[str]:
    """Claim a price alert whose condition just hit; the new execution_id, or
    None when the alert is no longer active.

    The monitor decides from a list it loaded earlier, so the claim itself
    checks the status: a pause made since then wins instead of being flipped
    to 'executing' and re-armed to 'active' after the run. ``last_run_at`` and
    the execution's ``created_at`` share one NOW(), as in
    ``claim_due_automations``, so settling can tell this firing from a manual
    run of the same alert.
    """
    execution_id = str(uuid4())
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE automations
                    SET status = 'executing', next_run_at = NULL, last_run_at = NOW()
                    WHERE automation_id = %s AND status = 'active'
                """, (automation_id,))
                if cur.rowcount == 0:
                    return None
                await cur.execute("""
                    INSERT INTO automation_executions (
                        automation_execution_id, automation_id,
                        status, scheduled_at, server_id, created_at,
                        heartbeat_at
                    )
                    VALUES (%s, %s, 'pending', NOW(), %s, NOW(), NOW())
                """, (execution_id, automation_id, server_id))
    return execution_id


async def update_automation_next_run(
    automation_id: str, next_run_at: Optional[datetime]
) -> None:
    """Schedule a claimed cron's next run, unless the row moved on meanwhile.

    The claim left ``next_run_at`` NULL. An edit to the schedule or zone made
    since has already set it from the new expression, and a pause cleared
    the status; either way the time computed from the claimed row is stale,
    so it is written only over a NULL on a row still active.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automations
                SET next_run_at = %s
                WHERE automation_id = %s
                  AND next_run_at IS NULL
                  AND status = 'active'
            """, (next_run_at, automation_id))


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
