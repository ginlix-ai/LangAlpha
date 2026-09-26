"""
Database utility functions for automation management.

Provides functions for creating, retrieving, updating, and deleting
automations and automation executions in PostgreSQL.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Sequence
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
from src.server.database.pool import get_db_connection
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
    e.delivery_result, e.created_at,
    e.result_excerpt AS excerpt
"""

_UNSETTLED = "('pending', 'waiting', 'running')"


def _not_a_server_skip(alias: str) -> str:
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
                       AND u.status IN {_UNSETTLED}
                     ORDER BY u.created_at DESC, u.automation_execution_id DESC
                     LIMIT 1),
                    (SELECT c.automation_execution_id
                     FROM automation_executions c
                     WHERE c.automation_id = {outer}.automation_id
                       AND {_not_a_server_skip("c")}
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
                SELECT {AUTOMATION_COLUMNS}, le.last_execution
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


async def update_automation(
    automation_id: str,
    user_id: str,
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """Partial update of an automation. Only provided kwargs are applied.

    Fields in ``nullable_fields`` are set even when their value is None
    (i.e. SET column = NULL).  All other fields are skipped when None.
    """
    nullable_fields = {
        "next_run_at", "last_run_at", "conversation_thread_id", "disable_reason",
    }
    builder = UpdateQueryBuilder()

    # Simple text/enum fields
    for field in [
        "name", "description", "cron_expression", "timezone",
        "agent_mode", "instruction", "workspace_id", "llm_model",
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
        return await get_automation(automation_id, user_id)

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
    """Update next_run_at after claiming."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automations
                SET next_run_at = %s
                WHERE automation_id = %s
            """, (next_run_at, automation_id))


async def _count_strike(cur, automation_id: str, *, fuse: bool = False) -> int:
    """Count a failure, disabling the automation once it reaches its limit,
    or at once for a ``fuse``.

    A disable writes ``disable_reason`` ('provider_auth' for a fuse, else
    'max_failures'), so the user is told which of the two switched it off.
    It keeps a one-time automation's time, as a pause does: a manual run that
    fused it leaves the scheduled one for a resume to restore. Only a live
    automation is disabled: a pause, a finish or an earlier disable, and its
    reason, stand.
    """
    disables = (
        "(%(fuse)s OR failure_count + 1 >= max_failures)"
        " AND status IN ('active', 'executing')"
    )
    await cur.execute(f"""
        UPDATE automations
        SET failure_count = failure_count + 1,
            status = CASE WHEN {disables} THEN 'disabled' ELSE status END,
            next_run_at = CASE WHEN {disables} AND trigger_type <> 'once'
                               THEN NULL ELSE next_run_at END,
            disable_reason = CASE WHEN {disables} THEN %(reason)s
                                  ELSE disable_reason END
        FROM (SELECT status AS was FROM automations
              WHERE automation_id = %(automation_id)s) before_strike
        WHERE automation_id = %(automation_id)s
        RETURNING failure_count, status = 'disabled' AND before_strike.was <> 'disabled' AS disabled
    """, {
        "fuse": fuse,
        "reason": "provider_auth" if fuse else "max_failures",
        "automation_id": automation_id,
    })
    row = await cur.fetchone()
    if not row:
        return 0
    if row["disabled"]:
        logger.warning(
            f"[automation_db] Auto-disabled automation {automation_id} "
            f"(reason={'provider_auth' if fuse else 'max_failures'}, "
            f"failure_count={row['failure_count']})"
        )
    return row["failure_count"]


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


def _execution_update(
    to: str, where_clause: str, where_params: list, fields: Dict[str, Any]
) -> tuple[str, tuple]:
    builder = UpdateQueryBuilder()
    builder.add_field("status", to)
    for column, value in fields.items():
        builder.add_field(column, value)
    return builder.build(
        table="automation_executions",
        where_clause=where_clause,
        where_params=where_params,
        returning_columns=["conversation_thread_id", "conversation_response_id"],
        include_updated_at=False,  # no updated_at column on executions
    )


async def transition_execution(
    execution_id: str,
    *,
    from_statuses: Sequence[str],
    to: str,
    automation_id: Optional[str] = None,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """Move an execution to ``to`` only while its status is one of
    ``from_statuses``; the row's thread and run, or None when it was not.

    The guarded write every move of a live firing makes, so the executor, a
    skip and the sweep can race and exactly one of them wins. Ending a firing
    is ``settle_execution``. A field passed as None leaves its column as it
    is.
    """
    where_clause = "automation_execution_id = %s AND status = ANY(%s)"
    where_params: list = [execution_id, list(from_statuses)]
    if automation_id is not None:
        where_clause += " AND automation_id = %s"
        where_params.append(automation_id)
    query, params = _execution_update(to, where_clause, where_params, fields)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return dict(row) if row else None


# What a settled firing leaves on its automation. A close only lands while the
# automation is still on the firing being settled: a reschedule, pause or
# disable made during the run wins, and a manual run never closes anything.
# The firing that claimed the automation is recognised by the stamp the claim
# shares with it (see claim_due_automations and claim_price_firing). A price
# alert with no stamp was claimed before claims wrote one, and only the
# 'executing' status its trigger set can speak for it.
_SCHEDULE_SQL = {
    "close_once": """
        UPDATE automations SET status = 'completed', next_run_at = NULL
        WHERE automation_id = %s AND status = 'active'
          AND next_run_at IS NULL AND last_run_at = %s
    """,
    "close_price": """
        UPDATE automations SET status = 'completed', next_run_at = NULL
        WHERE automation_id = %s AND status = 'executing'
          AND (last_run_at = %s OR last_run_at IS NULL)
    """,
    "rearm_price": """
        UPDATE automations SET status = 'active'
        WHERE automation_id = %s AND status = 'executing'
          AND (last_run_at = %s OR last_run_at IS NULL)
    """,
}

ScheduleAction = Literal["close_once", "close_price", "rearm_price"]


async def settle_execution(
    execution_id: str,
    *,
    automation_id: str,
    from_statuses: Sequence[str],
    to: str,
    strike: Optional[Literal["count", "fuse", "reset"]] = None,
    schedule: Optional[ScheduleAction] = None,
    quiet_for: Optional[int] = None,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """End a firing and write what it leaves on its automation, in one
    transaction; None when the firing was not in ``from_statuses``.

    One transaction, so a failure between the two can never leave a price
    alert 'executing' behind a settled firing. ``quiet_for`` settles only a
    firing whose heartbeat has been quiet that many seconds, which is what
    keeps the sweep off a firing whose process came back. Returns the row's
    thread and run, ``settled_from``, the status it left, and the
    ``previous_failure_reason`` and ``previous_delivery_result`` of the
    automation's newest firing settled before this one, a server skip aside,
    so a caller can tell a repeat the channel already heard from news.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Automation first, execution second: the order a delete's
                # cascade takes the same two rows in.
                await cur.execute("""
                    SELECT 1 FROM automations WHERE automation_id = %s
                    FOR NO KEY UPDATE
                """, (automation_id,))
                if await cur.fetchone() is None:
                    return None
                quiet = ""
                params: list = [execution_id, automation_id]
                if quiet_for is not None:
                    quiet = "AND heartbeat_at < NOW() - make_interval(secs => %s)"
                    params.append(quiet_for)
                await cur.execute(f"""
                    SELECT status, created_at FROM automation_executions
                    WHERE automation_execution_id = %s AND automation_id = %s
                      {quiet}
                    FOR UPDATE
                """, tuple(params))
                prior = await cur.fetchone()
                if prior is None or prior["status"] not in from_statuses:
                    return None

                await cur.execute(f"""
                    SELECT p.failure_reason, p.delivery_result
                    FROM automation_executions p
                    WHERE p.automation_id = %s AND p.automation_execution_id <> %s
                      AND p.status NOT IN {_UNSETTLED}
                      AND {_not_a_server_skip("p")}
                    ORDER BY p.created_at DESC, p.automation_execution_id DESC
                    LIMIT 1
                """, (automation_id, execution_id))
                previous = await cur.fetchone()

                query, update_params = _execution_update(
                    to, "automation_execution_id = %s", [execution_id], fields
                )
                await cur.execute(query, update_params)
                row = dict(await cur.fetchone())

                if strike in ("count", "fuse"):
                    await _count_strike(cur, automation_id, fuse=strike == "fuse")
                elif strike == "reset":
                    # A disabled automation keeps its count and reason until it
                    # is resumed: a firing that was already running when
                    # another one disabled it does not explain the disable away.
                    await cur.execute("""
                        UPDATE automations
                        SET failure_count = CASE WHEN status = 'disabled'
                                                 THEN failure_count ELSE 0 END,
                            disable_reason = CASE WHEN status = 'disabled'
                                                  THEN disable_reason END
                        WHERE automation_id = %s
                    """, (automation_id,))
                # After the strike, which may have disabled the automation.
                if schedule is not None:
                    await cur.execute(
                        _SCHEDULE_SQL[schedule], (automation_id, prior["created_at"])
                    )
                return {
                    **row,
                    "settled_from": prior["status"],
                    "previous_failure_reason": (
                        previous["failure_reason"] if previous else None
                    ),
                    "previous_delivery_result": (
                        previous["delivery_result"] if previous else None
                    ),
                }


async def record_delivery(execution_id: str, delivery_result: list) -> None:
    """Record what the webhook delivery returned, whatever the status."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automation_executions SET delivery_result = %s
                WHERE automation_execution_id = %s
            """, (Json(delivery_result), execution_id))


async def list_executions(
    user_id: str,
    *,
    automation_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[List[Dict[str, Any]], bool]:
    """A page of a user's executions newest first, narrowed to one
    automation, thread or status when given, and whether more follow it.

    Every row is scoped by its automation's owner, which is the ownership
    check: another user's automation lists nothing. ``workspace_id`` prefers
    the run thread's workspace: a flash automation stores none and runs in
    the user's shared flash workspace. One row past the page answers whether
    another follows, where a count would read every run the user has.
    """
    where_parts = ["a.user_id = %s"]
    params: list = [user_id]
    for column, value in (
        ("e.automation_id", automation_id),
        ("e.conversation_thread_id", thread_id),
        ("e.status", status),
    ):
        if value:
            where_parts.append(f"{column} = %s")
            params.append(value)
    where_clause = " AND ".join(where_parts)

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT
                    {EXECUTION_COLUMNS},
                    a.name AS automation_name, a.agent_mode, a.trigger_type,
                    COALESCE(t.workspace_id, a.workspace_id) AS workspace_id
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                LEFT JOIN conversation_threads t
                    ON t.conversation_thread_id = e.conversation_thread_id
                WHERE {where_clause}
                ORDER BY e.created_at DESC, e.automation_execution_id DESC
                LIMIT %s OFFSET %s
            """, (*params, limit + 1, offset))

            rows = [dict(row) for row in await cur.fetchall()]
            return rows[:limit], len(rows) > limit


async def count_executions(automation_id: str) -> int:
    """How many runs an automation has: one index range, unlike a count
    across every automation of a user."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT COUNT(*) FROM automation_executions
                WHERE automation_id = %s
            """, (automation_id,))
            return (await cur.fetchone())[0]


# =============================================================================
# Waiting for a busy thread
# =============================================================================
#
# An execution whose thread already has a turn running waits for it to end,
# rather than steering into it.


async def has_earlier_waiting_execution(
    automation_id: str, execution_id: str
) -> bool:
    """Whether a firing of this automation that came before this one is
    waiting too.

    Strictly earlier, by creation then id, so of two firings that start
    waiting together exactly one gives way.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 1
                FROM automation_executions w
                JOIN automation_executions me
                  ON me.automation_execution_id = %s
                WHERE w.automation_id = %s AND w.status = 'waiting'
                  AND (w.created_at, w.automation_execution_id)
                      < (me.created_at, me.automation_execution_id)
                LIMIT 1
            """, (execution_id, automation_id))
            return await cur.fetchone() is not None


async def get_execution_status(
    execution_id: str, *, automation_id: Optional[str] = None
) -> Optional[str]:
    """The execution's status; None when there is none, or none of
    ``automation_id``'s when that is given."""
    query = """
        SELECT status FROM automation_executions
        WHERE automation_execution_id = %s
    """
    params: tuple = (execution_id,)
    if automation_id is not None:
        query += " AND automation_id = %s"
        params += (automation_id,)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row["status"] if row else None


# =============================================================================
# Firings whose process died
# =============================================================================
#
# The process holding a firing touches ``heartbeat_at`` while the firing is
# pending, waiting or running. The row, not a process id, says whether anyone
# still holds it: every restart gets a fresh process, so an id recorded by the
# old one is never asked about again.


async def touch_execution(execution_id: str) -> None:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions SET heartbeat_at = NOW()
                WHERE automation_execution_id = %s
                  AND status IN {_UNSETTLED}
            """, (execution_id,))


async def settle_legacy_executions(error_message: str) -> int:
    """Close firings a build before the heartbeat left unsettled; how many.

    A NULL heartbeat is a row that predates the column. A process on the
    previous build may still be running it through a deploy, so it gets a
    day. After that nothing is waiting on it, and it is closed as a row write
    alone: a failure notice weeks late, or reviving whatever it left on its
    automation, would do more harm than the stale row.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions
                SET status = CASE WHEN status = 'waiting'
                                  THEN 'skipped' ELSE 'failed' END,
                    skip_reason = CASE WHEN status = 'waiting'
                                       THEN 'interrupted' END,
                    error_message = CASE WHEN status = 'waiting'
                                         THEN error_message ELSE %s END,
                    completed_at = NOW()
                WHERE status IN {_UNSETTLED}
                  AND heartbeat_at IS NULL
                  AND created_at < NOW() - INTERVAL '1 day'
            """, (error_message,))
            return cur.rowcount


async def list_abandoned_executions(
    quiet_seconds: int, limit: int = 50
) -> List[Dict[str, Any]]:
    """Firings whose heartbeat has been quiet ``quiet_seconds``, oldest first,
    with what settling each needs: its owner, thread, workspace and run.

    A firing whose run is still going is left out: the sweep would leave it
    alone anyway, and a page of long turns would starve the rest.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT e.automation_execution_id, e.automation_id, e.status,
                       e.conversation_thread_id, e.conversation_response_id,
                       a.user_id, t.workspace_id
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                LEFT JOIN conversation_threads t
                    ON t.conversation_thread_id = e.conversation_thread_id
                WHERE e.status IN {_UNSETTLED}
                  AND e.heartbeat_at < NOW() - make_interval(secs => %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses r
                      WHERE r.conversation_response_id = e.conversation_response_id
                        AND r.status = 'in_progress'
                  )
                ORDER BY e.heartbeat_at
                LIMIT %s
            """, (quiet_seconds, limit))
            return [dict(row) for row in await cur.fetchall()]


async def get_settling_run(run_id: str) -> Optional[Dict[str, Any]]:
    """The columns of a run's ledger row that settling its firing reads.

    ``sse_events`` is the turn's whole event archive, which can run to
    megabytes, so it is read only for a credit pause, whose interrupt
    carries the denial the user is told.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT conversation_response_id, status, interrupt_reason,
                       metadata, errors,
                       CASE WHEN interrupt_reason = %s THEN sse_events END
                           AS sse_events
                FROM conversation_responses
                WHERE conversation_response_id = %s
            """, (INTERRUPT_REASON_CREDIT_PAUSE, run_id))
            row = await cur.fetchone()
            return dict(row) if row else None


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
                    status, scheduled_at, server_id, created_at,
                    heartbeat_at
                )
                VALUES (%s, %s, 'pending', %s, %s, NOW(), NOW())
            """, (execution_id, automation_id, scheduled_at, server_id))
    return execution_id
