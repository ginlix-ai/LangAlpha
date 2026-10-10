"""Execution history queries for automations: transitions, settlement, delivery and the run feed."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Sequence
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
from src.server.database.automation import (
    EXECUTION_COLUMNS,
    UNSETTLED_STATUSES,
    not_a_server_skip,
)
from src.server.database.pool import get_db_connection
from src.server.utils.db import UpdateQueryBuilder

logger = logging.getLogger(__name__)


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
                      AND p.status NOT IN {UNSETTLED_STATUSES}
                      AND {not_a_server_skip("p")}
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


async def dismiss_execution(execution_id: str, *, automation_id: str) -> bool:
    """Mark a failed run of this automation dismissed. False when it has no
    such failed run. A second dismissal keeps the first one's time."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automation_executions
                SET dismissed_at = COALESCE(dismissed_at, NOW())
                WHERE automation_execution_id = %s AND automation_id = %s
                  AND status IN ('failed', 'timeout')
            """, (execution_id, automation_id))
            return cur.rowcount > 0


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


async def list_settled_since(
    user_id: str, since: datetime, *, limit: int = 10
) -> List[Dict[str, Any]]:
    """A user's runs that finished or failed at or after ``since``, newest first."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT {EXECUTION_COLUMNS}, a.name AS automation_name
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                WHERE a.user_id = %s AND e.completed_at >= %s
                  AND e.status IN ('completed', 'failed', 'timeout')
                ORDER BY e.completed_at DESC, e.automation_execution_id DESC
                LIMIT %s
            """, (user_id, since, limit))
            return [dict(row) for row in await cur.fetchall()]


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
                  AND status IN {UNSETTLED_STATUSES}
            """, (execution_id,))


async def settle_legacy_executions(error_message: str) -> int:
    """Close firings a build before the heartbeat left unsettled; how many.

    A NULL heartbeat is a row that predates the column. A process on the
    previous build may still be running it through a deploy, so it gets a
    day. After that nothing is waiting on it, and it is closed as a row write
    alone: a failure notice weeks late, or reviving whatever it left on its
    automation, would do more harm than the stale row. None of these rows is
    ``waiting``: that status came in with the heartbeat, and every firing
    that can wait was written with one.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions
                SET status = 'failed', failure_reason = 'interrupted',
                    error_message = %s, completed_at = NOW()
                WHERE status IN {UNSETTLED_STATUSES}
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
                WHERE e.status IN {UNSETTLED_STATUSES}
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
    megabytes, so it is read only for a credit pause settled before its
    finalize stamped the denial into ``metadata``: the pause's stored
    interrupt is then the one place that still has it.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT conversation_response_id, status, interrupt_reason,
                       metadata, errors,
                       CASE WHEN interrupt_reason = %s
                            AND NOT (COALESCE(metadata, '{}'::jsonb)
                                     ? 'credit_pause_message')
                       THEN sse_events END AS sse_events
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
