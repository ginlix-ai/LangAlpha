"""Execution history queries for automations: transitions, settlement, delivery and the run feed."""

import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Sequence
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.automation import (
    EXECUTION_COLUMNS,
    UNSETTLED_STATUSES,
    not_a_server_skip,
)
from src.server.contracts.status import RESTART_INTERRUPTED_META, RESTART_RESUME_TTL
from src.server.database.pool import get_db_connection
from src.server.database.ssh_tunnels import awaited_computer_back_sql
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
        returning_columns=[
            "conversation_thread_id", "conversation_response_id",
            "scheduled_at", "wait_reason", "awaits_computer",
        ],
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
    ``from_statuses``; the row's thread and run, when it was due and what it
    last waited for, or None when it was not.

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
    in_transaction: Optional[Callable[[Any, Dict[str, Any]], Awaitable[Any]]] = None,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """End a firing and write what it leaves on its automation, in one
    transaction; None when the firing was not in ``from_statuses``.

    One transaction, so a failure between the two can never leave a price
    alert 'executing' behind a settled firing. ``quiet_for`` settles only a
    firing whose heartbeat has been quiet that many seconds, which is what
    keeps the sweep off a firing whose process came back. Returns the row's
    thread and run, ``settled_from``, the status it left, the ``delivery``
    its start kept (``stamp_delivery``), and the ``previous_failure_reason``
    and ``previous_delivery_result`` of the automation's newest firing
    settled before this one, a server skip aside, so a caller can tell a
    repeat the channel already heard from news. ``in_transaction`` is
    awaited with the connection and that row before the commit, for work
    that must commit with the settle or not at all.
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
                    SELECT status, created_at, delivery
                    FROM automation_executions
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
                settled = {
                    **row,
                    "settled_from": prior["status"],
                    "delivery": prior["delivery"],
                    "previous_failure_reason": (
                        previous["failure_reason"] if previous else None
                    ),
                    "previous_delivery_result": (
                        previous["delivery_result"] if previous else None
                    ),
                }
            if in_transaction is not None:
                await in_transaction(conn, settled)
            return settled


async def stamp_delivery(execution_id: str, stamp: Optional[Dict[str, Any]]) -> bool:
    """Keep how the firing's start left its delivery in its ``delivery``
    column, or clear it with None; False once the firing settled.

    Its turn's run row carries the same stamp only from START, and a process
    that dies before then leaves the sweep this row alone to settle by: the
    settle reads the column under its lock (``settle_execution``). Only an
    unsettled firing is written, so a settled one keeps the start its settle
    went by.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions SET delivery = %s
                WHERE automation_execution_id = %s
                  AND status IN {UNSETTLED_STATUSES}
            """, (Json(stamp) if stamp is not None else None, execution_id))
            return cur.rowcount > 0


async def record_delivery(execution_id: str, delivery_result: list, *, conn=None) -> None:
    """Record what the webhook delivery returned, whatever the status.
    ``conn`` joins the caller's transaction."""
    async with get_db_connection(conn) as conn:
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
    the run thread's workspace, where the run happened, over the one the
    automation names now, which a move may have changed since. One row past
    the page answers whether another follows, where a count would read every
    run the user has.
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
                    a.name AS automation_name, a.trigger_type,
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
    waiting for the thread too.

    Strictly earlier, by creation then id, so of two firings that start
    waiting together exactly one gives way. A firing parked for its computer
    is not in this line: this one takes its wait over (``_SUPERSEDED``), so
    giving way to it would skip both.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                SELECT 1
                FROM automation_executions w
                JOIN automation_executions me
                  ON me.automation_execution_id = %s
                WHERE w.automation_id = %s AND w.status = 'waiting'
                  AND NOT {_PARKED.format(e="w")}
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
    """Beat a firing's heartbeat, never a parked one's: a touch already queued
    behind the park would otherwise commit after it and undo the heartbeat
    the park dated to its deadline."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions SET heartbeat_at = NOW()
                WHERE automation_execution_id = %s
                  AND status IN {UNSETTLED_STATUSES}
                  AND NOT {_PARKED.format(e="automation_executions")}
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

    A firing whose run is still going, or still owed a restart's resume, is
    left out: the sweep would leave it alone anyway, and a page of them would
    starve the rest.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT e.automation_execution_id, e.automation_id, e.status, e.wait_reason,
                       e.conversation_thread_id, e.conversation_response_id,
                       a.user_id, t.workspace_id
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                LEFT JOIN conversation_threads t
                    ON t.conversation_thread_id = e.conversation_thread_id
                WHERE e.status IN {UNSETTLED_STATUSES}
                  AND e.heartbeat_at < NOW() - make_interval(secs => %s)
                  AND NOT {_PARKED.format(e="e")}
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses r
                      WHERE r.conversation_response_id = e.conversation_response_id
                        AND (r.status = 'in_progress'
                             OR (r.status = 'cancelled' AND r.metadata ? %s
                                 AND r.cancel_requested_at > NOW() - %s))
                  )
                ORDER BY e.heartbeat_at
                LIMIT %s
            """, (quiet_seconds, RESTART_INTERRUPTED_META, RESTART_RESUME_TTL, limit))
            return [dict(row) for row in await cur.fetchall()]


# =============================================================================
# Waiting for a computer
# =============================================================================
#
# A firing whose computer cannot be reached is parked as ``waiting`` naming
# that computer (``awaits_computer``), and no process holds it: the
# scheduler's sweep starts it again once the computer's tunnel is live, or
# gives it up. Its heartbeat is quiet meanwhile, so the abandoned-firing sweep
# passes it over.

# Never NULL, so ``NOT`` of it holds for a row with no ``wait_reason``.
_PARKED = (
    "({e}.status = 'waiting'"
    " AND {e}.wait_reason IS NOT DISTINCT FROM 'computer_unreachable')"
)

# A firing of the same automation came after ``e``, whatever became of it: it
# takes the wait over, so a schedule's missed runs come back as one run.
_SUPERSEDED = """EXISTS (
    SELECT 1 FROM automation_executions w
    WHERE w.automation_id = e.automation_id
      AND (w.created_at, w.automation_execution_id)
          > (e.created_at, e.automation_execution_id))"""

# Its computer is back, or its automation's workspace is on another one now.
_REACHABLE = awaited_computer_back_sql(
    "e.awaits_computer::text",
    "(SELECT m.workspace_id FROM automations m WHERE m.automation_id = e.automation_id)",
)
_EXPIRED = "e.awaits_until < NOW()"


async def park_execution(
    execution_id: str,
    *,
    awaits_computer: str,
    parked_status: str,
    wait_seconds: int,
    checked_computer_id: Optional[str] = None,
) -> bool:
    """Park a pending or running firing as ``waiting`` for ``awaits_computer``;
    False when it had moved on.

    The wait's deadline is set by the first park and kept by every later
    one, so a machine that comes back only to drop again does not start the
    wait over. The heartbeat is dated to that deadline: nothing touches the
    row while it waits, and a build that predates parking (the other color
    through a cutover) must not take the quiet heartbeat for a dead process.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE automation_executions
                SET status = 'waiting', started_at = NOW(),
                    wait_reason = 'computer_unreachable',
                    awaits_computer = %(computer)s, parked_status = %(parked_status)s,
                    checked_computer_id = COALESCE(%(checked)s, checked_computer_id),
                    awaits_until = COALESCE(
                        awaits_until, NOW() + make_interval(secs => %(wait_s)s)),
                    heartbeat_at = COALESCE(
                        awaits_until, NOW() + make_interval(secs => %(wait_s)s))
                WHERE automation_execution_id = %(execution_id)s
                  AND status IN ('pending', 'running')
            """, {
                "execution_id": execution_id, "computer": awaits_computer,
                "parked_status": parked_status, "checked": checked_computer_id,
                "wait_s": wait_seconds,
            })
            return cur.rowcount == 1


async def list_parked_executions(limit: int = 50) -> List[Dict[str, Any]]:
    """Firings parked for a computer that the sweep has something to do with,
    soonest deadline first, with their owner, the automation's status when
    each parked, the computer a computer token's run was checked to run on,
    and the sweep's verdict on each: ``superseded`` by a later firing,
    ``reachable`` (its computer's tunnel is live, it names none, or its
    workspace is on another computer now) or ``expired`` (past its
    ``awaits_until``).

    Rows with none of those are left out before the limit: a page of
    machines asleep for the night must not starve one that is back.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT e.automation_execution_id, e.automation_id, e.parked_status,
                       e.checked_computer_id::text AS checked_computer_id, a.user_id,
                       {_SUPERSEDED} AS superseded,
                       {_REACHABLE} AS reachable,
                       {_EXPIRED} AS expired
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                WHERE {_PARKED.format(e="e")}
                  AND ({_SUPERSEDED} OR {_REACHABLE} OR {_EXPIRED})
                ORDER BY e.awaits_until NULLS LAST
                LIMIT %(limit)s
            """, {"limit": limit})
            return [dict(row) for row in await cur.fetchall()]


async def release_parked_execution(execution_id: str) -> bool:
    """Put a parked firing back in line as ``pending`` for the ordinary start,
    once its computer is reachable or its workspace is on another one now;
    False when another worker took it, it moved on, a later firing took its
    wait over, its deadline passed since the sweep read it (the next sweep
    skips it), or the computer is away again.

    One guarded statement, so of the workers whose sweeps reach the row
    exactly one starts it. The heartbeat starts again with it, and the thread
    is let go: the run takes the automation's thread as it stands after the
    wait, as it would had it waited in process.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                UPDATE automation_executions e
                SET status = 'pending', heartbeat_at = NOW(),
                    conversation_thread_id = NULL
                WHERE e.automation_execution_id = %(execution_id)s
                  AND {_PARKED.format(e="e")}
                  AND NOT {_SUPERSEDED}
                  AND NOT COALESCE({_EXPIRED}, false)
                  AND {_REACHABLE}
            """, {"execution_id": execution_id})
            return cur.rowcount == 1


async def hand_off_execution(
    execution_id: str, *, from_statuses: Sequence[str], thread_id: str, job: Any
) -> bool:
    """Put a live firing back to ``pending`` on ``thread_id`` and write
    ``job``, which the next process starts it from, in one transaction;
    False when it was settled or moved first. The heartbeat is touched, so the
    sweep gives the next process its full margin."""
    from src.server.database.runs.outbox import enqueue_hooks

    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE automation_executions
                    SET status = 'pending', heartbeat_at = NOW(),
                        conversation_thread_id = %s
                    WHERE automation_execution_id = %s AND status = ANY(%s)
                """, (thread_id, execution_id, list(from_statuses)))
                if cur.rowcount != 1:
                    return False
            await enqueue_hooks(conn, thread_id=thread_id, jobs=[job])
    return True


async def get_running_execution_for_run(run_id: str) -> Optional[Dict[str, Any]]:
    """The running firing whose linked run is ``run_id``, with its owner,
    thread and workspace; None when there is none."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT e.automation_execution_id, e.automation_id,
                       e.conversation_thread_id, a.user_id, t.workspace_id
                FROM automation_executions e
                JOIN automations a ON a.automation_id = e.automation_id
                LEFT JOIN conversation_threads t
                    ON t.conversation_thread_id = e.conversation_thread_id
                WHERE e.conversation_response_id = %s AND e.status = 'running'
            """, (run_id,))
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_settling_run(run_id: str) -> Optional[Dict[str, Any]]:
    """The columns of a run's ledger row that settling its firing reads."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT conversation_response_id, status, interrupt_reason,
                       metadata, errors, cancel_requested_at
                FROM conversation_responses
                WHERE conversation_response_id = %s
            """, (run_id,))
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
