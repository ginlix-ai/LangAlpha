"""The run ledger: one row per interval a metered computer runs (migration 088).

A run is opened and closed by the status writes in ``database/computer.py``
and ``database/workspace.py`` through ``services/computer_usage``, which keeps
every statement here off the lifecycle's failure path. Each statement guards
on the computer's committed status, so a hook that lands late, or for a write
that never reached the computer, changes nothing it should not; the sweep's
reconcile repairs what a lost hook left.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

import psycopg.pq
from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

# The statuses in which the sandbox may be up and billing, so a run spans
# them. A stop backs up the machine's files while the row reads 'stopping',
# and one that aborts goes back to 'running'. A running machine claimed for a
# replacement reads 'starting' while its old sandbox is backed up and torn
# down, and may be released back to 'running'; its run ends when the new
# sandbox binds (``open_computer_run``). A claim from 'stopped' has no run.
RUN_LIVE_STATUSES = ("running", "stopping", "starting")

# What a run hook's statements may cost the status write it follows, on a
# connection the hook set the limits of.
_HOOK_STATEMENT_TIMEOUT = "2s"
_HOOK_LOCK_TIMEOUT = "1s"

_RUN_COLS = """
    run_id, computer_id, user_id, provider_kind, provider_sandbox_id,
    resource_tier, cpu, memory_gb, disk_gb, started_at, ended_at, end_reason,
    sample_count, cpu_avg_pct, cpu_p95_pct, cpu_max_pct, cpu_pct_histogram,
    mem_peak_bytes, mem_peak_excl_cache_bytes, mem_total_bytes,
    disk_peak_bytes, disk_total_bytes, metrics_through, metrics_error,
    metrics_polled_at, metrics_final, oom_kills
"""


@asynccontextmanager
async def hook_cursor(conn=None):
    """A cursor whose statements cannot hold the status write up for long.

    On a connection already inside the caller's transaction the hook runs in
    a savepoint, so a failure here rolls back only the hook; it sets no
    timeouts there, since SET LOCAL would outlive the savepoint into the
    caller's transaction, and runs under the caller's own limits instead.
    """
    async with get_db_connection(conn) as owned:
        in_caller_txn = (
            owned.info.transaction_status != psycopg.pq.TransactionStatus.IDLE
        )
        async with owned.transaction():
            async with owned.cursor(row_factory=dict_row) as cur:
                if not in_caller_txn:
                    await cur.execute(
                        f"SET LOCAL statement_timeout = '{_HOOK_STATEMENT_TIMEOUT}'"
                    )
                    await cur.execute(f"SET LOCAL lock_timeout = '{_HOOK_LOCK_TIMEOUT}'")
                yield cur


async def get_computer_for_run(cur, computer_id: str) -> Optional[Dict[str, Any]]:
    """The fields a run is opened from, read on the hook's cursor."""
    await cur.execute(
        "SELECT computer_id, kind, provider_config, resource_tier, status"
        " FROM computers WHERE computer_id = %s",
        (computer_id,),
    )
    return await cur.fetchone()

_METRIC_COLUMNS = (
    "sample_count",
    "cpu_avg_pct",
    "cpu_p95_pct",
    "cpu_max_pct",
    "cpu_pct_histogram",
    "mem_peak_bytes",
    "mem_peak_excl_cache_bytes",
    "mem_total_bytes",
    "disk_peak_bytes",
    "disk_total_bytes",
    "metrics_through",
)


def _live() -> str:
    return ", ".join(f"'{s}'" for s in RUN_LIVE_STATUSES)


async def open_computer_run(
    cur,
    computer_id: str,
    *,
    metered_kinds: Sequence[str],
    cpu: Optional[float],
    memory_gb: Optional[float],
    disk_gb: Optional[float],
    metrics_final: bool,
) -> None:
    """Make the computer's open run the one for the sandbox it runs now.

    An open run on another sandbox, or one the computer stopped during (its
    close was lost), is closed first, at the stop where there was one. The
    insert reads the computer row it guards on, so a write that has already
    moved the machine out of 'running' opens nothing. An open run that
    survives is kept, with any reason a stop stamped on it cleared, since
    that stop did not happen.
    """
    params = {
        "computer_id": computer_id,
        "metered": list(metered_kinds),
        "cpu": cpu,
        "memory_gb": memory_gb,
        "disk_gb": disk_gb,
        "metrics_final": metrics_final,
    }
    await cur.execute(
        """
        UPDATE computer_runs r
        SET ended_at = CASE
                WHEN c.stopped_at > r.started_at THEN c.stopped_at
                ELSE GREATEST(NOW(), r.started_at)
            END,
            end_reason = CASE
                WHEN c.stopped_at > r.started_at
                    THEN COALESCE(r.end_reason, 'reconciled')
                ELSE 'replaced'
            END
        FROM computers c
        WHERE r.computer_id = %(computer_id)s
          AND r.ended_at IS NULL
          AND c.computer_id = r.computer_id
          AND c.status = 'running'
          AND (r.provider_sandbox_id IS DISTINCT FROM c.provider_ref
               OR c.stopped_at > r.started_at)
        """,
        params,
    )
    await cur.execute(
        """
        INSERT INTO computer_runs (
            computer_id, user_id, provider_kind, provider_sandbox_id,
            resource_tier, cpu, memory_gb, disk_gb, metrics_final
        )
        SELECT c.computer_id, c.user_id, c.kind, c.provider_ref,
               c.resource_tier, %(cpu)s, %(memory_gb)s, %(disk_gb)s,
               %(metrics_final)s
        FROM computers c
        WHERE c.computer_id = %(computer_id)s
          AND c.status = 'running'
          AND c.kind = ANY(%(metered)s)
        ON CONFLICT (computer_id) WHERE ended_at IS NULL
        DO UPDATE SET end_reason = NULL
        WHERE computer_runs.end_reason IS NOT NULL
        """,
        params,
    )


async def stamp_run_end_reason(cur, computer_id: str, reason: str) -> None:
    """Say why the open run is ending while the stop that ends it runs.

    Only while the computer reads 'stopping', so a stamp landing after the
    stop finished, or was aborted, says nothing about the run.
    """
    await cur.execute(
        """
        UPDATE computer_runs r SET end_reason = %s
        FROM computers c
        WHERE r.computer_id = %s AND r.ended_at IS NULL
          AND c.computer_id = r.computer_id AND c.status = 'stopping'
        """,
        (reason, computer_id),
    )


async def close_computer_run(
    cur,
    computer_id: str,
    *,
    reason: str,
    pausing_kinds: Sequence[str] = (),
) -> None:
    """Close the computer's open run, if the computer has left running.

    Guarded on the committed status rather than on the write that called it:
    a workspace mirror that never reached the computer, or a close that lands
    after the machine was started again, ends nothing. A reason the stop
    stamped wins over a plain stop's; an error or a delete wins over it,
    since the stop it named did not finish as one. A plain stop on a kind
    whose stop is a pause reads ``paused``. It ends at the computer's last
    stop when that came after the run started, which a close for a later
    status (a delete after a stop whose own hook was lost) must not bill past.
    """
    await cur.execute(
        f"""
        UPDATE computer_runs r
        SET ended_at = CASE
                WHEN c.stopped_at > r.started_at THEN c.stopped_at
                ELSE GREATEST(NOW(), r.started_at)
            END,
            end_reason = CASE
                WHEN %(reason)s IN ('error', 'deleted') THEN %(reason)s
                ELSE COALESCE(
                    r.end_reason,
                    CASE WHEN %(reason)s = 'stopped'
                              AND r.provider_kind = ANY(%(pausing)s)
                         THEN 'paused' ELSE %(reason)s END
                )
            END
        FROM computers c
        WHERE r.computer_id = %(computer_id)s
          AND r.ended_at IS NULL
          AND c.computer_id = r.computer_id
          AND c.status NOT IN ({_live()})
        """,
        {
            "computer_id": computer_id,
            "reason": reason,
            "pausing": list(pausing_kinds),
        },
    )


async def close_replaced_run(cur, computer_id: str, sandbox_id: str) -> None:
    """End the open run on a sandbox a replacement has deleted, at the delete.

    Not guarded on status: the machine reads 'starting' until the new
    sandbox binds, and the deleted one runs for none of that.
    """
    await cur.execute(
        """
        UPDATE computer_runs
        SET ended_at = GREATEST(NOW(), started_at), end_reason = 'replaced'
        WHERE computer_id = %s AND provider_sandbox_id = %s AND ended_at IS NULL
        """,
        (computer_id, sandbox_id),
    )


async def close_orphaned_runs(*, conn=None) -> int:
    """Close every open run whose computer has left running with no hook to say so.

    At the computer's last stop when that came after the run started, whatever
    status it has moved on to since, else now; either way ``reconciled``
    unless a stop stamped its own reason.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE computer_runs r
                SET ended_at = CASE
                        WHEN c.stopped_at >= r.started_at THEN c.stopped_at
                        ELSE GREATEST(NOW(), r.started_at)
                    END,
                    end_reason = COALESCE(r.end_reason, 'reconciled')
                FROM computers c
                WHERE r.ended_at IS NULL
                  AND c.computer_id = r.computer_id
                  AND c.status NOT IN ({_live()})
                """
            )
            return cur.rowcount


async def get_running_computers_without_run(
    metered_kinds: Sequence[str], *, limit: int = 1000, conn=None
) -> List[Dict[str, Any]]:
    """Running metered computers with no open run of their current sandbox.

    What reconcile opens one for. An open run left on a sandbox the machine
    has since replaced, or across a stop since, is not current: a lost hook
    leaves it so, and ``open_computer_run`` closes it before opening anew.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT c.computer_id, c.user_id, c.kind, c.provider_ref,
                       c.resource_tier, c.provider_config
                FROM computers c
                WHERE c.status = 'running'
                  AND c.kind = ANY(%s)
                  AND NOT EXISTS (
                      SELECT 1 FROM computer_runs r
                      WHERE r.computer_id = c.computer_id
                        AND r.ended_at IS NULL
                        AND r.provider_sandbox_id IS NOT DISTINCT FROM c.provider_ref
                        AND (c.stopped_at IS NULL OR c.stopped_at <= r.started_at)
                  )
                LIMIT %s
                """,
                (list(metered_kinds), limit),
            )
            return [dict(r) for r in await cur.fetchall()]


async def claim_runs_for_metrics(
    *,
    kinds: Sequence[str],
    poll_every_seconds: float,
    limit: int,
    settle_seconds: float = 0,
    computer_id: Optional[str] = None,
    conn=None,
) -> List[Dict[str, Any]]:
    """Claim runs whose metrics are due, so one worker reads each per cycle.

    Due is not yet given its last fold and not polled within
    ``poll_every_seconds``; a closed run is due only once it ended
    ``settle_seconds`` ago, when the provider holds all of its samples. The
    claim is a stamp, not a lock: a worker that dies holding it only delays
    the run to the next cycle. Naming a computer claims every unfinished run
    of it whenever it was last polled, for the read a delete asks for.
    """
    scope = (
        "AND computer_id = %(computer_id)s"
        if computer_id is not None
        else "AND (metrics_polled_at IS NULL OR metrics_polled_at"
        " < NOW() - make_interval(secs => %(every)s))"
        " AND (ended_at IS NULL OR ended_at"
        " < NOW() - make_interval(secs => %(settle)s))"
    )
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH due AS (
                    SELECT run_id FROM computer_runs
                    WHERE NOT metrics_final
                      AND provider_kind = ANY(%(kinds)s)
                      {scope}
                    ORDER BY metrics_polled_at NULLS FIRST
                    LIMIT %(limit)s
                    FOR UPDATE SKIP LOCKED
                ),
                claimed AS (
                    UPDATE computer_runs r
                    SET metrics_polled_at = NOW()
                    FROM due
                    WHERE r.run_id = due.run_id
                    RETURNING r.*
                )
                SELECT claimed.*, c.provider_config, c.root_dir
                FROM claimed
                JOIN computers c ON c.computer_id = claimed.computer_id
                """,
                {
                    "kinds": list(kinds),
                    "every": poll_every_seconds,
                    "settle": settle_seconds,
                    "limit": limit,
                    "computer_id": computer_id,
                },
            )
            return [dict(r) for r in await cur.fetchall()]


async def get_computer_run(run_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    """The run's row as it stands, for a fold that lost its cursor race."""
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_RUN_COLS} FROM computer_runs WHERE run_id = %s", (run_id,)
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def store_run_metrics(
    run_id: str,
    *,
    expected_through: Optional[datetime],
    aggregates: Mapping[str, Any],
    final: bool,
    conn=None,
) -> bool:
    """Write a fold, unless another fold moved the cursor since this one read it.

    The cursor check is what keeps two folds of one run from counting the
    same samples twice. A success clears the last error.
    """
    sets = ", ".join(f"{col} = %({col})s" for col in _METRIC_COLUMNS)
    params = {col: aggregates.get(col) for col in _METRIC_COLUMNS}
    params.update(run_id=run_id, expected=expected_through, final=final)
    async with get_db_connection(conn) as owned:
        async with owned.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE computer_runs
                SET {sets},
                    metrics_error = NULL,
                    metrics_final = metrics_final OR %(final)s
                WHERE run_id = %(run_id)s
                  AND metrics_through IS NOT DISTINCT FROM %(expected)s
                """,
                params,
            )
            return cur.rowcount > 0


async def store_run_metrics_error(
    run_id: str, error: str, *, final: bool, conn=None
) -> None:
    """Record why a fold failed; the aggregates and cursor stay as they were."""
    async with get_db_connection(conn) as owned:
        await owned.execute(
            """
            UPDATE computer_runs
            SET metrics_error = %s, metrics_final = metrics_final OR %s
            WHERE run_id = %s
            """,
            (error, final, run_id),
        )


async def record_oom_kill(
    provider_kind: str, sandbox_id: str, *, conn=None
) -> bool:
    """Count a SIGKILLed command against the open run on that sandbox."""
    async with get_db_connection(conn) as owned:
        async with owned.cursor() as cur:
            await cur.execute(
                """
                UPDATE computer_runs SET oom_kills = oom_kills + 1
                WHERE provider_kind = %s
                  AND provider_sandbox_id = %s
                  AND ended_at IS NULL
                """,
                (provider_kind, sandbox_id),
            )
            return cur.rowcount > 0
