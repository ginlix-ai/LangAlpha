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
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

import psycopg.pq
from psycopg.rows import dict_row

from ptc_agent.core.sandbox.runtime import RuntimeState, SandboxAllocation

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
    metrics_polled_at, metrics_final, oom_kills, size_source, archive_after_s,
    held_until, held_end, allocation_checked_at, seen_down_at
"""

# When the run after this one on the same sandbox started, on a run aliased ``r``.
_NEXT_START = """(SELECT min(n.started_at) FROM computer_runs n
    WHERE n.provider_kind = r.provider_kind
      AND n.provider_sandbox_id = r.provider_sandbox_id
      AND n.started_at > r.started_at)"""
# A deleted computer's delete, which bumped its ``updated_at`` last.
_DELETED_AT = """(SELECT c.updated_at FROM computers c
    WHERE c.computer_id = r.computer_id AND c.status = 'deleted')"""

# The provider states a sandbox is billed as running in, and those that end
# the hold on a stopped sandbox's disk (migration 122).
_UP_STATES = frozenset({RuntimeState.RUNNING, RuntimeState.STARTING, RuntimeState.STOPPING})
_HOLD_ENDING_STATES = frozenset({RuntimeState.ARCHIVED, RuntimeState.ERROR})

_ON_SANDBOX = "r.provider_kind = %(kind)s AND r.provider_sandbox_id = %(sandbox)s"

# A closed run whose stop no answer read since has found the sandbox down:
# it may still be up, so no archive ends its hold and an answer finding it
# up moves its end.
_STOP_UNCONFIRMED = """(r.ended_at IS NOT NULL AND r.held_until IS NULL
    AND NOT COALESCE(r.seen_down_at > r.ended_at, FALSE))"""

# When the provider's answer was read, on the database's clock: as early as
# the lookup started, so it speaks only for runs that had ended by then and
# not for a stop or restart another worker made while it was in flight.
_ANSWERED_AT = "(NOW() - make_interval(secs => %(age)s))"

# An unconfirmed stop the sweep has not asked about since it was made.
_FRESH_STOP = f"""({_STOP_UNCONFIRMED} AND (r.allocation_checked_at IS NULL
    OR r.allocation_checked_at <= r.ended_at))"""

# Every lookup, answered or not, so a sandbox waits its turn behind the rest.
_STAMP_CHECKED = f"""
    UPDATE computer_runs r SET allocation_checked_at = NOW() WHERE {_ON_SANDBOX}
"""
# An answer that found the sandbox down, or gone, which confirms every stop
# made before it was read.
_SEEN_DOWN = f"""
    UPDATE computer_runs r SET seen_down_at = {_ANSWERED_AT}
    WHERE {_ON_SANDBOX}
      AND (r.seen_down_at IS NULL OR r.seen_down_at < {_ANSWERED_AT})
"""


async def _end_holds(
    cur,
    scope: str,
    params: Mapping[str, Any],
    *,
    ends_now: bool = False,
    why: Optional[str] = None,
    answer_age_s: float = 0.0,
) -> None:
    """End the disk hold of every closed run in *scope* that something has ended.

    A hold ends at the earliest of: the next run on the sandbox starting,
    the sandbox's archive once its interval has passed and a check since
    the stop found the sandbox down (or a later run began, after which no
    check can move this run's end), its computer's delete, the run's own
    end when a replacement deleted the sandbox, and, when *ends_now* (a
    provider check that found it archived, errored or gone, or a delete
    just confirmed, read as *why*), the moment that answer was read,
    *answer_age_s* ago, for the runs that had ended by then. Every one but
    that answer is a stored time, so a hold comes out the same whenever this runs; a run
    whose end another worker moved while this one waited for its row is
    left for the next pass. Only runs
    whose archive moment is known, their sandbox asked about; the rest wait
    for the sweep. *scope* is a condition on the run aliased ``r``.
    """
    await cur.execute(
        f"""
        WITH h AS (
            SELECT r.run_id, r.ended_at,
                   {_NEXT_START} AS next_start,
                   r.ended_at + make_interval(secs => r.archive_after_s) AS archive_at,
                   {_DELETED_AT} AS deleted_at,
                   CASE WHEN r.end_reason = 'replaced' THEN r.ended_at END
                       AS replaced_at,
                   NOT {_STOP_UNCONFIRMED} AS stop_confirmed,
                   {_ANSWERED_AT} AS answered_at
            FROM computer_runs r
            WHERE r.ended_at IS NOT NULL AND r.held_until IS NULL
              AND r.size_source IS NOT NULL AND ({scope})
        ), e AS (
            SELECT h.*, LEAST(
                next_start,
                CASE WHEN (stop_confirmed OR next_start IS NOT NULL)
                          AND archive_at <= NOW()
                     THEN archive_at END,
                deleted_at,
                replaced_at,
                CASE WHEN %(ends_now)s AND ended_at <= answered_at
                     THEN answered_at END
            ) AS end_at
            FROM h
        )
        UPDATE computer_runs r
        SET held_until = GREATEST(e.end_at, e.ended_at),
            held_end = CASE
                WHEN e.end_at = e.next_start THEN 'restarted'
                WHEN e.end_at = e.archive_at THEN 'archived'
                WHEN e.end_at IN (e.deleted_at, e.replaced_at) THEN 'deleted'
                ELSE %(why)s
            END
        FROM e
        WHERE r.run_id = e.run_id AND e.end_at IS NOT NULL
          AND r.held_until IS NULL AND r.ended_at = e.ended_at
        """,
        {**params, "ends_now": ends_now, "why": why, "age": answer_age_s},
    )


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
    provider_sized: bool,
) -> None:
    """Make the computer's open run the one for the sandbox it runs now.

    An open run on another sandbox, or one the computer stopped during (its
    close was lost), is closed first, at the stop where there was one. The
    insert reads the computer row it guards on, so a write that has already
    moved the machine out of 'running' opens nothing. An open run that
    survives is kept, with any reason a stop stamped on it cleared, since
    that stop did not happen. The new run takes the size the provider
    reported for its sandbox when an earlier run has it (*cpu*,
    *memory_gb* and *disk_gb* are the tier's, until the sweep asks). A kind
    the sweep never asks (not *provider_sized*) keeps the tier's, read as
    final, so its holds end on its own events.
    """
    params = {
        "computer_id": computer_id,
        "metered": list(metered_kinds),
        "cpu": cpu,
        "memory_gb": memory_gb,
        "disk_gb": disk_gb,
        "metrics_final": metrics_final,
        "provider_sized": provider_sized,
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
            resource_tier, cpu, memory_gb, disk_gb, metrics_final,
            size_source, archive_after_s
        )
        SELECT c.computer_id, c.user_id, c.kind, c.provider_ref,
               c.resource_tier,
               CASE WHEN k.size_source IS NULL THEN %(cpu)s ELSE k.cpu END,
               CASE WHEN k.size_source IS NULL THEN %(memory_gb)s ELSE k.memory_gb END,
               CASE WHEN k.size_source IS NULL THEN %(disk_gb)s ELSE k.disk_gb END,
               %(metrics_final)s,
               CASE WHEN %(provider_sized)s THEN k.size_source ELSE 'tier' END,
               k.archive_after_s
        FROM computers c
        LEFT JOIN LATERAL (
            SELECT p.cpu, p.memory_gb, p.disk_gb, p.size_source, p.archive_after_s
            FROM computer_runs p
            WHERE p.provider_kind = c.kind
              AND p.provider_sandbox_id = c.provider_ref
              AND p.size_source IN ('provider', 'partial')
            ORDER BY p.started_at DESC
            LIMIT 1
        ) k ON TRUE
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
    The hold on the disk after it is ended by the sweep (``settle_holds``),
    from times already stored, so a status write never waits on it.
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
    sandbox binds, and the deleted one runs for none of that. The delete
    ends the hold of a stop before it, too.
    """
    params = {"computer_id": computer_id, "sandbox": sandbox_id}
    await cur.execute(
        """
        UPDATE computer_runs
        SET ended_at = GREATEST(NOW(), started_at), end_reason = 'replaced'
        WHERE computer_id = %(computer_id)s AND provider_sandbox_id = %(sandbox)s
          AND ended_at IS NULL
        """,
        params,
    )
    # The delete's time is stored nowhere else. Its own savepoint, so a
    # conflict with the sweep over these rows leaves the close standing; the
    # sweep then ends the hold at its next check, which finds the sandbox gone.
    try:
        async with cur.connection.transaction():
            await _end_holds(
                cur,
                "r.computer_id = %(computer_id)s"
                " AND r.provider_sandbox_id = %(sandbox)s",
                params,
                ends_now=True,
                why="deleted",
            )
    except psycopg.Error as e:
        logger.warning(
            f"Disk hold on replaced sandbox {sandbox_id} left to the sweep: "
            f"{type(e).__name__}: {e}"
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
    ended_at: Optional[datetime],
    conn=None,
) -> bool:
    """Write a fold, unless another fold moved the cursor since this one read it.

    The cursor check is what keeps two folds of one run from counting the
    same samples twice. A success clears the last error. *final* holds only
    while the run still ends at *ended_at*, the end the fold read through,
    since a check that found the stop unfinished may have moved it since.
    """
    sets = ", ".join(f"{col} = %({col})s" for col in _METRIC_COLUMNS)
    params = {col: aggregates.get(col) for col in _METRIC_COLUMNS}
    params.update(
        run_id=run_id, expected=expected_through, final=final, ended_at=ended_at
    )
    async with get_db_connection(conn) as owned:
        async with owned.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE computer_runs
                SET {sets},
                    metrics_error = NULL,
                    metrics_final = metrics_final
                        OR (%(final)s AND ended_at IS NOT DISTINCT FROM %(ended_at)s)
                WHERE run_id = %(run_id)s
                  AND metrics_through IS NOT DISTINCT FROM %(expected)s
                """,
                params,
            )
            return cur.rowcount > 0


async def store_run_metrics_error(
    run_id: str,
    error: str,
    *,
    final: bool,
    ended_at: Optional[datetime],
    conn=None,
) -> None:
    """Record why a fold failed; the aggregates and cursor stay as they were.

    *final* holds only while the run still ends at *ended_at*, as for a fold.
    """
    async with get_db_connection(conn) as owned:
        await owned.execute(
            """
            UPDATE computer_runs
            SET metrics_error = %(error)s,
                metrics_final = metrics_final
                    OR (%(final)s AND ended_at IS NOT DISTINCT FROM %(ended_at)s)
            WHERE run_id = %(run_id)s
            """,
            {"error": error, "final": final, "ended_at": ended_at, "run_id": run_id},
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


async def settle_holds(*, conn=None) -> int:
    """End every hold a stored time has ended, with no provider call.

    A later run, the archive (Daytona archives a stopped sandbox when its
    interval runs out) once a check has found the sandbox down, a delete:
    what a hook could not end, its sandbox not yet asked about, or its own
    write lost, ends here.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor() as cur:
            await _end_holds(cur, "TRUE", {})
            return cur.rowcount


async def get_sandboxes_due_allocation(
    *,
    kinds: Sequence[str],
    size_every_seconds: float,
    held_every_seconds: float,
    limit: int,
    conn=None,
) -> List[Dict[str, Any]]:
    """The sandboxes the provider should be asked about, one row each.

    One whose runs have no reported size, or only part of it, and one whose
    last stop no answer has confirmed, at most every *size_every_seconds*;
    one a closed run may still hold the disk of, every *held_every_seconds*.
    A stop not yet asked about since it was made comes first, the newest
    first, so it is confirmed within a cycle; then the least recently asked,
    so lookups that keep failing take their turn rather than every batch. Each row carries its
    computer's ``provider_config`` and ``root_dir`` for building the provider.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (r.provider_kind, r.provider_sandbox_id)
                           r.provider_kind, r.provider_sandbox_id, r.computer_id,
                           r.allocation_checked_at, c.provider_config, c.root_dir,
                           {_FRESH_STOP} AS unconfirmed_stop,
                           CASE WHEN {_FRESH_STOP} THEN r.ended_at END
                               AS unconfirmed_since
                    FROM computer_runs r
                    JOIN computers c ON c.computer_id = r.computer_id
                    WHERE r.provider_sandbox_id IS NOT NULL
                      AND r.provider_kind = ANY(%(kinds)s)
                      AND (
                        ((r.size_source IS NULL OR r.size_source = 'partial')
                         AND (r.allocation_checked_at IS NULL OR r.allocation_checked_at
                              < NOW() - make_interval(secs => %(size_every)s)))
                        OR {_FRESH_STOP}
                        OR ({_STOP_UNCONFIRMED}
                         AND r.allocation_checked_at
                             < NOW() - make_interval(secs => %(size_every)s))
                        OR (r.ended_at IS NOT NULL AND r.held_until IS NULL
                         AND (r.allocation_checked_at IS NULL
                              OR r.allocation_checked_at
                                 < NOW() - make_interval(secs => %(held_every)s)))
                      )
                    ORDER BY r.provider_kind, r.provider_sandbox_id,
                             unconfirmed_since DESC NULLS LAST,
                             r.allocation_checked_at NULLS FIRST
                ) due
                ORDER BY unconfirmed_since DESC NULLS LAST,
                         allocation_checked_at NULLS FIRST
                LIMIT %(limit)s
                """,
                {
                    "kinds": list(kinds),
                    "size_every": size_every_seconds,
                    "held_every": held_every_seconds,
                    "limit": limit,
                },
            )
            return [dict(r) for r in await cur.fetchall()]


async def mark_allocation_checked(
    provider_kind: str, sandbox_id: str, *, conn=None
) -> None:
    """Stamp a sandbox the provider could not be asked about, so it waits its turn.

    Without it, sandboxes that fail every lookup would fill each cycle's
    batch ahead of the rest. A stop the check was to confirm stays
    unconfirmed, and is asked about again in its turn.
    """
    async with get_db_connection(conn) as owned:
        await owned.execute(
            _STAMP_CHECKED, {"kind": provider_kind, "sandbox": sandbox_id}
        )


async def store_allocation(
    provider_kind: str,
    sandbox_id: str,
    found: SandboxAllocation,
    *,
    asked_at: float,
    conn=None,
) -> None:
    """Write what the provider holds for a sandbox over the runs on it.

    Its size goes on every run on the sandbox, which is never resized; a
    size it leaves out keeps what the run had and reads ``partial``, asked
    for again until an answer has it. A sandbox still up after its last run
    closed, its stop refused or never finished, is billed as running, so
    that run lasts until a check finds it down, and its samples are read
    again for the time added; the sweep asks again each cycle meanwhile. A
    stop an answer has already found down is not undone by a later one
    finding the sandbox up. An archived or errored sandbox ends the hold its last stop left on the disk
    at this check, unless a stored time ended it first. *asked_at* is the
    ``time.monotonic()`` the lookup started at: the answer says nothing of
    a stop made after it.
    """
    sized = None not in (found.cpu, found.memory_gb, found.disk_gb)
    params = {
        "kind": provider_kind,
        "sandbox": sandbox_id,
        "cpu": found.cpu,
        "memory_gb": found.memory_gb,
        "disk_gb": found.disk_gb,
        "archive": found.archive_after_s,
        "source": "provider" if sized else "partial",
    }
    async with get_db_connection(conn) as owned:
        async with owned.transaction():
            params["age"] = time.monotonic() - asked_at
            async with owned.cursor() as cur:
                await cur.execute(
                    f"""
                    UPDATE computer_runs r
                    SET cpu = COALESCE(%(cpu)s, r.cpu),
                        memory_gb = COALESCE(%(memory_gb)s, r.memory_gb),
                        disk_gb = COALESCE(%(disk_gb)s, r.disk_gb),
                        archive_after_s = %(archive)s,
                        size_source = CASE WHEN r.size_source = 'provider'
                                           THEN 'provider' ELSE %(source)s END
                    WHERE {_ON_SANDBOX}
                    """,
                    params,
                )
                if found.state in _UP_STATES:
                    # To when the answer was read, the last moment it is
                    # known up. Only while the computer is not starting
                    # again: a start brings the sandbox up before its run
                    # opens.
                    await cur.execute(
                        f"""
                        UPDATE computer_runs r
                        SET ended_at = {_ANSWERED_AT}, metrics_final = FALSE
                        FROM computers c
                        WHERE {_ON_SANDBOX} AND {_STOP_UNCONFIRMED}
                          AND r.ended_at < {_ANSWERED_AT}
                          AND {_NEXT_START} IS NULL
                          AND c.computer_id = r.computer_id
                          AND c.status NOT IN ({_live()})
                        """,
                        params,
                    )
                elif found.state is not None:
                    await cur.execute(_SEEN_DOWN, params)
                await cur.execute(_STAMP_CHECKED, params)
                await _end_holds(
                    cur,
                    _ON_SANDBOX,
                    params,
                    ends_now=found.state in _HOLD_ENDING_STATES,
                    why=found.state.value if found.state else None,
                    answer_age_s=params["age"],
                )


async def store_sandbox_gone(
    provider_kind: str,
    sandbox_id: str,
    *,
    archive_after_s: Optional[int],
    asked_at: float,
    conn=None,
) -> None:
    """Record that the provider no longer has a sandbox.

    Its runs keep the size they have for good, the tier's for any part the
    provider never gave, and a run whose sandbox was never asked about takes
    *archive_after_s*, the interval the provider is configured for, which
    bounds its hold; one the provider answered keeps what it said, a VM's
    no archive included. The hold ends at its computer's delete when there
    was one, else at this check. *asked_at* is as for ``store_allocation``.
    """
    params = {"kind": provider_kind, "sandbox": sandbox_id, "archive": archive_after_s}
    async with get_db_connection(conn) as owned:
        async with owned.transaction():
            params["age"] = time.monotonic() - asked_at
            async with owned.cursor() as cur:
                await cur.execute(
                    f"""
                    UPDATE computer_runs r
                    SET size_source = 'tier',
                        archive_after_s = CASE WHEN r.size_source IS NULL
                                               THEN %(archive)s
                                               ELSE r.archive_after_s END
                    WHERE {_ON_SANDBOX}
                      AND (r.size_source IS NULL OR r.size_source = 'partial')
                    """,
                    params,
                )
                await cur.execute(_SEEN_DOWN, params)
                await cur.execute(_STAMP_CHECKED, params)
                await _end_holds(
                    cur,
                    _ON_SANDBOX,
                    params,
                    ends_now=True,
                    why="gone",
                    answer_age_s=params["age"],
                )
