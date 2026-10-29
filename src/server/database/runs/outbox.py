"""Hook outbox (SQL layer) — durable post-commit effects of run finalize.

``enqueue_hooks`` writes the rows the decision table in ``hook_jobs`` returns,
on the finalize transaction; the claim/ack/nack trio is the lease protocol the
``HookOutboxDrainer`` runs (committed claims, per-ordering-key FIFO, expired-
lease reclaim as crash recovery, effect-before-ack retry safety).
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from psycopg.rows import dict_row

from src.server.database import pool
# Re-exported so the hook_jobs split stays invisible to every call site.
from src.server.database.runs.hook_jobs import (  # noqa: F401
    BurstReleasePayload,
    HookJob,
    NeedsInputWakePayload,
    ReportBackPayload,
    UserFeedPayload,
    WatchClearPayload,
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.database.market_targets import MARKET_TARGET_OWED_SQL
from src.server.database.runs.command_runs import COMMAND_OWED_SQL
from src.server.database.runs.command_runs import HOOK_TYPE as COMMAND_REPORT_HOOK
from src.server.database.runs.command_runs import NOTICE_HOOK_TYPE as COMMAND_NOTICE_HOOK
from src.server.database.runs.command_runs import (
    notice_ordering_key as command_notice_key,
)
from src.server.database.market_targets import ordering_key as market_target_key
from src.server.database.wakeups.contract import WAKEUP_OWED_SQL
from src.server.database.wakeups.contract import ordering_key as wakeup_key
from src.server.utils.pg_sanitize import SafeJson

# Advisory-lock class (int32) for per-ordering-key claim serialization.
# The two-int lock form is a separate keyspace from the WriterGuard's
# single-bigint locks, so no domain interference is possible.
_OKEY_LOCK_CLASS = 0x484F4F4B  # 'HOOK'


async def enqueue_hooks(
    conn, *, run_id: Optional[str] = None, thread_id: str, jobs: List[HookJob]
) -> None:
    """Write outbox rows on the caller's transaction: a run's finalize, or a
    transition with no run that makes a notice due (a wakeup's fire).

    ON CONFLICT DO NOTHING on idempotency_key: re-finalizing after a lost
    race never double-registers an effect. Each row is dated by the clock,
    not by the transaction, so jobs one transaction writes on a key run in
    the order written.
    """
    async with conn.cursor() as cur:
        for job in jobs:
            await cur.execute(
                """
                INSERT INTO hook_outbox (
                    run_id, conversation_thread_id, hook_type,
                    payload, ordering_key, idempotency_key, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, clock_timestamp())
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (
                    run_id,
                    thread_id,
                    job.hook_type,
                    SafeJson(job.payload),
                    job.ordering_key,
                    job.idempotency_key,
                ),
            )


# A pending steer keeps ``next_retry_at='infinity'`` for its whole life, so a
# build without a steer executor never sees it: across a blue/green flip the
# old color's drainer claims any pending row whose ``next_retry_at`` is due and
# nacks an unknown hook type toward dead. When a steer is due lives in its
# payload instead (``due_at``), which only this build's claim reads; every
# write that parks a steer again drops it.
def _steer_due_at_sql(payload: str, at: str) -> str:
    """``payload`` (a jsonb expression) due at ``at``."""
    return f"(({payload}) - 'due_at') || jsonb_build_object('due_at', {at})"


# Due by its own clock: released by its run's ending, or backing off a nack.
_STEER_DUE_SQL = """(
    o.hook_type = 'steer'
    AND o.next_retry_at = 'infinity'::timestamptz
    AND o.payload ? 'due_at'
    AND (o.payload->>'due_at')::timestamptz <= NOW()
)"""

# A parked steer (``next_retry_at='infinity'`` and no ``due_at``) whose owner
# is over and left it to nobody: a lost finalize CAS, a worker lost mid-turn, or a run finalized
# by a build from before steers lived here. A thread steer waits on its
# thread: no live run, and the latest one completed or was cancelled by
# something other than the user (a user stop returns the user's steers, an
# error keeps them for the user's next move, an interrupt for the resume). A
# task steer waits on its task run while on the task's chain; once its run
# ended and moved it to the thread's chain to resume the task, it waits on the
# thread like a thread steer. Its head is then due like any other, so the
# chain behind it moves too. A steer that ran out of attempts
# (``_exhaust_steers``) waits for a run's ending or the user instead: handing
# it straight back to the executor that gave up on it would loop.
_STEER_SAFETY_NET_SQL = """(
    o.hook_type = 'steer'
    AND o.next_retry_at = 'infinity'::timestamptz
    AND NOT (o.payload ? 'due_at')
    AND NOT (o.payload ? 'exhausted')
    AND CASE WHEN o.payload->>'target' = 'task'
                  AND o.ordering_key LIKE '%%:task:%%' THEN
        NOT EXISTS (
            SELECT 1 FROM subagent_runs sr
            WHERE sr.task_run_id = (o.payload->>'task_run_id')::uuid
              AND sr.status = 'in_progress'
        )
    ELSE
        NOT EXISTS (
            SELECT 1 FROM conversation_responses lr
            WHERE lr.conversation_thread_id = o.conversation_thread_id
              AND lr.status = 'in_progress'
        )
        AND EXISTS (
            SELECT 1 FROM (
                SELECT lr.status, lr.metadata FROM conversation_responses lr
                WHERE lr.conversation_thread_id = o.conversation_thread_id
                ORDER BY lr.run_seq DESC
                LIMIT 1
            ) latest
            WHERE latest.status = 'completed'
               OR (latest.status = 'cancelled'
                   AND COALESCE(latest.metadata->>'cancelled_by_user', '')
                       <> 'true')
        )
    END
)"""


def _sql_list(values: Tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


class _Alert(NamedTuple):
    #: Whether the notice of the job ``o`` is still owed, as its executor
    #: decides it.
    owed_sql: str
    #: The key a thread's notices of this type run in order under.
    ordering_key: Callable[[str], str]
    #: Whether a wake turn delivers it when no live turn takes it in. One
    #: that does not is never due: a turn's ending only drops a hold on it.
    wakes: bool = True


# The notices a thread is owed by its own watchers, each a job a live turn
# takes in as a report: a price target's (``market_target``, draining) and a
# wakeup's, which a wake turn delivers otherwise, and a background command's
# held for the thread's next turn (``command_notice``), which waits for one,
# since a turn of its own would start the computer again. Held for the
# thread's next turn rather than dead-lettered at the attempts ceiling.
# Every statement on alerts reads its types and their owed predicates from
# here.
_ALERTS: Dict[str, _Alert] = {
    "market_target": _Alert(MARKET_TARGET_OWED_SQL, market_target_key),
    "wakeup": _Alert(WAKEUP_OWED_SQL, wakeup_key),
    COMMAND_NOTICE_HOOK: _Alert(COMMAND_OWED_SQL, command_notice_key, wakes=False),
}
ALERT_HOOK_TYPES = tuple(_ALERTS)
_ALERT_TYPES_SQL = _sql_list(ALERT_HOOK_TYPES)
_SILENT_ALERT_TYPES_SQL = _sql_list(tuple(t for t, a in _ALERTS.items() if not a.wakes))


def alert_ordering_keys(thread_id: str) -> List[str]:
    """The keys a thread's alerts run under, one per type."""
    return [alert.ordering_key(str(thread_id)) for alert in _ALERTS.values()]


def waiting_alert_sql(a: str) -> str:
    """An alert its executor gave up waking the thread for
    (``_exhaust_alerts``), parked until the thread's next turn takes it in or
    a turn's ending makes it due. ``a`` is the hook_outbox alias."""
    return (
        f"({a}.hook_type IN {_ALERT_TYPES_SQL} AND {a}.status = 'pending'"
        f" AND {a}.next_retry_at = 'infinity'::timestamptz"
        f" AND {a}.payload ? 'exhausted')"
    )


def _owed_sql(owed: Dict[str, str]) -> str:
    """Whether the notice of the job ``o`` is still owed, by its type's own
    predicate; a type with none is not."""
    return (
        "(CASE o.hook_type\n"
        + "".join(f"    WHEN '{t}' THEN {sql}\n" for t, sql in owed.items())
        + "    ELSE false\nEND)"
    )


_ALERT_OWED_SQL = _owed_sql({t: a.owed_sql for t, a in _ALERTS.items()})

# The dead jobs ``revive_dead_alerts`` makes due again while still owed: the
# alerts that wake a thread, and a command's report, which a build that
# predates commands knows no executor for.
_REVIVED_OWED = {t: a.owed_sql for t, a in _ALERTS.items() if a.wakes} | {
    COMMAND_REPORT_HOOK: COMMAND_OWED_SQL
}
_REVIVED_TYPES_SQL = _sql_list(tuple(_REVIVED_OWED))
_REVIVED_OWED_SQL = _owed_sql(_REVIVED_OWED)


async def claim_outbox_jobs(
    limit: int = 10,
    lease_seconds: int = 60,
    *,
    max_attempts: int = 5,
    excluded_hook_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Lease a batch of due jobs: pending-and-due, or claimed with an
    expired lease below the attempts ceiling (crash/stall recovery —
    reclaim IS the startup recovery; at the ceiling only the park sweep
    may touch the row, so a lease expiring between park and claim can't
    mint attempt max+1).

    Ordering keys give per-key MUTUAL EXCLUSION plus best-effort FIFO.
    Two phases inside ONE transaction, because row-version inspection
    alone cannot see an UNCOMMITTED sibling claim (READ COMMITTED): phase
    1 takes a per-key advisory xact lock for each claimable key — any
    in-flight claimer, live-ownership renewal (``extend_job_lease``), or
    teardown guard (``fenced_job_guard``) of that key holds its lock
    until commit, so acquiring it proves every prior mutation of the
    key's ownership is committed; phase 2 then gates on committed state
    under a FRESH statement snapshot: a candidate is blocked while ANY
    same-key row holds a live lease (regardless of age — a job inserted
    by a slow transaction becomes visible with an OLDER created_at than
    an already-claimed sibling), else oldest-first among visible open
    rows. Phase 1 pre-applies the same head-eligibility gate on its own
    (possibly stale) snapshot and walks keys OLDEST ELIGIBLE HEAD FIRST —
    unordered discovery could re-pick the same hot chains every pass
    (their completions keep exposing fresh heads) and starve an eligible
    key forever, where an unserved head only ages until it sorts first.
    The try-locks are taken one key at a time, in that order, until
    ``limit`` acquisitions: PostgreSQL does not guarantee evaluation
    order or count for a volatile function in a LIMIT query, so an
    in-query try-lock could over-acquire far past the limit (advisory
    locks are pool-bounded) with no fairness contract. The discovery
    scan is itself bounded; a pass over a rival-saturated backlog
    under-claims and retries next poll instead of hoarding locks. The
    claim commits before execution; attempts counts leases handed out,
    not completions.

    ``excluded_hook_types`` skips CLAIMING those types this pass (per-type
    in-flight quotas) without disturbing chain discipline: an excluded head
    still blocks its ordering key's later jobs, it just isn't leased.

    A pending steer is due by its payload's ``due_at``, never by
    ``next_retry_at`` (``_STEER_DUE_SQL``). A parked one is also due once its
    owner is over and nothing will take it in (``_STEER_SAFETY_NET_SQL``), so
    its release does not hang on any one finalize path running to the end.

    A waiting alert (``waiting_alert_sql``) blocks nothing behind it: it is
    not claimable, and a later notice's wake is the turn that would take it
    in.
    """
    excluded = list(excluded_hook_types or [])
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    SELECT o.ordering_key AS okey
                    FROM hook_outbox o
                    WHERE o.ordering_key IS NOT NULL
                      AND o.hook_type <> ALL(%s::varchar[])
                      AND (
                        (o.status = 'pending'
                         AND (o.next_retry_at IS NULL
                              OR o.next_retry_at <= NOW()
                              OR {_STEER_DUE_SQL}
                              OR {_STEER_SAFETY_NET_SQL}))
                        OR (o.status = 'claimed'
                            AND o.lease_expires_at <= NOW()
                            AND o.attempts < %s)
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM hook_outbox p
                        WHERE p.ordering_key = o.ordering_key
                          AND p.hook_outbox_id <> o.hook_outbox_id
                          AND (
                            (p.status = 'claimed'
                             AND p.lease_expires_at > NOW())
                            OR (
                              ((p.status = 'pending'
                                AND NOT {waiting_alert_sql("p")})
                               OR (p.status = 'claimed'
                                   AND p.lease_expires_at <= NOW()))
                              AND (p.created_at, p.hook_outbox_id)
                                  < (o.created_at, o.hook_outbox_id)
                            )
                          )
                      )
                    GROUP BY o.ordering_key
                    ORDER BY MIN(o.created_at)
                    LIMIT %s
                    """,
                    (excluded, max_attempts, limit * 8),
                )
                candidate_keys = [r["okey"] for r in await cur.fetchall()]

                locked_keys = []
                for okey in candidate_keys:
                    if len(locked_keys) >= limit:
                        break
                    await cur.execute(
                        "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s)) AS locked",
                        (_OKEY_LOCK_CLASS, okey),
                    )
                    row = await cur.fetchone()
                    if row and row["locked"]:
                        locked_keys.append(okey)

                await cur.execute(
                    f"""
                    WITH due AS (
                        SELECT o.hook_outbox_id FROM hook_outbox o
                        WHERE o.hook_type <> ALL(%s::varchar[])
                        AND (
                            (o.status = 'pending'
                             AND (o.next_retry_at IS NULL
                                  OR o.next_retry_at <= NOW()
                                  OR {_STEER_DUE_SQL}
                                  OR {_STEER_SAFETY_NET_SQL}))
                            OR (o.status = 'claimed'
                                AND o.lease_expires_at <= NOW()
                                AND o.attempts < %s)
                        )
                        AND (
                            o.ordering_key IS NULL
                            OR (
                              o.ordering_key = ANY(%s)
                              AND NOT EXISTS (
                                SELECT 1 FROM hook_outbox p
                                WHERE p.ordering_key = o.ordering_key
                                  AND p.hook_outbox_id <> o.hook_outbox_id
                                  AND (
                                    (p.status = 'claimed'
                                     AND p.lease_expires_at > NOW())
                                    OR (
                                      ((p.status = 'pending'
                                        AND NOT {waiting_alert_sql("p")})
                                       OR (p.status = 'claimed'
                                           AND p.lease_expires_at <= NOW()))
                                      AND (p.created_at, p.hook_outbox_id)
                                          < (o.created_at, o.hook_outbox_id)
                                    )
                                  )
                              )
                            )
                        )
                        ORDER BY o.created_at, o.hook_outbox_id
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE hook_outbox h
                    SET status = 'claimed',
                        attempts = h.attempts + 1,
                        -- clock_timestamp(): NOW() is transaction start, and
                        -- phase 1's advisory-lock waits can consume real time —
                        -- a NOW()-based lease could commit with most (or all)
                        -- of its runway already burned.
                        lease_expires_at =
                            clock_timestamp() + make_interval(secs => %s),
                        -- A claimed steer is due by its lease; psycopg
                        -- cannot load 'infinity'. Every way back to pending
                        -- writes 'infinity' and its due_at again.
                        next_retry_at = CASE
                            WHEN h.next_retry_at = 'infinity'::timestamptz
                                THEN NULL
                            ELSE h.next_retry_at
                        END,
                        payload = CASE
                            WHEN h.hook_type = 'steer'
                                THEN h.payload - 'due_at'
                            ELSE h.payload
                        END
                    FROM due
                    WHERE h.hook_outbox_id = due.hook_outbox_id
                    RETURNING h.*
                    """,
                    (excluded, max_attempts, locked_keys, limit, float(lease_seconds)),
                )
                rows = await cur.fetchall()
                return [dict(r) for r in rows]


def _watch_clear_insert_sql(
    source: str, *, suffix: str, error_wake: bool, refuse_if_pointer: bool
) -> str:
    """The ``watch_clear`` an analyst's report_back owes once it leaves the
    open set, inserted from the ``source`` CTE in the same statement."""
    refuse = ",\n               'refuse_if_pointer', true" if refuse_if_pointer else ""
    return f"""
    INSERT INTO hook_outbox (
        run_id, conversation_thread_id, hook_type,
        payload, ordering_key, idempotency_key, created_at
    )
    SELECT run_id, conversation_thread_id, 'watch_clear',
           jsonb_build_object(
               'ptc_thread_id', payload->>'ptc_thread_id',
               'error_wake', {"true" if error_wake else "false"},
               'dispatch_gen', payload->'dispatch_gen'{refuse}
           ),
           ordering_key, hook_outbox_id || ':{suffix}', created_at
    FROM {source}
    WHERE hook_type = 'report_back' AND payload ? 'ptc_thread_id'
    ON CONFLICT (idempotency_key) DO NOTHING
"""


# A dead report_back still owes its watch/cap teardown. The compensation
# INSERT rides the SAME statement as the dead transition (data-modifying
# CTE): the dead commit and the compensation are atomic — no window where
# the row is dead but its cleanup was lost to a crash. created_at is
# INHERITED from the dead row so the compensation occupies its exact chain
# position instead of queueing behind already-pending successors. The
# dispatch generation is inherited too — a gen-less compensation would
# clear unconditionally and could destroy a re-dispatched pair's state.
# refuse_if_pointer: the dead source may have stalled MID-ADMISSION with
# the run pointer already claimed — the dead row holds no lease, so this
# compensation becomes chain head immediately and would otherwise drain
# that pointer under the in-flight route (the round-18 race). A live
# pointer means the admission's own lifecycle owns the teardown.
_DEAD_REPORT_BACK_COMPENSATION_SQL = _watch_clear_insert_sql(
    "dead_rows", suffix="dead_clear", error_wake=True, refuse_if_pointer=True
)


async def park_exhausted_jobs(*, max_attempts: int = 5) -> List[Dict[str, Any]]:
    """Park expired-lease jobs that already burned max_attempts leases as dead.

    Without this, a job whose workers crash before nacking (nack is what
    normally parks dead) is reclaimable forever; the claim ceiling alone
    would instead leave it claimed-expired forever, wedging its ordering
    chain. Dead report_backs get their watch_clear compensation in the
    same atomic statement. A steer is settled instead (``_exhaust_steers``)
    and an alert held for its thread's next turn (``_exhaust_alerts``), in
    the same transaction. Returns the parked rows,
    a steer's or an alert's with the status it was settled to (done, or
    pending).
    """
    expired_at_ceiling = (
        "o.status = 'claimed' AND o.lease_expires_at <= NOW()"
        " AND o.attempts >= %(max)s"
    )
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                steers = await _exhaust_steers(
                    cur, expired_at_ceiling, {"max": max_attempts}
                )
                alerts = await _exhaust_alerts(
                    cur, expired_at_ceiling, {"max": max_attempts}
                )
                await cur.execute(
                    f"""
                    WITH dead_rows AS (
                        UPDATE hook_outbox
                        SET status = 'dead', lease_expires_at = NULL,
                            completed_at = NOW()
                        WHERE status = 'claimed'
                          AND hook_type NOT IN {_sql_list(("steer", *ALERT_HOOK_TYPES))}
                          AND lease_expires_at <= NOW()
                          AND attempts >= %s
                        RETURNING *
                    ),
                    comp AS ({_DEAD_REPORT_BACK_COMPENSATION_SQL})
                    SELECT * FROM dead_rows
                    """,
                    (max_attempts,),
                )
                rows = await cur.fetchall()
                return [dict(r) for r in rows] + steers + alerts


async def revive_dead_cleanup_jobs(
    *, cooldown_seconds: int = 600, attempt_ceiling: int = 150
) -> List[Dict[str, Any]]:
    """Requeue dead terminal-cleanup jobs (watch_clear) for one more lease.

    A dead report_back gets a watch_clear compensation, but a watch_clear
    that itself dead-letters had no successor — the pair state it owes
    (memberships, cap slot, the error wake) would survive untouched to the
    24h origin TTL. Cleanup executors are idempotent and no-op once their
    state is gone, so revival is safe. ``attempts`` is NEVER reset —
    ``(job_id, attempts)`` is the ownership fence, and a reset would remint
    tokens a stale prior owner may still hold; each revival instead grants
    exactly one more lease (the pending claim has no ceiling; the very next
    nack or park re-deads at ``attempts >= max``). The cool-down anchors on
    ``completed_at`` — stamped by every dead transition — because
    ``created_at`` is CHAIN POSITION, not age (migrated rows are backdated
    ~30d), and ``next_retry_at`` predates a park-after-long-lease. The
    attempt ceiling absolutely bounds a poison row's lifetime cost
    (~ceiling leases, one per cool-down ≈ a day at the defaults — past the
    origin TTL, when there is nothing left to clear).
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'pending', next_retry_at = NOW(),
                    lease_expires_at = NULL, completed_at = NULL
                WHERE status = 'dead'
                  AND hook_type = 'watch_clear'
                  AND attempts < %s
                  AND COALESCE(completed_at, created_at)
                      <= NOW() - make_interval(secs => %s::float)
                RETURNING hook_outbox_id, hook_type, ordering_key
                """,
                (attempt_ceiling, cooldown_seconds),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


# Every column but next_retry_at, which psycopg cannot load as 'infinity'.
_PARKED_RETURNING_SQL = """o.hook_outbox_id, o.run_id, o.conversation_thread_id,
    o.hook_type, o.payload, o.ordering_key, o.idempotency_key, o.status,
    o.attempts, o.created_at, o.completed_at"""


async def _exhaust_alerts(
    cur, where: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Hold the alerts matching ``where`` for their thread's next turn, in
    the caller's transaction, instead of letting them go dead.

    Parked pending, so a live turn takes the notice in as a report owed to
    the thread, and a turn's ending makes it due for its executor to look
    again. ``exhausted`` stays for good: it keeps the parked row from
    blocking its chain, lets the closing sweep close it once its notice is
    no longer owed, and tells its executor that one more refusal holds it
    again rather than starting a fresh round of retries, so each further
    claim waits on another turn's ending. Leaves ``attempts`` alone, the
    fence token: a pending row has no claim ceiling, and every way back from
    claimed at the ceiling holds it here again. A wakeup's riders
    (``wakeups/riders.py``) are held with it in the same statement, since
    the turn that refused the head would refuse them too; each keeps the
    head it rode (``rode``), whose wake may yet turn out admitted. The head
    is locked before its riders, and they in id order, as the wakeups
    contract orders every lock on a wake's jobs. Needs a ``dict_row``
    cursor; returns the rows as held."""
    await cur.execute(
        f"""
        WITH held AS (
            UPDATE hook_outbox o
            SET status = 'pending',
                lease_expires_at = NULL,
                next_retry_at = 'infinity'::timestamptz,
                payload = (o.payload - 'steered_run_id' - 'due_at' - 'riders')
                    || jsonb_build_object('exhausted', true)
            WHERE o.hook_type IN {_ALERT_TYPES_SQL} AND {where}
            RETURNING {_PARKED_RETURNING_SQL}
        ),
        riders AS (
            UPDATE hook_outbox r
            SET next_retry_at = 'infinity'::timestamptz,
                payload = (r.payload - 'rides' - 'steered_run_id' - 'due_at')
                    || jsonb_build_object(
                        'exhausted', true, 'rode', r.payload->>'rides')
            WHERE r.hook_outbox_id IN (
                SELECT x.hook_outbox_id FROM hook_outbox x
                WHERE x.hook_type = 'wakeup' AND x.status = 'pending'
                  -- A rider is on its head's chain. Read through that, so
                  -- the park sweep, which runs this with nothing held on
                  -- nearly every pass, scans no other thread's jobs.
                  AND x.ordering_key IN (
                      SELECT h.ordering_key FROM held h
                      WHERE h.hook_type = 'wakeup')
                  AND x.payload->>'rides' IN (
                      SELECT h.hook_outbox_id::text FROM held h
                      WHERE h.hook_type = 'wakeup')
                ORDER BY x.hook_outbox_id
                FOR UPDATE
            )
        )
        SELECT * FROM held
        """,
        params,
    )
    return [dict(r) for r in await cur.fetchall()]


async def exhaust_alert(job_id: str, *, attempts: int) -> bool:
    """Hold a claimed alert whose wake was refused for its thread's next
    turn (``_exhaust_alerts``), fenced to the caller's lease. False when the
    fence was lost: another owner has the job."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            held = await _exhaust_alerts(
                cur,
                "o.hook_outbox_id = %(job_id)s::uuid AND o.status = 'claimed'"
                " AND o.attempts = %(attempts)s",
                {"job_id": job_id, "attempts": attempts},
            )
            return bool(held)


# How many times ``revive_dead_alerts`` brought the alert ``o`` back. Read
# only as a number, so a value of any other type cannot fail the statement.
_ALERT_REVIVALS_SQL = """(CASE WHEN jsonb_typeof(o.payload->'revived') = 'number'
    THEN (o.payload->>'revived')::int ELSE 0 END)"""


async def revive_dead_alerts(
    *, cooldown_seconds: int = 600, window_seconds: int = 86400, max_revivals: int = 2
) -> List[Dict[str, Any]]:
    """Make a dead alert, or a dead command report, whose notice is still
    owed due again.

    This build holds an alert rather than dead-lettering it, but a build
    that predates the hold, or one that knows no such job type, kills it at
    the attempts ceiling: across a blue/green cutover the old color's
    drainer nacks a job it cannot run until it dies, and this build never
    tried its wake. Revived due, its executor here tries one, and a refusal
    holds it as any other (at once when its attempts are past the ceiling).
    Only a row dead for ``cooldown_seconds`` comes back, so an old color
    still draining is less likely to kill it again, and ``revived`` counts
    revivals up to ``max_revivals``, so one it keeps killing stays dead. Only
    rows that died within ``window_seconds`` are read, which keeps the scan
    on the terminal-age index. ``attempts`` is kept, the fence token. A
    single guarded UPDATE, so every worker may run it at once: a second one
    waiting on the row finds it no longer dead."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET status = 'pending',
                    lease_expires_at = NULL,
                    completed_at = NULL,
                    next_retry_at = NOW(),
                    payload = (o.payload - 'steered_run_id' - 'due_at')
                        || jsonb_build_object('revived', {_ALERT_REVIVALS_SQL} + 1)
                WHERE o.status = 'dead'
                  AND o.hook_type IN {_REVIVED_TYPES_SQL}
                  AND COALESCE(o.completed_at, o.created_at)
                      <= NOW() - make_interval(secs => %(cooldown)s::float)
                  AND COALESCE(o.completed_at, o.created_at)
                      > NOW() - make_interval(secs => %(window)s::float)
                  AND {_ALERT_REVIVALS_SQL} < %(max)s
                  AND {_REVIVED_OWED_SQL}
                RETURNING o.hook_outbox_id, o.conversation_thread_id,
                          o.ordering_key, o.payload
                """,
                {
                    "cooldown": cooldown_seconds,
                    "window": window_seconds,
                    "max": max_revivals,
                },
            )
            return [dict(r) for r in await cur.fetchall()]


# No turn will come on the thread of the job ``o``: it is archived, or its
# workspace was deleted.
NO_TURN_SQL = """EXISTS (
    SELECT 1 FROM conversation_threads c
    LEFT JOIN workspaces w ON w.workspace_id = c.workspace_id
    WHERE c.conversation_thread_id = o.conversation_thread_id
      AND (c.archived_at IS NOT NULL OR w.status = 'deleted')
)"""


# How long a command's notice waits for its thread's next turn.
COMMAND_NOTICE_TTL_DAYS = 7


async def close_unowed_waiting_alerts() -> List[Dict[str, Any]]:
    """Close the alerts no turn will take in: a waiting one whose notice is
    no longer owed (a reminder whose target fired, expired or was cancelled
    since), and any pending one on a thread that is archived or whose
    workspace was deleted, held or merely parked. That includes a command's
    notice held for such a thread's next turn: no executor ever claims one,
    so nothing else would. So is one a week old, for a thread nobody came
    back to: a pending row costs every claim's scan, and a command that old
    no longer bears on the thread's next turn.

    Closed as their executor would drop them, without waiting on a turn the
    thread may never get. A wakeup's notice still owed there is made due
    instead: its executor ends the wakeup before dropping it, which archive
    and workspace delete only try once, and closing it here would leave the
    wakeup open with its fire lost. One a turn holds is left to that turn's
    confirm or ending. A deleted thread takes its rows with it, and an owner
    who turned the feature off is read at the turn or the claim that would
    show the notice. Guarded UPDATEs, so every worker may run them at once;
    the row lock orders each against a turn's hold and a claim."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox h SET next_retry_at = NOW()
                WHERE h.hook_outbox_id IN (
                    SELECT o.hook_outbox_id FROM hook_outbox o
                    WHERE o.hook_type = 'wakeup' AND o.status = 'pending'
                      AND o.next_retry_at = 'infinity'::timestamptz
                      AND NOT (o.payload ? 'steered_run_id')
                      AND {NO_TURN_SQL} AND {WAKEUP_OWED_SQL}
                    ORDER BY (o.payload ? 'rides'), o.hook_outbox_id
                    FOR UPDATE SKIP LOCKED
                )
                """
            )
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET status = 'done', completed_at = NOW(), next_retry_at = NULL
                WHERE o.hook_type IN {_ALERT_TYPES_SQL}
                  AND o.status = 'pending'
                  AND NOT (o.payload ? 'steered_run_id')
                  AND (
                    ({waiting_alert_sql("o")} AND NOT {_ALERT_OWED_SQL})
                    OR ({NO_TURN_SQL}
                        AND (o.hook_type <> 'wakeup' OR NOT {_ALERT_OWED_SQL}))
                    OR (o.hook_type = '{COMMAND_NOTICE_HOOK}'
                        AND o.created_at < NOW() - make_interval(days => {COMMAND_NOTICE_TTL_DAYS}))
                  )
                RETURNING o.hook_outbox_id, o.conversation_thread_id,
                          o.ordering_key, o.payload
                """
            )
            return [dict(r) for r in await cur.fetchall()]


async def purge_terminal_jobs(
    *, retention_seconds: float, batch_size: int = 5000
) -> int:
    """Delete done/dead rows older than the retention window, one batch.

    Terminal history is only read back within short windows (done-recents
    wake-miss recovery, the dead-revive cool-down/ceiling ≈ a day), so
    anything past the retention window is dead weight that the per-worker
    sweeps would otherwise scan forever. Batched so the first purge on a
    long-lived deployment can't hold a giant delete.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM hook_outbox
                WHERE hook_outbox_id IN (
                    SELECT hook_outbox_id FROM hook_outbox
                    WHERE status IN ('done', 'dead')
                      AND COALESCE(completed_at, created_at)
                          <= NOW() - make_interval(secs => %s::float)
                    LIMIT %s
                )
                """,
                (retention_seconds, batch_size),
            )
            return cur.rowcount


async def ack_outbox_job(job_id: str, *, attempts: int) -> bool:
    """Mark a claimed job done, fenced to the caller's lease generation.

    ``attempts`` increments on every claim, so it doubles as the fence
    token: a stale owner (lease expired, job reclaimed) matches nothing
    and cannot ack the new owner's claim. False = fence lost; never
    re-open the row.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'done', completed_at = NOW()
                WHERE hook_outbox_id = %s AND status = 'claimed'
                  AND attempts = %s
                """,
                (job_id, attempts),
            )
            return cur.rowcount > 0


_NACK_BACKOFF_SQL = (
    "NOW() + make_interval(secs => LEAST(POWER(2, attempts), 60)::float)"
)

# A claimed job out of attempts, fenced to the caller's lease.
_AT_CEILING_SQL = (
    "o.hook_outbox_id = %(job_id)s::uuid AND o.status = 'claimed'"
    " AND o.attempts = %(attempts)s AND o.attempts >= %(max)s"
)


def _fence_params(job_id: str, attempts: int, max_attempts: int) -> Dict[str, Any]:
    return {"job_id": job_id, "attempts": attempts, "max": max_attempts}


async def nack_outbox_job(
    job_id: str, *, attempts: int, max_attempts: int = 5
) -> Optional[str]:
    """Return a failed claimed job to pending with exponential backoff,
    or park it dead at max_attempts. Fenced by ``attempts`` (see
    ``ack_outbox_job``) so a stale owner can't nack — or dead-letter — the
    reclaiming owner's live lease (a fence-lost nack also never
    compensates a live job). A report_back going dead gets its watch_clear
    compensation in the same atomic statement; a steer at the ceiling is
    settled instead (``_exhaust_steers``), and an alert held
    for its thread's next turn (``_exhaust_alerts``). Returns the new status
    (``'exhausted'`` for either), or None if the fence was lost."""
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                fence = _fence_params(job_id, attempts, max_attempts)
                if await _exhaust_steers(cur, _AT_CEILING_SQL, fence):
                    return "exhausted"
                if await _exhaust_alerts(cur, _AT_CEILING_SQL, fence):
                    return "exhausted"
                await cur.execute(
                    f"""
                    WITH bumped AS (
                        UPDATE hook_outbox
                        SET status = CASE
                                WHEN attempts >= %s THEN 'dead' ELSE 'pending'
                            END,
                            -- A steer backs off in its payload (see
                            -- _STEER_DUE_SQL), so a build that cannot run it
                            -- never claims it.
                            next_retry_at = CASE
                                WHEN hook_type = 'steer' AND attempts < %s
                                    THEN 'infinity'::timestamptz
                                ELSE NOW() + make_interval(
                                    secs => LEAST(POWER(2, attempts), 60)::float
                                )
                            END,
                            payload = CASE
                                WHEN hook_type = 'steer' AND attempts < %s
                                    THEN {_steer_due_at_sql("payload", _NACK_BACKOFF_SQL)}
                                ELSE payload
                            END,
                            lease_expires_at = NULL,
                            completed_at = CASE
                                WHEN attempts >= %s THEN NOW() ELSE completed_at
                            END
                        WHERE hook_outbox_id = %s AND status = 'claimed'
                          AND attempts = %s
                        RETURNING *
                    ),
                    dead_rows AS (
                        SELECT * FROM bumped WHERE status = 'dead'
                    ),
                    comp AS ({_DEAD_REPORT_BACK_COMPENSATION_SQL})
                    SELECT status FROM bumped
                    """,
                    (
                        max_attempts,
                        max_attempts,
                        max_attempts,
                        max_attempts,
                        job_id,
                        attempts,
                    ),
                )
                row = await cur.fetchone()
                return row["status"] if row else None


async def _lock_job_ordering_key(cur, job_id: str) -> bool:
    """Take the per-key advisory xact lock for a job's ordering key (blocking).

    Live ownership may only be established or renewed under the same key
    protocol the claim uses — otherwise an expired owner could revive
    concurrently with a sibling claim the gate admitted on the strength of
    that expiry, putting two same-key jobs live at once. Blocking (not try):
    rivals hold the lock only for a claim transaction or a bounded teardown
    guard. Returns False when the row no longer exists (no key to lock).
    """
    await cur.execute(
        "SELECT ordering_key FROM hook_outbox WHERE hook_outbox_id = %s",
        (job_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return False
    okey = row["ordering_key"]
    if okey is not None:
        await cur.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_OKEY_LOCK_CLASS, okey),
        )
    return True


async def extend_job_lease(job_id: str, lease_seconds: int, *, attempts: int) -> bool:
    """Heartbeat for a long-running effect (report-back POST loop + terminal
    wait): pushes lease expiry out so the row isn't re-offered mid-execution.
    Fenced by ``attempts`` (see ``ack_outbox_job``) AND by lease liveness
    under the per-key advisory lock: an EXPIRED lease can never be renewed —
    once a sibling claimer may have observed the expiry, revival would put
    two same-key jobs live concurrently. ``clock_timestamp()`` (not
    transaction-start NOW(), stale by however long the key lock blocked) is
    the liveness clock. False = fence lost; the executor must stop and do NO
    further teardown, another drainer owns the key."""
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                if not await _lock_job_ordering_key(cur, job_id):
                    return False
                await cur.execute(
                    """
                    UPDATE hook_outbox
                    SET lease_expires_at =
                            clock_timestamp() + make_interval(secs => %s)
                    WHERE hook_outbox_id = %s AND status = 'claimed'
                      AND attempts = %s
                      AND lease_expires_at > clock_timestamp()
                    """,
                    (float(lease_seconds), job_id, attempts),
                )
                return cur.rowcount > 0


@dataclass(frozen=True)
class FencedJob:
    """A job ``fenced_job_guard`` holds: its row as fenced, and the cursor of
    the transaction holding it, for statements that must commit under the
    same fence."""

    row: Dict[str, Any]
    cur: Any


@asynccontextmanager
async def fenced_job_guard(job_id: str, attempts: int, *, lease_seconds: int = 60):
    """Key-lock + row-lock fence for an external teardown: yields the job
    (``FencedJob``) while owned.

    Holds BOTH the per-key advisory xact lock (no sibling claim on the
    ordering key can even be gated while a teardown is in flight — the
    row-lock alone can't block a claim of a DIFFERENT row on the same key)
    and ``SELECT ... FOR UPDATE`` on the job row at our lease generation,
    across the caller's Redis mutations. Ownership additionally requires a
    LIVE lease (``clock_timestamp()``): an expired owner must stand down —
    a sibling claim may already have been admitted on the strength of that
    expiry. On owned exit the lease is renewed with fresh runway so the
    park sweep waiting on the row lock can't dead-letter a job its owner
    is still executing. Yields None (fence lost) when not owned; the
    caller must do NO teardown.
    """
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                owned: Optional[FencedJob] = None
                if await _lock_job_ordering_key(cur, job_id):
                    await cur.execute(
                        """
                        SELECT hook_outbox_id, hook_type, payload, ordering_key
                        FROM hook_outbox
                        WHERE hook_outbox_id = %s AND status = 'claimed'
                          AND attempts = %s
                          AND lease_expires_at > clock_timestamp()
                        FOR UPDATE
                        """,
                        (job_id, attempts),
                    )
                    row = await cur.fetchone()
                    if row is not None:
                        owned = FencedJob(dict(row), cur)
                yield owned
                if owned:
                    await cur.execute(
                        """
                        UPDATE hook_outbox
                        SET lease_expires_at =
                                clock_timestamp() + make_interval(secs => %s)
                        WHERE hook_outbox_id = %s
                        """,
                        (float(lease_seconds), job_id),
                    )


async def enqueue_compensation_job(
    *,
    run_id: Optional[str],
    thread_id: str,
    hook_type: str,
    payload: Dict[str, Any],
    ordering_key: Optional[str],
    idempotency_key: str,
    defer: bool = False,
    conn=None,
) -> None:
    """Insert a single follow-up job.

    ``defer=True`` inserts the row with ``next_retry_at='infinity'`` — durable
    but never claimable until ``release_deferred_notifications`` flips it due
    (interrupted-root task report-backs wait for the thread's next completed
    finalize). ON CONFLICT DO NOTHING: a retried caller never double-registers.
    ``conn`` joins the caller's transaction — how a job commits atomically
    with the finalize that owes it (subagent report-back).
    """
    if conn is not None:
        async with conn.cursor() as cur:
            await _insert_compensation_job(
                cur, run_id, thread_id, hook_type, payload,
                ordering_key, idempotency_key, defer,
            )
        return
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await _insert_compensation_job(
                cur, run_id, thread_id, hook_type, payload,
                ordering_key, idempotency_key, defer,
            )


async def _insert_compensation_job(
    cur,
    run_id: Optional[str],
    thread_id: str,
    hook_type: str,
    payload: Dict[str, Any],
    ordering_key: Optional[str],
    idempotency_key: str,
    defer: bool,
) -> None:
    await cur.execute(
        """
        INSERT INTO hook_outbox (
            run_id, conversation_thread_id, hook_type,
            payload, ordering_key, idempotency_key, created_at,
            next_retry_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            NOW(),
            CASE WHEN %s THEN 'infinity'::timestamptz ELSE NULL END
        )
        ON CONFLICT (idempotency_key) DO NOTHING
        """,
        (
            run_id,
            thread_id,
            hook_type,
            SafeJson(payload),
            ordering_key,
            idempotency_key,
            defer,
        ),
    )


async def release_deferred_notifications(conn, thread_id: str) -> int:
    """Flip the deferred (``next_retry_at='infinity'``) report-backs and
    alerts owed to a thread due.

    A task report-back and an alert are the thread's own rows; an analyst's
    report-back (``report_back``) belongs to the analyst's thread and is keyed
    on the thread it reports to, so each is matched by its own column. Runs on the
    caller's connection so the release commits atomically with the finalize
    that makes posting safe again (a completed run means no pending HITL
    checkpoint to collide with). A turn's hold goes with the park, so the
    next live turn may take a released report in without waiting on its
    executor. An alert waiting on the thread's next turn (``_exhaust_alerts``)
    that the ending turn did not take in is released too and keeps
    ``exhausted``: its executor wakes the thread if it now can, and holds it
    again if not, so each further try waits on another turn's ending. An
    alert that never wakes the thread (``_Alert.wakes``) is never made due:
    only a hold the ending turn left on it is dropped, so the turn after
    takes it in.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE hook_outbox o
            SET next_retry_at = CASE WHEN o.hook_type IN {_SILENT_ALERT_TYPES_SQL}
                                     THEN o.next_retry_at ELSE NOW() END,
                payload = o.payload - 'steered_run_id'
            WHERE o.status = 'pending'
              AND o.next_retry_at = 'infinity'::timestamptz
              AND (
                (o.hook_type IN {_sql_list(("task_report_back", COMMAND_REPORT_HOOK, *ALERT_HOOK_TYPES))}
                 AND o.conversation_thread_id = %s)
                OR (o.hook_type = 'report_back' AND o.ordering_key = %s)
              )
              AND (o.hook_type NOT IN {_SILENT_ALERT_TYPES_SQL}
                   OR o.payload ? 'steered_run_id')
            """,
            (thread_id, str(thread_id)),
        )
        return cur.rowcount


async def defer_claimed_job(
    job_id: str, *, attempts: int, max_attempts: int = 5
) -> Optional[str]:
    """Park a CLAIMED job back to deferred (``next_retry_at='infinity'``).

    For a report-back whose receiving thread is live or interrupted: a live
    turn takes it in as a steer before its next model call, and posting into
    an interrupted one would collide with the pending HITL checkpoint, so the
    job re-parks until the next dead-turn finalize flips it due again. Fenced
    by ``attempts``. At the attempts ceiling the job is parked dead instead
    (a dead report_back with its watch_clear compensation), since a pending
    row below the ceiling blocks its ordering chain and an unclaimable-forever
    deferred row would wedge every later notification on the thread. A steer
    at the ceiling is settled instead (``_exhaust_steers``), and an
    alert held for its thread's next turn (``_exhaust_alerts``),
    which blocks no chain. Returns the new
    status ('pending', 'dead' or 'exhausted'), or None if the fence was lost;
    the caller stops either way and its fenced ack no-ops. A turn's hold on
    the job is dropped: a job its executor holds was never confirmed, so the
    next live turn may take it in again.
    """
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                fence = _fence_params(job_id, attempts, max_attempts)
                if await _exhaust_steers(cur, _AT_CEILING_SQL, fence):
                    return "exhausted"
                if await _exhaust_alerts(cur, _AT_CEILING_SQL, fence):
                    return "exhausted"
                await cur.execute(
                    f"""
                    WITH parked AS (
                        UPDATE hook_outbox
                        SET status = CASE
                                WHEN attempts >= %s THEN 'dead' ELSE 'pending'
                            END,
                            next_retry_at = CASE
                                WHEN attempts >= %s THEN NOW()
                                ELSE 'infinity'::timestamptz
                            END,
                            lease_expires_at = NULL,
                            completed_at = CASE
                                WHEN attempts >= %s THEN NOW() ELSE completed_at
                            END,
                            payload = payload - 'steered_run_id' - 'due_at'
                        WHERE hook_outbox_id = %s AND status = 'claimed'
                          AND attempts = %s
                        RETURNING *
                    ),
                    dead_rows AS (
                        SELECT * FROM parked WHERE status = 'dead'
                    ),
                    comp AS ({_DEAD_REPORT_BACK_COMPENSATION_SQL})
                    SELECT status FROM parked
                    """,
                    (max_attempts, max_attempts, max_attempts, job_id, attempts),
                )
                row = await cur.fetchone()
                return row["status"] if row else None


async def list_open_notification_jobs(
    thread_id: str, hook_type: str | Sequence[str], *, limit: int = 20
) -> List[Dict[str, Any]]:
    """Open (pending, backing off included, or claimed) jobs of a type, or of
    any of several, on a thread's chain, oldest first.

    The read-model behind a watcher thread's ``pending_report_back`` when the
    outbox rows ARE the pending-registry: a job stays open from enqueue
    through the executor's terminal wait, so the reader decides from each
    job's dispatched run whether it is still owed. Deferred rows
    (``next_retry_at='infinity'``) are invisible: their work is parked, not
    in progress.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT hook_outbox_id, hook_type, payload, status
                FROM hook_outbox
                WHERE ordering_key = %s
                  AND hook_type = ANY(%s::varchar[])
                  AND status IN ('pending', 'claimed')
                  AND (next_retry_at IS NULL
                       OR next_retry_at != 'infinity'::timestamptz)
                ORDER BY created_at, hook_outbox_id
                LIMIT %s
                """,
                (thread_id, _hook_types(hook_type), limit),
            )
            return [dict(row) for row in await cur.fetchall()]


# The report-back types a live turn may take in, the alerts among them. Every
# report statement names them, so none can touch a steer row, which shares
# the hold's lifecycle keys.
_REPORT_TYPES_SQL = (
    "hook_type IN "
    + _sql_list(
        ("task_report_back", "report_back", COMMAND_REPORT_HOOK, *ALERT_HOOK_TYPES)
    )
)
# The report-backs on a thread's own chain.
_THREAD_REPORT_TYPES_SQL = _sql_list(
    ("task_report_back", "report_back", COMMAND_REPORT_HOOK)
)


def _hook_types(hook_type: str | Sequence[str]) -> List[str]:
    return [hook_type] if isinstance(hook_type, str) else list(hook_type)

# A report-back a live turn may take in: pending in any form (due, backing
# off, parked), not dispatched (it has its turn), not held (stamped
# ``steered_run_id``: already in a turn's messages) and not riding a wake
# turn (``rides``: the head's POST carries it). A claimed job belongs to its
# executor, whose POST may already be in flight.
_STEERABLE_REPORT_SQL = (
    f"(status = 'pending' AND {_REPORT_TYPES_SQL}"
    " AND NOT (payload ? 'dispatched_run_id')"
    " AND NOT (payload ? 'steered_run_id')"
    " AND NOT (payload ? 'rides'))"
)


async def list_steerable_report_jobs(
    thread_id: str, *, limit: int = 10
) -> List[Dict[str, Any]]:
    """Report-backs and alerts owed to a thread that no executor or turn
    holds, oldest first."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT hook_outbox_id, hook_type, run_id, payload, created_at
                FROM hook_outbox
                WHERE ((ordering_key = %s
                        AND hook_type IN {_THREAD_REPORT_TYPES_SQL})
                       OR (ordering_key = ANY(%s)
                           AND hook_type IN {_ALERT_TYPES_SQL}))
                  AND {_STEERABLE_REPORT_SQL}
                ORDER BY created_at, hook_outbox_id
                LIMIT %s
                """,
                (str(thread_id), alert_ordering_keys(thread_id), limit),
            )
            return [dict(row) for row in await cur.fetchall()]


async def steerable_alert_senders(
    thread_id: str, senders: Mapping[str, str]
) -> Set[str]:
    """Which of ``senders``, each naming the alert type it stands for, have
    a notice waiting for the thread's live turn to take in; a run waiting on
    its background tasks asks this between polls.

    Only a parked one counts: nothing makes it claimable while the turn
    lives, so the trigger this answer injects finds it at the next model
    call. A due one may be claimed by the drainer before that call, leaving
    the trigger with no alert; its executor parks it behind the live turn,
    and a later poll counts it."""
    types = sorted({t for t in senders.values() if t in _ALERTS})
    if not types:
        return set()
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT DISTINCT hook_type FROM hook_outbox
                WHERE ordering_key = ANY(%s) AND hook_type = ANY(%s)
                  AND {_STEERABLE_REPORT_SQL}
                  AND next_retry_at = 'infinity'::timestamptz
                """,
                ([_ALERTS[t].ordering_key(str(thread_id)) for t in types], types),
            )
            found = {r[0] for r in await cur.fetchall()}
    return {sender for sender, t in senders.items() if t in found}


async def held_notice_jobs(job_ids: List[str], run_id: str) -> List[Dict[str, Any]]:
    """The report jobs among ``job_ids`` that ``run_id`` holds for its live
    turn, oldest first, for the confirm to stamp each notice whose delivery
    is stamped on a row of its own before it closes them."""
    if not job_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT hook_outbox_id, hook_type, payload, created_at
                FROM hook_outbox
                WHERE hook_outbox_id = ANY(%s::uuid[])
                  AND {_REPORT_TYPES_SQL} AND status = 'pending'
                  AND payload->>'steered_run_id' = %s::text
                ORDER BY created_at, hook_outbox_id
                """,
                ([str(i) for i in job_ids], run_id),
            )
            return [dict(r) for r in await cur.fetchall()]


async def close_unowed_held_alerts(job_ids: List[str], run_id: str) -> List[str]:
    """Close the alert jobs ``run_id`` just held whose notice stopped being
    owed between the offer's read and the hold; returns the ids closed.

    The turn must not show what that already settled: a delivery elsewhere,
    a target that moved on, or a cancel that ends a wakeup outright (a
    forced one, such as archive). A wakeup cancel that commits before a hold
    closes the job, which the hold then misses, and one after finds it held
    and too late."""
    if not job_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET status = 'done', completed_at = NOW(), next_retry_at = NULL
                WHERE o.hook_outbox_id = ANY(%s::uuid[])
                  AND o.hook_type IN {_ALERT_TYPES_SQL} AND o.status = 'pending'
                  AND o.payload->>'steered_run_id' = %s::text
                  AND NOT {_ALERT_OWED_SQL}
                RETURNING o.hook_outbox_id
                """,
                ([str(i) for i in job_ids], run_id),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def hold_steered_report_jobs(job_ids: List[str], *, run_id: str) -> List[str]:
    """Park report-backs and alerts a live turn is taking in as a steer,
    stamped with its run; returns the ids won.

    The same ``pending`` gate the drainer's claim takes, so a job is either
    steered or claimed, never both. Held, not closed: until the message
    carrying it is checkpointed, a turn that dies loses it, so it stays a
    parked report until ``confirm_steered_report_jobs`` closes it. Only
    while the run is ``in_progress``, its row locked FOR SHARE: the run's
    finalize commits its terminal UPDATE and its release of parked reports
    together, so a hold that commits first is released by that finalize, and
    a finalize that commits first leaves the hold nothing to take.
    """
    if not job_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                WITH owner AS (
                    SELECT 1 FROM conversation_responses
                    WHERE conversation_response_id = %s::uuid
                      AND status = 'in_progress'
                    FOR SHARE
                )
                UPDATE hook_outbox
                SET next_retry_at = 'infinity'::timestamptz,
                    payload = COALESCE(payload, '{{}}'::jsonb)
                        || jsonb_build_object('steered_run_id', %s::text)
                WHERE hook_outbox_id = ANY(%s::uuid[])
                  AND {_STEERABLE_REPORT_SQL}
                  AND EXISTS (SELECT 1 FROM owner)
                RETURNING hook_outbox_id
                """,
                (run_id, run_id, job_ids),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def release_steered_report_jobs(job_ids: List[str], *, run_id: str) -> List[str]:
    """Give back report-backs ``run_id`` holds but will not deliver; returns
    the ids released.

    They stay parked, as the hold left them, so the run's finalize hands them
    to their executors with the rest of the thread's parked reports, and the
    next live turn may take them in again meanwhile."""
    if not job_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox
                SET payload = payload - 'steered_run_id'
                WHERE hook_outbox_id = ANY(%s::uuid[])
                  AND status = 'pending'
                  AND payload->>'steered_run_id' = %s::text
                  AND {_REPORT_TYPES_SQL}
                RETURNING hook_outbox_id
                """,
                (job_ids, run_id),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def confirm_steered_report_jobs(job_ids: List[str], *, run_id: str) -> List[str]:
    """Close the report-backs ``run_id`` holds, once its model step confirms
    the message carrying them is checkpointed; returns the ids won."""
    # No refuse_if_pointer: a held row is parked, so no executor can have
    # claimed it or its run pointer since the turn found no summary of its own.
    return await _close_report_jobs(
        job_ids,
        f"status = 'pending' AND payload->>'steered_run_id' = %s::text"
        f" AND {_REPORT_TYPES_SQL}",
        (run_id,),
        refuse_if_pointer=False,
    )


async def close_unowed_report_jobs(job_ids: List[str]) -> List[str]:
    """Close report-backs nothing is owed for any more, as their executor
    would drop them, while no executor or turn holds them; returns the ids
    won."""
    # An unheld row may have been claimed before, and its run pointer with it.
    return await _close_report_jobs(
        job_ids, _STEERABLE_REPORT_SQL, (), refuse_if_pointer=True
    )


async def _close_report_jobs(
    job_ids: List[str], gate: str, gate_params: tuple, *, refuse_if_pointer: bool
) -> List[str]:
    """An analyst's report closes the way its summary run would: a
    ``watch_clear`` in the same statement releases the pair, at the report's
    own chain position (created_at inherited, as the dead compensation does)
    so it does not queue behind younger jobs. Not an error wake: the live
    turn is the delivery."""
    if not job_ids:
        return []
    clears = _watch_clear_insert_sql(
        "closed",
        suffix="steered_clear",
        error_wake=False,
        refuse_if_pointer=refuse_if_pointer,
    )
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                WITH closed AS (
                    UPDATE hook_outbox
                    SET status = 'done', completed_at = NOW(),
                        next_retry_at = NULL
                    WHERE hook_outbox_id = ANY(%s::uuid[]) AND {gate}
                    RETURNING *
                ),
                clears AS ({clears})
                SELECT hook_outbox_id FROM closed
                """,
                (job_ids, *gate_params),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def get_recent_notification_run_ids(
    thread_id: str,
    hook_type: str | Sequence[str],
    *,
    window_seconds: int = 900,
    limit: int = 10,
) -> List[str]:
    """Dispatched run ids of recently DONE jobs of a type, or of any of
    several, newest first.

    The durable recents ledger behind wake-miss recovery: once a job closes,
    the slice reads idle and only this list can still name the notification
    run to a client that held no subscription when the wake fired. Done rows
    are never purged, so this needs no companion write path.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT payload->>'dispatched_run_id'
                FROM hook_outbox
                WHERE ordering_key = %s
                  AND hook_type = ANY(%s::varchar[])
                  AND status = 'done'
                  AND payload->>'dispatched_run_id' IS NOT NULL
                  AND completed_at > NOW() - make_interval(secs => %s)
                ORDER BY completed_at DESC
                LIMIT %s
                """,
                (thread_id, _hook_types(hook_type), window_seconds, limit),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def requeue_job_with_key(
    job_id: str, *, attempts: int, ordering_key: str, max_attempts: int = 5
) -> Optional[str]:
    """Release a CLAIMED job back to pending under its correct ordering key.

    For a job claimed under a stale key (pre-deploy row that finalized
    unstamped): executing it there would busy-wait concurrently with the
    real chain and can permanently drop its effect at the wait cap, while
    a requeued row simply waits its turn on the chain. Chain position is
    claimed at REQUEUE time (``created_at`` bumps to now): inheriting the
    source chain's timestamp would let an older teardown overtake a
    same-key job that already started — the moment its live lease lapses,
    FIFO would select the older row as head mid-effect. Fenced by
    ``attempts``; immediately due. At the attempts ceiling the job is
    parked dead instead (with its watch_clear compensation) — a pending
    row is claimable without a ceiling, so requeueing there would mint
    attempt max+1 and reuse a fence token the park sweep considers spent.
    Returns the new status ('pending' or 'dead'), or None if the fence was
    lost. The caller must stop executing either way — its fenced ack will
    no-op against the transitioned row.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH bumped AS (
                    UPDATE hook_outbox
                    SET status = CASE
                            WHEN attempts >= %s THEN 'dead' ELSE 'pending'
                        END,
                        ordering_key = %s,
                        created_at = clock_timestamp(),
                        lease_expires_at = NULL, next_retry_at = NOW(),
                        completed_at = CASE
                            WHEN attempts >= %s THEN NOW() ELSE completed_at
                        END
                    WHERE hook_outbox_id = %s AND status = 'claimed'
                      AND attempts = %s
                    RETURNING *
                ),
                dead_rows AS (
                    SELECT * FROM bumped WHERE status = 'dead'
                ),
                comp AS ({_DEAD_REPORT_BACK_COMPENSATION_SQL})
                SELECT status FROM bumped
                """,
                (max_attempts, ordering_key, max_attempts, job_id, attempts),
            )
            row = await cur.fetchone()
            return row["status"] if row else None


async def merge_job_payload(
    job_id: str, patch: Dict[str, Any], *, remove: Optional[List[str]] = None, conn=None
) -> bool:
    """Durably merge keys into a job's payload (e.g. dispatched_run_id after
    the synthetic POST) so a reclaiming drainer resumes instead of re-doing
    the effect. ``remove`` drops keys in the SAME update (a task
    report-back scrubs its inlined result text the moment the dispatched
    run id lands — one atomic step, so a crash can never leave neither).
    Deliberately unfenced: a stale owner's merge carries the same
    request-key-deduped run id the live owner would write — the information
    is true even after the lease was lost. ``conn`` joins the caller's
    transaction, for a merge that must commit with the effect it records."""
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET payload =
                    (COALESCE(payload, '{}'::jsonb) || %s::jsonb) - %s::text[]
                WHERE hook_outbox_id = %s
                """,
                (SafeJson(patch), list(remove or []), job_id),
            )
            return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Steers: a message for a thread's (or a task's) live agent, from the user or
# the Chief of Staff. One ``steer`` row per message, parked at insert
# (``next_retry_at='infinity'``). The next model call on the thread holds it
# (``steered_run_id``) and the step that reads it confirms it; a row no turn
# takes becomes due (``due_at``, see ``_STEER_DUE_SQL``) and its executor
# starts a turn with it. Every ending
# writes an ``outcome`` and closes the row, so a row without one is live.
# ---------------------------------------------------------------------------

# A steer still owed to someone.
_LIVE_STEER_SQL = (
    "({a}.hook_type = 'steer' AND {a}.status IN ('pending', 'claimed')"
    " AND NOT ({a}.payload ? 'outcome'))"
)

# Held by a run or task run that is still going: already in that agent's
# messages, waiting for the step that reads it.
_STEER_HELD_SQL = """({a}.payload ? 'steered_run_id' AND (
    EXISTS (
        SELECT 1 FROM conversation_responses hr
        WHERE hr.conversation_response_id = ({a}.payload->>'steered_run_id')::uuid
          AND hr.status = 'in_progress'
    )
    OR EXISTS (
        SELECT 1 FROM subagent_runs hs
        WHERE hs.task_run_id = ({a}.payload->>'steered_run_id')::uuid
          AND hs.status = 'in_progress'
    )
))"""

# The run that took a steer in, as text, else NULL: the turn it became
# (``claim_steer_for_turn``, or its executor's ``finish_claimed_steer``), the
# run that read it (``confirm_jobs``), or a running turn holding it for its
# next model call. A hold counts as taken, but a release or its run's ending
# without reading it hands the steer back, so taken can turn queued again.
_STEER_TAKER_SQL = (
    "(CASE WHEN {a}.payload->>'outcome' = 'turn'"
    " THEN {a}.payload->>'dispatched_run_id'"
    " WHEN {a}.payload->>'outcome' = 'delivered'"
    " THEN {a}.payload->>'steered_run_id'"
    " WHEN " + _LIVE_STEER_SQL + " AND " + _STEER_HELD_SQL
    + " THEN {a}.payload->>'steered_run_id' END)"
)

# What a live agent may take in: live, held by no live agent, not yet the input
# of a turn of its own. A claimed row is taken from its executor (whose fenced
# writes then miss). A chat-app steer belongs to the gateway, which resubmits
# what its run missed, so only the run it was sent into takes it.
# A hand-off from one Home thread is never taken by a run that reports back
# to another: that run's report would go to the wrong Home.
_TAKEABLE_STEER_SQL = (
    "(" + _LIVE_STEER_SQL + " AND NOT " + _STEER_HELD_SQL
    + " AND NOT ({a}.payload ? 'dispatched_run_id')"
    " AND (NOT COALESCE(({a}.payload->>'chat_app')::boolean, false)"
    " OR {a}.run_id = %(run_id)s::uuid)"
    " AND NOT EXISTS (SELECT 1 FROM conversation_responses fr"
    " WHERE fr.conversation_response_id = %(run_id)s::uuid"
    " AND fr.metadata->>'origin_home_thread_id'"
    " <> {a}.payload->'origin'->>'origin_home_thread_id'))"
)


def _steer_sql(template: str, alias: str = "o") -> str:
    return template.replace("{a}", alias)


# A row's target. A task's steers move onto the thread's chain when the task
# run ends (to resume it), so the chain alone does not tell them apart.
_TARGET_SQL = "COALESCE({a}.payload->>'target', 'thread')"


def steer_ordering_key(thread_id: str, task_id: Optional[str] = None) -> str:
    """A steer's chain: the thread's own, apart from its report-backs so a
    parked steer never blocks one, or the task's."""
    if task_id:
        return f"{thread_id}:task:{task_id}"
    return f"{thread_id}:steer"


def _job_dict(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    job = dict(row)
    for key in ("id", "run_id"):
        if job.get(key) is not None:
            job[key] = str(job[key])
    return job


_STEER_JOB_COLUMNS = (
    "hook_outbox_id AS id, status, attempts, run_id, payload, created_at"
)


async def get_steer_job(idempotency_key: str, conn=None) -> Optional[Dict[str, Any]]:
    """A steer row by its key: ``{id, status, attempts, run_id, payload,
    created_at}``."""
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_STEER_JOB_COLUMNS} FROM hook_outbox"
                " WHERE idempotency_key = %s AND hook_type = 'steer'",
                (idempotency_key,),
            )
            return _job_dict(await cur.fetchone())


async def _admit(
    cur,
    *,
    owner_sql: str,
    params: Dict[str, Any],
    position_key: str,
    target: str,
) -> Dict[str, Any]:
    """Insert a parked steer only while its owner (``owner_sql``, locked FOR
    SHARE) takes steers; the owner's ending and its cancel stamp both lock
    the row first, so either sees the steer or the steer sees them."""
    await cur.execute(
        f"""
        WITH owner AS ({owner_sql} FOR SHARE)
        INSERT INTO hook_outbox (
            run_id, conversation_thread_id, hook_type, payload,
            ordering_key, idempotency_key, next_retry_at
        )
        SELECT owner.run_id, %(thread_id)s::uuid, 'steer', %(payload)s,
               %(ordering_key)s, %(idempotency_key)s, 'infinity'::timestamptz
        FROM owner
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING hook_outbox_id
        """,
        params,
    )
    inserted = await cur.fetchone()
    if inserted is not None:
        await cur.execute(
            f"""
            SELECT COUNT(*) AS n FROM hook_outbox o
            WHERE o.ordering_key = %s AND {_steer_sql(_LIVE_STEER_SQL)}
              AND {_steer_sql(_TARGET_SQL)} = %s
            """,
            (position_key, target),
        )
        position = (await cur.fetchone())["n"]
        return {
            "state": "accepted",
            "job_id": str(inserted["hook_outbox_id"]),
            "position": int(position),
        }
    existing = await get_steer_job(params["idempotency_key"], conn=cur.connection)
    if existing is not None:
        return {"state": "duplicate", "job": existing}
    return {"state": "not_running"}


async def admit_steer(
    *,
    thread_id: str,
    run_id: str,
    payload: Dict[str, Any],
    idempotency_key: str,
    origin_stamp: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Accept a steer into ``run_id``, the thread's live run, in one
    transaction.

    ``{"state": "accepted", "job_id", "position"}``, ``{"state": "duplicate",
    "job"}`` for a key already accepted, ``{"state": "owned_elsewhere"}`` when
    ``origin_stamp`` names a Home thread other than the one the run reports
    to, else ``{"state": "not_running"}`` (the run ended or was stopped).
    ``origin_stamp`` (``origin_home_thread_id``, ``origin_dispatch_gen``)
    stamps the run so its report-back goes to the steer's sender. Both the
    new generation and the one it displaces go into ``dispatch_gens``, so the
    orphan resolver finds each admitted.
    """
    ordering_key = steer_ordering_key(thread_id)
    params = {
        "thread_id": thread_id,
        "run_id": run_id,
        "payload": SafeJson(payload),
        "ordering_key": ordering_key,
        "idempotency_key": idempotency_key,
    }
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                if origin_stamp is not None:
                    existing = await get_steer_job(idempotency_key, conn=conn)
                    if existing is not None:
                        return {"state": "duplicate", "job": existing}
                    owner_home = str(origin_stamp["origin_home_thread_id"])
                    gen = str(origin_stamp["origin_dispatch_gen"])
                    await cur.execute(
                        """
                        UPDATE conversation_responses
                        SET metadata = COALESCE(metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'origin_home_thread_id', %(home)s::text,
                                'origin_dispatch_gen', %(gen)s::text,
                                'dispatch_gens',
                                (COALESCE(metadata->'dispatch_gens', '[]'::jsonb)
                                    - %(gen)s::text
                                    - COALESCE(metadata->>'origin_dispatch_gen', ''))
                                || CASE
                                    WHEN metadata->>'origin_dispatch_gen' IS NULL
                                      OR metadata->>'origin_dispatch_gen' = %(gen)s
                                    THEN '[]'::jsonb
                                    ELSE jsonb_build_array(
                                        metadata->>'origin_dispatch_gen')
                                   END
                                || jsonb_build_array(%(gen)s::text)
                            )
                        WHERE conversation_response_id = %(run_id)s::uuid
                          AND conversation_thread_id = %(thread_id)s::uuid
                          AND status = 'in_progress'
                          AND cancel_requested_at IS NULL
                          AND (metadata->>'origin_home_thread_id' IS NULL
                               OR metadata->>'origin_home_thread_id' = %(home)s)
                        RETURNING conversation_response_id
                        """,
                        {
                            "home": owner_home,
                            "gen": gen,
                            "run_id": run_id,
                            "thread_id": thread_id,
                        },
                    )
                    if await cur.fetchone() is None:
                        await cur.execute(
                            """
                            SELECT 1 FROM conversation_responses
                            WHERE conversation_response_id = %s::uuid
                              AND conversation_thread_id = %s::uuid
                              AND status = 'in_progress'
                              AND cancel_requested_at IS NULL
                            """,
                            (run_id, thread_id),
                        )
                        if await cur.fetchone() is not None:
                            return {"state": "owned_elsewhere"}
                        return {"state": "not_running"}
                return await _admit(
                    cur,
                    owner_sql="""
                        SELECT conversation_response_id AS run_id
                        FROM conversation_responses
                        WHERE conversation_response_id = %(run_id)s::uuid
                          AND conversation_thread_id = %(thread_id)s::uuid
                          AND status = 'in_progress'
                          AND cancel_requested_at IS NULL
                    """,
                    params=params,
                    position_key=ordering_key,
                    target="thread",
                )


async def admit_task_steer(
    *,
    thread_id: str,
    task_id: str,
    task_run_id: str,
    parent_run_id: Optional[str],
    payload: Dict[str, Any],
    idempotency_key: str,
) -> Dict[str, Any]:
    """Accept a steer into a background task's live run; same answers as
    ``admit_steer`` less ``owned_elsewhere``. The row hangs off the run that
    launched the task, as a task's report-back does."""
    ordering_key = steer_ordering_key(thread_id, task_id)
    params = {
        "thread_id": thread_id,
        "task_id": task_id,
        "task_run_id": task_run_id,
        "parent_run_id": parent_run_id,
        "payload": SafeJson(payload),
        "ordering_key": ordering_key,
        "idempotency_key": idempotency_key,
    }
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _admit(
                    cur,
                    owner_sql="""
                        SELECT COALESCE(%(parent_run_id)s::uuid, parent_run_id)
                            AS run_id
                        FROM subagent_runs
                        WHERE task_run_id = %(task_run_id)s::uuid
                          AND thread_id = %(thread_id)s::uuid
                          AND task_id = %(task_id)s
                          AND status = 'in_progress'
                          AND cancel_requested_at IS NULL
                          AND COALESCE(%(parent_run_id)s::uuid, parent_run_id)
                              IS NOT NULL
                    """,
                    params=params,
                    position_key=ordering_key,
                    target="task",
                )


async def steer_key_taken(
    conn, request_key: str, *, except_job_id: Optional[str] = None
) -> bool:
    """Whether a send under ``request_key`` was accepted as a steer, other
    than the steer ``except_job_id`` (the one a server-posted turn carries)."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM hook_outbox
                WHERE idempotency_key = 'steer:' || %s
                  AND hook_type = 'steer'
                  AND (%s::uuid IS NULL OR hook_outbox_id <> %s::uuid)
            )
            """,
            (request_key, except_job_id, except_job_id),
        )
        row = await cur.fetchone()
        return bool(row and row[0])


def _closed_steer_set(outcome_sql: str = "%(outcome)s::text") -> str:
    """The SET list that closes a steer with an outcome; the hold goes with
    it unless the outcome is a delivery, which names the run that read it."""
    return f"""
        status = 'done', completed_at = NOW(), next_retry_at = NULL,
        lease_expires_at = NULL,
        payload = CASE WHEN {outcome_sql} = 'delivered' THEN o.payload
                       ELSE o.payload - 'steered_run_id' END
            || jsonb_build_object('outcome', {outcome_sql})
    """


async def claim_steer_for_turn(conn, job_id: str, run_id: str) -> bool:
    """Make a parked steer the input of ``run_id``'s turn, in START's
    transaction. False when a live agent holds it or it already ended,
    unless it already is this run's input (a retried START)."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE hook_outbox o
            SET {_closed_steer_set("'turn'")}
                || jsonb_build_object('dispatched_run_id', %(run_id)s::text)
            WHERE o.hook_outbox_id = %(job_id)s::uuid
              AND {_steer_sql(_LIVE_STEER_SQL)}
              AND NOT ({_steer_sql(_STEER_HELD_SQL)}
                       AND o.payload->>'steered_run_id' <> %(run_id)s::text)
            RETURNING o.hook_outbox_id
            """,
            {"job_id": job_id, "run_id": run_id},
        )
        if await cur.fetchone() is not None:
            return True
        await cur.execute(
            """
            SELECT 1 FROM hook_outbox
            WHERE hook_outbox_id = %s::uuid AND hook_type = 'steer'
              AND payload->>'outcome' = 'turn'
              AND payload->>'dispatched_run_id' = %s::text
            """,
            (job_id, run_id),
        )
        return await cur.fetchone() is not None


# When a run's input was sent, which steers created before go ahead of: the
# steer the run was started for, else the message an edit's input replaces,
# else the run's own start. ``{r}`` is the run's conversation_responses row.
_INPUT_SENT_AT_SQL = """COALESCE(
    (SELECT min(t.created_at) FROM hook_outbox t
     WHERE t.ordering_key = {okey}
       AND t.hook_type = 'steer'
       AND t.payload->>'outcome' = 'turn'
       AND t.payload->>'dispatched_run_id' = {r}.conversation_response_id::text),
    ({r}.metadata->>'input_sent_at')::timestamptz,
    {r}.created_at
)"""

# Steer payload keys that belong to one pass through a run, not to the steer.
_STEER_PASS_KEYS = (
    "outcome", "steered_run_id", "dispatched_run_id", "returned_run_id",
    "due_at", "kept_by_stop", "exhausted",
)


async def lift_fork_steers(
    conn, thread_id: str, from_turn: int, *, edit: bool
) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """Take out of a fork's truncation the thread steers it must keep, in
    START's transaction, for ``land_fork_steers`` to put back under the
    fork's run once its row is in.

    Truncation deletes the rewound runs, and with them, by cascade, every row
    they own. Kept are the steers still owed to the thread whose owner is
    rewound (sent during a rewound turn, parked by its error or its stop, or
    due for a turn of their own), which the fork's turn reads after its
    input, as a Retry would. An edit also keeps the steers the edited turn
    read ahead of its input: they were sent before the message the edit
    replaces, and its turn opens with them again. A regenerate keeps those in
    the input checkpoint it starts over from. Returns the rows and, for an
    edit, when the replaced message was sent, the edit's ``input_sent_at``.
    A chat-app steer is never owed past its run's end; task steers go with
    their task's turn."""
    okey = steer_ordering_key(thread_id)
    params = {"thread_id": thread_id, "from_turn": from_turn, "okey": okey}
    async with conn.cursor(row_factory=dict_row) as cur:
        input_sent_at = None
        if edit:
            await cur.execute(
                f"""
                SELECT min({_INPUT_SENT_AT_SQL.format(okey="%(okey)s", r="r")}) AS at
                FROM conversation_responses r
                WHERE r.conversation_thread_id = %(thread_id)s::uuid
                  AND r.turn_index = %(from_turn)s
                """,
                params,
            )
            input_sent_at = (await cur.fetchone())["at"]
        await cur.execute(
            f"""
            DELETE FROM hook_outbox o
            USING (
                SELECT array_agg(conversation_response_id) AS ids,
                       array_agg(conversation_response_id::text)
                           FILTER (WHERE turn_index = %(from_turn)s) AS edited
                FROM conversation_responses
                WHERE conversation_thread_id = %(thread_id)s::uuid
                  AND turn_index >= %(from_turn)s
            ) rewound
            WHERE o.ordering_key = %(okey)s
              AND o.hook_type = 'steer'
              AND {_steer_sql(_TARGET_SQL)} = 'thread'
              AND NOT COALESCE((o.payload->>'chat_app')::boolean, false)
              AND (
                  ({_steer_sql(_LIVE_STEER_SQL)} AND o.run_id = ANY(rewound.ids))
                  OR (o.payload->>'outcome' = 'delivered'
                      AND o.payload->>'steered_run_id' = ANY(rewound.edited)
                      AND o.created_at < %(input_sent_at)s::timestamptz)
              )
            RETURNING o.hook_outbox_id, o.conversation_thread_id, o.payload,
                      o.ordering_key, o.idempotency_key, o.created_at
            """,
            {**params, "input_sent_at": input_sent_at},
        )
        rows = [dict(r) for r in await cur.fetchall()]
    return rows, input_sent_at


async def land_fork_steers(conn, rows: List[Dict[str, Any]], run_id: str) -> None:
    """Put back the steers ``lift_fork_steers`` took out, under the fork's
    run, parked for its turn to take in. Each keeps its id, key and sending
    time, which places it against the turn's input."""
    if not rows:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO hook_outbox (
                hook_outbox_id, run_id, conversation_thread_id, hook_type,
                payload, ordering_key, idempotency_key, status, next_retry_at,
                created_at
            )
            VALUES (%s, %s, %s, 'steer', %s, %s, %s, 'pending',
                    'infinity'::timestamptz, %s)
            """,
            [
                (
                    row["hook_outbox_id"],
                    run_id,
                    row["conversation_thread_id"],
                    SafeJson({
                        k: v
                        for k, v in (row["payload"] or {}).items()
                        if k not in _STEER_PASS_KEYS
                    }),
                    row["ordering_key"],
                    row["idempotency_key"],
                    row["created_at"],
                )
                for row in rows
            ],
        )


async def list_takeable_jobs(
    thread_id: str,
    hook_types: List[str],
    *,
    target: str = "thread",
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    """The rows of ``hook_types`` a live agent on the thread may take in,
    oldest first: report-backs as ``list_steerable_report_jobs`` reads them,
    and steers for ``target`` on the task's chain when ``task_id`` is given,
    else on the thread's, that no live agent holds. ``run_id`` is the taking run: it alone takes its chat-app steers,
    and ``before_run`` marks the rows created before its input was sent
    (``_INPUT_SENT_AT_SQL``), so a turn made of one missed steer reads the
    older ones first and the newer ones after. ``limit=None`` reads them all."""
    report_types = [t for t in hook_types if t != "steer"]
    rows: List[Dict[str, Any]] = []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            if report_types:
                await cur.execute(
                    f"""
                    SELECT hook_outbox_id, hook_type, run_id, payload, created_at,
                           false AS before_run
                    FROM hook_outbox
                    WHERE ordering_key = %s
                      AND hook_type = ANY(%s::varchar[])
                      AND {_STEERABLE_REPORT_SQL}
                    ORDER BY created_at, hook_outbox_id
                    LIMIT %s
                    """,
                    (str(thread_id), report_types, limit),
                )
                rows.extend(dict(r) for r in await cur.fetchall())
            if "steer" in hook_types:
                okey = steer_ordering_key(thread_id, task_id)
                await cur.execute(
                    f"""
                    SELECT o.hook_outbox_id, o.hook_type, o.run_id, o.payload,
                           o.created_at,
                           COALESCE(o.created_at < (
                               SELECT {_INPUT_SENT_AT_SQL.format(okey="%(okey)s", r="r")}
                               FROM conversation_responses r
                               WHERE r.conversation_response_id = %(run_id)s::uuid
                           ), false) AS before_run
                    FROM hook_outbox o
                    WHERE o.ordering_key = %(okey)s
                      AND {_steer_sql(_TAKEABLE_STEER_SQL)}
                      AND {_steer_sql(_TARGET_SQL)} = %(target)s
                    ORDER BY o.created_at, o.hook_outbox_id
                    LIMIT %(limit)s
                    """,
                    {
                        "okey": okey,
                        "run_id": run_id,
                        "limit": limit,
                        "target": target,
                    },
                )
                rows.extend(dict(r) for r in await cur.fetchall())
    return rows


def _owner_sql(owner: Tuple[str, str]) -> str:
    """The owner's row, while it may take a steer in."""
    kind, _ = owner
    if kind == "run":
        return """
            SELECT 1 FROM conversation_responses
            WHERE conversation_response_id = %(owner)s::uuid
              AND status = 'in_progress'
              AND cancel_requested_at IS NULL
        """
    if kind == "task":
        return """
            SELECT 1 FROM subagent_runs
            WHERE task_run_id = %(owner)s::uuid
              AND status = 'in_progress'
              AND cancel_requested_at IS NULL
        """
    raise ValueError(f"unknown steer owner kind {kind!r}")


async def hold_jobs(job_ids: List[str], *, owner: Tuple[str, str]) -> List[str]:
    """Hold rows a live agent is taking in; returns the ids won.

    ``owner`` is ``("run", run_id)`` or ``("task", task_run_id)``, locked
    while it is in progress and, for a steer, not stopped: its ending and its
    cancel stamp both settle the rows it holds, so a hold that commits first
    is settled with them, and one that waits behind them takes nothing. A run
    taking a Chief of Staff hand-off is stamped with its report-back origin
    in the same transaction, as ``admit_steer`` stamps the run it was
    sent into, so the run that reads it is the one that reports back. A
    report-back is held as ``hold_steered_report_jobs`` holds it."""
    if not job_ids:
        return []
    kind, owner_id = owner
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                home = None
                if kind == "run":
                    home = await _hand_off_home(cur, job_ids, owner_id)
                await cur.execute(
                    f"""
                    WITH owner AS ({_owner_sql(owner)} FOR SHARE)
                    UPDATE hook_outbox o
                    SET status = 'pending',
                        next_retry_at = 'infinity'::timestamptz,
                        lease_expires_at = NULL,
                        payload = (o.payload - 'due_at')
                            || jsonb_build_object('steered_run_id', %(owner)s::text)
                    WHERE o.hook_outbox_id = ANY(%(ids)s::uuid[])
                      AND {_steer_sql(_TAKEABLE_STEER_SQL)}
                      AND (%(home)s::text IS NULL OR COALESCE(
                          o.payload->'origin'->>'origin_home_thread_id',
                          %(home)s::text
                      ) = %(home)s::text)
                      AND EXISTS (SELECT 1 FROM owner)
                    RETURNING o.hook_outbox_id,
                              o.payload->'origin'->>'origin_dispatch_gen' AS gen
                    """,
                    {
                        "owner": owner_id,
                        "ids": job_ids,
                        "run_id": owner_id if kind == "run" else None,
                        "home": home,
                    },
                )
                rows = await cur.fetchall()
                won = [str(r["hook_outbox_id"]) for r in rows]
                gens = [r["gen"] for r in rows if r["gen"]]
                if home and gens:
                    await _stamp_hand_off_origin(cur, owner_id, home, gens)
    rest = [j for j in job_ids if j not in won]
    if kind == "run" and rest:
        won += await hold_steered_report_jobs(rest, run_id=owner_id)
    return won


async def _hand_off_home(cur, job_ids: List[str], run_id: str) -> Optional[str]:
    """The Home thread whose hand-offs ``run_id`` takes from ``job_ids``, its
    row locked against a concurrent stamp: the one it already reports back
    to, else the sender of the oldest takeable hand-off. A run takes only one
    Home's hand-offs, since it reports back to one thread."""
    await cur.execute(
        """
        SELECT metadata->>'origin_home_thread_id' AS home
        FROM conversation_responses
        WHERE conversation_response_id = %s::uuid
          AND status = 'in_progress'
          AND cancel_requested_at IS NULL
        FOR UPDATE
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    if row["home"]:
        return row["home"]
    await cur.execute(
        f"""
        SELECT o.payload->'origin'->>'origin_home_thread_id' AS home
        FROM hook_outbox o
        WHERE o.hook_outbox_id = ANY(%(ids)s::uuid[])
          AND {_steer_sql(_TAKEABLE_STEER_SQL)}
          AND o.payload->'origin'->>'origin_home_thread_id' IS NOT NULL
          AND o.payload->'origin'->>'origin_dispatch_gen' IS NOT NULL
        ORDER BY o.created_at, o.hook_outbox_id
        LIMIT 1
        """,
        {"ids": job_ids, "run_id": run_id},
    )
    row = await cur.fetchone()
    return row["home"] if row else None


async def _stamp_hand_off_origin(
    cur, run_id: str, home: str, gens: List[str]
) -> None:
    """Record the generations of the hand-offs ``run_id`` took as admitted
    on it. A run that already reports to ``home`` keeps its own generation,
    which was reserved after any hand-off still parked; one that reported
    nowhere takes the newest."""
    await cur.execute(
        """
        UPDATE conversation_responses
        SET metadata = COALESCE(metadata, '{}'::jsonb)
            || jsonb_build_object(
                'origin_home_thread_id', %(home)s::text,
                'origin_dispatch_gen',
                COALESCE(metadata->>'origin_dispatch_gen', %(latest)s::text),
                'dispatch_gens',
                (COALESCE(metadata->'dispatch_gens', '[]'::jsonb) - %(gens)s::text[])
                || to_jsonb(%(gens)s::text[])
            )
        WHERE conversation_response_id = %(run_id)s::uuid
        """,
        {"home": home, "latest": gens[-1], "gens": gens, "run_id": run_id},
    )


async def release_jobs(job_ids: List[str], *, owner: Tuple[str, str]) -> List[str]:
    """Give back rows ``owner`` holds but will not deliver; they stay parked
    for the next live agent, or for the owner's ending to settle."""
    if not job_ids:
        return []
    kind, owner_id = owner
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET payload = o.payload - 'steered_run_id'
                WHERE o.hook_outbox_id = ANY(%s::uuid[])
                  AND o.status = 'pending'
                  AND o.payload->>'steered_run_id' = %s::text
                  AND {_steer_sql(_LIVE_STEER_SQL)}
                RETURNING o.hook_outbox_id
                """,
                (job_ids, owner_id),
            )
            won = [str(r[0]) for r in await cur.fetchall()]
    rest = [j for j in job_ids if j not in won]
    if kind == "run" and rest:
        won += await release_steered_report_jobs(rest, run_id=owner_id)
    return won


_CONFIRMED_OUTCOME_SQL = (
    "(CASE WHEN o.payload ? 'resumed_by' THEN 'resumed'"
    " ELSE %(outcome)s::text END)"
)


async def confirm_jobs(
    job_ids: List[str], *, owner: Tuple[str, str], outcome: str = "delivered"
) -> List[str]:
    """Close the rows ``owner`` holds once the step reading them has begun
    (the message carrying them is checkpointed); returns the ids won. A steer
    keeps ``steered_run_id``, the run that read it, unless a resume handed it
    to that run (``hand_off_task_resumes``), which closes it ``resumed``; a
    report-back closes as ``confirm_steered_report_jobs`` closes it."""
    if not job_ids:
        return []
    kind, owner_id = owner
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET {_closed_steer_set(_CONFIRMED_OUTCOME_SQL)}
                WHERE o.hook_outbox_id = ANY(%(ids)s::uuid[])
                  AND o.status = 'pending'
                  AND o.payload->>'steered_run_id' = %(owner)s::text
                  AND {_steer_sql(_LIVE_STEER_SQL)}
                RETURNING o.hook_outbox_id
                """,
                {"ids": job_ids, "owner": owner_id, "outcome": outcome},
            )
            won = [str(r[0]) for r in await cur.fetchall()]
    rest = [j for j in job_ids if j not in won]
    if kind == "run" and rest:
        won += await confirm_steered_report_jobs(rest, run_id=owner_id)
    return won


async def pending_steer_senders(
    thread_id: str, run_id: Optional[str] = None
) -> list[str]:
    """Who sent the thread steers waiting that ``run_id`` would take in, by
    their first steer: what wakes a turn waiting on its background tasks, and
    what the trigger that wakes it names."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COALESCE(o.payload->>'sender', 'user') AS sender
                FROM hook_outbox o
                WHERE o.ordering_key = %(okey)s
                  AND {_steer_sql(_TAKEABLE_STEER_SQL)}
                  AND {_steer_sql(_TARGET_SQL)} = 'thread'
                GROUP BY 1
                ORDER BY MIN(o.created_at), MIN(o.hook_outbox_id::text)
                """,
                {"okey": steer_ordering_key(thread_id), "run_id": run_id},
            )
            return [str(row[0]) for row in await cur.fetchall()]


async def has_live_hand_off(ptc_thread_id: str, home_thread_id: str) -> bool:
    """Whether a hand-off from ``home_thread_id`` into the analyst's thread is
    still owed: the run that reads it reports back, so a report owed for an
    earlier run waits until it settles."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM hook_outbox o
                    WHERE o.ordering_key = %s
                      AND {_steer_sql(_LIVE_STEER_SQL)}
                      AND {_steer_sql(_TARGET_SQL)} = 'thread'
                      AND o.payload->'origin'->>'origin_home_thread_id' = %s
                )
                """,
                (steer_ordering_key(ptc_thread_id), home_thread_id),
            )
            row = await cur.fetchone()
            return bool(row and row[0])


async def finish_claimed_steer(
    job_id: str, *, attempts: int, outcome: str, patch: Optional[Dict[str, Any]] = None
) -> bool:
    """Close a steer its executor holds, fenced to the executor's lease: a
    live agent that took the row since leaves this a no-op."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET {_closed_steer_set()} || %(patch)s::jsonb
                WHERE o.hook_outbox_id = %(job_id)s::uuid
                  AND o.status = 'claimed' AND o.attempts = %(attempts)s
                  AND {_steer_sql(_LIVE_STEER_SQL)}
                """,
                {
                    "job_id": job_id,
                    "attempts": attempts,
                    "outcome": outcome,
                    "patch": SafeJson(patch or {}),
                },
            )
            return cur.rowcount > 0


# A thread's steers, for settling at its run's ending or stop.
_THREAD_STEERS_SQL = (
    "o.ordering_key = %(okey)s AND "
    + _steer_sql(_LIVE_STEER_SQL)
    + " AND "
    + _steer_sql(_TARGET_SQL)
    + " = 'thread'"
)
# A steer from a channel gateway (``steer_payload``'s ``chat_app``), not one
# from a surface langalpha draws, which also carries a ``platform``.
_IS_PLATFORM_SQL = "COALESCE((o.payload->>'chat_app')::boolean, false)"
_IS_USER_SQL = "COALESCE(o.payload->>'sender', 'user') = 'user'"
# A hand-off: an agent's steer to the thread.
_HAND_OFF_STEER_SQL = (
    "(o.hook_type = 'steer' AND " + _steer_sql(_TARGET_SQL) + " = 'thread'"
    " AND NOT " + _IS_USER_SQL + ")"
)
# A user's steer that a stop answered without (``keep_unanswered_steers``).
_KEPT_BY_STOP_SQL = (
    "COALESCE(o.payload->>'kept_by_stop' = %(run_id)s::text, false)"
)


async def _end_thread_steers(
    cur, params: Dict[str, Any], where: str, outcome: str
) -> List[Dict[str, Any]]:
    await cur.execute(
        f"""
        UPDATE hook_outbox o
        SET {_closed_steer_set()}
            || CASE WHEN %(outcome)s::text = 'returned'
                    THEN jsonb_build_object('returned_run_id', %(run_id)s::text)
                    ELSE '{{}}'::jsonb END
        WHERE {_THREAD_STEERS_SQL} AND {where}
        RETURNING o.payload, o.created_at
        """,
        {**params, "outcome": outcome},
    )
    return [dict(r) for r in await cur.fetchall()]


async def _repark_thread_steers(
    cur,
    params: Dict[str, Any],
    where: str,
    *,
    due: bool,
    claimed_too: bool = False,
    exhausted: bool = False,
) -> None:
    """Drop the holds and set when the rows come due: now (``due_at``), or
    never on their own (parked, for the next turn to take in). Either way
    ``next_retry_at`` stays ``infinity`` (see ``_STEER_DUE_SQL``).
    ``claimed_too`` takes a row back from an executor that has not posted it
    yet. ``exhausted`` keeps the row from the claim safety net."""
    statuses = "('pending', 'claimed')" if claimed_too else "('pending')"
    unheld = "o.payload - 'steered_run_id'"
    await cur.execute(
        f"""
        UPDATE hook_outbox o
        SET status = 'pending', lease_expires_at = NULL,
            next_retry_at = 'infinity'::timestamptz,
            payload = CASE WHEN %(due)s
                           THEN {_steer_due_at_sql(unheld, "NOW()")}
                           ELSE {unheld} - 'due_at' END
                || CASE WHEN %(exhausted)s
                        THEN jsonb_build_object('exhausted', true)
                        ELSE '{{}}'::jsonb END
        WHERE {_THREAD_STEERS_SQL} AND {where}
          AND o.status IN {statuses}
          AND NOT (o.payload ? 'dispatched_run_id')
        """,
        {**params, "due": due, "exhausted": exhausted},
    )


async def settle_run_steers(
    conn,
    *,
    thread_id: str,
    run_id: str,
    final_status: str,
    user_cancel: bool,
    agent_stop: bool = False,
) -> None:
    """Settle the thread's live steers in the finalize transaction of its run.

    A run that ran out (completed, or cancelled by anything but the user)
    leaves them due, to become the next turn; a chat-app steer goes back to
    the gateway, which resubmits it. An error keeps the user's for whatever
    the user does next, since a turn started for them would fail the same way
    and make the error's Retry stale, and drops the agents' (their senders
    hear of the error from its report-back). A stop by an agent settles as an
    error does: no one is there to take the user's back, and a turn started
    for them would resume the work the agent stopped. A user stop returns the
    user's and drops the rest, as the cancel stamp did. An interrupt keeps
    them for the resume, but a chat-app steer goes back to the gateway: only
    the run it was sent into takes it, and the resume is another run. Task
    steers are their task's to settle."""
    params = {"okey": steer_ordering_key(thread_id), "run_id": run_id}
    async with conn.cursor(row_factory=dict_row) as cur:
        if final_status == "interrupted":
            await _end_thread_steers(cur, params, _IS_PLATFORM_SQL, "returned")
            await _repark_thread_steers(cur, params, "true", due=False)
        elif final_status == "error" or (final_status == "cancelled" and agent_stop):
            await _end_thread_steers(cur, params, _IS_PLATFORM_SQL, "returned")
            await _end_thread_steers(cur, params, f"NOT {_IS_USER_SQL}", "dropped")
            await _repark_thread_steers(
                cur, params, _IS_USER_SQL, due=False, claimed_too=True
            )
        elif final_status == "cancelled" and user_cancel:
            await _end_thread_steers(
                cur, params, f"{_IS_USER_SQL} AND NOT ({_KEPT_BY_STOP_SQL})", "returned"
            )
            await _end_thread_steers(cur, params, f"NOT ({_KEPT_BY_STOP_SQL})", "dropped")
            await _repark_thread_steers(cur, params, _KEPT_BY_STOP_SQL, due=False)
        else:
            await _end_thread_steers(cur, params, _IS_PLATFORM_SQL, "returned")
            await _repark_thread_steers(cur, params, "true", due=True)


async def _exhaust_steers(
    cur, where: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Settle the live steers matching ``where`` that ran out of attempts, in
    the caller's transaction, as an errored turn settles its own: a chat-app
    steer goes back to its gateway, a task's closes as its stopped run would
    close it, an agent's is dropped, and the user's parks for the user's next
    move. An accepted steer must end with an outcome, never dead. The user's
    keeps ``exhausted`` for good: it only holds the row from the safety net,
    which would hand it straight back to the executor that gave up on it; a
    run's ending still makes it due, and its one more claim either posts the
    turn or exhausts it again. Needs a ``dict_row`` cursor; returns the rows
    as settled."""
    await cur.execute(
        f"""
        SELECT o.hook_outbox_id, o.ordering_key, o.run_id FROM hook_outbox o
        WHERE {_steer_sql(_LIVE_STEER_SQL)} AND {where}
        ORDER BY o.created_at, o.hook_outbox_id
        FOR UPDATE
        """,
        params,
    )
    rows = [dict(r) for r in await cur.fetchall()]
    if not rows:
        return []
    mine = "o.hook_outbox_id = %(id)s::uuid"
    for row in rows:
        p = {
            "id": row["hook_outbox_id"],
            "okey": row["ordering_key"],
            "run_id": str(row["run_id"]) if row["run_id"] else None,
        }
        await _close_task_steers(cur, mine, p)
        await _end_thread_steers(cur, p, f"{mine} AND {_IS_PLATFORM_SQL}", "returned")
        await _end_thread_steers(cur, p, f"{mine} AND NOT {_IS_USER_SQL}", "dropped")
        await _repark_thread_steers(
            cur, p, mine, due=False, claimed_too=True, exhausted=True
        )
    # Not next_retry_at: psycopg cannot load the 'infinity' a parked one has.
    await cur.execute(
        """
        SELECT hook_outbox_id, run_id, conversation_thread_id, hook_type,
               payload, ordering_key, idempotency_key, status, attempts,
               created_at, completed_at
        FROM hook_outbox
        WHERE hook_outbox_id = ANY(%s::uuid[])
        ORDER BY created_at, hook_outbox_id
        """,
        ([r["hook_outbox_id"] for r in rows],),
    )
    return [dict(r) for r in await cur.fetchall()]


async def stop_hand_off_steers(thread_id: str, run_id: str) -> List[str]:
    """At an agent's stop of ``run_id`` after it ended: drop the agents'
    steers sent into it that no turn has read, and name the live runs that
    took the others in (as their own turn or mid-turn), for the stop to end.
    The hand-off the stop meant outlived the run it was sent to."""
    params = {"okey": steer_ordering_key(thread_id), "run_id": run_id}
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    SELECT DISTINCT r.conversation_response_id::text AS run_id
                    FROM hook_outbox o
                    JOIN conversation_responses r
                      ON r.conversation_response_id::text = COALESCE(
                             o.payload->>'dispatched_run_id',
                             o.payload->>'steered_run_id')
                    WHERE o.ordering_key = %(okey)s
                      AND o.hook_type = 'steer'
                      AND o.run_id = %(run_id)s::uuid
                      AND NOT {_IS_USER_SQL}
                      AND r.status = 'in_progress'
                      AND r.conversation_response_id <> %(run_id)s::uuid
                    """,
                    params,
                )
                taken = [row["run_id"] for row in await cur.fetchall()]
                await _end_thread_steers(
                    cur,
                    params,
                    f"o.run_id = %(run_id)s::uuid AND NOT {_IS_USER_SQL}",
                    "dropped",
                )
    return taken


def _returned_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = row.get("payload") or {}
    return {
        "content": payload.get("content") or "",
        "request_key": payload.get("request_key"),
        "timestamp": payload.get("timestamp"),
    }


# Not held by the run being stopped. A row it holds is already in its
# messages, and its model call may be confirming it right now: the stamp
# leaves it to that confirm, or to the run's ending.
_NOT_HELD_BY_RUN_SQL = "o.payload->>'steered_run_id' IS DISTINCT FROM %(run_id)s::text"


async def cancel_run_steers(
    conn, *, thread_id: str, run_id: str, by_user: bool = True
) -> List[Dict[str, Any]]:
    """At a stop, in the cancel stamp's transaction: the agents' steers are
    dropped, and at a user stop the user's go back. Returns the user's own
    (not chat-app) steers, oldest first, as ``[{content, request_key,
    timestamp}]``; a chat-app steer reaches its gateway at the run's end
    instead. A stop by an agent leaves the user's for the run's ending to
    settle, as nobody is there to take them back. The rows the run holds wait
    for its ending (``count_held_user_steers``)."""
    params = {"okey": steer_ordering_key(thread_id), "run_id": run_id}
    free = _NOT_HELD_BY_RUN_SQL
    async with conn.cursor(row_factory=dict_row) as cur:
        if not by_user:
            await _end_thread_steers(
                cur, params, f"{free} AND NOT {_IS_USER_SQL}", "dropped"
            )
            return []
        returned = await _end_thread_steers(
            cur, params, f"{free} AND {_IS_USER_SQL}", "returned"
        )
        await _end_thread_steers(cur, params, free, "dropped")
    returned.sort(key=lambda r: r["created_at"])
    return [
        _returned_entry(r)
        for r in returned
        if not (r.get("payload") or {}).get("chat_app")
    ]


async def keep_unanswered_steers(thread_id: str, run_id: str) -> None:
    """At a user stop that answered before ``run_id`` settled: the user's
    steers the run still holds are kept on the thread for the user's next
    move (``pending_steers``), since a return would reach no one. A row the
    run's ending settled first is in ``list_returned_steers`` instead; the
    row lock orders the two."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE hook_outbox o
                SET payload = o.payload
                    || jsonb_build_object('kept_by_stop', %(run_id)s::text)
                WHERE o.ordering_key = %(okey)s
                  AND {_steer_sql(_LIVE_STEER_SQL)}
                  AND o.payload->>'steered_run_id' = %(run_id)s::text
                  AND {_IS_USER_SQL} AND NOT ({_IS_PLATFORM_SQL})
                """,
                {"okey": steer_ordering_key(thread_id), "run_id": run_id},
            )


async def count_held_user_steers(conn, *, ordering_key: str, owner_id: str) -> int:
    """The user's own (not chat-app) steers on ``ordering_key`` that the run
    or task run ``owner_id`` still holds: what its stop leaves for its
    ending to confirm or give back."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT COUNT(*) AS n FROM hook_outbox o
            WHERE o.ordering_key = %(okey)s
              AND {_steer_sql(_LIVE_STEER_SQL)}
              AND o.payload->>'steered_run_id' = %(owner)s::text
              AND {_IS_USER_SQL} AND NOT ({_IS_PLATFORM_SQL})
            """,
            {"okey": ordering_key, "owner": owner_id},
        )
        row = await cur.fetchone()
        return int(row["n"]) if row else 0


async def list_held_jobs(
    thread_id: str, *, owner: Tuple[str, str], task_id: Optional[str] = None
) -> List[str]:
    """Ids of the rows ``owner`` still holds on the thread: steers, and for a
    run the report-backs and alerts it took in as steers. Its ending reads
    them against its checkpoint (``report_back.steer.confirm_checkpointed_holds``)."""
    kind, owner_id = owner
    if kind == "task":
        where = f"o.ordering_key = %(okey)s AND {_steer_sql(_LIVE_STEER_SQL)}"
        okey = steer_ordering_key(thread_id, task_id) if task_id else None
        if okey is None:
            return []
    else:
        where = (
            f"((o.ordering_key = %(okey)s AND {_steer_sql(_LIVE_STEER_SQL)})"
            f" OR (o.ordering_key = %(thread)s AND o.{_REPORT_TYPES_SQL})"
            " OR (o.ordering_key = ANY(%(alerts)s)"
            f" AND o.hook_type IN {_ALERT_TYPES_SQL}))"
        )
        okey = steer_ordering_key(thread_id)
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT o.hook_outbox_id FROM hook_outbox o
                WHERE {where}
                  AND o.status = 'pending'
                  AND o.payload->>'steered_run_id' = %(owner)s::text
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                {
                    "okey": okey,
                    "thread": str(thread_id),
                    "alerts": alert_ordering_keys(thread_id),
                    "owner": owner_id,
                },
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def _list_run_steers(run_id: str, where: str) -> List[Dict[str, Any]]:
    """Steer rows on ``run_id``'s thread matching ``where``, oldest first."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT o.payload, o.created_at FROM hook_outbox o
                WHERE o.conversation_thread_id = (
                        SELECT conversation_thread_id FROM conversation_responses
                        WHERE conversation_response_id = %(run_id)s::uuid
                    )
                  AND o.ordering_key LIKE '%%:steer'
                  AND o.hook_type = 'steer'
                  AND {_steer_sql(_TARGET_SQL)} = 'thread'
                  AND {where}
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                {"run_id": run_id},
            )
            return [dict(r) for r in await cur.fetchall()]


async def list_returned_steers(run_id: str) -> List[Dict[str, Any]]:
    """The user's own steers ``run_id``'s stop or ending gave back:
    ``[{content, request_key, timestamp}]``, for a ``/cancel`` asked again."""
    rows = await _list_run_steers(
        run_id,
        "o.payload->>'outcome' = 'returned'"
        " AND o.payload->>'returned_run_id' = %(run_id)s::text"
        f" AND NOT ({_IS_PLATFORM_SQL})",
    )
    return [_returned_entry(r) for r in rows]


async def list_returned_platform_steers(run_id: str) -> List[Dict[str, Any]]:
    """The chat-app steers ``run_id`` gave back, oldest first:
    ``[{content, user_id}]``."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT o.payload FROM hook_outbox o
                WHERE o.conversation_thread_id = (
                        SELECT conversation_thread_id FROM conversation_responses
                        WHERE conversation_response_id = %(run_id)s::uuid
                    )
                  AND o.ordering_key LIKE '%%:steer'
                  AND o.hook_type = 'steer'
                  AND {_steer_sql(_TARGET_SQL)} = 'thread'
                  AND o.payload->>'outcome' = 'returned'
                  AND o.payload->>'returned_run_id' = %(run_id)s::text
                  AND {_IS_PLATFORM_SQL}
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                {"run_id": run_id},
            )
            rows = await cur.fetchall()
    return [
        {
            "content": (r["payload"] or {}).get("content") or "",
            "user_id": (r["payload"] or {}).get("user_id"),
        }
        for r in rows
    ]


async def list_delivered_steers(run_id: str) -> List[Dict[str, Any]]:
    """The thread steers ``run_id`` read, oldest first:
    ``[{content, request_key, timestamp}]``."""
    rows = await _list_run_steers(
        run_id,
        "o.payload->>'outcome' = 'delivered'"
        " AND o.payload->>'steered_run_id' = %(run_id)s::text",
    )
    return [_returned_entry(r) for r in rows]


async def list_pending_steers(thread_id: str) -> List[Dict[str, Any]]:
    """The user's own steers waiting on the thread for a turn to read them
    (after an error or an agent's stop, until the user's next move):
    ``[{content, request_key, timestamp}]``, oldest first."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT o.payload, o.created_at FROM hook_outbox o
                WHERE o.ordering_key = %(okey)s
                  AND {_steer_sql(_LIVE_STEER_SQL)}
                  AND NOT {_steer_sql(_STEER_HELD_SQL)}
                  AND NOT (o.payload ? 'dispatched_run_id')
                  AND {_steer_sql(_TARGET_SQL)} = 'thread'
                  AND {_IS_USER_SQL} AND NOT ({_IS_PLATFORM_SQL})
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                {"okey": steer_ordering_key(thread_id)},
            )
            return [_returned_entry(dict(r)) for r in await cur.fetchall()]


async def list_taken_hand_offs(thread_id: str, run_ids: List[str]) -> List[str]:
    """Ids of the hand-offs on the thread that one of ``run_ids`` took in
    (``_STEER_TAKER_SQL``), oldest first."""
    if not run_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT o.hook_outbox_id FROM hook_outbox o
                WHERE o.ordering_key = %(okey)s
                  AND {_HAND_OFF_STEER_SQL}
                  AND {_steer_sql(_STEER_TAKER_SQL)} = ANY(%(runs)s::text[])
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                {"okey": steer_ordering_key(thread_id), "runs": list(run_ids)},
            )
            return [str(r[0]) for r in await cur.fetchall()]


# ---------------------------------------------------------------------------
# Task steers once their run is over. A stopped run gives the user's back and
# drops the agent's; any other ending moves them onto the thread's chain, still
# ``target='task'``, where the next main turn resumes the task with them
# (``SteeringMiddleware``), or a turn posted for them does
# (``report_back.task_steer``).
# ---------------------------------------------------------------------------

_TASK_STEER_SQL = (
    _steer_sql(_LIVE_STEER_SQL) + " AND " + _steer_sql(_TARGET_SQL) + " = 'task'"
)
_IS_USER_STEER_SQL = "COALESCE(o.payload->>'sender', 'user') = 'user'"

# How a caller proves the rows are still its own to move or close: held by its
# run, claimed under its lease, or held by no live agent.
_TASK_FENCES = {
    "held": "o.status = 'pending' AND o.payload->>'steered_run_id' = %(fence)s::text",
    "claimed": "o.status = 'claimed' AND o.attempts = %(fence)s::int",
    "free": "NOT " + _steer_sql(_STEER_HELD_SQL),
}


# A run that could not resume a task is never offered its steers again; after
# this many such runs they are given up on, so a task that cannot resume
# cannot keep them, or the turns posted for them, coming back.
MAX_RESUME_REFUSALS = 3
_REFUSALS_SQL = "COALESCE((o.payload->>'resume_refusals')::int, 0) + 1"
_AT_REFUSAL_CAP_SQL = f"{_REFUSALS_SQL} >= %(max)s::int"


def _task_fence(fence: Tuple[str, Any]) -> Tuple[str, Dict[str, Any]]:
    kind, value = fence
    return _TASK_FENCES[kind], {"fence": value}


async def _close_task_steers(
    cur, where: str, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Return the user's steers and drop the agent's, writing the returns onto
    each run's ``replay_facts`` in the same transaction, since replay reads a
    run from its checkpoint, where nothing returned ever lands. Returns the
    entries returned, oldest first."""
    from src.server.database import replay_facts as facts_db

    outcome = (
        f"(CASE WHEN {_IS_USER_STEER_SQL} THEN 'returned' ELSE 'dropped' END)"
    )
    await cur.execute(
        f"""
        UPDATE hook_outbox o
        SET {_closed_steer_set(outcome)}
            || CASE WHEN {_IS_USER_STEER_SQL}
                    THEN jsonb_build_object(
                        'returned_run_id', o.payload->>'task_run_id')
                    ELSE '{{}}'::jsonb END
        WHERE {_TASK_STEER_SQL} AND {where}
        RETURNING o.payload, o.created_at
        """,
        params,
    )
    rows = sorted(
        (dict(r) for r in await cur.fetchall()), key=lambda r: r["created_at"]
    )
    by_run: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        payload = row["payload"] or {}
        if payload.get("outcome") != "returned" or not payload.get("task_run_id"):
            continue
        by_run.setdefault(str(payload["task_run_id"]), []).append(
            _task_returned_entry(payload)
        )
    for task_run_id, entries in by_run.items():
        await facts_db.append_returned_steering(
            task_run_id, entries, conn=cur.connection
        )
    return [entry for entries in by_run.values() for entry in entries]


def _task_returned_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    """A returned task steer, as its ``steering_returned`` frame and its run's
    replay fact name it."""
    return {
        "content": payload.get("content") or "",
        "input_id": payload.get("input_id"),
        "reason": "run_ended",
    }


async def _rekey_task_steers(
    cur, where: str, params: Dict[str, Any]
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Move task steers onto the thread's chain, parked: the claim safety net
    makes them due once the thread has no live turn, and a live turn resumes
    the task with them before its next model call.

    A steer a resume handed to the run that ended (``resumed_by``), which
    that run never read, counts as a refusal by the run that resumed it, as
    ``release_task_resumes`` counts one, and closes at the cap: a task that
    dies before its first model call would otherwise be resumed with it for
    good. Returns ``(moved ids, entries returned at the cap)``."""
    resumed = "o.payload ? 'resumed_by'"
    params = {**params, "max": MAX_RESUME_REFUSALS}
    returned = await _close_task_steers(
        cur, f"{where} AND {resumed} AND {_AT_REFUSAL_CAP_SQL}", params
    )
    await cur.execute(
        f"""
        UPDATE hook_outbox o
        SET ordering_key = o.conversation_thread_id::text || ':steer',
            status = 'pending', lease_expires_at = NULL,
            next_retry_at = 'infinity'::timestamptz,
            payload = (o.payload - 'steered_run_id' - 'due_at' - 'resumed_by')
                || CASE WHEN {resumed} THEN jsonb_build_object(
                        'resume_refusals', {_REFUSALS_SQL},
                        'resume_refused_by', o.payload->>'resumed_by')
                   ELSE '{{}}'::jsonb END
        WHERE {_TASK_STEER_SQL} AND {where}
        RETURNING o.hook_outbox_id
        """,
        params,
    )
    return [str(r["hook_outbox_id"]) for r in await cur.fetchall()], returned


async def settle_task_run_steers(
    conn,
    *,
    thread_id: str,
    task_id: str,
    cancelled: bool,
    spare_held_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Settle a task's live steers in the transaction that stamps its run's
    cancel or ends the run (the run's row is locked, and admission takes it
    FOR SHARE, so no steer gets in behind this). Stopped: the user's come
    back, ``[{content, input_id, reason}]``, and the agent's are dropped.
    Otherwise they move to resume the task, but for those a resume handed
    this run that reach the refusal cap, which come back the same way.
    ``spare_held_by`` (the stamp) leaves the rows that task run holds to its
    confirm or its ending, as ``cancel_run_steers`` does for a run."""
    params = {"okey": steer_ordering_key(thread_id, task_id), "run_id": spare_held_by}
    where = "o.ordering_key = %(okey)s"
    if spare_held_by is not None:
        where += f" AND {_NOT_HELD_BY_RUN_SQL}"
    async with conn.cursor(row_factory=dict_row) as cur:
        if cancelled:
            return await _close_task_steers(cur, where, params)
        _, returned = await _rekey_task_steers(cur, "o.ordering_key = %(okey)s", params)
    return returned


async def settle_task_steers(
    job_ids: List[str], *, fence: Tuple[str, Any]
) -> List[Dict[str, Any]]:
    """Close task steers that can no longer reach their task (it cannot be
    resumed, or a turn posted for them never read them), as a stop would."""
    if not job_ids:
        return []
    fence_sql, params = _task_fence(fence)
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _close_task_steers(
                    cur,
                    f"o.hook_outbox_id = ANY(%(ids)s::uuid[]) AND {fence_sql}",
                    {**params, "ids": job_ids},
                )


async def rekey_task_steers(job_ids: List[str], *, fence: Tuple[str, Any]) -> List[str]:
    """Move task steers whose run ended without settling them (a lost
    finalize, a run from an older build) as its finalize would have."""
    if not job_ids:
        return []
    fence_sql, params = _task_fence(fence)
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                moved, _ = await _rekey_task_steers(
                    cur,
                    f"o.hook_outbox_id = ANY(%(ids)s::uuid[]) AND {fence_sql}",
                    {**params, "ids": job_ids},
                )
    return moved


async def fenced_task_steers(job_ids: List[str], *, fence: Tuple[str, Any]) -> List[str]:
    """Which of ``job_ids`` are still live task steers under ``fence``."""
    if not job_ids:
        return []
    fence_sql, params = _task_fence(fence)
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT o.hook_outbox_id FROM hook_outbox o
                WHERE o.hook_outbox_id = ANY(%(ids)s::uuid[])
                  AND {_TASK_STEER_SQL} AND {fence_sql}
                """,
                {**params, "ids": job_ids},
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def retarget_task_steers(
    job_ids: List[str],
    *,
    thread_id: str,
    task_id: str,
    task_run_id: str,
    fence: Tuple[str, Any],
) -> List[str]:
    """Hand task steers to the task's live run instead of resuming it: back on
    the task's chain, parked for that run to take in. Gated like admission,
    on the run's row FOR SHARE, so its ending settles them or this misses."""
    if not job_ids:
        return []
    fence_sql, params = _task_fence(fence)
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                WITH owner AS (
                    SELECT 1 FROM subagent_runs
                    WHERE task_run_id = %(task_run_id)s::uuid
                      AND thread_id = %(thread_id)s::uuid
                      AND task_id = %(task_id)s
                      AND status = 'in_progress'
                      AND cancel_requested_at IS NULL
                    FOR SHARE
                )
                UPDATE hook_outbox o
                SET ordering_key = %(okey)s,
                    status = 'pending', lease_expires_at = NULL,
                    next_retry_at = 'infinity'::timestamptz,
                    payload = (o.payload - 'steered_run_id' - 'due_at')
                        || jsonb_build_object('task_run_id', %(task_run_id)s::text)
                WHERE o.hook_outbox_id = ANY(%(ids)s::uuid[])
                  AND {_TASK_STEER_SQL}
                  AND o.payload->>'task_id' = %(task_id)s
                  AND {fence_sql}
                  AND EXISTS (SELECT 1 FROM owner)
                RETURNING o.hook_outbox_id
                """,
                {
                    **params,
                    "ids": job_ids,
                    "thread_id": thread_id,
                    "task_id": task_id,
                    "task_run_id": task_run_id,
                    "okey": steer_ordering_key(thread_id, task_id),
                },
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def hand_off_task_resumes(
    job_ids: List[str],
    *,
    run_id: str,
    thread_id: str,
    task_id: str,
    task_run_id: str,
) -> List[str]:
    """Hand the task steers ``run_id`` holds to resume a task with to
    ``task_run_id``, the run the resume admitted: back on the task's chain,
    held by that run, so its first model call confirms them once the message
    carrying them is checkpointed, and its ending settles them if it never
    gets there. ``resumed_by`` marks them for that settle to count as a
    refusal by ``run_id``, and the confirm to close as ``resumed``.

    All or nothing, gated like admission on the run's row FOR SHARE: a run
    that ended or was stopped takes none, and neither does one that would
    get only some of them, since the run opens with them all."""
    if not job_ids:
        return []
    ids = list(dict.fromkeys(job_ids))
    params = {
        "ids": ids,
        "fence": run_id,
        "thread_id": thread_id,
        "task_id": task_id,
        "task_run_id": task_run_id,
        "okey": steer_ordering_key(thread_id, task_id),
    }
    held = (
        f"o.hook_outbox_id = ANY(%(ids)s::uuid[]) AND {_TASK_STEER_SQL}"
        f" AND o.payload->>'task_id' = %(task_id)s AND {_TASK_FENCES['held']}"
    )
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1 FROM subagent_runs
                    WHERE task_run_id = %(task_run_id)s::uuid
                      AND thread_id = %(thread_id)s::uuid
                      AND task_id = %(task_id)s
                      AND status = 'in_progress'
                      AND cancel_requested_at IS NULL
                    FOR SHARE
                    """,
                    params,
                )
                if await cur.fetchone() is None:
                    return []
                await cur.execute(
                    f"SELECT o.hook_outbox_id FROM hook_outbox o WHERE {held}"
                    " FOR UPDATE",
                    params,
                )
                if len(await cur.fetchall()) != len(ids):
                    return []
                await cur.execute(
                    f"""
                    UPDATE hook_outbox o
                    SET ordering_key = %(okey)s,
                        payload = (o.payload - 'due_at') || jsonb_build_object(
                            'task_run_id', %(task_run_id)s::text,
                            'steered_run_id', %(task_run_id)s::text,
                            'resumed_by', %(fence)s::text)
                    WHERE {held}
                    RETURNING o.hook_outbox_id
                    """,
                    params,
                )
                return [str(r[0]) for r in await cur.fetchall()]


async def list_resumable_task_steers(
    thread_id: str, run_id: str, *, limit: int = 50
) -> List[Dict[str, Any]]:
    """Task steers waiting on the thread for a turn to resume their task,
    oldest first, less those ``run_id`` already failed to resume: a run that
    could not resume a task gets no second try at it, the next turn does."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT o.hook_outbox_id, o.hook_type, o.run_id, o.payload,
                       o.created_at
                FROM hook_outbox o
                WHERE o.ordering_key = %(okey)s
                  AND {_steer_sql(_TAKEABLE_STEER_SQL)}
                  AND {_steer_sql(_TARGET_SQL)} = 'task'
                  AND COALESCE(o.payload->>'resume_refused_by', '')
                      <> %(run_id)s::text
                ORDER BY o.created_at, o.hook_outbox_id
                LIMIT %(limit)s
                """,
                {"okey": steer_ordering_key(thread_id), "run_id": run_id, "limit": limit},
            )
            return [dict(r) for r in await cur.fetchall()]


async def release_task_resumes(
    job_ids: List[str], *, run_id: str, max_refusals: int
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Give back task steers ``run_id`` held but could not resume their task
    with, counting the refusal on each row. A row refused ``max_refusals``
    times is closed instead (returned to the user, dropped for an agent), so
    a task that never resumes cannot keep a steer, or the turns posted for
    it, coming back. Returns ``(released ids, returned entries)``."""
    if not job_ids:
        return [], []
    params = {"ids": job_ids, "fence": run_id, "max": max_refusals}
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                held = (
                    "o.hook_outbox_id = ANY(%(ids)s::uuid[]) AND "
                    + _TASK_FENCES["held"]
                )
                returned = await _close_task_steers(
                    cur, f"{held} AND {_AT_REFUSAL_CAP_SQL}", params
                )
                await cur.execute(
                    f"""
                    UPDATE hook_outbox o
                    SET payload = (o.payload - 'steered_run_id')
                        || jsonb_build_object(
                            'resume_refusals', {_REFUSALS_SQL},
                            'resume_refused_by', %(fence)s::text)
                    WHERE {_TASK_STEER_SQL} AND {held}
                    RETURNING o.hook_outbox_id
                    """,
                    params,
                )
                released = [str(r["hook_outbox_id"]) for r in await cur.fetchall()]
    return released, returned


async def list_returned_task_steers(
    thread_id: str, task_id: str, task_run_id: str
) -> List[Dict[str, Any]]:
    """The user's steers ``task_run_id``'s stop or ending gave back, oldest
    first: ``[{content, input_id, timestamp}]``, for a task cancel asked
    again."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT o.payload FROM hook_outbox o
                WHERE o.conversation_thread_id = %s::uuid
                  AND o.hook_type = 'steer'
                  AND o.payload->>'target' = 'task'
                  AND o.payload->>'task_id' = %s
                  AND o.payload->>'outcome' = 'returned'
                  AND o.payload->>'returned_run_id' = %s::text
                ORDER BY o.created_at, o.hook_outbox_id
                """,
                (thread_id, task_id, task_run_id),
            )
            return [
                {
                    "content": (r["payload"] or {}).get("content") or "",
                    "input_id": (r["payload"] or {}).get("input_id"),
                    "timestamp": (r["payload"] or {}).get("timestamp"),
                }
                for r in await cur.fetchall()
            ]


async def task_steer_position(job_id: str) -> int:
    """Where a task steer stands among those waiting on its chain, for a
    resend of one already accepted; 0 once it was read or settled."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COUNT(*) FROM hook_outbox o, hook_outbox j
                WHERE j.hook_outbox_id = %s::uuid
                  AND {_steer_sql(_LIVE_STEER_SQL, "j")}
                  AND o.ordering_key = j.ordering_key
                  AND {_TASK_STEER_SQL}
                  AND (o.created_at, o.hook_outbox_id)
                      <= (j.created_at, j.hook_outbox_id)
                """,
                (job_id,),
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0


async def list_steer_jobs(job_ids: List[str]) -> List[Dict[str, Any]]:
    """Steer rows by id: ``[{hook_outbox_id, payload}]``."""
    if not job_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT hook_outbox_id, payload FROM hook_outbox"
                " WHERE hook_outbox_id = ANY(%s::uuid[]) AND hook_type = 'steer'",
                (job_ids,),
            )
            return [dict(r) for r in await cur.fetchall()]
