"""Hook outbox (SQL layer) — durable post-commit effects of run finalize.

``build_finalize_jobs`` is the one decision table mapping a run's
CAS-adopted final status to its hook jobs; ``finalize_run`` applies it via
``build_finalize_jobs_from_run_row`` as the DEFAULT, so no finalize path
can skip required effects (I5). ``enqueue_hooks`` writes the rows on the
finalize transaction; the claim/ack/nack trio is the lease protocol the
``HookOutboxDrainer`` runs (committed claims, per-ordering-key FIFO,
expired-lease reclaim as crash recovery, effect-before-ack retry safety).
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypedDict

from psycopg.rows import dict_row

from src.server.database import conversation as qr_db
from src.server.utils.pg_sanitize import SafeJson

# Advisory-lock class (int32) for per-ordering-key claim serialization.
# The two-int lock form is a separate keyspace from the WriterGuard's
# single-bigint locks, so no domain interference is possible.
_OKEY_LOCK_CLASS = 0x484F4F4B  # 'HOOK'


@dataclass
class HookJob:
    """One outbox row: a post-commit effect that must survive a crash."""

    hook_type: str
    idempotency_key: str
    payload: Dict[str, Any] = field(default_factory=dict)
    ordering_key: Optional[str] = None


# Payload contracts between the decision table below and the executors in
# services.hook_outbox. Rows round-trip through JSONB, so these are the
# documented shape, not a runtime guarantee.


class BurstReleasePayload(TypedDict):
    user_id: str
    slot_id: str


class ReportBackPayload(TypedDict):
    ptc_thread_id: str
    dispatch_gen: Optional[str]


class NeedsInputWakePayload(TypedDict):
    ptc_thread_id: str


class WatchClearPayload(TypedDict):
    ptc_thread_id: str
    user_id: Optional[str]
    error_wake: bool
    dispatch_gen: Optional[str]


def build_finalize_jobs(
    *,
    run_id: str,
    thread_id: str,
    msg_type: str,
    user_id: Optional[str] = None,
    burst_slot_id: Optional[str] = None,
    report_back_ptc_thread_id: Optional[str] = None,
    origin_flash_thread_id: Optional[str] = None,
    origin_dispatch_gen: Optional[str] = None,
) -> Callable[[str], List[HookJob]]:
    """The one decision table mapping a run's final status to its hook jobs.

    Returned callable is invoked inside the finalize transaction with the
    CAS-adopted final status (a durable cancel may have overridden the
    requested one). One ordering rule: every report-back-lifecycle job keys
    on the WATCHING flash thread (START-stamped ``origin_flash_thread_id``;
    a report-back flash run IS that thread, so its own ``thread_id`` lands
    in the same chain) — all completions reporting into one flash thread
    serialize strictly, across any number of drainer workers, and a
    completed run's report_back can never be overtaken by a later
    watch_clear. Runs without a flash origin fall back to their own
    thread_id. Pure and synchronous — it runs inside the finalize
    transaction and must not do I/O.
    """

    def _jobs(final_status: str) -> List[HookJob]:
        jobs: List[HookJob] = []
        is_ptc = msg_type == "ptc"
        rb_ptc = report_back_ptc_thread_id or thread_id
        okey = origin_flash_thread_id or thread_id

        if user_id and burst_slot_id:
            jobs.append(
                HookJob(
                    hook_type="burst_release",
                    idempotency_key=f"{run_id}:burst_release",
                    payload=BurstReleasePayload(
                        user_id=user_id, slot_id=burst_slot_id
                    ),
                )
            )

        if final_status == "completed" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="report_back",
                    idempotency_key=f"{run_id}:report_back",
                    payload=ReportBackPayload(
                        ptc_thread_id=thread_id,
                        dispatch_gen=origin_dispatch_gen,
                    ),
                    ordering_key=okey,
                )
            )
        elif final_status == "interrupted" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="needs_input_wake",
                    idempotency_key=f"{run_id}:needs_input_wake",
                    payload=NeedsInputWakePayload(ptc_thread_id=thread_id),
                    ordering_key=okey,
                )
            )

        if final_status in ("error", "cancelled") or (
            final_status == "completed"
            and not is_ptc
            and report_back_ptc_thread_id
        ):
            # error/cancelled: tear down any watch this run held open (a
            # dispatched PTC directly, a report-back flash run via its
            # origin id). completed flash WITH an origin id: consumption
            # clear — the report-back summary landed, release the watch.
            jobs.append(
                HookJob(
                    hook_type="watch_clear",
                    idempotency_key=f"{run_id}:watch_clear",
                    payload=WatchClearPayload(
                        ptc_thread_id=rb_ptc,
                        user_id=user_id,
                        error_wake=final_status in ("error", "cancelled"),
                        dispatch_gen=origin_dispatch_gen,
                    ),
                    ordering_key=okey,
                )
            )
        return jobs

    return _jobs


def build_finalize_jobs_from_run_row(
    run: Dict[str, Any],
) -> Callable[[str], List[HookJob]]:
    """Factory from the durable row alone: everything the decision table
    needs was stamped into run metadata at START. This is finalize_run's
    DEFAULT — no finalize path can skip required terminal effects (I5)."""
    meta = run.get("metadata") or {}
    return build_finalize_jobs(
        run_id=str(run["conversation_response_id"]),
        thread_id=str(run["conversation_thread_id"]),
        msg_type=meta.get("msg_type") or "ptc",
        user_id=meta.get("user_id"),
        burst_slot_id=meta.get("burst_slot_id"),
        report_back_ptc_thread_id=meta.get("report_back_ptc_thread_id"),
        origin_flash_thread_id=meta.get("origin_flash_thread_id"),
        origin_dispatch_gen=meta.get("origin_dispatch_gen"),
    )


async def enqueue_hooks(
    conn, *, run_id: str, thread_id: str, jobs: List[HookJob]
) -> None:
    """Write outbox rows on the caller's (finalize) transaction.

    ON CONFLICT DO NOTHING on idempotency_key: re-finalizing after a lost
    race never double-registers an effect.
    """
    async with conn.cursor() as cur:
        for job in jobs:
            await cur.execute(
                """
                INSERT INTO hook_outbox (
                    run_id, conversation_thread_id, hook_type,
                    payload, ordering_key, idempotency_key
                )
                VALUES (%s, %s, %s, %s, %s, %s)
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
    """
    excluded = list(excluded_hook_types or [])
    async with qr_db.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT o.ordering_key AS okey
                    FROM hook_outbox o
                    WHERE o.ordering_key IS NOT NULL
                      AND o.hook_type <> ALL(%s::varchar[])
                      AND (
                        (o.status = 'pending'
                         AND (o.next_retry_at IS NULL
                              OR o.next_retry_at <= NOW()))
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
                              (p.status = 'pending'
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
                    """
                    WITH due AS (
                        SELECT o.hook_outbox_id FROM hook_outbox o
                        WHERE o.hook_type <> ALL(%s::varchar[])
                        AND (
                            (o.status = 'pending'
                             AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW()))
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
                                      (p.status = 'pending'
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
                            clock_timestamp() + make_interval(secs => %s)
                    FROM due
                    WHERE h.hook_outbox_id = due.hook_outbox_id
                    RETURNING h.*
                    """,
                    (excluded, max_attempts, locked_keys, limit, float(lease_seconds)),
                )
                rows = await cur.fetchall()
                return [dict(r) for r in rows]


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
_DEAD_REPORT_BACK_COMPENSATION_SQL = """
    INSERT INTO hook_outbox (
        run_id, conversation_thread_id, hook_type,
        payload, ordering_key, idempotency_key, created_at
    )
    SELECT run_id, conversation_thread_id, 'watch_clear',
           jsonb_build_object(
               'ptc_thread_id', payload->>'ptc_thread_id',
               'error_wake', true,
               'dispatch_gen', payload->'dispatch_gen',
               'refuse_if_pointer', true
           ),
           ordering_key, hook_outbox_id || ':dead_clear', created_at
    FROM dead_rows
    WHERE hook_type = 'report_back' AND payload ? 'ptc_thread_id'
    ON CONFLICT (idempotency_key) DO NOTHING
"""


async def park_exhausted_jobs(*, max_attempts: int = 5) -> List[Dict[str, Any]]:
    """Park expired-lease jobs that already burned max_attempts leases as dead.

    Without this, a job whose workers crash before nacking (nack is what
    normally parks dead) is reclaimable forever; the claim ceiling alone
    would instead leave it claimed-expired forever, wedging its ordering
    chain. Dead report_backs get their watch_clear compensation in the
    same atomic statement. Returns the parked rows.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH dead_rows AS (
                    UPDATE hook_outbox
                    SET status = 'dead', lease_expires_at = NULL,
                        completed_at = NOW()
                    WHERE status = 'claimed'
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
            return [dict(r) for r in rows]


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
    async with qr_db.get_db_connection() as conn:
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


async def ack_outbox_job(job_id: str, *, attempts: int) -> bool:
    """Mark a claimed job done, fenced to the caller's lease generation.

    ``attempts`` increments on every claim, so it doubles as the fence
    token: a stale owner (lease expired, job reclaimed) matches nothing
    and cannot ack the new owner's claim. False = fence lost; never
    re-open the row.
    """
    async with qr_db.get_db_connection() as conn:
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


async def nack_outbox_job(
    job_id: str, *, attempts: int, max_attempts: int = 5
) -> Optional[str]:
    """Return a failed claimed job to pending with exponential backoff,
    or park it dead at max_attempts. Fenced by ``attempts`` (see
    ``ack_outbox_job``) so a stale owner can't nack — or dead-letter — the
    reclaiming owner's live lease (a fence-lost nack also never
    compensates a live job). A report_back going dead gets its watch_clear
    compensation in the same atomic statement. Returns the new status, or
    None if the fence was lost."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH bumped AS (
                    UPDATE hook_outbox
                    SET status = CASE
                            WHEN attempts >= %s THEN 'dead' ELSE 'pending'
                        END,
                        next_retry_at = NOW() + make_interval(
                            secs => LEAST(POWER(2, attempts), 60)::float
                        ),
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
                (max_attempts, max_attempts, job_id, attempts),
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
    async with qr_db.get_db_connection() as conn:
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


@asynccontextmanager
async def fenced_job_guard(job_id: str, attempts: int, *, lease_seconds: int = 60):
    """Key-lock + row-lock fence for an external teardown: yields ownership.

    Holds BOTH the per-key advisory xact lock (no sibling claim on the
    ordering key can even be gated while a teardown is in flight — the
    row-lock alone can't block a claim of a DIFFERENT row on the same key)
    and ``SELECT ... FOR UPDATE`` on the job row at our lease generation,
    across the caller's Redis mutations. Ownership additionally requires a
    LIVE lease (``clock_timestamp()``): an expired owner must stand down —
    a sibling claim may already have been admitted on the strength of that
    expiry. On owned exit the lease is renewed with fresh runway so the
    park sweep waiting on the row lock can't dead-letter a job its owner
    is still executing. Yields False (fence lost) when not owned; the
    caller must do NO teardown.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                owned = False
                if await _lock_job_ordering_key(cur, job_id):
                    await cur.execute(
                        """
                        SELECT 1 FROM hook_outbox
                        WHERE hook_outbox_id = %s AND status = 'claimed'
                          AND attempts = %s
                          AND lease_expires_at > clock_timestamp()
                        FOR UPDATE
                        """,
                        (job_id, attempts),
                    )
                    owned = (await cur.fetchone()) is not None
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
    run_id: str,
    thread_id: str,
    hook_type: str,
    payload: Dict[str, Any],
    ordering_key: Optional[str],
    idempotency_key: str,
    backdate_seconds: Optional[float] = None,
    defer: bool = False,
) -> None:
    """Insert a single follow-up job outside any finalize transaction.

    Used by the legacy-FIFO migration sweep (and any caller needing a
    durable one-off job). ``backdate_seconds`` shifts created_at into the
    past so a migrated entry keeps its chain position ahead of younger
    pending rows. ``defer=True`` inserts the row with
    ``next_retry_at='infinity'`` — durable but never claimable until
    ``release_deferred_jobs`` flips it due (interrupted-root task
    report-backs wait for the thread's next completed finalize). ON
    CONFLICT DO NOTHING: re-running the sweep never double-registers.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO hook_outbox (
                    run_id, conversation_thread_id, hook_type,
                    payload, ordering_key, idempotency_key, created_at,
                    next_retry_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    NOW() - make_interval(secs => %s),
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
                    float(backdate_seconds or 0.0),
                    defer,
                ),
            )


async def release_deferred_jobs(
    conn, thread_id: str, hook_type: str
) -> int:
    """Flip a thread's deferred (``next_retry_at='infinity'``) jobs due.

    Runs on the caller's connection so the release commits atomically with
    the finalize that makes posting safe again (a completed PTC run means
    no pending HITL checkpoint to collide with).
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE hook_outbox
            SET next_retry_at = NOW()
            WHERE conversation_thread_id = %s
              AND hook_type = %s
              AND status = 'pending'
              AND next_retry_at = 'infinity'::timestamptz
            """,
            (thread_id, hook_type),
        )
        return cur.rowcount


async def defer_claimed_job(
    job_id: str, *, attempts: int, max_attempts: int = 5
) -> Optional[str]:
    """Park a CLAIMED job back to deferred (``next_retry_at='infinity'``).

    For a task report-back whose thread turned interrupted between the
    deferred release and this claim: posting would collide with the pending
    HITL checkpoint, so the job re-parks until the next completed finalize
    flips it due again. Fenced by ``attempts``. At the attempts ceiling the
    job is parked dead instead — a pending row below the ceiling blocks its
    ordering chain, so an unclaimable-forever deferred row would wedge every
    later notification on the thread. Returns the new status ('pending' or
    'dead'), or None if the fence was lost; the caller stops either way and
    its fenced ack no-ops.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
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
                    END
                WHERE hook_outbox_id = %s AND status = 'claimed'
                  AND attempts = %s
                RETURNING status
                """,
                (max_attempts, max_attempts, max_attempts, job_id, attempts),
            )
            row = await cur.fetchone()
            return row["status"] if row else None


async def get_open_notification_job(
    thread_id: str, hook_type: str
) -> Optional[Dict[str, Any]]:
    """Oldest open (pending-and-due or claimed) job of one type for a thread.

    The read-model behind a watcher thread's ``pending_report_back`` when the
    outbox rows ARE the pending-registry: a job's open lifetime — enqueue
    through the executor's terminal wait — is exactly the pending window.
    Deferred rows (``next_retry_at='infinity'``) are invisible: their work
    is parked, not in progress.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT hook_outbox_id, payload, status
                FROM hook_outbox
                WHERE ordering_key = %s
                  AND hook_type = %s
                  AND status IN ('pending', 'claimed')
                  AND (next_retry_at IS NULL
                       OR next_retry_at != 'infinity'::timestamptz)
                ORDER BY created_at, hook_outbox_id
                LIMIT 1
                """,
                (thread_id, hook_type),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_recent_notification_run_ids(
    thread_id: str,
    hook_type: str,
    *,
    window_seconds: int = 900,
    limit: int = 10,
) -> List[str]:
    """Dispatched run ids of recently DONE jobs of one type, newest first.

    The durable recents ledger behind wake-miss recovery: once a job closes,
    the slice reads idle and only this list can still name the notification
    run to a client that held no subscription when the wake fired. Done rows
    are never purged, so this needs no companion write path.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT payload->>'dispatched_run_id'
                FROM hook_outbox
                WHERE ordering_key = %s
                  AND hook_type = %s
                  AND status = 'done'
                  AND payload->>'dispatched_run_id' IS NOT NULL
                  AND completed_at > NOW() - make_interval(secs => %s)
                ORDER BY completed_at DESC
                LIMIT %s
                """,
                (thread_id, hook_type, window_seconds, limit),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def list_pending_jobs(hook_type: str) -> List[Dict[str, Any]]:
    """Pending rows of one hook_type, for the one-shot legacy rekey sweep."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM hook_outbox
                WHERE status = 'pending' AND hook_type = %s
                ORDER BY created_at, hook_outbox_id
                """,
                (hook_type,),
            )
            return list(await cur.fetchall())


async def set_job_ordering_key(job_id: str, ordering_key: str) -> bool:
    """Rekey a still-pending job into its correct ordering chain.

    Guarded to ``pending`` so a job a drainer claimed in the meantime is
    left alone — rekeying a claimed row would move it under a chain whose
    gate it already bypassed.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET ordering_key = %s
                WHERE hook_outbox_id = %s AND status = 'pending'
                """,
                (ordering_key, job_id),
            )
            return cur.rowcount > 0


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
    async with qr_db.get_db_connection() as conn:
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
    job_id: str, patch: Dict[str, Any], *, remove: Optional[List[str]] = None
) -> bool:
    """Durably merge keys into a job's payload (e.g. dispatched_run_id after
    the synthetic POST) so a reclaiming drainer resumes instead of re-doing
    the effect. ``remove`` drops keys in the SAME update (a task
    report-back scrubs its inlined result text the moment the dispatched
    run id lands — one atomic step, so a crash can never leave neither).
    Deliberately unfenced: a stale owner's merge carries the same
    request-key-deduped run id the live owner would write — the information
    is true even after the lease was lost."""
    async with qr_db.get_db_connection() as conn:
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
