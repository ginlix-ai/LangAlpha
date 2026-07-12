"""Hook outbox (SQL layer) — durable post-commit effects of run finalize.

``build_finalize_jobs`` is the one decision table mapping a run's
CAS-adopted final status to its hook jobs; ``finalize_run`` applies it via
``build_finalize_jobs_from_run_row`` as the DEFAULT, so no finalize path
can skip required effects (I5). ``enqueue_hooks`` writes the rows on the
finalize transaction; the claim/ack/nack trio is the lease protocol the
``HookOutboxDrainer`` runs (committed claims, per-ordering-key FIFO,
expired-lease reclaim as crash recovery, effect-before-ack retry safety).
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypedDict

from psycopg.rows import dict_row

from src.server.database import conversation as qr_db
from src.server.utils.pg_sanitize import SafeJson


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


class NeedsInputWakePayload(TypedDict):
    ptc_thread_id: str


class WatchClearPayload(TypedDict):
    ptc_thread_id: str
    user_id: Optional[str]
    error_wake: bool


def build_finalize_jobs(
    *,
    run_id: str,
    thread_id: str,
    msg_type: str,
    user_id: Optional[str] = None,
    burst_slot_id: Optional[str] = None,
    report_back_ptc_thread_id: Optional[str] = None,
) -> Callable[[str], List[HookJob]]:
    """The one decision table mapping a run's final status to its hook jobs.

    Returned callable is invoked inside the finalize transaction with the
    CAS-adopted final status (a durable cancel may have overridden the
    requested one). Ordering keys serialize all jobs touching one PTC
    thread's report-back lifecycle, so a completed run's report_back can
    never be overtaken by a later watch_clear. Pure and synchronous — it
    runs inside the finalize transaction and must not do I/O.
    """

    def _jobs(final_status: str) -> List[HookJob]:
        jobs: List[HookJob] = []
        is_ptc = msg_type == "ptc"
        rb_ptc = report_back_ptc_thread_id or thread_id

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
                    payload=ReportBackPayload(ptc_thread_id=thread_id),
                    ordering_key=thread_id,
                )
            )
        elif final_status == "interrupted" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="needs_input_wake",
                    idempotency_key=f"{run_id}:needs_input_wake",
                    payload=NeedsInputWakePayload(ptc_thread_id=thread_id),
                    ordering_key=thread_id,
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
                    ),
                    ordering_key=rb_ptc,
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
    limit: int = 10, lease_seconds: int = 60
) -> List[Dict[str, Any]]:
    """Lease a batch of due jobs: pending-and-due, or claimed with an
    expired lease (crash/stall recovery — reclaim IS the startup recovery).

    Ordering keys serialize per key: a job stays invisible while an earlier
    open (pending/claimed) job shares its key, so a stuck head blocks its
    chain but nothing else. The claim commits before execution; attempts
    counts leases handed out, not completions.
    """
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT o.hook_outbox_id FROM hook_outbox o
                    WHERE (
                        (o.status = 'pending'
                         AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW()))
                        OR (o.status = 'claimed' AND o.lease_expires_at <= NOW())
                    )
                    AND (
                        o.ordering_key IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM hook_outbox p
                            WHERE p.ordering_key = o.ordering_key
                              AND p.status IN ('pending', 'claimed')
                              AND (p.created_at, p.hook_outbox_id)
                                  < (o.created_at, o.hook_outbox_id)
                        )
                    )
                    ORDER BY o.created_at, o.hook_outbox_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE hook_outbox h
                SET status = 'claimed',
                    attempts = h.attempts + 1,
                    lease_expires_at = NOW() + make_interval(secs => %s)
                FROM due
                WHERE h.hook_outbox_id = due.hook_outbox_id
                RETURNING h.*
                """,
                (limit, float(lease_seconds)),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def ack_outbox_job(job_id: str) -> bool:
    """Mark a claimed job done. False = the row wasn't ours anymore
    (lease expired and another drainer took it) — never re-open it."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'done', completed_at = NOW()
                WHERE hook_outbox_id = %s AND status = 'claimed'
                """,
                (job_id,),
            )
            return cur.rowcount > 0


async def nack_outbox_job(job_id: str, *, max_attempts: int = 5) -> Optional[str]:
    """Return a failed claimed job to pending with exponential backoff,
    or park it dead at max_attempts. Returns the new status, or None if
    the row wasn't claimed (lost lease)."""
    async with qr_db.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = CASE WHEN attempts >= %s THEN 'dead' ELSE 'pending' END,
                    next_retry_at = NOW() + make_interval(
                        secs => LEAST(POWER(2, attempts), 60)::float
                    ),
                    lease_expires_at = NULL
                WHERE hook_outbox_id = %s AND status = 'claimed'
                RETURNING status
                """,
                (max_attempts, job_id),
            )
            row = await cur.fetchone()
            return row["status"] if row else None
