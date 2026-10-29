"""The runtime credit gate's SQL surface, over both run tables.

Everything here exists because a run's platform spend is invisible until
finalize writes its usage row. The ledger columns make it visible in between,
so a reader can aggregate "unsettled in-flight spend per user" while the runs
are still going, and the resume query at the bottom is how a turn that stopped
for money finds the tasks that stopped with it.

Terminal and billing-settled are separate states throughout, deliberately: a
task run turns terminal in one CAS while its usage is collected later, so a row
keeps counting toward in-flight spend until ``usage_settled_at`` is stamped,
whatever its status says.

Held apart from ``lifecycle`` and ``subagent_runs`` rather than split between
them because it is one concept with one client. Both ledgers carry the same
columns and answer the same questions, so a lane names itself with ``kind`` and
the table is resolved below — the shape it replaced was the same two statements
written out twice, in two modules, kept in agreement by hand.
"""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Literal, Tuple

from psycopg.rows import dict_row

from src.server.contracts.status import (
    CREDIT_STOP_ERROR_TYPE,
    INTERRUPT_REASON_CREDIT_PAUSE,
)
from src.server.database import pool

logger = logging.getLogger(__name__)

RunKind = Literal["run", "task"]

# The only thing that differs between the two ledgers. Closed literal, and the
# only source of the identifiers interpolated into the statements below.
_TABLES: Dict[RunKind, Tuple[str, str]] = {
    "run": ("conversation_responses", "conversation_response_id"),
    "task": ("subagent_runs", "task_run_id"),
}

_ABANDONED_SETTLE_GRACE = "30 minutes"


async def heartbeat(kind: RunKind, run_id: str, credits: float) -> bool:
    """Absolute write of a live run's cumulative platform spend onto its own row.

    GREATEST keeps the value monotone under reordered or duplicate flushes; the
    status and settle guards make a late heartbeat racing finalize a no-op
    rather than a resurrection of already-billed spend. Always a fresh app-pool
    connection — this fires from a background refresher and must never touch a
    run's pinned checkpointer session. Returns False when the row was already
    terminal or settled.

    Bound as Decimal because GREATEST(numeric, float8) resolves in double
    precision: a float parameter would drag the stored column value out to
    binary float and back on every beat, rounding a money-shaped number for
    no reason.
    """
    table, key = _TABLES[kind]
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE {table}
                SET in_flight_credits = GREATEST(in_flight_credits, %s)
                WHERE {key} = %s
                  AND status = 'in_progress'
                  AND usage_settled_at IS NULL
                """,
                (Decimal(str(credits)), run_id),
            )
            return cur.rowcount == 1


async def settle_task_run(task_run_id: str, *, conn) -> bool:
    """Stamp a task run billing-settled, in the collector's usage transaction.

    ``conn`` is mandatory: the whole point of the stamp is that it commits or
    rolls back with the usage row the collector just inserted, so the row can
    never read settled while its spend is missing from billing, nor billed
    while still counting as in-flight.

    The chain, not the row: a resume re-points the task at a new run id while
    its predecessor's records stay merged into the batch being billed here, so
    the predecessor settles in the same transaction or never.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            WITH RECURSIVE chain AS (
                SELECT task_run_id, predecessor_run_id
                FROM subagent_runs WHERE task_run_id = %s
              -- UNION, not UNION ALL: the pair is functional, so dedup costs
              -- nothing on a real chain and is what terminates the walk if a
              -- predecessor ever points back into it. This runs inside the
              -- collector's open write transaction.
              UNION
                SELECT s.task_run_id, s.predecessor_run_id
                FROM subagent_runs s
                JOIN chain c ON s.task_run_id = c.predecessor_run_id
            )
            UPDATE subagent_runs
            SET usage_settled_at = NOW()
            WHERE task_run_id IN (SELECT task_run_id FROM chain)
              AND usage_settled_at IS NULL
            """,
            (task_run_id,),
        )
        return cur.rowcount >= 1


async def settle_abandoned(kind: RunKind) -> int:
    """Degraded settle: stamp terminal-but-unsettled rows no settle path reaches.

    The two lanes need different proof that none is coming. A terminal response
    row is proof on its own — finalize stamps the settle in the same CAS, so
    anything caught here was finalized by code predating the stamp. A terminal
    task run is not, whatever status it carries: the collector claims by
    ownership rather than status, so a row it is about to bill is
    indistinguishable from one whose parent died before collecting. So that
    lane needs two proofs rather than one, and both are the same grace read
    against different clocks: the collector is spawned only once the PARENT
    turn goes terminal, so its chance starts at whichever came later, the
    child's own finalization or the parent's. The child's age alone cannot see
    that — a subagent that finishes early under a long turn would be stamped
    billed 30 minutes later and drop out of the in-flight aggregate for the
    rest of that turn, understating the balance every admission in the window
    is measured against. The parent's status alone cannot see it either: a
    child that finished long before its parent satisfies its own grace already,
    so a bare "parent is not in_progress" opens the moment the parent commits,
    which is the moment BEFORE the collector it spawns has inserted anything.
    Hence the parent's terminal instant, and hence ``usage_settled_at`` for it:
    the finalize CAS stamps that in the same statement that turns the row
    terminal, and it is the only timestamp the response table keeps. A parent
    finalized by pre-gate code carries none, which reads as "chance not started"
    and holds the child until the run lane above stamps it — a grace later than
    strictly needed, in the direction that cannot overstate anyone's balance.

    ``user_id IS NOT NULL`` is what lets this use the partial in-flight index
    rather than scanning the table: the index carries that predicate, so
    without it here the planner cannot prove the index covers every matching
    row. It costs nothing to add, because an owner-less row is exactly the row
    this sweep exists to keep out of a per-user aggregate that can never see
    it anyway. On a healthy ledger the scan touches nothing.
    """
    table, _ = _TABLES[kind]
    guard = (
        f"""
                  AND finalized_at < NOW() - INTERVAL '{_ABANDONED_SETTLE_GRACE}'
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses r
                      WHERE r.conversation_response_id = {table}.parent_run_id
                        AND (
                            r.status = 'in_progress'
                            OR r.usage_settled_at IS NULL
                            OR r.usage_settled_at
                                > NOW() - INTERVAL '{_ABANDONED_SETTLE_GRACE}'
                        )
                  )
        """
        if kind == "task"
        else ""
    )
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE {table}
                SET usage_settled_at = NOW()
                WHERE usage_settled_at IS NULL
                  AND in_flight_credits > 0
                  AND user_id IS NOT NULL
                  AND status != 'in_progress'
                  {guard}
                """
            )
            return cur.rowcount


async def list_credit_stopped_for_resume(
    thread_id: str, resuming_run_id: str
) -> List[Dict[str, Any]]:
    """Task runs the credit gate stopped under the attempt being resumed.

    Feeds the resume-time injection: the model gets a mechanical record of
    which tasks stopped so it can re-enter them via Task(action="resume").

    The scope is the run immediately before ``resuming_run_id``, and it yields
    nothing unless that run is itself the credit pause. Anchoring on "the
    newest credit pause" instead would re-announce the same stops into
    checkpoint state on every later approval the thread ever asks for.

    The resuming run is excluded by attempt chain rather than by being newer
    than the query: a resume opens its row in the START transaction, well
    before this is called, so "latest row in the thread" is that row and every
    predicate below would miss. The chain rather than the single id, because a
    resume that fails retryably is retried as a NEW row chained by
    ``retry_of_run_id`` while the pause stays two rows back — excluding only
    the resuming run would anchor on the failed attempt, whose status is
    'error', and the retry that finally succeeds would announce nothing.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH RECURSIVE attempt_chain AS (
                    SELECT conversation_response_id, retry_of_run_id
                    FROM conversation_responses
                    WHERE conversation_response_id = %s
                  -- UNION, not UNION ALL: same reason as the predecessor walk
                  -- in settle_task_run, and the pair is functional so dedup
                  -- costs nothing on a real chain.
                  UNION
                    SELECT r.conversation_response_id, r.retry_of_run_id
                    FROM conversation_responses r
                    JOIN attempt_chain c
                      ON r.conversation_response_id = c.retry_of_run_id
                ),
                resumed AS (
                    SELECT conversation_response_id, status, interrupt_reason
                    FROM conversation_responses
                    WHERE conversation_thread_id = %s
                      AND conversation_response_id NOT IN (
                          SELECT conversation_response_id FROM attempt_chain
                      )
                    ORDER BY run_seq DESC
                    LIMIT 1
                )
                SELECT s.task_id
                FROM subagent_runs s
                JOIN resumed r ON s.parent_run_id = r.conversation_response_id
                -- Redundant against the join, but it is what makes this an
                -- index scan: parent_run_id carries no index of its own, so
                -- without the thread the join drives off a full table scan.
                WHERE s.thread_id = %s
                  AND r.status = 'interrupted'
                  AND r.interrupt_reason = %s
                  AND s.status = 'cancelled'
                  AND s.failure->>'error_type' = %s
                ORDER BY s.started_at
                """,
                (
                    resuming_run_id,
                    thread_id,
                    thread_id,
                    INTERRUPT_REASON_CREDIT_PAUSE,
                    CREDIT_STOP_ERROR_TYPE,
                ),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


__all__ = [
    "RunKind",
    "heartbeat",
    "list_credit_stopped_for_resume",
    "settle_abandoned",
    "settle_task_run",
]
