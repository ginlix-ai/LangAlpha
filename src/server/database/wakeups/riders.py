"""Wakeup notices that ride one wake turn.

A wake turn carries every notice owed on its thread when it starts, not only
its own job's: the head job stamps the others ``rides`` and keeps their ids
(``riders``), so they are neither claimed (the head's open lease holds the
chain) nor taken in by a live turn (the outbox's steerable predicate) nor
cancelled (``TOO_LATE_SQL``) while its POST may be in flight. A refusal holds
the riders with their head in the outbox's own exhaust statement, since that
must happen on every way a head runs out of attempts.
"""

from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.runs import outbox as outbox_db
from src.server.database.wakeups.contract import HOOK_TYPE, WAKEUP_OWED_SQL

# A thread holds at most ten open wakeups, a series one unread fire at a
# time, so this only bounds a malformed backlog.
_RIDERS_LIMIT = 20

_JOB_RETURNING_SQL = "o.hook_outbox_id, o.payload, o.created_at"


def _oldest_first(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        (dict(r) for r in rows),
        key=lambda r: (r["created_at"], str(r["hook_outbox_id"])),
    )


async def gather_wakeup_riders(
    head_job_id: str, *, attempts: int, lease_seconds: int = 60
) -> Optional[List[Dict[str, Any]]]:
    """The notices that ride the wake turn the claimed ``head_job_id`` is
    about to start, oldest first; None when the head's fence was lost.

    The thread's other wakeup jobs still owed that no turn holds and no
    other wake carries, each stamped ``rides``, and the head stamped with
    their ids. Frozen at the first gather: a retry after a crash or an
    unanswered POST sends the same block under the same request key, so
    whichever POST was admitted shows exactly what the settle stamps. A
    frozen rider no longer riding, closed or delivered on its own since, is
    left out of the retry; a cancel never closes one, a rider being past
    recall.

    Under the head's fence (``fenced_job_guard``), which renews its lease. A
    rider some other statement holds is skipped, not waited on: a cancel or a
    turn's hold racing for it either commits first, and it is not taken, or
    finds it riding and too late."""
    async with outbox_db.fenced_job_guard(
        head_job_id, attempts, lease_seconds=lease_seconds
    ) as fence:
        if not fence or fence.row["hook_type"] != HOOK_TYPE:
            return None
        cur = fence.cur
        frozen = (fence.row["payload"] or {}).get("riders")
        if isinstance(frozen, list):
            await cur.execute(
                f"""
                SELECT {_JOB_RETURNING_SQL} FROM hook_outbox o
                WHERE o.hook_outbox_id = ANY(%s::uuid[])
                  AND o.status = 'pending'
                  AND o.payload->>'rides' = %s::text
                """,
                ([str(i) for i in frozen], str(head_job_id)),
            )
            return _oldest_first(await cur.fetchall())
        await cur.execute(
            f"""
            WITH taken AS (
                SELECT o.hook_outbox_id FROM hook_outbox o
                WHERE o.ordering_key = %(okey)s
                  AND o.hook_type = %(type)s
                  AND o.hook_outbox_id <> %(head)s::uuid
                  AND o.status = 'pending'
                  AND NOT (o.payload ? 'steered_run_id')
                  AND NOT (o.payload ? 'rides')
                  AND {WAKEUP_OWED_SQL}
                ORDER BY o.created_at, o.hook_outbox_id
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE hook_outbox o
            SET payload = (o.payload - 'rode')
                || jsonb_build_object('rides', %(head)s::text)
            FROM taken
            WHERE o.hook_outbox_id = taken.hook_outbox_id
            RETURNING {_JOB_RETURNING_SQL}
            """,
            {
                "okey": fence.row["ordering_key"],
                "type": HOOK_TYPE,
                "head": str(head_job_id),
                "limit": _RIDERS_LIMIT,
            },
        )
        riders = _oldest_first(await cur.fetchall())
        await cur.execute(
            """
            UPDATE hook_outbox
            SET payload = payload || jsonb_build_object(
                    'riders', to_jsonb(%s::text[]))
            WHERE hook_outbox_id = %s
            """,
            ([str(r["hook_outbox_id"]) for r in riders], head_job_id),
        )
        return riders


async def settle_wakeup_riders(head_job_id: str, rider_ids: List[str]) -> List[str]:
    """Close the riders of ``head_job_id`` once its wake turn shows them;
    returns the ids closed. One the head's exhaustion held for a later turn
    (``rode``) is closed too, its notice being in that wake after all,
    unless a turn holds it again."""
    if not rider_ids:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'done', completed_at = NOW(), next_retry_at = NULL
                WHERE hook_outbox_id IN (
                    SELECT hook_outbox_id FROM hook_outbox
                    WHERE hook_outbox_id = ANY(%s::uuid[])
                      AND hook_type = %s AND status = 'pending'
                      AND NOT (payload ? 'steered_run_id')
                      AND (payload->>'rides' = %s::text
                           OR payload->>'rode' = %s::text)
                    ORDER BY hook_outbox_id
                    FOR UPDATE
                )
                RETURNING hook_outbox_id
                """,
                (
                    [str(i) for i in rider_ids],
                    HOOK_TYPE,
                    str(head_job_id),
                    str(head_job_id),
                ),
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def unfreeze_wakeup_riders(
    head_job_id: str, *, attempts: Optional[int] = None
) -> List[str]:
    """Let the riders of a head whose wake was not started go back to the
    thread: the head forgets them and each its ``rides``; returns their
    ids. For a POST that was definitely not admitted (the thread turned
    busy, or the notice was settled meanwhile); a refusal holds them with
    the head instead, and a head an earlier POST of which got no answer
    keeps them, since that wake may yet be admitted with them in it.
    ``attempts`` fences it to the caller's claim of the
    head; without it, to the head being unclaimed (parked), so a claim that
    took it meanwhile keeps what it froze."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                WITH head AS (
                    UPDATE hook_outbox
                    SET payload = payload - 'riders'
                    WHERE hook_outbox_id = %(head)s::uuid
                      AND hook_type = %(type)s
                      AND NOT (payload ? 'unanswered')
                      AND CASE WHEN %(attempts)s::int IS NULL
                            THEN status = 'pending'
                            ELSE status = 'claimed' AND attempts = %(attempts)s::int
                          END
                    RETURNING hook_outbox_id
                ),
                riders AS (
                    SELECT r.hook_outbox_id FROM hook_outbox r
                    WHERE r.hook_type = %(type)s AND r.status = 'pending'
                      AND r.payload->>'rides' = %(head)s::text
                      AND EXISTS (SELECT 1 FROM head)
                    ORDER BY r.hook_outbox_id
                    FOR UPDATE
                )
                UPDATE hook_outbox r
                SET payload = r.payload - 'rides'
                FROM riders
                WHERE r.hook_outbox_id = riders.hook_outbox_id
                RETURNING r.hook_outbox_id
                """,
                {"head": str(head_job_id), "type": HOOK_TYPE, "attempts": attempts},
            )
            return [str(r[0]) for r in await cur.fetchall()]


async def release_wakeup_ride(job_id: str, *, attempts: int) -> bool:
    """A claimed rider whose head's wake was never admitted delivers itself:
    it drops ``rides``, keeping the head it rode (``rode``). Fenced to the
    caller's claim."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET payload = (payload - 'rides')
                    || jsonb_build_object('rode', payload->>'rides')
                WHERE hook_outbox_id = %s AND hook_type = %s
                  AND status = 'claimed' AND attempts = %s
                  AND payload ? 'rides'
                """,
                (job_id, HOOK_TYPE, attempts),
            )
            return cur.rowcount > 0


async def wakeup_jobs(job_ids: List[str]) -> List[Dict[str, Any]]:
    """Wakeup jobs by id, whatever their state, oldest first: the notices a
    head's frozen riders name, read to settle a wake that was admitted."""
    if not job_ids:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_JOB_RETURNING_SQL}, o.status FROM hook_outbox o
                WHERE o.hook_outbox_id = ANY(%s::uuid[]) AND o.hook_type = %s
                """,
                ([str(i) for i in job_ids], HOOK_TYPE),
            )
            return _oldest_first(await cur.fetchall())
