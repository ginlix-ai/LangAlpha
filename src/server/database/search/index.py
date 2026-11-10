"""Filling ``thread_search_turns``, and finding the turns it is missing.

A turn's row is written only from its current response, the newest settled
attempt at its turn index, which is the row every history reader shows. The
write checks that in the statement itself, and replaces a row only with one
read from the same or a newer attempt no earlier than it was: two writers
racing across workers (the turn-end trigger, the sweep), or a resumed turn
whose text grew, never land the older text over the newer. A write that
would change nothing is skipped. A row goes with its response: an edit or a
regenerate deletes the responses it replaces, and the foreign key's cascade
takes their rows.

An answer whose read failed is written as unread: the row carries the
message and keeps an answer read before from the same response. Each failed
read counts in ``read_failures`` and sets ``retry_at`` further off, up to
``RETRY_CAP_S``; at ``MAX_READ_FAILURES`` the row stays unread with no
retry, until a read of the turn (its turn end, a later attempt) succeeds,
which clears both. A turn that committed no text is read, and empty.

A turn is stale when its current response has no row, or its row was read
from another one (an older attempt, before a retry settled). An unread
answer is not stale: its retries are its own (``due_retries``).

The backfill's walk over every thread keeps its place in
``thread_search_walk``'s one row, so the deployment walks once.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from psycopg.rows import class_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.database.search.sql import Keyset
from src.server.utils.pg_sanitize import normalize_uuid

# The current response of each settled turn of a thread, as history reads it.
_CURRENT_ATTEMPTS = _sql.settled_attempts(
    "conversation_response_id, turn_index, created_at"
)

# A response ``r`` that is its turn's current one, joined to its turn's row
# ``s``, which is missing or was read from another response.
_STALE_TURN = """
    LEFT JOIN thread_search_turns s
      ON s.conversation_thread_id = r.conversation_thread_id
     AND s.turn_index = r.turn_index
    WHERE r.status <> 'in_progress'
      AND NOT EXISTS (
          SELECT 1 FROM conversation_responses n
          WHERE n.conversation_thread_id = r.conversation_thread_id
            AND n.turn_index = r.turn_index
            AND n.status <> 'in_progress'
            AND n.attempt_no > r.attempt_no
      )
      AND s.conversation_response_id IS DISTINCT FROM r.conversation_response_id
"""

# An unread answer's retries: the first after ``RETRY_BASE_S``, each after
# twice the one before, up to ``RETRY_CAP_S``; none after the
# ``MAX_READ_FAILURES``th failure.
RETRY_BASE_S = 120
RETRY_CAP_S = 6 * 3600
MAX_READ_FAILURES = 8


def _retry_at(failures: str) -> str:
    """When a row with ``failures`` failed reads is read again; NULL for
    none."""
    return f"""CASE WHEN {failures} = 0 OR {failures} >= %(max_failures)s THEN NULL
        ELSE now() + make_interval(
            secs => LEAST(%(retry_base)s * 2 ^ ({failures} - 1), %(retry_cap)s))
        END"""


def _retry_params() -> dict:
    return {
        "max_failures": MAX_READ_FAILURES,
        "retry_base": RETRY_BASE_S,
        "retry_cap": RETRY_CAP_S,
    }


# A row's failed reads after a write: none once the answer is read, one
# more after a failed read of the same response, else one.
_FAILURES_AFTER = """CASE
        WHEN EXCLUDED.answer_read THEN 0
        WHEN NOT s.answer_read
             AND s.conversation_response_id = EXCLUDED.conversation_response_id
        THEN s.read_failures + 1
        ELSE 1 END"""


@dataclass(frozen=True)
class TurnSource:
    turn_index: int
    response_id: str
    user_text: str | None
    query_type: str | None
    turn_at: datetime


@dataclass(frozen=True)
class IndexedTurn:
    response_id: str
    user_text: str
    answer_text: str
    #: False when the answer exists but its read failed.
    answer_read: bool
    turn_at: datetime


@dataclass(frozen=True)
class StaleThread:
    thread_id: str
    #: Its stale turns, in order.
    turns: list[int]


@dataclass(frozen=True)
class WalkedThread:
    thread_id: str
    created_at: datetime
    #: Its stale turns, in order; empty for most.
    turns: list[int]

    @property
    def key(self) -> tuple[datetime, str]:
        """Where a walk of the threads stands after this one."""
        return (self.created_at, self.thread_id)


async def turns_to_index(
    thread_id: str,
    *,
    turn_indexes: Collection[int] | None = None,
    newest: int | None = None,
) -> tuple[list[TurnSource], datetime | None]:
    """The settled turns of a thread to index, newest first: those at
    ``turn_indexes``, or the ``newest`` few, with the user's message and when
    it was sent; and the database's time as it read them, which the rows
    read from them are written with (``write_turns``)."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return [], None
    where, params = "", [thread_id, thread_id]
    if turn_indexes is not None:
        where = "WHERE r.turn_index = ANY(%s)"
        params.append(list(turn_indexes))
    limit = ""
    if newest is not None:
        limit = "LIMIT %s"
        params.append(newest)
    async with pool.get_db_connection() as conn:
        # The database's clock, one for every writer, read before the turns
        # and so before their texts are.
        read_at = (await (await conn.execute("SELECT clock_timestamp()")).fetchone())[0]
        async with conn.cursor(row_factory=class_row(TurnSource)) as cur:
            await cur.execute(
                f"""
                SELECT r.turn_index, r.conversation_response_id::text AS response_id,
                       q.content AS user_text, q.type AS query_type,
                       COALESCE(q.created_at, r.created_at) AS turn_at
                FROM ({_CURRENT_ATTEMPTS}) r
                LEFT JOIN conversation_queries q
                  ON q.conversation_thread_id = %s AND q.turn_index = r.turn_index
                {where}
                ORDER BY r.turn_index DESC
                {limit}
                """,
                # The settled-attempts subquery binds first, then the join.
                params,
            )
            sources = await cur.fetchall()
    return sources, read_at


async def write_turns(
    thread_id: str, rows: Sequence[IndexedTurn], *, read_at: datetime | None
) -> int:
    """Write each turn's row from its response, read at ``read_at``, unless
    the response is no longer the turn's current one or the row holds a read
    as new. Returns the rows written or changed."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None or not rows or read_at is None:
        return 0
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                INSERT INTO thread_search_turns AS s (
                    conversation_thread_id, turn_index,
                    conversation_response_id, attempt_no, user_text,
                    answer_text, answer_read, read_failures, retry_at,
                    turn_at, read_at
                )
                SELECT r.conversation_thread_id, r.turn_index,
                       r.conversation_response_id, r.attempt_no, u.user_text,
                       u.answer_text, u.answer_read,
                       CASE WHEN u.answer_read THEN 0 ELSE 1 END,
                       {_retry_at("CASE WHEN u.answer_read THEN 0 ELSE 1 END")},
                       u.turn_at, %(read_at)s
                FROM unnest(%(response_ids)s::uuid[], %(user_texts)s::text[],
                            %(answer_texts)s::text[], %(answer_reads)s::boolean[],
                            %(turn_ats)s::timestamptz[])
                     AS u(response_id, user_text, answer_text, answer_read, turn_at)
                JOIN conversation_responses r
                  ON r.conversation_response_id = u.response_id
                WHERE r.conversation_thread_id = %(thread_id)s
                  AND r.status <> 'in_progress'
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses n
                      WHERE n.conversation_thread_id = r.conversation_thread_id
                        AND n.turn_index = r.turn_index
                        AND n.status <> 'in_progress'
                        AND n.attempt_no > r.attempt_no
                  )
                ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                SET conversation_response_id = EXCLUDED.conversation_response_id,
                    attempt_no = EXCLUDED.attempt_no,
                    user_text = EXCLUDED.user_text,
                    -- An unread answer keeps the one read before from the
                    -- same response.
                    answer_text = CASE
                        WHEN EXCLUDED.answer_read
                          OR s.conversation_response_id <> EXCLUDED.conversation_response_id
                        THEN EXCLUDED.answer_text ELSE s.answer_text END,
                    -- Whether the latest read reached the answer: a failed
                    -- reread of a read answer keeps its text searchable and
                    -- is retried, since the response may hold more now (an
                    -- interrupt answered at the next turn).
                    answer_read = EXCLUDED.answer_read,
                    read_failures = {_FAILURES_AFTER},
                    retry_at = {_retry_at(_FAILURES_AFTER)},
                    turn_at = EXCLUDED.turn_at,
                    read_at = EXCLUDED.read_at
                WHERE (EXCLUDED.attempt_no, EXCLUDED.read_at) >= (s.attempt_no, s.read_at)
                  AND (s.conversation_response_id <> EXCLUDED.conversation_response_id
                       OR s.user_text IS DISTINCT FROM EXCLUDED.user_text
                       OR s.turn_at IS DISTINCT FROM EXCLUDED.turn_at
                       -- A failed read always counts.
                       OR NOT EXCLUDED.answer_read
                       OR (EXCLUDED.answer_read
                           AND (NOT s.answer_read
                                OR s.answer_text IS DISTINCT FROM EXCLUDED.answer_text)))
                """,
                {
                    "response_ids": [row.response_id for row in rows],
                    "user_texts": [row.user_text for row in rows],
                    "answer_texts": [row.answer_text for row in rows],
                    "answer_reads": [row.answer_read for row in rows],
                    "turn_ats": [row.turn_at for row in rows],
                    "read_at": read_at,
                    "thread_id": thread_id,
                    **_retry_params(),
                },
            )
            return cur.rowcount


async def due_retries(*, limit: int) -> list[StaleThread]:
    """Threads with an unread answer whose retry is due, each with those
    turns, the longest due first. Threads in a deleted workspace are left
    out."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=class_row(StaleThread)) as cur:
            await cur.execute(
                """
                SELECT s.conversation_thread_id::text AS thread_id,
                       array_agg(s.turn_index ORDER BY s.turn_index) AS turns
                FROM thread_search_turns s
                JOIN conversation_threads t
                  ON t.conversation_thread_id = s.conversation_thread_id
                JOIN workspaces w ON w.workspace_id = t.workspace_id
                WHERE NOT s.answer_read AND s.retry_at <= now()
                  AND w.status <> 'deleted'
                GROUP BY s.conversation_thread_id
                ORDER BY min(s.retry_at)
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return await cur.fetchall()


async def defer_retries(thread_id: str, turn_indexes: Collection[int]) -> None:
    """Count a failed read against each unread answer at ``turn_indexes``,
    when the read failed before anything was written."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None or not turn_indexes:
        return
    failures = "(read_failures + 1)"
    async with pool.get_db_connection() as conn:
        await conn.execute(
            f"""
            UPDATE thread_search_turns
            SET read_failures = {failures}, retry_at = {_retry_at(failures)}
            WHERE conversation_thread_id = %(thread_id)s
              AND turn_index = ANY(%(turns)s) AND NOT answer_read
            """,
            {"thread_id": thread_id, "turns": list(turn_indexes), **_retry_params()},
        )


@dataclass(frozen=True)
class Walk:
    """Where the walk over every thread stands."""

    #: The ``WalkedThread.key`` the next slice starts after; None to start
    #: from the newest thread.
    after: tuple[datetime, str] | None
    #: Seconds until the next slice may run; 0 when it may now.
    wait_s: float
    #: Whether the walk last reached its end (the next slice starts anew).
    finished: bool


async def read_walk() -> Walk:
    async with pool.get_db_connection() as conn:
        row = await (
            await conn.execute(
                """
                SELECT after_created_at, after_thread_id::text, finished_at IS NOT NULL,
                       GREATEST(0, EXTRACT(EPOCH FROM next_slice_at - now()))::float8
                FROM thread_search_walk
                """
            )
        ).fetchone()
    if row is None:
        return Walk(None, 0.0, False)
    created_at, thread_id, finished, wait_s = row
    after = (created_at, thread_id) if created_at and thread_id and not finished else None
    return Walk(after, wait_s, finished)


async def save_walk(
    after: tuple[datetime, str] | None, *, rest_s: float, finished: bool
) -> None:
    """Record where the walk stands and when its next slice may run:
    ``rest_s`` from now, after which a finished walk starts anew."""
    created_at, thread_id = after if after and not finished else (None, None)
    async with pool.get_db_connection() as conn:
        await conn.execute(
            """
            INSERT INTO thread_search_walk AS w (
                one, after_created_at, after_thread_id, next_slice_at, finished_at
            )
            VALUES (TRUE, %(created_at)s, %(thread_id)s::uuid,
                    now() + make_interval(secs => %(rest_s)s),
                    CASE WHEN %(finished)s THEN now() END)
            ON CONFLICT (one) DO UPDATE
            SET after_created_at = EXCLUDED.after_created_at,
                after_thread_id = EXCLUDED.after_thread_id,
                next_slice_at = EXCLUDED.next_slice_at,
                finished_at = EXCLUDED.finished_at
            """,
            {
                "created_at": created_at,
                "thread_id": thread_id,
                "rest_s": float(rest_s),
                "finished": finished,
            },
        )


_BY_THREAD = Keyset((("r.conversation_thread_id", UUID),))


async def stale_since(
    since: datetime, *, limit: int, after: str | None = None
) -> list[StaleThread]:
    """Threads with a stale turn whose response settled since ``since``, each
    with its stale turns; ``after`` is the last thread of the page before.
    Threads in a deleted workspace are left out."""
    params: dict = {"since": since, "limit": limit}
    keyset = _BY_THREAD.after((after,) if after else None, params)
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=class_row(StaleThread)) as cur:
            await cur.execute(
                f"""
                SELECT r.conversation_thread_id::text AS thread_id,
                       array_agg(r.turn_index ORDER BY r.turn_index) AS turns
                FROM conversation_responses r
                JOIN conversation_threads t
                  ON t.conversation_thread_id = r.conversation_thread_id
                JOIN workspaces w ON w.workspace_id = t.workspace_id
                {_STALE_TURN}
                  AND r.usage_settled_at >= %(since)s
                  AND w.status <> 'deleted'
                  {keyset}
                GROUP BY r.conversation_thread_id
                ORDER BY {_BY_THREAD.order_by}
                LIMIT %(limit)s
                """,
                params,
            )
            return await cur.fetchall()


# Thread creation: never changes, so a walk by it passes every thread once.
# The column defaults to NOW() and nothing clears it.
_BY_CREATION = Keyset(
    (("t.created_at", datetime), ("t.conversation_thread_id", UUID))
)


async def walk_threads(
    *, limit: int, after: tuple[datetime, str] | None = None
) -> list[WalkedThread]:
    """The next ``limit`` threads, newest created first, each with its stale
    turns (none for most); ``after`` is the ``key`` of the last thread of the
    page before. Threads in a deleted workspace are left out."""
    params: dict = {"limit": limit}
    keyset = _BY_CREATION.after(after, params)
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=class_row(WalkedThread)) as cur:
            await cur.execute(
                f"""
                WITH page AS (
                    SELECT t.conversation_thread_id, t.created_at
                    FROM conversation_threads t
                    JOIN workspaces w ON w.workspace_id = t.workspace_id
                    WHERE w.status <> 'deleted' AND t.created_at IS NOT NULL
                      {keyset}
                    ORDER BY {_BY_CREATION.order_by}
                    LIMIT %(limit)s
                )
                SELECT p.conversation_thread_id::text AS thread_id, p.created_at,
                       COALESCE(stale.turns, '{{}}') AS turns
                FROM page p
                CROSS JOIN LATERAL (
                    SELECT array_agg(r.turn_index ORDER BY r.turn_index) AS turns
                    FROM conversation_responses r
                    {_STALE_TURN}
                      AND r.conversation_thread_id = p.conversation_thread_id
                ) stale
                ORDER BY p.created_at DESC, p.conversation_thread_id DESC
                """,
                params,
            )
            return await cur.fetchall()
