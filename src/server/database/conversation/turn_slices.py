"""Stored per-turn and per-subagent-run checkpoint slices (derived data).

Writes take a batch of rows in one statement. A row whose parent is already
gone (a concurrent fork or edit deletes the response, a thread delete takes
its runs) fails the whole batch on its foreign key, so the batch is retried a
row at a time and only the orphan is dropped: there is nothing left to keep.
"""

import logging
from typing import Any, NamedTuple

from psycopg.errors import ForeignKeyViolation
from psycopg.rows import dict_row

from src.server.database import pool
from src.server.utils.pg_sanitize import SafeJson

logger = logging.getLogger(__name__)


class StoredSlice(NamedTuple):
    """A slice as stored: the checkpoints it spans, the code that cut it, its
    bytes (None from a read that leaves them out) and, for a turn, the lines
    and run claims stored beside it."""

    input_checkpoint_id: str
    tail_checkpoint_id: str
    slice_key: str
    codec: str | None = None
    data: bytes | None = None
    lines_key: str | None = None
    lines: str | None = None
    claims: list[str | None] | None = None


class TurnRow(NamedTuple):
    response_id: str
    thread_id: str
    slice: StoredSlice


class TurnUpdate(NamedTuple):
    """Lines and run claims to set beside a stored slice; a None leaves its
    column. The tail keeps what was projected from a superseded slice from
    landing on its successor."""

    response_id: str
    tail_checkpoint_id: str
    lines_key: str | None
    lines: str | None
    claims: list[str | None] | None


class RunRow(NamedTuple):
    thread_id: str
    task_id: str
    task_run_id: str | None
    slice: StoredSlice


def _stored(row: dict[str, Any]) -> StoredSlice:
    data = row.get("slice")
    return StoredSlice(
        input_checkpoint_id=row["input_checkpoint_id"],
        tail_checkpoint_id=row["tail_checkpoint_id"],
        slice_key=row["slice_key"],
        codec=row.get("slice_codec"),
        data=bytes(data) if data is not None else None,
        lines_key=row.get("lines_key"),
        lines=row.get("lines"),
        claims=row.get("run_claims"),
    )


def _json(claims: list[str | None] | None) -> SafeJson | None:
    return SafeJson(claims) if claims is not None else None


_VALIDITY_COLUMNS = (
    "conversation_response_id::text AS response_id, input_checkpoint_id, "
    "tail_checkpoint_id, slice_key, lines_key, lines, run_claims"
)


async def _read_turns(response_ids: list[str], columns: str) -> dict[str, StoredSlice]:
    if not response_ids:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {columns} FROM turn_slices "
                "WHERE conversation_response_id = ANY(%s::uuid[])",
                (response_ids,),
            )
            return {r["response_id"]: _stored(r) for r in await cur.fetchall()}


async def get_turn_lines(response_ids: list[str]) -> dict[str, StoredSlice]:
    """Turns' stored slices by response id, never their bytes."""
    return await _read_turns(response_ids, _VALIDITY_COLUMNS)


async def get_turn_slices(response_ids: list[str]) -> dict[str, StoredSlice]:
    """Turns' stored slices by response id, bytes included."""
    return await _read_turns(response_ids, f"{_VALIDITY_COLUMNS}, slice_codec, slice")


async def has_turn_slices(thread_id: str, slice_key: str) -> bool:
    """Whether a thread holds any turn slice under ``slice_key``: one indexed
    probe that settles a thread not yet backfilled."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT EXISTS (SELECT 1 FROM turn_slices "
                "WHERE conversation_thread_id = %s::uuid AND slice_key = %s) AS held",
                (thread_id, slice_key),
            )
            row = await cur.fetchone()
            return bool(row and row["held"])


async def get_slices_at(
    thread_id: str, slice_key: str, input_checkpoint_ids: list[str]
) -> list[StoredSlice]:
    """A thread's turn slices under ``slice_key`` that start at these
    checkpoints, bytes included: what a reader holding turn anchors, and no
    response ids, reads."""
    if not input_checkpoint_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT input_checkpoint_id, tail_checkpoint_id, slice_key, "
                "slice_codec, slice FROM turn_slices "
                "WHERE conversation_thread_id = %s::uuid "
                "AND slice_key = %s AND input_checkpoint_id = ANY(%s)",
                (thread_id, slice_key, input_checkpoint_ids),
            )
            return [_stored(r) for r in await cur.fetchall()]


async def _write_many(sql: str, params: list[tuple[Any, ...]], what: str) -> None:
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.executemany(sql, params)
                return
            except ForeignKeyViolation:
                if len(params) == 1:
                    logger.debug("%s not stored, its parent row is gone", what)
                    return
            for row in params:
                try:
                    await cur.execute(sql, row)
                except ForeignKeyViolation:
                    logger.debug("%s not stored, its parent row is gone", what)


# Claims are written as given, never kept from the row: they were placed
# against the slice beside them, and a reader trusts them only while that
# slice is still the turn's.
_UPSERT_TURN = """
    INSERT INTO turn_slices
        (conversation_response_id, conversation_thread_id, input_checkpoint_id,
         tail_checkpoint_id, slice_key, slice_codec, slice, lines_key, lines,
         run_claims)
    VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (conversation_response_id) DO UPDATE SET
        conversation_thread_id = EXCLUDED.conversation_thread_id,
        input_checkpoint_id = EXCLUDED.input_checkpoint_id,
        tail_checkpoint_id = EXCLUDED.tail_checkpoint_id,
        slice_key = EXCLUDED.slice_key,
        slice_codec = EXCLUDED.slice_codec,
        slice = EXCLUDED.slice,
        lines_key = EXCLUDED.lines_key,
        lines = EXCLUDED.lines,
        run_claims = EXCLUDED.run_claims,
        built_at = NOW()
"""


async def upsert_turn_slices(rows: list[TurnRow]) -> None:
    """Store turns' slices, each with its lines and run claims when known."""
    if not rows:
        return
    await _write_many(
        _UPSERT_TURN,
        [
            (
                r.response_id,
                r.thread_id,
                r.slice.input_checkpoint_id,
                r.slice.tail_checkpoint_id,
                r.slice.slice_key,
                r.slice.codec,
                r.slice.data,
                r.slice.lines_key,
                r.slice.lines,
                _json(r.slice.claims),
            )
            for r in rows
        ],
        "turn slice",
    )


async def update_turns(rows: list[TurnUpdate]) -> None:
    """Set lines and run claims on stored slices."""
    if not rows:
        return
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """UPDATE turn_slices
                   SET lines_key = COALESCE(%s, lines_key),
                       lines = COALESCE(%s, lines),
                       run_claims = COALESCE(%s::jsonb, run_claims),
                       built_at = NOW()
                   WHERE conversation_response_id = %s::uuid
                     AND tail_checkpoint_id = %s""",
                [
                    (
                        r.lines_key,
                        r.lines,
                        _json(r.claims),
                        r.response_id,
                        r.tail_checkpoint_id,
                    )
                    for r in rows
                ],
            )


async def get_run_slices(thread_id: str, task_ids: list[str]) -> list[RunRow]:
    """Every stored run slice of the given tasks."""
    if not task_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT task_id, input_checkpoint_id,
                          task_run_id::text AS task_run_id, tail_checkpoint_id,
                          slice_key, slice_codec, slice
                   FROM task_run_slices
                   WHERE conversation_thread_id = %s::uuid
                     AND task_id = ANY(%s)""",
                (thread_id, task_ids),
            )
            return [
                RunRow(thread_id, r["task_id"], r["task_run_id"], _stored(r))
                for r in await cur.fetchall()
            ]


async def upsert_run_slices(rows: list[RunRow]) -> None:
    """Store subagent runs' slices."""
    if not rows:
        return
    await _write_many(
        """INSERT INTO task_run_slices
               (conversation_thread_id, task_id, input_checkpoint_id,
                task_run_id, tail_checkpoint_id, slice_key, slice_codec, slice)
           VALUES (%s::uuid, %s, %s, %s::uuid, %s, %s, %s, %s)
           ON CONFLICT (conversation_thread_id, task_id, input_checkpoint_id)
           DO UPDATE SET
               task_run_id = EXCLUDED.task_run_id,
               tail_checkpoint_id = EXCLUDED.tail_checkpoint_id,
               slice_key = EXCLUDED.slice_key,
               slice_codec = EXCLUDED.slice_codec,
               slice = EXCLUDED.slice,
               built_at = NOW()""",
        [
            (
                r.thread_id,
                r.task_id,
                r.slice.input_checkpoint_id,
                r.task_run_id,
                r.slice.tail_checkpoint_id,
                r.slice.slice_key,
                r.slice.codec,
                r.slice.data,
            )
            for r in rows
        ],
        "run slice",
    )
