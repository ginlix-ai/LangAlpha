"""Writing a fork's checkpoints and task runs from its source thread.

A fork holds one checkpoint of its own, the source's state at the cut, under
the cut's checkpoint id with no parent. ``messages`` and ``claude_session``
are ``DeltaChannel``s, which a checkpoint holds no value for but the writes
back to the last snapshot do, so the fork stores each as a
``_DeltaSnapshot`` of its value there; the plain channels' blobs and the
cut's own writes (which a stopped turn resumes from) are copied as stored.
The turns before the cut keep their rows in the source, which the fork shows
them from.

Background task lanes and the finished task runs the kept turns launched are
still copied whole, each copy under a new id, and every value naming a run
is rewritten to its copy's: the checkpoints' ``task_run_id`` stamps, the
lanes' ``ui`` records that name their run (a workflow's archived result,
which TaskOutput finds by the ledger's run id, and its lifecycle snapshot),
and the launch artifacts on ``messages``, by which replay finds the run a
Task call started. Checkpoints are immutable once written, so the copy needs
no lock on the source's chain; the ledger read is checked against a run
horizon instead.
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from langchain_core.messages import BaseMessage
from langgraph.checkpoint.serde.types import _DeltaSnapshot

from psycopg.rows import dict_row
from psycopg.types.json import Json

logger = logging.getLogger(__name__)

_CHAIN_SQL = """
    CREATE TEMP TABLE IF NOT EXISTS fork_chain (
        checkpoint_ns text NOT NULL,
        checkpoint_id text NOT NULL,
        PRIMARY KEY (checkpoint_ns, checkpoint_id)
    ) ON COMMIT DROP;
    """

# The tip's ancestors, by the same LATERAL primary-key probe the branch walk
# uses (``checkpoint_helpers._BRANCH_BOUNDARIES_SQL``): a plain join hashes
# the thread's whole history once per step.
_FILL_CHAIN_SQL = """
    INSERT INTO fork_chain (checkpoint_ns, checkpoint_id)
    WITH RECURSIVE chain AS (
        SELECT checkpoint_id, parent_checkpoint_id
        FROM checkpoints
        WHERE thread_id = %(src)s AND checkpoint_ns = %(ns)s
          AND checkpoint_id = %(tip)s
      UNION ALL
        SELECT p.checkpoint_id, p.parent_checkpoint_id
        FROM chain c CROSS JOIN LATERAL (
            SELECT k.checkpoint_id, k.parent_checkpoint_id
            FROM checkpoints k
            WHERE k.thread_id = %(src)s AND k.checkpoint_ns = %(ns)s
              AND k.checkpoint_id = c.parent_checkpoint_id
            LIMIT 1
        ) p
    )
    SELECT %(ns)s, checkpoint_id FROM chain
    ON CONFLICT DO NOTHING
"""

_COPY_CHECKPOINTS_SQL = """
    INSERT INTO checkpoints (
        thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
        type, checkpoint, metadata
    )
    SELECT %(dst)s, c.checkpoint_ns, c.checkpoint_id, c.parent_checkpoint_id,
           c.type, c.checkpoint,
           c.metadata
           || CASE WHEN %(runs)s::jsonb ? (c.metadata->>'run_id')
                   THEN jsonb_build_object(
                       'run_id', %(runs)s::jsonb -> (c.metadata->>'run_id'))
                   ELSE '{}'::jsonb END
           || CASE WHEN %(runs)s::jsonb ? (c.metadata->>'task_run_id')
                   THEN jsonb_build_object(
                       'task_run_id', %(runs)s::jsonb -> (c.metadata->>'task_run_id'))
                   ELSE '{}'::jsonb END
    FROM checkpoints c
    JOIN fork_chain f
      ON f.checkpoint_ns = c.checkpoint_ns AND f.checkpoint_id = c.checkpoint_id
    WHERE c.thread_id = %(src)s
"""

# Every write of the chain, the cut's included.
_COPY_WRITES_SQL = """
    INSERT INTO checkpoint_writes (
        thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel,
        type, blob, task_path
    )
    SELECT %(dst)s, w.checkpoint_ns, w.checkpoint_id, w.task_id, w.idx,
           w.channel, w.type, w.blob, w.task_path
    FROM checkpoint_writes w
    JOIN fork_chain f
      ON f.checkpoint_ns = w.checkpoint_ns AND f.checkpoint_id = w.checkpoint_id
    WHERE w.thread_id = %(src)s
"""

_COPY_BLOBS_SQL = """
    INSERT INTO checkpoint_blobs (
        thread_id, checkpoint_ns, channel, version, type, blob
    )
    SELECT %(dst)s, b.checkpoint_ns, b.channel, b.version, b.type, b.blob
    FROM checkpoint_blobs b
    WHERE b.thread_id = %(src)s
      AND (b.checkpoint_ns, b.channel, b.version) IN (
          SELECT DISTINCT c.checkpoint_ns, v.key, v.value
          FROM checkpoints c
          JOIN fork_chain f
            ON f.checkpoint_ns = c.checkpoint_ns
           AND f.checkpoint_id = c.checkpoint_id
          CROSS JOIN LATERAL jsonb_each_text(c.checkpoint->'channel_versions') v
          WHERE c.thread_id = %(src)s
      )
    ON CONFLICT DO NOTHING
"""

# A channel's copied values that name a task run: the writes that carried
# them and the snapshots that hold them. The ids are stored as plain bytes
# under msgpack, so a byte search skips every value naming none of them
# before anything is decoded.
_NAMES_A_RUN = """
    EXISTS (SELECT 1 FROM unnest(%(ids)s::text[]) i
            WHERE position(convert_to(i, 'UTF8') IN blob) > 0)
"""

_NAMING_WRITES_SQL = f"""
    SELECT checkpoint_ns, checkpoint_id, task_id, idx, type, blob
    FROM checkpoint_writes
    WHERE thread_id = %(dst)s AND checkpoint_ns = ANY(%(nss)s) AND channel = %(channel)s
      AND blob IS NOT NULL AND {_NAMES_A_RUN}
"""

_NAMING_BLOBS_SQL = f"""
    SELECT checkpoint_ns, version, type, blob
    FROM checkpoint_blobs
    WHERE thread_id = %(dst)s AND checkpoint_ns = ANY(%(nss)s) AND channel = %(channel)s
      AND blob IS NOT NULL AND {_NAMES_A_RUN}
"""

#: A record id that ends in a run id, as ``task-result-{task_run_id}`` and
#: ``workflow-run-{task_run_id}`` do.
_TRAILING_RUN_ID = re.compile(
    r"^(.*?)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
)


@dataclass(frozen=True)
class ChainTip:
    """One namespace's chain to copy: its newest checkpoint kept."""

    checkpoint_ns: str
    checkpoint_id: str


@dataclass
class LedgerPlan:
    """What a fork reads of the source's ledger, under one snapshot: the
    finished task runs it copies, with the ids their copies take, minted
    before anything is written so the checkpoint copy can stamp them first,
    and the response that ended the last kept turn."""

    source_thread_id: str = ""
    #: Each copied task's row and its finished runs, oldest first.
    tasks: list[tuple[dict[str, Any], list[dict[str, Any]]]] = field(default_factory=list)
    #: Source task run id to its copy's.
    run_map: dict[str, str] = field(default_factory=dict)
    #: The task lanes the copy carries, each to the newest checkpoint a
    #: copied run of it finished on.
    task_tips: list[ChainTip] = field(default_factory=list)
    #: The newest response of the newest kept turn that has one: while it
    #: exists, the source has not rewritten the turns the fork borrows.
    response_id: Optional[str] = None


class StaleSource(Exception):
    """A run that rewrote the kept turns started after the snapshot, or the
    source went away."""


@dataclass(frozen=True)
class ForkCheckpoint:
    """The fork's one checkpoint: the source's cut, its delta channels
    materialized (``forks.materialize``)."""

    checkpoint_id: str
    #: The cut's checkpoint as stored, primitives inline.
    checkpoint: dict[str, Any]
    #: The cut's ``step``.
    step: Any
    #: Each delta channel's value at the cut, by channel.
    deltas: dict[str, Any]
    #: The plain channels whose blobs the fork needs, at the cut's versions.
    required: frozenset[str]


async def write_snapshot(conn: Any, target_thread_id: str, cut: ForkCheckpoint) -> None:
    """Write ``cut`` as the target's only main checkpoint, inside the
    caller's transaction: the cut's id, no parent, its versions verbatim, and
    each delta channel as a ``_DeltaSnapshot`` at its version, so a load
    reads it without walking back. Saved by a saver of its own on ``conn``
    with the app saver's serde, so the values read back as written."""
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from src.server.utils.checkpoint_helpers import get_checkpointer

    raw = cut.checkpoint
    versions = raw.get("channel_versions") or {}
    deltas = {k: v for k, v in cut.deltas.items() if k in versions}
    checkpoint = {
        **raw,
        "id": cut.checkpoint_id,
        "channel_values": {
            **{k: v for k, v in (raw.get("channel_values") or {}).items() if k not in deltas},
            **{k: _DeltaSnapshot(v) for k, v in deltas.items()},
        },
    }
    saver = AsyncPostgresSaver(conn, serde=get_checkpointer().serde)
    await saver.aput(
        {"configurable": {"thread_id": target_thread_id, "checkpoint_ns": ""}},
        checkpoint,
        {"source": "fork", "step": cut.step, "parents": {}},
        {k: versions[k] for k in deltas},
    )


async def copy_cut_blobs(
    conn: Any, *, source_thread_id: str, target_thread_id: str, cut: ForkCheckpoint
) -> None:
    """Copy the plain channels' blobs at the cut's versions. Raises
    ``StaleSource`` when one the state holds a value for is not there: the
    fork would load without it."""
    versions = {
        k: v
        for k, v in (cut.checkpoint.get("channel_versions") or {}).items()
        if k not in cut.deltas
    }
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)
            SELECT %(dst)s, b.checkpoint_ns, b.channel, b.version, b.type, b.blob
            FROM checkpoint_blobs b
            JOIN jsonb_each_text(%(versions)s::jsonb) v
              ON v.key = b.channel AND v.value = b.version
            WHERE b.thread_id = %(src)s AND b.checkpoint_ns = ''
            ON CONFLICT DO NOTHING
            RETURNING channel
            """,
            {"src": source_thread_id, "dst": target_thread_id, "versions": Json(versions)},
        )
        copied = {row["channel"] for row in await cur.fetchall()}
    if missing := cut.required - copied:
        logger.warning(
            "[fork] cut %s of %s has no blob for %s", cut.checkpoint_id, source_thread_id, sorted(missing)
        )
        raise StaleSource(source_thread_id)


async def copy_cut_writes(
    conn: Any, *, source_thread_id: str, target_thread_id: str, checkpoint_id: str, run_map: dict[str, str]
) -> None:
    """Copy the cut's own writes, which a turn stopped there resumes from,
    with the launches on ``messages`` pointed at the task runs' copies."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO checkpoint_writes (
                thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel,
                type, blob, task_path
            )
            SELECT %s, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob, task_path
            FROM checkpoint_writes
            WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
            """,
            (target_thread_id, source_thread_id, checkpoint_id),
        )
    if run_map:
        await _remap_writes(conn, target_thread_id, "messages", [""], run_map, remap_launches)


async def copy_checkpoint_chains(
    conn: Any,
    *,
    source_thread_id: str,
    target_thread_id: str,
    tips: Iterable[ChainTip],
    run_map: dict[str, str],
) -> int:
    """Copy each task lane's chain from ``source_thread_id`` into
    ``target_thread_id``, inside the caller's transaction, stamping
    ``run_map``'s ids on the checkpoints and in the values that name a task
    run. Returns how many checkpoints were copied."""
    tips = list(tips)
    if not tips:
        return 0
    async with conn.cursor() as cur:
        await cur.execute(_CHAIN_SQL)
        for tip in tips:
            await cur.execute(
                _FILL_CHAIN_SQL,
                {"src": source_thread_id, "ns": tip.checkpoint_ns, "tip": tip.checkpoint_id},
            )
        params = {"src": source_thread_id, "dst": target_thread_id, "runs": Json(run_map)}
        await cur.execute(_COPY_CHECKPOINTS_SQL, params)
        copied = cur.rowcount
        await cur.execute(_COPY_WRITES_SQL, params)
        await cur.execute(_COPY_BLOBS_SQL, params)
    if run_map:
        lanes = sorted({t.checkpoint_ns for t in tips})
        await _remap_channel(conn, target_thread_id, "messages", lanes, run_map, remap_launches)
        await _remap_channel(conn, target_thread_id, "ui", lanes, run_map, remap_ui_records)
    return copied


async def _remap_channel(
    conn: Any,
    target_thread_id: str,
    channel: str,
    namespaces: list[str],
    run_map: dict[str, str],
    remap: Callable[[Any, dict[str, str]], Any],
) -> None:
    """Point a copied channel's values, its writes and its blobs, at the
    copies' task run ids.

    Replay finds the run a Task call started by the id on its launch artifact
    (``messages``), and TaskOutput finds a workflow's archived result by the
    record naming its run (a lane's ``ui``); both look up the ledger, where the
    copy has the new ids. Only values that change are rewritten; the rest keep
    their bytes.
    """
    await _remap_writes(conn, target_thread_id, channel, namespaces, run_map, remap)
    if not namespaces:
        return
    serde = _serde()
    params = {"dst": target_thread_id, "nss": namespaces, "channel": channel, "ids": list(run_map)}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_NAMING_BLOBS_SQL, params)
        for b in await cur.fetchall():
            blob = _remapped_blob(serde, b["type"], b["blob"], run_map, remap)
            if blob is not None:
                await cur.execute(
                    """
                    UPDATE checkpoint_blobs SET type = %s, blob = %s
                    WHERE thread_id = %s AND checkpoint_ns = %s AND channel = %s
                      AND version = %s
                    """,
                    (*blob, target_thread_id, b["checkpoint_ns"], channel, b["version"]),
                )


async def _remap_writes(
    conn: Any,
    target_thread_id: str,
    channel: str,
    namespaces: list[str],
    run_map: dict[str, str],
    remap: Callable[[Any, dict[str, str]], Any],
) -> None:
    if not namespaces:
        return
    serde = _serde()
    params = {"dst": target_thread_id, "nss": namespaces, "channel": channel, "ids": list(run_map)}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_NAMING_WRITES_SQL, params)
        for w in await cur.fetchall():
            blob = _remapped_blob(serde, w["type"], w["blob"], run_map, remap)
            if blob is not None:
                await cur.execute(
                    """
                    UPDATE checkpoint_writes SET type = %s, blob = %s
                    WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s
                      AND task_id = %s AND idx = %s
                    """,
                    (*blob, target_thread_id, w["checkpoint_ns"], w["checkpoint_id"],
                     w["task_id"], w["idx"]),
                )


def _serde() -> Any:
    """The app saver's own serde, so a rewritten value reads back as written."""
    from src.server.utils.checkpoint_helpers import get_checkpointer

    return get_checkpointer().serde


def _remapped_blob(
    serde: Any,
    type_: str,
    blob: bytes,
    run_map: dict[str, str],
    remap: Callable[[Any, dict[str, str]], Any],
) -> Optional[tuple[str, bytes]]:
    """The re-encoded value when ``remap`` changed it, else None."""
    if not type_ or type_ == "empty" or blob is None:
        return None
    value = serde.loads_typed((type_, bytes(blob)))
    remapped = remap(value, run_map)
    return None if remapped is value else serde.dumps_typed(remapped)


def remap_launches(value: Any, run_map: dict[str, str]) -> Any:
    """A ``messages`` value (a write's messages, or a snapshot of them) with
    each Task launch artifact's ``task_run_id`` mapped through ``run_map``.
    Returns ``value`` itself when nothing names a mapped run."""
    if isinstance(value, _DeltaSnapshot):
        inner = remap_launches(value.value, run_map)
        return value if inner is value.value else _DeltaSnapshot(inner)
    if isinstance(value, (list, tuple)):
        out = [remap_launches(v, run_map) for v in value]
        return value if all(a is b for a, b in zip(out, value)) else type(value)(out)
    if isinstance(value, BaseMessage):
        kwargs = value.additional_kwargs or {}
        artifact = kwargs.get("task_artifact")
        if isinstance(artifact, dict) and (new := run_map.get(str(artifact.get("task_run_id")))):
            return value.model_copy(
                update={"additional_kwargs": {**kwargs, "task_artifact": {**artifact, "task_run_id": new}}}
            )
    return value


def remap_ui_records(value: Any, run_map: dict[str, str]) -> Any:
    """``value`` (a ``ui`` record or a list of them) with each record's
    ``props.task_run_id`` and run-suffixed ``id`` mapped through ``run_map``.
    Returns ``value`` itself when nothing names a mapped run."""
    if isinstance(value, dict):
        return _remap_record(value, run_map)
    if not isinstance(value, list):
        return value
    out = [_remap_record(r, run_map) if isinstance(r, dict) else r for r in value]
    return value if all(a is b for a, b in zip(out, value)) else out


def _remap_record(record: dict[str, Any], run_map: dict[str, str]) -> dict[str, Any]:
    changed: dict[str, Any] = {}
    props = record.get("props")
    if isinstance(props, dict) and (new := run_map.get(str(props.get("task_run_id")))):
        changed["props"] = {**props, "task_run_id": new}
    rid = record.get("id")
    if isinstance(rid, str) and (m := _TRAILING_RUN_ID.match(rid)) and (new := run_map.get(m[2])):
        changed["id"] = m[1] + new
    return {**record, **changed} if changed else record


async def ledger_view(thread_id: str, turn_index: int) -> tuple[dict[int, str], Optional[str]]:
    """Each persisted turn's newest attempt status (None for a turn with a
    message and no attempt yet), and ``turn_index``'s message."""
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT q.turn_index, r.status
            FROM (
              SELECT turn_index FROM conversation_queries WHERE conversation_thread_id = %(t)s
              UNION
              SELECT turn_index FROM conversation_responses WHERE conversation_thread_id = %(t)s
            ) q
            LEFT JOIN LATERAL (
              SELECT status FROM conversation_responses
              WHERE conversation_thread_id = %(t)s AND turn_index = q.turn_index
              ORDER BY run_seq DESC LIMIT 1
            ) r ON TRUE
            """,
            {"t": thread_id},
        )
        newest = {row["turn_index"]: row["status"] for row in await cur.fetchall()}
        await cur.execute(
            """
            SELECT content FROM conversation_queries
            WHERE conversation_thread_id = %s AND turn_index = %s
            """,
            (thread_id, turn_index),
        )
        query = await cur.fetchone()
    return newest, query and query["content"]


async def read_one_snapshot(conn: Any) -> None:
    """Make the caller's just-opened transaction read one snapshot, read
    only. Set per transaction, so the pooled connection goes back unchanged."""
    await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")


async def try_lock_thread_shared(conn: Any, key: int) -> bool:
    """Take the thread lock ``key`` shared until the transaction ends, if free."""
    row = await (await conn.execute("SELECT pg_try_advisory_xact_lock_shared(%s)", (key,))).fetchone()
    return bool(row[0])


async def hold_thread_row(conn: Any, thread_id: str) -> None:
    """Hold the thread's row until the transaction ends: a change to who can
    read the thread (``thread_reach.set_shared`` locks it for update) waits
    on it, and one already made is the one read after."""
    await conn.execute(
        "SELECT 1 FROM conversation_threads WHERE conversation_thread_id = %s FOR SHARE",
        (thread_id,),
    )


async def lock_thread(conn: Any, key: int, *, timeout_s: Optional[int] = None) -> None:
    """Take the thread lock ``key`` exclusive until the transaction ends,
    waiting at most ``timeout_s`` when given."""
    if timeout_s is not None:
        await conn.execute(f"SET LOCAL lock_timeout = '{int(timeout_s)}s'")
    await conn.execute("SELECT pg_advisory_xact_lock(%s)", (key,))


async def delete_checkpoints(conn: Any, thread_id: str) -> None:
    """Remove a thread's checkpoints: a copy whose ledger half failed."""
    async with conn.cursor() as cur:
        for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
            await cur.execute(f"DELETE FROM {table} WHERE thread_id = %s", (thread_id,))


async def check_source(
    conn: Any,
    *,
    source_thread_id: str,
    read_through: Optional[int],
    run_horizon: int,
    tip: Optional[str],
) -> None:
    """Raise ``StaleSource`` unless the source still matches the snapshot a
    fork was cut from: the thread still exists, a cut taken from its tip is
    still its tip (``tip``; a turn's end moves the pointer and settles its
    run in one transaction), and no run started after ``run_horizon`` at a
    turn the fork read, through ``read_through`` (an edit or a regenerate
    rewrote it). Read errors propagate."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT latest_checkpoint_id FROM conversation_threads WHERE conversation_thread_id = %s",
            (source_thread_id,),
        )
        row = await cur.fetchone()
        if row is None or (tip is not None and row["latest_checkpoint_id"] != tip):
            raise StaleSource(source_thread_id)
        if read_through is None:
            return
        await cur.execute(
            """
            SELECT 1 FROM conversation_responses
            WHERE conversation_thread_id = %s AND run_seq > %s AND turn_index <= %s
            LIMIT 1
            """,
            (source_thread_id, run_horizon, read_through),
        )
        if await cur.fetchone():
            raise StaleSource(source_thread_id)


async def plan_ledger(
    conn: Any,
    *,
    source_thread_id: str,
    turn_indexes: list[int],
    read_through: Optional[int],
    run_horizon: int,
    tip: Optional[str],
    copy_task_runs: bool,
) -> LedgerPlan:
    """After ``check_source``, find the response that ended the newest kept
    turn and (``copy_task_runs``) the finished background task runs the kept
    turns launched, minting their copies' ids. Writes nothing; meant for one
    repeatable-read snapshot, so the reads agree with each other."""
    await check_source(
        conn, source_thread_id=source_thread_id, read_through=read_through,
        run_horizon=run_horizon, tip=tip,
    )
    plan = LedgerPlan(source_thread_id=source_thread_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        responses = []
        if turn_indexes:
            await cur.execute(
                """
                SELECT conversation_response_id, turn_index, run_seq, status
                FROM conversation_responses
                WHERE conversation_thread_id = %s AND turn_index = ANY(%s) AND run_seq <= %s
                ORDER BY turn_index, run_seq
                """,
                (source_thread_id, turn_indexes, run_horizon),
            )
            responses = await cur.fetchall()
        if responses:
            plan.response_id = str(responses[-1]["conversation_response_id"])
        settled = [str(r["conversation_response_id"]) for r in responses if r["status"] != "in_progress"]
        # Even with no turn of its own kept: a fork's source made with tasks
        # holds them for the turns it borrows.
        if copy_task_runs:
            await _plan_task_runs(cur, source_thread_id, settled, plan)
    return plan


async def _plan_task_runs(
    cur: Any, source_thread_id: str, response_ids: list[str], plan: LedgerPlan
) -> None:
    """The finished background task runs the kept turns launched, so the
    forked agent can resume a task its history names and its cards open.

    A task whose later runs belong to turns past the cut keeps only the runs
    before it, and its lane is copied to the newest of those runs' ends.

    A source that is itself a fork holds the runs it was made with under no
    parent, since the turns that launched them are borrowed. Each keeps its
    original start, before the source existed, which no run of its own can
    have; they all belong to turns before any cut of it, so they all carry.
    """
    await cur.execute(
        """
        SELECT * FROM subagent_runs
        WHERE thread_id = %(t)s AND status <> 'in_progress'
          AND (parent_run_id = ANY(%(parents)s::uuid[])
               OR (parent_run_id IS NULL AND started_at < (
                   SELECT created_at FROM conversation_threads
                   WHERE conversation_thread_id = %(t)s)))
        ORDER BY started_at
        """,
        {"t": source_thread_id, "parents": response_ids},
    )
    runs = await cur.fetchall()
    if not runs:
        return
    for r in runs:
        plan.run_map[str(r["task_run_id"])] = str(uuid.uuid4())
    by_task: dict[str, list[dict[str, Any]]] = {}
    for r in runs:
        by_task.setdefault(r["task_id"], []).append(r)
    await cur.execute(
        "SELECT * FROM subagent_tasks WHERE thread_id = %s AND task_id = ANY(%s)",
        (source_thread_id, list(by_task)),
    )
    for task in await cur.fetchall():
        task_runs = by_task[task["task_id"]]
        plan.tasks.append((task, task_runs))
        ends = [r["final_checkpoint_id"] for r in task_runs if r["final_checkpoint_id"]]
        if ends:
            plan.task_tips.append(ChainTip(f"task:{task['task_id']}", max(ends)))


def _json(value: Any) -> Optional[Json]:
    return Json(value) if value is not None else None


async def write_tasks(conn: Any, plan: LedgerPlan, target_thread_id: str) -> None:
    """Insert the planned task copies into ``target_thread_id``, inside the
    caller's transaction."""
    async with conn.cursor(row_factory=dict_row) as cur:
        for task, runs in plan.tasks:
            await _write_task(cur, target_thread_id, plan.run_map, task, runs)


def _remap(ids: dict[str, str], run_id: Any) -> Optional[str]:
    return ids.get(str(run_id)) if run_id else None


async def _settle(cur: Any, table: str, key: str, row_id: str, source: dict[str, Any]) -> None:
    """Give a copied run its source's ending. Run rows are born in progress
    and may only move from there (the lifecycle guard trigger), so a copy is
    inserted open and settled at once, before the next copy takes the slot."""
    await cur.execute(
        f"UPDATE {table} SET status = %s, cancel_requested_at = %s WHERE {key} = %s",
        (source["status"], source["cancel_requested_at"], row_id),
    )


async def _write_task(
    cur: Any, target_thread_id: str, ids: dict[str, str], task: dict[str, Any], runs: list[dict[str, Any]]
) -> None:
    """One task and its runs. A run's launching turn stays the source's, so
    the copy has no parent run: like a workflow's own children, it reports to
    no turn of the fork."""
    await cur.execute(
        """
        INSERT INTO subagent_tasks (
            thread_id, task_id, description, subagent_type, latest_run_id,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, NULL, %s, %s)
        """,
        (
            target_thread_id, task["task_id"], task["description"],
            task["subagent_type"], task["created_at"], task["updated_at"],
        ),
    )
    for r in runs:
        new_id = ids[str(r["task_run_id"])]
        await cur.execute(
            """
            INSERT INTO subagent_runs (
                task_run_id, thread_id, task_id, parent_run_id,
                launch_tool_call_id, predecessor_run_id, cause, status,
                cancel_requested_at, start_checkpoint_id, final_checkpoint_id,
                failure, result_delivered_at, started_at, finalized_at,
                user_id, usage_settled_at, replay_facts
            ) VALUES (%s, %s, %s, NULL, %s, %s, %s, 'in_progress', NULL, %s, %s, %s, %s, %s,
                      %s, %s, COALESCE(%s, NOW()), %s)
            """,
            (
                new_id, target_thread_id, r["task_id"],
                r["launch_tool_call_id"], _remap(ids, r["predecessor_run_id"]), r["cause"],
                r["start_checkpoint_id"], r["final_checkpoint_id"], _json(r["failure"]),
                r["result_delivered_at"], r["started_at"], r["finalized_at"],
                r["user_id"], r["usage_settled_at"], _json(r["replay_facts"]),
            ),
        )
        await _settle(cur, "subagent_runs", "task_run_id", new_id, r)
    await cur.execute(
        "UPDATE subagent_tasks SET latest_run_id = %s WHERE thread_id = %s AND task_id = %s",
        (ids[str(runs[-1]["task_run_id"])], target_thread_id, task["task_id"]),
    )


async def finish_thread(
    conn: Any, thread_id: str, *, checkpoint_id: Optional[str], llm_model: Optional[str]
) -> None:
    """Point the fork at its snapshot and keep the source's model."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE conversation_threads
            SET latest_checkpoint_id = %s, llm_model = %s, updated_at = NOW()
            WHERE conversation_thread_id = %s
            """,
            (checkpoint_id, llm_model, thread_id),
        )


async def claim_carry(thread_id: str) -> bool:
    """Stamp a fork's lineage as carried, once: true for the one turn that
    stamps it (``forks.carry``)."""
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE conversation_threads
            SET metadata = jsonb_set(metadata, '{fork,carried_at}', to_jsonb(NOW()))
            WHERE conversation_thread_id = %s
              AND metadata ? 'fork'
              AND metadata->'fork'->>'carried_at' IS NULL
            """,
            (thread_id,),
        )
        return cur.rowcount > 0

