"""Writing a fork's checkpoints and task runs from its source thread.

A fork holds one checkpoint of its own, the source's state at the cut, under
the cut's checkpoint id with no parent. ``messages`` and ``claude_session``
are ``DeltaChannel``s, which a checkpoint holds no value for but the writes
back to the last snapshot do, so the fork stores each as a
``_DeltaSnapshot`` of its value there; the plain channels' blobs and the
cut's own writes (which a stopped turn resumes from) are copied as stored.
The turns before the cut keep their rows in the source, which the fork shows
them from.

The finished task runs the kept turns launched are copied as ledger rows,
each under a new id, and the launch artifacts on the cut's ``messages`` are
pointed at the copies, by which replay finds the run a Task call started.
Their lanes stay in the threads that hold them, read there at the run's end
(``forks.tasks``); a resume writes one into the fork the same way the cut is
written, as one checkpoint at that end. Checkpoints are immutable once
written, so the fork needs no lock on the source's chain; the ledger read is
checked against a run horizon instead.
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from langchain_core.messages import BaseMessage

from psycopg.rows import dict_row, tuple_row
from psycopg.types.json import Json

logger = logging.getLogger(__name__)

#: A record id that ends in a run id, as ``task-result-{task_run_id}`` and
#: ``workflow-run-{task_run_id}`` do.
_TRAILING_RUN_ID = re.compile(
    r"^(.*?)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
)


@dataclass
class LedgerPlan:
    """What a fork reads of the source's ledger, under one snapshot: the
    finished task runs it copies, with the ids their copies take, minted
    before anything is written so the cut's launches can name them, and the
    response that ended the last kept turn."""

    source_thread_id: str = ""
    #: Each copied task's row and its finished runs, oldest first.
    tasks: list[tuple[dict[str, Any], list[dict[str, Any]]]] = field(default_factory=list)
    #: Source task run id to its copy's.
    run_map: dict[str, str] = field(default_factory=dict)
    #: The newest response of the newest kept turn that has one: while it
    #: exists, the source has not rewritten the turns the fork borrows.
    response_id: Optional[str] = None


class StaleSource(Exception):
    """A run that rewrote the kept turns started after the snapshot, or the
    source went away."""


@dataclass(frozen=True)
class ForkCheckpoint:
    """A checkpoint read from one thread to be written, parentless, into
    another (``forks.materialize``): a fork's cut, or a carried task's lane
    at its end."""

    checkpoint_id: str
    #: The namespace it is read from and written to: the thread's, or a
    #: task lane's (``task:<id>``).
    checkpoint_ns: str
    #: The checkpoint as stored, primitives inline.
    checkpoint: dict[str, Any]
    #: Channels written with the checkpoint rather than copied, by channel:
    #: each delta channel as a ``_DeltaSnapshot`` of its value, and a lane's
    #: ``ui``, whose records name the runs by the fork's ids.
    written: dict[str, Any]
    #: The plain channels whose blobs the state needs, at their versions.
    required: frozenset[str]
    #: The metadata it is written with, besides its source and parents.
    metadata: dict[str, Any]

    @property
    def versions(self) -> dict[str, Any]:
        return self.checkpoint.get("channel_versions") or {}


async def write_snapshot(conn: Any, target_thread_id: str, cut: ForkCheckpoint) -> None:
    """Write ``cut`` as the target's only checkpoint in its namespace, inside
    the caller's transaction: its id, no parent, its versions verbatim and
    its ``written`` channels at their versions, so a load reads it without
    walking back. Saved by a saver of its own on ``conn`` with the app
    saver's serde, so the values read back as written."""
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from src.server.utils.checkpoint_helpers import get_checkpointer

    inline = cut.checkpoint.get("channel_values") or {}
    checkpoint = {
        **cut.checkpoint,
        "id": cut.checkpoint_id,
        "channel_values": {**{k: v for k, v in inline.items() if k not in cut.written}, **cut.written},
    }
    saver = AsyncPostgresSaver(conn, serde=get_checkpointer().serde)
    await saver.aput(
        {"configurable": {"thread_id": target_thread_id, "checkpoint_ns": cut.checkpoint_ns}},
        checkpoint,
        {**cut.metadata, "source": "fork", "parents": {}},
        {k: cut.versions[k] for k in cut.written},
    )


async def copy_blobs(
    conn: Any, *, source_thread_id: str, target_thread_id: str, cut: ForkCheckpoint
) -> None:
    """Copy the blobs of the channels ``cut`` does not write, at its
    versions. Raises ``StaleSource`` when one the state holds a value for is
    not there: the target would load without it."""
    copied_versions = {k: v for k, v in cut.versions.items() if k not in cut.written}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)
            SELECT %(dst)s, b.checkpoint_ns, b.channel, b.version, b.type, b.blob
            FROM checkpoint_blobs b
            JOIN jsonb_each_text(%(versions)s::jsonb) v
              ON v.key = b.channel AND v.value = b.version
            WHERE b.thread_id = %(src)s AND b.checkpoint_ns = %(ns)s
            ON CONFLICT DO NOTHING
            RETURNING channel
            """,
            {
                "src": source_thread_id,
                "dst": target_thread_id,
                "ns": cut.checkpoint_ns,
                "versions": Json(copied_versions),
            },
        )
        copied = {row["channel"] for row in await cur.fetchall()}
    if missing := cut.required - copied:
        logger.warning(
            "[fork] checkpoint %s of %s has no blob for %s",
            cut.checkpoint_id, source_thread_id, sorted(missing),
        )
        raise StaleSource(source_thread_id)


async def lanes_held(conn: Any, lanes: list[tuple[str, str]]) -> frozenset[tuple[str, str]]:
    """Which of these (thread id, task id) pairs have a lane in that thread."""
    if not lanes:
        return frozenset()
    async with conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT c.thread_id, c.checkpoint_ns
            FROM checkpoints c
            JOIN unnest(%s::text[], %s::text[]) AS w(thread_id, checkpoint_ns)
              ON c.thread_id = w.thread_id AND c.checkpoint_ns = w.checkpoint_ns
            """,
            ([t for t, _ in lanes], [f"task:{task}" for _, task in lanes]),
        )
        return frozenset((t, ns.removeprefix("task:")) for t, ns in await cur.fetchall())


async def copy_cut_writes(
    conn: Any, *, source_thread_id: str, target_thread_id: str, checkpoint_id: str, run_map: dict[str, str]
) -> None:
    """Copy the cut's own writes, which a turn stopped there resumes from,
    with the launches on ``messages`` pointed at the task runs' copies."""
    async with conn.cursor(row_factory=dict_row) as cur:
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
        if not run_map:
            return
        # The ids are stored as plain bytes under msgpack, so a byte search
        # skips every write naming none of them before anything is decoded.
        await cur.execute(
            """
            SELECT task_id, idx, type, blob FROM checkpoint_writes
            WHERE thread_id = %(dst)s AND checkpoint_ns = '' AND checkpoint_id = %(cp)s
              AND channel = 'messages' AND blob IS NOT NULL
              AND EXISTS (SELECT 1 FROM unnest(%(ids)s::text[]) i
                          WHERE position(convert_to(i, 'UTF8') IN blob) > 0)
            """,
            {"dst": target_thread_id, "cp": checkpoint_id, "ids": list(run_map)},
        )
        serde = _serde()
        for w in await cur.fetchall():
            if (blob := _remapped_blob(serde, w["type"], w["blob"], run_map)) is not None:
                await cur.execute(
                    """
                    UPDATE checkpoint_writes SET type = %s, blob = %s
                    WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
                      AND task_id = %s AND idx = %s
                    """,
                    (*blob, target_thread_id, checkpoint_id, w["task_id"], w["idx"]),
                )


def _serde() -> Any:
    """The app saver's own serde, so a rewritten value reads back as written."""
    from src.server.utils.checkpoint_helpers import get_checkpointer

    return get_checkpointer().serde


def _remapped_blob(
    serde: Any, type_: str, blob: bytes, run_map: dict[str, str]
) -> Optional[tuple[str, bytes]]:
    """A ``messages`` write re-encoded with its launches remapped, or None
    when nothing in it changed."""
    if not type_ or type_ == "empty" or blob is None:
        return None
    value = serde.loads_typed((type_, bytes(blob)))
    remapped = remap_launches(value, run_map)
    return None if remapped is value else serde.dumps_typed(remapped)


def remap_launches(value: Any, run_map: dict[str, str]) -> Any:
    """A ``messages`` value (a message or a list of them) with each Task
    launch artifact's ``task_run_id`` mapped through ``run_map``. Returns
    ``value`` itself when nothing names a mapped run."""
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


def remap_ui_records(records: list[Any], run_map: dict[str, str]) -> list[Any]:
    """A task lane's ``ui`` records with each one's ``props.task_run_id`` and
    run-suffixed ``id`` mapped through ``run_map``."""
    return [_remap_record(r, run_map) if isinstance(r, dict) else r for r in records]


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
    before it, and its lane is read at the newest of those runs' ends.

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
        plan.tasks.append((task, by_task[task["task_id"]]))


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

