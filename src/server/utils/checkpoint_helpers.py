"""Checkpoint configuration and validation helpers.

Consolidates repeated checkpoint config building and checkpointer validation
patterns used across workflow endpoints.
"""

import asyncio
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, TypeVar

from fastapi import HTTPException
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


def _setup():
    """The app's setup module, read at call time.

    Importing it at module load builds the whole app, and the app's routers
    import history, which imports this module: a cold import of history
    then fails on the half-built cycle."""
    from src.server.app import setup

    return setup

# Type variable for decorated functions
F = TypeVar("F", bound=Callable[..., Any])


class CheckpointBranchTipNotFound(LookupError):
    """A requested checkpoint branch tip is absent from the thread history."""

    def __init__(self, thread_id: str, checkpoint_id: str) -> None:
        self.thread_id = thread_id
        self.checkpoint_id = checkpoint_id
        super().__init__(
            f"Checkpoint branch tip {checkpoint_id!r} not found for thread {thread_id!r}"
        )


def build_checkpoint_config(
    thread_id: str,
    checkpoint_id: str | None = None,
) -> dict[str, Any]:
    """Build a checkpoint configuration dict.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional specific checkpoint ID

    Returns:
        Configuration dict with "configurable" key containing thread_id
        and optionally checkpoint_id
    """
    config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
        }
    }
    if checkpoint_id:
        config["configurable"]["checkpoint_id"] = checkpoint_id
    return config


def require_checkpointer(func: F) -> F:
    """Decorator that ensures checkpointer is initialized before endpoint execution.

    Raises HTTPException with status 500 if checkpointer is not available.

    Usage:
        @router.get("/endpoint")
        @require_checkpointer
        async def my_endpoint(...):
            # checkpointer is guaranteed to exist here
            ...
    """
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _setup().checkpointer:
            raise HTTPException(
                status_code=500,
                detail="Checkpointer not initialized"
            )
        return await func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def get_checkpointer():
    """Get the checkpointer instance, raising HTTPException if not available.

    Returns:
        The initialized checkpointer

    Raises:
        HTTPException: If checkpointer is not initialized
    """
    checkpointer = _setup().checkpointer
    if not checkpointer:
        raise HTTPException(
            status_code=500,
            detail="Checkpointer not initialized"
        )
    return checkpointer


# LangGraph records a pending write on this channel when a node pauses via
# interrupt(). We match the stored channel name directly rather than importing
# the constant, which langgraph made private in v1.0 (deprecated import); the
# on-disk channel name is a stable storage-format detail.
INTERRUPT_CHANNEL = "__interrupt__"


@dataclass
class Boundary:
    """A turn boundary on the current branch."""

    checkpoint_id: str
    parent_checkpoint_id: str | None
    metadata: dict[str, Any]
    # The checkpoint's interrupt records. On a resume boundary they are the
    # answered interrupts of the turn it resumes: interrupt and resume writes
    # attach to the same checkpoint.
    interrupts: list[dict[str, Any]] = field(default_factory=list)

    @property
    def is_input(self) -> bool:
        """A new message; any other boundary resumes the turn before it."""
        return self.metadata.get("source") == "input"


def interrupt_records(pending_writes: Any) -> list[dict[str, Any]]:
    """``{"id", "value"}`` records from a checkpoint's ``__interrupt__`` writes."""
    records: list[dict[str, Any]] = []
    for _task_id, channel, value in pending_writes or ():
        if channel != INTERRUPT_CHANNEL:
            continue
        for intr in value if isinstance(value, (list, tuple)) else [value]:
            records.append(
                {"id": getattr(intr, "id", None), "value": getattr(intr, "value", None)}
            )
    return records


def is_turn_boundary(cp_tuple: Any) -> bool:
    """A checkpoint that starts a conversational turn.

    Either a ``source=input`` checkpoint (a user turn) or a HITL resume — a
    checkpoint carrying ``__resume__`` in pending_writes (``Command(resume=...)``
    yields ``source=loop``, not ``input``).
    """
    if (cp_tuple.metadata or {}).get("source") == "input":
        return True
    return any(
        channel == "__resume__" for _, channel, _ in (cp_tuple.pending_writes or [])
    )


def _resumed_by_later_run(branch: list[Any], i: int) -> bool:
    """An interrupted checkpoint that a later run carried on from.

    A resume normally marks itself: the waiting node runs again and its
    ``interrupt()`` writes ``__resume__``. When the thread changed agents while
    it waited, the waiting calls are cancelled or run by tools that never ask
    (``flash_handover``), so nothing writes the mark and the resume shows only
    as execution continuing under another run. Updates between the two (the
    cancellation, a stop's flush) are skipped; an ``input`` checkpoint is a
    new message, its own boundary, not a resume.
    """
    cp = branch[i]
    if not any(
        channel == INTERRUPT_CHANNEL for _, channel, _ in (cp.pending_writes or [])
    ):
        return False
    run_id = (cp.metadata or {}).get("run_id")
    for later in branch[i + 1 :]:
        metadata = later.metadata or {}
        if metadata.get("source") == "update":
            continue
        later_run = metadata.get("run_id")
        return metadata.get("source") == "loop" and later_run not in (None, run_id)
    return False


_BRANCH_TIP_SQL = """
    SELECT
        (SELECT checkpoint_id FROM checkpoints
         WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
           AND checkpoint_id = %(requested)s) AS requested,
        (SELECT checkpoint_id FROM checkpoints
         WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
         ORDER BY checkpoint_id DESC LIMIT 1) AS newest
"""

# Parent links from the tip, filtered by is_turn_boundary and
# _resumed_by_later_run, oldest first. Each step is a LATERAL primary-key
# probe: as a plain join, a generic plan (the checkpointer prepares every
# statement) may hash the thread's whole history once per step, which goes
# quadratic on a long thread. The LIMIT keeps the planner from flattening the
# probe back into that join. For the later-run test, ``moves_after`` counts the
# non-update checkpoints after each one and ``move_no`` numbers them from the
# tip, so the first non-update checkpoint after a row is the one whose
# ``move_no`` equals that row's ``moves_after``.
_BRANCH_BOUNDARIES_SQL = """
    WITH RECURSIVE branch AS (
        SELECT checkpoint_id, parent_checkpoint_id,
               metadata->>'source' AS source, metadata->>'run_id' AS run_id
        FROM checkpoints
        WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
          AND checkpoint_id = %(tip)s
      UNION ALL
        SELECT p.checkpoint_id, p.parent_checkpoint_id, p.source, p.run_id
        FROM branch b CROSS JOIN LATERAL (
            SELECT c.checkpoint_id, c.parent_checkpoint_id,
                   c.metadata->>'source' AS source,
                   c.metadata->>'run_id' AS run_id
            FROM checkpoints c
            WHERE c.thread_id = %(thread_id)s AND c.checkpoint_ns = %(ns)s
              AND c.checkpoint_id = b.parent_checkpoint_id
            LIMIT 1
        ) p
    ), marks AS (
        SELECT checkpoint_id,
               bool_or(channel = '__resume__') AS resumed,
               bool_or(channel = '__interrupt__') AS interrupted
        FROM checkpoint_writes
        WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
          AND channel IN ('__resume__', '__interrupt__')
        GROUP BY checkpoint_id
    ), steps AS (
        SELECT b.checkpoint_id, b.parent_checkpoint_id, b.source, b.run_id,
               coalesce(m.resumed, false) AS resumed,
               coalesce(m.interrupted, false) AS interrupted,
               count(*) FILTER (WHERE b.source IS DISTINCT FROM 'update') OVER (
                   ORDER BY b.checkpoint_id DESC
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ) AS moves_after,
               count(*) FILTER (WHERE b.source IS DISTINCT FROM 'update') OVER (
                   ORDER BY b.checkpoint_id DESC ROWS UNBOUNDED PRECEDING
               ) AS move_no
        FROM branch b LEFT JOIN marks m USING (checkpoint_id)
    ), bounds AS (
        SELECT s.checkpoint_id, s.parent_checkpoint_id
        FROM steps s
        LEFT JOIN steps nxt
          ON nxt.move_no = s.moves_after
         AND nxt.source IS DISTINCT FROM 'update'
        WHERE s.source = 'input'
           OR s.resumed
           OR (s.interrupted
               AND nxt.source = 'loop'
               AND nxt.run_id IS NOT NULL
               AND nxt.run_id IS DISTINCT FROM s.run_id)
    )
    SELECT b.checkpoint_id, b.parent_checkpoint_id, m.metadata
    FROM bounds b CROSS JOIN LATERAL (
        SELECT c.metadata
        FROM checkpoints c
        WHERE c.thread_id = %(thread_id)s AND c.checkpoint_ns = %(ns)s
          AND c.checkpoint_id = b.checkpoint_id
        LIMIT 1
    ) m
    ORDER BY b.checkpoint_id
"""

# The boundary rule needs only which channels were written, which the walk
# reads in SQL; the one payload a consumer reads is the interrupts.
_BOUNDARY_INTERRUPTS_SQL = """
    SELECT checkpoint_id, task_id, channel, type, blob
    FROM checkpoint_writes
    WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
      AND checkpoint_id = ANY(%(ids)s) AND channel = '__interrupt__'
    ORDER BY checkpoint_id, task_id, idx
"""


def _pick_tip(
    thread_id: str,
    requested: str | None,
    requested_exists: bool,
    newest: str | None,
    strict: bool,
) -> str | None:
    if requested is not None and not requested_exists and strict:
        raise CheckpointBranchTipNotFound(thread_id, requested)
    return requested if requested_exists else newest


async def _walk_via_tables(
    checkpointer: AsyncPostgresSaver,
    thread_id: str,
    requested: str | None,
    strict: bool,
    checkpoint_ns: str,
) -> tuple[list[Boundary], str | None]:
    """The branch walk run in Postgres.

    A long thread has tens of thousands of checkpoints and a few hundred
    turns, and decoding every row's metadata to keep the boundaries held the
    event loop for seconds. Couples to the checkpoint-postgres *schema*
    (stable, versioned) instead of its API.
    """
    conn_or_pool = checkpointer.conn
    if isinstance(conn_or_pool, AsyncConnectionPool):
        conn_ctx = conn_or_pool.connection()
    else:
        conn_ctx = nullcontext(conn_or_pool)  # caller-owned single connection

    params = {"thread_id": thread_id, "ns": checkpoint_ns, "requested": requested}
    write_rows: list[dict[str, Any]] = []
    async with conn_ctx as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_BRANCH_TIP_SQL, params)
        tip_row = await cur.fetchone()
        tip_id = _pick_tip(
            thread_id,
            requested,
            tip_row["requested"] is not None,
            tip_row["newest"],
            strict,
        )
        if tip_id is None:
            return [], None
        await cur.execute(_BRANCH_BOUNDARIES_SQL, {**params, "tip": tip_id})
        rows = await cur.fetchall()
        if rows:
            await cur.execute(
                _BOUNDARY_INTERRUPTS_SQL,
                {**params, "ids": [r["checkpoint_id"] for r in rows]},
            )
            write_rows = await cur.fetchall()

    writes_by_cp: dict[str, list[tuple[str, str, Any]]] = defaultdict(list)
    for w in write_rows:
        value = checkpointer.serde.loads_typed((w["type"], bytes(w["blob"])))
        writes_by_cp[w["checkpoint_id"]].append((w["task_id"], w["channel"], value))
    boundaries = [
        Boundary(
            checkpoint_id=r["checkpoint_id"],
            parent_checkpoint_id=r["parent_checkpoint_id"],
            metadata=r["metadata"] or {},
            interrupts=interrupt_records(writes_by_cp.get(r["checkpoint_id"])),
        )
        for r in rows
    ]
    return boundaries, tip_id


def _boundary(cp: CheckpointTuple) -> Boundary:
    parent = (cp.parent_config or {}).get("configurable") or {}
    return Boundary(
        checkpoint_id=cp.config["configurable"]["checkpoint_id"],
        parent_checkpoint_id=parent.get("checkpoint_id"),
        metadata=dict(cp.metadata or {}),
        interrupts=interrupt_records(cp.pending_writes),
    )


async def _walk_via_alist(
    checkpointer: Any,
    thread_id: str,
    requested: str | None,
    strict: bool,
    checkpoint_ns: str,
) -> tuple[list[Boundary], str | None]:
    """The walk for any other saver: every checkpoint via ``alist``, walked
    in process."""
    config = build_checkpoint_config(thread_id)
    config["configurable"]["checkpoint_ns"] = checkpoint_ns
    checkpoints = [cp_tuple async for cp_tuple in checkpointer.alist(config)]
    cp_by_id = {cp.config["configurable"]["checkpoint_id"]: cp for cp in checkpoints}
    tip_id = _pick_tip(
        thread_id,
        requested,
        requested in cp_by_id,
        checkpoints[0].config["configurable"]["checkpoint_id"] if checkpoints else None,
        strict,
    )
    if tip_id is None:
        return [], None

    current_branch: set[str] = set()
    cursor: str | None = tip_id
    while cursor and cursor in cp_by_id:
        current_branch.add(cursor)
        parent = cp_by_id[cursor].parent_config
        cursor = parent["configurable"].get("checkpoint_id") if parent else None

    branch = [
        cp
        for cp in reversed(checkpoints)  # alist is newest-first
        if cp.config["configurable"]["checkpoint_id"] in current_branch
    ]
    boundaries = [
        _boundary(cp)
        for i, cp in enumerate(branch)
        if is_turn_boundary(cp) or _resumed_by_later_run(branch, i)
    ]
    return boundaries, tip_id


async def walk_current_branch_boundaries(
    checkpointer: Any,
    thread_id: str,
    branch_tip_checkpoint_id: str | None = None,
    *,
    strict_branch_tip: bool = False,
    checkpoint_ns: str = "",
) -> tuple[list[Boundary], str | None]:
    """Chronological turn boundaries on the thread's current branch.

    Edit/regenerate fork the checkpoint graph, so only ancestors of the branch
    tip count as turns: the tip is ``branch_tip_checkpoint_id`` when present and
    on the graph, else the newest checkpoint. With ``strict_branch_tip=True``, a
    supplied-but-missing tip raises ``CheckpointBranchTipNotFound`` instead of
    silently reading the newest (possibly uncommitted) state. Returns
    ``(boundaries, tip_id)``, boundaries oldest-first; ``tip_id`` is None only
    when the thread has none.

    A Postgres saver walks in SQL only, and an error there raises: the
    in-process walk decodes every checkpoint of the thread on the event loop,
    the stall the SQL walk exists to remove, so it is no fallback.
    """
    walk = (
        _walk_via_tables
        if isinstance(checkpointer, AsyncPostgresSaver)
        else _walk_via_alist
    )
    return await walk(
        checkpointer,
        thread_id,
        branch_tip_checkpoint_id,
        strict_branch_tip,
        checkpoint_ns,
    )


async def update_at_commit(
    graph: Any,
    thread_id: str,
    built_on: str | None,
    values: dict[str, Any],
    *,
    timeout: float | None = None,
) -> bool:
    """Write ``values`` onto the thread's checkpoint ``built_on``, and move the
    thread's commit pointer onto the write when it still points there. True
    when the pointer moved; ``timeout`` bounds the write alone.

    ``built_on`` is both the write's anchor and the CAS guard. Unanchored, a
    turn that starts on another worker between the caller's read and the
    write would re-parent the write onto that turn's uncommitted checkpoint
    while the CAS still passed, publishing partial state as the commit
    pointer; so with no checkpoint to build on, nothing is written.
    """
    if not built_on:
        return False
    from src.server.database.conversation import threads_write

    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": built_on,
        }
    }
    new_config = await asyncio.wait_for(graph.aupdate_state(config, values), timeout)
    new_id = ((new_config or {}).get("configurable") or {}).get("checkpoint_id")
    if not new_id:
        return False
    return await threads_write.advance_thread_checkpoint_id(
        thread_id, from_checkpoint_id=built_on, to_checkpoint_id=new_id
    )
