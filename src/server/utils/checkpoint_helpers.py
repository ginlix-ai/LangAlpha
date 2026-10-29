"""Checkpoint configuration and validation helpers.

Consolidates repeated checkpoint config building and checkpointer validation
patterns used across workflow endpoints.
"""

import asyncio
import logging
from collections import defaultdict
from contextlib import AsyncExitStack, nullcontext
from dataclasses import dataclass, field
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from fastapi import HTTPException
from langgraph.checkpoint.base import CheckpointTuple, writes_sort_key
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

if TYPE_CHECKING:
    from src.server.database.runs.lifecycle import NewestCheckpoint

_logger = logging.getLogger(__name__)


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


# The empty state a thread's first message forks from. LangGraph builds a
# thread's first input on an empty checkpoint it never saves, so that input has
# no parent to fork at; an edit of the first message forks here instead, which
# the fork's START writes as it pins it (``write_fork_root``). One fixed id
# per thread keeps the write idempotent, and the lowest uuid6 sorts before
# every checkpoint the graph writes: it is never the newest beside them, and a
# fork at it drops every later transcript. Its source is neither ``input`` nor
# a resume, so no walk counts it as a turn.
FORK_ROOT_CHECKPOINT_ID = "00000000-0000-6000-8000-000000000000"


async def ensure_fork_root(checkpointer: Any, thread_id: str) -> None:
    """Write the thread's empty fork root, or rewrite the same one."""
    from langgraph.checkpoint.base import empty_checkpoint

    checkpoint = empty_checkpoint()
    checkpoint["id"] = FORK_ROOT_CHECKPOINT_ID
    # Step -2, as LangGraph numbers the empty checkpoint a first input builds
    # on, so a turn forked here is numbered like a thread's first turn.
    await checkpointer.aput(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        checkpoint,
        {"source": "fork", "step": -2, "parents": {}},
        {},
    )


async def write_fork_root(thread_id: str, conn: Any) -> None:
    """Write the fork root on START's connection, inside its transaction.

    START pins the thread's pointer to the root, so both commit or roll back
    together and a refused fork writes nothing; under the writer guard the
    connection is the run's pinned session, which holds the thread's fence.
    The guard runs only with the checkpoints in the app's database; without
    it (one unfenced worker) the root goes through the app's checkpointer.
    """
    from src.server.services import writer_guard

    if not writer_guard.guard_enabled():
        await ensure_fork_root(_setup().checkpointer, thread_id)
        return
    # A saver of its own, whose lock is not the session's: START holds that
    # one for the whole transaction.
    await ensure_fork_root(AsyncPostgresSaver(conn), thread_id)


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
    # On an answered boundary, the checkpoint the answered step finished on:
    # the next one on the branch, when the resume's own step wrote it and it
    # opens no turn of its own. None while that step has not finished.
    answered_checkpoint_id: str | None = None

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


def _answered_checkpoint_id(branch: list[Any], i: int) -> str | None:
    """Where the answer to ``branch[i]``'s interrupt has been applied.

    The step an answer resumes writes no checkpoint of its own until it
    finishes, and its next checkpoint is a ``loop`` one. An ``update`` or an
    ``input`` there means the step never finished (a stop's flush, a new
    message), so there is none.
    """
    cp = branch[i]
    if (cp.metadata or {}).get("source") == "input" or i + 1 >= len(branch):
        return None
    if not any(channel == "__resume__" for _, channel, _ in (cp.pending_writes or [])):
        return None
    nxt = branch[i + 1]
    if (nxt.metadata or {}).get("source") != "loop":
        return None
    return nxt.config["configurable"]["checkpoint_id"]


# Checkpoints that run no step: an update written beside a run, and the fork
# a run opens its branch with. The SQL walk below spells the same pair.
_NO_STEP_SOURCES = ("update", "fork")


def _resumed_by_later_run(branch: list[Any], i: int) -> bool:
    """An interrupted checkpoint that a later run carried on from.

    A resume normally marks itself: the waiting node runs again and its
    ``interrupt()`` writes ``__resume__``. When the thread changed agents while
    it waited, the waiting calls are cancelled or run by tools that never ask
    (``agent_change``), so nothing writes the mark and the resume shows only
    as execution continuing under another run. Updates between the two (the
    cancellation, a stop's flush) are skipped, as is the fork a run opens its
    branch with when it is addressed at an update the thread has moved past;
    an ``input`` checkpoint is a new message, its own boundary, not a resume.
    """
    cp = branch[i]
    if not any(
        channel == INTERRUPT_CHANNEL for _, channel, _ in (cp.pending_writes or [])
    ):
        return False
    run_id = (cp.metadata or {}).get("run_id")
    for later in branch[i + 1 :]:
        metadata = later.metadata or {}
        if metadata.get("source") in _NO_STEP_SOURCES:
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
# checkpoints after each one that ran a step (neither an update nor a fork) and
# ``move_no`` numbers them from the tip, so the first such checkpoint after a
# row is the one whose ``move_no`` equals that row's ``moves_after``. ``child_*`` is the row after
# it, for _answered_checkpoint_id.
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
               lag(b.checkpoint_id) OVER (ORDER BY b.checkpoint_id DESC) AS child_id,
               lag(b.source) OVER (ORDER BY b.checkpoint_id DESC) AS child_source,
               count(*) FILTER (
                   WHERE coalesce(b.source, '') NOT IN ('update', 'fork')
               ) OVER (
                   ORDER BY b.checkpoint_id DESC
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ) AS moves_after,
               count(*) FILTER (
                   WHERE coalesce(b.source, '') NOT IN ('update', 'fork')
               ) OVER (
                   ORDER BY b.checkpoint_id DESC ROWS UNBOUNDED PRECEDING
               ) AS move_no
        FROM branch b LEFT JOIN marks m USING (checkpoint_id)
    ), bounds AS (
        SELECT s.checkpoint_id, s.parent_checkpoint_id,
               CASE WHEN s.resumed AND s.source IS DISTINCT FROM 'input'
                         AND s.child_source = 'loop'
                    THEN s.child_id END AS answered_checkpoint_id
        FROM steps s
        LEFT JOIN steps nxt
          ON nxt.move_no = s.moves_after
         AND coalesce(nxt.source, '') NOT IN ('update', 'fork')
        WHERE s.source = 'input'
           OR s.resumed
           OR (s.interrupted
               AND nxt.source = 'loop'
               AND nxt.run_id IS NOT NULL
               AND nxt.run_id IS DISTINCT FROM s.run_id)
    )
    SELECT b.checkpoint_id, b.parent_checkpoint_id, b.answered_checkpoint_id,
           m.metadata
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
# reads in SQL; the one payload a consumer reads is the interrupts. They are
# sorted in Python by the saver's own key, so a boundary read here lists them
# as one loaded through the saver does, whatever the database's collation.
_BOUNDARY_INTERRUPTS_SQL = """
    SELECT checkpoint_id, task_id, task_path, idx, channel, type, blob
    FROM checkpoint_writes
    WHERE thread_id = %(thread_id)s AND checkpoint_ns = %(ns)s
      AND checkpoint_id = ANY(%(ids)s) AND channel = '__interrupt__'
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
    write_rows.sort(key=lambda w: writes_sort_key(w["task_path"], w["task_id"], w["idx"]))
    for w in write_rows:
        value = checkpointer.serde.loads_typed((w["type"], bytes(w["blob"])))
        writes_by_cp[w["checkpoint_id"]].append((w["task_id"], w["channel"], value))
    boundaries = [
        Boundary(
            checkpoint_id=r["checkpoint_id"],
            parent_checkpoint_id=r["parent_checkpoint_id"],
            metadata=r["metadata"] or {},
            interrupts=interrupt_records(writes_by_cp.get(r["checkpoint_id"])),
            answered_checkpoint_id=r["answered_checkpoint_id"],
        )
        for r in rows
    ]
    return boundaries, tip_id


def _boundary(branch: list[CheckpointTuple], i: int) -> Boundary:
    cp = branch[i]
    parent = (cp.parent_config or {}).get("configurable") or {}
    return Boundary(
        checkpoint_id=cp.config["configurable"]["checkpoint_id"],
        parent_checkpoint_id=parent.get("checkpoint_id"),
        metadata=dict(cp.metadata or {}),
        interrupts=interrupt_records(cp.pending_writes),
        answered_checkpoint_id=_answered_checkpoint_id(branch, i),
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
        _boundary(branch, i)
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
    boundaries, tip_id = await walk(
        checkpointer,
        thread_id,
        branch_tip_checkpoint_id,
        strict_branch_tip,
        checkpoint_ns,
    )
    # When the answered step's checkpoint is itself the next turn's boundary
    # (that step asked again, and was answered), a fork there deletes that
    # turn but leaves its answer written on the checkpoint, which a later walk
    # reads as a turn nothing stored. The turn falls back to its own boundary.
    starts = {b.checkpoint_id for b in boundaries}
    for boundary in boundaries:
        if boundary.answered_checkpoint_id in starts:
            boundary.answered_checkpoint_id = None
    return boundaries, tip_id


async def newest_checkpoint(
    checkpointer: Any, thread_id: str
) -> "NewestCheckpoint | None":
    """The thread's newest root checkpoint, and the run whose graph step
    wrote it.

    ``build_graph_config`` stamps every step of a run with the run's id; an
    update written beside a run (a stop's flush, a handover) is unstamped, so
    the walk reads past updates to the step beneath them.
    """
    from src.server.database.runs.lifecycle import NewestCheckpoint

    cp = await checkpointer.aget_tuple(build_checkpoint_config(thread_id))
    if cp is None:
        return None
    checkpoint_id = cp.config["configurable"]["checkpoint_id"]
    while (cp.metadata or {}).get("source") == "update" and cp.parent_config:
        parent = await checkpointer.aget_tuple(cp.parent_config)
        if parent is None:
            break
        cp = parent
    return NewestCheckpoint(checkpoint_id, (cp.metadata or {}).get("run_id"))


# Parent links from the tip down to the target, the same LATERAL PK probe as
# the branch walk. Checkpoint ids are uuid6, ordered by time, so an ancestor
# always sorts below its child and the walk stops once it passes the target.
_ON_BRANCH_SQL = """
    WITH RECURSIVE branch AS (
        SELECT checkpoint_id, parent_checkpoint_id
        FROM checkpoints
        WHERE thread_id = %(thread_id)s AND checkpoint_ns = ''
          AND checkpoint_id = %(tip)s
      UNION ALL
        SELECT p.checkpoint_id, p.parent_checkpoint_id
        FROM branch b CROSS JOIN LATERAL (
            SELECT c.checkpoint_id, c.parent_checkpoint_id
            FROM checkpoints c
            WHERE c.thread_id = %(thread_id)s AND c.checkpoint_ns = ''
              AND c.checkpoint_id = b.parent_checkpoint_id
            LIMIT 1
        ) p
        WHERE b.checkpoint_id COLLATE "C" > %(target)s COLLATE "C"
    )
    SELECT EXISTS (
        SELECT 1 FROM branch WHERE checkpoint_id = %(target)s
    ) AS on_branch
"""


async def checkpoint_on_branch(
    thread_id: str,
    checkpoint_id: str,
    tip: str,
    *,
    checkpointer: Any = None,
) -> bool:
    """True when ``checkpoint_id`` is ``tip`` or one of its ancestors.

    The checkpointer defaults to the app's, read at call time, so a fork can
    carry this check into START before the graph is built. The fork root is
    on every branch: each one's first turn starts from the empty state.
    """
    if checkpoint_id == FORK_ROOT_CHECKPOINT_ID:
        return True
    checkpointer = checkpointer or _setup().checkpointer
    if checkpointer is None:
        return False
    if isinstance(checkpointer, AsyncPostgresSaver):
        conn_or_pool = checkpointer.conn
        if isinstance(conn_or_pool, AsyncConnectionPool):
            conn_ctx = conn_or_pool.connection()
        else:
            conn_ctx = nullcontext(conn_or_pool)
        async with conn_ctx as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                _ON_BRANCH_SQL,
                {"thread_id": thread_id, "tip": tip, "target": checkpoint_id},
            )
            row = await cur.fetchone()
        return bool(row and row["on_branch"])
    config: dict[str, Any] | None = build_checkpoint_config(thread_id, tip)
    while config is not None:
        cp = await checkpointer.aget_tuple(config)
        if cp is None:
            return False
        current = cp.config["configurable"]["checkpoint_id"]
        if current == checkpoint_id:
            return True
        # Ids are uuid6, ordered by time, and a parent is older than its child.
        if current < checkpoint_id:
            return False
        config = cp.parent_config
    return False


# The newest checkpoint and each parent beneath an update, stopping at the
# pointer: the same walk ``newest_checkpoint`` takes to the step that wrote the
# newest checkpoint, which is the walk's one row that is not an update.
_NEWEST_STEP_SQL = """
    WITH RECURSIVE chain AS (
        SELECT * FROM (
            SELECT checkpoint_id, parent_checkpoint_id,
                   metadata->>'source' AS source, metadata->>'run_id' AS run_id
            FROM checkpoints
            WHERE thread_id = %(thread_id)s AND checkpoint_ns = ''
            ORDER BY checkpoint_id DESC
            LIMIT 1
        ) newest
      UNION ALL
        SELECT p.checkpoint_id, p.parent_checkpoint_id, p.source, p.run_id
        FROM chain c CROSS JOIN LATERAL (
            SELECT k.checkpoint_id, k.parent_checkpoint_id,
                   k.metadata->>'source' AS source, k.metadata->>'run_id' AS run_id
            FROM checkpoints k
            WHERE k.thread_id = %(thread_id)s AND k.checkpoint_ns = ''
              AND k.checkpoint_id = c.parent_checkpoint_id
            LIMIT 1
        ) p
        WHERE c.source = 'update' AND c.checkpoint_id <> %(pointer)s
    )
    SELECT
        EXISTS (SELECT 1 FROM chain WHERE checkpoint_id = %(pointer)s) AS extends,
        (SELECT run_id FROM chain WHERE source IS DISTINCT FROM 'update'
         LIMIT 1) AS step_run_id,
        EXISTS (
            SELECT 1 FROM checkpoints
            WHERE thread_id = %(thread_id)s AND checkpoint_ns = ''
              AND checkpoint_id = %(pointer)s
        ) AS pointer_exists
"""


async def _newest_step(
    checkpointer: Any, thread_id: str, pointer: str
) -> tuple[bool, str | None, bool]:
    """Whether the newest checkpoint is ``pointer`` or updates on it, else the
    run that wrote the step beneath its updates; and whether ``pointer`` is a
    checkpoint of the thread."""
    if isinstance(checkpointer, AsyncPostgresSaver):
        async with AsyncExitStack() as stack:
            if isinstance(checkpointer.conn, AsyncConnectionPool):
                conn = await stack.enter_async_context(checkpointer.conn.connection())
            else:
                # A run's fenced session installs its I/O mutex as the saver's
                # lock, which every statement on that connection must hold.
                await stack.enter_async_context(checkpointer.lock)
                conn = checkpointer.conn
            cur = await stack.enter_async_context(conn.cursor(row_factory=dict_row))
            await cur.execute(
                _NEWEST_STEP_SQL, {"thread_id": thread_id, "pointer": pointer}
            )
            row = await cur.fetchone()
        return bool(row["extends"]), row["step_run_id"], bool(row["pointer_exists"])
    cp = await checkpointer.aget_tuple(build_checkpoint_config(thread_id))
    while cp is not None:
        if cp.config["configurable"]["checkpoint_id"] == pointer:
            return True, None, True
        if (cp.metadata or {}).get("source") != "update" or not cp.parent_config:
            break
        cp = await checkpointer.aget_tuple(cp.parent_config)
    step_run_id = (cp.metadata or {}).get("run_id") if cp is not None else None
    exists = (
        await checkpointer.aget_tuple(build_checkpoint_config(thread_id, pointer))
        is not None
    )
    return False, step_run_id, exists


async def turn_base_checkpoint_id(checkpointer: Any, thread_id: str) -> str | None:
    """The checkpoint a turn that names none starts from, when it is not the
    newest one, which the graph loads by default.

    They differ after a fork whose run failed or was stopped before its first
    step: START pinned the pointer, the branch every reader shows, to the
    fork's base and deleted the rows of the turns it replaced, whose steps are
    still the newest. Only the fork's run row tells that pin from a pointer a
    turn left behind, which keeps the newest (``pointer_pinned_by_fork``).

    A read that fails raises: the newest checkpoint may be a replaced branch,
    and a turn run there would write that branch's turns back.
    """
    from src.server.database.conversation import threads_read
    from src.server.database.runs import lifecycle as tl_db

    if checkpointer is None:
        return None
    pointer = (await threads_read.get_thread_branch_read(thread_id)).checkpoint_id
    if pointer is None:
        return None
    extends, step_run_id, pointer_exists = await _newest_step(
        checkpointer, thread_id, pointer
    )
    if extends:
        return None
    if not await tl_db.pointer_pinned_by_fork(thread_id, pointer, step_run_id):
        return None
    if not pointer_exists:
        # Loading a checkpoint that is not there starts from an empty state.
        _logger.warning(
            f"[CHECKPOINT] pointer {pointer} of thread {thread_id} is not one "
            "of its checkpoints; the turn starts from the newest checkpoint"
        )
        return None
    return pointer


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
