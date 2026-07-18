"""Checkpoint configuration and validation helpers.

Consolidates repeated checkpoint config building and checkpointer validation
patterns used across workflow endpoints.
"""

import logging
from collections import defaultdict
from contextlib import nullcontext
from functools import wraps
from typing import Any, Callable, TypeVar

from fastapi import HTTPException
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)

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
        if not setup.checkpointer:
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
    if not setup.checkpointer:
        raise HTTPException(
            status_code=500,
            detail="Checkpointer not initialized"
        )
    return setup.checkpointer


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


# Pending-write channels the walk consumers read: ``__resume__`` marks a HITL
# resume boundary (is_turn_boundary), ``__interrupt__`` carries the answered
# interrupt payloads (history reader).
_BOUNDARY_WRITE_CHANNELS = ("__resume__", "__interrupt__")

_warned_skeleton_fallback = False


async def _list_skeletons_via_tables(
    checkpointer: AsyncPostgresSaver, thread_id: str, checkpoint_ns: str = ""
) -> list[CheckpointTuple]:
    """Single-namespace checkpoint skeletons straight from the saver's tables.

    ``alist`` eagerly joins every checkpoint's channel blobs and writes —
    hundreds of ms on long threads — while the walk only needs ids, parents,
    metadata, and the two boundary write channels. Couples to the
    checkpoint-postgres *schema* (stable, versioned) instead of its API; any
    failure here falls back to the ``alist`` path.
    """
    conn_or_pool = checkpointer.conn
    if isinstance(conn_or_pool, AsyncConnectionPool):
        conn_ctx = conn_or_pool.connection()
    else:
        conn_ctx = nullcontext(conn_or_pool)  # caller-owned single connection

    async with conn_ctx as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """SELECT checkpoint_id, parent_checkpoint_id, metadata
               FROM checkpoints
               WHERE thread_id = %s AND checkpoint_ns = %s
               ORDER BY checkpoint_id DESC""",
            (thread_id, checkpoint_ns),
        )
        rows = await cur.fetchall()
        await cur.execute(
            """SELECT checkpoint_id, task_id, channel, type, blob
               FROM checkpoint_writes
               WHERE thread_id = %s AND checkpoint_ns = %s
                 AND channel = ANY(%s)
               ORDER BY checkpoint_id, task_id, idx""",
            (thread_id, checkpoint_ns, list(_BOUNDARY_WRITE_CHANNELS)),
        )
        write_rows = await cur.fetchall()

    writes_by_cp: dict[str, list[tuple[str, str, Any]]] = defaultdict(list)
    for w in write_rows:
        value = checkpointer.serde.loads_typed((w["type"], bytes(w["blob"])))
        writes_by_cp[w["checkpoint_id"]].append((w["task_id"], w["channel"], value))

    def _config(checkpoint_id: str) -> dict[str, Any]:
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    return [
        CheckpointTuple(
            config=_config(r["checkpoint_id"]),
            checkpoint={},  # skeleton: channel values are never read by the walk
            metadata=r["metadata"] or {},
            parent_config=(
                _config(r["parent_checkpoint_id"])
                if r["parent_checkpoint_id"]
                else None
            ),
            pending_writes=writes_by_cp.get(r["checkpoint_id"], []),
        )
        for r in rows
    ]


async def _list_skeletons_via_alist(
    checkpointer: Any, thread_id: str, checkpoint_ns: str = ""
) -> list[CheckpointTuple]:
    """Public-API fallback: full tuples via ``alist``, one namespace only."""
    config = build_checkpoint_config(thread_id)
    config["configurable"]["checkpoint_ns"] = checkpoint_ns
    return [cp_tuple async for cp_tuple in checkpointer.alist(config)]


async def _list_checkpoint_skeletons(
    checkpointer: Any, thread_id: str, checkpoint_ns: str = ""
) -> list[CheckpointTuple]:
    """Newest-first single-namespace checkpoints, as walk-sufficient skeletons.

    Skeletons carry config/metadata/parent_config plus pending writes for the
    boundary channels only — no channel values. Fast path reads the postgres
    tables directly; anything else (in-memory savers, schema drift) uses the
    public ``alist`` API, so the walk survives either the schema or the API
    changing — just not both at once.
    """
    global _warned_skeleton_fallback
    if isinstance(checkpointer, AsyncPostgresSaver):
        try:
            return await _list_skeletons_via_tables(
                checkpointer, thread_id, checkpoint_ns
            )
        except Exception:
            if not _warned_skeleton_fallback:
                _warned_skeleton_fallback = True
                logger.warning(
                    "[CHECKPOINT] Table-level checkpoint listing failed; "
                    "falling back to alist (slow). Check checkpoint-postgres "
                    "schema compatibility.",
                    exc_info=True,
                )
    return await _list_skeletons_via_alist(checkpointer, thread_id, checkpoint_ns)


async def walk_current_branch_boundaries(
    checkpointer: Any,
    thread_id: str,
    branch_tip_checkpoint_id: str | None = None,
    *,
    strict_branch_tip: bool = False,
    checkpoint_ns: str = "",
) -> tuple[list[Any], str | None]:
    """Chronological turn-boundary checkpoints on the thread's current branch.

    Edit/regenerate fork the checkpoint graph, so only ancestors of the branch
    tip count as turns: the tip is ``branch_tip_checkpoint_id`` when present and
    on the graph, else the newest checkpoint. With ``strict_branch_tip=True``, a
    supplied-but-missing tip raises ``CheckpointBranchTipNotFound`` instead of
    silently reading the newest (possibly uncommitted) state. Canonical branch
    walk shared by ``checkpoint_handler.get_thread_turns`` (turn CRUD) and the
    history reader.

    Returns ``(boundaries, tip_id)`` — boundaries oldest-first as skeleton
    ``CheckpointTuple``s (no channel values; pending writes limited to the
    boundary channels); ``tip_id`` is None only when the thread has none.
    """
    checkpoints = await _list_checkpoint_skeletons(
        checkpointer, thread_id, checkpoint_ns
    )
    if not checkpoints:
        if strict_branch_tip and branch_tip_checkpoint_id is not None:
            raise CheckpointBranchTipNotFound(thread_id, branch_tip_checkpoint_id)
        return [], None

    cp_by_id = {
        cp.config["configurable"]["checkpoint_id"]: cp for cp in checkpoints
    }
    if branch_tip_checkpoint_id is not None:
        tip = cp_by_id.get(branch_tip_checkpoint_id)
        if tip is None:
            if strict_branch_tip:
                raise CheckpointBranchTipNotFound(
                    thread_id, branch_tip_checkpoint_id
                )
            tip = checkpoints[0]
    else:
        tip = checkpoints[0]  # alist is newest-first
    tip_id: str = tip.config["configurable"]["checkpoint_id"]

    current_branch: set[str] = set()
    cursor: str | None = tip_id
    while cursor and cursor in cp_by_id:
        current_branch.add(cursor)
        parent = cp_by_id[cursor].parent_config
        cursor = parent["configurable"].get("checkpoint_id") if parent else None

    boundaries = [
        cp
        for cp in reversed(checkpoints)
        if cp.config["configurable"]["checkpoint_id"] in current_branch
        and is_turn_boundary(cp)
    ]
    return boundaries, tip_id
