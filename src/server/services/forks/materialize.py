"""Reading the source's state at the cut, for the fork's one checkpoint, and
a carried task's lane at its end, for the one a resume writes.

Read before anything is written, since a checkpoint never changes once
written: the raw row (one row holds the channels of every graph that wrote
the namespace, so its plain values are taken from there, not from a graph's
view), and the two ``DeltaChannel``s materialized through graphs that declare
them. The transcript window at the cut (``ForkHead``) is taken over every
message there, so the fork's own transcript files number on from the
source's.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping

from langgraph.checkpoint.serde.types import _DeltaSnapshot
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ptc_agent.agent.transcript import Window
from src.server.database import thread_fork
from src.server.services.forks.model import ForkHead, ForkRefused
from src.server.utils.checkpoint_helpers import get_checkpointer, read_claude_session

if TYPE_CHECKING:
    from src.server.services.forks.tasks import CarriedLane

#: The graphs' delta channels, which a checkpoint keeps no value for.
_MESSAGES = "messages"
_SESSION = "claude_session"
#: A task lane's UI records, whose ids name its runs.
_UI = "ui"


@dataclass(frozen=True)
class CutState:
    """The source's state at the cut: the checkpoint the fork writes, and the
    transcript window there."""

    checkpoint: thread_fork.ForkCheckpoint
    head: ForkHead


def checkpointer_pool() -> AsyncConnectionPool:
    """The checkpoint saver's pool, which every read and write of a fork's
    checkpoints goes through: a fork reads and writes checkpoint rows in SQL,
    so only a Postgres saver can make one."""
    pool = getattr(get_checkpointer(), "conn", None)
    if not isinstance(pool, AsyncConnectionPool):
        raise ForkRefused(501, "unsupported", "Copying a chat needs a Postgres checkpointer.")
    return pool


async def read_cut(source_thread_id: str, checkpoint_id: str, run_map: dict[str, str]) -> CutState:
    """The source's state at ``checkpoint_id``, its launches pointed at
    ``run_map``'s task run copies. Raises ``StaleSource`` when the
    checkpoint is gone."""
    from src.server.services.history.reader import CheckpointHistoryReader

    state = await CheckpointHistoryReader.get_instance().aget_state(source_thread_id, checkpoint_id)
    values = state.values or {}
    messages = list(values.get(_MESSAGES) or [])
    launches = {**values, _MESSAGES: thread_fork.remap_launches(messages, run_map)} if run_map else values
    checkpoint = await _checkpoint(
        source_thread_id, checkpoint_id, "", launches, {}, lambda meta: {"step": meta.get("step")}
    )
    head = await asyncio.to_thread(Window.of(values).extend, messages)
    return CutState(checkpoint, ForkHead.of(head))


async def read_lane(lane: CarriedLane) -> thread_fork.ForkCheckpoint:
    """A carried task's lane at the carried runs' end, for the one checkpoint
    a resume writes into the fork, its run ids the fork's: the stamp that
    makes it the carried run on the fork's walk, and its ``ui`` records.
    Raises ``StaleSource`` when the checkpoint is gone."""
    task = lane.task
    values = await lane.values()
    ids = task.lane_to_fork

    def metadata(meta: dict[str, Any]) -> dict[str, Any]:
        stamp = meta.get("task_run_id")
        return {**meta, "task_run_id": ids.get(str(stamp), stamp)} if stamp else meta

    rewritten = {_UI: values[_UI]} if values.get(_UI) is not None else {}
    return await _checkpoint(
        task.thread_id, task.checkpoint_id, f"task:{task.task_id}", values, rewritten, metadata
    )


async def _checkpoint(
    thread_id: str,
    checkpoint_id: str,
    checkpoint_ns: str,
    values: Mapping[str, Any],
    rewritten: dict[str, Any],
    metadata: Callable[[dict[str, Any]], dict[str, Any]],
) -> thread_fork.ForkCheckpoint:
    """The checkpoint at ``checkpoint_id`` with the state ``values`` read
    there: each delta channel written as a snapshot of its value, the
    ``rewritten`` plain channels written as given, and the rest's blobs
    copied as stored."""
    raw = await _raw_row(thread_id, checkpoint_id, checkpoint_ns)
    if raw is None:
        raise thread_fork.StaleSource(thread_id)
    checkpoint = raw["checkpoint"]
    versions = checkpoint.get("channel_versions") or {}
    inline = checkpoint.get("channel_values") or {}
    written = {k: v for k, v in rewritten.items() if k in versions}
    if _MESSAGES in versions:
        written[_MESSAGES] = _DeltaSnapshot(list(values.get(_MESSAGES) or []))
    if _SESSION in versions:
        written[_SESSION] = _DeltaSnapshot(
            await read_claude_session(thread_id, checkpoint_id, checkpoint_ns)
        )
    # A channel the graph reads a value for had a blob at its version; one it
    # never declares (another graph's) is copied as found.
    required = frozenset(
        k
        for k, v in values.items()
        if k in versions and k not in written and k not in inline and not _primitive(v)
    )
    return thread_fork.ForkCheckpoint(
        checkpoint_id=checkpoint_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint=checkpoint,
        written=written,
        required=required,
        metadata=metadata(dict(raw["metadata"] or {})),
    )


def _primitive(value: Any) -> bool:
    # What the saver keeps in the checkpoint row rather than as a blob.
    return value is None or isinstance(value, (str, int, float, bool))


async def _raw_row(thread_id: str, checkpoint_id: str, checkpoint_ns: str) -> dict[str, Any] | None:
    async with checkpointer_pool().connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT checkpoint, metadata FROM checkpoints
            WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s
            """,
            (thread_id, checkpoint_ns, checkpoint_id),
        )
        return await cur.fetchone()
