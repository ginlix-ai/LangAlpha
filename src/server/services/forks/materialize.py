"""Reading the source's state at the cut, for the fork's one checkpoint.

Read before the fork's transaction opens, since a checkpoint never changes
once written: the raw cut row (one row holds the channels of every graph that
wrote the thread, so its plain values are taken from there, not from a
graph's view), and the two ``DeltaChannel``s materialized through graphs that
declare them. The transcript window at the cut (``ForkHead``) is taken over
every message there, so the fork's own transcript files number on from the
source's.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ptc_agent.agent.transcript import Window
from src.server.database import thread_fork
from src.server.services.forks.model import ForkHead, ForkRefused
from src.server.utils.checkpoint_helpers import get_checkpointer, read_claude_session

#: The main graph's delta channels, which a checkpoint keeps no value for.
_MESSAGES = "messages"
_SESSION = "claude_session"


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

    raw = await _raw_row(source_thread_id, checkpoint_id)
    if raw is None:
        raise thread_fork.StaleSource(source_thread_id)
    checkpoint = raw["checkpoint"]
    versions = checkpoint.get("channel_versions") or {}
    inline = checkpoint.get("channel_values") or {}

    state = await CheckpointHistoryReader.get_instance().aget_state(source_thread_id, checkpoint_id)
    values = state.values or {}
    messages = list(values.get(_MESSAGES) or [])
    deltas: dict[str, Any] = {}
    if _MESSAGES in versions:
        deltas[_MESSAGES] = thread_fork.remap_launches(messages, run_map) if run_map else messages
    if _SESSION in versions:
        deltas[_SESSION] = await read_claude_session(source_thread_id, checkpoint_id)
    # A channel the main graph reads a value for had a blob at its version;
    # one it never declares (another graph's) is copied as found.
    required = frozenset(
        k
        for k, v in values.items()
        if k in versions and k not in deltas and k not in inline and not _primitive(v)
    )
    window = Window.of(values)
    head = await asyncio.to_thread(window.extend, messages)
    return CutState(
        thread_fork.ForkCheckpoint(
            checkpoint_id=checkpoint_id,
            checkpoint=checkpoint,
            step=(raw["metadata"] or {}).get("step"),
            deltas=deltas,
            required=required,
        ),
        ForkHead.of(head),
    )


def _primitive(value: Any) -> bool:
    # What the saver keeps in the checkpoint row rather than as a blob.
    return value is None or isinstance(value, (str, int, float, bool))


async def _raw_row(thread_id: str, checkpoint_id: str) -> dict[str, Any] | None:
    async with checkpointer_pool().connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT checkpoint, metadata FROM checkpoints
            WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
            """,
            (thread_id, checkpoint_id),
        )
        return await cur.fetchone()
