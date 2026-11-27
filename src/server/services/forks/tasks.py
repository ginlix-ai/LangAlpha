"""The finished tasks a snapshot fork carries (``Borrowing.task_lanes``).

A task's lane is its whole checkpoint history, which can outweigh the thread
it ran in many times over, so a fork copies only the task's ledger rows. The
lane is read where it lives, at the carried runs' end, while the fork holds
none of it; a resume first writes it into the fork as one checkpoint there,
as the fork's own cut is written. Like the borrowed turns, a lane whose
thread was deleted, or that the fork's maker can no longer open, is gone.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

from src.server.database import thread_fork
from src.server.services.forks.model import BorrowedTask, Borrowing, ForkLineage
from src.server.services.forks.segments import reaches

logger = logging.getLogger(__name__)

#: The "could not start" reason for a resume of a carried task whose lane is
#: gone.
LANE_GONE = (
    "its earlier work is in the chat this one was forked from, which has since been "
    "deleted or can no longer be opened. To go on with it, launch a new task and give it "
    "the task's assignment and what it found so far, since the new task starts with none of it"
)


class LaneGone(Exception):
    """The carried task's lane is in a thread deleted since, or one the
    fork's maker can no longer open."""


def task_lanes(
    source_thread_id: str,
    plan: thread_fork.LedgerPlan,
    inherited: Optional[Borrowing],
    held_here: frozenset[str] = frozenset(),
) -> tuple[BorrowedTask, ...]:
    """Where a new fork reads each task it carries: at the newest carried
    run's end, in the source, or where the source reads it when the source
    borrows that task too, holding no lane of its own (``held_here``) and no
    run of its own since. A task that never checkpointed has no lane to
    read."""
    lanes = []
    for task, runs in plan.tasks:
        ended = [r for r in runs if r["final_checkpoint_id"]]
        if not ended:
            continue
        task_id = task["task_id"]
        held = (
            inherited.task_lane(task_id)
            if inherited is not None and task_id not in held_here
            else None
        )
        newest = str(ended[-1]["task_run_id"])
        lane_ids = held.runs if held is not None and newest in held.runs else {}
        ids = [str(r["task_run_id"]) for r in runs]
        lanes.append(
            BorrowedTask(
                task_id=task_id,
                thread_id=held.thread_id if lane_ids else source_thread_id,
                checkpoint_id=ended[-1]["final_checkpoint_id"],
                runs={plan.run_map[i]: lane_ids.get(i, i) for i in ids},
            )
        )
    return tuple(lanes)


@dataclass(frozen=True)
class CarriedLane:
    """A task the thread carries from the chat it was forked from."""

    thread_id: str
    task: BorrowedTask
    #: The fork's maker, as whom its sources are read, as a public link
    #: reads its borrowed turns.
    by: str

    async def readable(self, conn: Any = None) -> bool:
        """Whether the lane's thread still exists and the fork's maker can
        open it, its access read on ``conn`` when given."""
        from src.server.database.conversation import threads_read
        from src.server.database.conversation.thread_reach import is_live

        holder = self.task.thread_id
        if not await is_live(holder, conn=conn):
            return False
        meta = await threads_read.get_thread_auth_meta(holder, conn=conn) if conn is not None else None
        return await reaches(holder, self.by, meta=meta)

    async def values(self) -> dict[str, Any]:
        """The lane's state at the carried runs' end, its ``ui`` records
        naming the fork's runs, by which TaskOutput finds a workflow's
        archived result."""
        from src.server.services.history.reader import CheckpointHistoryReader

        state = await CheckpointHistoryReader.get_instance().aget_task_state(
            self.task.thread_id, self.task.task_id, self.task.checkpoint_id
        )
        values = dict(state.values or {})
        if values.get("ui") is not None:
            values["ui"] = thread_fork.remap_ui_records(values["ui"], self.task.lane_to_fork)
        return values


async def carried_lane(thread_id: str, task_id: str) -> Optional[CarriedLane]:
    from src.server.database.thread_transcripts import thread_checkpoint

    row = await thread_checkpoint(thread_id)
    lineage = ForkLineage.parse(row.fork) if row is not None else None
    if lineage is None or lineage.borrow is None:
        return None
    task = lineage.borrow.task_lane(task_id)
    return CarriedLane(thread_id, task, lineage.by) if task is not None else None


async def carry_task_lane(thread_id: str, task_id: str, fence: Any = None) -> None:
    """Write a carried task's lane into the thread that carries it, as one
    checkpoint at the carried runs' end, before a run of the task there.
    Nothing to do for a task whose lane the thread already holds. Raises
    ``LaneGone`` when the lane can no longer be read.

    Called holding the task's namespace fence, the run's ``WriterGuard``
    (None where no guard fences writes), which the write goes through so a
    lost fence fails it. The holder's row is held from the access check to
    the commit, as the fork's own copy holds its source's, so a change to
    who can read it lands before the check or after the copy."""
    from src.server.database.pool import get_db_connection
    from src.server.services.forks import materialize
    from src.server.services.history.reader import CheckpointHistoryReader

    lane = await carried_lane(thread_id, task_id)
    if lane is None:
        return
    reader = CheckpointHistoryReader.get_instance()
    if await reader.alatest_checkpoint_id(thread_id, f"task:{task_id}") is not None:
        return
    holder = lane.task.thread_id
    try:
        async with get_db_connection() as conn, conn.transaction():
            await thread_fork.hold_thread_row(conn, holder)
            if not await lane.readable(conn):
                raise LaneGone(task_id)
            checkpoint = await materialize.read_lane(lane)
            async with _lane_connection(fence, thread_id, checkpoint.checkpoint_ns) as cp, cp.transaction():
                await thread_fork.write_snapshot(cp, thread_id, checkpoint)
                await thread_fork.copy_blobs(
                    cp, source_thread_id=holder, target_thread_id=thread_id, cut=checkpoint
                )
    except thread_fork.StaleSource:
        raise LaneGone(task_id) from None
    logger.info(
        "[fork] carried task lane thread=%s task=%s from=%s checkpoint=%s",
        thread_id, task_id, holder, lane.task.checkpoint_id,
    )


@asynccontextmanager
async def _lane_connection(fence: Any, thread_id: str, checkpoint_ns: str) -> AsyncIterator[Any]:
    """The run's guarded session, the write authorized against its fence and
    counted until it lands (``WriterGuard.fenced_write``), or the saver's
    pool where no guard fences writes."""
    from src.server.services.forks import materialize

    if fence is None:
        async with materialize.checkpointer_pool().connection() as conn:
            yield conn
        return
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}}
    async with fence.fenced_write(config), fence.mutex:
        yield fence.conn
