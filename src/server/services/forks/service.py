"""Making a fork: a new thread holding the source's state at a cut.

The fork holds one checkpoint, the agent's state at the cut
(``materialize.py``), and none of the turns before it: it shows those from
the threads that hold them (``Borrowing.ancestors``) and numbers its own
turns on from theirs (``Borrowing.turn_base``). Background tasks the kept turns
finished are copied whole, so the forked agent can resume them.

The fork starts no turn. A caller that wants one sends it to the new thread
like any other message, so admission, billing and streaming stay the send
path's alone, for a user's click and a background service alike.

The checkpoints are written before the thread row exists. Where they live in
the app's database (the writer guard's condition) everything commits in one
transaction. Otherwise the checkpoints commit first under a thread id nothing
names yet, and the thread after; a thread row that fails to commit deletes
them, and a crash between the two leaves only checkpoints no thread points at.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Optional

from src.server.database import thread_fork
from src.server.database.conversation import create_thread, threads_read
from src.server.database.pool import get_db_connection
from src.server.database.shared_workspaces import is_holder
from src.server.services import writer_guard
from src.server.services.forks import materialize
from src.server.services.forks.model import (
    BorrowedSegment,
    Borrowing,
    ForkHead,
    ForkLineage,
    ForkRefused,
    ForkRequest,
    ThroughTurn,
    stale_refusal,
)
from src.server.services.forks.snapshot import Snapshot, take_snapshot
from src.server.services.shared_workspaces.access import thread_access

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ForkResult:
    thread_id: str
    #: The turns the fork shows from the threads that hold them.
    turns_borrowed: int
    #: For a ``BeforeTurn`` cut, the message of the turn cut off, for the
    #: caller to send again.
    prefill: Optional[str]


async def fork_thread(req: ForkRequest) -> ForkResult:
    started = time.monotonic()
    source = await threads_read.get_thread_auth_meta(req.source_thread_id)
    await thread_access(req.source_thread_id, req.user_id, meta=source)
    snapshot = await take_snapshot(req.source_thread_id, req.cut, req.kind)
    subagents_allowed = await threads_read.read_thread_subagents_allowed(req.source_thread_id)

    thread_id = str(uuid.uuid4())
    try:
        plan = await _plan(req, snapshot)
        cut = (
            await materialize.read_cut(req.source_thread_id, snapshot.checkpoint_id, plan.run_map)
            if snapshot.checkpoint_id
            else None
        )
        lineage = _lineage(req, snapshot, plan, cut, source)
        # The checkpoints commit on their own connection unless they share
        # the app's database (the writer guard's condition). Decided before
        # that commit, which can land and still raise.
        copied_apart = not writer_guard.guard_enabled()
        try:
            async with get_db_connection() as conn, conn.transaction():
                if copied_apart:
                    # Held to the commit's end, which a cleanup waits out.
                    await thread_fork.lock_thread(conn, writer_guard.thread_key(thread_id))
                else:
                    await _hold_source(conn, req.source_thread_id)
                # Again under the lock, which holds off a delete or compaction
                # from here on: one may have landed since the plan was read.
                await thread_fork.check_source(conn, **_source_check(snapshot))
                async with _checkpoint_connection(conn) as cp:
                    await _write_checkpoints(cp, req.source_thread_id, thread_id, cut, plan)
                await _write_thread(conn, req, lineage, plan, source, subagents_allowed, thread_id)
                # Last, under the source row's lock, so the fork commits only
                # while its maker can still read the source: its starter may
                # have made it private since the copy began.
                await thread_fork.hold_thread_row(conn, req.source_thread_id)
                await thread_access(
                    req.source_thread_id,
                    req.user_id,
                    meta=await threads_read.get_thread_auth_meta(req.source_thread_id, conn=conn),
                )
        except BaseException:
            if copied_apart:
                await _drop_unclaimed(thread_id)
            raise
    except thread_fork.StaleSource:
        raise stale_refusal() from None

    ancestors = lineage.borrow.ancestors if lineage.borrow is not None else ()
    borrowed = sum(s.through_turn - s.first_turn + 1 for s in ancestors)
    logger.info(
        "[fork] made thread=%s from=%s kind=%s cut=%s turn=%s checkpoint=%s turns=%d tasks=%d ms=%d",
        thread_id, req.source_thread_id, req.kind.name, req.cut.name, req.cut.turn_index,
        snapshot.checkpoint_id, borrowed, len(plan.tasks),
        (time.monotonic() - started) * 1000,
    )
    return ForkResult(thread_id=thread_id, turns_borrowed=borrowed, prefill=snapshot.prefill)


def _lineage(
    req: ForkRequest,
    snapshot: Snapshot,
    plan: thread_fork.LedgerPlan,
    cut: Optional[materialize.CutState],
    source: dict[str, Any],
) -> ForkLineage:
    """The fork's lineage: its own turns number on from the cut, and it
    borrows its source's borrowed turns and then the source's own up to the
    cut, each from the thread that holds them."""
    at = datetime.now(UTC)
    through_cut = isinstance(req.cut, ThroughTurn)
    turn_base = req.cut.turn_index + 1 if through_cut else req.cut.turn_index
    inherited = snapshot.source_borrow
    ancestors = list(inherited.ancestors) if inherited is not None else []
    head = cut.head if cut is not None else ForkHead()
    first = max(
        inherited.turn_base if inherited is not None else 0,
        min(snapshot.turn_indexes, default=turn_base),
    )
    through = turn_base - 1
    if first <= through and snapshot.turn_indexes:
        if plan.response_id is None:
            # Turns that never ran: nothing settles whether they changed.
            logger.warning(
                "[fork] source %s has no response through turn %d; its turns are not shown",
                req.source_thread_id, through,
            )
        else:
            ancestors.append(
                BorrowedSegment(
                    thread_id=req.source_thread_id,
                    first_turn=first,
                    through_turn=through,
                    response_id=plan.response_id,
                    run_horizon=snapshot.run_horizon,
                    runs_through=head.runs,
                )
            )
    title = req.title if req.title is not None else source.get("title")
    return ForkLineage(
        kind=req.kind.name,
        thread_id=req.source_thread_id,
        cut=req.cut.name,
        turn_index=req.cut.turn_index,
        checkpoint_id=snapshot.checkpoint_id,
        copy_scratchpad=req.kind.copy_scratchpad,
        tasks=len(plan.tasks),
        title=title,
        at=at,
        by=req.user_id,
        borrow=Borrowing(
            turn_base=turn_base,
            checkpoint_id=snapshot.checkpoint_id,
            ancestors=tuple(ancestors),
            head=head,
        ),
    )


def _source_check(snapshot: Snapshot) -> dict[str, Any]:
    return dict(
        source_thread_id=snapshot.source_thread_id,
        read_through=snapshot.read_through,
        run_horizon=snapshot.run_horizon,
        tip=snapshot.tip_read,
    )


async def _plan(req: ForkRequest, snapshot: Snapshot) -> thread_fork.LedgerPlan:
    """What the fork reads of the source's ledger, in one snapshot of its
    own, so the response that ends the kept turns and the task runs they
    launched agree. On its own connection, released before the copy takes
    one, so concurrent forks never each hold one while waiting for
    another."""
    async with get_db_connection() as conn, conn.transaction():
        await thread_fork.read_one_snapshot(conn)
        return await thread_fork.plan_ledger(
            conn,
            **_source_check(snapshot),
            turn_indexes=snapshot.turn_indexes,
            copy_task_runs=req.kind.copy_task_runs,
        )


async def _hold_source(conn: Any, source_thread_id: str) -> None:
    """Shared T(thread), as a running turn holds it: a fork reads beside a
    turn, but not while a compaction or a delete rewrites the thread. Tried
    for the window other brief holders are given before refusing."""
    key = writer_guard.thread_key(source_thread_id)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + writer_guard.LOCK_RETRY_WINDOW
    delay = 0.05
    while not await thread_fork.try_lock_thread_shared(conn, key):
        if loop.time() >= deadline:
            raise ForkRefused(409, "thread_busy", "This chat is being updated. Try again in a moment.")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 0.4)


async def _write_checkpoints(
    cp: Any,
    source_thread_id: str,
    thread_id: str,
    cut: Optional[materialize.CutState],
    plan: thread_fork.LedgerPlan,
) -> None:
    if cut is not None:
        checkpoint = cut.checkpoint
        await thread_fork.write_snapshot(cp, thread_id, checkpoint)
        await thread_fork.copy_cut_blobs(
            cp, source_thread_id=source_thread_id, target_thread_id=thread_id, cut=checkpoint
        )
        await thread_fork.copy_cut_writes(
            cp,
            source_thread_id=source_thread_id,
            target_thread_id=thread_id,
            checkpoint_id=checkpoint.checkpoint_id,
            run_map=plan.run_map,
        )
    await thread_fork.copy_checkpoint_chains(
        cp,
        source_thread_id=source_thread_id,
        target_thread_id=thread_id,
        tips=plan.task_tips,
        run_map=plan.run_map,
    )


async def _write_thread(
    conn: Any,
    req: ForkRequest,
    lineage: ForkLineage,
    plan: thread_fork.LedgerPlan,
    source: dict[str, Any],
    subagents_allowed: Optional[bool],
    thread_id: str,
) -> None:
    # In a shared workspace the fork is its maker's, private until they share
    # it, whoever started the source.
    created_by = req.user_id if is_holder(source["user_id"]) else None
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(source["workspace_id"]),
        current_status="completed",
        msg_type=source.get("msg_type") or "ptc",
        title=lineage.title,
        platform=req.platform,
        metadata=lineage.as_metadata(),
        subagents_allowed=subagents_allowed,
        reasoning_effort=source.get("reasoning_effort"),
        fast_mode=source.get("fast_mode"),
        created_by=created_by,
        conn=conn,
    )
    await thread_fork.write_tasks(conn, plan, thread_id)
    await thread_fork.finish_thread(
        conn, thread_id, checkpoint_id=lineage.checkpoint_id, llm_model=source.get("llm_model")
    )


async def _drop_unclaimed(thread_id: str) -> None:
    """Remove checkpoints committed apart from a thread row that failed. A
    commit that raised may still be landing, so this waits for the failed
    transaction's lock to go, then removes them only when no thread row
    names them."""
    try:
        async with get_db_connection() as conn, conn.transaction():
            await thread_fork.lock_thread(conn, writer_guard.thread_key(thread_id), timeout_s=30)
            if await threads_read.get_thread_auth_meta(thread_id, conn=conn) is not None:
                return
        async with _checkpoint_connection(None) as cp:
            await thread_fork.delete_checkpoints(cp, thread_id)
    except Exception:
        logger.warning("[fork] copied checkpoints left behind thread=%s", thread_id, exc_info=True)


@asynccontextmanager
async def _checkpoint_connection(app_conn: Any) -> AsyncIterator[Any]:
    """Where the checkpoints are written: the app transaction itself when
    they share its database, else a connection of the saver's pool in a
    transaction of its own, committed on exit."""
    if app_conn is not None and writer_guard.guard_enabled():
        yield app_conn
        return
    async with materialize.checkpointer_pool().connection() as conn, conn.transaction():
        yield conn
