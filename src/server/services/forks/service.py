"""Making a fork: a new thread holding the source's history up to a cut.

The fork starts no turn. A caller that wants one sends it to the new thread
like any other message, so admission, billing and streaming stay the send
path's alone, for a user's click and a background service alike.

The checkpoints are copied before the thread row exists. Where they live in
the app's database (the writer guard's condition) everything commits in one
transaction. Otherwise the checkpoints commit first under a thread id nothing
names yet, and the ledger after; a ledger that fails to commit deletes them,
and a crash between the two leaves only checkpoints no thread points at.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Optional

from psycopg_pool import AsyncConnectionPool

from src.server.database import thread_fork
from src.server.database.conversation import create_thread, threads_read
from src.server.database.pool import get_db_connection
from src.server.database.shared_workspaces import is_holder
from src.server.services import writer_guard
from src.server.services.forks.model import (
    ForkLineage,
    ForkRefused,
    ForkRequest,
    stale_refusal,
)
from src.server.services.forks.snapshot import Snapshot, take_snapshot
from src.server.services.shared_workspaces.access import thread_access
from src.server.utils.checkpoint_helpers import get_checkpointer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ForkResult:
    thread_id: str
    turns_copied: int
    #: For a ``BeforeTurn`` cut, the message of the turn cut off, for the
    #: caller to send again.
    prefill: Optional[str]


async def fork_thread(req: ForkRequest) -> ForkResult:
    source = await threads_read.get_thread_auth_meta(req.source_thread_id)
    await thread_access(req.source_thread_id, req.user_id, meta=source)
    snapshot = await take_snapshot(req.source_thread_id, req.cut, req.kind)
    subagents_allowed = await threads_read.read_thread_subagents_allowed(req.source_thread_id)

    thread_id = str(uuid.uuid4())
    try:
        plan = await _plan(req, snapshot)
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
                    await _copy_chains(cp, req.source_thread_id, thread_id, snapshot, plan)
                await _write_thread(conn, req, snapshot, plan, source, subagents_allowed, thread_id)
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

    _after_commit(thread_id, str(source["workspace_id"]), bool(snapshot.turn_indexes))
    logger.info(
        "[fork] made thread=%s from=%s kind=%s cut=%s turn=%s checkpoint=%s turns=%d tasks=%d",
        thread_id, req.source_thread_id, req.kind.name, req.cut.name, req.cut.turn_index,
        snapshot.checkpoint_id, len(snapshot.turn_indexes), len(plan.tasks),
    )
    return ForkResult(thread_id=thread_id, turns_copied=len(snapshot.turn_indexes), prefill=snapshot.prefill)


def _source_check(snapshot: Snapshot) -> dict[str, Any]:
    return dict(
        source_thread_id=snapshot.source_thread_id,
        read_through=snapshot.read_through,
        run_horizon=snapshot.run_horizon,
        tip=snapshot.tip_read,
    )


async def _plan(req: ForkRequest, snapshot: Snapshot) -> thread_fork.LedgerPlan:
    """The source's ledger rows, read in one snapshot of their own: an edit
    committing between two reads would otherwise pair one turn's new message
    with its old reply. On its own connection, released before the copy
    takes one, so concurrent forks never each hold one while waiting for
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


async def _copy_chains(
    cp: Any, source_thread_id: str, thread_id: str, snapshot: Snapshot, plan: thread_fork.LedgerPlan
) -> None:
    tips = [thread_fork.ChainTip("", snapshot.checkpoint_id)] if snapshot.checkpoint_id else []
    copied = await thread_fork.copy_checkpoint_chains(
        cp,
        source_thread_id=source_thread_id,
        target_thread_id=thread_id,
        tips=tips + plan.task_tips,
        run_map=plan.run_map,
        task_run_map=plan.task_run_map(),
    )
    # The source was deleted after the snapshot named its checkpoint.
    if tips and not copied:
        raise thread_fork.StaleSource(source_thread_id)


async def _write_thread(
    conn: Any,
    req: ForkRequest,
    snapshot: Snapshot,
    plan: thread_fork.LedgerPlan,
    source: dict[str, Any],
    subagents_allowed: Optional[bool],
    thread_id: str,
) -> None:
    title = req.title if req.title is not None else source.get("title")
    # In a shared workspace the fork is its maker's, private until they share
    # it, whoever started the source.
    created_by = req.user_id if is_holder(source["user_id"]) else None
    lineage = ForkLineage(
        kind=req.kind.name,
        thread_id=req.source_thread_id,
        cut=req.cut.name,
        turn_index=req.cut.turn_index,
        checkpoint_id=snapshot.checkpoint_id,
        copy_scratchpad=req.kind.copy_scratchpad,
        tasks=len(plan.tasks),
        title=title,
        at=datetime.now(UTC),
        by=req.user_id,
    )
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(source["workspace_id"]),
        current_status="completed",
        msg_type=source.get("msg_type") or "ptc",
        title=title,
        platform=req.platform,
        metadata=lineage.as_metadata(),
        subagents_allowed=subagents_allowed,
        reasoning_effort=source.get("reasoning_effort"),
        fast_mode=source.get("fast_mode"),
        created_by=created_by,
        conn=conn,
    )
    last_run_seq = await thread_fork.write_ledger(conn, plan, thread_id)
    await thread_fork.finish_thread(
        conn,
        thread_id,
        checkpoint_id=snapshot.checkpoint_id,
        last_run_seq=last_run_seq,
        llm_model=source.get("llm_model"),
        shared_reader=created_by,
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
    pool = getattr(get_checkpointer(), "conn", None)
    if not isinstance(pool, AsyncConnectionPool):
        raise ForkRefused(501, "unsupported", "Copying a chat needs a Postgres checkpointer.")
    async with pool.connection() as conn, conn.transaction():
        yield conn


def _after_commit(thread_id: str, workspace_id: str, has_turns: bool) -> None:
    """What the copied turns' ends would have built: the transcript and, for
    each turn, its search row."""
    from src.server.services.search.index import schedule_thread_search_index
    from src.server.services.transcripts import schedule_thread_export

    try:
        schedule_thread_export(thread_id, workspace_id)
        if has_turns:
            schedule_thread_search_index(thread_id)
    except Exception:
        logger.warning("[fork] transcript or search index not scheduled thread=%s", thread_id, exc_info=True)
