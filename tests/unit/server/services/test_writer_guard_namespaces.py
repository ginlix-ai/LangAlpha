"""WriterGuard task-namespace fence + root seal (v4 Phase 2.4e, I2).

Pins the namespace-ownership contract: exclusive N(thread, task:id) per
background-subagent writer on the run's pinned session, and a saver that —
once the guard sealed the root namespace at finalize — refuses every write
except to task namespaces this session still owns.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services import writer_guard as wg_module
from src.server.services.writer_guard import (
    GuardSessionLost,
    WriterGuard,
    namespace_key,
)


def _guard(thread_id: str = "t-1", run_id: str = "r-1") -> WriterGuard:
    conn = MagicMock()
    conn.closed = False
    conn.execute = AsyncMock()
    pool = MagicMock()
    pool.putconn = AsyncMock()
    return WriterGuard(pool, conn, thread_id, run_id)


def _cfg(ns: str) -> dict:
    return {"configurable": {"checkpoint_ns": ns}}


# ---------------------------------------------------------------------------
# check_write_ns / seal semantics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsealed_guard_permits_root_but_task_ns_requires_ownership():
    guard = _guard()
    guard.check_write_ns(_cfg(""))
    guard.check_write_ns(_cfg("anything|else"))
    # Task namespaces are ownership-gated even while root is unsealed: a
    # released task's late writer must refuse, not land in a namespace
    # whose lock a successor session may already hold.
    with pytest.raises(GuardSessionLost):
        guard.check_write_ns(_cfg("task:zzz"))
    guard._task_ns = {"zzz"}
    guard.check_write_ns(_cfg("task:zzz"))
    guard.check_write_ns(_cfg("task:zzz|child:uuid"))


@pytest.mark.asyncio
async def test_sealed_guard_permits_only_owned_task_namespaces():
    guard = _guard()
    guard._task_ns = {"abc"}
    guard.root_sealed = True

    # Owned task namespace — top-level and subgraph children.
    guard.check_write_ns(_cfg("task:abc"))
    guard.check_write_ns(_cfg("task:abc|child:uuid"))

    # Root, foreign task, ns-less config: all refused.
    for cfg in (_cfg(""), _cfg("task:zzz"), {"configurable": {}}, {}, None):
        with pytest.raises(GuardSessionLost):
            guard.check_write_ns(cfg)


@pytest.mark.asyncio
async def test_fenced_saver_gates_aput_and_aput_writes():
    """The seal is enforced at the saver, not just exposed as a flag."""
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    guard = _guard()
    guard.root_sealed = True
    guard._task_ns = {"abc"}

    with pytest.raises(GuardSessionLost):
        await guard.saver.aput(_cfg(""), {}, {}, {})
    with pytest.raises(GuardSessionLost):
        await guard.saver.aput_writes(_cfg("task:zzz"), [], "tid")

    # A permitted namespace delegates to the real saver.
    with (
        patch.object(AsyncPostgresSaver, "aput", new=AsyncMock()) as aput,
        patch.object(
            AsyncPostgresSaver, "aput_writes", new=AsyncMock()
        ) as aput_writes,
    ):
        await guard.saver.aput(_cfg("task:abc"), {}, {}, {})
        await guard.saver.aput_writes(_cfg("task:abc|x"), [], "tid")
        aput.assert_awaited_once()
        aput_writes.assert_awaited_once()


@pytest.mark.asyncio
async def test_demote_to_tail_seals_before_dropping_root():
    guard = _guard()
    guard._root_held = True
    guard._task_ns = {"abc"}

    await guard.demote_to_tail()

    assert guard.root_sealed is True
    guard.check_write_ns(_cfg("task:abc"))  # surviving tail still writes
    with pytest.raises(GuardSessionLost):
        guard.check_write_ns(_cfg(""))
    # N(root) was actually unlocked on the session.
    sql, params = guard.conn.execute.await_args.args
    assert "pg_advisory_unlock" in sql
    assert params == (namespace_key("t-1", "root"),)


@pytest.mark.asyncio
async def test_release_seals_everything():
    """Post-release the saver survives only for readers: no namespace may
    write, so a straggler fails loudly instead of landing via the pool."""
    guard = _guard()
    guard._task_ns = {"abc"}

    await guard.release()

    assert guard.root_sealed is True
    assert guard._task_ns == set()
    with pytest.raises(GuardSessionLost):
        guard.check_write_ns(_cfg("task:abc"))


# ---------------------------------------------------------------------------
# acquire_task_ns / release_task_ns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_task_ns_locks_the_derived_key_and_is_idempotent():
    guard = _guard()
    guard._try_lock = AsyncMock(return_value=True)

    assert await guard.acquire_task_ns("abc") is True
    assert await guard.acquire_task_ns("abc") is True  # held: no second lock

    guard._try_lock.assert_awaited_once_with(
        "pg_try_advisory_lock", namespace_key("t-1", "task:abc")
    )
    assert "abc" in guard._task_ns


@pytest.mark.asyncio
async def test_acquire_task_ns_fails_closed():
    # Unusable session: refused without touching the connection.
    lost = _guard()
    lost.lost = True
    lost._try_lock = AsyncMock()
    assert await lost.acquire_task_ns("abc") is False
    lost._try_lock.assert_not_awaited()

    # Query error: refused, nothing recorded as held.
    erroring = _guard()
    erroring._try_lock = AsyncMock(side_effect=RuntimeError("conn broke"))
    assert await erroring.acquire_task_ns("abc") is False
    assert "abc" not in erroring._task_ns

    # Contended: another session owns the namespace.
    contended = _guard()
    contended._try_lock = AsyncMock(return_value=False)
    assert await contended.acquire_task_ns("abc") is False
    assert "abc" not in contended._task_ns


@pytest.mark.asyncio
async def test_release_task_ns_unlocks_only_held_namespaces():
    guard = _guard()
    guard._try_lock = AsyncMock(return_value=True)
    await guard.acquire_task_ns("abc")

    await guard.release_task_ns("never-held")
    guard.conn.execute.assert_not_awaited()

    await guard.release_task_ns("abc")
    sql, params = guard.conn.execute.await_args.args
    assert "pg_advisory_unlock" in sql
    assert params == (namespace_key("t-1", "task:abc"),)

    guard.conn.execute.reset_mock()
    await guard.release_task_ns("abc")  # double release: no second unlock
    guard.conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_release_task_ns_drops_permit_even_when_unlock_fails():
    """The permit set shrinks first: post-seal writes to the namespace refuse
    even if the server-side unlock failed (unlock_all reclaims it later)."""
    guard = _guard()
    guard._try_lock = AsyncMock(return_value=True)
    await guard.acquire_task_ns("abc")
    guard.conn.execute = AsyncMock(side_effect=RuntimeError("conn broke"))

    await guard.release_task_ns("abc")  # must not raise

    guard.root_sealed = True
    with pytest.raises(GuardSessionLost):
        guard.check_write_ns(_cfg("task:abc"))


# ---------------------------------------------------------------------------
# fenced_write / in-flight drain: authorization is atomic with the fence
# state, and no fence transition drops a lock over an in-flight write (the
# base saver serializes params OUTSIDE its I/O lock, so a bare pre-check
# could authorize a write that lands after seal + release).
# ---------------------------------------------------------------------------


async def _hold_write(guard, cfg):
    """Enter fenced_write and hold it until the returned event is set."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _writer():
        async with guard.fenced_write(cfg):
            entered.set()
            await release.wait()

    task = asyncio.create_task(_writer())
    await entered.wait()
    return release, task


@pytest.mark.asyncio
async def test_fenced_write_authorizes_and_tracks_inflight():
    guard = _guard()
    async with guard.fenced_write(_cfg("")):
        assert guard._inflight_ns == {"": 1}
    assert guard._inflight_ns == {}

    guard.root_sealed = True
    with pytest.raises(GuardSessionLost):
        async with guard.fenced_write(_cfg("")):
            pass
    assert guard._inflight_ns == {}  # refused before being counted


@pytest.mark.asyncio
async def test_demote_seals_immediately_but_drops_root_only_after_drain():
    """An in-flight root write authorized before the seal must land before
    N(root) is unlocked — new root writes are refused the moment the seal
    lands, but the lock waits for the straggler."""
    guard = _guard()
    guard._root_held = True
    release, writer = await _hold_write(guard, _cfg(""))

    demote = asyncio.create_task(guard.demote_to_tail())
    await asyncio.sleep(0.05)
    assert guard.root_sealed is True  # sealed at once
    assert not demote.done()
    guard.conn.execute.assert_not_awaited()  # ...but N(root) still held

    release.set()
    await writer
    await demote
    sql, params = guard.conn.execute.await_args.args
    assert "pg_advisory_unlock" in sql
    assert params == (namespace_key("t-1", "root"),)


@pytest.mark.asyncio
async def test_demote_does_not_wait_on_task_namespace_writes():
    """Tail writers keep flowing through the demote: only root-namespace
    writes gate the N(root) drop — task namespaces keep their own locks."""
    guard = _guard()
    guard._root_held = True
    guard._task_ns = {"abc"}
    release, writer = await _hold_write(guard, _cfg("task:abc"))

    await asyncio.wait_for(guard.demote_to_tail(), timeout=1.0)
    assert guard.root_sealed is True
    guard.conn.execute.assert_awaited()  # root dropped despite the tail write

    release.set()
    await writer


@pytest.mark.asyncio
async def test_demote_drain_timeout_fails_the_session(monkeypatch):
    """A root write that never lands is a wedged session: demote must fail
    the guard (locks die with the discarded conn) rather than unlock N(root)
    over the pending write."""
    monkeypatch.setattr(wg_module, "WRITE_DRAIN_TIMEOUT", 0.05)
    guard = _guard()
    guard._root_held = True
    release, writer = await _hold_write(guard, _cfg(""))

    await guard.demote_to_tail()
    assert guard.lost is True
    guard.conn.execute.assert_not_awaited()  # N(root) never dropped

    release.set()
    await writer


@pytest.mark.asyncio
async def test_release_task_ns_keeps_lock_over_inflight_write(monkeypatch):
    """The permit falls but the server-side lock is kept when the task's own
    write is still in flight (e.g. a double-cancelled writer's abandoned
    saver op) — never hand the namespace to another session over it."""
    monkeypatch.setattr(wg_module, "WRITE_DRAIN_TIMEOUT", 0.05)
    guard = _guard()
    guard._try_lock = AsyncMock(return_value=True)
    await guard.acquire_task_ns("abc")
    release, writer = await _hold_write(guard, _cfg("task:abc"))

    await guard.release_task_ns("abc")
    assert "abc" not in guard._task_ns  # permit dropped regardless
    guard.conn.execute.assert_not_awaited()  # lock kept (fail closed)

    release.set()
    await writer


@pytest.mark.asyncio
async def test_write_queued_at_release_is_refused_not_uncounted():
    """A task writer that had NOT yet authorized when release_task_ns ran
    (queued behind the session mutex, so invisible to the in-flight drain)
    must be refused at authorization — not slip through uncounted and write
    after the namespace lock was handed to a successor session."""
    guard = _guard()
    guard._try_lock = AsyncMock(return_value=True)
    await guard.acquire_task_ns("abc")

    async def _attempt_write():
        async with guard.fenced_write(_cfg("task:abc")):
            pass

    async with guard.mutex:
        writer = asyncio.create_task(_attempt_write())
        await asyncio.sleep(0.01)  # writer is queued on the mutex, uncounted
        released = asyncio.create_task(guard.release_task_ns("abc"))
        await asyncio.sleep(0.01)  # membership discarded; drain saw no write
        assert guard._inflight_ns == {}

    with pytest.raises(GuardSessionLost):
        await writer
    await released
    sql, params = guard.conn.execute.await_args.args  # unlock did happen...
    assert "pg_advisory_unlock" in sql
    assert params == (namespace_key("t-1", "task:abc"),)


@pytest.mark.asyncio
async def test_release_discards_the_session_when_a_write_is_wedged(monkeypatch):
    """The clean release path must not unlock_all/retarget over an in-flight
    write: a drain timeout latches discard, so the closed conn fails the
    straggler loudly instead of letting it land via the pool."""
    monkeypatch.setattr(wg_module, "WRITE_DRAIN_TIMEOUT", 0.05)
    guard = _guard()
    guard.conn.close = AsyncMock()
    guard._task_ns = {"zzz"}  # owned before the write authorizes
    release, writer = await _hold_write(guard, _cfg("task:zzz"))

    await guard.release()
    guard.conn.close.assert_awaited()  # discard path
    assert guard.saver.conn is guard.conn  # never retargeted at the pool

    release.set()
    await writer
