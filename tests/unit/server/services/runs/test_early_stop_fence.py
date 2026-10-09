"""A Stop pressed before the executor handoff must not leave its thread fenced.

The disconnect cancels the stream task through an AnyIO scope, which cancels
again at every await. If the death-path teardown dies at its first await, the
run stays in_progress, its WriterGuard keeps the thread's fence alive with
monitor pings, the recovery scanner skips it as live, and admission refuses
every later turn on the thread with "stopping". Three layers pin it shut:
the teardown itself, the guard's owner check, and the scanner's warning.
"""

from __future__ import annotations

import asyncio
import gc
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

from src.server.services import writer_guard as wg_module
from src.server.services.runs.admission import RunScope
from src.server.services.runs.coordinator import RunCoordinator, RunHandle, RunOutcome
from src.server.services.runs.recovery import RecoveryScanner
from src.server.services.writer_guard import WriterGuard


def _handle() -> RunHandle:
    return RunHandle(
        run_id="r-1", thread_id="t-1", turn_index=0, attempt_no=1, msg_type="ptc"
    )


# ---------------------------------------------------------------------------
# RunScope.fail_open
# ---------------------------------------------------------------------------


_RELEASE = "src.server.dependencies.usage_limits.release_burst_slot"
_COORDINATOR = "src.server.services.runs.coordinator.RunCoordinator.get_instance"


@pytest.mark.asyncio
async def test_fail_open_settles_the_run_when_every_await_is_cancelled():
    """The slot release is a Redis round trip in platform mode; awaited first
    on the cancelled stream task, it took the run settle down with it."""
    settled = []
    done = asyncio.Event()

    def _record(step):
        settled.append(step)
        if len(settled) == 2:
            done.set()

    async def _finalize_run(handle, outcome):
        await asyncio.sleep(0)
        _record((handle.run_id, outcome.status))

    async def _release(user_id, slot_id):
        await asyncio.sleep(0)
        _record(("slot", slot_id))

    coordinator = RunCoordinator()
    finalize = AsyncMock(side_effect=_finalize_run)
    scope = RunScope(user_id="u-1", burst_slot_id="s-1")
    scope.attach_run(_handle())

    with (
        patch(_RELEASE, new=_release),
        patch.object(coordinator, "finalize_run", new=finalize),
        patch(_COORDINATOR, return_value=coordinator),
    ):
        with anyio.CancelScope() as cancel:
            cancel.cancel()
            await scope.fail_open("client disconnected during setup")
        assert cancel.cancelled_caught

        await asyncio.wait_for(done.wait(), timeout=1)
        # A second death path (the generator's finally) finds nothing to do.
        await scope.fail_open("again")

    assert sorted(settled) == [("r-1", "cancelled"), ("slot", "s-1")]
    finalize.assert_awaited_once()
    assert not scope.slot_owned


@pytest.mark.asyncio
async def test_a_settle_stuck_on_the_database_does_not_hold_the_burst_slot():
    """The slot is the user's, across every thread; a finalize blocked on a
    checkpoint read must not keep it until the slot reaper's horizon."""
    released = asyncio.Event()
    unstuck = asyncio.Event()

    async def _stuck_finalize(*_a, **_k):
        await unstuck.wait()

    coordinator = RunCoordinator()
    scope = RunScope(user_id="u-1", burst_slot_id="s-1")
    scope.attach_run(_handle())

    with (
        patch(_RELEASE, new=AsyncMock(side_effect=lambda *a: released.set())),
        patch.object(
            coordinator, "finalize_run", new=AsyncMock(side_effect=_stuck_finalize)
        ),
        patch(_COORDINATOR, return_value=coordinator),
    ):
        teardown = asyncio.create_task(scope.fail_open("generator closed"))
        try:
            await asyncio.wait_for(released.wait(), timeout=1)
        finally:
            unstuck.set()
            await teardown


@pytest.mark.asyncio
async def test_releasing_the_slot_is_not_a_cancellation_point():
    """Error paths release the slot and then settle the run, on a stream task
    a disconnect may be cancelling. A release that suspends gives that
    cancellation a place to skip the settle, and a release cut there holds
    the user's lease until the reaper."""
    released = asyncio.Event()
    reached = []

    async def _release(user_id, slot_id):
        await asyncio.sleep(0)
        released.set()

    scope = RunScope(user_id="u-1", burst_slot_id="s-1")
    with patch(_RELEASE, new=_release):
        with anyio.CancelScope() as cancel:
            cancel.cancel()
            await scope.release_slot()
            reached.append("the settle")
            await asyncio.sleep(0)
        assert cancel.cancelled_caught
        await asyncio.wait_for(released.wait(), timeout=1)

    assert reached == ["the settle"]
    assert not scope.slot_owned


# ---------------------------------------------------------------------------
# WriterGuard owner check
# ---------------------------------------------------------------------------


async def _until(condition) -> None:
    while not condition():
        await asyncio.sleep(0)


def _guard() -> WriterGuard:
    conn = MagicMock()
    conn.closed = False
    conn.execute = AsyncMock()
    conn.close = AsyncMock()
    pool = MagicMock()
    pool.putconn = AsyncMock()
    return WriterGuard(pool, conn, "t-1", "r-1")


@pytest.mark.asyncio
async def test_guard_releases_the_fence_of_a_run_nothing_can_settle(monkeypatch):
    monkeypatch.setattr(wg_module, "MONITOR_INTERVAL", 0)
    guard = _guard()
    handle = _handle()
    guard.bind_owner(handle)
    guard._monitor_task = asyncio.create_task(guard._monitor())

    del handle
    gc.collect()
    await asyncio.wait_for(guard._monitor_task, timeout=1)
    await asyncio.wait_for(guard._release_task, timeout=1)

    assert guard._released
    # Discarded, not recycled: a dead run's saver may still hold the conn.
    guard.conn.close.assert_awaited_once()
    guard._pool.putconn.assert_awaited_once_with(guard.conn)
    # The grace tick still pinged; the second one released.
    assert guard.conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_a_death_path_that_hands_over_within_the_grace_tick_keeps_the_fence(
    monkeypatch,
):
    """The cycle collector clears the handle's weakref before an abandoned
    stream generator's death path runs; that path then reaches finalize,
    which takes the guard over. Dropping the fence on the first dead tick
    would relabel the settle as a lost session."""
    monkeypatch.setattr(wg_module, "MONITOR_INTERVAL", 0)
    guard = _guard()
    handle = _handle()
    guard.bind_owner(handle)
    del handle
    gc.collect()
    guard._monitor_task = asyncio.create_task(guard._monitor())

    await asyncio.wait_for(_until(lambda: guard.conn.execute.await_count >= 1), 1)
    guard.bind_owner(None)  # finalize_run's first line
    await asyncio.wait_for(_until(lambda: guard.conn.execute.await_count >= 3), 1)
    guard._monitor_task.cancel()

    assert guard._release_task is None


@pytest.mark.asyncio
@pytest.mark.parametrize("handed_to_teardown", [False, True])
async def test_guard_keeps_the_fence_while_the_run_has_an_owner(
    monkeypatch, handed_to_teardown
):
    """A live handle, or finalize's teardown after ``bind_owner(None)``, still
    settles the run: the monitor only pings."""
    monkeypatch.setattr(wg_module, "MONITOR_INTERVAL", 0)
    guard = _guard()
    handle = _handle()
    guard.bind_owner(handle)
    if handed_to_teardown:
        guard.bind_owner(None)
        del handle
        gc.collect()
    guard._monitor_task = asyncio.create_task(guard._monitor())

    # Two pings: the owner check ran on two ticks and let both through.
    await asyncio.wait_for(_until(lambda: guard.conn.execute.await_count >= 2), 1)
    guard._monitor_task.cancel()

    assert guard._release_task is None
    guard.conn.execute.assert_any_await("SELECT 1")


@pytest.mark.asyncio
async def test_the_handle_owns_the_guard_until_finalize_takes_it_over():
    """START binds the guard to its handle; finalize unbinds it before its
    first await. Unbinding only at teardown let the monitor drop the fence
    under a death path's settle once the cycle collector had cleared the
    handle's weakref, and never unbinding would cut a background subagent
    still writing through the session after the turn."""
    guard = MagicMock(mutex=asyncio.Lock(), release=AsyncMock())
    row = {
        "turn_index": 0,
        "attempt_no": 1,
        "created_at": datetime.now(timezone.utc),
        "run_seq": 1,
    }
    with (
        patch(f"{wg_module.__name__}.guard_enabled", return_value=True),
        patch.object(
            wg_module.WriterGuard, "acquire_root", new=AsyncMock(return_value=guard)
        ),
        patch(
            "src.server.database.runs.lifecycle.start_run",
            new=AsyncMock(return_value=row),
        ),
        patch(
            "src.server.services.thread_control_stream.announce_run_started",
            new=AsyncMock(),
        ),
        patch(
            "src.server.services.thread_lifecycle_feed.publish_run_started",
            new=AsyncMock(),
        ),
    ):
        coordinator = RunCoordinator()
        handle = await coordinator.start_run(
            thread_id="t-1", run_id="r-1", msg_type="ptc"
        )

    guard.bind_owner.assert_called_once_with(handle)

    async def _checkpoint_read_hangs(_handle):
        await asyncio.Event().wait()

    with patch.object(coordinator, "_latest_checkpoint_id", new=_checkpoint_read_hangs):
        settle = asyncio.create_task(
            coordinator.finalize_run(handle, RunOutcome(status="cancelled"))
        )
        await asyncio.wait_for(
            _until(lambda: guard.bind_owner.call_args.args == (None,)), 1
        )
        settle.cancel()
        with pytest.raises(asyncio.CancelledError):
            await settle

    # Every exit from finalize still releases what it took over.
    await asyncio.wait_for(_until(lambda: guard.release.await_count == 1), 1)
    assert guard.release.await_args.kwargs == {"discard": True}


@pytest.mark.asyncio
async def test_a_disconnect_during_the_start_announcements_leaves_the_run_to_the_scope():
    """The row commits before START announces it, and no executor takes a run
    whose START raised, so the scope must already hold it when a disconnect
    lands there, or the death path settles nothing and the thread stays fenced
    until recovery. The guard goes with START's exit, so that settle runs on
    the pool."""
    guard = MagicMock(mutex=asyncio.Lock(), release=AsyncMock())
    row = {
        "turn_index": 0,
        "attempt_no": 1,
        "created_at": datetime.now(timezone.utc),
        "run_seq": 1,
    }
    scope = RunScope(user_id="u-1", burst_slot_id=None)
    with (
        patch(f"{wg_module.__name__}.guard_enabled", return_value=True),
        patch.object(
            wg_module.WriterGuard, "acquire_root", new=AsyncMock(return_value=guard)
        ),
        patch(
            "src.server.database.runs.lifecycle.start_run",
            new=AsyncMock(return_value=row),
        ),
        patch(
            "src.server.services.thread_control_stream.announce_run_started",
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ),
        patch(
            "src.server.services.thread_lifecycle_feed.publish_run_started",
            new=AsyncMock(),
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await RunCoordinator().start_run(
            thread_id="t-1", run_id="r-1", msg_type="ptc", on_started=scope.attach_run
        )

    handle = scope.owned_run_handle
    assert (handle.run_id, handle.guard) == ("r-1", None)
    guard.release.assert_awaited_once()


# ---------------------------------------------------------------------------
# RecoveryScanner fenced-run warning
# ---------------------------------------------------------------------------


def _fenced_lock_conn():
    cur = MagicMock(fetchone=AsyncMock(return_value=(False,)))
    return MagicMock(execute=AsyncMock(return_value=cur))


def _open_run(stop_age: timedelta | None) -> dict:
    return {
        "conversation_response_id": "r-1",
        "conversation_thread_id": "t-1",
        "cancel_requested_at": (
            None if stop_age is None else datetime.now(timezone.utc) - stop_age
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stop_age", "warnings"),
    [
        (None, 0),  # a live turn
        (timedelta(seconds=30), 0),  # a stop still settling
        (timedelta(minutes=5), 1),  # a stop whose owner is wedged
    ],
)
async def test_scanner_warns_once_for_a_stop_that_never_settled(
    caplog, stop_age, warnings
):
    scanner = RecoveryScanner()
    run = _open_run(stop_age)

    with caplog.at_level(logging.WARNING, logger="src.server.services.runs.recovery"):
        for _ in range(3):
            assert await scanner._scan([run], _fenced_lock_conn()) == 0

    stuck = [r for r in caplog.records if "after its stop request" in r.message]
    assert len(stuck) == warnings
