"""A worker freezes what its turns leave alive only while idle, and each
freeze frees what the last one held that has died since."""

import asyncio
import gc
import weakref
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.server.services import heap_settle


class _Cycle:
    def __init__(self) -> None:
        self.me = self


async def _settles(executor, tasks, until) -> None:
    """Run the settle loop until ``until``, with one more turn started once
    it is watching."""
    with (
        patch.object(heap_settle, "_CHECK_S", 0),
        patch.object(heap_settle, "_INTERVAL_S", 0),
        patch.object(heap_settle.LocalRunExecutor, "get_instance", return_value=executor),
        patch.object(heap_settle.BackgroundRegistryStore, "get_instance", return_value=tasks),
    ):
        settle = asyncio.create_task(heap_settle.run())
        await asyncio.sleep(0)
        executor.runs_started += 1
        try:
            for _ in range(200):
                await asyncio.sleep(0)
                if until():
                    return
        finally:
            settle.cancel()
            await asyncio.gather(settle, return_exceptions=True)


@pytest.fixture
def thawed():
    yield
    gc.unfreeze()


@pytest.mark.asyncio
async def test_a_cycle_that_dies_after_a_freeze_is_freed_at_the_next(thawed):
    executor = SimpleNamespace(runs_started=0, has_live_runs=lambda: False)
    tasks = SimpleNamespace(has_live_tasks=lambda: False)
    held = _Cycle()
    dead = weakref.ref(held)
    gc.freeze()  # the last settle froze it while it was alive
    del held
    gc.collect()
    assert dead() is not None  # frozen, so no collection frees the cycle

    await _settles(executor, tasks, until=lambda: dead() is None)
    assert dead() is None


@pytest.mark.asyncio
async def test_nothing_is_frozen_while_a_turn_runs(thawed):
    executor = SimpleNamespace(runs_started=0, has_live_runs=lambda: True)
    tasks = SimpleNamespace(has_live_tasks=lambda: False)
    frozen = gc.get_freeze_count()
    await _settles(executor, tasks, until=lambda: False)
    assert gc.get_freeze_count() == frozen


@pytest.mark.asyncio
async def test_a_turn_counts_as_live_from_admission_until_its_last_task_ends():
    """A turn sets up under its admission lock before its run registers, a
    run's finalize and a cancelled run's unwind go on after its status is
    terminal, as do an orphan collector and a stopped run's shielded finalize
    after its task ended, and a cancelled subagent's writer after it settled."""
    from src.server.services.background_registry_store import BackgroundRegistryStore
    from src.server.services.runs.executor import (
        LocalRunExecution,
        LocalRunExecutor,
        LocalRunStatus,
    )

    executor = LocalRunExecutor()
    admission = await executor.get_admission_lock("t")
    async with admission:
        assert executor.has_live_runs()
    assert not executor.has_live_runs()

    collecting = asyncio.create_task(asyncio.Event().wait())
    executor._track_orphan_collector("t", "r", collecting)
    assert executor.has_live_runs()
    collecting.cancel()
    await asyncio.gather(collecting, return_exceptions=True)
    await asyncio.sleep(0)
    assert not executor.has_live_runs()

    # A stopped run cancelled again mid-finalize: its task ends, the shielded
    # finalize goes on.
    done_finalizing = asyncio.Event()

    async def stopped_run():
        await executor._shielded(done_finalizing.wait())

    run = asyncio.create_task(stopped_run())
    await asyncio.sleep(0)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    assert executor.has_live_runs()
    done_finalizing.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not executor.has_live_runs()

    release = asyncio.Event()
    finalizing = asyncio.create_task(release.wait())
    executor.executions[("t", "r")] = LocalRunExecution(
        thread_id="t", run_id="r", status=LocalRunStatus.COMPLETED,
        created_at=None, task=finalizing,
    )
    tasks = BackgroundRegistryStore()
    writer = SimpleNamespace(completed=True, live_writers=[finalizing])
    tasks._registries["t"] = SimpleNamespace(_tasks={"k": writer})
    assert executor.has_live_runs() and tasks.has_live_tasks()

    release.set()
    await finalizing
    writer.live_writers = []
    assert not executor.has_live_runs() and not tasks.has_live_tasks()
