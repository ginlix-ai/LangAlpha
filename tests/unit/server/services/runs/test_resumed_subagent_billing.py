"""Billing for a subagent a later turn resumes before its first round is billed.

A turn's collector bills once, after its slowest subagent, and gives up on a
subagent that idles, so a finished task can still hold an earlier turn's
usage when the next turn resumes it. The resume bills that round at once,
under the turn that spent it and only if that turn's ending bills its
subagents, so the resuming turn's own ending never decides it. Drives the
real collector, resume reset, usage merge, run kill and billing sink, with
the run's settled row answered by a fake.
"""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.run_executor import (
    _merge_subagent_usage,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs import subagent_collection
from src.server.services.runs.subagent_usage import bill_resumed_round

THREAD = "thread-resume"
COLLECTION = "src.server.services.runs.subagent_collection"
REGISTRY = "ptc_agent.agent.middleware.background_subagent.registry"
RUN_ENDING = "src.server.database.runs.lifecycle.get_run_ending"

COMPLETED = {"status": "completed", "metadata": {"is_byok": False}}


def _trackers(records: list) -> tuple[MagicMock, MagicMock]:
    tracker = MagicMock()
    tracker.get_per_call_records.return_value = records
    tools = MagicMock()
    tools.get_summary.return_value = {}
    return tracker, tools


def _usage_service(billed: list):
    def make(**_kwargs):
        svc = MagicMock()
        svc._token_usage = {}

        async def track(records):
            svc.records = records

        async def persist(**kwargs):
            billed.append(
                (
                    svc._token_usage["task_id"],
                    svc.records,
                    kwargs["response_id"],
                    kwargs["settle_task_run_id"],
                )
            )
            return True

        svc.track_llm_usage = AsyncMock(side_effect=track)
        svc.persist_usage = AsyncMock(side_effect=persist)
        return svc

    return make


def _ending(row: dict) -> dict:
    return {**row, "workspace_id": "ws-1", "user_id": "user-1"}


class _Thread:
    """Turn one spawns a quick task and a slow one and ends completed; its
    collector bills nothing until the slow one settles."""

    def __init__(self, billed: list, endings: dict | None = None) -> None:
        self.billed = billed
        self.endings = {"run-1": COMPLETED, **(endings or {})}
        self.registry = BackgroundTaskRegistry(thread_id=THREAD)
        self.registry.usage_biller = bill_resumed_round
        self.store = BackgroundRegistryStore()
        self.store._registries[THREAD] = self.registry
        self.middleware = BackgroundSubagentMiddleware(
            registry=self.registry, enabled=True, subagent_types={"general-purpose"}
        )
        self.release_slow = asyncio.Event()
        self.collectors: list[asyncio.Task] = []

    async def turn_one(self) -> None:
        quick = await self.registry.register(
            "tc-quick", "d", "p", "general-purpose", run_id="run-1"
        )
        quick.task_run_id = "tr-quick-1"

        async def round_one():
            _merge_subagent_usage(quick, *_trackers([{"round": 1}]))
            return {"success": True}

        quick.asyncio_task = asyncio.create_task(round_one())
        slow = await self.registry.register(
            "tc-slow", "d", "p", "general-purpose", run_id="run-1"
        )
        slow.task_run_id = "tr-slow-1"

        async def slow_round():
            await self.release_slow.wait()
            _merge_subagent_usage(slow, *_trackers([{"slow": 1}]))
            return {"success": True}

        slow.asyncio_task = asyncio.create_task(slow_round())
        await quick.asyncio_task
        self.quick, self.slow = quick, slow
        self.collect("run-1", await self.registry.claim_run_subagents("run-1", "run-1"))
        await asyncio.sleep(0.05)

    def collect(self, run_id: str, claimed: list) -> asyncio.Task:
        collector = asyncio.create_task(
            subagent_collection.collect_subagent_results_for_turn(
                THREAD, run_id, claimed, "ws-1", "user-1", timeout=60,
                track_orphan_collector=lambda *_: None,
            )
        )
        self.collectors.append(collector)
        return collector

    async def resume_quick(self, *, finishes: bool) -> None:
        await self.resume(self.quick, finishes=finishes)

    async def resume(self, task, *, finishes: bool) -> None:
        """Turn two resumes ``task``, as ``task_actions`` does."""
        await self.middleware._reset_task_for_resume(
            task,
            task_run_id=task.task_run_id.replace("-1", "-2"),
            spawned_run_id="run-2",
        )
        hold = asyncio.Event()
        if finishes:
            hold.set()

        async def round_two():
            try:
                await hold.wait()
                return {"success": True}
            finally:
                _merge_subagent_usage(task, *_trackers([{"round": 2}]))

        task.asyncio_task = asyncio.create_task(round_two())
        await asyncio.sleep(0)

    async def run_ending(self, run_id: str):
        row = self.endings.get(run_id)
        return _ending(row) if row is not None else None

    def patches(self) -> ExitStack:
        cache = MagicMock(enabled=False)
        stack = ExitStack()
        stack.enter_context(
            patch.object(BackgroundRegistryStore, "get_instance", return_value=self.store)
        )
        stack.enter_context(
            patch(
                "src.server.services.persistence.usage.UsagePersistenceService",
                side_effect=_usage_service(self.billed),
            )
        )
        stack.enter_context(patch(f"{COLLECTION}.get_cache_client", return_value=cache))
        stack.enter_context(
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache)
        )
        stack.enter_context(patch(f"{COLLECTION}.publish_settled_wake", AsyncMock()))
        stack.enter_context(patch(f"{COLLECTION}.get_sse_drain_timeout", return_value=0.01))
        stack.enter_context(
            patch(f"{COLLECTION}.get_subagent_orphan_collector_timeout", return_value=0.3)
        )
        stack.enter_context(patch(RUN_ENDING, side_effect=self.run_ending))
        return stack

    def rows(self) -> list[tuple]:
        names = {self.quick.task_id: "quick", self.slow.task_id: "slow"}
        return [(names[task_id], *rest) for task_id, *rest in self.billed]


@pytest.mark.asyncio
async def test_a_resumed_round_ending_unbilled_leaves_the_first_round_billed():
    thread = _Thread([])
    with thread.patches():
        await thread.turn_one()
        await thread.resume_quick(finishes=False)
        # The resume billed the round turn one's collector owned, under the
        # run and ledger run that spent it.
        assert thread.rows() == [("quick", [{"round": 1}], "run-1", "tr-quick-1")]

        thread.release_slow.set()
        await thread.collectors[0]
        # Turn two ends in error: its subagents are killed unbilled.
        await thread.registry.cancel_run_tasks("run-2", force=True)

    assert thread.rows() == [
        ("quick", [{"round": 1}], "run-1", "tr-quick-1"),
        ("slow", [{"slow": 1}], "run-1", "tr-slow-1"),
    ]
    # The resumed round itself went unbilled with its turn.
    assert thread.quick.per_call_records == [{"round": 2}]


@pytest.mark.asyncio
async def test_each_round_is_billed_under_the_turn_that_spent_it():
    thread = _Thread([])
    with thread.patches():
        await thread.turn_one()
        await thread.resume_quick(finishes=True)

        # Turn two completes first, and its collector bills the resumed task.
        await thread.collect(
            "run-2", await thread.registry.claim_run_subagents("run-2", "run-2")
        )
        thread.release_slow.set()
        await thread.collectors[0]

    assert thread.rows() == [
        ("quick", [{"round": 1}], "run-1", "tr-quick-1"),
        ("quick", [{"round": 2}], "run-2", "tr-quick-2"),
        ("slow", [{"slow": 1}], "run-1", "tr-slow-1"),
    ]


@pytest.mark.asyncio
async def test_a_round_an_orphan_collector_owned_is_billed_when_it_collects_nothing():
    """An orphan collector bills only the tasks it ends up collecting. Once
    the resume steals one and the other idles out it collects none, so the
    resume is the only thing left to bill the stolen task's first round."""
    thread = _Thread([])
    registry = thread.registry
    with thread.patches():
        task = await registry.register("tc-t", "d", "p", "general-purpose", run_id="run-1")
        task.task_run_id = "tr-t-1"
        release = asyncio.Event()

        async def round_one():
            await release.wait()
            _merge_subagent_usage(task, *_trackers([{"round": 1}]))
            return {"success": True}

        task.asyncio_task = asyncio.create_task(round_one())
        idle = await registry.register("tc-s", "d", "p", "general-purpose", run_id="run-1")
        idle.task_run_id = "tr-s-1"
        idle.asyncio_task = asyncio.create_task(asyncio.Event().wait())

        claimed = await registry.claim_run_subagents("run-1", "run-1")
        orphan = asyncio.create_task(
            subagent_collection.collect_orphaned_subagent_results(
                THREAD, "run-1", [], claimed, "ws-1", "user-1"
            )
        )
        await asyncio.sleep(0.02)
        release.set()
        await asyncio.sleep(0.05)
        assert task.completed and task.collector_response_id == "run-1"

        await thread.resume(task, finishes=False)
        await orphan
        await registry.cancel_run_tasks("run-2", force=True)
        idle.asyncio_task.cancel()

    assert [(task_id, *rest) for task_id, *rest in thread.billed] == [
        (task.task_id, [{"round": 1}], "run-1", "tr-t-1")
    ]


@pytest.mark.asyncio
async def test_a_round_a_collector_gave_up_on_is_billed_when_resumed():
    """A collector that gives up on an idle subagent releases its claim, so
    what that subagent later spends is owned by nobody. Resumed, it is billed
    under the turn that spawned it."""
    thread = _Thread([])
    registry = thread.registry
    with thread.patches():
        task = await registry.register("tc-t", "d", "p", "general-purpose", run_id="run-1")
        task.task_run_id = "tr-t-1"
        release = asyncio.Event()

        async def round_one():
            await release.wait()
            _merge_subagent_usage(task, *_trackers([{"round": 1}]))
            return {"success": True}

        task.asyncio_task = asyncio.create_task(round_one())
        claimed = await registry.claim_run_subagents("run-1", "run-1")
        await subagent_collection.collect_orphaned_subagent_results(
            THREAD, "run-1", [], claimed, "ws-1", "user-1"
        )
        assert task.collector_response_id is None
        release.set()
        await task.asyncio_task
        task.terminal_status = "completed"

        await thread.resume(task, finishes=True)

    assert [(task_id, *rest) for task_id, *rest in thread.billed] == [
        (task.task_id, [{"round": 1}], "run-1", "tr-t-1")
    ]


@pytest.mark.asyncio
async def test_a_subagent_whose_model_call_still_beats_is_waited_out():
    """A model call can stream nothing for as long as its chunk timeout, and
    only its heartbeat says it is still running. The collector waits it out
    and bills it, where with no beat it gives up at its idle timeout."""
    thread = _Thread([])
    registry = thread.registry
    with thread.patches(), patch(f"{REGISTRY}._ALIVE_STAMP_INTERVAL_S", 0.0):
        task = await registry.register("tc-t", "d", "p", "general-purpose", run_id="run-1")
        task.task_run_id = "tr-t-1"

        async def silent_call():
            # Twice the idle timeout without an event, beating all along.
            for _ in range(6):
                await asyncio.sleep(0.1)
                await registry.note_alive(task)
            _merge_subagent_usage(task, *_trackers([{"round": 1}]))
            return {"success": True}

        task.asyncio_task = asyncio.create_task(silent_call())
        claimed = await registry.claim_run_subagents("run-1", "run-1")
        await subagent_collection.collect_orphaned_subagent_results(
            THREAD, "run-1", [], claimed, "ws-1", "user-1"
        )

    assert [(task_id, *rest) for task_id, *rest in thread.billed] == [
        (task.task_id, [{"round": 1}], "run-1", "tr-t-1")
    ]


@pytest.mark.asyncio
async def test_a_childs_beat_keeps_its_owner_alive():
    """The collector watches a workflow owner alone, so a child's beat is
    stamped on the owner as well."""
    registry = BackgroundTaskRegistry(thread_id=THREAD)
    owner = await registry.register("tc-o", "d", "p", "workflow", run_id="run-1")
    child = await registry.register("tc-c", "d", "p", "general-purpose", run_id="run-1")
    child.owner_task_id = owner.task_id
    owner.alive_at = child.alive_at = 0.0

    with patch(f"{REGISTRY}._ALIVE_STAMP_INTERVAL_S", 0.0):
        await registry.note_alive(child)

    assert owner.alive_at == child.alive_at > 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ending, billed",
    [
        pytest.param(COMPLETED, True, id="completed"),
        pytest.param({"status": "interrupted", "metadata": {}}, True, id="interrupted"),
        pytest.param(
            {"status": "cancelled", "metadata": {"cancelled_by_user": True}},
            True,
            id="user_stop",
        ),
        pytest.param(
            {"status": "cancelled", "metadata": {"cancelled_by_user": False}},
            False,
            id="shutdown",
        ),
        pytest.param({"status": "error", "metadata": {}}, False, id="error"),
        pytest.param({"status": "in_progress", "metadata": {}}, False, id="unsettled"),
        pytest.param(None, False, id="no_row"),
    ],
)
async def test_the_earlier_turns_ending_decides_whether_its_round_is_billed(
    ending, billed
):
    """The rule each turn's own collector or kill applies: a turn that ended
    by error, timeout or shutdown leaves its subagents unbilled, so its
    round is dropped here too rather than carried into the resumed one."""
    thread = _Thread([], endings={"run-1": ending})
    registry = thread.registry
    with thread.patches():
        task = await registry.register("tc-t", "d", "p", "general-purpose", run_id="run-1")
        task.task_run_id = "tr-t-1"
        task.terminal_status = "completed"
        task.per_call_records = [{"round": 1}]

        await thread.resume(task, finishes=False)

    assert [(task_id, *rest) for task_id, *rest in thread.billed] == (
        [(task.task_id, [{"round": 1}], "run-1", "tr-t-1")] if billed else []
    )
    assert task.per_call_records == []
    task.asyncio_task.cancel()


@pytest.mark.asyncio
async def test_a_resume_inside_the_spawning_turn_carries_the_usage_into_the_round():
    """Nothing owns it yet and the same turn's ending governs both rounds,
    so the round it is resumed into bills it."""
    registry = BackgroundTaskRegistry(thread_id=THREAD)
    registry.usage_biller = AsyncMock()
    task = await registry.register("tc-1", "d", "p", "general-purpose", run_id="run-1")
    task.terminal_status = "completed"
    task.per_call_records = [{"round": 1}]

    taken = await registry.reclaim_for_resume(
        task, task_run_id="tr-2", spawned_run_id="run-1"
    )

    assert taken is None
    assert task.per_call_records == [{"round": 1}]
    assert task.task_run_id == "tr-2"


@pytest.mark.asyncio
@pytest.mark.parametrize("collector_first", [True, False], ids=["collector", "resume"])
async def test_a_take_racing_the_resume_takes_the_round_once_under_its_own_run(
    collector_first,
):
    """The resume takes the usage and remints the run ids in one lock
    section, so a collector take queued beside it either takes the round
    under the run that spent it or finds the claim gone."""
    registry = BackgroundTaskRegistry(thread_id=THREAD)
    registry.usage_biller = AsyncMock()
    task = await registry.register("tc-1", "d", "p", "general-purpose", run_id="run-1")
    task.task_run_id = "tr-1"
    task.terminal_status = "completed"
    task.per_call_records = [{"round": 1}]
    task.collector_response_id = "run-1"

    def collector():
        return registry.take_owned_usage([task], "run-1")

    def resume():
        return registry.reclaim_for_resume(
            task, task_run_id="tr-2", spawned_run_id="run-2"
        )

    order = (collector, resume) if collector_first else (resume, collector)
    async with registry._lock:
        queued = [asyncio.create_task(step()) for step in order]
        await asyncio.sleep(0)
    results = dict(zip(order, await asyncio.gather(*queued)))

    takes = [*results[collector], *filter(None, [results[resume]])]
    assert [(t.records, t.settle_run_id, t.response_id) for t in takes] == [
        ([{"round": 1}], "tr-1", "run-1")
    ]
    assert (task.task_run_id, task.spawned_run_id) == ("tr-2", "run-2")


@pytest.mark.asyncio
async def test_a_shell_hydrated_on_another_worker_has_nothing_to_bill():
    """A resume on a worker that never ran the task rebuilds it from the
    checkpoint with no usage: the earlier round's usage lives on the worker
    that ran it, whose collector or kill bills it."""
    registry = BackgroundTaskRegistry(thread_id=THREAD)
    registry.usage_biller = AsyncMock()
    shell = BackgroundTask(
        tool_call_id="tc-1",
        task_id="abc123",
        description="d",
        prompt="d",
        subagent_type="general-purpose",
        terminal_status="completed",
        spawned_run_id="run-1",
        task_run_id="tr-1",
    )

    taken = await registry.reclaim_for_resume(
        shell, task_run_id="tr-2", spawned_run_id="run-2"
    )

    assert taken is None
    registry.usage_biller.assert_not_called()
    assert (shell.task_run_id, shell.spawned_run_id) == ("tr-2", "run-2")
    assert registry.get_by_tool_call_id("tc-1") is shell


@pytest.mark.asyncio
async def test_a_stop_landing_mid_bill_still_bills_the_earlier_round():
    """The resume has taken the round off the task by the time it bills, so a
    stop that cancels the resuming turn during that bill must not take the
    bill down with it: nothing else holds that usage."""
    registry = BackgroundTaskRegistry(thread_id=THREAD)
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True, subagent_types={"general-purpose"})
    billing, release = asyncio.Event(), asyncio.Event()
    billed: list = []

    async def bill(_thread_id, take):
        billing.set()
        await release.wait()
        billed.append((take.records, take.response_id))

    registry.usage_biller = bill
    task = await registry.register("tc-1", "d", "p", "general-purpose", run_id="run-1")
    task.terminal_status = "completed"
    task.per_call_records = [{"round": 1}]

    resume = asyncio.create_task(
        middleware._reset_task_for_resume(
            task, task_run_id="tr-2", spawned_run_id="run-2"
        )
    )
    await asyncio.wait_for(billing.wait(), timeout=1)
    resume.cancel()
    with pytest.raises(asyncio.CancelledError):
        await resume
    release.set()
    for _ in range(50):
        if billed:
            break
        await asyncio.sleep(0.01)

    assert billed == [([{"round": 1}], "run-1")]
