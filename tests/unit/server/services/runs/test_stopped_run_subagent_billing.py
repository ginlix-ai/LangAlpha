"""Subagent billing for a run that ends with no collector.

A run the user stopped bills each of its subagents once, on a msg_type='task'
row, whether the stop killed them or they had already finished; any other
ending kills them unbilled. Every such ending archives each subagent's lane
once, in the finalize when a stop's drain carried it, else after it. Every
case drives the real ``_finalize_run`` over a real registry, with the finalize
CAS answering from a fake row, so the stop is read where production reads it:
off the row the CAS settled.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager, suppress
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.database.runs.lifecycle import FinalizeResult
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs.executor import (
    LocalRunExecution,
    LocalRunExecutor,
    LocalRunStatus,
)
from src.server.services.runs.sse_producer import RunSSEProducer
from src.server.services.runs.teardown import StopTeardown

USAGE_SERVICE = "src.server.services.persistence.usage.UsagePersistenceService"
REGISTRY = "ptc_agent.agent.middleware.background_subagent.registry"
COORDINATOR = "src.server.services.runs.coordinator.RunCoordinator"
FINALIZATION = "src.server.services.runs.finalization"
READER = "src.server.services.history.reader.CheckpointHistoryReader"
COLLECTION = "src.server.services.runs.subagent_collection"
TEARDOWN = "src.server.services.runs.teardown"

THREAD, RUN = "thread-stop", "run-stop"


def _make_btm() -> LocalRunExecutor:
    mod = "src.server.services.runs.executor"
    with patch(f"{mod}.get_max_concurrent_workflows", return_value=10), \
         patch(f"{mod}.get_workflow_result_ttl", return_value=3600), \
         patch(f"{mod}.get_abandoned_workflow_timeout", return_value=3600), \
         patch(f"{mod}.get_cleanup_interval", return_value=60), \
         patch(f"{mod}.is_intermediate_storage_enabled", return_value=False), \
         patch(f"{mod}.get_max_stored_messages_per_agent", return_value=1000), \
         patch(f"{mod}.get_event_storage_backend", return_value="memory"), \
         patch(f"{mod}.get_redis_ttl_workflow_events", return_value=86400):
        return LocalRunExecutor()


async def _register(registry, name: str, run_id: str = RUN):
    return await registry.register(
        f"tc-{name}", "d", "p", "general-purpose", run_id=run_id
    )


async def _subagents(
    registry: BackgroundTaskRegistry, *, unadopted: bool = False, on_unwind=None
) -> dict:
    """The run's subagents in every state a stop finds them in, plus a prior
    turn's task that is not this run's to touch."""
    tasks = {}

    finished = await _register(registry, "finished")
    finished.terminal_status = "completed"
    finished.per_call_records = [{"call": "finished"}]
    tasks["finished"] = finished

    live = await _register(registry, "live")

    async def writer():
        try:
            await asyncio.Event().wait()
        finally:
            # The real writer merges its tracker onto the task as it unwinds.
            live.per_call_records = [{"call": "live"}]
            if on_unwind is not None:
                await on_unwind()

    live.asyncio_task = asyncio.create_task(writer())
    tasks["live"] = live

    if unadopted:
        # Its writer returned, but nothing adopted the outcome, so it is
        # neither pending nor settled.
        idle = await _register(registry, "unadopted")

        async def done():
            return {"success": True}

        idle.asyncio_task = asyncio.create_task(done())
        await idle.asyncio_task
        idle.per_call_records = [{"call": "unadopted"}]
        tasks["unadopted"] = idle

    prior = await _register(registry, "prior", run_id="run-prior")
    prior.terminal_status = "completed"
    prior.per_call_records = [{"call": "prior"}]
    tasks["prior"] = prior

    await asyncio.sleep(0)
    return tasks


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
                    kwargs["msg_type"],
                )
            )
            return True

        svc.track_llm_usage = AsyncMock(side_effect=track)
        svc.persist_usage = AsyncMock(side_effect=persist)
        return svc

    return make


def _lanes(events: list[dict]) -> set[str]:
    return {e["data"]["agent"].removeprefix("task:") for e in events}


async def _capture_stream(_thread_id, task):
    """A capture stream holding one record of the round that spawned the task,
    from a tool call its checkpoint saved (``_saved_fetches``)."""
    yield {
        "event": "provenance",
        "data": {
            "agent": f"task:{task.task_id}",
            "identifier": "https://x.example",
            "tool_call_id": f"fetch-{task.task_id}",
        },
        "run": task.spawned_run_id,
    }


def _saved_fetches():
    """Every task's checkpoint holding the result of its one fetch."""
    reader = MagicMock()
    reader.aget_task_history = AsyncMock(
        side_effect=lambda _thread, task_id: SimpleNamespace(
            messages=[ToolMessage(content="page", tool_call_id=f"fetch-{task_id}")]
        )
    )
    return reader


@contextmanager
def _archive_writes(btm: LocalRunExecutor, archive: list | None):
    """Record the lanes written after the finalize into ``archive``, reading
    each from a one-record capture stream; without one, drain nothing."""
    if archive is None:
        with patch.object(
            btm, "_drain_killed_subagent_events", AsyncMock(return_value=[])
        ):
            yield
        return

    async def persist(events, response_id, *_args, **_kwargs):
        archive.append((f"after:{response_id}", _lanes(events)))
        return True

    with patch(f"{TEARDOWN}.iter_subagent_events_full", side_effect=_capture_stream), \
         patch(f"{COLLECTION}.persist_collected_events", side_effect=persist):
        yield


async def _settled() -> None:
    """Wait out the archive and bill a run's ending leaves running."""
    from src.server.services.runs import subagent_collection

    await asyncio.gather(*(
        t for t in subagent_collection._collector_tasks
        if t.get_loop() is asyncio.get_running_loop()
    ))


@contextmanager
def _run(
    btm: LocalRunExecutor,
    registry: BackgroundTaskRegistry,
    billed: list,
    *,
    user_stop: bool,
    finalize,
    archive: list | None = None,
):
    """Register the run and answer its finalize CAS from ``finalize``, logging
    the lanes each archive write carries into ``archive`` when given."""
    info = LocalRunExecution(
        thread_id=THREAD,
        run_id=RUN,
        status=LocalRunStatus.RUNNING,
        created_at=datetime.now(),
        user_stop=user_stop,
        metadata={
            "workspace_id": "ws-1",
            "user_id": "user-1",
            "is_byok": False,
            "run_handle": MagicMock(guard=None),
            "handler": RunSSEProducer(thread_id=THREAD, run_id=RUN),
        },
    )
    btm.executions[(THREAD, RUN)] = info
    store = BackgroundRegistryStore()
    store._registries[THREAD] = registry
    coordinator = MagicMock()
    if isinstance(finalize, Exception):
        coordinator.finalize_run = AsyncMock(side_effect=finalize)
    else:
        async def cas(_handle, outcome, **_kwargs):
            if archive is not None and finalize.applied:
                archive.append(("finalize", _lanes(outcome.record.provenance)))
            return finalize

        coordinator.finalize_run = AsyncMock(side_effect=cas)

    with patch.object(BackgroundRegistryStore, "get_instance", return_value=store), \
         patch(USAGE_SERVICE, side_effect=_usage_service(billed)), \
         patch(f"{COORDINATOR}.get_instance", return_value=coordinator), \
         patch(f"{FINALIZATION}.get_token_usage_from_callback", return_value=(None, [])), \
         patch(f"{FINALIZATION}.get_tool_usage_from_handler", return_value={}), \
         patch(f"{FINALIZATION}.calculate_execution_time", return_value=1.0), \
         patch(f"{READER}.get_instance", return_value=_saved_fetches()), \
         _archive_writes(btm, archive):
        yield info


async def _end_run(
    btm: LocalRunExecutor,
    registry: BackgroundTaskRegistry,
    billed: list,
    *,
    teardown: bool,
    user_stop: bool,
    kind: str,
    finalize,
    archive: list | None = None,
) -> LocalRunExecution:
    """Run the stop teardown when the ending has one, then the finalize."""
    with _run(
        btm, registry, billed,
        user_stop=user_stop, finalize=finalize, archive=archive,
    ) as info:
        if teardown:
            info.stop_teardown = StopTeardown()
            await btm._teardown_subagents_on_stop(THREAD, RUN, info.stop_teardown)
        await btm._finalize_run(THREAD, RUN, kind=kind)
        await _settled()
    return info


USER_STOP_ROW = {"status": "cancelled", "metadata": {"cancelled_by_user": True}}

ENDINGS = [
    pytest.param(
        dict(teardown=True, user_stop=True, kind="cancelled",
             finalize=FinalizeResult(True, USER_STOP_ROW)),
        False,
        {"finished", "live"},
        id="user_stop",
    ),
    pytest.param(
        # A /cancel taken on another worker whose nudge never arrived: no
        # teardown ran here, and only the finalize learns of the stop.
        dict(teardown=False, user_stop=False, kind="stream_end",
             finalize=FinalizeResult(True, USER_STOP_ROW)),
        False,
        {"finished", "live"},
        id="user_stop_taken_on_another_worker",
    ),
    pytest.param(
        dict(teardown=True, user_stop=True, kind="cancelled",
             finalize=FinalizeResult(True, USER_STOP_ROW)),
        True,
        {"finished", "live", "unadopted"},
        id="user_stop_with_a_finished_but_unadopted_subagent",
    ),
    pytest.param(
        # The CAS stamps cancel_requested_at for a system cancel too, so only
        # cancelled_by_user says the user stopped the run.
        dict(teardown=True, user_stop=False, kind="cancelled",
             finalize=FinalizeResult(True, {
                 "status": "cancelled",
                 "cancel_requested_at": datetime.now(timezone.utc),
                 "metadata": {"cancelled_by_user": False},
             })),
        False,
        set(),
        id="shutdown",
    ),
    pytest.param(
        dict(teardown=False, user_stop=False, kind="failed",
             finalize=FinalizeResult(True, {"status": "error", "metadata": {}})),
        False,
        set(),
        id="error",
    ),
    pytest.param(
        # Another finalize settled the row first. The loser runs no terminal
        # effect, so the stop's claim is dropped unbilled.
        dict(teardown=True, user_stop=True, kind="cancelled",
             finalize=FinalizeResult(False, USER_STOP_ROW)),
        False,
        set(),
        id="lost_finalize_race",
    ),
    pytest.param(
        dict(teardown=True, user_stop=True, kind="cancelled",
             finalize=RuntimeError("db down")),
        False,
        set(),
        id="finalize_throws",
    ),
]


ENDING = {p.id: p.values[0] for p in ENDINGS}


def _name_of(tasks: dict, task_id: str) -> str:
    return next(name for name, task in tasks.items() if task.task_id == task_id)


class TestTheEndingDecidesBilling:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ending, unadopted, billed_tasks", ENDINGS)
    async def test_billed_rows(self, ending, unadopted, billed_tasks):
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        tasks = await _subagents(registry, unadopted=unadopted)
        billed: list = []

        info = await _end_run(btm, registry, billed, **ending)

        assert sorted(
            (_name_of(tasks, task_id), records, response_id, msg_type)
            for task_id, records, response_id, msg_type in billed
        ) == sorted(
            (name, [{"call": name}], RUN, "task") for name in billed_tasks
        )
        # Whatever the ending, the run's live subagent is killed and a prior
        # turn's task keeps its usage and stays unclaimed.
        assert tasks["live"].cancelled
        assert tasks["prior"].per_call_records == [{"call": "prior"}]
        assert tasks["prior"].collector_response_id is None
        # Nothing of this run is left registered, or for a later collector to
        # bill again.
        assert [t.tool_call_id for t in await registry.get_all_tasks()] == [
            "tc-prior"
        ]
        assert await registry.claim_run_subagents(RUN, "run-later") == []
        if isinstance(ending["finalize"], Exception):
            # The row is still in_progress: the entry stays for recovery.
            assert info.status is LocalRunStatus.RUNNING
        else:
            assert info.stop_teardown is None


def _named(tasks: dict, archive: list) -> list:
    return [(when, {_name_of(tasks, i) for i in ids}) for when, ids in archive]


async def _captured_subagents(registry: BackgroundTaskRegistry, **kwargs) -> dict:
    tasks = await _subagents(registry, **kwargs)
    for task in tasks.values():
        task.captured_event_count = 1
    return tasks


@asynccontextmanager
async def _stopped(btm: LocalRunExecutor, registry, archive: list):
    """A user stop torn down and finalized, its background work left running."""
    with _run(
        btm, registry, [], user_stop=True,
        finalize=FinalizeResult(True, USER_STOP_ROW), archive=archive,
    ) as info:
        info.stop_teardown = StopTeardown()
        await btm._teardown_subagents_on_stop(THREAD, RUN, info.stop_teardown)
        await btm._finalize_run(THREAD, RUN, kind="cancelled")
        yield


class TestEveryEndingWithoutACollectorArchivesItsLanes:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ending, at_finalize, after", [
        # The teardown's drain carries both lanes' provenance into the CAS.
        pytest.param("user_stop", {"finished", "live"}, set(), id="user_stop"),
        pytest.param("shutdown", {"finished", "live"}, set(), id="shutdown"),
        # No teardown ran, so the lanes are archived after the CAS.
        pytest.param(
            "user_stop_taken_on_another_worker", set(), {"finished", "live"},
            id="user_stop_taken_on_another_worker",
        ),
        pytest.param("error", set(), {"finished", "live"}, id="error"),
    ])
    async def test_each_lane_is_archived_once(self, ending, at_finalize, after):
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        tasks = await _captured_subagents(registry)
        archive: list = []

        await _end_run(btm, registry, [], **ENDING[ending], archive=archive)

        assert _named(tasks, archive) == [("finalize", at_finalize)] + (
            [(f"after:{RUN}", after)] if after else []
        )

    @pytest.mark.asyncio
    async def test_a_lane_the_stop_drain_withholds_is_archived_once_its_writer_settles(self):
        """A writer still unwinding when the stop's drain reads can append past
        that read, so the drain withholds its lane. The lane is archived once
        the writer settles instead of expiring with its capture stream."""
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        release = asyncio.Event()
        tasks = await _captured_subagents(registry, on_unwind=release.wait)
        archive: list = []

        with patch(f"{REGISTRY}.CANCEL_UNWIND_TIMEOUT", 0.05):
            async with _stopped(btm, registry, archive):
                await asyncio.sleep(0.05)
                assert _named(tasks, archive) == [("finalize", {"finished"})]
                release.set()
                await _settled()

        assert _named(tasks, archive) == [
            ("finalize", {"finished"}),
            (f"after:{RUN}", {"live"}),
        ]

    @pytest.mark.asyncio
    async def test_a_lane_resumed_before_its_writer_settles_is_left_to_the_resume(self):
        """A resume can take a killed task back while its writer unwinds. The
        resuming run archives that lane, so this run's archive skips it."""
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        release = asyncio.Event()
        tasks = await _captured_subagents(registry, on_unwind=release.wait)
        archive: list = []

        with patch(f"{REGISTRY}.CANCEL_UNWIND_TIMEOUT", 0.05):
            async with _stopped(btm, registry, archive):
                await registry.reclaim_for_resume(
                    tasks["live"], task_run_id=None, spawned_run_id="run-next"
                )
                release.set()
                await _settled()

        assert _named(tasks, archive) == [("finalize", {"finished"})]


class TestTheClaimRidesTheKill:
    @pytest.mark.asyncio
    async def test_a_task_resumed_while_the_kill_unwinds_stays_with_the_resume(self):
        """The claim shares the eviction's lock section, so a task a resume
        steals while the kill waits on unwinding writers is neither claimed
        nor evicted, and keeps the usage its new owner bills."""
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        resumed_writer = asyncio.create_task(asyncio.Event().wait())
        tasks: dict = {}

        async def resume_finished():
            finished = tasks["finished"]
            await registry.reclaim_for_resume(
                finished, task_run_id=finished.task_run_id, spawned_run_id=RUN
            )
            finished.terminal_status = None
            finished.asyncio_task = resumed_writer

        tasks.update(await _subagents(registry, on_unwind=resume_finished))
        try:
            killed = await registry.cancel_run_tasks(
                RUN, force=True, claim_for=RUN
            )
        finally:
            resumed_writer.cancel()

        finished, live = tasks["finished"], tasks["live"]
        assert killed.claimed == [live]
        assert finished.collector_response_id is None
        assert finished.per_call_records == [{"call": "finished"}]
        assert registry.get_by_tool_call_id("tc-finished") is finished
        assert registry.get_by_tool_call_id("tc-live") is None

    @pytest.mark.asyncio
    async def test_a_round_another_run_resumes_mid_kill_is_left_to_that_run(self):
        """A later run can resume a finished task while this run's kill waits
        on an unwinding sibling, and its round can finish before the kill's
        last section. That round is the later run's to settle and collect, so
        the kill neither adopts its writer, claims it nor evicts it."""
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        tasks: dict = {}

        async def resume_finished_in_another_run():
            finished = tasks["finished"]
            await registry.reclaim_for_resume(
                finished, task_run_id=None, spawned_run_id="run-next"
            )
            finished.terminal_status = None

            async def round_two():
                return {"success": True}

            finished.asyncio_task = asyncio.create_task(round_two())
            await finished.asyncio_task

        tasks.update(
            await _subagents(registry, on_unwind=resume_finished_in_another_run)
        )
        killed = await registry.cancel_run_tasks(RUN, force=True, claim_for=RUN)

        finished, live = tasks["finished"], tasks["live"]
        assert killed.claimed == [live]
        assert not finished.completed
        assert finished.collector_response_id is None
        assert registry.get_by_tool_call_id("tc-finished") is finished
        assert await registry.claim_run_subagents("run-next", "run-next") == [
            finished
        ]

    @pytest.mark.asyncio
    async def test_a_late_reaper_leaves_a_round_resumed_after_it_was_scheduled(self):
        """A killed task kept for an unwinding handler is evicted once that
        handler settles, unless a resume started a new round on it first:
        the wrapper is done, so the resume may proceed while the handler
        still unwinds, and the new round is not the reaper's to remove."""
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        task = await _register(registry, "slow")
        task.task_run_id = "tr-slow-1"
        release = asyncio.Event()

        async def handler():
            try:
                await asyncio.Event().wait()
            finally:
                await release.wait()

        async def wrapper():
            with suppress(asyncio.CancelledError):
                await asyncio.Event().wait()

        task.handler_task = asyncio.create_task(handler())
        task.asyncio_task = asyncio.create_task(wrapper())
        await asyncio.sleep(0)
        with patch(f"{REGISTRY}.CANCEL_UNWIND_TIMEOUT", 0.05):
            await registry.cancel_run_tasks(RUN, force=True)
        assert registry.get_by_tool_call_id("tc-slow") is task

        await registry.reclaim_for_resume(
            task, task_run_id="tr-slow-2", spawned_run_id="run-next"
        )
        task.terminal_status = None
        task.handler_task = None
        task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
        release.set()
        await asyncio.sleep(0.01)

        assert registry.get_by_tool_call_id("tc-slow") is task
        task.asyncio_task.cancel()

    @pytest.mark.asyncio
    async def test_a_kill_without_a_claim_claims_nothing(self):
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        tasks = await _subagents(registry)

        killed = await registry.cancel_run_tasks(RUN, force=True)

        assert killed.cancelled == 1
        assert killed.claimed == []
        assert all(t.collector_response_id is None for t in tasks.values())


async def _workflow():
    while True:
        await asyncio.sleep(0.01)
        yield "event"


def _reapers() -> list[asyncio.Task]:
    return [
        t for t in asyncio.all_tasks()
        if t.get_name().startswith("bg-task-late-remove-") and not t.done()
    ]


class TestTheStopBillsWhateverTheTeardownMeets:
    @pytest.mark.asyncio
    async def test_a_second_cancel_mid_teardown_still_bills_the_stop(self):
        """A second cancel on the run's task (a shutdown's force-cancel, the
        abandoned-run reaper) sends the finalize in while the stop's kill
        still waits on an unwinding writer. The claim that kill makes reaches
        the bill anyway, so the user's stop bills each subagent once."""
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        unwinding, release = asyncio.Event(), asyncio.Event()

        async def on_unwind():
            unwinding.set()
            await release.wait()

        tasks = await _subagents(registry, on_unwind=on_unwind)
        billed: list = []

        with _run(
            btm, registry, billed, user_stop=True,
            finalize=FinalizeResult(True, USER_STOP_ROW),
        ) as info, patch.object(btm, "_flush_checkpoint", AsyncMock()):
            info.explicit_cancel = True
            outer = asyncio.create_task(
                btm._run_workflow(THREAD, RUN, _workflow(), asyncio.Event())
            )
            await asyncio.sleep(0.05)
            info.inner_task.cancel()
            await asyncio.wait_for(unwinding.wait(), timeout=1)
            outer.cancel()
            # The finalize runs and the stop bill's own kill waits on the
            # same writer before it is let go.
            await asyncio.sleep(0.05)
            assert billed == []
            release.set()
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(outer, timeout=5)
            await asyncio.sleep(0.05)

        assert sorted(
            (_name_of(tasks, task_id), records, response_id)
            for task_id, records, response_id, _ in billed
        ) == [
            ("finished", [{"call": "finished"}], RUN),
            ("live", [{"call": "live"}], RUN),
        ]
        assert info.stop_teardown is None

    @pytest.mark.asyncio
    async def test_a_retried_cancel_mid_finalize_leaves_the_subagents_to_the_bill(self):
        """A stop taken on another worker reaches this one only through the
        finalize, so no teardown has claimed the subagents. A retried /cancel
        landing while the finalize appends run_end finds the run no longer
        active and runs the safety-net kill, which must wait for the stop
        bill's claim instead of evicting the subagents ahead of it."""
        from src.server.services import cancel_dispatch

        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        tasks = await _subagents(registry)
        billed: list = []
        retries: list[asyncio.Task] = []

        async def run_end_with_a_retried_cancel(*_args, **_kwargs):
            retries.append(
                asyncio.create_task(cancel_dispatch.cancel_workflow(THREAD, RUN))
            )
            await asyncio.sleep(0.05)

        mutations = MagicMock()
        mutations.request_stop = AsyncMock(return_value="none")
        with _run(
            btm, registry, billed, user_stop=False,
            finalize=FinalizeResult(True, USER_STOP_ROW),
        ), patch.object(LocalRunExecutor, "get_instance", return_value=btm), \
             patch.object(
                 btm, "append_run_end_event",
                 AsyncMock(side_effect=run_end_with_a_retried_cancel),
             ), \
             patch(
                 "src.server.database.runs.lifecycle.request_run_cancel",
                 AsyncMock(return_value={"state": "already_terminal"}),
             ), \
             patch(
                 "src.server.database.runs.outbox.list_returned_steers",
                 AsyncMock(return_value=[]),
             ), \
             patch(
                 "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
                 return_value=mutations,
             ):
            await btm._finalize_run(THREAD, RUN, kind="stream_end")
            await asyncio.wait_for(retries[0], timeout=5)
            for _ in range(50):
                if len(billed) == 2:
                    break
                await asyncio.sleep(0.01)

        assert sorted(
            (_name_of(tasks, task_id), records, response_id)
            for task_id, records, response_id, _ in billed
        ) == [
            ("finished", [{"call": "finished"}], RUN),
            ("live", [{"call": "live"}], RUN),
        ]

    @pytest.mark.asyncio
    async def test_a_subagent_unwinding_past_the_kill_is_billed_once_it_settles(self):
        """A killed writer merges its usage only as it settles, so one still
        unwinding when the kill gives up waiting holds nothing yet. The kill
        claims it anyway and the bill waits for what it merges; both kills
        that find it unwinding share one reaper to evict it."""
        btm = _make_btm()
        registry = BackgroundTaskRegistry(thread_id=THREAD)
        release = asyncio.Event()
        slow = await _register(registry, "slow")

        async def writer():
            try:
                await asyncio.Event().wait()
            finally:
                await release.wait()
                slow.per_call_records = [{"call": "slow"}]

        slow.asyncio_task = asyncio.create_task(writer())
        await asyncio.sleep(0)
        billed: list = []

        with patch(f"{REGISTRY}.CANCEL_UNWIND_TIMEOUT", 0.05), _run(
            btm, registry, billed, user_stop=True,
            finalize=FinalizeResult(True, USER_STOP_ROW),
        ) as info:
            info.stop_teardown = StopTeardown()
            await btm._teardown_subagents_on_stop(THREAD, RUN, info.stop_teardown)
            await btm._finalize_run(THREAD, RUN, kind="cancelled")
            assert billed == []
            assert len(_reapers()) == 1
            release.set()
            for _ in range(100):
                if billed and not _reapers():
                    break
                await asyncio.sleep(0.01)

        assert billed == [(slow.task_id, [{"call": "slow"}], RUN, "task")]
        assert registry.get_by_tool_call_id("tc-slow") is None
