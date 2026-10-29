"""Subagent run-ledger admission + finalize (M3 write path).

Pins the middleware side of the ledger contract: the row is born (via the
injected ``registry.run_ledger``) under the namespace fence BEFORE any spawn
side effect; a rejection or ledger outage refuses the spawn and releases the
fence (fail closed — a run we cannot record is a run we do not start); a
post-admission setup failure finalizes the just-born row instead of
stranding it; and the writer's settle finalizes the run BEFORE the fence
releases, with the outcome mapped from how the handler actually ended.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
    TaskRunRejected,
    TransportLostError,
    UsageTake,
)
from ptc_agent.agent.middleware.background_subagent.subagent import (
    _unknown_type_refusal,
)
from ptc_agent.agent.middleware.credit_gate import CreditStopError
from src.server.contracts.status import CREDIT_STOP_ERROR_TYPE


class FakeOwner:
    def __init__(self, journal: list | None = None) -> None:
        self.acquired: list[str] = []
        self.released: list[str] = []
        self.journal = journal if journal is not None else []

    async def acquire_task_ns(self, task_id: str) -> bool:
        self.acquired.append(task_id)
        return True

    async def release_task_ns(self, task_id: str) -> None:
        self.released.append(task_id)
        self.journal.append(("release", task_id))


class FakeLedger:
    def __init__(
        self,
        admit: str = "run-uuid-1",
        reject: str | None = None,
        infra_down: bool = False,
        journal: list | None = None,
    ) -> None:
        self.admit = admit
        self.reject = reject
        self.infra_down = infra_down
        self.started: list[dict] = []
        self.finalized: list[tuple] = []
        self.delivered: list[str] = []
        self.journal = journal if journal is not None else []

    async def mark_result_delivered(self, task_run_id: str) -> bool:
        self.delivered.append(task_run_id)
        return True

    async def start_task_run(self, **kwargs) -> str:
        self.started.append(kwargs)
        if self.reject:
            raise TaskRunRejected(self.reject)
        if self.infra_down:
            raise RuntimeError("ledger db down")
        return self.admit

    async def finalize_task_run(
        self, task_run_id, status, *, task_id=None, failure=None,
        defer_run_end=False,
    ):
        self.finalized.append((task_run_id, status, failure))
        self.journal.append(("finalize", task_run_id, status))
        return {
            "applied": True,
            "run": {"task_run_id": task_run_id, "status": status},
        }

    async def append_run_end(
        self, task_run_id, *, task_id, outcome, finalized_at=None
    ):
        self.journal.append(("run_end", task_run_id, outcome))


class _Request(SimpleNamespace):
    def override(self, **changes) -> "_Request":
        merged = {**vars(self), **changes}
        return _Request(**merged)


def _request(args: dict, tool_call_id: str = "tc-1") -> _Request:
    return _Request(
        tool_call={"name": "Task", "id": tool_call_id, "args": args},
        runtime=SimpleNamespace(
            config={
                "configurable": {"thread_id": "thread-x"},
                "metadata": {"run_id": "parent-run-1"},
            }
        ),
    )


async def _ok_handler(_request) -> ToolMessage:
    return ToolMessage(content="done", tool_call_id="tc-1", name="Task")


def _middleware(owner, ledger, subagent_models=None) -> BackgroundSubagentMiddleware:
    registry = BackgroundTaskRegistry(thread_id="")
    registry.run_ledger = ledger
    return BackgroundSubagentMiddleware(
        registry=registry,
        enabled=True,
        subagent_types={"general-purpose"},
        namespace_owner=owner,
        subagent_models=subagent_models,
    )


# ---------------------------------------------------------------------------
# init admission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_init_rejected_spawns_nothing_and_releases_fence():
    owner = FakeOwner()
    ledger = FakeLedger(reject="task already has a live run")
    mw = _middleware(owner, ledger)

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )

    assert isinstance(result, ToolMessage)
    assert "task already has a live run" in result.content
    task = mw.registry._tasks["tc-1"]
    assert task.asyncio_task is None
    assert task.terminal_status == "never_started"  # inert: no collector claims it
    assert owner.released == [task.task_id]  # fence not left held
    assert ledger.finalized == []  # nothing was born


@pytest.mark.asyncio
async def test_init_ledger_outage_fails_closed():
    owner = FakeOwner()
    mw = _middleware(owner, FakeLedger(infra_down=True))

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )

    assert "could not be recorded" in result.content
    task = mw.registry._tasks["tc-1"]
    assert task.asyncio_task is None
    assert owner.released == [task.task_id]


@pytest.mark.asyncio
async def test_init_admitted_stamps_identity_and_finalizes_before_release():
    journal: list = []
    owner = FakeOwner(journal=journal)
    ledger = FakeLedger(admit="run-uuid-9", journal=journal)
    mw = _middleware(owner, ledger)
    mw.registry.write_task_meta = AsyncMock()

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )
    task = mw.registry._tasks["tc-1"]
    assert task.task_run_id == "run-uuid-9"
    assert result.additional_kwargs["task_artifact"]["task_run_id"] == "run-uuid-9"
    started = ledger.started[0]
    assert started["cause"] == "init"
    assert started["parent_run_id"] == "parent-run-1"
    assert started["launch_tool_call_id"] == "tc-1"

    await task.asyncio_task  # writer settles
    assert ledger.finalized == [("run-uuid-9", "completed", None)]
    # Terminal CAS lands while the fence is still held.
    assert journal.index(("finalize", "run-uuid-9", "completed")) < journal.index(
        ("release", task.task_id)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "models", [{"general-purpose": {"id": "glm-5.3-cn", "provider": "z-ai-cn"}}, None]
)
async def test_admission_records_the_model_its_type_runs(models):
    """The row is born with the model, so every reader, live or replayed,
    gets the one value; a type on the main agent's model records none."""
    ledger = FakeLedger(admit="run-uuid-9")
    mw = _middleware(FakeOwner(), ledger, subagent_models=models)
    mw.registry.write_task_meta = AsyncMock()

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _ok_handler)
    await mw.registry._tasks["tc-1"].asyncio_task

    expected = (models or {}).get("general-purpose")
    assert ledger.started[0]["model"] == expected


@pytest.mark.asyncio
async def test_writer_error_finalizes_error_with_failure():
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _boom(_request):
        raise ValueError("subagent exploded")

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _boom)
    task = mw.registry._tasks["tc-1"]
    await task.asyncio_task

    (run_id, status, failure) = ledger.finalized[0]
    assert status == "error"
    assert failure == {"error": "subagent exploded", "error_type": "ValueError"}


@pytest.mark.asyncio
async def test_cancelled_flag_maps_to_cancelled_finalize():
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _cancelling_handler(_request):
        mw.registry._tasks["tc-1"].terminal_status = "cancelled"
        return ToolMessage(content="done", tool_call_id="tc-1", name="Task")

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _cancelling_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert ledger.finalized[0][1] == "cancelled"


@pytest.mark.asyncio
async def test_torn_stream_escalates_completed_to_transport_lost():
    """Retention contract: a handler that settles cleanly with the spill
    circuit open must finalize error(transport_lost), never completed —
    the replay archive has holes the consumer cannot detect."""
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _torn_handler(_request):
        mw.registry._tasks["tc-1"].redis_write_failed = True
        return ToolMessage(content="done", tool_call_id="tc-1", name="Task")

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _torn_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    (_run_id, status, failure) = ledger.finalized[0]
    assert status == "error"
    assert failure["error_type"] == "transport_lost"
    assert failure["error"].startswith("transport_lost:")


@pytest.mark.asyncio
async def test_abort_path_normalizes_error_type_spelling():
    """The abort loop raises TransportLostError; the ledger row must carry
    the contract spelling ("transport_lost"), never the class name."""
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _torn(_request):
        raise TransportLostError("transport_lost: spill failed mid-run")

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _torn)
    await mw.registry._tasks["tc-1"].asyncio_task

    (_run_id, status, failure) = ledger.finalized[0]
    assert status == "error"
    assert failure["error_type"] == "transport_lost"


@pytest.mark.asyncio
async def test_a_credit_stop_settles_cancelled_with_the_resume_query_spelling():
    """The gate stop is terminal-neutral, not a failure, and the resume query
    selects rows on exactly this pair. Were the status to drift back to
    "error", ``list_credit_stopped_for_resume`` would match nothing and every
    gate-stopped subagent would be stranded with no visible symptom."""
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _stopped(_request):
        raise CreditStopError("You have run out of credits.")

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _stopped)
    await mw.registry._tasks["tc-1"].asyncio_task

    (_run_id, status, failure) = ledger.finalized[0]
    assert status == "cancelled"  # not "error" — the resume query filters on it
    assert failure["error_type"] == CREDIT_STOP_ERROR_TYPE


@pytest.mark.asyncio
async def test_cancel_wins_over_torn_stream_escalation():
    """A user cancel stays cancelled even when the spill circuit is open —
    transport_lost only replaces a would-be completed."""
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _torn_cancelled_handler(_request):
        task = mw.registry._tasks["tc-1"]
        task.redis_write_failed = True
        task.terminal_status = "cancelled"
        return ToolMessage(content="done", tool_call_id="tc-1", name="Task")

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _torn_cancelled_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert ledger.finalized[0][1] == "cancelled"


@pytest.mark.asyncio
async def test_setup_failure_after_admission_aborts_the_born_run():
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-5")
    mw = _middleware(owner, ledger)
    mw.registry.write_task_meta = AsyncMock(side_effect=RuntimeError("redis gone"))

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )

    assert "setup failed before the subagent spawned" in result.content
    task = mw.registry._tasks["tc-1"]
    assert task.asyncio_task is None
    assert task.terminal_status == "never_started"
    # The admitted row terminates instead of stranding in_progress.
    assert ledger.finalized == [
        (
            "run-uuid-5",
            "error",
            {
                "error": "setup failed before spawn: redis gone",
                "error_type": "RuntimeError",
            },
        )
    ]
    assert owner.released == [task.task_id]
    # That error finalize owes a report-back, but the failure is already in
    # `result.content` above — the launching call reported it synchronously.
    # Unstamped, the executor would open a whole synthetic turn to announce a
    # launch failure the agent was handed in the same breath.
    assert ledger.delivered == ["run-uuid-5"]


@pytest.mark.asyncio
async def test_a_run_that_really_spawned_is_not_pre_stamped():
    """The stamp belongs only to a fate reported synchronously. Stamping a
    live run would silence the notification its writer actually owes.
    """
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-6")
    mw = _middleware(owner, ledger)

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )

    assert ledger.delivered == []


# ---------------------------------------------------------------------------
# resume admission
# ---------------------------------------------------------------------------


async def _register_settled_task(mw) -> object:
    task = await mw.registry.register(
        tool_call_id="tc-old",
        description="orig desc",
        prompt="orig prompt",
        subagent_type="general-purpose",
        asyncio_task=None,
    )
    task.terminal_status = "completed"
    task.task_run_id = "run-uuid-prev"
    return task


@pytest.mark.asyncio
async def test_resume_rejected_before_reset_leaves_task_resumable():
    owner = FakeOwner()
    ledger = FakeLedger(reject="task was already resumed")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    mw._reset_task_for_resume = AsyncMock()

    result = await mw.awrap_tool_call(
        _request(
            {"action": "resume", "task_id": task.task_id, "prompt": "more"},
            tool_call_id="tc-2",
        ),
        _ok_handler,
    )

    assert "task was already resumed" in result.content
    # The v1 streams were NOT destroyed for a resume that never ran.
    mw._reset_task_for_resume.assert_not_awaited()
    assert task.completed and not task.cancelled  # still resumable later
    assert task.task_run_id == "run-uuid-prev"  # identity not clobbered
    assert owner.released == [task.task_id]


@pytest.mark.asyncio
async def test_resume_of_a_type_off_the_roster_is_refused_before_admission():
    """A subagent deleted, renamed or turned off since its task ran. Spawned,
    the Task tool's refusal came after a reply saying the task resumed, and
    after an admitted row and a reset that made it the task's latest run."""
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    mw.subagent_types = frozenset({"general-purpose"})
    task = await _register_settled_task(mw)
    task.subagent_type = "gone-desk"
    mw._reset_task_for_resume = AsyncMock()

    result = await mw.awrap_tool_call(
        _request(
            {"action": "resume", "task_id": task.task_id, "prompt": "more"},
            tool_call_id="tc-2",
        ),
        _ok_handler,
    )

    assert result.status == "error"
    assert result.content == _unknown_type_refusal(
        "gone-desk", sorted(mw.subagent_types), resuming=True
    )
    assert ledger.started == [] and owner.acquired == []
    mw._reset_task_for_resume.assert_not_awaited()
    assert task.completed and task.task_run_id == "run-uuid-prev"

@pytest.mark.asyncio
async def test_resume_setup_failure_leaves_the_task_resumable_not_inert():
    """A resume that dies in setup must not go inert: its durable result is
    still in the ``task:{id}`` checkpoint, so the entry has to stay
    resumable — and marking it never-started would also flip ``result_seen``,
    silently dropping the turn notification for a task the agent never saw."""
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    task.result_seen = False

    async def rebind_only(task, *, task_run_id, spawned_run_id):
        task.task_run_id = task_run_id
        task.spawned_run_id = spawned_run_id

    mw._reset_task_for_resume = AsyncMock(side_effect=rebind_only)
    mw.registry.write_task_meta = AsyncMock(side_effect=RuntimeError("redis gone"))

    result = await mw.awrap_tool_call(
        _request(
            {"action": "resume", "task_id": task.task_id, "prompt": "more"},
            tool_call_id="tc-2",
        ),
        _ok_handler,
    )

    assert "could not resume" in result.content
    assert task.terminal_status == "error"
    assert task.error == "resume setup failed before spawn: redis gone"
    assert task.result_seen is False
    assert task.asyncio_task is None
    # The admitted row terminates rather than stranding in_progress.
    assert ledger.finalized[-1][:2] == ("run-uuid-next", "error")


@pytest.mark.asyncio
async def test_resume_admitted_restamps_identity_and_uses_task_description():
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    mw.registry.write_task_meta = AsyncMock()

    result = await mw.awrap_tool_call(
        _request(
            {"action": "resume", "task_id": task.task_id, "prompt": "more"},
            tool_call_id="tc-2",
        ),
        _ok_handler,
    )

    assert "Resumed" in result.content
    assert task.task_run_id == "run-uuid-next"
    assert result.additional_kwargs["task_artifact"]["task_run_id"] == "run-uuid-next"
    started = ledger.started[0]
    assert started["cause"] == "resume"
    # Model omitted description: the ledger gets the task's real one, not
    # the schema default (args aren't backfilled yet at admission time).
    assert started["description"] == "orig desc"
    await task.asyncio_task
    assert ledger.finalized[-1][:2] == ("run-uuid-next", "completed")


@pytest.mark.asyncio
async def test_resume_bills_an_earlier_runs_round_before_its_own():
    """A resume takes the usage an earlier run's collector owned but had not
    billed, and bills it under that run and the ledger run that spent it
    before the resumed round's writer exists. The task is reminted in the
    same step, so the take never pairs that usage with the new run id."""
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    mw.registry.write_task_meta = AsyncMock()
    task.collector_response_id = "response-prior"
    task.per_call_records = [{"round": 1}]
    billed: list = []

    async def bill(thread_id, take):
        billed.append((thread_id, take, task.asyncio_task, task.task_run_id))

    mw.registry.usage_biller = bill

    await mw.awrap_tool_call(
        _request(
            {"action": "resume", "task_id": task.task_id, "prompt": "more"},
            tool_call_id="tc-2",
        ),
        _ok_handler,
    )
    await task.asyncio_task

    assert billed == [
        (
            mw.registry.thread_id,
            UsageTake(task, [{"round": 1}], {}, "run-uuid-prev", "response-prior"),
            None,
            "run-uuid-next",
        )
    ]
    assert task.collector_response_id is None


# ---------------------------------------------------------------------------
# resume for steers its last run never read: the hand-off
# ---------------------------------------------------------------------------


async def _steer_resume(mw, task, hand_off):
    """Resume ``task`` the way SteeringMiddleware does for unread steers."""
    from ptc_agent.agent.middleware.background_subagent.context import (
        current_steer_resume,
    )

    token = current_steer_resume.set(
        {"task_id": task.task_id, "delivered": {"count": 1}, "hand_off": hand_off}
    )
    try:
        return await mw.awrap_tool_call(
            _request(
                {"action": "resume", "task_id": task.task_id, "prompt": "more"},
                tool_call_id="tc-2",
            ),
            _ok_handler,
        )
    finally:
        current_steer_resume.reset(token)


@pytest.mark.asyncio
async def test_a_steer_resume_hands_its_steers_to_the_admitted_run_before_spawning():
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    mw.registry.write_task_meta = AsyncMock()
    handed: list = []

    async def hand_off(task_run_id):
        # The run is admitted and rebound, and no writer exists yet.
        handed.append((task_run_id, task.task_run_id, task.asyncio_task))
        return True

    result = await _steer_resume(mw, task, hand_off)

    assert "Resumed" in result.content
    assert handed == [("run-uuid-next", "run-uuid-next", None)]
    await task.asyncio_task
    assert ledger.finalized[-1][:2] == ("run-uuid-next", "completed")


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", ["moved_nothing", "raised"])
async def test_a_failed_hand_off_aborts_the_admitted_run_and_refuses(fails):
    """The launching run still holds the steers, so the refusal gives them
    back; the admitted run never spawns and terminates rather than strand
    in_progress, and the task stays resumable."""
    owner = FakeOwner()
    ledger = FakeLedger(admit="run-uuid-next")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    mw.registry.write_task_meta = AsyncMock()

    async def hand_off(task_run_id):
        if fails == "raised":
            raise RuntimeError("db down")
        return False

    result = await _steer_resume(mw, task, hand_off)

    assert result.status == "error"
    assert "could not resume" in result.content
    assert task.asyncio_task is None
    # Only the abort's terminal meta: the spawn never wrote "running".
    assert "running" not in [c.args[1] for c in mw.registry.write_task_meta.await_args_list]
    assert ledger.finalized[-1][:2] == ("run-uuid-next", "error")
    assert task.terminal_status == "error"
    assert not task.cancelled


# ---------------------------------------------------------------------------
# record stamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_captured_records_carry_task_run_identity():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await registry.register(
        tool_call_id="tc-1",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        asyncio_task=None,
    )
    task.task_run_id = "run-uuid-3"
    registry._spill_records_to_redis = AsyncMock()

    await registry.append_captured_event(
        "tc-1", {"event": "message_chunk", "data": {"content": "hi"}}
    )

    (record,) = registry._spill_records_to_redis.await_args.args[1]
    assert record["task_run"] == "run-uuid-3"


# ---------------------------------------------------------------------------
# M6-A: run_end resequenced past the returned steering (CAS -> returned -> run_end)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_end_appends_after_the_returned_steering(monkeypatch):
    """steering_returned frames must precede the terminal frame, and
    nothing may follow run_end, so the wrapper defers it past them."""
    journal: list = []
    ledger = FakeLedger(admit="run-uuid-9", journal=journal)
    mw = _middleware(FakeOwner(journal=journal), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _returned(_registry, _task, _run_row):
        journal.append(("returned",))

    from ptc_agent.agent.middleware.background_subagent import run_executor

    monkeypatch.setattr(run_executor, "_emit_returned_steering", _returned)

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _ok_handler)
    await mw.registry._tasks["tc-1"].asyncio_task

    assert journal.index(("finalize", "run-uuid-9", "completed")) < journal.index(
        ("returned",)
    )
    assert journal.index(("returned",)) < journal.index(
        ("run_end", "run-uuid-9", "completed")
    )


@pytest.mark.asyncio
async def test_run_end_skipped_when_the_stream_is_torn(monkeypatch):
    """An open circuit means the v2 stream has a hole readers can't detect —
    run_end must not seal it as complete; the ledger probe + replay resolve."""
    journal: list = []
    ledger = FakeLedger(journal=journal)
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _torn_handler(_request):
        mw.registry._tasks["tc-1"].redis_write_failed = True
        return ToolMessage(content="done", tool_call_id="tc-1", name="Task")

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _torn_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert ledger.finalized  # the CAS still lands (error/transport_lost)
    assert not any(entry[0] == "run_end" for entry in journal)
