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
)


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
        self.journal = journal if journal is not None else []

    async def start_task_run(self, **kwargs) -> str:
        self.started.append(kwargs)
        if self.reject:
            raise TaskRunRejected(self.reject)
        if self.infra_down:
            raise RuntimeError("ledger db down")
        return self.admit

    async def finalize_task_run(
        self, task_run_id, status, *, task_id=None, failure=None
    ):
        self.finalized.append((task_run_id, status, failure))
        self.journal.append(("finalize", task_run_id, status))
        return {
            "applied": True,
            "run": {"task_run_id": task_run_id, "status": status},
        }

    async def mark_result_delivered(self, task_run_id) -> bool:
        return True


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


def _middleware(owner, ledger) -> BackgroundSubagentMiddleware:
    registry = BackgroundTaskRegistry(thread_id="")
    registry.run_ledger = ledger
    return BackgroundSubagentMiddleware(
        registry=registry, enabled=True, namespace_owner=owner
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
    assert task.completed and task.cancelled  # inert: no collector claims it
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
        mw.registry._tasks["tc-1"].cancelled = True
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
async def test_cancel_wins_over_torn_stream_escalation():
    """A user cancel stays cancelled even when the spill circuit is open —
    transport_lost only replaces a would-be completed."""
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()

    async def _torn_cancelled_handler(_request):
        task = mw.registry._tasks["tc-1"]
        task.redis_write_failed = True
        task.cancelled = True
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
    assert task.completed and task.cancelled
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
    task.completed = True
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
    registry._spill_record_to_redis = AsyncMock()

    await registry.append_captured_event(
        "tc-1", {"event": "message_chunk", "data": {"content": "hi"}}
    )

    record = registry._spill_record_to_redis.await_args.args[1]
    assert record["task_run"] == "run-uuid-3"
