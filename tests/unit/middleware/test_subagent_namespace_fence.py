"""Background-subagent namespace fence + cross-worker task meta (v4 2.4e).

Pins the middleware side of the contract: spawn/resume take the task's
namespace through ``namespace_owner`` before any writer starts, the writer's
settle releases it and mirrors terminal liveness to the task meta, and
hydration consults the meta so 'update' routes to the steering list the
remote writer actually consumes.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)


class FakeOwner:
    def __init__(self, grant: bool = True) -> None:
        self.grant = grant
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire_task_ns(self, task_id: str) -> bool:
        self.acquired.append(task_id)
        return self.grant

    async def release_task_ns(self, task_id: str) -> None:
        self.released.append(task_id)


def _request(args: dict, tool_call_id: str = "tc-1") -> SimpleNamespace:
    return SimpleNamespace(
        tool_call={"name": "Task", "id": tool_call_id, "args": args},
        runtime=SimpleNamespace(
            config={"configurable": {"thread_id": "thread-x"}, "run_id": "run-1"}
        ),
    )


async def _ok_handler(_request) -> ToolMessage:
    return ToolMessage(content="done", tool_call_id="tc-1", name="Task")


def _middleware(owner) -> BackgroundSubagentMiddleware:
    # thread_id="" keeps the real meta writer inert; tests patch it anyway.
    registry = BackgroundTaskRegistry(thread_id="")
    return BackgroundSubagentMiddleware(
        registry=registry, enabled=True, namespace_owner=owner
    )


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_init_refused_namespace_spawns_nothing():
    mw = _middleware(FakeOwner(grant=False))
    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )

    assert isinstance(result, ToolMessage)
    assert "could not start" in result.content
    task = mw.registry._tasks["tc-1"]
    assert task.asyncio_task is None  # no writer spawned
    assert task.completed and task.cancelled  # inert: no collector claims it


@pytest.mark.asyncio
async def test_init_acquires_then_releases_at_settle_with_meta_lifecycle():
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )
    assert "deployed" in result.content

    task = mw.registry._tasks["tc-1"]
    assert owner.acquired == [task.task_id]
    await task.asyncio_task  # writer settles

    assert owner.released == [task.task_id]
    statuses = [c.args[1] for c in mw.registry.write_task_meta.await_args_list]
    assert statuses == ["running", "completed"]


@pytest.mark.asyncio
async def test_init_without_owner_keeps_legacy_shape():
    mw = _middleware(owner=None)
    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )
    assert "deployed" in result.content
    await mw.registry._tasks["tc-1"].asyncio_task


# ---------------------------------------------------------------------------
# resume arbitration
# ---------------------------------------------------------------------------


def _seed_task(mw, *, completed: bool, asyncio_task=None) -> BackgroundTask:
    task = BackgroundTask(
        tool_call_id="tc-orig",
        task_id="abc123",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        agent_id="general-purpose:x",
        completed=completed,
        asyncio_task=asyncio_task,
    )
    mw.registry._tasks["tc-orig"] = task
    mw.registry._task_id_to_tool_call_id["abc123"] = "tc-orig"
    return task


@pytest.mark.asyncio
async def test_resume_refuses_locally_live_task_without_touching_fence():
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    live = asyncio.create_task(asyncio.sleep(30))
    _seed_task(mw, completed=False, asyncio_task=live)
    try:
        result = await mw.awrap_tool_call(
            _request(
                {
                    "action": "resume",
                    "task_id": "abc123",
                    "description": "d",
                    "prompt": "p",
                    "subagent_type": "general-purpose",
                }
            ),
            _ok_handler,
        )
        assert "still running" in result.content
        assert owner.acquired == []
    finally:
        live.cancel()


@pytest.mark.asyncio
async def test_resume_refuses_when_namespace_owned_elsewhere():
    """Non-local pending task (hydrated 'running elsewhere'): the namespace
    lock — not registry state — arbitrates, and contended means refuse."""
    owner = FakeOwner(grant=False)
    mw = _middleware(owner)
    _seed_task(mw, completed=False, asyncio_task=None)

    result = await mw.awrap_tool_call(
        _request(
            {
                "action": "resume",
                "task_id": "abc123",
                "description": "d",
                "prompt": "p",
                "subagent_type": "general-purpose",
            }
        ),
        _ok_handler,
    )

    assert "live elsewhere" in result.content
    assert owner.acquired == ["abc123"]


@pytest.mark.asyncio
async def test_resume_proceeds_when_lock_free_despite_stale_running_state():
    """A free namespace means no live writer anywhere — resume must unstick
    a task whose remote owner crashed and left stale 'running' state."""
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()
    task = _seed_task(mw, completed=False, asyncio_task=None)

    result = await mw.awrap_tool_call(
        _request(
            {
                "action": "resume",
                "task_id": "abc123",
                "description": "d",
                "prompt": "p",
                "subagent_type": "general-purpose",
            }
        ),
        _ok_handler,
    )

    assert "Resumed" in result.content
    assert owner.acquired == ["abc123"]
    await task.asyncio_task
    assert owner.released == ["abc123"]


@pytest.mark.asyncio
async def test_resume_without_owner_refuses_pending_as_before():
    mw = _middleware(owner=None)
    _seed_task(mw, completed=False, asyncio_task=None)

    result = await mw.awrap_tool_call(
        _request(
            {
                "action": "resume",
                "task_id": "abc123",
                "description": "d",
                "prompt": "p",
                "subagent_type": "general-purpose",
            }
        ),
        _ok_handler,
    )

    assert "still running" in result.content


# ---------------------------------------------------------------------------
# hydration
# ---------------------------------------------------------------------------


def _checkpointer(metadata: dict | None):
    cp = SimpleNamespace(metadata=metadata) if metadata is not None else None
    return SimpleNamespace(aget_tuple=AsyncMock(return_value=cp))


@pytest.mark.asyncio
async def test_hydrate_running_meta_builds_live_task_with_real_identity():
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=FakeOwner(),
        checkpointer=_checkpointer({"subagent_type": "research"}),
    )
    meta = {
        "tool_call_id": "tc-remote",
        "status": "running",
        "subagent_type": "research",
        "description": "remote work",
        "spawned_run_id": "run-9",
    }
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(return_value=meta),
    ):
        task = await mw._hydrate_from_checkpoint("abc123", "thread-x")

    assert task is not None
    assert task.tool_call_id == "tc-remote"  # update routes to the real list
    assert task.completed is False and task.is_pending
    assert task.spawned_run_id == "run-9"


@pytest.mark.asyncio
async def test_hydrate_running_meta_without_fence_stays_completed():
    """Single-writer deployment: this process is the only writer, so a
    'running' meta is necessarily stale — keep the legacy completed shape."""
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=None,
        checkpointer=_checkpointer({}),
    )
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(return_value={"tool_call_id": "tc-remote", "status": "running"}),
    ):
        task = await mw._hydrate_from_checkpoint("abc123", "thread-x")

    assert task is not None
    assert task.completed is True


@pytest.mark.asyncio
async def test_hydrate_running_meta_survives_missing_checkpoint():
    """A just-spawned remote task may have no checkpoint yet; the meta alone
    must be enough to resolve it."""
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=FakeOwner(),
        checkpointer=_checkpointer(None),
    )
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(
            return_value={"tool_call_id": "tc-remote", "status": "running"}
        ),
    ):
        task = await mw._hydrate_from_checkpoint("abc123", "thread-x")

    assert task is not None and task.is_pending


@pytest.mark.asyncio
async def test_hydrate_no_meta_no_checkpoint_returns_none():
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=FakeOwner(),
        checkpointer=_checkpointer(None),
    )
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(return_value=None),
    ):
        assert await mw._hydrate_from_checkpoint("abc123", "thread-x") is None


def _yielding_checkpointer(metadata: dict | None):
    """aget_tuple that suspends once — exposes the hydration race window."""
    cp = SimpleNamespace(metadata=metadata) if metadata is not None else None

    async def aget_tuple(_config):
        await asyncio.sleep(0)
        return cp

    return SimpleNamespace(aget_tuple=aget_tuple)


_COMPLETED_META = {
    "tool_call_id": "tc-remote",
    "status": "completed",
    "subagent_type": "general-purpose",
    "description": "remote work",
}


@pytest.mark.asyncio
async def test_concurrent_hydrations_publish_one_object():
    """Two resolves of one lost task racing through hydration must converge
    on a single published object — a second insert winning would repoint the
    registry at an inert duplicate while the first object spawns the writer."""
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=FakeOwner(),
        checkpointer=_yielding_checkpointer({}),
    )
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(return_value=dict(_COMPLETED_META)),
    ):
        t1, t2 = await asyncio.gather(
            mw._hydrate_from_checkpoint("abc123", "thread-x"),
            mw._hydrate_from_checkpoint("abc123", "thread-x"),
        )

    assert t1 is not None and t1 is t2
    assert await mw.registry.get_by_task_id("abc123") is t1


@pytest.mark.asyncio
async def test_parallel_resumes_through_hydration_register_the_spawned_writer():
    """Parallel resumes of a task the registry lost (worker restart): both
    hydrate, exactly one spawns — and the registry must point at the SPAWNED
    object. A losing hydration overwriting the mappings would leave drains
    and collectors watching an inert duplicate with no writer, releasing the
    namespace under the live one."""
    owner = FakeOwner(grant=True)
    mw = BackgroundSubagentMiddleware(
        registry=BackgroundTaskRegistry(thread_id="thread-x"),
        enabled=True,
        namespace_owner=owner,
        checkpointer=_yielding_checkpointer({}),
    )
    mw.registry.write_task_meta = AsyncMock()

    args = {
        "action": "resume",
        "task_id": "abc123",
        "description": "d",
        "prompt": "p",
        "subagent_type": "general-purpose",
    }
    with patch(
        "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
        AsyncMock(return_value=dict(_COMPLETED_META)),
    ):
        r1, r2 = await asyncio.gather(
            mw.awrap_tool_call(_request(args, tool_call_id="tc-r1"), _ok_handler),
            mw.awrap_tool_call(_request(args, tool_call_id="tc-r2"), _ok_handler),
        )

    contents = [r1.content, r2.content]
    assert sum("Resumed" in c for c in contents) == 1
    assert owner.acquired == ["abc123"]  # fence touched exactly once
    published = await mw.registry.get_by_task_id("abc123")
    assert published is not None
    assert published.asyncio_task is not None  # registry tracks the writer
    await published.asyncio_task


@pytest.mark.asyncio
async def test_resume_rebinds_run_ownership_to_the_current_run():
    """The resumed writer belongs to THIS run. Stop teardown, tail drain and
    collectors all select by spawned_run_id — left bound to the original
    (long-finalized) spawner, nothing would ever await or account for the
    resumed writer, and this run's guard would release under it."""
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()
    task = _seed_task(mw, completed=True)
    task.spawned_run_id = "run-OLD"

    result = await mw.awrap_tool_call(
        _request(
            {
                "action": "resume",
                "task_id": "abc123",
                "description": "d",
                "prompt": "p",
                "subagent_type": "general-purpose",
            }
        ),
        _ok_handler,
    )

    assert "Resumed" in result.content
    assert task.spawned_run_id == "run-1"  # the resuming turn's run_id
    await task.asyncio_task


@pytest.mark.asyncio
async def test_parallel_resumes_spawn_exactly_one_writer():
    """Two resume calls for one task in one model step: the liveness check
    and the spawn are separated by awaits, so without the synchronous claim
    both would pass and double-spawn writers for one namespace (the session
    fence is idempotent for this run and admits both)."""

    class YieldingOwner(FakeOwner):
        async def acquire_task_ns(self, task_id: str) -> bool:
            await asyncio.sleep(0)  # expose the pre-spawn window
            return await super().acquire_task_ns(task_id)

    owner = YieldingOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()
    task = _seed_task(mw, completed=True)

    args = {
        "action": "resume",
        "task_id": "abc123",
        "description": "d",
        "prompt": "p",
        "subagent_type": "general-purpose",
    }
    r1, r2 = await asyncio.gather(
        mw.awrap_tool_call(_request(args, tool_call_id="tc-r1"), _ok_handler),
        mw.awrap_tool_call(_request(args, tool_call_id="tc-r2"), _ok_handler),
    )

    contents = [r1.content, r2.content]
    assert sum("Resumed" in c for c in contents) == 1
    assert sum("already being resumed" in c for c in contents) == 1
    assert owner.acquired == ["abc123"]  # fence touched exactly once
    await task.asyncio_task
    statuses = [c.args[1] for c in mw.registry.write_task_meta.await_args_list]
    assert statuses == ["running", "completed"]  # one writer lifecycle


@pytest.mark.asyncio
async def test_settle_writes_terminal_meta_before_releasing_the_namespace():
    """Meta writes are namespace-lock-ordered: the terminal write lands while
    N(task:id) is still held, so a successor — who can acquire only after the
    release — always writes its 'running' AFTER this writer's terminal state,
    never under it."""
    events: list[str] = []

    class OrderedOwner(FakeOwner):
        async def release_task_ns(self, task_id: str) -> None:
            events.append("release")
            await super().release_task_ns(task_id)

    owner = OrderedOwner()
    mw = _middleware(owner)

    async def _record_meta(task, status):
        events.append(f"meta:{status}")

    mw.registry.write_task_meta = AsyncMock(side_effect=_record_meta)

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert events == ["meta:running", "meta:completed", "release"]
