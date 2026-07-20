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


def _request(
    args: dict, tool_call_id: str = "tc-1", config: dict | None = None
) -> SimpleNamespace:
    # Production shape: LangChain strips the top-level run_id from child
    # configs by the tool-call layer — only metadata carries it. Tests that
    # exercise the top-level fallback pass their own config.
    if config is None:
        config = {
            "configurable": {"thread_id": "thread-x"},
            "metadata": {"run_id": "run-1"},
        }
    return SimpleNamespace(
        tool_call={"name": "Task", "id": tool_call_id, "args": args},
        runtime=SimpleNamespace(config=config),
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
        "task_run_id": "tr-remote-1",
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
    # The run fence travels with the hydration: without it, a cross-worker
    # update would enqueue on the legacy task-lifetime queue with
    # expected_task_run_id=null — unfenced against a later resume.
    assert task.task_run_id == "tr-remote-1"


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
async def test_resume_rebinds_from_top_level_run_id_fallback():
    """No metadata (non-server invocation): the top-level key still works."""
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
            },
            config={"configurable": {"thread_id": "thread-x"}, "run_id": "run-top"},
        ),
        _ok_handler,
    )

    assert "Resumed" in result.content
    assert task.spawned_run_id == "run-top"
    await task.asyncio_task


@pytest.mark.asyncio
async def test_resume_rebinds_from_registry_when_config_has_no_run_id():
    """Config carries no run_id anywhere: the registry stamp (set by the
    workflow before graph execution) is the last resort — the stale spawner
    id must never survive a resume."""
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()
    mw.registry.current_run_id = "run-registry"
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
            },
            config={"configurable": {"thread_id": "thread-x"}},
        ),
        _ok_handler,
    )

    assert "Resumed" in result.content
    assert task.spawned_run_id == "run-registry"
    await task.asyncio_task


@pytest.mark.asyncio
async def test_resume_clears_spent_result_seen():
    """result_seen is per-round: left set from the prior round, the CLI
    orchestrator would treat the resumed round's completion as already
    announced and never nudge the agent to fetch it."""
    owner = FakeOwner(grant=True)
    mw = _middleware(owner)
    mw.registry.write_task_meta = AsyncMock()
    task = _seed_task(mw, completed=True)
    task.result_seen = True

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
    assert task.result_seen is False
    await task.asyncio_task


@pytest.mark.asyncio
async def test_resume_steals_claim_before_redis_deletes():
    """The collector claim is stolen (and membership restored) BEFORE the
    awaited Redis deletes: a stale collector racing the reset can no longer
    pass its ownership fence mid-delete and evict the entry the resumed
    writer is about to spawn onto. The deletes themselves run under the
    task's spill lock so an in-flight cleanup delete can't erase them."""
    owner = FakeOwner(grant=True)
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    mw = BackgroundSubagentMiddleware(
        registry=registry, enabled=True, namespace_owner=owner
    )
    mw.registry.write_task_meta = AsyncMock()
    task = _seed_task(mw, completed=True)
    task.collector_response_id = "run-old"

    evictions: list[bool] = []
    locked_during_delete: list[bool] = []

    class FakeCache:
        enabled = True

        async def delete(self, key: str) -> None:
            locked_during_delete.append(task.redis_spill_lock.locked())
            # Simulate the stale collector's in-flight pass landing mid-reset:
            # with the steal already done, its ownership fence must refuse.
            evictions.append(
                await registry.remove_task_if_owned("tc-orig", "run-old")
            )

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=FakeCache()
    ):
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
    assert evictions == [False, False, False]  # fence refused every attempt
    assert locked_during_delete == [True, True, True]
    assert registry._tasks.get("tc-orig") is task  # membership survived
    assert task.collector_response_id is None
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

    async def _record_meta(task, status, *, fenced=True):
        events.append(f"meta:{status}")

    mw.registry.write_task_meta = AsyncMock(side_effect=_record_meta)

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p"}), _ok_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert events == ["meta:running", "meta:completed", "release"]


# ---------------------------------------------------------------------------
# follow-up queue push-then-verify
# ---------------------------------------------------------------------------


def _fake_cache():
    client = SimpleNamespace(
        rpush=AsyncMock(), expire=AsyncMock(), lrem=AsyncMock()
    )
    return SimpleNamespace(enabled=True, client=client)


def _live_task(task_run_id: str = "run-9") -> BackgroundTask:
    return BackgroundTask(
        tool_call_id="tc-9",
        task_id="abc123",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        task_run_id=task_run_id,
    )


@pytest.mark.asyncio
async def test_followup_reclaimed_when_run_settles_mid_push():
    """The terminal sweep drains the queue exactly once, ordered AFTER the
    terminal meta write. A follow-up whose post-push meta read shows the run
    settled may have landed behind that sweep — reclaim it and report
    failure instead of acknowledging input nobody will ever read."""
    mw = _middleware(FakeOwner())
    cache = _fake_cache()

    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache,
        ),
        patch(
            "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
            AsyncMock(return_value={"status": "completed"}),
        ),
    ):
        input_id = await mw._queue_followup_to_redis(_live_task(), "more")

    assert input_id is None
    cache.client.lrem.assert_awaited_once()


@pytest.mark.asyncio
async def test_followup_stands_while_meta_still_running():
    """A post-push read that still says "running" FOR THIS RUN proves the
    sweep hadn't started at push time — the entry is either delivered or
    collected by the sweep, so the acknowledgement is honest."""
    mw = _middleware(FakeOwner())
    cache = _fake_cache()

    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache,
        ),
        patch(
            "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
            AsyncMock(
                return_value={"status": "running", "task_run_id": "run-9"}
            ),
        ),
    ):
        input_id = await mw._queue_followup_to_redis(_live_task(), "more")

    assert input_id is not None
    cache.client.rpush.assert_awaited_once()
    cache.client.lrem.assert_not_awaited()


@pytest.mark.asyncio
async def test_followup_reclaimed_when_epoch_rotates_mid_push():
    """Meta says "running" but for a different task_run_id: R1's queue has
    had its one sweep — a run-scoped entry there is unreachable forever, so
    it must be reclaimed even though the task is nominally live."""
    mw = _middleware(FakeOwner())
    cache = _fake_cache()

    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache,
        ),
        patch(
            "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
            AsyncMock(
                return_value={"status": "running", "task_run_id": "run-10"}
            ),
        ),
    ):
        input_id = await mw._queue_followup_to_redis(
            _live_task("run-9"), "more"
        )

    assert input_id is None
    cache.client.lrem.assert_awaited_once()


@pytest.mark.asyncio
async def test_followup_lapsed_meta_falls_back_to_the_injected_ledger():
    """Meta absent: the injected run ledger is the durable authority. A
    terminal latest run means the sweep is behind us — reclaim."""
    mw = _middleware(FakeOwner())
    cache = _fake_cache()
    mw.registry.run_ledger = SimpleNamespace(
        get_latest_run=AsyncMock(
            return_value={"status": "completed", "task_run_id": "run-9"}
        )
    )

    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache,
        ),
        patch(
            "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
            AsyncMock(return_value=None),
        ),
    ):
        input_id = await mw._queue_followup_to_redis(
            _live_task("run-9"), "more"
        )

    assert input_id is None
    cache.client.lrem.assert_awaited_once()


@pytest.mark.asyncio
async def test_followup_fails_open_when_no_authority_is_readable():
    """Meta absent and no ledger injected: the arbitration must never be
    worse than the admission — keep the accepted push."""
    mw = _middleware(FakeOwner())
    cache = _fake_cache()

    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache,
        ),
        patch(
            "ptc_agent.agent.middleware.background_subagent.registry.read_task_meta",
            AsyncMock(return_value=None),
        ),
    ):
        input_id = await mw._queue_followup_to_redis(
            _live_task("run-9"), "more"
        )

    assert input_id is not None
    cache.client.lrem.assert_not_awaited()


# ---------------------------------------------------------------------------
# terminal sweep: read -> surface -> delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_deletes_only_after_every_entry_is_surfaced():
    """The queue must outlive its own archival: DEL runs after the appends,
    so a crash or spill failure between the two leaves acknowledged input
    recoverable in Redis instead of silently destroyed."""
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        _return_unconsumed_steering,
    )

    order: list[str] = []
    payload = '{"content": "c", "expected_task_run_id": "run-9", "input_id": "i1"}'
    task = _live_task("run-9")

    def _surface(*_a, **_k):
        # Mirror the real append: seq advances before the spill — the
        # sweep's landed-check reads it to decide the DEL is safe.
        task.captured_event_seq += 1
        order.append("surface")

    client = SimpleNamespace(
        lrange=AsyncMock(
            side_effect=lambda *a: order.append("read") or [payload]
        ),
        delete=AsyncMock(side_effect=lambda *a: order.append("delete")),
    )
    cache = SimpleNamespace(enabled=True, client=client)
    registry = SimpleNamespace(
        append_event_for_task=AsyncMock(side_effect=_surface)
    )

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        await _return_unconsumed_steering(registry, task)

    assert order == ["read", "surface", "delete"]
    # Identity-exact: the sweep passes the task OBJECT, never re-resolves
    # by tool_call_id (the entry may be evicted or the id reused).
    assert registry.append_event_for_task.await_args.args[0] is task


@pytest.mark.asyncio
async def test_sweep_keeps_the_queue_when_the_spill_tears_mid_sweep():
    """An append that opens the write circuit means the entries never made
    the archive — the DEL is skipped so they survive to their TTL as the
    durable record of what was lost."""
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        _return_unconsumed_steering,
    )

    payload = '{"content": "c", "expected_task_run_id": "run-9", "input_id": "i1"}'
    task = _live_task("run-9")

    async def _torn_append(*_a, **_k):
        task.redis_write_failed = True

    client = SimpleNamespace(
        lrange=AsyncMock(return_value=[payload]),
        delete=AsyncMock(),
    )
    cache = SimpleNamespace(enabled=True, client=client)
    registry = SimpleNamespace(append_event_for_task=AsyncMock(side_effect=_torn_append))

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        await _return_unconsumed_steering(registry, task)

    client.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_withholds_delete_when_appends_do_not_land():
    """Landed-check backstop: if the appends fail to advance the seq
    counter (whatever the cause), the frames never reached the archive —
    the DEL must be withheld so the queue survives as the TTL record of
    acknowledged input."""
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        _return_unconsumed_steering,
    )

    payload = '{"content": "c", "expected_task_run_id": "run-9", "input_id": "i1"}'
    task = _live_task("run-9")
    client = SimpleNamespace(
        lrange=AsyncMock(return_value=[payload]),
        delete=AsyncMock(),
    )
    cache = SimpleNamespace(enabled=True, client=client)
    # An append that returns without advancing the seq counter.
    registry = SimpleNamespace(append_event_for_task=AsyncMock())

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        await _return_unconsumed_steering(registry, task)

    client.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_skips_entirely_when_the_circuit_is_already_open():
    """With a torn transport, appends would no-op against the circuit and
    the delete would erase unsurfaced input — leave everything to TTL."""
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        _return_unconsumed_steering,
    )

    task = _live_task("run-9")
    task.redis_write_failed = True
    client = SimpleNamespace(lrange=AsyncMock(), delete=AsyncMock())
    cache = SimpleNamespace(enabled=True, client=client)
    registry = SimpleNamespace(append_event_for_task=AsyncMock())

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        await _return_unconsumed_steering(registry, task)

    client.lrange.assert_not_awaited()
    client.delete.assert_not_awaited()
    registry.append_event_for_task.assert_not_awaited()
