"""Regression tests for adversarial findings A2, A7, A8, A4.

Covers:
- A2: Subagent collector filters by ``spawned_run_id`` so prior-turn
  subagents can't get claimed by a later turn's collector.
- A7: ``_await_drain_and_cleanup_tasks`` evicts collected tasks from the
  per-thread registry so the dict doesn't grow unboundedly across turns.
- A8: The claim loop in ``_finalize_run`` (``_spawn_subagent_collector``) holds
  ``bg_registry._lock`` so two concurrent collectors can't both observe
  ``collector_response_id`` is ``None`` for the same task and double-claim.
- A4: The legacy-fallback ``stream_from_log`` path polls WorkflowTracker
  instead of exiting eagerly, so a rolling-deploy reconnect can still
  drain in-flight events.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)


def _make_btm() -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        return BackgroundTaskManager()


def _make_subagent_task(
    *, tool_call_id: str, task_id: str, spawned_run_id: str | None
) -> BackgroundTask:
    """A BackgroundTask shaped enough for the collector claim loop to consider it."""
    t = BackgroundTask(
        tool_call_id=tool_call_id,
        task_id=task_id,
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        spawned_run_id=spawned_run_id,
    )
    # Make it claimable: completed with captured events, no asyncio_task pending.
    t.completed = True
    t.captured_event_count = 1
    return t


# ---------------------------------------------------------------------------
# A2 — collector filters by spawned_run_id
# ---------------------------------------------------------------------------


class TestCollectorFiltersBySpawnedRunId:

    @pytest.mark.asyncio
    async def test_prior_turn_subagents_not_claimed_by_later_turn(self):
        """Subagent registered under run-1 stays unclaimed when run-2's
        collector runs. Prevents the cross-turn event leak."""
        btm = _make_btm()

        # run-2 is the turn whose _finalize_run we are simulating.
        run_id_current = "run-2"
        thread_id = "thread-A"

        # The current turn is in-flight in BTM
        btm.tasks[(thread_id, run_id_current)] = TaskInfo(
            thread_id=thread_id,
            run_id=run_id_current,
            status=TaskStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1", "user_id": "u-1"},
        )

        # Registry holds one prior-turn subagent and one current-turn subagent.
        registry = BackgroundTaskRegistry(thread_id=thread_id)
        prior = _make_subagent_task(
            tool_call_id="tc-prior", task_id="prior1",
            spawned_run_id="run-1",
        )
        current = _make_subagent_task(
            tool_call_id="tc-current", task_id="curr1",
            spawned_run_id=run_id_current,
        )
        registry._tasks["tc-prior"] = prior
        registry._tasks["tc-current"] = current

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)

        # Patch out everything _finalize_run does AFTER the claim loop so
        # the test only exercises the claim logic.
        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch.object(btm, "_release_terminal_refs"), \
             patch.object(btm, "_collect_subagent_results_for_turn",
                          new_callable=AsyncMock) as collect_mock, \
             patch("src.server.services.background_task_manager.release_burst_slot",
                   new_callable=AsyncMock):
            await btm._finalize_run(thread_id, run_id_current, kind="stream_end")
            # The collector is spawned via asyncio.create_task; yield once
            # to let it run.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        # _collect_subagent_results_for_turn was called with ONLY the current task.
        assert collect_mock.await_count == 1
        kwargs = collect_mock.await_args.kwargs
        collected_ids = {t.tool_call_id for t in kwargs["tasks"]}
        assert collected_ids == {"tc-current"}
        # The prior task remains unclaimed and available for its own turn's
        # collector (or the orphan path) to handle.
        assert prior.collector_response_id is None
        assert current.collector_response_id == run_id_current

    @pytest.mark.asyncio
    async def test_unstamped_task_treated_as_compat(self):
        """A task with spawned_run_id=None (registered before the fix
        shipped) is still claimable so in-flight subagents during the
        deploy window don't get orphaned."""
        btm = _make_btm()
        run_id_current = "run-2"
        thread_id = "thread-B"
        btm.tasks[(thread_id, run_id_current)] = TaskInfo(
            thread_id=thread_id,
            run_id=run_id_current,
            status=TaskStatus.RUNNING,
            created_at=datetime.now(),
            metadata={"workspace_id": "ws-1", "user_id": "u-1"},
        )

        registry = BackgroundTaskRegistry(thread_id=thread_id)
        legacy = _make_subagent_task(
            tool_call_id="tc-legacy", task_id="leg1",
            spawned_run_id=None,
        )
        registry._tasks["tc-legacy"] = legacy

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)

        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch.object(btm, "_release_terminal_refs"), \
             patch.object(btm, "_collect_subagent_results_for_turn",
                          new_callable=AsyncMock) as collect_mock, \
             patch("src.server.services.background_task_manager.release_burst_slot",
                   new_callable=AsyncMock):
            await btm._finalize_run(thread_id, run_id_current, kind="stream_end")
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        assert collect_mock.await_count == 1
        collected_ids = {t.tool_call_id for t in collect_mock.await_args.kwargs["tasks"]}
        assert collected_ids == {"tc-legacy"}
        assert legacy.collector_response_id == run_id_current


# ---------------------------------------------------------------------------
# A8 — claim loop holds bg_registry._lock
# ---------------------------------------------------------------------------


class TestClaimLoopHoldsRegistryLock:

    @pytest.mark.asyncio
    async def test_claim_blocks_when_lock_held_elsewhere(self):
        """If a competing coroutine holds bg_registry._lock, _finalize_run's
        collector claim loop can't observe collector_response_id mid-mutation.
        We prove this by holding the lock and timing-out _finalize_run."""
        btm = _make_btm()
        thread_id = "thread-C"
        run_id = "run-C"
        btm.tasks[(thread_id, run_id)] = TaskInfo(
            thread_id=thread_id, run_id=run_id,
            status=TaskStatus.RUNNING, created_at=datetime.now(),
            metadata={"workspace_id": "ws-1", "user_id": "u-1"},
        )

        registry = BackgroundTaskRegistry(thread_id=thread_id)
        registry._tasks["tc-X"] = _make_subagent_task(
            tool_call_id="tc-X", task_id="X", spawned_run_id=run_id,
        )

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)

        async def hold_lock_then_release(release_after: float) -> None:
            async with registry._lock:
                await asyncio.sleep(release_after)

        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch.object(btm, "_release_terminal_refs"), \
             patch.object(btm, "_collect_subagent_results_for_turn",
                          new_callable=AsyncMock) as collect_mock, \
             patch("src.server.services.background_task_manager.release_burst_slot",
                   new_callable=AsyncMock):
            # Start a hog that holds the lock for ~150ms.
            hog = asyncio.create_task(hold_lock_then_release(0.15))
            await asyncio.sleep(0.01)  # let hog acquire

            # _finalize_run must block on the lock and not proceed past
            # the claim until the hog releases. We verify by checking that
            # the task has not been claimed before hog releases.
            mc_task = asyncio.create_task(
                btm._finalize_run(thread_id, run_id, kind="stream_end")
            )
            await asyncio.sleep(0.05)
            task = registry._tasks["tc-X"]
            assert task.collector_response_id is None, (
                "claim happened while lock was held — A8 not enforced"
            )

            await hog
            await mc_task
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        assert task.collector_response_id == run_id
        assert collect_mock.await_count == 1


# ---------------------------------------------------------------------------
# A7 — _await_drain_and_cleanup_tasks evicts from registry
# ---------------------------------------------------------------------------


class TestRegistryEvictionAfterDrain:

    @pytest.mark.asyncio
    async def test_drain_removes_task_from_registry_dict(self):
        """After cleanup, the registry's _tasks dict is empty so long-lived
        threads don't accumulate completed-subagent entries forever."""
        btm = _make_btm()
        thread_id = "thread-D"

        registry = BackgroundTaskRegistry(thread_id=thread_id)
        task_a = _make_subagent_task(
            tool_call_id="tc-a", task_id="a", spawned_run_id="run-1",
        )
        task_a.sse_drain_complete.set()
        task_a.collector_response_id = "resp-1"
        task_b = _make_subagent_task(
            tool_call_id="tc-b", task_id="b", spawned_run_id="run-1",
        )
        task_b.sse_drain_complete.set()
        task_b.collector_response_id = "resp-1"
        registry._tasks["tc-a"] = task_a
        registry._tasks["tc-b"] = task_b
        registry._task_id_to_tool_call_id["a"] = "tc-a"
        registry._task_id_to_tool_call_id["b"] = "tc-b"

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)

        # Stub the cache to a disabled-style object so deletes no-op.
        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=Exception("no cache"),
        ), patch(
            "src.server.services.background_task_manager.get_sse_drain_timeout",
            return_value=0.1,
        ):
            await btm._await_drain_and_cleanup_tasks(
                [task_a, task_b], thread_id, "resp-1"
            )

        assert registry._tasks == {}
        assert registry._task_id_to_tool_call_id == {}

    @pytest.mark.asyncio
    async def test_cleanup_skips_task_stolen_by_a_resume(self):
        """A resume steals the entry back mid-collection (clears the claim,
        installs a live writer). The stale collector's cleanup must not null
        the new writer's handles or evict the entry — the resuming run's
        tail drain and collector depend on both."""
        btm = _make_btm()
        thread_id = "thread-D"

        registry = BackgroundTaskRegistry(thread_id=thread_id)
        task = _make_subagent_task(
            tool_call_id="tc-a", task_id="a", spawned_run_id="run-2",
        )
        task.sse_drain_complete.set()
        # The steal: resume reset cleared the round-1 claim and respawned.
        task.collector_response_id = None
        task.completed = False
        new_writer = asyncio.create_task(asyncio.sleep(30))
        task.asyncio_task = new_writer
        task.per_call_records = [{"model": "m"}]
        registry._tasks["tc-a"] = task
        registry._task_id_to_tool_call_id["a"] = "tc-a"

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)
        cache = MagicMock()
        cache.delete = AsyncMock()

        try:
            with patch(
                "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
                return_value=bg_store,
            ), patch(
                "src.server.services.background_task_manager.get_cache_client",
                return_value=cache,
            ), patch(
                "src.server.services.background_task_manager.get_sse_drain_timeout",
                return_value=0.1,
            ):
                await btm._await_drain_and_cleanup_tasks(
                    [task], thread_id, "resp-1"
                )

            assert task.asyncio_task is new_writer  # handles untouched
            assert task.per_call_records == [{"model": "m"}]  # usage kept
            cache.delete.assert_not_awaited()  # round-2 keys not nuked
            assert registry._tasks.get("tc-a") is task  # not evicted
        finally:
            new_writer.cancel()

    @pytest.mark.asyncio
    async def test_remove_task_if_owned_respects_the_claim(self):
        registry = BackgroundTaskRegistry(thread_id="t")
        task = _make_subagent_task(
            tool_call_id="tc1", task_id="id1", spawned_run_id="r1",
        )
        task.collector_response_id = "resp-1"
        registry._tasks["tc1"] = task
        registry._task_id_to_tool_call_id["id1"] = "tc1"

        assert not await registry.remove_task_if_owned("tc1", "resp-OTHER")
        assert registry._tasks.get("tc1") is task

        assert await registry.remove_task_if_owned("tc1", "resp-1")
        assert "tc1" not in registry._tasks
        assert "id1" not in registry._task_id_to_tool_call_id


class TestRegistryRemoveEntry:

    @pytest.mark.asyncio
    async def test_remove_entry_drops_all_mappings(self):
        """_remove_entry_unlocked evicts the task, its task_id lookup, and results."""
        registry = BackgroundTaskRegistry(thread_id="t")
        task = _make_subagent_task(
            tool_call_id="tc1", task_id="id1", spawned_run_id="r1",
        )
        registry._tasks["tc1"] = task
        registry._task_id_to_tool_call_id["id1"] = "tc1"
        registry._results["tc1"] = {"success": True}

        async with registry._lock:
            registry._remove_entry_unlocked("tc1")

        assert "tc1" not in registry._tasks
        assert "id1" not in registry._task_id_to_tool_call_id
        assert "tc1" not in registry._results

    @pytest.mark.asyncio
    async def test_remove_missing_entry_is_noop(self):
        registry = BackgroundTaskRegistry(thread_id="t")
        async with registry._lock:
            registry._remove_entry_unlocked("does-not-exist")  # must not raise
        assert registry._tasks == {}


# ---------------------------------------------------------------------------
# A4 — run_id-less stream_from_log resolves from the ledger (v4 2.4)
# ---------------------------------------------------------------------------


class TestLedgerFallbackTerminalCheck:
    """A run_id-less reconnect with no in-process TaskInfo resolves the run
    from the ledger and polls the run row for terminality — worker-agnostic:
    a peer's live run keeps the watcher attached, and the terminal row
    (owner finalize or recovery scanner) releases it. No ledger row at all
    means nothing durable to stream."""

    @staticmethod
    def _manager():
        manager = MagicMock()
        manager._find_latest_for_thread = MagicMock(return_value=None)
        manager.task_lock = asyncio.Lock()
        manager.tasks = {}  # no local record → terminal_check asks the ledger
        manager.increment_connection = AsyncMock()
        manager.decrement_connection = AsyncMock()
        return manager

    @pytest.mark.asyncio
    async def test_resolves_run_from_ledger_and_drains_per_run_stream(self):
        from src.server.handlers.chat import stream_from_log as sfl

        cache = MagicMock()
        cache.enabled = True
        cache.client = MagicMock()
        # One batch on the PER-RUN key, then two empty rounds; the terminal
        # ledger row lets the two-empty-round handshake exit.
        cache.client.xread = AsyncMock(side_effect=[
            [(b"workflow:stream:t-leg:run-9", [
                (b"1-0", {b"event": b"id: 1\nevent: x\ndata: a\n\n"}),
            ])],
            [],
            [],
        ])

        with patch.object(sfl, "get_cache_client", return_value=cache), \
             patch.object(sfl.BackgroundTaskManager, "get_instance",
                          return_value=self._manager()), \
             patch("src.server.database.turn_lifecycle.get_active_run",
                   AsyncMock(return_value=None)), \
             patch("src.server.database.turn_lifecycle.get_latest_attempt",
                   AsyncMock(return_value={
                       "conversation_response_id": "run-9",
                       "status": "completed",
                   })), \
             patch("src.server.database.turn_lifecycle.get_run",
                   AsyncMock(return_value={"status": "completed"})) as get_run:
            collected: list[str] = []
            async for event in sfl.stream_from_log("t-leg", run_id=None, last_event_id=None):
                collected.append(event)

        # The event was yielded — we didn't exit before draining — and the
        # XREAD attached to the ledger-resolved per-run key.
        assert any("event: x" in e for e in collected)
        xread_key = next(iter(cache.client.xread.await_args.args[0]))
        assert xread_key == b"workflow:stream:t-leg:run-9"
        assert get_run.await_count >= 1

    @pytest.mark.asyncio
    async def test_in_progress_row_keeps_polling_until_terminal(self):
        """A live ledger row (any worker's) ⇒ terminal_check False, so the
        consumer keeps polling; the row flipping terminal releases it."""
        from src.server.handlers.chat import stream_from_log as sfl

        cache = MagicMock()
        cache.enabled = True
        cache.client = MagicMock()
        cache.client.xread = AsyncMock(side_effect=[[], [], [], []])

        rows = iter([
            {"status": "in_progress"},
            {"status": "in_progress"},
            {"status": "completed"},
            {"status": "completed"},
        ])

        with patch.object(sfl, "get_cache_client", return_value=cache), \
             patch.object(sfl.BackgroundTaskManager, "get_instance",
                          return_value=self._manager()), \
             patch("src.server.database.turn_lifecycle.get_active_run",
                   AsyncMock(return_value={
                       "conversation_response_id": "run-9",
                       "status": "in_progress",
                   })), \
             patch("src.server.database.turn_lifecycle.get_run",
                   AsyncMock(side_effect=lambda _rid: next(rows))) as get_run:
            collected: list[str] = []
            async for event in sfl.stream_from_log("t-leg", run_id=None, last_event_id=None):
                collected.append(event)
                if len(collected) > 10:
                    break  # safety

        # Polled through the live rounds and past the terminal flip.
        assert get_run.await_count >= 3

    @pytest.mark.asyncio
    async def test_no_ledger_row_yields_nothing(self):
        """No TaskInfo + no ledger row: nothing durable names a run, so the
        consumer exits without ever touching Redis (the legacy thread-only
        stream key died with the tracker cutover)."""
        from src.server.handlers.chat import stream_from_log as sfl

        cache = MagicMock()
        cache.enabled = True
        cache.client = MagicMock()
        cache.client.xread = AsyncMock(return_value=[])

        with patch.object(sfl, "get_cache_client", return_value=cache), \
             patch.object(sfl.BackgroundTaskManager, "get_instance",
                          return_value=self._manager()), \
             patch("src.server.database.turn_lifecycle.get_active_run",
                   AsyncMock(return_value=None)), \
             patch("src.server.database.turn_lifecycle.get_latest_attempt",
                   AsyncMock(return_value=None)):
            collected: list[str] = []
            async for event in sfl.stream_from_log("t-leg", run_id=None, last_event_id=None):
                collected.append(event)

        assert collected == []
        cache.client.xread.assert_not_awaited()
