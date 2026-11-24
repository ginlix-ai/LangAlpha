"""The asset catch-up a turn's acquire hands its sync to, and the gate it
holds on the workspace's calls until the sync ends."""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.asset_gate import AssetGate
from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from ptc_agent.core.sandbox.runtime import SandboxGoneError
from src.server.services.computer_manager import _catch_up
from src.server.services.computer_manager._types import MachineState
from src.server.services.livefs.mount import MountState
from tests.unit.server.services.test_computer_manager import (
    _LIFECYCLE,
    _Base,
    _make_binding,
    _make_manager,
    _make_session,
)

_MCP = "src.server.services.computer_manager._mcp"
_WORKSPACE_MANAGER = "src.server.services.workspace_manager"


def _session():
    return SimpleNamespace(sandbox=SimpleNamespace(assets=AssetGate()))


def _current_binding(workspace_id="ws-a", **overrides):
    return _make_binding(
        workspace_id, layout_version=CURRENT_LAYOUT_VERSION, **overrides
    )


async def _yield(times: int = 5) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


def _blocked_until(event: asyncio.Event, log: list[str] | None = None, name: str = ""):
    async def work():
        if log is not None:
            log.append(f"{name}-start")
        await event.wait()
        if log is not None:
            log.append(f"{name}-end")

    return work


def _returning(value):
    async def work():
        return value

    return work


def _raising(error: BaseException):
    async def work():
        raise error

    return work


def _queue(manager, workspace_id="ws-a", computer_id="comp-1"):
    return list(manager._machine(computer_id).catch_ups.get(workspace_id, ()))


class TestWhetherAnAcquireCatchesUpLater(_Base):
    def test_only_an_acquire_that_asked_on_a_current_layout_catches_up_later(self):
        manager = _make_manager()
        session = _session()

        assert manager._catches_up_later(_current_binding(), session, True) is True
        assert manager._catches_up_later(_current_binding(), session, False) is False
        assert not manager._catches_up_later(
            _make_binding("ws-a", layout_version=CURRENT_LAYOUT_VERSION - 1), session, True
        )
        assert not manager._catches_up_later(
            _make_binding("ws-a", layout_version=None), session, True
        )
        assert not manager._catches_up_later(
            _current_binding(), SimpleNamespace(sandbox=None), True
        )


@pytest.mark.asyncio
class TestACatchUp(_Base):
    async def test_the_gate_is_held_until_the_work_ends(self):
        manager = _make_manager()
        session = _session()
        done = asyncio.Event()

        manager._catch_up_soon(_current_binding(), session, _blocked_until(done), what="t")
        [task] = _queue(manager)
        await _yield()
        assert not session.sandbox.assets.settled("ws-a")

        done.set()
        await task
        assert session.sandbox.assets.settled("ws-a")
        assert manager._machine("comp-1").catch_ups == {}

    async def test_work_that_raises_opens_the_gate(self):
        manager = _make_manager()
        session = _session()

        async def work():
            raise RuntimeError("upload failed")

        manager._catch_up_soon(_current_binding(), session, work, what="t")
        [task] = _queue(manager)
        await task
        assert session.sandbox.assets.settled("ws-a")

    async def test_work_past_the_deadline_is_stopped_and_opens_the_gate(self, monkeypatch):
        monkeypatch.setattr(_catch_up, "CATCH_UP_DEADLINE_S", 0.01)
        manager = _make_manager()
        session = _session()

        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )
        [task] = _queue(manager)
        assert manager._catches_up_later(_current_binding(), session, True)
        await asyncio.wait_for(task, 1)
        assert session.sandbox.assets.settled("ws-a")
        # Slower than the deadline, its work only finishes on an acquire's
        # own path, which has none: every workspace on the computer, until
        # the session goes.
        assert not manager._catches_up_later(_current_binding(), session, True)
        assert not manager._catches_up_later(_current_binding("ws-b"), session, True)
        manager._machine("comp-1").forget_session()
        assert manager._catches_up_later(_current_binding(), session, True)

    async def test_a_timeout_the_work_raises_is_not_the_deadline(self):
        manager = _make_manager()
        session = _session()

        async def work():
            raise TimeoutError("the provider's own")

        manager._catch_up_soon(_current_binding(), session, work, what="t")
        [task] = _queue(manager)
        await task
        assert session.sandbox.assets.settled("ws-a")
        assert manager._catches_up_later(_current_binding(), session, True)

    async def test_a_cancelled_catch_up_opens_the_gate(self):
        manager = _make_manager()
        session = _session()

        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )
        [task] = _queue(manager)
        await _yield()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert session.sandbox.assets.settled("ws-a")

    async def test_two_catch_ups_for_one_workspace_run_in_the_order_handed_over(self):
        manager = _make_manager()
        session = _session()
        first_done = asyncio.Event()
        log: list[str] = []

        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(first_done, log, "first"), what="t"
        )
        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event(), log, "second"), what="t"
        )
        first, second = _queue(manager)
        await _yield()
        assert log == ["first-start"]

        first_done.set()
        await first
        await _yield()
        assert log == ["first-start", "first-end", "second-start"]
        second.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await second

    async def test_a_lost_sandbox_evicts_the_session_it_caught_up_and_opens_the_gate(self):
        manager = _make_manager()
        session = _session()

        async def work():
            raise SandboxGoneError("gone")

        with (
            patch.object(manager, "_cached_session", return_value=session),
            patch.object(manager, "_clear_session", AsyncMock()) as clear,
        ):
            manager._catch_up_soon(_current_binding(), session, work, what="t")
            [task] = _queue(manager)
            await task

        clear.assert_awaited_once_with("comp-1", evict_session=session)
        assert session.sandbox.assets.settled("ws-a")

    async def test_a_lost_sandbox_leaves_a_session_that_replaced_it_alone(self):
        manager = _make_manager()
        session = _session()

        async def work():
            raise SandboxGoneError("gone")

        with (
            patch.object(manager, "_cached_session", return_value=_session()),
            patch.object(manager, "_clear_session", AsyncMock()) as clear,
        ):
            manager._catch_up_soon(_current_binding(), session, work, what="t")
            [task] = _queue(manager)
            await task

        clear.assert_not_awaited()
        assert session.sandbox.assets.settled("ws-a")


@pytest.mark.asyncio
class TestStoppingCatchUps(_Base):
    async def test_forgetting_the_session_cancels_a_catch_up_still_queued(self):
        manager = _make_manager()
        session = _session()
        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )
        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )
        tasks = _queue(manager)
        await _yield()

        manager._machine("comp-1").forget_session()
        await asyncio.wait(tasks, timeout=1)

        assert all(task.cancelled() for task in tasks)
        assert session.sandbox.assets.settled("ws-a")

    async def test_a_delete_stops_its_own_workspaces_catch_ups_only(self):
        manager = _make_manager()
        session = _session()
        for workspace_id in ("ws-a", "ws-b"):
            manager._catch_up_soon(
                _current_binding(workspace_id),
                session,
                _blocked_until(asyncio.Event()),
                what="t",
            )
        [going], [staying] = _queue(manager, "ws-a"), _queue(manager, "ws-b")
        await _yield()

        await manager._stop_catch_ups("comp-1", workspace_id="ws-a")

        assert going.cancelled() and not staying.done()
        await manager._stop_catch_ups("comp-1")
        assert staying.cancelled()

    async def test_a_stop_returns_only_once_its_catch_ups_have_unwound(self):
        manager = _make_manager()
        session = _session()
        unwound: list[str] = []

        async def work():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.005)
                unwound.append("ws-a")
                raise

        manager._catch_up_soon(_current_binding(), session, work, what="t")
        [task] = _queue(manager)
        await _yield()

        await manager._stop_catch_ups("comp-1")

        assert task.cancelled()
        assert unwound == ["ws-a"]
        assert session.sandbox.assets.settled("ws-a")

    async def test_a_catch_up_stopped_before_it_first_ran_still_opens_the_gate(self):
        manager = _make_manager()
        session = _session()
        work = AsyncMock()

        manager._catch_up_soon(_current_binding(), session, work, what="t")
        await manager._stop_catch_ups("comp-1")

        work.assert_not_awaited()
        assert session.sandbox.assets.settled("ws-a")
        assert _queue(manager) == []

    async def test_a_stop_of_a_machine_this_worker_never_saw_does_nothing(self):
        manager = _make_manager()
        await manager._stop_catch_ups("comp-unknown")
        assert manager._machine_if_known("comp-unknown") is None


def test_forgetting_a_session_with_no_catch_ups_needs_no_running_loop():
    MachineState().forget_session()


@pytest.mark.asyncio
class TestAnAcquireThatWaitedOrRetriedIsStillItsTurns(_Base):
    """A turn that met another caller's start or stop comes back through the
    same acquire, which hands its sync over as the first try would have."""

    async def test_after_another_callers_start_the_attach_keeps_the_turns_flag(self):
        manager = _make_manager()
        session = _session()
        manager._wait_for_start_completion = AsyncMock(return_value={"status": "running"})
        manager._attach_running_session = AsyncMock(return_value=(session, True))

        got = await manager._await_in_flight_start(
            _current_binding(),
            user_id="user-1",
            workspace_user_id="user-1",
            on_state_observed=None,
            mark=lambda _phase: None,
            attempt=0,
            assets_later=True,
        )

        assert got is session
        assert manager._attach_running_session.await_args.kwargs["assets_later"] is True

    async def test_after_a_start_that_failed_the_retry_keeps_the_turns_flag(self):
        manager = _make_manager()
        session = _session()
        manager._wait_for_start_completion = AsyncMock(return_value={"status": "stopped"})
        manager._acquire_session = AsyncMock(return_value=session)

        got = await manager._await_in_flight_start(
            _current_binding(),
            user_id="user-1",
            workspace_user_id="user-1",
            on_state_observed=None,
            mark=lambda _phase: None,
            attempt=0,
            assets_later=True,
        )

        assert got is session
        assert manager._acquire_session.await_args.kwargs["assets_later"] is True

    @pytest.mark.parametrize("asked", [True, False])
    async def test_after_another_callers_phase_two_the_sync_is_the_turns_to_hand_over(
        self, asked
    ):
        """Not the owner of the start's second phase, it waits for the owner
        and then syncs for its own workspace: beside its turn when it is one."""
        manager = _make_manager()
        session = _turn_session()
        owner_done = asyncio.Event()
        owner_done.set()
        handed = AsyncMock()
        patches = (
            ("_apply_session_mcp", AsyncMock(return_value=object())),
            ("_freeze_tool_view", MagicMock()),
            ("_cached_session", MagicMock(return_value=session)),
            ("tool_view", MagicMock(return_value=SimpleNamespace(mcp_config_version=1))),
            ("_servers_needing_discovery", MagicMock(return_value=[])),
            ("_kick_mcp_discovery", MagicMock()),
            ("_record_sync", MagicMock()),
            ("_sync_or_catch_up", handed),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            stack.enter_context(patch(
                f"{_LIFECYCLE}.db_get_workspace",
                AsyncMock(return_value={"status": "running", "mcp_config_version": 1}),
            ))
            got = await manager._complete_phase2_sync(
                _current_binding(),
                session,
                workspace_user_id="user-1",
                needs_sync=True,
                needs_deferred_sync=False,
                phase2_owner=False,
                phase2_event=owner_done,
                mark=lambda _phase: None,
                assets_later=asked,
            )

        assert got is session
        assert handed.await_args.kwargs["later"] is asked
        assert handed.await_args.kwargs["what"] == "warm"


@pytest.mark.asyncio
class TestWhetherASyncIsOwed(_Base):
    async def test_only_a_recorded_sync_with_no_catch_up_left_for_its_workspace(self):
        manager = _make_manager()
        session = _session()
        assert not manager._sync_owed("comp-1", "ws-a")

        manager._machine("comp-1").sync_owed.update({"ws-a": object(), "ws-b": object()})
        assert manager._sync_owed("comp-1", "ws-a")

        done = asyncio.Event()
        manager._catch_up_soon(_current_binding(), session, _blocked_until(done), what="t")
        [task] = _queue(manager)
        assert not manager._sync_owed("comp-1", "ws-a")
        assert manager._sync_owed("comp-1", "ws-b")

        done.set()
        await task
        assert manager._sync_owed("comp-1", "ws-a")


@pytest.mark.asyncio
class TestRunningOrHandingOverWork(_Base):
    """Whatever the work, an attach recorded on its strength stands only on
    a result of exactly True, however the work ends and wherever it runs."""

    KEY = ("ws-a", "sandbox-abc")

    def _recorded(self):
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None
        return manager

    async def _run(self, manager, work, *, later, attach_key=KEY, session=None):
        await manager._run_or_catch_up(
            _current_binding(),
            session or _session(),
            work,
            later=later,
            what="t",
            attach_key=attach_key,
        )

    async def test_awaited_work_that_returns_true_keeps_the_attach(self):
        manager = self._recorded()

        await self._run(manager, _returning(True), later=False)

        assert self.KEY in manager._projects_attached
        assert _queue(manager) == []

    @pytest.mark.parametrize("result", [False, None, 1])
    async def test_awaited_work_that_returns_anything_else_withdraws_it(self, result):
        manager = self._recorded()

        await self._run(manager, _returning(result), later=False)

        assert self.KEY not in manager._projects_attached

    async def test_awaited_work_that_raises_withdraws_it(self):
        manager = self._recorded()

        with pytest.raises(RuntimeError):
            await self._run(manager, _raising(RuntimeError("upload failed")), later=False)

        assert self.KEY not in manager._projects_attached

    async def test_awaited_work_that_is_cancelled_withdraws_it(self):
        manager = self._recorded()
        running = asyncio.create_task(
            self._run(manager, _blocked_until(asyncio.Event()), later=False)
        )
        await _yield()
        assert self.KEY in manager._projects_attached

        running.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running

        assert self.KEY not in manager._projects_attached

    async def test_work_with_no_attach_of_its_own_leaves_the_record_alone(self):
        manager = self._recorded()

        await self._run(manager, _returning(False), later=False, attach_key=None)

        assert self.KEY in manager._projects_attached

    async def test_handed_over_work_that_returns_true_keeps_the_attach(self):
        manager = self._recorded()
        done = asyncio.Event()

        async def work():
            await done.wait()
            return True

        await self._run(manager, work, later=True)
        [task] = _queue(manager)
        await _yield()
        assert self.KEY in manager._projects_attached

        done.set()
        await task
        assert self.KEY in manager._projects_attached

    @pytest.mark.parametrize("result", [False, None])
    async def test_handed_over_work_that_returns_anything_else_withdraws_it(self, result):
        manager = self._recorded()

        await self._run(manager, _returning(result), later=True)
        [task] = _queue(manager)
        await task

        assert self.KEY not in manager._projects_attached

    async def test_handed_over_work_that_raises_withdraws_it(self):
        manager = self._recorded()

        await self._run(manager, _raising(RuntimeError("upload failed")), later=True)
        [task] = _queue(manager)
        await task

        assert self.KEY not in manager._projects_attached

    async def test_handed_over_work_past_the_deadline_withdraws_it(self, monkeypatch):
        monkeypatch.setattr(_catch_up, "CATCH_UP_DEADLINE_S", 0.01)
        manager = self._recorded()

        await self._run(manager, _blocked_until(asyncio.Event()), later=True)
        [task] = _queue(manager)
        await asyncio.wait_for(task, 1)

        assert self.KEY not in manager._projects_attached

    async def test_handed_over_work_cancelled_midway_withdraws_it(self):
        manager = self._recorded()
        work = AsyncMock(side_effect=_blocked_until(asyncio.Event()))

        await self._run(manager, work, later=True)
        await _yield()
        work.assert_awaited_once()
        await manager._stop_catch_ups("comp-1")

        assert self.KEY not in manager._projects_attached

    async def test_handed_over_work_cancelled_before_it_ran_withdraws_it(self):
        manager = self._recorded()
        work = AsyncMock(return_value=True)

        await self._run(manager, work, later=True)
        await manager._stop_catch_ups("comp-1")

        work.assert_not_awaited()
        assert self.KEY not in manager._projects_attached


@pytest.mark.asyncio
class TestTheAcquireWaitsOutACatchUpUnlessItAskedNotTo(_Base):
    async def _acquire(self, manager, session, *, assets_later):
        manager._acquire_session = AsyncMock(return_value=session)
        manager.resolve_binding = AsyncMock(return_value=_current_binding())
        manager._ensure_project_attached = AsyncMock()
        task = asyncio.create_task(
            manager.get_session_for_workspace("ws-a", user_id="user-1", assets_later=assets_later)
        )
        await _yield(20)
        return task

    async def test_an_ordinary_acquire_returns_only_once_the_catch_up_ended(self):
        manager = _make_manager()
        session = _session()
        done = asyncio.Event()
        manager._catch_up_soon(_current_binding(), session, _blocked_until(done), what="t")

        acquire = await self._acquire(manager, session, assets_later=False)
        assert not acquire.done()
        assert manager._acquire_session.await_args.kwargs["assets_later"] is False
        assert manager._ensure_project_attached.await_args.kwargs["assets_later"] is False

        done.set()
        assert await asyncio.wait_for(acquire, 1) is session

    async def test_a_turns_acquire_returns_beside_a_running_catch_up(self):
        manager = _make_manager()
        session = _session()
        done = asyncio.Event()
        manager._catch_up_soon(_current_binding(), session, _blocked_until(done), what="t")
        [running] = _queue(manager)

        acquire = await self._acquire(manager, session, assets_later=True)
        assert await asyncio.wait_for(acquire, 1) is session
        assert manager._acquire_session.await_args.kwargs["assets_later"] is True
        assert manager._ensure_project_attached.await_args.kwargs["assets_later"] is True
        assert not running.done()

        done.set()
        await running

    async def test_an_ordinary_acquire_waits_out_its_own_workspaces_catch_up_only(self):
        manager = _make_manager()
        session = _session()
        manager._catch_up_soon(
            _current_binding("ws-b"), session, _blocked_until(asyncio.Event()), what="t"
        )

        acquire = await self._acquire(manager, session, assets_later=False)

        assert await asyncio.wait_for(acquire, 1) is session
        await manager._stop_catch_ups("comp-1")

    async def test_an_ordinary_acquire_waits_to_the_catch_ups_end_not_a_calls_bound(self):
        """Several catch-ups queued outlast the wait a tool call gives up at."""
        manager = _make_manager()
        session = _session()
        session.sandbox.assets = MagicMock(wait=AsyncMock(return_value=0.0))

        acquire = await self._acquire(manager, session, assets_later=False)

        assert await asyncio.wait_for(acquire, 1) is session
        session.sandbox.assets.wait.assert_awaited_once_with("ws-a", timeout=None)

    async def test_a_reach_from_the_computers_root_waits_out_its_catch_ups(self):
        """Home's command there runs on what every workspace on it shares."""
        manager = _make_manager()
        session = _session()
        session.sandbox.runtime = object()
        done = asyncio.Event()
        manager._catch_up_soon(_current_binding(), session, _blocked_until(done), what="t")
        manager.wake_computer = AsyncMock(return_value=session)
        manager._keep_computer_tokens = AsyncMock()
        manager._settle_folders = AsyncMock()
        manager.restore_owed_folders = AsyncMock()
        with (
            patch(f"{_WORKSPACE_MANAGER}.get_computer", AsyncMock(return_value={"layout_version": 0})),
            patch.object(manager, "_binding_from_computer", return_value=_current_binding()),
        ):
            reach = asyncio.create_task(manager.reach_computer("comp-1", user_id="user-1"))
            await _yield(20)
            assert not reach.done()
            manager._keep_computer_tokens.assert_not_awaited()

            done.set()
            assert await asyncio.wait_for(reach, 1) is session

    async def test_a_session_cleaned_under_its_catch_up_is_not_waited_on(self):
        """A session's cleanup lets go of the sandbox before the machine's
        records are dropped; an acquire in between has nothing to wait at."""
        manager = _make_manager()
        session = _session()
        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )
        session.sandbox = None

        acquire = await self._acquire(manager, session, assets_later=False)

        assert await asyncio.wait_for(acquire, 1) is session
        await manager._stop_catch_ups("comp-1")


@pytest.mark.asyncio
class TestTheSyncLinkAndStamp(_Base):
    """The one unit a turn's acquire hands over and every other acquire awaits."""

    KEY = ("ws-a", "sandbox-abc")

    async def _run(self, manager, *, synced, later=True, attach_key=None, **kwargs):
        kwargs.setdefault("owed", object())
        session = SimpleNamespace(sandbox=MagicMock(), skills_signature="old")
        sync = AsyncMock(return_value=synced)
        with (
            patch.object(manager, "_sync_sandbox_assets", sync),
            patch.object(manager, "_reconcile_skills", AsyncMock(return_value=True)) as links,
        ):
            finished = await manager._sync_and_link(
                _current_binding(),
                "user-1",
                session,
                skills_signature="new",
                attach_key=attach_key,
                later=later,
                **kwargs,
            )
        return SimpleNamespace(session=session, sync=sync, links=links, finished=finished)

    async def test_a_sync_that_ran_to_its_end_says_so_stamps_and_is_owed_no_more(self):
        manager = _make_manager()
        owed = manager._machine("comp-1").sync_owed.setdefault("ws-a", object())

        seen = await self._run(
            manager, synced=SimpleNamespace(), link_source=None, owed=owed
        )

        assert seen.finished is True
        assert seen.session.skills_signature == "new"
        assert "ws-a" not in manager._machine("comp-1").sync_owed
        seen.links.assert_not_awaited()

    async def test_a_sync_that_ran_to_its_end_leaves_one_taken_on_since_owed(self):
        """The later one may be for connectors resolved after this one began."""
        manager = _make_manager()
        earlier = manager._machine("comp-1").sync_owed.setdefault("ws-a", object())
        manager._machine("comp-1").sync_owed["ws-a"] = object()

        seen = await self._run(
            manager, synced=SimpleNamespace(), link_source=None, owed=earlier
        )

        assert seen.finished is True
        assert "ws-a" in manager._machine("comp-1").sync_owed

    async def test_a_sync_that_did_not_finish_says_so_and_stays_owed_with_its_signature(self):
        manager = _make_manager()
        owed = manager._machine("comp-1").sync_owed.setdefault("ws-a", object())

        seen = await self._run(manager, synced=None, link_source=None, owed=owed)

        assert seen.finished is False
        assert seen.session.skills_signature == "old"
        assert "ws-a" in manager._machine("comp-1").sync_owed

    @pytest.mark.parametrize("later", [True, False])
    async def test_the_sync_is_told_whether_it_runs_beside_a_turn(self, later):
        manager = _make_manager()

        seen = await self._run(manager, synced=SimpleNamespace(), later=later, link_source=None)

        assert seen.sync.await_args.kwargs["later"] is later

    async def test_the_skill_pass_runs_when_a_source_is_named(self):
        manager = _make_manager()

        seen = await self._run(
            manager, synced=SimpleNamespace(), link_source="warm_skills_changed"
        )

        seen.links.assert_awaited_once_with(
            "ws-a", "user-1", seen.session.sandbox, source="warm_skills_changed", attach_key=None
        )

    @pytest.mark.parametrize("synced", [SimpleNamespace(), None])
    async def test_an_attach_recorded_for_this_work_goes_to_the_pass_either_way(self, synced):
        """Withdrawing it for a sync that did not finish is the caller's."""
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        seen = await self._run(
            manager, synced=synced, link_source="lazy_phase2", attach_key=self.KEY
        )

        assert seen.links.await_args.kwargs["attach_key"] == self.KEY
        assert self.KEY in manager._projects_attached


@pytest.mark.asyncio
class TestASyncOwedFromItsHandOver(_Base):
    KEY = ("ws-a", "sandbox-abc")

    async def _sync(self, manager, *, later, sync, attach_key=None):
        session = SimpleNamespace(
            sandbox=SimpleNamespace(assets=AssetGate()), skills_signature="old"
        )
        marks: list[str] = []
        with (
            patch.object(manager, "_sync_sandbox_assets", sync),
            patch.object(manager, "_reconcile_skills", AsyncMock(return_value=True)),
        ):
            await manager._sync_or_catch_up(
                _current_binding(),
                "user-1",
                session,
                later=later,
                what="t",
                link_source="lazy_phase2",
                skills_signature="new",
                attach_key=attach_key,
                mark=marks.append,
            )
            running = _queue(manager)
            owed_meanwhile = "ws-a" in manager._machine("comp-1").sync_owed
            await asyncio.gather(*running)
        return SimpleNamespace(
            session=session, marks=marks, running=running, owed_meanwhile=owed_meanwhile
        )

    async def test_a_turns_sync_is_owed_until_its_catch_up_finishes_it(self):
        manager = _make_manager()
        sync = AsyncMock(return_value=SimpleNamespace())

        seen = await self._sync(manager, later=True, sync=sync)

        assert len(seen.running) == 1 and seen.owed_meanwhile
        assert sync.await_args.kwargs["later"] is True
        assert seen.marks == ["catch_up_later"]
        assert seen.session.skills_signature == "new"
        assert "ws-a" not in manager._machine("comp-1").sync_owed

    async def test_any_other_sync_is_awaited_and_marks_its_own_phases(self):
        manager = _make_manager()
        sync = AsyncMock(return_value=SimpleNamespace())

        seen = await self._sync(manager, later=False, sync=sync)

        assert seen.running == []
        assert sync.await_args.kwargs["later"] is False
        assert seen.marks == ["asset_sync", "skill_reconcile"]
        assert "ws-a" not in manager._machine("comp-1").sync_owed

    @pytest.mark.parametrize("later", [True, False])
    async def test_a_sync_that_finished_keeps_the_attach_recorded_for_it(self, later):
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        await self._sync(
            manager,
            later=later,
            sync=AsyncMock(return_value=SimpleNamespace()),
            attach_key=self.KEY,
        )

        assert self.KEY in manager._projects_attached

    @pytest.mark.parametrize("later", [True, False])
    async def test_a_sync_that_did_not_finish_stays_owed_and_withdraws_its_attach(self, later):
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        await self._sync(
            manager, later=later, sync=AsyncMock(return_value=None), attach_key=self.KEY
        )

        assert self.KEY not in manager._projects_attached
        assert "ws-a" in manager._machine("comp-1").sync_owed

    async def test_a_sync_cut_short_stays_owed_and_withdraws_its_attach(self):
        """A stop cancels a catch-up; an attach left recorded would have no
        later acquire on this worker redo the sync and the links."""
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        with pytest.raises(asyncio.CancelledError):
            await self._sync(
                manager,
                later=False,
                sync=AsyncMock(side_effect=asyncio.CancelledError),
                attach_key=self.KEY,
            )

        assert self.KEY not in manager._projects_attached
        assert "ws-a" in manager._machine("comp-1").sync_owed


@pytest.mark.asyncio
class TestASyncBesideATurn(_Base):
    async def _sync(self, manager, **kwargs):
        sandbox = MagicMock()

        async def beside(_computer_id, _user_id, _sandbox, sync, **_kwargs):
            return await sync

        with (
            patch.object(manager, "_sync_project_assets", AsyncMock(return_value="synced")) as sync,
            patch.object(manager, "_livefs_beside", AsyncMock(side_effect=beside)) as mount,
        ):
            got = await manager._sync_sandbox_assets(
                _current_binding(), "user-1", sandbox, reusing_sandbox=True, **kwargs
            )
        assert got == "synced"
        # No turn waits for the mount, so every sync starts it beside itself.
        mount.assert_awaited_once()
        return sync

    async def test_it_moves_no_folders(self):
        manager = _make_manager()

        sync = await self._sync(manager, later=True, rebuild_overlay=True)

        assert sync.call_args.kwargs == {
            "rebuild_overlay": True,
            "known_layout_version": CURRENT_LAYOUT_VERSION,
        }

    async def test_any_other_sync_may_move_them(self):
        manager = _make_manager()

        sync = await self._sync(manager)

        assert sync.call_args.kwargs["known_layout_version"] is None


@pytest.mark.asyncio
class TestAnAttachToARunningMachine(_Base):
    @staticmethod
    def _handle():
        session = MagicMock()
        session._initialized = False
        session.initialize = AsyncMock()
        session.sandbox.sandbox_id = "sandbox-live"
        session.sandbox.assets = AssetGate()
        return session

    async def _attach(self, manager, session, *, later):
        workspace = {
            "workspace_id": "ws-new",
            "sandbox_id": "sandbox-live",
            "status": "running",
            "computer_id": "comp-1",
        }
        released = asyncio.Event()

        async def caught_up(*_args, **_kwargs):
            await released.wait()

        sync = AsyncMock()
        catch_up = AsyncMock(side_effect=caught_up)
        with (
            patch.object(manager, "_session_handle", return_value=session),
            patch.object(manager, "_apply_session_platform_secret", AsyncMock()),
            patch.object(manager, "_apply_session_mcp", AsyncMock(return_value=None)),
            patch.object(manager, "_sync_sandbox_assets", sync),
            patch.object(manager, "_reconcile_skills", AsyncMock()),
            patch.object(manager, "_maybe_migrate_sandbox", AsyncMock(return_value=None)),
            patch.object(manager, "_sync_and_link", catch_up),
            patch(f"{_LIFECYCLE}.stamp_running_computer", AsyncMock(return_value=True)),
        ):
            got, did_init = await manager._attach_running_session(
                _current_binding("ws-new", provider_ref="sandbox-live"),
                workspace,
                "user-1",
                None,
                lambda _p: None,
                skills_signature="new",
                assets_later=later,
            )
            running = _queue(manager, "ws-new")
            held = not any(task.done() for task in running) and not session.sandbox.assets.settled("ws-new")
            released.set()
            await asyncio.gather(*running)
        assert got is session and did_init is True
        return sync, catch_up, running, held

    async def test_a_turns_attach_hands_its_asset_sync_to_a_catch_up(self):
        manager = _make_manager()
        session = self._handle()

        sync, catch_up, running, held = await self._attach(manager, session, later=True)

        sync.assert_not_awaited()
        assert len(running) == 1 and held
        assert catch_up.await_args.kwargs == {
            "owed": manager._machine("comp-1").sync_owed["ws-new"],
            "link_source": None,
            "skills_signature": "new",
            "attach_key": None,
            "later": True,
            "mark": _catch_up._no_mark,
        }
        assert session.sandbox.assets.settled("ws-new")
        assert "ws-new" in manager._machine("comp-1").sync_owed

    async def test_an_attach_this_worker_recorded_before_has_the_sync_lay_the_links(self):
        """The attach that follows lays links only when it records the attach,
        and a worker that let go of its session kept the record."""
        manager = _make_manager()
        session = self._handle()
        manager._projects_attached[("ws-new", "sandbox-live")] = _current_binding(
            "ws-new"
        ).files_epoch

        _, catch_up, _, _ = await self._attach(manager, session, later=True)

        assert catch_up.await_args.kwargs["link_source"] == "attach_running"

    async def test_an_attach_that_did_not_ask_syncs_on_its_own_path(self):
        manager = _make_manager()
        session = self._handle()

        sync, catch_up, running, held = await self._attach(manager, session, later=False)

        sync.assert_awaited_once()
        assert running == []
        catch_up.assert_not_awaited()


def _turn_session():
    session = _make_session()
    session.sandbox.assets = AssetGate()
    session.sandbox.ensure_sandbox_ready = AsyncMock()
    session.skills_signature = "old"
    return session


def _held_until(released: asyncio.Event, result=None):
    async def held(*_args, **_kwargs):
        await released.wait()
        return result

    return AsyncMock(side_effect=held)


@pytest.mark.asyncio
class TestAStartOrAWarmSessionOnATurn(_Base):
    """Phase 2 of a turn's acquire: what it still does itself, and the unit it
    hands to a catch-up."""

    KEY = ("ws-a", "sandbox-abc")

    async def _phase2(self, manager, session, *, lazy, later=True, restored=True, mcp=None):
        @asynccontextmanager
        async def heartbeat(_computer_id):
            yield

        released = asyncio.Event()
        unit = _held_until(released, True)
        restore = AsyncMock(return_value=restored)
        earlier = _queue(manager)
        sync = AsyncMock(return_value=object())
        patches = (
            ("_computer_start_heartbeat", heartbeat),
            ("_apply_session_platform_secret", AsyncMock()),
            ("_apply_session_mcp", AsyncMock(return_value=mcp)),
            ("_freeze_tool_view", MagicMock()),
            ("_servers_needing_discovery", MagicMock(return_value=[])),
            ("_kick_mcp_discovery", MagicMock()),
            ("_ensure_workspace_dirs", AsyncMock()),
            ("_maybe_restore_files", restore),
            ("_sync_sandbox_assets", sync),
            ("_sync_and_link", unit),
            ("_record_sync", MagicMock()),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            got = await manager._complete_phase2_sync(
                _current_binding(),
                session,
                workspace_user_id="user-1",
                needs_sync=True,
                needs_deferred_sync=lazy,
                phase2_owner=True,
                phase2_event=None,
                mark=lambda _phase: None,
                skills_signature="new",
                assets_later=later,
            )
            assert got is session
            running = [task for task in _queue(manager) if task not in earlier]
            returned_ahead = bool(running) and not any(task.done() for task in running)
            recorded = self.KEY in manager._projects_attached
            released.set()
            await asyncio.gather(*running)
        sync.assert_not_awaited()
        return SimpleNamespace(
            unit=unit,
            restore=restore,
            running=running,
            returned_ahead=returned_ahead,
            recorded=recorded,
            kept=self.KEY in manager._projects_attached,
        )

    async def test_a_start_restores_the_folder_then_hands_over_sync_and_links(self):
        manager = _make_manager()

        seen = await self._phase2(manager, _turn_session(), lazy=True)

        seen.restore.assert_awaited_once()
        assert seen.recorded and seen.returned_ahead and seen.kept
        assert seen.unit.await_args.kwargs == {
            "owed": manager._machine("comp-1").sync_owed["ws-a"],
            "link_source": "lazy_phase2",
            "skills_signature": "new",
            "attach_key": self.KEY,
            "later": True,
            "mark": _catch_up._no_mark,
        }

    async def test_a_start_whose_restore_is_unfinished_records_no_attach(self):
        manager = _make_manager()

        seen = await self._phase2(manager, _turn_session(), lazy=True, restored=False)

        assert not seen.recorded
        assert seen.unit.await_args.kwargs["attach_key"] is None

    async def test_a_warm_session_with_changed_skills_hands_over_sync_and_links(self):
        manager = _make_manager()

        seen = await self._phase2(manager, _turn_session(), lazy=False)

        seen.restore.assert_not_awaited()
        assert seen.returned_ahead
        assert seen.unit.await_args.kwargs == {
            "owed": manager._machine("comp-1").sync_owed["ws-a"],
            "link_source": "warm_skills_changed",
            "skills_signature": "new",
            "attach_key": None,
            "later": True,
            "mark": _catch_up._no_mark,
        }

    async def test_a_warm_session_with_only_its_connectors_changed_lays_no_links(self):
        manager = _make_manager()
        session = _turn_session()
        session.skills_signature = "new"

        seen = await self._phase2(manager, session, lazy=False, mcp=object())

        assert seen.returned_ahead
        assert seen.unit.await_args.kwargs["link_source"] is None

    async def test_a_sync_this_worker_did_not_see_finish_is_handed_over_again(self):
        """Nothing the acquire compares changed: the connectors' version was
        stamped when they resolved, ahead of the sync that was cut short."""
        manager = _make_manager()
        session = _turn_session()
        session.skills_signature = "new"
        manager._machine("comp-1").sync_owed["ws-a"] = object()

        seen = await self._phase2(manager, session, lazy=False)

        assert seen.returned_ahead
        assert seen.unit.await_args.kwargs["link_source"] is None

    async def test_a_sync_still_queued_from_an_earlier_turn_is_not_handed_over_twice(self):
        manager = _make_manager()
        session = _turn_session()
        session.skills_signature = "new"
        manager._machine("comp-1").sync_owed["ws-a"] = object()
        manager._catch_up_soon(
            _current_binding(), session, _blocked_until(asyncio.Event()), what="t"
        )

        seen = await self._phase2(manager, session, lazy=False)

        assert seen.running == []
        seen.unit.assert_not_awaited()
        await manager._stop_catch_ups("comp-1")

    async def test_a_start_cut_short_before_its_hand_over_records_no_attach(self):
        manager = _make_manager()
        manager._machine("comp-1").pending_lazy_sync = True

        with (
            patch.object(
                manager,
                "_publish_workspace_running_with_current_always_on",
                AsyncMock(side_effect=asyncio.CancelledError),
            ),
            patch.object(manager, "_revert_unpromoted_lazy_start", AsyncMock()),
            pytest.raises(asyncio.CancelledError),
        ):
            await self._phase2(manager, _turn_session(), lazy=True)

        assert self.KEY not in manager._projects_attached
        assert _queue(manager) == []

    async def test_a_warm_session_with_nothing_changed_hands_nothing_over(self):
        manager = _make_manager()
        session = _turn_session()
        session.skills_signature = "new"

        seen = await self._phase2(manager, session, lazy=False)

        assert seen.running == []
        seen.unit.assert_not_awaited()

    async def test_an_acquire_that_did_not_ask_awaits_the_same_unit(self):
        manager = _make_manager()
        session = _turn_session()
        unit = AsyncMock(return_value=True)

        @asynccontextmanager
        async def heartbeat(_computer_id):
            yield

        def mark(_phase):
            pass

        patches = (
            ("_computer_start_heartbeat", heartbeat),
            ("_apply_session_platform_secret", AsyncMock()),
            ("_apply_session_mcp", AsyncMock(return_value=None)),
            ("_freeze_tool_view", MagicMock()),
            ("_sync_and_link", unit),
            ("_record_sync", MagicMock()),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            await manager._complete_phase2_sync(
                _current_binding(),
                session,
                workspace_user_id="user-1",
                needs_sync=True,
                needs_deferred_sync=False,
                phase2_owner=True,
                phase2_event=None,
                mark=mark,
                skills_signature="new",
            )

        assert _queue(manager) == []
        unit.assert_awaited_once()
        assert unit.await_args.kwargs["later"] is False
        assert unit.await_args.kwargs["mark"] is mark
        assert unit.await_args.kwargs["link_source"] == "warm_skills_changed"


@pytest.mark.asyncio
class TestAnAttachOnATurn(_Base):
    KEY = ("ws-a", "sandbox-abc")

    async def _attach(
        self,
        manager,
        session,
        *,
        later,
        overlay,
        links=None,
        restored=True,
        mount_owed=False,
        start_trusted=False,
    ):
        links = links or AsyncMock(return_value=True)
        restore = AsyncMock(return_value=restored)
        keep_mount = AsyncMock()
        browser = AsyncMock()
        hand_ahead = MagicMock()
        patches = (
            ("_keep_browser_settings", browser),
            ("_livefs_start_trusted", MagicMock(return_value=start_trusted)),
            ("_hand_livefs_ahead", hand_ahead),
            ("_ensure_workspace_dirs", AsyncMock()),
            ("_maybe_restore_files", restore),
            ("_load_livefs", AsyncMock()),
            ("_ensure_project_tool_overlay", overlay),
            ("_livefs_owed", MagicMock(return_value=mount_owed)),
            ("_keep_livefs", keep_mount),
            ("_reconcile_skills", links),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            await manager._ensure_project_attached(
                _current_binding(), session, user_id="user-1", assets_later=later
            )
            running = _queue(manager)
            yield_ = SimpleNamespace(
                running=running,
                links=links,
                overlay=overlay,
                restore=restore,
                keep_mount=keep_mount,
                browser=browser,
                hand_ahead=hand_ahead,
            )
            yield_.returned_ahead = bool(running) and not any(t.done() for t in running)
            yield_.finish = lambda: asyncio.gather(*running, return_exceptions=True)
            return yield_, stack.pop_all()

    async def test_a_turns_attach_hands_the_overlay_and_the_links_to_a_catch_up(self):
        manager = _make_manager()
        session = _turn_session()
        released = asyncio.Event()

        seen, patched = await self._attach(
            manager, session, later=True, overlay=_held_until(released, True)
        )
        with patched:
            assert seen.returned_ahead
            assert self.KEY in manager._projects_attached
            seen.links.assert_not_awaited()
            released.set()
            await seen.finish()

        seen.links.assert_awaited_once_with(
            "ws-a", "user-1", session.sandbox, source="attach_project"
        )
        assert self.KEY in manager._projects_attached
        assert seen.overlay.await_args.kwargs["later"] is True

    async def test_a_turn_hands_a_mount_start_it_trusts_to_a_catch_up_of_its_own(self):
        """Behind the attach, in the order handed over, and the sandbox is
        handed the mount ahead of it."""
        manager = _make_manager()
        session = _turn_session()
        released = asyncio.Event()

        seen, patched = await self._attach(
            manager,
            session,
            later=True,
            overlay=_held_until(released, True),
            mount_owed=True,
            start_trusted=True,
        )
        with patched:
            assert seen.returned_ahead
            attach, start = seen.running
            seen.hand_ahead.assert_called_once_with("comp-1", "user-1", session.sandbox, start)
            await _yield()
            seen.keep_mount.assert_not_awaited()
            released.set()
            await seen.finish()

        assert attach.done()
        seen.browser.assert_awaited_once()
        seen.keep_mount.assert_awaited_once_with("comp-1", "user-1", session.sandbox, "ws-a")

    async def test_a_turn_waits_for_a_mount_start_it_does_not_trust(self):
        """The attach is still handed over, but the prompt then says what the
        start answered rather than taking it to serve."""
        manager = _make_manager()
        session = _turn_session()
        released = asyncio.Event()

        seen, patched = await self._attach(
            manager,
            session,
            later=True,
            overlay=_held_until(released, True),
            mount_owed=True,
        )
        with patched:
            assert seen.returned_ahead and len(seen.running) == 1
            seen.keep_mount.assert_awaited_once_with(
                "comp-1", "user-1", session.sandbox, "ws-a"
            )
            seen.hand_ahead.assert_not_called()
            released.set()
            await seen.finish()

    @pytest.mark.parametrize(
        ("case", "trusted"),
        [
            ("due", True),
            ("failed", False),
            ("serving", False),
            ("never-mounts", False),
            ("no-user", False),
        ],
    )
    async def test_a_start_is_trusted_where_one_is_due_and_none_failed(self, case, trusted):
        manager = _make_manager()
        sandbox = SimpleNamespace(
            mounts_livefs=case != "never-mounts", sandbox_id="sandbox-abc", livefs=None
        )
        seen = manager._machine("comp-1").livefs
        if case == "failed":
            seen.saw(sandbox, MountState(False, error=MountError.UNREACHABLE))
        if case == "serving":
            sandbox.livefs = object()
            seen.saw(sandbox, MountState(True))
        user = None if case == "no-user" else "user-1"

        assert manager._livefs_start_trusted("comp-1", user, sandbox) is trusted

    async def test_an_attach_that_did_not_ask_finishes_before_it_returns(self):
        manager = _make_manager()
        session = _turn_session()

        seen, patched = await self._attach(
            manager, session, later=False, overlay=AsyncMock(return_value=True)
        )
        with patched:
            assert seen.running == []
            seen.links.assert_awaited_once()
            seen.browser.assert_awaited_once()
            assert seen.overlay.await_args.kwargs["later"] is False

    async def test_an_attach_cut_short_before_its_work_began_records_nothing(self):
        manager = _make_manager()
        session = _turn_session()
        restore = AsyncMock(return_value=True)

        with (
            patch.object(manager, "_keep_browser_settings", AsyncMock()),
            patch.object(manager, "_ensure_workspace_dirs", AsyncMock()),
            patch.object(manager, "_maybe_restore_files", restore),
            patch.object(
                manager, "_load_livefs", AsyncMock(side_effect=asyncio.CancelledError)
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await manager._ensure_project_attached(
                _current_binding(), session, user_id="user-1", assets_later=True
            )

        restore.assert_not_awaited()
        assert self.KEY not in manager._projects_attached
        assert _queue(manager) == []

    async def test_an_overlay_still_missing_withdraws_the_attach_and_the_mount_is_still_kept(
        self,
    ):
        manager = _make_manager()
        session = _turn_session()

        seen, patched = await self._attach(
            manager,
            session,
            later=False,
            overlay=AsyncMock(return_value=False),
            mount_owed=True,
        )
        with patched:
            pass

        assert seen.running == []
        seen.links.assert_not_awaited()
        assert self.KEY not in manager._projects_attached
        seen.keep_mount.assert_awaited_once_with("comp-1", "user-1", session.sandbox, "ws-a")

    async def test_an_attach_recorded_before_this_acquire_is_neither_restored_nor_relinked(
        self,
    ):
        manager = _make_manager()
        session = _turn_session()
        manager._projects_attached[self.KEY] = None

        seen, patched = await self._attach(
            manager, session, later=False, overlay=AsyncMock(return_value=False)
        )
        with patched:
            pass

        seen.restore.assert_not_awaited()
        seen.links.assert_not_awaited()
        seen.overlay.assert_awaited_once()
        assert self.KEY in manager._projects_attached

    async def test_a_folder_not_restored_drops_an_attach_recorded_on_older_files(self):
        manager = _make_manager()
        session = _turn_session()
        manager._projects_attached[self.KEY] = "epoch-before-a-stop"

        seen, patched = await self._attach(
            manager, session, later=False, overlay=AsyncMock(return_value=True), restored=False
        )
        with patched:
            pass

        seen.restore.assert_awaited_once()
        seen.overlay.assert_awaited_once()
        seen.links.assert_not_awaited()
        assert self.KEY not in manager._projects_attached

    @pytest.mark.parametrize("ran", [False, True])
    async def test_a_stopped_catch_up_withdraws_the_attach_it_recorded(self, ran):
        """A stop cancels the catch-up, which may not have run a line yet.
        Left recorded, no later acquire on this worker would lay the links."""
        manager = _make_manager()
        session = _turn_session()
        overlay = _held_until(asyncio.Event(), True)

        seen, patched = await self._attach(manager, session, later=True, overlay=overlay)
        with patched:
            assert seen.returned_ahead
            if ran:
                await _yield()
            assert overlay.await_count == int(ran)
            await manager._stop_catch_ups("comp-1")

        assert self.KEY not in manager._projects_attached

    @pytest.mark.parametrize("overlay_built", [False, True])
    async def test_an_overlay_or_a_pass_that_did_not_finish_withdraws_it_too(
        self, overlay_built
    ):
        manager = _make_manager()
        session = _turn_session()

        seen, patched = await self._attach(
            manager,
            session,
            later=True,
            overlay=AsyncMock(return_value=overlay_built),
            links=AsyncMock(return_value=False),
        )
        with patched:
            await seen.finish()

        assert self.KEY not in manager._projects_attached


@pytest.mark.asyncio
class TestAnAttachsOverlayAndLinks(_Base):
    KEY = ("ws-a", "sandbox-abc")

    async def _attach(self, manager, *, overlay_built, links, passed=True):
        session = _turn_session()
        overlay = AsyncMock(return_value=overlay_built)
        reconcile = AsyncMock(return_value=passed)
        with (
            patch.object(manager, "_ensure_project_tool_overlay", overlay),
            patch.object(manager, "_reconcile_skills", reconcile),
        ):
            stands = await manager._attach_overlay_and_skills(
                _current_binding(), session, user_id="user-1", links=links
            )
        return stands, reconcile

    @pytest.mark.parametrize("links", [True, False])
    async def test_a_missing_overlay_does_not_stand_and_lays_no_links(self, links):
        manager = _make_manager()

        stands, reconcile = await self._attach(manager, overlay_built=False, links=links)

        assert stands is False
        reconcile.assert_not_awaited()

    async def test_an_attach_not_recorded_by_this_acquire_stands_on_its_overlay(self):
        manager = _make_manager()

        stands, reconcile = await self._attach(manager, overlay_built=True, links=False)

        assert stands is True
        reconcile.assert_not_awaited()

    @pytest.mark.parametrize("passed", [True, False])
    async def test_one_this_acquire_recorded_stands_on_its_skill_pass(self, passed):
        manager = _make_manager()

        stands, reconcile = await self._attach(
            manager, overlay_built=True, links=True, passed=passed
        )

        assert stands is passed
        reconcile.assert_awaited_once()
        assert reconcile.await_args.kwargs == {"source": "attach_project"}

    async def test_it_withdraws_nothing_itself(self):
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        await self._attach(manager, overlay_built=False, links=True)

        assert self.KEY in manager._projects_attached


@pytest.mark.asyncio
class TestASkillPassCutShort(_Base):
    KEY = ("ws-a", "sandbox-abc")

    async def test_a_cancelled_pass_withdraws_the_attach_recorded_on_it(self):
        manager = _make_manager()
        manager._projects_attached[self.KEY] = None

        @asynccontextmanager
        async def held(_workspace_id):
            yield "alpha-1a2b"

        with (
            patch.object(manager, "_held_workspace_folder", held),
            patch(
                f"{_MCP}.reconcile_workspace_skills",
                AsyncMock(side_effect=asyncio.CancelledError),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await manager._reconcile_skills(
                "ws-a", "user-1", MagicMock(), source="lazy_phase2", attach_key=self.KEY
            )

        assert self.KEY not in manager._projects_attached

