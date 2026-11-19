"""A cancel that lands while a turn's stream acquires its computer ends the
turn there, on whichever worker the cancel reaches, instead of once the
provider answers: settled cancelled, closed with the run's own ``run_end``."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.report_back.handoff.carry import PriorAttempt
from src.server.services.runs.cancel import TurnCancelListener
from src.server.services.runs.executor import LocalRunExecutor
from src.server.services.turn_runtime import TurnRoute
from src.server.services.shared_workspaces.turn import TurnIdentity
from tests.unit.server.handlers.test_ptc_run_workspace_status import (
    _make_config,
    _make_request,
)

PTC = "src.server.handlers.chat.ptc_run"
RUN_END = 'event: run_end\ndata: {"status": "cancelled"}\n\n'

pytestmark = pytest.mark.asyncio


def _hung_workspace_manager(*, ready: bool):
    """A provider that never answers."""
    from src.server.services.workspace_manager import WorkspaceManager

    wm = MagicMock(spec=WorkspaceManager)
    wm.has_ready_session = MagicMock(return_value=ready)
    acquiring = asyncio.Event()
    cancelled = asyncio.Event()

    async def _hang(*_args, **_kwargs):
        acquiring.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    wm.get_session_for_workspace = AsyncMock(side_effect=_hang)
    return wm, acquiring, cancelled


async def _turn(wm, signal, *, settled=None, gave_back=None, closing=None, held=None):
    """Drive the stream until it ends; ``signal`` runs once the acquire is
    under way. ``settled`` is the row the settle returns, and ``gave_back``
    stands in for giving the send's file comments back. ``closing`` replaces
    the settle; ``held`` is handed the stream's task, whose cancellation then
    ends the stream with no events while the close runs on. The stream, the
    settle and the closing write it made."""
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    handle = MagicMock(run_id="r-1", turn_index=1, finalized=False)

    async def _begin(request, *, scope, **_kwargs):
        scope.attach_run(handle)
        return handle

    async def _log(thread_id, run_id, last_event_id=None):
        yield RUN_END

    fail_open_run = closing or AsyncMock(
        return_value=MagicMock(run=settled or {"status": "cancelled"})
    )
    coordinator = MagicMock(fail_open_run=fail_open_run)
    run_end = AsyncMock()
    manager = LocalRunExecutor.get_instance()
    with (
        patch(f"{PTC}.setup") as mock_setup,
        patch(f"{PTC}.ensure_thread", new_callable=AsyncMock),
        patch(f"{PTC}._resolve_fork", return_value=("Q", None)),
        patch(f"{PTC}.begin_run", side_effect=_begin),
        patch(f"{PTC}.wait_or_steer", new_callable=AsyncMock, return_value=(True, None)),
        patch("src.server.services.shared_workspaces.turn.get_user_profile_for_prompt", new_callable=AsyncMock, return_value=None),
        patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())),
        patch(f"{PTC}.apply_fetch_override"),
        patch(f"{PTC}.WorkspaceManager") as mock_wm_cls,
        patch(f"{PTC}._fire_and_forget"),
        patch(f"{PTC}.keep_named_model", new=AsyncMock()),
        patch(f"{PTC}.carry.prior_attempt", new=AsyncMock(return_value=PriorAttempt(latest=None, retried=None))),
        patch(f"{PTC}.stream_from_log", _log),
        patch(
            "src.server.services.runs.coordinator.RunCoordinator.get_instance",
            return_value=coordinator,
        ),
        patch.object(manager, "append_run_end_event", run_end),
        patch(
            "src.server.services.artifacts.comments.CommentSendScope.run_never_began",
            gave_back or AsyncMock(),
        ),
    ):
        mock_setup.agent_config = MagicMock()
        mock_wm_cls.get_instance.return_value = wm[0]
        gen = astream_ptc_workflow(
            request=_make_request(),
            thread_id="t-1",
            run_id="r-1",
            user_input="hi",
            user_id="u-1",
            route=TurnRoute("ws-1", "analyst", "u-1"),
            identity=TurnIdentity("u-1", "u-1", "u-1"),
            config=_make_config(),
        )

        async def _collect():
            return [event async for event in gen]

        stream = asyncio.create_task(_collect())
        if held is not None:
            held["stream"] = stream
        await asyncio.wait_for(wm[1].wait(), 5)
        await signal(manager)
        try:
            events = await asyncio.wait_for(stream, 2)
        except asyncio.CancelledError:
            if held is None or not stream.cancelled():
                raise
            events = None
            for _ in range(200):
                if run_end.await_count:
                    break
                await asyncio.sleep(0.01)
    return events, fail_open_run, run_end


async def _local_stop(manager):
    assert await manager.signal_cancel("t-1", "r-1") is True


@pytest.mark.parametrize("ready", [False, True])
async def test_a_cancel_ends_the_wait_and_the_turn_through_its_terminal(ready):
    wm = _hung_workspace_manager(ready=ready)
    events, fail_open_run, run_end = await _turn(wm, _local_stop)
    assert events[-1] == RUN_END
    assert fail_open_run.await_args.kwargs == {"status": "cancelled", "metadata": None}
    run_end.assert_awaited_once_with("t-1", "r-1", "cancelled", stopped_by=None)
    # The provider call is abandoned, and the wait leaves nothing behind.
    assert wm[2].is_set()
    assert LocalRunExecutor.get_instance()._awaiting_computer == {}


async def test_the_close_is_written_when_the_senders_request_goes_during_it():
    """The sender's own Stop drops its read once its cancel is answered,
    which can land while the close is still being written; the watchers'
    closing frame is written all the same."""
    held: dict = {}

    async def _settle(*_args, **_kwargs):
        held["stream"].cancel()
        await asyncio.sleep(0.05)
        return MagicMock(run=USER_STOPPED)

    events, _, run_end = await _turn(
        _hung_workspace_manager(ready=False),
        _local_stop,
        closing=AsyncMock(side_effect=_settle),
        held=held,
    )
    assert events is None
    run_end.assert_awaited_once_with("t-1", "r-1", "cancelled", stopped_by=None)


async def test_a_nudge_from_another_worker_reaches_the_wait():
    async def _nudge(_manager):
        await TurnCancelListener()._handle(
            json.dumps({"thread_id": "t-1", "run_id": "r-1"}).encode()
        )

    events, _, run_end = await _turn(_hung_workspace_manager(ready=False), _nudge)
    assert events[-1] == RUN_END
    run_end.assert_awaited_once()


async def test_a_lost_nudge_is_caught_by_the_reconcile_that_already_runs():
    stamped = AsyncMock(return_value={"r-1"})

    async def _reconcile(_manager):
        with (
            patch(
                "src.server.database.runs.lifecycle.cancel_requested_runs", stamped
            ),
            patch(
                "src.server.services.background_registry_store."
                "BackgroundRegistryStore.get_instance",
                return_value=MagicMock(live_task_runs=AsyncMock(return_value=[])),
            ),
            patch(
                "src.server.database.runs.subagent_runs.cancel_requested_task_runs",
                AsyncMock(return_value=set()),
            ),
        ):
            await TurnCancelListener()._reconcile()

    events, _, _ = await _turn(_hung_workspace_manager(ready=True), _reconcile)
    assert events[-1] == RUN_END
    assert "r-1" in stamped.await_args.args[0]


async def test_a_cancel_for_another_run_leaves_the_wait_alone():
    manager = LocalRunExecutor()
    with manager.awaiting_computer("t-1", "r-1") as stopped:
        assert await manager.signal_cancel("t-1", "r-2") is False
        assert not stopped.is_set()
        assert await manager.uncancelled_runs() == [("t-1", "r-1")]
        assert await manager.signal_cancel("t-1", "r-1") is True
        assert stopped.is_set()
        # Signalled once: the reconcile does not ask about it again.
        assert await manager.uncancelled_runs() == []
    assert manager._awaiting_computer == {}


async def test_with_nothing_waiting_the_reconcile_asks_nothing_more():
    manager = LocalRunExecutor()
    assert await manager.uncancelled_runs() == []


# --- a restart's drain ----------------------------------------------------------


@pytest.fixture
def drain_file(tmp_path, monkeypatch):
    import time

    from src.server.services.runs import restart_drain

    path = tmp_path / "langalpha-drain"
    monkeypatch.setattr(restart_drain, "DRAIN_FILE", str(path))
    monkeypatch.setattr(restart_drain, "_STARTED_AT", time.time() - 1)
    monkeypatch.setattr(restart_drain, "_draining", False)
    monkeypatch.setattr(restart_drain, "_drain_task", None)
    return path


async def _drain(manager):
    """This process's drain, run to its end against ``manager``."""
    from src.server.services.runs import restart_drain

    store = MagicMock(stop_all_for_restart=AsyncMock(return_value=([], set())))
    with (
        patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=store,
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=manager,
        ),
        patch.object(restart_drain, "_stop_turn_starters", AsyncMock()),
    ):
        await restart_drain.begin("test")


# What the settle asks the finalize for when the drain ended the wait; a stop
# already on the row overrides it there (tests/integration/
# test_restart_waiting_turn_db.py).
DRAIN_STOP = {"cancelled_by_user": False, "interrupted_by_restart": True}
# A send's text is on its query row, so the resume can run it again.
RESUMABLE = {
    "status": "cancelled",
    "metadata": {**DRAIN_STOP, "pending_input": {"checkpoint_id": None}},
}
USER_STOPPED = {"status": "cancelled", "metadata": {"cancelled_by_user": True}}
SUSPENDED = {
    "status": "cancelled",
    "metadata": {"cancelled_by_user": False, "stopped_by_suspension": True},
}


@pytest.mark.parametrize("ready", [False, True])
async def test_the_drain_ends_the_wait_and_leaves_the_turn_to_the_next_process(
    drain_file, ready
):
    """The provider may not answer before the process exits, so the turn is
    not left waiting into the shutdown's limit: it settles marked for resume
    and its stream closes with its own run_end."""
    gave_back = AsyncMock()
    wm = _hung_workspace_manager(ready=ready)
    events, fail_open_run, run_end = await _turn(
        wm, _drain, settled=RESUMABLE, gave_back=gave_back
    )
    assert events[-1] == RUN_END
    assert fail_open_run.await_args.kwargs == {
        "status": "cancelled", "metadata": DRAIN_STOP,
    }
    run_end.assert_awaited_once_with("t-1", "r-1", "cancelled", stopped_by=None)
    assert wm[2].is_set()
    # The resume's /retry sends the comments with the message again, so they
    # stay sent rather than waiting to be sent a second time.
    gave_back.assert_not_awaited()
    assert LocalRunExecutor.get_instance()._awaiting_computer == {}


@pytest.mark.parametrize("only_the_client_holds", ["resend", "context"])
async def test_a_drained_wait_the_resume_will_decline_gives_the_comments_back(
    drain_file, only_the_client_holds
):
    """The resume runs only input a row holds whole. A send with more than
    that is left for its client to send again, so its comments wait again
    rather than staying sent with no turn that read them."""
    row = {
        "status": "cancelled",
        "metadata": {**DRAIN_STOP, "pending_input": {only_the_client_holds: True}},
    }
    gave_back = AsyncMock()
    await _turn(
        _hung_workspace_manager(ready=False), _drain, settled=row, gave_back=gave_back
    )
    gave_back.assert_awaited_once()


async def test_a_turn_that_reaches_the_wait_on_a_draining_process_does_not_wait(
    drain_file,
):
    """The drain listed the waits there were when it ran; a turn admitted
    after that, or before the drain file was polled, ends at the wait too."""
    drain_file.touch()

    async def _nothing(_manager):
        pass

    events, fail_open_run, run_end = await _turn(
        _hung_workspace_manager(ready=True), _nothing, settled=RESUMABLE
    )
    assert events[-1] == RUN_END
    assert fail_open_run.await_args.kwargs["metadata"] == DRAIN_STOP
    run_end.assert_awaited_once()


@pytest.mark.parametrize(
    ("stop", "row"),
    [("user", USER_STOPPED), ("suspension", SUSPENDED)],
)
async def test_a_stop_during_the_drain_still_wins_and_gives_the_comments_back(
    drain_file, stop, row
):
    """The settle asks for resume because the process drains; the row the
    finalize returns says the stop won, so nothing resumes the turn and its
    comments wait to be sent again."""
    from src.server.services.cancel_dispatch import cancel_workflow

    request = AsyncMock(
        return_value={"state": "requested", "run": {}, "returned_steers": [], "held_steers": 0}
    )

    async def _stop_then_drain(manager):
        with (
            patch("src.server.database.runs.lifecycle.request_run_cancel", request),
            patch(
                "src.server.services.background_registry_store."
                "BackgroundRegistryStore.get_instance",
                return_value=MagicMock(cancel_run_tasks=AsyncMock()),
            ),
        ):
            await cancel_workflow("t-1", "r-1", by="suspension" if stop == "suspension" else "user")
        await _drain(manager)

    gave_back = AsyncMock()
    events, fail_open_run, run_end = await _turn(
        _hung_workspace_manager(ready=False),
        _stop_then_drain,
        settled=row,
        gave_back=gave_back,
    )
    assert request.await_args.kwargs["by"] == ("suspension" if stop == "suspension" else "user")
    assert events[-1] == RUN_END
    fail_open_run.assert_awaited_once()
    run_end.assert_awaited_once_with(
        "t-1", "r-1", "cancelled", stopped_by="suspension" if stop == "suspension" else None
    )
    gave_back.assert_awaited_once()


async def test_a_wait_stopped_with_no_drain_gives_the_comments_back():
    gave_back = AsyncMock()
    events, _, _ = await _turn(
        _hung_workspace_manager(ready=False),
        _local_stop,
        settled=USER_STOPPED,
        gave_back=gave_back,
    )
    assert events[-1] == RUN_END
    gave_back.assert_awaited_once()


async def test_a_computer_that_arrives_as_the_drain_ends_the_wait_is_stopped_at_registration(
    drain_file,
):
    """The session is taken (``_unless_stopped``), the turn registers on a
    draining process, and ``start_run`` stops it for the next process: one
    stop, through the executor's finalize, and no settle of the wait's."""
    manager = LocalRunExecutor()
    with manager.awaiting_computer("t-1", "r-1") as stopped:
        await _drain(manager)
        assert stopped.is_set()

    async def _workflow():
        yield {}

    async def _run_workflow(*_args, cancel_event, **_kwargs):
        await cancel_event.wait()

    with patch.object(manager, "_run_workflow", _run_workflow):
        info = await manager.start_run("t-1", "r-1", _workflow())
        assert info.explicit_cancel and info.restart and not info.user_stop
        await asyncio.wait_for(info.task, 2)


async def test_a_suspensions_stop_ends_the_wait_as_the_suspensions():
    from src.server.services.cancel_dispatch import cancel_workflow

    request = AsyncMock(
        return_value={"state": "requested", "run": {}, "returned_steers": [], "held_steers": 0}
    )

    async def _suspension_stop(_manager):
        with (
            patch("src.server.database.runs.lifecycle.request_run_cancel", request),
            patch(
                "src.server.services.background_registry_store."
                "BackgroundRegistryStore.get_instance",
                return_value=MagicMock(cancel_run_tasks=AsyncMock()),
            ),
        ):
            outcome = await cancel_workflow("t-1", "r-1", by="suspension")
        assert outcome["cancelled"] is True

    events, fail_open_run, run_end = await _turn(
        _hung_workspace_manager(ready=False), _suspension_stop
    )
    # The stamp names the suspension; the finalize reads who stopped it from
    # the row (tests/integration/test_suspension_stop_db.py).
    assert request.await_args.kwargs == {"thread_id": "t-1", "by": "suspension"}
    assert events[-1] == RUN_END
    assert fail_open_run.await_args.kwargs == {"status": "cancelled", "metadata": None}
    # This finalize returns no stamped row, so the close names no stopper;
    # the drain test above reads it from a stamped one.
    run_end.assert_awaited_once_with("t-1", "r-1", "cancelled", stopped_by=None)


async def test_a_computer_that_arrives_with_the_stop_is_taken():
    """The run then registers, and its registration applies the stop the
    row records, whoever asked (``start_run``'s recheck)."""
    from src.server.handlers.chat.computer_acquire import _unless_stopped

    async def _session():
        return "session"

    acquire = asyncio.create_task(_session())
    await acquire
    stopped = asyncio.Event()
    stopped.set()
    assert await _unless_stopped(acquire, stopped) == "session"
