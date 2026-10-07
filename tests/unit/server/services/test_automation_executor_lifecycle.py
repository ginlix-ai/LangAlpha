"""How a firing moves through its turn: admission, the line for a busy thread,
and each way it ends.

Locks the executor's side of the settlement contract: a turn that loses its
thread before admission gets back in line and runs as the automation now
stands; a wait ends skipped when the automation was paused or the server
stops, and survives a failed look; once its run reached the ledger, the
firing is that run's to settle (``automation_settlement``), however the drain
here ends.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.home_workspace import get_flash_workspace_id
from src.server.database.runs.lifecycle import RunSlotBusyError
from src.server.handlers.chat.admission_gate import (
    ADMISSION_CONFLICT_CODES,
    admission_conflict_detail,
)
from src.server.services import automation_delivery
from src.server.services import automation_executor as executor_mod
from src.server.services.automation_delivery import Delivery, Target
from src.server.services.automation_executor import AutomationExecutor
from src.server.services.automation_settlement import INTERRUPTED_ERROR
from src.server.services.runs.admission import BUSY_STATES
from src.server.services.writer_guard import WriterGuardUnavailable

_MOD = "src.server.services.automation_executor"
_USER = "user-1"
_EXEC = "exec-1"


@pytest.mark.parametrize("state", sorted(BUSY_STATES))
def test_every_busy_admission_answer_sends_the_firing_back_to_wait(state):
    busy = HTTPException(status_code=409, detail=admission_conflict_detail(state))
    assert state in ADMISSION_CONFLICT_CODES
    assert executor_mod._lost_thread_race(busy)


def test_a_steer_probes_refusal_is_not_a_busy_thread():
    refused = HTTPException(
        status_code=409, detail=admission_conflict_detail("not_running")
    )
    assert not executor_mod._lost_thread_race(refused)


def _automation(**overrides):
    return {
        "automation_id": "auto-1",
        "user_id": _USER,
        "name": "",
        "agent_mode": "flash",
        "instruction": "Summarize the market",
        "trigger_type": "cron",
        "workspace_id": None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "llm_model": None,
        "additional_context": None,
        "trigger_config": None,
        "status": "active",
        **overrides,
    }


def _run(run_id, status="completed", **metadata):
    return {"conversation_response_id": run_id, "status": status, "metadata": metadata}


@contextmanager
def _firing(turns, *, runs=None, busy=(), fresh=None, is_byok=(False,)):
    """Patch everything a firing touches.

    ``turns`` stand in for successive astream calls. ``runs`` maps a turn's
    index to what the ledger says of its run (in progress by default).
    ``busy`` is what successive idle checks of the thread answer.
    """
    runs = runs or {}
    db = MagicMock()
    db.transition_execution = AsyncMock(return_value={"conversation_thread_id": None})
    db.settle_execution = AsyncMock(
        return_value={
            "conversation_thread_id": None,
            "conversation_response_id": None,
            "settled_from": "running",
        }
    )
    db.record_delivery = AsyncMock()
    db.update_automation = AsyncMock()
    db.get_execution_status = AsyncMock(return_value="waiting")
    db.has_earlier_waiting_execution = AsyncMock(return_value=False)
    db.get_automation = AsyncMock(return_value=fresh)
    run_ids: list[str] = []

    def astream(**kwargs):
        run_ids.append(kwargs["run_id"])
        return turns[len(run_ids) - 1](**kwargs)

    async def get_run(run_id):
        index = run_ids.index(run_id)
        run = runs.get(index, "in_progress")
        if run is None or isinstance(run, dict):
            return run
        return _run(run_id, run)

    busy_answers = iter(busy)
    fx = SimpleNamespace(
        db=db,
        run_ids=run_ids,
        credit=AsyncMock(),
        byok=AsyncMock(side_effect=list(is_byok) * 4),
        started=AsyncMock(return_value=None),
        settled_webhook=AsyncMock(return_value=None),
        announce=AsyncMock(),
        astream=MagicMock(side_effect=astream),
    )
    with (
        patch(f"{_MOD}.auto_db", new=db),
        patch(f"{_MOD}.exec_db", new=db),
        patch("src.server.services.automation_settlement.auto_db", new=db),
        patch("src.server.services.automation_settlement.exec_db", new=db),
        patch(f"{_MOD}.is_byok_active", new=fx.byok),
        patch(f"{_MOD}.has_any_oauth_token", new=AsyncMock(return_value=False)),
        patch(f"{_MOD}.enforce_credit_limit", new=fx.credit),
        patch(
            "src.server.services.turn_runtime.home_enabled",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "src.server.database.workspace.get_or_create_flash_workspace",
            new=AsyncMock(return_value={"workspace_id": "ws-1"}),
        ),
        patch(f"{_MOD}.WebhookClient", return_value=MagicMock(fire_event=fx.started)),
        patch(
            "src.server.services.automation_settlement.WebhookClient",
            return_value=MagicMock(fire_event=fx.settled_webhook),
        ),
        patch(f"{_MOD}.publish_automation_wait", new=fx.announce),
        patch("src.server.services.automation_settlement.publish_automation_wait", new=AsyncMock()),
        patch(f"{_MOD}._thread_busy", new=AsyncMock(side_effect=lambda _: next(busy_answers, False))),
        patch(f"{_MOD}._WAIT_POLL_SECONDS", new=0),
        patch("src.server.database.runs.lifecycle.get_run", new=AsyncMock(side_effect=get_run)),
        patch("src.server.handlers.chat.astream_flash_workflow", new=fx.astream),
        patch("src.server.handlers.chat.astream_ptc_workflow", new=fx.astream),
    ):
        yield fx


async def _streams(**_):
    yield "event: message_chunk\ndata: {}\n\n"


def _loses(exc):
    async def turn(**_):
        raise exc
        yield  # an async generator, like the workflows

    return turn


def _settled(fx):
    return fx.db.settle_execution.await_args.kwargs


def _left_to_run(fx):
    """The run the firing was left to, which settles nothing here."""
    fx.db.settle_execution.assert_not_awaited()
    linked = [
        c.kwargs["conversation_response_id"]
        for c in fx.db.transition_execution.await_args_list
        if c.kwargs.get("conversation_response_id")
    ]
    assert len(set(linked)) == 1
    return linked[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lost",
    [
        RunSlotBusyError("thread-1"),
        HTTPException(status_code=409, detail=admission_conflict_detail("running")),
    ],
    ids=["run_slot", "busy_409"],
)
async def test_a_turn_that_loses_its_thread_runs_as_the_automation_now_stands(lost):
    fresh = _automation(instruction="Summarize the week")
    with _firing([_loses(lost), _streams], fresh=fresh, is_byok=(False, True)) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert fx.astream.call_count == 2
    second = fx.astream.call_args_list[1].kwargs
    assert second["user_input"] == "Summarize the week"
    # Credentials and the credit gate are read again after the wait.
    assert second["is_byok"] is True
    assert fx.credit.await_count == 2
    waited = [c.kwargs["to"] for c in fx.db.transition_execution.call_args_list]
    assert "waiting" in waited
    assert _left_to_run(fx) == fx.run_ids[1]


@pytest.mark.asyncio
async def test_a_wait_ends_skipped_when_the_automation_was_paused():
    lost = HTTPException(status_code=409, detail=admission_conflict_detail("running"))
    with _firing([_loses(lost)], fresh=_automation(status="paused")) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert fx.astream.call_count == 1
    assert _settled(fx)["to"] == "skipped"
    assert _settled(fx)["skip_reason"] == "user"
    assert _settled(fx)["from_statuses"] == ("waiting",)


@pytest.mark.asyncio
async def test_a_manual_run_of_a_paused_automation_runs_after_its_wait():
    paused = _automation(status="paused")
    lost = HTTPException(status_code=409, detail=admission_conflict_detail("running"))
    with _firing([_loses(lost), _streams], fresh=paused) as fx:
        await AutomationExecutor().execute(paused, _EXEC)

    assert fx.astream.call_count == 2
    assert _left_to_run(fx) == fx.run_ids[1]


@pytest.mark.asyncio
async def test_a_failed_look_keeps_the_firing_waiting():
    pinned = _automation(thread_strategy="continue", conversation_thread_id="thread-1")
    with _firing([_streams], busy=(True,), fresh=pinned) as fx:
        fx.db.get_execution_status.side_effect = [RuntimeError("db blip"), "waiting"]
        await AutomationExecutor().execute(pinned, _EXEC)

    assert fx.astream.call_count == 1
    assert _left_to_run(fx) == fx.run_ids[0]


@pytest.mark.asyncio
async def test_shutdown_ends_a_wait_promptly():
    pinned = _automation(thread_strategy="continue", conversation_thread_id="thread-1")
    executor = AutomationExecutor()
    executor.stop_waiting()
    with _firing([_streams], busy=(True,) * 100, fresh=pinned) as fx:
        with patch(f"{_MOD}._WAIT_POLL_SECONDS", new=60):
            await asyncio.wait_for(executor.execute(pinned, _EXEC), timeout=5)

    fx.astream.assert_not_called()
    assert _settled(fx)["to"] == "skipped"
    assert _settled(fx)["skip_reason"] == "interrupted"


@pytest.mark.asyncio
async def test_a_turn_stopped_before_its_first_event_is_left_to_its_run():
    """Seen only cancelled at admission, and still the firing's own turn:
    its run settles it as the stop it was."""
    stopped = _run("run-1", "cancelled", cancelled_by_user=True)
    with _firing([_streams], runs={0: stopped}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]
    fx.started.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("run", ["completed", "error"])
async def test_a_run_already_over_at_admission_is_not_announced(run):
    """Its terminal notice may already be out from the settle job, and a
    start behind it would leave a channel showing a run that ended."""
    with _firing([_streams], runs={0: run}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]
    fx.started.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unavailable_writer_guard_is_ours():
    with _firing([_loses(WriterGuardUnavailable("budget"))], runs={0: None}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _settled(fx)["to"] == "failed"
    assert _settled(fx)["strike"] is None
    # Never admitted, so no run is named.
    assert _settled(fx)["conversation_response_id"] is None
    fx.started.assert_not_awaited()


def _breaks_after_admission(**_):
    async def turn():
        yield "event: message_chunk\ndata: {}\n\n"
        raise ConnectionError("stream read reset")

    return turn()


@pytest.mark.asyncio
@pytest.mark.parametrize("run", ["in_progress", "completed", "error"])
async def test_a_drain_that_breaks_after_admission_ends_as_the_run_does(run):
    """The run lives in the run executor: losing its stream is this task's
    failure, never a strike of its own."""
    with _firing([_breaks_after_admission], runs={0: run}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]


def _held_turn(*, admitted: bool):
    """A turn that never ends, after or before its run was admitted."""

    async def turn(**_):
        if admitted:
            yield "event: message_chunk\ndata: {}\n\n"
        await asyncio.Event().wait()
        yield "never"

    return turn


@pytest.mark.asyncio
async def test_a_stop_after_admission_leaves_the_firing_to_its_run():
    with _firing([_held_turn(admitted=True)]) as fx:
        task = asyncio.create_task(AutomationExecutor().execute(_automation(), _EXEC))
        while not fx.started.await_count:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _left_to_run(fx) == fx.run_ids[0]


@pytest.mark.asyncio
async def test_a_stop_before_admission_interrupts_the_firing():
    with _firing([_held_turn(admitted=False)], runs={0: None}) as fx:
        task = asyncio.create_task(AutomationExecutor().execute(_automation(), _EXEC))
        while not fx.astream.call_count:
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _settled(fx)["to"] == "failed"
    assert _settled(fx)["error_message"] == INTERRUPTED_ERROR
    assert _settled(fx)["conversation_response_id"] is None


# ─── Delivery through the messaging service ──────────────────────────

_HELD = Delivery(_EXEC, [Target(entry="slack:T/C", address="slack:T/C", name="#demo", ok=True)])
# A start the service didn't take, which leaves the run to the webhook.
_LEFT = Delivery(
    _EXEC,
    [Target(entry="slack:T/C", address=None, name=None, ok=False, message="Not linked")],
)
_DELIVERING = {"delivery_config": {"methods": ["slack:T/C"]}}


@contextmanager
def _delivery(*starts, home: bool = True):
    """The messaging service's start, answering each of ``starts`` in turn
    (None: it asked nothing), and its finish.

    Entered after ``_firing``. With ``home``, an automation with no workspace
    runs as the Chief of Staff in Home, as every account does once Flash is
    gone; without, it runs on Flash, which the flag still allows.
    """
    fx = SimpleNamespace(
        start=AsyncMock(side_effect=list(starts)),
        finish=AsyncMock(
            return_value=automation_delivery.Finish([{"method": "slack:T/C", "success": False}])
        ),
        manager=MagicMock(ensure_home_bound=AsyncMock()),
    )
    with (
        patch(f"{_MOD}.automation_delivery.start_run", new=fx.start),
        patch(
            "src.server.services.automation_settlement.automation_delivery.finish_run",
            new=fx.finish,
        ),
        patch("src.server.services.turn_runtime.home_enabled", new=AsyncMock(return_value=home)),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=fx.manager,
        ),
    ):
        yield fx


def _turn_args(fx):
    return fx.astream.call_args.kwargs


@pytest.mark.asyncio
async def test_a_held_run_tells_its_agent_where_to_send():
    automation = _automation(
        **_DELIVERING, additional_context=[{"type": "directive", "content": "Be brief."}]
    )
    with _firing([_streams]) as fx, _delivery(_HELD) as dx:
        await AutomationExecutor().execute(automation, _EXEC)

    args = _turn_args(fx)
    assert args["role"] == "chief_of_staff"
    # The thread the run goes on, resolved before the start. Home is none of
    # the user's workspaces, so the run names none.
    assert args["thread_id"]
    dx.start.assert_awaited_once_with(automation, _EXEC, None, thread_id=args["thread_id"])
    contexts = [(c.type, c.content) for c in args["request"].additional_context]
    assert contexts == [
        ("directive", "Be brief."),
        ("directive", automation_delivery.reminder(_HELD.targets)),
    ]
    # Its sends name the firing, and its run row says how it delivers.
    assert args["extra_configurable"] == {"automation_execution_id": _EXEC}
    assert args["run_metadata"] == {
        "automation_execution_id": _EXEC,
        "automation_id": "auto-1",
        **automation_delivery.run_metadata(_HELD),
    }
    # Its agent sends the result; nothing announces the start.
    fx.started.assert_not_awaited()
    assert _left_to_run(fx) == fx.run_ids[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "automation",
    [
        {"agent_mode": "flash"},
        {"agent_mode": "ptc", "workspace_id": get_flash_workspace_id(_USER)},
    ],
    ids=["no_workspace", "in_home"],
)
async def test_a_run_in_home_names_no_workspace_to_the_service(automation):
    """Whether the automation names Home or no workspace, an entry naming
    only an app takes that app's preferred chat."""
    home_id = get_flash_workspace_id(_USER)
    home = {"workspace_id": home_id, "user_id": _USER, "status": "running"}
    with (
        _firing([_streams]) as fx,
        _delivery(_HELD) as dx,
        patch("src.server.database.workspace.get_workspace", new=AsyncMock(return_value=home)),
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING, **automation), _EXEC)

    args = _turn_args(fx)
    assert (args["workspace_id"], args["role"]) == (home_id, "chief_of_staff")
    assert dx.start.await_args.args[2] is None
    assert args["extra_configurable"] == {"automation_execution_id": _EXEC}


@pytest.mark.asyncio
async def test_an_analysts_run_names_its_workspace_to_the_service():
    workspace = {"workspace_id": "ws-9", "user_id": _USER, "status": "running"}
    automation = _automation(**_DELIVERING, agent_mode="ptc", workspace_id="ws-9")
    with (
        _firing([_streams]) as fx,
        _delivery(_HELD) as dx,
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(return_value=workspace),
        ),
    ):
        await AutomationExecutor().execute(automation, _EXEC)

    args = _turn_args(fx)
    assert (args["workspace_id"], args["role"]) == ("ws-9", "analyst")
    dx.start.assert_awaited_once_with(automation, _EXEC, "ws-9", thread_id=args["thread_id"])
    assert args["extra_configurable"] == {"automation_execution_id": _EXEC}


@pytest.mark.asyncio
async def test_a_run_on_flash_gets_flashs_own_arguments():
    """Flash has no messaging tools, so its turn carries nothing for them,
    and its workflow takes no argument for it."""
    with _firing([_streams]) as fx, _delivery(_HELD, home=False) as dx:
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    args = _turn_args(fx)
    assert args["flash_workspace"] == {"workspace_id": "ws-1"}
    assert "extra_configurable" not in args
    assert dx.start.await_args.args[2] is None


@pytest.mark.asyncio
async def test_a_run_the_messaging_service_did_not_take_runs_as_before():
    with _firing([_streams]) as fx, _delivery(None) as dx:
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    dx.start.assert_awaited_once()
    args = _turn_args(fx)
    assert args["request"].additional_context is None
    assert args["extra_configurable"] is None
    assert args["run_metadata"] == {"automation_execution_id": _EXEC, "automation_id": "auto-1"}
    fx.started.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_start_the_service_did_not_take_leaves_the_run_to_the_webhook():
    """Its agent is told nothing, and its run row carries why, for a settle
    whose webhook has nowhere to post."""
    with _firing([_streams]) as fx, _delivery(_LEFT):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    args = _turn_args(fx)
    assert args["request"].additional_context is None
    assert args["extra_configurable"] is None
    assert args["run_metadata"] == {
        "automation_execution_id": _EXEC,
        "automation_id": "auto-1",
        **automation_delivery.run_metadata(_LEFT),
    }
    fx.started.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_run_left_to_a_webhook_with_nowhere_to_post_records_why_it_failed():
    with (
        _firing([_loses(WriterGuardUnavailable("budget"))], runs={0: None}) as fx,
        _delivery(_LEFT) as dx,
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    dx.finish.assert_not_awaited()
    fx.settled_webhook.assert_awaited_once()
    fx.db.record_delivery.assert_awaited_once_with(_EXEC, automation_delivery.unsent(_LEFT))


@pytest.mark.asyncio
async def test_a_held_run_that_fails_before_admission_ends_with_the_service():
    with (
        _firing([_loses(WriterGuardUnavailable("budget"))], runs={0: None}) as fx,
        _delivery(_HELD) as dx,
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    assert _settled(fx)["to"] == "failed"
    dx.finish.assert_awaited_once()
    assert dx.finish.await_args.args[2] == "failed"
    assert dx.finish.await_args.kwargs["targets"] == _HELD.targets
    fx.settled_webhook.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_held_run_stopped_before_admission_ends_with_the_service():
    with (
        _firing([_held_turn(admitted=False)], runs={0: None}) as fx,
        _delivery(_HELD) as dx,
    ):
        task = asyncio.create_task(
            AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)
        )
        while not fx.astream.call_count:
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _settled(fx)["error_message"] == INTERRUPTED_ERROR
    dx.finish.assert_awaited_once()
    assert dx.finish.await_args.args[2] == "failed"
    assert dx.finish.await_args.kwargs["targets"] == _HELD.targets
    fx.settled_webhook.assert_not_awaited()


_LIMIT = HTTPException(status_code=429, detail={"type": "credit_limit", "message": "Limit."})


@pytest.mark.asyncio
async def test_a_credit_refusal_before_the_turn_ends_with_the_service():
    """Refused before its delivery was handed over, the run still ends where
    it would have delivered: its targets get the failure notice, and no
    webhook event fires."""
    with _firing([_streams]) as fx, _delivery(_HELD) as dx:
        fx.credit.side_effect = _LIMIT
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    fx.astream.assert_not_called()
    assert _settled(fx)["failure_reason"] == "usage_limit"
    dx.start.assert_awaited_once()
    assert dx.start.await_args.kwargs["thread_id"] is None
    dx.finish.assert_awaited_once()
    assert dx.finish.await_args.args[1:3] == (_EXEC, "failed")
    assert dx.finish.await_args.kwargs["targets"] == _HELD.targets
    fx.settled_webhook.assert_not_awaited()
    # Refused, so the user's computer is left as it was.
    dx.manager.ensure_home_bound.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_analysts_refusal_names_its_workspace_to_the_service():
    workspace = {"workspace_id": "ws-9", "user_id": _USER, "status": "running"}
    automation = _automation(**_DELIVERING, agent_mode="ptc", workspace_id="ws-9")
    with (
        _firing([_streams]) as fx,
        _delivery(_HELD) as dx,
        patch(
            "src.server.database.workspace.get_workspace",
            new=AsyncMock(return_value=workspace),
        ),
    ):
        fx.credit.side_effect = _LIMIT
        await AutomationExecutor().execute(automation, _EXEC)

    dx.start.assert_awaited_once_with(automation, _EXEC, "ws-9", thread_id=None)
    dx.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_refusal_with_nowhere_to_deliver_fires_the_webhook():
    with _firing([_streams]) as fx, _delivery(None) as dx:
        fx.credit.side_effect = _LIMIT
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    dx.start.assert_awaited_once()
    dx.finish.assert_not_awaited()
    fx.settled_webhook.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_firing_settled_before_its_turn_hands_nothing_over():
    with _firing([_streams]) as fx, _delivery(_HELD) as dx:
        fx.db.transition_execution.side_effect = [
            {"conversation_thread_id": None},  # pending -> running
            None,  # the thread could not be recorded: settled elsewhere
        ]
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    dx.start.assert_not_awaited()
    fx.astream.assert_not_called()


# ─── Delivery changed while the firing waited ─────────────────────────

_LOST = HTTPException(status_code=409, detail=admission_conflict_detail("running"))
_MOVED = {"delivery_config": {"methods": ["telegram:-100"]}}
# The start made again for the entries the automation has after the wait,
# filed under an id of its own.
_RESTARTED = Delivery(
    f"{_EXEC}.2",
    [Target(entry="telegram:-100", address="telegram:-100", name="Desk", ok=True)],
)


def _starts(dx):
    return [(c.args[0]["delivery_config"], c.args[1]) for c in dx.start.await_args_list]


@pytest.mark.asyncio
async def test_a_wait_that_outlasts_a_delivery_change_runs_with_the_new_one():
    """The run is reminded of, sends to and is stamped with the targets the
    automation has after the wait. The start made for the old entries is
    never finished, so nothing posts to a chat the user took off."""
    fresh = _automation(**_MOVED)
    with (
        _firing([_loses(_LOST), _streams], fresh=fresh) as fx,
        _delivery(_HELD, _RESTARTED) as dx,
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    assert _starts(dx) == [
        (_DELIVERING["delivery_config"], _EXEC),
        (_MOVED["delivery_config"], f"{_EXEC}.2"),
    ]
    # On the same thread as the first.
    first, second = (c.kwargs["thread_id"] for c in dx.start.await_args_list)
    assert first == second == _turn_args(fx)["thread_id"]
    args = _turn_args(fx)
    assert [c.content for c in args["request"].additional_context] == [
        automation_delivery.reminder(_RESTARTED.targets)
    ]
    assert args["extra_configurable"] == {"automation_execution_id": f"{_EXEC}.2"}
    assert args["run_metadata"] == {
        "automation_execution_id": _EXEC,
        "automation_id": "auto-1",
        **automation_delivery.run_metadata(_RESTARTED),
    }
    dx.finish.assert_not_awaited()
    assert _left_to_run(fx) == fx.run_ids[1]


@pytest.mark.asyncio
async def test_a_refusal_after_a_delivery_change_ends_only_the_new_start():
    """A failed finish posts a notice to every target, so it goes to the
    targets the run had at its end, never the ones the user took off."""
    limit = HTTPException(status_code=429, detail={"type": "credit_limit", "message": "Limit."})
    with (
        _firing([_loses(_LOST)], fresh=_automation(**_MOVED)) as fx,
        _delivery(_HELD, _RESTARTED) as dx,
    ):
        fx.credit.side_effect = [None, limit]
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    assert _settled(fx)["failure_reason"] == "usage_limit"
    dx.finish.assert_awaited_once()
    assert dx.finish.await_args.args[1:3] == (f"{_EXEC}.2", "failed")
    assert dx.finish.await_args.kwargs["targets"] == _RESTARTED.targets


@pytest.mark.asyncio
async def test_a_delivery_taken_off_during_the_wait_ends_nothing_with_the_service():
    """The run now delivers nowhere through the service, and the start made
    before the wait is left to lapse."""
    with (
        _firing([_loses(_LOST), _loses(WriterGuardUnavailable("budget"))],
                runs={1: None}, fresh=_automation()) as fx,
        _delivery(_HELD, None) as dx,
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    args = _turn_args(fx)
    assert args["request"].additional_context is None
    assert args["extra_configurable"] is None
    assert _settled(fx)["to"] == "failed"
    dx.finish.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_wait_with_the_same_delivery_keeps_its_start():
    fresh = _automation(**_DELIVERING, instruction="Summarize the week")
    with (
        _firing([_loses(_LOST), _streams], fresh=fresh) as fx,
        _delivery(_HELD) as dx,
    ):
        await AutomationExecutor().execute(_automation(**_DELIVERING), _EXEC)

    dx.start.assert_awaited_once()
    args = _turn_args(fx)
    assert args["user_input"] == "Summarize the week"
    assert args["extra_configurable"] == {"automation_execution_id": _EXEC}
    assert args["run_metadata"][automation_delivery.DELIVERY_KEY]["id"] == _EXEC


@pytest.mark.asyncio
@pytest.mark.parametrize("llm_model", ["m-auto", None])
async def test_an_automation_turn_runs_its_own_model_not_the_threads(llm_model):
    """An automation on a thread runs the model it was set up with (null: the
    account default) and leaves the thread's own model as it found it. It calls
    the run generators directly, past the send route that reads and keeps one."""
    pinned = _automation(
        thread_strategy="continue", conversation_thread_id="thread-1", llm_model=llm_model
    )
    turn_model = AsyncMock(return_value="m-thread")
    with (
        _firing([_streams], fresh=pinned) as fx,
        patch("src.server.services.llm.thread_model.turn_model", new=turn_model),
    ):
        await AutomationExecutor().execute(pinned, _EXEC)

    kwargs = fx.astream.call_args.kwargs
    assert kwargs["request"].llm_model == llm_model
    assert kwargs.get("named_model") is None
    turn_model.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flag", "agent", "role"),
    [(False, "flash", None), (True, "ptc", "chief_of_staff")],
    ids=["flag_off", "flag_on"],
)
async def test_a_chief_of_staff_automation_follows_the_flag(flag, agent, role):
    """The Chief of Staff files its automations as PTC in Home.

    With the flag off they run on Flash like every other turn there, so the
    rollback starts no computer; they once went straight to the full agent.
    """
    home_id = get_flash_workspace_id(_USER)
    home = {"workspace_id": home_id, "user_id": _USER, "status": "running", "computer_id": "c-1"}
    manager = MagicMock(ensure_home_bound=AsyncMock())
    with (
        _firing([_streams]) as fx,
        patch("src.server.services.turn_runtime.home_enabled", new=AsyncMock(return_value=flag)),
        patch("src.server.database.workspace.get_workspace", new=AsyncMock(return_value=home)),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=manager,
        ),
    ):
        await AutomationExecutor().execute(
            _automation(agent_mode="ptc", workspace_id=home_id), _EXEC
        )

    turn = fx.astream.call_args.kwargs
    assert turn["request"].agent_mode == agent
    assert turn["request"].workspace_id == home_id
    assert turn.get("role") == role
    assert manager.ensure_home_bound.await_count == int(flag)
