"""What a suspended account already has running stops: a turn and its
subagents end at their next model call, the change notice stops the rest
through the paths a user's own stop takes, and an automation run that ends
either way settles as suspended, its one-shot put off rather than used up.
Nothing is read for any of it with the ``teams`` flag off."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest

from src.server.services.account import ACCOUNT_SUSPENDED, AccountSuspended, Standing
from tests.unit.server.test_account_suspended_work import ACTIVE, SUSPENDED, _account

pytestmark = pytest.mark.asyncio


# --- a running turn ------------------------------------------------------------------


def _gate(port):
    from ptc_agent.agent.middleware.credit_gate import CreditGateState, CreditLease

    lease = CreditLease(recorded_as="u", sender="u", run_ref="r", port=port)
    return CreditGateState(run_ref="r", kind="run", port=port, lease=lease)


async def _boundary(gate):
    from ptc_agent.agent.middleware.credit_gate import (
        CreditGateMiddleware,
        current_credit_gate,
    )

    token = current_credit_gate.set(gate)
    try:
        return await CreditGateMiddleware().abefore_model({}, MagicMock())
    finally:
        current_credit_gate.reset(token)


@pytest.mark.parametrize("kind", ["run", "task"])
async def test_a_turn_and_its_subagents_end_at_the_next_model_call(kind):
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    gate = _gate(PlatformCreditGatePort())
    gate.kind = kind
    stack, read = _account(SUSPENDED)
    with stack, pytest.raises(AccountSuspended):
        await _boundary(gate)
    read.assert_awaited_once_with("u", unknown_on_cache_error=True)


async def test_an_active_or_unknown_account_runs_on():
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    for account in (ACTIVE, None):
        stack, _ = _account(account)
        with stack:
            assert await _boundary(_gate(PlatformCreditGatePort())) is None


async def test_the_check_reads_nothing_with_the_flag_off():
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    stack, read = _account(SUSPENDED, door=False)
    with stack:
        assert await _boundary(_gate(PlatformCreditGatePort())) is None
    read.assert_not_awaited()


async def test_a_check_that_fails_never_stops_a_turn():
    port = MagicMock(account_refusal=AsyncMock(side_effect=RuntimeError("redis")))
    assert await _boundary(_gate(port)) is None


async def test_a_port_without_the_check_is_never_asked():
    assert await _boundary(_gate(object())) is None


async def _after_batch(port):
    """The CLI's after-tool hook as the batch's last result comes back."""
    from langchain_core.messages import ToolMessage

    from ptc_agent.agent.claude_code.mid_turn import AfterTool, HandOff
    from tests.unit.ptc_agent.agent.claude_code.test_runtime import UNWATCHED
    from ptc_agent.agent.claude_code.tools_bridge import ToolResults
    from ptc_agent.agent.middleware.credit_gate import current_credit_gate

    async def take():
        return HandOff()

    results = ToolResults()
    results.resolve(ToolMessage("ok", tool_call_id="t1"))
    hook = AfterTool(results, send=AsyncMock(), plan=UNWATCHED)
    hook.open(["t1"], take)
    token = current_credit_gate.set(_gate(port))
    try:
        out = await hook({"hook_event_name": "PostToolUse"}, "t1", None)
    finally:
        current_credit_gate.reset(token)
    return out, results


async def test_claude_code_stops_after_a_batch_for_a_suspended_account():
    """The CLI asks Claude again on its own after a batch; it is stopped
    there, and the gate ahead of the next CLI ends the run."""
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    stack, read = _account(SUSPENDED)
    with stack:
        out, results = await _after_batch(PlatformCreditGatePort())
    assert out["continue"] is False
    assert results.owes_gate("t1")
    read.assert_awaited_once_with("u", unknown_on_cache_error=True)
    # Where the graph goes next: the gate's before_model, which ends the run.
    stack, _ = _account(SUSPENDED)
    with stack, pytest.raises(AccountSuspended):
        await _boundary(_gate(PlatformCreditGatePort()))


@pytest.mark.parametrize("account", [ACTIVE, None])
async def test_claude_code_carries_on_after_a_batch_for_an_active_or_unknown_account(account):
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    stack, _ = _account(account)
    with stack:
        out, results = await _after_batch(PlatformCreditGatePort())
    assert out.get("continue") is not False
    assert not results.owes_gate("t1")


async def test_claude_code_after_a_batch_reads_nothing_with_the_flag_off():
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    stack, read = _account(SUSPENDED, door=False)
    with stack:
        out, _ = await _after_batch(PlatformCreditGatePort())
    assert out.get("continue") is not False
    read.assert_not_awaited()


async def test_the_claude_code_model_step_ends_a_suspended_accounts_run():
    from langgraph.prebuilt import ToolNode

    from ptc_agent.agent.claude_code.runtime import build_claude_code_graph
    from ptc_agent.agent.middleware.credit_gate import current_credit_gate
    from src.server.services.credit_gate_port import PlatformCreditGatePort

    model = build_claude_code_graph(ToolNode([])).builder.nodes["model"].runnable
    stack, _ = _account(SUSPENDED)
    token = current_credit_gate.set(_gate(PlatformCreditGatePort()))
    try:
        with stack, pytest.raises(AccountSuspended):
            await model.ainvoke({"messages": []}, {})
    finally:
        current_credit_gate.reset(token)


async def test_the_turn_ends_on_the_suspension_frame_and_ledger_mark():
    from src.server.services.runs.stream_errors import (
        error_event_data,
        failure_kind_metadata,
    )

    refusal = Standing(SUSPENDED, SUSPENDED).refusal()
    frame = error_event_data("t-1", str(refusal), exc=refusal)
    assert frame == {
        "thread_id": "t-1",
        "error": str(refusal),
        "type": "workflow_error",
        "error_type": ACCOUNT_SUSPENDED,
        "error_class": "AccountSuspended",
        "organization": "Acme",
    }
    assert "recovery" not in frame and "hints" not in frame
    # Stamped on the run's row, and never reported as our own failure.
    assert failure_kind_metadata(refusal) == {"error_type": ACCOUNT_SUSPENDED}


# --- automations ------------------------------------------------------------------


def _run(metadata, status="error"):
    return {"status": status, "metadata": metadata}


async def test_a_run_the_suspension_ended_settles_as_suspended():
    from src.server.services.automation_outcomes import Outcome, ledger_outcome

    assert ledger_outcome(_run({"error_type": ACCOUNT_SUSPENDED})) is Outcome.SUSPENDED
    # Refused at admission after START, as the handler finalizes it.
    assert (
        ledger_outcome(_run({"error_type": ACCOUNT_SUSPENDED, "error_class": "AccountSuspended"}))
        is Outcome.SUSPENDED
    )


async def _settle_by_run(run):
    from src.server.services import automation_settlement as settlement

    settle = AsyncMock(return_value=True)
    with patch.object(
        settlement, "read_standing", AsyncMock(return_value=Standing(None, None, suspended_hint=True))
    ), patch.object(
        settlement, "settle", settle
    ), patch.object(settlement, "read_run_answer", AsyncMock(return_value=settlement.RunAnswer())):
        outcome = await settlement.settle_by_run(
            {"automation_id": "a", "user_id": "u"}, "e", "r",
            {**run, "cancel_requested_at": None},
            thread_id="t", workspace_id="w",
        )
    assert settle.await_args.args[2] is outcome
    return outcome


@pytest.mark.parametrize(
    "run",
    [
        _run({}),  # a failure on the way
        # A server's stop (a shutdown) while the owner is suspended. The
        # notice's own stop names itself: tests/integration/
        # test_suspension_stop_db.py settles the rows it really writes.
        _run({"cancelled_by_user": False}, status="cancelled"),
        {"status": "interrupted", "interrupt_reason": "credit_pause", "metadata": {}},
    ],
)
async def test_a_suspended_owners_run_cut_short_settles_as_suspended(run):
    from src.server.services.automation_outcomes import Outcome

    assert await _settle_by_run(run) is Outcome.SUSPENDED


@pytest.mark.parametrize(
    ("run", "expected"),
    [
        # Done before the suspension landed: delivered, and not fired again.
        (_run({}, status="completed"), "completed"),
        # The reader's own stop is theirs, suspended since or not.
        (_run({"cancelled_by_user": True}, status="cancelled"), "stopped"),
    ],
)
async def test_a_suspended_owners_run_that_ended_on_its_own_terms_settles_as_it_ended(
    run, expected
):
    assert await _settle_by_run(run) == expected


# --- the change notice ----------------------------------------------------------------


async def _notice(*, door, account):
    from src.server.app import users

    stop = MagicMock()
    stack, read = _account(account, door=door)
    with stack, patch(
        "src.server.services.membership.forget_membership",
        AsyncMock(return_value=7),
    ), patch("src.server.services.account.is_feature_enabled_system", return_value=door), patch(
        "src.server.services.membership.is_feature_enabled_system", return_value=door
    ), patch(
        "src.server.database.mcp_config_version.bump_user_workspaces_mcp_version",
        AsyncMock(),
    ), patch(
        "src.server.services.account_suspension.stop_if_suspended_soon", stop
    ):
        await users.membership_changed("u", MagicMock(body=AsyncMock(return_value=b"")))
    return stop, read


async def test_the_notice_starts_the_stop_after_its_answer_with_the_flag_on():
    stop, read = await _notice(door=True, account=SUSPENDED)
    # With the epoch the notice moved the membership to.
    stop.assert_called_once_with("u", 7)
    # Read in the background, not on the notice's own path.
    read.assert_not_awaited()


async def test_the_notice_stops_nothing_with_the_flag_off():
    stop, read = await _notice(door=False, account=SUSPENDED)
    stop.assert_not_called()
    read.assert_not_awaited()


def _background(account, *, running=True, epochs=(7,)):
    """The notice's background read: work running or not, the account as
    the auth service answers, and the epoch as each recheck finds it."""
    from src.server.services import account_suspension

    stack, read = _account(account)
    found = list(epochs)
    stack.enter_context(
        patch(
            "src.server.database.runs.lifecycle.user_has_running_work",
            AsyncMock(return_value=running),
        )
    )
    stack.enter_context(
        patch.object(
            account_suspension,
            "membership_epoch",
            AsyncMock(side_effect=lambda _u: found.pop(0) if len(found) > 1 else found[0]),
        )
    )
    stop = stack.enter_context(patch.object(account_suspension, "stop_account_work", AsyncMock()))
    return stack, read, stop


@pytest.mark.parametrize(("account", "stops"), [(SUSPENDED, True), (ACTIVE, False), (None, False)])
async def test_only_a_suspended_account_has_its_work_stopped(account, stops):
    from src.server.services import account_suspension

    stack, read, stop = _background(account)
    with stack:
        await account_suspension._stop_if_suspended("u", 7)
    assert stop.await_count == (1 if stops else 0)
    # Asked as the gate asks: a Redis error is unknown, not a request.
    read.assert_awaited_once_with("u", unknown_on_cache_error=True)


async def test_an_idle_accounts_notice_asks_the_auth_service_nothing():
    from src.server.services import account_suspension

    stack, read, stop = _background(SUSPENDED, running=False)
    with stack:
        await account_suspension._stop_if_suspended("u", 7)
    read.assert_not_awaited()
    stop.assert_not_awaited()


async def test_an_idle_accounts_notice_makes_no_validate_request_end_to_end():
    from src.server.services import account_suspension
    from tests.unit.server.services.test_account_contract import _Auth, _Cache, _deployment

    cache, auth = _Cache(), _Auth({"valid": True, "access_tier": 2})
    with _deployment(cache, auth, door=True), patch(
        "src.server.database.runs.lifecycle.user_has_running_work",
        AsyncMock(return_value=False),
    ):
        await account_suspension._stop_if_suspended("u", 1)
    assert auth.requests == []


async def test_a_slow_read_answered_after_a_later_notice_stops_nothing():
    """Suspended, then reinstated while the first read was out: the read
    still says suspended, but the epoch has moved."""
    from src.server.services import account_suspension

    stack, _, stop = _background(SUSPENDED, epochs=(8,))
    with stack:
        await account_suspension._stop_if_suspended("u", 7)
    stop.assert_not_awaited()


def _stop_paths(computers, manager):
    stack = ExitStack()
    patches = {
        "src.server.database.runs.lifecycle.open_runs_of_user": AsyncMock(return_value=[
            {"conversation_thread_id": "t-1", "conversation_response_id": "r-1"}
        ]),
        "src.server.services.cancel_dispatch.cancel_workflow": AsyncMock(),
        "src.server.database.runs.subagent_runs.open_task_runs_of_user": AsyncMock(
            return_value=[{"thread_id": "t-1", "task_id": "k", "task_run_id": "tr"}]
        ),
        "src.server.services.cancel_dispatch.cancel_subagent_task": AsyncMock(),
        "src.server.database.runs.command_runs.request_stops": AsyncMock(return_value=["cmd"]),
        "src.server.database.computer.get_computers_for_user": AsyncMock(return_value=computers),
        "src.server.services.preview_capabilities.revoke": AsyncMock(),
    }
    mocks = {name.rsplit(".", 1)[1]: stack.enter_context(patch(name, m)) for name, m in patches.items()}
    stack.enter_context(
        patch(
            "src.server.services.computer_manager.ComputerManager.get_instance",
            return_value=manager,
        )
    )
    return stack, mocks


async def test_the_stop_goes_through_each_path_a_users_own_stop_takes():
    from src.server.services import account_suspension

    computers = [
        {"computer_id": "c-1", "status": "running", "provider_ref": "sb-1"},
        {"computer_id": "c-2", "status": "stopped", "provider_ref": "sb-2"},
    ]
    manager = MagicMock(stop_computer=AsyncMock())
    stack, m = _stop_paths(computers, manager)
    m["cancel_workflow"].side_effect = RuntimeError("one step fails")
    with stack:
        await account_suspension.stop_account_work("u", AsyncMock(return_value=True))
    # Not the user's own stop: their steers stay queued.
    m["cancel_workflow"].assert_awaited_once_with("t-1", "r-1", by="suspension")
    # A step that failed leaves the rest to run.
    m["cancel_subagent_task"].assert_awaited_once_with("t-1", "k", task_run_id="tr")
    m["request_stops"].assert_awaited_once_with(
        user_id="u", by="suspended", started_before=None
    )
    assert m["revoke"].await_args_list == [call("sb-1"), call("sb-2")]
    # Running computers only; nothing saved is touched.
    manager.stop_computer.assert_awaited_once_with("c-1")
    assert not [c for c in manager.method_calls if "always_on" in c[0]]


async def test_only_work_started_before_the_notice_is_stopped():
    """A reinstatement and a new turn that land after a step's check: the
    step reads only work older than the notice, so the new turn is not it."""
    from datetime import datetime, timezone

    from src.server.services import account_suspension

    stack, m = _stop_paths([], MagicMock(stop_computer=AsyncMock()))
    before = datetime.now(timezone.utc)
    with stack, patch.object(account_suspension, "membership_epoch", AsyncMock(return_value=7)), patch(
        "src.server.database.runs.lifecycle.user_has_running_work", AsyncMock(return_value=True)
    ), patch(
        "src.server.services.account.read_standing",
        AsyncMock(return_value=MagicMock(suspended=True)),
    ):
        account_suspension.stop_if_suspended_soon("u", 7)
        await asyncio.gather(*account_suspension._stops)
    cutoff = m["open_runs_of_user"].await_args.kwargs["started_before"]
    assert before <= cutoff <= datetime.now(timezone.utc)
    assert m["open_task_runs_of_user"].await_args.kwargs["started_before"] == cutoff
    assert m["request_stops"].await_args.kwargs["started_before"] == cutoff


async def test_a_reinstatement_between_steps_stops_what_is_left_untouched():
    from src.server.services import account_suspension

    computers = [{"computer_id": "c-1", "status": "running", "provider_ref": "sb-1"}]
    manager = MagicMock(stop_computer=AsyncMock())
    stack, m = _stop_paths(computers, manager)
    # Suspended for the turns' step, reinstated before the tasks'.
    still = AsyncMock(side_effect=[True, False])
    with stack:
        await account_suspension.stop_account_work("u", still)
    m["cancel_workflow"].assert_awaited_once()
    m["cancel_subagent_task"].assert_not_awaited()
    m["request_stops"].assert_not_awaited()
    m["revoke"].assert_not_awaited()
    manager.stop_computer.assert_not_awaited()


async def test_a_computer_stop_retried_after_a_reinstatement_is_not_retried():
    from src.server.services import account_suspension

    computers = [{"computer_id": "c-1", "status": "running", "provider_ref": "sb-1"}]
    # Busy winding down the cancelled turn: the first stop fails.
    manager = MagicMock(stop_computer=AsyncMock(side_effect=RuntimeError("busy")))
    stack, _ = _stop_paths(computers, manager)
    # Every step before the computers', then reinstated before the retry.
    still = AsyncMock(side_effect=[True] * 5 + [False])
    with stack, patch.object(account_suspension, "_STOP_RETRY_S", 0):
        await account_suspension.stop_account_work("u", still)
    manager.stop_computer.assert_awaited_once_with("c-1")
    assert still.await_count == 6


async def test_suspend_then_reinstate_stops_nothing_end_to_end():
    """Two notices back to back: the first's task finds the epoch the second
    moved, the second's finds the account active."""
    from src.server.services import membership
    from src.server.services import account_suspension
    from tests.unit.server.services.test_account_contract import _Auth, _Cache, _deployment

    class _Epochs(_Cache):
        async def get(self, key):
            return self.store.get(key)

    cache = _Epochs()
    auth = _Auth(
        {"valid": True, "access_tier": 2, "org": {"id": "o", "name": "Acme", "suspended": True},
         "features": {"teams": True}},
        {"valid": True, "access_tier": 2, "org": {"id": "o", "name": "Acme"},
         "features": {"teams": True}},
    )
    with _deployment(cache, auth, door=True), patch(
        "src.server.database.runs.lifecycle.user_has_running_work",
        AsyncMock(return_value=True),
    ), patch.object(account_suspension, "stop_account_work", AsyncMock()) as stop:
        suspended_at = await membership.forget_membership("u")
        first = asyncio.create_task(account_suspension._stop_if_suspended("u", suspended_at))
        await asyncio.sleep(0)  # the first read is out
        reinstated_at = await membership.forget_membership("u")
        await first
        await account_suspension._stop_if_suspended("u", reinstated_at)
    stop.assert_not_awaited()


async def test_a_suspended_command_stop_reports_nothing():
    from src.server.database.runs.command_runs import Outcome

    assert Outcome("stopped", stopped_by="suspended").report == "none"


async def test_the_notice_is_answered_without_waiting_for_the_stop_and_even_when_it_fails():
    from fastapi import FastAPI

    from src.server.app.users import router
    from src.server.services import account_suspension

    app = FastAPI()
    app.include_router(router)
    release = asyncio.Event()
    order: list[str] = []

    async def running(_user_id):
        order.append("stop started")
        await release.wait()
        order.append("stop failed")
        raise RuntimeError("database down")

    with (
        patch("src.server.services.membership.teams_door_open", return_value=True),
        patch("src.server.services.account.is_feature_enabled_system", return_value=True),
        patch("src.server.services.membership.is_feature_enabled_system", return_value=True),
        patch(
            "src.server.services.membership.forget_membership",
            AsyncMock(return_value=1),
        ),
        patch(
            "src.server.database.mcp_config_version.bump_user_workspaces_mcp_version",
            AsyncMock(),
        ),
        patch("src.server.database.runs.lifecycle.user_has_running_work", running),
        patch("src.server.utils.api._SERVICE_TOKEN", "svc"),
    ):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            # Were the stop on the notice's path, this would wait on it forever.
            resp = await asyncio.wait_for(
                client.post(
                    "/api/v1/users/me/membership-changed",
                    headers={"X-Service-Token": "svc", "X-User-Id": "u"},
                ),
                timeout=5,
            )
        order.append("answered")
        release.set()
        for task in list(account_suspension._stops):
            await task
    assert resp.status_code == 204
    assert order.index("answered") < order.index("stop failed")
