"""No work starts or continues for a suspended account where it can be
stopped: the relay and the direct tool path refuse every call, a turn the
server dispatches is dropped, an automation's firing is skipped with no
strike, and an always-on computer stops being kept up. Nothing is read for
any of it with the ``teams`` flag off."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.account_contract import NO_ORG, Account, Org
from src.server.services.account import Standing

pytestmark = pytest.mark.asyncio

ACCOUNT = "src.server.services.account"
SUSPENDED = Account(org=Org("o-1", "Acme", suspended=True), features={"teams": True})
ACTIVE = Account(org=Org("o-1", "Acme"), features={"teams": True})


def _account(account, *, door=True):
    stack = ExitStack()
    stack.enter_context(patch("src.server.services.membership.teams_door_open", return_value=door))
    read = stack.enter_context(
        patch(f"{ACCOUNT}.read_standing", AsyncMock(return_value=Standing(account, account) if door else Standing(None, None)))
    )
    return stack, read


# --- the relay -------------------------------------------------------------------


class _Claims:
    def __init__(self, user_id):
        self.user_id = user_id
        self.caller = None


async def _prepare(grant):
    from src.server.services.egress import relay
    from tests.unit.server.services.test_egress_order_gate import _body

    from src.server.services.egress.policy_gate import PolicyGate

    resolve, own_server = AsyncMock(), AsyncMock()
    with (
        patch.object(relay, "fetch_grant_for_relay", AsyncMock(return_value=grant)),
        patch.object(relay, "_grant_is_reachable", return_value=True),
        patch.object(PolicyGate, "own_server", own_server),
        patch.object(relay, "resolve_vendor_credential", resolve),
    ):
        try:
            await relay.prepare_relay(
                "g", claims=_Claims(grant["user_id"]), raw_body=_body("read", {})
            )
        finally:
            # Refused ahead of every other check and of any credential.
            own_server.assert_not_awaited()
            resolve.assert_not_awaited()


@pytest.mark.parametrize("kind", ["oauth_mcp", "header_mcp", "org_mcp"])
async def test_every_frame_for_a_suspended_account_is_refused_at_the_relay(kind):
    from src.server.services.egress import RelayError, RelayRejection
    from tests.unit.server.services.test_egress_order_gate import _grant

    stack, _ = _account(SUSPENDED)
    with stack, pytest.raises(RelayRejection) as refused:
        await _prepare(_grant(kind=kind))
    assert (refused.value.status, refused.value.code) == (403, RelayError.ACCOUNT_SUSPENDED)


def _gate():
    from src.server.services.egress.policy_gate import PolicyGate

    return PolicyGate("u", via="relay", caller=None)


async def test_the_relay_reads_nothing_more_with_the_flag_off():
    stack, read = _account(SUSPENDED, door=False)
    with stack:
        assert await _gate().suspended() is False
    read.assert_not_awaited()


async def test_one_frame_reads_the_account_once_and_shares_the_read():
    stack, read = _account(ACTIVE)
    with stack, patch(
        "src.server.services.membership.share_membership_read"
    ) as shared:
        gate = _gate()
        assert await gate.suspended() is False
        assert await gate.own_server("https://a.example.test/mcp", server="a", tool="t") is None
    shared.assert_called_once_with("u")
    read.assert_awaited_once_with("u")


# --- the direct tool path --------------------------------------------------------


async def test_a_direct_call_for_a_suspended_account_is_refused_before_anything_is_read():
    from src.server.services.egress.direct_tools import DirectMCPBinding

    connection = AsyncMock()
    stack, _ = _account(SUSPENDED)
    with stack, patch(
        "src.server.services.egress.direct_tools.get_connection", connection
    ):
        refusal = await DirectMCPBinding(user_id="u").check("crm", "search")
    assert refusal == (
        "No call was made: this account's access through the user's "
        "organization has ended. Tell the user."
    )
    connection.assert_not_awaited()


# --- turns the server dispatches -------------------------------------------------


async def test_a_dispatched_turn_refused_for_suspension_is_dropped_at_once():
    from tests.unit.server.services.test_notify_turn_busy import _post, _Resp, _Session

    refused = _Resp(403, {"detail": {"type": "account_suspended", "message": "m"}})
    session = _Session([refused])
    assert await _post(session) == ("drop", None)
    assert session.posts == 1


# --- automations -------------------------------------------------------------------


async def test_a_suspended_accounts_firing_is_skipped_with_no_strike_and_no_channel():
    from tests.unit.server.services.test_automation_executor_credit import _gated

    stack, _ = _account(SUSPENDED)
    with stack:
        mock_adb, mock_astream, mock_settled = await _gated(None)
    mock_astream.assert_not_called()
    settled = mock_adb.settle_execution.await_args.kwargs
    assert settled["to"] == "skipped"
    assert settled["strike"] is None
    assert settled["failure_reason"] is None
    mock_settled.assert_not_awaited()


# --- always-on ---------------------------------------------------------------------


async def test_a_suspended_owners_always_on_computer_is_no_longer_kept_up():
    """Its own machine too; and the setting stays as the owner chose, for an
    account that is reinstated."""
    from src.server.services.workspace_manager import WorkspaceManager
    from tests.unit.server.services.test_workspace_manager import (
        _make_computer,
        _make_config,
    )

    manager = WorkspaceManager(_make_config())
    cloud = _make_computer(computer_id="c-cloud", is_always_on=True)
    own = _make_computer(computer_id="c-own", kind="ssh", is_always_on=True)
    stack, _ = _account(SUSPENDED)
    with stack, patch(
        "src.server.services.workspace_entitlements.read_standing",
        AsyncMock(return_value=Standing(None, None, suspended_hint=True)),
    ), patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        AsyncMock(return_value=False),
    ) as entitlement, patch.object(
        manager, "set_computer_always_on", AsyncMock(return_value={})
    ) as disable:
        exempt = await manager._reconcile_always_on_entitlements([cloud, own])
    assert exempt == set()
    disable.assert_not_awaited()
    entitlement.assert_not_awaited()
    assert cloud["is_always_on"] and own["is_always_on"]


async def test_an_entitled_owners_always_on_computer_is_kept_up_as_before():
    from src.server.services.workspace_manager import WorkspaceManager
    from tests.unit.server.services.test_workspace_manager import (
        _make_computer,
        _make_config,
    )

    manager = WorkspaceManager(_make_config())
    cloud = _make_computer(computer_id="c-cloud", is_always_on=True)
    own = _make_computer(computer_id="c-own", kind="ssh", is_always_on=True)
    with patch(
        "src.server.services.workspace_entitlements.read_standing",
        AsyncMock(return_value=Standing(NO_ORG, NO_ORG)),
    ), patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        AsyncMock(return_value=False),
    ):
        exempt = await manager._reconcile_always_on_entitlements([cloud, own])
    assert exempt == {"c-cloud", "c-own"}


async def test_with_the_flag_off_the_sweep_asks_about_no_owner():
    from src.server.services.workspace_manager import WorkspaceManager
    from tests.unit.server.services.test_workspace_manager import (
        _make_computer,
        _make_config,
    )

    manager = WorkspaceManager(_make_config())
    computers = [
        _make_computer(computer_id=f"c-{i}", user_id=f"u-{i}", is_always_on=True)
        for i in range(3)
    ]
    stack, _ = _account(SUSPENDED, door=False)
    suspended = AsyncMock(return_value=Standing(None, None, suspended_hint=True))
    with stack, patch(
        "src.server.services.workspace_entitlements.read_standing", suspended
    ), patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        AsyncMock(return_value=False),
    ):
        exempt = await manager._reconcile_always_on_entitlements(computers)
    # No coroutine made, not only none awaited: patched, ``read_standing``
    # counts its calls.
    assert suspended.call_count == 0
    assert exempt == {"c-0", "c-1", "c-2"}
