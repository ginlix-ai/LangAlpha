"""Claude Code's entry under Connect a subscription: one answer, read whether
the user turned Claude Code on or not, that says what is missing."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.config.features import FeatureGate
from src.server.models.claude_code import ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeStatus
from src.server.services.claude_code import checks
from src.server.services.claude_code.setup_state import setup_state, summary
from tests.unit.server.app.test_claude_code_computers import USER, _computer, world  # noqa: F401

SETUP = "src.server.services.claude_code.setup_state"
NOW = datetime(2026, 11, 4, tzinfo=timezone.utc)
OFF = ClaudeCodeStatus(state="off")


def _own(state, expires_at=None):
    return ClaudeCodeComputer(computer_id=state, name=state, kind="ssh", state=state, expires_at=expires_at)


@pytest.mark.parametrize(
    ("states", "status", "api_key_runs", "cloud_ready", "expected"),
    [
        (["offline", "signed_in"], OFF, False, False, "connected"),
        (["rejected", "unverified"], OFF, False, False, "connected"),
        ([], OFF, True, False, "connected"),
        (["rejected", "checking"], OFF, False, False, "checking"),
        (["no_token", "rejected"], OFF, False, False, "refused"),
        (["no_token", "offline"], OFF, False, False, "not_connected"),
        (["off", "offline"], OFF, False, False, "not_connected"),
        (["offline"], OFF, False, True, "not_connected"),
        (["offline", "needs_perl"], OFF, False, False, "offline"),
        (["needs_perl", "policy_blocked", "computer_unsupported"], OFF, False, False, "unsupported"),
        ([], OFF, False, False, "no_computer"),
        ([], ClaudeCodeStatus(state="no_token"), False, False, "not_connected"),
        ([], ClaudeCodeStatus(state="signed_in"), False, False, "connected"),
        ([], ClaudeCodeStatus(state="policy_blocked"), False, False, "unsupported"),
    ],
)
def test_the_first_state_that_holds_over_every_way_it_runs_wins(states, status, api_key_runs, cloud_ready, expected):
    computers = [_own(s) for s in states]
    assert summary(computers, status, api_key_runs=api_key_runs, cloud_ready=cloud_ready, now=NOW) == expected


def test_a_token_past_its_expiry_is_refused_though_its_last_check_passed():
    past, later = NOW - timedelta(minutes=1), NOW + timedelta(days=1)
    assert summary([_own("signed_in", past)], OFF, api_key_runs=False, cloud_ready=False, now=NOW) == "refused"
    saved = ClaudeCodeStatus(state="unverified", expires_at=past)
    assert summary([], saved, api_key_runs=False, cloud_ready=False, now=NOW) == "refused"
    assert summary([_own("signed_in", later)], OFF, api_key_runs=False, cloud_ready=False, now=NOW) == "connected"


async def _setup(world, **switches):  # noqa: F811
    with (
        patch(f"{SETUP}.rows_available", side_effect=lambda: world.rows),
        patch(f"{SETUP}.user_feature_enabled", new_callable=AsyncMock, side_effect=lambda *_: world.opted_in),
        patch(f"{SETUP}.backend_available", return_value=switches.get("backend", False)),
        patch(f"{SETUP}.local_managed", new_callable=AsyncMock, return_value=switches.get("managed", False)),
        patch(f"{SETUP}.hosted.custody", new_callable=AsyncMock, return_value=switches.get("custody")),
        patch(f"{SETUP}.hosted.choice_offered", return_value=switches.get("offered", False)),
        patch(f"{SETUP}.hosted.cloud_refusals", new_callable=AsyncMock, return_value=switches.get("refusals", {})),
        patch(f"{SETUP}.hosted.in_sandbox", return_value=switches.get("sandbox", False)),
        patch(f"{SETUP}.get_computers_for_user", switches.get("listed", AsyncMock(return_value=[]))),
        patch("src.server.services.claude_code.saved.get_oauth_connection", new_callable=AsyncMock, return_value=None),
    ):
        answer = await setup_state(USER)
        await asyncio.gather(*checks._checks)
    return answer


@pytest.mark.asyncio
async def test_a_server_that_offers_no_claude_code_answers_nothing(world):  # noqa: F811
    world.rows = False
    assert await _setup(world) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("gate", "opted_in", "answers"), [
    (FeatureGate.PLAN, False, False), (FeatureGate.NONE, False, False), (FeatureGate.PLAN, True, True),
    (FeatureGate.OPT_IN, False, True),
])
async def test_a_user_the_gate_keeps_off_has_nothing_to_set_up(world, gate, opted_in, answers):  # noqa: F811
    world.opted_in = opted_in
    with patch(f"{SETUP}.system_flag", return_value=SimpleNamespace(gate=gate)):
        answer = await _setup(world)
    assert (answer is not None) is answers


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "live", "state"), [("stopped", True, "offline"), ("running", False, "offline"), ("running", True, "off")])
async def test_before_opting_in_a_computer_reads_reachable_or_offline_and_is_never_checked(world, status, live, state):  # noqa: F811
    world.opted_in = False
    world.computer, world.tunnel_live = _computer(status), live
    answer = await _setup(world)
    assert (answer.on, answer.state, answer.computers[0].state) == (
        False, "not_connected" if state == "off" else "offline", state,
    )
    assert "check" not in world.machine.calls


def _custody(mode="local", refusal="hosted_off", runs_on=None, offered=("api_key",)):
    return ClaudeCodeCustody(mode=mode, api_key_saved=False, offered=list(offered), runs_on=runs_on, refusal=refusal)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "custody", "state"),
    [
        (False, None, "no_computer"),
        (True, None, "not_connected"),
        (False, _custody(), "not_connected"),
        (False, _custody(mode="api_key", refusal=None, runs_on="api_key"), "not_connected"),
        (False, _custody(mode="stored", refusal="stored_blocked", offered=("api_key",)), "not_connected"),
        (False, _custody(mode="stored", refusal="stored_blocked", offered=()), "no_computer"),
        (False, _custody(mode="api_key", refusal="hosted_paused", offered=()), "no_computer"),
        (False, _custody(mode="stored", refusal="hosted_paused", offered=("api_key",)), "not_connected"),
        (False, _custody(mode="api_key", refusal="needs_own_computer", offered=("api_key",)), "no_computer"),
    ],
)
async def test_with_no_own_computer_and_not_on_it_is_ready_only_where_something_else_would_run_it(
    world, backend, custody, state,  # noqa: F811
):
    world.opted_in = False
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, backend=backend, custody=custody)
    assert (answer.state, answer.computers, answer.saved_token_runs) == (state, [], backend)
    assert answer.cloud is (custody is not None and bool(custody.offered) and custody.refusal != "needs_own_computer")


@pytest.mark.asyncio
async def test_before_opting_in_a_server_under_an_organization_policy_offers_no_saved_token(world):  # noqa: F811
    world.opted_in = False
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, backend=True, managed=True)
    assert (answer.state, answer.saved_token_runs) == ("no_computer", False)


@pytest.mark.asyncio
async def test_a_server_that_offers_a_choice_for_cloud_computers_says_they_run_it(world):  # noqa: F811
    world.opted_in = False
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, offered=True, custody=_custody())
    assert (answer.state, answer.cloud, answer.saved_token_runs) == ("not_connected", True, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("sandbox", [True, False])
async def test_it_says_whether_cloud_computers_run_claude_code_in_their_own_sandbox(world, sandbox):  # noqa: F811
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, backend=True, sandbox=sandbox)
    assert answer.cli_in_sandbox is sandbox


@pytest.mark.asyncio
@pytest.mark.parametrize("opted_in", [True, False])
async def test_it_names_each_cloud_computer_too_small_for_claude_code_in_the_users_order(world, opted_in):  # noqa: F811
    world.opted_in = opted_in
    listed = AsyncMock(return_value=[
        {"computer_id": "c-4", "name": "Backtests", "kind": "e2b"},
        {"computer_id": "c-2", "name": "Research", "kind": "daytona"},
        {"computer_id": "c-3", "name": "Scratch", "kind": "daytona"},
    ])
    refusals = {"c-3": "needs_bigger_computer", "c-4": "needs_bigger_computer"}
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, custody=_custody(), refusals=refusals, listed=listed)
    assert [c.model_dump() for c in answer.too_small] == [
        {"computer_id": "c-4", "name": "Backtests"}, {"computer_id": "c-3", "name": "Scratch"},
    ]


@pytest.mark.asyncio
async def test_with_no_cloud_computer_too_small_it_names_none_and_reads_no_list(world):  # noqa: F811
    listed = AsyncMock(return_value=[{"computer_id": "c-3", "name": "Scratch", "kind": "daytona"}])
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, custody=_custody(), listed=listed)
    assert answer.too_small == []
    listed.assert_not_awaited()


@pytest.mark.asyncio
async def test_once_on_the_api_key_that_runs_on_cloud_computers_reads_connected(world):  # noqa: F811
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world, custody=_custody(mode="api_key", refusal=None, runs_on="api_key"))
    assert (answer.on, answer.state) == (True, "connected")


@pytest.mark.asyncio
async def test_before_opting_in_a_computer_a_recent_dial_could_not_reach_reads_offline(world):  # noqa: F811
    from src.server.services.claude_code.machine import unreached_key
    world.opted_in = False
    world.cache.values[unreached_key(world.computer["computer_id"])] = "1"
    answer = await _setup(world)
    assert (answer.state, answer.computers[0].state) == ("offline", "offline")


@pytest.mark.asyncio
async def test_once_on_an_own_computer_is_checked_and_reads_connected_when_claude_serves_its_token(world):  # noqa: F811
    world.machine.token = "sk-ant-oat01-mine"
    first = await _setup(world)
    assert (first.on, first.state) == (True, "checking")
    assert (await _setup(world)).state == "connected"


@pytest.mark.asyncio
async def test_once_on_a_computer_that_went_out_of_reach_reads_offline_though_its_token_last_worked(world):  # noqa: F811
    world.machine.token = "sk-ant-oat01-mine"
    await _setup(world)
    assert (await _setup(world)).state == "connected"
    world.tunnel_live = False
    answer = await _setup(world)
    assert (answer.state, answer.computers[0].state) == ("offline", "offline")
