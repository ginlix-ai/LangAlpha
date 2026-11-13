"""Claude Code's entry under Connect a subscription: one answer, read whether
the user turned Claude Code on or not, that says what is missing."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.config.features import FeatureGate
from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.claude_code_hosted import Custody
from src.server.models.claude_code import (
    ClaudeCodeChiefOfStaff, ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeStatus,
)
from src.server.services.claude_code import checks
from src.server.services.claude_code.hosted import Hosted, OnCloud, OnOwn, Refused
from src.server.services.claude_code.setup_state import chief_of_staff, setup_state, summary
from tests.unit.server.app.test_claude_code_computers import USER, _computer, world  # noqa: F401

SETUP = "src.server.services.claude_code.setup_state"
NOW = datetime(2026, 11, 4, tzinfo=timezone.utc)
OFF = ClaudeCodeStatus(state="off")


@pytest.fixture(autouse=True)
def chief_of_staff_runs_nowhere_read():
    """Where the Chief of Staff's Claude Code runs reads the user's computers,
    which only its own tests below give it."""
    nowhere = ClaudeCodeChiefOfStaff(kind="server")
    with patch(f"{SETUP}.chief_of_staff", new_callable=AsyncMock, return_value=nowhere) as read:
        yield read


def _own(state, expires_at=None):
    return ClaudeCodeComputer(clis=4, computer_id=state, name=state, kind="ssh", state=state, expires_at=expires_at)


@pytest.mark.parametrize(
    ("states", "status", "cloud", "expected"),
    [
        (["offline", "signed_in"], OFF, None, "connected"),
        (["rejected", "unverified"], OFF, None, "connected"),
        (["rejected", "checking"], OFF, None, "checking"),
        (["no_token", "rejected"], OFF, None, "refused"),
        (["no_token", "offline"], OFF, None, "not_connected"),
        (["off", "offline"], OFF, None, "not_connected"),
        (["offline"], OFF, "ready", "not_connected"),
        (["offline", "needs_perl"], OFF, None, "offline"),
        (["needs_perl", "policy_blocked", "computer_unsupported"], OFF, None, "unsupported"),
        ([], OFF, None, "no_computer"),
        ([], ClaudeCodeStatus(state="no_token"), None, "not_connected"),
        ([], ClaudeCodeStatus(state="signed_in"), None, "connected"),
        ([], ClaudeCodeStatus(state="policy_blocked"), None, "unsupported"),
    ],
)
def test_the_first_state_that_holds_over_every_way_it_runs_wins(states, status, cloud, expected):
    computers = [_own(s) for s in states]
    assert summary(computers, status, cloud=cloud, now=NOW) == expected


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        (["offline"], "offline"),
        (["offline", "needs_perl"], "offline"),
        (["no_token", "offline"], "not_connected"),
        (["needs_perl"], "not_connected"),
        ([], "not_connected"),
    ],
)
def test_an_offline_computer_is_said_first_where_cloud_computers_wait_on_the_users_choice(states, expected):
    computers = [_own(s) for s in states]
    assert summary(computers, OFF, cloud="chosen", now=NOW) == expected


def test_a_token_past_its_expiry_is_refused_though_its_last_check_passed():
    past, later = NOW - timedelta(minutes=1), NOW + timedelta(days=1)
    assert summary([_own("signed_in", past)], OFF, cloud=None, now=NOW) == "refused"
    saved = ClaudeCodeStatus(state="unverified", expires_at=past)
    assert summary([], saved, cloud=None, now=NOW) == "refused"
    assert summary([_own("signed_in", later)], OFF, cloud=None, now=NOW) == "connected"


async def _setup(world, **switches):  # noqa: F811
    """A deployment that gates Claude Code ``opt_in`` unless ``gate`` says
    otherwise, since a user before opting in exists only there."""
    with (
        patch(f"{SETUP}.system_flag", return_value=SimpleNamespace(gate=switches.get("gate", FeatureGate.OPT_IN))),
        patch(f"{SETUP}.rows_available", side_effect=lambda: world.rows),
        patch(f"{SETUP}.user_feature_enabled", new_callable=AsyncMock, side_effect=lambda *_: world.opted_in),
        patch(f"{SETUP}.backend_available", return_value=switches.get("backend", False)),
        patch(f"{SETUP}.local_managed", new_callable=AsyncMock, return_value=switches.get("managed", False)),
        patch(f"{SETUP}.hosted.custody", new_callable=AsyncMock, return_value=switches.get("custody")),
        patch(f"{SETUP}.hosted.choice_offered", return_value=switches.get("offered", False)),
        patch(
            f"{SETUP}.hosted.cloud_view", new_callable=AsyncMock,
            return_value=(switches.get("refusals", {}), OnCloud(Hosted("saved_token"))),
        ),
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
    answer = await _setup(world, gate=gate)
    assert (answer is not None) is answers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "live", "state"),
    [("stopped", True, "off"), ("stopped", False, "offline"), ("running", False, "offline"), ("running", True, "off")],
)
async def test_before_opting_in_a_computer_reads_reachable_or_offline_and_is_never_checked(world, status, live, state):  # noqa: F811
    """One the idle sweep stopped with its machine up is not offline, since a
    turn starts it, but nothing dials it: the look for a token left there
    waits for a computer that runs."""
    world.opted_in = False
    world.computer, world.tunnel_live = _computer(status), live
    answer = await _setup(world)
    assert (answer.on, answer.state, answer.computers[0].state) == (
        False, "not_connected" if state == "off" else "offline", state,
    )
    assert "check" not in world.machine.calls
    assert world.machine.calls == (["state"] if status == "running" and live else [])


def _custody(mode="local", refusal="hosted_off", runs_on=None, offered=("stored",)):
    return ClaudeCodeCustody(mode=mode, offered=list(offered), runs_on=runs_on, refusal=refusal)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "custody", "state"),
    [
        (False, None, "no_computer"),
        (True, None, "not_connected"),
        (False, _custody(), "not_connected"),
        (False, _custody(mode="stored", refusal=None, runs_on="stored_token"), "not_connected"),
        (False, _custody(refusal="stored_blocked", offered=()), "no_computer"),
        (False, _custody(mode="stored", refusal="needs_own_computer"), "no_computer"),
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
@pytest.mark.parametrize(("opted_in", "state"), [(False, "not_connected"), (True, "offline")])
async def test_an_offline_computer_is_said_first_only_once_on_where_cloud_computers_wait_on_the_users_choice(
    world, opted_in, state,  # noqa: F811
):
    """Off, where it runs is not chosen yet, and storing a token for cloud
    computers is still open: the entry offers to turn it on rather than wait
    on a computer of theirs that is out of reach."""
    world.opted_in = opted_in
    world.computer, world.tunnel_live = _computer("running"), False
    with patch("src.server.services.claude_code.hosted.get_custody", new_callable=AsyncMock, return_value=Custody()):
        answer = await _setup(world, offered=True, custody=_custody())
    assert (answer.state, answer.computers[0].state) == (state, "offline")


@pytest.mark.asyncio
async def test_once_on_with_no_own_computer_storing_a_token_for_cloud_computers_is_what_is_left(world):  # noqa: F811
    with (
        patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]),
        patch("src.server.services.claude_code.hosted.get_custody", new_callable=AsyncMock, return_value=Custody()),
    ):
        answer = await _setup(world, offered=True, custody=_custody())
    assert (answer.on, answer.state) == (True, "not_connected")


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
async def test_once_on_a_computer_the_idle_sweep_stopped_reads_as_its_token_last_worked_and_is_not_checked(world):  # noqa: F811
    """A turn there starts it, so it is not offline; a check would dial a
    machine nobody started, so none runs while it is stopped."""
    world.machine.token = "sk-ant-oat01-mine"
    await _setup(world)
    assert (await _setup(world)).state == "connected"
    checks = world.machine.calls.count("check")
    world.computer = _computer("stopped")
    answer = await _setup(world)
    assert (answer.state, answer.computers[0].state) == ("connected", "signed_in")
    assert world.machine.calls.count("check") == checks


@pytest.mark.asyncio
async def test_once_on_a_computer_that_went_out_of_reach_reads_offline_though_its_token_last_worked(world):  # noqa: F811
    world.machine.token = "sk-ant-oat01-mine"
    await _setup(world)
    assert (await _setup(world)).state == "connected"
    world.tunnel_live = False
    answer = await _setup(world)
    assert (answer.state, answer.computers[0].state) == ("offline", "offline")


# -- Where the Chief of Staff's Claude Code runs --

HOME = get_home_workspace_id(USER)
STUDIO = {"computer_id": "c-9", "name": "Studio", "kind": "ssh"}


DEFAULT_RUNS = "src.server.services.llm.claude_code.default_runs"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("where", "shown"),
    [
        (OnOwn(STUDIO), {"kind": "own", "computer_id": "c-9", "name": "Studio", "refusal": None}),
        (OnOwn(STUDIO, picked=True), {"kind": "picked", "computer_id": "c-9", "name": "Studio", "refusal": None}),
        (
            Refused("cli_computer_offline", {"name": "Studio"}),
            {"kind": "picked", "computer_id": None, "name": "Studio", "refusal": "cli_computer_offline"},
        ),
        (
            Refused("no_own_computer"),
            {"kind": "picked", "computer_id": None, "name": None, "refusal": "no_own_computer"},
        ),
        (
            Refused("needs_bigger_computer", {"home": True}),
            {"kind": "server", "computer_id": None, "name": None, "refusal": "needs_bigger_computer"},
        ),
    ],
)
@pytest.mark.parametrize("default_runs", [True, False])
async def test_the_chief_of_staff_runs_on_the_own_computer_a_home_turn_would_or_names_its_refusal(
    where, shown, default_runs,
):
    """By the rule every turn in Home follows, so Settings never says one thing and admission another."""
    placed = AsyncMock(return_value=where)
    unread = AsyncMock(side_effect=AssertionError("read Home's computer"))
    with (
        patch(f"{SETUP}.hosted.cli_placement", placed),
        patch(f"{SETUP}.hosted.in_sandbox", lambda: False),
        patch(DEFAULT_RUNS, AsyncMock(return_value=default_runs)) as ran,
        patch("src.server.database.computer.get_computer_for_workspace", unread),
    ):
        answer = await chief_of_staff(USER)
    assert answer.model_dump() == {**shown, "default_runs": default_runs}
    placed.assert_awaited_once_with(USER, HOME, token=True)
    ran.assert_awaited_once_with(USER, where)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("default_runs", "replied", "runs"),
    [(True, False, True), (True, True, False), (False, False, False), (False, True, False)],
)
async def test_a_home_thread_another_model_last_replied_on_runs_the_deployments_default(default_runs, replied, runs):
    """As its turn is admitted (``chief_of_staff_default_runs``), so the composer starts that thread
    on the model the server runs. The checkpoint is read only where the Default would run otherwise."""
    read = AsyncMock(return_value=replied)
    with (
        patch(f"{SETUP}.hosted.cli_placement", AsyncMock(return_value=OnOwn(STUDIO, picked=True))),
        patch(f"{SETUP}.hosted.in_sandbox", lambda: False),
        patch(DEFAULT_RUNS, AsyncMock(return_value=default_runs)),
        patch("src.server.services.llm.claude_code.another_model_replied", read),
    ):
        answer = await chief_of_staff(USER, thread_id="t-1")
    assert answer.default_runs is runs
    if default_runs:
        read.assert_awaited_once_with("t-1")
    else:
        read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("computer", "shown"),
    [
        ({"computer_id": "c-2", "name": "Research"}, ("c-2", "Research")),
        (None, (None, None)),
    ],
)
async def test_the_chief_of_staff_on_homes_cloud_computer_names_it_as_not_the_users_own(computer, shown):
    """The placement carries Home's computer (the primary one it binds to, where not bound yet), so no second read."""
    unread = AsyncMock(side_effect=AssertionError("read Home's computer"))
    with (
        patch(f"{SETUP}.hosted.cli_placement", AsyncMock(return_value=OnCloud(Hosted("stored_token"), computer))),
        patch(f"{SETUP}.hosted.in_sandbox", lambda: True),
        patch(DEFAULT_RUNS, AsyncMock(return_value=False)),
        patch("src.server.database.computer.get_computer_for_workspace", unread),
    ):
        runs_on = await chief_of_staff(USER)
    assert (runs_on.kind, runs_on.computer_id, runs_on.name, runs_on.refusal) == ("home_cloud", *shown, None)


@pytest.mark.asyncio
async def test_the_chief_of_staff_whose_cli_runs_in_this_servers_workers_names_no_computer():
    """Home's cloud computer holds its tools, not its CLI, so naming it would mislead."""
    unread = AsyncMock(side_effect=AssertionError("read Home's computer"))
    with (
        patch(
            f"{SETUP}.hosted.cli_placement",
            AsyncMock(return_value=OnCloud(Hosted("saved_token"), {"computer_id": "c-2", "name": "Research"})),
        ),
        patch(f"{SETUP}.hosted.in_sandbox", lambda: False),
        patch(DEFAULT_RUNS, AsyncMock(return_value=True)),
        patch("src.server.database.computer.get_computer_for_workspace", unread),
    ):
        runs_on = await chief_of_staff(USER)
    assert (runs_on.kind, runs_on.computer_id, runs_on.name, runs_on.default_runs) == ("server", None, None, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("opted_in", [True, False])
async def test_the_entry_says_where_the_chief_of_staff_runs_once_claude_code_is_on(
    world, chief_of_staff_runs_nowhere_read, opted_in,  # noqa: F811
):
    world.opted_in = opted_in
    runs_on = ClaudeCodeChiefOfStaff(kind="own", computer_id="c-9", name="Studio", default_runs=True)
    chief_of_staff_runs_nowhere_read.return_value = runs_on
    with patch("src.server.services.claude_code.machine.get_computer_roster", new_callable=AsyncMock, return_value=[]):
        answer = await _setup(world)
    assert answer.chief_of_staff == (runs_on if opted_in else None)
    assert chief_of_staff_runs_nowhere_read.await_count == int(opted_in)
