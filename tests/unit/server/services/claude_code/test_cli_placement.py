"""Where a Claude Code turn's CLI runs when the workspace's cloud computer,
Home's included, runs none for the user (``hosted.picks_own``): on the own
computer of theirs ``machine.pick_cli_computer`` finds signed in with its own
token and online, while the turn's tools stay on the workspace's computer.
Every other refusal stands, and nothing ever falls back from a cloud computer
that runs Claude Code to one of theirs.
"""

import time
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Machine
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.llms.claude_code import ClaudeTurnFailed
from src.server.database.home_workspace import get_home_workspace_id
from src.server.models.claude_code import ClaudeCodeComputers
from src.server.services.claude_code import hosted as hosted_module
from ptc_agent.config.agent import Admitted
from src.server.services.claude_code.hosted import Hosted, OnCloud, OnOwn, Refused
from src.server.services.claude_code.launchers import turn_launcher
from src.server.services.claude_code.machine import MachineAnswer, answer_key, pick_cli_computer, unreached_key
from src.server.services.claude_code.plan import UserPlan
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
    chief_of_staff_default_runs,
    claude_code_refusal,
    require_runnable,
)
from tests.unit.server.services.claude_code.fakes import Cache, serving
from tests.unit.server.services.claude_code.test_hosted import (
    CLOUD,
    CODE,
    HOSTED,
    OWN,
    SERVICE,
    SMALL,
    STUDIO,
    TOO_SMALL_SELF_HOSTED,
    platform,
)

MACHINES = "src.server.services.claude_code.machine"
CLI = f"{MACHINES}.pick_cli_computer"
HOME = get_home_workspace_id("user-1")
#: The own computer a launcher runs the CLI on, by the id its token file is named for.
CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
MAC = {"computer_id": CID, "name": "Studio", "kind": "ssh", "status": "running", "user_id": "user-1"}


@pytest.fixture(autouse=True)
def no_stored_task_runs():
    """No subagent run on the stored token unless a test says otherwise."""
    with patch(f"{HOSTED}.live_stored_task_runs", AsyncMock(return_value=[])) as runs:
        yield runs


# -- The own computer that runs it (machine.pick_cli_computer) --


def _own(computer_id, name, status="running", tunnel=None):
    config = {"tunnel": tunnel} if tunnel else {}
    return {"computer_id": computer_id, "name": name, "kind": "ssh", "status": status, "provider_config": config}


def _found(cache, computer_id, file, ago):
    cache.values[answer_key(computer_id)] = asdict(MachineAnswer(file, checked_at=time.time() - ago))


async def _pick(roster, cache, tunnels=None, conn=None):
    with (
        serving(cache),
        patch(f"{MACHINES}.get_computer_roster", AsyncMock(return_value=roster)) as read,
        patch(f"{MACHINES}.tunnels_for", AsyncMock(return_value=tunnels)),
    ):
        picked = await pick_cli_computer("user-1", conn=conn)
    read.assert_awaited_once_with("user-1", conn=conn)
    return picked


@pytest.mark.asyncio
async def test_the_cli_runs_on_the_first_own_computer_in_the_roster_signed_in_with_its_own_token():
    """A token the check could not verify still signs a turn in there; a
    cloud computer, and an own one whose token Claude refused or that has
    none, never run it, however recently found."""
    studio, laptop, refused, empty = (
        _own("c-9", "Studio"), _own("c-8", "Laptop"), _own("c-7", "Old Mac"), _own("c-6", "Spare"),
    )
    cache = Cache()
    _found(cache, "c-9", "unknown", ago=300)
    _found(cache, "c-8", "works", ago=200)
    _found(cache, "c-7", "refused", ago=10)
    _found(cache, "c-6", "none", ago=10)
    _found(cache, "c-2", "works", ago=1)
    conn = object()
    assert await _pick([CLOUD, refused, empty, studio, laptop], cache, conn=conn) == (studio, None)


@pytest.mark.asyncio
async def test_a_status_read_that_finds_another_computer_signed_in_since_moves_no_turn():
    """Every status read stamps the answer it finds, so a pick by the newest
    one would move successive turns, and their plans, between computers."""
    studio, laptop = _own("c-9", "Studio"), _own("c-8", "Laptop")
    cache = Cache()
    _found(cache, "c-9", "works", ago=300)
    _found(cache, "c-8", "works", ago=200)
    assert await _pick([studio, laptop], cache) == (studio, None)
    _found(cache, "c-8", "works", ago=0)
    assert await _pick([studio, laptop], cache) == (studio, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "tunnel", "live", "unreached", "picked"),
    [
        pytest.param("stopped", None, None, False, True, id="stopped-dialed-directly"),
        pytest.param("stopped", "tun-8", True, False, True, id="stopped-tunnel-live"),
        pytest.param("stopped", "tun-8", False, False, False, id="stopped-tunnel-down"),
        pytest.param("running", "tun-8", False, False, False, id="running-tunnel-down"),
        pytest.param("running", "tun-8", True, True, False, id="unreached"),
        pytest.param("error", None, None, False, False, id="error"),
        pytest.param("creating", None, None, False, False, id="creating"),
    ],
)
async def test_one_whose_host_is_reachable_is_picked_asleep_or_not_else_passed_over_for_the_next(
    status, tunnel, live, unreached, picked,
):
    """Never dialed: a stopped computer is picked while its host is up (the
    run starts it), a connect-out one only through a live tunnel; one a
    recent dial could not reach, and one neither running nor stopped, are
    passed over."""
    laptop = _own("c-8", "Laptop", status=status, tunnel=tunnel)
    studio = _own("c-9", "Studio")
    cache = Cache()
    _found(cache, "c-8", "works", ago=10)
    _found(cache, "c-9", "works", ago=300)
    if unreached:
        cache.values[unreached_key("c-8")] = "1"
    tunnels = {"tun-8": {"live": live}} if tunnel else None
    assert await _pick([laptop, studio], cache, tunnels) == ((laptop if picked else studio), None)


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["tunnel_down", "unreached", "error"])
async def test_with_every_one_signed_in_offline_none_runs_it_and_the_first_is_named(why):
    """A cloud computer is never named, though an answer is kept for it."""
    cache = Cache()
    _found(cache, "c-8", "works", ago=10)
    _found(cache, "c-9", "works", ago=300)
    _found(cache, "c-2", "works", ago=1)
    _found(cache, "c-7", "refused", ago=10)
    status = "error" if why == "error" else "stopped"
    studio = _own("c-9", "Studio", status=status, tunnel="tun-9")
    laptop = _own("c-8", "Laptop", status=status, tunnel="tun-8")
    if why == "unreached":
        cache.values[unreached_key("c-9")] = "1"
        cache.values[unreached_key("c-8")] = "1"
    tunnels = {"tun-9": {"live": why != "tunnel_down"}, "tun-8": {"live": why != "tunnel_down"}}
    assert await _pick([CLOUD, _own("c-7", "Old Mac"), studio, laptop], cache, tunnels) == (None, "Studio")


@pytest.mark.asyncio
async def test_one_never_checked_runs_it_only_where_none_was_found_signed_in():
    """Computers last found signed in with a token of their own run it, in
    roster order; one never checked (or whose answer lapsed) may have no token
    there, so it runs it only where none is signed in, and a signed-in one
    offline is named instead. One whose answer says its token fails is never
    picked."""
    first, second, third = _own("c-1", "Spare"), _own("c-2", "Laptop"), _own("c-3", "Studio")
    refused = _own("c-4", "Old Mac")
    cache = Cache()
    _found(cache, "c-4", "refused", ago=10)
    assert await _pick([refused, first], cache) == (first, None)
    cache.values[unreached_key("c-1")] = "1"
    assert await _pick([refused, first], cache) == (None, "Spare")
    cache.values.pop(unreached_key("c-1"))
    _found(cache, "c-2", "works", ago=10)
    assert await _pick([first, second], cache) == (second, None)
    cache.values[unreached_key("c-2")] = "1"
    assert await _pick([first, second], cache) == (None, "Laptop")
    cache.values.pop(unreached_key("c-2"))
    _found(cache, "c-3", "unknown", ago=10)
    assert await _pick([first, third, second], cache) == (third, None)
    assert await _pick([first, second, third], cache) == (second, None)


@pytest.mark.asyncio
async def test_one_whose_token_is_past_its_expiry_is_picked_only_where_no_other_is_found():
    """As Settings reads it (``setup_state._effective``): Claude refuses
    such a token, so a computer with one is passed over for the next, even
    one never checked. Alone, it is picked, so admission refuses the turn
    for its token rather than asking for a computer to sign in."""
    studio, laptop = _own("c-9", "Studio"), _own("c-8", "Laptop")
    cache = Cache()
    for computer_id, expires_at in (("c-9", "2020-01-01T00:00:00+00:00"), ("c-8", "2999-01-01T00:00:00+00:00")):
        cache.values[answer_key(computer_id)] = asdict(
            MachineAnswer("works", expires_at=expires_at, checked_at=time.time() - 10),
        )
    assert await _pick([studio, laptop], cache) == (laptop, None)
    assert await _pick([studio, _own("c-5", "Spare")], cache) == (_own("c-5", "Spare"), None)
    assert await _pick([studio], cache) == (studio, None)


@pytest.mark.asyncio
async def test_with_none_signed_in_with_its_own_token_none_runs_it_and_none_is_named():
    cache = Cache()
    _found(cache, "c-7", "refused", ago=10)
    _found(cache, "c-6", "none", ago=10)
    assert await _pick([_own("c-7", "Old Mac"), _own("c-6", "Spare"), CLOUD], cache) == (None, None)
    assert await _pick([], Cache()) == (None, None)


# -- Where a turn's CLI runs (hosted.cli_placement) --


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "given",
    [{"mode": "local"}, {"mode": "stored", "blocked": True}, {"stored": False}],
    ids=["chose-own-computers", "storing-blocked", "switch-off"],
)
@pytest.mark.parametrize("workspace", ["ws-1", HOME], ids=["workspace", "home"])
async def test_a_cloud_workspace_that_runs_none_runs_its_cli_on_the_picked_own_computer(given, workspace):
    """Home's cloud computer included, so the Chief of Staff runs there too."""
    with platform(cli=(STUDIO, None), **given):
        where = await hosted_module.cli_placement("user-1", workspace)
    assert where == OnOwn(STUDIO, picked=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cli", "refusal", "name"),
    [((None, None), "no_own_computer", None), ((None, "Studio"), "cli_computer_offline", "Studio")],
)
async def test_with_no_own_computer_to_run_it_the_turn_is_refused_naming_the_one_asleep(cli, refusal, name):
    with platform(mode="local", cli=cli):
        where = await hosted_module.cli_placement("user-1", "ws-1")
    assert where == Refused(refusal, {"name": name} if name else {})


@pytest.mark.asyncio
async def test_a_cloud_workspace_that_runs_claude_code_runs_it_in_its_own_vm_and_picks_no_own_computer():
    with platform(mode="stored"), patch(CLI, AsyncMock(return_value=(STUDIO, None))) as picked:
        where = await hosted_module.cli_placement("user-1", "ws-1")
    assert isinstance(where, OnCloud) and where.hosted == Hosted("stored_token")
    picked.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_workspace_on_an_own_computer_runs_its_cli_there_whatever_the_choice():
    with platform(mode="local", computer=OWN), patch(CLI, AsyncMock(return_value=(STUDIO, None))) as picked:
        where = await hosted_module.cli_placement("user-1", "ws-1")
    assert where == OnOwn(OWN) and not where.picked
    picked.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "backend", "managed", "refusal"),
    [
        ({"mode": "stored", "computer": SMALL}, False, False, "needs_bigger_computer"),
        ({"placement": False}, True, True, "policy_blocked"),
    ],
)
async def test_a_refusal_of_a_cloud_computer_that_runs_claude_code_stands_and_picks_no_own_computer(
    given, backend, managed, refusal,
):
    """Its user chose to run it there, so a cloud computer too small, or a
    policy on the server, is theirs to fix, not a reason to move."""
    with (
        platform(**given), patch(CLI, AsyncMock(return_value=(STUDIO, None))) as picked,
        patch(f"{HOSTED}.backend_available", lambda: backend),
        patch("ptc_agent.core.claude_code_managed.local_managed", AsyncMock(return_value=["m.json"] if managed else [])),
    ):
        where = await hosted_module.cli_placement("user-1", "ws-1")
    assert isinstance(where, Refused) and where.reason == refusal
    picked.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("primary", "own"), [(OWN, True), (CLOUD, False)])
async def test_home_not_bound_yet_is_judged_on_the_primary_computer_it_binds_to(primary, own):
    primary_read = AsyncMock(return_value=primary)
    with (
        platform(mode="stored", computer=None), patch("src.server.database.computer.get_primary_computer", primary_read),
    ):
        where = await hosted_module.cli_placement("user-1", HOME)
        elsewhere = await hosted_module.cli_placement("user-1", "ws-1")
    assert isinstance(where, OnOwn) is own
    assert own or where.hosted == Hosted("stored_token")
    assert not isinstance(elsewhere, OnOwn), "a workspace not bound yet is no Home"
    primary_read.assert_awaited_once_with("user-1", conn=None)


# -- Admission and the run's launcher --


@pytest.mark.asyncio
async def test_the_chief_of_staff_is_admitted_on_the_answer_of_the_own_computer_that_runs_its_cli():
    """Home's cloud computer has no answer of its own to read."""
    config = SimpleNamespace(feature_enabled=lambda _: True)
    with (
        platform(mode="local", cli=(STUDIO, None)),
        patch("src.server.services.llm.claude_code.read_answer", AsyncMock(return_value=MachineAnswer("refused", checked_at=time.time()))) as read,
        pytest.raises(HTTPException) as refused,
    ):
        await require_runnable(config, CODE, user_id="user-1", workspace_id=HOME)
    read.assert_awaited_once_with("c-9")
    assert refused.value.detail["reason"] == "token_refused"


def _cloud_sandbox():
    return SimpleNamespace(provider=MagicMock(), working_dir="/home/workspace")


@pytest.fixture
def still_picked():
    """The rule admission asked still picks an own computer as the run starts."""
    with patch("src.server.services.claude_code.launchers.cli_placement", AsyncMock(return_value=OnOwn(MAC, picked=True))):
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", ["stored_token", None])
@pytest.mark.usefixtures("still_picked")
async def test_a_turn_admitted_on_an_own_computer_runs_its_cli_there_from_its_root_dialed_once(saved):
    """Its tools stay on the cloud sandbox; only the CLI goes to the machine,
    on that computer's own token file and Claude plan, dialed as the run
    starts rather than as it is built. Each link is that computer's activity,
    so the idle sweep never stops one that runs only other workspaces' CLIs."""
    link = object()
    dial = AsyncMock(return_value=Machine(AsyncMock(return_value=link), lambda: "/Users/me/langalpha"))
    active = AsyncMock(return_value=True)
    with (
        patch("src.server.database.computer.get_computer", AsyncMock(return_value=MAC)) as read,
        patch("src.server.database.computer.update_computer_activity", active),
        patch(f"{MACHINES}.machine_of", dial),
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(saved=saved, cli_computer=CID),
        )
        assert isinstance(launcher, SSHLauncher) and launcher.token_file == token_name(CID)
        assert (launcher.credential is not None) is (saved is not None)
        assert isinstance(launcher.plan, UserPlan) and launcher.plan.computer_id == CID
        dial.assert_not_awaited()
        assert await launcher.machine.link() is link
        assert await launcher.machine.link() is link
    read.assert_awaited_once_with(CID)
    dial.assert_awaited_once_with(MAC)
    assert active.await_args_list == [((CID,),), ((CID,),)]
    assert launcher.machine.root() == "/Users/me/langalpha"


@pytest.mark.asyncio
@pytest.mark.usefixtures("still_picked")
async def test_an_own_computer_that_drops_after_the_first_dial_ends_the_turn_naming_it():
    """A later link goes over the machine dialed first, and its failure reads
    as that computer offline, never as a broken turn."""
    links = AsyncMock(side_effect=[object(), SandboxTransientError("connection reset")])
    with (
        patch("src.server.database.computer.get_computer", AsyncMock(return_value=MAC)),
        patch("src.server.database.computer.update_computer_activity", AsyncMock(return_value=True)),
        patch(f"{MACHINES}.machine_of", AsyncMock(return_value=Machine(links, lambda: "/Users/me/langalpha"))),
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(cli_computer=CID),
        )
        await launcher.machine.link()
        with pytest.raises(ClaudeTurnFailed) as failed:
            await launcher.machine.link()
    assert (failed.value.kind, failed.value.fields) == ("cli_computer_offline", {"name": "Studio"})


@pytest.mark.asyncio
@pytest.mark.parametrize(("computer", "name"), [(MAC, "Studio"), (None, CID)])
@pytest.mark.usefixtures("still_picked")
async def test_an_own_computer_gone_out_of_reach_since_admission_ends_the_turn_naming_it(computer, name):
    with (
        patch("src.server.database.computer.get_computer", AsyncMock(return_value=computer)),
        patch(f"{MACHINES}.machine_of", AsyncMock(side_effect=SandboxTransientError("no route to host"))),
        pytest.raises(ClaudeTurnFailed) as failed,
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(cli_computer=CID),
        )
        await launcher.machine.link()
    assert claude_code_refusal(failed.value, CODE) == {
        "message": CLAUDE_CODE_OFF["cli_computer_offline"].format(name=name), "type": "claude_code_off",
        "model": CODE, "reason": "cli_computer_offline", "name": name,
    }



WAKE = "src.server.services.workspace_manager.WorkspaceManager.get_instance"


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped", ["stopped", "starting", "stopping"])
@pytest.mark.usefixtures("still_picked")
async def test_an_own_computer_the_idle_sweep_stopped_since_admission_is_started_before_it_is_dialed(stopped):
    """As a workspace's acquire starts its computer; the row dialed is read
    again after the start, and the dial counts as the computer's activity."""
    order = []
    reads = AsyncMock(side_effect=lambda _: order.append("read") or ({**MAC, "status": stopped} if len(order) == 1 else MAC))
    wake = AsyncMock(side_effect=lambda *a, **kw: order.append("wake"))
    dial = AsyncMock(
        side_effect=lambda computer: order.append("dial") or Machine(AsyncMock(return_value=object()), lambda: "/r"),
    )
    with (
        patch("src.server.database.computer.get_computer", reads),
        patch("src.server.database.computer.update_computer_activity", AsyncMock(return_value=True)) as active,
        patch(f"{MACHINES}.machine_of", dial),
        patch(WAKE, lambda: SimpleNamespace(wake_computer=wake)),
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(cli_computer=CID),
        )
        await launcher.machine.link()
    wake.assert_awaited_once_with(CID, user_id="user-1")
    dial.assert_awaited_once_with(MAC)
    assert order == ["read", "wake", "read", "dial"]
    active.assert_awaited_once_with(CID)


@pytest.mark.asyncio
@pytest.mark.usefixtures("still_picked")
async def test_an_own_computer_that_cannot_be_started_since_admission_ends_the_turn_naming_it():
    with (
        patch("src.server.database.computer.get_computer", AsyncMock(return_value={**MAC, "status": "stopped"})),
        patch(f"{MACHINES}.machine_of", AsyncMock()) as dial,
        patch(WAKE, lambda: SimpleNamespace(wake_computer=AsyncMock(side_effect=RuntimeError("no route")))),
        pytest.raises(ClaudeTurnFailed) as failed,
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(cli_computer=CID),
        )
        await launcher.machine.link()
    dial.assert_not_awaited()
    assert (failed.value.kind, failed.value.fields) == ("cli_computer_offline", {"name": "Studio"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "computer",
    [{**MAC, "status": "error"}, {**MAC, "status": "creating"}, {**MAC, "user_id": "user-2"}],
    ids=["error", "creating", "not-theirs"],
)
@pytest.mark.usefixtures("still_picked")
async def test_an_own_computer_in_error_not_ready_or_not_theirs_since_admission_runs_no_cli_and_is_never_started(computer):
    dial, wake = AsyncMock(), AsyncMock()
    with (
        patch("src.server.database.computer.get_computer", AsyncMock(return_value=computer)),
        patch(f"{MACHINES}.machine_of", dial),
        patch(WAKE, lambda: SimpleNamespace(wake_computer=wake)),
        pytest.raises(ClaudeTurnFailed) as failed,
    ):
        launcher = turn_launcher(
            _cloud_sandbox(), computer_id="c-2", workspace_id="ws-1", user_id="user-1",
            admitted=Admitted(cli_computer=CID),
        )
        await launcher.machine.link()
    dial.assert_not_awaited()
    wake.assert_not_awaited()
    assert (failed.value.kind, failed.value.fields) == ("cli_computer_offline", {"name": "Studio"})

# -- Automations --


@pytest.mark.asyncio
@pytest.mark.parametrize("model", [CODE, None])
@pytest.mark.parametrize("cli", [(None, None), (None, "Studio")])
@pytest.mark.parametrize("workspace", ["ws-1", HOME], ids=["workspace", "home"])
async def test_an_automation_with_no_own_computer_awake_now_is_saved_since_one_may_be_when_it_fires(
    model, cli, workspace,
):
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    with platform(mode="local", cli=cli):
        await _check_claude_code_hosted("user-1", model, workspace, pref={"preferred_model": CODE})


@pytest.mark.asyncio
async def test_an_automation_whose_cli_runs_on_an_own_computer_relayed_the_stored_token_is_saved():
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    with (
        platform(mode="local", cli=(STUDIO, None)),
        patch(f"{MACHINES}.read_answer", AsyncMock(return_value=MachineAnswer("none"))),
    ):
        await _check_claude_code_hosted("user-1", CODE, HOME)


def _self_hosted_on_a_small_computer():
    """A self-hosted server running the saved token's CLI in a cloud computer's
    sandbox, on a computer below the tier that runs it."""
    return (
        platform(computer=SMALL), patch("src.config.settings.HOST_MODE", "oss"),
        patch(f"{HOSTED}.backend_available", lambda: True),
    )


@pytest.mark.asyncio
async def test_a_home_automation_on_a_claude_code_model_too_small_to_run_is_refused_naming_homes_computer():
    """Home moves to no other computer, so only its spec is offered."""
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    deployment, host, backend = _self_hosted_on_a_small_computer()
    with deployment, host, backend, pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, HOME)
    assert (str(refused.value), refused.value.field) == (
        "Claude Code runs on Medium or larger cloud computers, and Home's computer is smaller. Change its spec.",
        "llm_model",
    )


@pytest.mark.asyncio
async def test_a_home_automation_naming_no_model_is_saved_where_the_claude_code_default_has_nowhere_to_run():
    """Its firing runs the deployment's model in that default's place; in a
    workspace the same default is refused, as every firing there would fail."""
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    pref = {"preferred_model": CODE}
    deployment, host, backend = _self_hosted_on_a_small_computer()
    with deployment, host, backend:
        await _check_claude_code_hosted("user-1", None, HOME, pref=pref)
        with pytest.raises(AutomationRefusal) as refused:
            await _check_claude_code_hosted("user-1", None, "ws-1", pref=pref)
    assert str(refused.value) == TOO_SMALL_SELF_HOSTED


# -- Settings --


@pytest.mark.asyncio
async def test_the_own_computer_cloud_computers_run_the_cli_on_is_read_only_where_they_run_none():
    with platform(mode="local", cli=(STUDIO, None)):
        refusals, where = await hosted_module.cloud_view("user-1")
    assert (where, hosted_module.picks_own(where), refusals) == (OnOwn(STUDIO, picked=True), True, {})
    with platform(mode="stored"), patch(CLI, AsyncMock(return_value=(STUDIO, None))) as picked:
        _, where = await hosted_module.cloud_view("user-1")
    assert isinstance(where, OnCloud) and not hosted_module.picks_own(where)
    picked.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_cloud_computer_too_small_is_named_only_where_its_turns_run_in_its_sandbox_not_on_an_own_computer():
    small = {str(SMALL["computer_id"]): "needs_bigger_computer"}
    with platform(mode="stored", computer=SMALL):
        refusals, _ = await hosted_module.cloud_view("user-1")
    assert refusals == small
    with platform(mode="local", computer=SMALL, cli=(STUDIO, None)):
        refusals, where = await hosted_module.cloud_view("user-1")
    assert (refusals, where) == ({}, OnOwn(STUDIO, picked=True))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "where", "shown"),
    [
        (
            True, OnOwn(STUDIO, picked=True),
            {"kind": "picked", "computer_id": "c-9", "name": "Studio", "refusal": None},
        ),
        (
            True, Refused("cli_computer_offline", {"name": "Studio"}),
            {"kind": "picked", "computer_id": None, "name": "Studio", "refusal": "cli_computer_offline"},
        ),
        (
            True, Refused("no_own_computer"),
            {"kind": "picked", "computer_id": None, "name": None, "refusal": "no_own_computer"},
        ),
        (True, OnCloud(Hosted("stored_token"), CLOUD), None),
        (False, OnOwn(STUDIO, picked=True), None),
    ],
)
async def test_the_computers_list_names_where_cloud_computers_cli_runs_while_claude_code_is_on(rows, where, shown):
    from src.server.app import claude_code_computers as route

    refusals = {"c-3": "needs_bigger_computer"}
    with (
        patch.object(route, "list_computers", AsyncMock(return_value=ClaudeCodeComputers(computers=[]))),
        patch.object(route, "cloud_view", AsyncMock(return_value=(refusals, where))),
        patch.object(route, "rows_on", AsyncMock(return_value=rows)),
    ):
        listed = await route.get_computers("user-1")
    assert (listed.cli_computer.model_dump() if listed.cli_computer else None) == shown
    assert listed.cloud_refusals == (refusals if rows else {})


# -- Where the user chose to store their token --


@pytest.mark.asyncio
async def test_a_user_who_chose_to_store_their_token_where_cloud_computers_run_none_is_refused_never_moved():
    """Nothing falls back from that choice to their own computers."""
    with platform(mode="stored", placement=False), patch(CLI, AsyncMock(return_value=(STUDIO, None))) as picked:
        where = await hosted_module.cli_placement("user-1", "ws-1")
        _, view = await hosted_module.cloud_view("user-1")
    assert where == view == Refused("needs_own_computer")
    picked.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_chief_of_staff_runs_a_claude_code_default_on_the_stored_token():
    """Its wakeups and automations run on that token as its other turns do,
    so a Home row naming Claude Code is saved."""
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    with platform(mode="stored"):
        assert await chief_of_staff_default_runs("user-1", HOME) == OnCloud(Hosted("stored_token"), CLOUD)
        await _check_claude_code_hosted("user-1", None, HOME, pref={"preferred_model": CODE})
        await _check_claude_code_hosted("user-1", CODE, HOME)
    with platform(mode="local", cli=(STUDIO, None)):
        assert await chief_of_staff_default_runs("user-1", HOME) == OnOwn(STUDIO, picked=True)


def _fresh(file, **kw):
    return MachineAnswer(file, checked_at=time.time(), **kw)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("file", "runs"),
    [
        pytest.param("works", True, id="signed_in"),
        pytest.param("unknown", True, id="unverified"),
        pytest.param("refused", False, id="rejected"),
        pytest.param("none", False, id="no_token"),
        pytest.param("policy", False, id="policy_blocked"),
        pytest.param("no_perl", False, id="needs_perl"),
        pytest.param("unsupported", False, id="computer_unsupported"),
    ],
)
async def test_the_chief_of_staff_default_runs_on_an_own_home_only_where_its_fresh_answer_lets_the_cli_run(file, runs):
    """The same rule admission refuses by, so Home on one of the user's own
    computers never runs a default its turn would then be refused."""
    with platform(mode="local", computer=OWN), patch(f"{SERVICE}.read_answer", AsyncMock(return_value=_fresh(file))):
        where = await chief_of_staff_default_runs("user-1", HOME)
    assert where == (OnOwn(OWN) if runs else None)


@pytest.mark.asyncio
async def test_the_chief_of_staff_default_runs_on_an_own_home_relayed_the_stored_token():
    with platform(mode="stored", computer=OWN), patch(f"{SERVICE}.read_answer", AsyncMock(return_value=None)):
        assert await chief_of_staff_default_runs("user-1", HOME) == OnOwn(OWN)
    with platform(mode="stored", computer=OWN), patch(f"{SERVICE}.read_answer", AsyncMock(return_value=_fresh("works"))):
        assert await chief_of_staff_default_runs("user-1", HOME) == OnOwn(OWN)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "mode", "token"), [(True, "local", "saved_token"), (False, "stored", "stored_token")],
)
async def test_the_chief_of_staff_default_runs_on_homes_cloud_computer_on_the_token_its_turns_run_on(
    backend, mode, token,
):
    """A self-hosted server's own saved token, or the one the user stored on
    the platform."""
    with platform(mode=mode, computer=CLOUD), patch(f"{HOSTED}.backend_available", lambda: backend):
        where = await chief_of_staff_default_runs("user-1", HOME)
    assert isinstance(where, OnCloud) and where.hosted == Hosted(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "on"), [(False, True), (True, False)], ids=["server", "account"])
async def test_whether_claude_code_is_off_is_the_callers_to_check_before_the_chief_of_staff_default_is_placed(rows, on):
    """Home's default is judged on where its CLI would run alone: the server
    and account switches are the caller's (``resolve_llm_config``), checked
    before this is asked."""
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    flags = AsyncMock(return_value={"claude_code_runtime": on})
    with (
        platform(mode="local", cli=(STUDIO, None)), patch(f"{SERVICE}.rows_available", lambda: rows),
        patch("src.server.services.features.effective_flags_for_user", flags),
        patch(f"{MACHINES}.read_answer", AsyncMock(return_value=None)),
    ):
        assert await chief_of_staff_default_runs("user-1", HOME) == OnOwn(STUDIO, picked=True)
        await _check_claude_code_hosted("user-1", None, HOME, pref={"preferred_model": CODE})
    flags.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_chief_of_staff_runs_no_claude_code_default_on_a_saved_token_known_not_to_work():
    with (
        platform(), patch(f"{HOSTED}.backend_available", lambda: True),
        patch("src.server.services.claude_code.saved.saved_refusal", AsyncMock(return_value="token_refused")),
    ):
        assert not await chief_of_staff_default_runs("user-1", HOME)
