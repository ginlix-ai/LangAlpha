"""Claude Code on cloud computers on the platform: the user's own choice,
behind the operator's switch, decides where a turn runs and on what.

With the switch off the platform answers as before the choice existed, and
nothing reads a user's choice (the deploy bar). With it on, a user who chose
nothing runs none there, one who stored their setup token runs on it, up to
their cap of turns at once, and the choice is read again as the run starts. The CLI runs inside the computer's own sandbox, so there is
no choice unless the operator placed it there, and a computer below the tier
it runs on is refused once the credential would run.
"""

from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from ptc_agent.agent.claude_code.launcher import Credential
from ptc_agent.config.core import SandboxConfig
from src.llms.claude_code import ClaudeTurnFailed
from src.server.app import claude_code as route
from src.server.database.claude_code_hosted import CLAUDE_CODE_TURN, CLI_COMPUTER, CLOUD_TURN, Custody
from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.claude_code import hosted as hosted_module
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.saved import token_hash
from src.server.services.computer_manager._providers import ProviderMixin
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
    Admitted,
    claude_code_off_message,
    claude_code_refusal,
    require_runnable,
    run_stamp,
)
from src.server.services.oauth_signin import OAuthRefusal
from tests.conftest import create_test_app

HOSTED = "src.server.services.claude_code.hosted"
SERVICE = "src.server.services.llm.claude_code"
LAUNCHERS = "src.server.services.claude_code.launchers"
CODE = "claude-haiku-5-5-code"
CLOUD = {"computer_id": "c-2", "name": "Research", "kind": "daytona", "resource_tier": "performance"}
OWN = {"computer_id": "c-1", "kind": "ssh"}
STUDIO = {"computer_id": "c-9", "name": "Studio", "kind": "ssh"}
TOKEN = "sk-ant-oat01-" + "t" * 40
ROW = {"access_token": TOKEN, "updated_at": "saved-at"}


@pytest.fixture(autouse=True)
def no_stored_task_runs():
    """No subagent run on the stored token unless a test says otherwise."""
    with patch(f"{HOSTED}.live_stored_task_runs", AsyncMock(return_value=[])) as runs:
        yield runs


def sizer(sandbox):
    """A computer manager that sizes computers as one on ``sandbox`` would."""
    manager = SimpleNamespace(config=SimpleNamespace(to_core_config=lambda: SimpleNamespace(sandbox=sandbox)))
    manager._provider_settings = lambda kind, overrides: ProviderMixin._provider_settings(manager, kind, overrides)
    manager.tier_size = lambda computer, tier: ProviderMixin.tier_size(manager, computer, tier)
    return manager


@contextmanager
def platform(
    *, stored=True, mode="local", blocked=False, live=0, computer=CLOUD, placement=True, cli=(None, None), memory=800,
):
    """A platform deployment with the runtime on, the CLI placed in cloud
    computers' sandboxes (``placement``), storing a setup token offered as
    given (``stored``), a stored token known to work, and one user's choice,
    running turns and computer, which ``reads.computer`` hands the run's start.
    ``memory`` is the operator's count of each cloud CLI's memory.
    ``cli`` is the own computer picked to run the CLI where cloud computers
    run none, and the offline one's name (``machine.cli_computer``)."""
    reads = SimpleNamespace(
        mode=AsyncMock(return_value=Custody(mode, blocked)), live=AsyncMock(return_value=live),
        computer=AsyncMock(return_value=computer),
    )
    config = SimpleNamespace(
        stored_custody=stored, hosted_turns_per_user=3, sandbox_placement=placement, cli_memory_mb=memory,
    )
    with ExitStack() as stack:
        for target, value in [
            ("src.config.settings.HOST_MODE", "platform"),
            (f"{HOSTED}.rows_available", lambda: True),
            (f"{HOSTED}.backend_available", lambda: False),
            (f"{HOSTED}.get_claude_code_config", lambda: config),
            (f"{HOSTED}.get_custody", reads.mode),
            ("src.server.services.claude_code.saved.saved_refusal", AsyncMock(return_value=None)),
            (f"{SERVICE}.rows_available", lambda: True),
            (f"{SERVICE}.get_claude_code_config", lambda: config),
            (f"{SERVICE}.live_cloud_turns", reads.live),
            ("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value=computer)),
            ("src.server.database.computer.get_computers_for_user", AsyncMock(return_value=[computer])),
            ("src.server.database.computer.get_computer", reads.computer),
            ("src.server.services.computer_manager.ComputerManager.get_instance", lambda: sizer(SandboxConfig())),
            ("src.server.services.computer_manager.ComputerManager.current", lambda: sizer(SandboxConfig())),
            (f"{SERVICE}.read_answer", AsyncMock(return_value=None)),
            ("src.server.services.claude_code.machine.pick_cli_computer", AsyncMock(return_value=cli)),
        ]:
            stack.enter_context(patch(target, value))
        yield reads


async def _admit():
    config = SimpleNamespace(feature_enabled=lambda _: True)
    try:
        await require_runnable(config, CODE, user_id="user-1", workspace_id="ws-1")
    except HTTPException as e:
        return e.detail
    return None


# -- The deploy bar: the switch off is the platform as it was --


@pytest.mark.asyncio
async def test_with_the_switch_off_a_cloud_computer_runs_its_cli_on_an_own_computer_and_no_choice_is_read():
    with platform(stored=False) as reads:
        assert await _admit() == {
            "message": CLAUDE_CODE_OFF["no_own_computer"], "type": "claude_code_off", "model": CODE,
            "reason": "no_own_computer",
        }
    with platform(stored=False, cli=(STUDIO, None)) as reads:
        assert await _admit() is None
        assert await hosted_module.custody("user-1") is None
    reads.mode.assert_not_awaited()
    reads.live.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_own_computer_never_reads_the_choice_for_cloud_computers():
    """Where no stored token is relayed to it."""
    with platform(computer=OWN, stored=False) as reads:
        assert await _admit() is None
    reads.mode.assert_not_awaited()


# -- Admission with the switch on --


@pytest.mark.asyncio
async def test_a_user_who_chose_their_own_computers_runs_a_cloud_workspaces_cli_on_one_or_is_told_which():
    with platform(mode="local"):
        detail = await _admit()
    assert detail["reason"] == "no_own_computer"
    assert detail["message"] == (
        "Claude Code runs on your own computers, and none of yours with its own setup token is online now. "
        "Turn one on, or add a token for one in Settings under Model access."
    )
    with platform(mode="local", cli=(None, "Studio")):
        detail = await _admit()
    assert detail == {
        "message": "Studio, where Claude Code runs for you, is offline. Turn it on, or pick another model.",
        "type": "claude_code_off", "model": CODE, "reason": "cli_computer_offline", "name": "Studio",
    }




@pytest.mark.asyncio
async def test_admission_hands_on_what_the_turn_runs_on():
    config = SimpleNamespace(feature_enabled=lambda _: True)
    with platform(mode="stored"):
        assert await require_runnable(config, CODE, user_id="user-1", workspace_id="ws-1") == Admitted(
            "stored_token", "stored_token",
        )
    with platform(computer=OWN):
        assert await require_runnable(config, CODE, user_id="user-1", workspace_id="ws-1") == Admitted()
    with platform(mode="local", cli=(STUDIO, None)):
        admitted = await require_runnable(config, CODE, user_id="user-1", workspace_id="ws-1")
        assert admitted == Admitted(cli_computer="c-9")


@pytest.mark.asyncio
async def test_the_stored_choice_runs_until_the_users_cap_of_turns_at_once():
    with platform(mode="stored", live=2):
        assert await _admit() is None
    with platform(mode="stored", live=3):
        detail = await _admit()
    assert detail == {
        "message": (
            "You've reached the limit on Claude Code threads running at once on cloud computers (3). Try again "
            "when one finishes."
        ),
        "type": "claude_code_off", "model": CODE, "reason": "too_many_turns", "limit": 3,
    }


@pytest.mark.asyncio
async def test_a_message_to_a_thread_does_not_count_that_threads_own_run_against_the_cap():
    config = SimpleNamespace(feature_enabled=lambda _: True)
    with platform(mode="stored", live=2) as reads:
        await require_runnable(config, CODE, user_id="user-1", workspace_id="ws-1", thread_id="t-1")
    reads.live.assert_awaited_once_with("user-1", besides="t-1")


# -- As the run starts --



@pytest.mark.asyncio
async def test_a_run_whose_choice_changed_since_admission_never_starts_and_says_why():
    """Sent again, it runs on the user's own computer."""
    with platform(mode="local"), patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=ROW)) as read:
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", "stored_token", "c-2").read()
    read.assert_not_awaited()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "choice_changed"


@pytest.mark.asyncio
async def test_a_run_admitted_on_an_own_computer_that_starts_on_a_cloud_one_never_starts_uncounted():
    with platform(mode="stored"), patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=ROW)) as read:
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", None, "c-2").read()
    read.assert_not_awaited()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "choice_changed"


@pytest.mark.asyncio
async def test_with_the_switch_turned_off_since_admission_the_run_is_refused_to_be_sent_again():
    with platform(stored=False), pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "stored_token", "c-2").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "choice_changed"



@pytest.mark.asyncio
async def test_a_refused_setup_token_is_flagged_and_ends_the_turn_saying_so():
    saved = {"access_token": "sk-ant-oat01-tok", "updated_at": "saved-at"}
    credential = HostedCredential("user-1", "saved_token", None)
    with (
        patch(
            f"{LAUNCHERS}.cli_placement",
            AsyncMock(return_value=hosted_module.OnCloud(hosted_module.Hosted("saved_token"))),
        ),
        patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=saved)),
        patch(f"{LAUNCHERS}.refuse", AsyncMock()) as flag,
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await credential.read()
        await credential.refused()
    flag.assert_awaited_once_with("user-1", token_hash("sk-ant-oat01-tok"), "saved-at")
    assert claude_code_refusal(refused.value, CODE)["reason"] == "token_refused"


# -- Where the CLI runs, decided once --


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "given", "computer", "runs_in"),
    [
        (True, {"placement": False}, CLOUD, "worker"),
        (True, {"placement": True}, CLOUD, "sandbox"),
        (False, {"placement": True, "mode": "stored"}, CLOUD, "sandbox"),
        (False, {"placement": True, "stored": False}, CLOUD, None),
        (False, {"placement": False, "mode": "stored"}, CLOUD, None),
        (False, {"placement": True}, OWN, "own_computer"),
        (True, {"placement": True}, OWN, "own_computer"),
    ],
)
async def test_where_the_cli_runs_by_the_computer_and_this_servers_switches(backend, given, computer, runs_in):
    with platform(computer=computer, **given), patch(f"{HOSTED}.backend_available", lambda: backend):
        where = await hosted_module.cli_placement("user-1", "ws-1")
        assert isinstance(where, hosted_module.OnOwn) == (runs_in == "own_computer")
        if computer is CLOUD:
            assert hosted_module.cloud_runs_in() == runs_in


@pytest.mark.asyncio
@pytest.mark.parametrize(("placement", "refusal"), [(False, "policy_blocked"), (True, None)])
async def test_only_the_worker_placement_minds_an_organizations_policy_on_the_worker(placement, refusal):
    with (
        platform(placement=placement),
        patch(f"{HOSTED}.backend_available", lambda: True),
        patch("ptc_agent.core.claude_code_managed.local_managed", AsyncMock(return_value=["managed-settings.json"])),
    ):
        where = await hosted_module.cli_placement("user-1", "ws-1")
        assert (where.reason if isinstance(where, hosted_module.Refused) else None) == refusal


# -- The tier gate: the CLI runs inside the computer's own sandbox --

SMALL = {"computer_id": "c-3", "name": "Scratch", "kind": "daytona", "resource_tier": "standard"}
TOO_SMALL = (
    "Claude Code runs on Medium or larger cloud computers, and this workspace's computer is smaller. Change its "
    "spec or upgrade your LangAlpha plan, or choose to run Claude Code on your own computers in Settings under "
    "Model access."
)
TOO_SMALL_SELF_HOSTED = (
    "Claude Code runs on Medium or larger cloud computers, and this workspace's computer is smaller. "
    "Change its spec, or move this workspace to a computer of your own."
)


def _resizing(tier, from_tier, state="in_progress", *, error=None, stale=False):
    change = {"target_tier": tier, "from_tier": from_tier, "state": state, "started_at": "2026-11-04T00:00:00Z"}
    if error:
        change["error"] = {"code": error, "message": "x"}
    return {**CLOUD, "resource_tier": tier, "spec_change": change, "spec_change_stale": stale}


@pytest.mark.parametrize(
    ("computer", "min_tier", "refusal"),
    [
        ({"kind": "daytona", "resource_tier": "performance"}, "performance", None),
        ({"kind": "daytona", "resource_tier": "max"}, "performance", None),
        ({"kind": "daytona", "resource_tier": "standard"}, "performance", "needs_bigger_computer"),
        ({"kind": "daytona", "resource_tier": "retired"}, "performance", "needs_bigger_computer"),
        ({"kind": "daytona", "resource_tier": "performance"}, "max", "needs_bigger_computer"),
        ({"kind": "e2b", "resource_tier": "xlarge"}, "xlarge", None),
        ({"kind": "e2b", "resource_tier": "max"}, "xlarge", "needs_bigger_computer"),
        (_resizing("performance", "standard"), "performance", "needs_bigger_computer"),
        (_resizing("standard", "performance"), "performance", "needs_bigger_computer"),
        (_resizing("performance", "standard", state="succeeded"), "performance", None),
        (_resizing("performance", "standard", stale=True), "performance", "needs_bigger_computer"),
        (_resizing("performance", "standard", state="failed", error="interrupted"), "performance", "needs_bigger_computer"),
        (_resizing("performance", "standard", state="failed", error="unknown"), "performance", None),
    ],
)
def test_a_computer_fits_where_the_tier_its_sandbox_runs_at_reaches_the_smallest_in_cpu_and_memory(
    computer, min_tier, refusal,
):
    with platform(), patch(f"{HOSTED}.SANDBOX_MIN_TIER", min_tier):
        assert hosted_module.tier_refusal(computer) == refusal


@pytest.mark.parametrize(
    ("computer", "cap"),
    [
        (OWN, 4),
        ({**CLOUD, "resource_tier": "performance"}, 5),
        ({**CLOUD, "resource_tier": "max"}, 10),
        ({"computer_id": "c-3", "kind": "docker", "resource_tier": "standard"}, 5),
        (_resizing("max", "performance"), 5),
        ({**CLOUD, "resource_tier": "retired"}, None),
    ],
)
def test_a_computer_runs_a_cli_per_800_mb_of_its_smallest_size_and_an_own_one_as_many_as_its_slots(computer, cap):
    """Turns and subagents together, wherever the CLIs run; a size the server
    cannot tell caps nothing."""
    with platform():
        assert hosted_module.cli_cap(computer) == cap


@pytest.mark.parametrize(
    ("config", "cap"),
    [({"claude_code_clis": 6}, 6), ({"claude_code_clis": 8}, 8), ({"claude_code_clis": 1}, 1), ({}, 4), (None, 4),
     ({"claude_code_clis": 9}, 4), ({"claude_code_clis": 0}, 4), ({"claude_code_clis": "6"}, 4),
     ({"claude_code_clis": True}, 4)],
)
def test_an_own_computer_runs_as_many_clis_as_its_owner_set_for_it(config, cap):
    """A count that is not one Settings could have saved reads as the default."""
    with platform():
        assert hosted_module.cli_cap({**OWN, "config": config}) == cap


@pytest.mark.parametrize(("memory", "cap"), [(400, 10), (1600, 2), (8192, 1)], ids=["400", "1600", "past-its-memory"])
def test_a_cloud_computer_runs_as_many_clis_as_the_memory_the_operator_counts_each_at_fits(memory, cap):
    with platform(memory=memory):
        assert hosted_module.cli_cap({**CLOUD, "resource_tier": "performance"}) == cap


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("computer", "clis"),
    [
        ({**CLOUD, "resource_tier": "performance"}, 5),
        ({**OWN, "config": {"claude_code_clis": 6}}, 6),
        ({**CLOUD, "resource_tier": "retired"}, 4),
        (None, 4),
    ],
    ids=["cloud", "own", "size-unknown", "gone"],
)
async def test_a_cli_starting_on_a_computer_is_held_to_its_cap_and_one_unsized_or_gone_to_the_default(computer, clis):
    with platform(computer=computer):
        assert await hosted_module.computer_clis("c-x") == clis


def test_the_memory_each_cloud_cli_is_counted_at_is_read_from_the_claude_code_config_section():
    from src.config.models import ClaudeCodeConfig

    assert ClaudeCodeConfig().cli_memory_mb == 800
    assert ClaudeCodeConfig(cli_memory_mb=512).cli_memory_mb == 512
    with pytest.raises(ValueError):
        ClaudeCodeConfig(cli_memory_mb=64)


@pytest.mark.parametrize("tier", ["standard", "max", "retired"])
def test_where_the_clis_run_in_the_backend_a_computer_runs_as_many_as_a_medium_one(tier):
    """Its own size is not theirs there."""
    with platform(), patch(f"{HOSTED}.in_sandbox", lambda: False):
        assert hosted_module.cli_cap({**CLOUD, "resource_tier": tier}) == 5


def test_a_kind_this_server_cannot_size_fails_rather_than_blame_the_computer():
    with platform(), pytest.raises(ValueError):
        hosted_module.tier_refusal({"kind": "mainframe", "resource_tier": "max"})


@pytest.mark.parametrize(
    ("kind", "tier", "refusal"),
    [("docker", "standard", None), ("docker", None, None), ("docker", "retired", None), ("daytona", "standard", "needs_bigger_computer")],
)
def test_a_computer_is_sized_as_its_provider_builds_it(kind, tier, refusal):
    """A Docker container is floored at the operator's own size, Medium's by
    default, so its smaller tiers still run Claude Code."""
    with platform():
        assert hosted_module.tier_refusal({"kind": kind, "resource_tier": tier}) == refusal


ALL_KINDS = SandboxConfig(offered_providers=("daytona", "e2b", "docker"))


@pytest.mark.parametrize(
    ("placement", "sandbox", "min_tier", "fails"),
    [
        (True, ALL_KINDS, "xlarge", True),
        (True, ALL_KINDS, "performance", False),
        (False, ALL_KINDS, "xlarge", False),
        (True, SandboxConfig(provider="docker"), "no-such-tier", False),
    ],
)
def test_a_server_whose_cloud_computers_cannot_be_medium_does_not_start_with_the_cli_in_their_sandboxes(
    placement, sandbox, min_tier, fails,
):
    """A Docker container is floored at the operator's own size, so a Docker
    server sizes one whatever its tiers are named."""
    with platform(placement=placement), patch(f"{HOSTED}.SANDBOX_MIN_TIER", min_tier):
        if not fails:
            hosted_module.check_sandbox_tiers(sandbox)
            return
        with pytest.raises(RuntimeError, match=min_tier):
            hosted_module.check_sandbox_tiers(sandbox)


@pytest.mark.asyncio
async def test_a_workspace_on_no_computer_yet_is_sized_as_its_run_starts_not_at_admission():
    with platform(mode="stored", computer=None):
        assert await _admit() is None


@pytest.mark.asyncio
async def test_with_the_cli_off_the_sandbox_the_platform_runs_none_on_cloud_computers_and_keeps_the_choice():
    """The choice stays the user's to change, since their stored token still
    signs in their own computers and leaving ``stored`` deletes it."""
    with platform(placement=False, mode="stored"):
        assert (await _admit())["reason"] == "needs_own_computer"
        current = await hosted_module.custody("user-1")
        assert (current.mode, current.runs_on, current.refusal) == ("stored", None, "needs_own_computer")
        assert hosted_module.relay_offered()


@pytest.mark.asyncio
@pytest.mark.parametrize(("placement", "refusal"), [(False, None), (True, "needs_bigger_computer")])
async def test_a_self_hosted_server_runs_the_saved_token_and_gates_the_size_only_with_the_cli_in_the_sandbox(
    placement, refusal,
):
    with platform(placement=placement, computer=SMALL), patch(f"{HOSTED}.backend_available", lambda: True):
        where = await hosted_module.cli_placement("user-1", "ws-1")
    assert where.hosted == hosted_module.Hosted("saved_token")
    assert (where.reason if isinstance(where, hosted_module.Refused) else None) == refusal


@pytest.mark.asyncio
@pytest.mark.parametrize("placement", [True, False])
async def test_a_self_hosted_server_caps_the_saved_tokens_turns_at_once_wherever_their_cli_runs(placement):
    """The cap is the one bound on the CLIs a cloud computer's sandbox or a worker runs at once."""
    for live, reason in [(2, None), (3, "too_many_turns")]:
        with (
            platform(placement=placement, live=live),
            patch("src.config.settings.HOST_MODE", "oss"),
            patch(f"{HOSTED}.backend_available", lambda: True),
            patch("src.server.services.claude_code.saved.saved_refusal", AsyncMock(return_value=None)),
            patch("ptc_agent.core.claude_code_managed.local_managed", AsyncMock(return_value=())),
        ):
            detail = await _admit()
        assert (detail and detail["reason"]) == reason


@pytest.mark.asyncio
async def test_a_computer_below_the_tier_is_refused_at_admission_saying_how_to_fix_it():
    with platform(mode="stored", computer=SMALL):
        assert await _admit() == {
            "message": TOO_SMALL, "type": "claude_code_off", "model": CODE, "reason": "needs_bigger_computer",
            "home": False, "computer_id": "c-3", "name": "Scratch",
        }


@pytest.mark.parametrize(
    ("mode", "message", "home"),
    [
        ("platform", TOO_SMALL, TOO_SMALL.replace("this workspace's computer", "Home's computer")),
        (
            "oss", TOO_SMALL_SELF_HOSTED,
            "Claude Code runs on Medium or larger cloud computers, and Home's computer is smaller. Change its spec.",
        ),
    ],
)
def test_the_platform_fixes_a_computer_too_small_with_a_plan_and_a_self_hosted_server_with_its_size(mode, message, home):
    """Home moves to no other computer, so a self-hosted server offers only its spec."""
    with patch("src.config.settings.HOST_MODE", mode):
        assert claude_code_off_message("needs_bigger_computer") == message
        assert claude_code_off_message("needs_bigger_computer", home=True) == home


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "reason"),
    [
        ({"mode": "local"}, "no_own_computer"),
        ({"mode": "stored", "blocked": True}, "no_own_computer"),
        ({"mode": "stored", "live": 3}, "needs_bigger_computer"),
    ],
)
async def test_the_size_is_told_only_once_the_credential_would_run(given, reason):
    """On the user's own computers, a cloud computer's size is never theirs."""
    with platform(computer=SMALL, **given):
        assert (await _admit())["reason"] == reason


@pytest.mark.asyncio
async def test_a_stored_token_known_not_to_work_is_told_before_the_size():
    refusal = "src.server.services.claude_code.saved.saved_refusal"
    with platform(mode="stored", computer=SMALL), patch(refusal, AsyncMock(return_value="token_refused")):
        assert (await _admit())["reason"] == "token_refused"
    with platform(mode="stored", computer=SMALL), patch(refusal, AsyncMock(return_value=None)):
        assert (await _admit())["reason"] == "needs_bigger_computer"


@pytest.mark.asyncio
async def test_a_computer_downsized_between_two_turns_of_a_thread_stops_the_second_as_it_starts():
    downsized = AsyncMock(return_value={**CLOUD, "resource_tier": "standard"})
    with platform(mode="stored"), patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=ROW)) as token:
        assert await _admit() is None
        assert await HostedCredential("user-1", "stored_token", "ws-1").read() == Credential(TOKEN)
        token.reset_mock()
        with (
            patch("src.server.database.computer.get_computer_for_workspace", downsized),
            pytest.raises(ClaudeTurnFailed) as refused,
        ):
            await HostedCredential("user-1", "stored_token", "ws-1").read()
        assert claude_code_refusal(refused.value, CODE) == {
            "message": TOO_SMALL, "type": "claude_code_off", "model": CODE, "reason": "needs_bigger_computer",
            "home": False, "computer_id": "c-2", "name": "Research",
        }
    downsized.assert_awaited_with("ws-1", conn=None)
    token.assert_not_awaited()


@pytest.mark.asyncio
async def test_home_downsized_since_its_turn_was_admitted_is_refused_as_it_starts_naming_homes_computer():
    """Home moves to no other computer, so the words name its own."""
    with platform(mode="stored", computer={**CLOUD, "resource_tier": "standard"}):
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", "stored_token", get_home_workspace_id("user-1")).read()
        words = claude_code_off_message("needs_bigger_computer", home=True)
        refusal = claude_code_refusal(refused.value, CODE)
    assert "Home's computer is smaller" in words
    assert refusal == {
        "message": words, "type": "claude_code_off", "model": CODE, "reason": "needs_bigger_computer", "home": True,
        "computer_id": "c-2", "name": "Research",
    }


@pytest.mark.asyncio
async def test_the_credential_is_told_before_the_size_as_the_run_starts_too():
    with (
        platform(mode="local", computer=SMALL),
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await HostedCredential("user-1", "stored_token", "c-3").read()
    assert refused.value.kind == "choice_changed"
    with (
        platform(mode="stored", computer=SMALL),
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await HostedCredential("user-1", "saved_token", "c-3").read()
    assert refused.value.kind == "choice_changed"


@pytest.mark.asyncio
async def test_the_run_is_not_sized_where_its_size_cannot_refuse_it():
    saved = {"access_token": "sk-ant-oat01-tok", "updated_at": "saved-at"}
    with (
        platform(placement=False, computer=SMALL), patch(f"{HOSTED}.backend_available", lambda: True),
        patch("ptc_agent.core.claude_code_managed.local_managed", AsyncMock(return_value=())),
        patch(f"{HOSTED}.tier_refusal") as sized,
        patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=saved)),
    ):
        assert await HostedCredential("user-1", "saved_token", "ws-1").read() == Credential("sk-ant-oat01-tok")
    sized.assert_not_called()


@pytest.mark.asyncio
async def test_custody_carries_the_credential_verdict_alone():
    with platform(mode="stored"):
        current = await hosted_module.custody("user-1")
    assert current.runs_on == "stored_token" and "computer_refusals" not in current.model_dump()


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "listed"), [(True, {"c-3": "needs_bigger_computer"}), (False, {})])
async def test_the_computers_list_names_each_cloud_computer_too_small_while_claude_code_is_on(rows, listed):
    from src.server.app import claude_code_computers as computers_route
    from src.server.models.claude_code import ClaudeCodeComputers

    computers = AsyncMock(return_value=[CLOUD, SMALL, OWN])
    with (
        platform(), patch("src.server.database.computer.get_computers_for_user", computers),
        patch(f"{HOSTED}.backend_available", lambda: True),
        patch.object(computers_route, "list_computers", AsyncMock(return_value=ClaudeCodeComputers(computers=[]))),
        patch.object(computers_route, "rows_on", AsyncMock(return_value=rows)),
    ):
        assert (await computers_route.get_computers("user-1")).cloud_refusals == listed


@pytest.mark.asyncio
async def test_each_cloud_computer_is_read_by_the_rule_admission_gives_it():
    unsized = {"computer_id": "c-9", "kind": "mainframe", "resource_tier": "max"}
    computers = AsyncMock(return_value=[CLOUD, SMALL, OWN, unsized])
    with platform(mode="stored"), patch("src.server.database.computer.get_computers_for_user", computers):
        refusals, _ = await hosted_module.cloud_view("user-1")
    assert refusals == {"c-3": "needs_bigger_computer"}


@pytest.mark.asyncio
@pytest.mark.parametrize("given", [{"mode": "local"}, {"mode": "stored", "blocked": True}, {"stored": False}])
async def test_no_cloud_computer_is_too_small_where_their_cli_runs_on_the_users_own_computers(given):
    """A small cloud computer refuses nothing there: its turns' CLIs run on an own computer."""
    computers = AsyncMock(return_value=[CLOUD, SMALL, OWN])
    with platform(**given), patch("src.server.database.computer.get_computers_for_user", computers):
        refusals, where = await hosted_module.cloud_view("user-1")
    assert refusals == {}
    assert hosted_module.picks_own(where)
    computers.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["local", "stored"])
async def test_the_setup_names_each_cloud_computer_too_small_whatever_the_user_chose(mode):
    """So choosing to run it on cloud computers can be warned of before it is made."""
    computers = AsyncMock(return_value=[CLOUD, SMALL, OWN])
    with platform(mode=mode), patch("src.server.database.computer.get_computers_for_user", computers):
        assert await hosted_module.tier_refusals("user-1") == {"c-3": "needs_bigger_computer"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "backend"),
    [({"placement": False}, False), ({"stored": False}, False), ({"placement": False}, True)],
)
async def test_no_cloud_computer_is_too_small_where_none_runs_claude_code_in_its_sandbox(given, backend):
    with (
        platform(**given), patch(f"{HOSTED}.backend_available", lambda: backend),
        patch("ptc_agent.core.claude_code_managed.local_managed", AsyncMock(return_value=())),
    ):
        refusals, _ = await hosted_module.cloud_view("user-1")
    assert refusals == {}


@pytest.mark.asyncio
async def test_a_self_hosted_server_with_the_cli_in_the_sandbox_names_its_cloud_computers_too_small():
    with platform(computer=SMALL), patch(f"{HOSTED}.backend_available", lambda: True):
        refusals, _ = await hosted_module.cloud_view("user-1")
    assert refusals == {"c-3": "needs_bigger_computer"}


# -- The run's row, which the cap counts --


@pytest.mark.parametrize(
    ("mode", "claude_code", "hosted", "saved", "stamp"),
    [
        ("platform", True, "stored_token", None, {**CLAUDE_CODE_TURN, **CLOUD_TURN}),
        ("platform", True, None, None, CLAUDE_CODE_TURN),
        ("oss", True, "saved_token", "saved_token", {**CLAUDE_CODE_TURN, **CLOUD_TURN}),
        ("oss", True, None, "saved_token", CLAUDE_CODE_TURN),
        ("oss", False, None, None, {}),
    ],
)
def test_a_turn_admitted_on_a_cloud_computer_is_counted_on_any_server(mode, claude_code, hosted, saved, stamp):
    """Every Claude Code turn is counted as a CLI its computer runs, and one
    on a cloud computer as a turn of its user's there too."""
    config = SimpleNamespace(claude_code=claude_code, claude_code_admitted=Admitted(hosted, saved))
    with patch("src.config.settings.HOST_MODE", mode):
        assert run_stamp(config) == stamp


def test_a_turn_whose_cli_runs_on_an_own_computer_for_another_names_that_computer():
    """So that computer counts it as one of its CLIs, and the workspace's does not."""
    config = SimpleNamespace(claude_code=True, claude_code_admitted=Admitted(cli_computer="c-9"))
    assert run_stamp(config) == {**CLAUDE_CODE_TURN, CLI_COMPUTER: "c-9"}


# -- Settings --


@pytest_asyncio.fixture
async def client():
    app = create_test_app(route.router)
    app.add_exception_handler(
        OAuthRefusal, lambda _, exc: JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code)
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_the_choice_is_not_offered_with_the_switch_off(client):
    with platform(stored=False) as reads, patch("src.server.services.claude_code.flow.rows_on", AsyncMock(return_value=True)):
        got = await client.get("/api/v1/claude-code/custody")
        put = await client.put("/api/v1/claude-code/custody", json={"mode": "local"})
    assert got.status_code == put.status_code == 404
    reads.mode.assert_not_awaited()


@pytest.mark.asyncio
async def test_settings_reads_and_saves_the_choice(client):
    saved = AsyncMock(return_value=None)
    with (
        platform(mode="local"),
        patch("src.server.services.claude_code.flow.rows_on", AsyncMock(return_value=True)),
        patch(f"{HOSTED}.write_custody", saved),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[])),
    ):
        got = await client.get("/api/v1/claude-code/custody")
        put = await client.put("/api/v1/claude-code/custody", json={"mode": "local"})
        api_key = await client.put("/api/v1/claude-code/custody", json={"mode": "api_key"})
    assert got.json() == {
        "mode": "local", "offered": ["stored"], "blocked": False, "runs_on": None,
        "refusal": "hosted_off",
    }
    assert put.status_code == 200
    saved.assert_awaited_once_with("test-user-123", "local", block_reason=None)
    assert api_key.status_code == 422, "the API key choice is gone"


# -- Automations --


@pytest.mark.asyncio
async def test_an_automation_on_a_cloud_computer_is_refused_where_its_turns_would_be():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    for cli in [(None, None), (None, "Studio"), (STUDIO, None)]:
        with platform(mode="local", computer=SMALL, cli=cli):
            await _check_claude_code_hosted("user-1", CODE, "ws-1")
    with platform(mode="local", computer=OWN):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    with platform(mode="stored"):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    with platform(mode="stored", computer=SMALL), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert (str(refused.value), refused.value.field) == (TOO_SMALL, "llm_model")
    with (
        platform(computer=SMALL), patch("src.config.settings.HOST_MODE", "oss"),
        patch(f"{HOSTED}.backend_available", lambda: True), pytest.raises(AutomationRefusal) as refused,
    ):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert (str(refused.value), refused.value.field) == (TOO_SMALL_SELF_HOSTED, "llm_model")
    with platform(stored=False) as reads:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    reads.mode.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_automation_relayed_a_stored_token_is_saved_with_the_cli_off_the_sandbox():
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    with (
        platform(mode="stored", placement=False, computer=OWN),
        patch("src.server.services.claude_code.machine.read_answer", AsyncMock(return_value=None)),
    ):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")


@pytest.mark.asyncio
async def test_an_automation_checked_inside_a_save_reads_on_the_saves_own_connection():
    """A file save holds its connection and the user's locks through the
    check, so a read on a second pool slot could wait out the pool."""
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    conn = object()
    with (
        platform(mode="stored") as reads,
        patch("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value=SMALL)) as computer,
        pytest.raises(AutomationRefusal),
    ):
        await _check_claude_code_hosted("user-1", CODE, "ws-1", conn)
    computer.assert_awaited_once_with("ws-1", conn=conn)
    reads.mode.assert_awaited_once_with("user-1", conn=conn)


def test_the_web_sends_the_disclosure_version_of_the_copy_it_shows():
    """The consent dialog submits its own bundle's version, so the two move together."""
    from pathlib import Path

    from src.server.services.claude_code.hosted import DISCLOSURE

    web = (Path(__file__).parents[5] / "web" / "src" / "lib" / "claudeCode.ts").read_text()
    assert f"export const CLAUDE_CODE_DISCLOSURE = '{DISCLOSURE}';" in web
