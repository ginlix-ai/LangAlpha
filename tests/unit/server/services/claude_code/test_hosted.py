"""Claude Code on cloud computers on the platform: the user's own choice,
behind the operator's switch, decides where a turn runs and on what.

With the switch off the platform answers as before the choice existed, and
nothing reads a user's choice (the deploy bar). With it on, a user who chose
nothing runs none there, one who chose their API key runs on it once a key
is saved, up to their cap of turns at once, and the choice is read again as
the run starts. The CLI runs inside the computer's own sandbox, so there is
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
from src.server.database.claude_code_hosted import CLOUD_TURN, Custody
from src.server.services.claude_code import hosted as hosted_module
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.saved import token_hash
from src.server.services.computer_manager._providers import ProviderMixin
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
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
KEY = "sk-ant-api03-" + "k" * 40
SAVED = {"api_key": KEY, "base_url": None}


def sizer(sandbox):
    """A computer manager that sizes computers as one on ``sandbox`` would."""
    manager = SimpleNamespace(config=SimpleNamespace(to_core_config=lambda: SimpleNamespace(sandbox=sandbox)))
    manager._provider_settings = lambda kind, overrides: ProviderMixin._provider_settings(manager, kind, overrides)
    manager.tier_size = lambda computer, tier: ProviderMixin.tier_size(manager, computer, tier)
    return manager


@contextmanager
def platform(
    *, switch=True, stored=False, mode="local", blocked=False, key=True, live=0, computer=CLOUD, placement=True,
):
    """A platform deployment with the runtime on, the CLI placed in cloud
    computers' sandboxes (`placement`), the API-key switch (`switch`) and
    the stored-token one as given, and one user's choice, saved key, running
    turns and computer, which `reads.computer` hands the run's start."""
    saved = key if isinstance(key, dict) else ({"base_url": None} if key else None)
    reads = SimpleNamespace(
        mode=AsyncMock(return_value=Custody(mode, blocked)), key=AsyncMock(return_value=saved),
        live=AsyncMock(return_value=live), computer=AsyncMock(return_value=computer),
    )
    config = SimpleNamespace(
        hosted_api_key=switch, stored_custody=stored, hosted_turns_per_user=3, sandbox_placement=placement,
    )
    with ExitStack() as stack:
        for target, value in [
            ("src.config.settings.HOST_MODE", "platform"),
            (f"{HOSTED}.rows_available", lambda: True),
            (f"{HOSTED}.backend_available", lambda: False),
            (f"{HOSTED}.get_claude_code_config", lambda: config),
            (f"{HOSTED}.get_custody", reads.mode),
            (f"{HOSTED}.saved_key", reads.key),
            (f"{SERVICE}.rows_available", lambda: True),
            (f"{SERVICE}.get_claude_code_config", lambda: config),
            (f"{SERVICE}.live_cloud_turns", reads.live),
            ("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value=computer)),
            ("src.server.database.computer.get_computers_for_user", AsyncMock(return_value=[computer])),
            ("src.server.database.computer.get_computer", reads.computer),
            ("src.server.services.computer_manager.ComputerManager.get_instance", lambda: sizer(SandboxConfig())),
            ("src.server.services.computer_manager.ComputerManager.current", lambda: sizer(SandboxConfig())),
            (f"{SERVICE}.read_answer", AsyncMock(return_value=None)),
        ]:
            stack.enter_context(patch(target, value))
        yield reads


async def _admit():
    config = SimpleNamespace(feature_enabled=lambda _: True)
    try:
        await require_runnable(config, CODE, "analyst", user_id="user-1", workspace_id="ws-1")
    except HTTPException as e:
        return e.detail
    return None


# -- The deploy bar: the switch off is the platform as it was --


@pytest.mark.asyncio
async def test_with_the_switch_off_a_cloud_computer_is_refused_as_before_and_no_choice_is_read():
    with platform(switch=False) as reads:
        assert await _admit() == {
            "message": CLAUDE_CODE_OFF["needs_own_computer"], "type": "claude_code_off", "model": CODE,
            "reason": "needs_own_computer",
        }
        assert await hosted_module.custody("user-1") is None
    reads.mode.assert_not_awaited()
    reads.key.assert_not_awaited()
    reads.live.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_own_computer_never_reads_the_choice_for_cloud_computers():
    with platform(computer=OWN) as reads:
        assert await _admit() is None
    reads.mode.assert_not_awaited()


# -- Admission with the switch on --


@pytest.mark.asyncio
async def test_a_user_who_chose_nothing_runs_nothing_on_a_cloud_computer():
    with platform(mode="local"):
        detail = await _admit()
    assert detail["reason"] == "hosted_off"
    assert detail["message"] == (
        "Claude Code runs only on your own computers for your account, so workspaces on cloud computers "
        "can't use Claude Code models. Choose where it runs in Settings under Model access, or move this "
        "workspace to one of your computers."
    )


@pytest.mark.asyncio
async def test_the_api_key_choice_needs_a_saved_key():
    with platform(mode="api_key", key=False):
        detail = await _admit()
    assert detail["reason"] == "needs_api_key"
    assert detail["message"] == (
        "On cloud computers, Claude Code runs on the computer itself with your Anthropic API key, and you haven't "
        "saved one. Add one in Settings under Model access, or move this workspace to one of your computers."
    )


@pytest.mark.asyncio
async def test_a_key_set_to_another_endpoint_is_refused_since_the_worker_sends_it_to_anthropic_alone():
    with platform(mode="api_key", key={"base_url": "https://gateway.example"}):
        detail = await _admit()
    assert detail["reason"] == "api_key_endpoint"
    assert detail["message"] == (
        "Claude Code on cloud computers sends your Anthropic API key only to Anthropic, and yours is set to a "
        "custom Base URL. Clear the Base URL in Settings under Model access, or move this workspace to one of "
        "your computers."
    )


@pytest.mark.asyncio
async def test_admission_hands_on_what_the_turn_runs_on():
    config = SimpleNamespace(feature_enabled=lambda _: True)
    with platform(mode="api_key"):
        assert await require_runnable(config, CODE, "analyst", user_id="user-1", workspace_id="ws-1") == ("api_key", None)
    with platform(computer=OWN):
        assert await require_runnable(config, CODE, "analyst", user_id="user-1", workspace_id="ws-1") == (None, None)


@pytest.mark.asyncio
async def test_the_api_key_choice_with_a_key_runs_until_the_users_cap_of_turns_at_once():
    with platform(mode="api_key", live=2):
        assert await _admit() is None
    with platform(mode="api_key", live=3):
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
    with platform(mode="api_key", live=2) as reads:
        await require_runnable(config, CODE, "analyst", user_id="user-1", workspace_id="ws-1", thread_id="t-1")
    reads.live.assert_awaited_once_with("user-1", besides="t-1")


# -- As the run starts --


@pytest.mark.asyncio
async def test_the_run_reads_the_users_key_as_it_starts_for_its_api_key_descriptor():
    with platform(mode="api_key"), patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value=SAVED)) as read:
        credential = await HostedCredential("user-1", "api_key", "c-2").read()
    read.assert_awaited_once_with("user-1", "anthropic")
    assert credential == Credential(KEY, kind="api_key")
    assert KEY not in repr(credential)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "key", "stored", "reason"),
    [
        ("local", True, SAVED, "hosted_off"),
        ("api_key", False, SAVED, "needs_api_key"),
        ("api_key", True, None, "needs_api_key"),
        ("api_key", True, {"api_key": KEY, "base_url": "https://llm.example.com"}, "api_key_endpoint"),
    ],
)
async def test_a_run_whose_choice_or_key_changed_since_admission_never_starts_and_says_why(mode, key, stored, reason):
    with platform(mode=mode, key=key), patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value=stored)):
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", "api_key", "c-2").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == reason


@pytest.mark.asyncio
async def test_a_run_admitted_on_an_own_computer_that_starts_on_a_cloud_one_never_starts_uncounted():
    with platform(mode="api_key"), patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value=SAVED)) as read:
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", None, "c-2").read()
    read.assert_not_awaited()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "choice_changed"


@pytest.mark.asyncio
async def test_with_the_switch_turned_off_since_admission_the_run_is_refused_as_before():
    with platform(switch=False), pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "api_key", "c-2").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "needs_own_computer"


@pytest.mark.asyncio
async def test_a_refused_api_key_ends_the_turn_saying_so_and_is_never_flagged():
    credential = HostedCredential("user-1", "api_key", "c-2")
    with (
        patch(
            f"{LAUNCHERS}.cloud_placement",
            AsyncMock(return_value=hosted_module.Placement("sandbox", hosted=hosted_module.Hosted("api_key"))),
        ),
        patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value={"api_key": KEY, "base_url": None})),
        patch(f"{LAUNCHERS}.refuse", AsyncMock()) as flag,
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        assert await credential.read() == Credential(KEY, kind="api_key")
        await credential.refused()
    flag.assert_not_awaited()
    assert claude_code_refusal(refused.value, CODE) == {
        "message": (
            "Anthropic refused your API key, or its account has no credit left. Check the key in Settings under "
            "Model access."
        ),
        "type": "claude_code_off", "model": CODE, "reason": "api_key_refused",
    }
    assert KEY not in str(refused.value)


@pytest.mark.asyncio
async def test_a_refused_setup_token_is_still_flagged_and_ends_the_turn_saying_so():
    saved = {"access_token": "sk-ant-oat01-tok", "updated_at": "saved-at"}
    credential = HostedCredential("user-1", "saved_token", None)
    with (
        patch(
            f"{LAUNCHERS}.cloud_placement",
            AsyncMock(return_value=hosted_module.Placement("worker", hosted=hosted_module.Hosted("saved_token"))),
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
        (False, {"placement": True, "mode": "api_key"}, CLOUD, "sandbox"),
        (False, {"placement": True, "switch": False}, CLOUD, None),
        (False, {"placement": False, "mode": "api_key"}, CLOUD, None),
        (False, {"placement": True}, OWN, "own_computer"),
        (True, {"placement": True}, OWN, "own_computer"),
    ],
)
async def test_where_the_cli_runs_by_the_computer_and_this_servers_switches(backend, given, computer, runs_in):
    with platform(computer=computer, **given), patch(f"{HOSTED}.backend_available", lambda: backend):
        where = await hosted_module.placement("user-1", "ws-1")
        assert (where.own_computer is not None) == (runs_in == "own_computer")
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
        assert (await hosted_module.placement("user-1", "ws-1")).refusal == refusal


# -- The tier gate: the CLI runs inside the computer's own sandbox --

SMALL = {"computer_id": "c-3", "name": "Scratch", "kind": "daytona", "resource_tier": "standard"}
TOO_SMALL = (
    "Claude Code runs on Medium or larger cloud computers, and this workspace's computer is smaller. "
    "Change its spec or upgrade your LangAlpha plan, or move this workspace to a computer of your own."
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
    with platform(mode="api_key", computer=None):
        assert await _admit() is None


@pytest.mark.asyncio
async def test_with_the_cli_off_the_sandbox_the_platform_runs_none_on_cloud_computers_and_keeps_the_choice():
    """The choice stays the user's to change, since their stored token still
    signs in their own computers and leaving ``stored`` deletes it."""
    with platform(placement=False, mode="api_key", stored=True):
        assert (await _admit())["reason"] == "needs_own_computer"
        current = await hosted_module.custody("user-1")
        assert (current.mode, current.runs_on, current.refusal) == ("api_key", None, "needs_own_computer")
        assert hosted_module.relay_offered()


@pytest.mark.asyncio
@pytest.mark.parametrize(("placement", "refusal"), [(False, None), (True, "needs_bigger_computer")])
async def test_a_self_hosted_server_runs_the_saved_token_and_gates_the_size_only_with_the_cli_in_the_sandbox(
    placement, refusal,
):
    with platform(placement=placement, computer=SMALL), patch(f"{HOSTED}.backend_available", lambda: True):
        where = await hosted_module.placement("user-1", "ws-1")
    assert where.hosted == hosted_module.Hosted("saved_token")
    assert where.refusal == refusal


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
    with platform(mode="api_key", computer=SMALL):
        assert await _admit() == {
            "message": TOO_SMALL, "type": "claude_code_off", "model": CODE, "reason": "needs_bigger_computer",
        }


@pytest.mark.parametrize(("mode", "message"), [("platform", TOO_SMALL), ("oss", TOO_SMALL_SELF_HOSTED)])
def test_the_platform_fixes_a_computer_too_small_with_a_plan_and_a_self_hosted_server_with_its_size(mode, message):
    with patch("src.config.settings.HOST_MODE", mode):
        assert claude_code_off_message("needs_bigger_computer") == message
        assert claude_code_off_message("hosted_off") == CLAUDE_CODE_OFF["hosted_off"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "reason"),
    [
        ({"mode": "local"}, "hosted_off"),
        ({"mode": "api_key", "key": False}, "needs_api_key"),
        ({"mode": "api_key", "key": {"base_url": "https://gateway.example"}}, "api_key_endpoint"),
        ({"mode": "api_key", "switch": False, "stored": True}, "hosted_paused"),
        ({"mode": "api_key", "live": 3}, "needs_bigger_computer"),
    ],
)
async def test_the_size_is_told_only_once_the_credential_would_run(given, reason):
    with platform(computer=SMALL, **given):
        assert (await _admit())["reason"] == reason


@pytest.mark.asyncio
async def test_a_stored_token_known_not_to_work_is_told_before_the_size():
    refusal = "src.server.services.claude_code.saved.saved_refusal"
    with platform(mode="stored", stored=True, computer=SMALL), patch(refusal, AsyncMock(return_value="token_refused")):
        assert (await _admit())["reason"] == "token_refused"
    with platform(mode="stored", stored=True, computer=SMALL), patch(refusal, AsyncMock(return_value=None)):
        assert (await _admit())["reason"] == "needs_bigger_computer"


@pytest.mark.asyncio
async def test_a_computer_downsized_between_two_turns_of_a_thread_stops_the_second_as_it_starts():
    with platform(mode="api_key") as reads, patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value=SAVED)) as key:
        assert await _admit() is None
        assert await HostedCredential("user-1", "api_key", "c-2").read() == Credential(KEY, kind="api_key")
        reads.computer.return_value = {**CLOUD, "resource_tier": "standard"}
        key.reset_mock()
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", "api_key", "c-2").read()
        assert claude_code_refusal(refused.value, CODE) == {
            "message": TOO_SMALL, "type": "claude_code_off", "model": CODE, "reason": "needs_bigger_computer",
        }
    reads.computer.assert_awaited_with("c-2")
    key.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_credential_is_told_before_the_size_as_the_run_starts_too():
    with (
        platform(mode="local", computer=SMALL),
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await HostedCredential("user-1", "api_key", "c-3").read()
    assert refused.value.kind == "hosted_off"
    with (
        platform(mode="stored", stored=True, computer=SMALL),
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await HostedCredential("user-1", "api_key", "c-3").read()
    assert refused.value.kind == "choice_changed"


@pytest.mark.asyncio
async def test_the_run_reads_no_computer_where_its_size_cannot_refuse_it():
    saved = {"access_token": "sk-ant-oat01-tok", "updated_at": "saved-at"}
    with (
        platform(placement=False) as reads, patch(f"{HOSTED}.backend_available", lambda: True),
        patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=saved)),
    ):
        assert await HostedCredential("user-1", "saved_token", "c-2").read() == Credential("sk-ant-oat01-tok")
    reads.computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_custody_carries_the_credential_verdict_alone():
    with platform(mode="api_key"):
        current = await hosted_module.custody("user-1")
    assert current.runs_on == "api_key" and "computer_refusals" not in current.model_dump()


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
    with platform(mode="local"), patch("src.server.database.computer.get_computers_for_user", computers):
        assert await hosted_module.cloud_refusals("user-1") == {"c-3": "needs_bigger_computer"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "backend"),
    [({"placement": False}, False), ({"switch": False}, False), ({"placement": False}, True)],
)
async def test_no_cloud_computer_is_too_small_where_none_runs_claude_code_in_its_sandbox(given, backend):
    with platform(**given), patch(f"{HOSTED}.backend_available", lambda: backend):
        assert await hosted_module.cloud_refusals("user-1") == {}


@pytest.mark.asyncio
async def test_a_self_hosted_server_with_the_cli_in_the_sandbox_names_its_cloud_computers_too_small():
    with platform(computer=SMALL), patch(f"{HOSTED}.backend_available", lambda: True):
        assert await hosted_module.cloud_refusals("user-1") == {"c-3": "needs_bigger_computer"}


# -- The run's row, which the cap counts --


@pytest.mark.parametrize(
    ("mode", "hosted", "saved", "stamp"),
    [
        ("platform", "api_key", None, CLOUD_TURN),
        ("platform", None, None, {}),
        ("oss", "saved_token", "saved_token", CLOUD_TURN),
        ("oss", None, "saved_token", {}),
    ],
)
def test_a_turn_admitted_on_a_cloud_computer_is_counted_on_any_server(mode, hosted, saved, stamp):
    config = SimpleNamespace(claude_code_hosted=hosted, claude_code_saved=saved)
    with patch("src.config.settings.HOST_MODE", mode):
        assert run_stamp(config) == stamp


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
    with platform(switch=False) as reads, patch("src.server.services.claude_code.flow.rows_on", AsyncMock(return_value=True)):
        got = await client.get("/api/v1/claude-code/custody")
        put = await client.put("/api/v1/claude-code/custody", json={"mode": "api_key"})
    assert got.status_code == put.status_code == 404
    reads.mode.assert_not_awaited()


@pytest.mark.asyncio
async def test_settings_reads_and_saves_the_choice(client):
    saved = AsyncMock(return_value=None)
    with (
        platform(mode="local", key=False),
        patch("src.server.services.claude_code.flow.rows_on", AsyncMock(return_value=True)),
        patch(f"{HOSTED}.write_custody", saved),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[])),
    ):
        got = await client.get("/api/v1/claude-code/custody")
        put = await client.put("/api/v1/claude-code/custody", json={"mode": "api_key"})
        stored = await client.put("/api/v1/claude-code/custody", json={"mode": "stored"})
    assert got.json() == {
        "mode": "local", "api_key_saved": False, "offered": ["api_key"], "blocked": False, "runs_on": None,
        "refusal": "hosted_off", "disclosure": "2026-11-v1",
    }
    assert put.status_code == 200
    saved.assert_awaited_once_with("test-user-123", "api_key", block_reason=None)
    assert stored.status_code == 409, "storing a setup token is not offered with its switch off"


# -- Automations --


@pytest.mark.asyncio
async def test_an_automation_on_a_cloud_computer_is_refused_where_its_turns_would_be():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    with platform(mode="local"), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert (str(refused.value), refused.value.field) == (CLAUDE_CODE_OFF["hosted_off"], "llm_model")
    with platform(mode="local", computer=OWN):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    with platform(mode="api_key"):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    with platform(mode="api_key", computer=SMALL), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert (str(refused.value), refused.value.field) == (TOO_SMALL, "llm_model")
    with platform(mode="local", computer=SMALL), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert str(refused.value) == CLAUDE_CODE_OFF["hosted_off"], "the credential is told first"
    with platform(switch=False) as reads:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    reads.mode.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_automation_relayed_a_stored_token_is_refused_with_the_cli_off_the_sandbox():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    with (
        platform(mode="stored", stored=True, placement=False, computer=OWN),
        patch("src.server.services.automations.lifecycle.relayed_token", AsyncMock(return_value="stored_token")),
        patch("src.server.services.claude_code.machine.read_answer", AsyncMock(return_value=None)),
        pytest.raises(AutomationRefusal) as refused,
    ):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert str(refused.value) == CLAUDE_CODE_OFF["stored_unattended"]


@pytest.mark.asyncio
async def test_an_automation_checked_inside_a_save_reads_on_the_saves_own_connection():
    """A file save holds its connection and the user's locks through the
    check, so a read on a second pool slot could wait out the pool."""
    from src.server.services.automations.lifecycle import _check_claude_code_hosted

    conn = object()
    with (
        platform(mode="api_key") as reads,
        patch("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value=CLOUD)) as computer,
    ):
        await _check_claude_code_hosted("user-1", CODE, "ws-1", conn)
    computer.assert_awaited_once_with("ws-1", conn=conn)
    reads.mode.assert_awaited_once_with("user-1", conn=conn)
    reads.key.assert_awaited_once_with("user-1", "anthropic", conn=conn)
