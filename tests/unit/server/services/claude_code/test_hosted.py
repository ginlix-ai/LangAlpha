"""Claude Code on cloud computers on the platform: the user's own choice,
behind the operator's switch, decides where a turn runs and on what.

With the switch off the platform answers as before the choice existed, and
nothing reads a user's choice (the deploy bar). With it on, a user who chose
nothing runs none there, one who chose their API key runs on it once a key
is saved, up to their cap of turns at once, and the choice is read again as
the run starts.
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
from src.llms.claude_code import ClaudeTurnFailed
from src.server.app import claude_code as route
from src.server.database.claude_code_hosted import WORKER_TURN, Custody
from src.server.services.claude_code import hosted as hosted_module
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.saved import token_hash
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
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
CLOUD = {"computer_id": "c-2", "kind": "daytona"}
OWN = {"computer_id": "c-1", "kind": "ssh"}
KEY = "sk-ant-api03-" + "k" * 40
SAVED = {"api_key": KEY, "base_url": None}


@contextmanager
def platform(*, switch=True, stored=False, mode="local", blocked=False, key=True, live=0, computer=CLOUD):
    """A platform deployment with the runtime on, the API-key switch
    (`switch`) and the stored-token one as given, and one user's choice,
    saved key and running turns."""
    saved = key if isinstance(key, dict) else ({"base_url": None} if key else None)
    reads = SimpleNamespace(
        mode=AsyncMock(return_value=Custody(mode, blocked)), key=AsyncMock(return_value=saved),
        live=AsyncMock(return_value=live),
    )
    config = SimpleNamespace(hosted_api_key=switch, stored_custody=stored, hosted_turns_per_user=3, worker_cli_slots=8)
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
            (f"{SERVICE}.live_worker_turns", reads.live),
            ("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value=computer)),
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
        "For workspaces on cloud computers, LangAlpha runs Claude Code on its servers with your Anthropic API "
        "key, and you haven't saved one. Add one in Settings under Model access, or move this workspace to one of "
        "your computers."
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
        credential = await HostedCredential("user-1", "api_key").read()
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
            await HostedCredential("user-1", "api_key").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == reason


@pytest.mark.asyncio
async def test_a_run_admitted_on_an_own_computer_that_starts_on_a_cloud_one_never_starts_uncounted():
    with platform(mode="api_key"), patch(f"{LAUNCHERS}.get_key_with_base_url", AsyncMock(return_value=SAVED)) as read:
        with pytest.raises(ClaudeTurnFailed) as refused:
            await HostedCredential("user-1", None).read()
    read.assert_not_awaited()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "choice_changed"


@pytest.mark.asyncio
async def test_with_the_switch_turned_off_since_admission_the_run_is_refused_as_before():
    with platform(switch=False), pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "api_key").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == "needs_own_computer"


@pytest.mark.asyncio
async def test_a_refused_api_key_ends_the_turn_saying_so_and_is_never_flagged():
    with patch(f"{LAUNCHERS}.refuse", AsyncMock()) as flag, pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "api_key").refused(Credential(KEY, kind="api_key"))
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
    credential = HostedCredential("user-1", "saved_token")
    with (
        patch(f"{LAUNCHERS}.hosted", AsyncMock(return_value=SimpleNamespace(refusal=None, runs_on="saved_token"))),
        patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=saved)),
        patch(f"{LAUNCHERS}.refuse", AsyncMock()) as flag,
        pytest.raises(ClaudeTurnFailed) as refused,
    ):
        await credential.refused(await credential.read())
    flag.assert_awaited_once_with("user-1", token_hash("sk-ant-oat01-tok"), "saved-at")
    assert claude_code_refusal(refused.value, CODE)["reason"] == "token_refused"


# -- The run's row, which the cap counts --


@pytest.mark.parametrize(
    ("mode", "hosted", "stamp"),
    [("platform", "api_key", WORKER_TURN), ("platform", None, {}), ("oss", "saved_token", {})],
)
def test_only_a_turn_admitted_on_a_cloud_computer_on_the_platform_is_counted(mode, hosted, stamp):
    config = SimpleNamespace(claude_code_hosted=hosted, claude_code_saved=None)
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
    with platform(switch=False) as reads:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    reads.mode.assert_not_awaited()


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
