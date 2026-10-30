"""Where a Claude Code turn's CLI runs: on the user's own computer, on its
machine with the token kept there (or the saved one, relayed where the
server may); anywhere else on the backend with the saved token. Picked by
the server, which hands it to the agent build."""

from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.server.services.claude_code.launchers import turn_launcher
from src.server.services.claude_code.machine import ComputerToken, MachineAnswer, answer_key
from src.server.services.claude_code.saved import SavedToken
from src.server.services.thread_agent import AgentInputs
from tests.unit.server.services.claude_code.fakes import Cache, serving

CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"


def _ssh_sandbox():
    sandbox = MagicMock()
    sandbox.provider = MagicMock(spec=SSHProvider)
    sandbox.working_dir = "/home/me/langalpha"
    sandbox.wait_ready = AsyncMock()
    sandbox.runtime.link = AsyncMock(return_value="the-link")
    return sandbox


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", [True, False])
async def test_a_turn_on_an_own_computer_runs_there_and_relays_the_saved_token_only_self_hosted(backend):
    with patch("src.server.services.claude_code.launchers.backend_available", return_value=backend):
        launcher = turn_launcher(_ssh_sandbox(), computer_id=CID, user_id="user-1")
    assert isinstance(launcher, SSHLauncher) and launcher.install is True and launcher.token_file == CID
    assert (launcher.machine.root(), launcher.identity) == ("/home/me/langalpha", "user-1")
    assert await launcher.machine.link() == "the-link"
    assert (launcher.relay is not None) is backend
    cache = Cache()
    with serving(cache):
        await launcher.on_refused("file")
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "refused", "recorded for that computer"


def _kept(cache, file, checked_at):
    cache.values[answer_key(CID)] = asdict(MachineAnswer(file, "a@example.com", None, checked_at))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kept", "later", "file"),
    [("works", False, "refused"), ("works", True, "works"), ("none", True, "none"), ("unknown", True, "unknown")],
)
async def test_a_refusal_never_overrides_an_answer_kept_since_the_turn_was_built(kept, later, file):
    """A turn still on the old token is refused after a new one was placed, or the token removed."""
    token = ComputerToken("user-1", CID)
    cache = Cache()
    _kept(cache, kept, token.built_at + (1 if later else -1))
    with serving(cache):
        await token.refused("file")
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == file


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("origin", "kept", "later", "forgotten"),
    [("none", "works", False, True), ("relay", "refused", False, True), ("none", "works", True, False),
     ("file", "works", False, False), ("none", "none", False, False)],
)
async def test_a_turn_that_found_no_token_file_forgets_an_answer_that_says_there_is_one(origin, kept, later, forgotten):
    token = ComputerToken("user-1", CID)
    cache = Cache()
    _kept(cache, kept, token.built_at + (1 if later else -1))
    with serving(cache):
        await token.announced(origin)
    assert (answer_key(CID) not in cache.values) is forgotten


def test_a_turn_anywhere_else_runs_on_the_backend_with_the_saved_token():
    launcher = turn_launcher(SimpleNamespace(provider=MagicMock()), computer_id="c-2", user_id="user-1")
    assert isinstance(launcher, LocalLauncher) and launcher.saved == SavedToken("user-1") and launcher.token is None


def test_a_turn_on_an_own_computer_that_names_none_is_refused():
    with pytest.raises(RuntimeError, match="names that computer"):
        turn_launcher(_ssh_sandbox(), computer_id=None, user_id="user-1")


def test_the_agent_build_is_handed_the_turns_launcher_for_its_computer():
    inputs = AgentInputs(
        user_id="user-1", thread_id="t-1", workspace_id="ws-1", role="analyst", project=None, tool_view=MagicMock(),
        user_profile=None, direct_mcp=MagicMock(), computer_id="c-1",
    )
    sandbox = _ssh_sandbox()
    with patch("src.server.services.claude_code.launchers.turn_launcher", return_value="launcher") as picked:
        assert inputs.claude_code_launcher(sandbox) == "launcher"
    picked.assert_called_once_with(sandbox, computer_id="c-1", user_id="user-1")
    assert inputs.build_kwargs()["claude_code_launcher"] == inputs.claude_code_launcher
