"""Where a Claude Code turn's CLI runs: on the user's own computer, on its
machine with the token kept there (or the saved one, relayed where the
server may); anywhere else on the backend with the saved token. Picked by
the server, which hands it to the agent build."""

from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core import claude_code_managed
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.server.services.claude_code import checks
from src.server.services.claude_code.launchers import turn_launcher
from src.server.services.claude_code.machine import ComputerToken, MachineAnswer, answer_key, computer_state
from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.claude_code.saved import SavedToken
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF, claude_code_refusal
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
    cache = Cache()
    with serving(cache):
        await launcher.on_blocked(False)
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "policy", "so is a policy installed there"
    cache = Cache()
    with serving(cache):
        await launcher.on_no_perl(False)
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "no_perl", "and a machine without perl"
    cache = Cache()
    with serving(cache):
        await launcher.on_unsupported(False)
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "unsupported", "and one on musl"


@pytest.mark.asyncio
@pytest.mark.parametrize(("kept", "later", "there", "file", "present"), [
    (None, False, False, "no_perl", False), (None, False, True, "no_perl", True),
    ("works", False, True, "no_perl", True), ("works", False, False, "no_perl", False),
    ("none", False, True, "no_perl", True), ("works", True, False, "works", True),
])
async def test_perl_missing_at_a_turns_start_reads_as_needs_perl_until_the_machine_is_checked(kept, later, there, file, present):
    """A check that found perl keeps admitting turns for its hour otherwise,
    each failing at start; a token placed since is newer than the turn.
    Whether a token file is there is as the launcher found it, so Settings
    offers to remove one even where nothing was known of the machine before."""
    token = ComputerToken("user-1", CID)
    cache = Cache()
    if kept:
        _kept(cache, kept, token.built_at + (1 if later else -1))
    with serving(cache):
        await token.no_perl(there)
    answer = MachineAnswer(**cache.values[answer_key(CID)])
    assert (answer.file, answer.names_file) == (file, present)
    if not later:
        assert computer_state(answer, None) == "needs_perl"


@pytest.mark.asyncio
@pytest.mark.parametrize(("kept", "present"), [(None, False), (None, True), ("works", True), ("works", False)])
async def test_musl_found_at_a_turns_start_reads_as_unsupported_and_relays_nothing(kept, present):
    token = ComputerToken("user-1", CID)
    cache = Cache()
    if kept:
        _kept(cache, kept, token.built_at - 1)
    with serving(cache):
        await token.unsupported(present)
    answer = MachineAnswer(**cache.values[answer_key(CID)])
    assert (answer.file, answer.names_file, answer.relays) == ("unsupported", present, False)
    assert computer_state(answer, None) == "computer_unsupported"


@pytest.mark.asyncio
@pytest.mark.parametrize(("runs", "reason", "file"), [("none", "needs_perl", "no_perl"), ("musl", "computer_unsupported", "unsupported")])
async def test_a_turn_first_to_find_claude_code_cannot_run_there_is_refused_as_admission_refuses_it(runs, reason, file):
    """With nothing kept of the machine, admission let the turn run: it ends
    with the refusal admission gives from then on, and what it keeps still
    names the token file there, so Settings offers to remove it."""
    machine = SimpleNamespace(
        target=SimpleNamespace(host="studio"),
        run=AsyncMock(return_value=SimpleNamespace(exit_code=0, stdout=f"\n1\n{runs}\n".encode(), stderr=b"")),
    )
    sandbox = _ssh_sandbox()
    sandbox.runtime.link = AsyncMock(return_value=machine)
    launcher = turn_launcher(sandbox, computer_id=CID, user_id="user-1")
    cache = Cache()
    with serving(cache), pytest.raises(ClaudeTurnFailed) as refused:
        await launcher.prepare("t-1", None, [], identity="user-1", system_prompt="s")
    assert claude_code_refusal(refused.value, "claude-haiku-5-5-code") == {
        "message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": "claude-haiku-5-5-code", "reason": reason,
    }
    answer = MachineAnswer(**cache.values[answer_key(CID)])
    assert (answer.file, answer.names_file, computer_state(answer, None)) == (file, True, reason)


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
@pytest.mark.parametrize("kept", [None, "works", "refused", "none"])
async def test_a_policy_the_launcher_found_is_kept_briefly_with_what_the_last_answer_knew(kept):
    """Settings and admission say so at once; the answer is short-lived, so a
    policy lifted there shows soon, and the token's account stays named."""
    token = ComputerToken("user-1", CID)
    cache = Cache()
    if kept:
        _kept(cache, kept, token.built_at - 1)
    with serving(cache):
        await token.blocked(True)
    answer = MachineAnswer(**cache.values[answer_key(CID)])
    assert (answer.file, answer.present, answer.fresh) == ("policy", True, True)
    assert answer.email == ("a@example.com" if kept else None)
    assert replace(answer, checked_at=answer.checked_at - checks.SHORT_TTL_S - 1).fresh is False


@pytest.mark.asyncio
async def test_a_policy_the_launcher_found_never_overrides_an_answer_kept_since_the_turn_was_built():
    token = ComputerToken("user-1", CID)
    cache = Cache()
    _kept(cache, "works", token.built_at + 1)
    with serving(cache):
        await token.blocked(False)
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "works"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("origin", "kept", "later", "file"),
    [("none", "works", False, "none"), ("relay", "refused", False, "none"), ("none", None, False, "none"),
     ("relay", "no_perl", False, "none"), ("none", "works", True, "works"), ("file", "works", False, "works"),
     ("none", "invalid", False, "invalid")],
)
async def test_a_turn_that_found_no_token_file_keeps_that(origin, kept, later, file):
    """Over any answer but one naming a file the machine refuses, which announces the same."""
    token = ComputerToken("user-1", CID)
    cache = Cache()
    if kept:
        _kept(cache, kept, token.built_at + (1 if later else -1))
    with serving(cache):
        await token.announced(origin)
    answer = MachineAnswer(**cache.values[answer_key(CID)])
    assert answer.file == file
    if file == "none":
        assert (answer.email, answer.fresh) == (None, True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "word",
    [lambda token: token.refused("file"), lambda token: token.blocked(False), lambda token: token.announced("none")],
    ids=["refused", "blocked", "announced"],
)
async def test_a_turns_word_never_lands_over_an_answer_kept_after_it_read_the_last_one(word):
    """A replacement's answer kept between the turn's read and its write stands."""
    token = ComputerToken("user-1", CID)
    cache = Cache()

    async def read_then_replaced(computer_id):
        _kept(cache, "works", token.built_at + 1)
        return MachineAnswer("works", checked_at=token.built_at - 1)

    with serving(cache), patch("src.server.services.claude_code.machine.read_answer", new=read_then_replaced):
        await word(token)
    assert cache.values[answer_key(CID)]["checked_at"] == token.built_at + 1


def test_a_turn_anywhere_else_runs_on_the_backend_with_the_saved_token():
    launcher = turn_launcher(SimpleNamespace(provider=MagicMock()), computer_id="c-2", user_id="user-1")
    assert isinstance(launcher, LocalLauncher) and launcher.saved == SavedToken("user-1") and launcher.token is None


@pytest.mark.asyncio
async def test_a_backend_turn_on_a_hosted_server_never_starts_and_is_refused_as_admission_refuses_it():
    """Admission read the workspace's computer before the session was taken,
    and the workspace may have moved off an own computer since."""
    launcher = turn_launcher(SimpleNamespace(provider=MagicMock()), computer_id="c-2", user_id="user-1")
    with patch("src.server.services.claude_code.saved.backend_available", return_value=False), \
            patch("src.server.services.claude_code.saved.stored_token", AsyncMock(return_value="sk-ant-oat01-saved")), \
            patch("tempfile.mkdtemp") as laid_out, pytest.raises(ClaudeTurnFailed) as refused:
        await launcher.prepare("t-1", None, [], identity="user-1", system_prompt="s")
    laid_out.assert_not_called()
    assert claude_code_refusal(refused.value, "claude-haiku-5-5-code") == {
        "message": CLAUDE_CODE_OFF["needs_own_computer"], "type": "claude_code_off",
        "model": "claude-haiku-5-5-code", "reason": "needs_own_computer",
    }


@pytest.mark.asyncio
async def test_a_backend_turn_where_an_organizations_policy_is_installed_never_starts_and_is_refused_so(tmp_path, monkeypatch):
    """The backend's own machine, on a self-hosted server, as an own computer is."""
    managed = tmp_path / "claude-code"
    managed.mkdir()
    (managed / "managed-settings.json").write_text("{}")
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_LINUX", str(managed))
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_MAC", str(managed))
    launcher = turn_launcher(SimpleNamespace(provider=MagicMock()), computer_id="c-2", user_id="user-1")
    with patch("src.server.services.claude_code.saved.backend_available", return_value=True), \
            patch("src.server.services.claude_code.saved.stored_token", AsyncMock(return_value="sk-ant-oat01-saved")), \
            patch("tempfile.mkdtemp") as laid_out:
        for start in (lambda: launcher.prepare("t-1", None, [], identity="user-1", system_prompt="s"), launcher.scratch):
            with pytest.raises(ClaudeTurnFailed) as refused:
                await start()
            assert claude_code_refusal(refused.value, "claude-haiku-5-5-code") == {
                "message": CLAUDE_CODE_OFF["policy_blocked"], "type": "claude_code_off",
                "model": "claude-haiku-5-5-code", "reason": "policy_blocked",
            }
    laid_out.assert_not_called()


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
