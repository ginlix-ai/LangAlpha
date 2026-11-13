"""Where a Claude Code turn's CLI runs: on the user's own computer, on its
machine with the token kept there (or the saved one, relayed where the
server may); anywhere else on the backend with the saved token. Picked by
the server, which hands it to the agent build."""

import contextlib
import re
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.sandbox_launcher import SandboxLink
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core import claude_code_managed
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.config.agent import Admitted
from src.server.services.claude_code import checks
from src.server.services.claude_code.hosted import Hosted, OnCloud, OnOwn, Refused
from src.server.services.claude_code.launchers import HostedCredential, turn_launcher
from src.server.services.claude_code.machine import ComputerToken, MachineAnswer, answer_key, computer_state
from src.llms.claude_code import ClaudeTurnFailed
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
@pytest.mark.parametrize("saved", ["saved_token", "stored_token", None])
async def test_a_turn_on_an_own_computer_runs_there_and_relays_the_saved_token_only_where_admission_let_it(saved):
    launcher = turn_launcher(
        _ssh_sandbox(), computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=Admitted(saved=saved),
    )
    assert isinstance(launcher, SSHLauncher) and launcher.install is True and launcher.token_file == token_name(CID)
    assert (launcher.machine.root(), launcher.identity) == ("/home/me/langalpha", "user-1")
    assert await launcher.machine.link() == "the-link"
    assert (launcher.credential is not None) is (saved is not None)
    cache = Cache()
    with serving(cache):
        await launcher.on_file_refused()
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


def test_a_subagents_cli_keeps_the_claude_plan_on_the_rows_its_turns_do():
    """Its rate limit reports go where a turn's go, so a limit either one
    reaches stops the other at its next request, and Settings shows them as one."""
    from src.server.services.claude_code.plan import UserPlan

    admitted = Admitted(saved="saved_token")
    turn = turn_launcher(_ssh_sandbox(), computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=admitted)
    subagent = turn_launcher(
        _ssh_sandbox(), computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=admitted, slot_wait_s=30
    )
    assert isinstance(subagent.plan, UserPlan) and isinstance(turn.plan, UserPlan)
    assert (subagent.plan.user_id, subagent.plan.computer_id) == (turn.plan.user_id, turn.plan.computer_id)
    assert subagent.plan.computer_id == CID


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
        host="studio",
        run=AsyncMock(return_value=SimpleNamespace(exit_code=0, stdout=f"\n1\n{runs}\n".encode(), stderr=b"")),
    )
    sandbox = _ssh_sandbox()
    sandbox.runtime.link = AsyncMock(return_value=machine)
    launcher = turn_launcher(sandbox, computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=Admitted())
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
        await token.file_refused()
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
    [lambda token: token.file_refused(), lambda token: token.blocked(False), lambda token: token.announced("none")],
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
    launcher = turn_launcher(
        SimpleNamespace(provider=MagicMock()), computer_id="c-2", workspace_id="ws-2", user_id="user-1", admitted=Admitted(),
    )
    assert isinstance(launcher, LocalLauncher) and isinstance(launcher.saved, HostedCredential) and launcher.token is None
    assert (launcher.saved.user_id, launcher.saved.workspace_id) == ("user-1", "ws-2")


@pytest.mark.asyncio
async def test_a_backend_turn_on_a_hosted_server_never_starts_and_is_refused_as_admission_refuses_it():
    """Admission read the workspace's computer before the session was taken,
    and the workspace may have moved off an own computer since."""
    launcher = turn_launcher(
        SimpleNamespace(provider=MagicMock()), computer_id="c-2", workspace_id="ws-2", user_id="user-1", admitted=Admitted(),
    )
    with patch("src.server.services.claude_code.hosted.backend_available", return_value=False), \
            patch("src.server.services.claude_code.machine.pick_cli_computer", AsyncMock(return_value=(None, None))), \
            patch("src.server.services.claude_code.launchers.stored_row", AsyncMock(return_value={"access_token": "sk-ant-oat01-saved", "updated_at": None})), \
            patch("tempfile.mkdtemp") as laid_out, pytest.raises(ClaudeTurnFailed) as refused:
        await launcher.prepare("t-1", None, [], identity="user-1", system_prompt="s")
    laid_out.assert_not_called()
    assert claude_code_refusal(refused.value, "claude-haiku-5-5-code") == {
        "message": CLAUDE_CODE_OFF["choice_changed"], "type": "claude_code_off",
        "model": "claude-haiku-5-5-code", "reason": "choice_changed",
    }


@pytest.mark.asyncio
async def test_a_backend_turn_where_an_organizations_policy_is_installed_never_starts_and_is_refused_so(tmp_path, monkeypatch):
    """The backend's own machine, on a self-hosted server, as an own computer is."""
    managed = tmp_path / "claude-code"
    managed.mkdir()
    (managed / "managed-settings.json").write_text("{}")
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_LINUX", str(managed))
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_MAC", str(managed))
    launcher = turn_launcher(
        SimpleNamespace(provider=MagicMock()), computer_id="c-2", workspace_id="ws-2", user_id="user-1",
        admitted=Admitted(hosted="saved_token", saved="saved_token"),
    )
    with patch("src.server.services.claude_code.hosted.backend_available", return_value=True), \
            patch("src.server.services.claude_code.launchers.stored_row", AsyncMock(return_value={"access_token": "sk-ant-oat01-saved", "updated_at": None})), \
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
        turn_launcher(_ssh_sandbox(), computer_id=None, workspace_id="ws-1", user_id="user-1", admitted=Admitted())


def test_the_agent_build_is_handed_the_turns_launcher_for_its_computer():
    inputs = AgentInputs(
        user_id="user-1", thread_id="t-1", workspace_id="ws-1", role="analyst", project=None, tool_view=MagicMock(),
        user_profile=None, direct_mcp=MagicMock(), computer_id="c-1",
    )
    sandbox = _ssh_sandbox()
    admitted = Admitted(saved="saved_token")
    picked_on = Admitted(saved="saved_token", cli_computer="c-9")
    with patch("src.server.services.claude_code.launchers.turn_launcher", return_value="launcher") as picked:
        assert inputs.claude_code_launcher(sandbox, admitted) == "launcher"
        assert inputs.claude_code_launcher(sandbox, admitted, slot_wait_s=30) == "launcher"
        assert inputs.claude_code_launcher(sandbox, picked_on) == "launcher"
    assert [c.kwargs["slot_wait_s"] for c in picked.call_args_list] == [None, 30, None]
    assert [c.kwargs["admitted"] for c in picked.call_args_list] == [admitted, admitted, picked_on]
    assert {(c.kwargs["computer_id"], c.kwargs["workspace_id"], c.kwargs["user_id"]) for c in picked.call_args_list} == {
        ("c-1", "ws-1", "user-1"),
    }
    assert inputs.build_kwargs()["claude_code_launcher"] == inputs.claude_code_launcher
    with patch("src.server.services.claude_code.launchers.turn_launcher", return_value="launcher") as picked:
        replace(inputs, role="chief_of_staff").claude_code_launcher(sandbox, admitted)
    assert picked.call_args.kwargs["workspace_id"] == "ws-1", "the Chief of Staff's CLI is placed by the same rule as an analyst's"

@pytest.mark.asyncio
async def test_claude_refusing_a_computers_own_token_ends_the_turn_saying_so():
    """Not as a missing token: Settings shows the same computer as refused."""
    launcher = turn_launcher(_ssh_sandbox(), computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=Admitted())
    launcher.source = "file"
    cache = Cache()
    with serving(cache), pytest.raises(ClaudeTurnFailed) as failed:
        await launcher.refused()
    assert failed.value.kind == "token_refused"
    assert MachineAnswer(**cache.values[answer_key(CID)]).file == "refused"
    assert claude_code_refusal(failed.value, "claude-code/sonnet")["reason"] == "token_refused"


def test_every_failure_worded_as_admissions_refusal_has_words():
    """A runtime failure read as admission's refusal is worded from
    ``CLAUDE_CODE_OFF``; a reason with no entry there raises while the turn's
    error is built, instead of ending it with words."""
    from src.server.services.llm.claude_code import _ADMISSION_REASON

    assert set(_ADMISSION_REASON.values()) <= set(CLAUDE_CODE_OFF)

@pytest.mark.asyncio
@pytest.mark.parametrize("on", [True, False], ids=["sandbox-placement", "worker-placement"])
async def test_a_cloud_computers_cli_runs_in_its_sandbox_only_where_the_switch_is_on(on, monkeypatch):
    from src.server.services.claude_code import launchers

    monkeypatch.setattr(launchers, "cloud_runs_in", lambda: "sandbox" if on else "worker")
    sandbox = MagicMock()
    sandbox.working_dir = "/home/workspace"
    sandbox.wait_ready = AsyncMock()
    launcher = turn_launcher(
        sandbox, computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=Admitted(hosted="api_key"),
    )
    if not on:
        assert isinstance(launcher, LocalLauncher)
        return
    assert isinstance(launcher, SSHLauncher) and launcher.token_file is None
    assert isinstance(launcher.credential, HostedCredential)
    assert (launcher.credential.user_id, launcher.credential.admitted, launcher.credential.workspace_id) == (
        "user-1", "api_key", "ws-1",
    )
    assert (launcher.machine.root(), launcher.identity) == ("/home/workspace", "user-1")
    link = await launcher.machine.link()
    assert isinstance(link, SandboxLink) and link.runtime is sandbox.runtime and await launcher.machine.link() is link


@pytest.mark.asyncio
@pytest.mark.parametrize("slot_wait_s", [None, 30], ids=["turn", "subagent"])
async def test_a_cloud_computers_cli_in_its_sandbox_holds_one_of_the_clis_that_computer_runs(monkeypatch, slot_wait_s):
    """Turns as well as subagents, so the computer's size bounds its turns too;
    one none frees for in time is refused before anything starts in the sandbox."""
    from claude_agent_sdk import ClaudeAgentOptions, CLIConnectionError

    from ptc_agent.agent.claude_code.ssh_launcher import CLI_SLOT_WAIT_S, SSHTransport, cli_slots_full
    from ptc_agent.core.sandbox.providers._ssh_transport import SlotWaitTimeout
    from src.server.services.claude_code import launchers

    asked = []

    @contextlib.asynccontextmanager
    async def full(computer_id, wait_s):
        asked.append((computer_id, wait_s))
        raise SlotWaitTimeout("cli", 1)
        yield

    monkeypatch.setattr(launchers, "cloud_runs_in", lambda: "sandbox")
    monkeypatch.setattr(launchers, "computer_cli_slot", full)
    sandbox = SimpleNamespace(
        provider=MagicMock(), wait_ready=AsyncMock(), runtime=SimpleNamespace(open_stream=AsyncMock()),
        working_dir="/home/workspace",
    )
    launcher = turn_launcher(
        sandbox, computer_id=CID, workspace_id="ws-1", user_id="user-1", admitted=Admitted(hosted="api_key"),
        slot_wait_s=slot_wait_s,
    )
    link = await launcher.machine.link()
    assert isinstance(link, SandboxLink)
    transport = SSHTransport(launcher, link, ClaudeAgentOptions(cwd="/home/workspace", cli_path="/opt/claude"))
    with pytest.raises(CLIConnectionError, match=f"^{re.escape(cli_slots_full(1))}$"):
        await transport.connect()
    assert asked == [(CID, CLI_SLOT_WAIT_S if slot_wait_s is None else 30)]
    sandbox.runtime.open_stream.assert_not_awaited()


def _picked(rows, *, awaited_wake, where=None):
    """A picked computer whose row reads as each of ``rows`` in turn (the last
    stands once they run out), placed where admission placed it unless
    ``where`` says otherwise, with every seam it dials through patched at its source."""
    from contextlib import ExitStack

    reads = AsyncMock(side_effect=rows + [rows[-1]] * 3)
    machine = SimpleNamespace(link=AsyncMock(return_value="the-link"), root=lambda: "/home/me/langalpha")
    seams = SimpleNamespace(
        get_computer=reads, wake=awaited_wake, machine_of=AsyncMock(return_value=machine),
        activity=AsyncMock(), machine=machine,
        placement=AsyncMock(return_value=where or OnOwn(_row("running"), picked=True)),
    )
    stack = ExitStack()
    for target, value in [
        ("src.server.services.claude_code.launchers.cli_placement", seams.placement),
        ("src.server.database.computer.get_computer", seams.get_computer),
        ("src.server.database.computer.update_computer_activity", seams.activity),
        ("src.server.services.claude_code.machine.machine_of", seams.machine_of),
        (
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            lambda: SimpleNamespace(wake_computer=seams.wake),
        ),
    ]:
        stack.enter_context(patch(target, value))
    return stack, seams


def _row(status, **overrides):
    return {"computer_id": CID, "user_id": "user-1", "name": "Studio", "kind": "ssh", "status": status, **overrides}


@pytest.mark.asyncio
async def test_a_turn_picked_onto_a_stopped_computer_starts_it_then_dials_the_row_it_read_after():
    """The idle sweep stops a computer that runs only other workspaces' CLIs,
    so the run starts it as a workspace's acquire does before it dials."""
    from src.server.services.claude_code.launchers import _PickedComputer

    stopped, running = _row("stopped"), _row("running", host="studio.local")
    stack, seams = _picked([stopped, running], awaited_wake=AsyncMock())
    with stack:
        picked = _PickedComputer(CID, "user-1", "ws-2")
        assert await picked.link() == "the-link" and picked.root() == "/home/me/langalpha"
    seams.wake.assert_awaited_once_with(CID, user_id="user-1")
    seams.machine_of.assert_awaited_once_with(running)
    seams.activity.assert_awaited_once_with(CID)


@pytest.mark.asyncio
async def test_a_turn_picked_onto_a_running_computer_never_starts_it_and_each_link_counts_as_activity():
    from src.server.services.claude_code.launchers import _PickedComputer

    stack, seams = _picked([_row("running")], awaited_wake=AsyncMock())
    with stack:
        picked = _PickedComputer(CID, "user-1", "ws-2")
        await picked.link()
        await picked.link()
    seams.wake.assert_not_awaited()
    seams.machine_of.assert_awaited_once()
    assert seams.activity.await_args_list == [call(CID), call(CID)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [_row("running", user_id="user-2"), _row("error"), None],
    ids=["another-users", "in-error", "gone"],
)
async def test_a_picked_computer_that_is_not_the_users_or_cannot_run_ends_the_turn_saying_it_is_offline(row):
    from src.server.services.claude_code.launchers import _PickedComputer

    stack, seams = _picked([row], awaited_wake=AsyncMock())
    with stack, pytest.raises(ClaudeTurnFailed) as failed:
        await _PickedComputer(CID, "user-1", "ws-2").link()
    assert failed.value.kind == "cli_computer_offline"
    assert failed.value.fields == {"name": row["name"] if row else CID}
    assert claude_code_refusal(failed.value, "claude-haiku-5-5-code")["reason"] == "cli_computer_offline"
    seams.wake.assert_not_awaited()
    seams.machine_of.assert_not_awaited()
    seams.activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_picked_computer_out_of_reach_when_dialed_ends_the_turn_saying_it_is_offline():
    from src.server.services.claude_code.launchers import _PickedComputer

    stack, seams = _picked([_row("running")], awaited_wake=AsyncMock())
    seams.machine.link.side_effect = OSError("no route")
    with stack, pytest.raises(ClaudeTurnFailed) as failed:
        await _PickedComputer(CID, "user-1", "ws-2").link()
    assert (failed.value.kind, failed.value.fields) == ("cli_computer_offline", {"name": "Studio"})
    seams.activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_picked_computer_asks_where_the_turn_runs_once_as_the_run_starts():
    """As the credential's read does on a cloud computer, by the rule admission
    asked for the turn's workspace, and only on the first dial."""
    stack, seams = _picked([_row("running")], awaited_wake=AsyncMock())
    launcher = turn_launcher(
        SimpleNamespace(provider=MagicMock()), computer_id="c-2", workspace_id="ws-2", user_id="user-1",
        admitted=Admitted(saved=None, cli_computer=CID),
    )
    with stack:
        await launcher.machine.link()
        await launcher.machine.link()
    seams.placement.assert_awaited_once_with("user-1", "ws-2")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "where",
    [
        OnCloud(Hosted("stored_token"), {"computer_id": "c-2", "name": "Research"}),
        Refused("needs_bigger_computer", {"home": False}, hosted=Hosted("stored_token")),
        Refused("needs_own_computer"),
    ],
    ids=["runs-on-the-cloud-computer", "refused-on-its-terms", "where-cloud-computers-run-none"],
)
async def test_a_picked_computer_whose_user_moved_to_the_stored_token_since_admission_never_runs(where):
    """The user chose to store their token between admission and the run's start."""
    from src.server.services.claude_code.launchers import _PickedComputer

    stack, seams = _picked([_row("running")], awaited_wake=AsyncMock(), where=where)
    with stack, pytest.raises(ClaudeTurnFailed) as failed:
        await _PickedComputer(CID, "user-1", "ws-2").link()
    assert failed.value.kind == "choice_changed"
    seams.get_computer.assert_not_awaited()
    seams.machine_of.assert_not_awaited()
    seams.activity.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "where", [Refused("cli_computer_offline", {"name": "Lab"}), Refused("no_own_computer")], ids=["offline", "none-left"],
)
async def test_a_pick_that_now_finds_none_keeps_the_admitted_computer_another_turn_is_starting(where):
    """The pick reads a computer that is starting as out of reach, so a second
    turn admitted onto one the first is waking finds none; the choice is the
    same, and the dial waits the start out as it would for its own."""
    from src.server.services.claude_code.launchers import _PickedComputer

    wake = AsyncMock()
    stack, seams = _picked([_row("starting"), _row("running")], awaited_wake=wake, where=where)
    with stack:
        assert await _PickedComputer(CID, "user-1", "ws-2").link() == "the-link"
    wake.assert_awaited_once()
    seams.machine_of.assert_awaited_once_with(_row("running"))


@pytest.mark.asyncio
async def test_a_pick_that_now_lands_on_another_own_computer_keeps_the_admitted_one():
    """The user's choice is the same; the admitted computer's own reach is what the dial checks."""
    from src.server.services.claude_code.launchers import _PickedComputer

    other = _row("running", computer_id="c-other", name="Lab")
    stack, seams = _picked([_row("running")], awaited_wake=AsyncMock(), where=OnOwn(other, picked=True))
    with stack:
        assert await _PickedComputer(CID, "user-1", "ws-2").link() == "the-link"
    seams.get_computer.assert_awaited_once_with(CID)
    seams.machine_of.assert_awaited_once_with(_row("running"))


def test_a_turn_admitted_on_a_picked_computer_runs_its_cli_there_whatever_its_own_sandbox_is():
    admitted = Admitted(saved="saved_token", cli_computer=CID)
    launcher = turn_launcher(
        SimpleNamespace(provider=MagicMock()), computer_id="c-2", workspace_id="ws-2", user_id="user-1", admitted=admitted,
    )
    assert isinstance(launcher, SSHLauncher) and launcher.token_file == token_name(CID)
    assert launcher.credential is not None and launcher.plan.computer_id == CID

@pytest.mark.parametrize("picked", [False, True])
def test_a_turns_cli_holds_one_of_the_computers_it_runs_on_and_a_checks_none(picked):
    """A check reads a token, so it is no agent and takes none of the
    computer's count, only a slot on its connection."""
    from src.server.services.claude_code.cli_slots import computer_cli_slot
    from src.server.services.claude_code.machine import check_launcher

    admitted = Admitted(saved="saved_token", cli_computer=CID if picked else None)
    sandbox = SimpleNamespace(provider=MagicMock()) if picked else _ssh_sandbox()
    launcher = turn_launcher(sandbox, computer_id="c-2" if picked else CID, workspace_id="ws-1", user_id="user-1",
                             admitted=admitted)
    assert (launcher.cli_slot.func, launcher.cli_slot.args) == (computer_cli_slot, (CID,))
    assert check_launcher(MagicMock(), "user-1", token_name(CID)).cli_slot is None
