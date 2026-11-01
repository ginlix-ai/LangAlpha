"""Turns on a computer under an organization's Claude Code policy: the bundled
CLI on a real sshd, against the loopback Messages API. The machine and its
fixtures are ``test_ssh_launcher``'s.

A computer with managed files in /etc/claude-code, which the test writes as
root, is refused before the CLI starts. A policy no file on the machine shows,
such as one an organization's account sets, or one installed after the
launcher looked, still reaches the CLI; flag settings, which the CLI applies
as it applies a policy, stand in for the first.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode

from ptc_agent.agent.claude_code.runtime import ClaudeTurnFailed, build_claude_code_graph
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers import _ssh_transport
from tests.unit.ptc_agent.agent.claude_code.fake_api import TOKEN, FakeMessagesAPI
from tests.unit.ptc_agent.agent.claude_code.harness import run_turn

from . import test_ssh_launcher
from .test_ssh_launcher import CID, ROOT, SPEC, Machine, _launcher, write_machine_token

machine, api, _bundled_cli = test_ssh_launcher.machine, test_ssh_launcher.api, test_ssh_launcher._bundled_cli

pytestmark = [pytest.mark.integration, pytest.mark.provider_docker, pytest.mark.enable_socket, pytest.mark.asyncio]


@contextlib.contextmanager
def managed(m: Machine, files: dict[str, dict]):
    """The machine's managed Claude Code files, written as an administrator would."""
    for name, body in files.items():
        m.sh(f"mkdir -p /etc/claude-code/managed-settings.d && cat > /etc/claude-code/{name}", user="root",
             stdin=json.dumps(body))
    try:
        yield
    finally:
        m.sh("rm -rf /etc/claude-code", user="root")


@dataclasses.dataclass
class _Configured(SSHLauncher):
    """A launcher whose CLI also takes ``settings`` as flag settings."""

    settings: dict = dataclasses.field(default_factory=dict)

    def transport(self, options):
        merged = {**json.loads(options.settings or "{}"), **self.settings}
        return super().transport(dataclasses.replace(options, settings=json.dumps(merged)))


def _configured(m: Machine, api: FakeMessagesAPI, settings: dict) -> _Configured:
    return _Configured(_ssh_transport.Machine(m.link, lambda: ROOT), identity="test-user", token_file=CID,
                       env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, settings=settings)


@pytest.mark.parametrize(("files", "sources"), [
    ({"managed-settings.json": {"permissions": {"deny": ["WebFetch"]}}}, ("settings",)),
    ({"managed-settings.d/10-org.json": {"availableModels": ["claude-sonnet-5-5"]}}, ("dropins",)),
    ({"managed-mcp.json": {"mcpServers": {}}}, ("mcp",)),
], ids=["settings", "dropins", "mcp"])
async def test_a_computer_with_an_organizations_policy_installed_is_refused_before_the_cli_starts(
    machine, api, files, sources,
):
    await write_machine_token(await machine.link(), TOKEN)
    told = []

    async def on_blocked(present):
        told.append(present)

    launcher = _launcher(machine, api, on_blocked=on_blocked)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    with managed(machine, files), pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, launcher, graph, "t-blocked", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "policy_blocked" and str(list(sources)) in failed.value.detail
    assert told == [True], "told the token file is there"
    assert api.requests == [] and machine.clis() == []


async def test_a_call_the_policy_denies_never_runs_and_keeps_the_clis_refusal(machine, api):
    ran = []

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        ran.append(command)
        return f"ran: {command}"

    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver())
    launcher = _configured(machine, api, {"permissions": {"deny": ["mcp__langalpha__Bash"]}})
    values = await run_turn(SPEC, launcher, graph, "t-denied", [HumanMessage("RUN_TOOL please")])
    call, refused, reply = values["messages"][1:]
    assert ran == []
    assert (refused.tool_call_id, refused.content, refused.status) == (
        call.tool_calls[0]["id"], "Permission to use Bash has been denied.", "error")
    assert isinstance(refused, ToolMessage) and "Permission to use Bash has been denied." in reply.text


async def test_a_model_the_policy_does_not_allow_fails_the_turn_before_any_reply(machine, api):
    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    launcher = _configured(machine, api, {"availableModels": ["claude-sonnet-5-5"]})
    with pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, launcher, graph, "t-model", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "model_unavailable"
    assert "does not allow claude-haiku-5-5" in str(failed.value)
    values = (await graph.aget_state({"configurable": {"thread_id": "t-model"}})).values
    assert not any(isinstance(m, AIMessage) for m in values.get("messages", []))


async def test_a_cli_the_policy_stops_at_startup_fails_the_turn_in_its_own_words(machine, api):
    """A policy installed once the launcher has looked: the CLI refuses to start beside it."""
    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    launcher = _launcher(machine, api)
    await launcher.ready()
    with managed(machine, {"managed-mcp.json": {"mcpServers": {}}}), pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, launcher, graph, "t-startup", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "startup_failed"
    assert "You cannot use --strict-mcp-config when an enterprise MCP config is present" in str(failed.value)
    assert TOKEN not in str(failed.value)
