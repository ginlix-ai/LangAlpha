"""Turns on a computer whose Claude Code is managed: the bundled CLI on a real
sshd whose /etc/claude-code the test writes as root, against the loopback
Messages API. The machine and its fixtures are ``test_ssh_launcher``'s.
"""

from __future__ import annotations

import contextlib
import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode

from ptc_agent.agent.claude_code.runtime import ClaudeTurnFailed, build_claude_code_graph
from tests.unit.ptc_agent.agent.claude_code.fake_api import TOKEN
from tests.unit.ptc_agent.agent.claude_code.harness import run_turn

from . import test_ssh_launcher
from .test_ssh_launcher import SPEC, Machine, Relay, _launcher, write_machine_token

machine, api, _bundled_cli = test_ssh_launcher.machine, test_ssh_launcher.api, test_ssh_launcher._bundled_cli

pytestmark = [pytest.mark.integration, pytest.mark.provider_docker, pytest.mark.enable_socket, pytest.mark.asyncio]


@contextlib.contextmanager
def managed(m: Machine, files: dict[str, dict]):
    """The machine's managed Claude Code files, written as an administrator would."""
    for name, body in files.items():
        m.sh(f"mkdir -p /etc/claude-code && cat > /etc/claude-code/{name}", user="root", stdin=json.dumps(body))
    try:
        yield
    finally:
        m.sh("rm -rf /etc/claude-code", user="root")


async def test_a_call_the_managed_settings_deny_never_runs_and_keeps_the_clis_refusal(machine, api):
    ran = []

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        ran.append(command)
        return f"ran: {command}"

    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver())
    with managed(machine, {"managed-settings.json": {"permissions": {"deny": ["mcp__langalpha__Bash"]}}}):
        values = await run_turn(SPEC, _launcher(machine, api), graph, "t-denied", [HumanMessage("RUN_TOOL please")])
    call, refused, reply = values["messages"][1:]
    assert ran == []
    assert (refused.tool_call_id, refused.content, refused.status) == (
        call.tool_calls[0]["id"], "Permission to use Bash has been denied.", "error")
    assert isinstance(refused, ToolMessage) and "Permission to use Bash has been denied." in reply.text


async def test_a_model_the_managed_settings_do_not_allow_fails_the_turn_before_any_reply(machine, api):
    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    with managed(machine, {"managed-settings.json": {"availableModels": ["claude-sonnet-5-5"]}}), \
            pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, _launcher(machine, api), graph, "t-model", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "model_unavailable"
    assert "does not allow claude-haiku-5-5" in str(failed.value)
    values = (await graph.aget_state({"configurable": {"thread_id": "t-model"}})).values
    assert not any(isinstance(m, AIMessage) for m in values.get("messages", []))


@pytest.mark.parametrize(("files", "says"), [
    ({"managed-mcp.json": {"mcpServers": {}}}, "You cannot use --strict-mcp-config when an enterprise MCP config is present"),
    ({"managed-settings.json": {"forceLoginOrgUUID": "00000000-0000-4000-8000-000000000000"}},
     "This machine requires organization 00000000-0000-4000-8000-000000000000"),
], ids=["managed-mcp", "login-org"])
async def test_a_cli_the_managed_settings_stop_at_startup_fails_the_turn_in_its_own_words(machine, api, files, says):
    await write_machine_token(await machine.link(), TOKEN)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    with managed(machine, files), pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, _launcher(machine, api), graph, "t-startup", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "startup_failed" and says in str(failed.value)
    assert TOKEN not in str(failed.value)


async def test_a_key_a_managed_api_key_helper_supplies_is_never_reported_as_the_machines_token(machine, api):
    await write_machine_token(await machine.link(), TOKEN)
    relay = Relay(None)
    launcher = _launcher(machine, api, relay=relay.read, on_refused=relay.refused)
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    # The fixture's API refuses this key.
    with managed(machine, {"managed-settings.json": {"apiKeyHelper": "echo sk-ant-oat01-revoked"}}), \
            pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, launcher, graph, "t-helper", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "key_refused" and relay.refusals == []
    assert TOKEN not in api.credentials
