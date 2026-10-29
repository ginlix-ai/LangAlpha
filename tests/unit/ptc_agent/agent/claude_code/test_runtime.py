from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from claude_agent_sdk import ResultMessage, SystemMessage
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool

from pathlib import Path

from ptc_agent.agent.claude_code.launcher import LocalLauncher, Prepared
from ptc_agent.agent.claude_code.runtime import (
    _END, ClaudeCodeState, ClaudeTurn, ClaudeTurnFailed, RuntimeSpec, _Message, _Run, options_for, turn_failure, turn_prompt,
)
from ptc_agent.agent.claude_code.session import CheckpointSessionStore, SessionDrift
from ptc_agent.agent.main_state import MainAgentState


def result(**kw) -> ResultMessage:
    base = dict(subtype="success", duration_ms=1, duration_api_ms=1, is_error=False, num_turns=1, session_id="s",
                result="", terminal_reason="completed")
    return ResultMessage(**{**base, **kw})


@pytest.mark.parametrize(
    ("res", "errors", "kind"),
    [
        (result(), [], None),
        (result(terminal_reason="tool_deferred"), [], None),
        (None, [], "no_result"),
        (result(), [("authentication_failed", "Not logged in")], "auth_failed"),
        (result(), [("rate_limit", "limit")], "plan_exhausted"),
        (result(), [("server_error", "overloaded")], "api_error"),
        (result(terminal_reason="api_error", result="Not logged in \u00b7 Please run /login"), [], "auth_failed"),
        (result(terminal_reason="api_error", result="unauthorized", api_error_status=401), [], "auth_failed"),
        (result(terminal_reason="api_error", result="MCP server needs authentication for its upstream"), [], "api_error"),
        (result(terminal_reason="aborted_tools"), [], "interrupted"),
        (result(terminal_reason="api_error", result="rejected", api_error_status=429), [], "plan_exhausted"),
        (result(terminal_reason="api_error", result="boom"), [], "api_error"),
    ],
)
def test_a_query_that_did_not_do_its_turn_is_a_failure(res, errors, kind):
    failed = turn_failure(res, errors)
    assert (failed.kind if failed else None) == kind


def test_a_rejected_rate_limit_is_a_spent_plan_only_when_the_query_failed():
    event = SimpleNamespace(rate_limit_info=SimpleNamespace(status="rejected", rate_limit_type="five_hour", resets_at=1))
    assert turn_failure(result(terminal_reason="api_error", result="limit"), [], [event]).kind == "plan_exhausted"
    assert turn_failure(result(), [], [event]) is None


def test_a_claude_code_thread_keeps_every_main_agent_channel():
    assert set(MainAgentState.__annotations__) < set(ClaudeCodeState.__annotations__)
    assert set(ClaudeCodeState.__annotations__) - set(MainAgentState.__annotations__) == {"claude_session"}


def test_the_prompt_is_what_the_turn_added_since_claude_spoke():
    earlier = [HumanMessage("old"), AIMessage("reply")]
    assert turn_prompt({"messages": [*earlier, HumanMessage("new")]}) == "new"
    assert turn_prompt({"messages": [*earlier, HumanMessage("row"), HumanMessage([{"type": "text", "text": "ask"}])]}) == [
        {"type": "text", "text": "row"},
        {"type": "text", "text": "ask"},
    ]
    image = {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGk="}}
    assert turn_prompt({"messages": [HumanMessage(["see", image])]}) == [
        {"type": "text", "text": "see"},
        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}},
    ]
    with pytest.raises(ClaudeTurnFailed, match="nothing_to_resume"):
        turn_prompt({"messages": earlier})


def test_a_tools_step_resumed_without_its_cli_fails_the_turn():
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[], billing={}), LocalLauncher(), identity="u", thread_key="t")
    with pytest.raises(ClaudeTurnFailed, match="nothing_to_resume"):
        turn.current


def test_a_checkpoint_after_tool_results_resumes_with_no_new_prompt():
    call = AIMessage("", tool_calls=[{"name": "Bash", "args": {}, "id": "t1"}])
    after_tools = [HumanMessage("go"), call, ToolMessage("done", tool_call_id="t1")]
    assert turn_prompt({"messages": after_tools, "claude_session": {"session_id": "s", "entries": []}}) is None
    with pytest.raises(ClaudeTurnFailed, match="nothing_to_resume"):
        turn_prompt({"messages": after_tools})


@tool("Bash")
def bash(command: str) -> str:
    """Run a shell command."""
    return ""


def test_options_give_claude_code_none_of_its_own_prompt_settings_or_tools():
    spec = RuntimeSpec(system_prompt="sys", model="claude-sonnet-5-5", tools=[bash], billing={}, effort="high")
    options = options_for(spec, server=object(), store=CheckpointSessionStore(), session_id=None, prepared=Prepared("/c", "/w"))
    assert options.tools == [] and options.setting_sources == [] and options.strict_mcp_config is True
    assert options.system_prompt == {"type": "file", "path": "/c/system-prompt.md"} and options.allowed_tools == ["mcp__langalpha__Bash"]
    assert options.verbatim_prompts is True and options.cwd == "/w"
    assert options.env["CLAUDE_AGENT_SDK_MCP_NO_PREFIX"] == "1" and options.env["CLAUDE_CONFIG_DIR"] == "/c"
    assert options.env["ENABLE_TOOL_SEARCH"] == "false"
    assert json.loads(options.settings) == {"includeGitInstructions": False}
    assert options.permission_mode == "dontAsk" and options.resume is None


class _NoTransport(LocalLauncher):
    def transport(self, options):
        raise OSError("no transport")


@pytest.mark.asyncio
async def test_a_turn_that_fails_to_start_leaves_nothing_on_the_machine(monkeypatch):
    prepared = []
    real = LocalLauncher.prepare

    async def keep(self, *args, **kwargs):
        prepared.append(await real(self, *args, **kwargs))
        return prepared[-1]

    monkeypatch.setattr(LocalLauncher, "prepare", keep)
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[bash], billing={}), _NoTransport(), identity="u", thread_key="t-nostart")
    with pytest.raises(OSError, match="no transport"):
        await turn.run({"messages": [HumanMessage("hi")]})
    (p,) = prepared
    assert not Path(p.config_dir).exists()


class _MirrorFails:
    async def receive_messages(self):
        yield SystemMessage(subtype="mirror_error", data={"error": "disk full"})


@pytest.mark.asyncio
async def test_a_drift_the_cli_side_reports_fails_the_step_at_once_with_its_reason():
    run = _Run(None, None, CheckpointSessionStore(), _MirrorFails(), None)
    with pytest.raises(SessionDrift, match="disk full"):
        await asyncio.wait_for(run.settle(["never-arrives"]), 2)


class _Events:
    def __init__(self, items):
        self.items, self.held = list(items), None

    async def next_event(self):
        return self.items.pop(0)

    def hold(self, item):
        self.held = item


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tail", "stopped", "restarted"),
    [([{"type": "message_stop"}], True, False), ([_END], False, False), ([{"type": "message_start"}], False, True)],
)
async def test_a_message_is_whole_only_when_its_stream_reaches_message_stop(tail, stopped, restarted):
    run = _Events([{"type": "content_block_delta"}, *tail])
    message = _Message({"type": "message_start", "message": {"id": "m1"}}, run)
    assert [e["type"] async for e in message.events()][0] == "message_start"
    assert (message.stopped, message.restarted) == (stopped, restarted)
    assert (run.held is not None) is restarted
