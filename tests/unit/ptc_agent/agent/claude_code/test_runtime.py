from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from claude_agent_sdk import ProcessError, ResultMessage, SystemMessage
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool

from pathlib import Path

from ptc_agent.agent.claude_code.cli import RuntimeSpec, options_for
from ptc_agent.agent.claude_code.launcher import LocalLauncher, Prepared
from ptc_agent.agent.claude_code.mid_turn import AfterTool, mid_turn_messages
from ptc_agent.agent.claude_code.runtime import (
    _END, ClaudeCodeState, ClaudeTurn, ClaudeTurnFailed, _Message, _Run, turn_failure, turn_prompt,
)
from ptc_agent.agent.claude_code.session import CheckpointSessionStore, SessionDrift
from ptc_agent.agent.middleware.file_operations.multimodal import MAX_CONTENT_BYTES
from ptc_agent.agent.claude_code.tools_bridge import ToolResults, tool_specs
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder


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
        (result(is_error=True, terminal_reason="prompt_too_long", result="FMP said: Invalid API key"), [], "api_error"),
        (result(subtype="error_during_execution", is_error=True, terminal_reason=None, errors=["tool: 401 Invalid API key"]), [], "api_error"),
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


def _claude(content: str, **kwargs) -> AIMessage:
    return AIMessage(content, response_metadata={"claude_message_id": "msg_1"}, **kwargs)


def test_the_prompt_is_what_the_turn_added_since_claude_spoke():
    earlier = [HumanMessage("old"), _claude("reply")]
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


def test_a_thread_another_model_last_replied_on_is_not_carried_on():
    switched_in = [HumanMessage("old"), AIMessage("another model's reply"), HumanMessage("new")]
    with pytest.raises(ClaudeTurnFailed, match="other_model_history"):
        turn_prompt({"messages": switched_in})
    back_again = [HumanMessage("a"), _claude("b"), HumanMessage("c"), AIMessage("d"), HumanMessage("e")]
    with pytest.raises(ClaudeTurnFailed, match="other_model_history"):
        turn_prompt({"messages": back_again, "claude_session": {"session_id": "s", "entries": []}})


def test_a_baseline_goes_ahead_of_the_turn_until_claude_holds_its_epoch():
    def turn(baseline):
        return ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[], billing={}), LocalLauncher(),
                          identity="u", thread_key="t", baseline=baseline)

    assert turn(lambda _s: ("e1", "BASE"))._baseline_due({}) == ("e1", "BASE")
    assert turn(lambda _s: ("e1", "BASE"))._baseline_due({"claude_session": {"baseline": "e1"}}) == ("e1", None)
    assert turn(lambda _s: ("e2", "BASE"))._baseline_due({"claude_session": {"baseline": "e1"}}) == ("e2", "BASE")

    def broken(_s):
        raise ValueError("render failed")

    assert turn(broken)._baseline_due({}) == ("", None)
    assert turn_prompt({"messages": [HumanMessage("new")]}, baseline="BASE") == [
        {"type": "text", "text": frame_reminder("BASE")},
        {"type": "text", "text": "new"},
    ]


def test_a_session_the_cli_compacted_forgets_the_baseline_it_was_handed():
    from ptc_agent.agent.claude_code.session import session_records_reducer

    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t", baseline=lambda _s: ("e1", "BASE"))
    held = {"session_id": "s", "entries": [], "pinned": {"system_prompt": "s", "tools": []}}
    turn.opening({"messages": [HumanMessage("hi")], "claude_session": held})
    plain = [{"session_id": "s", "entries": [{"uuid": "a", "type": "assistant"}]}]
    first = turn.commit([dict(w) for w in plain])["claude_session"]
    assert first[0]["baseline"] == "e1" and "pinned" not in first[0]
    assert turn.commit([dict(w) for w in plain]) == {"claude_session": plain}
    records = session_records_reducer(held, [first])
    assert records["baseline"] == "e1"
    compacted = [{"session_id": "s", "entries": [{"uuid": "b", "type": "system", "subtype": "compact_boundary"}]}]
    after = session_records_reducer(records, [compacted])
    assert "baseline" not in after and "pinned" not in after


def test_a_session_the_cli_starts_over_holds_only_what_the_run_handed_it():
    from ptc_agent.agent.claude_code.session import session_records_reducer

    old = {"system_prompt": "then", "tools": []}
    held = {"session_id": "s", "entries": [{"uuid": "x"}], "pinned": old, "baseline": "e1"}
    turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t", baseline=lambda _s: ("e1", "BASE"))
    assert turn.opening({"messages": [HumanMessage("hi")], "claude_session": held}) == (old, "hi")
    over = turn.commit([{"session_id": "s2", "entries": [{"uuid": "a"}]}])["claude_session"]
    assert over[0]["pinned"] == old and "baseline" not in over[0]
    records = session_records_reducer(held, [over])
    assert records == {"session_id": "s2", "entries": [{"uuid": "a"}], "pinned": old}
    assert turn._baseline_due({"claude_session": records}) == ("e1", "BASE")


def test_a_tools_step_resumed_without_its_cli_fails_the_turn():
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[], billing={}), LocalLauncher(), identity="u", thread_key="t")
    with pytest.raises(ClaudeTurnFailed, match="nothing_to_resume"):
        turn.current


def test_a_checkpoint_after_tool_results_resumes_with_no_new_prompt():
    call = _claude("", tool_calls=[{"name": "Bash", "args": {}, "id": "t1"}])
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
    options = options_for(spec, pin={"system_prompt": "sys", "tools": tool_specs([bash])}, server=object(), store=CheckpointSessionStore(), session_id=None, prepared=Prepared("/c", "/w"),
                          after_tool=AfterTool(ToolResults(), send=_unsent))
    assert options.tools == [] and options.setting_sources == [] and options.strict_mcp_config is True
    assert options.system_prompt == {"type": "file", "path": "/c/system-prompt.md"} and options.allowed_tools == ["mcp__langalpha__Bash"]
    assert options.verbatim_prompts is True and options.cwd == "/w"
    assert options.env["CLAUDE_AGENT_SDK_MCP_NO_PREFIX"] == "1" and options.env["CLAUDE_CONFIG_DIR"] == "/c"
    assert options.env["ENABLE_TOOL_SEARCH"] == "false"
    assert options.env["CLAUDE_CODE_PROMPT_CACHE_TTL"] == "1h"
    assert options.env["CLAUDE_CODE_DISABLE_ADVISOR_TOOL"] == options.env["CLAUDE_CODE_DISABLE_CLAUDE_MDS"] == "1"
    assert json.loads(options.settings) == {"includeGitInstructions": False}
    assert options.permission_mode == "dontAsk" and options.resume is None
    assert options.max_buffer_size == 2 * MAX_CONTENT_BYTES


async def _unsent(text: str) -> None:
    raise AssertionError(f"sent the CLI {text!r}")


def _run(client, spec, launcher=None, results=None, store=None) -> _Run:
    results = results or ToolResults()
    return _Run(launcher, None, store or CheckpointSessionStore(), client, results, spec, AfterTool(results, send=_unsent))


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
    spec = RuntimeSpec(system_prompt="s", model="m", tools=[], billing={})
    run = _run(_MirrorFails(), spec)
    with pytest.raises(SessionDrift, match="disk full"):
        await asyncio.wait_for(run.settle(["never-arrives"]), 2)


class _ExitsAfter:
    def __init__(self, *said):
        self.said = said

    async def receive_messages(self):
        for m in self.said:
            yield m
        raise ProcessError("Command failed with exit code 1", exit_code=1, stderr="no room left on the machine")


@pytest.mark.asyncio
async def test_a_cli_that_exits_before_its_init_never_started_and_after_it_fails_the_turn_as_it_exits():
    spec = RuntimeSpec(system_prompt="s", model="m", tools=[], billing={})
    before = _run(_ExitsAfter(), spec)
    with pytest.raises(ClaudeTurnFailed, match="^Claude Code could not start: no room left on the machine$"):
        await asyncio.wait_for(before.finish(), 2)
    after = _run(_ExitsAfter(SystemMessage(subtype="init", data={"model": "m"})), spec)
    with pytest.raises(ProcessError):
        await asyncio.wait_for(after.finish(), 2)


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


@pytest.mark.asyncio
async def test_a_stop_flush_keeps_claudes_session_as_it_was():
    """The stop flush re-writes state it already holds; a session record
    without an id would otherwise be appended a second time."""
    from src.server.services.runs.teardown import flush_checkpoint

    from .harness import new_graph

    graph = new_graph()
    config = {"configurable": {"thread_id": "t-flush"}}
    await graph.aupdate_state(
        config,
        {"messages": [HumanMessage("go")], "claude_session": [{"session_id": "s", "entries": [{"type": "custom-title"}]}]},
        as_node="model",
    )
    await flush_checkpoint(graph, "t-flush")
    assert (await graph.aget_state(config)).values["claude_session"]["entries"] == [{"type": "custom-title"}]


def test_a_session_keeps_its_pin_adopts_one_once_and_drops_it_when_compacted():
    from ptc_agent.agent.claude_code.session import session_records_reducer

    def turn():
        return ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                          identity="u", thread_key="t")

    old = {"system_prompt": "then", "tools": []}
    step = [{"session_id": "s", "entries": [{"uuid": "a"}]}]
    asked = [HumanMessage("hi")]

    pinned = turn()
    launched, _ = pinned.opening({"messages": asked, "claude_session": {"session_id": "s", "entries": [], "pinned": old}})
    assert launched == old and "pinned" not in pinned.commit([dict(w) for w in step])["claude_session"][0]

    adopting = turn()
    adopting.opening({"messages": asked, "claude_session": {"session_id": "s", "entries": []}})
    first = adopting.commit([dict(w) for w in step])["claude_session"]
    assert first[0]["pinned"] == {"system_prompt": "now", "tools": []}
    assert "pinned" not in adopting.commit([dict(w) for w in step])["claude_session"][0]
    state = session_records_reducer({"session_id": "s", "entries": []}, [first])
    assert state["pinned"]["system_prompt"] == "now"

    boundary = [{"session_id": "s", "entries": [{"uuid": "b", "subtype": "compact_boundary"}]}]
    assert "pinned" not in session_records_reducer(state, [adopting.commit(boundary)["claude_session"]])
    assert "pinned" not in session_records_reducer(state, [[{"session_id": "s2", "entries": [{"uuid": "c"}]}]])



class _WaitsOnACall:
    """A CLI waiting on one of our calls, which ends its query once interrupted."""

    def __init__(self, order):
        self.order, self.cut = order, asyncio.Event()

    async def receive_messages(self):
        await self.cut.wait()
        return
        yield

    async def interrupt(self):
        self.order.append("interrupt")
        self.cut.set()

    async def disconnect(self):
        self.order.append("disconnect")


class _Failed(ToolResults):
    def __init__(self, order):
        super().__init__()
        self.order = order

    def fail_all(self, reason):
        self.order.append("fail the calls")
        super().fail_all(reason)


class _Releases:
    async def release(self, prepared):
        pass


@pytest.mark.asyncio
async def test_a_close_mid_query_interrupts_the_cli_and_waits_for_it_before_failing_the_calls_it_waits_on():
    order: list[str] = []
    spec = RuntimeSpec(system_prompt="s", model="m", tools=[], billing={})
    run = _run(_WaitsOnACall(order), spec, launcher=_Releases(), results=_Failed(order))
    await asyncio.wait_for(run.close(), 10)
    assert order == ["interrupt", "fail the calls", "disconnect"]


@pytest.mark.asyncio
async def test_a_closing_run_takes_nothing_more_in_as_its_calls_return():
    taken = []

    async def take():
        taken.append(True)

    hook = AfterTool(ToolResults(), send=_unsent)
    hook.open(["t1"], take)
    hook.close()
    assert await hook({"hook_event_name": "PostToolUseFailure"}, "t1", None) == {}
    assert taken == []


class _Ends:
    async def receive_messages(self):
        return
        yield

    async def query(self, prompt):
        pass


@pytest.mark.asyncio
async def test_a_query_sent_after_another_starts_with_nothing_the_last_ones_end_said():
    spec = RuntimeSpec(system_prompt="s", model="m", tools=[], billing={})
    run = _run(_Ends(), spec)
    await run._pump
    run.rate_limits, run.assistant_errors = [object()], [("rate_limit", "x")]
    await run.again("next")
    assert (run.rate_limits, run.assistant_errors, run.result, run.queries) == ([], [], None, 2)


@pytest.mark.asyncio
async def test_a_cut_off_call_the_cli_recorded_no_result_for_is_answered_as_a_langalpha_turn_answers_it(monkeypatch):
    monkeypatch.setattr("ptc_agent.agent.claude_code.runtime.SETTLE_TIMEOUT_S", 0.1)
    spec = RuntimeSpec(system_prompt="s", model="m", tools=[], billing={})
    store = CheckpointSessionStore()
    await store.append({"session_id": "s"}, [{"type": "user", "uuid": "r1", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "t1", "content": "interrupted", "is_error": True}]}}])
    run = _run(_WaitsOnACall([]), spec, launcher=_Releases(), store=store)
    try:
        answers, uuids = await run.cut_off([{"id": "t1", "name": "Bash", "args": {}}, {"id": "t2", "name": "Read", "args": {}}])
    finally:
        await run.close()
    assert uuids == ["r1"]
    assert [(a.content, a.status) for a in answers] == [
        ("interrupted", "error"),
        ("Tool call Read with id t2 did not complete - no result was recorded. It may have been cancelled or interrupted.", "error"),
    ]



def test_steers_go_to_the_cli_as_the_user_wrote_them_and_another_senders_under_its_name():
    delivered = {"messages": [
        {"content": "", "wakeup": {"id": "w1"}},
        {"content": "first", "user_id": "u"},
        {"content": "second", "user_id": "u"},
        {"content": "check the filing", "sender": "chief_of_staff"},
    ]}
    assert mid_turn_messages(HumanMessage("...", additional_kwargs={"steering_delivered": delivered})) == [
        "first",
        "second",
        "The Chief of Staff sent a new message on the user's behalf while you were working:\ncheck the filing",
    ]
