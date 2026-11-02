from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from claude_agent_sdk import ProcessError, ResultMessage, SystemMessage
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph

from pathlib import Path

from ptc_agent.agent.claude_code.cli import (
    COMPACT_INSTRUCTIONS,
    RuntimeSpec,
    compaction_failure,
    is_haiku,
    options_for,
    turn_failure,
)
from ptc_agent.agent.claude_code import compaction as compaction_module
from ptc_agent.agent.claude_code.compaction import CompactionRules
from ptc_agent.agent.transcript import pointer
from ptc_agent.agent.transcript.pointer import transcript_note
from ptc_agent.agent.transcript.store import TranscriptTarget
from ptc_agent.core.sandbox.livefs_mount import TranscriptReach
from ptc_agent.agent.claude_code.mid_turn import NOT_RUN, AfterTool, BeforeTool, mid_turn_messages
from ptc_agent.agent.claude_code.launcher import LocalLauncher, Prepared
from ptc_agent.agent.claude_code.runtime import (
    _END, ClaudeCodeState, ClaudeTurn, ClaudeTurnFailed, _Message, _Run, turn_prompt,
)
from ptc_agent.agent.claude_code.session import CheckpointSessionStore, SessionDrift
from ptc_agent.agent.middleware.file_operations.multimodal import MAX_CONTENT_BYTES
from ptc_agent.agent.claude_code.tools_bridge import Deferred, ToolResults, tool_specs
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.credit_gate import CreditGateState, CreditLease, current_credit_gate
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
    assert set(ClaudeCodeState.__annotations__) - set(MainAgentState.__annotations__) == {"claude_session", "preapproved_calls"}
    ours, main = StateGraph(ClaudeCodeState).channels, StateGraph(MainAgentState).channels
    assert {k: type(main[k]) for k in main} == {k: type(ours[k]) for k in main}


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
    held = {"session_id": "s", "entries": [], "pinned": turn.current_pin()}
    turn.opening({"messages": [HumanMessage("hi")], "claude_session": held})
    plain = [{"session_id": "s", "entries": [{"uuid": "a", "type": "assistant"}]}]
    first = turn.commit([dict(w) for w in plain])["claude_session"]
    assert first[0]["baseline"] == "e1" and "pinned" not in first[0]
    assert turn.commit([dict(w) for w in plain]) == {"claude_session": plain}
    records = session_records_reducer(held, [first])
    assert records["baseline"] == "e1"
    compacted = [{"session_id": "s", "entries": [{"uuid": "b", "type": "system", "subtype": "compact_boundary"}]}]
    after = session_records_reducer(records, [compacted])
    assert "baseline" not in after and after["pinned"] == held["pinned"]


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
                          before_tool=BeforeTool(ToolResults(), spec.waits, spec.ends_turn_call), after_tool=AfterTool(ToolResults(), send=_unsent))
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
    assert "CLAUDE_CODE_AUTO_COMPACT_WINDOW" not in options.env


async def _unsent(text: str) -> None:
    raise AssertionError(f"sent the CLI {text!r}")


def _run(client, spec, launcher=None, results=None, store=None) -> _Run:
    results = results or ToolResults()
    return _Run(launcher, None, store or CheckpointSessionStore(), client, results, spec, BeforeTool(results, spec.waits, spec.ends_turn_call),
                AfterTool(results, send=_unsent))


def test_a_turns_launch_hooks_each_call_before_it_runs_and_a_compactions_does_not():
    spec = RuntimeSpec(system_prompt="sys", model="claude-sonnet-5-5", tools=[bash], billing={})
    common = dict(pin={"system_prompt": "sys", "tools": tool_specs([bash])}, server=object(), store=CheckpointSessionStore(),
                  session_id=None, prepared=Prepared("/c", "/w"), after_tool=AfterTool(ToolResults(), send=_unsent))
    before = BeforeTool(ToolResults(), spec.waits, spec.ends_turn_call)
    assert [m.hooks for m in options_for(spec, before_tool=before, **common).hooks["PreToolUse"]] == [[before]]
    assert "PreToolUse" not in options_for(spec, compaction=True, **common).hooks


@pytest.mark.parametrize(("threshold", "window"), [(500_000, "533000"), (20_000, "100000"), (990_000, "1000000")])
def test_the_cli_is_launched_to_compact_at_the_threads_threshold(threshold, window):
    spec = RuntimeSpec(system_prompt="sys", model="claude-sonnet-5-5", tools=[bash], billing={})
    options = options_for(spec, pin={"system_prompt": "sys", "tools": tool_specs([bash])}, server=object(), store=CheckpointSessionStore(), session_id=None, prepared=Prepared("/c", "/w"), after_tool=AfterTool(ToolResults(), send=_unsent), compact_at=threshold)
    assert options.env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] == window


@pytest.mark.asyncio
async def test_a_thread_with_compaction_off_launches_a_cli_that_makes_none_on_its_own(monkeypatch):
    """Not one that compacts near the model's limit, its default: the turn
    fails there as an API thread's does."""
    from ptc_agent.agent.claude_code import runtime

    spec = RuntimeSpec(system_prompt="sys", model="claude-sonnet-5-5", tools=[bash], billing={})
    common = dict(pin={"system_prompt": "sys", "tools": tool_specs([bash])}, server=object(), store=CheckpointSessionStore(),
                  session_id=None, prepared=Prepared("/c", "/w"), after_tool=AfterTool(ToolResults(), send=_unsent))
    assert "DISABLE_AUTO_COMPACT" not in options_for(spec, **common, compact_at=120_000).env
    assert options_for(spec, **common, auto_compact=False).env["DISABLE_AUTO_COMPACT"] == "1"

    launched = []

    async def launch(*args, **kwargs):
        launched.append(kwargs["auto_compact"])
        raise RuntimeError("launched")

    monkeypatch.setattr(runtime, "launch", launch)
    for enabled in (True, False):
        turn = ClaudeTurn(spec, LocalLauncher(), identity="u", thread_key="t",
                          compaction=CompactionRules(0, threshold=120_000 if enabled else 0, enabled=enabled))
        with pytest.raises(RuntimeError, match="launched"):
            await turn.run({"messages": [HumanMessage("hi")]})
    assert launched == [True, False]


@pytest.mark.parametrize(("model", "haiku"), [
    ("claude-haiku-5-5", True), ("claude-haiku-4-5-20251001", True),
    ("claude-sonnet-5-5", False), ("claude-opus-5-5", False), ("claude-fable-5-1", False),
])
def test_only_a_claude_haiku_is_haiku(model, haiku):
    assert is_haiku(model) is haiku


@pytest.mark.asyncio
@pytest.mark.parametrize("compaction", [False, True])
@pytest.mark.parametrize("model", ["claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5-5"])
async def test_only_a_haiku_launch_asks_the_clis_summary_for_the_whole_conversation(model, compaction):
    """Every other model's summary request is the CLI's own."""
    spec = RuntimeSpec(system_prompt="sys", model=model, tools=[bash], billing={})
    options = options_for(spec, pin={"system_prompt": "sys", "tools": tool_specs([bash])}, server=object(), store=CheckpointSessionStore(), session_id=None, prepared=Prepared("/c", "/w"), after_tool=AfterTool(ToolResults(), send=_unsent), compaction=compaction)
    if not is_haiku(model):
        assert "PreCompact" not in options.hooks
        return
    (matcher,) = options.hooks["PreCompact"]
    (hook,) = matcher.hooks
    assert await hook({"hook_event_name": "PreCompact", "trigger": "auto"}, None, {}) == {"systemMessage": COMPACT_INSTRUCTIONS}


def test_the_summary_instruction_asks_for_the_whole_conversation_and_no_answer():
    assert COMPACT_INSTRUCTIONS == (
        "Summarize the whole conversation from its first message, as instructed above. "
        "Do not answer the last user message here."
    )


class _NoTransport(LocalLauncher):
    def transport(self, options):
        raise OSError("no transport")


#: Where the CLI points its summary: the launch's own config dir, gone by the next launch.
_DEAD = "/tmp/claude-cfg-k2/projects/-tmp-langalpha-claude-t-note/s.jsonl"
_CLI_SUMMARY = (
    "This session is being continued from a previous conversation that ran out of context.\n\nSummary:\nthe work\n\n"
    "If you need specific details from before compaction (like exact code snippets, error messages, or content you "
    f"generated), read the full transcript at: {_DEAD}\n\nNote: the earliest part of the conversation was too large to "
    "include and is NOT covered by this summary (the full transcript mentioned above still has it). Say so.\n"
    "Continue the conversation from where it left off."
)


def _summarized(content) -> dict:
    return {"session_id": "s", "entries": [
        {"uuid": "b", "type": "system", "subtype": "compact_boundary"},
        {"uuid": "sum", "type": "user", "isCompactSummary": True, "message": {"role": "user", "content": content}},
        {"uuid": "r", "type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "kept"}]}},
    ]}


def _backend(*, routes_serve: bool | None = None, mount_settles: bool | None = None, saves: bool = True,
             saved: list | None = None):
    async def save_transcript(transcript, messages, *, window):
        if saved is not None:
            saved.append((transcript, messages))
        return saves

    if routes_serve is not None:
        reach = TranscriptReach(SimpleNamespace(serves=lambda workspace_id: routes_serve and workspace_id == "w-1",
                                                save_transcript=save_transcript), shell=False)
    else:
        reach = TranscriptReach(SimpleNamespace(save_transcript=save_transcript), shell=True)

    async def settled_livefs(workspace_id):
        if mount_settles is None:
            await asyncio.Event().wait()
        return reach.home if mount_settles and workspace_id == "w-1" else None

    return SimpleNamespace(transcripts=reach, settled_livefs=settled_livefs)


def _summary_text(entries) -> str:
    content = next(e for e in entries if e.get("isCompactSummary"))["message"]["content"]
    return content if isinstance(content, str) else "".join(b["text"] for b in content)


@pytest.mark.asyncio
@pytest.mark.parametrize(("backend", "served"), [
    (_backend(routes_serve=True), True),
    (_backend(mount_settles=True), True),
    (_backend(routes_serve=False), False),
    (_backend(mount_settles=False), False),
    (_backend(routes_serve=True, saves=False), False),
    (_backend(mount_settles=True, saves=False), False),
])
@pytest.mark.parametrize("blocks", [False, True])
async def test_a_launch_points_a_summary_at_the_threads_transcript_where_it_is_read_else_at_none(monkeypatch, backend, served, blocks):
    prepared = []
    real = LocalLauncher.prepare

    async def keep(self, thread_key, session_id, entries, **kwargs):
        prepared.append(entries)
        return await real(self, thread_key, session_id, entries, **kwargs)

    monkeypatch.setattr(LocalLauncher, "prepare", keep)
    records = _summarized([{"type": "text", "text": _CLI_SUMMARY}] if blocks else _CLI_SUMMARY)
    rules = CompactionRules(0, backend=backend, workspace_id="w-1")
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[bash], billing={}), _NoTransport(), identity="u",
                      thread_key="t-note", compaction=rules)
    with pytest.raises(OSError, match="no transport"):
        await turn.run({"messages": [HumanMessage("hi")], "claude_session": records})
    (entries,) = prepared
    text = _summary_text(entries)
    note = transcript_note(TranscriptTarget("t-note"))
    assert _DEAD not in text and "read the full transcript at" not in text
    assert (note in text) is served
    assert ("the full transcript mentioned above" in text) is served
    assert text.startswith("This session is being continued") and text.endswith("Continue the conversation from where it left off.")
    assert entries[0] == records["entries"][0] and entries[2] == records["entries"][2]
    assert _summary_text(records["entries"]) == _CLI_SUMMARY


@pytest.mark.asyncio
async def test_a_launch_points_a_summary_only_at_a_transcript_saved_from_the_threads_messages(monkeypatch):
    monkeypatch.setattr(LocalLauncher, "prepare", _stop_at_layout)
    saved = []
    rules = CompactionRules(0, backend=_backend(routes_serve=True, saved=saved), workspace_id="w-1")
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[bash], billing={}), _NoTransport(), identity="u",
                      thread_key="t-note", compaction=rules)
    messages = [HumanMessage("first"), AIMessage("answer", response_metadata={"claude_message_id": "msg_1"}), HumanMessage("hi")]
    with pytest.raises(OSError, match="stop at the layout"):
        await turn.run({"messages": messages, "claude_session": _summarized(_CLI_SUMMARY)})
    assert saved == [(TranscriptTarget("t-note"), messages)]


@pytest.mark.asyncio
async def test_a_launch_waits_a_bounded_time_for_the_mount_and_then_points_at_nothing(monkeypatch):
    monkeypatch.setattr(pointer, "_EXPORT_TIMEOUT", 0.05)
    prepared = []

    async def keep(self, thread_key, session_id, entries, **kwargs):
        prepared.append(entries)
        raise OSError("stop at the layout")

    monkeypatch.setattr(LocalLauncher, "prepare", keep)
    rules = CompactionRules(0, backend=_backend(), workspace_id="w-1")
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[bash], billing={}), _NoTransport(), identity="u",
                      thread_key="t-note", compaction=rules)
    with pytest.raises(OSError, match="stop at the layout"):
        await asyncio.wait_for(turn.run({"messages": [HumanMessage("hi")], "claude_session": _summarized(_CLI_SUMMARY)}), 5)
    (entries,) = prepared
    assert "read the full transcript at" not in _summary_text(entries) and ".agents/transcripts" not in _summary_text(entries)


async def _stop_at_layout(self, thread_key, session_id, entries, **kwargs):
    raise OSError("stop at the layout")


@pytest.mark.asyncio
async def test_a_compaction_launch_points_the_summaries_it_resumes_at_the_threads_transcript(monkeypatch):
    prepared = []

    async def keep(self, thread_key, session_id, entries, **kwargs):
        prepared.append(entries)
        raise OSError("stop at the layout")

    monkeypatch.setattr(LocalLauncher, "prepare", keep)
    rules = CompactionRules(0, backend=_backend(routes_serve=True), workspace_id="w-1")
    turn = ClaudeTurn(RuntimeSpec(system_prompt="s", model="m", tools=[bash], billing={}), _NoTransport(), identity="u",
                      thread_key="t-note", compaction=rules)
    with pytest.raises(OSError, match="stop at the layout"):
        await turn._compact({"messages": [HumanMessage("hi")], "claude_session": _summarized(_CLI_SUMMARY)})
    (entries,) = prepared
    assert transcript_note(TranscriptTarget("t-note")) in _summary_text(entries) and _DEAD not in _summary_text(entries)


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


def test_a_session_keeps_its_pin_through_a_compaction_and_adopts_one_once():
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
    assert {k: first[0]["pinned"][k] for k in ("system_prompt", "tools")} == {"system_prompt": "now", "tools": []}
    assert first[0]["pinned"]["source_digest"] and "prompt_version" in first[0]["pinned"]
    assert "pinned" not in adopting.commit([dict(w) for w in step])["claude_session"][0]
    state = session_records_reducer({"session_id": "s", "entries": []}, [first])
    assert state["pinned"]["system_prompt"] == "now"

    boundary = [{"session_id": "s", "entries": [{"uuid": "b", "subtype": "compact_boundary"}]}]
    assert session_records_reducer(state, [adopting.commit(boundary)["claude_session"]])["pinned"] == state["pinned"]
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


def _asks(name: str, args) -> bool:
    return name == "AskUserQuestion"


def _quiet(name: str, args) -> bool:
    return name == "schedule_wakeup"


async def _before(hook: BeforeTool, name: str, tool_use_id: str) -> str | None:
    out = await hook({"tool_name": name, "tool_input": {}}, tool_use_id, None)
    decision = out.get("hookSpecificOutput", {})
    return decision.get("permissionDecisionReason") or decision.get("permissionDecision")


@pytest.mark.asyncio
async def test_a_batch_defers_its_first_call_that_waits_for_the_user_and_answers_another_as_not_run():
    results = ToolResults()
    hook = BeforeTool(results, _asks, _quiet)
    assert [await _before(hook, n, i) for n, i in [("Bash", "t1"), ("AskUserQuestion", "t2"), ("AskUserQuestion", "t3")]] == [
        None, "defer", NOT_RUN,
    ]
    assert isinstance(await results.due("t2"), Deferred)
    hook.reopen()
    assert await _before(hook, "AskUserQuestion", "t4") == "defer"


@pytest.mark.asyncio
async def test_the_call_a_resumed_cli_asks_for_again_runs_with_the_result_the_thread_holds():
    results = ToolResults()
    results.resolve(ToolMessage("User answered: apple", tool_call_id="t1"))
    assert await _before(BeforeTool(results, _asks, _quiet), "AskUserQuestion", "t1") is None


@pytest.mark.asyncio
async def test_the_credit_gate_defers_the_first_call_past_its_boundary_and_lets_the_rest_of_the_batch_and_a_quiet_end_run():
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    lease.denial = {"message": "Out of credits."}
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    try:
        hook = BeforeTool(ToolResults(), _asks, _quiet)
        decisions = [await _before(hook, "Bash", i) for i in ("t1", "t2")]
        ending = await _before(BeforeTool(ToolResults(), _asks, _quiet), "schedule_wakeup", "t3")
    finally:
        current_credit_gate.reset(token)
    assert decisions == ["defer", None] and ending is None


@pytest.mark.asyncio
async def test_a_result_that_ends_the_turn_stops_the_cli_once_it_has_it():
    results = ToolResults(ends_turn=lambda message: message.content == "Ended without a reply.")
    results.resolve(ToolMessage("Ended without a reply.", tool_call_id="t1"))
    results.resolve(ToolMessage("ok", tool_call_id="t2"))
    hook = AfterTool(results, send=_unsent)
    assert (await hook({"hook_event_name": "PostToolUse"}, "t1", None))["continue"] is False
    assert "continue" not in await hook({"hook_event_name": "PostToolUse"}, "t2", None)


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


def test_a_session_on_this_builds_prefix_takes_this_builds_pin_so_the_pin_names_its_build():
    turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t")
    stale = {"system_prompt": "now", "tools": [], "source_digest": "an older build", "prompt_version": 0}
    launched, _ = turn.opening({"messages": [HumanMessage("hi")], "claude_session": {"session_id": "s", "entries": [], "pinned": stale}})
    first = turn.commit([{"session_id": "s", "entries": [{"uuid": "a"}]}])["claude_session"][0]
    assert launched == first["pinned"] == turn.current_pin() != stale


@pytest.mark.asyncio
async def test_a_turn_whose_last_reply_is_another_models_does_not_compact_claudes_session():
    """The turn fails on that history, so a summary made first would be paid for and dropped."""
    from ptc_agent.agent.claude_code.compaction import CompactionRules

    class Due(CompactionRules):
        def repin_plan(self, records, current, now):
            return True, "due"

    turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t", compaction=Due(0))
    compacted = []

    async def compact(state):
        compacted.append(state["claude_session"])
        return []

    turn._compact = compact
    session = {"session_id": "s", "entries": [], "pinned": {"system_prompt": "then", "tools": []}}
    replied = AIMessage("hello", response_metadata={"claude_message_id": "msg_1"})
    assert await turn._repin({"messages": [HumanMessage("hi"), AIMessage("other"), HumanMessage("again")], "claude_session": session}) == []
    assert compacted == []
    await turn._repin({"messages": [HumanMessage("hi"), replied, HumanMessage("again")], "claude_session": session})
    assert compacted == [session]


@pytest.mark.parametrize(("error", "kind"), [
    ("Not enough messages to compact.", None),
    ("No messages to compact", None),
    ("Error during compaction: Failed to authenticate. API Error: 401 invalid token", "auth_failed"),
    ("Error during compaction: API Error: 429 rate limited", "plan_exhausted"),
    ("Error during compaction: Connection error.", "api_error"),
    ("", "api_error"),
])
def test_a_failed_compaction_reads_as_a_turns_failure_unless_there_was_nothing_to_summarize(error, kind):
    failed = compaction_failure(error)
    assert (failed and failed.kind) == kind


def _old_reducer(state, writes):
    """The reducer of builds that voided a session's pin at a compaction."""
    from ptc_agent.agent.claude_code.session import compacted

    state = dict(state)
    for w in writes:
        state["pinned"] = w.get("pinned", state.get("pinned"))
        if any(compacted(e) for e in w["entries"]):
            state["pinned"] = None
    return state


def test_a_compaction_leaves_its_pin_behind_its_records_so_an_earlier_reducer_keeps_it():
    turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t")
    pin, _ = turn.opening({"messages": [HumanMessage("hi")], "claude_session": {"session_id": "s", "entries": []}})
    boundary = [{"session_id": "s", "entries": [{"uuid": "a"}, {"uuid": "b", "subtype": "compact_boundary"}]}]
    writes = turn.commit(boundary)["claude_session"]
    assert _old_reducer({}, writes)["pinned"] == pin
    plain = turn.commit([{"session_id": "s", "entries": [{"uuid": "c"}]}])["claude_session"]
    assert plain == [{"session_id": "s", "entries": [{"uuid": "c"}]}]


def _repinning_turn(compact):
    from ptc_agent.agent.claude_code.compaction import CompactionRules

    class Due(CompactionRules):
        def repin_plan(self, records, current, now):
            return True, "due"

    turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                      identity="u", thread_key="t", compaction=Due(0))
    turn._compact = compact
    state = {"messages": [HumanMessage("hi"), AIMessage("hello", response_metadata={"claude_message_id": "msg_1"}),
                          HumanMessage("again")],
             "claude_session": {"session_id": "s", "pinned": {"system_prompt": "then", "tools": []}, "entries": [
                 {"uuid": "u1", "type": "user", "message": {"role": "user", "content": "hi"}},
                 {"uuid": "a1", "type": "assistant", "message": {"id": "msg_1"}},
             ]}}
    return turn, state


class _Released:
    async def release(self, prepared):
        pass


@pytest.mark.asyncio
async def test_a_cli_that_exits_after_its_init_fails_a_compaction_as_its_request_failing_does(monkeypatch):
    async def launched(*args, **kwargs):
        return Prepared("/c", "/w"), object(), None

    async def received(client):
        yield SystemMessage(subtype="init", data={"model": "m"})
        raise ProcessError("Command failed with exit code 1", exit_code=1, stderr="Authorization: Bearer abcdefgh12345")

    monkeypatch.setattr(compaction_module, "launch", launched)
    monkeypatch.setattr(compaction_module, "received", received)
    with pytest.raises(ClaudeTurnFailed) as failed:
        await compaction_module.compact(RuntimeSpec(system_prompt="s", model="m", tools=[], billing={}),
                                        {"system_prompt": "s", "tools": []}, _Released(), thread_key="t",
                                        identity="u", records={"session_id": "s", "entries": []})
    assert failed.value.kind == "api_error" and "exited with code 1" in str(failed.value) and "abcdefgh12345" not in str(failed.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5-5"])
async def test_only_a_haiku_compaction_is_handed_its_session_ending_on_one_more_answer(monkeypatch, model):
    """So the part the CLI summarizes ends on the session's last answer; the
    thread's records never hold the one it is handed."""
    handed = []

    async def launched(spec, pin, launcher, **kwargs):
        handed.append(kwargs["records"])
        raise RuntimeError("launched")

    monkeypatch.setattr(compaction_module, "launch", launched)
    answered = {"uuid": "a1", "type": "assistant", "requestId": "req_1", "sessionId": "s",
                "message": {"id": "msg_1", "role": "assistant", "content": [{"type": "text", "text": "hello"}],
                            "stop_reason": "end_turn", "usage": {"input_tokens": 5, "output_tokens": 2}}}
    records = {"session_id": "s", "entries": [{"uuid": "u1", "type": "user", "message": {"role": "user", "content": "hi"}},
                                              answered, {"type": "last-prompt"}]}
    with pytest.raises(RuntimeError, match="launched"):
        await compaction_module.compact(RuntimeSpec(system_prompt="s", model=model, tools=[], billing={}),
                                        {"system_prompt": "s", "tools": []}, _Released(), thread_key="t", identity="u",
                                        records=records)
    (given,) = handed
    if not is_haiku(model):
        assert given is records
        return
    *kept, answer = given["entries"]
    assert kept == records["entries"] and len(records["entries"]) == 3
    assert (answer["type"], answer["parentUuid"], answer["sessionId"], answer["message"]["content"], answer["message"]["stop_reason"]) == (
        "assistant", "a1", "s", [{"type": "text", "text": "Noted."}], "end_turn",
    )
    assert "requestId" not in answer and answer["uuid"] not in ("u1", "a1") and answer["message"]["id"] != "msg_1"


@pytest.mark.asyncio
async def test_a_compactions_summary_request_is_metered_as_a_turns_requests_are(monkeypatch):
    """Off the messages stream and under the run's billing stamp, failed or
    not: the CLI streams the request to no one and its result's usage is empty."""
    from langchain_core.callbacks import AsyncCallbackHandler
    from langchain_core.runnables.config import var_child_runnable_config

    from src.utils.tracking import TokenTrackingManager

    async def launched(*args, **kwargs):
        return Prepared("/c", "/w"), object(), None

    async def received(client):
        yield SystemMessage(subtype="init", data={"model": "m"})
        yield SystemMessage(subtype="status", data={"compact_result": "failed", "compact_error": "overloaded"})
        yield ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1, is_error=False, num_turns=0,
                            session_id="s", usage={"input_tokens": 0, "output_tokens": 0},
                            model_usage={"m": {"inputTokens": 2035, "outputTokens": 823, "cacheReadInputTokens": 14685,
                                               "cacheCreationInputTokens": 12129, "webSearchRequests": 0, "costUSD": 0.01}})

    seen = []

    class Tags(AsyncCallbackHandler):
        async def on_chat_model_start(self, *args, tags=None, **kwargs):
            seen.append(tags)

    monkeypatch.setattr(compaction_module, "launch", launched)
    monkeypatch.setattr(compaction_module, "received", received)
    tracker = TokenTrackingManager.initialize_tracking(thread_id="t")
    tracker.run_inline = True
    ambient = var_child_runnable_config.set({"callbacks": [tracker, Tags()]})
    try:
        with pytest.raises(ClaudeTurnFailed):
            await compaction_module.compact(
                RuntimeSpec(system_prompt="s", model="m", tools=[], billing={"billing_type": "oauth", "manifest_model": "m"}),
                {"system_prompt": "s", "tools": []}, _Released(), thread_key="t", identity="u",
                records={"session_id": "s", "entries": []})
    finally:
        var_child_runnable_config.reset(ambient)
    (record,) = tracker.get_per_call_records()
    assert (record["model_name"], record["billing_type"]) == ("m", "oauth")
    assert {k: record["usage"][k] for k in ("input_tokens", "cached_tokens", "cache_5m_tokens", "output_tokens")} == {
        "input_tokens": 2035 + 14685 + 12129, "cached_tokens": 14685, "cache_5m_tokens": 12129, "output_tokens": 823,
    }
    assert seen == [["nostream"]]


@pytest.mark.asyncio
async def test_a_turn_start_compaction_is_signalled_and_its_summary_rides_the_reply_after_it(monkeypatch):
    """As an API thread's: live, the compacting indicator and then the notice;
    on replay, the notice read off the reply's stamp."""
    from ptc_agent.agent.claude_code import runtime
    from ptc_agent.agent.middleware.compaction.utils import compaction_summary

    signals = []
    monkeypatch.setattr(runtime, "context_signal", lambda action, signal, **f: signals.append((action, signal, f)))

    async def compact(records):
        return [{"session_id": "s", "entries": [
            {"uuid": "b", "type": "system", "subtype": "compact_boundary"},
            {"uuid": "sum", "type": "user", "isCompactSummary": True, "message": {"role": "user", "content": "the summary"}},
        ]}]

    turn, state = _repinning_turn(compact)
    assert await turn._repin(state)
    notice = {"summary_length": 11, "original_message_count": 2, "summary_text": "the summary", "source": "provider"}
    assert signals == [("summarize", "start", {}), ("summarize", "complete", notice)]
    reply, later = AIMessage("ok"), AIMessage("more")
    turn.stamp(reply)
    turn.stamp(later)
    assert compaction_summary(reply) == {"summary_message_id": "b", **notice} and compaction_summary(later) is None


@pytest.mark.asyncio
async def test_a_turn_start_compaction_that_fails_closes_its_signal_and_the_turn_runs_on(monkeypatch):
    from ptc_agent.agent.claude_code import runtime

    signals = []
    monkeypatch.setattr(runtime, "context_signal", lambda action, signal, **f: signals.append((action, signal)))

    async def compact(records):
        raise ClaudeTurnFailed("api_error", "overloaded")

    turn, state = _repinning_turn(compact)
    assert await turn._repin(state) == []
    assert signals == [("summarize", "start"), ("summarize", "error")]


@pytest.mark.asyncio
async def test_a_turn_start_compaction_is_saved_before_the_launch_it_moves_the_session_for(monkeypatch):
    """A step of its own: a launch that fails after it leaves the session
    compacted, so the next turn neither makes nor bills it again."""
    from ptc_agent.agent.claude_code.compaction import CompactionRules
    from ptc_agent.agent.claude_code.runtime import TURN_KEY

    from .harness import new_graph

    class Due(CompactionRules):
        def repin_plan(self, records, current, now):
            return (True, "due") if records["pinned"]["system_prompt"] != current["system_prompt"] else None

    made = []

    def new_turn():
        turn = ClaudeTurn(RuntimeSpec(system_prompt="now", model="m", tools=[], billing={}), LocalLauncher(),
                          identity="u", thread_key="t", compaction=Due(0))

        async def compact(_):
            made.append("compacted")
            return [{"session_id": "s", "entries": [
                {"uuid": "b", "type": "system", "subtype": "compact_boundary"},
                {"uuid": "sum", "type": "user", "isCompactSummary": True, "message": {"role": "user", "content": "the summary"}},
            ]}, {"session_id": "s", "entries": [], "pinned": turn.current_pin()}]

        turn._compact = compact
        return turn

    async def start(cls, *args, **kwargs):
        raise ClaudeTurnFailed("api_error", "the launch failed")

    monkeypatch.setattr(_Run, "start", classmethod(start))
    graph = new_graph()
    config = {"configurable": {"thread_id": "t-repin-saved"}}
    await graph.aupdate_state(config, {
        "messages": [HumanMessage("hi"), AIMessage("hello", response_metadata={"claude_message_id": "msg_1"})],
        "claude_session": [{"session_id": "s", "pinned": {"system_prompt": "then", "tools": []}, "entries": [
            {"uuid": "u1", "type": "user", "message": {"role": "user", "content": "hi"}},
            {"uuid": "a1", "type": "assistant", "message": {"id": "msg_1"}},
        ]}],
    }, as_node="model")
    for text in ("again", "and again"):
        with pytest.raises(ClaudeTurnFailed, match="the launch failed"):
            await graph.ainvoke({"messages": [HumanMessage(text)]}, {"configurable": {**config["configurable"], TURN_KEY: new_turn()}})
    session = (await graph.aget_state(config)).values["claude_session"]
    assert made == ["compacted"]
    assert [e["uuid"] for e in session["entries"]][-2:] == ["b", "sum"] and session["pinned"]["system_prompt"] == "now"


@pytest.mark.asyncio
async def test_a_compaction_failure_is_signalled_without_the_credentials_its_text_carried(monkeypatch):
    from ptc_agent.agent.claude_code.runtime import _Compacting

    emitted = []
    monkeypatch.setattr(compaction_module, "get_stream_writer", lambda: emitted.append)

    async def compact(records):
        raise ClaudeTurnFailed("api_error", "Authorization: Bearer abcdefgh12345")

    turn, state = _repinning_turn(compact)
    assert await turn._repin(state) == []
    await turn.compacting(state, _Compacting(error="Authorization: Bearer abcdefgh12345"))
    errors = [s["error"] for s in emitted if s["signal"] == "error"]
    assert len(errors) == 2 and all("Authorization" in e and "abcdefgh12345" not in e for e in errors)


@pytest.mark.asyncio
async def test_a_compaction_whose_records_arrive_late_is_announced_with_the_reply_after_it(monkeypatch):
    """Not with an empty notice when the wait runs out: the reply's own
    records arrive behind the compaction's."""
    from ptc_agent.agent.claude_code import runtime
    from ptc_agent.agent.claude_code.runtime import _Compacting
    from ptc_agent.agent.middleware.compaction.utils import compaction_summary

    signals = []
    monkeypatch.setattr(runtime, "context_signal", lambda action, signal, **f: signals.append((action, signal, f)))
    monkeypatch.setattr(runtime, "SETTLE_TIMEOUT_S", 0.01)
    turn, state = _repinning_turn(None)
    turn._run = SimpleNamespace(store=CheckpointSessionStore())
    await turn.compacting(state, _Compacting(boundary="b"))
    assert signals == []
    await turn._run.store.append({"session_id": "s"}, [
        {"uuid": "b", "type": "system", "subtype": "compact_boundary"},
        {"uuid": "sum", "type": "user", "isCompactSummary": True, "message": {"role": "user", "content": "the summary"}},
    ])
    reply = AIMessage("ok")
    turn.settled(state)
    turn.stamp(reply)
    notice = {"summary_length": 11, "original_message_count": 2, "summary_text": "the summary", "source": "provider"}
    assert signals == [("summarize", "complete", notice)]
    assert compaction_summary(reply) == {"summary_message_id": "b", **notice}


@pytest.mark.asyncio
async def test_a_turn_start_compaction_runs_on_the_session_without_a_deferral_the_thread_went_on_from(monkeypatch):
    """A compaction launch has no PreToolUse hook, so a deferral left in would
    have it ask for the call again with nothing to run it."""
    from ptc_agent.agent.claude_code import runtime

    compacted = []

    async def compact(spec, pin, launcher, *, records, **kwargs):
        compacted.append(records["entries"])
        return []

    monkeypatch.setattr(runtime, "compact", compact)
    monkeypatch.setattr(runtime, "context_signal", lambda *args, **kwargs: None)
    turn, state = _repinning_turn(None)
    del turn._compact
    kept = state["claude_session"]["entries"]
    deferral = {"uuid": "d1", "type": "attachment", "attachment": {"type": "hook_deferred_tool", "toolUseID": "t1"}}
    state["claude_session"]["entries"] = [*kept, deferral]
    assert await turn._repin(state) == []
    assert compacted == [kept]
