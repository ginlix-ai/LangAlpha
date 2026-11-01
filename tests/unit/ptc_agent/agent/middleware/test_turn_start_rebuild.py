"""A due thread takes its new system prompt at a turn start.

The main agent's compaction reads the pin against the build before the call
that would mark it stale, so a threshold compaction on the first call after an
idle deploy keeps no turns (T1), a turn start under the threshold rebuilds a
thread a deploy made due once its context reaches the floor and its prompt
cache has expired (T2), a manual compaction between turns keeps no turns
(T5), and a turn that ended on tool results is a turn start to its next run's
first call, never to a later one. A cold Claude thread of at least
``cold_tokens`` is summarized at a turn start under its threshold too,
keeping its tail when nothing is due (T9) and none when its prompt is due.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware.types import AgentMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from src.llms.extension.anthropic_oauth import ChatAnthropicOAuth
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.between_turns import Compact, ask_between_turns
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.rebuild import (
    LAST_ANSWER_KEY,
    RebuildPlan,
    StalePinRebuilds,
    TurnStartRebuilds,
    answer_reading,
    cache_ttl,
    next_summary,
)
from ptc_agent.agent.middleware.compaction.summary_request import summary_prompt
from ptc_agent.agent.middleware.compaction.utils import settled_end, turn_start_cut
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import STATE_SYSTEM_PIN, SystemPinMiddleware
from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
from ptc_agent.agent.prompts import release
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    api_drops,
    is_compaction_request,
    refusal,
    reply,
)

THRESHOLD = 10_000
FLOOR = 5_000
COLD = 7_000
CFG = {"configurable": {"thread_id": "t-turn-start", "user_id": "u-1"}}
NO_TAIL = summary_prompt("native", False, notes=False)
KEEPS_TAIL = summary_prompt("native", True, notes=False)
CLAUDE = SimpleNamespace(model_name="claude-opus-5-5")


# -- where a turn starts -----------------------------------------------------


def _calls(n: int, *ids: str) -> AIMessage:
    return AIMessage("", id=f"a{n}", tool_calls=[{"name": "get_data", "args": {}, "id": i} for i in ids])


def test_a_run_starts_after_a_complete_tool_group_the_users_message_follows():
    """After a cancel, PatchToolCalls answers the calls left open, so the run
    that follows opens on a complete group and then the user's message."""
    group = [HumanMessage("Get a and b."), _calls(1, "c1", "c2"), ToolMessage("cancelled", tool_call_id="c2"), ToolMessage("a", tool_call_id="c1")]
    opened = [*group, HumanMessage("<row/>"), HumanMessage("Try again.")]

    assert turn_start_cut(opened, after_tools=True) == 4
    assert turn_start_cut(opened) == 0
    assert turn_start_cut(group, after_tools=True) == 0
    assert turn_start_cut([*group[:3], HumanMessage("Try again.")], after_tools=True) == 0
    answered = [HumanMessage("Hi."), AIMessage("Hello.", id="a1"), HumanMessage("Again.")]
    assert turn_start_cut(answered) == turn_start_cut(answered, after_tools=True) == 2


def test_a_resume_steering_opened_is_no_turn_start_since_the_users_message_precedes_it():
    """Steering resumes a task before the run's first call, so its call and
    result follow the user's new message, which a cut after them would
    summarize."""
    resume = AIMessage(
        "",
        id="steering-resume-1",
        tool_calls=[{"name": "Task", "args": {"action": "resume"}, "id": "r1"}],
        additional_kwargs={"lc_source": "steering"},
    )
    opened = [HumanMessage("Hi."), AIMessage("Hello.", id="a1"), HumanMessage("Get b.")]
    resumed = [*opened, resume, ToolMessage("resumed", tool_call_id="r1"), HumanMessage("<row/>")]

    assert turn_start_cut(resumed, after_tools=True) == 0
    assert turn_start_cut(opened, after_tools=True) == 2


def test_between_turns_the_whole_view_is_summarized_only_when_its_last_step_is_complete():
    answered = [HumanMessage("Hi."), AIMessage("Hello.", id="a1")]
    cancelled = [HumanMessage("Get a."), _calls(1, "c1")]

    assert settled_end(answered) == 2
    assert settled_end([*answered, HumanMessage("<row/>")]) == 3
    assert settled_end(cancelled) == 0
    assert settled_end([*cancelled, ToolMessage("a", tool_call_id="c1")]) == 3
    assert settled_end([HumanMessage("Hi.")]) == 0


# -- whether the cache has expired -------------------------------------------


def _answer(model_name: str = "claude-opus-5-5", **written: int) -> AIMessage:
    details = {"cache_read": 0, "ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 0, **written}
    return AIMessage(
        [{"type": "thinking", "thinking": "", "signature": "sig"}, {"type": "text", "text": "ok"}],
        response_metadata={"model_name": model_name},
        usage_metadata={"input_tokens": 10, "output_tokens": 1, "total_tokens": 11, "input_token_details": details},
    )


def test_the_ttl_is_what_the_newest_claude_answer_that_wrote_cache_reports():
    hour, five = _answer(ephemeral_1h_input_tokens=900), _answer(ephemeral_5m_input_tokens=900)
    both = _answer(ephemeral_1h_input_tokens=900, ephemeral_5m_input_tokens=40)

    assert cache_ttl([five, hour], CLAUDE) == 3600
    assert cache_ttl([hour, five], CLAUDE) == 300
    assert cache_ttl([both], CLAUDE) == 300
    # An answer that only read leaves the earlier one standing, and another
    # model's usage says nothing of Claude's cache.
    assert cache_ttl([hour, _answer(), _answer("kimi-k3", ephemeral_5m_input_tokens=900)], CLAUDE) == 3600


def test_with_nothing_recorded_the_route_decides_the_ttl():
    oauth = ChatAnthropicOAuth(model_name="claude-opus-5-5", api_key="sk-ant-oat-placeholder")

    assert cache_ttl([HumanMessage("Hi.")], oauth) == 3600
    assert cache_ttl([HumanMessage("Hi.")], CLAUDE) == 300


# -- what a due turn start does ------------------------------------------------


def _due_request(*, source: str = "an earlier deploy", last_answer_at: float = 1_000.0, **pin):
    return SimpleNamespace(
        model=CLAUDE,
        system_message=SimpleNamespace(content="Prompt B."),
        state={
            STATE_SYSTEM_PIN: {"model": "claude-opus-5-5", "content": "Prompt A.", "source_digest": source, **pin},
            "_last_model_response_at": last_answer_at,
        },
    )


TURN = [HumanMessage("Get a."), _answer(ephemeral_5m_input_tokens=900), HumanMessage("Get b.")]


def _plan(
    request,
    tokens: int = 9_000,
    now: float = 1_000_000.0,
    view=TURN,
    floor: int = FLOOR,
    settled_at=None,
    cold: int = 0,
    keep: bool = False,
):
    return TurnStartRebuilds(floor, settled_at=settled_at, cold_tokens=cold, keep_full_context=keep).plan(
        request, view, tokens, THRESHOLD, now, first_call=False
    )


def test_the_cache_clock_starts_at_the_later_of_the_last_answer_and_the_last_run():
    """A call cancelled mid-turn refreshed the cache but stamped nothing; the
    run ledger's settle time covers it."""
    settled = datetime.fromtimestamp(1_200, UTC)

    assert _plan(_due_request(), now=1_400.0).scope == "last_tool"
    assert _plan(_due_request(), now=1_400.0, settled_at=settled).scope is None
    assert _plan(_due_request(), now=1_400.0, settled_at=settled.replace(tzinfo=None)).scope is None
    assert _plan(_due_request(last_answer_at=1_200.0), now=1_400.0).scope is None


def test_a_deploy_is_forced_under_the_threshold_only_large_and_cold():
    forced = _plan(_due_request())

    assert (forced.cut, forced.forced, forced.reason) == (2, True, "deploy_cold")
    assert _plan(_due_request(), tokens=4_000).waits == "under_floor"
    assert _plan(_due_request(), now=1_010.0).waits == "warm"
    assert _plan(_due_request(), floor=0).waits == "off"
    assert _plan(_due_request(source=release.source_digest())).waits == "inputs_only"
    waiting = _plan(_due_request(), tokens=4_000)
    assert (waiting.cut, waiting.forced, waiting.reason) == (2, False, "digest")
    assert _plan(_due_request(), tokens=THRESHOLD).reason == "digest"


def test_a_thread_a_fallback_answered_last_is_not_forced():
    """The pin only follows Claude: a summary a fallback answers after leaves
    the thread due, and it would be forced again at every turn start."""
    view = [*TURN[:2], HumanMessage("Again."), _answer("deepseek-chat"), TURN[2]]

    assert _plan(_due_request(), view=view).waits == "fallback"


def test_a_subagent_rebuilds_only_once_stale_and_only_after_an_answer():
    pinned = _due_request()
    stale = _due_request(stale=True)
    after_tools = [HumanMessage("Get a."), _calls(1, "c1"), ToolMessage("a", tool_call_id="c1"), HumanMessage("Again.")]

    assert StalePinRebuilds().plan(pinned, TURN, 20_000, THRESHOLD, 1e6, first_call=False).due is False
    assert StalePinRebuilds().plan(stale, TURN, 20_000, THRESHOLD, 1e6, first_call=False).reason == "turn_start"
    assert StalePinRebuilds().plan(stale, after_tools, 20_000, THRESHOLD, 1e6, first_call=True).cut == 0


# -- the threads ---------------------------------------------------------------


@tool("get_data")
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


class _Rows(AgentMiddleware):
    """Writes a row after each tool result, as market watch and the notes
    rows do in the middle of a run."""

    def before_model(self, state, runtime):
        return self._row(state)

    async def abefore_model(self, state, runtime):
        return self._row(state)

    @staticmethod
    def _row(state):
        if isinstance(state["messages"][-1], ToolMessage):
            return {"messages": [HumanMessage("<market-watch>AAPL 190.12</market-watch>")]}
        return None


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)
    monkeypatch.setattr(mw_module, "time", clock)
    return clock


def _deploy(monkeypatch) -> None:
    monkeypatch.setattr(release, "source_digest", lambda: "the next deploy")


def _agent(saver: InMemorySaver, system: str, *, floor: int = 0, rows: bool = False, cold: int = 0, keep: bool = False):
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
    ).with_turn_start_rebuilds(TurnStartRebuilds(floor, cold_tokens=cold, keep_full_context=keep))
    return create_agent(
        model=anthropic_model(),
        tools=[get_data],
        system_prompt=system,
        middleware=[
            compaction,
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            ToolSurfaceMiddleware(),
            *([_Rows()] if rows else []),
            SystemPinMiddleware(),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {"input_tokens": input_tokens, "output_tokens": 50, "total_tokens": input_tokens + 50}
    return message


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "The user asked for data.", "signature": "cmp-1"}],
        id="msg_cmp",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


def _system(payload: dict) -> str:
    return json.dumps([block["text"] for block in payload["system"]])


def _last_tool_only(payload: dict) -> bool:
    """Whether the request marks the last tool and nothing else for the cache."""
    return (
        "cache_control" not in payload
        and not any("cache_control" in block for block in payload["system"])
        and [("cache_control" in tool) for tool in payload["tools"]][-1:] == [True]
        and "cache_control" not in json.dumps(payload["messages"])
    )


def _instructions(payload: dict) -> str:
    return payload["extra_body"]["compaction"]["instructions"]


async def _first_turn(saver: InMemorySaver) -> None:
    await _agent(saver, "Prompt A.").ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)


def _script(last_tokens: int, *after) -> list:
    return [
        _measured(reply(1, tool=("get_data", {"query": "a"})), 2_000),
        _measured(reply(2, text="Here it is."), last_tokens),
        *after,
    ]


async def _pin(saver: InMemorySaver) -> dict:
    state = await _agent(saver, "x").aget_state(CFG)
    return state.values.get(STATE_SYSTEM_PIN) or {}


@pytest.mark.parametrize(("idle", "cold"), [(60.0, False), (600.0, True)], ids=["warm", "cold"])
@pytest.mark.asyncio
async def test_a_deploy_while_idle_is_rebuilt_by_the_threshold_summary_on_the_first_call(monkeypatch, clock, idle, cold):
    """The pin is not stale yet on that call: only the digest says it is due.
    A warm cache gives the summary the history from it; a cold one writes
    only the tools, which the call after it reads back."""
    wire = Wire(_script(9_995, _compaction, _measured(reply(4, text="Done."), 1_500))).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    _deploy(monkeypatch)
    clock.now += idle
    await _agent(saver, "Prompt B.").ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    summary, after = wire.payloads[2], wire.payloads[3]
    assert is_compaction_request(summary)
    assert _instructions(summary) == NO_TAIL
    assert "Prompt A." in _system(summary)
    assert _last_tool_only(summary) is cold and not _last_tool_only(after)
    assert "Prompt B." in _system(after) and [m["role"] for m in after["messages"]] == ["assistant", "user"]
    assert api_drops(wire) == {}
    pin = await _pin(saver)
    assert pin.get("stale") is None and pin["source_digest"] == "the next deploy"


@pytest.mark.asyncio
async def test_the_run_after_a_turn_that_ended_on_tool_results_starts_a_turn(monkeypatch, clock):
    """The first turn fails after its tool ran, as a cancel mid-tool leaves it
    once its calls are answered: the next run's first call still cuts there."""
    wire = Wire(
        [
            _measured(reply(1, tool=("get_data", {"query": "a"})), 9_995),
            refusal("upstream failed", status=400),
            _compaction,
            _measured(reply(4, text="Done."), 1_500),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    with pytest.raises(Exception, match="upstream failed"):
        await _first_turn(saver)
    _deploy(monkeypatch)
    await _agent(saver, "Prompt B.").ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert _instructions(wire.payloads[2]) == NO_TAIL
    assert "Prompt B." in _system(wire.payloads[3])
    assert api_drops(wire) == {}


@pytest.mark.asyncio
async def test_mid_run_a_row_after_tool_results_is_no_turn_start(monkeypatch, clock, caplog):
    """The run's first call is past, so a summary at the threshold on a due
    thread ends after the step under way and carries it, as mid-turn."""
    caplog.set_level("INFO")
    wire = Wire(
        _script(
            2_100,
            _measured(reply(3, tool=("get_data", {"query": "b"})), 9_995),
            _compaction,
            _measured(reply(5, text="Done."), 4_000),
        )
    ).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    _deploy(monkeypatch)
    await _agent(saver, "Prompt B.", rows=True).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    summary, after = wire.payloads[3], json.dumps(wire.payloads[4]["messages"])
    assert "market-watch" in after and "Your last step" in after
    assert _instructions(summary) == summary_prompt("native", False, notes=False, carried=True)
    assert "Prompt B." in _system(wire.payloads[4])
    assert api_drops(wire) == {}
    assert "rebuild=yes reason=mid_turn" in caplog.text


@pytest.mark.parametrize(("idle", "rebuilt"), [(600.0, True), (60.0, False)], ids=["cold", "warm"])
@pytest.mark.asyncio
async def test_a_deploy_rebuilds_a_large_thread_under_its_threshold_once_the_cache_expired(monkeypatch, clock, idle, rebuilt):
    after = [_compaction, _measured(reply(4, text="Done."), 1_500)] if rebuilt else [_measured(reply(3, text="Done."), 6_100)]
    wire = Wire(_script(6_000, *after)).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    _deploy(monkeypatch)
    clock.now += idle
    await _agent(saver, "Prompt B.", floor=FLOOR).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert is_compaction_request(wire.payloads[2]) is rebuilt
    assert _last_tool_only(wire.payloads[2]) is rebuilt
    assert ("Prompt B." in _system(wire.payloads[-1])) is rebuilt
    assert not _last_tool_only(wire.payloads[-1])
    assert api_drops(wire) == {}


@pytest.mark.asyncio
async def test_an_input_that_changed_the_prompt_waits_for_a_summary_that_runs_anyway(monkeypatch, clock):
    wire = Wire(_script(6_000, _measured(reply(3, text="Done."), 6_100))).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    clock.now += 600
    await _agent(saver, "Prompt B.", floor=FLOOR).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert not is_compaction_request(wire.payloads[2])
    assert "Prompt A." in _system(wire.payloads[2])


@pytest.mark.asyncio
async def test_a_manual_compaction_of_a_due_thread_keeps_no_turns(monkeypatch, clock):
    wire = Wire(_script(2_100, _compaction)).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    agent = _agent(saver, "Prompt B.")
    state = await agent.aget_state(CFG)

    compaction = await ask_between_turns(agent, state.values, Compact(), thread_id=CFG["configurable"]["thread_id"])

    assert compaction.preserved == []
    assert _instructions(wire.payloads[2]) == NO_TAIL
    assert "Prompt A." in _system(wire.payloads[2])


@pytest.mark.asyncio
async def test_a_manual_compaction_after_a_cancel_mid_tool_keeps_its_tail(monkeypatch, clock, caplog):
    """The open call is answered only at the next turn's start, so a summary
    of all of the view could not be sent; this one keeps the tail."""
    caplog.set_level("INFO")
    wire = Wire(_script(9_995, _compaction)).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    agent = _agent(saver, "Prompt B.")
    await agent.aupdate_state(CFG, {"messages": [HumanMessage("Get c."), _calls(9, "toolu_09")]}, as_node="model")
    state = await agent.aget_state(CFG)

    compaction = await ask_between_turns(agent, state.values, Compact(), thread_id=CFG["configurable"]["thread_id"])

    assert compaction.preserved
    assert _instructions(wire.payloads[2]) == KEEPS_TAIL
    assert "rebuild=deferred reason=unanswered_tool_call" in caplog.text


# -- the urgency marker --------------------------------------------------------


def _release(monkeypatch, version: int, before: int) -> None:
    monkeypatch.setattr(release, "prompt_version", lambda: version)
    monkeypatch.setattr(release, "rebuild_before", lambda: before)


def test_a_thread_pinned_below_rebuild_before_rebuilds_however_small_or_warm(monkeypatch):
    _release(monkeypatch, 1, 1)

    for pinned in ({"prompt_version": 0}, {}):
        urgent = _plan(_due_request(source=release.source_digest(), **pinned), tokens=100, now=1_010.0)
        assert (urgent.forced, urgent.reason) == (True, "urgent")
    assert _plan(_due_request(prompt_version=1), tokens=100, now=1_010.0).waits == "under_floor"


def test_an_urgent_rebuild_does_not_wait_for_claude_to_answer_again(monkeypatch):
    """A fallback answered last, but the summary leaves no bound thinking in
    view, so the thread is not due again on the next turn."""
    _release(monkeypatch, 1, 1)
    view = [*TURN[:2], HumanMessage("Again."), _answer("deepseek-chat"), TURN[2]]

    urgent = _plan(_due_request(prompt_version=0), view=view, tokens=100, now=1_010.0)

    assert (urgent.forced, urgent.reason) == (True, "urgent")
    assert _plan(_due_request(prompt_version=1), view=view).waits == "fallback"


def test_rebuild_before_never_passes_the_builds_own_version(monkeypatch):
    """A rebuilt pin records ``prompt_version``, so a higher ``rebuild_before``
    would force that thread again on every due."""
    assert release._config().get("rebuild_before", 0) <= release._config().get("prompt_version", 0)

    monkeypatch.setattr(release, "_config", lambda: {"prompt_version": 2, "rebuild_before": 5})
    assert release.rebuild_before() == 2


def test_a_build_older_than_the_pin_forces_nothing(monkeypatch):
    """After a rollback the thread is due again, and waits for a summary
    that runs anyway rather than rebuild back."""
    _release(monkeypatch, 1, 3)

    assert _plan(_due_request(prompt_version=2)).waits == "rollback"


def test_a_pin_the_build_refreshed_after_a_bump_waits_on_an_input(monkeypatch):
    """The pin took the bumped version while its content still matched, so
    an input that later changes the prompt is not mistaken for urgent."""
    _release(monkeypatch, 1, 1)

    assert _plan(_due_request(source=release.source_digest(), prompt_version=1)).waits == "inputs_only"


# -- a cold thread over cold_tokens (T9) ------------------------------------


def _settled_request(**pin):
    """A Claude thread whose kept prompt is the build's: nothing is due."""
    return _due_request(content="Prompt B.", **pin)


def test_a_cold_thread_that_is_not_due_is_summarized_with_its_tail_from_cold_tokens():
    cold = _plan(_settled_request(), cold=COLD)

    assert (cold.due, cold.cut, cold.forced, cold.scope, cold.threshold) == (False, 0, False, "system_and_tools", COLD)
    assert _plan(_settled_request(), cold=COLD, now=1_010.0) == RebuildPlan()
    under = _plan(_settled_request(), cold=COLD, tokens=COLD - 1)
    assert (under.forced, under.threshold) == (False, COLD)
    assert _plan(_settled_request(), cold=0).threshold is None
    assert _plan(_settled_request(), cold=THRESHOLD).threshold is None
    other = SimpleNamespace(**{**vars(_settled_request()), "model": SimpleNamespace(model_name="gemini-3.8-flash")})
    assert _plan(other, cold=COLD).threshold is None
    mid_run = [*TURN[:2], _calls(3, "c1"), ToolMessage("a", tool_call_id="c1")]
    assert _plan(_settled_request(), cold=COLD, view=mid_run) == RebuildPlan()


def test_a_cold_threshold_summary_that_keeps_the_tail_marks_only_the_system_and_tools():
    """The threshold fires anyway; the history it summarizes is never sent again."""
    over = _plan(_settled_request(), cold=COLD, tokens=THRESHOLD)

    assert (over.forced, over.scope) == (False, "system_and_tools")
    assert _plan(_settled_request(), cold=0, tokens=THRESHOLD).scope == "system_and_tools"
    assert _plan(_settled_request(), cold=COLD, tokens=THRESHOLD, now=1_010.0).scope is None


def test_only_a_thread_claude_answered_last_has_a_cache_known_to_have_expired():
    view = [*TURN[:2], HumanMessage("Again."), _answer("deepseek-chat"), TURN[2]]

    assert _plan(_settled_request(), cold=COLD, view=view) == RebuildPlan()


def test_a_due_thread_over_cold_tokens_keeps_no_tail():
    """A deploy is T2's. A prompt due on an input alone, which T2 leaves
    waiting, is summarized at the cold threshold like any due thread at its
    threshold, keeping no turns."""
    deploy = _plan(_due_request(), cold=COLD)
    inputs = _plan(_due_request(source=release.source_digest()), cold=COLD)

    assert (deploy.cut, deploy.forced, deploy.reason, deploy.scope) == (2, True, "deploy_cold", "last_tool")
    assert (inputs.cut, inputs.forced, inputs.waits, inputs.scope, inputs.threshold) == (2, False, "inputs_only", "last_tool", COLD)
    warm = _plan(_due_request(source=release.source_digest()), cold=COLD, now=1_010.0)
    assert (warm.waits, warm.threshold) == ("inputs_only", None)
    mid_run = [*TURN[:2], _calls(3, "c1"), ToolMessage("a", tool_call_id="c1")]
    crossing = _plan(_due_request(source=release.source_digest()), cold=COLD, view=mid_run)
    assert (crossing.cut, crossing.carry, crossing.reason, crossing.threshold) == (4, True, "mid_turn", None)


def test_a_due_thread_a_fallback_answered_last_is_not_summarized_over_cold_tokens():
    """Only a Claude answer reports the ttl its cache was written with."""
    view = [*TURN[:2], HumanMessage("Again."), _answer("deepseek-chat"), TURN[2]]

    waiting = _plan(_due_request(source=release.source_digest()), cold=COLD, view=view)

    assert (waiting.forced, waiting.threshold, waiting.waits) == (False, None, "fallback")


def test_a_subagent_never_summarizes_a_cold_thread_under_its_threshold():
    assert StalePinRebuilds().plan(_settled_request(), TURN, 9_000, THRESHOLD, 1e6, first_call=False) == RebuildPlan()


def test_a_skip_leaves_a_cold_thread_whole_for_its_turn_alone():
    assert _plan(_settled_request(), cold=COLD, keep=True).threshold is None
    assert _plan(_settled_request(), cold=COLD).threshold == COLD


def test_a_skip_never_skips_the_threshold_or_a_deploy():
    """It skips what a cold cache alone would summarize: a summary the
    context's size or the system's prompt forces still runs."""
    over = _plan(_settled_request(), cold=COLD, tokens=THRESHOLD, keep=True)
    deploy = _plan(_due_request(), cold=COLD, keep=True)
    inputs = _plan(_due_request(source=release.source_digest()), cold=COLD, keep=True)

    assert (over.scope, over.threshold) == ("system_and_tools", None)
    assert (deploy.forced, deploy.reason) == (True, "deploy_cold")
    assert (inputs.forced, inputs.waits, inputs.threshold) == (False, "inputs_only", None)


# -- what the composer says the next turn start summarizes ---------------------


def _reading(
    model_name: str = "claude-opus-5-5",
    threshold: int = THRESHOLD,
    previous=None,
    model=CLAUDE,
    cold: int = COLD,
    floor: int = FLOOR,
    **written,
):
    answer = _answer(model_name, **(written or {"ephemeral_5m_input_tokens": 900}))
    return answer_reading(answer, threshold, model, previous, TurnStartRebuilds(floor, cold_tokens=cold))


def _between_turns(tokens: int, reading: dict | None = None, **pin) -> dict:
    return {
        "_cached_input_tokens": tokens - 100,
        "_cached_output_tokens": 100,
        "_last_model_response_at": 1_000.0,
        LAST_ANSWER_KEY: _reading() if reading is None else reading,
        STATE_SYSTEM_PIN: {"model": "claude-opus-5-5", "content": "Prompt A.", "source_digest": release.source_digest(), **pin},
    }


def _next(state: dict, *, settled_at=None):
    return next_summary(state, settled_at=settled_at)


def test_the_composer_is_told_of_a_cold_summary_with_the_time_the_cache_expires():
    assert _next(_between_turns(COLD)) == {"reason": "cold", "at": 1_300.0}
    assert _next(_between_turns(COLD), settled_at=datetime.fromtimestamp(1_200, UTC)) == {"reason": "cold", "at": 1_500.0}


def test_the_cache_expires_by_the_ttl_the_turn_start_reads():
    """The newest Claude answer that wrote cache decides, as ``cache_ttl``
    reads it from the view; with none recorded, the route."""
    hour = _reading(ephemeral_1h_input_tokens=900)
    kept = _reading("claude-opus-5-5", previous=hour, cache_read_input_tokens=900)

    assert _next(_between_turns(COLD, hour))["at"] == 4_600.0
    assert _next(_between_turns(COLD, kept))["at"] == 4_600.0
    oauth = ChatAnthropicOAuth(model_name="claude-opus-5-5", api_key="sk-ant-oat-placeholder")
    assert _next(_between_turns(COLD, _reading(model=oauth, cache_read_input_tokens=900)))["at"] == 4_600.0


def test_at_the_threshold_its_last_answer_was_measured_against_the_summary_is_told_without_a_time():
    """The reading is the user's own threshold, so a profile counts."""
    assert _next(_between_turns(THRESHOLD, _reading(cold=0))) == {"reason": "threshold"}
    assert _next(_between_turns(THRESHOLD // 2, _reading(threshold=THRESHOLD // 2))) == {"reason": "threshold"}


def test_the_composer_is_told_nothing_the_turn_start_would_not_summarize():
    assert _next(_between_turns(COLD - 1)) is None
    assert _next(_between_turns(COLD, _reading(cold=0))) is None
    assert _next(_between_turns(COLD, _reading("deepseek-chat"))) is None
    assert _next(_between_turns(COLD, _reading(model=SimpleNamespace(model_name="gemini-3.8-flash")))) is None
    assert _next(_between_turns(COLD, {})) is None
    assert _next({}) is None


def test_a_thread_that_may_be_due_is_told_of_the_rebuild_a_send_cannot_skip(monkeypatch):
    """A summary runs either way; only one the deploy forces takes no skip."""
    deployed = _between_turns(COLD, source_digest="an earlier deploy")

    assert _next(deployed) == {"reason": "rebuild", "at": 1_300.0}
    assert _next({**deployed, LAST_ANSWER_KEY: _reading(floor=0)}) == {"reason": "cold", "at": 1_300.0}
    assert _next(_between_turns(COLD, stale=True)) == {"reason": "cold", "at": 1_300.0}
    assert _next(_between_turns(THRESHOLD, stale=True)) == {"reason": "threshold"}
    _release(monkeypatch, 2, 2)
    assert _next(_between_turns(COLD, stale=True, prompt_version=1)) == {"reason": "rebuild"}


def test_a_rebuild_is_told_whatever_the_cold_setting_and_an_urgent_one_at_once(monkeypatch):
    """A deploy's is forced from the floor, cold summaries on or off, and an
    urgent one at the next send, warm or cold, whoever answered last."""
    deployed = {**_between_turns(FLOOR, source_digest="an earlier deploy"), LAST_ANSWER_KEY: _reading(cold=0)}

    assert _next(deployed) == {"reason": "rebuild", "at": 1_300.0}
    assert _next(_between_turns(FLOOR, _reading(cold=0), stale=True)) is None
    _release(monkeypatch, 2, 2)
    urgent = _between_turns(FLOOR // 2, _reading("deepseek-chat", cold=0), stale=True, prompt_version=1)
    assert _next(urgent) == {"reason": "rebuild"}
    other = SimpleNamespace(model_name="gemini-3.8-flash")
    assert _next({**urgent, LAST_ANSWER_KEY: _reading(model=other)}) is None


def _marked(payload: dict) -> dict:
    """Where the request marks the cache, by what it marks."""
    return {
        "top": "cache_control" in payload,
        "system": any("cache_control" in block for block in payload["system"]),
        "tools": [("cache_control" in tool) for tool in payload["tools"]][-1:] == [True],
        "messages": "cache_control" in json.dumps(payload["messages"]),
    }


@pytest.mark.parametrize(("idle", "summarized"), [(600.0, True), (60.0, False)], ids=["cold", "warm"])
@pytest.mark.asyncio
async def test_a_cold_thread_over_cold_tokens_keeps_its_tail_under_its_prompt(monkeypatch, clock, caplog, idle, summarized):
    caplog.set_level("INFO")
    after = [_compaction, _measured(reply(4, text="Done."), 4_000)] if summarized else [_measured(reply(3, text="Done."), 8_100)]
    wire = Wire(_script(8_000, *after)).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    clock.now += idle
    await _agent(saver, "Prompt A.", cold=COLD).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert is_compaction_request(wire.payloads[2]) is summarized
    assert bool(re.search(rf"trigger=cold tokens=\d+ threshold={COLD} ", caplog.text)) is summarized
    if summarized:
        summary, answer = wire.payloads[2], wire.payloads[3]
        assert _instructions(summary) == KEEPS_TAIL
        assert _marked(summary) == {"top": False, "system": True, "tools": True, "messages": False}
        assert "Prompt A." in _system(answer)
        assert any(block.get("type") == "thinking" for m in answer["messages"] for block in m["content"] if isinstance(block, dict))
    assert api_drops(wire) == {}


@pytest.mark.asyncio
async def test_a_send_that_keeps_the_full_context_skips_the_cold_summary_and_says_so(monkeypatch, clock, caplog):
    caplog.set_level("INFO")
    wire = Wire(_script(8_000, _measured(reply(3, text="Done."), 8_100))).install(monkeypatch)
    saver = InMemorySaver()
    await _first_turn(saver)
    clock.now += 600.0
    await _agent(saver, "Prompt A.", cold=COLD, keep=True).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert not is_compaction_request(wire.payloads[2])
    assert "[Compaction] trigger=cold skipped=user tokens=" in caplog.text
    assert "thread_id=t-turn-start agent=main" in caplog.text
    assert api_drops(wire) == {}


@pytest.mark.parametrize("cold", [COLD, 0], ids=["on", "off"])
@pytest.mark.asyncio
async def test_an_answer_leaves_what_the_composer_reads_between_turns(monkeypatch, clock, cold):
    """Read from the checkpoint's values, as the thread's status reads them,
    with no messages replayed and the turn's own settings: with
    ``cold_compaction`` off its cold_tokens is 0, and nothing cold is told."""
    Wire(_script(8_000)).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.", floor=FLOOR, cold=cold).ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    values = saver.get_tuple(CFG).checkpoint["channel_values"]

    assert (values[LAST_ANSWER_KEY]["threshold"], values[LAST_ANSWER_KEY]["cold_tokens"]) == (THRESHOLD, cold)
    told = next_summary(values, settled_at=None)
    assert told == ({"reason": "cold", "at": values["_last_model_response_at"] + 300} if cold else None)
