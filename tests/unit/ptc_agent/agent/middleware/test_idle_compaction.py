"""A main-agent Claude thread on an hour's prompt cache is summarized while
it is idle after a turn, before the entry the summary reads expires: a due
thread rebuilds keeping no turns (I1), and a large one that is not due keeps
its tail (I2). Only the provider's own summary from its cache is taken, and
only on the request the turn cached.
"""

from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace

import pytest
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.between_turns import IdleCompact, IdleSkipped, ask_between_turns
from ptc_agent.agent.middleware.compaction.compact import NoOwnSummary, Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction import idle as idle_module
from ptc_agent.agent.middleware.compaction.idle import IDLE_DEADLINE_MARGIN_S, request_digest, request_started_at
from ptc_agent.agent.middleware.compaction.measure import measured_tokens
from ptc_agent.agent.middleware.compaction.rebuild import IDLE_TTL_SECONDS, RebuildPlan, StalePinRebuilds, TurnStartRebuilds
from ptc_agent.agent.middleware.compaction.summary_request import summary_prompt
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import STATE_SYSTEM_PIN, SystemPinMiddleware
from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
from ptc_agent.agent.prompts import release
from ptc_agent.config.agent import CompactionConfig
from src.config import features
from src.server.services.idle_compaction import plan_firing
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    is_compaction_request,
    refusal,
    reply,
)

THRESHOLD = 10_000
FLOOR = 5_000
COLD = 7_000
CFG = {"configurable": {"thread_id": "t-idle", "user_id": "u-1"}}
NO_TAIL = summary_prompt("native", False, notes=False)
KEEPS_TAIL = summary_prompt("native", True, notes=False)
HOUR = SimpleNamespace(model_name="claude-opus-5-5", cache_ttl="1h")
FIVE = SimpleNamespace(model_name="claude-opus-5-5")


# -- what an idle thread is summarized as ----------------------------------------


def _call(call_id: str) -> dict:
    return {"name": "get_data", "args": {}, "id": call_id, "type": "tool_call"}


def _answer(model_name: str = "claude-opus-5-5", *, tool_calls=(), **written: int) -> AIMessage:
    details = {"cache_read": 0, "ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 0, **written}
    return AIMessage(
        [{"type": "thinking", "thinking": "", "signature": "sig"}, {"type": "text", "text": "ok"}],
        tool_calls=list(tool_calls),
        response_metadata={"model_name": model_name},
        usage_metadata={"input_tokens": 10, "output_tokens": 1, "total_tokens": 11, "input_token_details": details},
    )


ANSWERED = [HumanMessage("Get a."), _answer(ephemeral_1h_input_tokens=900)]


def _request(*, prompt: str = "Prompt A.", source: str = "an earlier deploy", model=HOUR):
    """The thread's request between turns: due when ``prompt`` is not the
    pinned one, by a deploy unless ``source`` is the build's."""
    return SimpleNamespace(
        model=model,
        system_message=SimpleNamespace(content=prompt),
        state={STATE_SYSTEM_PIN: {"model": "claude-opus-5-5", "content": "Prompt A.", "source_digest": source}},
    )


def _idle(
    request, *, view=ANSWERED, tokens: int = 9_000, floor: int = FLOOR, cold: int = 0, idle: int | None = None
) -> RebuildPlan:
    """idle_tokens defaults to cold_tokens, as idle_compact.threshold_tokens does."""
    rebuilds = TurnStartRebuilds(floor, cold_tokens=cold, idle_tokens=cold if idle is None else idle)
    return rebuilds.idle(request, view, tokens, THRESHOLD)


def _skipped(request, **kwargs) -> str:
    with pytest.raises(IdleSkipped) as skipped:
        _idle(request, **kwargs)
    return skipped.value.reason


def test_a_thread_a_deploy_made_due_rebuilds_keeping_no_turns_from_the_floor():
    rebuilt = _idle(_request(prompt="Prompt B."))

    assert (rebuilt.due, rebuilt.cut, rebuilt.forced, rebuilt.reason) == (True, 2, True, "idle")
    assert _skipped(_request(prompt="Prompt B."), tokens=FLOOR - 1) == "under_floor"
    assert _skipped(_request(prompt="Prompt B."), floor=0) == "off"
    assert _idle(_request(prompt="Prompt B."), floor=0, cold=COLD, tokens=COLD).cut == 2


def test_a_prompt_due_on_an_input_alone_rebuilds_only_from_cold_tokens():
    """As at a cold turn start (T9): the summary runs anyway there."""
    inputs = _request(prompt="Prompt B.", source=release.source_digest())

    assert _skipped(inputs) == "inputs_only"
    assert _idle(inputs, cold=COLD, tokens=COLD).cut == 2


def test_a_thread_that_is_not_due_keeps_its_tail_from_cold_tokens():
    assert _idle(_request(), cold=COLD, tokens=COLD) == RebuildPlan(forced=True)
    assert _skipped(_request(), cold=COLD, tokens=COLD - 1) == "under_cold"
    assert _skipped(_request(), cold=0) == "under_cold"


def test_a_thread_that_is_not_due_keeps_its_tail_from_the_idle_threshold_set_apart():
    assert _idle(_request(), cold=9_500, idle=COLD, tokens=COLD) == RebuildPlan(forced=True)
    assert _skipped(_request(), cold=9_500, idle=COLD, tokens=COLD - 1) == "under_cold"
    assert _skipped(_request(), cold=COLD, idle=9_500, tokens=COLD) == "under_cold"


def test_only_a_settled_claude_thread_on_an_hours_cache_under_its_threshold_is_summarized():
    due = _request(prompt="Prompt B.")
    five = [HumanMessage("Get a."), _answer(ephemeral_5m_input_tokens=900)]
    fallback = [*ANSWERED, HumanMessage("Again."), _answer("deepseek-chat")]
    calling = [HumanMessage("Get a."), _answer(ephemeral_1h_input_tokens=900, tool_calls=[_call("c1")])]

    assert _skipped(due, view=five) == "short_ttl"
    assert _skipped(_request(prompt="Prompt B.", model=FIVE), view=[HumanMessage("Get a."), _answer()]) == "short_ttl"
    assert _skipped(due, tokens=THRESHOLD) == "over_threshold"
    assert _skipped(due, view=fallback) == "not_claude"
    assert _skipped(due, view=calling) == "unanswered_tool_call"
    with pytest.raises(IdleSkipped, match="not_main"):
        StalePinRebuilds().idle(due, ANSWERED, 9_000, THRESHOLD)


# -- what keys the cache besides the messages -------------------------------------


@tool("get_data")
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


@tool("get_more")
def get_more(query: str) -> str:
    """Look something else up."""
    return f"more for {query}"


def _asked(model, tools=(get_data,), **settings):
    return SimpleNamespace(model=model, tools=list(tools), model_settings=settings)


def test_the_request_digest_follows_the_tools_and_the_model_settings():
    model = anthropic_model()
    digest = request_digest(_asked(model))
    thinking = model.model_copy(update={"thinking": {"type": "enabled", "budget_tokens": 2_048}})

    assert request_digest(_asked(model)) == digest
    assert request_digest(_asked(model, tools=(get_data, get_more))) != digest
    assert request_digest(_asked(model, effort="low")) != digest
    assert request_digest(_asked(thinking)) != digest


# -- the threads ---------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


def _feature(monkeypatch, on: bool) -> None:
    spec = features.FEATURES["idle_compaction"]
    monkeypatch.setitem(features.FEATURES, "idle_compaction", dataclasses.replace(spec, enabled=on))


@pytest.fixture(autouse=True)
def feature_on(monkeypatch):
    """The deployment turned the idle_compaction feature on."""
    _feature(monkeypatch, True)


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)
    monkeypatch.setattr(mw_module, "time", clock)
    monkeypatch.setattr(idle_module, "time", clock)
    return clock


def _agent(
    saver,
    system: str,
    *,
    model=None,
    cold: int = 0,
    idle: int | None = None,
    skip: bool = False,
    main: bool = True,
    tools=(get_data,),
):
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
    )
    if main:
        compaction = compaction.with_turn_start_rebuilds(TurnStartRebuilds(
                FLOOR, cold_tokens=cold, keep_full_context=skip, idle_tokens=cold if idle is None else idle
            )
        )
    return create_agent(
        model=model or anthropic_model("claude-opus-5-5-oauth"),
        tools=list(tools),
        system_prompt=system,
        middleware=[
            compaction,
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            ToolSurfaceMiddleware(),
            SystemPinMiddleware(),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {"input_tokens": input_tokens, "output_tokens": 50, "total_tokens": input_tokens + 50}
    return message


def _later(clock: _Clock, message: AIMessage, seconds: float = 10.0):
    """``message`` as an answer that takes ``seconds``, so the call after it
    starts that much later."""

    def answer(payload: dict) -> AIMessage:
        clock.now += seconds
        return message

    return answer


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "The user asked for data.", "signature": "cmp-1"}],
        id="msg_cmp",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


def _turn(clock: _Clock, last_tokens: int) -> list:
    return [
        _later(clock, _measured(reply(1, tool=("get_data", {"query": "a"})), 2_000)),
        _later(clock, _measured(reply(2, text="Here it is."), last_tokens)),
    ]


async def _state(agent) -> dict:
    return (await agent.aget_state(CFG)).values


def _instructions(payload: dict) -> str:
    return payload["extra_body"]["compaction"]["instructions"]


def _system(payload: dict) -> str:
    return json.dumps([block["text"] for block in payload["system"]])


@pytest.mark.asyncio
async def test_a_final_claude_answer_leaves_the_idle_clock_from_its_requests_start(monkeypatch, clock):
    Wire(_turn(clock, 6_000)).install(monkeypatch)
    started = clock.now
    agent = _agent(InMemorySaver(), "Prompt A.")
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)

    values = await _state(agent)
    answers = [m for m in values["messages"] if isinstance(m, AIMessage)]
    assert [request_started_at(m) for m in answers] == [started, started + 10]
    idle = values["_idle_clock"]
    assert (idle["requested_at"], idle["tail_requested_at"]) == (started + 10, None)
    assert idle["digest"]
    assert values["_last_answer"]["threshold"] == THRESHOLD


@pytest.mark.asyncio
async def test_with_the_feature_off_an_answer_is_neither_stamped_nor_leaves_a_clock(monkeypatch, clock):
    _feature(monkeypatch, False)
    Wire(_turn(clock, 6_000)).install(monkeypatch)
    agent = _agent(InMemorySaver(), "Prompt A.", cold=COLD)
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)

    values = await _state(agent)
    assert "_idle_clock" not in values
    assert [request_started_at(m) for m in values["messages"] if isinstance(m, AIMessage)] == [None, None]


@pytest.mark.asyncio
@pytest.mark.parametrize(("idle", "kind"), [(7_000, "I2"), (9_000, "I1")])
async def test_the_arm_reads_the_idle_threshold_the_turn_resolved(monkeypatch, clock, idle, kind):
    """cold_tokens (9,500) is never reached here; the idle threshold alone
    decides whether the summary keeping the tail arms."""
    Wire([*_turn(clock, 8_000), _later(clock, _measured(reply(3, text="Done."), 8_100))]).install(monkeypatch)
    saver = InMemorySaver()
    for text in ("Get a.", "Get b."):
        await _agent(saver, "Prompt A.", cold=9_500, idle=idle).ainvoke({"messages": [HumanMessage(text)]}, CFG)

    values = await _state(_agent(saver, "Prompt A."))
    assert values["_last_answer"]["idle_tokens"] == idle
    assert _armed(values).kind == kind


@pytest.mark.asyncio
async def test_a_five_minute_route_leaves_no_clock(monkeypatch, clock):
    """Its slot would come minutes after the answer, a pause to read it."""
    Wire(_turn(clock, 6_000)).install(monkeypatch)
    agent = _agent(InMemorySaver(), "Prompt A.", model=anthropic_model())
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)

    assert "_idle_clock" not in await _state(agent)


@pytest.mark.asyncio
async def test_no_clock_is_left_by_a_subagent_or_a_turn_that_ended_on_a_tool_call(monkeypatch, clock):
    Wire(
        [
            *_turn(clock, 6_000),
            _measured(reply(3, tool=("get_data", {"query": "b"})), 6_100),
            refusal("upstream failed"),
        ]
    ).install(monkeypatch)
    subagent = _agent(InMemorySaver(), "Prompt A.", main=False)
    await subagent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    main = _agent(InMemorySaver(), "Prompt A.")
    with pytest.raises(Exception, match="upstream failed"):
        await main.ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    assert "_idle_clock" not in await _state(subagent)
    assert "_idle_clock" not in await _state(main)


@pytest.mark.asyncio
async def test_an_answer_that_takes_the_context_past_cold_tokens_places_the_tail(monkeypatch, clock):
    """The call was sent under ``cold_tokens``; the context its answer leaves,
    which the arm reads, is over."""
    Wire(_turn(clock, 8_000)).install(monkeypatch)
    agent = _agent(InMemorySaver(), "Prompt A.", cold=COLD)
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)

    assert (await _state(agent))["_idle_clock"]["tail_requested_at"] is not None


def _armed(values: dict):
    return plan_firing(
        values["_idle_clock"],
        values["_last_answer"],
        tokens=measured_tokens(values),
        answered_at=values["_last_model_response_at"],
        stop_at=None,
        lead=CompactionConfig().idle_compact.lead_seconds,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("skip", "kind"), [(False, "I2"), (True, "I1")])
async def test_a_turn_sent_with_skip_arms_no_summary_keeping_the_tail(monkeypatch, clock, skip, kind):
    """The user chose the whole context over that summary; the next send
    can choose again. Only a rebuild, which a deploy may force, is armed."""
    Wire([*_turn(clock, 8_000), _later(clock, _measured(reply(3, text="Done."), 8_100))]).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.", cold=COLD).ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    await _agent(saver, "Prompt A.", cold=COLD, skip=skip).ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    values = await _state(_agent(saver, "Prompt A.", cold=COLD))
    assert (values["_idle_clock"]["tail_requested_at"] is None) is skip
    assert _armed(values).kind == kind


@pytest.mark.asyncio
async def test_a_deploy_after_a_turn_sent_with_skip_still_rebuilds_idle(monkeypatch, clock, caplog):
    """A send never skips a deploy's rebuild."""
    caplog.set_level("INFO")
    wire = Wire([*_turn(clock, 8_000), _compaction]).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.", cold=COLD, skip=True).ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    assert _armed(await _state(_agent(saver, "Prompt A."))).kind == "I1"
    monkeypatch.setattr(release, "source_digest", lambda: "the next deploy")
    clock.now += 2_700

    compaction = await _idle_ask(_agent(saver, "Prompt B.", cold=COLD))

    assert _instructions(wire.payloads[2]) == NO_TAIL
    assert compaction.preserved == []
    assert "idle=fire kind=I1" in caplog.text


@pytest.mark.asyncio
async def test_from_cold_tokens_the_clock_places_the_entry_at_the_tails_cut(monkeypatch, clock):
    Wire([*_turn(clock, 8_000), _later(clock, _measured(reply(3, text="Done."), 8_100))]).install(monkeypatch)
    agent = _agent(InMemorySaver(), "Prompt A.", cold=COLD)
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    await agent.ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    values = await _state(agent)
    idle = values["_idle_clock"]
    earlier = [request_started_at(m) for m in values["messages"] if isinstance(m, AIMessage)][:-1]
    assert idle["tail_requested_at"] in earlier
    assert idle["tail_requested_at"] < idle["requested_at"]


async def _idle_ask(agent):
    return await ask_between_turns(agent, await _state(agent), IdleCompact(), thread_id=CFG["configurable"]["thread_id"])


async def _deployed_after_a_turn(monkeypatch, clock, *after) -> tuple[Wire, InMemorySaver]:
    wire = Wire([*_turn(clock, 6_000), *after]).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(saver, "Prompt A.").ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    monkeypatch.setattr(release, "source_digest", lambda: "the next deploy")
    return wire, saver


@pytest.mark.asyncio
async def test_a_deployed_thread_is_rebuilt_from_its_warm_cache_keeping_no_turns(monkeypatch, clock, caplog):
    caplog.set_level("INFO")
    wire, saver = await _deployed_after_a_turn(monkeypatch, clock, _compaction)
    clock.now += 2_700

    compaction = await _idle_ask(_agent(saver, "Prompt B."))

    summary = wire.payloads[2]
    assert is_compaction_request(summary)
    assert _instructions(summary) == NO_TAIL
    assert "Prompt A." in _system(summary)
    assert compaction.preserved == []
    assert "idle=fire kind=I1" in caplog.text and "rebuild=yes reason=idle" in caplog.text


@pytest.mark.asyncio
async def test_a_large_thread_that_is_not_due_keeps_its_tail(monkeypatch, clock, caplog):
    caplog.set_level("INFO")
    wire = Wire([*_turn(clock, 8_000), _later(clock, _measured(reply(3, text="Done."), 8_100)), _compaction]).install(monkeypatch)
    agent = _agent(InMemorySaver(), "Prompt A.", cold=COLD)
    await agent.ainvoke({"messages": [HumanMessage("Get a.")]}, CFG)
    await agent.ainvoke({"messages": [HumanMessage("Get b.")]}, CFG)

    compaction = await _idle_ask(agent)

    assert _instructions(wire.payloads[3]) == KEEPS_TAIL
    assert compaction.preserved
    assert "idle=fire kind=I2" in caplog.text


@pytest.mark.asyncio
async def test_nothing_is_sent_once_the_request_differs_from_the_turns(monkeypatch, clock):
    """Another tool list reads none of the prefix the turn cached."""
    wire, saver = await _deployed_after_a_turn(monkeypatch, clock)

    with pytest.raises(IdleSkipped, match="request_changed"):
        await _idle_ask(_agent(saver, "Prompt B.", tools=(get_data, get_more)))
    assert len(wire.payloads) == 2


@pytest.mark.asyncio
async def test_nothing_is_sent_within_the_margin_of_the_entrys_expiry(monkeypatch, clock):
    wire, saver = await _deployed_after_a_turn(monkeypatch, clock)
    agent = _agent(saver, "Prompt B.")
    entry = request_started_at((await _state(agent))["messages"][-1])
    clock.now = entry + IDLE_TTL_SECONDS - IDLE_DEADLINE_MARGIN_S + 1

    with pytest.raises(IdleSkipped, match="expired"):
        await _idle_ask(agent)
    assert len(wire.payloads) == 2


@pytest.mark.asyncio
async def test_a_provider_that_writes_no_summary_gets_no_other_summarizer(monkeypatch, clock):
    """The summarizer would read the whole history uncached, the cost an
    idle compaction runs early to avoid."""
    wire, saver = await _deployed_after_a_turn(monkeypatch, clock, refusal("compaction failed"))

    with pytest.raises(NoOwnSummary):
        await _idle_ask(_agent(saver, "Prompt B."))
    assert len(wire.payloads) == 3
