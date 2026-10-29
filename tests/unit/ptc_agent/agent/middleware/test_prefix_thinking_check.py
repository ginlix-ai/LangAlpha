"""The doc's own check that kept thinking survived a compaction.

Compact the older turns while keeping one that holds a thinking block, then
send the next request with ``prefix_mismatch_behavior: "error"``: a 200 whose
``input_transformations`` is empty means no thinking block failed the check,
and a 400 naming the first block that did means one did. The Wire runs the
API's check, so a failure here is the 400 the API would send. It runs twice:
through the agent's compaction middleware, and as the doc's four steps sent
with the client alone, which also shows the beta without ``block_binding``
flagging a failing block while still sending it.
"""

from __future__ import annotations

import anthropic
import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    is_compaction_request,
    listed,
    reply,
    thinking_count,
)

THREAD = "d4e5f6a7-0000-4000-8000-000000000000"
THRESHOLD = 10_000


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


def _measured(message: AIMessage, input_tokens: int) -> AIMessage:
    message.usage_metadata = {"input_tokens": input_tokens, "output_tokens": 50, "total_tokens": input_tokens + 50}
    return message


def _compaction(payload: dict) -> AIMessage:
    return AIMessage(
        content=[{"type": "compaction", "content": "The user asked for help.", "signature": "cmp-1"}],
        id="msg_cmp",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        usage_metadata={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    )


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


def _agent(saver: InMemorySaver, rules: int = 800):
    model = anthropic_model()
    model.thinking = {**model.thinking, "block_binding": {"prefix_mismatch_behavior": "error"}}
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=THRESHOLD,
        keep_tokens=THRESHOLD // 3,
        claude_keep_tokens=THRESHOLD // 3,
    )
    return create_agent(
        model=model,
        tools=[get_data],
        system_prompt=" ".join("Answer plainly and briefly." for _ in range(rules)),
        middleware=[compaction, ReasoningCompatibilityMiddleware()],
        checkpointer=saver,
    )


def _replies() -> list:
    return [
            _measured(reply(1, text="Hello."), 2_000),
            _measured(reply(2, text="Ready."), 7_000),
            _measured(reply(3, tool=("get_data", {"query": "a"})), 7_100),
            _measured(reply(4, tool=("get_data", {"query": "b"})), 7_200),
            _measured(reply(5, tool=("get_data", {"query": "c"})), 9_990),
            _compaction,
            _measured(reply(7, text="Done."), 4_000),
            _measured(reply(8, tool=("get_data", {"query": "d"})), 4_100),
            _measured(reply(9, text="Still done."), 4_200),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_kept_thinking_passes_the_check_after_a_compaction(monkeypatch, stream):
    wire = Wire(_replies()).install(monkeypatch, stream=stream)
    cfg = {"configurable": {"thread_id": THREAD, "user_id": "u-1"}}
    saver = InMemorySaver()
    for text in ("Hi.", "Get ready.", "Look at NVDA.", "And now?"):
        await _agent(saver).ainvoke({"messages": [HumanMessage(text)]}, cfg)

    assert len(wire.payloads) == 9
    assert is_compaction_request(wire.payloads[5])
    assert wire.payloads[6]["messages"][0]["content"][0]["type"] == "compaction"
    assert [thinking_count(p) for p in wire.payloads[6:]] == [4, 5, 6]
    assert wire.transformations[6:] == [[], [], []]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_a_system_prompt_changed_after_the_compaction_is_refused(monkeypatch, stream):
    """The check this test relies on is live: a changed ``system`` fails the
    first kept block, and the request is refused naming it."""
    wire = Wire(_replies()).install(monkeypatch, stream=stream)
    cfg = {"configurable": {"thread_id": THREAD, "user_id": "u-1"}}
    saver = InMemorySaver()
    for text in ("Hi.", "Get ready.", "Look at NVDA."):
        await _agent(saver).ainvoke({"messages": [HumanMessage(text)]}, cfg)

    with pytest.raises(anthropic.BadRequestError, match=r"messages\.1\.content\.0: .*different conversation"):
        await _agent(saver, rules=801).ainvoke({"messages": [HumanMessage("And now?")]}, cfg)
    assert wire.transformations[-1] is None


def _client(behavior: str | None):
    model = anthropic_model()
    thinking = {k: v for k, v in model.thinking.items() if k != "block_binding"}
    if behavior is not None:
        thinking["block_binding"] = {"prefix_mismatch_behavior": behavior}
    model.thinking = thinking
    return model


def _recipe_replies() -> list:
    return [
        reply(1, text="Recipe, Ingredient and Step."),
        reply(2, text="March 18."),
        AIMessage(
            content=[{"type": "compaction", "content": "The user is planning a release.", "signature": "cmp-1"}],
            response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "compaction"},
        ),
        reply(4, text="Thursday."),
    ]


async def _recipe(model, kept_user: str = "When does testing end?"):
    """The doc's four steps: two turns with thinking, a summary of the first,
    then the block, the kept turn and a new question."""
    system = SystemMessage("You help plan a recipe app's release. Keep answers short.")
    history = [system, HumanMessage("What are the main entities?")]
    history += [await model.ainvoke(history), HumanMessage("When does testing end?")]
    history.append(await model.ainvoke(history))
    summary = await model.ainvoke(history[:3], extra_body={"compaction": {"type": "summarize"}})
    kept = [HumanMessage(kept_user), history[4]]
    return await model.ainvoke(
        [system, AIMessage(content=summary.content), *kept, HumanMessage("Which day should it go out?")]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_the_docs_recipe_passes_the_check(monkeypatch, stream):
    wire = Wire(_recipe_replies()).install(monkeypatch, stream=stream)
    await _recipe(_client("error"))

    assert is_compaction_request(wire.payloads[2])
    assert thinking_count(wire.payloads[3]) == 1
    assert wire.transformations[3] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_a_kept_turn_sent_changed_is_refused(monkeypatch, stream):
    Wire(_recipe_replies()).install(monkeypatch, stream=stream)
    with pytest.raises(anthropic.BadRequestError, match=r"messages\.2\.content\.0: .*different conversation"):
        await _recipe(_client("error"), kept_user="When does the testing end?")


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_the_beta_alone_flags_a_failing_block_and_sends_it(monkeypatch, stream):
    """With the beta and no ``block_binding``, an older account's request is
    not enforced: a failing block reaches the model and the answer lists it."""
    wire = Wire(_recipe_replies()).install(monkeypatch, stream=stream)
    answer = await _recipe(_client(None), kept_user="When does the testing end?")

    flagged = [{"type": "thinking_mismatch_allowed", "path": "messages.2.content.0", "reason": "prefix_binding_mismatch"}]
    assert wire.transformations[3] == flagged
    assert thinking_count(wire.payloads[3]) == 1
    assert listed([answer]) == [flagged]
