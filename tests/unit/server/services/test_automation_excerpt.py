"""The one-line excerpt an automation card leads with, and what a completed
run said: its answer, and the result it sent."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.automation_excerpt import RunAnswer, plain_excerpt, read_run_answer


@pytest.mark.parametrize(
    "answer, excerpt",
    [
        ("## Summary\n\nStocks **rose** today.", "Summary Stocks rose today."),
        ("Intro:\n\n- one\n- two\n\n> quoted", "Intro: one two quoted"),
        ("| a | b |\n|---|:---:|\n| 1 | 2 |", "a b 1 2"),
        ("See [the filing](https://example.com) now.", "See the filing now."),
        ("Before\n```python\nprint(1)\n```\nAfter", "Before After"),
        # Escaped markup is stripped like markup, never revived by the unescape.
        ("Text &lt;b&gt;bold&lt;/b&gt; &amp; more", "Text bold & more"),
        # Markup is recognised within a line only.
        ("Total\n-\nNext", "Total - Next"),
    ],
)
def test_markup_flattens_to_prose(answer, excerpt):
    assert plain_excerpt(answer) == excerpt


def test_a_long_answer_is_cut_at_a_word():
    excerpt = plain_excerpt("word " * 400)
    assert excerpt.endswith("…")
    assert len(excerpt) <= 321
    assert not excerpt[:-1].endswith(" ")


# ─── What a completed run said ────────────────────────────────────────

_READER = "src.server.services.history.reader.CheckpointHistoryReader.get_instance"


def _send(call_id, text="", files=None):
    args = {"text": text, **({"files": files} if files else {})}
    return AIMessage(
        content="",
        tool_calls=[{"name": "send_message", "args": args, "id": call_id, "type": "tool_call"}],
    )


def _result(call_id, status, *, artifact=True, error=False):
    return ToolMessage(
        content=f"status: {status}\nto: slack:T/C",
        tool_call_id=call_id,
        artifact={"type": "message_delivery", "status": status} if artifact else None,
        status="error" if error else "success",
    )


async def _read(*messages, run_id="run-1"):
    turn = SimpleNamespace(run_id=run_id, messages=[HumanMessage("Brief me"), *messages])
    reader = SimpleNamespace(
        aget_run_turn=AsyncMock(
            side_effect=lambda _thread, run: turn if run == turn.run_id else None
        )
    )
    with patch(_READER, return_value=reader):
        return await read_run_answer("thread-1", "run-1")


@pytest.mark.asyncio
async def test_a_sign_off_after_a_send_leaves_the_send_as_what_it_sent():
    answer = await _read(
        _send("c1", "**Markets** rose."),
        _result("c1", "sent"),
        AIMessage("Sent the brief to #demo."),
    )

    assert answer == RunAnswer(text="Sent the brief to #demo.", sent="**Markets** rose.")


@pytest.mark.asyncio
async def test_a_run_that_sent_nothing_has_only_its_answer():
    answer = await _read(AIMessage("Markets rose."))

    assert answer == RunAnswer(text="Markets rose.", sent=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missed",
    [
        _result("c2", "failed"),
        _result("c2", "unknown"),
        # The tool raised: nothing went out, whatever the text says.
        _result("c2", "sent", error=True),
    ],
    ids=["failed", "unknown", "raised"],
)
async def test_a_send_that_did_not_go_out_is_passed_over(missed):
    answer = await _read(
        _send("c1", "The brief."),
        _result("c1", "sent"),
        _send("c2", "The brief, again."),
        missed,
        AIMessage("Done."),
    )

    assert answer.sent == "The brief."


@pytest.mark.asyncio
async def test_a_send_of_files_alone_is_passed_over():
    answer = await _read(
        _send("c1", "The brief."),
        _result("c1", "sent"),
        _send("c2", "  ", files=["results/brief.xlsx"]),
        _result("c2", "sent"),
    )

    assert answer.sent == "The brief."


@pytest.mark.asyncio
async def test_a_partial_send_went_out_and_a_result_without_its_artifact_reads_its_text():
    """On partial the text went and some files didn't; with no delivery
    artifact the status is the first line the model read."""
    answer = await _read(_send("c1", "The brief."), _result("c1", "partial", artifact=False))

    assert answer.sent == "The brief."


@pytest.mark.asyncio
async def test_another_runs_turn_says_nothing():
    answer = await _read(
        _send("c1", "The brief."), _result("c1", "sent"), AIMessage("Done."), run_id="run-2"
    )

    assert answer == RunAnswer()


@pytest.mark.asyncio
async def test_a_failed_read_says_nothing():
    reader = SimpleNamespace(aget_run_turn=AsyncMock(side_effect=RuntimeError("db")))
    with patch(_READER, return_value=reader):
        assert await read_run_answer("thread-1", "run-1") == RunAnswer()


@pytest.mark.asyncio
async def test_a_turn_the_thread_took_since_does_not_hide_the_runs_answer():
    """The settle can run after a follow-up turn started on the thread: the
    run's own turn is still the one read."""
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import START, StateGraph

    from ptc_agent.agent.state import DeltaAgentState
    from src.server.services.history.reader import CheckpointHistoryReader

    def agent(state):
        asked = [m for m in state["messages"] if isinstance(m, HumanMessage)][-1]
        return {"messages": [AIMessage(f"Answer to {asked.content}", id=f"ai-{asked.id}")]}

    saver = InMemorySaver()
    graph = (
        StateGraph(DeltaAgentState)
        .add_node("agent", agent)
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    for run, question in (("run-1", "Brief me"), ("run-2", "And bonds?")):
        await graph.ainvoke(
            {"messages": [HumanMessage(question, id=run)]},
            {"configurable": {"thread_id": "thread-1"}, "metadata": {"run_id": run}},
        )

    with patch(_READER, return_value=CheckpointHistoryReader(saver)):
        answer = await read_run_answer("thread-1", "run-1")

    assert answer == RunAnswer(text="Answer to Brief me", sent=None)


async def _two_runs(monkeypatch):
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import START, StateGraph

    from ptc_agent.agent.state import DeltaAgentState
    from src.server.services.history.reader import CheckpointHistoryReader

    saver = InMemorySaver()
    graph = (
        StateGraph(DeltaAgentState)
        .add_node(
            "agent",
            lambda s: {"messages": [AIMessage(f"**Answer** {len(s['messages'])}")]},
        )
        .add_edge(START, "agent")
        .compile(checkpointer=saver)
    )
    for n in range(2):
        await graph.ainvoke(
            {"messages": [HumanMessage(f"q{n}")]},
            {"configurable": {"thread_id": "t"}, "metadata": {"run_id": f"run-{n}"}},
        )
    monkeypatch.setattr(
        CheckpointHistoryReader, "get_instance", lambda: CheckpointHistoryReader(saver)
    )


@pytest.mark.asyncio
async def test_the_newest_run_reads_its_answer(monkeypatch):
    await _two_runs(monkeypatch)
    assert await read_run_answer("t", "run-1") == RunAnswer(text="**Answer** 3")


@pytest.mark.asyncio
async def test_a_run_a_newer_turn_followed_still_reads_its_own(monkeypatch):
    await _two_runs(monkeypatch)
    assert await read_run_answer("t", "run-0") == RunAnswer(text="**Answer** 1")


@pytest.mark.asyncio
async def test_a_run_with_no_turn_has_no_answer(monkeypatch):
    await _two_runs(monkeypatch)
    assert await read_run_answer("t", "run-9") == RunAnswer()
