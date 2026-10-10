"""The one-line excerpt an automation card leads with."""

import pytest

from src.server.services.automation_excerpt import plain_excerpt


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


async def _two_runs(monkeypatch):
    from langchain_core.messages import AIMessage, HumanMessage
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
    from src.server.services.automation_excerpt import read_run_excerpt

    await _two_runs(monkeypatch)
    assert await read_run_excerpt("t", "run-1") == "Answer 3"


@pytest.mark.asyncio
async def test_a_run_a_newer_turn_followed_leaves_none(monkeypatch):
    from src.server.services.automation_excerpt import read_run_excerpt

    await _two_runs(monkeypatch)
    assert await read_run_excerpt("t", "run-0") is None
