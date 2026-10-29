"""`aupdate_state` writes give their messages stable ids (compaction Bug A).

The orchestrator's notifications and the steering triggers reach the thread
through `graph.aupdate_state`. Through langgraph 1.2.14 that path skipped
`ensure_message_ids`, so an id-less injection persisted with `id=None` under
`messages_delta_reducer` and a later full-list re-write appended it again.
langgraph 1.2.15 stamps update writes as the loop's `put_writes` does; the
`ptc_agent.agent.state.ensure_message_ids` the injectors call does the same
ahead of it.
"""

from __future__ import annotations

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from ptc_agent.agent.state import DeltaAgentState, ensure_message_ids


def _build_graph(saver):
    builder = StateGraph(DeltaAgentState)

    def noop(_state: DeltaAgentState) -> dict:
        return {}

    builder.add_node("noop", noop)
    builder.add_edge(START, "noop")
    builder.add_edge("noop", END)
    return builder.compile(checkpointer=saver)


def _ids(graph, config) -> list:
    return [getattr(m, "id", None) for m in graph.get_state(config).values["messages"]]


def _full_rewrite(graph, config) -> None:
    """Re-apply the reconstructed message list — the pre-fix flush shape."""
    msgs = graph.get_state(config).values["messages"]
    graph.update_state(config, {"messages": msgs}, as_node="noop")


def test_aupdate_state_stamps_an_idless_injection():
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "idless-inject"}}

    graph.update_state(
        config, {"messages": [HumanMessage(content="hi")]}, as_node="noop"
    )

    ids = _ids(graph, config)
    assert len(ids) == 1 and ids[0] is not None
    assert _ids(graph, config) == ids  # deterministic across reconstruction


def test_idless_injection_dedups_on_full_rewrite():
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "idless-dup"}}

    graph.update_state(
        config, {"messages": [HumanMessage(content="hi")]}, as_node="noop"
    )
    ids = _ids(graph, config)

    _full_rewrite(graph, config)
    assert _ids(graph, config) == ids, "an injected message must not duplicate"


def test_stamped_injection_is_stable_and_dedups_on_rewrite():
    """With `ensure_message_ids` the injected message carries a stable id,
    reconstructs deterministically, and a full-list re-write does not duplicate."""
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "stamped-inject"}}

    graph.update_state(
        config,
        {"messages": ensure_message_ids([HumanMessage(content="hi")])},
        as_node="noop",
    )

    ids_a = _ids(graph, config)
    assert ids_a[0] is not None
    assert _ids(graph, config) == ids_a  # deterministic across reconstruction

    _full_rewrite(graph, config)
    assert _ids(graph, config) == ids_a, (
        "stamped injection must dedup on full-list re-write"
    )
