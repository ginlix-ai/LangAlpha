"""`aupdate_state` injection must pre-stamp message ids (compaction Bug A).

Unlike the normal Pregel write path (covered by
`test_delta_channel_determinism.py`), `graph.aupdate_state` does NOT run
langgraph's `ensure_message_ids`. A message injected id-less via `aupdate_state`
(orchestrator notifications / steering triggers) therefore persists with
`id=None` under `messages_delta_reducer`, and a later full-list re-write appends
it again as a duplicate. `ptc_agent.agent.state.ensure_message_ids`, applied at
the injection boundary, closes the hole — the injected message carries a stable
id, reconstructs deterministically, and dedups on re-write.
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


def test_aupdate_state_does_not_stamp_idless_injection():
    """Baseline: an id-less `aupdate_state` injection persists with `id=None`.

    This is the hole the fix closes. If a langgraph bump starts stamping here,
    this assertion flips and `ensure_message_ids` becomes a redundant no-op
    (still harmless) — a deliberate canary on that behaviour.
    """
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "idless-inject"}}

    graph.update_state(
        config, {"messages": [HumanMessage(content="hi")]}, as_node="noop"
    )

    assert _ids(graph, config) == [None]


def test_unstamped_injection_duplicates_on_full_rewrite():
    """The concrete harm: an id-less injection duplicates on a full-list re-write."""
    graph = _build_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "idless-dup"}}

    graph.update_state(
        config, {"messages": [HumanMessage(content="hi")]}, as_node="noop"
    )
    before = len(graph.get_state(config).values["messages"])

    _full_rewrite(graph, config)
    after = len(graph.get_state(config).values["messages"])

    assert before == 1 and after == 2, (
        "id-less injection must duplicate on full-list re-write (the bug the "
        "injection-time stamp + flush exclusion fix together close)"
    )


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
