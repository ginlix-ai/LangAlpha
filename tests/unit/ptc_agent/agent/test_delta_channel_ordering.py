"""Same-superstep replay ordering for `DeltaChannel`-backed `messages`.

When two or more parallel tasks write to ``messages`` in the same superstep, a
non-snapshot checkpoint rebuilds the channel by replaying their stored writes.
Live ``apply_writes`` applies them in ``task_path`` order; through
langgraph-checkpoint 4.1 the savers replayed them in another (Postgres by
``task_id, idx``, in memory nondeterministically), so a reload could reorder
what the model saw live. langgraph-checkpoint 4.3 makes ``writes_sort_key``
the order both sides use, so reconstruction must now equal the live order.

The graph is a genuine fan-out: one seed node fans out to two sibling nodes
that each write one ``messages`` update in the same superstep (a single node
returning a two-element list is one write and would never reorder).
"""

from __future__ import annotations

from collections import Counter

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, StateGraph

from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.state import DeltaAgentState

# Stable ids so we compare by id/content, independent of stamping behaviour.
_SEED_ID = "seed"
_LEFT_ID = "left"
_RIGHT_ID = "right"

# Live (apply_writes / task_path) order, measured stable across 50 runs.
_EXPECTED_LIVE_ORDER = [_SEED_ID, _LEFT_ID, _RIGHT_ID]


def _build_fan_out_graph(saver):
    """seed -> {left, right} in parallel -> END; left+right write in one superstep."""
    builder = StateGraph(DeltaAgentState)

    builder.add_node("seed", lambda _s: {"messages": [HumanMessage("seed", id=_SEED_ID)]})
    builder.add_node("left", lambda _s: {"messages": [AIMessage("left", id=_LEFT_ID)]})
    builder.add_node("right", lambda _s: {"messages": [AIMessage("right", id=_RIGHT_ID)]})

    builder.add_edge(START, "seed")
    # Fan-out: two outgoing edges from one node -> both run in the same superstep.
    builder.add_edge("seed", "left")
    builder.add_edge("seed", "right")
    builder.add_edge("left", END)
    builder.add_edge("right", END)
    return builder.compile(checkpointer=saver)


def test_fan_out_live_order_is_deterministic():
    """LIVE same-superstep order is the deterministic apply_writes / task_path order.

    This is the rock-solid guard: the sequential orchestrator path and the model's
    live context depend on this ordering, and it must stay stable. If a future
    langgraph/langchain change perturbs live fan-out ordering this fails loudly.
    """
    graph = _build_fan_out_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "fanout-live"}}
    result = graph.invoke({"messages": []}, config)

    live_ids = [m.id for m in result["messages"]]
    assert live_ids == _EXPECTED_LIVE_ORDER, (
        "live same-superstep fan-out order changed; the orchestrator and model "
        f"context rely on a stable order. got {live_ids}"
    )


def test_fan_out_head_checkpoint_is_non_snapshot():
    """The head checkpoint is a sentinel, so reconstruction exercises delta replay."""
    saver = InMemorySaver()
    graph = _build_fan_out_graph(saver)
    config = {"configurable": {"thread_id": "fanout-sentinel"}}
    graph.invoke({"messages": []}, config)

    channel_values = saver.get_tuple(config).checkpoint["channel_values"]
    assert "messages" not in channel_values, (
        "expected a non-snapshot delta step so reconstruction replays writes; "
        f"head stored messages directly: {list(channel_values)}"
    )


def test_fan_out_reconstruction_preserves_all_messages():
    """Integrity guard: reconstruction loses or duplicates no message.

    Multiset-equality is the part that IS reliable — a dropped or duplicated
    message on delta replay is a real corruption regression and must fail loudly.
    Order is asserted separately below, so a failure says which of the two broke.
    """
    graph = _build_fan_out_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "fanout-recon-set"}}
    result = graph.invoke({"messages": []}, config)
    live_ids = [m.id for m in result["messages"]]

    recon_ids = [m.id for m in graph.get_state(config).values["messages"]]

    # Counter, not set: a DUPLICATED id must fail here too, and set() collapses
    # duplicates (so a [seed, A, A] reconstruction would pass against {seed, A}).
    # Duplication is exactly the corruption this guard exists to catch; order is
    # the next test's concern, so compare as a multiset, not a sequence.
    assert Counter(recon_ids) == Counter(live_ids), (
        f"reconstruction lost or duplicated a message: recon {sorted(recon_ids)} "
        f"!= live {sorted(live_ids)}"
    )


def test_fan_out_reconstruction_preserves_live_order():
    """Reconstructed order matches live order on every run.

    Repeated because the in-memory replay this replaced reordered about half
    the time, so one run proves little.
    """
    for run in range(20):
        graph = _build_fan_out_graph(InMemorySaver())
        config = {"configurable": {"thread_id": f"fanout-recon-{run}"}}
        result = graph.invoke({"messages": []}, config)
        live_ids = [m.id for m in result["messages"]]

        recon_ids = [m.id for m in graph.get_state(config).values["messages"]]

        assert recon_ids == live_ids, (
            f"run {run}: reconstructed order {recon_ids} != live order {live_ids}"
        )
