"""Same-superstep replay ordering for `DeltaChannel`-backed `messages` (P2).

ACCEPTED RESIDUAL RISK (documented, not a blocker). When two or more parallel
tasks write to the ``messages`` channel in the *same* superstep, the order they
reconstruct from a non-snapshot delta checkpoint may differ from the order they
ran live. Live ``apply_writes`` orders same-superstep task writes by ``task_path``
(`langgraph/pregel/_algo.py`); the **Postgres** saver's delta stage-2 replay
(`aget_delta_channel_history`) orders by ``(task_id, idx)`` and ignores the stored
``task_path`` — so the two can disagree. This is library-level (beta); there is
no app-side fix, and the plan accepts it because the UI renders from persisted
SSE events in emission order, never from the reconstructed checkpoint list.

What this test does:

1. Builds a genuine FAN-OUT — one seed node fans out to two sibling nodes that
   each write one ``messages`` update in the SAME superstep (NOT a single node
   returning a 2-element list, which is one write and would never reorder).
2. Asserts the LIVE order is deterministic and is the expected ``apply_writes`` /
   ``task_path`` order. This guards the path the sequential orchestrator and the
   model's live context actually depend on, and it is rock-solid (measured stable
   across 50 runs).
3. Best-effort: compares the reconstructed order against the live order.

KNOWN LIMITATION — why the reconstruction comparison is `xfail`:

The real, documented divergence (`task_id,idx` vs `task_path`) lives specifically
in the POSTGRES saver's two-stage SQL replay. The in-memory saver has its OWN
``get_delta_channel_history`` implementation that does NOT reproduce that
particular ordering rule. What it DOES show is that same-superstep reconstruction
order is *non-deterministic* in-memory (measured: live order is always
``[seed, A, B]`` but reconstruction is ``[seed, A, B]`` or ``[seed, B, A]`` ~50/50
across runs). That is a flaky, different-mechanism reorder, so we cannot make a
stable in-memory assertion that reconstruction == live. We therefore mark the
reconstruction-order comparison ``xfail(strict=False)``: it passes when the run
happens to preserve order and "x-fails" when it doesn't, and the reason documents
that exercising the *real* Postgres divergence requires a Postgres saver. This is
exactly the residual the plan says to document, not block on.
"""

from __future__ import annotations

from collections import Counter

import pytest
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
    """Integrity guard (reliable, NOT xfail): reconstruction loses/dups no message.

    Multiset-equality is the part that IS reliable — a dropped or duplicated
    message on delta replay is a real corruption regression and must fail loudly.
    Only the *order* is the accepted non-deterministic residual, asserted separately
    in the xfail test below; keeping integrity out of the xfail umbrella means a
    genuine message-loss bug can no longer hide as an expected failure.
    """
    graph = _build_fan_out_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "fanout-recon-set"}}
    result = graph.invoke({"messages": []}, config)
    live_ids = [m.id for m in result["messages"]]

    recon_ids = [m.id for m in graph.get_state(config).values["messages"]]

    # Counter, not set: a DUPLICATED id must fail here too, and set() collapses
    # duplicates (so a [seed, A, A] reconstruction would pass against {seed, A}).
    # Duplication is exactly the corruption this guard exists to catch; order is
    # the separate xfail concern, so compare as a multiset, not a sequence.
    assert Counter(recon_ids) == Counter(live_ids), (
        f"reconstruction lost or duplicated a message: recon {sorted(recon_ids)} "
        f"!= live {sorted(live_ids)}"
    )


@pytest.mark.xfail(
    strict=False,
    reason=(
        "In-memory delta reconstruction of same-superstep fan-out writes is "
        "non-deterministic (~50/50 order across runs) and does NOT reproduce the "
        "specific documented Postgres divergence (task_id,idx vs task_path). The "
        "real divergence lives in AsyncPostgresSaver.aget_delta_channel_history and "
        "requires a Postgres saver to exercise; this is an accepted residual risk "
        "with no app-side fix. See module docstring."
    ),
)
def test_fan_out_reconstruction_preserves_live_order():
    """Best-effort (xfail): reconstructed ORDER matches live order.

    Passes when the in-memory replay happens to preserve order; x-fails when it
    reorders. Message integrity (nothing lost/duplicated) is asserted reliably and
    separately in ``test_fan_out_reconstruction_preserves_all_messages`` — only the
    order is the accepted P2 residual absorbed here. Exercising the real Postgres
    divergence is left to integration tests against a Postgres saver.
    """
    graph = _build_fan_out_graph(InMemorySaver())
    config = {"configurable": {"thread_id": "fanout-recon"}}
    result = graph.invoke({"messages": []}, config)
    live_ids = [m.id for m in result["messages"]]

    recon_ids = [m.id for m in graph.get_state(config).values["messages"]]

    # Order is the residual: assert it; xfail absorbs the in-memory non-determinism.
    assert recon_ids == live_ids, (
        f"reconstructed order {recon_ids} != live order {live_ids} "
        "(accepted P2 residual under in-memory replay)"
    )
