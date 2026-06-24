"""Tests for the one-off DeltaChannel backfill (scripts/ops/backfill_delta.py).

The core regression: a legacy ``add_messages`` thread, after ``messages`` is
opted into ``DeltaChannel``, drops its first post-delta message unless the lineage
is re-snapshotted first (Finding B). These tests pin that ``apply=True`` prevents
the loss, preserves other state channels, and is idempotent; that ``apply=False``
(dry-run) classifies without writing; plus the store-marker guard short-circuits.
"""

import asyncio

import pytest
from langchain.agents import AgentState
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph

from scripts.ops.backfill_delta import (
    _build_resnapshot_graph,
    _migrate_lineage,
    run_delta_backfill_sweep,
)
from src.server.utils.checkpointer import IdStampingCheckpointerMixin


class _ShimmedMemorySaver(IdStampingCheckpointerMixin, InMemorySaver):
    """In-memory saver with the prod id-stamping shim (matches the real saver)."""


class _LegacyRichState(AgentState):
    # An extra non-messages channel, to prove the re-snapshot preserves channels
    # the re-snapshot graph never declares.
    note: str


def _legacy_graph(saver):
    """A plain add_messages graph — simulates a pre-delta thread."""
    builder = StateGraph(_LegacyRichState)
    builder.add_node(
        "respond", lambda _s: {"messages": [AIMessage("a1")], "note": "keep"}
    )
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)
    return builder.compile(checkpointer=saver)


@pytest.mark.asyncio
async def test_resnapshot_migrates_legacy_lineage_without_loss():
    saver = _ShimmedMemorySaver()
    cfg = {"configurable": {"thread_id": "t-legacy", "checkpoint_ns": ""}}
    await _legacy_graph(saver).ainvoke(
        {"messages": [HumanMessage("q1")], "note": "keep"}, cfg
    )

    # Head is legacy: messages stored as a plain list (q1 + a1).
    head = (await saver.aget_tuple(cfg)).checkpoint["channel_values"]
    assert isinstance(head["messages"], list)
    before = len(head["messages"])
    assert before == 2

    graph = _build_resnapshot_graph(saver)
    sem = asyncio.Semaphore(1)
    assert (
        await _migrate_lineage(graph, saver, "t-legacy", sem, apply=True) == "migrated"
    )

    # Head is now a delta checkpoint (messages no longer a plain list), and the
    # unrelated "note" channel survived the re-snapshot.
    after = (await saver.aget_tuple(cfg)).checkpoint
    assert not isinstance(after["channel_values"].get("messages"), list)
    assert "note" in after["channel_versions"]

    # A real post-migration turn (user + assistant): both survive -> count +2.
    await graph.aupdate_state(cfg, {"messages": [HumanMessage("q2"), AIMessage("a2")]})
    contents = [m.content for m in (await graph.aget_state(cfg)).values["messages"]]
    assert contents == ["q1", "a1", "q2", "a2"]
    assert len(contents) == before + 2

    # Idempotent: a re-run skips the already-migrated lineage.
    assert await _migrate_lineage(graph, saver, "t-legacy", sem, apply=True) == "delta"


@pytest.mark.asyncio
async def test_without_resnapshot_first_post_delta_message_is_lost():
    """Control: proves the sweep is meaningful — writing straight onto the legacy
    head (no re-snapshot) loses the first post-delta message (Finding B)."""
    saver = _ShimmedMemorySaver()
    cfg = {"configurable": {"thread_id": "t-nofix", "checkpoint_ns": ""}}
    await _legacy_graph(saver).ainvoke(
        {"messages": [HumanMessage("q1")], "note": "keep"}, cfg
    )

    graph = _build_resnapshot_graph(saver)
    await graph.aupdate_state(cfg, {"messages": [HumanMessage("q2")]})
    contents = [m.content for m in (await graph.aget_state(cfg)).values["messages"]]
    assert "q2" not in contents


@pytest.mark.asyncio
async def test_dry_run_reports_without_writing():
    """apply=False classifies a legacy lineage as would-migrate but writes nothing."""
    saver = _ShimmedMemorySaver()
    cfg = {"configurable": {"thread_id": "t-dry", "checkpoint_ns": ""}}
    await _legacy_graph(saver).ainvoke(
        {"messages": [HumanMessage("q1")], "note": "keep"}, cfg
    )

    graph = _build_resnapshot_graph(saver)
    sem = asyncio.Semaphore(1)
    # Reports it would migrate...
    assert (
        await _migrate_lineage(graph, saver, "t-dry", sem, apply=False) == "migrated"
    )
    # ...but the head is untouched: messages still a plain legacy list.
    head = (await saver.aget_tuple(cfg)).checkpoint["channel_values"]
    assert isinstance(head["messages"], list)


@pytest.mark.asyncio
async def test_sweep_no_store_is_noop():
    """In-memory mode (no Postgres store) -> nothing durable to migrate."""
    assert await run_delta_backfill_sweep(_ShimmedMemorySaver(), None) is None


@pytest.mark.asyncio
async def test_sweep_skips_when_marker_present():
    """Once the marker is set, the sweep skips the rescan and writes nothing."""

    class _FakeStore:
        def __init__(self):
            self.put_calls = []

        async def aget(self, ns, key):
            return {"completed_at": "earlier"}

        async def aput(self, ns, key, value):
            self.put_calls.append(value)

    class _FakeCkpt:
        conn = object()  # satisfies the Postgres-checkpointer guard

    store = _FakeStore()
    assert await run_delta_backfill_sweep(_FakeCkpt(), store, apply=True) is None
    assert store.put_calls == []
