"""The server's side of the window: replay reads the same turns from a trimmed
thread, the coverage the agent asks before a trim, the trimmed runs read back
for a transcript with no stored copy to carry them, and the commit pointer a
manual compaction moves.

The thread is ``window_harness``'s scenario, run with the trim and without
it on the real graph and checkpointer, read by the real history reader."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import pytest
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, MessagesState, StateGraph
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langsmith import tracing_context

from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.transcript import (
    EarlierTurnsMissing,
    TranscriptTarget,
    Window,
    build_directory,
)
from ptc_agent.agent.transcript.render import split_runs
from src.server.services.history import slices
from src.server.database.conversation import turn_slices as slices_db
from src.server.services import transcripts
from src.server.services.history import replay
from src.server.services.history import window as window_module
from src.server.services.history.reader import CheckpointHistoryReader
from tests.unit.middleware.compaction import window_harness as h
from tests.unit.server.services.history.replay_builders import (
    thread_rows,
    SliceStore,
    _cache_probe,
    _query,
    _response,
)

pytestmark = pytest.mark.asyncio

TURNS = len(h.SCENARIO)


@dataclass
class Thread:
    run: h.ThreadRun
    saver: InMemorySaver

    @property
    def tip(self) -> str:
        return self.run.tails[-1]

    @property
    def base(self) -> int:
        return self.run.held[-1][1]

    def head(self, runs: int) -> Window:
        """The thread's first ``runs`` runs, as a trim of them would leave
        its head."""
        whole = h.whole_threads(self.run)[-1]
        return Window().extend([m for run in split_runs(whole)[:runs] for m in run])

    async def trimmed(self) -> Window:
        """What the window left trimmed at the tip."""
        values = (await self.run.graph.aget_state(h.config())).values
        return Window.of(values)


@pytest.fixture(scope="module")
def threads() -> tuple[Thread, Thread]:
    async def run() -> tuple[Thread, Thread]:
        off, on = InMemorySaver(), InMemorySaver()
        return (
            Thread(await h.run_scenario(off, window=False), off),
            Thread(await h.run_scenario(on, window=True), on),
        )

    return asyncio.run(run())


def _serve(monkeypatch: pytest.MonkeyPatch, thread: Thread, store: SliceStore) -> None:
    """The real reader over ``thread``'s checkpoints, and ``store`` for the
    slice tables, the anchor-keyed reads the window makes included."""
    reader = CheckpointHistoryReader(thread.saver)
    monkeypatch.setattr(
        CheckpointHistoryReader, "get_instance", classmethod(lambda cls: reader)
    )
    for name in (
        "get_turn_lines",
        "get_turn_slices",
        "has_turn_slices",
        "get_slices_at",
        "upsert_turn_slices",
        "update_turns",
        "get_run_slices",
        "upsert_run_slices",
    ):
        monkeypatch.setattr(slices_db, name, getattr(store, name))

    async def pointer(thread_id):
        return thread.tip

    monkeypatch.setattr(window_module, "get_thread_checkpoint_id", pointer)


async def _page(monkeypatch, thread: Thread, store: SliceStore) -> str:
    _serve(monkeypatch, thread, store)
    page = await replay.build_replay_page(
        thread_rows([_query(i, content=f"Question {i + 1}") for i in range(TURNS)], {i: _response(i) for i in range(TURNS)}, thread_id=h.THREAD),
    )
    return h.Normalizer()(json.dumps([line.item() for line in page.lines], default=str))


def _text(messages) -> str:
    return h.Normalizer()(h.dump(list(messages)))


# ------------------------------------------------------------ replay


async def test_replay_reads_a_trimmed_thread_as_the_whole_one(
    monkeypatch, threads
) -> None:
    off, on = threads
    _cache_probe(monkeypatch)
    stores = SliceStore(), SliceStore()
    cold = (
        await _page(monkeypatch, off, stores[0]),
        await _page(monkeypatch, on, stores[1]),
    )
    assert cold[1] == cold[0]
    assert len(stores[1].turns) == TURNS
    warm = (
        await _page(monkeypatch, off, stores[0]),
        await _page(monkeypatch, on, stores[1]),
    )
    assert warm == cold


# ------------------------------------------------------------ coverage


async def test_the_agent_and_the_server_name_the_same_head(threads) -> None:
    _, on = threads
    trimmed = await on.trimmed()
    assert trimmed.runs == on.base > 0
    assert trimmed == on.head(on.base)


async def test_coverage_holds_once_the_slices_are_stored(monkeypatch, threads) -> None:
    _, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    _serve(monkeypatch, on, store)
    assert await window_module.runs_held(h.THREAD, Window())
    assert not await window_module.runs_held(h.THREAD, on.head(on.base))
    await _page(monkeypatch, on, store)
    assert await window_module.runs_held(h.THREAD, on.head(on.base))
    whole = on.head(TURNS)
    assert await window_module.runs_held(h.THREAD, whole)
    beyond = Window(TURNS + 1, whole.messages + 1, whole.digest)
    assert not await window_module.runs_held(h.THREAD, beyond)
    other = Window().extend([HumanMessage("Question 1, edited", id="h-1")])
    assert not await window_module.runs_held(h.THREAD, other)


async def test_a_slice_from_another_branch_does_not_count(monkeypatch, threads) -> None:
    _, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    await _page(monkeypatch, on, store)
    row = store.turns["resp-1"]
    row["tail_checkpoint_id"] = "another-branch"
    assert await window_module.runs_held(h.THREAD, on.head(1))
    assert not await window_module.runs_held(h.THREAD, on.head(2))


async def test_a_slice_replay_would_not_serve_does_not_count(
    monkeypatch, threads
) -> None:
    """A trim leaves the slices as the trimmed runs' only copy, so a slice
    counts only while replay would read it: under the running slice key, and
    decodable. The read back takes such a turn from its checkpoints."""
    _, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    await _page(monkeypatch, on, store)
    head = on.head(on.base)
    extracted = await window_module.earlier_runs(h.THREAD, on.tip, head)
    row = store.turns["resp-0"]
    assert await window_module.runs_held(h.THREAD, head)

    key, row["slice_key"] = row["slice_key"], "an-older-key"
    assert not await window_module.runs_held(h.THREAD, head)
    row["slice_key"], row["slice"] = key, b"undecodable"
    assert not await window_module.runs_held(h.THREAD, head)
    read_back = await window_module.earlier_runs(h.THREAD, on.tip, head)
    assert read_back is not None and _text(read_back) == _text(extracted)


# ------------------------------------------------------------ the trimmed runs


async def test_the_trimmed_runs_read_back_as_they_were(monkeypatch, threads) -> None:
    """From stored slices where they are, from the checkpoints where not."""
    off, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    _serve(monkeypatch, on, store)
    extracted = await window_module.earlier_runs(h.THREAD, on.tip, await on.trimmed())
    await _page(monkeypatch, on, store)
    stored = await window_module.earlier_runs(h.THREAD, on.tip, await on.trimmed())
    assert extracted is not None and stored is not None
    assert _text(stored) == _text(extracted)

    whole_off = (await off.run.graph.aget_state(h.config())).values["messages"]
    trimmed_off = [m for run in split_runs(whole_off)[: on.base] for m in run]
    assert _text(stored) == _text(trimmed_off)
    window = (await on.run.graph.aget_state(h.config())).values["messages"]
    assert (
        build_directory([*stored, *window], window=Window()).manifest
        == build_directory(h.whole_threads(on.run)[-1], window=Window()).manifest
    )


def _job(on: Thread, window, previous: str | None = None) -> transcripts._Job:
    return transcripts._Job(
        TranscriptTarget(h.THREAD),
        list(window),
        {},
        "fp",
        on.tip,
        previous,
        on.head(on.base),
    )


async def test_a_render_with_no_copy_reads_the_trimmed_runs(
    monkeypatch, threads
) -> None:
    _, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    await _page(monkeypatch, on, store)
    window = (await on.run.graph.aget_state(h.config())).values["messages"]
    rendered = await transcripts._render_job(_job(on, window), inline=True)
    whole = build_directory(h.whole_threads(on.run)[-1], header={}, window=Window())
    assert rendered.copy.manifest == whole.manifest


async def test_a_copy_from_another_branch_is_rendered_over(monkeypatch, threads) -> None:
    """A truncation keeps a live copy labelled with the fork point, which can
    hold the other branch's turns under the same numbers: its entries for
    the trimmed runs are not carried, and the runs are read back instead."""
    _, on = threads
    _cache_probe(monkeypatch)
    store = SliceStore()
    await _page(monkeypatch, on, store)
    whole = h.whole_threads(on.run)[-1]
    other = [
        m.model_copy(update={"content": "Question 1, asked on another branch"})
        if m.id == "h-1"
        else m
        for m in whole
    ]
    copy = build_directory(other, header={}, window=Window())
    window = (await on.run.graph.aget_state(h.config())).values["messages"]
    rendered = await transcripts._render_job(_job(on, window, copy.manifest), inline=True)
    assert rendered.copy.manifest == build_directory(whole, header={}, window=Window()).manifest


async def test_a_render_that_cannot_read_them_fails(monkeypatch, threads) -> None:
    _, on = threads
    _serve(monkeypatch, on, SliceStore())

    async def unreadable(thread_id, tip, head):
        return None

    monkeypatch.setattr(window_module, "earlier_runs", unreadable)
    window = (await on.run.graph.aget_state(h.config())).values["messages"]
    with pytest.raises(EarlierTurnsMissing):
        await transcripts._render_job(_job(on, window), inline=True)


async def test_the_read_back_must_be_the_trimmed_messages(monkeypatch, threads) -> None:
    _, on = threads
    _serve(monkeypatch, on, SliceStore())
    reader = CheckpointHistoryReader.get_instance()
    real_slices = reader.aget_turn_slices

    async def merged(thread_id, anchors):
        turns = await real_slices(thread_id, anchors)
        for turn in turns[1:]:
            turn.messages[:] = [m for m in turn.messages if m.type != "human"]
        return turns

    monkeypatch.setattr(reader, "aget_turn_slices", merged)
    assert await window_module.earlier_runs(h.THREAD, on.tip, on.head(on.base)) is None


# ------------------------------------------------------------ thread shapes

SHAPED = "5e1f0000-0000-4000-8000-0000000005a9"


async def _shaped(monkeypatch, requests: list[tuple[list, list | None]]):
    """A thread of one turn per request, ``(input, reply)``, a reply of None
    being a turn killed before it ran, with every turn's slice stored as
    replay stores it. Returns the messages at the tip."""
    saver = InMemorySaver()
    replies = [reply for _, reply in requests]

    def answer(state):
        reply = replies.pop(0)
        return {"messages": reply} if reply else {}

    graph = (
        StateGraph(DeltaAgentState)
        .add_node("answer", answer)
        .add_edge(START, "answer")
        .compile(checkpointer=saver)
    )
    with tracing_context(enabled=False):
        for i, (given, _) in enumerate(requests):
            config = {
                "configurable": {"thread_id": SHAPED},
                "metadata": {"turn_index": i, "run_id": f"run-{i}"},
            }
            await graph.ainvoke({"messages": given}, config)
    state = await graph.aget_state({"configurable": {"thread_id": SHAPED}})
    tip = state.config["configurable"]["checkpoint_id"]

    reader = CheckpointHistoryReader(saver)
    monkeypatch.setattr(CheckpointHistoryReader, "get_instance", classmethod(lambda cls: reader))
    anchors, _ = await reader.aget_turn_anchors(SHAPED, tip)
    assert len(anchors) == len(requests)
    rows = [
        slices_db.StoredSlice(
            a.input_checkpoint_id,
            a.tail_checkpoint_id,
            slices.SLICE_KEY,
            *slices.encode(reader.serde, turn),
        )
        for a, turn in zip(anchors, await reader.aget_turn_slices(SHAPED, anchors))
    ]

    async def held(thread_id, key):
        return bool(rows)

    async def at(thread_id, key, inputs):
        return [row for row in rows if row.input_checkpoint_id in inputs]

    async def pointer(thread_id):
        return tip

    monkeypatch.setattr(slices_db, "has_turn_slices", held)
    monkeypatch.setattr(slices_db, "get_slices_at", at)
    monkeypatch.setattr(window_module, "get_thread_checkpoint_id", pointer)
    return tip, list(state.values["messages"])


async def test_turns_that_open_no_run_or_several(monkeypatch) -> None:
    """Counted by turns, each of these misplaces the runs: a request that
    opens with an assistant message (its message belongs to the run
    before), one killed before it ran, and two questions in one request."""
    tip, messages = await _shaped(
        monkeypatch,
        [
            ([AIMessage("Welcome.", id="a-0"), HumanMessage("Q1", id="h-1")], [AIMessage("A1", id="a-1")]),
            ([], None),
            ([HumanMessage("Q2", id="h-2"), HumanMessage("Q3", id="h-3")], [AIMessage("A3", id="a-3")]),
            ([AIMessage("Also:", id="a-3b"), HumanMessage("Q4", id="h-4")], [AIMessage("A4", id="a-4")]),
            ([HumanMessage("Q5", id="h-5")], [AIMessage("A5", id="a-5")]),
        ],
    )
    runs = split_runs(messages)
    assert [[m.id for m in run] for run in runs[:4]] == [
        ["a-0", "h-1", "a-1"],
        ["h-2"],
        ["h-3", "a-3", "a-3b"],
        ["h-4", "a-4"],
    ]
    for n in range(1, 5):
        trimmed = [m for run in runs[:n] for m in run]
        head = Window().extend(trimmed)
        assert head.runs == n
        assert await window_module.runs_held(SHAPED, head), n
        read_back = await window_module.earlier_runs(SHAPED, tip, head)
        assert [m.id for m in read_back or ()] == [m.id for m in trimmed], n


async def test_a_thread_the_old_summarizer_compacted_is_not_trimmed(monkeypatch) -> None:
    """Its summary replaced the messages before it in the checkpoint, while
    the slices still hold them: no trim may leave the summary's turn to
    them."""
    summary = HumanMessage("Summary of the conversation to date: Q1 and Q2.", id="s-1")
    tip, messages = await _shaped(
        monkeypatch,
        [
            ([HumanMessage("Q1", id="h-1")], [AIMessage("A1", id="a-1")]),
            ([HumanMessage("Q2", id="h-2")], [AIMessage("A2", id="a-2")]),
            (
                [HumanMessage("Q3", id="h-3")],
                [
                    RemoveMessage(id=REMOVE_ALL_MESSAGES),
                    summary,
                    AIMessage("A2", id="a-2"),
                    HumanMessage("Q3", id="h-3"),
                    AIMessage("A3", id="a-3"),
                ],
            ),
            ([HumanMessage("Q4", id="h-4")], [AIMessage("A4", id="a-4")]),
        ],
    )
    assert [m.id for m in messages] == ["s-1", "a-2", "h-3", "a-3", "h-4", "a-4"]
    head = Window().extend(messages[:4])
    assert not await window_module.runs_held(SHAPED, head)
    assert await window_module.earlier_runs(SHAPED, tip, head) is None


# ------------------------------------------------------------ manual writes


async def test_a_manual_write_moves_the_commit_pointer(monkeypatch) -> None:
    from src.server.database.conversation import threads_write
    from src.server.handlers.thread_maintenance import _update_graph_state

    graph = (
        StateGraph(MessagesState)
        .add_node("echo", lambda state: {"messages": [AIMessage("ok")]})
        .add_edge(START, "echo")
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "t"}}
    with tracing_context(enabled=False):
        await graph.ainvoke({"messages": [HumanMessage("q")]}, config)
    state = await graph.aget_state(config)
    moved = []

    async def advance(thread_id, *, from_checkpoint_id, to_checkpoint_id):
        moved.append((thread_id, from_checkpoint_id, to_checkpoint_id))
        return False

    monkeypatch.setattr(threads_write, "advance_thread_checkpoint_id", advance)
    await _update_graph_state(graph, state, {"messages": [AIMessage("x")]}, "t", "compact")
    written = await graph.aget_state(config)
    built_on = state.config["configurable"]["checkpoint_id"]
    assert written.parent_config["configurable"]["checkpoint_id"] == built_on
    assert moved == [("t", built_on, written.config["configurable"]["checkpoint_id"])]
