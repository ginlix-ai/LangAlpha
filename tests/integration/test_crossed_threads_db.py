"""A thread that crossed into DeltaChannel the way production's did is trimmed
by the window itself.

A crossed thread starts with turns under a plain ``add_messages`` state, each
fanning out to parallel tool nodes, which that reducer applied in node order.
Its first turns under DeltaChannel run on the saver production crossed on,
which loaded by replaying the stored writes from the root and ordered a
checkpoint's writes by task id, so the tool results of each legacy turn came
back in another order, which the rollout then stored as a snapshot. Turn
slices are cut from the legacy lists and keep the original order. Six
parallel tools make a turn whose task ids happen to sort in node order one in
720, and there are two.

The window's digest takes each batch of tool answers in call-id order, so the
slices hold the checkpoint's head in either order and the window trims the
thread at its next turn. Twin threads are built from the same script and run
the same turns, a summary among them, with the window on for one and off for
the other: what the model and the summary model are sent must not tell them
apart.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from typing import Annotated, Any, TypedDict

import psycopg
import pytest
import pytest_asyncio
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from psycopg.rows import dict_row, tuple_row

from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import notes_window_carry
from ptc_agent.agent.middleware.compaction.types import OffloadSettings
from ptc_agent.agent.middleware.compaction.window import window_cut
from ptc_agent.agent.middleware.subagent_switch import subagents_window_carry
from ptc_agent.agent.transcript import Window
from ptc_agent.core.sandbox.livefs_mount import TranscriptReach
from scripts.ops import thin_message_snapshots as thinning
from scripts.ops._delta_loads import Mode
from tests.unit.middleware.compaction import window_harness as h

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

TOOLS = "abcdef"
LEGACY_TURNS = 2
DELTA_TURNS = 5
#: Messages in view, the system prompt counted, at which a summary runs.
THRESHOLD = 28
KEEP = 4
OFFLOAD = OffloadSettings(keep_messages=KEEP, max_length=200, idle_seconds=0.0)


class _Legacy(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


def _number(state) -> int:
    return sum(isinstance(m, HumanMessage) for m in state["messages"])


def _legacy_graph(saver):
    """A turn that calls every tool at once and answers once they are back."""

    def ask(state):
        n = _number(state)
        calls = [{"name": f"tool_{x}", "id": f"call-{n}-{x}", "args": {"n": n}} for x in TOOLS]
        return {"messages": [AIMessage("", id=f"ai-{n}-ask", tool_calls=calls)]}

    def tool(x: str):
        def run(state):
            n = _number(state)
            return {
                "messages": [
                    ToolMessage(f"result {n}{x}", tool_call_id=f"call-{n}-{x}", id=f"tool-{n}-{x}")
                ]
            }

        return run

    def answer(state):
        n = _number(state)
        return {"messages": [AIMessage(f"Answer {n}.", id=f"ai-{n}-end")]}

    graph = StateGraph(_Legacy).add_node("ask", ask).add_node("answer", answer)
    graph.add_edge(START, "ask")
    for x in TOOLS:
        graph.add_node(f"tool_{x}", tool(x)).add_edge("ask", f"tool_{x}")
    graph.add_edge([f"tool_{x}" for x in TOOLS], "answer").add_edge("answer", END)
    return graph.compile(checkpointer=saver)


def _agent(saver, thread_id: str, model, summary, *, threshold: int, asked: list | None = None):
    """Compaction as the main agent's stack holds it, with the window on when
    ``asked`` collects the answers of the server's own coverage check."""
    compaction = CompactionMiddleware(
        Summarizer(summary, limit=10**9, counter=len),
        token_threshold=threshold,
        keep_tokens=KEEP,
        offload=OFFLOAD,
        backend=_backend(),
        workspace_id="ws-1",
    )
    if asked is not None:
        from src.server.services.history.window import runs_held

        async def coverage(window: Window, trimmed: Sequence[AnyMessage]) -> Window | None:
            head = await runs_held(thread_id, window, trimmed)
            asked.append((window.extend(trimmed).runs, head is not None))
            return head

        compaction = compaction.with_window(
            coverage, [notes_window_carry(thread_id), subagents_window_carry]
        )
    return create_agent(
        model,
        system_prompt="You are the test analyst.",
        tools=[],
        middleware=[compaction],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


class _Mount:
    """The computer's file mount as far as compaction uses it: a transcript
    saved the way the server saves one mid-turn."""

    async def save_transcript(self, target, messages, *, window: Window) -> bool:
        from src.server.services import transcripts

        return await transcripts.save_live(target, messages, window=window)


def _backend() -> Any:
    mount = _Mount()

    async def settled_livefs(workspace_id=None):
        return mount

    return SimpleNamespace(
        livefs=mount,
        transcripts=TranscriptReach(home=mount, shell=True),
        settled_livefs=settled_livefs,
        normalize_path=lambda path: path,
    )


def _sent(requests: list, thread_id: str) -> list[str]:
    """Requests as text, the thread's own ids and run-time ids normalized."""
    normalize = h.Normalizer()
    return [
        normalize(h.dump(r).replace(thread_id, "<thread>").replace(thread_id[:8], "<thread>"))
        for r in requests
    ]


async def _turn(saver, thread_id: str, graph, text: str) -> str:
    """One turn the way the server runs it: its rows started, the graph run
    with the turn stamped on its config, the run finalized at its tip. The
    input carries no id, as production's legacy inputs did not."""
    from src.server.database.runs.lifecycle import QuerySpec, RunOutcome, finalize_run, start_run
    from src.server.utils.checkpoint_helpers import newest_checkpoint

    run_id = str(uuid.uuid4())
    row = await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        query=QuerySpec(query_id=str(uuid.uuid4()), content=text, query_type="initial"),
        metadata={"msg_type": "ptc"},
    )
    config = {
        "configurable": {"thread_id": thread_id},
        "metadata": {"run_id": run_id, "turn_index": row["turn_index"]},
    }
    await graph.ainvoke({"messages": [HumanMessage(text)]}, config)
    newest = await newest_checkpoint(saver, thread_id)
    await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        outcome=RunOutcome(status="completed"),
        newest=newest,
    )
    return newest.checkpoint_id


async def _newest(saver, thread_id: str) -> str:
    newest = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    return newest.config["configurable"]["checkpoint_id"]


async def _project(thread_id: str) -> None:
    """Every turn's slice, as ``backfill_turn_slices`` stores them."""
    from src.server.services.history.replay import load_thread_inputs, project_turns

    rows, tip = await load_thread_inputs(thread_id)
    await project_turns(rows, tip, last_n_turns=10**6)


@contextmanager
def _saver_production_crossed_on() -> Iterator[None]:
    """Delta loads as langgraph-checkpoint-postgres before 3.2.0 made them: a
    walk stopped only where ``channel_values`` held the channel, so it passed
    the legacy lists to the root, and a checkpoint's writes replayed by task id."""
    from langgraph.checkpoint.postgres import base

    saver = base.BasePostgresSaver
    advance, sort_key = saver.__dict__["_try_advance_walks"], base.writes_sort_key

    def flagged_only(target, channels, parent_of, versions, _blobs, inline, *rest):
        flags = [{cid: value is not None for cid, value in inline[i].items()} for i in range(len(channels))]
        return advance.__func__(target, channels, parent_of, versions, flags, inline, *rest)

    saver._try_advance_walks = staticmethod(flagged_only)
    base.writes_sort_key = lambda task_path, task_id="", idx=0: (task_id, idx)
    try:
        yield
    finally:
        saver._try_advance_walks, base.writes_sort_key = advance, sort_key


async def _seed_the_crossing(test_db_uri: str, thread_id: str) -> None:
    """What ``--keep all`` stored before langgraph-checkpoint-postgres 3.2.0:
    the crossed load of the first checkpoint past the stored lists, as its
    snapshot, so the crossing outlives the saver that made it."""
    from langgraph.checkpoint.serde.types import _DeltaSnapshot

    from scripts.ops._delta_loads import Loader, read_tree

    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        tree, stored = await read_tree(conn, thread_id)
        first = next(
            cid for cid in tree.order if tree.nodes[cid].version not in (None, *stored)
        )
        loader = Loader(conn)
        value, _ = await loader.channel_value(thread_id, first)
        typed = loader.serde.dumps_typed(_DeltaSnapshot(value))
        await conn.execute(
            "INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)"
            " VALUES (%s, '', 'messages', %s, %s, %s)",
            (thread_id, tree.nodes[first].version, *typed),
        )
        await conn.execute(
            "UPDATE checkpoints"
            " SET checkpoint = jsonb_set(checkpoint, '{channel_values,messages}', 'true')"
            " WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s",
            (thread_id, first),
        )


async def _crossed(saver, test_db_uri: str, workspace_id: str) -> str:
    """Legacy turns, then turns under DeltaChannel compacting as they go on the
    saver production crossed on; then the rollout: the crossing seeded, the
    stored lists flagged, and every turn's slice stored."""
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=workspace_id,
        current_status="completed",
        msg_type="ptc",
    )
    legacy = _legacy_graph(saver)
    for n in range(1, LEGACY_TURNS + 1):
        await _turn(saver, thread_id, legacy, f"Question {n}")
    model, summary = h.RecordingModel(), h.SummaryModel()
    with _saver_production_crossed_on():
        for n in range(LEGACY_TURNS + 1, LEGACY_TURNS + DELTA_TURNS + 1):
            model.script = [AIMessage(f"Answer {n}.", id=f"ai-{n}-end")]
            graph = _agent(saver, thread_id, model, summary, threshold=THRESHOLD)
            tip = await _turn(saver, thread_id, graph, f"Question {n}")
        await _seed_the_crossing(test_db_uri, thread_id)
    assert summary.requests, "the delta turns never compacted"
    thinned = await thinning.thin_thread(
        test_db_uri, thread_id, tip, Mode.APPLY, thinning.Options(keep_all=True, verify_all=True)
    )
    assert not thinned.failed
    await _project(thread_id)
    return thread_id


async def _head(thread_id: str) -> tuple[Window, list[AnyMessage], list[AnyMessage]]:
    """What the window's next trim would drop at the tip, the tip's messages
    it covers, and the same messages as the stored slices hold them."""
    from src.server.services.history import window as window_reads
    from src.server.services.history.reader import CheckpointHistoryReader

    reader = CheckpointHistoryReader.get_instance()
    values = (await reader.aget_state(thread_id)).values
    messages = values["messages"]
    cut = window_cut(messages, values.get("_summarization_event"))
    head = Window.of(values).extend(messages[:cut])
    anchors, _ = await reader.aget_turn_anchors(thread_id)
    stored = await window_reads._head(thread_id, anchors, head.messages, extract=False)
    return head, messages[:cut], stored


def _keys(messages) -> list[str]:
    """Each message as the window's digest reads it: what it says and the
    calls it makes or answers, never its id."""
    return [
        json.dumps(
            [
                m.type,
                m.content,
                getattr(m, "tool_call_id", None),
                [call["id"] for call in getattr(m, "tool_calls", None) or ()],
            ],
            sort_keys=True,
            default=str,
        )
        for m in messages
    ]


@pytest_asyncio.fixture(loop_scope="session")
async def saver(test_db_pool, patched_get_db_connection, monkeypatch):
    """The test pool as the server's checkpointer. The transcript store reads
    rows as tuples, as the app pool returns them, not as the test pool's dicts."""
    from src.server.app import setup
    from src.server.services.history.reader import CheckpointHistoryReader

    @asynccontextmanager
    async def tuple_rows():
        async with test_db_pool.connection() as conn:
            conn.row_factory = tuple_row
            try:
                yield conn
            finally:
                conn.row_factory = dict_row

    monkeypatch.setattr("src.server.database.thread_transcripts.get_db_connection", tuple_rows)
    saver = AsyncPostgresSaver(test_db_pool)
    monkeypatch.setattr(setup, "checkpointer", saver)
    CheckpointHistoryReader.reset_instance()
    yield saver
    CheckpointHistoryReader.reset_instance()



@pytest_asyncio.fixture(loop_scope="session")
async def twins(saver, test_db_uri, seed_workspace):
    """Two crossed threads from one script."""
    workspace_id = str(seed_workspace["workspace_id"])
    first = await _crossed(saver, test_db_uri, workspace_id)
    second = await _crossed(saver, test_db_uri, workspace_id)
    yield first, second
    for thread_id in (first, second):
        await saver.adelete_thread(thread_id)


async def test_a_crossed_thread_is_held_as_it_loads(saver, twins):
    from src.server.services.history.window import earlier_runs, runs_held

    thread_id, _ = twins
    head, tip_head, stored = await _head(thread_id)

    # The crossing as production's: the slices hold the tip's head messages in
    # another order within the legacy turns, and in the same order past them.
    legacy = LEGACY_TURNS * (len(TOOLS) + 3)
    assert head.runs > LEGACY_TURNS and head.messages > legacy
    assert sorted(_keys(tip_head)) == sorted(_keys(stored))
    assert _keys(tip_head[:legacy]) != _keys(stored[:legacy])
    assert _keys(tip_head[legacy:]) == _keys(stored[legacy:])
    assert head.messages == len(tip_head)

    # Either order hashes the same, so the window holds the head as the tip
    # loads it, and reads its messages back as the slices hold them.
    held = await runs_held(thread_id, Window(), tip_head)
    assert held == Window().extend(tip_head) == Window().extend(stored)
    read = await earlier_runs(thread_id, await _newest(saver, thread_id), held)
    assert read is not None and _keys(read) == _keys(stored)


async def test_a_crossed_thread_trims_and_the_models_are_sent_the_same(saver, twins):
    """The same turns on both twins, a summary among them, with the window on
    for one: it trims where the other keeps every message, and the model and
    the summary model are sent the same on both."""
    from src.server.services.history.reader import CheckpointHistoryReader

    on, off = twins
    head, _, _ = await _head(on)
    reader = CheckpointHistoryReader.get_instance()

    sent: dict[str, tuple[list, list]] = {}
    windows: dict[str, list[int]] = {}
    asked: dict[str, list | None] = {on: [], off: None}
    start = LEGACY_TURNS + DELTA_TURNS + 1
    for thread_id in (on, off):
        model, summary = h.RecordingModel(), h.SummaryModel()
        windows[thread_id] = []
        # A plain turn, one that summarizes, and one that trims to that summary.
        for offset, threshold in enumerate((10**6, 0, 10**6)):
            n = start + offset
            model.script = [AIMessage(f"Answer {n}.", id=f"ai-{n}-end")]
            graph = _agent(
                saver, thread_id, model, summary, threshold=threshold, asked=asked[thread_id]
            )
            await _turn(saver, thread_id, graph, f"Question {n}")
            await _project(thread_id)
            values = (await reader.aget_state(thread_id)).values
            windows[thread_id].append(Window.of(values).runs)
        sent[thread_id] = (_sent(model.requests, thread_id), _sent(summary.requests, thread_id))

    assert sent[on] == sent[off]
    assert len(sent[on][0]) == 3 and len(sent[on][1]) == 1
    # The summary cites the transcript, which the trimmed twin renders with
    # its trimmed turns read back from their slices.
    assert "<transcript_citations>" in sent[on][1][0]
    # The delta turns' summary kept the view from Answer 5, so the first turn
    # trims turns 1-4. Its anchor's run then opens the list, which leaves
    # nothing to cut until the second turn's summary. Its zero threshold
    # leaves no room for a tail (a third of the threshold at most), so it
    # keeps only Question 9, and the third trims turns 5 to 8.
    assert head.runs == 4
    assert asked[on] == [(4, True), (8, True)]
    assert windows == {on: [4, 4, 8], off: [0, 0, 0]}
