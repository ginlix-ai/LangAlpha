"""scripts/ops/repair_crossed_threads.py on threads that crossed into
DeltaChannel the way production's did.

A crossed thread starts with turns under a plain ``add_messages`` state, each
fanning out to parallel tool nodes, which that reducer applied in node order.
Its first turn under DeltaChannel loaded by replaying the stored writes from
the root, which orders a checkpoint's writes by task id, so the tool results
of each legacy turn came back in another order and stayed in it. Turn slices
are cut from the legacy lists and keep the original order, so the window's
coverage check never holds the head. Six parallel tools make a turn whose
task ids happen to sort in node order one in 720, and there are two.

Twin threads are built from the same script, one repaired and one left
crossed, and run the same turns after it through compaction with the window
on: what the model and the summary model are sent must not tell them apart.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
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

import src.server.utils.checkpointer  # noqa: F401  (installs the walk guard)
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import notes_window_carry
from ptc_agent.agent.middleware.compaction.types import OffloadSettings
from ptc_agent.agent.middleware.compaction.window import window_cut
from ptc_agent.agent.middleware.subagent_switch import subagents_window_carry
from ptc_agent.agent.transcript import Window
from scripts.ops import repair_crossed_threads as repair
from scripts.ops import thin_message_snapshots as thinning
from scripts.ops._delta_loads import Mode, Status
from tests.unit.middleware.compaction import window_harness as h

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

TOOLS = "abcdef"
LEGACY_TURNS = 2
DELTA_TURNS = 5
#: Messages in view, the system prompt counted, at which a summary runs.
THRESHOLD = 28
KEEP = 4
OFFLOAD = OffloadSettings(keep_messages=KEEP, max_length=200, idle_seconds=0.0)
SETTINGS = repair.Settings(keep_messages=KEEP, offload=OFFLOAD)
VERIFY_ALL = repair.Options(verify_all=True)


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
        keep_messages=KEEP,
        offload=OFFLOAD,
        backend=_backend(),
        workspace_id="ws-1",
    )
    if asked is not None:
        from src.server.services.history.window import runs_held

        async def coverage(head: Window) -> bool:
            held = await runs_held(thread_id, head)
            asked.append((head.runs, held))
            return held

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
        livefs=mount, settled_livefs=settled_livefs, normalize_path=lambda path: path
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
    tip = await _newest(saver, thread_id)
    await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        outcome=RunOutcome(status="completed"),
        checkpoint_id=tip,
    )
    return tip


async def _newest(saver, thread_id: str) -> str:
    newest = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    return newest.config["configurable"]["checkpoint_id"]


async def _project(thread_id: str) -> None:
    """Every turn's slice, as ``backfill_turn_slices`` stores them."""
    from src.server.services.history.replay import load_thread_inputs, project_turns

    rows, tip = await load_thread_inputs(thread_id)
    await project_turns(rows, tip, last_n_turns=10**6)


async def _crossed(saver, test_db_uri: str, workspace_id: str) -> str:
    """Legacy turns, then turns under DeltaChannel compacting as they go, the
    first of which crossed; then the rollout before the repair: the stored
    lists kept as seeds, and every turn's slice stored."""
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
    for n in range(LEGACY_TURNS + 1, LEGACY_TURNS + DELTA_TURNS + 1):
        model.script = [AIMessage(f"Answer {n}.", id=f"ai-{n}-end")]
        graph = _agent(saver, thread_id, model, summary, threshold=THRESHOLD)
        tip = await _turn(saver, thread_id, graph, f"Question {n}")
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
    return [repair._key(m) for m in messages]


async def _repair(test_db_uri: str, thread_id: str, mode: Mode = Mode.APPLY):
    return await repair.repair_thread(test_db_uri, thread_id, mode, VERIFY_ALL, SETTINGS)


async def _rows(test_db_uri: str, thread_id: str) -> list[tuple]:
    """Every row of the thread, with its row version."""
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        out = []
        for table, key in (
            ("checkpoints", "thread_id"),
            ("checkpoint_blobs", "thread_id"),
            ("checkpoint_writes", "thread_id"),
            ("conversation_threads", "conversation_thread_id::text"),
            ("turn_slices", "conversation_thread_id::text"),
        ):
            cur = await conn.execute(
                f"SELECT md5(t::text), xmin::text FROM {table} t WHERE {key} = %s ORDER BY 1",
                (thread_id,),
            )
            out.append((table, await cur.fetchall()))
        return out


async def _pointer(test_db_uri: str, thread_id: str) -> str:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT latest_checkpoint_id FROM conversation_threads"
            " WHERE conversation_thread_id = %s",
            (thread_id,),
        )
        return (await cur.fetchone())[0]


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
    """Two threads from one script: ``repaired`` is repaired, ``crossed`` is not."""
    workspace_id = str(seed_workspace["workspace_id"])
    repaired = await _crossed(saver, test_db_uri, workspace_id)
    crossed = await _crossed(saver, test_db_uri, workspace_id)
    yield repaired, crossed
    for thread_id in (repaired, crossed):
        await saver.adelete_thread(thread_id)


async def test_a_crossed_thread_is_refused_until_repaired(saver, test_db_uri, twins):
    thread_id, _ = twins
    head, tip_head, stored = await _head(thread_id)
    from src.server.services.history.window import runs_held

    # The crossing as production's: the slices hold the tip's head messages in
    # another order, so the window's coverage check refuses every trim.
    legacy = LEGACY_TURNS * (len(TOOLS) + 3)
    assert head.runs > LEGACY_TURNS and head.messages > legacy
    assert sorted(_keys(tip_head)) == sorted(_keys(stored))
    assert _keys(tip_head[:legacy]) != _keys(stored[:legacy])
    assert _keys(tip_head[legacy:]) == _keys(stored[legacy:])
    assert not await runs_held(thread_id, head)

    before = await _rows(test_db_uri, thread_id)
    planned = await _repair(test_db_uri, thread_id, Mode.DRY_RUN)
    assert planned.status is Status.PLANNED, planned.reason
    report = planned.report
    assert (report.crossed, report.refused, report.cut) == ("yes", "", head.messages)
    assert report.requests_match and report.requests == 4
    assert report.span[1] < report.replayed
    rehearsed = await _repair(test_db_uri, thread_id, Mode.REHEARSE)
    assert rehearsed.status is Status.REHEARSED and not rehearsed.failed
    assert await _rows(test_db_uri, thread_id) == before

    tip = await _newest(saver, thread_id)
    applied = await _repair(test_db_uri, thread_id)
    assert applied.status is Status.WRITTEN and not applied.failed, applied.reason
    assert applied.committed_match and all(check.match for check in applied.checks)
    assert applied.report.requests_match
    assert applied.report.reprojected_same
    # The trim's own writes are not left on the old tip, where a load of any
    # other child of it would replay them.
    assert dict(await _rows(test_db_uri, thread_id))["checkpoint_writes"] == dict(before)[
        "checkpoint_writes"
    ]

    # The window a trim of an uncrossed thread would leave: the slices' head.
    from src.server.services.history.reader import CheckpointHistoryReader

    reader = CheckpointHistoryReader.get_instance()
    values = (await reader.aget_state(thread_id)).values
    window = Window.of(values)
    canonical = Window().extend(stored)
    assert (window.runs, window.messages, window.digest, dict(window.earlier)) == (
        canonical.runs,
        canonical.messages,
        canonical.digest,
        dict(canonical.earlier),
    )
    assert await runs_held(thread_id, window)
    kept = (await reader.aget_state(thread_id, tip)).values["messages"][head.messages :]
    assert [m.id for m in values["messages"]] == [m.id for m in kept]
    assert _keys(values["messages"]) == _keys(kept)

    # A rerun finds nothing to do and writes nothing.
    after = await _rows(test_db_uri, thread_id)
    for mode in (Mode.DRY_RUN, Mode.APPLY):
        again = await _repair(test_db_uri, thread_id, mode)
        assert again.status is Status.NOTHING and again.report.crossed == "no"
    assert await _rows(test_db_uri, thread_id) == after


async def test_a_last_turn_that_fails_to_store_is_reported(
    saver, test_db_uri, seed_workspace, monkeypatch
):
    """The repair commits, but the turn it moved is not stored again, so the
    old row, with the messages it held, is no proof the turn projects."""

    async def refused(rows):
        raise RuntimeError("store refused")

    thread_id = await _crossed(saver, test_db_uri, str(seed_workspace["workspace_id"]))
    monkeypatch.setattr(
        "src.server.database.conversation.turn_slices.upsert_turn_slices", refused
    )
    applied = await _repair(test_db_uri, thread_id)
    assert applied.status is Status.WRITTEN and applied.committed_match, applied.reason
    assert applied.report.reprojected
    assert applied.report.reprojected_same is False
    await saver.adelete_thread(thread_id)


async def test_the_repair_changes_nothing_the_models_are_sent(saver, test_db_uri, twins):
    """The same turns after the repair on both twins, a summary among them:
    the repaired one trims where the crossed one is refused, and the model
    and the summary model are sent the same on both."""
    from src.server.services.history.reader import CheckpointHistoryReader

    repaired, crossed = twins
    result = await _repair(test_db_uri, repaired)
    assert result.status is Status.WRITTEN and not result.failed, result.reason
    reader = CheckpointHistoryReader.get_instance()
    runs = Window.of((await reader.aget_state(repaired)).values).runs

    sent: dict[str, tuple[list, list, list]] = {}
    windows: dict[str, list[int]] = {}
    start = LEGACY_TURNS + DELTA_TURNS + 1
    for thread_id in (repaired, crossed):
        model, summary, asked = h.RecordingModel(), h.SummaryModel(), []
        windows[thread_id] = []
        # A plain turn, one that summarizes, and one that trims to that summary.
        for offset, threshold in enumerate((10**6, 0, 10**6)):
            n = start + offset
            model.script = [AIMessage(f"Answer {n}.", id=f"ai-{n}-end")]
            graph = _agent(saver, thread_id, model, summary, threshold=threshold, asked=asked)
            await _turn(saver, thread_id, graph, f"Question {n}")
            await _project(thread_id)
            values = (await reader.aget_state(thread_id)).values
            windows[thread_id].append(Window.of(values).runs)
        sent[thread_id] = (
            _sent(model.requests, thread_id), _sent(summary.requests, thread_id), asked
        )

    assert sent[repaired][:2] == sent[crossed][:2]
    assert len(sent[repaired][0]) == 3 and len(sent[repaired][1]) == 1
    # The summary cites the transcript, which the repaired thread renders
    # with its trimmed turns read back from their slices.
    assert "<transcript_citations>" in sent[repaired][1][0]
    # Only the trim after the summary asks of the repaired thread, and is held;
    # every trim the crossed one would make is refused.
    assert [held for _, held in sent[repaired][2]] == [True]
    assert sent[crossed][2] and not any(held for _, held in sent[crossed][2])
    assert windows[repaired][:2] == [runs, runs] and windows[repaired][2] > runs
    assert windows[crossed] == [0, 0, 0]


async def test_a_thread_is_refused_while_a_run_is_live_or_a_slice_is_missing(
    saver, test_db_uri, seed_workspace
):
    from src.server.database.runs.lifecycle import QuerySpec, RunOutcome, finalize_run, start_run

    thread_id = await _crossed(saver, test_db_uri, str(seed_workspace["workspace_id"]))
    before = await _rows(test_db_uri, thread_id)
    pointer = await _pointer(test_db_uri, thread_id)

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        query=QuerySpec(query_id=str(uuid.uuid4()), content="live", query_type="initial"),
    )
    live = await _repair(test_db_uri, thread_id)
    assert live.report.refused == "a run is in progress"
    await finalize_run(run_id=run_id, thread_id=thread_id, outcome=RunOutcome(status="cancelled"))

    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        await conn.execute(
            "DELETE FROM turn_slices s USING conversation_responses r"
            " WHERE s.conversation_response_id = r.conversation_response_id"
            " AND s.conversation_thread_id = %s AND r.turn_index = 0",
            (thread_id,),
        )
    missing = await _repair(test_db_uri, thread_id)
    assert missing.report.refused.startswith("a turn before the cut has no stored slice")

    # Neither wrote a checkpoint or moved the pointer; the run and the slice
    # are the test's own changes to the other rows.
    after = dict(await _rows(test_db_uri, thread_id))
    for table, rows in before:
        if table.startswith("checkpoint"):
            assert after[table] == rows, table
    assert await _pointer(test_db_uri, thread_id) == pointer
    await saver.adelete_thread(thread_id)


async def test_a_kept_message_stored_without_an_id_is_written_with_the_planned_one(
    saver, test_db_uri, seed_workspace
):
    """A message injected through ``aupdate_state`` is stored without an id,
    and the trim stamps one on every kept message: the repair writes the id
    its plan checked, not a second fresh one."""
    from src.server.services.history.reader import CheckpointHistoryReader
    from src.server.utils.checkpoint_helpers import update_at_commit

    thread_id = await _crossed(saver, test_db_uri, str(seed_workspace["workspace_id"]))
    graph = _agent(saver, thread_id, h.RecordingModel(), h.SummaryModel(), threshold=10**6)
    tip = await _newest(saver, thread_id)
    assert await update_at_commit(graph, thread_id, tip, {"messages": [HumanMessage("A note.")]})
    reader = CheckpointHistoryReader.get_instance()
    assert (await reader.aget_state(thread_id)).values["messages"][-1].id is None

    applied = await _repair(test_db_uri, thread_id)
    assert applied.status is Status.WRITTEN and not applied.failed, applied.reason
    note = (await reader.aget_state(thread_id)).values["messages"][-1]
    assert note.content == "A note." and note.id is not None
    await saver.adelete_thread(thread_id)
