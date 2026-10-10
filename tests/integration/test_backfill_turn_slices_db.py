"""scripts/utils/backfill_turn_slices.py against real Postgres: the first pass,
and the re-key after a deploy changes the projection or the slice cut.

A deploy leaves every stored row in place under a key no read accepts, so the
selection has to find rows that are present but stale, not only absent ones.
One thread has a turn whose run died before its first checkpoint: it never
gets a row, so that thread stays a candidate each pass confirms current
without projecting anything.
"""

from __future__ import annotations

import argparse
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph

import src.server.utils.checkpointer  # noqa: F401  (installs the walk guard)
from ptc_agent.agent.state import DeltaAgentState
from scripts.utils import backfill_turn_slices as backfill
from src.server.services.history import slices
from src.server.services.history.replay import keys

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

DEAD_TURN = 1


def _graph(saver):
    def reply(state):
        n = len(state["messages"])
        return {"messages": [AIMessage(content=f"reply {n}", id=f"ai-{uuid.uuid4()}")]}

    return (
        StateGraph(DeltaAgentState)
        .add_node("reply", reply)
        .add_edge(START, "reply")
        .compile(checkpointer=saver)
    )


async def _thread(saver, workspace_id: str, *, dead: int | None) -> str:
    """Three turns run and finalized the way a turn is; ``dead``'s run fails
    before the graph writes anything."""
    from src.server.database.conversation import create_thread
    from src.server.database.runs.lifecycle import (
        QuerySpec,
        RunOutcome,
        finalize_run,
        start_run,
    )

    graph = _graph(saver)
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=workspace_id,
        current_status="completed",
        msg_type="ptc",
    )
    for text in ("q0", "q1", "q2"):
        run_id = str(uuid.uuid4())
        row = await start_run(
            run_id=run_id,
            thread_id=thread_id,
            request_key=str(uuid.uuid4()),
            query=QuerySpec(query_id=str(uuid.uuid4()), content=text, query_type="initial"),
            metadata={"msg_type": "ptc"},
        )
        turn_index = row["turn_index"]
        if turn_index == dead:
            await finalize_run(
                run_id=run_id, thread_id=thread_id, outcome=RunOutcome(status="error")
            )
            continue
        config = {
            "configurable": {"thread_id": thread_id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        }
        await graph.ainvoke({"messages": [HumanMessage(content=text)]}, config)
        state = await graph.aget_state({"configurable": {"thread_id": thread_id}})
        await finalize_run(
            run_id=run_id,
            thread_id=thread_id,
            outcome=RunOutcome(status="completed"),
            checkpoint_id=state.config["configurable"]["checkpoint_id"],
        )
    return thread_id


@pytest_asyncio.fixture(loop_scope="session")
async def threads(seed_workspace, test_db_pool, patched_get_db_connection, monkeypatch):
    from src.server.app import setup
    from src.server.services.history.reader import CheckpointHistoryReader

    saver = AsyncPostgresSaver(test_db_pool)
    monkeypatch.setattr(setup, "checkpointer", saver)
    CheckpointHistoryReader.reset_instance()
    workspace_id = str(seed_workspace["workspace_id"])
    out = SimpleNamespace(
        whole=await _thread(saver, workspace_id, dead=None),
        dead=await _thread(saver, workspace_id, dead=DEAD_TURN),
    )
    yield out
    CheckpointHistoryReader.reset_instance()
    for thread_id in (out.whole, out.dead):
        await saver.adelete_thread(thread_id)


def _args(*threads: str, apply: bool = True, recheck: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        apply=apply,
        days=None,
        user=None,
        workspace=None,
        thread=list(threads),
        recheck=recheck,
        concurrency=2,
        pause=0,
    )


async def _candidates(*threads: str) -> dict[str, tuple[int, int]]:
    return {c.thread_id: (c.missing, c.stale) for c in await backfill._threads(_args(*threads))}


async def _rows(*threads: str) -> list[tuple]:
    from psycopg.rows import tuple_row

    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT s.conversation_thread_id::text, r.turn_index, s.slice_key,"
            " s.lines_key, s.lines IS NOT NULL, s.built_at FROM turn_slices s"
            " JOIN conversation_responses r USING (conversation_response_id)"
            " WHERE s.conversation_thread_id = ANY(%s::uuid[]) ORDER BY 1, 2",
            (list(threads),),
        )
        return await cur.fetchall()


def _all_current(rows: list[tuple]) -> bool:
    epoch = f"{keys.lines_epoch()}."
    return all(
        slice_key == slices.SLICE_KEY and lines_key.startswith(epoch) and has_lines
        for _, _, slice_key, lines_key, has_lines, _ in rows
    )


async def test_a_first_pass_stores_every_turn_and_a_rerun_projects_nothing(threads, capsys):
    both = (threads.whole, threads.dead)
    assert await _candidates(*both) == {threads.whole: (3, 0), threads.dead: (3, 0)}

    assert await backfill._run(_args(*both, apply=False)) == 0
    assert await _rows(*both) == []

    assert await backfill._run(_args(*both)) == 0
    rows = await _rows(*both)
    assert [(t, i) for t, i, *_ in rows] == sorted(
        [(threads.whole, i) for i in (0, 1, 2)] + [(threads.dead, i) for i in (0, 2)]
    )
    assert _all_current(rows)
    assert "2 projected (5 turns)" in capsys.readouterr().out

    # The dead turn never gets a row, so its thread stays a candidate; the
    # read path's own check finds nothing to project there.
    assert await _candidates(*both) == {threads.dead: (1, 0)}
    assert await backfill._run(_args(*both)) == 0
    assert "0 projected (0 turns), 1 already current" in capsys.readouterr().out
    assert await _rows(*both) == rows


@pytest.mark.parametrize("change", ["projection", "slice cut"])
async def test_a_deploy_that_changes_the_projection_rekeys_every_stored_turn(
    threads, monkeypatch, capsys, change
):
    both = (threads.whole, threads.dead)
    await backfill._run(_args(*both))
    before = await _rows(*both)

    if change == "slice cut":
        monkeypatch.setattr(slices, "SLICE_KEY", f"{slices.SLICE_KEY}-next")
    monkeypatch.setattr(
        keys, "_PROJECTION_VERSION", f"{keys.LINES_VERSION}.{slices.SLICE_KEY}.next"
    )
    assert not _all_current(before)
    assert await _candidates(*both) == {threads.whole: (0, 3), threads.dead: (1, 2)}

    capsys.readouterr()
    assert await backfill._run(_args(*both)) == 0
    after = await _rows(*both)
    assert [(t, i) for t, i, *_ in after] == [(t, i) for t, i, *_ in before]
    assert _all_current(after)
    assert "2 projected (5 turns)" in capsys.readouterr().out

    assert await _candidates(*both) == {threads.dead: (1, 0)}
    assert await backfill._run(_args(*both)) == 0
    assert await _rows(*both) == after


async def test_a_bulk_row_update_is_found_by_a_recheck(threads, capsys):
    """An UPDATE leaves every key's epoch current but moves the row versions
    the keys cover, so only the read path's own check sees it."""
    from src.server.database.pool import get_db_connection

    both = (threads.whole, threads.dead)
    await backfill._run(_args(*both))
    before = {(t, i): key for t, i, _, key, *_ in await _rows(*both)}
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE conversation_responses SET sse_events = NULL"
            " WHERE conversation_thread_id = %s::uuid",
            (threads.whole,),
        )
    assert await _candidates(*both) == {threads.dead: (1, 0)}

    capsys.readouterr()
    assert await backfill._run(_args(*both, recheck=True)) == 0
    assert "1 projected (3 turns), 1 already current" in capsys.readouterr().out
    after = {(t, i): key for t, i, _, key, *_ in await _rows(*both)}
    assert [k for k in before if before[k] != after[k]] == [(threads.whole, i) for i in (0, 1, 2)]
    assert _all_current(await _rows(*both))

    assert await backfill._run(_args(*both, recheck=True)) == 0
    assert "0 projected (0 turns), 2 already current" in capsys.readouterr().out
