"""Edit and regenerate against real Postgres, on a thread with a turn whose
run died before its first checkpoint.

That turn holds a turn number and rows but no boundary on the branch, so
every turn after it sits one place lower among the boundaries than its
number. A fork deletes rows from the turn it replaces onward, and the
turn it replaces is fixed by the checkpoint it forks at: deleting by
position would drop the rows (and the slice) of a turn the branch keeps.

That turn is read off the branch before the fork's START, so the last tests
interleave two requests the way two workers can: a fork whose branch moved
between its validation and its START must be refused with nothing deleted.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState

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


async def _start(
    thread_id,
    text,
    *,
    fork=None,
    turn_index=None,
    query=True,
    query_type="initial",
    request_key=None,
    conn=None,
):
    from src.server.database.runs.lifecycle import QuerySpec, start_run

    run_id = str(uuid.uuid4())
    row = await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=request_key or str(uuid.uuid4()),
        turn_index=turn_index,
        query=(
            QuerySpec(query_id=str(uuid.uuid4()), content=text, query_type=query_type)
            if query
            else None
        ),
        fork=fork,
        metadata={"msg_type": "ptc"},
        conn=conn,
    )
    return run_id, row["turn_index"]


async def _run(
    graph, thread_id, run_id, turn_index, text, *, checkpoint_id=None, conn=None
):
    """The graph half of a turn: stamped the way ``build_graph_config`` stamps
    a run, then committed the way the finalize commits it."""
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run
    from src.server.utils.checkpoint_helpers import newest_checkpoint

    configurable = {"thread_id": thread_id}
    if checkpoint_id:
        configurable["checkpoint_id"] = checkpoint_id
    config = {
        "configurable": configurable,
        "metadata": {"run_id": run_id, "turn_index": turn_index},
    }
    payload = {"messages": [HumanMessage(content=text)]} if text else None
    await graph.ainvoke(payload, config)
    await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        outcome=RunOutcome(status="completed"),
        newest=await newest_checkpoint(graph.checkpointer, thread_id),
        conn=conn,
    )


@pytest_asyncio.fixture(loop_scope="session")
async def thread(seed_workspace, test_db_pool, patched_get_db_connection):
    """Turns 0, 2 and 3 ran; turn 1's run failed before the graph wrote
    anything. Turn 2's slice is stored, as the turn finalize stores it."""
    from src.server.database.conversation import create_thread
    from src.server.database.conversation import turn_slices as slices_db
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    saver = AsyncPostgresSaver(test_db_pool)
    graph = _graph(saver)
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    runs = {}
    for text in ("q0", "q1", "q2", "q3"):
        run_id, turn_index = await _start(thread_id, text)
        runs[turn_index] = run_id
        if turn_index == DEAD_TURN:
            await finalize_run(
                run_id=run_id, thread_id=thread_id, outcome=RunOutcome(status="error")
            )
        else:
            await _run(graph, thread_id, run_id, turn_index, text)
    await slices_db.upsert_turn_slices(
        [
            slices_db.TurnRow(
                runs[2],
                thread_id,
                slices_db.StoredSlice("in-2", "tail-2", "k", "msgpack", b"\x80"),
            )
        ]
    )
    from src.server.app import setup

    with (
        patch(
            "src.server.handlers.checkpoint_handler.get_checkpointer",
            return_value=saver,
        ),
        patch.object(setup, "checkpointer", saver),
    ):
        yield SimpleNamespace(id=thread_id, graph=graph, saver=saver, runs=runs)
    await saver.adelete_thread(thread_id)


async def _turns(thread_id):
    from src.server.database.conversation import get_thread_checkpoint_id
    from src.server.handlers.checkpoint_handler import get_thread_turns

    tip = await get_thread_checkpoint_id(thread_id)
    return (await get_thread_turns(thread_id, branch_tip_checkpoint_id=tip)).turns


async def _settled(thread_id, checkpoint_id, client_turn, *, regenerate, messages=None):
    """The request the send route settles for a fork, and the spec the request
    prep hands ``start_run`` for it."""
    from src.server.handlers.chat.request_prep import _resolve_fork
    from src.server.handlers.checkpoint_handler import settle_fork_request
    from src.server.models.chat import ChatRequest

    request = await settle_fork_request(
        thread_id,
        ChatRequest(
            messages=(
                []
                if regenerate
                else messages or [{"role": "user", "content": "edited"}]
            ),
            checkpoint_id=checkpoint_id,
            fork_from_turn=client_turn,
        ),
    )
    _, fork = _resolve_fork(request=request, thread_id=thread_id)
    return request, fork


async def _fork(thread_id, checkpoint_id, client_turn, *, regenerate):
    """What the send route and the request prep hand ``start_run`` for a fork
    request."""
    _, fork = await _settled(
        thread_id, checkpoint_id, client_turn, regenerate=regenerate
    )
    return fork


async def _rows(conn, table, thread_id):
    cur = await conn.execute(
        f"SELECT turn_index FROM {table} WHERE conversation_thread_id = %s "
        "ORDER BY turn_index",
        (thread_id,),
    )
    return [r["turn_index"] for r in await cur.fetchall()]


async def _slices(conn, thread_id):
    cur = await conn.execute(
        "SELECT r.turn_index FROM turn_slices s "
        "JOIN conversation_responses r USING (conversation_response_id) "
        "WHERE s.conversation_thread_id = %s",
        (thread_id,),
    )
    return [r["turn_index"] for r in await cur.fetchall()]


async def test_turns_carry_the_turn_number_not_the_position(thread):
    turns = await _turns(thread.id)

    assert [t.turn_index for t in turns] == [0, 2, 3]


async def test_editing_the_latest_turn_keeps_every_turn_before_it(thread, db_conn):
    latest = (await _turns(thread.id))[-1]

    fork = await _fork(
        thread.id, latest.edit_checkpoint_id, latest.turn_index, regenerate=False
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q3 edited",
        checkpoint_id=latest.edit_checkpoint_id,
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert await _slices(db_conn, thread.id) == [2]
    # Every boundary on the new branch still names a turn with rows.
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 3]


async def test_regenerating_the_latest_turn_keeps_its_question(thread, db_conn):
    latest = (await _turns(thread.id))[-1]

    fork = await _fork(
        thread.id, latest.regenerate_checkpoint_id, latest.turn_index, regenerate=True
    )
    _, turn_index = await _start(
        thread.id, "", fork=fork, turn_index=fork.from_turn, query=False
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert await _slices(db_conn, thread.id) == [2]


async def test_a_client_count_cannot_move_the_fork(thread, db_conn):
    """A client that counted boundaries names the turn before the one its
    checkpoint replaces; the checkpoint decides."""
    latest = (await _turns(thread.id))[-1]

    edit = await _fork(thread.id, latest.edit_checkpoint_id, 2, regenerate=False)
    regenerate = await _fork(
        thread.id, latest.regenerate_checkpoint_id, 2, regenerate=True
    )

    assert (edit.from_turn, regenerate.from_turn) == (3, 3)
    await _start(thread.id, "q3 edited", fork=edit)
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert await _slices(db_conn, thread.id) == [2]


async def test_rewriting_the_failed_message_replaces_it(thread, db_conn):
    """Turn 1 left nothing on the branch, so its message forks where turn 2's
    does; the fork drops turn 1 and every turn after it."""
    _, second, _ = await _turns(thread.id)

    fork = await _fork(thread.id, second.edit_checkpoint_id, DEAD_TURN, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q1 again", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q1 again",
        checkpoint_id=second.edit_checkpoint_id,
    )

    assert turn_index == DEAD_TURN
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1]
    assert await _slices(db_conn, thread.id) == []
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 1]


async def test_a_fork_at_no_turn_of_the_branch_is_refused(thread, db_conn):
    from src.server.handlers.checkpoint_handler import resolve_fork_turn

    latest = (await _turns(thread.id))[-1]

    with pytest.raises(HTTPException) as refused:
        # A regenerate names the turn's own boundary, never its parent.
        await resolve_fork_turn(thread.id, latest.edit_checkpoint_id, regenerate=True)
    assert refused.value.status_code == 409
    with pytest.raises(HTTPException):
        await resolve_fork_turn(thread.id, "not-a-checkpoint", regenerate=False)
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]


async def _tip(conn, thread_id):
    cur = await conn.execute(
        "SELECT latest_checkpoint_id FROM conversation_threads "
        "WHERE conversation_thread_id = %s",
        (thread_id,),
    )
    return (await cur.fetchone())["latest_checkpoint_id"]


async def test_a_fork_validated_before_another_fork_landed_is_refused(thread, db_conn):
    """Admitted second, the edit of turn 3 would pin the branch the edit of
    turn 2 abandoned, under the rows that edit wrote."""
    from src.server.database.runs.lifecycle import StaleForkError

    _, second, latest = await _turns(thread.id)
    late = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    early = await _fork(thread.id, second.edit_checkpoint_id, 2, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q2 edited", fork=early)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q2 edited",
        checkpoint_id=second.edit_checkpoint_id,
    )
    tip = await _tip(db_conn, thread.id)

    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=late)

    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2]
    assert await _tip(db_conn, thread.id) == tip


async def test_a_fork_validated_before_a_new_turn_landed_is_refused(thread, db_conn):
    """The edit was read off a branch without turn 4, so it is not the one to
    delete it."""
    from src.server.database.runs.lifecycle import StaleForkError

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q4")
    await _run(thread.graph, thread.id, run_id, turn_index, "q4")

    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=fork)

    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3, 4]
    assert await _slices(db_conn, thread.id) == [2]


async def test_a_fork_waits_for_an_uncommitted_fork_then_refuses(
    thread, test_db_pool, db_conn
):
    """Two workers' STARTs overlapping: the tip check holds the thread row's
    lock, so it reads the tip the other fork commits, not the one before it."""
    from src.server.database.runs.lifecycle import StaleForkError

    _, second, latest = await _turns(thread.id)
    late = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    early = await _fork(thread.id, second.edit_checkpoint_id, 2, regenerate=False)

    async with test_db_pool.connection() as winner, winner.transaction():
        run_id, turn_index = await _start(
            thread.id, "q2 edited", fork=early, conn=winner
        )
        await _run(
            thread.graph,
            thread.id,
            run_id,
            turn_index,
            "q2 edited",
            checkpoint_id=second.edit_checkpoint_id,
            conn=winner,
        )
        loser = asyncio.create_task(_start(thread.id, "q3 edited", fork=late))
        async with asyncio.timeout(10):
            while not loser.done():
                cur = await db_conn.execute(
                    "SELECT count(*) AS waiting FROM pg_locks WHERE NOT granted"
                )
                if (await cur.fetchone())["waiting"]:
                    break
                await asyncio.sleep(0.05)
        assert not loser.done()

    with pytest.raises(StaleForkError):
        await loser
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2]


async def _waiting_on_a_lock(db_conn, task):
    """Return once ``task`` is blocked on a lock another transaction holds."""
    async with asyncio.timeout(10):
        while not task.done():
            cur = await db_conn.execute(
                "SELECT count(*) AS waiting FROM pg_locks WHERE NOT granted"
            )
            if (await cur.fetchone())["waiting"]:
                break
            await asyncio.sleep(0.05)
    assert not task.done()


async def test_a_fork_retransmit_that_waited_on_its_first_copy_reconnects(
    thread, test_db_pool, db_conn
):
    """Both copies probed the key before either START committed, so the
    retransmit waits on the thread row its first copy moves. Once that START
    commits, it is the same request already accepted, not a stale one."""
    from src.server.database.runs.lifecycle import DuplicateRequestError

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    key = str(uuid.uuid4())

    async with test_db_pool.connection() as first, first.transaction():
        run_id, _ = await _start(
            thread.id, "q3 edited", fork=fork, request_key=key, conn=first
        )
        retransmit = asyncio.create_task(
            _start(thread.id, "q3 edited", fork=fork, request_key=key)
        )
        await _waiting_on_a_lock(db_conn, retransmit)

    with pytest.raises(DuplicateRequestError) as duplicate:
        await retransmit
    assert str(duplicate.value.existing_run["conversation_response_id"]) == run_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]


async def _settle(
    thread, run_id, turn_index, status, metadata=None, *, pending_input=None
):
    """The owner's finalize, which reads the checkpoint it commits itself.
    ``pending_input`` is what START handed the run's handle."""
    from src.server.app import setup
    from src.server.services.runs.coordinator import (
        RunCoordinator,
        RunHandle,
        RunOutcome,
    )

    coordinator = RunCoordinator()
    for tail in (
        "_schedule_replay_refresh",
        "_schedule_transcript_export",
        "_schedule_archive_prune",
        "_nudge_hook_drainer",
    ):
        setattr(coordinator, tail, MagicMock())
    handle = RunHandle(
        run_id=run_id,
        thread_id=thread.id,
        turn_index=turn_index,
        attempt_no=1,
        pending_input=pending_input or {},
    )
    with patch.object(setup, "checkpointer", thread.saver):
        result = await coordinator.finalize_run(
            handle, RunOutcome(status=status, metadata=metadata or {})
        )
    assert result.applied


async def _stop_flush(thread):
    """A stop's flush: an update on the thread's newest checkpoint, written
    outside any graph step, so it names no run."""
    from src.server.services.runs.teardown import flush_checkpoint

    await flush_checkpoint(thread.graph, thread.id)


async def _newest(thread):
    return await thread.saver.aget_tuple({"configurable": {"thread_id": thread.id}})


async def test_an_edit_that_failed_before_its_first_step_keeps_its_fork(
    thread, db_conn
):
    """START deleted turn 3 for the edit; the old turn 3 is still the newest
    checkpoint, and must not come back as the thread's branch. The edited
    turn, which never reached the graph, forks from the pinned checkpoint."""
    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)

    await _settle(thread, run_id, turn_index, "error")

    assert await _tip(db_conn, thread.id) == latest.edit_checkpoint_id
    turns = await _turns(thread.id)
    assert [t.turn_index for t in turns] == [0, 2, 3]
    assert (turns[-1].edit_checkpoint_id, turns[-1].regenerate_checkpoint_id) == (
        latest.edit_checkpoint_id,
        latest.edit_checkpoint_id,
    )


async def _recover(thread, run_id):
    """The recovery scanner's settle of a run whose worker died, which a
    sibling worker makes from the run's row alone."""
    from src.server.app import setup
    from src.server.database.runs.lifecycle import ProducerRecord, list_open_runs
    from src.server.services.runs.coordinator import RunCoordinator
    from src.server.services.runs.recovery import RecoveryScanner

    coordinator = RunCoordinator()
    for tail in (
        "_schedule_replay_refresh",
        "_schedule_transcript_export",
        "_schedule_archive_prune",
        "_nudge_hook_drainer",
    ):
        setattr(coordinator, tail, MagicMock())
    scanner = RecoveryScanner()
    scanner._salvage = AsyncMock(
        return_value=(ProducerRecord(provenance=[]), "empty_stream")
    )
    [run] = [
        r for r in await list_open_runs() if str(r["conversation_response_id"]) == run_id
    ]
    with (
        patch.object(setup, "checkpointer", thread.saver),
        patch(
            "src.server.services.runs.coordinator.RunCoordinator.get_instance",
            return_value=coordinator,
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=SimpleNamespace(append_run_end_event=AsyncMock()),
        ),
    ):
        assert await scanner._recover_run(run, run_id, thread.id)


async def test_a_recovered_run_commits_the_checkpoint_its_step_wrote(thread, db_conn):
    from src.server.database.runs.lifecycle import QuerySpec, get_run, start_run

    run_id = str(uuid.uuid4())
    row = await start_run(
        run_id=run_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        query=QuerySpec(query_id=str(uuid.uuid4()), content="q4", query_type="initial"),
        metadata={"msg_type": "ptc"},
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": row["turn_index"]},
        },
    )

    await _recover(thread, run_id)

    assert (await get_run(run_id))["status"] == "error"
    assert await _tip(db_conn, thread.id) == (await _newest(thread)).checkpoint["id"]


async def test_a_recovered_edit_that_wrote_nothing_keeps_its_fork(thread, db_conn):
    from src.server.database.runs.lifecycle import QuerySpec, start_run

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        query=QuerySpec(
            query_id=str(uuid.uuid4()), content="q3 edited", query_type="initial"
        ),
        fork=fork,
        metadata={"msg_type": "ptc"},
    )

    await _recover(thread, run_id)

    assert await _tip(db_conn, thread.id) == latest.edit_checkpoint_id


async def test_a_regenerate_stopped_before_its_first_step_keeps_its_fork(
    thread, db_conn
):
    """The stop's flush lands on the newest checkpoint, the old answer's, so
    the newest checkpoint is new and still not the regenerate's."""
    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.regenerate_checkpoint_id, 3, regenerate=True)
    run_id, turn_index = await _start(
        thread.id, "", fork=fork, turn_index=fork.from_turn, query=False
    )
    before = await _newest(thread)
    await _stop_flush(thread)
    assert (await _newest(thread)).config != before.config

    await _settle(thread, run_id, turn_index, "cancelled")

    assert await _tip(db_conn, thread.id) == latest.regenerate_checkpoint_id


async def test_a_stopped_turn_commits_the_flush_on_its_own_step(thread, db_conn):
    run_id, turn_index = await _start(thread.id, "q4")
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    await _stop_flush(thread)
    flushed = await _newest(thread)
    assert flushed.metadata["source"] == "update"

    await _settle(thread, run_id, turn_index, "cancelled")

    assert await _tip(db_conn, thread.id) == flushed.checkpoint["id"]
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 3, 4]


async def test_a_fork_validated_before_a_stop_settled_is_admitted(thread, db_conn):
    """The edit is read while turn 4 is stopping, and the stop's teardown then
    commits its flush: the pointer moved, but only along the branch the edit
    was read off, and turn 4 was already there to be discarded."""
    run_id, turn_index = await _start(thread.id, "q4")
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    third = next(t for t in await _turns(thread.id) if t.turn_index == 3)
    fork = await _fork(thread.id, third.edit_checkpoint_id, 3, regenerate=False)
    await _stop_flush(thread)
    await _settle(thread, run_id, turn_index, "cancelled")
    assert await _tip(db_conn, thread.id) != fork.branch_tip

    _, forked_turn = await _start(thread.id, "q3 edited", fork=fork)

    assert forked_turn == 3
    assert await _tip(db_conn, thread.id) == third.edit_checkpoint_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]


async def test_a_fork_validated_before_a_compaction_landed_is_admitted(
    thread, db_conn
):
    """A manual compaction writes on top of the branch and moves the pointer
    there without starting a run."""
    from src.server.utils.checkpoint_helpers import update_at_commit

    _, second, _ = await _turns(thread.id)
    fork = await _fork(thread.id, second.regenerate_checkpoint_id, 2, regenerate=True)
    compacted = await update_at_commit(
        thread.graph,
        thread.id,
        fork.branch_tip,
        {"messages": [AIMessage(content="summary", id=f"ai-{uuid.uuid4()}")]},
    )
    assert compacted
    assert await _tip(db_conn, thread.id) != fork.branch_tip

    _, forked_turn = await _start(
        thread.id, "", fork=fork, turn_index=fork.from_turn, query=False
    )

    assert forked_turn == 2
    assert await _tip(db_conn, thread.id) == second.regenerate_checkpoint_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2]


async def test_a_fork_read_before_a_run_that_failed_unseen_is_refused(thread, db_conn):
    """A send started after the edit was read and failed before its first
    checkpoint, so the pointer never moved; the edit would still delete the
    turn that send added, which its user never saw."""
    from src.server.database.runs.lifecycle import StaleForkError

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q4")
    await _settle(thread, run_id, turn_index, "error")
    assert await _tip(db_conn, thread.id) == fork.branch_tip

    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=fork)

    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3, 4]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3, 4]


async def test_a_fork_whose_branch_walk_fails_is_refused(thread, db_conn):
    """The pointer moved after the read and the walk that would show the move
    only extended the branch errored: nothing proves the fork still fits."""
    from src.server.database.runs.lifecycle import StaleForkError

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    await _stop_flush(thread)
    flushed = (await _newest(thread)).checkpoint["id"]
    await db_conn.execute(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (flushed, thread.id),
    )

    async def broken_walk(pointer):
        raise RuntimeError("checkpointer pool exhausted")

    fork.on_branch = broken_walk
    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=fork)

    assert await _tip(db_conn, thread.id) == flushed
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]


async def test_a_pointer_beside_the_read_tip_refuses_the_fork(thread, db_conn):
    """The pointer now names a branch that keeps the fork's checkpoint but not
    the tip the fork was read off, so the move did not extend that branch."""
    from src.server.database.runs.lifecycle import StaleForkError

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    beside = await thread.graph.aupdate_state(
        {
            "configurable": {
                "thread_id": thread.id,
                "checkpoint_ns": "",
                "checkpoint_id": latest.edit_checkpoint_id,
            }
        },
        {"messages": [AIMessage(content="summary", id=f"ai-{uuid.uuid4()}")]},
    )
    beside_id = beside["configurable"]["checkpoint_id"]
    await db_conn.execute(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (beside_id, thread.id),
    )

    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=fork)

    assert await _tip(db_conn, thread.id) == beside_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]


async def test_a_fork_whose_checkpoint_left_the_branch_is_refused(thread, db_conn):
    """No run started, but the pointer now names a branch without the fork's
    checkpoint."""
    from src.server.database.runs.lifecycle import StaleForkError

    first, _, latest = await _turns(thread.id)
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    await db_conn.execute(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (first.regenerate_checkpoint_id, thread.id),
    )

    with pytest.raises(StaleForkError):
        await _start(thread.id, "q3 edited", fork=fork)

    assert await _tip(db_conn, thread.id) == first.regenerate_checkpoint_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]


def _said(text, image=None):
    content = text if image is None else [
        {"type": "text", "text": text},
        {"type": "image", "image_url": image},
    ]
    return [{"role": "user", "content": content}]


async def _fail_before_first_step(thread, text, request, fork):
    """A fork's START, then the finalize of its run failing before the graph
    took it, stamped the way the error path stamps it."""
    from src.server.services.runs.pending_input import pending_input_stamp

    run_id, turn_index = await _start(thread.id, text, fork=fork)
    await _settle(
        thread, run_id, turn_index, "error", metadata=pending_input_stamp(request)
    )


async def _last_human(thread):
    state = await thread.graph.aget_state({"configurable": {"thread_id": thread.id}})
    humans = [m for m in state.values["messages"] if m.type == "human"]
    return humans[-1].content


async def test_a_failed_edit_is_retried_as_the_same_edit(thread, db_conn):
    """The edit's turn has rows and nothing on the branch, which still forks
    from the pinned checkpoint: its own entry names it there."""
    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    await _fail_before_first_step(thread, "q3 edited", request, fork)

    pending = (await _turns(thread.id))[-1]
    assert (pending.turn_index, pending.edit_checkpoint_id) == (
        3,
        latest.edit_checkpoint_id,
    )
    _, fork = await _settled(
        thread.id, pending.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q3 edited",
        checkpoint_id=pending.edit_checkpoint_id,
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 3]
    assert await _last_human(thread) == "q3 edited"


async def test_a_failed_edit_is_retried_as_a_regenerate(thread, db_conn):
    """No checkpoint holds the edited message, so the regenerate sends it
    again from the turn's query row instead of answering turn 2 twice."""
    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    await _fail_before_first_step(thread, "q3 edited", request, fork)

    pending = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, pending.regenerate_checkpoint_id, 3, regenerate=True
    )
    assert [m.content for m in request.messages] == ["q3 edited"]
    assert (fork.from_turn, fork.preserve_query_at_fork) == (3, False)
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _run(
        thread.graph,
        thread.id,
        run_id,
        turn_index,
        "q3 edited",
        checkpoint_id=request.checkpoint_id,
    )

    assert turn_index == 3
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 3]
    assert await _last_human(thread) == "q3 edited"


async def test_a_failed_edit_with_an_image_is_resent_as_the_same_fork(
    thread, db_conn
):
    """The query row keeps only the text, so the client sends the edit again
    as it was; a regenerate, which would drop the image, is refused."""
    from src.server.services.runs.pending_input import (
        needs_resend,
        pending_input_stamp,
    )

    latest = (await _turns(thread.id))[-1]
    said = _said("q3 edited", image="data:image/png;base64,iVBORw0KGgo=")
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False, messages=said
    )
    assert needs_resend(pending_input_stamp(request))
    await _fail_before_first_step(thread, "q3 edited", request, fork)

    pending = (await _turns(thread.id))[-1]
    with pytest.raises(HTTPException) as refused:
        await _settled(thread.id, pending.regenerate_checkpoint_id, 3, regenerate=True)
    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "resend_required"

    resent, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False, messages=said
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)

    assert turn_index == 3
    assert resent.messages == request.messages
    assert await _tip(db_conn, thread.id) == latest.edit_checkpoint_id
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0, 1, 2, 3]


async def _messages(thread):
    state = await thread.graph.aget_state({"configurable": {"thread_id": thread.id}})
    return state.values["messages"]


async def _humans(thread):
    return [m.content for m in await _messages(thread) if m.type == "human"]


async def _send_base(thread, request):
    """Where the request prep starts a send's graph."""
    from src.server.handlers.chat.request_prep import turn_start_checkpoint_id

    return await turn_start_checkpoint_id(thread.id, request, thread.saver)


async def _failed_edit(thread):
    """An edit of turn 3 whose run failed before its first step; returns the
    checkpoint START pinned for it."""
    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    await _fail_before_first_step(thread, "q3 edited", request, fork)
    return latest.edit_checkpoint_id


async def test_a_send_after_a_failed_edit_runs_on_the_fork(thread, db_conn):
    """The old turn 3 is still the newest checkpoint, but START deleted its
    rows for the edit: the next message follows turn 2, as the thread shows
    it, and not the turn the user replaced."""
    from src.server.models.chat import ChatRequest

    pinned = await _failed_edit(thread)

    run_id, turn_index = await _start(thread.id, "q4")
    base = await _send_base(thread, ChatRequest(messages=_said("q4")))
    await _run(thread.graph, thread.id, run_id, turn_index, "q4", checkpoint_id=base)

    assert await _humans(thread) == ["q0", "q2", "q4"]
    assert base == pinned
    assert [t.turn_index for t in await _turns(thread.id)] == [0, 2, 4]


async def _unstamp(conn, thread):
    """The thread's checkpoints as a graph wrote them before runs were
    stamped: none names the run that wrote it."""
    await conn.execute(
        "UPDATE checkpoints SET metadata = metadata - 'run_id' WHERE thread_id = %s",
        (thread.id,),
    )


async def test_a_send_after_a_failed_edit_of_an_unstamped_thread_runs_on_the_fork(
    thread, db_conn
):
    """The replaced turn 3 names no run, so only the edit's run row shows the
    pointer is the edit's pin; a send that also failed before its first step
    changes nothing."""
    from src.server.models.chat import ChatRequest

    await _unstamp(db_conn, thread)
    pinned = await _failed_edit(thread)
    run_id, turn_index = await _start(thread.id, "q4")
    await _settle(thread, run_id, turn_index, "error")

    run_id, turn_index = await _start(thread.id, "q5")
    base = await _send_base(thread, ChatRequest(messages=_said("q5")))
    await _run(thread.graph, thread.id, run_id, turn_index, "q5", checkpoint_id=base)

    assert base == pinned
    assert await _humans(thread) == ["q0", "q2", "q5"]


@pytest.mark.parametrize("gap", ["unstamped", "rowless"])
async def test_a_pointer_a_turn_left_behind_keeps_the_newest_checkpoint(
    thread, db_conn, gap
):
    """Turn 3 ran past the pointer without moving it: its run wrote before
    runs were stamped, or never had a row. No fork pinned the pointer, so the
    send keeps turn 3 rather than start behind it."""
    from src.server.models.chat import ChatRequest

    behind = (await _turns(thread.id))[-1].edit_checkpoint_id
    await db_conn.execute(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (behind, thread.id),
    )
    if gap == "unstamped":
        await _unstamp(db_conn, thread)
    else:
        await db_conn.execute(
            "DELETE FROM conversation_responses WHERE conversation_response_id = %s",
            (thread.runs[3],),
        )

    assert await _send_base(thread, ChatRequest(messages=_said("q4"))) is None


async def test_a_fork_whose_finalize_missed_its_steps_keeps_them(thread, db_conn):
    """The edit's run row names the pointer as its pin, but the run wrote
    steps past it that its finalize never read: the send follows them."""
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run
    from src.server.models.chat import ChatRequest

    latest = (await _turns(thread.id))[-1]
    fork = await _fork(thread.id, latest.edit_checkpoint_id, 3, regenerate=False)
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q3 edited")]},
        {
            "configurable": {
                "thread_id": thread.id,
                "checkpoint_id": latest.edit_checkpoint_id,
            },
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    await finalize_run(
        run_id=run_id, thread_id=thread.id, outcome=RunOutcome(status="error")
    )

    assert await _tip(db_conn, thread.id) == latest.edit_checkpoint_id
    assert await _send_base(thread, ChatRequest(messages=_said("q4"))) is None
    assert await _humans(thread) == ["q0", "q2", "q3 edited"]


async def _answer(thread, named):
    """A HITL answer, naming the checkpoint it resumes (an API client may) or
    resuming the newest, which is the paused turn's own checkpoint."""
    from src.server.models.chat import ChatRequest, HITLResponse

    pause = (await _newest(thread)).config["configurable"]["checkpoint_id"]
    return ChatRequest(
        messages=[],
        hitl_response={"pause": HITLResponse()},
        checkpoint_id=pause if named else None,
    )


_NAMED = pytest.mark.parametrize("named", [False, True], ids=["newest", "named"])


@_NAMED
async def test_an_answer_to_a_pause_a_failed_edit_replaced_is_refused(
    thread, db_conn, named
):
    """The pause the answer resumes is the turn the edit replaced, so the
    route refuses it as stale instead of resuming a turn the thread no longer
    shows."""
    from src.server.handlers.checkpoint_handler import settle_resume_request

    answer = await _answer(thread, named)
    await settle_resume_request(thread.id, answer)

    await _failed_edit(thread)
    with pytest.raises(HTTPException) as refused:
        await settle_resume_request(thread.id, answer)
    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "stale_fork"


async def test_an_answer_naming_a_pause_an_edit_replaced_is_refused(thread, db_conn):
    """The edit's run wrote its own branch and the pointer moved onto it, so
    the pause the answer names is on no branch the thread shows. Resumed
    there, its finalize would move the pointer back onto the replaced turn."""
    from src.server.handlers.checkpoint_handler import settle_resume_request

    answer = await _answer(thread, named=True)
    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _run(
        thread.graph, thread.id, run_id, turn_index, "q3 edited",
        checkpoint_id=request.checkpoint_id,
    )

    with pytest.raises(HTTPException) as refused:
        await settle_resume_request(thread.id, answer)
    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "stale_fork"


@_NAMED
async def test_an_answer_whose_pause_an_edit_replaced_after_the_check_is_refused(
    thread, db_conn, named
):
    """An edit admitted between the route's check and the answer's START
    replaced the pause, so START refuses the answer instead of resuming it
    there."""
    from src.server.database.runs.lifecycle import StaleForkError, start_run
    from src.server.handlers.checkpoint_handler import settle_resume_request

    answer = await settle_resume_request(thread.id, await _answer(thread, named))
    await _failed_edit(thread)
    responses = await _rows(db_conn, "conversation_responses", thread.id)

    with pytest.raises(StaleForkError):
        await start_run(
            run_id=str(uuid.uuid4()),
            thread_id=thread.id,
            request_key=str(uuid.uuid4()),
            run_horizon=answer.resume_run_horizon,
            metadata={"msg_type": "ptc"},
        )
    assert await _rows(db_conn, "conversation_responses", thread.id) == responses


@_NAMED
async def test_an_answer_no_run_followed_starts(thread, db_conn, named):
    """The horizon is the newest run when the route read it, so the run the
    pause belongs to is inside it and the answer starts."""
    from src.server.database.runs.lifecycle import start_run
    from src.server.handlers.checkpoint_handler import settle_resume_request

    answer = await settle_resume_request(thread.id, await _answer(thread, named))
    row = await start_run(
        run_id=str(uuid.uuid4()),
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        run_horizon=answer.resume_run_horizon,
        metadata={"msg_type": "ptc"},
    )
    assert row["status"] == "in_progress"


async def test_an_answer_retransmit_that_waited_on_its_first_copy_reconnects(
    thread, test_db_pool, db_conn
):
    """The first copy's run is past the answer's horizon once its START
    commits, so the retransmit waiting on the thread row reads it as a run
    started since. Its key names that run: reconnect, not stale_fork."""
    from src.server.database.runs.lifecycle import DuplicateRequestError, start_run
    from src.server.handlers.checkpoint_handler import settle_resume_request

    answer = await settle_resume_request(thread.id, await _answer(thread, False))
    key = str(uuid.uuid4())

    def _resume(conn=None):
        return start_run(
            run_id=str(uuid.uuid4()),
            thread_id=thread.id,
            request_key=key,
            run_horizon=answer.resume_run_horizon,
            metadata={"msg_type": "ptc"},
            conn=conn,
        )

    async with test_db_pool.connection() as first, first.transaction():
        row = await _resume(first)
        retransmit = asyncio.create_task(_resume())
        await _waiting_on_a_lock(db_conn, retransmit)

    with pytest.raises(DuplicateRequestError) as duplicate:
        await retransmit
    assert (
        duplicate.value.existing_run["conversation_response_id"]
        == row["conversation_response_id"]
    )


async def test_a_retried_send_after_a_failed_edit_runs_on_the_fork(thread, db_conn):
    """/retry runs a send that never reached the graph as that send again,
    from where a send starts."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        pending_input_stamp,
        read_pending_input,
    )

    pinned = await _failed_edit(thread)
    send = ChatRequest(messages=_said("q4"))
    run_id, turn_index = await _start(thread.id, "q4")
    await _settle(
        thread, run_id, turn_index, "error", metadata=pending_input_stamp(send)
    )

    failed = await get_latest_attempt(thread.id)
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    base = await _send_base(thread, retry)
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": base}},
    )

    assert await _humans(thread) == ["q0", "q2", "q4"]
    assert base == pinned


async def test_a_retried_regenerate_without_pending_input_replays_its_fork(
    thread, db_conn
):
    """A run lost after the executor took it stamps no pending input, so
    /retry replays the checkpoint it finds: the regenerate's, where the thread
    is, not the old answer's, which would run nothing."""
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint

    latest = (await _turns(thread.id))[-1]
    old_answer = (await _messages(thread))[-1].id
    fork = await _fork(thread.id, latest.regenerate_checkpoint_id, 3, regenerate=True)
    run_id, turn_index = await _start(
        thread.id, "", fork=fork, turn_index=fork.from_turn, query=False
    )
    await _settle(thread, run_id, turn_index, "error")

    checkpoint_id = await get_retry_checkpoint(thread.id)
    await thread.graph.ainvoke(
        None, {"configurable": {"thread_id": thread.id, "checkpoint_id": checkpoint_id}}
    )

    messages = await _messages(thread)
    assert (messages[-2].content, messages[-1].type) == ("q3", "ai")
    assert messages[-1].id != old_answer
    assert checkpoint_id == latest.regenerate_checkpoint_id


async def test_a_send_on_the_thread_branch_starts_from_the_newest_checkpoint(
    thread, db_conn
):
    """The graph's default stands while the newest checkpoint is on the
    thread's branch: the pointer itself, a stop's flush on it, or steps of a
    run finalized without reading them."""
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint
    from src.server.models.chat import ChatRequest

    send = ChatRequest(messages=_said("q4"))
    assert await _send_base(thread, send) is None

    pointer = await _tip(db_conn, thread.id)
    await _stop_flush(thread)
    flushed = (await _newest(thread)).checkpoint["id"]
    assert flushed != pointer
    assert await _send_base(thread, send) is None
    assert await get_retry_checkpoint(thread.id) == flushed

    run_id, turn_index = await _start(thread.id, "q4")
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    await finalize_run(
        run_id=run_id, thread_id=thread.id, outcome=RunOutcome(status="error")
    )
    assert await _tip(db_conn, thread.id) == pointer
    assert await _send_base(thread, send) is None


async def test_an_edit_that_failed_in_its_executor_is_retried_as_the_edit(
    thread, db_conn
):
    """The executor's failure path stamps nothing, so the finalize stamps the
    edit when no step on the thread is its run's: /retry sends the edit again
    instead of replaying the fork's base, which would run nothing."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.services.runs.pending_input import (
        executor_input_stamp,
        input_rerun_request,
        read_pending_input,
    )

    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    run_id, turn_index = await _start(thread.id, "q3 edited", fork=fork)
    await _settle(
        thread, run_id, turn_index, "error",
        pending_input=executor_input_stamp(request),
    )

    failed = await get_latest_attempt(thread.id)
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": retry.checkpoint_id}},
    )

    assert retry.checkpoint_id == latest.edit_checkpoint_id
    assert await _humans(thread) == ["q0", "q2", "q3 edited"]


async def _start_owned(thread, text, request, fork=None):
    """START as the coordinator runs it for ``request``, which keeps on the
    run's row the stamp its input needs if the graph never takes it."""
    from src.server.database.runs.lifecycle import QuerySpec
    from src.server.services.runs.coordinator import RunCoordinator
    from src.server.services.runs.pending_input import executor_input_stamp

    run_id = str(uuid.uuid4())
    with (
        patch(
            "src.server.services.thread_control_stream.announce_run_started",
            new=AsyncMock(),
        ),
        patch(
            "src.server.services.thread_lifecycle_feed.publish_run_started",
            new=AsyncMock(),
        ),
    ):
        handle = await RunCoordinator().start_run(
            thread_id=thread.id,
            run_id=run_id,
            request_key=str(uuid.uuid4()),
            query=QuerySpec(
                query_id=str(uuid.uuid4()), content=text, query_type="initial"
            ),
            fork=fork,
            pending_input=executor_input_stamp(request),
        )
    if handle.guard is not None:
        # No graph runs here, so the run's pinned session goes back at once.
        await handle.guard.release()
    return run_id, handle.turn_index


async def test_an_edit_whose_worker_died_before_its_graph_is_retried_as_the_edit(
    thread, db_conn
):
    """The recovery scanner settles the run with no handle, from the stamp
    START kept on its row: /retry sends the edit again, as after a failure in
    the executor."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
    )

    latest = (await _turns(thread.id))[-1]
    request, fork = await _settled(
        thread.id, latest.edit_checkpoint_id, 3, regenerate=False,
        messages=_said("q3 edited"),
    )
    run_id, _ = await _start_owned(thread, "q3 edited", request, fork)
    await _recover(thread, run_id)

    failed = await get_latest_attempt(thread.id)
    assert (str(failed["conversation_response_id"]), failed["status"]) == (
        run_id,
        "error",
    )
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": retry.checkpoint_id}},
    )

    assert retry.checkpoint_id == latest.edit_checkpoint_id
    assert await _humans(thread) == ["q0", "q2", "q3 edited"]


async def test_a_recovered_run_that_reached_the_graph_is_retried_from_its_checkpoint(
    thread, db_conn
):
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import read_pending_input

    run_id, turn_index = await _start_owned(
        thread, "q4", ChatRequest(messages=_said("q4"))
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    await _recover(thread, run_id)

    assert read_pending_input(await get_latest_attempt(thread.id)) is None


async def test_a_run_that_reached_the_graph_is_retried_from_its_checkpoint(
    thread, db_conn
):
    """Once the graph wrote the run's input, the thread holds it, and a retry
    resends nothing."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import (
        executor_input_stamp,
        read_pending_input,
    )

    run_id, turn_index = await _start(thread.id, "q4")
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    await _settle(
        thread, run_id, turn_index, "error",
        pending_input=executor_input_stamp(ChatRequest(messages=_said("q4"))),
    )

    assert read_pending_input(await get_latest_attempt(thread.id)) is None



async def _reconcile(thread, run_id):
    """The dispatch consumer's last-resort settle of a run its generator left
    open, which knows the thread and the run and nothing of the request."""
    from src.server.app import setup
    from src.server.services.runs.coordinator import RunCoordinator

    coordinator = RunCoordinator()
    for tail in (
        "_schedule_replay_refresh",
        "_schedule_transcript_export",
        "_schedule_archive_prune",
        "_nudge_hook_drainer",
    ):
        setattr(coordinator, tail, MagicMock())
    with (
        patch.object(setup, "checkpointer", thread.saver),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=SimpleNamespace(append_run_end_event=AsyncMock()),
        ),
    ):
        return await coordinator.reconcile_orphaned_dispatch(thread.id, run_id)


async def test_a_send_whose_dispatch_died_before_its_graph_is_retried_as_the_send(
    thread, db_conn
):
    """Settled unstamped, the failed send would be retried as a replay of the
    thread's checkpoint, which answers turn 3 again and drops the question."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
    )

    tip = await _tip(db_conn, thread.id)
    run_id, _ = await _start_owned(thread, "q4", ChatRequest(messages=_said("q4")))
    assert await _reconcile(thread, run_id) == "error"

    failed = await get_latest_attempt(thread.id)
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    base = await _send_base(thread, retry)
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": base}},
    )

    assert str(failed["conversation_response_id"]) == run_id
    assert await _tip(db_conn, thread.id) == tip
    assert await _humans(thread) == ["q0", "q2", "q3", "q4"]


async def test_a_dispatch_that_died_after_its_graph_step_commits_that_step(
    thread, db_conn
):
    """The graph took the input, so the thread holds it: no stamp, and the
    pointer moves to the run's step, where the next turn starts."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import read_pending_input

    run_id, turn_index = await _start_owned(
        thread, "q4", ChatRequest(messages=_said("q4"))
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content="q4")]},
        {
            "configurable": {"thread_id": thread.id},
            "metadata": {"run_id": run_id, "turn_index": turn_index},
        },
    )
    assert await _reconcile(thread, run_id) == "error"

    assert read_pending_input(await get_latest_attempt(thread.id)) is None
    assert await _tip(db_conn, thread.id) == (await _newest(thread)).checkpoint["id"]


async def _regenerated_in_place(thread, turn_index, text, base=None):
    """Regenerate the thread's last turn, one that never reached the graph,
    as the client asks for it, and run the request the route settles."""
    tip = (await _turns(thread.id))[-1]
    assert tip.turn_index == turn_index
    base = base or tip.regenerate_checkpoint_id
    request, fork = await _settled(thread.id, base, turn_index, regenerate=True)
    run_id, forked = await _start(thread.id, text, fork=fork)
    await _run(
        thread.graph, thread.id, run_id, forked, text,
        checkpoint_id=request.checkpoint_id,
    )
    assert [m.content for m in request.messages] == [text]
    assert forked == turn_index


async def test_a_send_stopped_before_its_graph_is_regenerated_in_place(
    thread, db_conn
):
    """No checkpoint holds the stopped question, so the regenerate sends its
    stored text again; unstamped, it was refused as resend_required."""
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import executor_input_stamp

    request = ChatRequest(messages=_said("q4"))
    run_id, turn_index = await _start_owned(thread, "q4", request)
    await _stop_flush(thread)
    await _settle(
        thread, run_id, turn_index, "cancelled",
        pending_input=executor_input_stamp(request),
    )

    await _regenerated_in_place(thread, 4, "q4")

    assert await _humans(thread) == ["q0", "q2", "q3", "q4"]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0, 1, 2, 3, 4]


async def test_a_send_stopped_on_a_dead_worker_is_regenerated_in_place(
    thread, db_conn
):
    """The scanner adopts the stop from the row's durable intent and stamps
    it from the row, as it stamps a failure."""
    from src.server.database.runs.lifecycle import get_run, request_run_cancel
    from src.server.models.chat import ChatRequest

    run_id, _ = await _start_owned(thread, "q4", ChatRequest(messages=_said("q4")))
    assert (await request_run_cancel(run_id, thread.id))["state"] == "requested"
    await _recover(thread, run_id)
    assert (await get_run(run_id))["status"] == "cancelled"

    await _regenerated_in_place(thread, 4, "q4")

    assert await _humans(thread) == ["q0", "q2", "q3", "q4"]

# The first message: LangGraph saves no checkpoint before a thread's first
# input, so its edit forks from the empty root the route writes.


async def _root_turns(thread):
    from src.server.utils.checkpoint_helpers import FORK_ROOT_CHECKPOINT_ID

    return FORK_ROOT_CHECKPOINT_ID, await _turns(thread.id)


async def test_editing_the_first_message_replaces_the_conversation(thread, db_conn):
    root, turns = await _root_turns(thread)
    assert turns[0].edit_checkpoint_id == root

    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, turn_index = await _start(thread.id, "q0 edited", fork=fork)
    await _run(
        thread.graph, thread.id, run_id, turn_index, "q0 edited",
        checkpoint_id=request.checkpoint_id,
    )

    assert turn_index == 0
    assert await _humans(thread) == ["q0 edited"]
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0]
    assert await _rows(db_conn, "conversation_responses", thread.id) == [0]
    assert await _slices(db_conn, thread.id) == []
    turns = await _turns(thread.id)
    assert [(t.turn_index, t.edit_checkpoint_id) for t in turns] == [(0, root)]


async def _failed_first_edit(thread):
    """An edit of the first message whose run failed before its first step,
    stamped the way its finalize stamps it."""
    from src.server.services.runs.pending_input import executor_input_stamp

    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, turn_index = await _start(thread.id, "q0 edited", fork=fork)
    await _settle(
        thread, run_id, turn_index, "error",
        pending_input=executor_input_stamp(request),
    )
    return root


async def test_a_send_after_a_failed_first_message_edit_starts_empty(thread, db_conn):
    """START pinned the root and deleted every turn, whose steps are still the
    newest: the next message starts from nothing, as the thread shows it."""
    from src.server.models.chat import ChatRequest

    root = await _failed_first_edit(thread)
    assert await _tip(db_conn, thread.id) == root
    turns = await _turns(thread.id)
    assert [
        (t.turn_index, t.edit_checkpoint_id, t.regenerate_checkpoint_id)
        for t in turns
    ] == [(0, root, root)]

    run_id, turn_index = await _start(thread.id, "q1")
    base = await _send_base(thread, ChatRequest(messages=_said("q1")))
    await _run(thread.graph, thread.id, run_id, turn_index, "q1", checkpoint_id=base)

    assert base == root
    assert await _humans(thread) == ["q1"]


async def test_a_failed_first_message_edit_is_retried_in_place(thread, db_conn):
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
    )

    root = await _failed_first_edit(thread)

    failed = await get_latest_attempt(thread.id)
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": retry.checkpoint_id}},
    )

    assert (failed["turn_index"], retry.checkpoint_id) == (0, root)
    assert await _humans(thread) == ["q0 edited"]


async def test_a_first_message_edit_whose_worker_died_is_retried_in_place(
    thread, db_conn
):
    """The recovery scanner stamps the edit from its row, so /retry sends it
    again from the root instead of replaying the root, which holds no input."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
    )

    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, _ = await _start_owned(thread, "q0 edited", request, fork)
    await _recover(thread, run_id)

    failed = await get_latest_attempt(thread.id)
    retry = await input_rerun_request(
        thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    await thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": thread.id, "checkpoint_id": retry.checkpoint_id}},
    )

    assert (failed["status"], failed["turn_index"]) == ("error", 0)
    assert retry.checkpoint_id == root
    assert await _humans(thread) == ["q0 edited"]


async def test_a_first_message_edit_settled_without_a_stamp_is_not_retryable(
    thread, db_conn
):
    """A finalize whose newest-checkpoint read failed stamps nothing, which
    leaves /retry the root to replay: it holds no input, so the retry is
    refused rather than run into an empty graph."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint
    from src.server.services.runs.pending_input import (
        executor_input_stamp,
        read_pending_input,
    )

    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, turn_index = await _start(thread.id, "q0 edited", fork=fork)
    with patch(
        "src.server.utils.checkpoint_helpers.newest_checkpoint",
        AsyncMock(side_effect=RuntimeError("checkpointer down")),
    ):
        await _settle(
            thread, run_id, turn_index, "error",
            pending_input=executor_input_stamp(request),
        )
    assert read_pending_input(await get_latest_attempt(thread.id)) is None
    assert await _tip(db_conn, thread.id) == root

    for named in (None, root):
        with pytest.raises(HTTPException) as refused:
            await get_retry_checkpoint(thread.id, named)
        assert refused.value.status_code == 409
        assert refused.value.detail["code"] == "not_retryable"


async def test_a_first_message_edit_is_admitted_past_a_stop(thread, db_conn):
    """The root is on every branch, so a pointer a stop's flush moved without
    a run still admits it."""
    root, _ = await _root_turns(thread)
    fork = await _fork(thread.id, root, 0, regenerate=False)
    await _stop_flush(thread)
    pointer = (await _newest(thread)).checkpoint["id"]
    await db_conn.execute(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (pointer, thread.id),
    )

    _, turn_index = await _start(thread.id, "q0 edited", fork=fork)

    assert turn_index == 0
    assert await _tip(db_conn, thread.id) == root
    assert await _rows(db_conn, "conversation_queries", thread.id) == [0]



@pytest_asyncio.fixture(loop_scope="session")
async def fenced(test_db_uri):
    """The writer guard on, as it runs with the checkpoints in the app's
    database: every coordinator START takes the run's pinned session."""
    from src.server.services import writer_guard

    await writer_guard.open_writer_pool(test_db_uri)
    try:
        yield
    finally:
        await writer_guard.close_writer_pool()


async def _root(thread):
    from src.server.utils.checkpoint_helpers import (
        FORK_ROOT_CHECKPOINT_ID,
        build_checkpoint_config,
    )

    return await thread.saver.aget_tuple(
        build_checkpoint_config(thread.id, FORK_ROOT_CHECKPOINT_ID)
    )


async def test_a_fenced_first_message_edit_writes_the_root_as_start_pins_it(
    thread, db_conn, fenced
):
    """The route writes no checkpoint; START writes the root on the run's
    pinned session, in the transaction that pins the pointer to it."""
    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    assert await _root(thread) is None

    await _start_owned(thread, "q0 edited", request, fork)

    assert await _root(thread) is not None
    assert await _tip(db_conn, thread.id) == root


async def test_a_refused_first_message_edit_writes_no_root(thread, db_conn, fenced):
    """A turn landed after the edit was read, so START refuses it, and the
    refused request leaves no checkpoint behind."""
    from src.server.database.runs.lifecycle import StaleForkError

    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, turn_index = await _start(thread.id, "q4")
    await _run(thread.graph, thread.id, run_id, turn_index, "q4")

    with pytest.raises(StaleForkError):
        await _start_owned(thread, "q0 edited", request, fork)
    assert await _root(thread) is None


async def test_a_first_message_edit_that_fails_in_start_writes_no_root(
    thread, db_conn, fenced
):
    """A failure after the root's write rolls it back with the pin."""
    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    pointer = await _tip(db_conn, thread.id)

    with (
        patch(
            "src.server.database.order_attempts.refuse_attempts_on_fork",
            AsyncMock(side_effect=RuntimeError("down")),
        ),
        pytest.raises(RuntimeError),
    ):
        await _start_owned(thread, "q0 edited", request, fork)
    assert await _root(thread) is None
    assert await _tip(db_conn, thread.id) == pointer


async def test_a_first_message_edit_whose_session_died_writes_no_root(
    thread, db_conn, fenced
):
    """A worker whose pinned session died no longer holds the thread's fence,
    and the root goes down with the session instead of landing beside it."""
    import psycopg

    from src.server.services.writer_guard import WriterGuard

    root, _ = await _root_turns(thread)
    request, fork = await _settled(
        thread.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    acquire = WriterGuard.acquire_root.__func__

    async def acquire_then_die(cls, **kwargs):
        guard = await acquire(cls, **kwargs)
        await db_conn.execute(
            "SELECT pg_terminate_backend(%s)", (guard.conn.info.backend_pid,)
        )
        return guard

    with (
        patch.object(WriterGuard, "acquire_root", classmethod(acquire_then_die)),
        pytest.raises(psycopg.OperationalError),
    ):
        await _start_owned(thread, "q0 edited", request, fork)
    assert await _root(thread) is None


async def _store_own(thread, checkpoint_id, *, live=False):
    """The thread's own transcript as a save leaves it: rendered at
    ``checkpoint_id``, or saved live by compaction labelled there."""
    from src.server.database.conversation import get_thread_by_id
    from src.server.database.thread_transcripts import (
        LIVE_FINGERPRINT,
        StoredTranscript,
        save_stored,
    )
    from src.server.database.workspace import workspace_owner

    workspace_id = str((await get_thread_by_id(thread.id))["workspace_id"])
    copy = StoredTranscript(
        LIVE_FINGERPRINT if live else f"fp-{checkpoint_id}", checkpoint_id, "{}"
    )
    assert await save_stored(thread.id, "", await workspace_owner(workspace_id), copy)


async def _own_copy(conn, thread_id):
    cur = await conn.execute(
        "SELECT checkpoint_id FROM thread_transcripts "
        "WHERE conversation_thread_id = %s AND prefix = ''",
        (thread_id,),
    )
    row = await cur.fetchone()
    return row["checkpoint_id"] if row else None


@contextmanager
def _exporting(thread):
    """An export's reads: the thread's checkpoints, and the store's rows as
    the app pool returns them, not as the test pool's dict rows."""
    from psycopg.rows import dict_row, tuple_row

    from src.server.services.history.reader import CheckpointHistoryReader

    @asynccontextmanager
    async def connection():
        async with thread.saver.conn.connection() as conn:
            conn.row_factory = tuple_row
            try:
                yield conn
            finally:
                conn.row_factory = dict_row

    with (
        patch.object(
            CheckpointHistoryReader,
            "get_instance",
            return_value=CheckpointHistoryReader(thread.saver),
        ),
        patch("src.server.database.thread_transcripts.get_db_connection", connection),
    ):
        yield


async def test_a_failed_first_message_edit_leaves_no_copy_of_the_turns_it_replaced(
    thread, db_conn
):
    """START drops the own copy, but an export that read the replaced turns
    can land after it. The root renders nothing over that copy, which reads
    as newer than the root, so the next export drops it."""
    from src.server.services import transcripts

    replaced = await _tip(db_conn, thread.id)
    await _store_own(thread, replaced)
    root = await _failed_first_edit(thread)
    assert await _own_copy(db_conn, thread.id) is None
    await _store_own(thread, replaced)

    with _exporting(thread):
        await transcripts._export(thread.id, None)

    assert await _tip(db_conn, thread.id) == root
    assert await _own_copy(db_conn, thread.id) is None


async def test_a_failed_edit_leaves_no_copy_of_the_turns_it_replaced(thread, db_conn):
    """The same late export after an edit further on: its copy of the
    replaced turn reads as newer than the checkpoint START pinned, so the
    next export drops it and renders the turns the edit kept in its place."""
    from src.server.services import transcripts

    replaced = await _tip(db_conn, thread.id)
    await _store_own(thread, replaced)
    pinned = await _failed_edit(thread)
    assert await _own_copy(db_conn, thread.id) is None
    await _store_own(thread, replaced)

    with _exporting(thread):
        await transcripts._export(thread.id, None)

    assert await _tip(db_conn, thread.id) == pinned
    assert await _own_copy(db_conn, thread.id) == pinned


async def test_a_failed_edit_leaves_no_live_copy_of_the_turn_it_replaced(thread, db_conn):
    """A copy compaction saved live is labelled at the checkpoint its turn
    started from and holds that turn's messages, so one labelled where the
    edit forks shows the turn it replaced: START drops it with the turns."""
    pinned = (await _turns(thread.id))[-1].edit_checkpoint_id
    await _store_own(thread, pinned, live=True)

    assert await _failed_edit(thread) == pinned

    assert await _own_copy(db_conn, thread.id) is None


async def test_a_run_open_from_the_root_keeps_its_live_copy(thread, db_conn):
    from src.server.services import transcripts

    root = await _failed_first_edit(thread)
    await _start(thread.id, "q0 again")
    await _store_own(thread, root, live=True)

    with _exporting(thread):
        await transcripts._export(thread.id, None)

    assert await _own_copy(db_conn, thread.id) == root


async def test_an_export_that_read_the_root_keeps_a_turn_that_landed_since(
    thread, db_conn
):
    from src.server.database.thread_transcripts import load_stored
    from src.server.services import transcripts

    root = await _failed_first_edit(thread)
    run_id, turn_index = await _start(thread.id, "q1")
    await _run(thread.graph, thread.id, run_id, turn_index, "q1", checkpoint_id=root)
    landed = await _tip(db_conn, thread.id)
    await _store_own(thread, landed)

    stale = transcripts.Behind(thread.id, root, {}, {""}, set())
    with _exporting(thread):
        stored = (await load_stored([thread.id])).get(thread.id, {})
        await transcripts._render(transcripts._Target("ws", "user", True), stale, stored)

    assert landed != root
    assert await _own_copy(db_conn, thread.id) == landed

@pytest_asyncio.fixture(loop_scope="session")
async def unstarted(request, seed_workspace, test_db_pool, patched_get_db_connection):
    """A new thread whose first send failed before the graph took it: one
    turn of rows, stamped, and no checkpoint. The send's query type is the
    fixture's parameter, ``initial`` by default."""
    from src.server.app import setup
    from src.server.database.conversation import create_thread
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import pending_input_stamp

    saver = AsyncPostgresSaver(test_db_pool)
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    with (
        patch(
            "src.server.handlers.checkpoint_handler.get_checkpointer",
            return_value=saver,
        ),
        patch.object(setup, "checkpointer", saver),
    ):
        thread = SimpleNamespace(id=thread_id, graph=_graph(saver), saver=saver)
        run_id, turn_index = await _start(
            thread_id, "q0", query_type=getattr(request, "param", "initial")
        )
        await _settle(
            thread, run_id, turn_index, "error",
            metadata=pending_input_stamp(ChatRequest(messages=_said("q0"))),
        )
        yield thread
    await saver.adelete_thread(thread_id)


async def test_a_first_question_that_never_ran_is_edited_in_place(unstarted, db_conn):
    root, turns = await _root_turns(unstarted)
    assert [
        (t.turn_index, t.edit_checkpoint_id, t.regenerate_checkpoint_id)
        for t in turns
    ] == [(0, root, root)]

    request, fork = await _settled(
        unstarted.id, root, 0, regenerate=False, messages=_said("q0 edited")
    )
    run_id, turn_index = await _start(unstarted.id, "q0 edited", fork=fork)
    await _run(
        unstarted.graph, unstarted.id, run_id, turn_index, "q0 edited",
        checkpoint_id=request.checkpoint_id,
    )

    assert turn_index == 0
    assert await _humans(unstarted) == ["q0 edited"]
    assert await _rows(db_conn, "conversation_queries", unstarted.id) == [0]
    assert await _rows(db_conn, "conversation_responses", unstarted.id) == [0]


async def test_a_first_question_that_never_ran_is_regenerated_in_place(
    unstarted, db_conn
):
    """No checkpoint holds the question, so the regenerate sends its stored
    text again from the empty root."""
    root, _ = await _root_turns(unstarted)

    request, fork = await _settled(unstarted.id, root, 0, regenerate=True)
    run_id, turn_index = await _start(unstarted.id, "q0", fork=fork)
    await _run(
        unstarted.graph, unstarted.id, run_id, turn_index, "q0",
        checkpoint_id=request.checkpoint_id,
    )

    assert [m.content for m in request.messages] == ["q0"]
    assert (turn_index, request.checkpoint_id) == (0, root)
    assert await _humans(unstarted) == ["q0"]
    assert await _rows(db_conn, "conversation_queries", unstarted.id) == [0]
    assert await _rows(db_conn, "conversation_responses", unstarted.id) == [0]


@pytest.mark.parametrize("unstarted", ["system"], indirect=True)
async def test_a_regenerated_system_turn_that_never_ran_stays_a_system_turn(
    unstarted, db_conn
):
    """A report-back's message is the system's, not the user's: sent again in
    place of the replay, it must not come back as a user question."""
    from src.server.handlers.chat.request_prep import _resolve_fork

    root, _ = await _root_turns(unstarted)
    request, fork = await _settled(unstarted.id, root, 0, regenerate=True)
    query_type, _ = _resolve_fork(request=request, thread_id=unstarted.id)
    await _start(unstarted.id, "q0", fork=fork, query_type=query_type)

    assert query_type == "system"
    cur = await db_conn.execute(
        "SELECT turn_index, type FROM conversation_queries "
        "WHERE conversation_thread_id = %s",
        (unstarted.id,),
    )
    assert [(r["turn_index"], r["type"]) for r in await cur.fetchall()] == [
        (0, "system")
    ]


@pytest_asyncio.fixture(loop_scope="session")
async def new_thread(seed_workspace, test_db_pool, patched_get_db_connection):
    """A thread with no turn yet, and so no checkpoint."""
    from src.server.app import setup
    from src.server.database.conversation import create_thread

    saver = AsyncPostgresSaver(test_db_pool)
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    with (
        patch(
            "src.server.handlers.checkpoint_handler.get_checkpointer",
            return_value=saver,
        ),
        patch.object(setup, "checkpointer", saver),
    ):
        yield SimpleNamespace(id=thread_id, graph=_graph(saver), saver=saver)
    await saver.adelete_thread(thread_id)


async def test_a_first_send_whose_dispatch_died_is_retried_in_place(
    new_thread, db_conn
):
    """Unstamped, the retry would look for a checkpoint to replay on a thread
    that has none and answer 404."""
    from src.server.database.runs.lifecycle import get_latest_attempt
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
    )

    run_id, _ = await _start_owned(
        new_thread, "q0", ChatRequest(messages=_said("q0"))
    )
    assert await _reconcile(new_thread, run_id) == "error"

    failed = await get_latest_attempt(new_thread.id)
    retry = await input_rerun_request(
        new_thread.id, failed, read_pending_input(failed), workspace_id="ws", body=None
    )
    await new_thread.graph.ainvoke(
        {"messages": [HumanMessage(content=retry.messages[0].content)]},
        {"configurable": {"thread_id": new_thread.id}},
    )

    assert (failed["turn_index"], retry.checkpoint_id) == (0, None)
    assert await _humans(new_thread) == ["q0"]


async def test_a_first_send_a_disconnect_stopped_at_start_is_regenerated_in_place(
    new_thread, db_conn
):
    """START committed and the client went away before it heard back: the
    run is settled stopped from its row, with the stamp START kept there."""
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.admission import _committed_start
    from src.server.services.runs.coordinator import RunCoordinator
    from src.server.utils.checkpoint_helpers import FORK_ROOT_CHECKPOINT_ID

    run_id, _ = await _start_owned(
        new_thread, "q0", ChatRequest(messages=_said("q0"))
    )
    handle = await _committed_start(
        run_id,
        thread_id=new_thread.id,
        workspace_id=None,
        user_id=None,
        is_byok=False,
    )
    coordinator = RunCoordinator()
    coordinator.post_finalize_tail = MagicMock()
    result = await coordinator.fail_open_run(
        handle, "client disconnected during START", status="cancelled"
    )
    assert result.run["status"] == "cancelled"

    await _regenerated_in_place(new_thread, 0, "q0", base=FORK_ROOT_CHECKPOINT_ID)

    assert await _humans(new_thread) == ["q0"]
