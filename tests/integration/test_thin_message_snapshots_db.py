"""scripts/ops/thin_message_snapshots.py on threads built through the Postgres saver.

Threads snapshot ``messages`` every second update so a few turns leave several
snapshots, and a migrated thread starts with turns under a plain
``add_messages`` state, the way production's did. Each test writes its own
thread and the conversation row that names its tip.
"""

from __future__ import annotations

import uuid
from typing import Annotated, TypedDict

import psycopg
import pytest
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langgraph.channels.delta import DeltaChannel
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import START, StateGraph
from langgraph.graph.message import add_messages

import src.server.utils.checkpointer  # noqa: F401  (installs the walk guard)
from ptc_agent.agent.state import DeltaAgentState, messages_delta_reducer
from scripts.ops import thin_message_snapshots as thinning
from scripts.ops._delta_loads import (
    Loader,
    Mode,
    Rewrite,
    Status,
    Timeouts,
    Walk,
    messages_digest,
    read_tree,
    rewrite_thread,
    unsupported_saver,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Every snapshot is a candidate: no spacing keep, no recent-turn keep.
_EVERYTHING = thinning.Options(spacing=10**6, keep_recent_turns=0, verify_all=True)


class _PreDeltaState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


class _SnapshotEverySecondUpdate(TypedDict):
    messages: Annotated[
        list[AnyMessage], DeltaChannel(messages_delta_reducer, snapshot_frequency=2)
    ]


def _reply(state) -> dict:
    question = state["messages"][-1]
    return {"messages": [AIMessage(f"re {question.content}", id=f"a-{uuid.uuid4()}")]}


def _graph(schema, saver):
    return StateGraph(schema).add_node("reply", _reply).add_edge(START, "reply").compile(checkpointer=saver)


async def _turns(saver, thread_id: str, count: int, *, start: int = 0, at: str | None = None,
                 pre_delta: int = 0, delta=_SnapshotEverySecondUpdate) -> str:
    """Run ``count`` turns from ``at`` (the newest checkpoint without it) and
    return the checkpoint the last one ended on."""
    tip = at
    for i in range(start, start + count):
        config = {"configurable": {"thread_id": thread_id}}
        if tip is not None:
            config["configurable"]["checkpoint_id"] = tip
        schema = _PreDeltaState if i < pre_delta else delta
        await _graph(schema, saver).ainvoke({"messages": [HumanMessage(f"q{i}")]}, config)
        # Runs are sequential, so the newest checkpoint ends the turn just run.
        newest = [c async for c in saver.alist({"configurable": {"thread_id": thread_id}}, limit=1)]
        tip = newest[0].config["configurable"]["checkpoint_id"]
    assert tip is not None
    return tip


async def _register(test_db_uri: str, workspace_id: str, thread_id: str, tip_id: str) -> None:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        await conn.execute(
            "INSERT INTO conversation_threads (conversation_thread_id, workspace_id, current_status,"
            " thread_index, latest_checkpoint_id) VALUES (%s, %s, 'completed', 0, %s)",
            (thread_id, workspace_id, tip_id),
        )


async def _loads(test_db_uri: str, thread_id: str) -> dict[str, tuple[str, int]]:
    """Every root checkpoint's messages as the reader serves them: digest and walk length."""
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT checkpoint_id FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = ''"
            " ORDER BY checkpoint_id",
            (thread_id,),
        )
        ids = [r[0] for r in await cur.fetchall()]
        loader = Loader(conn)
        out = {}
        for cid in ids:
            messages, walk = await loader.state_messages(thread_id, cid)
            out[cid] = (messages_digest(loader.serde, messages), walk.chain)
        return out


async def _rows(test_db_uri: str, thread_id: str) -> tuple[list, list]:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT checkpoint_id, checkpoint::text FROM checkpoints WHERE thread_id = %s ORDER BY 1",
            (thread_id,),
        )
        checkpoints = await cur.fetchall()
        cur = await conn.execute(
            "SELECT version, type, md5(blob) FROM checkpoint_blobs"
            " WHERE thread_id = %s AND channel = 'messages' ORDER BY 1",
            (thread_id,),
        )
        return checkpoints, await cur.fetchall()


async def _new_thread(test_db_pool, test_db_uri, workspace, *, turns: int, pre_delta: int = 0,
                      delta=_SnapshotEverySecondUpdate) -> tuple[str, str]:
    thread_id = str(uuid.uuid4())
    tip = await _turns(AsyncPostgresSaver(test_db_pool), thread_id, turns, pre_delta=pre_delta, delta=delta)
    await _register(test_db_uri, str(workspace["workspace_id"]), thread_id, tip)
    return thread_id, tip


@pytest.mark.parametrize("pre_delta", [0, 3], ids=["delta", "migrated"])
async def test_thinning_keeps_every_load_the_same(test_db_pool, test_db_uri, seed_workspace, pre_delta):
    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=8, pre_delta=pre_delta)
    before = await _loads(test_db_uri, thread_id)
    _, blobs_before = await _rows(test_db_uri, thread_id)

    out = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.APPLY, _EVERYTHING)

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    assert out.report.dropped > 0 and out.report.reclaimed > 0
    assert len(out.checks) == len(before) and all(c.match for c in out.checks)
    assert out.report.anchors_match and out.committed_match
    _, blobs_after = await _rows(test_db_uri, thread_id)
    assert len(blobs_after) == len(blobs_before) - out.report.dropped + out.report.seeded
    after = await _loads(test_db_uri, thread_id)
    assert {c: d for c, (d, _) in after.items()} == {c: d for c, (d, _) in before.items()}
    assert after[tip][1] <= before[tip][1]
    # Idempotent: a second pass finds nothing it may drop.
    again = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.APPLY, _EVERYTHING)
    assert again.status is Status.NOTHING, (again.status, again.reason)


async def test_a_rehearsal_and_a_dry_run_leave_the_thread_as_it_was(test_db_pool, test_db_uri, seed_workspace):
    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=6, pre_delta=2)
    rows = await _rows(test_db_uri, thread_id)

    dry = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.DRY_RUN, _EVERYTHING)
    rehearsed = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.REHEARSE, _EVERYTHING)

    assert dry.status is Status.PLANNED and dry.report.dropped > 0 and not dry.checks
    assert rehearsed.status is Status.REHEARSED and rehearsed.report.dropped_bytes > 0
    assert rehearsed.checks and all(c.match for c in rehearsed.checks)
    assert await _rows(test_db_uri, thread_id) == rows


async def test_a_fork_tip_keeps_the_snapshot_it_loads(test_db_pool, test_db_uri, seed_workspace):
    saver = AsyncPostgresSaver(test_db_pool)
    thread_id = str(uuid.uuid4())
    turn_ends = []
    for i in range(6):
        turn_ends.append(await _turns(saver, thread_id, 1, start=i, at=turn_ends[-1] if turn_ends else None))
    fork_tip = await _turns(saver, thread_id, 1, start=100, at=turn_ends[2])
    tip = await _turns(saver, thread_id, 2, start=6, at=turn_ends[-1])
    await _register(test_db_uri, str(seed_workspace["workspace_id"]), thread_id, tip)
    before = await _loads(test_db_uri, thread_id)

    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        tree, sizes = await read_tree(conn, thread_id)
    flags = {c for c, n in tree.nodes.items() if n.flagged}
    seed, _ = tree.walk(fork_tip, flags)
    fork_version = (
        tree.nodes[fork_tip].version
        if tree.nodes[fork_tip].version in sizes
        else tree.nodes[seed].version
    )
    assert fork_version in sizes

    out = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.APPLY, _EVERYTHING)

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    assert out.report.dropped > 0 and out.report.kept["branch tip"] >= 1
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        _, sizes_after = await read_tree(conn, thread_id)
    assert fork_version in sizes_after
    after = await _loads(test_db_uri, thread_id)
    assert after[fork_tip] == before[fork_tip]
    assert {c: d for c, (d, _) in after.items()} == {c: d for c, (d, _) in before.items()}


async def test_a_thread_whose_turns_have_no_stored_lines_is_not_a_candidate(
    test_db_pool, test_db_uri, seed_workspace, seed_response, patched_get_db_connection
):
    from src.server.database.conversation.queries import create_query

    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=2)
    for turn in range(2):
        await create_query(str(uuid.uuid4()), thread_id, turn, f"q{turn}", "initial" if turn == 0 else "follow_up")
        await seed_response(str(uuid.uuid4()), thread_id, turn)

    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        candidacy = await thinning.preconditions(conn, thread_id, Loader(conn))
        unchecked = await thinning.preconditions(conn, thread_id, Loader(conn), coverage=False)

    assert candidacy == thinning.NotCandidate("turns without current stored lines", 2)
    assert unchecked == thinning.Candidate(tip)  # --keep all makes nothing slower


async def test_a_thread_with_a_run_in_progress_is_not_a_candidate(
    test_db_pool, test_db_uri, seed_workspace, seed_response, patched_get_db_connection
):
    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=2)
    await seed_response(str(uuid.uuid4()), thread_id, 2, status="in_progress")

    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        candidacy = await thinning.preconditions(conn, thread_id, Loader(conn), coverage=False)
    out = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.APPLY, _EVERYTHING)

    assert candidacy == thinning.NotCandidate("a run is in progress")
    assert (out.status, out.reason) == (Status.SKIPPED, "a run is in progress")


async def test_an_unverified_saver_release_is_refused(
    test_db_pool, test_db_uri, seed_workspace, monkeypatch
):
    """On the per-thread path too, which a caller reaches without ``run()``."""
    import importlib.metadata

    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=8)
    rows = await _rows(test_db_uri, thread_id)
    monkeypatch.setattr(importlib.metadata, "version", lambda dist: "0.0.1")

    out = await thinning.thin_thread(test_db_uri, thread_id, tip, Mode.APPLY, _EVERYTHING)

    assert "not a verified release" in (unsupported_saver() or "")
    assert out.status is Status.ERROR and "not a verified release" in out.reason
    assert await _rows(test_db_uri, thread_id) == rows


# --------------------------------------------------------------------------
# --keep all: lists stored before DeltaChannel, at production's cadence
# --------------------------------------------------------------------------

_KEEP_ALL = thinning.Options(keep_all=True, verify_all=True)


async def _load(test_db_uri: str, thread_id: str, checkpoint_id: str) -> tuple[list, Walk]:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        return await Loader(conn).state_messages(thread_id, checkpoint_id)


async def _flagged(test_db_uri: str, thread_id: str) -> set[str]:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT checkpoint_id FROM checkpoints WHERE thread_id = %s"
            " AND checkpoint -> 'channel_values' -> 'messages' IS NOT NULL",
            (thread_id,),
        )
        return {r[0] for r in await cur.fetchall()}


async def _listed_for_keep_all(test_db_uri: str, thread_id: str) -> bool:
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        cur = await conn.execute(thinning._UNFLAGGED_LISTS_SQL, (0,))
        return thread_id in {r[0] for r in await cur.fetchall()}


def _continues(before: list, after: list) -> bool:
    """``after`` is ``before`` unchanged, ids and order included, then one new turn."""
    serde = JsonPlusSerializer()
    prefix = after[: len(before)]
    return (
        [m.id for m in prefix] == [m.id for m in before]
        and messages_digest(serde, prefix) == messages_digest(serde, before)
        and [m.type for m in after[len(before) :]] == ["human", "ai"]
    )


async def _migrated(test_db_pool, test_db_uri, workspace, *, pre_delta: int, delta: int):
    """``pre_delta`` turns before DeltaChannel, then ``delta`` at production's
    cadence; returns the thread and the checkpoint each turn ended on."""
    saver = AsyncPostgresSaver(test_db_pool)
    thread_id = str(uuid.uuid4())
    ends: list[str] = []
    for i in range(pre_delta + delta):
        at = ends[-1] if ends else None
        ends.append(await _turns(saver, thread_id, 1, start=i, at=at, pre_delta=pre_delta,
                                 delta=DeltaAgentState))
    await _register(test_db_uri, str(workspace["workspace_id"]), thread_id, ends[-1])
    return thread_id, ends


async def _keep_all(test_db_uri: str, thread_id: str, tip: str, mode: Mode = Mode.APPLY):
    return await thinning.thin_thread(test_db_uri, thread_id, tip, mode, _KEEP_ALL)


async def test_keep_all_flags_the_lists_of_a_thread_that_has_not_crossed(
    test_db_pool, test_db_uri, seed_workspace
):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=3, delta=0)
    before = await _loads(test_db_uri, thread_id)
    _, blobs_before = await _rows(test_db_uri, thread_id)

    out = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    report = out.report
    assert (report.dropped, report.seeded, report.promoted) == (0, 0, report.blobs)
    assert all(c.match for c in out.checks) and out.committed_match
    assert set(ends) <= await _flagged(test_db_uri, thread_id)
    assert await _loads(test_db_uri, thread_id) == before
    assert (await _rows(test_db_uri, thread_id))[1] == blobs_before  # no blob written
    assert not await _listed_for_keep_all(test_db_uri, thread_id)


async def test_after_keep_all_the_first_delta_turn_continues_the_stored_list(
    test_db_pool, test_db_uri, seed_workspace
):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=3, delta=0)
    await _keep_all(test_db_uri, thread_id, ends[-1])
    stored, _ = await _load(test_db_uri, thread_id, ends[-1])

    tip = await _turns(AsyncPostgresSaver(test_db_pool), thread_id, 1, start=3, delta=DeltaAgentState)

    after, walk = await _load(test_db_uri, thread_id, tip)
    assert _continues(stored, after)
    assert walk.seed and walk.writes == 2  # seeded at the list: the input and the reply


async def test_after_keep_all_a_branch_forked_at_an_older_list_continues_it(
    test_db_pool, test_db_uri, seed_workspace
):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=3, delta=0)
    await _keep_all(test_db_uri, thread_id, ends[-1])
    stored, _ = await _load(test_db_uri, thread_id, ends[0])

    tip = await _turns(
        AsyncPostgresSaver(test_db_pool), thread_id, 1, start=100, at=ends[0], delta=DeltaAgentState
    )

    after, walk = await _load(test_db_uri, thread_id, tip)
    assert _continues(stored, after)
    assert walk.seed and walk.writes == 2


async def test_keep_all_seeds_a_crossed_thread_with_the_value_it_loads_today(
    test_db_pool, test_db_uri, seed_workspace
):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=2, delta=1)
    before = await _loads(test_db_uri, thread_id)

    out = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    assert (out.report.dropped, out.report.seeded) == (0, 1)
    assert all(c.match for c in out.checks) and out.committed_match
    tip = out.committed
    assert tip is not None and tip.before.pages and not tip.before.seed  # walked to the root
    assert tip.after is not None and tip.after.chain < tip.before.chain
    assert {c: d for c, (d, _) in (await _loads(test_db_uri, thread_id)).items()} == {
        c: d for c, (d, _) in before.items()
    }


async def test_keep_all_flags_the_lists_a_crossed_walk_passed_over(
    test_db_pool, test_db_uri, seed_workspace
):
    """A list a delta walk passes over is flagged too, or a branch off it
    would replay the old writes from the root."""
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=2, delta=1)

    out = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    assert ends[0] in await _flagged(test_db_uri, thread_id)
    stored, _ = await _load(test_db_uri, thread_id, ends[0])
    tip = await _turns(
        AsyncPostgresSaver(test_db_pool), thread_id, 1, start=100, at=ends[0], delta=DeltaAgentState
    )
    after, walk = await _load(test_db_uri, thread_id, tip)
    assert _continues(stored, after)
    assert walk.seed and walk.writes == 2


@pytest.mark.parametrize(("pre_delta", "delta"), [(3, 0), (2, 1)], ids=["not crossed", "crossed"])
async def test_keep_all_dry_run_and_rehearsal_leave_the_thread_as_it_was(
    test_db_pool, test_db_uri, seed_workspace, pre_delta, delta
):
    thread_id, ends = await _migrated(
        test_db_pool, test_db_uri, seed_workspace, pre_delta=pre_delta, delta=delta
    )
    rows = await _rows(test_db_uri, thread_id)

    dry = await _keep_all(test_db_uri, thread_id, ends[-1], Mode.DRY_RUN)
    rehearsed = await _keep_all(test_db_uri, thread_id, ends[-1], Mode.REHEARSE)

    assert dry.status is Status.PLANNED and dry.report.promoted and not dry.checks
    assert rehearsed.status is Status.REHEARSED and all(c.match for c in rehearsed.checks)
    assert await _rows(test_db_uri, thread_id) == rows
    assert await _listed_for_keep_all(test_db_uri, thread_id)


@pytest.mark.parametrize(("pre_delta", "delta"), [(3, 0), (2, 1)], ids=["not crossed", "crossed"])
async def test_keep_all_rerun_is_a_no_op(test_db_pool, test_db_uri, seed_workspace, pre_delta, delta):
    thread_id, ends = await _migrated(
        test_db_pool, test_db_uri, seed_workspace, pre_delta=pre_delta, delta=delta
    )
    assert (await _keep_all(test_db_uri, thread_id, ends[-1])).status is Status.WRITTEN
    rows = await _rows(test_db_uri, thread_id)

    again = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert again.status is Status.NOTHING, (again.status, again.reason)
    assert await _rows(test_db_uri, thread_id) == rows
    assert not await _listed_for_keep_all(test_db_uri, thread_id)


async def test_keep_all_skips_a_checkpoint_that_changes_before_the_write(
    test_db_pool, test_db_uri, seed_workspace, monkeypatch
):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=1, delta=1)
    rows = await _rows(test_db_uri, thread_id)
    write = thinning._write

    async def a_turn_rewrites_a_list_first(conn, *args):
        async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as other:
            await other.execute(
                "UPDATE checkpoints SET metadata = metadata WHERE thread_id = %s AND checkpoint_id = %s",
                (thread_id, ends[0]),
            )
        return await write(conn, *args)

    monkeypatch.setattr(thinning, "_write", a_turn_rewrites_a_list_first)
    out = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert (out.status, out.reason) == (Status.SKIPPED, "a checkpoint changed concurrently")
    assert await _rows(test_db_uri, thread_id) == rows


async def test_keep_all_never_seeds_over_a_stored_list(test_db_pool, test_db_uri, seed_workspace):
    """A list stored where a crossing walk would be seeded is flagged as one."""
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=1, delta=1)
    async with await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as conn:
        tree, sizes = await read_tree(conn, thread_id)
        first_delta = next(
            c for c in tree.order if tree.nodes[c].version and tree.nodes[c].version not in sizes
        )
        await conn.execute(
            "INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)"
            " VALUES (%s, '', 'messages', %s, 'msgpack', '\\x90'::bytea)",
            (thread_id, tree.nodes[first_delta].version),
        )
    before = await _loads(test_db_uri, thread_id)
    _, blobs = await _rows(test_db_uri, thread_id)

    out = await _keep_all(test_db_uri, thread_id, ends[-1])

    assert out.status is Status.WRITTEN, (out.status, out.reason)
    assert first_delta in await _flagged(test_db_uri, thread_id)
    _, blobs_after = await _rows(test_db_uri, thread_id)
    assert set(blobs) <= set(blobs_after) and len(blobs_after) == len(blobs) + out.report.seeded
    assert {c: d for c, (d, _) in (await _loads(test_db_uri, thread_id)).items()} == {
        c: d for c, (d, _) in before.items()
    }


async def test_keep_all_leaves_a_thread_written_after_deltachannel(
    test_db_pool, test_db_uri, seed_workspace
):
    thread_id, tip = await _new_thread(test_db_pool, test_db_uri, seed_workspace, turns=4)

    out = await _keep_all(test_db_uri, thread_id, tip)

    assert out.status is Status.NOTHING, (out.status, out.reason)
    assert not await _listed_for_keep_all(test_db_uri, thread_id)


# --------------------------------------------------------------------------
# The harness: nothing commits unverified
# --------------------------------------------------------------------------


async def test_a_thread_transaction_may_sit_idle_no_longer_than_a_statement(test_db_uri):
    seen = {}

    async def plan(conn, loader):
        cur = await conn.execute(
            "SELECT current_setting('idle_in_transaction_session_timeout'),"
            " current_setting('statement_timeout')"
        )
        seen["idle"], seen["statement"] = await cur.fetchone()
        return Rewrite(None)

    out = await rewrite_thread(test_db_uri, "t", Mode.DRY_RUN, Timeouts(statement_s=7), plan)

    assert out.status is Status.NOTHING
    assert seen == {"idle": "7s", "statement": "7s"}


async def _rewrite_tip(test_db_uri, thread_id, tip, statement, verify=None):
    async def plan(conn, loader):
        async def write():
            await conn.execute(statement, (thread_id,))

        return Rewrite(None, [tip], tip, write, verify)

    return await rewrite_thread(test_db_uri, thread_id, Mode.APPLY, Timeouts(), plan)


async def test_a_write_that_changes_a_load_is_rolled_back(test_db_pool, test_db_uri, seed_workspace):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=2, delta=0)
    rows = await _rows(test_db_uri, thread_id)

    out = await _rewrite_tip(
        test_db_uri, thread_id, ends[-1],
        "UPDATE checkpoint_blobs SET type = 'msgpack', blob = '\\x90'::bytea"
        " WHERE thread_id = %s AND channel = 'messages'",  # every list now loads empty
    )

    assert out.status is Status.MISMATCH and out.failed
    assert [c.match for c in out.checks] == [False]
    assert await _rows(test_db_uri, thread_id) == rows


async def test_a_write_its_planner_rejects_is_rolled_back(test_db_pool, test_db_uri, seed_workspace):
    thread_id, ends = await _migrated(test_db_pool, test_db_uri, seed_workspace, pre_delta=2, delta=0)
    rows = await _rows(test_db_uri, thread_id)

    async def reject(checks):
        assert all(c.match for c in checks.values())
        return "rejected"

    out = await _rewrite_tip(
        test_db_uri, thread_id, ends[-1],
        "UPDATE checkpoints SET metadata = metadata || '{\"x\": 1}' WHERE thread_id = %s",
        reject,
    )

    assert (out.status, out.reason) == (Status.MISMATCH, "rolled back: rejected")
    assert await _rows(test_db_uri, thread_id) == rows
