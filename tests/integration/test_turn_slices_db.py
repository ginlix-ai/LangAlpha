"""Batched slice writes against real Postgres: one statement per batch, a
row whose parent is gone dropped alone, and lines kept off a successor
slice."""

from __future__ import annotations

import uuid

import pytest

from src.server.database.conversation.turn_slices import (
    RunRow,
    StoredSlice,
    TurnRow,
    TurnUpdate,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _turn_row(response_id, thread_id, turn_index, *, tail, lines=None, claims=None):
    return TurnRow(
        response_id,
        thread_id,
        StoredSlice(
            input_checkpoint_id=f"in-{turn_index}",
            tail_checkpoint_id=tail,
            slice_key="k",
            codec="msgpack",
            data=b"\x80",
            lines_key="lk" if lines is not None else None,
            lines=lines,
            claims=claims,
        ),
    )


async def _thread(seed_workspace, seed_response, turns):
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    response_ids = []
    for turn_index in range(turns):
        response_id = str(uuid.uuid4())
        await seed_response(response_id, thread_id, turn_index)
        response_ids.append(response_id)
    return thread_id, response_ids


async def test_a_batch_stores_every_row_but_an_orphan(
    seed_workspace, seed_response, patched_get_db_connection
):
    from src.server.database.conversation import turn_slices as slices_db

    thread_id, (r0, r1) = await _thread(seed_workspace, seed_response, 2)
    gone = str(uuid.uuid4())

    await slices_db.upsert_turn_slices(
        [
            _turn_row(r0, thread_id, 0, tail="t0", lines="a\t\t{}", claims=["c0"]),
            _turn_row(gone, thread_id, 1, tail="tx"),
            _turn_row(r1, thread_id, 1, tail="t1"),
        ]
    )

    rows = await slices_db.get_turn_slices([r0, r1, gone])
    assert sorted(rows) == sorted([r0, r1])
    assert rows[r0].lines == "a\t\t{}"
    assert rows[r0].claims == ["c0"]
    assert rows[r1].data == b"\x80"
    assert rows[r1].lines is None


async def test_an_update_lands_only_on_the_slice_it_was_projected_from(
    seed_workspace, seed_response, patched_get_db_connection
):
    from src.server.database.conversation import turn_slices as slices_db

    thread_id, (r0, r1) = await _thread(seed_workspace, seed_response, 2)
    await slices_db.upsert_turn_slices(
        [
            _turn_row(r0, thread_id, 0, tail="t0", claims=["c0"]),
            _turn_row(r1, thread_id, 1, tail="t1-new"),
        ]
    )

    await slices_db.update_turns(
        [
            TurnUpdate(r0, "t0", "lk", "x\t\t{}", None),
            TurnUpdate(r1, "t1-old", "lk", "stale\t\t{}", ["c9"]),
        ]
    )

    rows = await slices_db.get_turn_lines([r0, r1])
    assert rows[r0].lines == "x\t\t{}"
    assert rows[r0].claims == ["c0"]
    assert rows[r0].data is None
    assert rows[r1].lines is None
    assert rows[r1].claims is None

    # A slice cut again replaces its claims rather than keeping the old ones.
    await slices_db.upsert_turn_slices([_turn_row(r0, thread_id, 0, tail="t0b")])
    assert (await slices_db.get_turn_lines([r0]))[r0].claims is None


async def test_run_slices_store_in_one_batch_and_a_rewrite_moves_the_tail(
    seed_workspace, seed_response, patched_get_db_connection
):
    from src.server.database.conversation import turn_slices as slices_db

    thread_id, _ = await _thread(seed_workspace, seed_response, 0)

    def run(task_id, ordinal, tail):
        return RunRow(
            thread_id,
            task_id,
            None,
            StoredSlice(f"{task_id}:in-{ordinal}", tail, "k", "msgpack", b"\x80"),
        )

    await slices_db.upsert_run_slices(
        [run("b", 0, "b0"), run("a", 1, "a1"), run("a", 0, "a0")]
    )
    await slices_db.upsert_run_slices([run("a", 1, "a1-moved")])

    rows = await slices_db.get_run_slices(thread_id, ["a", "b"])
    assert sorted((r.slice.input_checkpoint_id, r.slice.tail_checkpoint_id) for r in rows) == [
        ("a:in-0", "a0"),
        ("a:in-1", "a1-moved"),
        ("b:in-0", "b0"),
    ]
