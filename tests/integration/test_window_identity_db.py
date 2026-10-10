"""The window on the Postgres saver: the scenario of ``window_harness`` run with
the trim and without it, compared as the unit suite compares it on the
in-memory saver. Postgres is where the trim's ``Overwrite`` has to start a
new snapshot for loading the tip to decode only the window, and where the
history reader walks the thread's turns from table metadata.
"""

from __future__ import annotations

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

import src.server.utils.checkpointer  # noqa: F401  (installs the walk guard)
from tests.unit.middleware.compaction import window_harness as h
from tests.unit.server.services.history.replay_builders import SliceStore, _cache_probe
from tests.unit.server.services.history.test_window import Thread, _page
from src.server.services.history import window as window_module

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _run(saver, monkeypatch, *, window: bool):
    run = await h.run_scenario(saver, window=window)
    thread = Thread(run, saver)
    decoded = await h.decoded_messages(saver, run.graph)
    store = SliceStore()
    page = await _page(monkeypatch, thread, store)
    head = await thread.trimmed() if window else thread.head(1)
    covered = await window_module.runs_held(h.THREAD, head)
    scenario_calls = len(run.model.requests)
    await h.run_edit(run, saver, 3, window=window)
    await saver.adelete_thread(h.THREAD)
    return run, decoded, page, covered, scenario_calls


async def test_the_window_on_postgres(test_db_pool, monkeypatch) -> None:
    saver = AsyncPostgresSaver(test_db_pool)
    _cache_probe(monkeypatch)
    off, off_decoded, off_page, _, calls = await _run(saver, monkeypatch, window=False)
    on, on_decoded, on_page, covered, _ = await _run(saver, monkeypatch, window=True)

    assert h.digests(on.model.requests) == h.digests(off.model.requests)
    assert h.digests(on.summary_model.requests) == h.digests(off.summary_model.requests)
    assert len(on.model.requests) > calls
    assert on_page == off_page
    assert covered
    assert on.asked and all(whole for _, whole, _ in on.asked)
    assert h.transcript(on.store) == h.transcript(off.store)

    scenario = len(h.SCENARIO) - 1
    held_on, held_off = on.held[scenario], off.held[scenario]
    assert held_on[1] > 0 and held_off[1] == 0
    assert on_decoded <= held_on[0] + 2
    assert off_decoded >= held_off[0]


async def test_the_window_reads_the_threads_slices(
    seed_workspace, seed_user, seed_response, patched_get_db_connection
) -> None:
    """The two reads the window makes, against the real table: whether a
    thread holds slices under one slice key, and the rows of those that start
    at given checkpoints."""
    import uuid

    from src.server.database.conversation import ensure_thread_exists
    from src.server.database.conversation import turn_slices as slices_db

    threads = [str(uuid.uuid4()), str(uuid.uuid4())]
    for thread_id in threads:
        await ensure_thread_exists(
            workspace_id=str(seed_workspace["workspace_id"]),
            conversation_thread_id=thread_id,
            user_id=seed_user["user_id"],
            initial_query="q",
        )
    rows = [
        (threads[0], 0, "key"),
        (threads[0], 1, "key"),
        (threads[0], 2, "older-key"),
        (threads[1], 0, "key"),
    ]
    stored = []
    for thread_id, turn, key in rows:
        response_id = str(uuid.uuid4())
        await seed_response(response_id, thread_id, turn)
        stored.append(
            slices_db.TurnRow(
                response_id,
                thread_id,
                slices_db.StoredSlice(
                    f"in-{turn}", f"tail-{turn}", key, "msgpack", f"slice-{turn}".encode()
                ),
            )
        )
    await slices_db.upsert_turn_slices(stored)

    assert await slices_db.has_turn_slices(threads[0], "key")
    assert await slices_db.has_turn_slices(threads[0], "older-key")
    assert not await slices_db.has_turn_slices(threads[1], "older-key")
    assert await slices_db.get_slices_at(threads[0], "key", ["in-1", "in-2"]) == [
        slices_db.StoredSlice("in-1", "tail-1", "key", "msgpack", b"slice-1")
    ]
    assert await slices_db.get_slices_at(threads[0], "key", []) == []
