"""What replay stores, and when a stored row is trusted.

A stored run slice is trusted as final forever, stored lines as the turn's
replay while their key matches, and stored claims as each legacy launch's
run: each may only be written from evidence that outlives the read.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history import replay, slices, task_streams
from src.server.services.history.slices import SpanDelta
from src.server.services.history.replay import (
    build_checkpoint_replay_items,
    build_replay_page,
    run_lane,
)
from src.server.services.history.replay import cold
from src.server.services.history.replay import lines as replay_lines
from src.server.services.history.replay.turn import Inputs
from tests.unit.server.services.history.replay_builders import (
    thread_rows,
    THREAD,
    ThreadHistory,
    _mock_reader,
    _query,
    _response,
    _slice_store,
    _turn,
)

pytestmark = pytest.mark.asyncio


def _launch(ordinal, prompt, *, action="init", run_id=None, task_id="tsk1"):
    artifact = {
        "task_id": task_id,
        "action": action,
        "description": "d",
        "prompt": prompt,
    }
    if run_id:
        artifact["task_run_id"] = run_id
    return [
        AIMessage(
            content="",
            id=f"ai-{ordinal}",
            tool_calls=[{"name": "Task", "args": {}, "id": f"tc-{ordinal}"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id=f"tc-{ordinal}",
            name="Task",
            id=f"tm-{ordinal}",
            additional_kwargs={"task_artifact": artifact},
        ),
    ]


def _plain(ordinal):
    return [AIMessage(content=f"answer {ordinal}", id=f"ai-{ordinal}")]


def _turns(messages_by_turn):
    out = []
    for ordinal, messages in enumerate(messages_by_turn):
        turn = _turn(ordinal, messages)
        turn.anchor.tail_checkpoint_id = f"tail-{ordinal}"
        out.append(turn)
    return out


def _rows(n):
    return [_query(i) for i in range(n)], {i: _response(i) for i in range(n)}


def _seal_evidence(
    monkeypatch, *, probe=None, stream=task_streams.STREAM_SEALED, ledger=None
):
    """Liveness probe, task stream states and ledger rows for the lane."""

    async def streams(thread_id, task_ids):
        return dict.fromkeys(task_ids, stream)

    monkeypatch.setattr(
        run_lane, "resolve_task_details", probe or AsyncMock(return_value={})
    )
    monkeypatch.setattr(task_streams, "task_stream_states", streams)
    monkeypatch.setattr(
        run_lane.sr_db,
        "list_runs_for_thread",
        ledger if ledger is not None else AsyncMock(return_value=[]),
    )


def _task_text(items):
    return [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk"
        and i["data"].get("agent") == "task:tsk1"
        and i["data"].get("role") == "assistant"
    ]


async def test_a_run_final_only_by_a_failed_probe_is_never_stored(monkeypatch):
    """The liveness probe fails open, so a legacy run still writing reads as
    finished. Its slice serves that read only: stored, it would be trusted
    as final ever after, and the turn's lines sealed with the run cut short.
    """
    store = _slice_store(monkeypatch)
    _seal_evidence(
        monkeypatch,
        probe=AsyncMock(side_effect=RuntimeError("probe down")),
        stream=task_streams.STREAM_OPEN,
    )
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=_turns([_launch(0, "go")])),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="partial", id="sub-ai-1"),
        ],
    )
    queries, responses = _rows(1)

    first = await build_checkpoint_replay_items(thread_rows(queries, responses))
    assert _task_text(first) == ["partial"]
    assert store.runs == {}
    assert store.lined == []

    # The run wrote on; the next read cuts it again rather than trusting a
    # copy from the first.
    reader.aget_task_history.return_value.messages.append(
        AIMessage(content="more", id="sub-ai-2")
    )
    second = await build_checkpoint_replay_items(thread_rows(queries, responses))
    assert _task_text(second) == ["partial", "more"]
    assert store.runs == {}
    assert store.lined == []

    # The stream's end sentinel settles it without the probe.
    _seal_evidence(monkeypatch, probe=AsyncMock(side_effect=RuntimeError("probe down")))
    third = await build_checkpoint_replay_items(thread_rows(queries, responses))
    assert _task_text(third) == ["partial", "more"]
    assert [r["tail_checkpoint_id"] for r in store.runs.values()] == ["tsk1:tail-0"]
    assert store.lined == ["tail-0"]


async def test_nothing_resolved_without_the_ledger_is_stored(monkeypatch):
    """With the ledger unreadable every run's end rests on the probe: no run
    slice, no claims and no lines of a launching turn are stored. A turn
    that launched nothing never asked the ledger and stores as usual."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch, ledger=AsyncMock(side_effect=RuntimeError("down")))
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=_turns([_launch(0, "go"), _plain(1)])),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    queries, responses = _rows(2)

    items = await build_checkpoint_replay_items(thread_rows(queries, responses))

    assert _task_text(items) == ["done"]
    assert store.runs == {}
    assert store.turns["resp-0"]["lines"] is None
    assert store.turns["resp-0"]["run_claims"] is None
    assert store.lined == ["tail-1"]


async def test_a_stored_run_the_walk_ends_elsewhere_is_cut_again(monkeypatch):
    """A run that wrote on after its slice was stored no longer ends at the
    stored tail; once the task is walked, that slice is dropped and the run
    cut again."""
    store = _slice_store(monkeypatch)
    _seal_evidence(
        monkeypatch,
        ledger=AsyncMock(
            return_value=[
                {"task_run_id": "run-1", "status": "completed", "started_at": None},
                {"task_run_id": "run-2", "status": "completed", "started_at": None},
            ]
        ),
    )
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns(
                [
                    _launch(0, "first", run_id="run-1"),
                    _launch(1, "second", action="resume", run_id="run-2"),
                ]
            ),
        ),
        task_messages=[
            HumanMessage(content="first", id="sub-h-1"),
            AIMessage(content="first done", id="sub-ai-1"),
            HumanMessage(content="second", id="sub-h-2"),
            AIMessage(content="second done", id="sub-ai-2"),
        ],
    )
    reader.task_run_stamps = AsyncMock(return_value=["run-1", "run-2"])
    codec, data = slices.encode(
        reader.serde,
        SpanDelta(
            messages=[
                HumanMessage(content="first", id="sub-h-1"),
                AIMessage(content="cut short", id="sub-ai-0"),
            ]
        ),
    )
    store.runs[("tsk1", "tsk1:in-0")] = {
        "task_id": "tsk1",
        "input_checkpoint_id": "tsk1:in-0",
        "task_run_id": "run-1",
        "tail_checkpoint_id": "tsk1:tail-before",
        "slice_key": slices.SLICE_KEY,
        "slice_codec": codec,
        "slice": data,
    }
    queries, responses = _rows(2)

    items = await build_checkpoint_replay_items(thread_rows(queries, responses))

    assert _task_text(items) == ["first done", "second done"]
    assert store.runs[("tsk1", "tsk1:in-0")]["tail_checkpoint_id"] == "tsk1:tail-0"


async def test_the_claims_pass_stores_the_settled_runs_outside_the_page(
    monkeypatch,
):
    """The pass reads every task's seal evidence, not only the evidence of
    the batch that needed it, so a sealed run it claims for a turn outside
    the page is stored rather than cut again by the read that pages to it."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns([_launch(0, "go"), _launch(1, "go", task_id="tsk2")]),
        ),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    queries, responses = _rows(2)

    page = await build_replay_page(thread_rows(queries, responses), limit=1)

    assert page.first_turn_index == 1
    assert sorted(store.runs) == [("tsk1", "tsk1:in-0"), ("tsk2", "tsk2:in-0")]
    assert store.turns["resp-0"]["run_claims"] == ["tsk1:in-0"]
    assert store.lined == ["tail-1"]


async def test_a_claim_on_a_run_the_walk_no_longer_holds_is_unavailable(
    monkeypatch,
):
    """Stored claims name the run's input checkpoint. When the walk no
    longer has that run and nothing stored its slice, its transcript cannot
    be read, and the read says so rather than cut a slice it cannot bound."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    turn = _turns([_launch(0, "go")])[0]
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[turn]),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    anchors, _tip = await reader.aget_turn_anchors(THREAD)
    codec, data = slices.encode(reader.serde, turn)
    store.turns["resp-0"] = {
        "response_id": "resp-0",
        "input_checkpoint_id": anchors[0].input_checkpoint_id,
        "tail_checkpoint_id": anchors[0].tail_checkpoint_id,
        "slice_key": slices.SLICE_KEY,
        "slice_codec": codec,
        "slice": data,
        "lines_key": None,
        "lines": None,
        "run_claims": ["tsk1:in-gone"],
    }
    queries, responses = _rows(1)

    with pytest.raises(replay.CheckpointReplayUnavailable):
        await build_checkpoint_replay_items(thread_rows(queries, responses))
    reader.aget_run_slices.assert_not_awaited()
    assert store.runs == {}


async def test_claims_count_only_beside_their_slice(monkeypatch):
    """Claims were placed against the slice stored beside them; once the
    branch no longer agrees with that slice, neither are they trusted."""
    store = _slice_store(monkeypatch)
    turn = _turns([_launch(0, "go")])[0]
    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    anchors, _tip = await reader.aget_turn_anchors(THREAD)
    queries, responses = _rows(1)
    inputs = Inputs.of(thread_rows(queries, responses))
    codec, data = slices.encode(reader.serde, turn)
    store.turns["resp-0"] = {
        "response_id": "resp-0",
        "input_checkpoint_id": anchors[0].input_checkpoint_id,
        "tail_checkpoint_id": anchors[0].tail_checkpoint_id,
        "slice_key": slices.SLICE_KEY,
        "slice_codec": codec,
        "slice": data,
        "lines_key": None,
        "lines": None,
        "run_claims": ["tsk1:in-0"],
    }

    _turns_read, claims, fresh = await cold.load_turns(
        reader, inputs, [(0, anchors[0])]
    )
    assert claims == {0: ["tsk1:in-0"]}
    assert fresh == set()

    store.turns["resp-0"]["tail_checkpoint_id"] = "tail-moved"
    _turns_read, claims, fresh = await cold.load_turns(
        reader, inputs, [(0, anchors[0])]
    )
    assert claims == {0: None}
    assert fresh == {0}


async def test_undecodable_stored_rows_are_rebuilt(monkeypatch):
    """A row that no longer decodes costs a rebuild of that row, never the
    read: it is treated as stale and stored over."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=_turns([_launch(0, "go"), _plain(1)])),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    queries, responses = _rows(2)
    first = await build_checkpoint_replay_items(thread_rows(queries, responses))

    store.turns["resp-0"]["lines"] = "no tabs here"
    store.turns["resp-0"]["slice"] = b"\x00not a slice"
    store.turns["resp-1"]["lines"] = "no tabs here either"
    (run_key,) = store.runs
    store.runs[run_key]["slice"] = b"\x00not a run slice"

    second = await build_checkpoint_replay_items(thread_rows(queries, responses))

    assert second == first
    for row in store.turns.values():
        replay_lines.decode(row["lines"])
    slices.decode(
        reader.serde, store.turns["resp-0"]["slice_codec"], store.turns["resp-0"]["slice"]
    )
    slices.decode(
        reader.serde, store.runs[run_key]["slice_codec"], store.runs[run_key]["slice"]
    )


async def test_a_cold_open_writes_each_turn_once(monkeypatch):
    """The claims pass reads every turn; the ones it cuts from checkpoints are
    projected from memory and written once, slice and lines together, a
    statement per batch. A pass that starts mid-thread waits for the stores
    before it, and the request returns only once every store has landed."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    n = 20
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns(
                [_plain(i) for i in range(n - 2)] + [_launch(18, "go"), _plain(19)]
            ),
        ),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    queries, responses = _rows(n)

    await build_checkpoint_replay_items(thread_rows(queries, responses))

    assert sorted(store.written) == sorted(f"resp-{i}" for i in range(n))
    assert [kind for kind, _ in store.statements] == [
        "upsert_turn_slices",
        "upsert_run_slices",
        "upsert_turn_slices",
    ]
    assert sorted(store.lined) == sorted(f"tail-{i}" for i in range(n))
    assert store.turns["resp-18"]["run_claims"] == ["tsk1:in-0"]


async def test_a_paged_cold_open_stores_the_pass_once(monkeypatch):
    """A page's claims pass cuts turns outside the page too: those store
    their slices and claims once, the page's turns their lines as well."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns(
                [
                    _launch(0, "go"),
                    _plain(1),
                    _plain(2),
                    _launch(3, "again", action="resume"),
                ]
            ),
        ),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
            HumanMessage(content="again", id="sub-h-2"),
            AIMessage(content="done again", id="sub-ai-2"),
        ],
    )
    queries, responses = _rows(4)

    page = await build_replay_page(thread_rows(queries, responses), limit=2)

    assert page.first_turn_index == 2
    assert sorted(store.written) == ["resp-0", "resp-1", "resp-2", "resp-3"]
    assert store.turns["resp-0"]["run_claims"] == ["tsk1:in-0"]
    assert store.turns["resp-3"]["run_claims"] == ["tsk1:in-1"]
    assert sorted(store.lined) == ["tail-2", "tail-3"]
    assert sorted(store.runs) == [("tsk1", "tsk1:in-0"), ("tsk1", "tsk1:in-1")]

    # The next page reads the pass's work back: no checkpoint is cut again.
    before = len(store.statements)
    cuts = reader.aget_turn_slices.await_count, reader.aget_run_slices.await_count
    older = await build_replay_page(
        thread_rows(queries, responses),
        before_turn=2,
        limit=2,
    )
    assert older.first_turn_index == 0
    assert (
        reader.aget_turn_slices.await_count,
        reader.aget_run_slices.await_count,
    ) == cuts
    assert [kind for kind, _ in store.statements[before:]] == ["update_turns"]


async def test_an_uncached_replay_reads_and_writes_no_stored_row(monkeypatch):
    """``cache=False`` cuts every turn and run from checkpoints, though
    current rows are stored, and stores nothing: what a parity run compares
    is what the code projects today."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    reader = _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns([_launch(0, "go"), _plain(1)]),
        ),
        task_messages=[
            HumanMessage(content="go", id="sub-h-1"),
            AIMessage(content="done", id="sub-ai-1"),
        ],
    )
    rows = thread_rows(*_rows(2))
    cached = await build_checkpoint_replay_items(rows)
    assert store.lined and store.runs

    for name in ("get_turn_lines", "get_turn_slices", "get_run_slices"):
        monkeypatch.setattr(
            cold.slices_db, name, AsyncMock(side_effect=AssertionError(name))
        )
    before = len(store.statements)
    cuts = reader.aget_turn_slices.await_count, reader.aget_run_slices.await_count

    assert await build_checkpoint_replay_items(rows, cache=False) == cached
    assert len(store.statements) == before
    assert reader.aget_turn_slices.await_count > cuts[0]
    assert reader.aget_run_slices.await_count > cuts[1]


async def test_a_change_to_the_cut_moves_the_slice_key(monkeypatch, tmp_path):
    """The slice key digests the code that cuts a slice, so a fix there
    rebuilds the slices stored before it with no manual bump."""
    copies = []
    for source in slices.slice_sources():
        copy = tmp_path / source.relative_to(slices._SRC)
        copy.parent.mkdir(parents=True, exist_ok=True)
        copy.write_bytes(source.read_bytes())
        copies.append(copy)
    monkeypatch.setattr(slices, "_SRC", tmp_path)
    monkeypatch.setattr(slices, "slice_sources", lambda: copies)
    before = slices._slice_key()
    assert before == slices.SLICE_KEY

    cut = copies[0]
    cut.write_text(
        cut.read_text().replace("has_input: bool = True", "has_input: bool = False")
    )
    assert slices._slice_key() != before
    assert before.startswith(f"{slices.SLICE_VERSION}.")


# ------------------------------------------------------------ fallback scope


def _three_batches(monkeypatch):
    """A batch launching a run, one launching nothing, and one more launching,
    every launch unledgered so only the claims pass can place it."""
    size = cold.PROJECT_BATCH
    n = 2 * size + 2
    launches = {0: "go", 2 * size: "again"}
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=_turns(
                [
                    _launch(i, launches[i]) if i in launches else _plain(i)
                    for i in range(n)
                ]
            ),
        ),
    )
    return size, n


def _answers(page):
    return {
        line.item()["data"]["content"]
        for line in page.lines
        if line.event == "message_chunk"
        and str(line.item()["data"].get("content", "")).startswith("answer ")
    }


async def test_a_failed_claims_pass_runs_once_per_read(monkeypatch):
    """Retrying turn by turn cannot place a run the whole-thread pass could
    not: the batches that need it replay from storage, the one that does not
    projects, and the pass is never run again in the read."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    size, n = _three_batches(monkeypatch)
    place = AsyncMock(side_effect=RuntimeError("a ledger this build cannot read"))
    monkeypatch.setattr(cold, "_place_launches", place)
    queries, responses = _rows(n)

    page = await build_replay_page(thread_rows(queries, responses), turn_fallback=True)

    assert place.await_count == 1
    assert _answers(page) == {f"answer {i}" for i in range(size, 2 * size)}
    assert sorted(store.lined) == sorted(f"tail-{i}" for i in range(size, 2 * size))


async def test_a_failed_claims_pass_is_unavailable_without_turn_fallback(
    monkeypatch,
):
    """A read that may not fall back reports the thread unavailable (the
    endpoint's 409), not a failure."""
    _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    _size, n = _three_batches(monkeypatch)
    place = AsyncMock(side_effect=RuntimeError("a ledger this build cannot read"))
    monkeypatch.setattr(cold, "_place_launches", place)
    queries, responses = _rows(n)

    with pytest.raises(replay.CheckpointReplayUnavailable):
        await replay.read_replay_page(
            thread_rows(queries, responses),
            source="checkpoint",
        )


async def test_a_failed_claims_pass_answers_409_for_a_checkpoint_replay(
    monkeypatch, threads_client
):
    """The endpoint's side of the contract above: a thread the pass cannot
    place is unavailable from checkpoints, not a server error."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    _size, n = _three_batches(monkeypatch)
    place = AsyncMock(side_effect=RuntimeError("a ledger this build cannot read"))
    monkeypatch.setattr(cold, "_place_launches", place)
    queries, responses = _rows(n)
    rows = dataclasses.replace(
        thread_rows(queries, responses), owner_id="test-user-123"
    )

    with patch(
        "src.server.app.threads.messaging.get_replay_thread_data",
        new=AsyncMock(return_value=rows),
    ):
        resp = await threads_client.get(
            f"/api/v1/threads/{THREAD}/messages/replay",
            params={"source": "checkpoint"},
        )

    assert resp.status_code == 409
    assert resp.json()["detail"].startswith("Checkpoint replay unavailable")
    place.assert_awaited_once()
    assert store.lined == []


async def test_a_caller_that_cannot_wait_is_never_made_to_read_the_whole_thread(
    monkeypatch,
):
    """A turn whose launch only the claims pass can place raises instead of
    running it; a turn that needs no pass still projects."""
    store = _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    _size, n = _three_batches(monkeypatch)
    place = AsyncMock(side_effect=AssertionError("ran the claims pass"))
    monkeypatch.setattr(cold, "_place_launches", place)
    queries, responses = _rows(n)

    with pytest.raises(replay.ClaimsPassNeeded):
        await replay.project_turns(
            thread_rows(queries, responses),
            "cp-tip",
            turn_indexes=[0],
            claims_pass=False,
        )
    lines = await replay.project_turns(
        thread_rows(queries, responses),
        "cp-tip",
        turn_indexes=[1],
        claims_pass=False,
    )

    place.assert_not_awaited()
    assert list(lines) == [1]
    assert store.lined == ["tail-1"]


async def test_a_database_fault_ends_the_read_without_per_turn_retries(
    monkeypatch,
):
    from src.server.database.conversation import turn_slices as slices_db
    from src.server.database.pool import AppDataPoolTimeout

    _slice_store(monkeypatch)
    _seal_evidence(monkeypatch)
    _size, n = _three_batches(monkeypatch)
    reads = AsyncMock(side_effect=AppDataPoolTimeout("couldn't get a connection"))
    monkeypatch.setattr(slices_db, "get_turn_slices", reads)
    queries, responses = _rows(n)

    # Nor does it fall back to storage, which waits on the same pool.
    with pytest.raises(AppDataPoolTimeout):
        await replay.read_replay_page(thread_rows(queries, responses))
    assert reads.await_count == 1


async def test_a_busy_checkpointer_pool_still_falls_back_to_storage(monkeypatch):
    from psycopg_pool import PoolTimeout

    reader = _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[]))
    reader.thread_history.side_effect = PoolTimeout("couldn't get a connection")
    queries, responses = _rows(1)

    # Storage is read through the app-data pool, which this wait never held.
    page, source = await replay.read_replay_page(thread_rows(queries, responses))
    assert (source, page.first_turn_index) == ("sse", 0)
