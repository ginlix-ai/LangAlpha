"""The legacy facts backfill against real Postgres.

A backfill write races the run's own writers, so only a real database can show
that it keeps the keys they set, lands only on the row version it derived
from, is not repeated, and that a later write to the stored events takes the
facts away again; and that a backfilled row's events are left out of the
replay read. The checkpoint side is the unit suites' mocked reader: the rows
are what is under test here.
"""

from __future__ import annotations

import uuid
from collections import Counter
from types import SimpleNamespace

import pytest
import pytest_asyncio
from langchain_core.messages import AIMessage

from scripts.utils import backfill_replay_facts as backfill
from src.server.services.history.replay import legacy
from tests.unit.server.services.history.replay_builders import (
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _turn,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TIP = "cp-tip"
RUN_FACTS = {"v": 1, "reasoning_ms": {"msg_1": [250]}}
STORED = [
    {"event": "message_chunk", "data": {"id": "lc-1", "content_type": "text", "content": "answer"}},
    {"event": "context_window", "data": {"action": "offload", "kind": "tool_args"}},
]
LATER = {"event": "provenance", "data": {"source_type": "web", "identifier": "https://a.example"}}


@pytest_asyncio.fixture
async def turn(seed_workspace, seed_response, patched_get_db_connection, monkeypatch):
    """A one-turn thread whose completed row has stored events and the run
    facts its finalize wrote, over a checkpoint that projects the turn."""
    from src.server.database.conversation import create_query, create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    await create_query(
        conversation_query_id=str(uuid.uuid4()),
        conversation_thread_id=thread_id,
        turn_index=0,
        content="question",
        query_type="initial",
    )
    response_id = str(uuid.uuid4())
    await seed_response(response_id, thread_id, 0, sse_events=STORED)

    async def sql(query, params=()):
        async with patched_get_db_connection() as conn:
            cur = await conn.execute(query, params)
            return await cur.fetchone() if cur.description else None

    await sql(
        "UPDATE conversation_threads SET latest_checkpoint_id = %s "
        "WHERE conversation_thread_id = %s",
        (TIP, thread_id),
    )
    await sql(
        "UPDATE conversation_responses SET replay_facts = %s::jsonb "
        "WHERE conversation_response_id = %s",
        (_json(RUN_FACTS), response_id),
    )
    _cache_probe(monkeypatch)
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=thread_id, turns=[_turn(0, [AIMessage("answer", id="ai-1")])]),
    )

    async def row():
        return await sql(
            "SELECT replay_facts, sse_events, metadata, xmin::text AS xmin "
            "FROM conversation_responses WHERE conversation_response_id = %s",
            (response_id,),
        )

    return SimpleNamespace(thread_id=thread_id, response_id=response_id, sql=sql, row=row)


def _json(value) -> str:
    import json

    return json.dumps(value)


async def _backfill(thread_id: str) -> Counter:
    stats: Counter = Counter()
    mismatched: list[str] = []
    await backfill._backfill_thread(thread_id, stats, mismatched)
    assert mismatched == []
    return stats


async def _lines(thread_id: str):
    """The thread's turn 0 as a replay projects it now, from its rows."""
    from src.server.database.conversation import get_replay_thread_data
    from src.server.services.history.replay import project_turns

    rows = await get_replay_thread_data(thread_id)
    lines = await project_turns(rows, TIP, turn_indexes=[0], reproject=True)
    return lines[0]


async def test_a_backfill_keeps_live_keys_runs_once_and_ends_event_reads(turn):
    from src.server.database.conversation import get_replay_responses

    before = await _lines(turn.thread_id)

    stats = await _backfill(turn.thread_id)

    assert (stats["turns/written"], stats["turns/refreshed"]) == (1, 1)
    facts = (await turn.row())["replay_facts"]
    assert {k: facts[k] for k in RUN_FACTS} == RUN_FACTS
    assert facts["legacy"]["v"] == legacy.FACTS_VERSION
    assert [s["event"] for s in facts["legacy"]["merge"]["signals"]] == ["context_window"]

    # A rerun finds nothing pending and leaves the row's version alone.
    version = (await turn.row())["xmin"]
    assert await _backfill(turn.thread_id) == Counter()
    assert (await turn.row())["xmin"] == version

    backfilled = await get_replay_responses(
        [turn.response_id], legacy_facts=legacy.FACTS_VERSION
    )
    assert backfilled[turn.response_id]["sse_events"] is None
    verbatim = await get_replay_responses([turn.response_id])
    assert verbatim[turn.response_id]["sse_events"] == STORED

    # With the column gone the turn replays the same.
    await turn.sql(
        "UPDATE conversation_responses SET sse_events = NULL "
        "WHERE conversation_response_id = %s",
        (turn.response_id,),
    )
    assert await _lines(turn.thread_id) == before


async def test_a_row_without_stored_events_is_marked_known_empty(turn):
    await turn.sql(
        "UPDATE conversation_responses SET sse_events = NULL "
        "WHERE conversation_response_id = %s",
        (turn.response_id,),
    )

    await _backfill(turn.thread_id)

    facts = (await turn.row())["replay_facts"]
    assert facts["legacy"] == {"v": legacy.FACTS_VERSION}
    assert await backfill._pending_rows(turn.thread_id) == {}


@pytest.mark.parametrize("events", [None, STORED], ids=["no_events", "with_events"])
async def test_a_turn_without_a_boundary_is_marked_only_when_it_stored_nothing(
    turn, seed_response, events
):
    """A turn the checkpoints do not cover yet replays as its stub; facts
    known empty would hide its events from the projection that covers it."""
    stub_id = str(uuid.uuid4())
    await seed_response(stub_id, turn.thread_id, 1, status="interrupted", sse_events=events)

    stats = await _backfill(turn.thread_id)

    facts = await turn.sql(
        "SELECT replay_facts FROM conversation_responses WHERE conversation_response_id = %s",
        (stub_id,),
    )
    if events is None:
        assert stats["turns/stub"] == 1
        assert facts["replay_facts"]["legacy"] == {"v": legacy.FACTS_VERSION}
        assert set(await backfill._pending_rows(turn.thread_id)) == set()
    else:
        assert stats["turns/stub_with_events"] == 1
        assert "legacy" not in (facts["replay_facts"] or {})
        assert set(await backfill._pending_rows(turn.thread_id)) == {1}


async def test_a_row_written_meanwhile_is_left_for_the_next_run(turn, monkeypatch):
    from src.server.database.conversation.responses import append_sse_event

    real_write = backfill._write_facts

    async def after_a_live_append(rows):
        await append_sse_event(turn.thread_id, LATER)
        return await real_write(rows)

    monkeypatch.setattr(backfill, "_write_facts", after_a_live_append)
    stats = await _backfill(turn.thread_id)

    assert (stats["turns/written"], stats["turns/moved"]) == (0, 1)
    assert stats["turns/refreshed"] == 0
    assert "legacy" not in (await turn.row())["replay_facts"]

    monkeypatch.setattr(backfill, "_write_facts", real_write)
    assert (await _backfill(turn.thread_id))["turns/written"] == 1
    signals = (await turn.row())["replay_facts"]["legacy"]["merge"]["signals"]
    assert [s["event"] for s in signals] == ["context_window", "provenance"]


async def test_facts_that_would_replay_differently_are_never_written(turn, monkeypatch):
    """A row cannot hold a NUL, so facts carrying one would lose it."""
    from src.server.services.history.replay import stored_merge

    real_derive = stored_merge.derive_turn

    def derive_with_a_nul(*args, **kwargs):
        facts = real_derive(*args, **kwargs)
        facts["merge"]["signals"][0]["data"]["kind"] = "tool\x00args"
        return facts

    monkeypatch.setattr(stored_merge, "derive_turn", derive_with_a_nul)
    stats: Counter = Counter()
    mismatched: list[str] = []
    await backfill._backfill_thread(turn.thread_id, stats, mismatched)

    assert mismatched == [f"{turn.thread_id} turn 0"]
    assert (stats["turns/mismatch"], stats["turns/written"]) == (1, 0)
    assert (await turn.row())["replay_facts"] == RUN_FACTS
    assert set(await backfill._pending_rows(turn.thread_id)) == {0}


@pytest.mark.parametrize("writer", ["append", "replace"])
async def test_a_write_to_the_stored_events_takes_the_facts_away(turn, writer):
    from src.server.database.conversation.responses import (
        append_sse_event,
        replace_agent_events,
    )

    await _backfill(turn.thread_id)
    assert "legacy" in (await turn.row())["replay_facts"]

    if writer == "append":
        assert await append_sse_event(turn.thread_id, LATER)
    else:
        assert await replace_agent_events(
            turn.response_id,
            [{"event": "message_chunk", "data": {"agent": "task:k1", "content": "t"}}],
        )

    facts = (await turn.row())["replay_facts"]
    assert facts == RUN_FACTS
    assert set(await backfill._pending_rows(turn.thread_id)) == {0}


async def test_credit_pauses_are_stamped_once(turn, seed_response):
    from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
    from src.server.database.automation_executions import get_settling_run

    def pause(message):
        request = {"type": INTERRUPT_REASON_CREDIT_PAUSE}
        if message is not None:
            request["message"] = message
        return [{"event": "interrupt", "data": {"action_requests": [request]}}]

    ids = {}
    for turn_index, (name, events, metadata) in enumerate(
        [
            ("worded", pause("Out of credits."), None),
            ("wordless", pause(None), None),
            ("finalized", pause("Older words."), {"credit_pause_message": "Stamped."}),
        ],
        start=1,
    ):
        ids[name] = str(uuid.uuid4())
        await seed_response(
            ids[name], turn.thread_id, turn_index, status="interrupted", sse_events=events
        )
        await turn.sql(
            "UPDATE conversation_responses SET interrupt_reason = %s, "
            "metadata = metadata || %s::jsonb WHERE conversation_response_id = %s",
            (INTERRUPT_REASON_CREDIT_PAUSE, _json(metadata or {}), ids[name]),
        )

    args = SimpleNamespace(thread=[turn.thread_id], workspace=None, user=None, days=None)
    pending = await backfill._pause_rows(args)
    assert sorted(pending) == sorted([ids["worded"], ids["wordless"]])

    assert await backfill._stamp_pauses(pending) == (2, 1)
    assert await backfill._pause_rows(args) == []
    assert await backfill._stamp_pauses(pending) == (0, 0)

    stamps = {}
    for name, response_id in ids.items():
        run = await get_settling_run(response_id)
        assert run["sse_events"] is None  # settling no longer reads them
        stamps[name] = run["metadata"]["credit_pause_message"]
    assert stamps == {"worded": "Out of credits.", "wordless": None, "finalized": "Stamped."}
