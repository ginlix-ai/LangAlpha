"""Per-turn projection cache: primitives, fast-path assembly, backfill."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history import projection_cache
from src.server.services.history.reader import ThreadHistory, TurnAnchor, TurnSlice
from src.server.services.history.replay import build_checkpoint_replay_items

pytestmark = pytest.mark.asyncio

THREAD = "thread-pc"

_END_SENTINEL = json.dumps({"event": "subagent_stream_end"})


class _FakeRedis:
    """Raw-client stand-in: per-key subagent stream tails (None = no stream)."""

    def __init__(self):
        self.stream_tails: dict[str, str] = {}

    async def xrevrange(self, key, count=1):
        raw = self.stream_tails.get(key)
        if raw is None:
            return []
        return [(b"0-1", {b"event": raw.encode()})]


class _FakeCacheClient:
    """In-memory stand-in matching the RedisCacheClient surface used here."""

    def __init__(self):
        self.store: dict[str, object] = {}
        self.enabled = True
        self.client = _FakeRedis()

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        return True


@pytest.fixture
def fake_cache(monkeypatch):
    client = _FakeCacheClient()
    monkeypatch.setattr(projection_cache, "get_cache_client", lambda: client)
    return client


def _anchor(ordinal, tail, turn_index=None):
    return TurnAnchor(
        turn_ordinal=ordinal,
        input_checkpoint_id=f"cp-in-{ordinal}",
        tail_checkpoint_id=tail,
        turn_index=turn_index,
    )


def _turn(ordinal, messages, tail, turn_index=None):
    return TurnSlice(
        turn_ordinal=ordinal,
        input_checkpoint_id=f"cp-in-{ordinal}",
        end_checkpoint_id=f"cp-end-{ordinal}",
        user_message=HumanMessage(content="hello", id=f"h-{ordinal}"),
        messages=messages,
        turn_index=turn_index,
        tail_checkpoint_id=tail,
    )


def _query(turn_index, content="hello"):
    return {"turn_index": turn_index, "content": content, "type": "user", "created_at": "t0"}


def _response(turn_index, status="completed"):
    return {
        "conversation_response_id": f"resp-{turn_index}",
        "sse_events": [],
        "status": status,
    }


def _mock_reader(monkeypatch, *, anchors=None, tip="cp-tip", history=None,
                 tip_interrupts=None):
    reader = MagicMock()
    reader.aget_turn_anchors = AsyncMock(return_value=(anchors or [], tip))
    reader.aget_tip_interrupts = AsyncMock(return_value=tip_interrupts or [])
    reader.aget_thread_history = AsyncMock(
        return_value=history or ThreadHistory(thread_id=THREAD)
    )
    reader.aget_recent_history = AsyncMock(
        return_value=history or ThreadHistory(thread_id=THREAD)
    )
    reader.aget_task_messages = AsyncMock(return_value=[])
    monkeypatch.setattr(
        "src.server.services.history.replay.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    return reader


# ---------------------------------------------------------------- primitives


async def test_store_and_get_round_trip(fake_cache):
    items = [{"event": "user_message", "data": {"content": "hi"}}]
    await projection_cache.store_turn(THREAD, "tail-1", items)
    cached = await projection_cache.get_cached_turns(THREAD, ["tail-1", "tail-2"])
    assert cached["tail-1"] == items
    assert cached["tail-2"] is None


async def test_store_canonicalizes_to_wire_json(fake_cache):
    # The endpoint serializes with json.dumps(default=str); cached entries
    # must round-trip datetimes to the identical wire text (str(), not
    # isoformat — space separator).
    ts = datetime(2026, 7, 7, 12, 0, 30, tzinfo=timezone.utc)
    await projection_cache.store_turn(
        THREAD, "tail-1", [{"event": "user_message", "data": {"timestamp": ts}}]
    )
    cached = await projection_cache.get_cached_turns(THREAD, ["tail-1"])
    assert cached["tail-1"][0]["data"]["timestamp"] == str(ts)


async def test_store_skips_oversize_and_missing_tail(fake_cache):
    await projection_cache.store_turn(THREAD, None, [{"event": "x", "data": {}}])
    big = [{"event": "x", "data": {"blob": "a" * (projection_cache._MAX_ENTRY_BYTES + 1)}}]
    await projection_cache.store_turn(THREAD, "tail-big", big)
    assert fake_cache.store == {}


async def test_inactive_without_connected_client(fake_cache):
    fake_cache.client = None
    assert projection_cache.cache_active() is False


async def test_zero_ttl_disables(fake_cache, monkeypatch):
    monkeypatch.setattr(
        projection_cache, "get_replay_projection_cache_ttl", lambda: 0
    )
    assert projection_cache.cache_active() is False


# ---------------------------------------------------------- replay fast path


async def test_full_hit_assembles_without_materializing(monkeypatch, fake_cache):
    entry0 = [{"event": "user_message", "data": {"thread_id": THREAD, "turn_index": 0}}]
    entry1 = [
        {"event": "user_message", "data": {"thread_id": THREAD, "turn_index": 1}},
        {"event": "message_chunk", "data": {"content": "hi", "turn_index": 1}},
    ]
    fake_cache.store[projection_cache._key(THREAD, "tail-0")] = entry0
    fake_cache.store[projection_cache._key(THREAD, "tail-1")] = entry1
    reader = _mock_reader(
        monkeypatch,
        anchors=[_anchor(0, "tail-0", 0), _anchor(1, "tail-1", 1)],
        tip="tail-1",
        tip_interrupts=[{"id": "int-1", "value": {"action_requests": []}}],
    )

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1)],
        {0: _response(0), 1: _response(1)},
    )

    assert items[: len(entry0) + len(entry1)] == entry0 + entry1
    assert items[-1]["event"] == "interrupt"
    reader.aget_thread_history.assert_not_called()
    reader.aget_recent_history.assert_not_called()


async def test_fast_path_stubs_inflight_turn_from_rows(monkeypatch, fake_cache):
    fake_cache.store[projection_cache._key(THREAD, "tail-0")] = [
        {"event": "user_message", "data": {"turn_index": 0}}
    ]
    _mock_reader(monkeypatch, anchors=[_anchor(0, "tail-0", 0)], tip="tail-0")

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1, content="in flight")],
        {0: _response(0), 1: _response(1, status="streaming")},
    )

    assert [i["event"] for i in items] == ["user_message", "user_message"]
    assert items[1]["data"]["content"] == "in flight"


async def test_any_miss_rebuilds_and_backfills(monkeypatch, fake_cache):
    turns = [
        _turn(0, [HumanMessage(content="hello", id="h-0"),
                  AIMessage(content="a0", id="ai-0")], "tail-0", 0),
        _turn(1, [HumanMessage(content="hello", id="h-1"),
                  AIMessage(content="a1", id="ai-1")], "tail-1", 1),
    ]
    fake_cache.store[projection_cache._key(THREAD, "tail-0")] = [
        {"event": "user_message", "data": {"turn_index": 0}}
    ]  # tail-1 missing → full rebuild
    reader = _mock_reader(
        monkeypatch,
        anchors=[_anchor(0, "tail-0", 0), _anchor(1, "tail-1", 1)],
        tip="tail-1",
        history=ThreadHistory(thread_id=THREAD, turns=turns),
    )

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1)],
        {0: _response(0), 1: _response(1)},
    )

    reader.aget_thread_history.assert_called_once()
    assert [i["event"] for i in items] == [
        "user_message", "message_chunk", "user_message", "message_chunk",
    ]
    # Both turns backfilled under their tail keys, wire-canonical.
    for tail, turn_index in (("tail-0", 0), ("tail-1", 1)):
        entry = fake_cache.store[projection_cache._key(THREAD, tail)]
        assert [i["event"] for i in entry] == ["user_message", "message_chunk"]
        assert entry[-1]["data"]["turn_index"] == turn_index


async def test_missing_tail_falls_back_to_rebuild(monkeypatch, fake_cache):
    turns = [_turn(0, [HumanMessage(content="hello", id="h-0"),
                       AIMessage(content="a0", id="ai-0")], None, 0)]
    reader = _mock_reader(
        monkeypatch,
        anchors=[_anchor(0, None, 0)],
        history=ThreadHistory(thread_id=THREAD, turns=turns),
    )

    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})

    reader.aget_thread_history.assert_called_once()
    assert [i["event"] for i in items] == ["user_message", "message_chunk"]
    assert fake_cache.store == {}  # no tail → nothing cached


async def test_refresh_writes_last_two_turn_entries(monkeypatch, fake_cache):
    # The post-persist refresh rebuilds a two-turn window: the just-persisted
    # turn's entry is new, and the PREVIOUS turn's entry is overwritten under
    # its unchanged tail key — that is how an interrupted turn's
    # ending_interrupts (known only at the resume boundary) reach the cache.
    turns = [
        _turn(0, [HumanMessage(content="hello", id="h-0"),
                  AIMessage(content="a0", id="ai-0")], "tail-0", 0),
        _turn(1, [HumanMessage(content="hello", id="h-1"),
                  AIMessage(content="a1", id="ai-1")], "tail-1", 1),
    ]
    _mock_reader(
        monkeypatch,
        anchors=[_anchor(0, "tail-0", 0), _anchor(1, "tail-1", 1)],
        tip="tail-1",
        history=ThreadHistory(thread_id=THREAD, turns=turns),
    )
    # Stale previous-turn entry that the refresh must overwrite.
    fake_cache.store[projection_cache._key(THREAD, "tail-0")] = [
        {"event": "user_message", "data": {"stale": True}}
    ]
    monkeypatch.setattr(
        "src.server.database.conversation.get_replay_thread_data",
        AsyncMock(return_value=(
            "owner",
            {"latest_checkpoint_id": "tail-1"},
            [_query(0), _query(1)],
            [_response(0), _response(1)],
            [],
            [],
        )),
    )

    await projection_cache.refresh_thread_projection(THREAD)

    for tail in ("tail-0", "tail-1"):
        entry = fake_cache.store[projection_cache._key(THREAD, tail)]
        assert [i["event"] for i in entry] == ["user_message", "message_chunk"]
    assert "stale" not in str(fake_cache.store[projection_cache._key(THREAD, "tail-0")])


async def test_refresh_noops_without_commit_pointer(monkeypatch, fake_cache):
    monkeypatch.setattr(
        "src.server.database.conversation.get_replay_thread_data",
        AsyncMock(return_value=("owner", {"latest_checkpoint_id": None}, [], [], [], [])),
    )
    await projection_cache.refresh_thread_projection(THREAD)
    assert fake_cache.store == {}


async def test_task_streams_live_variants(fake_cache):
    key = f"subagent:stream:{THREAD}:tsk1"
    # No stream at all (TTL'd or never spilled) → terminal.
    assert await projection_cache.task_streams_live(THREAD, {"tsk1"}) is False
    # Last entry is a wire string → producer still writing.
    fake_cache.client.stream_tails[key] = "id: 7\nevent: message_chunk\ndata: {}\n\n"
    assert await projection_cache.task_streams_live(THREAD, {"tsk1"}) is True
    # Finalized sentinel → terminal.
    fake_cache.client.stream_tails[key] = _END_SENTINEL
    assert await projection_cache.task_streams_live(THREAD, {"tsk1"}) is False
    # No tasks → trivially terminal; disconnected client → conservative live.
    assert await projection_cache.task_streams_live(THREAD, set()) is False
    fake_cache.client = None
    assert await projection_cache.task_streams_live(THREAD, {"tsk1"}) is True


async def test_live_subagent_turn_not_cached_until_finalized(monkeypatch, fake_cache):
    # Tail mode: the dispatch turn persists while the subagent still writes.
    # Its transcript is projected from task-ns state that keeps growing AFTER
    # this turn's tail is fixed — caching it would freeze a partial transcript
    # forever. The turn must skip the store until the task stream finalizes.
    task_artifact = {"task_id": "tsk1", "action": "init", "description": "d", "prompt": "p"}
    turn_msgs = [
        HumanMessage(content="hello", id="h-0"),
        AIMessage(content="", id="ai-1",
                  tool_calls=[{"name": "Task", "args": {}, "id": "tc-t"}]),
        ToolMessage(content="dispatched", tool_call_id="tc-t", name="Task",
                    id="tm-1", additional_kwargs={"task_artifact": task_artifact}),
    ]
    history = ThreadHistory(
        thread_id=THREAD, turns=[_turn(0, turn_msgs, "tail-0", 0)]
    )
    reader = _mock_reader(
        monkeypatch, anchors=[_anchor(0, "tail-0", 0)], tip="tail-0", history=history
    )
    reader.aget_task_messages = AsyncMock(
        return_value=[AIMessage(content="partial", id="sub-1")]
    )
    stream_key = f"subagent:stream:{THREAD}:tsk1"
    fake_cache.client.stream_tails[stream_key] = "id: 3\nevent: message_chunk\ndata: {}\n\n"

    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    assert any(i["data"].get("agent") == "task:tsk1" for i in items)
    assert fake_cache.store == {}  # live stream → nothing frozen

    # Subagent finishes (sentinel lands); the transcript is final now.
    fake_cache.client.stream_tails[stream_key] = _END_SENTINEL
    reader.aget_task_messages = AsyncMock(
        return_value=[AIMessage(content="partial", id="sub-1"),
                      AIMessage(content="final", id="sub-2")]
    )
    items = await build_checkpoint_replay_items(THREAD, [_query(0)], {0: _response(0)})
    entry = fake_cache.store[projection_cache._key(THREAD, "tail-0")]
    assert sum(1 for i in entry if i["data"].get("agent") == "task:tsk1") == 2


async def test_windowed_fast_path_keeps_absolute_pairing(monkeypatch, fake_cache):
    entry = [{"event": "user_message", "data": {"turn_index": 2}}]
    fake_cache.store[projection_cache._key(THREAD, "tail-2")] = entry
    reader = _mock_reader(
        monkeypatch,
        anchors=[_anchor(0, "tail-0", 0), _anchor(1, "tail-1", 1), _anchor(2, "tail-2", 2)],
        tip="tail-2",
    )

    items = await build_checkpoint_replay_items(
        THREAD,
        [_query(0), _query(1), _query(2)],
        {i: _response(i) for i in range(3)},
        last_n_turns=1,
    )

    assert items == entry
    reader.aget_recent_history.assert_not_called()
