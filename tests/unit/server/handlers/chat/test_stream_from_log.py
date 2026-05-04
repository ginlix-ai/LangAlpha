"""Unit tests for the Redis-Streams-backed SSE consumer.

Validates:
- XREAD cursor selection (0/<seq>-0) for new vs replay vs resume connections
- SSE payload pass-through (UTF-8 decode of bytes)
- Keepalive emission on BLOCK timeout + terminal-after-empty exit handshake
- on_attach / on_detach hooks fire even when generator is cancelled mid-flight
- Subagent JSON-record rendering to SSE wire format
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.stream_from_log import (
    _record_to_sse,
    _stream_from_redis_log,
    stream_from_log,
    stream_subagent_from_log,
)


def _make_cache(xread_returns: list[Any]) -> MagicMock:
    """A cache mock whose client.xread iterates through the given sequence."""
    cache = MagicMock()
    cache.enabled = True
    redis = MagicMock()
    cache.client = redis
    redis.xread = AsyncMock(side_effect=xread_returns)
    return cache


@pytest.mark.asyncio
async def test_yields_decoded_payloads_in_order():
    cache = _make_cache(
        [
            [
                (
                    b"workflow:stream:t1",
                    [
                        (b"1-0", {b"event": b"id: 1\nevent: x\ndata: a\n\n"}),
                        (b"2-0", {b"event": b"id: 2\nevent: x\ndata: b\n\n"}),
                    ],
                )
            ],
            [],  # BLOCK timeout
            [],  # terminal=True confirmed; should exit after second empty
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = []
        async for ev in _stream_from_redis_log(
            stream_key="workflow:stream:t1",
            terminal_check=terminal,
            last_event_id=None,
        ):
            out.append(ev)

    # Two real events + one keepalive between the only-data round and the
    # exit handshake.
    assert "id: 1" in out[0]
    assert "id: 2" in out[1]
    assert ":keepalive\n\n" in out


@pytest.mark.asyncio
async def test_none_last_event_id_replays_from_start():
    """``last_event_id=None`` (no resume cursor) → start at 0 to replay
    everything in the stream. Subagent attaches need this; main first-
    connect doesn't lose anything because the stream is empty at start.
    """
    cache = _make_cache([[], []])  # two empty rounds → terminal handshake

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=None,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"0"}


@pytest.mark.asyncio
async def test_replay_uses_zero_cursor():
    cache = _make_cache([[], []])

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"0"}


@pytest.mark.asyncio
async def test_resume_uses_seq_dash_zero_cursor():
    cache = _make_cache([[], []])

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=42,
        ):
            pass

    args, kwargs = cache.client.xread.call_args_list[0]
    streams = args[0] if args else kwargs.get("streams")
    assert streams == {b"k": b"42-0"}


@pytest.mark.asyncio
async def test_advances_cursor_through_entries():
    """Across multiple XREAD rounds, cursor should advance to last seen ID."""
    cache = _make_cache(
        [
            [
                (
                    b"k",
                    [
                        (b"5-0", {b"event": b"id: 5\ndata: a\n\n"}),
                        (b"6-0", {b"event": b"id: 6\ndata: b\n\n"}),
                    ],
                )
            ],
            [],  # terminal becomes True; first empty round
            [],  # second empty round → exit
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=4,
        ):
            pass

    cursors = [
        (call.args[0] if call.args else call.kwargs.get("streams"))[b"k"]
        for call in cache.client.xread.call_args_list
    ]
    assert cursors[0] == b"4-0"  # initial resume cursor
    assert cursors[1] == b"6-0"  # advanced past the two yielded entries


@pytest.mark.asyncio
async def test_cursor_advances_past_skipped_last_entry():
    """Regression: when the *last* entry in an XREAD batch is skipped (missing
    ``event`` field, non-UTF8 bytes), the cursor must still advance past it.

    Otherwise the next XREAD reads using the prior entry's id and gets the
    same skipped entry back forever — a tight retry loop that never makes
    progress."""
    # Round 1: two entries; the LAST one is skipped (missing ``event`` field).
    # Round 2: empty (terminal handshake round 1).
    # Round 3: empty (handshake round 2 → exit).
    cache = _make_cache(
        [
            [
                (
                    b"k",
                    [
                        (b"5-0", {b"event": b"id: 5\ndata: a\n\n"}),
                        (b"6-0", {}),  # missing ``event`` — skipped via continue
                    ],
                )
            ],
            [],
            [],
        ]
    )

    async def terminal() -> bool:
        return True

    yielded: list[str] = []
    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for ev in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=4,
        ):
            if ev != ":keepalive\n\n":
                yielded.append(ev)

    cursors = [
        (call.args[0] if call.args else call.kwargs.get("streams"))[b"k"]
        for call in cache.client.xread.call_args_list
    ]
    assert yielded == ["id: 5\ndata: a\n\n"]
    # Critical assertion: cursor moved to 6-0 even though that entry was
    # skipped. Without the fix, cursors[1] would be b"5-0" (the last
    # successfully-yielded entry) and round 2 would re-read 6-0 forever.
    assert cursors[1] == b"6-0"


@pytest.mark.asyncio
async def test_terminal_handshake_requires_two_empty_rounds():
    """If terminal=True but new events still arrive between rounds, don't exit early."""
    cache = _make_cache(
        [
            [],  # empty + terminal=True → set terminal_seen
            [
                (
                    b"k",
                    [(b"1-0", {b"event": b"id: 1\ndata: x\n\n"})],
                )
            ],  # late event resets terminal_seen
            [],  # empty + terminal=True → first
            [],  # empty + terminal=True → second; exit
        ]
    )

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = []
        async for ev in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
        ):
            out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    assert len(payloads) == 1
    assert "id: 1" in payloads[0]


@pytest.mark.asyncio
async def test_attach_detach_hooks_fire():
    cache = _make_cache([[], []])
    attach_calls = []
    detach_calls = []

    async def on_attach() -> None:
        attach_calls.append(True)

    async def on_detach() -> None:
        detach_calls.append(True)

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
            on_attach=on_attach,
            on_detach=on_detach,
        ):
            pass

    assert attach_calls == [True]
    assert detach_calls == [True]


def test_classify_subagent_payload_distinguishes_all_four_kinds():
    """Lock in the contract that ``_classify_subagent_payload`` recognises:
    pre-rendered SSE wire strings, stream-end sentinels, legacy JSON
    records, and unknown/garbage. The classifier is the single source of
    truth on the consumer hot path — drift would re-introduce the
    redundant byte-zero check the consolidation removed."""
    from src.server.handlers.chat.stream_from_log import (
        _PAYLOAD_RECORD,
        _PAYLOAD_SENTINEL,
        _PAYLOAD_UNKNOWN,
        _PAYLOAD_WIRE,
        _classify_subagent_payload,
    )

    # SSE wire strings (event + keepalive) bail before JSON decode.
    assert _classify_subagent_payload("id: 5\nevent: x\ndata: {}\n\n") == (_PAYLOAD_WIRE, None)
    assert _classify_subagent_payload(":keepalive\n\n") == (_PAYLOAD_WIRE, None)
    assert _classify_subagent_payload("") == (_PAYLOAD_WIRE, None)

    # Sentinel: ``event`` set, ``seq`` absent → consumer should exit.
    sentinel_kind, sentinel_parsed = _classify_subagent_payload(
        json.dumps({"event": "subagent_stream_end"})
    )
    assert sentinel_kind == _PAYLOAD_SENTINEL
    assert sentinel_parsed is None

    # Legacy JSON record: ``seq`` present → render via _record_to_sse.
    record_kind, record_parsed = _classify_subagent_payload(
        json.dumps({"seq": 5, "event": "message_chunk", "data": {"content": "x"}})
    )
    assert record_kind == _PAYLOAD_RECORD
    assert record_parsed == {"seq": 5, "event": "message_chunk", "data": {"content": "x"}}

    # Garbage / non-dict / wrong shape → unknown, fall through to raw.
    assert _classify_subagent_payload("not json{")[0] == _PAYLOAD_UNKNOWN
    assert _classify_subagent_payload("[1, 2, 3]")[0] == _PAYLOAD_UNKNOWN
    # An ``event`` field without sentinel value AND without ``seq`` is
    # also unknown — neither pre-rendered nor a legacy record.
    assert _classify_subagent_payload(json.dumps({"event": "other"}))[0] == _PAYLOAD_UNKNOWN
    # A stray sentinel that *also* carries ``seq`` (defensive: shouldn't
    # happen, but if a future producer regression added one, we'd rather
    # render it as a record than silently exit the consumer).
    sneaky_kind, _ = _classify_subagent_payload(
        json.dumps({"event": "subagent_stream_end", "seq": 1})
    )
    assert sneaky_kind == _PAYLOAD_RECORD


def test_record_to_sse_injects_thread_and_task_ids():
    record = {
        "seq": 7,
        "event": "message_chunk",
        "data": {"content": "hello"},
        # ``agent_id`` is the LangGraph namespace UUID — must NOT leak as the
        # user-facing ``agent`` label.
        "agent_id": "general-purpose:c7de8b8b-07e0-451d-bd8d-96a173f9018c",
    }
    sse = _record_to_sse(record, thread_id="t1", task_id="abc123")
    assert sse.startswith("id: 7\n")
    assert "event: message_chunk\n" in sse
    body = sse.split("data: ", 1)[1].rstrip("\n")
    payload = json.loads(body)
    assert payload["thread_id"] == "t1"
    assert payload["agent"] == "task:abc123"
    assert payload["content"] == "hello"


def test_record_to_sse_uses_task_id_label_even_without_agent_id():
    """No ``agent_id`` on the record: still derive ``task:{task_id}``."""
    sse = _record_to_sse(
        {"seq": 1, "event": "tool_calls", "data": {"x": 1}},
        thread_id="t1",
        task_id="xyz",
    )
    body = sse.split("data: ", 1)[1].rstrip("\n")
    assert json.loads(body)["agent"] == "task:xyz"


def test_record_to_sse_canonical_fields_win_over_inner_data():
    """Caller-injected thread_id/agent must shadow producer-side stray keys
    inside the inner ``data`` dict. The consumer is the source of truth for
    routing identity, and ``task:{task_id}`` is the canonical label — never
    the namespace-UUID ``agent_id``."""
    record = {
        "seq": 3,
        "event": "message_chunk",
        "data": {
            # Producer accidentally stamped wrong identity into the inner dict.
            "thread_id": "WRONG-thread",
            "agent": "WRONG-agent",
            "content": "hi",
        },
        # Namespace UUID — would surface as "general-purpose:<uuid>" if we
        # accidentally used it as the user-facing label.
        "agent_id": "general-purpose:c7de8b8b-07e0-451d-bd8d-96a173f9018c",
    }
    sse = _record_to_sse(record, thread_id="t1", task_id="correct-task")
    body = sse.split("data: ", 1)[1].rstrip("\n")
    payload = json.loads(body)
    assert payload["thread_id"] == "t1"
    assert payload["agent"] == "task:correct-task"
    assert payload["content"] == "hi"


@pytest.mark.asyncio
async def test_subagent_consumer_passes_through_pre_rendered_sse(monkeypatch):
    """Steady-state: producer writes SSE wire strings into the Stream so the
    consumer is a pass-through. No JSON-decode + re-render branch on the hot
    path."""
    from src.server.handlers.chat import stream_from_log as sfl_mod

    sse_bytes = b"id: 5\nevent: message_chunk\ndata: {\"content\": \"delta\", \"agent\": \"task:abc\"}\n\n"

    cache = _make_cache(
        [
            [(b"subagent:stream:t1:abc", [(b"5-0", {b"event": sse_bytes})])],
            [],
            [],
        ]
    )
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: cache)

    async def _no_task(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sfl_mod, "_wait_for_subagent_task", _no_task)

    out = []
    async for ev in stream_subagent_from_log("t1", "abc", last_event_id=0):
        out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    assert len(payloads) == 1
    # Byte-for-byte pass-through — no re-rendering.
    assert payloads[0] == sse_bytes.decode("utf-8")


@pytest.mark.asyncio
async def test_subagent_consumer_renders_legacy_json_records(monkeypatch):
    """Legacy JSON-record shim: streams written before the wire-string cutover
    must be rendered on the fly until their TTL expires."""
    from src.server.handlers.chat import stream_from_log as sfl_mod

    payload_bytes = json.dumps(
        {
            "seq": 5,
            "event": "message_chunk",
            "data": {"content": "delta"},
            # Namespace UUID — must NOT leak as the agent label.
            "agent_id": "general-purpose:c7de8b8b-07e0-451d-bd8d-96a173f9018c",
        }
    ).encode("utf-8")

    cache = _make_cache(
        [
            [(b"subagent:stream:t1:abc", [(b"5-0", {b"event": payload_bytes})])],
            [],
            [],
        ]
    )
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: cache)

    async def _no_task(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sfl_mod, "_wait_for_subagent_task", _no_task)

    out = []
    async for ev in stream_subagent_from_log("t1", "abc", last_event_id=0):
        out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    assert len(payloads) == 1
    rendered = payloads[0]
    assert rendered.startswith("id: 5\n")
    assert "event: message_chunk\n" in rendered
    assert '"content": "delta"' in rendered
    assert '"agent": "task:abc"' in rendered


@pytest.mark.asyncio
async def test_subagent_consumer_exits_on_stream_end_sentinel(monkeypatch):
    """Producer writes a ``subagent_stream_end`` sentinel record when the
    forwarder finalises. Consumer must (a) not yield it to the wire and
    (b) exit the generator immediately so the SSE response closes — that
    flip is what marks the subagent card as completed on the frontend."""
    from src.server.handlers.chat import stream_from_log as sfl_mod

    real_event_bytes = b"id: 5\nevent: message_chunk\ndata: {\"content\": \"delta\", \"agent\": \"task:abc\"}\n\n"
    sentinel_bytes = json.dumps({"event": "subagent_stream_end"}).encode("utf-8")
    # An XREAD round delivering one real event followed by the sentinel.
    cache = _make_cache(
        [
            [
                (
                    b"subagent:stream:t1:abc",
                    [
                        (b"5-0", {b"event": real_event_bytes}),
                        (b"6-0", {b"event": sentinel_bytes}),
                    ],
                )
            ],
            # If the consumer doesn't exit on the sentinel it will keep
            # XREAD-ing — fail loudly by exhausting the side_effect rather
            # than hanging the test.
        ]
    )
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: cache)

    async def _no_task(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sfl_mod, "_wait_for_subagent_task", _no_task)

    out = []
    async for ev in stream_subagent_from_log("t1", "abc", last_event_id=0):
        out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    # Only the real event is yielded — the sentinel never reaches the wire.
    assert payloads == [real_event_bytes.decode("utf-8")]
    # And the consumer exited inside the same XREAD round, so xread was
    # called exactly once (no second round attempted).
    assert cache.client.xread.await_count == 1


@pytest.mark.asyncio
async def test_subagent_consumer_exits_on_sentinel_only_stream(monkeypatch):
    """A subagent that emits no tokens (e.g., immediate failure inside the
    handler) still has finalize() run, so the stream contains just the
    sentinel. Consumer must exit cleanly without yielding anything."""
    from src.server.handlers.chat import stream_from_log as sfl_mod

    sentinel_bytes = json.dumps({"event": "subagent_stream_end"}).encode("utf-8")
    cache = _make_cache(
        [
            [(b"subagent:stream:t1:abc", [(b"1-0", {b"event": sentinel_bytes})])],
        ]
    )
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: cache)

    async def _no_task(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sfl_mod, "_wait_for_subagent_task", _no_task)

    out = []
    async for ev in stream_subagent_from_log("t1", "abc", last_event_id=0):
        out.append(ev)

    payloads = [e for e in out if not e.startswith(":keepalive")]
    assert payloads == []


@pytest.mark.asyncio
async def test_stream_from_log_bumps_workflow_connection_counter(monkeypatch):
    """The workflow consumer must bump
    ``BackgroundTaskManager.active_connections`` on attach and decrement on
    detach. Without this, the abandoned-task cleanup at
    ``BackgroundTaskManager._periodic_cleanup`` would force-cancel any
    long-running RUNNING task whose client connected once and then went
    silent (no further reconnect calls to refresh ``last_access_at``)."""
    from src.server.handlers.chat import stream_from_log as sfl_mod
    from src.server.services.background_task_manager import (
        BackgroundTaskManager,
        TaskStatus,
    )

    cache = _make_cache([[], []])  # two empty rounds → terminal handshake
    monkeypatch.setattr(sfl_mod, "get_cache_client", lambda: cache)

    fake_manager = MagicMock()
    fake_manager.get_task_status = AsyncMock(return_value=TaskStatus.COMPLETED)
    fake_manager.increment_connection = AsyncMock(return_value=True)
    fake_manager.decrement_connection = AsyncMock(return_value=True)
    monkeypatch.setattr(
        BackgroundTaskManager, "get_instance", classmethod(lambda cls: fake_manager)
    )

    async for _ in stream_from_log("t-housekeeping", last_event_id=None):
        pass

    fake_manager.increment_connection.assert_awaited_once_with("t-housekeeping")
    fake_manager.decrement_connection.assert_awaited_once_with("t-housekeeping")


@pytest.mark.asyncio
async def test_attach_skipped_when_cache_disabled_no_detach_either(monkeypatch):
    """If the cache is disabled the generator returns immediately without
    invoking on_attach; on_detach must therefore not fire either (would leak
    a counter decrement onto a never-attached consumer)."""
    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    attach_calls = []
    detach_calls = []

    async def on_attach() -> None:
        attach_calls.append(True)

    async def on_detach() -> None:
        detach_calls.append(True)

    async def terminal() -> bool:
        return True

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        async for _ in _stream_from_redis_log(
            stream_key="k",
            terminal_check=terminal,
            last_event_id=0,
            on_attach=on_attach,
            on_detach=on_detach,
        ):
            pass

    assert attach_calls == []
    assert detach_calls == []


@pytest.mark.asyncio
async def test_disabled_cache_returns_empty_immediately():
    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    async def terminal() -> bool:
        return False

    with patch(
        "src.server.handlers.chat.stream_from_log.get_cache_client",
        return_value=cache,
    ):
        out = [
            ev
            async for ev in _stream_from_redis_log(
                stream_key="k",
                terminal_check=terminal,
                last_event_id=None,
            )
        ]
    assert out == []
