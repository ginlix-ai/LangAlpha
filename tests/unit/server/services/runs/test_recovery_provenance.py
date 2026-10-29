"""The recovery scanner keeps what a dead run streamed only for what its
checkpoints saved: the provenance of the saved tool calls, and the reasoning
durations of the saved messages, keyed by the saved message ids.

A run that stopped or failed may have streamed past its last save, which the
turn it leaves never shows, so those sources are dropped. An interrupted
run's finished calls are committed by its resume, so it keeps them all, and a
source no tool call read (market watch) is always kept. With no stream
nothing is recorded; with no checkpoint to check against, every source is.

A stopped or failed run's fallback notices are kept as its finalize keeps
them, rebuilt from the main lane's frames that name their ui record.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from src.server.services.runs.recovery import RecoveryScanner
from src.server.services.runs.sse_producer import RunSSEProducer

THREAD, RUN = "t-lost", "r-lost"


def _wire(event: str, data: dict) -> dict:
    return {b"event": f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()}


def _source(tool_call_id: str, identifier: str) -> dict:
    return {
        "thread_id": THREAD,
        "agent": "main",
        "tool_call_id": tool_call_id,
        "source_type": "web",
        "identifier": identifier,
        "timestamp": "2026-10-13T10:00:00+00:00",
    }


def _chunk(chunk_id: str, content_type: str, content: str, **extra) -> dict:
    return {
        "agent": "model:3e17",
        "id": chunk_id,
        "role": "assistant",
        "content_type": content_type,
        "content": content,
        **extra,
    }


def _call(chunk_id: str, tool_call_id: str) -> dict:
    return {
        "agent": "model:3e17",
        "id": chunk_id,
        "tool_calls": [{"name": "WebFetch", "args": {}, "id": tool_call_id}],
    }


def _cache(entries):
    client = MagicMock()
    client.xrange = AsyncMock(return_value=entries)
    return SimpleNamespace(enabled=True, client=client)


def _reader(messages, *, error=None):
    reader = MagicMock()
    if error is not None:
        reader.aget_run_state = AsyncMock(side_effect=error)
    else:
        reader.aget_run_state = AsyncMock(
            return_value=SimpleNamespace(values={"messages": messages})
        )
    return reader


# The first fetch's result was saved; the worker died after streaming the
# second's provenance and before saving its result. Both calls' messages,
# with the thinking that led to them, were saved.
_SAVED_STATE = [
    AIMessage(
        content="",
        id="ai-1",
        tool_calls=[{"name": "WebFetch", "args": {}, "id": "tc-1"}],
    ),
    ToolMessage(content="page one", tool_call_id="tc-1", id="tm-1"),
    AIMessage(
        content="",
        id="ai-2",
        tool_calls=[{"name": "WebFetch", "args": {}, "id": "tc-2"}],
    ),
]
_STREAM = [
    (b"1-0", _wire("message_chunk", _chunk("lc_run--a", "reasoning_signal", "start"))),
    (
        b"2-0",
        _wire(
            "message_chunk",
            _chunk("lc_run--a", "reasoning_signal", "complete", elapsed_ms=291),
        ),
    ),
    (b"3-0", _wire("tool_calls", _call("lc_run--a", "tc-1"))),
    (b"4-0", _wire("provenance", _source("tc-1", "https://one.test"))),
    (
        b"5-0",
        _wire(
            "message_chunk",
            _chunk("lc_run--b", "reasoning_signal", "complete", elapsed_ms=117),
        ),
    ),
    (b"6-0", _wire("tool_calls", _call("lc_run--b", "tc-2"))),
    (b"7-0", _wire("provenance", _source("tc-2", "https://two.test"))),
]


async def _salvage(entries, reader, status="error"):
    with (
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=_cache(entries),
        ),
        patch(
            "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
            return_value=reader,
        ),
    ):
        return await RecoveryScanner()._salvage(THREAD, RUN, status)


@pytest.mark.asyncio
async def test_only_the_saved_tool_calls_provenance_is_kept():
    reader = _reader(_SAVED_STATE)

    record, quality = await _salvage(_STREAM, reader)

    assert record.provenance == [
        {"event": "provenance", "data": _source("tc-1", "https://one.test")}
    ]
    assert quality == "salvaged"
    reader.aget_run_state.assert_awaited_once_with(THREAD, RUN)


@pytest.mark.asyncio
async def test_an_interrupted_run_keeps_every_source():
    record, _ = await _salvage(_STREAM, _reader(_SAVED_STATE), status="interrupted")

    assert [e["data"]["tool_call_id"] for e in record.provenance] == ["tc-1", "tc-2"]


@pytest.mark.asyncio
async def test_durations_are_kept_by_saved_message_for_saved_messages_only():
    # The call the worker died in thought too, but saved no message.
    dying = [
        (
            b"8-0",
            _wire(
                "message_chunk",
                _chunk("lc_run--c", "reasoning_signal", "complete", elapsed_ms=50),
            ),
        ),
        (b"9-0", _wire("tool_calls", _call("lc_run--c", "tc-3"))),
    ]

    record, _ = await _salvage(_STREAM + dying, _reader(_SAVED_STATE))

    assert record.reasoning_ms == {"ai-1": [291], "ai-2": [117]}


@pytest.mark.asyncio
async def test_a_message_without_tool_calls_is_matched_by_its_text():
    # The saved copy names the chart by the URL it was captured to.
    saved = _SAVED_STATE + [
        AIMessage(content="Done. ![chart](https://cdn.example.com/c.png)", id="ai-3")
    ]
    final = [
        (
            b"8-0",
            _wire(
                "message_chunk",
                _chunk("lc_run--c", "reasoning_signal", "complete", elapsed_ms=40),
            ),
        ),
        (b"9-0", _wire("message_chunk", _chunk("lc_run--c", "text", "Done. ![ch"))),
        (b"10-0", _wire("message_chunk", _chunk("lc_run--c", "text", "art](c.png)"))),
    ]

    record, _ = await _salvage(_STREAM + final, _reader(saved))

    assert record.reasoning_ms == {"ai-1": [291], "ai-2": [117], "ai-3": [40]}


@pytest.mark.asyncio
async def test_a_source_no_tool_call_read_is_kept():
    watched = {
        **_source("", "AAPL"),
        "tool_call_id": None,
        "source_type": "market_data",
    }
    stream = _STREAM + [(b"8-0", _wire("provenance", watched))]

    record, _ = await _salvage(stream, _reader(_SAVED_STATE))

    assert [e["data"]["identifier"] for e in record.provenance] == [
        "https://one.test",
        "AAPL",
    ]


@pytest.mark.asyncio
async def test_a_repeated_text_gives_the_dying_call_no_durations():
    # The worker died in a call that streamed the same words as the saved
    # message before it; the saved message keeps its own duration alone.
    saved = _SAVED_STATE + [AIMessage(content="Checking.", id="ai-3")]

    def _said(chunk_id: str, entry: int, elapsed: int) -> list:
        return [
            (
                f"{entry}-0".encode(),
                _wire(
                    "message_chunk",
                    _chunk(
                        chunk_id, "reasoning_signal", "complete", elapsed_ms=elapsed
                    ),
                ),
            ),
            (
                f"{entry}-1".encode(),
                _wire("message_chunk", _chunk(chunk_id, "text", "Checking.")),
            ),
        ]

    stream = _STREAM + _said("lc_run--c", 8, 100) + _said("lc_run--d", 9, 900)

    record, _ = await _salvage(stream, _reader(saved))

    assert record.reasoning_ms == {"ai-1": [291], "ai-2": [117], "ai-3": [100]}


@pytest.mark.asyncio
async def test_a_failed_checkpoint_read_records_every_source_and_no_durations():
    record, quality = await _salvage(
        _STREAM, _reader([], error=RuntimeError("pool closed"))
    )

    assert [e["data"]["tool_call_id"] for e in record.provenance] == ["tc-1", "tc-2"]
    assert record.reasoning_ms is None
    assert quality == "salvaged"


@pytest.mark.asyncio
async def test_a_run_that_saved_nothing_records_none():
    reader = _reader([])
    reader.aget_run_state = AsyncMock(return_value=None)

    record, _ = await _salvage(_STREAM, reader)

    assert record.provenance == []
    assert record.reasoning_ms == {}


@pytest.mark.asyncio
async def test_no_stream_records_none_without_reading_the_checkpoint():
    reader = _reader(_SAVED_STATE)

    record, quality = await _salvage([], reader)

    assert (record.provenance, record.reasoning_ms, quality) == ([], None, "empty_stream")
    reader.aget_run_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_lost_run_is_finalized_with_its_saved_messages_durations():
    tip = SimpleNamespace(
        metadata={"run_id": RUN},
        config={"configurable": {"checkpoint_id": "cp-tip"}},
        pending_writes=[],
    )
    finalize = AsyncMock(return_value=SimpleNamespace(applied=True, run={}))
    with (
        patch(
            "src.server.app.setup.checkpointer",
            SimpleNamespace(aget_tuple=AsyncMock(return_value=tip)),
        ),
        patch(
            "src.server.services.runs.coordinator.RunCoordinator.get_instance",
            return_value=SimpleNamespace(finalize_detached_run=finalize),
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=_cache(_STREAM),
        ),
        patch(
            "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
            return_value=_reader(_SAVED_STATE),
        ),
    ):
        await RecoveryScanner()._recover_run({}, RUN, THREAD)

    outcome = finalize.await_args.args[2]
    assert outcome.status == "error"
    assert outcome.record.reasoning_ms == {"ai-1": [291], "ai-2": [117]}
    assert [e["data"]["tool_call_id"] for e in outcome.record.provenance] == ["tc-1"]


# ─── Fallback notices ─────────────────────────────────────────────────


class _Graph:
    """Streams each ``(namespace, mode, payload)`` step to the producer."""

    def __init__(self, *steps):
        self._steps = steps

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _gen():
            for step in self._steps:
                yield step

        return _gen()


async def _streamed_fallback():
    """A producer that streamed one main-lane notice, and the stream it left."""
    handler = RunSSEProducer(thread_id=THREAD, run_id=RUN)
    notice = {
        "type": "ui",
        "id": "fb-main",
        "name": "model_fallback",
        "props": {
            "from_model": "primary",
            "to_model": "backup",
            "from_is_primary": True,
            "error": "429 at https://user:sekret@api.example.com/v1",
            "status_code": 429,
            "attempts_on_from": 3,
        },
        "metadata": {"merge": False, "tags": ["t"], "after_message_id": "ai-2"},
    }
    frames = [
        frame
        async for frame in handler.stream_workflow(
            _Graph(((), "custom", notice)), input_state={}, config={}
        )
    ]
    entries = [
        (f"{100 + i}-0".encode(), {b"event": frame.encode()})
        for i, frame in enumerate(frames)
    ]
    return handler, entries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kept"),
    [("error", True), ("cancelled", True), ("interrupted", False)],
)
async def test_a_stopped_or_failed_runs_notices_are_kept_as_its_finalize_keeps_them(
    status, kept
):
    # Skipped: another lane's notice, which its own run keeps, and a frame
    # an older server streamed, which names no record replay could match.
    handler, streamed = await _streamed_fallback()
    skipped = [
        (
            b"200-0",
            _wire(
                "model_fallback",
                {"agent": "task:k7", "fallback_id": "fb-task", "to_model": "b"},
            ),
        ),
        (b"201-0", _wire("model_fallback", {"agent": "main", "to_model": "b"})),
    ]

    record, _ = await _salvage(
        streamed + skipped, _reader(_SAVED_STATE), status=status
    )

    finalized = handler.record().model_fallbacks
    assert [r["id"] for r in finalized] == ["fb-main"]
    assert "sekret" not in json.dumps(finalized)
    assert record.model_fallbacks == (finalized if kept else [])


# ─── The error frame ──────────────────────────────────────────────────


class _FailingGraph:
    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _gen():
            raise RuntimeError("boom")
            yield  # pragma: no cover

        return _gen()


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "kept"), [("error", True), ("cancelled", False)])
async def test_a_failed_runs_error_frame_is_kept_as_its_finalize_keeps_it(
    status, kept
):
    handler = RunSSEProducer(thread_id=THREAD, run_id=RUN)
    frames = []
    with pytest.raises(RuntimeError):
        async for frame in handler.stream_workflow(
            _FailingGraph(), input_state={}, config={}
        ):
            frames.append(frame)
    entries = [
        (f"{100 + i}-0".encode(), {b"event": frame.encode()})
        for i, frame in enumerate(frames)
    ]
    # Another lane's failure belongs to its own card.
    entries.append((b"200-0", _wire("error", {"agent": "task:k7", "error": "lane"})))

    record, _ = await _salvage(entries, _reader(_SAVED_STATE), status=status)

    finalized = handler.record().error_frame
    assert finalized["error"] == "boom"
    assert record.error_frame == (finalized if kept else None)
