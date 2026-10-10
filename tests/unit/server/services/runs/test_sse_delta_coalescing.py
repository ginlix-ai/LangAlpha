"""The run stream joins streamed deltas before numbering them.

Pins the producer seam: frames are built (and accumulated for persistence) one
per piece, then held and joined, and numbered only as they leave, so ids stay
contiguous whatever was joined.
"""

from __future__ import annotations

import json

import pytest
from langchain_core.messages import AIMessageChunk

from src.server.services.runs.sse_producer import RunSSEProducer
from src.utils.stream_coalescing import HEARTBEAT_EVENT_TYPE, DeltaCoalescer


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Graph:
    """Yields scripted ``(namespace, mode, data)`` events. Each step may set
    the clock first, and records how many frames the consumer had received
    when the graph was asked for it."""

    def __init__(self, steps, *, clock=None, received=None, raise_at_end=None):
        self._steps = steps
        self._clock = clock
        self._received = received
        self._raise_at_end = raise_at_end
        self.seen_before: list[int] = []

    def astream(self, *_args, **_kwargs):
        async def _gen():
            for at, event in self._steps:
                if self._received is not None:
                    self.seen_before.append(len(self._received))
                if self._clock is not None and at is not None:
                    self._clock.now = at
                yield event
            if self._raise_at_end is not None:
                raise self._raise_at_end

        return _gen()


def _msg(content, *, msg_id="m1", **kwargs):
    return ((), "messages", (AIMessageChunk(content=content, id=msg_id, **kwargs), {}))


def _reasoning(text):
    return _msg("", additional_kwargs={"reasoning_content": text})


def _updates():
    return ((), "updates", {"model": {}})


def _parse(frame: str) -> tuple[int, str, dict]:
    lines = frame.split("\n")
    return (
        int(lines[0].removeprefix("id: ")),
        lines[1].removeprefix("event: "),
        json.loads(lines[2].removeprefix("data: ")),
    )


def _producer(clock=None) -> RunSSEProducer:
    producer = RunSSEProducer(thread_id="t-co", run_id="r-co")
    if clock is not None:
        producer._coalescer = DeltaCoalescer(clock=clock)
    return producer


async def _run(producer, graph, received=None) -> list[tuple[int, str, dict]]:
    out = received if received is not None else []
    async for frame in producer.stream_workflow(graph, input_state={}, config={}):
        out.append(frame)
    return [_parse(f) for f in out]


def _steps(*events):
    return [(None, e) for e in events]


@pytest.mark.asyncio
async def test_deltas_join_and_ids_stay_contiguous() -> None:
    producer = _producer(_Clock())
    frames = await _run(
        producer,
        _Graph(
            _steps(
                _reasoning("Let "),
                _reasoning("me "),
                _reasoning("think"),
                _msg("Hel"),
                _msg("lo"),
                _msg(" world"),
                _msg("", response_metadata={"finish_reason": "stop"}),
            )
        ),
    )
    assert [seq for seq, _, _ in frames] == list(range(1, len(frames) + 1))
    shapes = [
        (event, data.get("content_type"), data.get("content"), data.get("finish_reason"))
        for _, event, data in frames
    ]
    assert shapes == [
        ("metadata", None, None, None),
        ("message_chunk", "reasoning_signal", "start", None),
        ("message_chunk", "reasoning", "Let me think", None),
        ("message_chunk", "reasoning_signal", "complete", None),
        ("message_chunk", "text", "Hello world", None),
        # The finish goes out on its own, after the joined text.
        ("message_chunk", None, None, "stop"),
    ]
    assert producer.event_sequence == len(frames)


@pytest.mark.asyncio
async def test_streamed_tool_args_join_into_one_frame() -> None:
    def block(item):
        return _msg([item])

    frames = await _run(
        _producer(_Clock()),
        _Graph(
            _steps(
                block({"type": "tool_use", "id": "call-1", "name": "bash", "index": 1}),
                block({"type": "input_json_delta", "partial_json": '{"co', "index": 1}),
                block({"type": "input_json_delta", "partial_json": 'mmand"', "index": 1}),
                block({"type": "input_json_delta", "partial_json": ': "ls"}', "index": 1}),
            )
        ),
    )
    chunks = [data["tool_call_chunks"] for _, event, data in frames if event == "tool_call_chunks"]
    assert chunks == [[{"args": '{"command": "ls"}', "index": 1, "type": "tool_call_chunk"}]]


@pytest.mark.asyncio
async def test_the_held_delta_goes_out_when_the_stream_ends() -> None:
    frames = await _run(_producer(_Clock()), _Graph(_steps(_msg("tail "), _msg("piece"))))
    assert [(seq, event, data.get("content")) for seq, event, data in frames] == [
        (1, "metadata", None),
        (2, "message_chunk", "tail piece"),
    ]


@pytest.mark.asyncio
async def test_a_failing_graph_still_sends_the_hold_before_its_error() -> None:
    producer = _producer(_Clock())
    received: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        await _run(
            producer,
            _Graph(_steps(_msg("partial "), _msg("answer")), raise_at_end=RuntimeError("boom")),
            received,
        )
    frames = [_parse(f) for f in received]
    assert [seq for seq, _, _ in frames] == [1, 2, 3]
    assert [(event, data.get("content")) for _, event, data in frames[1:]] == [
        ("message_chunk", "partial answer"),
        ("error", None),
    ]


@pytest.mark.asyncio
async def test_the_window_is_checked_on_events_that_emit_nothing() -> None:
    clock = _Clock()
    received: list[str] = []
    graph = _Graph(
        [
            (0.0, _msg("a")),
            # A state update emits no frame, but its arrival is past the
            # window, so the held "a" must go out before the graph moves on.
            (0.15, _updates()),
            (0.16, _msg("b")),
        ],
        clock=clock,
        received=received,
    )
    frames = await _run(_producer(clock), graph, received)
    # Asked for the third event, the graph sees metadata and "a" delivered.
    assert graph.seen_before == [1, 1, 2]
    assert [data.get("content") for _, _, data in frames[1:]] == ["a", "b"]


@pytest.mark.asyncio
async def test_a_close_loses_the_hold_live_but_not_from_the_persisted_copy() -> None:
    producer = _producer(_Clock())
    stream = producer.stream_workflow(
        _Graph(_steps(_msg("one"), _msg("two", msg_id="m2"), _msg(" more", msg_id="m2"))),
        input_state={},
        config={},
    )
    live = [_parse(await stream.__anext__()) for _ in range(2)]
    # "two" started another message, which released "one" and is held now.
    await stream.aclose()
    assert [data.get("content") for _, _, data in live] == [None, "one"]
    persisted = producer.get_sse_events()
    assert [e["data"]["content"] for e in persisted] == ["one", "two"]


@pytest.mark.asyncio
async def test_a_frame_that_fails_to_serialize_fails_the_run_with_an_error_frame() -> None:
    """Serializing happens as frames leave the coalescer, outside the body, so
    the failure is handed back to the body to report as its own."""
    unserializable = (
        (),
        "custom",
        {"artifact_type": "chart", "payload": {"made": object()}},
    )
    received: list[str] = []
    with pytest.raises(TypeError):
        await _run(
            _producer(_Clock()),
            _Graph(_steps(_msg("before"), unserializable, _msg("never sent"))),
            received,
        )
    frames = [_parse(f) for f in received]
    assert [(eid, event) for eid, event, _ in frames] == [
        (1, "metadata"),
        (2, "message_chunk"),
        (3, "error"),
    ]


@pytest.mark.asyncio
async def test_a_heartbeat_sends_the_held_delta_through_a_pause() -> None:
    clock = _Clock()
    received: list[str] = []
    heartbeat = ((), "custom", {"type": HEARTBEAT_EVENT_TYPE})
    graph = _Graph(
        [
            (0.0, _msg("before the pause")),
            # The model is quiet; only its call's heartbeat arrives.
            (0.05, heartbeat),
            (0.12, heartbeat),
            (3.0, _msg(" and after")),
        ],
        clock=clock,
        received=received,
    )
    frames = await _run(_producer(clock), graph, received)
    # Asked for the words after the pause, the graph sees the ones before it
    # delivered: the second beat is past the window.
    assert graph.seen_before == [1, 1, 1, 2]
    # The beat itself is never a frame.
    assert [(event, data.get("content")) for _, event, data in frames] == [
        ("metadata", None),
        ("message_chunk", "before the pause"),
        ("message_chunk", " and after"),
    ]
