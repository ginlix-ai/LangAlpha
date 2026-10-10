"""The delta coalescer both event lanes share: what joins, and when it goes out."""

from __future__ import annotations

import asyncio

import pytest

from src.utils.stream_coalescing import (
    COALESCE_MAX_CHARS,
    HEARTBEAT_EVENT_TYPE,
    DeltaCoalescer,
    StreamFrame,
    stream_heartbeat,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _text(content: str, *, msg_id: str = "m1", content_type: str = "text", **extra):
    return StreamFrame(
        "message_chunk",
        {
            "thread_id": "t",
            "agent": "main",
            "id": msg_id,
            "role": "assistant",
            "content": content,
            "content_type": content_type,
            "finish_reason": None,
            **extra,
        },
    )


def _tool(args, *, index=0, call_id=None, name=None, msg_id: str = "m1"):
    return StreamFrame(
        "tool_call_chunks",
        {
            "thread_id": "t",
            "agent": "main",
            "id": msg_id,
            "role": "assistant",
            "tool_call_chunks": [
                {"name": name, "args": args, "id": call_id, "index": index, "type": "tool_call_chunk"}
            ],
        },
    )


def _feed(coalescer: DeltaCoalescer, frames) -> list[StreamFrame]:
    out: list[StreamFrame] = []
    for frame in frames:
        out.extend(coalescer.offer(frame))
    out.extend(coalescer.drain())
    return out


def test_text_and_reasoning_join_only_within_their_own_stream() -> None:
    out = _feed(
        DeltaCoalescer(clock=_Clock()),
        [
            _text("Let ", content_type="reasoning"),
            _text("me think", content_type="reasoning"),
            _text("The ", content_type="text"),
            _text("answer", content_type="text"),
            _text(" is", content_type="text", msg_id="m2"),
            _text(" 4", content_type="text", msg_id="m2", agent="task:abc"),
        ],
    )
    assert [(f.data["content_type"], f.data["id"], f.data["content"]) for f in out] == [
        ("reasoning", "m1", "Let me think"),
        ("text", "m1", "The answer"),
        ("text", "m2", " is"),
        ("text", "m2", " 4"),
    ]


def test_tool_args_join_by_index_including_id_less_continuations() -> None:
    out = _feed(
        DeltaCoalescer(clock=_Clock()),
        [
            _tool("", call_id="call-a", name="execute_code", index=0),
            _tool('{"co', index=0),
            _tool('de": 1}', index=0),
            # A new index is a new call.
            _tool("", call_id="call-b", name="bash", index=1),
            _tool('{"cmd"', index=1),
            # A new id at the same index is a new call too.
            _tool('{"x": 2}', call_id="call-c", name="bash", index=1),
        ],
    )
    chunks = [f.data["tool_call_chunks"] for f in out]
    assert chunks == [
        [{"name": "execute_code", "args": '{"code": 1}', "id": "call-a", "index": 0, "type": "tool_call_chunk"}],
        [{"name": "bash", "args": '{"cmd"', "id": "call-b", "index": 1, "type": "tool_call_chunk"}],
        [{"name": "bash", "args": '{"x": 2}', "id": "call-c", "index": 1, "type": "tool_call_chunk"}],
    ]


def test_a_multi_call_frame_is_never_held() -> None:
    both = StreamFrame(
        "tool_call_chunks",
        {
            "id": "m1",
            "tool_call_chunks": [
                {"args": "{", "index": 0},
                {"args": "{", "index": 1},
            ],
        },
    )
    coalescer = DeltaCoalescer(clock=_Clock())
    assert coalescer.offer(_tool("{", index=0, call_id="c", name="n")) == []
    out = coalescer.offer(both)
    assert [f.data["tool_call_chunks"][0]["args"] for f in out] == ["{", "{"]
    assert out[1] is both
    assert not coalescer.holding


def test_a_non_delta_flushes_the_hold_first_and_keeps_order() -> None:
    signal = StreamFrame(
        "message_chunk",
        {"id": "m1", "agent": "main", "content": "complete", "content_type": "reasoning_signal"},
    )
    tool_calls = StreamFrame("tool_calls", {"id": "m1", "tool_calls": []})
    out = _feed(
        DeltaCoalescer(clock=_Clock()),
        [
            _text("a", content_type="reasoning"),
            _text("b", content_type="reasoning"),
            signal,
            _text("c"),
            tool_calls,
        ],
    )
    assert [f.event for f in out] == ["message_chunk", "message_chunk", "message_chunk", "tool_calls"]
    assert out[0].data["content"] == "ab"
    assert out[1] is signal
    assert out[2].data["content"] == "c"
    assert out[3] is tool_calls


def test_finish_reason_goes_out_as_its_own_frame_after_the_joined_delta() -> None:
    finish = _text("", finish_reason="stop")
    finish.data.pop("content")
    out = _feed(DeltaCoalescer(clock=_Clock()), [_text("Hel"), _text("lo"), finish])
    assert [f.data.get("content") for f in out] == ["Hello", None]
    assert out[1] is finish


def test_a_finish_carrying_content_passes_through_unjoined() -> None:
    """The client ignores a finish that arrives with content, so joining one
    into the text before it would leave the message streaming."""
    last = _text("!", finish_reason="stop")
    out = _feed(DeltaCoalescer(clock=_Clock()), [_text("Hi"), last])
    assert [f.data["content"] for f in out] == ["Hi", "!"]
    assert out[1] is last


def test_a_held_delta_never_grows_past_the_char_cap() -> None:
    piece = "x" * 100
    out = _feed(DeltaCoalescer(clock=_Clock()), [_text(piece) for _ in range(12)])
    sizes = [len(f.data["content"]) for f in out]
    assert sum(sizes) == 1200
    assert all(size <= COALESCE_MAX_CHARS for size in sizes)
    assert sizes == [500, 500, 200]


def test_reaching_the_cap_exactly_sends_without_waiting() -> None:
    coalescer = DeltaCoalescer(clock=_Clock())
    assert coalescer.offer(_text("x" * 500)) == []
    out = coalescer.offer(_text("y" * 12))
    assert [len(f.data["content"]) for f in out] == [COALESCE_MAX_CHARS]
    assert not coalescer.holding


def test_one_oversized_piece_goes_out_whole() -> None:
    coalescer = DeltaCoalescer(clock=_Clock())
    out = coalescer.offer(_text("z" * 2000))
    assert [len(f.data["content"]) for f in out] == [2000]


def test_the_window_releases_a_hold_on_the_next_poll_or_offer() -> None:
    clock = _Clock()
    coalescer = DeltaCoalescer(clock=clock)
    assert coalescer.offer(_text("a")) == []
    clock.now = 0.05
    assert coalescer.offer(_text("b")) == []
    assert coalescer.poll() == []
    # The window runs from the first held piece, not the latest.
    clock.now = 0.1
    out = coalescer.poll()
    assert [f.data["content"] for f in out] == ["ab"]
    assert not coalescer.holding

    assert coalescer.offer(_text("c")) == []
    clock.now = 0.25
    out = coalescer.offer(_text("d"))
    # The due hold goes out first, then the new piece starts its own window.
    assert [f.data["content"] for f in out] == ["c"]
    assert coalescer.holding
    assert [f.data["content"] for f in coalescer.drain()] == ["d"]


def test_held_frames_are_copies_so_growing_one_leaves_the_caller_alone() -> None:
    first = _tool('{"a', call_id="c", name="n")
    coalescer = DeltaCoalescer(clock=_Clock())
    coalescer.offer(first)
    coalescer.offer(_tool('": 1}'))
    (out,) = coalescer.drain()
    assert out.data["tool_call_chunks"][0]["args"] == '{"a": 1}'
    assert first.data["tool_call_chunks"][0]["args"] == '{"a'


def test_discard_drops_the_hold() -> None:
    coalescer = DeltaCoalescer(clock=_Clock())
    coalescer.offer(_text("gone"))
    coalescer.discard()
    assert coalescer.drain() == []


@pytest.mark.asyncio
async def test_the_heartbeat_beats_only_while_its_block_runs() -> None:
    beats: list[dict] = []
    async with stream_heartbeat(beats.append, interval=0.01):
        while len(beats) < 3:
            await asyncio.sleep(0.005)
    seen = len(beats)
    await asyncio.sleep(0.05)
    assert len(beats) == seen
    assert beats[0] == {"type": HEARTBEAT_EVENT_TYPE}


@pytest.mark.asyncio
async def test_no_writer_means_no_heartbeat() -> None:
    async with stream_heartbeat(None, interval=0.01):
        await asyncio.sleep(0.03)
