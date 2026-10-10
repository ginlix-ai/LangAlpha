"""Joining streamed token deltas into fewer SSE frames.

Some models stream a tool call's arguments two or three characters per chunk
and their reasoning a few characters per chunk, and a run whose every chunk is
its own frame spends its stream's event quota on frames that carry almost
nothing. Both lanes, a run's own stream and each subagent's task stream, hold
one delta at a time and grow it with the pieces that continue it. The merge
keys live here so the live lanes and the persisted transcript agree on what
one stream of pieces is.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, NamedTuple

# A held delta goes out once this much time has passed since its first piece,
# so a slow stream still reads as live.
COALESCE_WINDOW_S = 0.1

# The client shows an update of about 600 characters at once instead of
# typing it out, so a joined frame stays well under that.
COALESCE_MAX_CHARS = 512

# Written to a graph's custom stream while a model call runs. Both lanes wake
# on it and drop it, as a custom type they do not forward.
HEARTBEAT_EVENT_TYPE = "stream_heartbeat"

MESSAGE_EVENTS = frozenset({"message_chunk", "compaction_chunk"})
TOOL_CALL_CHUNKS = "tool_call_chunks"
REASONING_SIGNAL = "reasoning_signal"
_DELTA_CONTENT_TYPES = frozenset({"text", "reasoning"})

MESSAGE_MERGE_KEYS = ("thread_id", "agent", "id", "role", "content_type", "phase")
TOOL_MESSAGE_MERGE_KEYS = ("thread_id", "agent", "id")


class StreamFrame(NamedTuple):
    """One SSE event before it is numbered. ``ts`` rides along on a
    subagent's task stream, where each record carries its capture time."""

    event: str
    data: dict[str, Any]
    ts: float | None = None


def same_message_stream(prev: dict[str, Any], incoming: dict[str, Any]) -> bool:
    """Whether two message chunks are pieces of one text or reasoning stream.

    A reasoning signal opens or closes a block and is never a piece of one.
    """
    if REASONING_SIGNAL in (prev.get("content_type"), incoming.get("content_type")):
        return False
    return all(prev.get(k) == incoming.get(k) for k in MESSAGE_MERGE_KEYS)


def same_tool_message(prev: dict[str, Any], incoming: dict[str, Any]) -> bool:
    """Whether two tool_call_chunks frames belong to one assistant message."""
    return all(prev.get(k) == incoming.get(k) for k in TOOL_MESSAGE_MERGE_KEYS)


def single_tool_chunk(data: dict[str, Any]) -> dict[str, Any] | None:
    """The frame's only tool-call chunk; None when it carries zero or several."""
    chunks = data.get(TOOL_CALL_CHUNKS)
    if isinstance(chunks, list) and len(chunks) == 1 and isinstance(chunks[0], dict):
        return chunks[0]
    return None


def delta_piece(frame: StreamFrame) -> str | None:
    """The text a frame adds to a stream, or None when it is not a delta.

    A frame that carries a ``finish_reason`` is never a delta: the client
    ignores a finish that arrives with content, so joining one into the text
    before it would leave the message streaming forever.
    """
    data = frame.data
    if not isinstance(data, dict) or data.get("finish_reason") is not None:
        return None
    if frame.event in MESSAGE_EVENTS:
        content = data.get("content")
        if (
            isinstance(content, str)
            and content
            and data.get("content_type") in _DELTA_CONTENT_TYPES
        ):
            return content
        return None
    if frame.event == TOOL_CALL_CHUNKS:
        chunk = single_tool_chunk(data)
        if chunk is None:
            return None
        args = chunk.get("args")
        if args is None:
            return ""
        return args if isinstance(args, str) else None
    return None


class DeltaCoalescer:
    """Holds at most one streaming delta and grows it with the pieces that
    continue it.

    Frames come out in the order they went in, and only consecutive pieces of
    one stream are joined, so every boundary a non-delta frame marks is kept.
    There is no timer: the caller polls whenever an event arrives, so a held
    piece waits for the next one at most.

    Tool-call pieces join by ``index``, and a piece with no id and no name
    continues the one that named the call. That is the shape a model streams,
    and the client keys these chunks by index and appends their args, so it
    reads a joined chunk as it read the pieces.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        window_s: float = COALESCE_WINDOW_S,
        max_chars: int = COALESCE_MAX_CHARS,
    ) -> None:
        self._clock = clock
        self._window_s = window_s
        self._max_chars = max_chars
        self._held: StreamFrame | None = None
        self._held_len = 0
        self._held_since = 0.0

    @property
    def holding(self) -> bool:
        return self._held is not None

    def offer(self, frame: StreamFrame) -> list[StreamFrame]:
        """Take the next frame and return the frames ready to go out, in order."""
        out = self.poll()
        piece = delta_piece(frame)
        if piece is None:
            out.extend(self.drain())
            out.append(frame)
            return out
        if (
            self._held is not None
            and self._held_len + len(piece) <= self._max_chars
            and self._continues(frame)
        ):
            self._extend(piece)
        else:
            out.extend(self.drain())
            self._hold(frame, piece)
        if self._held_len >= self._max_chars:
            out.extend(self.drain())
        return out

    def poll(self) -> list[StreamFrame]:
        """Release the held delta once its window has passed."""
        if self._held is not None and self._clock() - self._held_since >= self._window_s:
            return self.drain()
        return []

    def drain(self) -> list[StreamFrame]:
        """Release the held delta now."""
        held = self._held
        if held is None:
            return []
        self._held = None
        self._held_len = 0
        return [held]

    def discard(self) -> None:
        """Drop the held delta without releasing it."""
        self._held = None
        self._held_len = 0

    def _continues(self, frame: StreamFrame) -> bool:
        held = self._held
        if held is None or held.event != frame.event:
            return False
        if frame.event != TOOL_CALL_CHUNKS:
            return same_message_stream(held.data, frame.data)
        if not same_tool_message(held.data, frame.data):
            return False
        held_chunk = single_tool_chunk(held.data)
        chunk = single_tool_chunk(frame.data)
        if held_chunk is None or chunk is None:
            return False
        if held_chunk.get("index") != chunk.get("index"):
            return False
        # A new id or name starts another call even at the same index.
        return all(
            chunk.get(field) is None or chunk.get(field) == held_chunk.get(field)
            for field in ("id", "name")
        )

    def _hold(self, frame: StreamFrame, piece: str) -> None:
        # Copies, because the held frame is grown in place and the incoming
        # dicts may belong to the caller (a model chunk's own tool_call_chunks).
        data = dict(frame.data)
        if frame.event == TOOL_CALL_CHUNKS:
            data[TOOL_CALL_CHUNKS] = [dict(data[TOOL_CALL_CHUNKS][0])]
        self._held = StreamFrame(frame.event, data, frame.ts)
        self._held_len = len(piece)
        self._held_since = self._clock()

    def _extend(self, piece: str) -> None:
        held = self._held
        if held is None:
            return
        data = held.data
        if held.event == TOOL_CALL_CHUNKS:
            chunk = data[TOOL_CALL_CHUNKS][0]
            chunk["args"] = (chunk.get("args") or "") + piece
        else:
            data["content"] = data["content"] + piece
        self._held_len += len(piece)


@asynccontextmanager
async def stream_heartbeat(
    writer: Callable[[Any], None] | None, interval: float = COALESCE_WINDOW_S
) -> AsyncIterator[None]:
    """Write a heartbeat to a graph's custom stream every ``interval`` seconds
    while the block runs.

    A lane checks its held delta's window only when it wakes for a graph
    event, and a model can go quiet mid-message for seconds (between
    reasoning-summary parts, between thinking bursts) with its last words
    held. The beat wakes the lane so they go out on time.
    """
    if writer is None:
        yield
        return

    async def beat() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                writer({"type": HEARTBEAT_EVENT_TYPE})
            except Exception:
                return  # the run's stream is gone; nothing is left to wake

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
