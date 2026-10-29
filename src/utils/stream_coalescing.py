"""Joining streamed token deltas into fewer SSE frames.

Some models stream a tool call's arguments two or three characters per chunk
and their reasoning a few characters per chunk, and a run whose every chunk is
its own frame spends its stream's event quota on frames that carry almost
nothing. Both lanes, a run's own stream and each subagent's task stream, hold
one delta at a time and grow it with the pieces that continue it. The event
archive merges by a policy of its own (a byte cap instead of a window, and a
stricter rule for tool calls), but it reads which frames belong together
through the same keys, defined here.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, NamedTuple

# A held delta goes out once this much time has passed since its first piece,
# so a slow stream still reads as live.
COALESCE_WINDOW_S = 0.1

# The client shows an update of about 600 characters at once instead of
# typing it out, so a joined frame stays well under that.
COALESCE_MAX_CHARS = 512

# Written to a graph's custom stream while a model call runs. Both lanes wake
# on it and drop it.
HEARTBEAT_EVENT_TYPE = "stream_heartbeat"

_MESSAGE_EVENTS = frozenset({"message_chunk", "compaction_chunk"})
_TOOL_CALL_CHUNKS = "tool_call_chunks"
_REASONING_SIGNAL = "reasoning_signal"
_DELTA_CONTENT_TYPES = frozenset({"text", "reasoning"})

_MESSAGE_MERGE_KEYS = ("thread_id", "agent", "id", "role", "content_type", "phase")
_TOOL_MESSAGE_MERGE_KEYS = ("thread_id", "agent", "id")


class StreamFrame(NamedTuple):
    """One SSE event before it is numbered.

    ``ts`` rides along on a subagent's task stream, where each record carries
    its capture time. ``encoded`` is a run stream frame's JSON, serialized
    when the frame is built so a frame that cannot be fails there; a delta
    leaves it unset and is serialized when it goes out, joined or not.
    """

    event: str
    data: dict[str, Any]
    ts: float | None = None
    encoded: str | None = None


def same_message_stream(prev: dict[str, Any], incoming: dict[str, Any]) -> bool:
    """Whether two message chunks are pieces of one text or reasoning stream.

    A reasoning signal opens or closes a block and is never a piece of one.
    """
    if _REASONING_SIGNAL in (prev.get("content_type"), incoming.get("content_type")):
        return False
    return all(prev.get(k) == incoming.get(k) for k in _MESSAGE_MERGE_KEYS)


def tool_chunk_pair(
    prev: dict[str, Any], incoming: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """The two frames' tool-call chunks when both come from one assistant
    message and carry exactly one chunk each; otherwise None.

    Whether the two chunks are one call is the caller's rule.
    """
    if not all(prev.get(k) == incoming.get(k) for k in _TOOL_MESSAGE_MERGE_KEYS):
        return None
    prev_chunk = _single_tool_chunk(prev)
    chunk = _single_tool_chunk(incoming)
    if prev_chunk is None or chunk is None:
        return None
    return prev_chunk, chunk


def _single_tool_chunk(data: dict[str, Any]) -> dict[str, Any] | None:
    chunks = data.get(_TOOL_CALL_CHUNKS)
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
    if frame.event in _MESSAGE_EVENTS:
        content = data.get("content")
        if (
            isinstance(content, str)
            and content
            and data.get("content_type") in _DELTA_CONTENT_TYPES
        ):
            return content
        return None
    if frame.event == _TOOL_CALL_CHUNKS:
        chunk = _single_tool_chunk(data)
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

    The coalescer never writes into a frame it is given. A lone piece goes
    out as it came in; joined pieces go out as a new frame built from the
    first one when the hold is released.
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
        self._first: StreamFrame | None = None
        self._pieces: list[str] = []
        self._held_len = 0
        self._held_since = 0.0

    @property
    def holding(self) -> bool:
        return self._first is not None

    @property
    def due(self) -> bool:
        """Whether a held delta's window has passed, so ``poll`` releases it."""
        return (
            self._first is not None
            and self._clock() - self._held_since >= self._window_s
        )

    def offer(self, frame: StreamFrame) -> list[StreamFrame]:
        """Take the next frame and return the frames ready to go out, in order."""
        out = self.poll()
        piece = delta_piece(frame)
        if piece is None:
            out.extend(self.drain())
            out.append(frame)
            return out
        if (
            self._first is not None
            and self._held_len + len(piece) <= self._max_chars
            and self._continues(frame)
        ):
            self._pieces.append(piece)
            self._held_len += len(piece)
        else:
            out.extend(self.drain())
            self._first = frame
            self._pieces = [piece]
            self._held_len = len(piece)
            self._held_since = self._clock()
        if self._held_len >= self._max_chars:
            out.extend(self.drain())
        return out

    def poll(self) -> list[StreamFrame]:
        """Release the held delta once its window has passed."""
        return self.drain() if self.due else []

    def drain(self) -> list[StreamFrame]:
        """Release the held delta now."""
        first, pieces = self._first, self._pieces
        if first is None:
            return []
        self.discard()
        if len(pieces) == 1:
            return [first]
        return [_joined(first, "".join(pieces))]

    def discard(self) -> None:
        """Drop the held delta without releasing it."""
        self._first = None
        self._pieces = []
        self._held_len = 0

    def _continues(self, frame: StreamFrame) -> bool:
        first = self._first
        if first is None or first.event != frame.event:
            return False
        if frame.event != _TOOL_CALL_CHUNKS:
            return same_message_stream(first.data, frame.data)
        pair = tool_chunk_pair(first.data, frame.data)
        if pair is None:
            return False
        held_chunk, chunk = pair
        if held_chunk.get("index") != chunk.get("index"):
            return False
        # A new id or name starts another call even at the same index.
        return all(
            chunk.get(field) is None or chunk.get(field) == held_chunk.get(field)
            for field in ("id", "name")
        )


def _joined(first: StreamFrame, text: str) -> StreamFrame:
    if first.event == _TOOL_CALL_CHUNKS:
        (chunk,) = first.data[_TOOL_CALL_CHUNKS]
        data = {**first.data, _TOOL_CALL_CHUNKS: [{**chunk, "args": text}]}
    else:
        data = {**first.data, "content": text}
    return first._replace(data=data)
