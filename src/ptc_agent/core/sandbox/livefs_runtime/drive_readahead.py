"""Which blocks of a large file to fetch ahead of its readers: pure, so it is
tested without threads. One per file, called under the cache's lock.

A read continues a stream when it starts within ``STREAM_SLACK`` or two
blocks of the stream's front, behind or ahead, since the kernel's own
read-ahead sends a reader's pages out of order over several loop threads.
A stream counts as sequential only while it has read at least half the span
it covers: a columnar reader taking one column's chunk from each row group
also moves forward within the slack, but reads a tenth of what it passes,
and fetching ahead for it would fetch the whole file.
"""

from __future__ import annotations

from collections.abc import Callable

#: How far from a stream's front a read may start and still continue it:
#: twice the kernel's read-ahead, whose requests arrive out of order.
STREAM_SLACK = 8 << 20
#: Streams followed per file at most, the least recently read dropped first.
STREAMS_KEPT = 16
#: A stream nothing read for this long is another reader's, gone: forgotten,
#: so a sequential reader meeting what it left (random reads leave one every
#: few MB) does not count as sparse.
STREAM_FORGET_S = 2.0
#: What was fetched ahead of a stream nothing read for this long is
#: cancelled: its reader went quiet or moved elsewhere.
AHEAD_IDLE_S = 2.0


class Stream:
    """A reader going through a file in order, as far as reads show one."""

    __slots__ = ("start", "front", "run", "at")

    def __init__(self, start: int, front: int, run: int, at: float) -> None:
        self.start = start
        self.front = front
        self.run = run
        self.at = at


class ReadAhead:
    def __init__(self, size: int, block: int, readahead: int) -> None:
        self.size = size
        self.block = block
        self.readahead = readahead
        self.streams: list[Stream] = []

    def observe(self, offset: int, end: int, now: float, missing: Callable[[int], bool],
                most: int) -> tuple[Stream, list[int]]:
        """The read of ``offset`` to ``end``: its stream, and the blocks to
        fetch ahead of it now (``missing``: neither held nor being fetched).
        As many bytes ahead as the stream has read past its first block, up
        to ``readahead``, counted from its front. The window slides a block
        at a time, and asking for each block as it opens pays a first byte
        per block, so blocks are asked for once a request's worth (``most``)
        is missing or the window reaches the end; a short stream (one row
        group of a columnar file, read by one of the reader's threads) never
        opens a request's worth, so its window is the most it waits for.
        Blocks already held in the window (left by earlier random reads)
        keep it from ever missing a request's worth, so a missing block in
        the nearer half of the window goes anyway, before the reader reaches
        it."""
        block = self.block
        slack = max(2 * block, STREAM_SLACK)
        self.streams = [s for s in self.streams if now - s.at <= STREAM_FORGET_S]
        stream = next((s for s in self.streams if s.front - slack <= offset <= s.front + slack), None)
        if stream is None:
            stream = Stream(offset, end, end - offset, now)
            self.streams.append(stream)
            if len(self.streams) > STREAMS_KEPT:
                self.streams.pop(0)
        else:
            self.streams.remove(stream)
            self.streams.append(stream)
            stream.start = min(stream.start, offset)
            stream.front = max(stream.front, end)
            # A read behind the front fills a gap a reordered read left.
            stream.run += end - offset
            stream.at = now
        if 2 * stream.run < stream.front - stream.start:
            return stream, []
        last = (end - 1) // block
        depth = -(-min(self.readahead, max(stream.run - block, 0)) // block)
        first = max(last, (stream.front - 1) // block) + 1
        window = range(first, min(-(-self.size // block), first + depth))
        ahead = [i for i in window if missing(i)]
        if not ahead:
            return stream, []
        batch = min(most, self.readahead, len(window) * block)
        near = 2 * (ahead[0] - last) <= len(window)
        if len(ahead) * block >= batch or near or window.stop * block >= self.size:
            return stream, ahead
        return stream, []

    def live(self, stream: Stream, now: float) -> bool:
        """Whether a fetch ahead of ``stream`` is still wanted."""
        return stream in self.streams and now - stream.at <= AHEAD_IDLE_S
