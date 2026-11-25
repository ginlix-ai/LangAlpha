"""Large binaries read a block at a time, never whole into memory.

A binary over ``whole_max`` is read through its partial file in the cache
(``drive_cache``): a read waits only for the blocks it covers, fetched as
ranges (neighbours missing together go in one request), and a reader going
through a file in order has the blocks after it fetched ahead
(``drive_readahead``), so a loop thread never waits on more than it asked
for. Every fetch claims its bytes from the cache first, so fetches running
at once cannot pass its budget or its disk reserve. A fetch that fails, or
is cancelled because the reader it ran ahead of went quiet or moved
elsewhere, gives its claim back and punches out whatever it wrote. The
fetch that brings a file's last block finishes it into a whole file.

Every fetch's start, join, cancel and end happens under the cache's lock
(``drive_cache``), so a read either joins a fetch before it is cancelled,
and keeps it from being cancelled, or finds its blocks free to fetch again.
"""

from __future__ import annotations

import errno
import itertools
import os
import queue
import sys
import threading
import time
from collections import Counter

from .drive_cache import Cache, Item
from .drive_disk import DiskFull, write_at
from .drive_readahead import ReadAhead, Stream
from .drive_source import Gone, Source
from .drive_tree import Entry, Space
from .remote import fail

#: A binary at most this large is fetched whole in one request; a larger one
#: is read by block. At 30 MB/s a whole fetch of this size holds the reading
#: loop thread about 0.15 s.
WHOLE_MAX = 4 << 20
#: Bytes fetched ahead of a reader going through a file in order, at most.
READAHEAD = 64 << 20
#: Fetches under way at once, each on its own connection. A columnar reader
#: keeps one read per thread of its own in flight: at 80 ms to first byte a
#: cold scan of a 5 GB Parquet took 59 s with 8 and 22 s with 32.
CONNECTIONS = 32
#: What a fetch reads from its connection and writes to disk at a time.
CHUNK = 1 << 20
#: How long a read waits for its blocks before failing with EIO.
WAIT_S = 120.0


class _Cancelled(Exception):
    """A fetch ahead of a reader that went quiet or moved elsewhere."""


class Fetch:
    __slots__ = ("space", "item", "first", "last", "done", "error", "data", "stream", "waiters")

    def __init__(self, space: Space, item: Item, first: int, last: int, stream: Stream | None) -> None:
        self.space = space
        self.item = item
        self.first = first
        self.last = last
        self.done = threading.Event()
        self.error: OSError | None = None
        #: The bytes, when the cache had no room to keep them.
        self.data: bytes | None = None
        #: Run ahead of ``stream`` rather than asked for by a read.
        self.stream = stream
        #: Reads waiting on it: one waited on is never cancelled.
        self.waiters = 0


class Fetcher:
    def __init__(self, cache: Cache, source: Source, counts: Counter[str], *, readahead: int = READAHEAD,
                 connections: int = CONNECTIONS, whole_max: int = WHOLE_MAX) -> None:
        self.cache = cache
        self.source = source
        self.counts = counts
        self.block = cache.block
        self.readahead = readahead
        self.connections = connections
        self.whole_max = whole_max
        self._queue: queue.PriorityQueue = queue.PriorityQueue()
        self._order = itertools.count()
        self._workers: list[threading.Thread] = []
        self._start = threading.Lock()

    def large(self, entry: Entry) -> bool:
        return not entry.is_text and entry.size > self.whole_max

    # --- reads ------------------------------------------------------------------

    def read(self, space: Space, sha: str, total: int, size: int, offset: int) -> bytes:
        """``size`` bytes from ``offset`` of version ``sha`` (``total`` long)."""
        end = min(offset + size, total)
        if offset >= end:
            return b""
        most = self.source.request_max(space, sha)
        first, last = offset // self.block, (end - 1) // self.block
        waits: list[Fetch] = []
        cache = self.cache
        with cache.lock:
            item = cache.item_for(sha, total)
            whole = item.have is None
            if not whole:
                missing: list[int] = []
                for i in range(first, last + 1):
                    if item.have[i]:
                        cache.use(item, i)
                    elif i in item.fetching:
                        fetch = item.fetching[i]
                        if fetch not in waits:
                            fetch.waiters += 1
                            waits.append(fetch)
                    else:
                        missing.append(i)
                for fetch in self._plan(space, item, missing, most, None):
                    fetch.waiters += 1
                    waits.append(fetch)
                if item.readahead is None:
                    item.readahead = ReadAhead(total, self.block, self.readahead)
                have, fetching = item.have, item.fetching
                stream, ahead = item.readahead.observe(
                    offset, end, time.monotonic(), lambda i: not have[i] and i not in fetching, most)
                self._plan(space, item, ahead, most, stream)
                item.using.update(range(first, last + 1))
                item.users += 1
        if whole:
            # Finished into a whole file since the caller looked.
            data = cache.pread(sha, end - offset, offset)
            return data if data is not None else self.read(space, sha, total, size, offset)
        try:
            return self._wait_and_read(item, waits, offset, end)
        finally:
            with cache.lock:
                item.using.subtract(range(first, last + 1))
                item.using += Counter()
                for fetch in waits:
                    fetch.waiters -= 1
            cache.release(item)

    def _wait_and_read(self, item: Item, waits: list[Fetch], offset: int, end: int) -> bytes:
        deadline = time.monotonic() + WAIT_S
        for fetch in waits:
            if not fetch.done.wait(max(deadline - time.monotonic(), 0.0)):
                raise fail(errno.EIO)
            if fetch.error is not None:
                raise fetch.error if isinstance(fetch.error, Gone) else fail(errno.EIO)
        out = []
        at = offset
        while at < end:
            i = at // self.block
            upto = min(end, (i + 1) * self.block)
            have = item.have
            if have is None or have[i]:
                out.append(os.pread(item.fd, upto - at, at))
            else:
                fetch = next((w for w in waits if w.first <= i <= w.last and w.data is not None), None)
                if fetch is None:
                    raise fail(errno.EIO)
                base = fetch.first * self.block
                out.append(fetch.data[at - base : upto - base])
            at = upto
        data = b"".join(out)
        if len(data) != end - offset:
            raise fail(errno.EIO)
        return data

    def _plan(self, space: Space, item: Item, blocks: list[int], most: int, stream: Stream | None) -> list[Fetch]:
        """Queue fetches for ``blocks`` (missing, not under way), contiguous
        ones together up to ``most`` bytes. With the cache's lock."""
        fetches: list[Fetch] = []
        run: list[int] = []
        for i in [*blocks, None]:
            if run and (i is None or i != run[-1] + 1 or (len(run) + 1) * self.block > most):
                fetch = Fetch(space, item, run[0], run[-1], stream)
                for j in run:
                    item.fetching[j] = fetch
                fetches.append(fetch)
                self._queue.put((1 if stream is not None else 0, next(self._order), fetch))
                run = []
            if i is not None:
                run.append(i)
        if fetches:
            self.counts["fetch_ahead" if stream is not None else "fetch_demand"] += len(fetches)
            self._start_workers()
        return fetches

    def _stale(self, fetch: Fetch) -> bool:
        """A fetch ahead that no read waits on, of a stream that went quiet,
        was dropped or whose reader moved elsewhere, or of a file the cache
        let go. With the cache's lock."""
        if fetch.stream is None or fetch.waiters:
            return False
        item = fetch.item
        return item.gone or item.readahead is None or not item.readahead.live(fetch.stream, time.monotonic())

    def _cancel(self, fetch: Fetch) -> bool:
        """Whether ``fetch`` is given up as stale; if so its blocks are let go
        in the same hold, so no read can join it after."""
        with self.cache.lock:
            if not self._stale(fetch):
                return False
            self._let_go(fetch)
            return True

    @staticmethod
    def _let_go(fetch: Fetch) -> None:
        fetching = fetch.item.fetching
        for i in range(fetch.first, fetch.last + 1):
            if fetching.get(i) is fetch:
                del fetching[i]

    # --- fetchers ----------------------------------------------------------------

    def _start_workers(self) -> None:
        with self._start:
            while len(self._workers) < self.connections:
                worker = threading.Thread(target=self._work, daemon=True, name="drive-block")
                self._workers.append(worker)
                worker.start()

    def _work(self) -> None:
        buf = memoryview(bytearray(CHUNK))
        while True:
            _, _, fetch = self._queue.get()
            try:
                complete = self._fetch(fetch, buf)
            except Exception as exc:  # noqa: BLE001 - a fetcher must outlive one bad fetch
                print(f"drive: a block fetch failed: {exc!r}", file=sys.stderr, flush=True)
                with self.cache.lock:
                    self._let_go(fetch)
                fetch.error, complete = fail(errno.EIO), False
            fetch.done.set()
            if complete:
                self.counts["promoted" if self.cache.finish(fetch.item) else "promote_refused"] += 1

    def _fetch(self, fetch: Fetch, buf: memoryview) -> bool:
        """Fetch one range into the partial file; whether the file is now
        complete. Its bytes are not kept when it is cancelled, failed
        (``error``), or read into memory for the reads waiting on it when the
        cache has no room (``data``)."""
        item = fetch.item
        start = fetch.first * self.block
        stop = min(item.size, (fetch.last + 1) * self.block)
        n = stop - start
        cache = self.cache
        with cache.lock:
            if self._stale(fetch):
                self._let_go(fetch)
                self.counts["fetch_cancelled"] += 1
                return False
            if item.gone or item.fd is None:
                self._let_go(fetch)
                fetch.error = fail(errno.EIO) if fetch.waiters else None
                return False
            item.users += 1
        try:
            if not cache.claim(n):
                self.counts["block_refused"] += 1
                return self._into_memory(fetch, start, stop, buf)

            def sink(at: int, chunk: memoryview) -> None:
                if self._cancel(fetch):
                    raise _Cancelled
                write_at(item.fd, chunk, at)

            try:
                self.source.fetch(fetch.space, item.sha, start, stop, sink, buf)
            except BaseException as exc:
                # Nothing of a fetch that did not finish stays held.
                cache.cut(item, fetch.first, fetch.last, fetch, n)
                if isinstance(exc, _Cancelled):
                    self.counts["fetch_cancelled"] += 1
                    return False
                if isinstance(exc, DiskFull):
                    self.counts["block_refused"] += 1
                    return self._into_memory(fetch, start, stop, buf)
                if isinstance(exc, OSError):
                    fetch.error = exc
                    return False
                raise
            with cache.lock:
                self._let_go(fetch)
                return cache.landed(item, fetch.first, fetch.last, n)
        finally:
            cache.release(item)

    def _into_memory(self, fetch: Fetch, start: int, stop: int, buf: memoryview) -> bool:
        """For the reads waiting on it, when the cache has no room; a fetch
        ahead no read waits on is dropped."""
        with self.cache.lock:
            if fetch.stream is not None and not fetch.waiters:
                self._let_go(fetch)
                return False
        data = bytearray(stop - start)

        def sink(at: int, chunk: memoryview) -> None:
            data[at - start : at - start + len(chunk)] = chunk

        try:
            self.source.fetch(fetch.space, fetch.item.sha, start, stop, sink, buf)
            fetch.data = bytes(data)
        except OSError as exc:
            fetch.error = exc
        with self.cache.lock:
            self._let_go(fetch)
        return False
