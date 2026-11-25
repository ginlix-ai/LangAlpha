"""Bytes of stored files: from the cache on disk, from memory when the cache
had no room, or from the server. Text not on disk is fetched with the rest
of its folder (which a search reads next), a small binary whole, a large
one by block (``drive_fetch``), read at the version its readers are held on
(``drive_holds``).
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import threading
from collections import Counter, OrderedDict
from collections.abc import Callable

from .drive_cache import Cache
from .drive_client import DriveClient, Refused
from .drive_disk import pwrite_all
from .drive_effects import Effects
from .drive_fetch import Fetcher
from .drive_holds import Holds, key_of
from .drive_names import Names
from .drive_source import Source
from .drive_tree import Entry, Space, split
from .remote import fail

#: Bytes fetched that the cache had no room for, kept in memory for the
#: reads that follow at most.
SPILL_BYTES = 64 * 1024 * 1024

#: A large file opened for writing is copied into its record this much at a time.
COPY_CHUNK = 8 * 1024 * 1024

#: At most this many text files fetched together when a read finds one not
#: yet on disk (the rest of its folder, which a search reads next).
TEXT_BATCH = 256


class Reads:
    def __init__(self, cache: Cache, client: DriveClient, names: Names, effects: Effects,
                 counts: Counter[str], fetcher: Fetcher | None = None,
                 caller: Callable[[], int] | None = None, root: str | None = None) -> None:
        self.cache = cache
        self.fetcher = fetcher or Fetcher(cache, Source(client, counts), counts)
        self.holds = Holds(effects, counts, root)
        self.client = client
        self.names = names
        self.counts = counts
        self._caller = caller
        #: Guards the spill only.
        self._mu = threading.Lock()
        self._spill: OrderedDict[str, bytes] = OrderedDict()
        self._spilled = 0

    def large(self, entry: Entry) -> bool:
        return self.fetcher.large(entry)

    def read(self, space: Space, key: str, entry: Entry | None, size: int, offset: int) -> bytes:
        """``size`` bytes from ``offset`` of what a read at the mount path
        ``key`` gets: the file its readers are held on (``drive_holds``),
        else ``entry``, what the path shows now. ENOENT when neither."""
        large = entry is not None and entry.sha256 is not None and self.large(entry)
        key = key_of(space, entry) if entry is not None else key
        got = self.holds.resolve(space, key, entry, self._caller() if self._caller else None, large=large)
        if got is None:
            raise fail(errno.ENOENT)
        if got.is_dir:
            raise fail(errno.EISDIR)
        return self.read_version(space, got, size, offset)

    def read_version(self, space: Space, entry: Entry, size: int, offset: int) -> bytes:
        """``size`` bytes of exactly the version ``entry`` names, never the
        one a hold shows: a writer's base is read this way."""
        sha = entry.sha256
        if sha is None and entry.size:
            raise fail(errno.EIO)  # no digest the server named that is one
        if offset >= entry.size or sha is None:
            return b""
        data = self._held(sha, size, offset)
        if data is None:
            data = self._read_missing(space, entry, size, offset)
        return data

    def shown(self, space: Space, entry: Entry) -> Entry:
        return self.holds.shown(space, entry)

    def head(self, space: Space, entry: Entry) -> bytes:
        """A file's whole head, for a writer starting from it."""
        sha = entry.sha256
        assert sha is not None
        if not self.cache.has(sha):
            self.counts["fetch"] += 1
            if entry.is_text:
                with contextlib.suppress(Refused):
                    self.fetch_texts(space, [entry])
            if not self.cache.has(sha):
                with self._mu:
                    spilled = self._spill.get(sha)
                if spilled is not None:
                    return spilled
                self._keep(sha, self.client.content(space.ref, sha), text=entry.is_text)
        data = self._held(sha, entry.size, 0)
        if data is None or len(data) != entry.size:
            raise fail(errno.EIO)
        return data

    def copy_into(self, space: Space, entry: Entry, fd: int, shortfall: Callable[[int], int]) -> int:
        """A large file's head into ``fd`` (a file being written), through the
        cache a chunk at a time, never whole in memory.
        Raises ENOSPC (with the bytes missing as ``short``) where the disk
        cannot hold the copy."""
        at = 0
        while at < entry.size:
            n = min(COPY_CHUNK, entry.size - at)
            short = shortfall(n)
            if short:
                exc = fail(errno.ENOSPC)
                exc.short = short + (entry.size - at - n)  # type: ignore[attr-defined]
                raise exc
            data = self.read_version(space, entry, n, at)
            if len(data) != n:
                raise fail(errno.EIO)
            pwrite_all(fd, data, at)
            at += n
        return entry.size

    def fetch_texts(self, space: Space, entries: list[Entry]) -> int:
        """Fetch the text of ``entries`` into the cache; how many landed."""
        landed = 0
        for _, sha, data in self.client.texts(space.ref, [e.id for e in entries]):
            self._keep(sha, data, text=True)
            if self.cache.has(sha):
                landed += 1
        return landed

    # --- held here -----------------------------------------------------------

    def _held(self, sha: str, size: int, offset: int) -> bytes | None:
        data = self.cache.pread(sha, size, offset)
        if data is not None:
            return data
        with self._mu:
            spilled = self._spill.get(sha)
            if spilled is not None:
                self._spill.move_to_end(sha)
        return None if spilled is None else spilled[offset : offset + size]

    def _keep(self, sha: str, data: bytes, *, text: bool) -> None:
        """Into the cache, or memory when the cache has no room for it."""
        if self.cache.put(sha, data, text=text):
            return
        if hashlib.sha256(data).hexdigest() != sha or len(data) > SPILL_BYTES:
            return
        with self._mu:
            if sha in self._spill:
                return
            self._spill[sha] = data
            self._spilled += len(data)
            while self._spilled > SPILL_BYTES:
                _, old = self._spill.popitem(last=False)
                self._spilled -= len(old)

    # --- from the server ---------------------------------------------------------

    def _read_missing(self, space: Space, entry: Entry, size: int, offset: int) -> bytes:
        self.counts["fetch"] += 1
        sha = entry.sha256
        assert sha is not None
        if entry.is_text:
            with contextlib.suppress(Refused):
                self.fetch_texts(space, self._text_batch(space, entry))
        elif self.large(entry):
            return self.fetcher.read(space, sha, entry.size, size, offset)
        data = self._held(sha, size, offset)
        if data is None:
            # Text the server now holds other bytes for (``texts`` sends a
            # file's current head), or a small binary: by its digest, which
            # the server answers for any version it keeps.
            self.counts["fetch_sha"] += 1
            self._keep(sha, self.client.content(space.ref, sha), text=entry.is_text)
            data = self._held(sha, size, offset)
        if data is None:
            raise fail(errno.EIO)
        return data

    def _text_batch(self, space: Space, entry: Entry) -> list[Entry]:
        if self.cache.missing_text:
            # Not all text fits on this disk: siblings fetched now would
            # only push out what the next read wants.
            return [entry]
        batch = [entry]
        for sibling in self.names.siblings(entry, split(entry.path)[0], space):
            if len(batch) >= TEXT_BATCH:
                break
            if sibling.is_text and not sibling.is_dir and not self.cache.has(sibling.sha256):
                batch.append(sibling)
        return batch
