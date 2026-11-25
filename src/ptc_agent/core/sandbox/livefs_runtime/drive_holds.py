"""The file a large file's readers are on, held while they read.

The mount answers no opens, so there is no handle to tie a reader to a
file, and the kernel names every read by path and keeps one inode, and one
page cache, per path: serving each reader its own version would mix both in
that cache. So a path that was read as a large file holds the file its
readers are on (that entry, at that version) while any of them (a process,
named by the FUSE request's pid) keeps reading, a reader that starts
meanwhile joining it, whatever the path shows meanwhile: a newer version,
text or a small file in its place, another file made there, or nothing.
Once they have all gone quiet or exited the hold ends, the kernel is told to
forget what it cached, and what is there now shows. A reader the kernel's
cache answers is quiet here though still partway, so one that still holds
the file open keeps the hold. A writer edits the
version shown, so the kernel's offsets (an append's, the size) match the
bytes the edit starts from.
"""

from __future__ import annotations

import os
import threading
import time
from collections import Counter

from .drive_effects import Effects
from .drive_tree import Entry, Space

#: A large file's readers count as done this long after their last read
#: (sooner when their process exits), and a version is held for them at
#: most this long after a newer one arrived. Well inside the hour the
#: server keeps a replaced version's bytes.
HOLD_IDLE_S = 120.0
HOLD_MAX_S = 1800.0
#: A process's identity cached this long by pid (a scan reads thousands of
#: times a second).
PROC_TTL_S = 2.0
#: The readers of one quiet hold whose descriptors are looked at, the most
#: recent first: each look lists one process's descriptors, and is made only
#: once the hold has gone quiet, so at most once an idle time a hold.
OPEN_LOOKS = 16

Proc = tuple[int, int] | None


def _proc(pid: int) -> Proc:
    """A process as (thread group id, start time): a pid alone may be reused."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            fields = f.read().rpartition(b")")[2].split()
        tgid = pid
        with open(f"/proc/{pid}/status", "rb") as f:
            for line in f:
                if line.startswith(b"Tgid:"):
                    tgid = int(line.split()[1])
                    break
        if tgid != pid:
            with open(f"/proc/{tgid}/stat", "rb") as f:
                fields = f.read().rpartition(b")")[2].split()
        return tgid, int(fields[19])
    except (OSError, ValueError, IndexError):
        return None


def _open_at(proc: tuple[int, int], target: str) -> bool:
    """Whether the process holds a descriptor open on ``target``. One on a
    file removed since (``<path> (deleted)``) does not count: its reads
    fail (``DriveFS.read``), so nothing is held for it."""
    fds = f"/proc/{proc[0]}/fd"
    try:
        names = os.listdir(fds)
    except OSError:
        return False
    for name in names:
        try:
            if os.readlink(f"{fds}/{name}") == target:
                return True
        except OSError:
            continue
    return False


def _alive(proc: tuple[int, int]) -> bool:
    try:
        with open(f"/proc/{proc[0]}/stat", "rb") as f:
            return int(f.read().rpartition(b")")[2].split()[19]) == proc[1]
    except (OSError, ValueError, IndexError):
        return False


class _Hold:
    __slots__ = ("space", "entry", "readers", "since", "shown")

    def __init__(self, space: Space, entry: Entry) -> None:
        self.space = space
        self.entry = entry
        #: Each reader to when it last read.
        self.readers: dict[Proc, float] = {}
        #: When a newer version was first seen while it was being read; 0
        #: while it is still the current one.
        self.since = 0.0
        #: Whether ``getattr`` was shown it over a newer version: the kernel
        #: holds its attributes, to be forgotten when the hold ends.
        self.shown = False

    def live(self, now: float) -> bool:
        if self.since and now - self.since >= HOLD_MAX_S:
            return False
        return any(now - at < HOLD_IDLE_S and (proc is None or _alive(proc)) for proc, at in self.readers.items())


def key_of(space: Space, entry: Entry) -> str:
    """The mount path a hold is kept under: what the kernel names reads by."""
    return f"/{space.name}/{entry.path}"


class Holds:
    def __init__(self, effects: Effects, counts: Counter[str], root: str | None = None) -> None:
        self.effects = effects
        self.counts = counts
        #: Where the mount is, as a reader's descriptors name its files.
        self.root = root
        self._mu = threading.Lock()
        #: Large files being read, by the mount path they are read at.
        self._holds: dict[str, _Hold] = {}
        self._procs: dict[int, tuple[Proc, float]] = {}

    def _who(self, pid: int | None) -> Proc:
        if pid is None:
            return None
        now = time.monotonic()
        hit = self._procs.get(pid)
        if hit is not None and hit[1] > now:
            return hit[0]
        proc = _proc(pid)
        if len(self._procs) > 4096:
            self._procs.clear()
        self._procs[pid] = (proc, now + PROC_TTL_S)
        return proc

    def resolve(self, space: Space, key: str, entry: Entry | None, pid: int | None, *, large: bool) -> Entry | None:
        """The file a read at ``key`` by ``pid`` gets, ``entry`` being what
        the path shows now (None: nothing), noting its process as a reader.
        A path read as a large file (``large``) is held from then on."""
        proc = self._who(pid)
        now = time.monotonic()
        released = None
        with self._mu:
            hold = self._holds.get(key)
            if hold is not None and not _same(hold.entry, entry):
                # As ``tick`` keeps it: a reader with the file still open.
                if hold.live(now) or self._still_open(key, hold, now):
                    hold.since = hold.since or now
                    hold.readers[proc] = now
                    self.counts["pin_held_read"] += 1
                    return hold.entry
                released, hold = hold, None
                del self._holds[key]
            if hold is None and entry is not None and large:
                hold = self._holds[key] = _Hold(space, entry)
            if hold is not None:
                hold.readers[proc] = now
        if released is not None:
            self._release(key)
        return entry

    def shown(self, space: Space, entry: Entry) -> Entry:
        """What the path of ``entry`` shows: the file its readers are on while
        they read (``getattr`` asks, so the size matches the bytes)."""
        key = key_of(space, entry)
        hold = self._holds.get(key)
        if hold is None or _same(hold.entry, entry):
            return entry
        with self._mu:
            hold = self._holds.get(key)
            if hold is not None and not _same(hold.entry, entry) and hold.live(time.monotonic()):
                hold.shown = True
                return hold.entry
        return entry

    def kept(self) -> set[str]:
        """Versions held for readers, which the cache must keep."""
        with self._mu:
            return {h.entry.sha256 for h in self._holds.values() if h.since and h.entry.sha256}

    def tick(self) -> None:
        """End holds whose readers are done; forget paths nobody reads."""
        now = time.monotonic()
        released: list[str] = []
        with self._mu:
            for key, hold in list(self._holds.items()):
                if hold.live(now) or self._still_open(key, hold, now):
                    continue
                del self._holds[key]
                if hold.since or hold.shown:
                    released.append(key)
        for key in released:
            self._release(key)

    def _still_open(self, key: str, hold: _Hold, now: float) -> bool:
        """A reader gone quiet that still holds the file open: its reads may
        be answered from the kernel's cache, and it is partway. Counted as
        reading now, so it is looked at again only once it goes quiet again.
        Under ``_mu``."""
        if self.root is None or (hold.since and now - hold.since >= HOLD_MAX_S):
            return False
        target = self.root + key
        recent = sorted(hold.readers.items(), key=lambda item: item[1], reverse=True)[:OPEN_LOOKS]
        for proc, _ in recent:
            if proc is not None and _alive(proc) and _open_at(proc, target):
                hold.readers[proc] = now
                self.counts["pin_kept_open"] += 1
                return True
        return False

    def _release(self, key: str) -> None:
        """The hold is over: the kernel forgets what it cached at the path."""
        self.counts["pin_released"] += 1
        self.effects.forget(key)


def _same(held: Entry, entry: Entry | None) -> bool:
    return entry is not None and held.id == entry.id and held.sha256 == entry.sha256
