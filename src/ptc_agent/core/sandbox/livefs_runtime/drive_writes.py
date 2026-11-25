"""Files being written: each a ``Pending``, its bytes in a journal record on
disk (``drive_journal``) until it is saved. Which file is being written at
which path is ``drive_names``'s, as the manifest is.

A ``Pending`` is in one phase at a time:

  clean       its bytes are the head it was opened from, or the last save
  dirty       changed since; saved when its writes go quiet (``drive_saves``)
  unanswered  a save got no answer: each body sent then may have landed, so
              their digests are kept until a save is answered, and it is
              sent again at ``retry_at``
  closed      let go: saved and back to the store, emptied out, replaced, or
              refused (``errno``)

and moves between them only by its own transitions (``resumed`` from the
journal, ``changed_now``, ``head_landed``, ``accepted``, ``unanswered``,
``close``), which keep its journal record's metadata and say what the agent
is to be told (a ``Tell``) for the saver to file. A save in flight is not a
phase: it is ``saving``, a lock held across each of its requests so the
file's own name changes wait for that request's answer.

Lock order, everywhere: ``Pending.saving``, then the operations' ``mu``
(which guards the manifests and which file is being written where), then
``Pending.lock`` (its bytes and phase). Only this module and ``drive_names``
take ``Pending.lock``; nothing holds ``mu`` or ``lock`` across a request to
the server.
"""

from __future__ import annotations

import contextlib
import enum
import errno
import os
import random
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from .drive_disk import pread_exact, pwrite_all, sha256_fd
from .drive_effects import Effects
from .drive_names import Names
from .drive_journal import Journal, Record
from .drive_notices import LOST, SAVED, UNSAVED, Notices
from .drive_protocol import PROCESS_PREFIX
from .drive_tree import Space
from .remote import fail

#: After a save of a file is refused, writes to it fail with the refusal's
#: errno until it is emptied or made again, or this long passes with no
#: write to it: a writer still holding the file must not splice its later
#: bytes onto the server's version, or onto nothing.
REFUSED_HOLD_S = 60.0
REFUSED_KEPT = 4096

#: A file growing on this disk asks for room this much at a time, so a
#: copy's thousands of writes ask a few hundred times.
ROOM_STEP = 16 * 1024 * 1024

#: A save with no answer is sent again after 1, 2, 4 ... seconds, at most
#: this far apart: an answer that never changes (a proxy's page) costs a
#: resend every quarter hour, not every minute.
RETRY_MAX_S = 900.0
#: This long after a save first went unanswered it has failed for good, and
#: the agent is told. Its bytes stay on this computer, at the file's path,
#: for ``KEPT_S`` more, still tried every ``LATE_TRY_S`` (and at once on a
#: write), then go, told. The journal's disk is held that long at most;
#: removing the file frees it at once.
GIVE_UP_S = 6 * 3600.0
KEPT_S = 7 * 24 * 3600.0
LATE_TRY_S = 3600.0


class Phase(enum.Enum):
    CLEAN = "clean"
    DIRTY = "dirty"
    UNANSWERED = "unanswered"
    CLOSED = "closed"


@dataclass(frozen=True)
class Tell:
    """A notice a transition leaves for the saver to file for the agent."""

    kind: str
    call: str | None


class Pending:
    """A file being written: its bytes so far, and the head it started from."""

    __slots__ = (
        "space", "rel", "fd", "size", "base", "rev", "call", "mtime_ns", "changed", "first",
        "journal", "record", "phase", "sent", "retry_at", "attempts", "told_calls", "errno",
        "lock", "saving", "room_to", "upload", "why", "parts", "digest", "failing_since", "gave_up",
        "kept_until",
    )

    def __init__(self, space: Space, rel: str, fd: int, size: int, base: str | None,
                 call: str | None, journal: Journal, record: str) -> None:
        self.space = space
        self.rel = rel
        self.fd = fd
        self.size = size
        #: The head sha256 the bytes were edited from; None for a new file.
        self.base = base
        #: Moved by every change, so a save answered after a later write
        #: leaves the file dirty and that write is saved too.
        self.rev = 0
        self.call = call
        self.mtime_ns = time.time_ns()
        #: When it last changed, and when it last went from saved to not.
        self.changed = time.monotonic()
        self.first = self.changed
        #: Its journal record, which holds the bytes ``fd`` names.
        self.journal = journal
        self.record = record
        self.phase = Phase.CLEAN
        #: Unanswered: the digest of each body sent with no answer.
        self.sent: set[str] = set()
        self.retry_at = 0.0
        self.attempts = 0
        #: The calls told it is not saved yet: each command that wrote it is
        #: told, and told again once it is saved.
        self.told_calls: list[str | None] = []
        #: Closed by a refusal: its errno, for an fsync that asks.
        self.errno = 0
        self.lock = threading.Lock()
        #: Held across a save's request, and by anything that must come
        #: before or after one (a rename, an unlink of the file).
        self.saving = threading.Lock()
        #: How far the file may grow before the disk is asked for room again.
        self.room_to = size
        #: A save in parts under way (``drive_uploads.Upload``), which a
        #: rename or removal abandons rather than waits for.
        self.upload: Any = None
        #: Why its last save did not land, for the agent.
        self.why = ""
        #: The upload of a save in parts that went unanswered, which may
        #: still land, and the sha256 it carries: a rename or removal ends
        #: it first, and a save of other bytes too, so none is left open.
        self.parts: tuple[str, str] | None = None
        #: The sha256 of the bytes at a rev, so a save sent again over an
        #: unchanged file does not read it all again.
        self.digest: tuple[int, str] | None = None
        #: When its saves first went unanswered (0: they have not), and
        #: whether tries have stopped (``GIVE_UP_S``).
        self.failing_since = 0.0
        self.gave_up = False
        #: When the bytes of a save failed for good go (``KEPT_S``).
        self.kept_until = 0.0

    @property
    def open(self) -> bool:
        return self.phase is not Phase.CLOSED

    @property
    def unsaved(self) -> bool:
        return self.phase in (Phase.DIRTY, Phase.UNANSWERED)

    @property
    def clean(self) -> bool:
        return self.phase is Phase.CLEAN

    # --- transitions, under ``lock`` unless said ------------------------------

    @classmethod
    def resumed(cls, space: Space, journal: Journal, record: Record, fd: int, size: int, digest: str) -> Pending:
        """A record taken up from the journal, unanswered and due now. The
        daemon may have died after its save of these very bytes (``digest``)
        committed, so the save checks the head first and builds on it. Told
        already when a save of it went unanswered."""
        p = cls(space, record.rel, fd, size, record.base, record.call, journal, record.id)
        p.sent = {*record.unanswered, digest}
        p.phase = Phase.UNANSWERED
        p.retry_at = time.monotonic()
        p.told_calls = [call_id(record.call)] if record.unanswered else []
        p.digest = (p.rev, digest)
        return p

    @property
    def told(self) -> bool:
        return bool(self.told_calls)

    def changed_now(self, size: int, call: str | None = None) -> None:
        """The bytes changed (now ``size`` long, by ``call``): due to be saved."""
        now = time.monotonic()
        if self.phase is Phase.CLEAN:
            self.phase = Phase.DIRTY
            self.first = now
        self.size = size
        self.call = call or self.call
        self.rev += 1
        self.changed = now
        self.mtime_ns = time.time_ns()
        if self.gave_up:
            # Written again: saved again, its tries counted afresh.
            self.gave_up = False
            self.failing_since = 0.0
            self.kept_until = 0.0
            self.attempts = 0
            self.retry_at = 0.0

    def moved(self, rel: str, base: str | None) -> None:
        """Renamed to ``rel``, now over ``base``."""
        self.rel = rel
        self.base = base

    def head_landed(self, sha: str | None) -> bool:
        """Whether the server's head ``sha`` is a body sent with no answer
        that landed after all, which the save then builds on. Under
        ``saving``."""
        if sha is None or sha not in self.sent or sha == self.base:
            return False
        self.base = sha
        return True

    def accepted(self, sha: str | None, rev: int, call: str | None) -> list[Tell]:
        """A save of the bytes at ``rev`` by ``call`` was answered with head
        ``sha``: clean, unless written meanwhile. A file the agent was told
        is not saved yet is said saved once clean, under each call told."""
        self.base = sha
        self.sent = set()
        self.parts = None
        self.retry_at = 0.0
        self.attempts = 0
        self.failing_since = 0.0
        self.gave_up = False
        self.kept_until = 0.0
        if self.rev != rev:
            # Written meanwhile: its wait starts again from here.
            self.phase = Phase.DIRTY
            self.first = time.monotonic()
            return []
        self.phase = Phase.CLEAN
        told, self.told_calls = self.told_calls, []
        return [Tell(SAVED, c) for c in told]

    def unanswered(self, digest: str | None, after: float | None, call: str | None) -> bool:
        """A save by ``call`` got no answer (``digest``: the body it sent,
        which may have landed), or did not reach the server (None). Sent
        again ``after`` seconds when the server said, else after a back-off.
        True when this try is the one that failed it for good."""
        if digest is not None:
            self.sent.add(digest)
        now = time.monotonic()
        self.attempts += 1
        self.failing_since = self.failing_since or now
        if after is None:
            after = min(RETRY_MAX_S, 2.0 ** min(self.attempts - 1, 20)) * random.uniform(0.8, 1.2)
        self.retry_at = now + min(after, RETRY_MAX_S)
        now_failed = False
        if now - self.failing_since >= GIVE_UP_S:
            now_failed = not self.gave_up
            if now_failed:
                self.gave_up = True
                self.kept_until = self.failing_since + GIVE_UP_S + KEPT_S
            # Failed for good: still tried, hourly, while its bytes are kept.
            self.retry_at = min(now + LATE_TRY_S * random.uniform(0.8, 1.2), self.kept_until)
        self.phase = Phase.UNANSWERED
        # The save's call only where none wrote it: a later command's write
        # is that command's to be told about.
        self.call = self.call or call
        return now_failed

    def tell_unsaved(self) -> Tell | None:
        """The agent is told once per command that the file is not saved
        yet, under the call that last wrote it. Not under ``lock``: only its
        savers and a settle that waited on it ask."""
        call = call_id(self.call)
        if call in self.told_calls:
            return None
        self.told_calls.append(call)
        return Tell(UNSAVED, call)

    def close(self, err: int = 0) -> None:
        """Let go: saved, emptied out, replaced, or refused (``err``). The
        record goes first, while its data is still locked, so no other daemon
        takes it up."""
        if not self.open:
            return
        self.phase = Phase.CLOSED
        self.errno = err
        self.journal.drop(self.record)
        os.close(self.fd)

    def describe(self) -> None:
        """Write where the record goes and what it was edited from, and which
        bodies sent may have landed, for the save a restart sends."""
        if not self.open:
            return
        try:
            self.journal.describe(self.record, self.space.name, self.rel, self.base, sorted(self.sent), self.call)
        except OSError as exc:
            print(f"drive: a file being written not recorded: {exc}", file=sys.stderr, flush=True)


def call_id(call: str | None) -> str | None:
    """A tool call's id; None for a process no call started."""
    return None if not call or call.startswith(PROCESS_PREFIX) else call


class Writes:
    """The bytes of the files being written, their journal records, and the
    paths whose saves were refused. Which file is being written where is
    ``Names``'s (``drive_names``), as the manifest is."""

    def __init__(self, names: Names, journal: Journal, notices: Notices, effects: Effects,
                 shortfall: Callable[[int], int] | None = None) -> None:
        self.names = names
        self.journal = journal
        self.notices = notices
        self.effects = effects
        #: Room on this disk for a file growing by n bytes: the bytes still
        #: missing after the cache gave back what it could (0: room).
        self.shortfall = shortfall or (lambda n: 0)
        #: Paths whose last save was refused: (errno, when last written).
        #: Under ``names.mu``.
        self._refused: dict[str, tuple[int, float]] = {}

    # --- bytes -------------------------------------------------------------------

    def pread(self, p: Pending, size: int, offset: int) -> bytes | None:
        with p.lock:
            return os.pread(p.fd, size, offset) if p.open else None

    def write(self, p: Pending, data: bytes, offset: int, call: str | None, limit: int) -> int | None:
        """How many bytes went onto the disk, which a full disk can make fewer
        than were sent, so the program never counts bytes it lost; None when
        ``p`` closed meanwhile (the caller finds the file again).
        Past ``limit`` the whole edit is refused at the write, where the
        program sees it, and the file closed: nothing of it is saved. Room on
        the disk is made before the file's lock is taken: making it may evict
        from the cache, which must not hold up the file's other requests."""
        end = offset + len(data)
        if end > limit:
            if not p.open:
                return None
            self.drop_refused(p, errno.EFBIG)
            raise fail(errno.EFBIG)
        room_to = p.room_to
        ahead = ROOM_STEP
        if end > room_to:
            short = self.shortfall(end - max(room_to, p.size) + ROOM_STEP)
            if short:
                # No room for the step ahead: perhaps for this write alone.
                ahead = 0
                short = self.shortfall(end - max(room_to, p.size))
            if short:
                if not p.open:
                    return None
                self.no_room(p.space, p.rel, call, short)
                self.drop_refused(p, errno.ENOSPC)
                raise fail(errno.ENOSPC)
        with p.lock:
            if not p.open:
                return None
            p.room_to = max(p.room_to, end + ahead) if end > room_to else p.room_to
            view, done = memoryview(data), 0
            try:
                while done < len(view):
                    n = os.pwrite(p.fd, view[done:], offset + done)
                    if n <= 0:
                        raise fail(errno.ENOSPC)
                    done += n
            except OSError:
                if not done:
                    raise
            finally:
                if done:
                    p.changed_now(max(p.size, offset + done), call)
            return done

    def sync(self, p: Pending) -> None:
        """Its bytes onto this disk."""
        with p.lock:
            if p.open:
                os.fsync(p.fd)

    def truncate(self, p: Pending, length: int, call: str | None, limit: int) -> bool:
        with p.lock:
            if not p.open:
                return False
            if length <= limit:
                os.ftruncate(p.fd, length)
                p.changed_now(length, call)
                return True
        self.drop_refused(p, errno.EFBIG)
        raise fail(errno.EFBIG)

    def snapshot(self, p: Pending) -> tuple[bytes, int] | None:
        """The bytes to save and the change they are at; None when there is
        nothing to save."""
        with p.lock:
            if not p.unsaved:
                return None
            return pread_exact(p.fd, p.size, 0), p.rev

    # --- making ------------------------------------------------------------------

    def _new(self, space: Space, rel: str, size: int, base: str | None, call: str | None) -> Pending:
        record = self.journal.new_id()
        return Pending(space, rel, self.journal.open(record), size, base, call, self.journal, record)

    def _discard(self, fd: int, record: str) -> None:
        self.journal.drop(record)
        os.close(fd)

    def opened(self, path: str, space: Space, rel: str, base: str | None, head: bytes | None,
               call: str | None, fill: Callable[[int], int] | None = None) -> Pending:
        """The file at ``path`` opened for writing from ``head`` (None: from
        nothing), or from what ``fill`` writes into the record's descriptor
        (a large file, copied a chunk at a time; it answers the size), or the
        one another request opened meanwhile."""
        p = self._new(space, rel, len(head or b""), base, call)
        try:
            if head:
                pwrite_all(p.fd, head, 0)
            elif fill is not None:
                p.size = p.room_to = fill(p.fd)
            # A remote change may have come in meanwhile; the save's
            # precondition is what catches it.
            old = self.names.start_writing(path, p)
        except BaseException:
            self._discard(p.fd, p.record)
            raise
        if old is not None:
            self._discard(p.fd, p.record)
            return old
        p.describe()
        return p

    def made(self, path: str, space: Space, rel: str, base: str | None, call: str | None) -> None:
        """A file made, or emptied by an open, at ``path``: whatever was being
        written there before is replaced."""
        p = self._new(space, rel, 0, base, call)
        p.changed_now(0)
        old = self.names.replace_writing(path, p)
        p.describe()
        if old is not None:
            self.names.let_go(old)

    def emptied(self, p: Pending) -> bool:
        """False when ``p`` closed meanwhile."""
        with p.lock:
            if not p.open:
                return False
            os.ftruncate(p.fd, 0)
            p.changed_now(0)
            return True

    def wait(self, p: Pending, after: float) -> None:
        """Hold ``p``'s next save ``after`` seconds (0: due now), as it is."""
        with p.lock:
            if p.open:
                p.retry_at = time.monotonic() + after

    def unanswered(self, p: Pending, digest: str | None, call: str | None, after: float | None = None) -> bool:
        """True when this try failed it for good (``Pending.unanswered``)."""
        with p.lock:
            if not p.open:
                return False
            now_failed = p.unanswered(digest, after, call)
        # Which versions may have landed, for the save a restart sends.
        p.describe()
        return now_failed

    # --- refusals ----------------------------------------------------------------

    def drop_refused(self, p: Pending, err: int) -> None:
        """Show the server's file again after an edit it refused (or that
        grew past the size limit). The server filed a refusal for the
        command, whose result names it, so bytes kept here would only be a
        copy no other computer, and no later save, could rely on: a refused
        new file goes, an edit goes back to the head. Writes to it fail for
        a while (``REFUSED_HOLD_S``)."""
        key = Names.key(p)
        with self.names.mu:
            self._refused[key] = (err, time.monotonic())
            if len(self._refused) > REFUSED_KEPT:
                self._refused.pop(next(iter(self._refused)))
        self.names.let_go(p, err)
        self.effects.forget(key)

    def no_room(self, space: Space, rel: str, call: str | None, short: int) -> None:
        """Tell the agent a file could not be kept here to be saved, and how
        much more room it needed. Filed as not saved: nothing reached the
        server."""
        mb = max(short // 1_000_000, 1)
        with contextlib.suppress(OSError):
            self.notices.add(LOST, call_id(call), space.name, rel,
                             f"this computer's disk has no room left to keep it while it is saved; "
                             f"it needs about {mb} MB more free")
        self.effects.noticed()

    def check_refusal(self, path: str) -> None:
        """Fail a write to a file whose save was refused (``REFUSED_HOLD_S``)."""
        if not self._refused:
            return
        now = time.monotonic()
        with self.names.mu:
            hit = self._refused.get(path)
            if hit is None:
                return
            if now - hit[1] >= REFUSED_HOLD_S:
                del self._refused[path]
                return
            self._refused[path] = (hit[0], now)
        raise fail(hit[0])

    def fresh(self, path: str) -> None:
        """The file was emptied or made again: a new start after a refusal."""
        if self._refused:
            with self.names.mu:
                self._refused.pop(path, None)

    # --- renames -----------------------------------------------------------------

    @staticmethod
    @contextlib.contextmanager
    def holding(files: list[Pending | None]) -> Iterator[None]:
        """Hold every file's ``saving`` (in one order, so two holders never
        wait on each other): no save of any starts or answers meanwhile."""
        with contextlib.ExitStack() as stack:
            for p in sorted({id(p): p for p in files if p is not None}.values(), key=id):
                stack.enter_context(p.saving)
            yield

    # --- the journal: records a daemon left -----------------------------------------

    def restore(self, claimed: list[tuple[Record, int]]) -> int:
        """Take up records claimed from the journal (``Journal.claim``, each
        with its data open and locked): each is a file being written again,
        due to be sent now. Returns how many."""
        taken = 0
        for record, fd in claimed:
            space = self.names.spaces.get(record.space)
            if space is None:
                with contextlib.suppress(OSError):
                    self.notices.add(LOST, call_id(record.call), record.space, record.rel,
                                     "its space is no longer reachable from this computer")
                self.effects.noticed()
                self._discard(fd, record.id)
                continue
            size = os.fstat(fd).st_size
            digest = sha256_fd(fd, size)
            if not record.unanswered and record.base is not None and digest == record.base:
                # Opened for writing and never changed.
                self._discard(fd, record.id)
                continue
            # This daemon writing it anew already: that start wins.
            if not self.names.resume_writing(Pending.resumed(space, self.journal, record, fd, size, digest)):
                self._discard(fd, record.id)
                continue
            taken += 1
        return taken
