"""The file operations the Drive mount answers: FUSE dispatch only. mfusepy
registers every attribute named like a FUSE operation, so the public
methods here are exactly what the kernel calls; where a path is lives in
``drive_names``, stored bytes in ``drive_reads``, files being written in
``drive_writes`` and their saves in ``drive_saves``.

The kernel is told to cache everything (attributes, names, absences, file
data and listings) for a day, and ``open`` answers ENOSYS so it stops
sending open and release at all. So the operations here see only what the
cache misses: a name looked up for the first time, a file's first read, and
every write. A change made elsewhere reaches the kernel as an explicit
invalidation from the feed (``drive_sync``), never by a cache timing out.
With no open there is no file handle: everything is keyed by path, so a
file removed while open (``hard_remove``: libfuse then names no path) reads
what the page cache kept of it, and EIO for the rest. Telling which file a
path-less request is for, the only way to serve one, is a handle from
``open``: an id per open naming the entry and version, set as ``fh`` with
``keep_cache`` so the page cache still lasts across opens, read by ``fh``
and dropped at release. It costs a request to this daemon at every open and
close (a search over many files sends one each, which ENOSYS stops today)
and a table of handles kept for as long as files stay open. ``flush``
answers ENOSYS too, so the kernel stops sending one at every close (a
search's every file read sent one); saves go as ``drive_saves`` says.

libfuse runs several requests at once, so a read of bytes already on disk
never waits behind one held up on the server: nothing holds a lock across a
request to the server but ``Pending.saving``, which only that file's own
changes take.

A name the server takes for a stored one resolves to it, and a new file
under such a name is refused at once (EEXIST).
"""

from __future__ import annotations

import contextlib
import errno
import os
import stat
import threading
import time
from collections import Counter
from collections.abc import Callable

from .drive_cache import Cache
from .drive_client import DriveClient
from .drive_effects import Effects
from .drive_fetch import Fetcher
from .drive_journal import Journal
from .drive_libfuse import without_atomic_trunc
from .drive_names import Names
from .drive_notices import Notices
from .drive_protocol import FILE_MAX_BYTES, IGNORE_NAME
from .drive_reads import Reads
from .drive_saves import Saves
from .drive_tree import Entry, Space, entry_of, nfc, split
from .drive_writes import Pending, Writes
from .remote import fail

_RENAME_NOREPLACE = 1
_RENAME_EXCHANGE = 2

#: Background (read-ahead) requests the kernel keeps in flight on the mount.
#: At the kernel's 12 a cold scan of a large file at 80 ms to first byte
#: stayed near 125 MB/s with 16 or 32 loop threads and connections alike:
#: each holds a loop thread on the cache while its block is fetched.
MAX_BACKGROUND = 64

PID_TTL_S = 2.0
PID_CACHE_LIMIT = 4096


class _Calls:
    """The tool call each process runs under, cached a moment by pid (a
    command's writes all ask)."""

    def __init__(self, caller: Callable[[], int], call_of: Callable[[int], str | None]) -> None:
        self._caller = caller
        self._call_of = call_of
        self._pids: dict[int, tuple[str | None, float]] = {}

    def __call__(self) -> str | None:
        pid = self._caller()
        now = time.monotonic()
        hit = self._pids.get(pid)
        if hit and hit[1] > now:
            return hit[0]
        call = self._call_of(pid)
        if len(self._pids) > PID_CACHE_LIMIT:
            self._pids.clear()
        self._pids[pid] = (call, now + PID_TTL_S)
        return call

    def forget(self) -> None:
        self._pids.clear()


class DriveFS:
    use_ns = True

    def __init__(
        self,
        spaces: list[Space],
        cache: Cache,
        client: DriveClient,
        uid: int,
        gid: int,
        *,
        journal: Journal,
        notices: Notices,
        caller: Callable[[], int],
        call_of: Callable[[int], str | None],
        effects: Effects | None = None,
        on_init: Callable[[], None] | None = None,
        fetcher: Fetcher | None = None,
        counts: Counter[str] | None = None,
        root: str | None = None,
    ) -> None:
        effects = effects or Effects()
        # One lock for the manifests and the files being written, held only
        # for bookkeeping. Not ``lock``: mfusepy would ask that at every close.
        mu = threading.Lock()
        self.counts: Counter[str] = counts if counts is not None else Counter()
        self.names = Names(spaces, mu)
        self.reads = Reads(cache, client, self.names, effects, self.counts, fetcher, caller, root)
        self.writes = Writes(self.names, journal, notices, effects,
                             lambda n: cache.shortfall(n, journal.root))
        self.saves = Saves(self.writes, client, cache, journal, notices, effects, self.counts)
        self.client = client
        self.effects = effects
        self.calls = _Calls(caller, call_of)
        self._uid = uid
        self._gid = gid
        self._on_init = on_init
        self._started = time.time_ns()

    # --- helpers -------------------------------------------------------------

    def _canon(self, path: str) -> str:
        """``path`` under the spelling the server stored or a file being
        written here has, when it names one another way; else itself."""
        if self.names.held(path):
            return path
        space, rel = self.names.where(path)
        if space is None or not rel:
            return path
        return self.names.stored(path) or self.names.spelled(space, rel) or path

    def _limit(self, space: Space) -> int:
        """The largest file a write may grow: what a save in parts carries,
        else what one request does."""
        return int(space.limits.get("upload_bytes") or space.limits.get("file_bytes") or FILE_MAX_BYTES)

    def _writable(self, path: str, *, load: bool) -> Pending:
        """The file being written at ``path`` (under its stored spelling),
        opened from its head when ``load`` (a write into the middle needs
        the rest)."""
        p = self.names.pending(path)
        if p is not None:
            return p
        space, rel = self.names.resolve(path)
        if not rel or self.names.generated(space, rel):
            raise fail(errno.EACCES)
        entry = space.get(rel)
        if entry is not None and entry.is_dir:
            raise fail(errno.EISDIR)
        if entry is None:
            self._parent_folder(space, rel)
        else:
            # The version the path shows, which a hold may keep on an older
            # one: the kernel places this write (an append's offset, the
            # size) by what it shows, so the edit is of that version, and a
            # newer one on the server refuses its save as changed.
            entry = self.reads.shown(space, entry)
        call = self.calls()
        rel = path.split("/", 2)[2]
        head = fill = None
        if entry is not None and load and entry.size:
            if self.reads.large(entry):
                # Copied through the cache, never whole in memory.
                def fill(fd: int) -> int:
                    return self.reads.copy_into(space, entry, fd, self.writes.shortfall)
            else:
                head = self.reads.head(space, entry)
        try:
            return self.writes.opened(path, space, rel, entry.sha256 if entry else None, head, call, fill)
        except OSError as exc:
            if exc.errno == errno.ENOSPC and getattr(exc, "short", 0):
                self.writes.no_room(space, rel, call, exc.short)
            raise

    @staticmethod
    def _parent_folder(space: Space, rel: str) -> None:
        parent = split(rel)[0]
        folder = space.get(parent) if parent else None
        if parent and (folder is None or not folder.is_dir):
            raise fail(errno.ENOENT)

    # --- attributes ----------------------------------------------------------

    def _dir_attrs(self, mtime: int | None = None) -> dict:
        t = mtime or self._started
        return {
            "st_mode": stat.S_IFDIR | 0o755,
            "st_nlink": 2,
            "st_uid": self._uid,
            "st_gid": self._gid,
            "st_mtime": t,
            "st_ctime": t,
            "st_atime": t,
        }

    def _file_attrs(self, size: int, mtime: int, mode: int = 0o644) -> dict:
        return {
            "st_mode": stat.S_IFREG | mode,
            "st_nlink": 1,
            "st_size": size,
            "st_blocks": (size + 511) // 512,
            "st_blksize": 4096,
            "st_uid": self._uid,
            "st_gid": self._gid,
            "st_mtime": mtime,
            "st_ctime": mtime,
            "st_atime": mtime,
        }

    def getattr(self, path, fh=None):
        self.counts["getattr"] += 1
        if path is None:
            raise fail(errno.EIO)  # a removed open file: see ``read``
        p = self.names.pending(path)
        if p is not None:
            return self._file_attrs(p.size, p.mtime_ns)
        space, rel = self.names.where(path)
        if space is None:
            if rel is None:
                return self._dir_attrs()
            raise fail(errno.ENOENT)
        if not rel:
            return self._dir_attrs()
        entry = space.by_path.get(rel)
        if entry is None:
            if self.names.generated(space, rel):
                return self._file_attrs(len(self.names.ignore(space)), self._started, 0o444)
            entry = space.get(rel)
            if entry is None:
                spelled = self.names.spelled(space, rel)
                p = self.names.pending(spelled) if spelled else None
                if p is not None:
                    return self._file_attrs(p.size, p.mtime_ns)
                self.names.saw_miss(space, rel, path)
                raise fail(errno.ENOENT)
            self.names.saw_alias(space, entry, path)
            p = self.names.pending(f"/{space.name}/{entry.path}")
            if p is not None:
                return self._file_attrs(p.size, p.mtime_ns)
        if entry.is_dir:
            return self._dir_attrs(entry.mtime_ns)
        entry = self.reads.shown(space, entry)
        return self._file_attrs(entry.size, entry.mtime_ns)

    def opendir(self, path):
        # Registers the callback; the daemon's FUSE class answers it, as only
        # it sees the file info that asks the kernel to keep the listing.
        return 0

    def readdir(self, path, fh):
        self.counts["readdir"] += 1
        space, rel = self.names.where(path)
        if space is None:
            if rel is None:
                return [".", "..", *((name, {"st_mode": stat.S_IFDIR}, 0) for name in sorted(self.names.spaces))]
            raise fail(errno.ENOENT)
        kids = self.names.listing(space, rel)
        if kids is None:
            raise fail(errno.ENOTDIR if space.get(rel) is not None else errno.ENOENT)
        out = {name: stat.S_IFDIR if is_dir else stat.S_IFREG for name, is_dir in kids.items()}
        folder = space.get(rel) if rel else None
        for name in self.names.pending_names_in(f"/{space.name}/{folder.path}" if folder is not None else path):
            out.setdefault(name, stat.S_IFREG)
        if not rel and space.generates_ignore():
            out.setdefault(IGNORE_NAME, stat.S_IFREG)
        return [".", "..", *((n, {"st_mode": m}, 0) for n, m in sorted(out.items()))]

    def statfs(self, path):
        self.counts["statfs"] += 1
        space = next(iter(self.names.spaces.values()), None)
        usage = space.usage if space is not None else {}
        block = 4096
        # No figure is no cap; an allowance of nothing is a full Drive, which
        # a program reading the free space must see before it writes.
        max_bytes, max_entries = usage.get("max_bytes"), usage.get("max_entries")
        total = (1 << 40) if max_bytes is None else int(max_bytes)
        used = int(usage.get("bytes") or 0)
        files = (1 << 20) if max_entries is None else int(max_entries)
        entries = int(usage.get("entries") or 0)
        free = max(total - used, 0) // block
        return {
            "f_bsize": block,
            "f_frsize": block,
            "f_blocks": total // block,
            "f_bfree": free,
            "f_bavail": free,
            "f_files": files,
            "f_ffree": max(files - entries, 0),
            "f_favail": max(files - entries, 0),
            "f_namemax": 255,
        }

    # --- reads -----------------------------------------------------------------

    def open(self, path, flags):
        # Once, at the first open: from then on the kernel opens and closes
        # files on this mount without asking (see the module docstring).
        self.counts["open"] += 1
        raise fail(errno.ENOSYS)

    def read(self, path, size, offset, fh):
        self.counts["read"] += 1
        if path is None:
            # A file removed while a process had it open, and not yet in
            # the page cache: nothing in the request says which file it is.
            raise fail(errno.EIO)
        path = self._canon(path)
        p = self.names.pending(path)
        if p is not None:
            data = self.writes.pread(p, size, offset)
            if data is not None:
                return data
        space, rel = self.names.resolve(path)
        entry = space.get(rel)
        if entry is None and self.names.generated(space, rel):
            return self.names.ignore(space)[offset : offset + size]
        # A reader partway through a large file goes on reading that file
        # whatever the path shows now, even nothing (``drive_holds``).
        return self.reads.read(space, path, entry, size, offset)

    # --- writes ----------------------------------------------------------------

    def create(self, path, mode, flags=0):
        self.counts["create"] += 1
        space, rel = self.names.resolve(path)
        if not rel or self.names.generated(space, rel):
            raise fail(errno.EACCES)
        self._parent_folder(space, rel)
        entry = space.get(rel)
        if entry is not None:
            if entry.path != nfc(rel):
                # Another name for a stored file (another case): the server
                # would save over that one.
                raise fail(errno.EEXIST)
            # The kernel held the name as absent, or under another
            # normalization; the file is there.
            if entry.is_dir:
                raise fail(errno.EISDIR)
            if flags & os.O_EXCL:
                raise fail(errno.EEXIST)
            path = f"/{space.name}/{entry.path}"
        elif not self.names.held(path):
            spelled = self.names.spelled(space, rel)
            other = self.names.pending(spelled) if spelled is not None else None
            if other is not None:
                if nfc(other.rel) != nfc(rel):
                    # Another case of a file being written here.
                    raise fail(errno.EEXIST)
                path = spelled
        if flags & os.O_TRUNC or entry is None:
            self.writes.fresh(path)
        p = self.names.pending(path)
        if p is not None:
            if flags & os.O_EXCL:
                raise fail(errno.EEXIST)
            if not flags & os.O_TRUNC or self.writes.emptied(p):
                return 0
        if entry is not None and not flags & os.O_TRUNC:
            # An open of the existing file, which ``>>`` and a plain write at
            # an offset must find whole.
            self._writable(path, load=True)
            return 0
        self.writes.made(path, space, path.split("/", 2)[2], entry.sha256 if entry else None, self.calls())
        return 0

    def write(self, path, data, offset, fh):
        self.counts["write"] += 1
        path = self._canon(path)
        while True:
            self.writes.check_refusal(path)
            p = self._writable(path, load=True)
            done = self.writes.write(p, data, offset, self.calls(), self._limit(p.space))
            if done is not None:
                return done

    def truncate(self, path, length, fh=None):
        self.counts["truncate"] += 1
        path = self._canon(path)
        if length == 0:
            self.writes.fresh(path)
        while True:
            if length:
                self.writes.check_refusal(path)
            p = self._writable(path, load=length > 0)
            if self.writes.truncate(p, length, self.calls(), self._limit(p.space)):
                return 0

    def flush(self, path, fh):
        # Once: ENOSYS tells the kernel this mount wants no flush, and it
        # sends none again.
        self.counts["flush"] += 1
        raise fail(errno.ENOSYS)

    def fsync(self, path, datasync, fh):
        """Save now and answer for the save that covers what the caller
        wrote: one under way is waited for, and a refusal that closed the
        file is answered with its errno, one with no answer at once with
        its error (the file stays to be sent again). A file too large for
        one request takes minutes to save: its bytes are synced to this
        disk, where a restart takes them up, and its save is queued."""
        self.counts["fsync"] += 1
        path = self._canon(path)
        p = self.names.pending_or_closed(path)
        if p is None:
            self.writes.check_refusal(path)
            return 0
        if p.open and self.saves.large(p):
            self.writes.sync(p)
            self.saves.queue(p)
            return 0
        with p.saving:
            if not p.open:
                if p.errno:
                    raise fail(p.errno)
                return 0
            self.saves.save_held(p, self.calls())
        return 0

    def release(self, path, fh):
        # Sent only for files this mount created.
        self.counts["release"] += 1
        return 0

    # --- names -----------------------------------------------------------------

    def unlink(self, path):
        self.counts["unlink"] += 1
        path = self._canon(path)
        space, rel = self.names.resolve(path)
        if not rel or self.names.generated(space, rel):
            raise fail(errno.EACCES)
        self.writes.fresh(path)
        p = self.names.pending(path)
        if p is None:
            entry = space.get(rel)
            if entry is None:
                raise fail(errno.ENOENT)
            if entry.is_dir:
                raise fail(errno.EISDIR)
            answer = self.client.trash(space.ref, entry.path, "file", self.calls())
        else:
            # After any save request of it under way, so the delete follows
            # it to the server rather than leaving it to land after; a save
            # in parts is abandoned rather than waited for. The edit is let
            # go only once the server took the delete: refused, it stays.
            self.saves.abandon([p])
            with p.saving:
                entry = space.get(rel)
                if entry is None:
                    with contextlib.suppress(OSError):
                        landed = self.saves.landed(p)
                        entry = entry_of(landed) if landed is not None else None
                if entry is None:
                    self.names.let_go(p)
                    return 0  # never reached the server
                if entry.is_dir:
                    raise fail(errno.EISDIR)
                answer = self.client.trash(space.ref, entry.path, "file", self.calls())
                self.names.let_go(p)
        self.names.commit_drop(space, entry.id, int(answer.get("cursor") or 0))
        return 0

    def mkdir(self, path, mode):
        self.counts["mkdir"] += 1
        space, rel = self.names.resolve(path)
        if not rel:
            raise fail(errno.EEXIST)
        if space.get(rel) is not None or self.names.held(path):
            raise fail(errno.EEXIST)
        answer = self.client.mkdir(space.ref, rel, self.calls())
        if not self.names.commit_put(space, entry_of(answer["entry"])):
            self.effects.forget(path)
        return 0

    def rmdir(self, path):
        self.counts["rmdir"] += 1
        path = self._canon(path)
        space, rel = self.names.resolve(path)
        if not rel:
            raise fail(errno.EACCES)
        entry = space.get(rel)
        if entry is None:
            raise fail(errno.ENOENT)
        if not entry.is_dir:
            raise fail(errno.ENOTDIR)
        self._trash_empty_folder(space, entry, path, self.calls())
        return 0

    def _trash_empty_folder(self, space: Space, folder: Entry, path: str, call: str | None) -> None:
        if self.names.has_children(space, folder.path) or self.names.pending_under(path):
            raise fail(errno.ENOTEMPTY)
        answer = self.client.trash(space.ref, folder.path, "folder", call)
        self.names.commit_drop(space, folder.id, int(answer.get("cursor") or 0))

    def _target(self, space: Space, src_is_dir: bool, dst: Entry | None, q: Pending | None,
                new: str, flags: int) -> bool:
        """What a rename may do to its target, ``dst`` stored there and ``q``
        being written there: raises when it may not; True when the move
        replaces a stored file."""
        if flags & _RENAME_NOREPLACE and (dst is not None or q is not None):
            raise fail(errno.EEXIST)
        if dst is None:
            if src_is_dir and self.names.pending_under(new):
                raise fail(errno.ENOTEMPTY)
            return False
        if dst.is_dir:
            if not src_is_dir:
                raise fail(errno.EISDIR)
            if self.names.has_children(space, dst.path) or self.names.pending_under(new):
                raise fail(errno.ENOTEMPTY)
            return False
        if src_is_dir:
            raise fail(errno.ENOTDIR)
        return True

    def rename(self, old, new, flags=0):
        self.counts["rename"] += 1
        if flags & _RENAME_EXCHANGE:
            raise fail(errno.EINVAL)
        old = self._canon(old)
        space, src_rel = self.names.resolve(old)
        dst_space, dst_rel = self.names.resolve(new)
        if dst_space is not space:
            raise fail(errno.EXDEV)
        if not src_rel or not dst_rel or self.names.generated(space, src_rel) or self.names.generated(space, dst_rel):
            raise fail(errno.EACCES)
        call = self.calls()
        p = self.names.pending(old)
        # A save in parts of anything the move carries or replaces is
        # abandoned, not waited for: the file is saved again where it ends up.
        self.saves.abandon([p, self.names.pending(new), *self.names.pending_under(old)])
        landed = None
        if p is not None and p.sent and self.saves.large(p) and space.get(src_rel) is None:
            # Its save in parts went unanswered and is not waited for: on the
            # server only if it landed, and then moved there.
            with p.saving:
                landed = self.saves.landed(p)
                if landed is None:
                    with p.lock:
                        p.sent = set()
        if p is not None and not p.sent and space.get(src_rel) is None:
            # Written here and never saved: an editor's or ``sed -i``'s temp
            # file renamed into place, or git's lock file. It moves here and
            # is saved under its new name, over what is there, as one write.
            if self._rename_unsaved(old, new, p, space, src_rel, dst_rel, flags):
                return 0
        # Every file the move carries (the file, or everything being written
        # under the folder) and what it replaces are held across it: a save
        # of any answers before the move or is sent after it, never across.
        with self.writes.holding([p, self.names.pending(new), *self.names.pending_under(old)]):
            if p is not None and p.unsaved and not self.saves.large(p):
                # The server must hold what is being moved. A file too large
                # for one request is not waited for: the server's head moves
                # and the file follows it, saved again under its new name.
                self.saves.save_held(p, call)
            src = space.get(src_rel)
            if src is None and landed is not None:
                src = entry_of(landed)
            if src is None:
                raise fail(errno.ENOENT)
            dst = space.get(dst_rel)
            if dst is not None and dst.id == src.id:
                dst = None  # another spelling of itself: a plain rename
            q = self.names.pending(new)
            replace = self._target(space, src.is_dir, dst, q, new, flags)
            if dst is not None and dst.is_dir:
                self._trash_empty_folder(space, dst, new, call)
            answer = self.client.move(space.ref, src.path, dst_rel, replace, call)
            if q is not None and q is not p and q not in self.names.pending_under(old):
                # Whatever was being written there is replaced, as on a disk;
                # only once the move took.
                self.names.let_go(q)
            current = self.names.commit_move(
                space, src, old, new, dst_rel, entry_of(answer["entry"]),
                list(answer.get("removed") or ()), int(answer.get("cursor") or 0),
            )
        if not current:
            self.effects.forget(old)
            self.effects.forget(new)
        return 0

    def _rename_unsaved(self, old: str, new: str, p: Pending, space: Space, src_rel: str, dst_rel: str,
                        flags: int) -> bool:
        """Move a file never saved, here; False to move it on the server
        instead (a save of it landed meanwhile, or one went unanswered: it
        may be there, and must not stay there under its old name)."""
        q = self.names.pending(new)
        with self.writes.holding([p, q]):
            if not p.open or p.sent or space.get(src_rel) is not None:
                return False
            if self.names.pending(new) is not q:
                return False
            if q is p:
                return True
            dst = space.get(dst_rel)
            self._target(space, False, dst, q, new, flags)
            if q is not None:
                # Whatever was being written there is replaced, as on a disk.
                self.names.let_go(q)
            self.writes.fresh(new)
            dst = space.get(dst_rel)
            if not self.names.commit_rename_unsaved(old, p, dst.path if dst else dst_rel, dst.sha256 if dst else None):
                raise fail(errno.ENOENT)
        return True

    # --- accepted and ignored ------------------------------------------------

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        return 0

    def utimens(self, path, times=None):
        return 0

    # --- lifecycle -------------------------------------------------------------

    def init_with_config(self, conn, config):
        without_atomic_trunc(conn)
        if conn is not None:
            conn.max_background = MAX_BACKGROUND
            conn.congestion_threshold = MAX_BACKGROUND * 3 // 4
        if self._on_init is not None:
            self._on_init()

    def destroy(self, path):
        self.saves.save_all()
