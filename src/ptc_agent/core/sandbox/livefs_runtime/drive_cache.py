"""The Drive's bytes on the computer's disk, one file per sha256: whole files,
and large binaries a block at a time while they are being read.

Locking: every change to what the cache holds, and every fetch's start,
join, cancel and end (``drive_fetch``), happens under ``Cache.lock``, which
is never held across a network request or a wait for one; and room for a
file being written is made before that file's own lock is taken, so nothing
takes ``Cache.lock`` while it holds another lock.

Kept in root's private directory, not in a workspace folder: it is a cache of
what the server holds, so it must outlive a daemon restart (or every restart
would fetch the whole Drive again) but never be backed up, searched or
edited as the user's files. A whole file is ``objects/<sha[:2]>/<sha>`` and
never changes once there. A large binary being read is a sparse file in
``objects/partial/``, holding the blocks read so far; once every block
arrived and the bytes match the digest it is renamed into place, which
keeps its inode, so the descriptors reading it stay valid. Each partial file
is held under ``flock`` by the daemon filling it, so a daemon starting
removes exactly those no running daemon holds, whatever its pid. Which
blocks a partial file holds lives in memory only: keeping a map on disk
would mean writing it before every eviction, or trusting one that may name
blocks a crash left as holes.

One budget for binaries, whole files and blocks alike, counted with every
fetch's claim before a byte is fetched (``claim``), and one order to evict
them in, by last use. Every write first leaves ``reserve`` bytes free on the
filesystem; text is evicted only to keep that, after every binary that may
go, and once some has had to go the cache no longer holds all of it
(``missing_text``), so text is fetched on demand as binaries are. Content
written within ``GRACE_S`` is never evicted: a save or a prefetch stores it a
moment before the manifest names it.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import os
import secrets
import tempfile
import threading
import time
from collections import Counter, OrderedDict
from typing import Any

from .drive_disk import punch, sha256_fd
from .drive_tree import sha256_or_none

#: What the cache holds and evicts of a large binary. A columnar reader
#: takes one column's chunk (about 0.5 MB in a DuckDB Parquet) from each row
#: group; at 1 MiB a one-column scan of a 5 GB file fetches 1.7 GB, at
#: 4 MiB the whole file.
BLOCK = 1 << 20
#: Descriptors of whole files kept open for reads, most recently used last.
FD_OPEN = 64
#: Free bytes left on the filesystem at least, and the share of the
#: filesystem when that is larger.
RESERVE_MIN = 512 * 1024 * 1024
RESERVE_SHARE = 0.05
GRACE_S = 60.0
#: A temp file older than this is a write cut short: no daemon writes one
#: for so long.
TMP_STALE_S = 600.0

_Unit = tuple[str, int | None]


def _digest(sha: str) -> str:
    """``sha`` as a file name, only when it is a sha256: digests come from
    the server (``drive_tree.sha256_or_none`` checks them there too)."""
    if sha256_or_none(sha) is None:
        raise OSError(errno.EIO, "not a sha256")
    return sha


class Item:
    """One sha256 held here, whole (``have`` is None) or in part."""

    __slots__ = ("sha", "size", "text", "path", "fd", "users", "used", "have", "dark", "fetching",
                 "using", "readahead", "finishing", "gone")

    def __init__(self, sha: str, size: int, path: str, used: float, have: bytearray | None = None,
                 fd: int | None = None) -> None:
        self.sha = sha
        self.size = size
        self.text = False
        self.path = path
        self.fd = fd
        #: Threads using ``fd`` now (a read's pread, a fetch's writes): it is
        #: closed only once none is.
        self.users = 0
        self.used = used
        #: Of a partial file, each block held (1) or not.
        self.have = have
        #: Bytes a cut fetch left on a disk that cannot punch them out:
        #: counted held, freed only with the whole file.
        self.dark = 0
        #: Blocks being fetched, to the fetch (``drive_fetch``'s).
        self.fetching: dict[int, Any] = {}
        #: Blocks a read is between its wait and its pread of: never evicted.
        self.using: Counter[int] = Counter()
        #: Its readers' streams (``drive_readahead``).
        self.readahead: Any = None
        #: Being checked against its digest: none of its blocks may go.
        self.finishing = False
        self.gone = False

    @property
    def whole(self) -> bool:
        return self.have is None


class Cache:
    def __init__(self, root: str, budget: int, reserve: int | None = None, *, block: int = BLOCK,
                 counts: Counter[str] | None = None) -> None:
        self.root = root
        self.counts: Counter[str] = counts if counts is not None else Counter()
        self.objects = os.path.join(root, "objects")
        self.partials = os.path.join(self.objects, "partial")
        self.tmp = os.path.join(root, "tmp")
        self.budget = budget
        self.block = block
        self._reserve = reserve
        self.lock = threading.Lock()
        self._items: dict[str, Item] = {}
        #: Binary units by last use, least recent first: (sha, None) for a
        #: whole binary, (sha, i) for a block of a partial one.
        self._lru: OrderedDict[_Unit, None] = OrderedDict()
        #: Bytes of binaries held, whole and in blocks.
        self.binaries = 0
        #: Bytes claimed for fetches and writes not landed yet.
        self.claimed = 0
        #: Whole files with a descriptor open, least recently read first.
        self._open: OrderedDict[str, Item] = OrderedDict()
        self.missing_text = False
        #: Writes refused for room, since start.
        self.refused = 0

    def _path(self, sha: str) -> str:
        return os.path.join(self.objects, sha[:2], _digest(sha))

    # --- start ----------------------------------------------------------------

    def scan(self) -> None:
        """Learn what an earlier daemon left. A temp file is a write cut short;
        a partial file no running daemon holds is a read cut short."""
        for folder in (self.objects, self.partials, self.tmp):
            os.makedirs(folder, mode=0o700, exist_ok=True)
        now = time.time()
        for name in os.listdir(self.tmp):
            path = os.path.join(self.tmp, name)
            with contextlib.suppress(OSError):
                # A daemon this one replaces may still be writing a new one.
                if now - os.stat(path).st_mtime >= TMP_STALE_S:
                    os.unlink(path)
        for name in os.listdir(self.partials):
            path = os.path.join(self.partials, name)
            with contextlib.suppress(OSError):
                fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    os.unlink(path)
                except BlockingIOError:
                    pass  # a daemon this one replaces is still reading it
                finally:
                    os.close(fd)
        items: dict[str, Item] = {}
        for bucket in os.listdir(self.objects):
            if bucket == "partial":
                continue
            with contextlib.suppress(OSError), os.scandir(os.path.join(self.objects, bucket)) as it:
                for entry in it:
                    if sha256_or_none(entry.name) is None:
                        continue
                    st = entry.stat(follow_symlinks=False)
                    items[entry.name] = Item(entry.name, st.st_size, entry.path, st.st_mtime)
        with self.lock:
            self._items = items
            self._lru = OrderedDict(((s, None), None) for s, _ in sorted(items.items(), key=lambda kv: kv[1].used))
            self.binaries = sum(i.size for i in items.values())

    # --- what is held -------------------------------------------------------

    def has(self, sha: str | None) -> bool:
        item = self._items.get(sha) if sha is not None else None
        return item is not None and item.have is None

    def files(self) -> int:
        with self.lock:
            return sum(1 for i in self._items.values() if i.have is None)

    def bytes_held(self) -> int:
        with self.lock:
            return self.binaries + sum(i.size for i in self._items.values() if i.text)

    def _touch(self, item: Item, unit: _Unit) -> None:
        item.used = time.time()
        if not item.text and unit in self._lru:
            self._lru.move_to_end(unit)

    # --- room -----------------------------------------------------------------

    def reserve(self) -> int:
        if self._reserve is not None:
            return self._reserve
        try:
            st = os.statvfs(self.root)
        except OSError:
            return RESERVE_MIN
        return max(RESERVE_MIN, int(st.f_blocks * st.f_frsize * RESERVE_SHARE))

    def free(self) -> int:
        try:
            st = os.statvfs(self.root)
        except OSError:
            return 0
        return st.f_bavail * st.f_frsize

    def claim(self, size: int, *, binary: bool = True) -> bool:
        """Set ``size`` bytes aside for a write about to land, under the
        binary budget (a binary's) and the disk's reserve, both counted with
        every claim not landed yet, evicting what was used least recently to
        make room. False when there is no room even then; the caller writes
        nothing. Given back with ``unclaim``, or by ``landed``."""
        with self.lock:
            for _ in range(2):
                over = self.binaries + self.claimed + size - self.budget if binary else 0
                short = size + self.reserve() + self.claimed - self.free()
                if over <= 0 and short <= 0:
                    self.claimed += size
                    return True
                self._evict(max(over, short), text=short > 0)
            return False

    def unclaim(self, size: int) -> None:
        with self.lock:
            self.claimed = max(self.claimed - size, 0)

    def shortfall(self, size: int, where: str | None = None) -> int:
        """Make room for ``size`` bytes written outside the cache, at ``where``
        (a file being written here, in the journal); how many bytes are still
        missing (0: room). On the cache's own disk it gives back what it can
        first; on another, only that disk's free room counts."""
        if where is not None and not self._same_disk(where):
            try:
                st = os.statvfs(where)
            except OSError:
                return 0
            free = st.f_bavail * st.f_frsize
            keep = max(RESERVE_MIN, int(st.f_blocks * st.f_frsize * RESERVE_SHARE))
            return max(size + keep - free, 0)
        with self.lock:
            short = size + self.reserve() + self.claimed - self.free()
            if short > 0:
                self._evict(short, text=True)
                short = size + self.reserve() + self.claimed - self.free()
            return max(short, 0)

    def _same_disk(self, where: str) -> bool:
        try:
            return os.stat(where).st_dev == os.stat(self.root).st_dev
        except OSError:
            return True

    def _evict(self, need: int, *, text: bool) -> int:
        """Give back ``need`` bytes: binaries by last use, never one written
        within ``GRACE_S``, a block a read is about to take or a fetch is
        filling; then, only when ``text``, text. Returns the bytes freed, as
        the disk freed them. With ``lock``."""
        now = time.time()
        freed = 0
        for unit in list(self._lru):
            if freed >= need:
                break
            sha, i = unit
            item = self._items.get(sha)
            if item is None:
                self._lru.pop(unit, None)  # dropped whole earlier in this pass
                continue
            if i is None:
                if now - item.used < GRACE_S:
                    continue
                freed += self._drop(item)
                continue
            if item.using[i] or i in item.fetching or item.finishing:
                continue
            span = self.span(item, i)
            if punch(item.fd, i * self.block, span):
                item.have[i] = 0
                del self._lru[unit]
                self.binaries -= span
                freed += span
                self.counts["block_evicted"] += 1
            elif not item.fetching and not +item.using:
                # No holes on this filesystem: only the whole file gives
                # its disk back, once nothing reads or fills it.
                self.counts["block_evicted"] += sum(item.have)
                freed += self._drop(item)
        if text and freed < need:
            for item in sorted((x for x in self._items.values() if x.text and x.have is None), key=lambda x: x.used):
                if freed >= need:
                    break
                if now - item.used < GRACE_S:
                    continue
                self.missing_text = True
                freed += self._drop(item)
        return freed

    def _drop(self, item: Item) -> int:
        """Forget ``item`` and remove its file; the binary bytes that frees.
        Its descriptor closes once no thread uses it. With ``lock``."""
        if item.gone:
            return 0
        item.gone = True
        if self._items.get(item.sha) is item:
            del self._items[item.sha]
        if item.have is None:
            freed = item.size
            self._lru.pop((item.sha, None), None)
            if not item.text:
                self.binaries -= item.size
        else:
            freed = item.dark + sum(self.span(item, i) for i, on in enumerate(item.have) if on)
            for i, on in enumerate(item.have):
                if on:
                    self._lru.pop((item.sha, i), None)
            self.binaries -= freed
        self._open.pop(item.sha, None)
        with contextlib.suppress(OSError):
            os.unlink(item.path)
        if item.fd is not None and item.users == 0:
            os.close(item.fd)
            item.fd = None
        return freed

    def span(self, item: Item, i: int) -> int:
        return min(self.block, item.size - i * self.block)

    # --- whole files ----------------------------------------------------------

    def put(self, sha: str, data: bytes, *, text: bool = False) -> bool:
        """Keep ``data`` under ``sha``. False when it is not that content, or
        there is no room for it (the caller serves it from memory then)."""
        if hashlib.sha256(data).hexdigest() != sha:
            return False
        with self.lock:
            item = self._items.get(sha)
            if item is not None and item.have is None:
                self._touch(item, (sha, None))
                return True
        if not self.claim(len(data), binary=not text):
            self.refused += 1
            if text:
                self.missing_text = True
            return False
        try:
            fd, tmp = tempfile.mkstemp(dir=self.tmp)
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                os.makedirs(os.path.dirname(self._path(sha)), mode=0o700, exist_ok=True)
                os.replace(tmp, self._path(sha))
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
        except OSError as exc:
            self.unclaim(len(data))
            if exc.errno not in (errno.ENOSPC, errno.EDQUOT):
                raise
            # The disk filled under the check (something else wrote).
            self.refused += 1
            if text:
                self.missing_text = True
            return False
        with self.lock:
            self.claimed = max(self.claimed - len(data), 0)
            self._add_whole(sha, len(data), text)
        return True

    def install(self, sha: str, path: str, size: int) -> bool:
        """Take a file already on this disk (a save's bytes, whole and named
        by their sha256) as the content of ``sha``, by a second name: no
        copy, no fetch back."""
        with self.lock:
            if self.has(sha):
                return True
        try:
            os.makedirs(os.path.dirname(self._path(sha)), mode=0o700, exist_ok=True)
            os.link(path, self._path(sha))
        except FileExistsError:
            pass
        except OSError:
            return False
        with self.lock:
            self._add_whole(sha, size, False)
            over = self.binaries + self.claimed - self.budget
            if over > 0:
                self._evict(over, text=False)
        return True

    def _add_whole(self, sha: str, size: int, text: bool) -> None:
        """With ``lock``. A partial file of the same bytes stays its own
        until it finishes or goes."""
        old = self._items.get(sha)
        if old is not None and old.have is None:
            return
        if old is not None:
            self._drop(old)
        item = self._items[sha] = Item(sha, size, self._path(sha), time.time())
        item.text = text
        if not text:
            self._lru[(sha, None)] = None
            self.binaries += size

    def pread(self, sha: str, size: int, offset: int) -> bytes | None:
        """Bytes of the whole file ``sha``, or None when it is not here. Any
        number of threads at once."""
        with self.lock:
            item = self._items.get(sha)
            if item is None or item.have is not None:
                return None
            if item.fd is None:
                try:
                    item.fd = os.open(item.path, os.O_RDONLY | os.O_CLOEXEC)
                except FileNotFoundError:
                    self._drop(item)
                    return None
            closing = self._keep_open(item)
            item.users += 1
            self._touch(item, (sha, None))
        for fd in closing:
            os.close(fd)
        try:
            data = os.pread(item.fd, size, offset)
            if len(data) < size or offset + size >= item.size:
                # The kernel keeps what it read in the mount's own page cache;
                # a second copy of the same bytes here would only crowd it.
                with contextlib.suppress(OSError):
                    os.posix_fadvise(item.fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            self.release(item)
        return data

    def _keep_open(self, item: Item) -> list[int]:
        """Keep ``item``'s descriptor among the ``FD_OPEN`` read last; the
        descriptors that falls out of it, for the caller to close outside
        ``lock``. With ``lock``."""
        closing: list[int] = []
        self._open[item.sha] = item
        self._open.move_to_end(item.sha)
        while len(self._open) > FD_OPEN:
            _, old = self._open.popitem(last=False)
            if old.users == 0 and old.fd is not None:
                closing.append(old.fd)
                old.fd = None
        return closing

    def release(self, item: Item) -> None:
        """A thread is done with ``item``'s descriptor."""
        with self.lock:
            item.users -= 1
            # A partial file's stays open for its fetches; a whole file's
            # while it is among the ``FD_OPEN`` read last.
            closes = item.gone or (item.whole and item.sha not in self._open)
            if item.users or item.fd is None or not closes:
                return
            fd, item.fd = item.fd, None
        os.close(fd)

    # --- partial files (``drive_fetch``) ---------------------------------------

    def item_for(self, sha: str, size: int) -> Item:
        """The item of ``sha``: whole, or partial, made empty here when it is
        not held at all. With ``lock``."""
        item = self._items.get(sha)
        if item is not None:
            return item
        path = os.path.join(self.partials, f"{_digest(sha)}.{secrets.token_hex(4)}")
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, size)
        except BaseException:
            os.close(fd)
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise
        item = self._items[sha] = Item(sha, size, path, time.time(), bytearray(-(-size // self.block)), fd)
        return item

    def use(self, item: Item, i: int) -> None:
        """A read found block ``i`` held. With ``lock``."""
        self._touch(item, (item.sha, None) if item.have is None else (item.sha, i))

    def landed(self, item: Item, first: int, last: int, claimed: int) -> bool:
        """A fetch wrote blocks ``first`` to ``last``: held now, in place of
        its claim. True when the file now holds every block. With ``lock``."""
        self.claimed = max(self.claimed - claimed, 0)
        if item.gone or item.have is None:
            return False
        for i in range(first, last + 1):
            if not item.have[i]:
                item.have[i] = 1
                self.binaries += self.span(item, i)
                self._lru[(item.sha, i)] = None
        item.used = time.time()
        return all(item.have)

    def cut(self, item: Item, first: int, last: int, owner: Any, claimed: int) -> None:
        """A fetch that did not finish: nothing it wrote stays held, and its
        blocks are let go in the same hold. A block it was cancelled off may
        have been fetched again since, so only a block no other fetch holds or
        has landed is punched. Where the disk cannot punch it out it is counted
        held until the whole file goes."""
        with self.lock:
            self.claimed = max(self.claimed - claimed, 0)
            fetching = item.fetching
            spans: list[list[int]] = []
            for i in range(first, last + 1):
                mine = fetching.get(i)
                if mine is owner:
                    del fetching[i]
                elif mine is not None:
                    continue
                if item.have is not None and item.have[i]:
                    continue
                start = i * self.block
                stop = min(item.size, start + self.block)
                if spans and spans[-1][1] == start:
                    spans[-1][1] = stop
                else:
                    spans.append([start, stop])
            if item.gone or item.fd is None:
                return
            for start, stop in spans:
                if not punch(item.fd, start, stop - start):
                    item.dark += stop - start
                    self.binaries += stop - start

    def finish(self, item: Item) -> bool:
        """A partial file whose every block arrived: checked against its
        digest and renamed into place, from then on a whole file. Its blocks
        are dropped, to be read again, when the bytes are not the file."""
        with self.lock:
            if item.gone or item.have is None or item.finishing:
                return False
            item.users += 1
            item.finishing = True
        try:
            digest = sha256_fd(item.fd, item.size)
        except OSError:
            digest = None
        finally:
            # Still ``finishing``: eviction keeps off its blocks until the
            # digest is acted on, below, under the same lock as the rename.
            self.release(item)
        with self.lock:
            item.finishing = False
            if item.gone or item.have is None:
                return False
            if digest != item.sha:
                self._drop(item)
                return False
            try:
                os.makedirs(os.path.dirname(self._path(item.sha)), mode=0o700, exist_ok=True)
                os.rename(item.path, self._path(item.sha))
            except OSError:
                return False
            for i in range(len(item.have)):
                self._lru.pop((item.sha, i), None)
            self.binaries += item.size - item.dark - sum(self.span(item, i) for i, on in enumerate(item.have) if on)
            item.have, item.dark, item.path = None, 0, self._path(item.sha)
            item.readahead = None
            self._lru[(item.sha, None)] = None
            closing = self._keep_open(item)
        for fd in closing:
            os.close(fd)
        return True

    # --- collecting -------------------------------------------------------------

    def collect(self, keep: set[str], text: set[str]) -> int:
        """Remove what no live entry names, then the least recently used
        binaries past the budget. Text is evicted only for room on the disk
        (``claim``): a search should not wait on the server. Content written
        within ``GRACE_S`` stays, and so does anything a read or fetch is on.
        Returns the bytes removed."""
        now = time.time()
        with self.lock:
            for item in self._items.values():
                if item.have is None and item.text != (item.sha in text):
                    item.text = item.sha in text
                    if item.text:
                        self._lru.pop((item.sha, None), None)
                        self.binaries -= item.size
                    else:
                        self._lru[(item.sha, None)] = None
                        self.binaries += item.size
            freed = 0
            for item in list(self._items.values()):
                if (item.sha not in keep and now - item.used >= GRACE_S and not item.users
                        and not item.fetching and not +item.using):
                    freed += self._drop(item)
            over = self.binaries + self.claimed - self.budget
            if over > 0:
                freed += self._evict(over, text=False)
            return freed
