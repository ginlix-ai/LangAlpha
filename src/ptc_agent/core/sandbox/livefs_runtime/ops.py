"""The file operations the mount answers, each a request to the server.

What a command sees and what outlives it are ``views``: each operation here
asks the server, then records what it learned there with one change.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import stat
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, NamedTuple

from .protocol import (
    MAX_FILE_BYTES,
    PROVISIONAL_HEADER,
    Refusal,
    RemovedAnswer,
    SaveAnswer,
    code_of,
    etag,
    etag_version,
)
from .remote import Remote, error_for, fail
from .views import CONTENT_BYTES, DIR, Entry, View, Views, split

if TYPE_CHECKING:
    import http.client

PID_TTL_S = 2.0
#: Entries the pid cache holds before it starts over: pids are reused, so a
#: busy sandbox would otherwise grow it for good.
PID_CACHE_LIMIT = 4096

_GONE = (Refusal.NOT_FOUND, Refusal.NOT_DIRECTORY)


def _version_read(resp: http.client.HTTPResponse) -> str:
    """The version a ``/read`` answered with, failing for none: a save naming
    no version would make the file anew. It is not worked out from the bytes,
    being the route's and not always their digest (an automation keeps its
    version while its run state moves)."""
    version = etag_version(resp.getheader("ETag"))
    if version is None:
        raise fail(errno.EIO)
    return version


class _Sent(NamedTuple):
    body: bytes
    rev: int


class Handle:
    __slots__ = (
        "path",
        "buf",
        "base",
        "dirty",
        "written",
        "append",
        "call",
        "writable",
        "writing",
        "mtime",
        "unanswered",
        "refused",
        "rev",
        "lock",
        "saving",
    )

    def __init__(
        self, path, buf, base, *, dirty, append, call, writable, writing=True, mtime=0
    ) -> None:
        self.path = path
        #: Bytes until the first write or resize, so a read copies nothing.
        self.buf: bytes | bytearray = buf
        self.base = base
        self.dirty = dirty
        #: Written or resized since open, not only created or truncated by it.
        self.written = False
        self.append = append
        self.call = call
        self.writable = writable
        #: Opened to write: only such a handle holds bytes the server has not.
        self.writing = writing
        #: A read handle's bytes are the file's as it was opened, so it keeps
        #: their time; the path's own time moves on with later saves.
        self.mtime = mtime
        #: What its last save sent, when that got no answer: it may have
        #: landed, and the next save then goes over it.
        self.unanswered: _Sent | None = None
        #: Grown past the size limit once, so ``buf`` holds only part of what
        #: the program meant to write: nothing from this handle is saved.
        self.refused = False
        #: Moved by every change to ``buf``: a save answered after a later
        #: write leaves the handle dirty, so that write is saved too.
        self.rev = 0
        #: ``lock`` guards ``buf`` and ``rev``; ``saving`` lets one save of
        #: the handle run at a time without holding writes for the request.
        self.lock = threading.Lock()
        self.saving = threading.Lock()

    def mutable(self) -> bytearray:
        if not isinstance(self.buf, bytearray):
            self.buf = bytearray(self.buf)
        return self.buf


class LiveFS:
    """The mfusepy operations. mfusepy registers the methods it finds by
    name, so every public method here is a FUSE callback.

    ``caller`` names the pid of the process a request comes from, and
    ``call_of`` the call that pid runs for.
    """

    use_ns = True

    def __init__(
        self,
        remote: Remote,
        uid: int,
        gid: int,
        *,
        caller: Callable[[], int],
        call_of: Callable[[int], str | None],
        layout: str | None = None,
        content_bytes: int = CONTENT_BYTES,
    ) -> None:
        self._remote = remote
        self._uid = uid
        self._gid = gid
        self._caller = caller
        self._call_of = call_of
        self._views = Views(content_bytes, layout)
        #: The one lock, which also guards the handles here.
        self._lock = self._views.lock
        self._pids: dict[int, tuple[str | None, float]] = {}
        self._handles: dict[int, Handle] = {}
        self._next_fh = 1
        self._mtimes: dict[str, tuple[str, int]] = {}
        self._started = time.time_ns()

    # --- whose view -----------------------------------------------------

    def _call(self) -> str | None:
        pid = self._caller()
        now = time.monotonic()
        with self._lock:
            hit = self._pids.get(pid)
            if hit and hit[1] > now:
                return hit[0]
        call = self._call_of(pid)
        with self._lock:
            if len(self._pids) > PID_CACHE_LIMIT:
                self._pids.clear()
            self._pids[pid] = (call, now + PID_TTL_S)
        return call

    def _here(self) -> tuple[str | None, View]:
        call = self._call()
        return call, self._views.view(call)

    def _refused(self, path: str, status: int, data: bytes, view: View | None = None) -> OSError:
        """The error a refusal reaches the program as."""
        if status == 404 or code_of(data) in _GONE:
            self._views.gone(path, view)
        return error_for(status, data)

    # --- server reads ---------------------------------------------------

    def _listing(
        self, path: str, view: View, call: str | None, name: str | None = None
    ) -> dict[str, Entry]:
        entries = self._views.listing(path, view, name)
        if entries is not None:
            return entries
        status, _, data = self._remote.request(
            "GET", "list", {"path": path.lstrip("/")}, call=call
        )
        if status != 200:
            raise self._refused(path, status, data, view)
        return self._views.listed(path, json.loads(data), view, name)

    def _on_server(self, path: str, view: View, call: str | None) -> bool:
        """Whether the server holds files under the directory. A directory
        kept here may also still be there, and then the server's rules for
        it apply, not the local ones."""
        try:
            self._listing(path, view, call)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return False
            raise
        return True

    def _require_creatable(self, directory: str) -> None:
        """Refuse at create, where the caller sees it; a refusal at close
        is one most programs never check."""
        call, view = self._here()
        if not self._on_server(directory, view, call):
            if self._views.is_local(directory, view):
                return
            raise fail(errno.ENOENT)
        if not self._views.creatable(directory, view):
            raise fail(errno.EACCES)

    def _entry(self, path: str, view: View, call: str | None) -> Entry | None:
        if path == "/":
            return DIR
        parent, name = split(path)
        try:
            entries = self._listing(parent, view, call, name)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return None
            raise
        return entries.get(name)

    def _held(self, path: str, version: str, call: str | None) -> bytes | None:
        """That version's bytes, from a handle the command still has open on
        it: another command's may hold other bytes under the same version."""
        with self._lock:
            for handle in self._handles.values():
                if handle.path != path or handle.call != call:
                    continue
                with handle.lock:
                    if handle.base == version and not (handle.dirty or handle.written):
                        return bytes(handle.buf)
        return None

    def _content(
        self, path: str, view: View, call: str | None, version: str | None
    ) -> tuple[bytes, str]:
        """The file's bytes as this command sees them: the version it read
        or saved, else ``version``, the one its listing names.

        The daemon may have let a version's bytes go since this command read
        them. Fetched again as another version, the file changed after this
        command read it, which it cannot be shown as if it had not.
        """
        pinned, data = self._views.cached(path, version, view)
        if data is None and pinned:
            data = self._held(path, pinned, call)
        if data is not None:
            return data, pinned or version
        status, resp, data = self._remote.request(
            "GET", "read", {"path": path.lstrip("/")}, call=call
        )
        if status != 200:
            raise self._refused(path, status, data, view)
        got = _version_read(resp)
        # A version that is not a digest can come back with other bytes;
        # those are shown, as they would be had nothing been let go.
        if pinned and got != pinned:
            raise fail(errno.ESTALE)
        self._views.read(path, got, data, view)
        return data, got

    # --- attributes -----------------------------------------------------

    def _mtime(self, path: str, version: str | None) -> int:
        with self._lock:
            seen = self._mtimes.get(path)
            if seen is None or seen[0] != version:
                seen = (version, time.time_ns() if seen else self._started)
                self._mtimes[path] = seen
            return seen[1]

    def _dir_attrs(self) -> dict:
        return {
            "st_mode": stat.S_IFDIR | 0o755,
            "st_nlink": 2,
            "st_uid": self._uid,
            "st_gid": self._gid,
            "st_mtime": self._started,
            "st_ctime": self._started,
            "st_atime": self._started,
        }

    def _file_attrs(self, size: int, writable: bool, mtime: int) -> dict:
        return {
            "st_mode": stat.S_IFREG | (0o644 if writable else 0o444),
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

    def _pending(self, path: str) -> Handle | None:
        """An open handle whose bytes are newer than the server's."""
        with self._lock:
            for handle in self._handles.values():
                if (
                    handle.path == path
                    and handle.writing
                    and (handle.dirty or handle.base is None)
                ):
                    return handle
        return None

    def getattr(self, path, fh=None):
        handle = self._handles.get(fh) if fh else None
        if handle is not None and not handle.writing:
            return self._file_attrs(len(handle.buf), handle.writable, handle.mtime)
        handle = handle or self._pending(path)
        if handle is not None:
            return self._file_attrs(len(handle.buf), True, time.time_ns())
        if self._views.is_local(path, None):
            return self._dir_attrs()
        call, view = self._here()
        if self._views.is_local(path, view):
            return self._dir_attrs()
        entry = self._entry(path, view, call)
        if entry is None:
            raise fail(errno.ENOENT)
        if entry.is_dir:
            return self._dir_attrs()
        return self._file_attrs(entry.size, entry.writable, self._mtime(path, entry.version))

    def readdir(self, path, fh):
        call, view = self._here()
        try:
            names = set(self._listing(path, view, call))
        except OSError as exc:
            if exc.errno != errno.ENOENT or not self._views.is_local(path, view):
                raise
            names = set()
        names |= self._views.local_names(path, view)
        with self._lock:
            names |= {
                split(h.path)[1]
                for h in self._handles.values()
                if h.writing and h.base is None and split(h.path)[0] == path
            }
        return [".", "..", *sorted(names)]

    # --- files ----------------------------------------------------------

    def _add(self, handle: Handle) -> int:
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = handle
            return fh

    def open(self, path, flags):
        writing = (flags & os.O_ACCMODE) != os.O_RDONLY
        if writing:
            # A handle that made or emptied the file saves first, so this one
            # starts from the file it made.
            self._settle(path)
        else:
            pending = self._pending(path)
            if pending is not None:
                # What the writer holds so far, as a disk would show it.
                with pending.lock:
                    held = bytes(pending.buf)
                return self._add(
                    Handle(
                        path,
                        held,
                        None,
                        dirty=False,
                        append=False,
                        call=self._call(),
                        writable=True,
                        writing=False,
                        mtime=time.time_ns(),
                    )
                )
        call, view = self._here()
        entry = self._entry(path, view, call)
        if entry is None:
            raise fail(errno.ENOENT)
        if entry.is_dir:
            raise fail(errno.EISDIR)
        writable = entry.writable
        if writing and not writable:
            raise fail(errno.EACCES)
        if writing and flags & os.O_TRUNC:
            buf, base = b"", entry.version
        else:
            buf, base = self._content(path, view, call, entry.version)
        return self._add(
            Handle(
                path,
                buf,
                base,
                dirty=bool(writing and flags & os.O_TRUNC),
                append=bool(flags & os.O_APPEND),
                call=call,
                writable=writable,
                writing=writing,
                mtime=0 if writing else self._mtime(path, base),
            )
        )

    def create(self, path, mode, flags=0):
        parent, _ = split(path)
        if self.getattr(parent)["st_mode"] & stat.S_IFDIR == 0:
            raise fail(errno.ENOTDIR)
        self._require_creatable(parent)
        return self._add(
            Handle(
                path,
                b"",
                None,
                dirty=True,
                append=bool(flags & os.O_APPEND),
                call=self._call(),
                writable=True,
            )
        )

    def read(self, path, size, offset, fh):
        buf = self._handles[fh].buf
        if isinstance(buf, bytes):
            return buf[offset : offset + size]
        return bytes(buf[offset : offset + size])

    def write(self, path, data, offset, fh):
        handle = self._handles[fh]
        with handle.lock:
            buf = handle.mutable()
            if handle.append:
                offset = len(buf)
            end = offset + len(data)
            if end > MAX_FILE_BYTES:
                handle.refused = True
                raise fail(errno.EFBIG)
            if offset > len(buf):
                buf.extend(b"\0" * (offset - len(buf)))
            buf[offset:end] = data
            handle.dirty = handle.written = True
            handle.rev += 1
        return len(data)

    def truncate(self, path, length, fh=None):
        if length > MAX_FILE_BYTES:
            if fh is not None:
                self._handles[fh].refused = True
            raise fail(errno.EFBIG)
        if fh is not None:
            self._resize(self._handles[fh], length)
            return 0
        fh = self.open(path, os.O_WRONLY)
        try:
            handle = self._handles[fh]
            self._resize(handle, length)
            self._save(handle)
        finally:
            self.release(path, fh)
        return 0

    @staticmethod
    def _resize(handle: Handle, length: int) -> None:
        with handle.lock:
            buf = handle.mutable()
            if length < len(buf):
                del buf[length:]
            else:
                buf.extend(b"\0" * (length - len(buf)))
            handle.dirty = handle.written = True
            handle.rev += 1

    def flush(self, path, fh):
        # Every close() flushes, including the shell's close of the fd it
        # has just dup2'd for `> file`, before anything is written. A handle
        # that has only made or emptied its file saves at release instead,
        # or once another operation on the path needs the server to hold it
        # (``_settle``), so the bytes the command then writes make one save.
        handle = self._handles.get(fh)
        if handle is not None and handle.dirty and handle.written:
            self._save(handle)
        return 0

    def fsync(self, path, datasync, fh):
        handle = self._handles.get(fh)
        if handle is not None and handle.dirty:
            self._save(handle)
        return 0

    def release(self, path, fh):
        handle = self._handles.get(fh)
        try:
            if handle is not None and handle.dirty:
                first = handle.unanswered is None
                try:
                    self._save(handle)
                except OSError:
                    # A handle that only made or emptied its file is not saved
                    # at flush, so this was its first save. With no answer it
                    # gets the one checked retry a flushed save gets here;
                    # dropped instead, a save lost on the way would leave the
                    # file as it was, unreported.
                    if not first or handle.unanswered is None:
                        raise
                    self._save(handle)
        finally:
            with self._lock:
                self._handles.pop(fh, None)
        return 0

    def _settle(self, path: str, *, written: bool = False) -> None:
        """Send the saves ``flush`` left for release on ``path``, for an
        operation the server must see the made or emptied file for first;
        ``written`` sends unflushed writes too, which a rename carries along.
        Provisional, since bytes may still follow on those handles: a refusal
        is not reported, and release sends it again if none did."""
        with self._lock:
            waiting = [
                h
                for h in self._handles.values()
                if h.path == path and h.writing and h.dirty and (written or not h.written)
            ]
        for handle in waiting:
            with contextlib.suppress(OSError):
                self._save(handle, provisional=True)

    def _save(self, handle: Handle, *, provisional: bool = False) -> None:
        # A write may land while the request is out: the bytes sent are
        # taken at ``rev``, and only an answer for the latest one leaves the
        # handle clean.
        with handle.saving:
            with handle.lock:
                if not handle.dirty:
                    return  # the save this one waited for sent it all
                if handle.refused:
                    # Failed once, like a refusal the server reports.
                    handle.dirty = False
                    raise fail(errno.EFBIG)
                body, rev = bytes(handle.buf), handle.rev
            if handle.unanswered is not None and self._landed(handle):
                with handle.lock:
                    if not handle.dirty:
                        return  # the save with no answer had sent it all
            headers = (
                {"If-Match": etag(handle.base)}
                if handle.base
                else {"If-None-Match": "*"}
            )
            if provisional:
                headers[PROVISIONAL_HEADER] = "1"
            call = self._call() or handle.call
            try:
                status, _, data = self._remote.request(
                    "PUT",
                    "write",
                    {"path": handle.path.lstrip("/")},
                    body=body,
                    headers=headers,
                    call=call,
                )
            except OSError:
                # It may have landed, and sent again its precondition would
                # refuse it as a conflict: the next try asks first.
                handle.unanswered = _Sent(body, rev)
                raise
            handle.unanswered = None
            if status != 200:
                # A reported refusal is not retried at the next flush or
                # release, which would report it twice.
                with handle.lock:
                    if handle.rev == rev:
                        handle.dirty = provisional
                raise self._refused(handle.path, status, data)
            saved: SaveAnswer | RemovedAnswer = json.loads(data)
            if "removed" in saved:
                self._removed(handle, rev, call)
                return
            self._saved(
                handle, body, rev, saved["version"], saved["size"], bool(saved.get("as_sent"))
            )

    def _landed(self, handle: Handle) -> bool:
        """Whether the server holds exactly what the save with no answer
        sent, whose version the handle then takes as its base, not the bytes
        written since. Only a save stored byte for byte is recognized: one
        the route rewrote reads back otherwise and is reported not saved."""
        sent = handle.unanswered
        status, resp, data = self._remote.request(
            "GET", "read", {"path": handle.path.lstrip("/")}, call=self._call() or handle.call
        )
        if status != 200 or data != sent.body:
            return False
        self._saved(handle, sent.body, sent.rev, _version_read(resp), len(sent.body), True)
        return True

    def _saved(
        self, handle: Handle, body: bytes, rev: int, version: str, size: int, as_sent: bool
    ) -> None:
        with handle.lock:
            handle.base = version
            handle.unanswered = None
            if handle.rev == rev:
                handle.dirty = False
        self._views.saved(handle.path, handle.call, body, version, size, as_sent)

    def _removed(self, handle: Handle, rev: int, call: str | None) -> None:
        """A save that deleted its file leaves the views as an unlink does.
        The handle keeps its base, so a save through it again is refused
        as over a deleted file rather than making a new one."""
        with handle.lock:
            handle.unanswered = None
            if handle.rev == rev:
                handle.dirty = False
        self._views.removed(handle.path, self._views.view(call))

    def unlink(self, path):
        self._settle(path)
        call, view = self._here()
        status, _, data = self._remote.request(
            "POST", "delete", {"path": path.lstrip("/")}, call=call
        )
        if status != 204:
            raise self._refused(path, status, data, view)
        self._views.removed(path, view)
        return 0

    def rename(self, old, new):
        call, view = self._here()
        if self._views.is_local(old, view) and not self._on_server(old, view, call):
            if self._exists(new):
                raise fail(errno.EEXIST)
            self._views.moved_dir(old, new, view)
            return 0
        self._settle(old, written=True)
        self._settle(new)
        status, _, data = self._remote.request(
            "POST",
            "rename",
            {"path": old.lstrip("/"), "to": new.lstrip("/")},
            call=call,
        )
        if code_of(data) == Refusal.IS_DIRECTORY:
            # mv answers a cross-device rename by copying file by file.
            raise fail(errno.EXDEV)
        if status != 204:
            raise self._refused(old, status, data, view)
        self._views.renamed(old, new, view)
        # An open file follows its rename, as a temp file written, replaced
        # into place and then closed relies on. A move keeps the version.
        with self._lock:
            for handle in self._handles.values():
                if handle.path == old:
                    handle.path = new
        return 0

    # --- directories ----------------------------------------------------

    def _exists(self, path: str) -> bool:
        try:
            self.getattr(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise
        return True

    def mkdir(self, path, mode):
        if self._exists(path):
            raise fail(errno.EEXIST)
        parent, _ = split(path)
        if self.getattr(parent)["st_mode"] & stat.S_IFDIR == 0:
            raise fail(errno.ENOTDIR)
        self._require_creatable(parent)
        self._views.made_dir(path)
        return 0

    def rmdir(self, path):
        names = [n for n in self.readdir(path, 0) if n not in (".", "..")]
        if names:
            raise fail(errno.ENOTEMPTY)
        _, view = self._here()
        if self._views.is_local(path, view):
            self._views.dropped_dir(path, view)
            return 0
        raise fail(errno.EBUSY)

    # --- accepted and ignored -------------------------------------------

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        return 0

    def utimens(self, path, times=None):
        return 0

    def statfs(self, path):
        return {
            "f_bsize": 4096,
            "f_frsize": 4096,
            "f_blocks": 1 << 18,
            "f_bfree": 1 << 17,
            "f_bavail": 1 << 17,
            "f_files": 1 << 16,
            "f_ffree": 1 << 15,
            "f_favail": 1 << 15,
            "f_namemax": 255,
        }
