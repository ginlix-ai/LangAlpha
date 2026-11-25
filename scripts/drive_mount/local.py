"""The Drive daemon's operations in this process, against the fake server's
``Drive`` in this process too: for races a kernel mount cannot hit on cue
(a write landing between two steps of a save, a daemon replaced mid-write,
a save that commits after its answer was lost).

``LocalClient`` has ``DriveClient``'s methods over ``fake_server.Drive``,
with hooks run before an action applies (to stall it, fail it, or change the
server first). ``Daemon`` builds the operations as the daemon does and is
the one seam the checks use into the daemon's insides: everything a check
does that the kernel could not ask for goes through a method here, so the
daemon can be restructured under the checks by changing only this file.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable

import fake_server
from fake_server import STATUS, Drive, DriveError, Endpoint, wire

# The cache hints the page cache, which macOS has no call for.
if not hasattr(os, "posix_fadvise"):
    os.posix_fadvise = lambda *a: None  # type: ignore[attr-defined]
    os.POSIX_FADV_DONTNEED = 4  # type: ignore[attr-defined]

DEFAULT_RUNTIME = os.environ.get("DRIVE_SRC") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "ptc_agent", "core", "sandbox", "livefs_runtime"
)


class Runtime:
    """The package under test, copied and imported as ``livefs``, as the
    host ships it."""

    _loaded: Runtime | None = None

    def __init__(self, src: str) -> None:
        self.tmp = tempfile.mkdtemp(prefix="drive-local-")
        shutil.copytree(src, os.path.join(self.tmp, "livefs"), ignore=shutil.ignore_patterns("__pycache__"))
        sys.path.insert(0, self.tmp)
        from livefs import (
            drive_client,
            drive_effects,
            drive_journal,
            drive_notices,
            drive_ops,
            drive_cache,
            drive_saves,
            drive_sync,
            drive_tree,
            drive_writes,
        )

        self.client, self.journal, self.notices = drive_client, drive_journal, drive_notices
        self.ops, self.cache, self.sync, self.tree = drive_ops, drive_cache, drive_sync, drive_tree
        self.effects, self.saves, self.writes = drive_effects, drive_saves, drive_writes

    @classmethod
    def load(cls, src: str = DEFAULT_RUNTIME) -> Runtime:
        if cls._loaded is None:
            cls._loaded = cls(os.path.abspath(src))
        return cls._loaded


class LocalClient:
    """``DriveClient`` over a ``Drive`` in this process."""

    def __init__(self, drive: Drive, runtime: Runtime) -> None:
        self.drive = drive
        self.endpoint = Endpoint(drive)
        self.Refused = runtime.client.Refused
        self._hooks: dict[tuple[str, str], Callable] = {}
        self._mu = threading.Lock()
        #: Refusals the server answered, by code, in order.
        self.refusals: list[str] = []
        #: The most saves the server held at once.
        self.peak = 0
        self._active = 0

    # --- hooks -----------------------------------------------------------

    def on(self, action: str, path: str, fn: Callable) -> None:
        """Run ``fn`` before ``action`` on ``path`` applies (``fn(body)`` for a
        write, ``fn()`` otherwise); it may sleep, raise or change the server,
        and a dict it returns is the answer in place of the server's."""
        self._hooks[(action, path)] = fn

    def clear(self) -> None:
        self._hooks.clear()

    def refuse(self, code: str):
        """What a hook raises to have the server refuse."""
        self.refusals.append(code)
        raise self.Refused(STATUS.get(code, 400), json.dumps({"code": code, "message": code}).encode())

    def _run(self, action: str, path: str, fn: Callable, *hook_args):
        hook = self._hooks.get((action, path)) or self._hooks.get((action, "*"))
        if hook is not None:
            answer = hook(*hook_args)
            if isinstance(answer, dict):
                return answer
        try:
            return fn()
        except DriveError as exc:
            self.refuse(exc.code)

    # --- DriveClient -----------------------------------------------------

    def spaces(self) -> list[dict]:
        return [{"ref": "personal", "name": "personal", "id": self.drive.space_id}]

    def manifest(self, space: str) -> dict:
        return self.drive.manifest()

    def texts(self, space: str, ids: list[str]):
        return iter(self.endpoint.texts(ids))

    def all_texts(self, space: str):
        return iter(self.endpoint.texts(None))

    def content(self, space: str, sha: str, start: int | None = None, end: int | None = None) -> bytes:
        try:
            data = self.drive.bytes_of(sha)
        except DriveError as exc:
            self.refuse(exc.code)
        return data[start:end] if start is not None else data

    def source(self, space: str, sha: str) -> dict:
        try:
            _, size = self.drive.file_of(sha)
        except DriveError as exc:
            self.refuse(exc.code)
        return {"via": "api", "sha256": sha, "size": size}

    def content_into(self, space: str, sha: str, start: int, end: int, sink, buf) -> None:
        data = self.content(space, sha)
        for at in range(start, end, len(buf)):
            sink(memoryview(data[at : min(end, at + len(buf))]))

    # Saves in parts, through the server (``fake_server.Drive``'s uploads).

    def upload_begin(self, space, path, size, sha, base, call):
        return self._run("upload_begin", path, lambda: self.endpoint.upload_begin(
            path, size, sha, base, base is None, call))

    def upload_sign(self, space, upload_id, parts):
        raise self.Refused(409, json.dumps({"code": "upload_ended", "message": "relay only"}).encode())

    def upload_part(self, space, upload_id, number, body, md5):
        return self._run("upload_part", upload_id, lambda: self.drive.upload_part(upload_id, number, body), body)

    def upload_complete(self, space, upload_id):
        def complete():
            try:
                return self.drive.upload_complete(upload_id)
            except DriveError as exc:
                missing = getattr(exc, "missing", None)
                body = {"code": exc.code, "message": exc.message, **({"missing_parts": missing} if missing else {})}
                raise self.Refused(STATUS.get(exc.code, 400), json.dumps(body).encode()) from None
        return self._run("upload_complete", upload_id, complete)

    def upload_status(self, space, upload_id):
        with self.drive.lock:
            failing, self.drive.status_fails = self.drive.status_fails > 0, max(0, self.drive.status_fails - 1)
        if failing:
            raise self.Refused(502, b"<html>bad gateway</html>")
        return self._run("upload_status", upload_id, lambda: self.drive.upload_status(upload_id))

    def upload_cancel(self, space, upload_id):
        hook = self._hooks.get(("upload_cancel", upload_id)) or self._hooks.get(("upload_cancel", "*"))
        if hook is not None:
            hook()
        try:
            self.drive.upload_cancel(upload_id)
            return True
        except DriveError as exc:
            return exc.code == "not_found" or getattr(exc, "state", None) in ("failed", "aborted")

    def entry(self, space: str, path: str) -> dict | None:
        found = self.drive.get(path)
        return wire(found) if found else None

    def changes(self, space: str, since: int, wait: int = 0) -> dict:
        return self.drive.changes(since, 0)

    def write(self, space, path, body, base, call):
        with self._mu:
            self._active += 1
            self.peak = max(self.peak, self._active)
        try:
            return self._run("write", path, lambda: self.endpoint.write(path, body, base, base is None, call), body)
        finally:
            with self._mu:
                self._active -= 1

    def notices(self, notes: list[dict]) -> dict:
        self.drive.notices.extend(notes)
        return {"filed": True}

    def mkdir(self, space, path, call):
        return self._run("mkdir", path, lambda: self.endpoint.mkdir(path, call))

    def move(self, space, src, dst, replace, call):
        return self._run("move", src, lambda: self.endpoint.move(src, dst, replace, call))

    def trash(self, space, path, kind, call, if_match=None):
        return self._run("trash", path, lambda: self.endpoint.trash(path, kind, kind == "folder", call, if_match))

    # --- the server's side -------------------------------------------------

    def put(self, path: str, data: bytes) -> None:
        """A save made elsewhere (the app, another computer)."""
        found = self.drive.get(path)
        self.drive.write(path, data, found["sha256"] if found else None, found is None)

    def bytes(self, path: str) -> bytes | None:
        found = self.drive.get(path)
        return None if found is None or found["kind"] == "folder" else self.drive.bytes_of(found["sha256"])

    def has(self, path: str) -> bool:
        return self.drive.get(path) is not None


def server(runtime: Runtime) -> LocalClient:
    return LocalClient(Drive(tempfile.mkdtemp(dir=runtime.tmp)), runtime)


class Daemon:
    """One daemon's operations over ``client``, built as ``serve`` builds
    them, with the few ways into its insides the checks need."""

    def __init__(self, runtime: Runtime, client: LocalClient, root: str | None = None,
                 call: list[str] | None = None) -> None:
        self.rt = runtime
        self.client = client
        self.root = root or tempfile.mkdtemp(dir=runtime.tmp)
        os.makedirs(self.root, exist_ok=True)
        self.call = call or ["call-one"]
        self.store = runtime.cache.Cache(os.path.join(self.root, "store"), 1 << 30, 0)
        self.store.scan()
        self.journal = runtime.journal.Journal(os.path.join(self.root, "journal"))
        self.notices = runtime.notices.Notices(os.path.join(self.root, "notices.jsonl"))
        #: Paths the kernel was told to forget.
        self.forgot: list[str] = []
        forgot = self.forgot

        class Recorded(runtime.effects.Effects):
            def forget(self, path: str) -> None:
                forgot.append(path)

        spaces = runtime.sync.load_spaces(client)
        self.space = spaces[0]
        self.fs = runtime.ops.DriveFS(spaces, self.store, client, 0, 0, journal=self.journal,
                                      notices=self.notices, caller=os.getpid,
                                      call_of=lambda pid: self.call[0], effects=Recorded())
        self.writes = self.fs.writes
        self.names = self.fs.names

    # --- files being written ------------------------------------------------

    def _p(self, path: str):
        return self.names.pending_or_closed(path)

    def pending(self, path: str) -> bool:
        return self.names.pending(path) is not None

    def pending_paths(self) -> list[str]:
        return [self.names.key(p) for p in self.names.writing()]

    def pending_bytes(self, path: str) -> bytes | None:
        p = self.names.pending(path)
        return None if p is None else self.writes.pread(p, 1 << 20, 0)

    def dirty(self, path: str) -> bool:
        return self._p(path).unsaved

    def age(self, path: str | None = None, seconds: float = 60.0, *, changed: bool = True,
            first: bool = True) -> None:
        """As if ``seconds`` passed since the file (every file) last changed
        and since its first change not yet saved."""
        for p in self.names.writing():
            if path is None or self.names.key(p) == path:
                if changed:
                    p.changed -= seconds
                if first:
                    p.first -= seconds

    def since_change(self, path: str, seconds: float) -> None:
        """Its last change ``seconds`` ago, exactly."""
        self._p(path).changed = time.monotonic() - seconds

    # --- saves --------------------------------------------------------------

    def save(self, path: str) -> None:
        self.fs.saves.save(self._p(path), None)

    def save_in_background(self, path: str) -> threading.Thread:
        """A save no program waits on, as the saver's pool runs it."""
        p = self._p(path)

        def run() -> None:
            with contextlib.suppress(OSError):
                self.fs.saves.save(p, None)

        t = threading.Thread(target=run)
        t.start()
        return t

    def save_all(self) -> None:
        self.fs.saves.save_all()

    def due(self) -> bool:
        now = time.monotonic()
        return any(self.rt.saves.ripe(p, now) for p in self.names.writing())

    def tick(self) -> None:
        """One turn of the saver thread."""
        self.fs.saves.tick(time.monotonic())

    def settle(self, call: str, wait_s: float) -> int:
        return self.fs.saves.settle(call, wait_s)

    def set_call(self, call: str) -> None:
        self.call[0] = call
        self.fs.calls.forget()

    # --- daemons coming and going ---------------------------------------------

    def restore(self) -> int:
        return self.writes.restore(self.journal.claim())

    def adopt(self) -> int:
        return self.fs.saves.adopt()

    def die(self) -> None:
        """Gone at once, as a killed daemon: its files left as they are."""
        for p in self.names.writing():
            with contextlib.suppress(OSError):
                os.close(p.fd)

    # --- what a check races against -------------------------------------------

    @contextlib.contextmanager
    def write_paused_after_lookup(self, path: str, seconds: float = 1.0):
        """The next write to ``path`` pauses ``seconds`` once it has found its
        file; yields an event set when it has."""
        found = threading.Event()
        lookup = self.fs._writable
        target = self._p(path)

        def paused(path_, load):
            q = lookup(path_, load=load)
            if q is target:
                found.set()
                time.sleep(seconds)
            return q

        self.fs._writable = paused
        try:
            yield found
        finally:
            del self.fs._writable

    def while_writing(self, path: str, during: Callable[[], None], data: bytes) -> None:
        """A write that holds the file while ``during`` runs, then lands ``data``."""
        p = self._p(path)
        with p.lock:
            during()
            os.pwrite(p.fd, data, 0)
            p.changed_now(p.size)

    def deliver(self, path: str) -> None:
        """The feed brings the server's current entry for ``path``."""
        self.put_entry(self.rt.tree.entry_of(self.client.entry("personal", path)))

    def entry(self, *fields):
        """A manifest entry: id, path, kind, size, sha256, is_text, modified_ms, seq."""
        return self.rt.tree.Entry(*fields)

    def put_entry(self, entry) -> None:
        with self.fs.names.mu:
            self.space.put(entry)

    def churn_pending(self, key: str) -> None:
        """A file made and let go in the table, as the saver does."""
        p = self.rt.writes.Pending(self.space, key.split("/", 2)[2], -1, 0, None, None, self.journal, "churn")
        p.phase = self.rt.writes.Phase.CLOSED
        with self.names.mu:
            self.names._pending[key] = p
        with self.names.mu:
            self.names._pending.pop(key, None)

    def sync(self):
        return self.rt.sync.Sync(self.fs, self.client, self.rt.effects.Signals(), "/nonexistent", threading.Event())


__all__ = ["Daemon", "LocalClient", "Runtime", "fake_server", "server"]
