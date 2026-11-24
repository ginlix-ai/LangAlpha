"""What the Drive daemon brings to the computer beside answering the
kernel: every text file at start (while the disk has room for it), the
server's change feed into the manifest, the cache and the kernel's caches,
and the kernel's caches warmed once the text is here.
What the file operations ask of these threads comes as ``Signals``
(``drive_effects``); ``drive_daemon`` runs each loop on its own thread.

Every thread here talks to the kernel only through the mount (a walk, a
read) or through libfuse's notify calls, and never holds the operations'
lock while doing either: an invalidation waits on the directory lock a
request being answered may hold.
"""

from __future__ import annotations

import contextlib
import os
import sys
import threading
import time

from .drive_client import DriveClient, Refused
from .drive_effects import Signals
from .drive_libfuse import Kernel
from .drive_ops import DriveFS
from .drive_protocol import FEED_WAIT_S, IGNORE_NAME, TEXTS_MAX_BYTES, TEXTS_MAX_IDS
from .drive_tree import Entry, Space, entry_of_row, plan

#: How often what the operations asked the kernel to forget is told it.
FORGET_EVERY_S = 0.1
HOLDS_EVERY_S = 1.0
FEED_BACKOFF_MAX_S = 30.0
FEED_SETTLE_S = 0.25
#: Text the warmer reads through the mount at most, so a Drive larger than
#: the computer's memory does not push out everything else cached.
WARM_DEFAULT_BYTES = 512 * 1024 * 1024

#: A space missing at least this many texts takes them in one stream.
STREAM_FROM = 200


class Sync:
    def __init__(
        self,
        drive: DriveFS,
        client: DriveClient,
        signals: Signals,
        mountpoint: str,
        stop: threading.Event,
        *,
        warm_bytes: int = WARM_DEFAULT_BYTES,
        warm: bool = True,
    ) -> None:
        self.names = drive.names
        self.reads = drive.reads
        self.writes = drive.writes
        self.cache = drive.reads.cache
        self.client = client
        self.signals = signals
        self.mount = mountpoint
        self.warm_bytes = warm_bytes
        self.warm_enabled = warm
        #: The running mount's invalidation calls, once it answers.
        self.kernel: Kernel | None = None
        self._stop = stop
        #: What each loop did, for the daemon's status.
        self.state: dict = {
            "phase": "starting",
            "text": {"files": 0, "bytes": 0, "total_files": 0, "total_bytes": 0, "seconds": None},
            "warm": {"files": 0, "bytes": 0, "seconds": None},
            "feed": {"batches": 0, "entries": 0, "resyncs": 0, "errors": 0, "last": None},
            "invalidations": {"names": 0, "attrs": 0},
        }

    # --- text ------------------------------------------------------------

    def _missing_texts(self) -> list[tuple[Space, Entry]]:
        return [
            (space, entry)
            for space in self.names.all()
            for entry in self.names.files(space)
            if entry.is_text and not self.cache.has(entry.sha256)
        ]

    def fetch_all_texts(self) -> None:
        """Every text file not on disk, in path order (a search's order), a
        page at a time. A page that fails is asked again after a pause; a
        read meanwhile fetches its own folder."""
        t0 = time.monotonic()
        missing = self._missing_texts()
        text = self.state["text"]
        streamed = self._stream_texts(missing)
        if streamed:
            text["streamed"] = streamed
            missing = self._missing_texts()
        text["total_files"] = len(missing)
        text["total_bytes"] = sum(e.size for _, e in missing)
        self.state["phase"] = "fetching text"
        i, n = 0, len(missing)
        while i < n and not self._stop.is_set() and not self.cache.missing_text:
            space = missing[i][0]
            page: list[Entry] = []
            size = 0
            while (
                i < n
                and missing[i][0] is space
                and len(page) < TEXTS_MAX_IDS
                and (not page or size + missing[i][1].size <= TEXTS_MAX_BYTES)
            ):
                entry = missing[i][1]
                i += 1
                # A read may have fetched it with its folder meanwhile.
                if not self.cache.has(entry.sha256):
                    page.append(entry)
                    size += entry.size
            pause = 1.0
            while page and not self._stop.is_set():
                try:
                    text["files"] += self.reads.fetch_texts(space, page)
                except Refused:
                    break
                except OSError:
                    self._stop.wait(pause)
                    pause = min(pause * 2, FEED_BACKOFF_MAX_S)
                    continue
                text["bytes"] += sum(e.size for e in page if self.cache.has(e.sha256))
                break
        text["seconds"] = round(time.monotonic() - t0, 3)

    def _stream_texts(self, missing: list[tuple[Space, Entry]]) -> int:
        """Take each space mostly missing its text in one streamed answer;
        how many files landed. Whatever it leaves out goes by page."""
        landed = 0
        for space in self.names.all():
            wanted = {e.sha256 for s, e in missing if s is space}
            if len(wanted) < STREAM_FROM or self._stop.is_set():
                continue
            try:
                for _, sha, data in self.client.all_texts(space.ref):
                    if self._stop.is_set() or self.cache.missing_text:
                        # The disk holds no more: the rest is read on demand.
                        break
                    if sha in wanted and not self.cache.has(sha) and self.cache.put(sha, data, text=True):
                        landed += 1
            except OSError as exc:
                print(f"drive: text stream for {space.name} failed: {exc}", file=sys.stderr, flush=True)
        return landed

    def texts_then_warm(self) -> None:
        self.fetch_all_texts()
        self.collect()
        if self.warm_enabled and not self._stop.is_set():
            self.state["phase"] = "warming"
            self.warm()
        self.state["phase"] = "ready"

    # --- warming ---------------------------------------------------------

    def warm(self) -> None:
        """Walk the mount, and read its text files, as a first search would:
        the kernel then holds every name, attribute and (up to a budget)
        page, and the first real search runs warm. Through the mount, so the
        loop serves it a request at a time between real ones."""
        t0 = time.monotonic()
        warm = self.state["warm"]
        budget = self.warm_bytes
        for space in self.names.all():
            ignored = {e.path for e in self.names.files(space) if not e.is_text}
            top = os.path.join(self.mount, space.name)
            stack = [(top, "")]
            while stack and not self._stop.is_set():
                folder, rel = stack.pop()
                try:
                    items = sorted(os.scandir(folder), key=lambda d: d.name)
                except OSError:
                    continue
                for item in items:
                    child = f"{rel}/{item.name}" if rel else item.name
                    try:
                        item.stat(follow_symlinks=False)  # its attributes, into the kernel's cache
                    except OSError:
                        continue
                    if item.is_dir(follow_symlinks=False):
                        stack.append((item.path, child))
                        continue
                    warm["files"] += 1
                    if child in ignored or child == IGNORE_NAME or budget <= 0:
                        continue
                    with contextlib.suppress(OSError):
                        fd = os.open(item.path, os.O_RDONLY | os.O_NOATIME)
                        try:
                            # No further than the budget: a large file is
                            # read only that far.
                            while budget > 0 and (got := len(os.read(fd, min(1 << 20, budget)))):
                                budget -= got
                                warm["bytes"] += got
                        finally:
                            os.close(fd)
        warm["seconds"] = round(time.monotonic() - t0, 3)

    # --- the change feed -------------------------------------------------

    def follow_feed(self) -> None:
        pause = 1.0
        while not self._stop.is_set():
            for space in self.names.all():
                try:
                    answer = self.client.changes(space.ref, space.cursor, FEED_WAIT_S)
                except OSError:
                    self.state["feed"]["errors"] += 1
                    self._stop.wait(pause)
                    pause = min(pause * 2, FEED_BACKOFF_MAX_S)
                    break
                pause = 1.0
                try:
                    self.apply(space, answer)
                except OSError:
                    self.state["feed"]["errors"] += 1
                    self._stop.wait(pause)
                if answer.get("entries"):
                    # A burst (this computer's own ``rm -r`` among them) wakes
                    # the feed once per change; asking again a moment later
                    # takes the rest in one answer and leaves the requests to
                    # the changes themselves. The first change is not delayed.
                    self._stop.wait(FEED_SETTLE_S)

    def apply(self, space: Space, answer: dict) -> None:
        """One ``changes`` answer: fetch the new text first, so a reader the
        invalidation sends back to the daemon finds it on disk, then update
        the manifest, then tell the kernel."""
        t0 = time.monotonic()
        feed = self.state["feed"]
        if answer.get("resync"):
            feed["resyncs"] += 1
            manifest = self.client.manifest(space.ref)
            entries = [entry_of_row(r) for r in manifest["entries"]]
            self._prefetch(space, [e for e in entries if e.is_text and not e.is_dir])
            changes = self.names.reload(space, entries, manifest)
        else:
            wires = answer.get("entries") or []
            if not wires:
                # No entry changed; the usage may have (a purge, a trim).
                self.names.apply(space, [], int(answer.get("cursor") or 0), answer.get("usage"))
                return
            fresh = [
                w for w in wires
                if not w.get("deleted") and w.get("kind") == "file" and w.get("is_text")
            ]
            self._prefetch_wires(space, fresh)
            changes = self.names.apply(space, wires, int(answer.get("cursor") or 0), answer.get("usage"))
        if not changes:
            return
        names, attrs = plan(space, changes)
        more_names, more_attrs = self.names.spellings(space, changes)
        self.invalidate(names + more_names, attrs + more_attrs)
        feed["batches"] += 1
        feed["entries"] += len(changes)
        feed["last"] = {
            "entries": len(changes),
            "apply_ms": round((time.monotonic() - t0) * 1000, 1),
            "at": time.time(),
        }
        self.collect()

    def _prefetch(self, space: Space, entries: list[Entry]) -> None:
        if self.cache.missing_text:
            return
        missing = [e for e in entries if not self.cache.has(e.sha256)]
        for at in range(0, len(missing), TEXTS_MAX_IDS):
            with contextlib.suppress(OSError):
                self.reads.fetch_texts(space, missing[at : at + TEXTS_MAX_IDS])

    def _prefetch_wires(self, space: Space, wires: list[dict]) -> None:
        if self.cache.missing_text:
            return
        wanted = [w for w in wires if not self.cache.has(w.get("sha256"))]
        for at in range(0, len(wanted), TEXTS_MAX_IDS):
            ids = [w["id"] for w in wanted[at : at + TEXTS_MAX_IDS]]
            with contextlib.suppress(OSError):
                for _, sha, data in self.client.texts(space.ref, ids):
                    self.cache.put(sha, data, text=True)

    def invalidate(self, names: list[tuple[str, str]], attrs: list[str]) -> None:
        if self.kernel is None:
            return
        counts = self.state["invalidations"]
        for parent, name in names:
            self.kernel.forget_name(parent, name)
            counts["names"] += 1
        for path in attrs:
            self.kernel.forget_attrs(path)
            counts["attrs"] += 1

    def collect(self) -> None:
        """Let the cache remove what no live entry, file being written or
        reader held on an older version names."""
        keep, text = self.names.digests()
        keep |= self.reads.holds.kept()
        self.cache.collect(keep, text)

    # --- what the operations asked to forget --------------------------------

    def forget_asked(self) -> None:
        """Tell the kernel what the operations asked it to forget, a beat at
        a time: they cannot from inside a request. Ends the holds whose
        readers are done here too, about once a second, since what ending
        one asks is that the kernel forget."""
        held_at = 0.0
        while not self._stop.wait(FORGET_EVERY_S):
            now = time.monotonic()
            if now - held_at >= HOLDS_EVERY_S:
                held_at = now
                self.reads.holds.tick()
            if not self.signals.forgets:
                continue
            names, attrs = [], set()
            for path in sorted(self.signals.take_forgets()):
                parent, _, name = path.rpartition("/")
                names.append((parent or "/", name))
                # The listing that holds it, which the kernel keeps too.
                attrs.update((path, parent or "/"))
            self.invalidate(names, sorted(attrs))


def load_spaces(client: DriveClient) -> list[Space]:
    """Every space the computer's user reaches, with its whole manifest."""
    spaces = []
    for found in client.spaces():
        space = Space(found["ref"], found.get("name") or found["ref"], found.get("id") or "")
        manifest = client.manifest(space.ref)
        space.load((entry_of_row(r) for r in manifest["entries"]), int(manifest["cursor"]))
        space.limits = manifest.get("limits") or {}
        space.usage = manifest.get("usage") or {}
        spaces.append(space)
    return spaces
