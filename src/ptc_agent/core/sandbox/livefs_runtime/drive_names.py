"""Where a mount path is: its space, its entry under the server's rule for
names, the file being written there, and the spellings the kernel holds for
it. ``Names`` is the one owner of all of these: every change to a manifest
or to which file is being written where goes through it.

Names follow the server's rule (``drive_tree.path_key``: NFC and full case
folding): a name the server takes for a stored one resolves to it. The
kernel caches each spelling it was answered for apart, so the spellings it
was given for an entry (aliases) and those it was told are absent (misses)
are remembered, and dropped with the stored name's when the feed changes
it.

A file being written (``drive_writes.Pending``) sits under its current key,
``/<space>/<rel>``, and only a rename changes that, while it holds the
file's ``saving``. So a save sees one path from its send to its answer.

Everything here is guarded by the operations' one lock (``mu``), held only
for bookkeeping, never across a request; ``drive_writes`` states the lock
order. Lookups of one path read a dict without it, as a lookup is one step.
"""

from __future__ import annotations

import errno
import threading
from collections import OrderedDict
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .drive_protocol import IGNORE_NAME
from .drive_tree import Entry, Space, path_key
from .remote import fail

if TYPE_CHECKING:
    from .drive_writes import Pending, Tell

#: Names the kernel was told were absent, by the server's key, at most: a
#: file made elsewhere under another spelling must drop those too.
MISSES_KEPT = 4096


class Names:
    def __init__(self, spaces: list[Space], mu: threading.Lock) -> None:
        self.spaces = {s.name: s for s in spaces}
        self.mu = mu
        self._aliases: dict[tuple[str, str], set[str]] = {}
        self._misses: OrderedDict[tuple[str, str], set[str]] = OrderedDict()
        #: The files being written, by key (and one just closed, for its errno).
        self._pending: dict[str, Pending] = {}

    # --- paths -------------------------------------------------------------

    def where(self, path: str) -> tuple[Space | None, str | None]:
        """(space, path in it); (None, None) for the mount root, and
        (None, '') for a name at the root that is no space."""
        if path == "/":
            return None, None
        name, _, rel = path[1:].partition("/")
        space = self.spaces.get(name)
        return (space, rel) if space is not None else (None, "")

    def resolve(self, path: str) -> tuple[Space, str]:
        space, rel = self.where(path)
        if space is None:
            raise fail(errno.ENOENT if rel is not None else errno.EACCES)
        if not rel.isascii():
            try:
                rel.encode("utf-8")
            except UnicodeEncodeError:
                # Bytes that are not UTF-8 (decoded to lone surrogates): no
                # Drive path can hold them.
                raise fail(errno.EINVAL) from None
        return space, rel

    def stored(self, path: str) -> str | None:
        """``path`` under the spelling the server stored, when it names a
        stored entry another way; None when it names none."""
        space, rel = self.where(path)
        if space is None or not rel:
            return None
        if rel in space.by_path:
            return path
        entry = space.get(rel)
        return f"/{space.name}/{entry.path}" if entry is not None else None

    @staticmethod
    def generated(space: Space, rel: str) -> bool:
        return rel == IGNORE_NAME and space.generates_ignore()

    def ignore(self, space: Space) -> bytes:
        with self.mu:
            return space.ignore_text()

    def listing(self, space: Space, rel: str) -> dict[str, bool] | None:
        """A folder's children, name to whether it is a folder; None when
        ``rel`` is no folder."""
        with self.mu:
            kids = space.listdir(rel)
            return None if kids is None else {name: e.is_dir for name, e in kids.items()}

    def has_children(self, space: Space, rel: str) -> bool:
        with self.mu:
            return bool(space.listdir(rel))

    # --- spellings the kernel holds ------------------------------------------

    def saw_alias(self, space: Space, entry: Entry, path: str) -> None:
        with self.mu:
            self._aliases.setdefault((space.name, entry.id), set()).add(path)

    def saw_miss(self, space: Space, rel: str, path: str) -> None:
        key = (space.name, path_key(rel))
        with self.mu:
            self._misses.setdefault(key, set()).add(path)
            self._misses.move_to_end(key)
            while len(self._misses) > MISSES_KEPT:
                self._misses.popitem(last=False)

    def spellings(
        self, space: Space, changes: list[tuple[Entry | None, Entry | None]]
    ) -> tuple[list[tuple[str, str]], list[str]]:
        """What else the kernel must forget for ``changes``: the other
        spellings it holds for an entry that changed, and those it holds as
        absent for one that came. Each is asked again, so it is let go."""
        names, attrs = [], []
        with self.mu:
            for old, new in changes:
                entry = old or new
                assert entry is not None
                for path in self._aliases.pop((space.name, entry.id), ()):
                    parent, _, name = path.rpartition("/")
                    names.append((parent or "/", name))
                    attrs.append(path)
                if new is not None:
                    for path in self._misses.pop((space.name, path_key(new.path)), ()):
                        parent, _, name = path.rpartition("/")
                        names.append((parent or "/", name))
        return names, attrs

    # --- files being written -------------------------------------------------

    @staticmethod
    def key(p: Pending) -> str:
        return f"/{p.space.name}/{p.rel}"

    def pending(self, path: str) -> Pending | None:
        """The open file being written at ``path``."""
        p = self._pending.get(path)
        return p if p is not None and p.open else None

    def pending_or_closed(self, path: str) -> Pending | None:
        """The file at ``path``, open or just closed (for its errno)."""
        return self._pending.get(path)

    def held(self, path: str) -> bool:
        return path in self._pending

    def spelled(self, space: Space, rel: str) -> str | None:
        """The path of a file being written, not on the server yet, that the
        server will take ``rel`` for (as ``space.get`` does for stored ones).
        Few files are being written at once, so a scan."""
        key = path_key(rel)
        with self.mu:
            for path, p in self._pending.items():
                if p.space is space and p.open and path_key(p.rel) == key:
                    return path
        return None

    def pending_under(self, path: str) -> list[Pending]:
        prefix = path + "/"
        with self.mu:
            return [p for k, p in self._pending.items() if k.startswith(prefix)]

    def pending_names_in(self, folder: str) -> list[str]:
        """Names of files being written directly in ``folder`` (a mount path)."""
        prefix = folder + "/"
        with self.mu:
            return [k[len(prefix):] for k in self._pending if k.startswith(prefix) and "/" not in k[len(prefix):]]

    def writing(self) -> list[Pending]:
        with self.mu:
            return list(self._pending.values())

    def records(self) -> set[str]:
        """The journal records the files being written hold."""
        with self.mu:
            return {p.record for p in self._pending.values()}

    def start_writing(self, path: str, p: Pending) -> Pending | None:
        """``p`` is being written at ``path``, unless another request opened
        a file there meanwhile: then that one, and ``p`` is not listed."""
        with self.mu:
            old = self.pending(path)
            if old is None:
                self._pending[path] = p
            return old

    def replace_writing(self, path: str, p: Pending) -> Pending | None:
        """``p`` is being written at ``path`` in place of whatever was (returned)."""
        with self.mu:
            old = self._pending.pop(path, None)
            self._pending[path] = p
        return old

    def resume_writing(self, p: Pending) -> bool:
        """List a file taken up from the journal, unless one is being written
        at its path already."""
        key = self.key(p)
        with self.mu:
            if self.pending(key) is not None:
                return False
            self._pending[key] = p
        return True

    def let_go(self, p: Pending, err: int = 0) -> None:
        """Out of the table and closed (``err``: refused), in one hold."""
        with self.mu, p.lock:
            key = self.key(p)
            if self._pending.get(key) is p:
                del self._pending[key]
            p.close(err)

    def let_go_if_clean(self, p: Pending) -> None:
        """Let go of a file opened for writing that never changed. Under its
        own lock: a write holding it has either landed or finds it closed."""
        with self.mu, p.lock:
            key = self.key(p)
            if p.clean and self._pending.get(key) is p:
                del self._pending[key]
                p.close()

    # --- what a change of ours was answered with --------------------------------

    def commit_saved(self, p: Pending, entry: Entry | None, rev: int, call: str | None,
                     sha: str | None = None) -> tuple[bool, list[Tell]]:
        """A save of ``p`` at ``rev`` by ``call`` answered with ``entry``: the
        manifest and base move first, so a store that cannot keep the bytes
        costs a later fetch, never a save sent over a stale head. No entry:
        saved as ``sha`` on a file removed since, which the feed brings.
        Returns whether the entry was current (not behind the feed), and
        what the agent is told."""
        with self.mu:
            current = entry is not None and p.space.newer(entry)
            if current:
                p.space.put(entry)
            key = self.key(p)
            with p.lock:
                tells = p.accepted(entry.sha256 if entry is not None else sha, rev, call)
                done = p.clean
                if done and self._pending.get(key) is p:
                    # Reads go back to the store, which holds these bytes (or
                    # the feed's newer ones). Closed in this same hold, so no
                    # write already holding ``p`` lands in it after.
                    del self._pending[key]
                    p.close()
        if not done:
            p.describe()
        return current, tells

    def commit_put(self, space: Space, entry: Entry) -> bool:
        """Apply an entry a change of ours answered with, unless the feed
        already brought a newer one (then False: the kernel must forget)."""
        with self.mu:
            current = space.newer(entry)
            if current:
                space.put(entry)
            return current

    def commit_drop(self, space: Space, entry_id: str, cursor: int) -> None:
        with self.mu:
            space.drop(entry_id, cursor)

    def commit_rename_unsaved(self, old: str, p: Pending, rel: str, base: str | None) -> bool:
        """A file never saved, renamed here to ``rel`` over ``base`` (what is
        stored there, or None); False when it is no longer at ``old``."""
        with self.mu:
            if not p.open or self._pending.get(old) is not p:
                return False
            del self._pending[old]
            with p.lock:
                p.moved(rel, base)
                p.changed_now(p.size)
            self._pending[self.key(p)] = p
        p.describe()
        return True

    def commit_move(self, space: Space, src: Entry, old: str, new: str, dst_rel: str,
                    moved: Entry, removed: list[str], cursor: int) -> bool:
        """After the server moved ``src``: the manifest, and every file being
        written that the move carried (a writer that still has it open
        follows it, as on a disk), in one hold. Returns whether the move's
        entry was current (not behind the feed)."""
        followed: list[Pending] = []
        with self.mu:
            for gone in removed:
                space.drop(gone, cursor)
            current = space.newer(moved)
            if src.is_dir:
                if current:
                    space.move_under(src.path, dst_rel)
                prefix = old + "/"
                for key in [k for k in self._pending if k.startswith(prefix)]:
                    x = self._pending.pop(key)
                    x.moved(dst_rel + key[len(old):], x.base)
                    self._pending[new + key[len(old):]] = x
                    followed.append(x)
            if current:
                space.put(moved)
            x = self._pending.pop(old, None)
            if x is not None:
                x.moved(dst_rel, moved.sha256)
                self._pending[new] = x
                followed.append(x)
        for x in followed:
            x.describe()
        return current

    # --- the change feed ------------------------------------------------------

    def apply(self, space: Space, wires: list[dict], cursor: int, usage: dict | None):
        """One page of the change feed; the changes it made."""
        with self.mu:
            changes = space.apply(wires)
            space.cursor = max(space.cursor, cursor)
            if usage is not None:
                space.usage = usage
            return changes

    def reload(self, space: Space, entries: list[Entry], manifest: dict):
        """A whole manifest in place of what the space held; the changes."""
        with self.mu:
            changes = space.diff_load(entries, int(manifest["cursor"]))
            space.limits = manifest.get("limits") or space.limits
            space.usage = manifest.get("usage") or space.usage
            return changes

    # --- snapshots ----------------------------------------------------------------

    def files(self, space: Space) -> list[Entry]:
        """Every file in ``space``, in path order."""
        with self.mu:
            return [space.by_path[p] for p in sorted(space.by_path) if not space.by_path[p].is_dir]

    def siblings(self, entry: Entry, parent: str, space: Space) -> list[Entry]:
        with self.mu:
            return [e for e in space.children.get(parent, {}).values() if e is not entry]

    def digests(self) -> tuple[set[str], set[str]]:
        """Every digest a live entry or a file being written names, and
        those of stored text: what the store keeps."""
        keep, text = set(), set()
        with self.mu:
            for space in self.spaces.values():
                for entry in space.by_path.values():
                    if entry.sha256:
                        keep.add(entry.sha256)
                        if entry.is_text:
                            text.add(entry.sha256)
            keep.update(p.base for p in self._pending.values() if p.base)
        return keep, text

    def counts(self) -> dict:
        """Each space's entries and cursor, and the files being written."""
        with self.mu:
            spaces = {s.name: {"entries": len(s.by_id), "cursor": s.cursor} for s in self.spaces.values()}
            return {"spaces": spaces, "pending": len(self._pending)}

    def all(self) -> Iterable[Space]:
        return list(self.spaces.values())
