"""The Drive as the daemon holds it in memory: every entry of every space,
and what a change to them means for the kernel's caches.

Pure bookkeeping, no I/O: the operations and the feed hold one lock around
every call here.
"""

from __future__ import annotations

import re
import unicodedata
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass

from .drive_protocol import IGNORE_NAME, ROW_FIELDS, WireEntry


@dataclass(slots=True)
class Entry:
    id: str
    #: Relative to the space root.
    path: str
    kind: str
    size: int
    sha256: str | None
    is_text: bool
    mtime_ns: int
    seq: int

    @property
    def is_dir(self) -> bool:
        return self.kind == "folder"

    def same(self, other: Entry) -> bool:
        """Whether a reader could tell the two apart."""
        return (
            self.path == other.path
            and self.kind == other.kind
            and self.size == other.size
            and self.sha256 == other.sha256
            and self.mtime_ns == other.mtime_ns
        )


_SHA256 = re.compile(r"[0-9a-f]{64}")


def sha256_or_none(value: object) -> str | None:
    """A digest from the server, kept only when it is one: it names files on
    this computer's disk (``drive_cache``), so anything else must never get
    that far. A file whose digest is not one reads as an error."""
    return value if isinstance(value, str) and _SHA256.fullmatch(value) else None


def entry_of(wire: WireEntry) -> Entry:
    return Entry(
        id=wire["id"],
        path=wire["path"],
        kind=wire["kind"],
        size=int(wire.get("size") or 0),
        sha256=sha256_or_none(wire.get("sha256")),
        is_text=bool(wire.get("is_text")),
        mtime_ns=int(wire.get("modified_ms") or 0) * 1_000_000,
        seq=int(wire.get("seq") or 0),
    )


def entry_of_row(row: list) -> Entry:
    return entry_of(dict(zip(ROW_FIELDS, row, strict=False)))  # type: ignore[arg-type]


def _fold(segment: str) -> str:
    return unicodedata.normalize("NFC", unicodedata.normalize("NFC", segment).casefold())


def path_key(path: str) -> str:
    """The server's key for a path (``src/server/database/drive/keys.py``,
    mirrored here since the daemon is stdlib only and pinned to it by a
    test): NFC and full case folding a segment at a time. Two names with one
    key are one file on the server."""
    return "/".join(_fold(s) for s in path.split("/")) if path else ""


def nfc(path: str) -> str:
    """A path as the server stores it."""
    return unicodedata.normalize("NFC", path)


#: Ids of entries gone from a space remembered at most, with the seq they
#: went at, so a late answer cannot bring one back.
DROPPED_KEPT = 10000


def split(rel: str) -> tuple[str, str]:
    parent, _, name = rel.rpartition("/")
    return parent, name


def _escape(path: str) -> str:
    """A path as a literal gitignore pattern."""
    out = "".join("\\" + c if c in "\\*?[" else c for c in path)
    if out.endswith(" "):
        out = out[:-1] + "\\ "
    return out


class Space:
    """One space: its entries by id and by path, and each folder's children."""

    def __init__(self, ref: str, name: str, space_id: str) -> None:
        self.ref = ref
        #: The directory it appears as at the mount root.
        self.name = name
        self.space_id = space_id
        self.cursor = 0
        self.limits: dict = {}
        self.usage: dict = {}
        self.by_id: dict[str, Entry] = {}
        self.by_path: dict[str, Entry] = {}
        #: ``path_key`` to entry: the server's idea of which name is which file.
        self.by_key: dict[str, Entry] = {}
        #: Entries that left, by id, with the seq they left at.
        self.dropped: OrderedDict[str, int] = OrderedDict()
        #: Folder path ('' for the space root) to its children by name.
        self.children: dict[str, dict[str, Entry]] = {"": {}}
        self._ignore: bytes | None = None

    # --- loading ---------------------------------------------------------

    def load(self, entries: Iterable[Entry], cursor: int) -> None:
        self.by_id.clear()
        self.by_path.clear()
        self.by_key.clear()
        self.children = {"": {}}
        # Folders first, so every file's parent exists however the rows came.
        for entry in sorted(entries, key=lambda e: (not e.is_dir, e.path.count("/"))):
            self._place(entry)
        self.cursor = cursor
        self._ignore = None

    def _place(self, entry: Entry) -> None:
        self.by_id[entry.id] = entry
        self.by_path[entry.path] = entry
        self.by_key[path_key(entry.path)] = entry
        parent, name = split(entry.path)
        self.children.setdefault(parent, {})[name] = entry
        if entry.is_dir:
            self.children.setdefault(entry.path, {})

    def _unplace(self, entry: Entry) -> None:
        """Take ``entry`` off its path, unless another entry has since taken it."""
        if self.by_path.get(entry.path) is entry:
            del self.by_path[entry.path]
        key = path_key(entry.path)
        if self.by_key.get(key) is entry:
            del self.by_key[key]
        parent, name = split(entry.path)
        kids = self.children.get(parent)
        if kids is not None and kids.get(name) is entry:
            del kids[name]
        if entry.is_dir and not self.children.get(entry.path):
            self.children.pop(entry.path, None)

    # --- reads -----------------------------------------------------------

    def get(self, rel: str) -> Entry | None:
        """The entry ``rel`` names, by its stored name or any name the server
        takes for the same (another normalization or case)."""
        found = self.by_path.get(rel)
        if found is None and rel:
            found = self.by_key.get(path_key(rel))
        return found

    def listdir(self, rel: str) -> dict[str, Entry] | None:
        if not rel:
            return self.children.get("", {})
        found = self.get(rel)
        if found is None or not found.is_dir:
            return None
        return self.children.get(found.path, {})

    def generates_ignore(self) -> bool:
        """The space's own file of that name, if it has one, wins."""
        return IGNORE_NAME not in self.by_path

    def ignore_text(self) -> bytes:
        """What ``rg`` should skip: an extension when every file carrying it
        is binary and no folder does (``rg`` applies a glob to folders too),
        else the binary files by path."""
        if self._ignore is not None:
            return self._ignore
        binary: dict[str, list[str]] = {}
        texty: set[str] = set()
        for entry in self.by_path.values():
            name = split(entry.path)[1]
            ext = name.rpartition(".")[2] if "." in name.lstrip(".") else ""
            if entry.is_dir or entry.is_text:
                texty.add(ext)
            else:
                binary.setdefault(ext, []).append(entry.path)
        lines = [
            "# Written by the Drive mount: the binary files here, which a search",
            "# would read in full and never match. rg --no-ignore searches them too.",
        ]
        for ext in sorted(binary):
            if ext and ext not in texty:
                lines.append(f"*.{_escape(ext)}")
            else:
                lines.extend("/" + _escape(p) for p in sorted(binary[ext]))
        self._ignore = ("\n".join(lines) + "\n").encode()
        return self._ignore

    # --- changes ---------------------------------------------------------

    def newer(self, entry: Entry) -> bool:
        """Whether ``entry`` (a change's own answer) is news: not older than
        what the feed already put here, nor an entry the feed saw go since."""
        known = self.by_id.get(entry.id)
        if known is not None:
            return not (entry.seq and known.seq > entry.seq)
        gone = self.dropped.get(entry.id)
        return gone is None or (entry.seq > gone)

    def put(self, entry: Entry) -> Entry | None:
        """Record ``entry`` (new, changed or moved); the entry it replaces."""
        old = self.by_id.get(entry.id)
        if old is not None:
            self._unplace(old)
        clash = self.by_key.get(path_key(entry.path))
        if clash is not None and clash.id != entry.id:
            # The path's previous holder left in a change not seen yet.
            self._unplace(clash)
            del self.by_id[clash.id]
        self._place(entry)
        if old is None or old.is_text != entry.is_text or old.path != entry.path:
            self._ignore = None
        return old

    def drop(self, entry_id: str, seq: int = 0) -> Entry | None:
        held = self.by_id.get(entry_id)
        if held is not None and seq and held.seq > seq:
            return None  # a version newer than the delete already came
        old = self.by_id.pop(entry_id, None)
        self.dropped[entry_id] = max(seq, old.seq if old is not None else 0, self.dropped.get(entry_id, 0))
        self.dropped.move_to_end(entry_id)
        while len(self.dropped) > DROPPED_KEPT:
            self.dropped.popitem(last=False)
        if old is not None:
            self._unplace(old)
            if not old.is_text:
                self._ignore = None
        return old

    def move_under(self, old_dir: str, new_dir: str) -> list[tuple[Entry, Entry]]:
        """Rewrite the paths of everything under a folder the daemon moved
        itself; the feed brings the same moves later with their seqs."""
        prefix = old_dir + "/"
        moved = []
        for entry in [e for p, e in self.by_path.items() if p.startswith(prefix)]:
            new = Entry(
                entry.id, new_dir + entry.path[len(old_dir):], entry.kind, entry.size,
                entry.sha256, entry.is_text, entry.mtime_ns, entry.seq,
            )
            moved.append((entry, new))
        for old, _ in moved:
            self._unplace(old)
        for _, new in moved:
            self._place(new)
        self._ignore = None
        return moved

    def apply(self, wires: Iterable[WireEntry]) -> list[tuple[Entry | None, Entry | None]]:
        """Apply a ``changes`` batch; each change that moved anything, as
        (before, after). A change at or below the seq already held is the
        daemon's own save coming back, or older news."""
        changed: list[tuple[Entry | None, Entry | None]] = []
        for wire in wires:
            known = self.by_id.get(wire["id"])
            seq = int(wire.get("seq") or 0)
            if known is not None and seq and seq <= known.seq:
                continue
            if known is None and seq and seq <= self.dropped.get(wire["id"], 0):
                continue  # removed here since, in a change this answer predates
            if wire.get("deleted"):
                if known is not None:
                    changed.append((self.drop(known.id, seq), None))
                else:
                    self.drop(wire["id"], seq)
                continue
            new = entry_of(wire)
            old = self.put(new)
            if old is None or not old.same(new):
                changed.append((old, new))
        return changed

    def diff_load(self, entries: list[Entry], cursor: int) -> list[tuple[Entry | None, Entry | None]]:
        """Replace everything (a resync), returning what differs."""
        before = dict(self.by_id)
        self.load(entries, cursor)
        changed: list[tuple[Entry | None, Entry | None]] = []
        for entry in entries:
            old = before.pop(entry.id, None)
            if old is None or not old.same(entry):
                changed.append((old, entry))
        changed.extend((old, None) for old in before.values())
        return changed


def plan(space: Space, changes: list[tuple[Entry | None, Entry | None]]) -> tuple[list[tuple[str, str]], list[str]]:
    """What the kernel must forget for ``changes``, by mount path.

    Returns names to drop from a directory (``(parent, name)``: a cached entry
    for a name that moved or went, or a cached absence for one that came) and
    paths whose attributes and data to drop (changed files, and every
    directory whose listing changed). Names are ordered shallowest first, and
    one under a name already dropped is left out: dropping a directory's
    entry drops everything cached below it.
    """
    root = "/" + space.name
    names: set[tuple[str, str]] = set()
    inodes: set[str] = set()
    for old, new in changes:
        if old is not None and (new is None or old.path != new.path):
            parent, name = split(old.path)
            names.add((parent, name))
            inodes.add(parent)
        if new is not None and (old is None or old.path != new.path):
            parent, name = split(new.path)
            names.add((parent, name))
            inodes.add(parent)
        if old is not None and new is not None and old.path == new.path:
            inodes.add(new.path)
        if (old is not None and not old.is_text) or (new is not None and not new.is_text):
            if space.generates_ignore():
                inodes.add(IGNORE_NAME)

    def mount(rel: str) -> str:
        return f"{root}/{rel}" if rel else root

    ordered: list[tuple[str, str]] = []
    dropped: list[str] = []
    for parent, name in sorted(names, key=lambda pn: (pn[0].count("/") if pn[0] else -1, pn)):
        full = f"{parent}/{name}" if parent else name
        if any(full == d or full.startswith(d + "/") for d in dropped):
            continue
        dropped.append(full)
        ordered.append((mount(parent), name))
    return ordered, sorted(mount(p) for p in inodes)
