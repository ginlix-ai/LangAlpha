"""What the daemon knows of the files, whoever asks.

A command sees the files as they were when it first touched them, plus its
own and its siblings' saves through this mount: its view is keyed by the call
id the tool put in the command's environment, so the next command starts
fresh without the daemon polling anything.

Two things outlive a command without showing it anything stale. The
directories the server makes up (the root, ``workspaces``, each workspace,
``user``) change only when the host runs ``link`` again, which rewrites the
daemon's state file. File bytes are kept by version, and a command still takes
each file's version from a listing of its own.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import NamedTuple

from .contents import Contents
from .protocol import WireListing

VIEW_LIMIT = 32
UNTAGGED_TTL_S = 2.0
#: How long a made-up directory's listing answers without being asked again:
#: a backstop for a layout change no ``link`` followed.
STRUCTURE_TTL_S = 300.0
#: The file bytes kept, at most; less where the sandbox has little memory.
CONTENT_BYTES = 64 * 1024 * 1024


def split(path: str) -> tuple[str, str]:
    parent, _, name = path.rpartition("/")
    return parent or "/", name


def _above(path: str) -> list[str]:
    """The directories above ``path``, nearest first, "/" last."""
    found = []
    while path != "/":
        path = split(path)[0]
        found.append(path)
    return found


class Entry(NamedTuple):
    """One name in a directory listing."""

    is_dir: bool
    size: int = 0
    version: str | None = None
    writable: bool = True


DIR = Entry(is_dir=True)


@dataclass(slots=True)
class Listing:
    """A directory's entries, and whether new files can be made in it.

    A made-up directory's listing is every command's, so it also carries
    when it is asked again and the names a command's own listing lacked.
    """

    entries: dict[str, Entry]
    creatable: bool
    until: float = 0.0
    absent: set[str] = field(default_factory=set)


class View:
    """What one command has seen: directory listings, and the version of each
    file it read or saved. The bytes are the daemon's (``Contents``), so a
    command that reads much pins none of it."""

    __slots__ = ("listings", "versions", "kept", "born")

    def __init__(self) -> None:
        self.listings: dict[str, Listing] = {}
        self.versions: dict[str, str] = {}
        #: Directories above a file this command removed. The server drops a
        #: directory with its last file, and `rm -r` then rmdirs it.
        self.kept: set[str] = set()
        self.born = time.monotonic()


def _exact(version: str | None, data: bytes) -> bool:
    """Whether ``version`` names these bytes and no others. Hashed outside
    the lock: a large file takes milliseconds."""
    return bool(version) and len(version) >= 16 and (
        hashlib.sha256(data).hexdigest().startswith(version)
    )


class _Content(NamedTuple):
    path: str
    version: str | None
    data: bytes
    exact: bool


class _Decoded(NamedTuple):
    """One directory of a ``/list`` answer, and the file bytes it carried."""

    listing: Listing
    contents: list[_Content]


def _decode(path: str, wire: WireListing) -> _Decoded:
    base = path.rstrip("/")
    entries: dict[str, Entry] = {}
    contents: list[_Content] = []
    for item in wire["entries"]:
        name, content, version = item["name"], item.get("content"), item.get("version")
        if isinstance(content, str) and len(data := content.encode()) == item.get("size"):
            contents.append(_Content(f"{base}/{name}", version, data, _exact(version, data)))
        if item.get("type") == "dir":
            entries[name] = DIR
        else:
            entries[name] = Entry(False, item.get("size", 0), version, item.get("writable", True))
    return _Decoded(Listing(entries, bool(wire.get("writable"))), contents)


def _placed(view: View, path: str, entry: Entry | None) -> None:
    """Show a file the server now holds in ``view``'s listing of its
    directory (asked again when its ``entry`` is not known), and each
    directory above it in the listing above that, which a listing taken
    before the directory was made lacks."""
    parent, name = split(path)
    listing = view.listings.get(parent)
    if listing is not None:
        if entry is None:
            del view.listings[parent]
        else:
            listing.entries[name] = entry
    child = parent
    while child != "/":
        directory, dirname = split(child)
        listing = view.listings.get(directory)
        if listing is not None and dirname not in listing.entries:
            listing.entries[dirname] = DIR
        child = directory


class Views:
    """Every command's view, and what the daemon keeps across them.

    ``layout`` is the state file ``link`` rewrites whenever the computer's
    workspaces or links may have changed. Each operation asks the server
    first and then records what it learned with one change here, so ``lock``
    is never held across a request.
    """

    def __init__(self, content_bytes: int = CONTENT_BYTES, layout: str | None = None) -> None:
        self.lock = threading.RLock()
        self._views: OrderedDict[str | None, View] = OrderedDict()
        #: Directories made here and not yet holding a saved file. Daemon-wide,
        #: since an empty directory has to outlast the command that made it.
        self._local_dirs: set[str] = set()
        self._made: dict[str, Listing] = {}
        self._bytes = Contents(content_bytes)
        self._layout = layout
        self._layout_seen: object = None
        self._sources: list[str] | None = None

    # --- reads ----------------------------------------------------------

    def view(self, call: str | None) -> View:
        with self.lock:
            view = self._views.get(call)
            stale = (
                view is not None
                and call is None
                and time.monotonic() - view.born > UNTAGGED_TTL_S
            )
            if view is None or stale:
                self._check_layout()
                if stale:
                    self._bytes.forget(view)
                view = View()
                self._views[call] = view
                while len(self._views) > VIEW_LIMIT:
                    self._bytes.forget(self._views.popitem(last=False)[1])
            else:
                self._views.move_to_end(call)
            return view

    def _check_layout(self) -> None:
        """Forget the made-up directories once ``link`` has linked a different
        set of mount paths, which it does whenever the computer's workspaces
        or layout change. Checked as each command starts, so no command's
        walk spans two layouts; a rewrite that changed no link (a new token)
        keeps them."""
        if self._layout is None:
            return
        try:
            info = os.stat(self._layout)
            seen: object = (info.st_ino, info.st_mtime_ns, info.st_size)
        except OSError:
            seen = None
        if seen == self._layout_seen:
            return
        self._layout_seen = seen
        try:
            with open(self._layout) as f:
                sources = json.load(f).get("sources")
        except (OSError, ValueError, AttributeError):
            sources = None
        if sources is not None and sources == self._sources:
            return
        self._sources = sources
        self._made.clear()

    def listing(
        self, path: str, view: View, name: str | None = None
    ) -> dict[str, Entry] | None:
        """A directory's entries as this command sees them, if known.

        A made-up directory's listing, taken by any command, answers for a
        ``name`` it holds; a name it lacks is asked again, so a workspace
        added since is never hidden, and the fresh answer is this command's.
        A name still missing then answers as missing until the listing goes:
        a search looks for the same ignore files in every directory above
        the one it searches, which would list them all again each time.
        """
        with self.lock:
            held = view.listings.get(path)
            if held is not None:
                self._note_absent(path, name, held.entries)
                return held.entries
            made = self._made.get(path)
            if made is not None:
                if made.until <= time.monotonic():
                    del self._made[path]
                elif name is None or name in made.entries or name in made.absent:
                    return made.entries
        return None

    def _note_absent(self, path: str, name: str | None, entries: dict[str, Entry]) -> None:
        """Remember a name this command's own listing of a made-up
        directory lacks. Caller holds the lock."""
        made = self._made.get(path)
        if made is not None and name is not None and name not in entries:
            made.absent.add(name)

    def creatable(self, path: str, view: View) -> bool:
        with self.lock:
            held = view.listings.get(path) or self._made.get(path)
            return held.creatable if held is not None else True

    def is_local(self, path: str, view: View | None) -> bool:
        return path in self._local_dirs or (view is not None and path in view.kept)

    def local_names(self, path: str, view: View) -> set[str]:
        """The directories kept here directly inside ``path``."""
        with self.lock:
            return {
                split(d)[1] for d in self._local_dirs | view.kept if split(d)[0] == path
            }

    def cached(
        self, path: str, version: str | None, view: View
    ) -> tuple[str | None, bytes | None]:
        """The version this command read or saved of the file, and the bytes
        kept of that version, else of ``version``, the one its listing names.
        Bytes found become this command's version."""
        with self.lock:
            pinned = view.versions.get(path)
            wanted = pinned or version
            data = self._bytes.get(path, wanted, view)
            if data is not None:
                view.versions[path] = wanted
            return pinned, data

    # --- changes --------------------------------------------------------

    def listed(
        self, path: str, answer: WireListing, view: View, name: str | None = None
    ) -> dict[str, Entry]:
        """Hold a directory's listing, and those below it the answer
        carried, as this command's."""
        here = _decode(path, answer)
        base = path.rstrip("/")
        below = {
            f"{base}/{relative}": _decode(f"{base}/{relative}", listed)
            for relative, listed in (answer.get("below") or {}).items()
        }
        entries = here.listing.entries
        with self.lock:
            self._keep(path, here, view)
            for directory, decoded in below.items():
                # One this command listed already stands: it may have changed
                # files there since.
                if directory not in view.listings:
                    self._keep(directory, decoded, view)
            # One holding a file is this command's alone: a file's version
            # moves with no ``link``.
            if answer.get("structural") and all(e.is_dir for e in entries.values()):
                before = self._made.get(path)
                self._made[path] = Listing(
                    dict(entries),
                    here.listing.creatable,
                    time.monotonic() + STRUCTURE_TTL_S,
                    before.absent - entries.keys() if before else set(),
                )
            self._note_absent(path, name, entries)
        return entries

    def _keep(self, path: str, decoded: _Decoded, view: View) -> None:
        """Caller holds the lock."""
        owner = self._owner(view)
        for content in decoded.contents:
            self._bytes.put(content.path, content.version, content.data, owner, content.exact)
        view.listings[path] = decoded.listing

    def _owner(self, view: View) -> View | None:
        """``view``, unless it was let go while its request was out: what
        is kept for it then would be kept for no command."""
        return view if view in self._views.values() else None

    def read(self, path: str, version: str, data: bytes, view: View) -> None:
        """Keep the bytes the server read back, as the version this command
        now sees."""
        exact = _exact(version, data)
        with self.lock:
            self._bytes.put(path, version, data, self._owner(view), exact)
            if version:
                view.versions[path] = version

    def saved(
        self, path: str, call: str | None, body: bytes, version: str, size: int, as_sent: bool
    ) -> None:
        entry = Entry(False, size, version, True)
        # The server holds the directories above a saved file, and drops
        # them again once the last file inside goes.
        above = set(_above(path)) - {"/"}
        exact = as_sent and _exact(version, body)
        with self.lock:
            self._local_dirs -= above
            if as_sent:
                # For any command when the version names these bytes, else
                # for the one that saved them.
                owner = None if exact else self._views.get(call)
                self._bytes.put(path, version, body, owner, exact)
            else:
                # Stored otherwise than sent, under a version that may be
                # one already kept with other bytes.
                self._bytes.drop(path)
            for view in self._views.values():
                view.kept -= above
                _placed(view, path, entry)
                if as_sent:
                    view.versions[path] = version
                else:
                    view.versions.pop(path, None)

    def removed(self, path: str, seen: View) -> None:
        """Take a file the server no longer holds out of every view's
        listing. When its directory held nothing else, as ``seen`` (the
        remover's view) listed it, the server dropped that too, so each view
        asks again for the listings from there up. The made-up directories
        stay: no file is ever directly inside one.

        The remover keeps the directories above it, which must not vanish
        under a caller about to rmdir them, as `rm -r` is."""
        parent, name = split(path)
        with self.lock:
            seen.kept.update(set(_above(path)) - {"/"})
            # A file made there again may be given a version seen before.
            self._bytes.drop(path)
            listing = seen.listings.get(parent)
            emptied = listing is None or listing.entries.keys() <= {name}
            for view in self._views.values():
                view.versions.pop(path, None)
                listing = view.listings.get(parent)
                if listing is not None:
                    listing.entries.pop(name, None)
                if emptied:
                    for directory in (parent, *_above(parent)):
                        view.listings.pop(directory, None)

    def renamed(self, old: str, new: str, seen: View) -> None:
        self.removed(old, seen)
        with self.lock:
            self._bytes.drop(new)
            for view in self._views.values():
                view.versions.pop(new, None)
                _placed(view, new, None)

    def made_dir(self, path: str) -> None:
        # Kept here until a file is saved inside it: the server holds
        # files, and a directory is only the path they share.
        with self.lock:
            self._local_dirs.add(path)

    def moved_dir(self, old: str, new: str, view: View) -> None:
        """Move a directory kept only here, with those kept inside it."""
        with self.lock:
            for dirs in (self._local_dirs, view.kept):
                moved = {d for d in dirs if d == old or d.startswith(old + "/")}
                dirs.difference_update(moved)
                dirs.update(new + d[len(old):] for d in moved)

    def dropped_dir(self, path: str, view: View) -> None:
        parent, name = split(path)
        with self.lock:
            self._local_dirs.discard(path)
            view.kept.discard(path)
            for each in self._views.values():
                each.listings.pop(path, None)
                listing = each.listings.get(parent)
                if listing is not None:
                    listing.entries.pop(name, None)

    def gone(self, path: str, view: View | None) -> None:
        """A path the server found gone below a made-up directory may mean
        that directory changed, so it and those above it are asked again;
        below a directory kept only here, the server holding nothing is
        expected."""
        with self.lock:
            chain = [path, *_above(path)]
            if not any(self.is_local(p, view) for p in chain):
                for p in chain:
                    self._made.pop(p, None)
