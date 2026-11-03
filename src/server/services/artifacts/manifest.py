"""The exact set of files a published version holds: the entry plus what it uses.

A visitor gets the files on this list and nothing else, so the list is what
the owner reviews before publishing. It is computed from the files as they
are now, by reading them the way the owner's own file routes do, and a
version freezes it: a report that later gains an asset shows the owner the
drift rather than widening what visitors see on its own.

What a file's source names comes from ``references``. What the live page
reported loading joins the walk as ``loaded``, since a path built in code is
known to no scan of its source.
"""

from __future__ import annotations

import asyncio
import posixpath
import re
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any
from urllib.parse import unquote

from src.server.services.artifacts.reader import NotInStoredCopy, Reader
from src.server.services.artifacts.references import (
    REASON_LINK,
    REASON_MARKDOWN,
    REASON_PAGE,
    REASON_SCRIPT,
    REASON_STYLE,
    file_kind,
    file_references,
    url_path,
)
from src.server.services.served_paths import may_be_artifact
from src.server.services.workspace_file_containment import contained_relative_path

#: What a version may hold. The walk stops here, before reading further, since
#: no publish could carry more.
MAX_FILES = 200
MAX_BYTES = 25 * 1024 * 1024
#: Every listed path is read and stored as its own file, so a file reached by
#: its link and its own path costs twice: a pnpm package in ``node_modules``.
#: No more copies than that, or one file named through many links multiplies.
MAX_LISTED_BYTES = 2 * MAX_BYTES
# Every path a page names costs a probe and a stat in the sandbox, so this
# bounds the walk's execs as well as its list: a page naming 100k local files
# would otherwise be thousands of execs, most for files that do not exist.
_MAX_CANDIDATES = 5 * MAX_FILES
# A string that only looks like a path is not a claim that the page uses a
# file, and a data file can hold thousands. Past this many the rest go unprobed
# rather than refuse the publish; what the page really loads still arrives as
# ``loaded``.
_MAX_GUESSES = _MAX_CANDIDATES

REASON_ENTRY = "entry"
REASON_LOADED = "loaded"

# A page linked from the entry is followed in full. A page further down is still
# parsed, so everything it loads is listed and a visitor who clicks through sees
# it whole, but its links are not followed, which is what keeps a multi-page site
# from pulling in every page reachable from its navigation.
_HTML_PARSE_DEPTH = 1

_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")


class ManifestTooLarge(Exception):
    """The set is over a cap; sharing is refused rather than truncated.
    ``entries`` and ``left_out`` are what the walk had listed when it stopped,
    the file that crossed the cap last, so the review can still offer the
    owner what they may leave out to fit."""

    def __init__(
        self,
        code: str,
        limit: int,
        entries: Sequence[ManifestEntry] = (),
        left_out: Sequence[ManifestEntry] = (),
    ) -> None:
        super().__init__(f"{code} (limit {limit})")
        self.code = code
        self.limit = limit
        self.entries = list(entries)
        self.left_out = list(left_out)


class ManifestEntryMissing(Exception):
    """The entry file is not in the workspace, or may not be shared."""


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    #: None only for a file the owner left out that nothing could probe: one
    #: the stored copy never keeps, while its computer is stopped.
    size: int | None
    reason: str
    #: The owner may leave it out: nothing says the page needs it to draw now.
    optional: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "size": self.size,
            "reason": self.reason,
            "optional": self.optional,
        }


@dataclass(frozen=True)
class Manifest:
    #: What a publish carries, entry first, in discovery order.
    entries: list[ManifestEntry]
    #: What the owner left out and the page still names, so the review can
    #: offer it back. Nothing here is read, counted or carried.
    left_out: list[ManifestEntry]
    #: The bytes it carries. A file listed under a link and under the path it
    #: names counts once, as a pnpm ``node_modules`` lists every package.
    byte_total: int
    #: What the walk read to scan, by listed path: a version seals these bytes,
    #: not a later read, so what it holds is what its list was built from.
    contents: dict[str, bytes] = field(default_factory=dict)
    #: What the files load from the root of the origin (``/assets/app.js``, a
    #: built app's default), which no view of a workspace file serves: listed
    #: for the review to warn about, as written, never as files to carry.
    root_refs: list[str] = field(default_factory=list)


def resolve_ref(
    ref: str, referrer: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str | None:
    """The workspace path a reference names, or None when it names nothing here.

    Relative references resolve against the referencing file's folder, and
    ``../`` may climb as far as the workspace root. A sandbox-absolute or
    ``file:`` spelling folds the way every file route folds it. Anything with
    another scheme, a ``data:`` body or only a fragment is not a file. A
    backslash in the path is a slash, as the browser's URL parser reads it;
    one written ``%5C`` stays part of a name.
    """
    raw = (ref or "").strip()
    if not raw or raw.startswith("#"):
        return None
    if _SCHEME_RE.match(raw) and not raw.lower().startswith("file:"):
        return None
    raw = url_path(raw.split("#", 1)[0].split("?", 1)[0])
    if raw.startswith("//"):
        return None
    raw = unquote(raw)
    if not raw or "\x00" in raw:
        return None
    return _folded(raw, referrer, work_dir, previous_dir_names)


def _folded(
    raw: str, referrer: str, work_dir: str, previous_dir_names: Sequence[str]
) -> str | None:
    if raw.startswith("/") or raw.lower().startswith("file:"):
        return contained_relative_path(raw, work_dir, previous_dir_names)
    joined = posixpath.normpath(posixpath.join(posixpath.dirname(referrer), raw))
    if joined in (".", "..") or joined.startswith(("/", "../")):
        return None
    return joined


def _names_origin_root(ref: str) -> bool:
    """Whether a browser reads the reference from the root of the origin
    serving the page (or refuses it, for ``file:``), where no workspace file is
    in the live view or a version: only the Markdown viewer reads a path from
    the workspace root, so listing one from a page would carry a file its
    version still answers 404 for."""
    raw = url_path((ref or "").strip())
    return (raw.startswith("/") and not raw.startswith("//")) or raw.lower().startswith("file:")


def _loaded_path(path: str, reader: Reader) -> str | None:
    """The workspace path a live page reported, folded as ``resolve_ref`` folds.

    The page reports a decoded path rather than a URL, so a "#" or "%" in it is
    part of the file's name and not read as one. A workspace file's path never
    starts with "/": one that does is a load from the root of the page's host.
    """
    raw = (path or "").strip()
    if not raw or "\x00" in raw or raw.startswith("/"):
        return None
    return _folded(raw, "", reader.work_dir, reader.previous_dirs)


def _resolved_references(
    kind: str,
    content: bytes,
    referrers: tuple[str, ...],
    reader: Reader,
    follow_links: bool,
) -> tuple[dict[str, str], list[str]]:
    """The files a file names, each with the reason it was first named, and
    what it loads from the root of the origin (``Manifest.root_refs``). Off the
    event loop: a page near the byte cap is seconds of parsing, and inlined data
    names tens of thousands of strings to resolve, which would stall every stream
    this worker is serving."""
    found: dict[str, str] = {}
    rooted: list[str] = []
    for ref, reason in dict.fromkeys(file_references(kind, content, follow_links)):
        if kind != "markdown" and _names_origin_root(ref):
            # Only what markup or a stylesheet loads: a link home ("/") is a
            # page's habit, and a string in a script may be no address at all.
            ref = ref.strip()
            if reason in _LOADS and url_path(ref).startswith("/") and len(ref) <= _MAX_REF_LENGTH:
                rooted.append(ref)
            continue
        for referrer in referrers:
            resolved = resolve_ref(ref, referrer, reader.work_dir, reader.previous_dirs)
            if resolved is not None:
                found.setdefault(resolved, reason)
    return found, rooted


_LOADS = frozenset({REASON_PAGE, REASON_STYLE})
#: Enough of them for the owner to see the pattern.
_MAX_ROOT_REFS = 20
_MAX_REF_LENGTH = 1024


def _referrers(kind: str, path: str, page: str) -> tuple[str, ...]:
    """Where a file's references resolve from. The viewer reads a Markdown image
    path from the workspace root, the way the owner's panel does, so the list
    has to as well. A script's string is a URL its page resolves (an image, a
    fetch) or one it resolves itself (an import), so both are tried; only files
    that exist are listed."""
    if kind == "markdown":
        return ("",)
    if kind == "script":
        return tuple(dict.fromkeys((path, page)))
    return (path,)


def _scan_key(pending: "_Pending") -> tuple[str, str]:
    """What a scan of a file depends on: its spelling, and for a script the
    page that runs it, whose folder its fetches resolve from (``_referrers``)."""
    return pending.path, pending.page if file_kind(pending.path) == "script" else ""


@dataclass(frozen=True)
class _Pending:
    """A path to probe. ``depth`` counts pages, so the link limit applies to
    pages only and an asset chain of any length is followed; ``page`` is the
    document the file was loaded into."""

    path: str
    reason: str
    depth: int
    page: str
    #: A load an earlier report named and this one did not.
    remembered: bool = False

    @property
    def guess(self) -> bool:
        return self.reason == REASON_SCRIPT

    @property
    def optional(self) -> bool:
        """Nothing says the page needs it to draw, so the owner may leave it out:
        a link, a guess, a load only an earlier report named. A file the page
        does load goes along whatever they said, or the page would break for
        the visitors it was published for."""
        return self.reason == REASON_LINK or self.guess or self.remembered

    @property
    def rank(self) -> int:
        """Who decides a path several name at once: what it loads, then what it
        links to, then what merely looks like a path."""
        return 2 if self.guess else 1 if self.reason == REASON_LINK else 0


class _Walk:
    """The running totals of one walk, kept across its static pass and what the
    page reported, so both share every cap and no path is probed twice."""

    def __init__(self, reader: Reader, left_out: Sequence[str]) -> None:
        self._reader = reader
        self._unwanted = frozenset(left_out)
        self.entries: list[ManifestEntry] = []
        self.left_out: dict[str, ManifestEntry] = {}
        self._listed: set[str] = set()
        #: Each listed path's spellings, the one asked for and the file's own.
        self._spellings: dict[str, tuple[str, ...]] = {}
        self._probed: set[str] = set()
        self.total = 0
        self._listed_bytes = 0
        self._named = 0
        self._guessed = 0
        self.contents: dict[str, bytes] = {}
        #: Each spelling scanned for what it names, by ``_scan_key``: a
        #: relative reference resolves against the spelling that named the
        #: file, and a script's also against each page that runs it.
        self._scanned: set[tuple[str, str]] = set()
        self._rescans: list[_Pending] = []
        self.root_refs: dict[str, None] = {}

    async def follow(self, level: list[_Pending]) -> None:
        while level:
            # One stat per level.
            batch = self._admit(level)
            found = await self._stat(batch)
            level = []
            for path, pending in batch.items():
                if path not in found:
                    continue
                client_path, size = found[path]
                if not may_be_artifact(client_path):
                    # A link to a file that may not leave: the read would refuse
                    # it, so it is as missing as a file that is not there.
                    continue
                if pending.optional and not self._unwanted.isdisjoint(
                    (path, client_path)
                ):
                    # Not followed: what only this file uses stays out with it.
                    # Under the spelling the owner left out, which putting it
                    # back has to name.
                    spelling = path if path in self._unwanted else client_path
                    self.left_out[path] = ManifestEntry(
                        spelling, size, pending.reason, optional=True
                    )
                    continue
                self.left_out.pop(path, None)
                self._list(pending, client_path, size)
                level.extend(await self._named_by(pending))
            rescans, self._rescans = self._rescans, []
            for pending in rescans:
                level.extend(await self._named_by(pending))

    async def _stat(self, batch: dict[str, _Pending]) -> dict[str, tuple[str, int | None]]:
        """Each path's own spelling and size. A file the owner left out is no
        reason to need the computer: one the stored copy cannot speak for keeps
        its row as they left it, its size unknown, while a file the page needs
        still refuses the whole read."""
        asked = list(batch)
        unprobed: dict[str, tuple[str, int | None]] = {}
        while asked:
            try:
                return {**await self._reader.files.stat(asked), **unprobed}
            except NotInStoredCopy as refused:
                pending = batch.get(refused.path)
                if pending is None or not pending.optional or refused.path not in self._unwanted:
                    raise
                asked.remove(refused.path)
                unprobed[refused.path] = (refused.path, None)
        return unprobed

    def _admit(self, level: list[_Pending]) -> dict[str, _Pending]:
        """The paths of this level worth a probe; the first reference to a path
        decides its reason, and a name goes before a guess."""
        batch: dict[str, _Pending] = {}
        for pending in sorted(level, key=lambda p: p.rank):
            path = pending.path
            if path in batch or not may_be_artifact(path):
                continue
            if path in self._listed:
                # Listed as a link or a guess, and loaded after all: by a page
                # further on, or by the live page, whose reports come last.
                if not pending.optional:
                    self._require(path)
                # Listed as another spelling's file, or a script another page
                # runs too: what it names from here resolves against this
                # folder or page, not that one.
                if (key := _scan_key(pending)) not in self._scanned:
                    self._scanned.add(key)
                    self._rescans.append(pending)
                continue
            if path in self._probed and (path not in self.left_out or pending.optional):
                continue
            if pending.guess:
                if self._guessed >= _MAX_GUESSES:
                    continue
                self._guessed += 1
            else:
                self._named += 1
                if self._named > _MAX_CANDIDATES:
                    raise self._over("too_many_files", MAX_FILES)
            batch[path] = pending
        self._probed.update(batch)
        return batch

    def _list(self, pending: _Pending, client_path: str, size: int) -> None:
        new = [
            p for p in dict.fromkeys((pending.path, client_path))
            if p not in self._listed
        ]
        self._listed.update(new)
        for p in new:
            self._spellings[p] = (pending.path, client_path)
        self.entries.extend(
            ManifestEntry(p, size, pending.reason, pending.optional) for p in new
        )
        if client_path in new:
            # Every path to one file is that file's bytes, counted once.
            self.total += size
        self._listed_bytes += size * len(new)
        if len(self.entries) > MAX_FILES:
            raise self._over("too_many_files", MAX_FILES)
        # The limit named is the one crossed, for the owner to get under.
        if self.total > MAX_BYTES:
            raise self._over("too_many_bytes", MAX_BYTES)
        if self._listed_bytes > MAX_LISTED_BYTES:
            raise self._over("too_many_bytes", MAX_LISTED_BYTES)

    def _over(self, code: str, limit: int) -> ManifestTooLarge:
        return ManifestTooLarge(code, limit, self.entries, self.left_out_rows())

    def left_out_rows(self) -> list[ManifestEntry]:
        """One row per spelling left out, and none for one that went along: two
        links to a file under one spelling, or a file another reference loads,
        are not two choices nor one to undo."""
        rows: dict[str, ManifestEntry] = {}
        for entry in self.left_out.values():
            if entry.path not in self._listed:
                rows.setdefault(entry.path, entry)
        return list(rows.values())

    def _require(self, path: str) -> None:
        spellings = self._spellings[path]
        self.entries = [
            replace(e, optional=False) if e.optional and e.path in spellings else e
            for e in self.entries
        ]

    async def _named_by(self, file: _Pending) -> list[_Pending]:
        kind = file_kind(file.path)
        # A Markdown image is drawn from its bytes, so nothing it names (an
        # SVG's own references) is ever loaded.
        if kind is None or file.reason == REASON_MARKDOWN:
            return []
        if kind == "markdown" and file.depth > 0:
            return []
        self._scanned.add(_scan_key(file))
        # A file scanned again for another page is the bytes the version keeps.
        content = self.contents.get(file.path)
        if content is None:
            content = await self._reader.files.read(file.path)
        if content is None:
            return []
        for spelling in self._spellings.get(file.path, ()):
            self.contents.setdefault(spelling, content)
        self.contents[file.path] = content
        is_document = kind in ("html", "markdown")
        page = file.path if is_document else file.page
        named, rooted = await asyncio.to_thread(
            _resolved_references,
            kind,
            content,
            _referrers(kind, file.path, page),
            self._reader,
            file.depth <= _HTML_PARSE_DEPTH,
        )
        for ref in rooted:
            if len(self.root_refs) >= _MAX_ROOT_REFS:
                break
            self.root_refs.setdefault(ref, None)
        next_depth = file.depth + 1 if is_document else file.depth
        return [
            _Pending(path, reason, next_depth, page) for path, reason in named.items()
        ]


async def walk_manifest(
    reader: Reader,
    entry_path: str,
    *,
    loaded: Sequence[str] = (),
    remembered: Sequence[str] = (),
    left_out: Sequence[str] = (),
) -> Manifest:
    """The files ``entry_path`` exposes, entry first, in discovery order, read
    through a reader the caller holds so it can go on to read the same files
    from the same source.

    ``entry_path`` is already folded to a workspace-relative path. ``loaded``
    holds the workspace paths the live page reported loading; they are followed
    like references of the entry. ``remembered`` holds what earlier reports
    named, which a file only a click loads needs, and which the owner may leave
    out once the page stops loading it. ``left_out`` holds what the owner took
    off the list. Raises ``ManifestEntryMissing`` when the entry
    is not a shareable file here and ``ManifestTooLarge`` past either cap.
    """
    walk = _Walk(reader, left_out)
    await walk.follow([_Pending(entry_path, REASON_ENTRY, 0, entry_path)])
    if not walk.entries:
        raise ManifestEntryMissing(entry_path)
    # A reported path starting with "/" is an address the page built from the
    # root of its host, which no view serves: a warning, never a file.
    for raw in (*loaded, *remembered):
        ref = (raw or "").strip()
        if ref.startswith("/") and len(walk.root_refs) < _MAX_ROOT_REFS:
            walk.root_refs.setdefault(ref, None)
    reported = {p for raw in loaded if (p := _loaded_path(raw, reader))}
    loads: dict[str, bool] = {}
    for raw in (*remembered, *loaded):
        if (p := _loaded_path(raw, reader)) and p not in loads:
            loads[p] = p not in reported
    await walk.follow(
        [
            _Pending(path, REASON_LOADED, 1, entry_path, remembered=before)
            for path, before in loads.items()
        ]
    )
    listed = {e.path for e in walk.entries}
    return Manifest(
        walk.entries,
        walk.left_out_rows(),
        walk.total,
        {p: data for p, data in walk.contents.items() if p in listed},
        list(walk.root_refs),
    )
