"""The exact set of files a shared file exposes: the entry plus what it uses.

A visitor gets the files on this list and nothing else, so the list is what
the owner reviews before sharing. It is computed from the files as they are
now, by reading them the way the owner's own file routes do, and the caller
freezes it: a report that later gains an asset shows the owner the drift
rather than widening the share on its own.

Detection is static. Page references, stylesheet ``url()``s and Markdown
links are exact; a path found as a string literal in a script is a guess,
marked ``script`` so the dialog can say so.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
import re
import shlex
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from html.parser import HTMLParser
from typing import Any
from urllib.parse import unquote

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.app.share_access import ShareScope, shared_path_visible
from src.server.app.workspace_files._shared import _to_client_path, owner_work_dir
from src.server.app.workspace_files.serve import _db_fallback_bytes, warm_sandbox_bytes
from src.server.models.workspace import served_from_mirror
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.workspace_file_containment import (
    contained_absolute_path,
    contained_relative_path,
    contained_sandbox_paths,
    FileTooLargeToServe,
    is_within,
)
from src.server.services.workspace_files import (
    acquire_sandbox_to_change,
    previous_dir_names_of,
)

logger = logging.getLogger(__name__)

MAX_FILES = 200
MAX_TOTAL_BYTES = 256 * 1024 * 1024
# Every path a page names costs a probe and a stat in the sandbox, so this
# bounds the walk's execs as well as its list: a page naming 100k local files
# would otherwise be thousands of execs, most for files that do not exist.
_MAX_CANDIDATES = 5 * MAX_FILES
# Paths per exec. The kernel caps one argument at 128 KiB, and a command is
# one argument once the sandbox wraps it; a few hundred paths stay well under.
_STAT_CHUNK = 200

REASON_ENTRY = "entry"
REASON_PAGE = "page"
REASON_STYLE = "style"
REASON_MARKDOWN = "markdown"
REASON_SCRIPT = "script"

_HTML_SUFFIXES = (".html", ".htm", ".xhtml", ".svg")
_CSS_SUFFIXES = (".css",)
_MARKDOWN_SUFFIXES = (".md", ".markdown")
_SCRIPT_SUFFIXES = (".js", ".mjs", ".cjs")
# The shared page renders Markdown with the app's viewer, which draws these
# and turns every other file link into plain text, so an image is the only
# thing a Markdown file can show a visitor.
_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp")

# A page linked from the entry is parsed; a page linked from that one is
# listed but not parsed, which is what keeps a multi-page site from pulling in
# every page reachable from its navigation.
_HTML_PARSE_DEPTH = 1

# A file past this is listed but never parsed for references.
_MAX_PARSE_BYTES = 8 * 1024 * 1024

_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")
# A quoted URL runs to its closing quote, so a name holding parentheses or
# spaces ("revenue (1).png") is read whole; an unquoted one stops at either.
_CSS_URL_RE = re.compile(
    r"""url\(\s*(?:"([^"]+)"|'([^']+)'|([^'"()\s]+))\s*\)""", re.IGNORECASE
)
_CSS_IMPORT_RE = re.compile(r"""@import\s+(?!url\()['"]([^'"]+)['"]""", re.IGNORECASE)
# A label holds no "[", so a run of unmatched ones costs one scan each to the
# next bracket instead of one to the end of the file. A destination is either
# bracketed, where a space is part of the name, or runs to the first space.
_MD_LINK_RE = re.compile(r"!?\[[^\[\]]*\]\(\s*(?:<([^<>\n]+)>|([^\s)]+))")
_MD_REF_RE = re.compile(
    r"^ {0,3}\[[^\[\]\n]+\]:[ \t]*(?:<([^<>\n]+)>|(\S+))", re.MULTILINE
)
_META_REFRESH_RE = re.compile(r"url\s*=\s*(['\"]?)([^'\"]+)\1", re.IGNORECASE)
# A quoted string that could be a relative file: no scheme, no whitespace, no
# template or glob punctuation, and an extension, which a query or fragment
# ("chart.png?v=2") may follow.
_SCRIPT_PATH_RE = re.compile(
    r"""['"`]([^'"`\s:{}$<>*?|]*[^'"`\s:{}$<>*?|/.]\.[A-Za-z0-9]{1,8})"""
    r"""(?:[?#][^'"`\s]*)?['"`]"""
)


class ManifestTooLarge(Exception):
    """The set is over a cap; sharing is refused rather than truncated."""

    def __init__(self, code: str, limit: int) -> None:
        super().__init__(f"{code} (limit {limit})")
        self.code = code
        self.limit = limit


class ManifestEntryMissing(Exception):
    """The entry file is not in the workspace, or may not be shared."""


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    size: int
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {"path": self.path, "size": self.size, "reason": self.reason}


# --- reference extraction -------------------------------------------------


class _RefCollector(HTMLParser):
    """Every URL an HTML or SVG document asks the browser to fetch or follow."""

    _URL_ATTRS = ("src", "href", "poster", "data", "xlink:href")

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.page: list[str] = []
        self.style: list[str] = []
        self.script: list[str] = []
        self._block: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {k.lower(): v for k, v in attrs if v is not None}
        if tag == "style":
            self._block = "style"
        elif tag == "script" and "src" not in attributes:
            self._block = "script"
        for name in self._URL_ATTRS:
            if name in attributes:
                self.page.append(attributes[name])
        if "srcset" in attributes:
            for candidate in attributes["srcset"].split(","):
                url = candidate.strip().split(None, 1)[0] if candidate.strip() else ""
                if url:
                    self.page.append(url)
        if "style" in attributes:
            self.style.extend(css_refs(attributes["style"]))
        if tag == "meta" and attributes.get("http-equiv", "").lower() == "refresh":
            match = _META_REFRESH_RE.search(attributes.get("content", ""))
            if match:
                self.page.append(match.group(2))

    def handle_endtag(self, tag: str) -> None:
        if tag in ("style", "script"):
            self._block = None

    def handle_data(self, data: str) -> None:
        if self._block == "style":
            self.style.extend(css_refs(data))
        elif self._block == "script":
            self.script.extend(script_refs(data))


def html_refs(text: str) -> tuple[list[str], list[str], list[str]]:
    """Page, style and script references, in that order."""
    collector = _RefCollector()
    try:
        collector.feed(text)
        collector.close()
    except Exception:
        logger.debug("HTML reference scan stopped early", exc_info=True)
    return collector.page, collector.style, collector.script


def css_refs(text: str) -> list[str]:
    refs = [m.group(1) or m.group(2) or m.group(3) for m in _CSS_URL_RE.finditer(text)]
    refs.extend(m.group(1) for m in _CSS_IMPORT_RE.finditer(text))
    return refs


def markdown_refs(text: str) -> list[str]:
    matches = [*_MD_LINK_RE.finditer(text), *_MD_REF_RE.finditer(text)]
    return [m.group(1) or m.group(2) for m in matches]


def script_refs(text: str) -> list[str]:
    return [m.group(1) for m in _SCRIPT_PATH_RE.finditer(text)]


def resolve_ref(
    ref: str, referrer: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str | None:
    """The workspace path a reference names, or None when it names nothing here.

    Relative references resolve against the referencing file's folder, and
    ``../`` may climb as far as the workspace root. A sandbox-absolute or
    ``file:`` spelling folds the way every file route folds it. Anything with
    another scheme, a ``data:`` body or only a fragment is not a file.
    """
    raw = (ref or "").strip()
    if not raw or raw.startswith(("#", "//")):
        return None
    if _SCHEME_RE.match(raw) and not raw.lower().startswith("file:"):
        return None
    raw = raw.split("#", 1)[0].split("?", 1)[0]
    raw = unquote(raw)
    if not raw or "\x00" in raw:
        return None
    if raw.startswith("/") or raw.lower().startswith("file:"):
        return contained_relative_path(raw, work_dir, previous_dir_names)
    joined = posixpath.normpath(posixpath.join(posixpath.dirname(referrer), raw))
    if joined in (".", "..") or joined.startswith(("/", "../")):
        return None
    return joined


# --- file access ---------------------------------------------------------


class _MirrorReader:
    """Sizes and bytes from the persisted copy, for a workspace that is not running."""

    def __init__(self, workspace: dict[str, Any]) -> None:
        self._workspace = workspace
        self._workspace_id = str(workspace["workspace_id"])
        self._sizes: dict[str, int] | None = None

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        if self._sizes is None:
            tree = await FilePersistenceService.get_file_tree(self._workspace_id)
            self._sizes = {f["path"]: int(f.get("size") or 0) for f in tree}
        return {p: (p, self._sizes[p]) for p in paths if p in self._sizes}

    async def read(self, path: str) -> bytes | None:
        # The mime is the serve route's concern; only the bytes are parsed here.
        resolved = await _db_fallback_bytes(self._workspace, self._workspace_id, path, "")
        return resolved[0] if resolved else None


def _stat_command(paths: list[str]) -> str:
    quoted = " ".join(shlex.quote(p) for p in paths)
    return (
        f"set -- {quoted}; "
        'for t in "$@"; do '
        'if [ -f "$t" ]; then s=$(wc -c < "$t" 2>/dev/null | tr -d " "); '
        "printf 'y%s\\n' \"${s:-0}\"; "
        "else printf 'n\\n'; fi; done"
    )


class _SandboxReader:
    """Sizes and bytes from the live tree, judged on canonical paths."""

    def __init__(self, sandbox: Any, work_dir: str, scope: ShareScope) -> None:
        self._sandbox = sandbox
        self._work_dir = work_dir
        self._visible = partial(shared_path_visible, scope)

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        found: dict[str, tuple[str, int]] = {}
        for start in range(0, len(paths), _STAT_CHUNK):
            found.update(await self._stat(paths[start : start + _STAT_CHUNK]))
        return found

    async def _stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        absolute = [contained_absolute_path(p, self._work_dir) for p in paths]
        candidates = [
            (p, a) for p, a in zip(paths, absolute)
            if a is not None and self._sandbox.validate_path(a)
        ]
        if not candidates:
            return {}
        # Strict: a probe that fails would otherwise read as every file
        # missing, and the owner would share a list short of its assets.
        canonicals = await contained_sandbox_paths(
            self._sandbox,
            [a for _, a in candidates],
            work_dir=self._work_dir,
            strict=True,
        )
        contained = [
            (p, c) for (p, _), c in zip(candidates, canonicals)
            if c is not None and is_within(self._work_dir, c)
        ]
        if not contained:
            return {}
        try:
            result = await self._sandbox.runtime.exec(
                _stat_command([c for _, c in contained]), timeout=10
            )
        except RuntimeError:
            raise
        except Exception as e:
            raise SandboxTransientError(f"Manifest stat failed: {e}") from e
        lines = str(getattr(result, "stdout", "") or "").splitlines()
        if getattr(result, "exit_code", None) != 0 or len(lines) != len(contained):
            raise SandboxTransientError("Manifest stat returned an unexpected shape")
        found: dict[str, tuple[str, int]] = {}
        for (requested, canonical), line in zip(contained, lines):
            if line[:1] != "y":
                continue
            try:
                size = int(line[1:] or "0")
            except ValueError:
                size = 0
            client = _to_client_path(self._sandbox, canonical, self._work_dir)
            found[requested] = (client, size)
        return found

    async def read(self, path: str) -> bytes | None:
        try:
            resolved = await warm_sandbox_bytes(
                self._sandbox, path, work_dir=self._work_dir, visible=self._visible
            )
        except FileTooLargeToServe:
            # Grew past an exec read since it was sized: listed, not parsed.
            return None
        return resolved[1] if resolved else None


@asynccontextmanager
async def _reader_for(
    workspace: dict[str, Any], user_id: str, work_dir: str
) -> AsyncIterator[tuple[Any, str, tuple[str, ...]]]:
    """The same source the owner's own file routes read for this status.

    Yields the reader with the folder and former folders to fold references
    against. A live walk holds the folder until the list is built: a rename
    settling mid-walk would drop every file read after the move.
    """
    if served_from_mirror(workspace.get("status")):
        yield _MirrorReader(workspace), work_dir, previous_dir_names_of(workspace)
        return
    workspace_id = str(workspace["workspace_id"])
    async with acquire_sandbox_to_change(workspace_id, user_id) as (sandbox, held):
        work_dir = owner_work_dir(held)
        yield (
            _SandboxReader(sandbox, work_dir, ShareScope(workspace_id, "")),
            work_dir,
            previous_dir_names_of(held),
        )


# --- the walk ------------------------------------------------------------


def _kind(path: str) -> str | None:
    lower = path.lower()
    if lower.endswith(_HTML_SUFFIXES):
        return "html"
    if lower.endswith(_CSS_SUFFIXES):
        return "css"
    if lower.endswith(_MARKDOWN_SUFFIXES):
        return "markdown"
    if lower.endswith(_SCRIPT_SUFFIXES):
        return "script"
    return None


def _references(kind: str, content: bytes) -> list[tuple[str, str]]:
    """``(ref, reason)`` pairs a file of this kind makes."""
    text = content.decode("utf-8", errors="replace")
    if kind == "html":
        page, style, script = html_refs(text)
        return (
            [(r, REASON_PAGE) for r in page]
            + [(r, REASON_STYLE) for r in style]
            + [(r, REASON_SCRIPT) for r in script]
        )
    if kind == "css":
        return [(r, REASON_STYLE) for r in css_refs(text)]
    if kind == "markdown":
        page, _, _ = html_refs(text)
        return [
            (r, REASON_MARKDOWN)
            for r in markdown_refs(text) + page
            if r.split("#", 1)[0].split("?", 1)[0].lower().endswith(_IMAGE_SUFFIXES)
        ]
    return [(r, REASON_SCRIPT) for r in script_refs(text)]


async def build_manifest(
    workspace: dict[str, Any], entry_path: str, *, user_id: str, work_dir: str
) -> list[ManifestEntry]:
    """The files ``entry_path`` exposes, entry first, in discovery order.

    ``entry_path`` is already folded to a workspace-relative path. Raises
    ``ManifestEntryMissing`` when it is not a shareable file here and
    ``ManifestTooLarge`` past either cap.
    """
    workspace_id = str(workspace["workspace_id"])
    scope = ShareScope(workspace_id, "")
    if not shared_path_visible(scope, entry_path):
        raise ManifestEntryMissing(entry_path)

    async with _reader_for(workspace, user_id, work_dir) as (
        reader,
        work_dir,
        previous_dirs,
    ):
        return await _walk(reader, entry_path, scope, work_dir, previous_dirs)


async def _walk(
    reader: Any,
    entry_path: str,
    scope: ShareScope,
    work_dir: str,
    previous_dirs: tuple[str, ...],
) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    listed: set[str] = set()
    total = 0
    candidates = 0
    # (path, reason, depth, page): depth counts pages, so the parse limit
    # applies to pages only and an asset chain of any length is followed.
    # ``page`` is the document the file was loaded into.
    level: list[tuple[str, str, int, str]] = [(entry_path, REASON_ENTRY, 0, entry_path)]

    while level:
        # One stat per level; the first reference to a path decides its reason.
        batch: dict[str, tuple[str, int, str]] = {}
        for path, reason, depth, page in level:
            if path not in listed and shared_path_visible(scope, path):
                batch.setdefault(path, (reason, depth, page))
        candidates += len(batch)
        if candidates > _MAX_CANDIDATES:
            raise ManifestTooLarge("too_many_files", MAX_FILES)
        found = await reader.stat(list(batch)) if batch else {}
        level = []

        for path, (reason, depth, page) in batch.items():
            if path not in found:
                continue
            client_path, size = found[path]
            new = [
                p for p in dict.fromkeys((path, client_path))
                if p not in listed and shared_path_visible(scope, p)
            ]
            listed.update(new)
            entries.extend(ManifestEntry(p, size, reason) for p in new)
            if new:
                # A path and the canonical file it names are one file's bytes.
                total += size
            if len(entries) > MAX_FILES:
                raise ManifestTooLarge("too_many_files", MAX_FILES)
            if total > MAX_TOTAL_BYTES:
                raise ManifestTooLarge("too_many_bytes", MAX_TOTAL_BYTES)

            kind = _kind(path)
            # A Markdown image is drawn from its bytes, so nothing it names
            # (an SVG's own references) is ever fetched.
            if kind is None or reason == REASON_MARKDOWN or size > _MAX_PARSE_BYTES:
                continue
            if kind == "html" and depth > _HTML_PARSE_DEPTH:
                continue
            if kind == "markdown" and depth > 0:
                continue
            content = await reader.read(path)
            if content is None:
                continue
            next_depth = depth + 1 if kind in ("html", "markdown") else depth
            if kind in ("html", "markdown"):
                page = path
            # The viewer reads a Markdown image path from the workspace root,
            # the way the owner's panel does, so the list has to as well. A
            # script's string is a URL its page resolves (an image, a fetch)
            # or one it resolves itself (an import), so both are tried; only
            # files that exist are listed.
            if kind == "markdown":
                referrers: tuple[str, ...] = ("",)
            elif kind == "script":
                referrers = tuple(dict.fromkeys((path, page)))
            else:
                referrers = (path,)
            # Parsing a file of up to _MAX_PARSE_BYTES is seconds of CPU; on
            # the loop it would stall every stream this worker is serving.
            refs = await asyncio.to_thread(_references, kind, content)
            for ref, ref_reason in refs:
                for referrer in referrers:
                    resolved = resolve_ref(ref, referrer, work_dir, previous_dirs)
                    if resolved is not None and resolved not in listed:
                        level.append((resolved, ref_reason, next_depth, page))

    if not entries:
        raise ManifestEntryMissing(entry_path)
    return entries


def manifest_drift(
    current: list[ManifestEntry], stored: list[str] | None
) -> dict[str, list[str]] | None:
    """What changed since the owner confirmed ``stored``, or None when nothing did."""
    if stored is None:
        return None
    now = {e.path for e in current}
    before = set(stored)
    if now == before:
        return None
    return {
        "added": sorted(now - before),
        "removed": sorted(before - now),
    }
