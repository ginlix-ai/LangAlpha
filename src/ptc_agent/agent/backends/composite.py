"""Composite filesystem backend — prefix-routed fan-out over the rich-method surface."""

from __future__ import annotations

import asyncio
import functools
import posixpath
from typing import Any, Callable, Protocol, Sequence, TypeVar

import structlog

from ptc_agent.agent.backends.results import EditTextResult, WriteTextResult
from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.backends.search_match import DiskGlob, RgRefused, RgWalk
from ptc_agent.core.paths import (
    MOUNTED_AGENT_SUBDIRS,
    computer_tier_relative_path,
    grep_working_dir,
    lexical_path,
)
from ptc_agent.core.sandbox.grep_render import GrepLine

logger = structlog.get_logger(__name__)

_T = TypeVar("_T")

# A Grep filter longer than this is matched off the event loop: matching
# costs about what the filter is long for each route file, and one of
# thousands of alternatives over a thousand files would hold the loop for
# a third of a second.
_INLINE_FILTER_CHARS = 1024

# The computer's server-held folders, which the file mount links in at the
# root and in every folder. The sandbox glob goes through such a link only by
# the pattern's literal prefix, so Glob finds their files only that way. The
# workspace's own memory is linked too but stays in a wildcard's reach, as
# Glob has always listed it.
_LINKED_COMPUTER_DIRS = tuple(
    subdir
    for subdir in MOUNTED_AGENT_SUBDIRS
    if computer_tier_relative_path(subdir) is not None
)


class FilesystemRoute(Protocol):
    """Structural contract for a prefix-mounted route (StoreBackend,
    WorkflowsBackend, UserDataBackend, …): the composite dispatches these
    seven members, and reads the optional one below where a route has it.

    Glob lists a route with ``aglob_paths("*", root_prefix)``, wherever the
    search starts, which has to name every file the route holds. Glob's
    pattern and Grep's ``glob`` and ``type`` are matched here, against each
    file's path, so ``agrep_rich`` is never handed a filter or a type.

    ``fixed_names`` (None when absent) names every file the route can hold,
    relative to its root, for a route that knows them without a read. A
    search at or above such a route skips it when its pattern can match none
    of them, and Grep reads only the ones it can match, each by
    ``agrep_rich`` at that file's path, which has to search that file alone."""

    @property
    def root_prefix(self) -> str: ...

    async def aread_text(self, file_path: str) -> str | None: ...

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None: ...

    async def awrite_text(
        self, file_path: str, content: str
    ) -> bool | WriteTextResult: ...

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> EditTextResult: ...

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]: ...

    async def agrep_rich(self, pattern: str, path: str = ".", **kwargs: Any) -> Any: ...


def _grep_entry_path(entry: Any, output_mode: str) -> str | None:
    """The file a grep entry came from, in any output mode; None for a line
    that names none, such as the ``--`` between groups."""
    if output_mode == "count":
        return str(entry[0])
    if output_mode == "content":
        return entry.path if isinstance(entry, GrepLine) else None
    return str(entry)


def _spelled_below(route: FilesystemRoute, rel: str, path: str) -> str:
    """A route's file spelled from the base, its root being ``rel`` there."""
    return posixpath.join(rel, path[len(route.root_prefix) :])


def _page(entries: list[Any], offset: int, head_limit: int | None) -> list[Any]:
    """The slice of ``entries`` a Grep's ``offset`` and ``head_limit`` ask for."""
    start = max(0, offset)
    if head_limit is not None:
        return entries[start : start + head_limit]
    return entries[start:]


async def _matched(match: Callable[[], _T], glob: str | None) -> _T:
    """``match()``, off the event loop for a long filter."""
    if glob is not None and len(glob) > _INLINE_FILTER_CHARS:
        return await asyncio.to_thread(match)
    return match()


class CompositeFilesystemBackend:
    """Rich-method filesystem backend that routes by path prefix."""

    def __init__(
        self,
        *,
        sandbox: SandboxBackend,
        routes: Sequence[FilesystemRoute],
    ) -> None:
        self._sandbox = sandbox
        # Longest prefix wins.
        self._routes: list[FilesystemRoute] = sorted(
            routes, key=lambda b: len(b.root_prefix), reverse=True
        )

    def _escapes_a_mount(self, path: str) -> bool:
        """True if a `..` in `path` steps out of — or into — a mounted route.

        Asked of every route rather than a fixed tier list, so a new mount is
        covered the moment it is registered. Both directions have to be
        checked: the head before each `..` catches a path leaving a mount,
        which the collapse the sandbox normalizer now does would otherwise
        hide, and the collapsed target catches one walking back in, which
        would route to the sandbox FS and shadow the store's copy for good.
        """
        segments = path.split("/")
        if ".." not in segments:
            return False
        for index, segment in enumerate(segments):
            if segment != "..":
                continue
            head = "/".join(segments[:index])
            if head and self._route_for(self._sandbox.normalize_path(head)):
                return True
        collapsed = posixpath.normpath(self._sandbox.normalize_path(path))
        return self._route_for(collapsed) is not None

    def normalize_path(self, path: str) -> str:
        # `..` on a mounted path could normalize into the sandbox FS and
        # silently bypass the store. Reject at the perimeter.
        if self._escapes_a_mount(path):
            raise ValueError("Path traversal (..) is not allowed on mounted paths")
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    @property
    def sandbox(self) -> SandboxBackend:
        return self._sandbox

    def __getattr__(self, name: str) -> Any:
        # Only fires when normal attribute lookup fails, so never shadows
        # methods defined on the composite. Delegates non-filesystem ops
        # (aexecute, skills_manifest, etc.) to the sandbox.
        sandbox = self.__dict__.get("_sandbox")
        if sandbox is None:
            raise AttributeError(name)
        return getattr(sandbox, name)

    def route_for(self, path: str) -> FilesystemRoute | None:
        """The mounted route that owns ``path``, or None where the sandbox does."""
        return self._route_for(self.normalize_path(path))

    def _route_for(self, normalized_path: str) -> FilesystemRoute | None:
        for route in self._routes:
            prefix = route.root_prefix
            if normalized_path.startswith(prefix):
                return route
            if normalized_path.rstrip("/") == prefix.rstrip("/"):
                return route
        return None

    def _spelling(self, path: str) -> str | None:
        """``path`` relative to the turn's folder, as the agent spells it and
        the Glob and Grep tools print it; None where it has no such spelling,
        such as the computer root or another workspace's folder."""
        virtual = self._sandbox.virtualize_path(path)
        return virtual.strip("/") if virtual != path else None

    def _routes_below(self, base: str) -> list[tuple[FilesystemRoute, str]]:
        """Every route a search from ``base`` reaches, each with its root
        spelled relative to ``base``.

        A route is below ``base`` where its root is, and also where the
        agent's spelling of its root is. The user tier is rooted at the
        computer root, but from the turn's folder it is spelled
        ``.agents/user/...``, which is where a read of that name lands, so a
        search from the folder reaches it at that spelling. Nothing else at
        the root is reached that way, nor anything in another folder.
        """
        base = base.rstrip("/")
        base_spelling = self._spelling(base) if base else None
        reached: list[tuple[FilesystemRoute, str]] = []
        for route in self._routes:
            root = route.root_prefix.rstrip("/")
            if not base or root.startswith(base + "/"):
                reached.append((route, root[len(base) :].lstrip("/")))
                continue
            spelled = self._spelling(root) if base_spelling is not None else None
            if spelled is None:
                continue
            if not base_spelling:
                reached.append((route, spelled))
            elif spelled.startswith(base_spelling + "/"):
                reached.append((route, spelled[len(base_spelling) + 1 :]))
        return reached

    def _found_at(self, path: str) -> str:
        """Where the agent finds ``path``: at its spelling in the turn's
        folder, where the file mount links the user tier in, or, where it has
        no spelling there, at ``path`` itself."""
        spelled = self._spelling(path.rstrip("/"))
        if spelled is None:
            return path.rstrip("/")
        return posixpath.join(self._sandbox.workspace_dir, spelled).rstrip("/")

    def _grep_starts(
        self, base: str, below: list[tuple[FilesystemRoute, str]]
    ) -> list[tuple[FilesystemRoute, str, str]]:
        """``below`` with where rg starts the walk to each route, and the
        route's root spelled from there: ``base``, except for a Grep from the
        turn's folder.

        That one also searches the folder's own routes, the ones it spells
        outside the computer tier, as though ``path`` named them beside the
        folder, so it searches this workspace's ``.agents/memory/`` by
        default. Such a route's walk starts at its root, spelled ``""``, and
        only a dot-name below that is skipped. The user tier is the
        computer's, not the folder's, so its walk starts at the folder."""
        start = base.rstrip("/")
        if self._spelling(start) != "":
            return [(route, rel, start) for route, rel in below]
        return [
            (route, rel, start)
            if computer_tier_relative_path(rel) is not None
            else (route, "", self._found_at(route.root_prefix))
            for route, rel in below
        ]

    def _spellings(self, route: FilesystemRoute) -> tuple[str, ...]:
        """Where a route's root is on disk: at the root and, for a route at
        the computer root, at its spelling in the turn's folder, where the
        file mount is linked in too. Its files are files at either."""
        root = route.root_prefix.rstrip("/")
        found = self._found_at(root)
        return (root,) if found == root else (root, found)

    def _aliases(self) -> list[tuple[str, str]]:
        """Each spelling of each route's root, longest route first, with the
        root it stands for: the pairs ``_canonical`` reads, worked out once
        for a whole answer, since each spelling asks the sandbox."""
        return [
            (spelled, route.root_prefix.rstrip("/"))
            for route in self._routes
            for spelled in self._spellings(route)
        ]

    @staticmethod
    def _canonical(path: str, aliases: list[tuple[str, str]]) -> str:
        """``path`` at its route's root where it is a route's file under
        either spelling, as both name the one file; any other path as is."""
        plain = lexical_path(path)
        for spelled, root in aliases:
            if plain == spelled or plain.startswith(spelled + "/"):
                return root + plain[len(spelled) :]
        return path

    def _glob_roots(self, route: FilesystemRoute, wanted: DiskGlob) -> tuple[str, ...]:
        """The spellings of ``route``'s root a Glob walks into: every one,
        but for a route under a linked computer folder only those where the
        walk enters the folder's link."""
        roots = self._spellings(route)
        rel = self._spelling(route.root_prefix.rstrip("/"))
        if rel is None:
            return roots
        for linked in _LINKED_COMPUTER_DIRS:
            if rel == linked or rel.startswith(linked + "/"):
                below = len(rel) - len(linked)
                return tuple(
                    root for root in roots if wanted.enters(root[: len(root) - below])
                )
        return roots

    def _glob_lists(self, route: FilesystemRoute, wanted: DiskGlob) -> bool:
        """Whether Glob could find one of ``route``'s files, without a read
        for a route whose ``fixed_names`` say none can match."""
        roots = self._glob_roots(route, wanted)
        if not any(wanted.reaches(root) for root in roots):
            return False
        names = getattr(route, "fixed_names", None)
        if names is None:
            return True
        return any(wanted.finds(f"{root}/{name}") for root in roots for name in names)

    def _glob_found(
        self, routes: list[FilesystemRoute], listings: list[list[str]], wanted: DiskGlob
    ) -> list[str]:
        """The listed files Glob finds at either spelling of their route."""
        found: list[str] = []
        for route, listing in zip(routes, listings):
            roots = self._glob_roots(route, wanted)
            for path in listing:
                if path.startswith(route.root_prefix):
                    rel = path[len(route.root_prefix) :]
                    if any(wanted.finds(f"{root}/{rel}") for root in roots):
                        found.append(path)
        return found

    @staticmethod
    def _grep_reads(
        route: FilesystemRoute, rel: str, start: str, walk: RgWalk
    ) -> list[str]:
        """Where a walk from ``start`` reads a route whose root it spells
        ``rel``: at its root, or, for a route with ``fixed_names``, at each
        file it searches, so that no other is fetched. Nowhere when it can
        search none of them."""
        if not walk.reaches(start, rel):
            return []
        names = getattr(route, "fixed_names", None)
        if names is None:
            return [route.root_prefix]
        return [
            posixpath.join(route.root_prefix, name)
            for name in sorted(names)
            if walk.finds(start, posixpath.join(rel, name))
        ]

    def _respelled(self, entries: list[Any], output_mode: str) -> list[Any]:
        """The sandbox's grep entries with each route file's path at its
        route's root, however rg reached it."""
        aliases = self._aliases()
        if output_mode == "count":
            return [
                (self._canonical(str(path), aliases), count) for path, count in entries
            ]
        if output_mode != "content":
            return [self._canonical(str(path), aliases) for path in entries]
        canonical: dict[str, str] = {}
        respelled = []
        for line in entries:
            if isinstance(line, GrepLine):
                if line.path not in canonical:
                    canonical[line.path] = self._canonical(line.path, aliases)
                line = line.at(canonical[line.path])
            respelled.append(line)
        return respelled

    async def aread_text(self, file_path: str) -> str | None:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aread_text(normalized)
        return await self._sandbox.aread_text(normalized)

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aread_range(normalized, offset, limit)
        return await self._sandbox.aread_range(normalized, offset, limit)

    async def awrite_text(
        self, file_path: str, content: str
    ) -> bool | WriteTextResult:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.awrite_text(normalized, content)
        return await self._sandbox.awrite_text(normalized, content)

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> EditTextResult:
        normalized = self.normalize_path(file_path)
        route = self._route_for(normalized)
        if route is not None:
            return await route.aedit_text(
                normalized, old_string, new_string, replace_all=replace_all
            )
        return await self._sandbox.aedit_text(
            normalized, old_string, new_string, replace_all=replace_all
        )

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        normalized = self.normalize_path(path)
        route = self._route_for(normalized)
        # Each route is listed whole and matched here, since a route knows
        # neither the base nor the names the agent gives its root. Its files
        # are files at both of their spellings, matched as the sandbox glob
        # matches the disk.
        wanted = DiskGlob(pattern, normalized)
        if route is not None and not wanted.absolute and not wanted.climbs:
            # Below a path inside a route are the route's files alone, so
            # the sandbox is not asked.
            reached = [route] if self._glob_lists(route, wanted) else []
            listings = await asyncio.gather(
                *(r.aglob_paths("*", r.root_prefix) for r in reached)
            )
            return self._glob_found(reached, list(listings), wanted)

        # Above or beside the routes, or with an absolute pattern, which
        # names files wherever they are whatever the path, the sandbox's own
        # hits come first, newest first, each route file at its route's
        # root, and a route adds the files they lack. With a `..`, which the
        # disk alone resolves, they are all there is.
        reached = [r for r in self._routes if self._glob_lists(r, wanted)]
        sandbox_paths, *listings = await asyncio.gather(
            self._sandbox.aglob_paths(pattern, normalized),
            *(r.aglob_paths("*", r.root_prefix) for r in reached),
        )
        aliases = self._aliases()
        found = [self._canonical(p, aliases) for p in sandbox_paths]
        found.extend(self._glob_found(reached, listings, wanted))
        return list(dict.fromkeys(found))

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        normalized = self.normalize_path(path)
        route = self._route_for(normalized)
        # Grep walks as rg walks the disk: the path it is given is searched
        # whatever its name, and below it each entry meets the filter, then
        # the type, then the dot-name rule. rg matches the filter against
        # each path from the directory it runs in, and the walk here starts
        # from the same one, so a filter matches a route's file as rg would
        # match it on disk. A route's files are held to the same rules as the
        # sandbox's, so no route sees the filter or type.
        cwd = grep_working_dir(
            normalized,
            folder=self._sandbox.workspace_dir,
            root=self._sandbox.computer_root,
        )
        try:
            walk: RgWalk | None = RgWalk(glob, type, cwd)
        except RgRefused:
            walk = None
        sandbox_kwargs: dict[str, Any] = dict(
            output_mode=output_mode,
            glob=glob,
            type=type,
            case_insensitive=case_insensitive,
            show_line_numbers=show_line_numbers,
            lines_after=lines_after,
            lines_before=lines_before,
            lines_context=lines_context,
            multiline=multiline,
        )
        if route is not None:
            if walk is None:
                # rg refuses the filter or the type before it searches
                # anything, even a file it is given; its answer is the one
                # a search outside a route gets.
                return await self._sandbox.agrep_rich(
                    pattern,
                    path=normalized,
                    head_limit=head_limit,
                    offset=offset,
                    **sandbox_kwargs,
                )
            base = normalized.rstrip("/")
            # The route's own folder is read as a search from above reads it,
            # at each file the walk can search; a path below it, as given.
            if base == route.root_prefix.rstrip("/"):
                wheres = await _matched(
                    functools.partial(self._grep_reads, route, "", base, walk), glob
                )
            else:
                wheres = [normalized]
            results = await asyncio.gather(
                *(
                    route.agrep_rich(
                        pattern,
                        path=where,
                        output_mode=output_mode,
                        case_insensitive=case_insensitive,
                        show_line_numbers=show_line_numbers,
                        lines_after=lines_after,
                        lines_before=lines_before,
                        lines_context=lines_context,
                        multiline=multiline,
                        head_limit=None,
                        offset=0,
                    )
                    for where in wheres
                )
            )
            for found in results:
                if not isinstance(found, list):
                    return found

            def kept(walk: RgWalk) -> list[Any]:
                # Every line of a file meets the walk as the file does, so
                # each file is asked about once.
                verdicts: dict[str | None, bool] = {None: True}
                kept: list[Any] = []
                for found in results:
                    for entry in found:
                        hit = _grep_entry_path(entry, output_mode)
                        if hit not in verdicts:
                            inside = hit.startswith(base + "/")
                            verdicts[hit] = walk.finds(
                                base, hit[len(base) + 1 :] if inside else ""
                            )
                        if verdicts[hit]:
                            kept.append(entry)
                return kept

            found = await _matched(functools.partial(kept, walk), glob)
            return _page(found, offset, head_limit)

        # Gather all hits first, then apply one global offset/head_limit slice:
        # per-backend pagination would produce the wrong slice at the root.
        def reads(
            starts: list[tuple[FilesystemRoute, str, str]], walk: RgWalk
        ) -> list[tuple[FilesystemRoute, str, str, str]]:
            return [
                (route, rel, start, where)
                for route, rel, start in starts
                for where in self._grep_reads(route, rel, start, walk)
            ]

        searched: list[tuple[FilesystemRoute, str, str, str]] = []
        if walk is not None:
            starts = self._grep_starts(normalized, self._routes_below(normalized))
            searched = await _matched(functools.partial(reads, starts, walk), glob)
        sandbox_result, *route_results = await asyncio.gather(
            self._sandbox.agrep_rich(
                pattern, path=normalized, head_limit=None, offset=0, **sandbox_kwargs
            ),
            *(
                route.agrep_rich(
                    pattern,
                    path=where,
                    output_mode=output_mode,
                    case_insensitive=case_insensitive,
                    show_line_numbers=show_line_numbers,
                    head_limit=None,
                    offset=0,
                )
                for route, _, _, where in searched
            ),
        )

        if not isinstance(sandbox_result, list):
            return sandbox_result

        # rg's own hits stand, a route's file among them however it reached
        # one, and a route adds only the files rg did not list: rg's copy of
        # a file carries what a route's cannot, such as context lines and
        # multiline matches, and stands when a route fails to answer.
        combined = self._respelled(sandbox_result, output_mode)
        rg_files = {_grep_entry_path(entry, output_mode) for entry in combined}

        def added(walk: RgWalk) -> list[Any]:
            verdicts: dict[str, bool] = {}
            added: list[Any] = []
            for (route, rel, start, _), route_result in zip(searched, route_results):
                if not isinstance(route_result, list):
                    continue
                for entry in route_result:
                    hit = _grep_entry_path(entry, output_mode)
                    if hit is None:
                        continue
                    if hit not in verdicts:
                        verdicts[hit] = hit not in rg_files and walk.finds(
                            start, _spelled_below(route, rel, hit)
                        )
                    if verdicts[hit]:
                        added.append(entry)
            return added

        if walk is not None and searched:
            combined.extend(await _matched(functools.partial(added, walk), glob))
        return _page(combined, offset, head_limit)
