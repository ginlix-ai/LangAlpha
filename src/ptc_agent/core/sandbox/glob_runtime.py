"""The glob traversal ``aglob_files`` runs inside the sandbox.

Sent whole with each call through ``python3 -I -c`` rather than shipped with
the asset sync, so a glob never depends on what a warm sandbox last synced and
always matches the host that built its arguments. Standard library only: the
interpreter runs isolated, so a workspace's own ``glob.py`` cannot shadow it.
"""

import fnmatch
import json
import os
import sys
from collections.abc import Iterable, Iterator


# The kernel's own limit: a longer chain fails to resolve, so no walk that
# gives up here would have reached the mount either.
_MAX_LINK_HOPS = 40


def _magic(segment: str) -> bool:
    return any(c in segment for c in "*?[")


def agent_glob_pattern(pattern: str) -> str:
    """The pattern the agent's Glob walks for ``pattern``. A bare name looks
    at every depth, so the server, matching a store's files for the same
    Glob, has to read it the same way."""
    if "**" not in pattern and "/" not in pattern:
        return f"**/{pattern}"
    return pattern


# Each pattern a Glob's braces expand to is walked on its own, and a few
# groups multiply past any search a person means.
MAX_BRACE_PATTERNS = 64


class TooManyPatterns(ValueError):
    """A pattern whose brace groups expand past ``MAX_BRACE_PATTERNS``."""


def _next(pattern: str, char: str, start: int) -> int:
    at = pattern.find(char, start)
    return len(pattern) if at < 0 else at


def _brace_syntax(pattern: str) -> Iterator[tuple[int, str]]:
    """Each ``{``, ``,`` and ``}`` brace expansion reads in ``pattern``, and
    each backslash escaping one. Inside a ``[...]`` class they are the
    class's own, as ripgrep reads them, so ``[{]`` is a literal brace."""
    # The first `]` and `/` past where the last class could start: each only
    # moves forward, so a run of open `[` costs one pass, not one each.
    close = slash = -1
    at = 0
    while at < len(pattern):
        char = pattern[at]
        if char == "\\" and pattern[at + 1 : at + 2] in ("{", "}", ","):
            yield at, char
            at += 2
            continue
        if char == "[":
            # As fnmatch reads a class: a leading `!` and then a leading `]`
            # are its own, and with no `]` before the name ends the `[` is a
            # plain character.
            start = at + 1
            if pattern[start : start + 1] == "!":
                start += 1
            if pattern[start : start + 1] == "]":
                start += 1
            if close < start:
                close = _next(pattern, "]", start)
            if slash < start:
                slash = _next(pattern, "/", start)
            if close < slash:
                at = close + 1
                continue
        if char in "{},":
            yield at, char
        at += 1


def _first_brace_group(pattern: str) -> list[int] | None:
    """The first group in ``pattern``: its ``{``, each comma of its own, and
    its ``}``. As in a shell, a ``{`` nothing closes, or whose braces hold
    no comma of their own, is a plain character."""
    open_groups: list[list[int]] = []
    first: list[int] | None = None
    for at, char in _brace_syntax(pattern):
        if char == "{":
            open_groups.append([at])
        elif char == "," and open_groups:
            open_groups[-1].append(at)
        elif char == "}" and open_groups:
            bounds = open_groups.pop()
            if len(bounds) > 1 and (first is None or bounds[0] < first[0]):
                first = [*bounds, at]
    return first


def _expand(pattern: str) -> list[str]:
    bounds = _first_brace_group(pattern)
    if bounds is None:
        return [pattern]
    head = pattern[: bounds[0]]
    tails = _expand(pattern[bounds[-1] + 1 :])
    out: list[str] = []
    for start, end in zip(bounds, bounds[1:]):
        for middle in _expand(pattern[start + 1 : end]):
            for tail in tails:
                out.append(head + middle + tail)
                if len(out) > MAX_BRACE_PATTERNS:
                    raise TooManyPatterns(
                        f"pattern expands to more than {MAX_BRACE_PATTERNS} patterns"
                    )
    return out


def expand_braces(pattern: str) -> list[str]:
    """The patterns ``pattern``'s ``{a,b}`` groups expand to, in order and
    each once, as a shell expands them: groups nest, and a backslash before
    a ``{``, ``,`` or ``}`` makes it literal. Raises ``TooManyPatterns`` past
    ``MAX_BRACE_PATTERNS``."""
    expanded = []
    for each in _expand(pattern):
        escapes = {at for at, char in _brace_syntax(each) if char == "\\"}
        expanded.append("".join(c for at, c in enumerate(each) if at not in escapes))
    # A shell drops an alternative that leaves nothing, as in `{a.md,}`;
    # kept, it would walk the whole tree to match no file.
    return list(dict.fromkeys(each for each in expanded if each or not pattern))


def find(
    pattern: str,
    search_path: str,
    excluded_dirs: list[str],
    history_children: dict[str, list[str]],
    mount: str,
) -> list[str]:
    """Files matching ``pattern`` below ``search_path``, newest first.

    Each pattern its brace groups expand to is matched on its own, as a shell
    globs each word a brace expands to, and a file several of them find is
    listed once. Their walks share each folder's listing.
    """
    patterns = list(dict.fromkeys(agent_glob_pattern(p) for p in expand_braces(pattern)))
    listings: dict[str, list[os.DirEntry]] | None = {} if len(patterns) > 1 else None
    # Two patterns can spell one file two ways, as `{a.md,./a.md}` does, so
    # a file is known by its normalized path and listed at its first spelling.
    files: dict[str, str] = {}
    for each in patterns:
        for path in _find(
            each, search_path, excluded_dirs, history_children, mount, listings
        ):
            files.setdefault(os.path.normpath(path), path)
    return sorted(files.values(), key=_mtime, reverse=True)


def _mtime(path: str) -> float:
    # A file removed since the walk found it sorts last, not the rest unsorted.
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def _find(
    pattern: str,
    search_path: str,
    excluded_dirs: list[str],
    history_children: dict[str, list[str]],
    mount: str,
    listings: dict[str, list[os.DirEntry]] | None,
) -> list[str]:
    """Files matching ``pattern``, which holds no brace group, below
    ``search_path``.

    Matches as ``glob.glob(recursive=True, include_hidden=True)`` does, with
    one walker of its own so that nothing a wildcard reaches below the
    pattern's literal prefix is a link into ``mount``, directly or through
    other links: those are server-held files, where every stat is a request,
    so a walk skips them unread (a readlink is local). A literal prefix that
    names one is still followed. ``listings`` keeps each folder's listing
    for the next pattern's walk.
    """
    excluded = set(excluded_dirs)
    history = {parent: set(names) for parent, names in history_children.items()}
    # ``mount`` is itself a link to the live mount under its hidden sibling,
    # where every earlier generation stays mounted too.
    mounts = (mount, os.path.join(os.path.dirname(mount), "." + os.path.basename(mount)))

    def history_hidden(parent: str, name: str) -> bool:
        return name in history.get(parent, ())

    def into_mount(path: str) -> bool:
        # Hop by readlink alone, stopping at the first target in the mount,
        # since resolving one there is a request.
        for _ in range(_MAX_LINK_HOPS):
            try:
                target = os.readlink(path)
            except OSError:
                return False
            path = os.path.normpath(os.path.join(os.path.dirname(path), target))
            if any(path == m or path.startswith(m + "/") for m in mounts):
                return True
        return False

    def listing(dirpath: str) -> list[os.DirEntry]:
        if listings is not None and dirpath in listings:
            return listings[dirpath]
        entries: list[os.DirEntry] = []
        if listings is not None:
            listings[dirpath] = entries
        try:
            with os.scandir(dirpath or ".") as it:
                for entry in it:
                    try:
                        if entry.is_symlink() and into_mount(os.path.join(dirpath, entry.name)):
                            continue
                    except OSError:
                        continue
                    entries.append(entry)
        except OSError:
            pass
        return entries

    def is_dir(entry: os.DirEntry) -> bool:
        try:
            return entry.is_dir()
        except OSError:
            return False

    # A recursive name search ('**/*.py', and the file panel's '**/*') stays
    # out of linked directories, which may lead anywhere on the machine; a
    # pattern with path structure follows them, as glob does.
    tail = pattern[3:] if pattern.startswith("**/") else None
    follow_links = tail is None or "/" in tail or "**" in tail

    # Excluded dirs are judged below the search root, never on the root itself
    # (so globbing directly into an excluded dir still works) and never on the
    # basename (so a regular file that shares a noise-dir name is not dropped).
    # A history dir the pattern spells out literally was asked for, so history
    # is judged below the pattern's literal prefix rather than below the search
    # root. The root is literal whatever its name holds.
    literal = [] if os.path.isabs(pattern) else [search_path.rstrip("/")]
    for seg in pattern.split("/")[:-1]:
        if _magic(seg):
            break
        literal.append(seg)
    history_root = "/".join(literal) or "/"

    def kept(path: str) -> bool:
        if not os.path.isfile(path):
            return False
        inner_dirs = os.path.relpath(path, search_path).split(os.sep)[:-1]
        if set(inner_dirs) & excluded:
            return False
        if history:
            rel_dir = os.path.relpath(os.path.dirname(path), history_root)
            below = [] if rel_dir == "." else rel_dir.split(os.sep)
            parents = [os.path.basename(history_root), *below[:-1]]
            if any(history_hidden(p, d) for p, d in zip(parents, below)):
                return False
        return True

    candidates: Iterable[str]
    full = os.path.join(search_path, pattern)
    segments = full.split("/")
    rooted = 0 if os.path.isabs(pattern) else len(search_path.rstrip("/").split("/"))
    first = next((i for i in range(rooted, len(segments)) if _magic(segments[i])), None)
    if first is None:
        candidates = [full] if os.path.lexists(full) else []
    else:
        # Split where glob splits: the literal dir as os.path.split leaves it,
        # then the segments below it, a trailing '' meaning directories only.
        head = "/".join(segments[:first]) + "/" if first else ""
        root = head.rstrip("/") or head
        below_root = [seg for seg in segments[first:-1] if seg] + segments[-1:]

        # ``kept`` decides; pruning while walking only skips directories whose
        # every file ``kept`` would drop. That holds while each step down adds
        # one plain name, so the dirs below the root read as the names walked.
        prunable = not any(seg in (".", "..") for seg in below_root)
        history_prunable = prunable and os.path.relpath(root or ".", history_root) == "."
        search_root = os.path.normpath(search_path)
        root_chain = os.path.basename(history_root)

        def pruned(parent: str, name: str, path: str) -> bool:
            if history_prunable and history_hidden(parent, name):
                return True
            if not prunable or name not in excluded:
                return False
            # Judged below the search root, so never on a dir holding it.
            path = os.path.normpath(path)
            return path != search_root and not search_root.startswith(path + "/")

        def present(dirpath: str, name: str, entries: list[os.DirEntry] | None) -> bool:
            if entries is not None and name not in (".", ".."):
                return any(entry.name == name for entry in entries)
            path = os.path.join(dirpath, name)
            return not into_mount(path) and os.path.lexists(path)

        def walk(
            dirpath: str,
            chain: str,
            segs: list[str],
            entries: list[os.DirEntry] | None = None,
        ) -> Iterator[str]:
            """Paths below ``dirpath`` that ``segs`` match, in glob's order.
            ``chain`` is ``dirpath``'s name as the history rules read it."""
            seg, rest = segs[0], segs[1:]
            if seg == "**":
                if entries is None:
                    entries = listing(dirpath)
                if rest:
                    yield from walk(dirpath, chain, rest, entries)
                for entry in entries:
                    path = os.path.join(dirpath, entry.name)
                    if not rest:
                        yield path
                    if (
                        (follow_links or not entry.is_symlink())
                        and is_dir(entry)
                        and not pruned(chain, entry.name, path)
                    ):
                        yield from walk(path, entry.name, segs)
            elif _magic(seg):
                if entries is None:
                    entries = listing(dirpath)
                for entry in entries:
                    if not fnmatch.fnmatchcase(entry.name, seg):
                        continue
                    path = os.path.join(dirpath, entry.name)
                    if not rest:
                        yield path
                    elif is_dir(entry) and not pruned(chain, entry.name, path):
                        yield from walk(path, entry.name, rest)
            elif seg and present(dirpath, seg, entries):
                path = os.path.join(dirpath, seg)
                if not rest:
                    yield path
                elif not pruned(chain, seg, path):
                    yield from walk(path, seg, rest)

        candidates = walk(root, root_chain, below_root)

    return [path for path in candidates if kept(path)]


if __name__ == "__main__":
    for path in find(**json.loads(sys.argv[1])):
        print(path)
