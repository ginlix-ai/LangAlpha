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


def find(
    pattern: str,
    search_path: str,
    excluded_dirs: list[str],
    history_children: dict[str, list[str]],
    mount: str,
) -> list[str]:
    """Files matching ``pattern`` below ``search_path``, newest first.

    Matches as ``glob.glob(recursive=True, include_hidden=True)`` does, with
    one walker of its own so that nothing a wildcard reaches below the
    pattern's literal prefix is a link into ``mount``, directly or through
    other links: those are server-held files, where every stat is a request,
    so a walk skips them unread (a readlink is local). A literal prefix that
    names one is still followed.
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
        entries: list[os.DirEntry] = []
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

    files = [path for path in candidates if kept(path)]
    try:
        return sorted(files, key=os.path.getmtime, reverse=True)
    except OSError:
        return files


if __name__ == "__main__":
    for path in find(**json.loads(sys.argv[1])):
        print(path)
