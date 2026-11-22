"""A Git repository is backed up whole or not at all.

The manifest mirrors files one by one, and a repository is not a pile of
independent files: a ref names a commit, a commit names trees, and an object
the backup missed makes everything that names it unreadable. A pass that could
not take all of a repository (a file over the cap, an unreadable one, an upload
that failed, git writing as the walk went by) therefore leaves the repository's
rows as the last complete pass recorded them and stores none of this pass's:
an older history restores, half of a newer one does not.

The working tree around the ``.git`` is ordinary files and is backed up either
way, so a held repository costs history, never the files themselves.

Git's locks and half-written files are never stored: no git is running on a
rebuilt computer to finish or remove them, so a restored lock would only make
the next git command refuse to write.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable
from dataclasses import dataclass

from src.server.services.persistence.sync_result import UnsavedFile
from src.server.services.persistence.transfer import GIT_DIR, GIT_HISTORY_MAX_BYTES, ScanEntry

#: How long a lock or temporary file may sit untouched and still mean git is
#: writing. A git that is killed (a command timeout, a stop mid-clone) leaves
#: them behind for good, and one counted forever would hold its repository
#: and refuse every teardown; a live writer touches its files as it goes.
_WRITING_FRESH_NS = 15 * 60 * 1_000_000_000


def repo_of(path: str) -> str | None:
    """The ``.git`` directory ``path`` is in, or is, or None outside every one.

    A submodule's history lives under its parent's ``.git/modules/``, so the
    first ``.git`` on the path is the repository that owns it.
    """
    parts = path.split("/")
    try:
        cut = parts.index(GIT_DIR)
    except ValueError:
        return None
    return "/".join(parts[: cut + 1])


#: An object store's half-written files: a push's quarantine, a loose object
#: or a pack being written, a repack's new pack. In a submodule's too.
_GIT_TEMP = re.compile(r"(?:^|/)objects/(?:incoming-|(?:[0-9a-f]{2}|pack)/(?:tmp_|\.tmp-))")


def _git_is_writing(rel: str) -> bool:
    """A lock git holds while it rewrites a ref, the index or a pack, a file
    still being written, or a running ``gc``, by its path inside the
    ``.git``. A ref may not end in ``.lock``, so that suffix is git's
    anywhere; the rest are names a branch could take, so not under refs (a
    ``refs`` straight under ``modules/`` is a submodule's name)."""
    parts = rel.split("/")
    if parts[-1].endswith(".lock"):
        return True
    if any(p == "refs" and (i == 0 or parts[i - 1] != "modules") for i, p in enumerate(parts)):
        return False
    return parts[-1] == "gc.pid" or _GIT_TEMP.search(rel) is not None


def is_transient(path: str) -> bool:
    """Whether ``path`` is one of git's locks or half-written files."""
    repo = repo_of(path)
    return repo is not None and path != repo and _git_is_writing(path[len(repo) + 1 :])


@dataclass(frozen=True, slots=True)
class RepoHold:
    """The repositories a pass stores nothing of, and its unsaved list as
    they leave it: one line per repository, saying why."""

    #: Every held repository: none of this pass's entries are stored.
    repos: frozenset[str]
    #: Those whose last complete backup stands. The rest are too large to
    #: carry, so their rows go with the next prune and a restore brings back
    #: no history, rather than an old one beside a newer working tree.
    kept: frozenset[str]
    unsaved: list[UnsavedFile]

    #: Git's locks and half-written files, in any repository.
    transient: frozenset[str] = frozenset()

    def covers(self, path: str) -> bool:
        repo = repo_of(path)
        return repo is not None and repo in self.repos

    def stores(self, path: str) -> bool:
        return not self.covers(path) and path not in self.transient

    def rows_of(self, existing: Iterable[str]) -> set[str]:
        """The manifest rows the prune must keep: the last complete history."""
        if not self.kept:
            return set()
        return {path for path in existing if repo_of(path) in self.kept}


_TOO_LARGE = frozenset({"too_large", "history_too_large"})


@dataclass(slots=True)
class _Repo:
    size: int = 0
    writing: bool = False
    too_large: bool = False
    unsaved: bool = False


def hold_repos(
    unsaved: list[UnsavedFile],
    entries: Iterable[ScanEntry] = (),
    *,
    now_ns: int | None = None,
    hold_all: bool = False,
    pending: Collection[str] = frozenset(),
    max_bytes: int = GIT_HISTORY_MAX_BYTES,
) -> RepoHold:
    """Which repositories a pass must not store, given what it could not take.

    A repository git is writing to is held until it settles, ahead of
    anything else: a repack holds the old packs and the new one at once, and
    read as too large it dropped the last complete history. A history too
    large for its limit (sized by the scan without reading it, or by what it
    lists) or holding a file the transfer path cannot carry was never
    promised: its line replaces the too-large files' own, which would
    otherwise read as at risk and refuse each teardown. Any other file left
    behind holds its repository and keeps its own line, whose reason says
    whether a later pass can save it. A pass that cannot prune holds every
    repository, and a ``pending`` one, whose rows a restore has yet to bring
    back, is held too, since writing its new files beside the rows of the
    ones git deleted stores two states as one. Called with the scan's ``entries``
    (and git's files the scan saw only by name) before the transfer, and
    with only the transfer's failures after it.
    """
    repos: dict[str, _Repo] = {}
    transient: set[str] = set()
    for entry in entries:
        repo = repo_of(entry.path)
        # A ``.git`` file is a worktree's pointer to its history, not one.
        if repo is None or (entry.path == repo and entry.kind != "dir"):
            continue
        state = repos.setdefault(repo, _Repo())
        if not _git_is_writing(entry.path[len(repo) + 1 :]):
            if entry.kind == "file":
                state.size += entry.size
            continue
        transient.add(entry.path)
        if entry.kind == "file":
            fresh = now_ns is None or entry.mtime_ns >= now_ns - _WRITING_FRESH_NS
            state.writing = state.writing or fresh
    for f in unsaved:
        repo = repo_of(f.path)
        if repo is None:
            continue
        state = repos.setdefault(repo, _Repo())
        if f.reason in _TOO_LARGE:
            state.size += f.size or 0
            state.too_large = True
        elif f.reason == "changed":
            # A file that vanished as the scan reached it: git at work, as
            # much as a fresh lock is, a repack above all.
            state.writing = True
        else:
            state.unsaved = True

    lines: dict[str, UnsavedFile] = {}
    held: set[str] = set()
    for repo, state in repos.items():
        if state.writing:
            lines[repo] = UnsavedFile(repo, "changed")
        elif state.too_large or state.size > max_bytes:
            lines[repo] = UnsavedFile(repo, "history_too_large", state.size)
        elif state.unsaved:
            lines[repo] = UnsavedFile(repo, "history_held")
        elif not (hold_all or repo in pending):
            continue
        held.add(repo)
    too_large = {repo for repo, line in lines.items() if line.reason == "history_too_large"}
    return RepoHold(
        repos=frozenset(held),
        kept=frozenset(held - too_large),
        transient=frozenset(transient),
        unsaved=[
            f for f in unsaved if not (f.reason in _TOO_LARGE and repo_of(f.path) in lines)
        ]
        + list(lines.values()),
    )
