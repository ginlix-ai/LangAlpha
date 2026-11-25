"""The computer's server-held folders, and how an agent's spelling of a path
folds onto the file it names.

Each server-held folder is linked into the sandbox by a mount, so the lists
the mounts, backups and file routes read are derived from one table here, and
the fold (``resolve_agent_path`` and its inverse) reads the same lists to keep
a folder's spelling of a computer folder on the computer's. ``paths`` re-exports
every public name, so importers spell them from either module.
"""

from __future__ import annotations

import posixpath
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from ptc_agent.core.paths import (
    COMPUTER_ROOT_ENTRIES,
    SandboxLayout,
    WorkspaceLayout,
    lexical_path,
    strip_previous_dir_name,
)

# The folders whose files the server holds, each linked in by one of the
# computer's two mounts: the file mount (``files``) or the Drive's own
# (``drive``). Workspace-relative: every workspace folder holds all of them. A
# folder ``at_root`` is the computer's own, linked at the root, with every
# workspace folder's link leading to the root's, so the path fold reads a
# folder's spelling of it as the computer's; the others belong to each
# workspace. The server is their only copy: a backup never reads them and a
# restore never writes them, including rows an older manifest recorded before
# they were mounted. Every list below is read off this one.
@dataclass(frozen=True)
class ServerHeldFolder:
    path: str
    at_root: bool
    mount: Literal["files", "drive"]


SERVER_HELD_FOLDERS: tuple[ServerHeldFolder, ...] = (
    ServerHeldFolder(SandboxLayout.USER_DIR, at_root=True, mount="files"),
    ServerHeldFolder(SandboxLayout.WORKFLOWS_DIR, at_root=True, mount="files"),
    ServerHeldFolder(WorkspaceLayout.MEMORY_DIR, at_root=False, mount="files"),
    ServerHeldFolder(WorkspaceLayout.TRANSCRIPTS_DIR, at_root=False, mount="files"),
    ServerHeldFolder(SandboxLayout.DRIVE_DIR, at_root=True, mount="drive"),
)

# Where the file mount links the user's server-held files in.
MOUNTED_AGENT_SUBDIRS: tuple[str, ...] = tuple(
    f.path for f in SERVER_HELD_FOLDERS if f.mount == "files"
)

# The computer's index of past threads, which the file mount links in at the
# root alone and writes as it is read.
THREAD_INDEX: str = f"{SandboxLayout.AGENTS_DIR}/threads.jsonl"

# What the file mount links in at the computer root, besides the thread index;
# the per-workspace subtrees join them only where an unsplit workspace owns
# the root.
ROOT_MOUNTED_AGENT_SUBDIRS: tuple[str, ...] = tuple(
    f.path for f in SERVER_HELD_FOLDERS if f.mount == "files" and f.at_root
)

# Every computer-root folder a workspace folder links to the root's, whichever
# mount serves it.
FOLDER_LINKED_AGENT_SUBDIRS: tuple[str, ...] = tuple(
    f.path for f in SERVER_HELD_FOLDERS if f.at_root
)


def _under_any(relative: str, linked: tuple[str, ...]) -> bool:
    return any(relative == d or relative.startswith(f"{d}/") for d in linked)


def on_file_mount(path: str, workspace: WorkspaceLayout) -> bool:
    """Whether ``path``, absolute in the sandbox, is one the file mount serves
    while it runs, for a turn in ``workspace``: under one of its links at the
    computer root or in a workspace folder."""
    relative = posixpath.relpath(path, workspace.root)
    if relative == ".." or relative.startswith("../"):
        return False
    if relative == THREAD_INDEX:
        return True
    at_root = (
        MOUNTED_AGENT_SUBDIRS if not workspace.dir_name else ROOT_MOUNTED_AGENT_SUBDIRS
    )
    return _under_any(relative, at_root) or _under_any(
        relative.partition("/")[2], MOUNTED_AGENT_SUBDIRS
    )


# Exclude ephemeral agent data from backup. Both tiers appear because the
# names are matched workspace-relative and the two tiers can share a folder.
# The tool package is here because every byte of it (wrappers, docs, config)
# is re-emitted by the MCP sync into whatever sandbox the restore lands in.
BACKUP_EXCLUDE_AGENT_SUBDIRS: tuple[str, ...] = (
    WorkspaceLayout.THREADS_DIR,
    WorkspaceLayout.TOOLS_DIR,
    *(f.path for f in SERVER_HELD_FOLDERS),
)

# Fold shared, store-backed user mounts onto the computer root or writes
# bypass the store. Workspace skills, memory, tools and thread scratch are
# excluded because they belong to the project folder. ``.agents/tmp`` is the
# computer's only from a relative spelling: no link puts it in a folder, so a
# folder's is a real directory of its own.
COMPUTER_AGENT_SUBTREES: tuple[str, ...] = (
    *FOLDER_LINKED_AGENT_SUBDIRS,
    SandboxLayout.TMP_DIR,
)


def computer_tier_relative_path(path: str | None) -> str | None:
    """Accept mount-relative spellings; an absolute path in a workspace folder
    is folded by :func:`_folder_link_to_root`."""
    candidate = (path or "").strip()
    if candidate.startswith("/"):
        candidate = candidate.lstrip("/")
    while candidate.startswith("./"):
        candidate = candidate[2:]
    for subtree in COMPUTER_AGENT_SUBTREES:
        if candidate == subtree or candidate.startswith(f"{subtree}/"):
            return candidate
    return None


def _folder_link_to_root(
    path: str,
    *,
    workspace: str,
    root: str,
    siblings: Sequence[str] | None,
    previous: Sequence[str] = (),
) -> str | None:
    """A folder's link into the computer tier, spelled at the root it names.

    The store-backed routes are keyed on the root-anchored prefix. Left as
    written, ``/home/workspace/Home/.agents/user/profile/portfolio.json``
    misses them and reaches the sandbox, where the link leads into the file
    mount and the mount refuses the write's temporary file beside the target.
    Only a folder the turn knows is linked: its own, a sibling's, or the one it
    was renamed out of.
    """
    root = root.rstrip("/")
    if workspace == root or not path.startswith(f"{root}/"):
        return None
    relative = path[len(root) + 1 :]
    first, _, rest = relative.partition("/")
    known = (
        first == posixpath.basename(workspace)
        or first in (siblings or ())
        or strip_previous_dir_name(relative, previous) is not None
    )
    return f"{root}/{rest}" if known and _under_any(rest, FOLDER_LINKED_AGENT_SUBDIRS) else None


def _root_entry_in_folder(
    path: str,
    *,
    workspace: str,
    root: str,
    siblings: Sequence[str] | None,
    previous: Sequence[str] = (),
) -> str | None:
    """A loose root entry re-read as this project's own file, or None to keep the path.

    The computer root's real entries are the reserved tiers and one folder per
    project, so an absolute root path naming anything else is a pre-split
    spelling of this project's file: a turn resumed from before the folder
    existed, or a link stored when the workspace was the whole machine. Left at
    the root the write lands beside the folders, where no route serves it and
    no mirror scans it. A folder the workspace was renamed out of (``previous``)
    names the current folder, so its entry is dropped rather than nested. A
    sibling's folder is spelled as itself, since a turn may write there on
    purpose, which is why ``siblings`` of None -- a caller that cannot
    enumerate the machine's folders -- leaves every path alone rather than
    reading a sibling's as its own.
    """
    if siblings is None or workspace == root or not path.startswith(f"{root}/"):
        return None
    relative = path[len(root) + 1 :]
    first = relative.split("/", 1)[0]
    if (
        not first
        or first in COMPUTER_ROOT_ENTRIES
        or first in siblings
        or first == posixpath.basename(workspace)
    ):
        return None
    inside = strip_previous_dir_name(relative, previous)
    return lexical_path(f"{workspace}/{relative if inside is None else inside}")


def resolve_agent_path(
    path: str | None,
    *,
    workspace: str,
    root: str,
    allowed: Sequence[str],
    sibling_dir_names: Sequence[str] | None = None,
    previous_dir_names: Sequence[str] = (),
) -> str:
    """Fold an agent's spelling of a path onto the directory it names.

    The one funnel every agent path reaches the sandbox and the validator
    through, so every result is collapsed (``lexical_path``): a ``..`` left in
    place would name a sibling's folder while still passing a prefix test
    against the computer root. ``workspace`` is the turn's folder and ``root``
    the computer; the split matters because the store-backed user mounts are
    keyed on the root-anchored prefix, so a computer-tier name folded into the
    folder would miss every route and write a real file the store never sees.
    An absolute path that lands on an unowned root entry, or in a folder the
    workspace was renamed out of, folds too, per :func:`_root_entry_in_folder`.
    """
    if path in (None, "", ".", "/"):
        return workspace
    assert path is not None
    path = path.strip()
    for allowed_dir in allowed:
        if path == allowed_dir or path.startswith(allowed_dir.rstrip("/") + "/"):
            collapsed = lexical_path(path)
            return (
                _folder_link_to_root(
                    collapsed,
                    workspace=workspace,
                    root=root,
                    siblings=sibling_dir_names,
                    previous=previous_dir_names,
                )
                or _root_entry_in_folder(
                    collapsed,
                    workspace=workspace,
                    root=root,
                    siblings=sibling_dir_names,
                    previous=previous_dir_names,
                )
                or collapsed
            )
    computer_relative = computer_tier_relative_path(path)
    if computer_relative is not None:
        return lexical_path(f"{root}/{computer_relative}")
    # A spelling that reaches the computer tier only once collapsed
    # (``data/../.agents/user/...``) folds as its absolute spelling does.
    collapsed = lexical_path(f"{workspace}{path}" if path.startswith("/") else f"{workspace}/{path}")
    return (
        _folder_link_to_root(
            collapsed,
            workspace=workspace,
            root=root,
            siblings=sibling_dir_names,
            previous=previous_dir_names,
        )
        or collapsed
    )


def machine_path(path: str | None, *, base: str, home: str | None = None) -> str:
    """A path as the machine spells it: absolute as written, relative from
    ``base``. For a call with no virtual namespace, where folding ``/x`` under
    a folder would quietly name another file than the one the agent meant.

    ``~`` and ``~/x`` name ``home``, the login user's on that machine. Without
    one, and for ``~user``, the path is left as written, which no allow list
    holds.
    """
    candidate = (path or "").strip()
    if candidate in ("", "."):
        return base
    if home and (candidate == "~" or candidate.startswith("~/")):
        return lexical_path(f"{home.rstrip('/')}/{candidate[2:]}")
    if candidate.startswith(("/", "~")):
        return lexical_path(candidate)
    return lexical_path(f"{base}/{candidate}")


def virtual_agent_path(path: str, *, workspace: str, root: str) -> str:
    """The inverse of :func:`resolve_agent_path`, where one exists.

    A path only gets a virtual spelling when that spelling folds back onto the
    same file. The virtual namespace is anchored on the turn's folder, so the
    two that qualify are a file inside the folder and a computer-tier name.
    Everything else (a sibling's folder, the root itself, ``/tmp``) keeps its
    absolute path, which is already a spelling both directions agree on.
    """
    path = lexical_path(path)
    if path == workspace:
        return "/"
    if path.startswith(workspace + "/"):
        virtual = path[len(workspace) :]
        if workspace != root and computer_tier_relative_path(virtual) is not None:
            # A real file left inside the folder under a computer-tier name has
            # no virtual spelling of its own: that spelling belongs to the
            # mount at the root.
            return path
        return virtual
    if path.startswith(root + "/"):
        virtual = path[len(root) :]
        if computer_tier_relative_path(virtual) is not None:
            return virtual
    return path
