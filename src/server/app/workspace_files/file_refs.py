"""Resolve a file reference the agent wrote to one workspace file.

The agent names files loosely: from the workspace root, from the directory of
the document holding the link, by bare name, or at a path it has since moved.
Resolution matches the reference's file name against the real file list, so a
click opens the file the agent meant, or reports that the choice is ambiguous
or that nothing by that name exists, instead of guessing from a stale listing.
"""

from __future__ import annotations

import posixpath
import re
from typing import Annotated, Any

from pydantic import BaseModel, Field, StringConstraints

from ._shared import (
    _is_always_hidden_path,
    _is_hidden_path,
    _is_system_path,
    workspace_relative_path,
)

_GLOB_SPECIAL_RE = re.compile(r"([*?\[])")

# A reference is a workspace path, and the longest a filesystem will hold is
# well under this. The bound is on the item because the shared-thread route is
# the one body-accepting endpoint on the unauthenticated router: FastAPI parses
# the body while solving dependencies, before the share token is looked up, so
# a list capped only in length still buffers whatever each entry weighs.
_RefPath = Annotated[str, StringConstraints(max_length=1024)]


class ResolveFileRefRequest(BaseModel):
    candidates: list[_RefPath] = Field(
        ...,
        min_length=1,
        max_length=4,
        description="Readings of one reference, most likely first. All share a file name.",
    )
    recent_writes: list[_RefPath] = Field(
        default_factory=list,
        max_length=200,
        description="Paths this thread wrote or edited, newest first; break ties between namesakes.",
    )


def clean_path(value: str, work_dir: str) -> str | None:
    """A workspace-relative path, or None for one that names no workspace file.

    ``work_dir`` is the workspace's own folder, never the computer root it sits
    on: several workspaces share that root, and a reference relative to it
    would name a sibling's file. A reference that spells the root anyway is the
    older spelling of a file that now lives in the folder, and
    ``workspace_relative_path`` folds it there.

    Four call sites read the result as workspace-relative, so a path still
    absolute after the fold is refused here rather than handed on to a glob
    that would search for it under the workspace anyway.
    """
    path = workspace_relative_path(value, work_dir)
    if not path or path.startswith("/") or ".." in path.split("/"):
        return None
    # A ``file:`` URL can spell a NUL percent-encoded, and the result of this
    # function becomes a glob the sandbox runs, so it is refused here too.
    if "\x00" in path:
        return None
    return path


def clean_candidates(raw: list[str], work_dir: str) -> list[str]:
    """Distinct cleaned candidates that share the first one's file name."""
    out = list(dict.fromkeys(p for p in (clean_path(v, work_dir) for v in raw) if p))
    if not out:
        return out
    name = posixpath.basename(out[0])
    return [p for p in out if posixpath.basename(p) == name]


def name_glob(name: str) -> str:
    """A glob matching exactly this file name at any depth."""
    return "**/" + _GLOB_SPECIAL_RE.sub(r"[\1]", name)


def visible_paths(paths: list[str], candidates: list[str]) -> list[str]:
    """Drop paths the file panel never shows, unless the reference points into one."""
    wants_hidden = any(_is_hidden_path(c) for c in candidates)
    return [
        p
        for p in paths
        if not _is_always_hidden_path(p) and (wants_hidden or not _is_hidden_path(p))
    ]


def resolve_file_ref(
    candidates: list[str],
    paths: list[str],
    recent_writes: list[str] | None = None,
) -> dict[str, Any]:
    """Pick the file a reference names from every path carrying its file name.

    Precedence: an exact candidate; then the single best namesake, where a path
    ending in a candidate that has a directory beats a bare namesake and a work
    file beats a system-directory file (unless the reference points there);
    then this thread's newest write among equally good namesakes. Anything else
    is ambiguous, and the ranked matches go back for the user to choose.
    """
    if not candidates:
        return {"status": "missing", "matches": []}
    name = posixpath.basename(candidates[0])
    hits = list(dict.fromkeys(p for p in paths if posixpath.basename(p) == name))

    for candidate in candidates:
        if candidate in hits:
            return {"status": "resolved", "path": candidate, "match": "exact", "matches": [candidate]}
    if not hits:
        return {"status": "missing", "matches": []}

    wants_system = any(_is_system_path(c) for c in candidates)

    def carries_candidate(path: str) -> bool:
        return any("/" in c and path.endswith(f"/{c}") for c in candidates)

    def rank(path: str) -> tuple[int, int]:
        return (
            0 if carries_candidate(path) else 1,
            1 if not wants_system and _is_system_path(path) else 0,
        )

    ranked = sorted(hits, key=lambda p: (rank(p), len(p), p))
    best = [p for p in ranked if rank(p) == rank(ranked[0])]
    match = "suffix" if rank(ranked[0])[0] == 0 else "name"

    if len(best) == 1:
        return {"status": "resolved", "path": best[0], "match": match, "matches": ranked}
    for written in recent_writes or []:
        if written in best:
            return {"status": "resolved", "path": written, "match": "recent_write", "matches": ranked}
    return {"status": "ambiguous", "matches": ranked}
