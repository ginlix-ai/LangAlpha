"""Where a snapshot reads a workspace's files: the live tree of a running
computer, or the copy stored for one that is not, as its ``ReadMode`` allows.
"""

from __future__ import annotations

import shlex
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.persistence.transfer import EXCLUDE_DIR_NAMES
from src.server.services.served_paths import may_be_artifact
from src.server.services.workspace_file_containment import (
    contained_absolute_path,
    contained_sandbox_paths,
    FileTooLargeToServe,
    is_within,
)
from src.server.services.warm_files import (
    db_fallback_bytes,
    warm_sandbox,
    warm_sandbox_bytes,
)
from src.server.services.workspace_files import (
    acquire_sandbox_to_change,
    held_workspace_row,
    owner_work_dir,
    previous_dir_names_of,
    to_client_path,
)

# Paths per exec. The kernel caps one argument at 128 KiB, and a command is
# one argument once the sandbox wraps it; a few hundred paths stay well under.
_STAT_CHUNK = 200


class NotInStoredCopy(LookupError):
    """A path under a folder the stored copy never keeps (a dependency tree or
    a build output), so whether the file exists is only the computer's to say."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.path = path


def _refuse_unkept(paths: list[str]) -> None:
    for path in paths:
        if not EXCLUDE_DIR_NAMES.isdisjoint(path.split("/")[:-1]):
            raise NotInStoredCopy(path)


class _MirrorReader:
    """Sizes and bytes from the persisted copy, for a workspace that is not running."""

    def __init__(self, workspace: dict[str, Any]) -> None:
        self._workspace = workspace
        self._workspace_id = str(workspace["workspace_id"])
        self._sizes: dict[str, int] | None = None

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]:
        # Answering "not found" would drop a page's libraries without a word.
        _refuse_unkept(paths)
        if self._sizes is None:
            tree = await FilePersistenceService.get_file_tree(self._workspace_id)
            self._sizes = {f["path"]: int(f.get("size") or 0) for f in tree}
        return {p: (p, self._sizes[p]) for p in paths if p in self._sizes}

    async def read(self, path: str) -> bytes | None:
        _refuse_unkept([path])
        # The mime is the serve route's concern; only the bytes are parsed here.
        resolved = await db_fallback_bytes(self._workspace, self._workspace_id, path, "")
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

    def __init__(self, sandbox: Any, work_dir: str) -> None:
        self._sandbox = sandbox
        self._work_dir = work_dir

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
            client = to_client_path(self._sandbox, canonical, self._work_dir)
            found[requested] = (client, size)
        return found

    async def read(self, path: str) -> bytes | None:
        try:
            resolved = await warm_sandbox_bytes(
                self._sandbox, path, work_dir=self._work_dir, visible=may_be_artifact
            )
        except FileTooLargeToServe:
            # Grew past an exec read since it was sized: listed, not parsed.
            return None
        return resolved[1] if resolved else None


class ReadMode(Enum):
    """Where a snapshot may read a workspace's files from."""

    #: The owner's own request: a running computer is acquired as the
    #: owner's file routes acquire it, else the stored copy.
    OWNER = "owner"
    #: A session this worker already holds, as the share and wsfiles routes
    #: read, else the stored copy. Nothing a visitor asks starts a computer.
    WARM_OR_STORED = "warm_or_stored"
    #: A session this worker already holds, or nothing: a stored copy can lag
    #: the computer, and the end of a turn must not cut a version from it.
    WARM_ONLY = "warm_only"


class NoLiveSession(LookupError):
    """``ReadMode.WARM_ONLY`` found no session this worker holds."""


class FileSource(Protocol):
    """``stat`` gives each found path its canonical path and size; ``read`` a
    path's bytes, or None."""

    async def stat(self, paths: list[str]) -> dict[str, tuple[str, int]]: ...

    async def read(self, path: str) -> bytes | None: ...


@dataclass(frozen=True)
class Reader:
    """Where a snapshot reads files, and the folders references fold against."""

    files: FileSource
    work_dir: str
    previous_dirs: tuple[str, ...]


@asynccontextmanager
async def snapshot_reader(
    workspace: dict[str, Any], user_id: str, mode: ReadMode
) -> AsyncIterator[Reader]:
    """A reader for copying files into a version, held until the caller is done.

    Only a running workspace is read live, and a live read holds the folder
    until the caller leaves: a rename settling mid-walk would drop every file
    read after the move.
    """
    workspace_id = str(workspace["workspace_id"])
    if workspace.get("status") == "running":
        if mode is ReadMode.OWNER:
            async with acquire_sandbox_to_change(workspace_id, user_id) as (
                sandbox,
                held,
            ):
                yield _live_reader(sandbox, held)
            return
        sandbox = warm_sandbox(workspace, workspace_id)
        if sandbox is not None:
            async with held_workspace_row(workspace_id) as held:
                yield _live_reader(sandbox, held)
            return
    if mode is ReadMode.WARM_ONLY:
        raise NoLiveSession(workspace_id)
    yield Reader(
        _MirrorReader(workspace),
        owner_work_dir(workspace),
        previous_dir_names_of(workspace),
    )


def _live_reader(sandbox: Any, held: dict[str, Any]) -> Reader:
    work_dir = owner_work_dir(held)
    return Reader(
        _SandboxReader(sandbox, work_dir), work_dir, previous_dir_names_of(held)
    )
